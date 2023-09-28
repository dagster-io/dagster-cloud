import logging
import re
import time
from contextlib import ExitStack, contextmanager
from email.utils import mktime_tz, parsedate_tz
from typing import Any, Dict, Mapping, Optional

import dagster._check as check
import requests
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
    HTTPError,
    ReadTimeout as RequestsReadTimeout,
)

from .errors import DagsterCloudHTTPError, DagsterCloudMaintenanceException, GraphQLStorageError
from .headers.auth import DagsterCloudInstanceScope
from .headers.impl import get_dagster_cloud_api_headers

DEFAULT_RETRIES = 6
RETRY_BACKOFF_FACTOR = 0.5
DEFAULT_TIMEOUT = 60

logger = logging.getLogger("dagster_cloud")


RETRY_STATUS_CODES = [
    # retry on server errors to recover on transient issue
    500,
    502,
    503,
    504,
    429,
]


class GqlShimClient:
    """Adapter for gql.Client that wraps errors in human-readable format."""

    def __init__(
        self,
        url: str,
        session: requests.Session,
        headers: Optional[Dict[str, Any]] = None,
        verify: bool = True,
        timeout: int = DEFAULT_TIMEOUT,
        cookies: Optional[Dict[str, Any]] = None,
        proxies: Optional[Dict[str, Any]] = None,
        max_retries: int = 0,
    ):
        self._exit_stack = ExitStack()

        self.url = url
        self.headers = headers
        self.verify = verify
        self.timeout = timeout
        self.cookies = cookies
        self._session = session
        self._proxies = proxies
        self._max_retries = max_retries

    @property
    def session(self) -> requests.Session:
        return self._session

    def execute(
        self,
        query: str,
        variable_values: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        idempotent_mutation: bool = False,
    ):
        start_time = time.time()
        retry_number = 0
        error_msg_set = set()
        requested_sleep_time = None
        while True:
            try:
                return self._execute_retry(query, variable_values, headers)
            except (HTTPError, RequestsConnectionError, RequestsReadTimeout) as e:
                retryable_error = False
                if isinstance(e, HTTPError):
                    retryable_error = e.response.status_code in RETRY_STATUS_CODES
                    error_msg = e.response.status_code
                    requested_sleep_time = _get_retry_after_sleep_time(e.response.headers)
                elif isinstance(e, RequestsReadTimeout):
                    # "mutation " must appear in the document if its a mutation
                    if "mutation " in query and not idempotent_mutation:
                        # mutations can be made idempotent if they use Idempotency-Key header
                        retryable_error = (
                            bool(headers.get("Idempotency-Key")) if headers is not None else False
                        )
                    # otherwise assume its a query that is naturally idempotent
                    else:
                        retryable_error = True

                    error_msg = str(e)
                else:
                    retryable_error = True
                    error_msg = str(e)

                error_msg_set.add(error_msg)
                if retryable_error and retry_number < self._max_retries:
                    retry_number += 1
                    sleep_time = 0
                    if requested_sleep_time:
                        sleep_time = requested_sleep_time
                    elif retry_number > 1:
                        sleep_time = RETRY_BACKOFF_FACTOR * (2 ** (retry_number - 1))

                    if sleep_time > 0:
                        logger.warning(
                            f"Error in Dagster Cloud request ({error_msg}). Retrying in"
                            f" {sleep_time} seconds..."
                        )
                        time.sleep(sleep_time)
                    else:
                        logger.warning(
                            f"Error in Dagster Cloud request ({error_msg}). Retrying now."
                        )
                else:
                    # Throw the error straight if no retries were involved
                    if self._max_retries == 0 or not retryable_error:
                        if isinstance(e, HTTPError):
                            raise DagsterCloudHTTPError(e) from e
                        else:
                            raise GraphQLStorageError(str(e)) from e
                    else:
                        if len(error_msg_set) == 1:
                            status_code_msg = str(next(iter(error_msg_set)))
                        else:
                            status_code_msg = str(error_msg_set)
                        raise GraphQLStorageError(
                            f"Max retries ({self._max_retries}) exceeded, too many"
                            f" {status_code_msg} error responses."
                        ) from e
            except DagsterCloudMaintenanceException as e:
                if time.time() - start_time > e.timeout:
                    raise

                logger.warning(
                    "Dagster Cloud is currently unavailable due to scheduled maintenance. Retrying"
                    f" in {e.retry_interval} seconds..."
                )
                time.sleep(e.retry_interval)
            except Exception as e:
                raise GraphQLStorageError(str(e)) from e

    def _execute_retry(
        self,
        query: str,
        variable_values: Optional[Mapping[str, Any]],
        headers: Optional[Mapping[str, Any]],
    ):
        response = self._session.post(
            self.url,
            headers={
                **(self.headers if self.headers is not None else {}),
                **(headers if headers is not None else {}),
                "Content-type": "application/json",
            },
            cookies=self.cookies,
            timeout=self.timeout,
            verify=self.verify,
            json={
                "query": query,
                "variables": variable_values if variable_values else {},
            },
            proxies=self._proxies,
        )
        try:
            result = response.json()
            if not isinstance(result, dict):
                result = {}
        except ValueError:
            result = {}

        if "errors" not in result and "data" not in result and "maintenance" not in result:
            response.raise_for_status()
            raise requests.HTTPError("Unexpected GraphQL response", response=response)

        if "maintenance" in result:
            maintenance_info = result["maintenance"]
            raise DagsterCloudMaintenanceException(
                message=maintenance_info.get("message"),
                timeout=maintenance_info.get("timeout"),
                retry_interval=maintenance_info.get("retry_interval"),
            )

        if "errors" in result:
            raise GraphQLStorageError(f"Error in GraphQL response: {result['errors']}")
        else:
            return result


def get_agent_headers(config_value: Dict[str, Any], scope: DagsterCloudInstanceScope):
    return get_dagster_cloud_api_headers(
        config_value["agent_token"],
        scope=scope,
        deployment_name=config_value.get("deployment"),
        additional_headers=config_value.get("headers"),
    )


@contextmanager
def create_graphql_requests_session():
    with requests.Session() as session:
        yield session


def create_proxy_client(
    session: requests.Session,
    url: str,
    config_value: Dict[str, Any],
    scope: DagsterCloudInstanceScope = DagsterCloudInstanceScope.DEPLOYMENT,
):
    return GqlShimClient(
        url=url,
        headers=get_agent_headers(config_value, scope=scope),
        verify=config_value.get("verify", True),
        timeout=config_value.get("timeout", DEFAULT_TIMEOUT),
        cookies=config_value.get("cookies", {}),
        # Requests library modifies proxies dictionary so create a copy
        proxies=(
            check.is_dict(config_value.get("proxies")).copy() if config_value.get("proxies") else {}
        ),
        session=session,
        max_retries=config_value.get("retries", DEFAULT_RETRIES),
    )


def _get_retry_after_sleep_time(headers):
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After

    retry_after = headers.get("Retry-After")
    if retry_after is None:
        return None

    if re.match(r"^\s*[0-9]+\s*$", retry_after):
        seconds = int(retry_after)
    else:
        retry_date = parsedate_tz(retry_after)
        if retry_date is None:
            return None
        retry_date = mktime_tz(retry_date)
        seconds = retry_date - time.time()

    return max(seconds, 0)


@contextmanager
def create_cloud_webserver_client(url: str, api_token: str, retries=3):
    with create_graphql_requests_session() as session:
        yield GqlShimClient(
            session=session,
            url=f"{url}/graphql",
            headers=get_dagster_cloud_api_headers(
                api_token, scope=DagsterCloudInstanceScope.DEPLOYMENT
            ),
            max_retries=retries,
        )
