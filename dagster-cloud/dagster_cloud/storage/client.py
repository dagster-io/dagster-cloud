import logging
import time
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import Any, Dict, Optional

import requests
import urllib3
from dagster import Field, IntSource, Noneable, Permissive, StringSource
from dagster import _check as check
from dagster.utils import merge_dicts
from packaging.version import Version, parse
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3 import Retry

from ..errors import DagsterCloudHTTPError
from ..headers.impl import get_dagster_cloud_api_headers
from ..storage.errors import DagsterCloudMaintenanceException, GraphQLStorageError

DEFAULT_RETRIES = 6
RETRY_BACKOFF_FACTOR = 0.5
DEFAULT_TIMEOUT = 60

logger = logging.getLogger("dagster_cloud")


class GqlShimClient(AbstractContextManager):
    """Adapter for gql.Client that wraps errors in human-readable format."""

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, Any]] = None,
        verify: bool = True,
        retries: int = 1,
        timeout: int = DEFAULT_TIMEOUT,
        cookies: Optional[Dict[str, Any]] = None,
        session: Optional[requests.Session] = None,
    ):
        self._exit_stack = ExitStack()

        self.url = check.str_param(url, "url")
        self.headers = check.opt_dict_param(headers, "headers", key_type=str)
        self.verify = check.bool_param(verify, "verify")
        self.retries = check.int_param(retries, "retries")
        self.timeout = check.opt_int_param(timeout, "timeout")
        self.cookies = check.opt_dict_param(cookies, "cookies", key_type=str)
        self._session = (
            session
            if session
            else self._exit_stack.enter_context(create_cloud_requests_session(self.retries))
        )

    @property
    def session(self) -> requests.Session:
        return self._session

    def execute(self, query: str, variable_values: Dict[str, Any] = None):

        start_time = time.time()

        while True:
            try:
                return self._execute_retry(query, variable_values)
            except DagsterCloudMaintenanceException as e:
                if time.time() - start_time > e.timeout:
                    raise

                logger.warning(
                    "Dagster Cloud is currently unavailable due to scheduled maintenance. Retrying in {retry_interval} seconds...".format(
                        retry_interval=e.retry_interval
                    )
                )
                time.sleep(e.retry_interval)

    def _execute_retry(self, query: str, variable_values: Dict[str, Any] = None):

        post_args = {
            "headers": merge_dicts(self.headers, {"Content-type": "application/json"}),
            "cookies": self.cookies,
            "timeout": self.timeout,
            "verify": self.verify,
            "json": merge_dicts(
                {
                    "query": query,
                },
                {"variables": variable_values} if variable_values else {},
            ),
        }

        try:
            response = self._session.post(self.url, **post_args)
            try:
                result = response.json()
                if not isinstance(result, dict):
                    result = {}
            except ValueError:
                result = {}

            if "errors" not in result and "data" not in result and "maintenance" not in result:
                response.raise_for_status()
                raise requests.HTTPError("Unexpected GraphQL response", response=response)

        except HTTPError as http_error:
            raise DagsterCloudHTTPError(http_error) from http_error
        except Exception as exc:
            raise GraphQLStorageError(exc.__str__()) from exc

        if "maintenance" in result:
            maintenance_info = result["maintenance"]
            raise DagsterCloudMaintenanceException(
                message=maintenance_info.get("message"),
                timeout=maintenance_info.get("timeout"),
                retry_interval=maintenance_info.get("retry_interval"),
            )

        if "errors" in result:
            raise GraphQLStorageError(f"Error in GraphQL response: {str(result['errors'])}")
        else:
            return result

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception_value, _traceback):
        self._exit_stack.close()


def get_agent_headers(config_value: Dict[str, Any]):
    return get_dagster_cloud_api_headers(
        config_value["agent_token"],
        deployment_name=config_value.get("deployment"),
        additional_headers=config_value.get("headers"),
    )


@contextmanager
def create_cloud_requests_session(retries: int):
    with requests.Session() as session:
        urllib_version = parse(urllib3.__version__)  # type: ignore[attr-defined]
        # method for whitelisting all methods (GET/POST/etc.) is moving from method_whitelist
        # to allowed_methods
        allowed_method_param = (
            {"allowed_methods": None}
            if urllib_version >= Version("1.26.0")
            else {"method_whitelist": None}
        )

        adapter = HTTPAdapter(
            max_retries=Retry(  # pylint: disable=unexpected-keyword-arg
                total=retries,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=[500, 502, 503, 504],
                **allowed_method_param,  # type: ignore
            )
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        yield session


def create_proxy_client(
    url: str, config_value: Dict[str, Any], session: Optional[requests.Session] = None
):
    return GqlShimClient(
        url=url,
        headers=merge_dicts({"Content-type": "application/json"}, get_agent_headers(config_value)),
        verify=config_value.get("verify", True),
        retries=config_value.get("retries", DEFAULT_RETRIES),
        timeout=config_value.get("timeout", DEFAULT_TIMEOUT),
        cookies=config_value.get("cookies", {}),
        session=session,
    )


def dagster_cloud_api_config():
    return {
        "url": Field(StringSource, is_required=False),
        "agent_token": Field(StringSource, is_required=True),
        "deployment": Field(StringSource, is_required=False),
        "headers": Field(Permissive(), default_value={}),
        "cookies": Field(Permissive(), default_value={}),
        "timeout": Field(Noneable(IntSource), default_value=DEFAULT_TIMEOUT),
        "verify": Field(bool, default_value=True),
        # This may be how we want to implement our retry policy, but it may be too restrictive:
        # we may want to put this logic into the storage itself so that we can do some kind of
        # logging
        "retries": Field(IntSource, default_value=DEFAULT_RETRIES),
        "method": Field(StringSource, default_value="POST"),
        "agent_label": Field(StringSource, is_required=False),
    }
