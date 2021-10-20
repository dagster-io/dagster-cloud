from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import Any, Dict

import requests
from dagster import Field, IntSource, Noneable, Permissive, StringSource
from dagster.utils import merge_dicts
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError

from ..errors import DagsterCloudHTTPError
from ..headers.impl import get_dagster_cloud_api_headers
from ..storage.errors import GraphQLStorageError


class GqlShimClient(AbstractContextManager):
    """Adapter for gql.Client that wraps errors in human-readable format."""

    def __init__(self, transport, fetch_schema_from_transport=True):
        self._exit_stack = ExitStack()

        try:
            # Use persistent connection when making requests
            self._client = self._exit_stack.enter_context(
                Client(transport=transport, fetch_schema_from_transport=fetch_schema_from_transport)
            )
        except HTTPError as e:
            raise DagsterCloudHTTPError(e)
        except Exception as exc:  # pylint: disable=broad-except
            raise GraphQLStorageError(exc.__str__()) from exc

    def execute(self, query: str, variable_values: Dict[str, Any] = None):
        try:
            return {"data": self._client.execute(gql(query), variable_values=variable_values)}
        except HTTPError as http_error:
            raise DagsterCloudHTTPError(http_error)
        except Exception as exc:  # pylint: disable=broad-except
            raise GraphQLStorageError(exc.__str__()) from exc

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
def create_agent_requests_session():
    with requests.Session() as session:
        adapter = HTTPAdapter(
            max_retries=Retry(total=8, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        yield session


def create_proxy_client(url: str, config_value: Dict[str, Any], fetch_schema_from_transport=True):
    transport = RequestsHTTPTransport(
        url=url,
        use_json=True,
        headers=merge_dicts({"Content-type": "application/json"}, get_agent_headers(config_value)),
        verify=config_value.get("verify", True),
        retries=config_value.get("retries", 8),
        method=config_value.get("method", "POST"),
        timeout=config_value.get("timeout", None),
        cookies=config_value.get("cookies", {}),
    )
    return GqlShimClient(
        transport=transport, fetch_schema_from_transport=fetch_schema_from_transport
    )


def dagster_cloud_api_config():
    return {
        "url": Field(StringSource, is_required=True),
        "agent_token": Field(StringSource, is_required=True),
        "deployment": Field(StringSource, is_required=False),
        "headers": Field(Permissive(), default_value={}),
        "cookies": Field(Permissive(), default_value={}),
        "timeout": Field(Noneable(IntSource), default_value=None),
        "verify": Field(bool, default_value=True),
        # This may be how we want to implement our retry policy, but it may be too restrictive:
        # we may want to put this logic into the storage itself so that we can do some kind of
        # logging
        "retries": Field(IntSource, default_value=8),
        "method": Field(StringSource, default_value="POST"),
    }
