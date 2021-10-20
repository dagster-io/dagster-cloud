from dagster.utils import merge_dicts
from gql.transport.requests import RequestsHTTPTransport

from ..headers.impl import get_dagster_cloud_api_headers
from ..storage.client import GqlShimClient


def create_cloud_dagit_client(url: str, api_token: str, retries=3):
    transport = RequestsHTTPTransport(
        url=f"{url}/graphql",
        use_json=True,
        headers=merge_dicts(
            {"Content-type": "application/json"},
            get_dagster_cloud_api_headers(api_token),
        ),
        retries=retries,
    )
    return GqlShimClient(transport=transport, fetch_schema_from_transport=True)
