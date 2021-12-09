from ..headers.impl import get_dagster_cloud_api_headers
from ..storage.client import GqlShimClient


def create_cloud_dagit_client(url: str, api_token: str, retries=3):
    return GqlShimClient(
        url=f"{url}/graphql",
        headers=get_dagster_cloud_api_headers(api_token),
        retries=retries,
    )
