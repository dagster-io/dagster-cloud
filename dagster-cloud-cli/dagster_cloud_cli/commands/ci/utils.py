import os
from contextlib import contextmanager
from typing import Any, Optional

from dagster_cloud_cli import gql


@contextmanager
def client_from_env(url: str, deployment: Optional[str] = None):
    if deployment:
        url = url + "/" + deployment
    with gql.graphql_client_from_url(
        url, os.environ["DAGSTER_CLOUD_API_TOKEN"], deployment_name=deployment
    ) as client:
        yield client


def get_registry_info(url: str) -> dict[str, Any]:
    with client_from_env(url) as client:
        return gql.get_ecr_info(client)
