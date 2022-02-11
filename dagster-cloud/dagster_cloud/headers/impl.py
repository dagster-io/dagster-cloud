import platform
from typing import Dict, Optional

from dagster.utils import merge_dicts

from ..auth import API_TOKEN_HEADER, DEPLOYMENT_NAME_HEADER
from ..version import __version__
from .versioning.constants import DAGSTER_CLOUD_VERSION_HEADER, PYTHON_VERSION_HEADER


def get_dagster_cloud_api_headers(
    agent_token: str,
    deployment_name: Optional[str] = None,
    additional_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    return merge_dicts(
        {
            API_TOKEN_HEADER: agent_token,
            PYTHON_VERSION_HEADER: platform.python_version(),
            DAGSTER_CLOUD_VERSION_HEADER: __version__,
        },
        {DEPLOYMENT_NAME_HEADER: deployment_name} if deployment_name else {},
        additional_headers if additional_headers else {},
    )
