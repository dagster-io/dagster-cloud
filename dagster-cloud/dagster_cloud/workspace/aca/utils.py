import re

from dagster_cloud.workspace.user_code_launcher.utils import (
    get_human_readable_label,
    unique_resource_name,
)

# ACA Container App names must be 2-32 characters: lowercase alphanumeric and hyphens.
ACA_NAME_MAX_LEN = 32


def _sanitize_aca_name(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "", name.lower()).strip("-")


def unique_aca_resource_name(deployment_name: str, location_name: str) -> str:
    return unique_resource_name(
        deployment_name,
        location_name,
        length_limit=ACA_NAME_MAX_LEN,
        sanitize_fn=_sanitize_aca_name,
    )


def get_aca_human_readable_label(name: str) -> str:
    return get_human_readable_label(
        name,
        length_limit=ACA_NAME_MAX_LEN,
        sanitize_fn=_sanitize_aca_name,
    )
