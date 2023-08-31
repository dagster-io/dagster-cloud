import re

from ..user_code_launcher.utils import unique_resource_name


def unique_docker_resource_name(deployment_name, location_name):
    return unique_resource_name(
        deployment_name,
        location_name,
        length_limit=63,  # Max DNS hostname limit
        sanitize_fn=lambda name: re.sub("[^a-z0-9-]", "", name).strip("-"),
    )
