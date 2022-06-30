import re

from ..user_code_launcher.utils import get_human_readable_label, unique_resource_name


def unique_ecs_resource_name(deployment_name, location_name):
    return unique_resource_name(
        deployment_name,
        location_name,
        length_limit=60,  # Service discovery name must satisfy DNS
        sanitize_fn=lambda name: re.sub("[^a-z0-9-]", "", name).strip("-"),
    )


def get_ecs_human_readable_label(name):
    return get_human_readable_label(
        name,
        length_limit=60,  # Service discovery name must satisfy DNS
        sanitize_fn=lambda name: re.sub("[^a-z0-9-]", "", name).strip("-"),
    )
