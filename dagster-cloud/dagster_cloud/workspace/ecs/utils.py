import hashlib
import re
from typing import Optional

from dagster_aws.ecs.utils import sanitize_family

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


def get_task_definition_family(
    prefix: str,
    organization_name: Optional[str],
    deployment_name: str,
    location_name: str,
) -> str:
    # Truncate the location name if it's too long (but add a unique suffix at the end so that no matter what it's unique)
    # Relies on the fact that org name and deployment name are always <= 64 characters long to
    # stay well underneath the 255 character limit imposed by ECS
    m = hashlib.sha1()
    m.update(location_name.encode("utf-8"))
    location_name_hash = m.hexdigest()[:8]

    assert len(str(organization_name)) <= 64
    assert len(deployment_name) <= 64
    assert len(prefix) <= 32

    # Make a unique location name that's still under 64 characters
    truncated_location_name = f"{location_name[:55]}_{location_name_hash}"
    assert len(truncated_location_name) <= 64

    final_family = f"{prefix}_{organization_name}_{deployment_name}_{truncated_location_name}"

    assert len(final_family) <= 255

    return sanitize_family(final_family)
