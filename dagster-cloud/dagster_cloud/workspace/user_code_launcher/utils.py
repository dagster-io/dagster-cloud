import hashlib
import uuid


def unique_resource_name(deployment_name, location_name, length_limit, sanitize_fn):
    hash_value = str(uuid.uuid4().hex)[0:6]

    name = f"{location_name}-{deployment_name}"
    sanitized_location_name = sanitize_fn(name) if sanitize_fn else name
    truncated_location_name = sanitized_location_name[: (length_limit - 7)]
    sanitized_unique_name = (
        f"{truncated_location_name}-{hash_value}" if truncated_location_name else hash_value
    )
    assert len(sanitized_unique_name) <= length_limit
    return sanitized_unique_name


def get_human_readable_label(name, length_limit, sanitize_fn):
    truncated_name = name[:length_limit]
    return sanitize_fn(truncated_name) if sanitize_fn else truncated_name


def deterministic_label_for_location(deployment_name: str, location_name: str) -> str:
    """Need a label here that is a unique function of location name since we use it to
    search for existing deployments on update and remove them. Does not need to be human-readable.
    """
    m = hashlib.sha1()  # Creates a 40-byte hash
    m.update(f"{deployment_name}-{location_name}".encode("utf-8"))

    unique_label = m.hexdigest()
    return unique_label
