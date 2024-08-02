import hashlib
import uuid

from dagster._serdes import serialize_value
from dagster._utils.error import SerializableErrorInfo


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


ERROR_CLASS_NAME_SIZE_LIMIT = 1000


def truncate_serialized_error(
    error_info: SerializableErrorInfo, field_size_limit: int, max_depth: int
):
    if error_info.cause:
        if max_depth == 0:
            new_cause = (
                error_info.cause
                if len(serialize_value(error_info.cause)) <= field_size_limit
                else SerializableErrorInfo(
                    message="(Cause truncated due to size limitations)",
                    stack=[],
                    cls_name=None,
                )
            )
        else:
            new_cause = truncate_serialized_error(
                error_info.cause, field_size_limit, max_depth=max_depth - 1
            )
        error_info = error_info._replace(cause=new_cause)

    if error_info.context:
        if max_depth == 0:
            new_context = (
                error_info.context
                if len(serialize_value(error_info.context)) <= field_size_limit
                else SerializableErrorInfo(
                    message="(Context truncated due to size limitations)",
                    stack=[],
                    cls_name=None,
                )
            )
        else:
            new_context = truncate_serialized_error(
                error_info.context, field_size_limit, max_depth=max_depth - 1
            )
        error_info = error_info._replace(context=new_context)

    stack_size_so_far = 0
    truncated_stack = []
    for stack_elem in error_info.stack:
        stack_size_so_far += len(stack_elem)
        if stack_size_so_far > field_size_limit:
            truncated_stack.append("(TRUNCATED)")
            break

        truncated_stack.append(stack_elem)

    error_info = error_info._replace(stack=truncated_stack)

    if len(error_info.message) > field_size_limit:
        error_info = error_info._replace(
            message=error_info.message[:field_size_limit] + " (TRUNCATED)"
        )

    if error_info.cls_name and len(error_info.cls_name) > ERROR_CLASS_NAME_SIZE_LIMIT:
        error_info = error_info._replace(
            cls_name=error_info.cls_name[:ERROR_CLASS_NAME_SIZE_LIMIT] + " (TRUNCATED)"
        )

    return error_info
