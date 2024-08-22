from dagster._serdes import serialize_value
from dagster._utils.error import SerializableErrorInfo

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
