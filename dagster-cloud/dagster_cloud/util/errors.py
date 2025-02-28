from collections.abc import Sequence
from typing import Optional

from dagster._serdes import serialize_value
from dagster._utils.error import SerializableErrorInfo

ERROR_CLASS_NAME_SIZE_LIMIT = 1000


def unwrap_user_code_error(error_info: SerializableErrorInfo) -> SerializableErrorInfo:
    """Extracts the underlying error from the passed error, if it is a DagsterUserCodeLoadError."""
    if error_info.cls_name == "DagsterUserCodeLoadError":
        return unwrap_user_code_error(error_info.cause)
    return error_info


def truncate_serialized_error(
    error_info: SerializableErrorInfo,
    field_size_limit: int,
    max_depth: int,
    truncations: Optional[list[str]] = None,
):
    truncations = [] if truncations is None else truncations

    if error_info.cause:
        if max_depth == 0:
            truncations.append("cause")
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
                error_info.cause,
                field_size_limit,
                max_depth=max_depth - 1,
                truncations=truncations,
            )
        error_info = error_info._replace(cause=new_cause)

    if error_info.context:
        if max_depth == 0:
            truncations.append("context")
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
                error_info.context,
                field_size_limit,
                max_depth=max_depth - 1,
                truncations=truncations,
            )
        error_info = error_info._replace(context=new_context)

    stack_size_so_far = 0
    truncated_stack = []
    for stack_elem in error_info.stack:
        stack_size_so_far += len(stack_elem)
        if stack_size_so_far > field_size_limit:
            truncations.append("stack")
            truncated_stack.append("(TRUNCATED)")
            break

        truncated_stack.append(stack_elem)

    error_info = error_info._replace(stack=truncated_stack)

    msg_len = len(error_info.message)
    if msg_len > field_size_limit:
        truncations.append(f"message from {msg_len} to {field_size_limit}")
        error_info = error_info._replace(
            message=error_info.message[:field_size_limit] + " (TRUNCATED)"
        )

    if error_info.cls_name and len(error_info.cls_name) > ERROR_CLASS_NAME_SIZE_LIMIT:
        truncations.append("cls_name")
        error_info = error_info._replace(
            cls_name=error_info.cls_name[:ERROR_CLASS_NAME_SIZE_LIMIT] + " (TRUNCATED)"
        )

    return error_info


DAGSTER_FRAMEWORK_SUBSTRINGS = [
    "/site-packages/dagster/",
    "/python_modules/dagster/dagster",
]


def remove_dagster_framework_lines_from_serializable_exc_info(error_info: SerializableErrorInfo):
    return error_info._replace(
        stack=remove_dagster_framework_lines_from_stack_trace(error_info.stack),
        cause=(
            remove_dagster_framework_lines_from_serializable_exc_info(error_info.cause)
            if error_info.cause
            else None
        ),
        context=(
            remove_dagster_framework_lines_from_serializable_exc_info(error_info.context)
            if error_info.context
            else None
        ),
    )


def remove_dagster_framework_lines_from_stack_trace(
    stack: Sequence[str],
) -> Sequence[str]:
    # Remove lines until you find the first non-dagster framework line

    for i in range(len(stack)):
        if not _line_contains_dagster_framework_file(stack[i]):
            return stack[i:]

    # Return the full stack trace if its all Dagster framework lines,
    # to not be left with an empty stack trace
    return stack


def _line_contains_dagster_framework_file(line: str):
    # stack trace line starts with something like
    # File "/usr/local/lib/python3.11/site-packages/dagster/_core/execution/plan/utils.py",
    split_by_comma = line.split(",")
    if not split_by_comma:
        return False

    file_portion = split_by_comma[0]
    return any(
        framework_substring in file_portion for framework_substring in DAGSTER_FRAMEWORK_SUBSTRINGS
    )
