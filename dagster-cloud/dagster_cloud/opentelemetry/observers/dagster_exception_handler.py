from dagster import DagsterError
from dagster._utils.error import SerializableErrorInfo
from grpc import Call as GrpcCall


def extract_dagster_error_attributes(error: DagsterError) -> dict[str, str]:
    """Extracts attributes from a DagsterError that can be used for logging or metrics."""
    error_attributes = {
        "exception": type(error).__name__,
    }

    # DagsterError subtypes often have a field that contains a SerializableErrorInfo object
    # which identify the cause of the error. This field names vary between error types and is not
    # always present, but if they are they often identify the root cause of the error.
    for field in dir(error):
        if field.startswith("__"):
            # Skip magic methods and fields
            continue

        attr = getattr(error, field)
        if type(attr) is list and all(isinstance(item, SerializableErrorInfo) for item in attr):
            serializable_error_infos: list[SerializableErrorInfo] = getattr(error, field)
            for serializable_error_info in serializable_error_infos:
                if serializable_error_info.cause:
                    error_attributes["cause"] = serializable_error_info.cause.cls_name
                    return error_attributes

    # If not obtained from SerializableErrorInfo, use __cause__ to determine the error attributes
    if hasattr(error, "__cause__") and error.__cause__:
        error_cause = error.__cause__
        cause_name = type(error_cause).__name__
        if cause_name:
            error_attributes["cause"] = cause_name

        if isinstance(error_cause, GrpcCall):
            grpc_call: GrpcCall = error_cause
            code = grpc_call.code()
            error_attributes["code"] = code.name

    return error_attributes
