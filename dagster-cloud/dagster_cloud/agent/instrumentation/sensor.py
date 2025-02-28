from dagster import deserialize_value
from dagster._core.definitions.sensor_definition import SensorExecutionData
from dagster._core.remote_representation import SensorExecutionErrorSnap

from dagster_cloud.opentelemetry.observers.execution_observer import ExecutionObserverInstruments


def inspect_sensor_result(
    serialized_data_or_error: str,
    instruments: ExecutionObserverInstruments,
    attributes: dict[str, str],
) -> str:
    run_requests = []
    status: str = "unknown"
    try:
        evaluation_execution_data = deserialize_value(serialized_data_or_error)
        if isinstance(evaluation_execution_data, SensorExecutionData):
            run_requests = evaluation_execution_data.run_requests or []
            if run_requests:
                status = "success"
            else:
                status = "skipped"
        elif isinstance(evaluation_execution_data, SensorExecutionErrorSnap):
            status = "error"
    finally:
        if run_requests:
            meter = instruments.meter
            counter = meter.get_counter(
                name=f"{meter.name}.run_requests",
                description="Number of run requests triggered by sensors",
            )
            counter.add(len(run_requests), attributes)

    return status
