from dagster import Enum, Field, IntSource, Shape

from dagster_cloud.opentelemetry.enum import LogRecordProcessorEnum

log_record_processor_enum = Enum.from_python_enum_direct_values(LogRecordProcessorEnum)


def log_record_processor_schema() -> Shape:
    return Shape(
        description="Provide configuration for the OpenTelemetry log record processor",
        fields={
            "type": Field(
                config=log_record_processor_enum,
                description="The type of log record processor to use.",
                is_required=False,
            ),
            "params": Shape(
                description="The custom parameters for the OpenTelemetry log record processor.",
                fields={
                    # Used by BatchLogRecordProcessor
                    "schedule_delay_millis": Field(
                        IntSource,
                        description="The delay between batches, in milliseconds.",
                        is_required=False,
                    ),
                    "export_timeout_millis": Field(
                        IntSource,
                        description="The timeout for exporting, in milliseconds.",
                        is_required=False,
                    ),
                    "max_export_batch_size": Field(
                        IntSource, description="The maximum batch size.", is_required=False
                    ),
                    "max_queue_size": Field(
                        IntSource, description="The maximum queue size.", is_required=False
                    ),
                },
            ),
        },
    )
