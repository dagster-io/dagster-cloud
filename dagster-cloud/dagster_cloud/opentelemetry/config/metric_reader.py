from dagster import Enum, Field, IntSource, Permissive, Shape

from dagster_cloud.opentelemetry.enum import MetricsReaderEnum

metric_reader_enum = Enum.from_python_enum_direct_values(MetricsReaderEnum)


def metric_reader_config_schema() -> Field:
    return Field(
        config=Shape(
            description="Provide configuration for an OpenTelemetry metric reader.",
            fields={
                "type": Field(
                    config=metric_reader_enum,
                    description="The type of reader to use to capture and export metrics.",
                    is_required=False,
                ),
                "params": Field(
                    description="Provide custom parameters for the OpenTelemetry metric reader.",
                    is_required=False,
                    config=Permissive(
                        description="Provide configuration for the OpenTelemetry metric reader.",
                        fields={
                            "export_interval_millis": Field(
                                IntSource,
                                description="The interval in milliseconds at which to export metrics.",
                                is_required=False,
                            ),
                            "export_timeout_millis": Field(
                                IntSource,
                                description="The timeout in milliseconds for exporting metrics.",
                                is_required=False,
                            ),
                        },
                    ),
                ),
            },
        )
    )
