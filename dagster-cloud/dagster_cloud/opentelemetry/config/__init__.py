from dagster import BoolSource, Field, Shape, StringSource

from dagster_cloud.opentelemetry.config.exporter import exporter_config_schema
from dagster_cloud.opentelemetry.config.log_record_processor import log_record_processor_schema
from dagster_cloud.opentelemetry.config.logging_handler import logging_handler_schema
from dagster_cloud.opentelemetry.config.meter_provider import meter_provider_config_schema
from dagster_cloud.opentelemetry.config.metric_reader import metric_reader_config_schema


def opentelemetry_config_schema():
    return {
        "enabled": Field(
            BoolSource,
            description="Enables OpenTelemetry instrumentation.",
            is_required=False,
            default_value=True,
        ),
        "service_name": Field(
            StringSource,
            description="Name of the service to which to send telemetry",
            is_required=False,
            default_value="dagster-cloud-agent",
        ),
        "logging": Field(
            description="The logging configuration.",
            is_required=False,
            config=Shape(
                fields={
                    "enabled": Field(
                        BoolSource,
                        description="Enables logging instrumentation.",
                        is_required=False,
                        default_value=False,
                    ),
                    "exporter": exporter_config_schema("logging"),
                    "processor": log_record_processor_schema(),
                    "handler": logging_handler_schema(),
                },
            ),
        ),
        "metrics": Field(
            description="The metrics configuration.",
            is_required=False,
            config=Shape(
                fields={
                    "enabled": Field(
                        BoolSource,
                        description="Enables metrics instrumentation.",
                        is_required=False,
                        default_value=False,
                    ),
                    "exporter": exporter_config_schema("metrics"),
                    "reader": metric_reader_config_schema(),
                    "provider": meter_provider_config_schema(),
                },
            ),
        ),
        # TODO: Add tracing configuration when it's supported.
        # "tracing": Field(
        #     description="The tracing configuration.",
        #     config=Shape(
        #         fields={
        #             "enabled": Field(
        #                 BoolSource,
        #                 description="Enables tracing instrumentation.",
        #                 is_required=False,
        #                 default_value=False,
        #             ),
        #             "exporter": exporter_config_schema("tracing"),
        #         },
        #     ),
        # ),
    }
