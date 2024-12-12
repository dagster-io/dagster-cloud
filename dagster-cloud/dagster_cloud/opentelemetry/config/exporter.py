from dagster import BoolSource, Enum, Field, IntSource, Map, Permissive, Shape, StringSource

from ..enum import (
    AggregationTemporalityEnum,
    CompressionEnum,
    LoggingExporterEnum,
    MetricsExporterEnum,
    MetricsInstrumentTypesEnum,
    TracingExporterEnum,
    ViewAggregationEnum,
)

# Convert Python to Dagster enums
compression_enum = Enum.from_python_enum_direct_values(CompressionEnum)
logging_exporter_enum = Enum.from_python_enum_direct_values(LoggingExporterEnum)
metrics_exporter_enum = Enum.from_python_enum_direct_values(MetricsExporterEnum)
tracing_exporter_enum = Enum.from_python_enum_direct_values(TracingExporterEnum)

aggregation_temporality_enum = Enum.from_python_enum_direct_values(AggregationTemporalityEnum)
metrics_instrument_types_enum = Enum.from_python_enum_direct_values(MetricsInstrumentTypesEnum)
view_aggregation_enum = Enum.from_python_enum_direct_values(ViewAggregationEnum)


def exporter_config_schema(pillar: str) -> Shape:
    """Different observability pillars have different supported exporters but share common parameters.
    Valid pillars are: logging, metrics, tracing.
    """
    exporter_type_field: Field
    exporter_extra_params: dict = {}
    if pillar == "logging":
        exporter_type_field = Field(
            config=logging_exporter_enum,
            is_required=False,
            default_value=LoggingExporterEnum.ConsoleLogExporter.value,
        )
    elif pillar == "metrics":
        exporter_type_field = Field(
            config=metrics_exporter_enum,
            is_required=False,
            default_value=MetricsExporterEnum.ConsoleMetricExporter.value,
        )
        exporter_extra_params = {
            "preferred_temporality": Field(
                config=Map(str, aggregation_temporality_enum), is_required=False
            ),
            "preferred_aggregation": Field(
                config=Map(str, view_aggregation_enum), is_required=False
            ),
        }
    elif pillar == "tracing":
        exporter_type_field = Field(
            config=tracing_exporter_enum,
        )
    else:
        raise ValueError(f"Invalid pillar: {pillar}")

    return Shape(
        description="Provide configuration for an OpenTelemetry exporter.",
        fields={
            "type": exporter_type_field,
            "params": Permissive(
                fields={
                    # These fields are common to most exporters. No defaults are provided, to allow either
                    # OpenTelemetry's default or environment variables to be used.
                    # Ref: https://opentelemetry-python.readthedocs.io/en/latest/sdk/environment_variables.html
                    "endpoint": Field(StringSource, is_required=False),
                    "compression": Field(config=compression_enum, is_required=False),
                    "headers": Field(
                        description="Custom headers to send", config=Permissive(), is_required=False
                    ),
                    "certificate_file": Field(StringSource, is_required=False),
                    "client_key_file": Field(StringSource, is_required=False),
                    "client_certificate_file": Field(StringSource, is_required=False),
                    "insecure": Field(BoolSource, is_required=False),
                    "timeout": Field(IntSource, is_required=False),
                    # These fields are specific an exporter.
                    **exporter_extra_params,
                }
            ),
        },
    )
