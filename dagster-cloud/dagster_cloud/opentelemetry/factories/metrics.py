from typing import Optional

from opentelemetry.metrics import MeterProvider as APIMeterProvider
from opentelemetry.sdk.metrics.export import MetricExporter, MetricReader

from dagster_cloud.opentelemetry.config.exporter import metrics_instrument_types_enum
from dagster_cloud.opentelemetry.enum import MetricsExporterEnum, MetricsReaderEnum


def _validate_dict_keys_as_metric_instruments(src: dict, dict_name: str) -> None:
    """Validate that all keys in a dictionary are valid metric instrument types."""
    for key in src:
        if key not in metrics_instrument_types_enum.enum_values:
            raise ValueError(
                f"OpenTelemetry configuration error - Invalid instrument type '{key}' in {dict_name}."
            )


def build_metric_exporter(exporter_config: dict) -> MetricExporter:
    """Build an OpenTelemetry metric exporter.
    params:
        exporter: dict: The exporter configuration.
          - type: str: The exporter type.
          - params: dict: The exporter parameters (varies).

    Returns:
        MetricExporter: The metric exporter instance.
    """
    exporter_type = exporter_config.get("type", MetricsExporterEnum.ConsoleMetricExporter.value)
    exporter_params = exporter_config.get("params", {})

    if "preferred_temporality" in exporter_params:
        _validate_dict_keys_as_metric_instruments(
            exporter_params["preferred_temporality"], "preferred_temporality"
        )

    if "preferred_aggregation" in exporter_params:
        _validate_dict_keys_as_metric_instruments(
            exporter_params["preferred_aggregation"], "preferred_aggregation"
        )

    if exporter_type == MetricsExporterEnum.HttpProtoOTLPExporter.value:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

        return OTLPMetricExporter(**exporter_params)
    elif exporter_type == MetricsExporterEnum.GrpcProtoOTLPExporter.value:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        return OTLPMetricExporter(**exporter_params)
    elif exporter_type == MetricsExporterEnum.ConsoleMetricExporter.value:
        from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

        return ConsoleMetricExporter(**exporter_params)

    raise ValueError(f"Invalid exporter type: {exporter_type}")


def build_metric_reader(
    exporter: Optional[MetricExporter] = None, reader_config: Optional[dict] = None
) -> MetricReader:
    """Build an OpenTelemetry metric reader.
    params:
        reader: dict: The reader configuration.

    Returns:
        MetricReader: The metric reader instance.
    """
    reader_config = reader_config or {}
    reader_type = reader_config.get("type", MetricsReaderEnum.PeriodicExportingMetricReader.value)
    reader_params = reader_config.get("params", {})

    if reader_type == MetricsReaderEnum.PeriodicExportingMetricReader.value:
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        if exporter is None:
            raise ValueError("Exporter is required for PeriodicExportingMetricReader.")
        return PeriodicExportingMetricReader(exporter=exporter, **reader_params)
    elif reader_type == MetricsReaderEnum.InMemoryMetricReader.value:
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        return InMemoryMetricReader(**reader_params)

    raise ValueError(f"Invalid metric reader type: {reader_type}")


def build_meter_provider(
    metric_readers: list[MetricReader],
    resource_attributes: Optional[dict[str, str]] = None,
) -> APIMeterProvider:
    """Build an OpenTelemetry meter provider.
    params:
        metric_reader: MetricReader: The metric reader instance.
        resource: Resource (optional): The resource instance.

    Returns:
        MeterProvider: The meter provider instance.
    """
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource

    resource = (
        Resource.create(attributes=resource_attributes)
        if resource_attributes
        else Resource.create({})
    )

    return MeterProvider(
        metric_readers=metric_readers,
        resource=resource,
        shutdown_on_exit=False,
        # TODO - implement support for views?
        # https://opentelemetry-python.readthedocs.io/en/latest/_modules/opentelemetry/sdk/metrics/_internal/view.html#View
        # views=views,
    )


def build_noop_meter_provider() -> APIMeterProvider:
    """Build a no-op OpenTelemetry meter provider."""
    from opentelemetry.metrics import NoOpMeterProvider

    return NoOpMeterProvider()
