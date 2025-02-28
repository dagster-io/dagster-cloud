from typing import Optional

from opentelemetry._logs import LoggerProvider as APILoggerProvider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, LogRecordProcessor
from opentelemetry.sdk._logs.export import LogExporter
from opentelemetry.sdk.resources import Resource

from dagster_cloud.opentelemetry.enum import LoggingExporterEnum, LogRecordProcessorEnum


def build_log_exporter(exporter_config: dict) -> LogExporter:
    """Build an OpenTelemetry log exporter.
    params:
        exporter_config: dict: The exporter configuration.

    Returns:
        LogExporter: The log exporter instance.
    """
    exporter_type = exporter_config.get("type", LoggingExporterEnum.ConsoleLogExporter.value)
    exporter_params = exporter_config.get("params", {})

    if exporter_type == LoggingExporterEnum.HttpProtoOTLPExporter.value:
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

        return OTLPLogExporter(**exporter_params)
    elif exporter_type == LoggingExporterEnum.GrpcProtoOTLPExporter.value:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

        return OTLPLogExporter(**exporter_params)
    elif exporter_type == LoggingExporterEnum.ConsoleLogExporter.value:
        from opentelemetry.sdk._logs.export import ConsoleLogExporter

        return ConsoleLogExporter(**exporter_params)

    raise ValueError(f"Invalid exporter type: {exporter_type}")


def build_log_record_processor(exporter: LogExporter, processor_config: dict) -> LogRecordProcessor:
    """Build an OpenTelemetry log record processor.
    params:
        processor_config: dict: The processor configuration.

    Returns:
        LogRecordProcessor: The log record processor instance.
    """
    processor_type = processor_config.get(
        "type", LogRecordProcessorEnum.SimpleLogRecordProcessor.value
    )
    processor_params = processor_config.get("params", {})

    if processor_type == LogRecordProcessorEnum.BatchLogRecordProcessor.value:
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

        return BatchLogRecordProcessor(exporter, **processor_params)
    elif processor_type == LogRecordProcessorEnum.SimpleLogRecordProcessor.value:
        from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor

        return SimpleLogRecordProcessor(exporter)

    raise ValueError(f"Invalid log record processor type: {processor_type}")


def build_logger_provider(
    processor: LogRecordProcessor,
    resource_attributes: Optional[dict[str, str]] = None,
) -> APILoggerProvider:
    """Build an OpenTelemetry logger provider.
    params:
        processor: LogRecordProcessor: The log record processor instance.
        resource: Resource (optional): The resource instance.

    Returns:
        LoggerProvider: The logger provider instance.
    """
    from opentelemetry.sdk._logs import LoggerProvider

    resource = (
        Resource.create(attributes=resource_attributes)
        if resource_attributes
        else Resource.create({})
    )

    logger_provider = LoggerProvider(
        resource=resource,
        shutdown_on_exit=False,
    )
    logger_provider.add_log_record_processor(processor)
    return logger_provider


def build_logging_handler(
    logger_provider: LoggerProvider, handler_config: Optional[dict] = None
) -> LoggingHandler:
    """Build an OpenTelemetry logging handler.
    Params:
        logger_provider (LoggerProvider): The logger provider instance.
        handler_config (Optional[dict]): The handler configuration.

    Returns:
        LoggingHandler: The logging handler instance.
    """
    from opentelemetry.sdk._logs import LoggingHandler

    handler_config = handler_config or {}

    return LoggingHandler(logger_provider=logger_provider, **handler_config)


def build_noop_logger_provider() -> APILoggerProvider:
    """Build a no-op OpenTelemetry meter provider."""
    from opentelemetry._logs import NoOpLoggerProvider

    return NoOpLoggerProvider()
