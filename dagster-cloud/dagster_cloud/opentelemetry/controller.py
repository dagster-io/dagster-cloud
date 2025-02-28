import logging
from collections.abc import Mapping
from threading import Lock
from typing import TYPE_CHECKING, Any, Optional

import dagster_cloud.opentelemetry.factories.logs as logs_factory
import dagster_cloud.opentelemetry.factories.metrics as metrics_factory
from dagster_cloud.opentelemetry.metrics.meter import Meter

if TYPE_CHECKING:
    from opentelemetry._logs import LoggerProvider as APILoggerProvider
    from opentelemetry.metrics import MeterProvider as APIMeterProvider
    from opentelemetry.sdk._logs import LogRecordProcessor
    from opentelemetry.sdk._logs.export import LogExporter
    from opentelemetry.sdk.metrics.export import MetricExporter, MetricReader


DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5_000


class OpenTelemetryController:
    """The OpenTelemetryController manages the initialization and shutdown of OpenTelemetry components.

    Provided with a configuration, the OpenTelemetryController will initialize the logging and metrics components.
    The python logging will be instrumented with OpenTelemetry and captured and exported.

    The get_meter() method allow to obtain a meter than can create metric instruments.
    See: https://opentelemetry.io/docs/languages/python/instrumentation/#metrics

    Note: As some OpenTelemetry pipeline components run their own thread, it's important to shut down properly.

    Examples:
        .. code-block:: python
            import logging
            import sys
            import time

            from dagster_cloud.opentelemetry.controller import OpenTelemetryController

            otel_config = {
                "enabled": True,
                "logging": {
                    "enabled": True,
                    "exporter": {
                        "type": "ConsoleLogExporter",
                    },
                },
                "metrics": {
                    "enabled": True,
                    "reader": {
                        "type": "PeriodicExportingMetricReader",
                        "params": {
                            # this is the interval at which metrics are captured and exported
                            "export_interval_millis": 800,
                        },
                    },
                    "exporter": {
                        "type": "ConsoleMetricExporter",
                    },
                },
            }


            def main():
                logging.basicConfig(level=logging.INFO, stream=sys.stdout)
                logger = logging.getLogger("my_logger")

                logger.info("OpenTelemetry is not started. This log will not be captured by OpenTelemetry.")
                opentelemetry = OpenTelemetryController(
                    instance_id="my_instance",
                    version="0.0.1",
                    config=otel_config,
                    logger=logger,  # this and its children will be instrumented with OpenTelemetry
                )
                logger.info("OpenTelemetry started. This log will be exported by OpenTelemetry.")
                meter = opentelemetry.get_meter(name="item_processor", attributes={"environment": "dev"})
                # the counter should be captured as a metric
                counter = meter.create_counter(
                    name="items_processed", description="The number of items processed.", unit="unit"
                )

                # simulate doing some work, and emitting metrics that will be exported by OpenTelemetry
                counter.add(1)
                time.sleep(1)
                counter.add(1)
                time.sleep(1)

                opentelemetry.dispose()
                logger.info("OpenTelemetry stopped. This log will not be captured by OpenTelemetry.")


            if __name__ == "__main__":
                main()
    """

    def __init__(
        self,
        instance_id: str,
        version: str,
        config: Optional[Mapping[str, Any]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the telemetry instance.
        params:
            instance_id: str: The unique identifier of the service instance.
            version: str: The version of the service.
            config: dict: The configuration for the telemetry components.
            logger: Logger: The logger to use for telemetry logs. Defaults to the root logger.
        """
        self._shutdown = False

        self._log_exporter: Optional[LogExporter] = None
        self._logger_provider: APILoggerProvider = logs_factory.build_noop_logger_provider()
        self._log_record_processor: Optional[LogRecordProcessor] = None
        self._logging_handler: Optional[logging.Handler] = None

        self._metric_exporter: Optional[MetricExporter] = None
        self._metric_reader: Optional[MetricReader] = None
        self._meter_provider: APIMeterProvider = metrics_factory.build_noop_meter_provider()
        self._meters: dict[str, Meter] = {}

        self._config = config or {}

        service_name = self._config["service_name"]

        self._logger = logger or logging.getLogger()
        self._resource_attributes = {
            "service.name": service_name,
            "service.version": version,
            "service.instance.id": instance_id,
        }
        self._lock = Lock()
        self._initialize()

    def _initialize(self):
        """Initialize the OpenTelemetry components."""
        # an initialized controller can be used in code without having to check if it's
        # enabled as the components will default to NoOp implementations if disabled
        if not self.enabled:
            self._logger.debug(
                "OpenTelemetry is disabled by configuration. Telemetry will not be captured."
            )
            return

        self._initialize_logging()
        self._initialize_metrics()
        # TODO - self._initialize_tracing()

    @property
    def enabled(self):
        """Check if the OpenTelemetry instance is enabled. If not enabled,
        all the components will default to NoOp implementations.
        """
        if not self._config:
            return False

        if not self._config.get("enabled", False):
            return False

        # If no specific opentelemetry components are enabled, then we can effectively disable opentelemetry
        return any(
            [
                self._config.get("metrics", {}).get("enabled", False),
                self._config.get("logging", {}).get("enabled", False),
                self._config.get("tracing", {}).get("enabled", False),
            ]
        )

    @property
    def active(self) -> bool:
        """Check if the OpenTelemetry instance is active.

        Returns: bool: True if the instance is active, otherwise False.
        """
        return not self._shutdown

    @property
    def logging_enabled(self) -> bool:
        """Check if the logging component is enabled."""
        return self.enabled and self._config.get("logging", {}).get("enabled", False)

    @property
    def metrics_enabled(self) -> bool:
        """Check if the metrics component is enabled."""
        return self.enabled and self._config.get("metrics", {}).get("enabled", False)

    @property
    def tracing_enabled(self) -> bool:
        """Check if the tracing component is enabled."""
        return self.enabled and self._config.get("tracing", {}).get("enabled", False)

    def dispose(self):
        """Shutdown the OpenTelemetry components."""
        with self._lock:
            if self._shutdown:
                self._logger.warning("OpenTelemetry: Already shutdown, skipping.")
                return

            # TODO - self._shutdown_tracing()
            self._shutdown_metrics()
            self._shutdown_logging()
            self._shutdown = True

    def get_meter(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> Meter:
        """Get or create a meter.

        params:
            name: str: The name of the meter.
            version: str: The version of the meter.
            schema_url: str: The schema URL of the meter.
            attributes: dict: The attributes of the meter.

        Returns: Meter: The meter instance.
        """
        if self._shutdown:
            raise RuntimeError("OpenTelemetry: Cannot get meter, already shutdown.")

        with self._lock:
            # guarding with a lock because concurrent calls to get_meter
            # could attempt to create multiple identical meters
            if name not in self._meters:
                meter = self._meter_provider.get_meter(
                    name=name,
                    version=version,
                    schema_url=schema_url,
                    attributes=attributes,
                )
                self._meters[name] = Meter(meter)
            return self._meters[name]

    def _initialize_logging(self):
        logging_config = self._config.get("logging", {})
        if not logging_config.get("enabled"):
            self._logger.info(
                "OpenTelemetry: Logging instrumentation is disabled by configuration."
            )
            return

        self._log_exporter = logs_factory.build_log_exporter(logging_config.get("exporter"))  # pyright: ignore[reportArgumentType]
        self._log_record_processor = logs_factory.build_log_record_processor(
            self._log_exporter, logging_config.get("processor", {})
        )
        self._logger_provider = logs_factory.build_logger_provider(
            self._log_record_processor, self._resource_attributes
        )
        self._logging_handler = logs_factory.build_logging_handler(
            self._logger_provider,  # pyright: ignore[reportArgumentType]
            logging_config.get("handler", {}),  # pyright: ignore[reportArgumentType]
        )
        self._logger.addHandler(self._logging_handler)

    def _initialize_metrics(self):
        metrics_config = self._config.get("metrics", {})
        if not metrics_config.get("enabled"):
            self._logger.info("OpenTelemetry: Metric instrumentation is disabled by configuration.")
            return

        self._metric_exporter = metrics_factory.build_metric_exporter(
            metrics_config.get("exporter", {})
        )

        self._metric_reader = metrics_factory.build_metric_reader(
            self._metric_exporter, metrics_config.get("reader", {})
        )
        self._meter_provider = metrics_factory.build_meter_provider(
            [self._metric_reader], self._resource_attributes
        )

    def _initialize_tracing(self):
        tracing_config = self._config.get("tracing", {})
        if not tracing_config.get("enabled"):
            self._logger.info(
                "OpenTelemetry: Tracing instrumentation is disabled by configuration."
            )
            return
        raise NotImplementedError("Tracing instrumentation is not yet implemented.")

    def _shutdown_logging(self):
        if self._logger and self._logging_handler:
            self._logging_handler.flush()
            self._logger.removeHandler(self._logging_handler)

        # The LoggerProvider will shut down the record processor, which in turns handles the exporter.
        if self._logger_provider:
            # The NoOpLoggerProvider does not have a force_flush or shutdown method.
            if _validate_object_has_method(self._logger_provider, "force_flush"):
                self._logger_provider.force_flush(timeout_millis=DEFAULT_SHUTDOWN_TIMEOUT_MILLIS)  # type: ignore
            if _validate_object_has_method(self._logger_provider, "shutdown"):
                self._logger_provider.shutdown()  # pyright: ignore[reportAttributeAccessIssue]

    def _shutdown_metrics(self):
        # The meter provider will shut down the reader and the PeriodicExportingMetricReader will handle the exporter.
        # Note: the InMemoryMetricReader will not shut down the exporter, it should only be used for testing and
        # does not accept an exporter and one should not exist.
        if self._meter_provider:
            # Duck typing: The base interface and the NoOpMeterProvider do not have a force_flush or shutdown method.
            if _validate_object_has_method(self._meter_provider, "force_flush"):
                self._meter_provider.force_flush(timeout_millis=DEFAULT_SHUTDOWN_TIMEOUT_MILLIS)  # type: ignore
            if _validate_object_has_method(self._meter_provider, "shutdown"):
                self._meter_provider.shutdown()  # type: ignore
        self._meters = {}

    def _shutdown_tracing(self):
        #  TODO - Tracing instrumentation is not yet implemented.
        pass


def _validate_object_has_method(obj, method_name) -> bool:
    """Check if an object has a callable method.

    Some OpenTelemetry components do not have some methods, so we need to check if they exist before calling them.
    """
    return hasattr(obj, method_name) and callable(getattr(obj, method_name))
