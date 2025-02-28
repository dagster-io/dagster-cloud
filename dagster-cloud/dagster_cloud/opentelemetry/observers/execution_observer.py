from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Callable, Optional

from dagster import DagsterError
from dagster._time import get_current_timestamp

from ..controller import OpenTelemetryController
from .dagster_exception_handler import extract_dagster_error_attributes

ResultEvaluatorCallback = Callable[..., str]


class ExecutionObserverInstruments:
    """OpenTelemetry instruments used to record generic code block executions."""

    def __init__(
        self, opentelemetry: OpenTelemetryController, metric_base_name: str, description: str
    ):
        self._meter = opentelemetry.get_meter(
            name=metric_base_name,
            attributes={
                "description": description,
            },
        )

        self._executions = self._meter.get_counter(
            name=metric_base_name,
            description=f"Number of {description}",
        )

        self._completions = self._meter.get_counter(
            name=f"{metric_base_name}.completions",
            description=f"Number of {description} completions by status",
        )

        self._exceptions = self._meter.get_counter(
            name=f"{metric_base_name}.exceptions",
            description=f"Number of exceptions caught in {description}",
        )

        self._duration = self._meter.get_histogram(
            name=f"{metric_base_name}.duration",
            description=f"Duration of {description}",
            unit="seconds",
        )

    @property
    def meter(self):
        """Get the OpenTelemetry meter. This is useful for adding custom metrics."""
        return self._meter

    @property
    def executions(self):
        """Get the OpenTelemetry counter for the number of executions of the observed code block."""
        return self._executions

    @property
    def completions(self):
        """Get the OpenTelemetry counter for the number of completions of the observed code block."""
        return self._completions

    @property
    def exceptions(self):
        """Get the OpenTelemetry counter for the number of exceptions caught in the observed code block."""
        return self._exceptions

    @property
    def duration(self):
        """Get the OpenTelemetry gauge for the duration of the observed code block."""
        return self._duration


class ExecutionResultObserver(ABC):
    @abstractmethod
    def evaluate_result(self, *args, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")


class _NoopExecutionResultObserver(ExecutionResultObserver):
    """A no-op observer that does not record any metrics."""

    def evaluate_result(self, *args, **kwargs):
        pass


class _DagsterExecutionResultObserver(ExecutionResultObserver):
    def __init__(
        self,
        instruments: ExecutionObserverInstruments,
        attributes: dict[str, str],
        result_evaluator_callback: Optional[ResultEvaluatorCallback] = None,
    ):
        self._result_evaluator_callback = result_evaluator_callback
        self._instruments = instruments
        self._attributes = attributes or {}
        self._status = None

    @property
    def meter(self):
        """Get the OpenTelemetry meter. This is useful for adding custom metrics."""
        return self._instruments.meter

    def evaluate_result(self, *args, **kwargs):
        """Evaluate the result of the observed code block.

        If a result evaluator callback is present, it will be called with the arguments passed to
        this method, as well as the attributes and instruments passed to the observer.

        If no result evaluator callback is present, the status will be set to "success".
        """
        if self._result_evaluator_callback:
            self._status = self._result_evaluator_callback(
                *args, attributes=self._attributes, instruments=self._instruments, **kwargs
            )
        else:
            self._status = "success"

    def set_status(self, status: str):
        self._status = status

    def get_status(self) -> Optional[str]:
        return self._status


@contextmanager
def observe_execution(
    opentelemetry: Optional[OpenTelemetryController],
    event_key: str,
    short_description: str,
    result_evaluator_callback: Optional[ResultEvaluatorCallback] = None,
    attributes: Optional[dict[str, str]] = None,
):
    """This context manager is used to observe the execution of a code block.

    It records the number of executions, completions, exceptions, and the duration of the code block.
    An optional evaluator callback can be provided to evaluate the result of the code block,
    setting a completion status and also optionally implementing other side effects such as
    recording additional metrics or logging based on the results.
    """
    attributes = attributes or {}
    if opentelemetry and opentelemetry.metrics_enabled:
        instruments = ExecutionObserverInstruments(
            opentelemetry=opentelemetry,
            metric_base_name=event_key,
            description=short_description,
        )
        instruments.executions.add(1, attributes)

        start_time = get_current_timestamp()
        observer = _DagsterExecutionResultObserver(
            instruments=instruments,
            attributes=attributes,
            result_evaluator_callback=result_evaluator_callback,
        )

        try:
            yield observer
        except DagsterError as e:
            err_attributes = extract_dagster_error_attributes(e)
            observer.set_status("exception")
            instruments.exceptions.add(1, attributes={**attributes, **err_attributes})
            raise
        except Exception as e:
            err_attributes = {"exception": type(e).__name__, **attributes}
            observer.set_status("exception")
            instruments.exceptions.add(1, err_attributes)
            raise
        finally:
            instruments.completions.add(
                1, attributes={**attributes, "status": observer.get_status() or "unknown"}
            )
            instruments.duration.record(get_current_timestamp() - start_time, attributes)

    else:
        yield _NoopExecutionResultObserver()
