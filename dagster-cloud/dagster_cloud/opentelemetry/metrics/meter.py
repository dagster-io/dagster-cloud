from collections.abc import Generator, Iterable, Sequence
from typing import Callable, Optional, Union, cast

# (Gauge is only exposed as a private class)
from opentelemetry.metrics import (
    CallbackOptions,
    Counter,
    Histogram,
    Instrument,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    Observation,
    UpDownCounter,
)
from opentelemetry.metrics._internal import Meter as SDKMeter

InstrumentKeyType = tuple[str, str, str]
ObservableCallback = Callable[[CallbackOptions], Iterable[Observation]]
ObservableGenerator = Generator[Iterable[Observation], CallbackOptions, None]
ObserverType = Union[ObservableCallback, ObservableGenerator]


class Meter:
    def __init__(self, meter: SDKMeter):
        self._meter = meter
        self._instruments: dict[InstrumentKeyType, Instrument] = {}

    @property
    def name(self) -> str:
        return self._meter.name

    @property
    def version(self) -> Optional[str]:
        return self._meter.version

    @property
    def schema_url(self) -> Optional[str]:
        return self._meter.schema_url

    def get_counter(self, name: str, description: str = "", unit: str = "") -> Counter:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_counter(
                name=name,
                description=description,
                unit=unit,
            )
            self._instruments[key] = instrument

        return cast(Counter, instrument)

    def get_up_down_counter(
        self, name: str, description: str = "", unit: str = ""
    ) -> UpDownCounter:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_up_down_counter(
                name=name,
                description=description,
                unit=unit,
            )
            self._instruments[key] = instrument

        return cast(UpDownCounter, instrument)

    def get_histogram(self, name: str, description: str = "", unit: str = "") -> Histogram:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_histogram(
                name=name,
                description=description,
                unit=unit,
            )
            self._instruments[key] = instrument

        return cast(Histogram, instrument)

    def get_observable_counter(
        self,
        name: str,
        description: str = "",
        unit: str = "",
        observers: Optional[Sequence[ObserverType]] = None,
    ) -> ObservableCounter:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_observable_counter(
                name=name,
                description=description,
                unit=unit,
                callbacks=observers,
            )
            self._instruments[key] = instrument

        return cast(ObservableCounter, instrument)

    def get_observable_up_down_counter(
        self,
        name: str,
        description: str = "",
        unit: str = "",
        observers: Optional[Sequence[ObserverType]] = None,
    ) -> ObservableUpDownCounter:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_observable_up_down_counter(
                name=name,
                description=description,
                unit=unit,
                callbacks=observers,
            )
            self._instruments[key] = instrument

        return cast(ObservableUpDownCounter, instrument)

    def get_observable_gauge(
        self,
        name: str,
        description: str = "",
        unit: str = "",
        observers: Optional[Sequence[ObserverType]] = None,
    ) -> ObservableGauge:
        key = (name, description, unit)
        instrument = self._instruments.get(key)
        if not instrument:
            instrument = self._meter.create_observable_gauge(
                name=name,
                description=description,
                unit=unit,
                callbacks=observers,
            )
            self._instruments[key] = instrument

        return cast(ObservableGauge, instrument)
