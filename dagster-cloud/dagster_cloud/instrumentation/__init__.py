from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from typing import Optional, Protocol


class Instrumentation(Protocol):
    def tags(self, tags: list[str]) -> "Instrumentation": ...

    def histogram(self, name: str, value: float) -> None: ...

    def increment(self, name: str) -> None: ...

    def instrument_context(
        self, name: str, buckets_ms: Optional[list[int]]
    ) -> AbstractContextManager[None]: ...


class NoOpInstrumentation(Instrumentation):
    def tags(self, tags: list[str]) -> Instrumentation:
        return self

    def histogram(self, name: str, value: float) -> None:
        pass

    def increment(self, name: str) -> None:
        pass

    @contextmanager
    def instrument_context(
        self, name: str, buckets_ms: Optional[list[int]]
    ) -> Generator[None, None, None]:
        yield
