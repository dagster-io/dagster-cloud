from contextlib import contextmanager
from typing import ContextManager, Generator, List, Optional, Protocol


class Instrumentation(Protocol):
    def tags(self, tags: List[str]) -> "Instrumentation": ...

    def histogram(self, name: str, value: float) -> None: ...

    def increment(self, name: str) -> None: ...

    def instrument_context(
        self, name: str, buckets_ms: Optional[List[int]]
    ) -> ContextManager[None]: ...


class NoOpInstrumentation(Instrumentation):
    def tags(self, tags: List[str]) -> Instrumentation:
        return self

    def histogram(self, name: str, value: float) -> None:
        pass

    def increment(self, name: str) -> None:
        pass

    @contextmanager
    def instrument_context(
        self, name: str, buckets_ms: Optional[List[int]]
    ) -> Generator[None, None, None]:
        yield
