import logging
import os
from concurrent.futures import Future, TimeoutError
from contextlib import contextmanager
from queue import Empty, Full, Queue
from threading import Lock
from typing import Callable, Generator, Generic, List, Optional, Tuple, TypeVar

import dagster._check as check

from dagster_cloud.instrumentation import Instrumentation, NoOpInstrumentation

logger = logging.getLogger(__name__)

I = TypeVar("I")  # noqa: E741
O = TypeVar("O")  # noqa: E741
QueueItem = Tuple[I, Future[O]]


DEFAULT_MAX_WAIT_MS = 1000
DEFAULT_MAX_BATCH_SIZE = 100
DEFAULT_MAX_QUEUE_SIZE = 1000


def _get_default_for_name(setting: str, name: str) -> Optional[int]:
    env_name = f"DAGSTER_BATCHING__{name.upper().replace('-', '_')}__{setting.upper()}"
    value = os.getenv(env_name)
    if value is None:
        return None

    try:
        value_int = int(value)
        if value_int <= 0:
            logger.warning(
                f"Environment variable misconfiguration for {env_name} (should be positive int, got: '{value}')"
            )
            return None
        return value_int
    except ValueError:
        logger.warning(
            f"Environment variable misconfiguration for {env_name} (should be positive int, got: '{value}')"
        )
        return None


class Batcher(Generic[I, O]):
    """the basic algorithm is.

    1. insert (item, future) into queue
    2. wait for future to complete, with max timeout
      2a. if future completes, return result
      2b. on timeout, acquire lock, then drain the queue until
          the future completes

    NOTE: if the queue is full, submit() will raise an exception
    NOTE: the lock means that only one thread will ever be running the batcher_fn
          at a time. the algorithm would still be correct without the lock but
          locking leads to larger batches. HOWEVER without the lock we might try
          to submit empty batches, which there is currently an invariant to protect
          against
    NOTE: the max queue size is meant to cap the number of inflight requests
          in order to fail faster if the underlying function is taking too long
          (database issues).
    """

    def __init__(
        self,
        name: str,
        batcher_fn: Callable[[List[I]], List[O]],
        max_queue_size: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        max_wait_ms: Optional[int] = None,
        instrumentation: Optional[Instrumentation] = None,
    ) -> None:
        check.invariant(
            max_wait_ms is None or max_wait_ms > 0,
            "max wait, if provided, must be set to a positive integer",
        )
        check.invariant(
            max_queue_size is None or max_queue_size > 0,
            "max queue size, if provided, must be set to a positive integer",
        )
        check.invariant(
            max_batch_size is None or max_batch_size > 0,
            "max batch size, if provided, must be set to a positive integer",
        )
        if max_queue_size and max_batch_size:
            check.invariant(
                max_batch_size <= max_queue_size,
                "if max batch size and max queue size are provided, max batch size must be "
                "less than or equal to max queue size",
            )
        self._name = name
        self._batcher_fn = batcher_fn
        self._max_batch_size = (
            max_batch_size
            or _get_default_for_name("max_batch_size", name)
            or DEFAULT_MAX_BATCH_SIZE
        )
        self._max_wait_ms: float = (
            max_wait_ms or _get_default_for_name("max_wait_ms", name) or DEFAULT_MAX_WAIT_MS
        )
        self._queue: Queue[QueueItem] = Queue(
            maxsize=max_queue_size
            or _get_default_for_name("max_queue_size", name)
            or DEFAULT_MAX_QUEUE_SIZE
        )
        self._drain_lock = Lock()
        self._instrumentation = instrumentation or NoOpInstrumentation()

    def _submit_batch(self, batch: List[QueueItem]) -> None:
        check.invariant(len(batch) > 0, "should never submit an empty batch")
        self._instrument_batch_size(len(batch))
        try:
            with self._time("batcher_fn"):
                results = self._batcher_fn([i for i, _ in batch])
        except Exception as e:
            for _, fut in batch:
                fut.set_exception(e)
        else:
            check.invariant(
                len(results) == len(batch), "batcher returned fewer results than expected"
            )
            for (_, fut), result in zip(batch, results):
                fut.set_result(result)

    def _build_batch(self) -> List[QueueItem]:
        batch = []
        for i in range(self._max_batch_size):
            try:
                batch.append(self._queue.get(block=False))
            except Empty:
                break
        return batch

    @contextmanager
    def _lock(self):
        with self._time("lock_acquisition"):
            self._drain_lock.acquire()
        try:
            yield
        finally:
            self._drain_lock.release()

    def _drain_batch(self, fut: Future[O]) -> O:
        with self._lock(), self._time("drain_batch"):
            while not fut.done():
                self._submit_batch(self._build_batch())
            return fut.result()

    def submit(self, i: I) -> O:
        with self._time("submit"):
            fut: Future[O] = Future()
            try:
                self._queue.put((i, fut), block=False)
            except Full:
                self._instrumentation.increment(f"dagster.batching.{self._name}.full")
                raise
            else:
                try:
                    queue_size = self._queue.qsize()
                    self._instrument_queue_size(queue_size)
                    timeout = 0 if queue_size >= self._max_batch_size else self._max_wait_ms / 1000
                    return fut.result(timeout=timeout)
                except TimeoutError:
                    self._instrumentation.increment(f"dagster.batching.{self._name}.timeout")
                    self._drain_batch(fut)
                    return fut.result()

    def _instrument_queue_size(self, queue_size: int) -> None:
        self._instrumentation.histogram(f"dagster.batching.{self._name}.queue_size", queue_size)
        for bucket in [5, 10, 100]:
            if queue_size >= bucket:
                self._instrumentation.increment(
                    f"dagster.batching.{self._name}.queue_size.ge_{bucket}"
                )
            else:
                break

    def _instrument_batch_size(self, batch_size: int) -> None:
        self._instrumentation.histogram(f"dagster.batching.{self._name}.batch_size", batch_size)
        for bucket in [5, 10, 100]:
            if batch_size >= bucket:
                self._instrumentation.increment(
                    f"dagster.batching.{self._name}.batch_size.ge_{bucket}"
                )
            else:
                break

    @contextmanager
    def _time(self, metric_name: str) -> Generator[None, None, None]:
        with self._instrumentation.instrument_context(
            f"dagster.batching.{self._name}.{metric_name}",
            buckets_ms=[10, 100, 500, 1000],
        ):
            yield
