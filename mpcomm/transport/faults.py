from __future__ import annotations

import heapq
import itertools
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from mpcomm.model import Address
from mpcomm.transport.channel import Channel


@dataclass(frozen=True, slots=True)
class FaultProfile:
    loss: float = 0.0
    duplication: float = 0.0
    delay_ms: float = 0.0
    jitter_ms: float = 0.0
    seed: int | None = None

    def __post_init__(self) -> None:
        if not 0.0 <= self.loss < 1.0:
            raise ValueError("loss must be in [0, 1)")
        if not 0.0 <= self.duplication <= 1.0:
            raise ValueError("duplication must be in [0, 1]")
        if self.delay_ms < 0 or self.jitter_ms < 0:
            raise ValueError("delay and jitter must be non-negative")

    @property
    def active(self) -> bool:
        return any((self.loss, self.duplication, self.delay_ms, self.jitter_ms))


class FaultyChannel:
    def __init__(self, inner: Channel, profile: FaultProfile) -> None:
        self._inner = inner
        self._profile = profile
        self._random = random.Random(profile.seed)
        self._lock = threading.Lock()
        self._dropped = 0
        self._scheduler = _DelayScheduler(inner.send)

    @property
    def dropped(self) -> int:
        with self._lock:
            return self._dropped

    def send(self, data: bytes, address: Address) -> None:
        with self._lock:
            if self._random.random() < self._profile.loss:
                self._dropped += 1
                return
            copies = 2 if self._random.random() < self._profile.duplication else 1
            delays = [self._sample_delay() for _ in range(copies)]
        for delay in delays:
            if delay > 0:
                self._scheduler.schedule(delay, data, address)
            else:
                self._inner.send(data, address)

    def close(self) -> None:
        self._scheduler.close()

    def _sample_delay(self) -> float:
        delay_ms = self._random.gauss(self._profile.delay_ms, self._profile.jitter_ms)
        return max(0.0, delay_ms) / 1000


class _DelayScheduler:
    def __init__(self, send: Callable[[bytes, Address], None]) -> None:
        self._send = send
        self._queue: list[tuple[float, int, bytes, Address]] = []
        self._order = itertools.count()
        self._condition = threading.Condition()
        self._closed = False
        self._worker = threading.Thread(target=self._run, name="delay-scheduler", daemon=True)
        self._worker.start()

    def schedule(self, delay: float, data: bytes, address: Address) -> None:
        with self._condition:
            heapq.heappush(
                self._queue, (time.monotonic() + delay, next(self._order), data, address)
            )
            self._condition.notify()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        self._worker.join()

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._closed and not self._has_due_packet():
                    self._condition.wait(self._time_to_next_packet())
                if self._closed:
                    return
                _, _, data, address = heapq.heappop(self._queue)
            self._send(data, address)

    def _has_due_packet(self) -> bool:
        return bool(self._queue) and self._queue[0][0] <= time.monotonic()

    def _time_to_next_packet(self) -> float | None:
        if not self._queue:
            return None
        return max(0.0, self._queue[0][0] - time.monotonic())
