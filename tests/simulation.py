from __future__ import annotations

import heapq
import itertools
import random
from collections.abc import Callable

from mpcomm.model import Address
from mpcomm.transport.messages import Packet, decode

Event = Callable[[], None]


class SimulatedNetwork:
    """Discrete-event network with a virtual clock that loses, duplicates and reorders packets."""

    def __init__(
        self,
        seed: int,
        *,
        loss: float = 0.0,
        duplication: float = 0.0,
        min_delay: float = 0.001,
        max_delay: float = 0.05,
    ) -> None:
        self.now = 0.0
        self.sent = 0
        self.lost = 0
        self._random = random.Random(seed)
        self._loss = loss
        self._duplication = duplication
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._events: list[tuple[float, int, Event]] = []
        self._order = itertools.count()
        self._receivers: dict[Address, Callable[[Packet], None]] = {}

    def clock(self) -> float:
        return self.now

    def attach(self, address: Address, receiver: Callable[[Packet], None]) -> None:
        self._receivers[address] = receiver

    def send(self, data: bytes, address: Address) -> None:
        self.sent += 1
        if self._random.random() < self._loss:
            self.lost += 1
            return
        copies = 2 if self._random.random() < self._duplication else 1
        for _ in range(copies):
            delay = self._random.uniform(self._min_delay, self._max_delay)
            self.at(self.now + delay, lambda: self._receive(data, address))

    def at(self, moment: float, event: Event) -> None:
        heapq.heappush(self._events, (moment, next(self._order), event))

    def every(self, interval: float, event: Event) -> None:
        def tick() -> None:
            event()
            self.at(self.now + interval, tick)

        self.at(self.now + interval, tick)

    def run_until(self, done: Callable[[], bool], limit: float = 300.0) -> bool:
        while self._events and self.now <= limit:
            moment, _, event = heapq.heappop(self._events)
            self.now = moment
            event()
            if done():
                return True
        return done()

    def _receive(self, data: bytes, address: Address) -> None:
        receiver = self._receivers.get(address)
        if receiver is not None:
            receiver(decode(data))
