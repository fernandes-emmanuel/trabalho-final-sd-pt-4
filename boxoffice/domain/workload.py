from __future__ import annotations

import random
from collections.abc import Mapping
from dataclasses import dataclass

from boxoffice.domain.commands import Operation
from boxoffice.domain.seats import Venue

CUSTOMERS = (
    "ana", "bruno", "carla", "diego", "elisa", "fabio",
    "gabriela", "heitor", "isabela", "joao", "karina", "lucas",
)  # fmt: skip
CANCEL_PROBABILITY = 0.15
GROUP_PROBABILITY = 0.15


@dataclass(frozen=True, slots=True)
class WorkloadSpec:
    commands: int
    seed: int = 0
    hot_seats: int = 4
    hot_probability: float = 0.6
    min_interval_ms: int = 5
    max_interval_ms: int = 40

    def __post_init__(self) -> None:
        if self.commands <= 0:
            raise ValueError("commands must be positive")
        if self.hot_seats <= 0:
            raise ValueError("hot_seats must be positive")
        if not 0.0 <= self.hot_probability <= 1.0:
            raise ValueError("hot_probability must be in [0, 1]")
        if not 0 <= self.min_interval_ms <= self.max_interval_ms:
            raise ValueError("intervals must satisfy 0 <= min <= max")


@dataclass(frozen=True, slots=True)
class Request:
    operation: Operation
    seats: tuple[str, ...]
    customer: str


class WorkloadGenerator:
    """Simulates customers buying tickets at one office, concentrating part of the demand on
    a few disputed seats so that concurrent offices compete for them."""

    def __init__(self, venue: Venue, spec: WorkloadSpec, office_id: int) -> None:
        self._venue = venue
        self._spec = spec
        self._random = random.Random(f"{spec.seed}-{office_id}")
        self._seats = list(venue.seats())
        self._hot = self._seats[: min(spec.hot_seats, len(self._seats))]

    def next_request(self, owners: Mapping[str, str]) -> Request:
        roll = self._random.random()
        if roll < CANCEL_PROBABILITY and owners:
            seat, customer = self._random.choice(sorted(owners.items()))
            return Request(Operation.CANCEL, (seat,), customer)
        customer = self._random.choice(CUSTOMERS)
        if roll < CANCEL_PROBABILITY + GROUP_PROBABILITY and self._venue.seats_per_row > 1:
            return Request(Operation.RESERVE_GROUP, self._adjacent_seats(), customer)
        return Request(Operation.RESERVE, (self._single_seat(),), customer)

    def pause(self) -> float:
        interval = self._random.uniform(self._spec.min_interval_ms, self._spec.max_interval_ms)
        return interval / 1000

    def _single_seat(self) -> str:
        if self._random.random() < self._spec.hot_probability:
            return self._random.choice(self._hot)
        return self._random.choice(self._seats)

    def _adjacent_seats(self) -> tuple[str, ...]:
        size = self._random.randint(2, min(3, self._venue.seats_per_row))
        row = self._random.choice(self._venue.row_labels)
        start = self._random.randint(1, self._venue.seats_per_row - size + 1)
        return tuple(f"{row}{number}" for number in range(start, start + size))
