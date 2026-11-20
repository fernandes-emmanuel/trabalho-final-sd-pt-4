from __future__ import annotations

import itertools
import logging
import statistics
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from boxoffice.domain.commands import AppliedCommand, Command, InvalidCommand, Operation, Outcome
from boxoffice.domain.seats import SeatMap, Venue

log = logging.getLogger(__name__)


class PendingCommand:
    def __init__(self, command: Command) -> None:
        self.command = command
        self.created = time.monotonic()
        self._applied = threading.Event()
        self._outcome: Outcome | None = None

    def resolve(self, outcome: Outcome) -> float:
        self._outcome = outcome
        self._applied.set()
        return time.monotonic() - self.created

    def wait(self, timeout: float | None = None) -> Outcome | None:
        self._applied.wait(timeout)
        return self._outcome


@dataclass(frozen=True, slots=True)
class LatencySummary:
    mean_ms: float = 0.0
    p95_ms: float = 0.0


@dataclass(frozen=True, slots=True)
class ReplicaView:
    owners: Mapping[str, str]
    applied: int
    digest: str


class Replica:
    """Replica of the seat map: applies the commands delivered by group communication and
    tells the local clients the outcome of the commands they issued."""

    def __init__(self, office_id: int, seat_map: SeatMap) -> None:
        self.office_id = office_id
        self._seat_map = seat_map
        self._lock = threading.Lock()
        self._sequence = itertools.count(1)
        self._log: list[AppliedCommand] = []
        self._pending: dict[str, PendingCommand] = {}
        self._latencies: list[float] = []

    @property
    def venue(self) -> Venue:
        return self._seat_map.venue

    def prepare(
        self, operation: Operation, seats: tuple[str, ...], customer: str
    ) -> PendingCommand:
        with self._lock:
            command_id = f"{self.office_id}.{next(self._sequence)}"
            command = Command(command_id, operation, tuple(s.upper() for s in seats), customer)
            pending = PendingCommand(command)
            self._pending[command_id] = pending
        return pending

    def forget(self, pending: PendingCommand) -> None:
        with self._lock:
            self._pending.pop(pending.command.command_id, None)

    def apply_delivered(self, sender: int, payload: Any) -> None:
        try:
            command = Command.from_payload(payload)
        except InvalidCommand as error:
            log.error("ignoring command from office %s: %s", sender, error)
            return
        with self._lock:
            outcome = self._seat_map.apply(command)
            self._log.append(AppliedCommand(command, outcome))
            pending = self._pending.pop(command.command_id, None)
        if pending is not None:
            latency = pending.resolve(outcome)
            with self._lock:
                self._latencies.append(latency)

    def view(self) -> ReplicaView:
        with self._lock:
            return ReplicaView(
                dict(self._seat_map.owners()), len(self._log), self._seat_map.digest()
            )

    def applied(self) -> tuple[AppliedCommand, ...]:
        with self._lock:
            return tuple(self._log)

    def latency(self) -> LatencySummary:
        with self._lock:
            samples = sorted(latency * 1000 for latency in self._latencies)
        if not samples:
            return LatencySummary()
        p95 = samples[min(len(samples) - 1, round(0.95 * (len(samples) - 1)))]
        return LatencySummary(statistics.fmean(samples), p95)
