from __future__ import annotations

import hashlib
import json
import string
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from boxoffice.domain.commands import Command, Operation, Outcome

MAX_ROWS = len(string.ascii_uppercase)
MAX_SEATS_PER_ROW = 99


@dataclass(frozen=True, slots=True)
class Venue:
    rows: int
    seats_per_row: int

    def __post_init__(self) -> None:
        if not 1 <= self.rows <= MAX_ROWS:
            raise ValueError(f"rows must be between 1 and {MAX_ROWS}")
        if not 1 <= self.seats_per_row <= MAX_SEATS_PER_ROW:
            raise ValueError(f"seats per row must be between 1 and {MAX_SEATS_PER_ROW}")

    @property
    def row_labels(self) -> str:
        return string.ascii_uppercase[: self.rows]

    @property
    def capacity(self) -> int:
        return self.rows * self.seats_per_row

    def seats(self) -> Iterator[str]:
        for row in self.row_labels:
            for number in range(1, self.seats_per_row + 1):
                yield f"{row}{number}"


class SeatMap:
    """Deterministic state machine: replicas that apply the same commands in the same order
    end up in the same state; the order of conflicting commands decides who gets a seat."""

    def __init__(self, venue: Venue) -> None:
        self.venue = venue
        self._owners: dict[str, str | None] = dict.fromkeys(venue.seats())

    def apply(self, command: Command) -> Outcome:
        if any(seat not in self._owners for seat in command.seats):
            return Outcome.UNKNOWN_SEAT
        match command.operation:
            case Operation.RESERVE | Operation.RESERVE_GROUP:
                return self._reserve(command.seats, command.customer)
            case Operation.CANCEL:
                return self._cancel(command.seats[0], command.customer)

    def owner(self, seat: str) -> str | None:
        return self._owners[seat]

    def owners(self) -> Mapping[str, str]:
        return {seat: owner for seat, owner in self._owners.items() if owner is not None}

    def digest(self) -> str:
        canonical = json.dumps(sorted(self.owners().items()), separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _reserve(self, seats: tuple[str, ...], customer: str) -> Outcome:
        if any(self._owners[seat] is not None for seat in seats):
            return Outcome.SEAT_TAKEN
        for seat in seats:
            self._owners[seat] = customer
        return Outcome.ACCEPTED

    def _cancel(self, seat: str, customer: str) -> Outcome:
        if self._owners[seat] != customer:
            return Outcome.NOT_OWNER
        self._owners[seat] = None
        return Outcome.ACCEPTED
