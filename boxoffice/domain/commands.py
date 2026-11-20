from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Operation(StrEnum):
    RESERVE = "reserve"
    RESERVE_GROUP = "reserve_group"
    CANCEL = "cancel"


class Outcome(StrEnum):
    ACCEPTED = "accepted"
    SEAT_TAKEN = "seat_taken"
    NOT_OWNER = "not_owner"
    UNKNOWN_SEAT = "unknown_seat"


class InvalidCommand(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Command:
    command_id: str
    operation: Operation
    seats: tuple[str, ...]
    customer: str

    def __post_init__(self) -> None:
        if not self.customer:
            raise InvalidCommand("customer is required")
        if not self.seats:
            raise InvalidCommand("at least one seat is required")
        if len(set(self.seats)) != len(self.seats):
            raise InvalidCommand("seats must not repeat")
        if self.operation is not Operation.RESERVE_GROUP and len(self.seats) != 1:
            raise InvalidCommand(f"{self.operation} takes exactly one seat")

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.command_id,
            "op": self.operation.value,
            "seats": list(self.seats),
            "customer": self.customer,
        }

    @classmethod
    def from_payload(cls, payload: Any) -> Command:
        try:
            return cls(
                command_id=str(payload["id"]),
                operation=Operation(payload["op"]),
                seats=tuple(str(seat).upper() for seat in payload["seats"]),
                customer=str(payload["customer"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise InvalidCommand(f"malformed command {payload!r}: {error}") from error


@dataclass(frozen=True, slots=True)
class AppliedCommand:
    command: Command
    outcome: Outcome
