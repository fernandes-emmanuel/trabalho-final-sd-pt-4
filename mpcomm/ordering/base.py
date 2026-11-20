from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

Transmit = Callable[[dict[str, Any]], None]
Deliver = Callable[[int, Any], None]


class Ordering(StrEnum):
    FIFO = "fifo"
    TOTAL = "total"
    SEQUENCER = "sequencer"


@dataclass(slots=True)
class OrderingCounters:
    messages_sent: int = 0
    control_sent: int = 0
    queue_peak: int = 0


class OrderedMulticast(Protocol):
    """Ordering layer placed on top of reliable FIFO multicast.

    ``announce_end`` multicasts, in the same order as the messages, a marker after which the
    member sends nothing else; once every member's marker has been delivered, every message
    of the session has been delivered too.
    """

    counters: OrderingCounters

    def multicast(self, body: Any) -> object: ...

    def announce_end(self) -> object: ...

    def receive(self, sender: int, payload: dict[str, Any]) -> None: ...

    def wait_until_everyone_ended(self, timeout: float | None) -> bool: ...
