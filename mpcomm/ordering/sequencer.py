from __future__ import annotations

import logging
import threading
from collections.abc import Iterable
from typing import Any

from mpcomm.ordering.base import Deliver, OrderingCounters, Transmit

log = logging.getLogger(__name__)

Reference = tuple[int, int]


class SequencerMulticast:
    """Totally ordered multicast through a fixed sequencer (Tanenbaum & van Steen, chapter 7).

    Every member multicasts its messages once to the whole group, tagged with a local
    identifier. One designated member, the sequencer, gives each message the next global
    sequence number in the order it receives them and multicasts that assignment. Members
    deliver messages in sequence-number order as soon as they hold both the message and its
    number.

    Only the sequencer decides the order, so every member delivers the same sequence. The
    transport must be reliable and FIFO: assignments then reach every member, and the
    messages of each sender reach the sequencer in the order they were sent, so the global
    order also respects the order of every sender.
    """

    MESSAGE = "msg"
    END = "end"
    SEQUENCE = "seq"

    def __init__(
        self,
        *,
        self_id: int,
        members: Iterable[int],
        sequencer: int,
        transmit: Transmit,
        deliver: Deliver,
    ) -> None:
        self._members = frozenset(members)
        if sequencer not in self._members:
            raise ValueError(f"sequencer {sequencer} is not a member of the group")
        self._self_id = self_id
        self.sequencer = sequencer
        self._transmit = transmit
        self._deliver = deliver
        self._lock = threading.Lock()
        self._next_local = 0
        self._next_global = 0
        self._next_delivery = 0
        self._received: dict[Reference, tuple[str, Any]] = {}
        self._assigned: dict[int, Reference] = {}
        self._ended: set[int] = set()
        self._everyone_ended = threading.Event()
        self.counters = OrderingCounters()

    @property
    def is_sequencer(self) -> bool:
        return self._self_id == self.sequencer

    def multicast(self, body: Any) -> Reference:
        return self._send(self.MESSAGE, body)

    def announce_end(self) -> Reference:
        return self._send(self.END, None)

    def wait_until_everyone_ended(self, timeout: float | None) -> bool:
        return self._everyone_ended.wait(timeout)

    def receive(self, sender: int, payload: dict[str, Any]) -> None:
        try:
            kind = payload["order"]
            if kind == self.SEQUENCE:
                number = int(payload["seq"])
                origin, local = (int(value) for value in payload["ref"])
            elif kind in (self.MESSAGE, self.END):
                reference = (sender, int(payload["id"]))
            else:
                raise ValueError(f"unknown kind {kind!r}")
        except (KeyError, TypeError, ValueError):
            log.error("discarding malformed sequencer message from %s: %r", sender, payload)
            return
        with self._lock:
            if kind == self.SEQUENCE:
                if sender != self.sequencer:
                    log.error("ignoring sequence number assigned by non-sequencer %s", sender)
                    return
                self._assigned[number] = (origin, local)
            else:
                self._received[reference] = (kind, payload.get("body"))
                if self.is_sequencer:
                    self._assign(reference)
            self.counters.queue_peak = max(self.counters.queue_peak, len(self._received))
            ready = self._release_deliverable()
        for origin, body in ready:
            self._deliver(origin, body)

    def _send(self, kind: str, body: Any) -> Reference:
        with self._lock:
            reference = (self._self_id, self._next_local)
            self._next_local += 1
            self._transmit({"order": kind, "id": reference[1], "body": body})
            self.counters.messages_sent += 1
        return reference

    def _assign(self, reference: Reference) -> None:
        self._transmit({"order": self.SEQUENCE, "seq": self._next_global, "ref": list(reference)})
        self._next_global += 1
        self.counters.control_sent += 1

    def _release_deliverable(self) -> list[tuple[int, Any]]:
        ready: list[tuple[int, Any]] = []
        while self._assigned.get(self._next_delivery) in self._received:
            reference = self._assigned.pop(self._next_delivery)
            kind, body = self._received.pop(reference)
            self._next_delivery += 1
            if kind == self.END:
                self._ended.add(reference[0])
                if self._ended >= self._members:
                    self._everyone_ended.set()
            else:
                ready.append((reference[0], body))
        return ready
