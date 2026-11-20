from __future__ import annotations

import heapq
import logging
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from mpcomm.ordering.base import Deliver, OrderingCounters, Transmit

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, order=True)
class Stamp:
    time: int
    sender: int


class LamportClock:
    def __init__(self) -> None:
        self.time = 0

    def tick(self) -> int:
        self.time += 1
        return self.time

    def observe(self, time: int) -> None:
        self.time = max(self.time, time) + 1


@dataclass(slots=True)
class _Queued:
    stamp: Stamp
    kind: str
    body: Any


class TotalOrderMulticast:
    """Totally ordered multicast with Lamport clocks (Tanenbaum & van Steen, section 5.2).

    Every message is stamped with the sender's Lamport clock and multicast to the whole
    group, the sender included. Each receiver queues the message ordered by
    (timestamp, sender) and multicasts an acknowledgement. A message is delivered when it is
    at the head of the queue and every member has acknowledged it.

    The transport must be reliable and FIFO: an acknowledgement from a member then proves
    that every message that member sent with a smaller timestamp has already been received,
    so no message can later overtake the head of the queue. All members deliver the same
    messages in the same order.
    """

    MESSAGE = "msg"
    END = "end"
    ACK = "ack"

    def __init__(
        self, *, self_id: int, members: Iterable[int], transmit: Transmit, deliver: Deliver
    ) -> None:
        self._self_id = self_id
        self._members = frozenset(members)
        self._transmit = transmit
        self._deliver = deliver
        self._lock = threading.Lock()
        self._clock = LamportClock()
        self._queue: list[Stamp] = []
        self._queued: dict[Stamp, _Queued] = {}
        self._acks: dict[Stamp, set[int]] = {}
        self._ended: set[int] = set()
        self._everyone_ended = threading.Event()
        self.counters = OrderingCounters()

    @property
    def clock(self) -> int:
        with self._lock:
            return self._clock.time

    def multicast(self, body: Any) -> Stamp:
        return self._stamp_and_send(self.MESSAGE, body)

    def announce_end(self) -> Stamp:
        return self._stamp_and_send(self.END, None)

    def wait_until_everyone_ended(self, timeout: float | None) -> bool:
        return self._everyone_ended.wait(timeout)

    def receive(self, sender: int, payload: dict[str, Any]) -> None:
        try:
            kind = payload["order"]
            time = int(payload["ts"])
            reference = Stamp(*payload["ref"]) if kind == self.ACK else Stamp(time, sender)
        except (KeyError, TypeError, ValueError):
            log.error("discarding malformed total order message from %s: %r", sender, payload)
            return
        with self._lock:
            self._clock.observe(time)
            if kind == self.ACK:
                self._acks.setdefault(reference, set()).add(sender)
            elif kind in (self.MESSAGE, self.END):
                self._enqueue(_Queued(reference, kind, payload.get("body")))
            ready = self._release_deliverable()
        for queued in ready:
            if queued.kind == self.MESSAGE:
                self._deliver(queued.stamp.sender, queued.body)

    def _stamp_and_send(self, kind: str, body: Any) -> Stamp:
        with self._lock:
            stamp = Stamp(self._clock.tick(), self._self_id)
            self._transmit({"order": kind, "ts": stamp.time, "body": body})
            self.counters.messages_sent += 1
        return stamp

    def _enqueue(self, queued: _Queued) -> None:
        if queued.stamp in self._queued:
            return
        self._queued[queued.stamp] = queued
        heapq.heappush(self._queue, queued.stamp)
        self.counters.queue_peak = max(self.counters.queue_peak, len(self._queue))
        ack_time = self._clock.tick()
        self._transmit(
            {"order": self.ACK, "ts": ack_time, "ref": [queued.stamp.time, queued.stamp.sender]}
        )
        self.counters.control_sent += 1

    def _release_deliverable(self) -> list[_Queued]:
        ready: list[_Queued] = []
        while self._queue and self._acks.get(self._queue[0], set()) >= self._members:
            stamp = heapq.heappop(self._queue)
            del self._acks[stamp]
            queued = self._queued.pop(stamp)
            if queued.kind == self.END:
                self._ended.add(stamp.sender)
                if self._ended >= self._members:
                    self._everyone_ended.set()
            ready.append(queued)
        return ready
