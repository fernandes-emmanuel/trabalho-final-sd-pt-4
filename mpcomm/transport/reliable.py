from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from mpcomm.model import Address
from mpcomm.transport.channel import Channel
from mpcomm.transport.messages import AckPacket, DataPacket, Kind, Packet, encode

log = logging.getLogger(__name__)

DeliverCallback = Callable[[DataPacket], None]
Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class RetransmissionPolicy:
    initial_timeout: float = 0.2
    min_timeout: float = 0.05
    max_timeout: float = 2.0
    give_up_after: float = 30.0


class RttEstimator:
    """Jacobson/Karels estimator: RTO = SRTT + 4 * RTTVAR."""

    ALPHA = 1 / 8
    BETA = 1 / 4

    def __init__(self, policy: RetransmissionPolicy) -> None:
        self._policy = policy
        self._smoothed: float | None = None
        self._variation = 0.0
        self.timeout = policy.initial_timeout

    def observe(self, sample: float) -> None:
        if self._smoothed is None:
            self._smoothed = sample
            self._variation = sample / 2
        else:
            self._variation += self.BETA * (abs(self._smoothed - sample) - self._variation)
            self._smoothed += self.ALPHA * (sample - self._smoothed)
        estimate = self._smoothed + 4 * self._variation
        self.timeout = min(max(estimate, self._policy.min_timeout), self._policy.max_timeout)


@dataclass(slots=True)
class ReliabilityCounters:
    data_sent: int = 0
    retransmissions: int = 0
    acks_sent: int = 0
    duplicates_discarded: int = 0
    held_back: int = 0


@dataclass(slots=True)
class _Outgoing:
    data: bytes
    waiting: set[int]
    first_sent: float
    deadline: float
    timeout: float
    retransmitted: bool = False


class ReliableFifoMulticast:
    """Reliable multicast over an unreliable channel with per-sender FIFO delivery.

    Senders number their messages and keep them until every member acknowledges them,
    retransmitting on timeout. Receivers acknowledge every copy, discard duplicates and
    hold back messages that arrive ahead of a gap until the gap is filled.

    Messages are handed to ``deliver`` outside the internal lock, so upper layers may
    multicast from the callback. ``handle`` must be called by a single receiving thread,
    which keeps deliveries in order.
    """

    def __init__(
        self,
        *,
        round_id: int,
        self_id: int,
        members: Mapping[int, Address],
        channel: Channel,
        deliver: DeliverCallback,
        policy: RetransmissionPolicy | None = None,
        clock: Clock = time.monotonic,
    ) -> None:
        if self_id not in members:
            raise ValueError(f"process {self_id} is not a member of the group")
        self._round_id = round_id
        self._self_id = self_id
        self._members = dict(members)
        self._channel = channel
        self._deliver = deliver
        self._policy = policy or RetransmissionPolicy()
        self._clock = clock
        self._lock = threading.RLock()
        self._idle = threading.Condition(self._lock)
        self._next_seq = 0
        self._outgoing: dict[int, _Outgoing] = {}
        self._expected = dict.fromkeys(self._members, 0)
        self._holdback: dict[int, dict[int, DataPacket]] = {member: {} for member in self._members}
        self._unreachable: set[int] = set()
        self._rtt = RttEstimator(self._policy)
        self.counters = ReliabilityCounters()

    @property
    def round_id(self) -> int:
        return self._round_id

    @property
    def unreachable(self) -> frozenset[int]:
        with self._lock:
            return frozenset(self._unreachable)

    def multicast(self, kind: Kind, payload: Any = None) -> DataPacket:
        with self._lock:
            packet = DataPacket(self._round_id, self._self_id, self._next_seq, kind, payload)
            self._next_seq += 1
            data = encode(packet)
            receivers = tuple(member for member in self._members if member not in self._unreachable)
            if receivers:
                now = self._clock()
                timeout = self._rtt.timeout
                self._outgoing[packet.seq] = _Outgoing(
                    data, set(receivers), now, now + timeout, timeout
                )
            self.counters.data_sent += 1
        for member in receivers:
            self._channel.send(data, self._members[member])
        return packet

    def handle(self, packet: Packet) -> None:
        if packet.round_id != self._round_id:
            return
        if isinstance(packet, DataPacket):
            self._on_data(packet)
        else:
            self._on_ack(packet)

    def retransmit_due(self) -> None:
        now = self._clock()
        resend: list[tuple[bytes, Address]] = []
        with self._lock:
            for seq, outgoing in list(self._outgoing.items()):
                if seq not in self._outgoing or outgoing.deadline > now:
                    continue
                if now - outgoing.first_sent >= self._policy.give_up_after:
                    self._give_up_on(set(outgoing.waiting))
                    continue
                outgoing.retransmitted = True
                outgoing.timeout = min(outgoing.timeout * 2, self._policy.max_timeout)
                outgoing.deadline = now + outgoing.timeout
                self.counters.retransmissions += len(outgoing.waiting)
                resend.extend((outgoing.data, self._members[member]) for member in outgoing.waiting)
            self._notify_if_idle()
        for data, address in resend:
            self._channel.send(data, address)

    def wait_until_acknowledged(self, timeout: float | None = None) -> bool:
        with self._idle:
            return self._idle.wait_for(lambda: not self._outgoing, timeout)

    def _on_data(self, packet: DataPacket) -> None:
        if packet.sender not in self._members:
            log.warning("ignoring message from unknown process %s", packet.sender)
            return
        self._acknowledge(packet)
        with self._lock:
            expected = self._expected[packet.sender]
            held = self._holdback[packet.sender]
            if packet.seq < expected or packet.seq in held:
                self.counters.duplicates_discarded += 1
                return
            if packet.seq > expected:
                held[packet.seq] = packet
                self.counters.held_back += 1
                return
            ready = self._release_in_order(packet)
        for deliverable in ready:
            self._deliver(deliverable)

    def _release_in_order(self, packet: DataPacket) -> list[DataPacket]:
        held = self._holdback[packet.sender]
        ready: list[DataPacket] = []
        next_packet: DataPacket | None = packet
        while next_packet is not None:
            ready.append(next_packet)
            self._expected[next_packet.sender] = next_packet.seq + 1
            next_packet = held.pop(next_packet.seq + 1, None)
        return ready

    def _acknowledge(self, packet: DataPacket) -> None:
        ack = AckPacket(self._round_id, self._self_id, packet.sender, packet.seq)
        with self._lock:
            self.counters.acks_sent += 1
        self._channel.send(encode(ack), self._members[packet.sender])

    def _on_ack(self, ack: AckPacket) -> None:
        if ack.origin != self._self_id:
            return
        with self._lock:
            outgoing = self._outgoing.get(ack.seq)
            if outgoing is None or ack.acker not in outgoing.waiting:
                return
            if not outgoing.retransmitted:
                self._rtt.observe(self._clock() - outgoing.first_sent)
            outgoing.waiting.discard(ack.acker)
            if not outgoing.waiting:
                del self._outgoing[ack.seq]
                self._notify_if_idle()

    def _give_up_on(self, members: set[int]) -> None:
        newly_unreachable = members - self._unreachable
        if newly_unreachable:
            log.warning("giving up on unresponsive processes %s", sorted(newly_unreachable))
        self._unreachable |= members
        for seq, outgoing in list(self._outgoing.items()):
            outgoing.waiting -= members
            if not outgoing.waiting:
                del self._outgoing[seq]

    def _notify_if_idle(self) -> None:
        if not self._outgoing:
            self._idle.notify_all()


class PeriodicTask:
    def __init__(self, interval: float, action: Callable[[], None], name: str) -> None:
        self._interval = interval
        self._action = action
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        if self._thread.is_alive():
            self._thread.join()

    def _run(self) -> None:
        while not self._stopped.wait(self._interval):
            try:
                self._action()
            except Exception:
                log.exception("periodic task %s failed", self._thread.name)
