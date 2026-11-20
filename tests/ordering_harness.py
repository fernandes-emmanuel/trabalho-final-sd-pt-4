from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from mpcomm.ordering.base import OrderedMulticast, Ordering
from mpcomm.ordering.sequencer import SequencerMulticast
from mpcomm.ordering.total_order import TotalOrderMulticast
from mpcomm.transport.messages import Kind
from mpcomm.transport.reliable import ReliableFifoMulticast
from tests.simulation import SimulatedNetwork

SESSION = 1
END_OF_TRAFFIC = 0.6


@dataclass
class Member:
    reliable: ReliableFifoMulticast
    ordered: OrderedMulticast | None = None
    delivered: list[tuple[int, Any]] = field(default_factory=list)

    def send(self, body: Any) -> None:
        if self.ordered is not None:
            self.ordered.multicast(body)
        else:
            self.reliable.multicast(Kind.DATA, body)

    def receive(self, sender: int, payload: Any) -> None:
        if self.ordered is not None:
            self.ordered.receive(sender, payload)
        else:
            self.delivered.append((sender, payload))

    def finished(self, expected: int) -> bool:
        if self.ordered is not None:
            return self.ordered.wait_until_everyone_ended(0)
        return len(self.delivered) == expected


def build_group(
    network: SimulatedNetwork, size: int, ordering: Ordering, sequencer: int = 0
) -> dict[int, Member]:
    addresses = {member: ("node", member) for member in range(size)}
    group: dict[int, Member] = {}
    for member_id in addresses:
        reliable = ReliableFifoMulticast(
            round_id=SESSION,
            self_id=member_id,
            members=addresses,
            channel=network,
            deliver=lambda packet, m=member_id: group[m].receive(packet.sender, packet.payload),
            clock=network.clock,
        )
        member = Member(reliable)
        member.ordered = _ordering_layer(ordering, member_id, addresses, sequencer, member)
        network.attach(addresses[member_id], reliable.handle)
        network.every(0.01, reliable.retransmit_due)
        group[member_id] = member
    return group


def schedule_traffic(
    network: SimulatedNetwork, group: dict[int, Member], count: int, seed: int
) -> None:
    rng = random.Random(seed)
    for member_id, member in group.items():
        moments = sorted(rng.uniform(0, END_OF_TRAFFIC - 0.1) for _ in range(count))
        for number, moment in enumerate(moments):
            body = {"from": member_id, "n": number}
            network.at(moment, lambda member=member, body=body: member.send(body))
        if member.ordered is not None:
            network.at(END_OF_TRAFFIC, member.ordered.announce_end)


def run_to_completion(network: SimulatedNetwork, group: dict[int, Member], count: int) -> bool:
    expected = len(group) * count
    return network.run_until(lambda: all(member.finished(expected) for member in group.values()))


def _ordering_layer(
    ordering: Ordering, member_id: int, addresses: dict, sequencer: int, member: Member
) -> OrderedMulticast | None:
    def transmit(payload: dict[str, Any]) -> None:
        member.reliable.multicast(Kind.DATA, payload)

    def deliver(sender: int, body: Any) -> None:
        member.delivered.append((sender, body))

    match ordering:
        case Ordering.FIFO:
            return None
        case Ordering.TOTAL:
            return TotalOrderMulticast(
                self_id=member_id, members=addresses, transmit=transmit, deliver=deliver
            )
        case Ordering.SEQUENCER:
            return SequencerMulticast(
                self_id=member_id,
                members=addresses,
                sequencer=sequencer,
                transmit=transmit,
                deliver=deliver,
            )
