from collections import defaultdict

import pytest

from mpcomm.transport.messages import AckPacket, DataPacket, Kind, decode
from mpcomm.transport.reliable import ReliableFifoMulticast, RetransmissionPolicy, RttEstimator
from tests.simulation import SimulatedNetwork

ROUND = 7


def build_group(network: SimulatedNetwork, size: int, delivered: dict[int, list[DataPacket]]):
    members = {member: ("node", member) for member in range(size)}
    nodes = {}
    for member in members:
        node = ReliableFifoMulticast(
            round_id=ROUND,
            self_id=member,
            members=members,
            channel=network,
            deliver=delivered[member].append,
            clock=network.clock,
        )
        network.attach(members[member], node.handle)
        network.every(0.01, node.retransmit_due)
        nodes[member] = node
    return nodes


def schedule_messages(network: SimulatedNetwork, nodes, count: int, spacing: float) -> None:
    for member, node in nodes.items():
        for number in range(count):
            moment = number * spacing + member * spacing / len(nodes)
            network.at(moment, lambda node=node, number=number: node.multicast(Kind.DATA, number))


def assert_fifo_and_complete(delivered, group_size: int, count: int) -> None:
    for packets in delivered.values():
        by_sender = defaultdict(list)
        for packet in packets:
            by_sender[packet.sender].append(packet.payload)
        assert set(by_sender) == set(range(group_size))
        for sequence in by_sender.values():
            assert sequence == list(range(count))


@pytest.mark.parametrize("seed", range(12))
def test_every_member_delivers_every_message_in_fifo_order_despite_faults(seed):
    network = SimulatedNetwork(seed, loss=0.3, duplication=0.1, min_delay=0.001, max_delay=0.08)
    delivered = defaultdict(list)
    size, count = 5, 30
    nodes = build_group(network, size, delivered)
    schedule_messages(network, nodes, count, spacing=0.005)

    def finished() -> bool:
        everything_delivered = all(len(delivered[m]) == size * count for m in nodes)
        return everything_delivered and all(n.wait_until_acknowledged(0) for n in nodes.values())

    assert network.run_until(finished)
    assert_fifo_and_complete(delivered, size, count)
    assert network.lost > 0
    assert sum(node.counters.retransmissions for node in nodes.values()) > 0
    assert sum(node.counters.held_back for node in nodes.values()) > 0
    assert all(not node.unreachable for node in nodes.values())


def test_reliable_network_needs_no_retransmissions():
    network = SimulatedNetwork(1, min_delay=0.001, max_delay=0.01)
    delivered = defaultdict(list)
    nodes = build_group(network, 4, delivered)
    schedule_messages(network, nodes, 20, spacing=0.02)

    assert network.run_until(lambda: all(len(delivered[m]) == 80 for m in nodes))
    assert_fifo_and_complete(delivered, 4, 20)
    assert all(node.counters.retransmissions == 0 for node in nodes.values())
    assert all(node.counters.duplicates_discarded == 0 for node in nodes.values())


class RecordingChannel:
    def __init__(self):
        self.sent = []

    def send(self, data, address):
        self.sent.append((decode(data), address))


def make_node(channel, delivered, *, clock=lambda: 0.0, policy=None, members=(0, 1)):
    return ReliableFifoMulticast(
        round_id=ROUND,
        self_id=0,
        members={member: ("node", member) for member in members},
        channel=channel,
        deliver=delivered.append,
        policy=policy,
        clock=clock,
    )


def test_out_of_order_messages_are_held_back_until_the_gap_is_filled():
    channel, delivered = RecordingChannel(), []
    node = make_node(channel, delivered)

    node.handle(DataPacket(ROUND, 1, 2, Kind.DATA, "c"))
    node.handle(DataPacket(ROUND, 1, 1, Kind.DATA, "b"))
    assert delivered == []

    node.handle(DataPacket(ROUND, 1, 0, Kind.DATA, "a"))
    assert [packet.payload for packet in delivered] == ["a", "b", "c"]
    assert node.counters.held_back == 2


def test_duplicates_are_acknowledged_but_delivered_once():
    channel, delivered = RecordingChannel(), []
    node = make_node(channel, delivered)
    packet = DataPacket(ROUND, 1, 0, Kind.DATA, "a")

    node.handle(packet)
    node.handle(packet)

    assert len(delivered) == 1
    assert node.counters.duplicates_discarded == 1
    acks = [sent for sent, _ in channel.sent if isinstance(sent, AckPacket)]
    assert acks == [AckPacket(ROUND, 0, 1, 0)] * 2


def test_packets_from_other_rounds_are_ignored():
    channel, delivered = RecordingChannel(), []
    node = make_node(channel, delivered)

    node.handle(DataPacket(ROUND + 1, 1, 0, Kind.DATA, "a"))

    assert delivered == []
    assert channel.sent == []


def test_unacknowledged_message_is_retransmitted_with_exponential_backoff():
    now = [0.0]
    channel, delivered = RecordingChannel(), []
    policy = RetransmissionPolicy(initial_timeout=0.125, max_timeout=0.5, give_up_after=10)
    node = make_node(channel, delivered, clock=lambda: now[0], policy=policy)
    node.multicast(Kind.DATA, "x")
    node.handle(AckPacket(ROUND, 0, 0, 0))
    channel.sent.clear()

    retransmission_times = []
    for step in range(1, 49):
        now[0] = step / 32
        node.retransmit_due()
        if channel.sent:
            retransmission_times.append(now[0])
            assert [address for _, address in channel.sent] == [("node", 1)]
            channel.sent.clear()

    assert retransmission_times == [0.125, 0.375, 0.875, 1.375]


def test_unresponsive_member_is_eventually_declared_unreachable():
    now = [0.0]
    channel, delivered = RecordingChannel(), []
    policy = RetransmissionPolicy(initial_timeout=0.1, max_timeout=0.2, give_up_after=1.0)
    node = make_node(channel, delivered, clock=lambda: now[0], policy=policy)
    node.multicast(Kind.DATA, "x")
    node.handle(AckPacket(ROUND, 0, 0, 0))

    assert not node.wait_until_acknowledged(0)
    now[0] = 1.5
    node.retransmit_due()

    assert node.unreachable == {1}
    assert node.wait_until_acknowledged(0)


def test_rtt_estimator_converges_and_respects_bounds():
    policy = RetransmissionPolicy(initial_timeout=0.2, min_timeout=0.05, max_timeout=2.0)
    estimator = RttEstimator(policy)
    for _ in range(50):
        estimator.observe(0.1)
    assert 0.1 <= estimator.timeout < 0.12

    for _ in range(50):
        estimator.observe(0.001)
    assert estimator.timeout == pytest.approx(0.05)

    estimator.observe(100)
    assert estimator.timeout == 2.0
