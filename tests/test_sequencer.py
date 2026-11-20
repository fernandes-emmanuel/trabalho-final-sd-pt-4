import pytest

from mpcomm.ordering.base import Ordering
from mpcomm.ordering.sequencer import SequencerMulticast
from tests.ordering_harness import build_group, run_to_completion, schedule_traffic
from tests.simulation import SimulatedNetwork


@pytest.mark.parametrize("seed", range(25))
def test_every_member_delivers_the_same_sequence_despite_loss_and_reordering(seed):
    network = SimulatedNetwork(seed, loss=0.2, duplication=0.05, min_delay=0.001, max_delay=0.1)
    size, count = 4, 15
    group = build_group(network, size, Ordering.SEQUENCER, sequencer=seed % size)
    schedule_traffic(network, group, count, seed)

    assert run_to_completion(network, group, count)
    sequences = [member.delivered for member in group.values()]
    assert all(sequence == sequences[0] for sequence in sequences)
    assert len(sequences[0]) == size * count
    for sender in group:
        numbers = [body["n"] for origin, body in sequences[0] if origin == sender]
        assert numbers == list(range(count))


@pytest.mark.parametrize("size", [3, 5, 8])
def test_sequencer_needs_one_control_message_per_message_instead_of_one_per_member(size):
    count = 10
    totals = {}
    for ordering in (Ordering.TOTAL, Ordering.SEQUENCER):
        network = SimulatedNetwork(size, min_delay=0.001, max_delay=0.02)
        group = build_group(network, size, ordering)
        schedule_traffic(network, group, count, seed=size)
        assert run_to_completion(network, group, count)
        totals[ordering] = sum(member.ordered.counters.control_sent for member in group.values())

    ordered_messages = size * count + size
    assert totals[Ordering.SEQUENCER] == ordered_messages
    assert totals[Ordering.TOTAL] == size * ordered_messages


class Recorder:
    def __init__(self):
        self.sent = []
        self.delivered = []


def make_member(self_id, sequencer=0, members=(0, 1, 2)):
    recorder = Recorder()
    member = SequencerMulticast(
        self_id=self_id,
        members=members,
        sequencer=sequencer,
        transmit=recorder.sent.append,
        deliver=lambda sender, body: recorder.delivered.append((sender, body)),
    )
    return member, recorder


def test_sequencer_numbers_messages_in_the_order_it_receives_them():
    sequencer, recorder = make_member(0)

    sequencer.receive(2, {"order": "msg", "id": 0, "body": "a"})
    sequencer.receive(1, {"order": "msg", "id": 0, "body": "b"})
    sequencer.receive(2, {"order": "msg", "id": 1, "body": "c"})

    assignments = [message for message in recorder.sent if message["order"] == "seq"]
    assert [(m["seq"], m["ref"]) for m in assignments] == [(0, [2, 0]), (1, [1, 0]), (2, [2, 1])]
    assert sequencer.is_sequencer
    assert sequencer.counters.control_sent == 3


def test_member_delivers_only_with_both_the_message_and_its_number():
    member, recorder = make_member(1)

    member.receive(0, {"order": "seq", "seq": 0, "ref": [2, 0]})
    assert recorder.delivered == []

    member.receive(2, {"order": "msg", "id": 0, "body": "a"})
    assert recorder.delivered == [(2, "a")]

    member.receive(2, {"order": "msg", "id": 1, "body": "b"})
    assert recorder.delivered == [(2, "a")]
    assert not member.is_sequencer
    assert member.counters.control_sent == 0


def test_gaps_in_the_global_sequence_block_later_messages():
    member, recorder = make_member(1)
    member.receive(2, {"order": "msg", "id": 0, "body": "first"})
    member.receive(1, {"order": "msg", "id": 0, "body": "second"})
    member.receive(0, {"order": "seq", "seq": 1, "ref": [1, 0]})
    assert recorder.delivered == []

    member.receive(0, {"order": "seq", "seq": 0, "ref": [2, 0]})
    assert [body for _, body in recorder.delivered] == ["first", "second"]


def test_numbers_from_a_member_other_than_the_sequencer_are_ignored():
    member, recorder = make_member(1)
    member.receive(2, {"order": "msg", "id": 0, "body": "a"})
    member.receive(2, {"order": "seq", "seq": 0, "ref": [2, 0]})
    assert recorder.delivered == []


def test_end_markers_are_ordered_but_not_delivered_to_the_application():
    member, recorder = make_member(1, members=(0, 1))
    member.announce_end()
    member.receive(1, {"order": "end", "id": 0})
    member.receive(0, {"order": "end", "id": 0})
    member.receive(0, {"order": "seq", "seq": 0, "ref": [1, 0]})
    assert not member.wait_until_everyone_ended(0)

    member.receive(0, {"order": "seq", "seq": 1, "ref": [0, 0]})
    assert member.wait_until_everyone_ended(0)
    assert recorder.delivered == []


def test_malformed_messages_are_ignored():
    member, recorder = make_member(0)
    member.receive(1, {"order": "msg"})
    member.receive(1, {"order": "seq", "seq": "x", "ref": [1, 0]})
    member.receive(1, {"order": "bogus", "id": 0})
    assert recorder.delivered == []
    assert recorder.sent == []


def test_sequencer_must_belong_to_the_group():
    with pytest.raises(ValueError):
        make_member(0, sequencer=7)
