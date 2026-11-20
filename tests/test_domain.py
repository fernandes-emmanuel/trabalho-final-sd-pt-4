import pytest

from boxoffice.domain.commands import Command, InvalidCommand, Operation, Outcome
from boxoffice.domain.replica import Replica
from boxoffice.domain.seats import SeatMap, Venue
from boxoffice.domain.workload import WorkloadGenerator, WorkloadSpec


def reserve(command_id, seat, customer):
    return Command(command_id, Operation.RESERVE, (seat,), customer)


def cancel(command_id, seat, customer):
    return Command(command_id, Operation.CANCEL, (seat,), customer)


def group(command_id, seats, customer):
    return Command(command_id, Operation.RESERVE_GROUP, tuple(seats), customer)


@pytest.fixture
def seat_map():
    return SeatMap(Venue(2, 3))


def test_venue_labels_seats_by_row_and_number():
    assert list(Venue(2, 3).seats()) == ["A1", "A2", "A3", "B1", "B2", "B3"]
    assert Venue(5, 10).capacity == 50
    for rows, seats in ((0, 5), (27, 5), (5, 0), (5, 100)):
        with pytest.raises(ValueError):
            Venue(rows, seats)


def test_first_reservation_wins(seat_map):
    assert seat_map.apply(reserve("0.1", "A1", "ana")) is Outcome.ACCEPTED
    assert seat_map.apply(reserve("1.1", "A1", "bruno")) is Outcome.SEAT_TAKEN
    assert seat_map.owner("A1") == "ana"


def test_only_the_owner_can_cancel(seat_map):
    seat_map.apply(reserve("0.1", "A1", "ana"))

    assert seat_map.apply(cancel("1.1", "A1", "bruno")) is Outcome.NOT_OWNER
    assert seat_map.apply(cancel("0.2", "A1", "ana")) is Outcome.ACCEPTED
    assert seat_map.owner("A1") is None


def test_group_reservation_is_all_or_nothing(seat_map):
    seat_map.apply(reserve("0.1", "A2", "ana"))

    assert seat_map.apply(group("1.1", ["A1", "A2", "A3"], "bruno")) is Outcome.SEAT_TAKEN
    assert seat_map.owners() == {"A2": "ana"}
    assert seat_map.apply(group("1.2", ["B1", "B2"], "bruno")) is Outcome.ACCEPTED
    assert seat_map.owners() == {"A2": "ana", "B1": "bruno", "B2": "bruno"}


def test_unknown_seats_are_rejected(seat_map):
    assert seat_map.apply(reserve("0.1", "Z9", "ana")) is Outcome.UNKNOWN_SEAT


def test_order_of_conflicting_commands_decides_the_final_state():
    commands = [
        reserve("0.1", "A1", "ana"),
        cancel("0.2", "A1", "ana"),
        reserve("1.1", "A1", "bruno"),
    ]
    in_order, reordered = SeatMap(Venue(1, 2)), SeatMap(Venue(1, 2))
    for command in commands:
        in_order.apply(command)
    for command in (commands[0], commands[2], commands[1]):
        reordered.apply(command)

    assert in_order.owner("A1") == "bruno"
    assert reordered.owner("A1") is None
    assert in_order.digest() != reordered.digest()


def test_same_commands_in_same_order_give_the_same_digest():
    commands = [reserve("0.1", "A1", "ana"), group("1.1", ["B1", "B2"], "bruno")]
    first, second = SeatMap(Venue(2, 3)), SeatMap(Venue(2, 3))
    for command in commands:
        first.apply(command)
        second.apply(command)
    assert first.digest() == second.digest()


def test_command_payload_round_trip():
    command = group("2.7", ["A1", "A2"], "carla")
    assert Command.from_payload(command.to_payload()) == command


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"id": "1", "op": "explode", "seats": ["A1"], "customer": "x"},
        {"id": "1", "op": "reserve", "seats": ["A1", "A2"], "customer": "x"},
        {"id": "1", "op": "reserve", "seats": ["A1"], "customer": ""},
        {"id": "1", "op": "reserve_group", "seats": ["A1", "A1"], "customer": "x"},
    ],
)
def test_invalid_payloads_are_rejected(payload):
    with pytest.raises(InvalidCommand):
        Command.from_payload(payload)


def test_replica_resolves_pending_commands_when_they_are_applied():
    replica = Replica(3, SeatMap(Venue(1, 2)))
    pending = replica.prepare(Operation.RESERVE, ("a1",), "ana")

    assert pending.command.command_id == "3.1"
    assert pending.command.seats == ("A1",)
    assert pending.wait(0) is None

    replica.apply_delivered(1, reserve("1.1", "A1", "bruno").to_payload())
    replica.apply_delivered(3, pending.command.to_payload())

    assert pending.wait(0) is Outcome.SEAT_TAKEN
    assert [entry.outcome for entry in replica.applied()] == [Outcome.ACCEPTED, Outcome.SEAT_TAKEN]
    assert replica.view().owners == {"A1": "bruno"}


def test_replica_measures_the_latency_of_its_own_commands():
    replica = Replica(0, SeatMap(Venue(1, 2)))
    assert replica.latency().mean_ms == 0.0

    for seat in ("A1", "A2"):
        pending = replica.prepare(Operation.RESERVE, (seat,), "ana")
        replica.apply_delivered(0, pending.command.to_payload())
    replica.apply_delivered(1, reserve("1.1", "A1", "bruno").to_payload())

    summary = replica.latency()
    assert summary.mean_ms >= 0.0
    assert summary.p95_ms >= summary.mean_ms or summary.p95_ms == summary.mean_ms


def test_replica_ignores_malformed_commands():
    replica = Replica(0, SeatMap(Venue(1, 2)))
    replica.apply_delivered(1, {"garbage": True})
    assert replica.applied() == ()


def test_workload_is_reproducible_and_produces_valid_requests():
    venue = Venue(5, 10)
    spec = WorkloadSpec(commands=200, seed=9, hot_seats=3, hot_probability=0.8)
    first = WorkloadGenerator(venue, spec, office_id=1)
    second = WorkloadGenerator(venue, spec, office_id=1)
    owners = {"A1": "ana"}

    requests = [first.next_request(owners) for _ in range(200)]

    assert requests == [second.next_request(owners) for _ in range(200)]
    seats = set(venue.seats())
    for request in requests:
        Command("x", request.operation, request.seats, request.customer)
        assert set(request.seats) <= seats
    operations = {request.operation for request in requests}
    assert operations == {Operation.RESERVE, Operation.RESERVE_GROUP, Operation.CANCEL}
    hot = sum(
        r.seats[0] in {"A1", "A2", "A3"} for r in requests if r.operation is Operation.RESERVE
    )
    assert hot > 0.6 * sum(r.operation is Operation.RESERVE for r in requests)


def test_different_offices_generate_different_workloads():
    venue, spec = Venue(5, 10), WorkloadSpec(commands=50, seed=1)
    first = [WorkloadGenerator(venue, spec, 0).next_request({}) for _ in range(20)]
    second = [WorkloadGenerator(venue, spec, 1).next_request({}) for _ in range(20)]
    assert first != second
