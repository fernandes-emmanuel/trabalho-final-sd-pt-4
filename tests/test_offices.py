import threading
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from boxoffice.auditor.server import SnapshotCollector
from boxoffice.domain.commands import Outcome
from boxoffice.domain.seats import Venue
from boxoffice.office.node import OfficeNode, OfficeSettings
from boxoffice.rpc import boxoffice_pb2_grpc as rpc
from boxoffice.rpc.clients import OfficeControlClient, TicketOfficeClient
from mpcomm.membership.group_manager import GroupManagerService, GroupRegistry
from mpcomm.model import Endpoint
from mpcomm.ordering.base import Ordering
from mpcomm.rpc import membership_pb2_grpc as membership_rpc
from mpcomm.rpc.server import start_server, stop_server
from mpcomm.transport.faults import FaultProfile

LOCALHOST = "127.0.0.1"
BASE_PORT = 7900
NAMES = ("sao-paulo", "rio", "lisboa")
DISTANT = FaultProfile(delay_ms=40, jitter_ms=20, seed=1)


@pytest.fixture
def cluster():
    group_manager = GroupManagerService(GroupRegistry(), threading.Event())
    gm_server, gm_port = start_server(
        Endpoint(LOCALHOST, 0),
        lambda s: membership_rpc.add_GroupManagerServicer_to_server(group_manager, s),
    )
    collector = SnapshotCollector()
    auditor_server, auditor_port = start_server(
        Endpoint(LOCALHOST, 0), lambda s: rpc.add_AuditorServicer_to_server(collector, s)
    )
    offices, threads = [], []
    for index, name in enumerate(NAMES):
        node = OfficeNode(
            OfficeSettings(
                name=name,
                group_manager=Endpoint(LOCALHOST, gm_port),
                bind_host=LOCALHOST,
                advertise_host=LOCALHOST,
                udp_port=BASE_PORT + index,
                control_port=BASE_PORT + 50 + index,
                faults=DISTANT if name == "lisboa" else FaultProfile(),
            )
        )
        thread = threading.Thread(target=node.run, daemon=True)
        thread.start()
        offices.append(node)
        threads.append(thread)
    _wait_until_registered(group_manager)
    endpoints = [Endpoint(LOCALHOST, BASE_PORT + 50 + index) for index in range(len(NAMES))]
    yield offices, endpoints, collector, Endpoint(LOCALHOST, auditor_port)
    for node in offices:
        node.stop()
    for thread in threads:
        thread.join(10)
    stop_server(auditor_server)
    stop_server(gm_server)


def _wait_until_registered(service: GroupManagerService) -> None:
    registry = service._registry
    for _ in range(100):
        if len(registry.peers()) == len(NAMES):
            return
        threading.Event().wait(0.1)
    raise TimeoutError("offices did not register")


def test_clients_buy_tickets_and_every_replica_converges(cluster):
    offices, endpoints, collector, auditor = cluster
    venue = Venue(2, 4)
    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            assert control.open_session(1, venue, auditor) == len(NAMES)

    with TicketOfficeClient(endpoints[0]) as sao_paulo, TicketOfficeClient(endpoints[2]) as lisboa:
        assert sao_paulo.reserve("A1", "ana").outcome is Outcome.ACCEPTED
        assert lisboa.reserve_group(["B1", "B2"], "bruno").outcome is Outcome.ACCEPTED
        assert lisboa.cancel("A1", "bruno").outcome is Outcome.NOT_OWNER
        result = lisboa.reserve("A1", "carla")
        assert result.outcome is Outcome.SEAT_TAKEN
        assert result.office_name == "lisboa"

    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.close_session(1)
    snapshots = collector.collect(1, {office.office_id for office in offices}, timeout=30)

    assert len(snapshots) == len(NAMES)
    assert {s.digest for s in snapshots.values()} == {next(iter(snapshots.values())).digest}
    for snapshot in snapshots.values():
        assert snapshot.completed
        assert snapshot.owners == {"A1": "ana", "B1": "bruno", "B2": "bruno"}
        assert len(snapshot.applied) == 4

    with TicketOfficeClient(endpoints[1]) as rio:
        seat_map = rio.seat_map()
        assert seat_map.office_name == "rio"
        assert seat_map.owners == {"A1": "ana", "B1": "bruno", "B2": "bruno"}
        with pytest.raises(grpc.RpcError) as closed:
            rio.reserve("A2", "diego")
    assert closed.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_simultaneous_requests_for_a_seat_have_a_single_winner_everywhere(cluster):
    offices, endpoints, collector, auditor = cluster
    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.open_session(3, Venue(1, 4), auditor)
    customers = ("ana", "bruno", "carla")

    def reserve(index):
        with TicketOfficeClient(endpoints[index]) as client:
            client.wait_until_connected(5)
            return client.reserve("A1", customers[index])

    for _ in range(3):
        with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
            outcomes = [result.outcome for result in pool.map(reserve, range(len(endpoints)))]
        assert outcomes.count(Outcome.ACCEPTED) <= 1

    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.close_session(3)
    snapshots = collector.collect(3, {office.office_id for office in offices}, timeout=60)

    assert len({snapshot.digest for snapshot in snapshots.values()}) == 1
    logs = [[entry.command.command_id for entry in s.applied] for s in snapshots.values()]
    assert all(log == logs[0] for log in logs)
    winners = [
        entry
        for entry in next(iter(snapshots.values())).applied
        if entry.outcome is Outcome.ACCEPTED
    ]
    assert len(winners) == 1


@pytest.mark.parametrize(
    ("ordering", "sequencer"), [(Ordering.TOTAL, ""), (Ordering.SEQUENCER, "lisboa")]
)
def test_every_ordering_keeps_replicas_identical_under_contention(cluster, ordering, sequencer):
    offices, endpoints, collector, auditor = cluster
    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.open_session(4, Venue(1, 4), auditor, ordering, sequencer)

    def reserve(index):
        with TicketOfficeClient(endpoints[index]) as client:
            client.wait_until_connected(5)
            return [client.reserve(seat, NAMES[index]).outcome for seat in ("A1", "A2", "A3")]

    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        outcomes = [
            outcome for result in pool.map(reserve, range(len(endpoints))) for outcome in result
        ]
    assert outcomes.count(Outcome.ACCEPTED) == 3

    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.close_session(4)
    snapshots = collector.collect(4, {office.office_id for office in offices}, timeout=60)

    assert len({snapshot.digest for snapshot in snapshots.values()}) == 1
    assert {snapshot.ordering for snapshot in snapshots.values()} == {ordering.value}
    assert {snapshot.sequencer for snapshot in snapshots.values()} == {sequencer}


def test_unknown_sequencer_is_rejected(cluster):
    _, endpoints, _, auditor = cluster
    with OfficeControlClient(endpoints[0]) as control, pytest.raises(grpc.RpcError) as unknown:
        control.open_session(5, Venue(1, 2), auditor, Ordering.SEQUENCER, "marte")
    assert unknown.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_invalid_requests_are_rejected_with_proper_status_codes(cluster):
    _, endpoints, _, auditor = cluster

    with TicketOfficeClient(endpoints[0]) as client:
        with pytest.raises(grpc.RpcError) as no_session:
            client.reserve("A1", "ana")
        assert no_session.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    for endpoint in endpoints:
        with OfficeControlClient(endpoint) as control:
            control.open_session(2, Venue(1, 2), auditor)
    with OfficeControlClient(endpoints[0]) as control, pytest.raises(grpc.RpcError) as twice:
        control.open_session(2, Venue(1, 2), auditor)
    assert twice.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    with TicketOfficeClient(endpoints[0]) as client:
        with pytest.raises(grpc.RpcError) as invalid:
            client.reserve("A1", "")
        assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert client.reserve("Z9", "ana").outcome is Outcome.UNKNOWN_SEAT
