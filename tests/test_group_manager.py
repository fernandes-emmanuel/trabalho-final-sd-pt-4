import threading

import grpc
import pytest

from mpcomm.membership.group_manager import GroupManagerService, GroupRegistry
from mpcomm.model import Endpoint, Peer
from mpcomm.rpc import membership_pb2_grpc as rpc
from mpcomm.rpc.clients import GroupManagerClient
from mpcomm.rpc.server import start_server, stop_server


@pytest.fixture
def group_manager():
    stop = threading.Event()
    service = GroupManagerService(GroupRegistry(), stop)
    server, port = start_server(
        Endpoint("127.0.0.1", 0), lambda s: rpc.add_GroupManagerServicer_to_server(service, s)
    )
    client = GroupManagerClient(Endpoint("127.0.0.1", port))
    yield client, stop
    client.close()
    stop_server(server)


def test_register_list_and_unregister(group_manager):
    client, _ = group_manager

    first = client.register("sao-paulo", "10.0.0.1", 6789, 5679)
    second = client.register("", "10.0.0.2", 6789, 5679)
    assert client.list_peers() == [
        Peer(first, "sao-paulo", "10.0.0.1", 6789, 5679),
        Peer(second, f"peer-{second}", "10.0.0.2", 6789, 5679),
    ]

    client.unregister(first)
    assert [peer.peer_id for peer in client.list_peers()] == [second]


def test_reregistering_the_same_address_keeps_the_identifier(group_manager):
    client, _ = group_manager

    original = client.register("rio", "10.0.0.1", 6789, 5679)
    again = client.register("rio", "10.0.0.1", 6789, 5800)

    assert again == original
    assert client.list_peers() == [Peer(original, "rio", "10.0.0.1", 6789, 5800)]


def test_invalid_requests_return_proper_status_codes(group_manager):
    client, _ = group_manager

    with pytest.raises(grpc.RpcError) as invalid:
        client.register("x", "", 6789, 5679)
    assert invalid.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as missing:
        client.unregister(99)
    assert missing.value.code() == grpc.StatusCode.NOT_FOUND


def test_shutdown_signals_the_server_to_stop(group_manager):
    client, stop = group_manager

    client.shutdown()

    assert stop.is_set()
