from __future__ import annotations

import itertools
import logging
import threading

import grpc

from mpcomm.model import Endpoint, Peer
from mpcomm.rpc import membership_pb2 as pb
from mpcomm.rpc import membership_pb2_grpc as rpc
from mpcomm.rpc.clients import peer_to_proto
from mpcomm.rpc.server import start_server, stop_server, wait_until_stopped

log = logging.getLogger(__name__)


class GroupRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids = itertools.count()
        self._peers: dict[int, Peer] = {}

    def register(self, name: str, host: str, udp_port: int, control_port: int) -> Peer:
        with self._lock:
            existing = next(
                (p for p in self._peers.values() if (p.host, p.udp_port) == (host, udp_port)),
                None,
            )
            peer_id = existing.peer_id if existing else next(self._ids)
            peer = Peer(peer_id, name or f"peer-{peer_id}", host, udp_port, control_port)
            self._peers[peer_id] = peer
            return peer

    def unregister(self, peer_id: int) -> bool:
        with self._lock:
            return self._peers.pop(peer_id, None) is not None

    def peers(self) -> list[Peer]:
        with self._lock:
            return sorted(self._peers.values(), key=lambda peer: peer.peer_id)


class GroupManagerService(rpc.GroupManagerServicer):
    def __init__(self, registry: GroupRegistry, stop: threading.Event) -> None:
        self._registry = registry
        self._stop = stop

    def Register(self, request: pb.RegisterRequest, context: grpc.ServicerContext):
        if not request.host or not _valid_port(request.udp_port):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "host and udp_port are required")
        if not _valid_port(request.control_port):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "control_port is invalid")
        peer = self._registry.register(
            request.name, request.host, request.udp_port, request.control_port
        )
        log.info(
            "registered %s as peer %s at %s:%s", peer.name, peer.peer_id, peer.host, peer.udp_port
        )
        return pb.RegisterReply(peer_id=peer.peer_id)

    def Unregister(self, request: pb.UnregisterRequest, context: grpc.ServicerContext):
        if not self._registry.unregister(request.peer_id):
            context.abort(grpc.StatusCode.NOT_FOUND, f"peer {request.peer_id} is not registered")
        log.info("unregistered peer %s", request.peer_id)
        return pb.Empty()

    def ListPeers(self, request: pb.Empty, context: grpc.ServicerContext):
        return pb.PeerList(peers=[peer_to_proto(peer) for peer in self._registry.peers()])

    def Shutdown(self, request: pb.Empty, context: grpc.ServicerContext):
        log.info("shutdown requested")
        self._stop.set()
        return pb.Empty()


def run_group_manager(listen: Endpoint) -> None:
    stop = threading.Event()
    service = GroupManagerService(GroupRegistry(), stop)
    server, port = start_server(
        listen, lambda s: rpc.add_GroupManagerServicer_to_server(service, s)
    )
    log.info("group manager listening on %s:%s", listen.host, port)
    wait_until_stopped(stop)
    stop_server(server)
    log.info("group manager stopped")


def _valid_port(port: int) -> bool:
    return 0 < port < 65_536
