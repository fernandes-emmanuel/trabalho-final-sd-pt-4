from __future__ import annotations

from types import TracebackType
from typing import Self

import grpc

from mpcomm.model import Endpoint, Peer
from mpcomm.rpc import membership_pb2 as pb
from mpcomm.rpc import membership_pb2_grpc as rpc

DEFAULT_TIMEOUT = 5.0


class RpcClient:
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._channel = grpc.insecure_channel(str(endpoint))
        self._timeout = timeout

    def wait_until_connected(self, timeout: float) -> None:
        grpc.channel_ready_future(self._channel).result(timeout=timeout)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class GroupManagerClient(RpcClient):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.GroupManagerStub(self._channel)

    def register(
        self, name: str, host: str, udp_port: int, control_port: int, wait: float = 0.0
    ) -> int:
        request = pb.RegisterRequest(
            name=name, host=host, udp_port=udp_port, control_port=control_port
        )
        reply = self._stub.Register(
            request, timeout=max(wait, self._timeout), wait_for_ready=wait > 0
        )
        return reply.peer_id

    def unregister(self, peer_id: int) -> None:
        self._stub.Unregister(pb.UnregisterRequest(peer_id=peer_id), timeout=self._timeout)

    def list_peers(self) -> list[Peer]:
        reply = self._stub.ListPeers(pb.Empty(), timeout=self._timeout)
        return [peer_from_proto(peer) for peer in reply.peers]

    def shutdown(self) -> None:
        self._stub.Shutdown(pb.Empty(), timeout=self._timeout)


def peer_to_proto(peer: Peer) -> pb.PeerInfo:
    return pb.PeerInfo(
        peer_id=peer.peer_id,
        name=peer.name,
        host=peer.host,
        udp_port=peer.udp_port,
        control_port=peer.control_port,
    )


def peer_from_proto(message: pb.PeerInfo) -> Peer:
    return Peer(message.peer_id, message.name, message.host, message.udp_port, message.control_port)
