from __future__ import annotations

from dataclasses import dataclass, field

Address = tuple[str, int]


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int

    @classmethod
    def parse(cls, value: str) -> Endpoint:
        host, separator, port = value.strip().rpartition(":")
        if not separator or not host or not port.isdigit():
            raise ValueError(f"invalid endpoint {value!r}, expected host:port")
        return cls(host, int(port))

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True, slots=True)
class Peer:
    peer_id: int
    name: str
    host: str
    udp_port: int
    control_port: int

    @property
    def udp_address(self) -> Address:
        return (self.host, self.udp_port)

    @property
    def control_endpoint(self) -> Endpoint:
        return Endpoint(self.host, self.control_port)


@dataclass(frozen=True, slots=True)
class TransportStats:
    data_sent: int = 0
    retransmissions: int = 0
    acks_sent: int = 0
    duplicates_discarded: int = 0
    held_back: int = 0
    dropped_by_fault_injector: int = 0
    unreachable_peers: tuple[int, ...] = field(default_factory=tuple)
    order_messages_sent: int = 0
    order_control_sent: int = 0
    order_queue_peak: int = 0
