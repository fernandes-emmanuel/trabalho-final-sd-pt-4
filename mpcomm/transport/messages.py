from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Kind(StrEnum):
    HELLO = "hello"
    DATA = "data"
    END = "end"


@dataclass(frozen=True, slots=True)
class DataPacket:
    round_id: int
    sender: int
    seq: int
    kind: Kind
    payload: Any = None


@dataclass(frozen=True, slots=True)
class AckPacket:
    round_id: int
    acker: int
    origin: int
    seq: int


Packet = DataPacket | AckPacket


class MalformedPacket(ValueError):
    pass


def encode(packet: Packet) -> bytes:
    match packet:
        case DataPacket():
            body = {
                "type": "data",
                "round": packet.round_id,
                "sender": packet.sender,
                "seq": packet.seq,
                "kind": packet.kind.value,
                "payload": packet.payload,
            }
        case AckPacket():
            body = {
                "type": "ack",
                "round": packet.round_id,
                "acker": packet.acker,
                "origin": packet.origin,
                "seq": packet.seq,
            }
    return json.dumps(body, separators=(",", ":")).encode()


def decode(raw: bytes) -> Packet:
    try:
        body = json.loads(raw)
        packet_type = body["type"]
        if packet_type == "data":
            return DataPacket(
                round_id=int(body["round"]),
                sender=int(body["sender"]),
                seq=int(body["seq"]),
                kind=Kind(body["kind"]),
                payload=body.get("payload"),
            )
        if packet_type == "ack":
            return AckPacket(
                round_id=int(body["round"]),
                acker=int(body["acker"]),
                origin=int(body["origin"]),
                seq=int(body["seq"]),
            )
    except (ValueError, KeyError, TypeError) as error:
        raise MalformedPacket(str(error)) from error
    raise MalformedPacket(f"unknown packet type {packet_type!r}")
