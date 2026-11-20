import pytest

from mpcomm.model import Endpoint
from mpcomm.transport.messages import AckPacket, DataPacket, Kind, MalformedPacket, decode, encode


@pytest.mark.parametrize(
    "packet",
    [
        DataPacket(3, 1, 0, Kind.HELLO),
        DataPacket(3, 1, 5, Kind.DATA, 4),
        DataPacket(3, 1, 9, Kind.DATA, {"seat": "A7", "customer": "ana"}),
        DataPacket(3, 2, 6, Kind.END),
        AckPacket(3, 0, 1, 5),
    ],
)
def test_packets_survive_a_round_trip(packet):
    assert decode(encode(packet)) == packet


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"[]",
        b'{"type": "data"}',
        b'{"type": "data", "round": 1, "sender": 0, "seq": 0, "kind": "bogus"}',
        b'{"type": "unknown"}',
    ],
)
def test_malformed_datagrams_are_rejected(raw):
    with pytest.raises(MalformedPacket):
        decode(raw)


def test_endpoint_parsing():
    assert Endpoint.parse("10.0.0.1:5680") == Endpoint("10.0.0.1", 5680)
    assert str(Endpoint.parse(" group-manager:5680 ")) == "group-manager:5680"
    for invalid in ("localhost", ":5680", "host:port"):
        with pytest.raises(ValueError):
            Endpoint.parse(invalid)
