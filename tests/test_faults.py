import threading
import time

import pytest

from mpcomm.transport.faults import FaultProfile, FaultyChannel


class CollectingChannel:
    def __init__(self):
        self.received = []
        self.arrived = threading.Event()

    def send(self, data, address):
        self.received.append(data)
        self.arrived.set()


def test_loss_rate_is_close_to_the_configured_probability():
    inner = CollectingChannel()
    channel = FaultyChannel(inner, FaultProfile(loss=0.3, seed=42))
    for _ in range(10_000):
        channel.send(b"x", ("host", 1))
    channel.close()

    assert 2_700 < channel.dropped < 3_300
    assert len(inner.received) == 10_000 - channel.dropped


def test_duplication_sends_extra_copies():
    inner = CollectingChannel()
    channel = FaultyChannel(inner, FaultProfile(duplication=1.0, seed=1))
    channel.send(b"x", ("host", 1))
    channel.close()

    assert inner.received == [b"x", b"x"]


def test_delayed_packets_are_delivered_later():
    inner = CollectingChannel()
    channel = FaultyChannel(inner, FaultProfile(delay_ms=50, seed=1))
    started = time.monotonic()
    channel.send(b"x", ("host", 1))

    assert inner.received == []
    assert inner.arrived.wait(2)
    assert time.monotonic() - started >= 0.04
    channel.close()


@pytest.mark.parametrize(
    "arguments", [{"loss": 1.0}, {"loss": -0.1}, {"duplication": 2}, {"delay_ms": -1}]
)
def test_invalid_profiles_are_rejected(arguments):
    with pytest.raises(ValueError):
        FaultProfile(**arguments)


def test_profile_is_inactive_by_default():
    assert not FaultProfile().active
    assert FaultProfile(jitter_ms=5).active
