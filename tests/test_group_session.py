import threading
from collections import defaultdict

import pytest

from mpcomm.group.session import GroupSession, SessionClosed, SessionState
from mpcomm.ordering.base import Ordering
from mpcomm.transport.faults import FaultProfile, FaultyChannel
from mpcomm.transport.messages import decode
from mpcomm.transport.reliable import RetransmissionPolicy
from mpcomm.transport.udp import UdpEndpoint

SESSION = 42


@pytest.fixture(params=list(Ordering))
def group(request):
    endpoints = {member: UdpEndpoint("127.0.0.1", 0) for member in range(3)}
    members = {member: ("127.0.0.1", endpoint.port) for member, endpoint in endpoints.items()}
    delivered = defaultdict(list)
    channels, sessions = [], {}
    for member, endpoint in endpoints.items():
        channel = FaultyChannel(endpoint, FaultProfile(loss=0.2, jitter_ms=5, seed=member))
        session = GroupSession(
            session_id=SESSION,
            self_id=member,
            members=members,
            channel=channel,
            deliver=lambda sender, payload, member=member: delivered[member].append(
                (sender, payload)
            ),
            ordering=request.param,
            policy=RetransmissionPolicy(initial_timeout=0.05),
        )
        endpoint.serve(lambda data, _, session=session: session.handle(decode(data)))
        channels.append(channel)
        sessions[member] = session
    yield sessions, delivered
    for channel in channels:
        channel.close()
    for endpoint in endpoints.values():
        endpoint.close()


def run_concurrently(sessions, action):
    results = {}

    def run(member, session):
        results[member] = action(member, session)

    threads = [threading.Thread(target=run, args=item) for item in sessions.items()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def test_members_open_exchange_and_close_reliably_over_lossy_udp(group):
    sessions, delivered = group
    count = 25

    assert all(run_concurrently(sessions, lambda _, s: s.open(timeout=10)).values())

    def send_and_close(member, session):
        for number in range(count):
            session.multicast({"from": member, "n": number})
        return session.close(timeout=20)

    assert all(run_concurrently(sessions, send_and_close).values())
    for member in sessions:
        assert len(delivered[member]) == len(sessions) * count
        for sender in sessions:
            numbers = [payload["n"] for origin, payload in delivered[member] if origin == sender]
            assert numbers == list(range(count))
    assert all(session.state is SessionState.CLOSED for session in sessions.values())
    if sessions[0].ordering is not Ordering.FIFO:
        assert all(delivered[member] == delivered[0] for member in sessions)
        control = {member: s.stats().order_control_sent for member, s in sessions.items()}
        if sessions[0].ordering is Ordering.SEQUENCER:
            assert control[0] > 0
            assert all(sent == 0 for member, sent in control.items() if member != 0)
        else:
            assert all(sent > 0 for sent in control.values())
    assert sum(session.stats().retransmissions for session in sessions.values()) > 0


def test_multicast_is_refused_before_opening_and_after_closing(group):
    sessions, _ = group
    session = sessions[0]

    with pytest.raises(SessionClosed):
        session.multicast("too early")

    run_concurrently(sessions, lambda _, s: s.open(timeout=10))
    run_concurrently(sessions, lambda _, s: s.close(timeout=20))

    with pytest.raises(SessionClosed):
        session.multicast("too late")
