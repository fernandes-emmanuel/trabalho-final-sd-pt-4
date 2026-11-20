from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

import grpc

from boxoffice.domain.seats import Venue
from boxoffice.domain.workload import WorkloadSpec
from boxoffice.office.sales import (
    SalesSession,
    SalesUnavailable,
    SessionRejected,
    SessionTiming,
)
from boxoffice.office.services import register_services
from boxoffice.rpc.clients import AuditorClient
from mpcomm.model import Address, Endpoint, Peer
from mpcomm.ordering.base import Ordering
from mpcomm.rpc.clients import GroupManagerClient
from mpcomm.rpc.server import start_server, stop_server, wait_until_stopped
from mpcomm.transport.channel import Channel
from mpcomm.transport.faults import FaultProfile, FaultyChannel
from mpcomm.transport.messages import (
    AckPacket,
    DataPacket,
    MalformedPacket,
    Packet,
    decode,
    encode,
)
from mpcomm.transport.udp import UdpEndpoint

log = logging.getLogger(__name__)

EARLY_PACKET_LIMIT = 10_000


@dataclass(frozen=True, slots=True)
class OfficeSettings:
    name: str
    group_manager: Endpoint
    bind_host: str
    advertise_host: str
    udp_port: int
    control_port: int
    faults: FaultProfile = field(default_factory=FaultProfile)
    timing: SessionTiming = field(default_factory=SessionTiming)
    registration_timeout: float = 30.0


class OfficeNode:
    def __init__(self, settings: OfficeSettings) -> None:
        self.name = settings.name
        self._settings = settings
        self._endpoint = UdpEndpoint(settings.bind_host, settings.udp_port)
        self._faulty = (
            FaultyChannel(self._endpoint, settings.faults) if settings.faults.active else None
        )
        self._channel: Channel = self._faulty or self._endpoint
        self._group_manager = GroupManagerClient(settings.group_manager)
        self._lock = threading.Lock()
        self._dispatch_lock = threading.Lock()
        self._early: dict[int, list[Packet]] = {}
        self._current: SalesSession | None = None
        self._closing = False
        self._members: dict[int, Address] = {}
        self._closed_sessions: set[int] = set()
        self._stop = threading.Event()
        self.office_id = -1

    def run(self) -> None:
        self._endpoint.serve(self._on_datagram)
        listen = Endpoint(self._settings.bind_host, self._settings.control_port)
        server, _ = start_server(listen, lambda s: register_services(self, s))
        try:
            self.office_id = self._group_manager.register(
                self.name,
                self._settings.advertise_host,
                self._endpoint.port,
                self._settings.control_port,
                wait=self._settings.registration_timeout,
            )
            log.info(
                "office %s (%s) registered at %s:%s",
                self.office_id,
                self.name,
                self._settings.advertise_host,
                self._endpoint.port,
            )
            wait_until_stopped(self._stop)
        finally:
            self._leave_group()
            stop_server(server)
            self._close_transport()
        log.info("office %s stopped", self.name)

    def stop(self) -> None:
        self._stop.set()

    def open_session(
        self,
        session_id: int,
        venue: Venue,
        report_to: Endpoint,
        ordering: Ordering,
        sequencer_name: str = "",
    ) -> int:
        with self._lock:
            current = self._current
            if session_id in self._closed_sessions or (
                current is not None and current.session_id == session_id
            ):
                raise SessionRejected(f"session {session_id} was already opened")
            if current is not None and current.session_id not in self._closed_sessions:
                raise SessionRejected(f"session {current.session_id} is still open at this office")
        peers = self._group_manager.list_peers()
        members = {peer.peer_id: peer.udp_address for peer in peers}
        if self.office_id not in members:
            raise SessionRejected(f"office {self.name} is not registered in the group")
        sequencer = _choose_sequencer(peers, sequencer_name)
        session = SalesSession(
            session_id=session_id,
            office_id=self.office_id,
            office_name=self.name,
            members=members,
            channel=self._channel,
            venue=venue,
            report_to=report_to,
            timing=self._settings.timing,
            ordering=ordering,
            sequencer=sequencer.peer_id,
            sequencer_name=sequencer.name,
        )
        with self._dispatch_lock:
            with self._lock:
                self._current = session
                self._closing = False
                self._members = members
                early = self._early.pop(session_id, [])
                self._early.clear()
            for packet in early:
                session.handle(packet)
        threading.Thread(target=self._open, args=(session,), daemon=True).start()
        log.info(
            "office %s opening sales session %s (%s offices, %s order, %sx%s venue)",
            self.name,
            session_id,
            len(members),
            ordering.value,
            venue.rows,
            venue.seats_per_row,
        )
        return len(members)

    def session(self, session_id: int | None = None) -> SalesSession:
        with self._lock:
            current = self._current
        if current is None:
            raise SalesUnavailable("no sales session has been opened at this office")
        if session_id is not None and current.session_id != session_id:
            raise SalesUnavailable(f"session {session_id} is not the current session")
        return current

    def run_workload(self, session_id: int, spec: WorkloadSpec) -> tuple[int, int]:
        issued, accepted = self.session(session_id).run_workload(spec)
        log.info("office %s issued %s commands, %s accepted locally", self.name, issued, accepted)
        return issued, accepted

    def close_session(self, session_id: int) -> None:
        session = self.session(session_id)
        with self._lock:
            if self._closing or session_id in self._closed_sessions:
                raise SalesUnavailable(f"session {session_id} is already closing")
            self._closing = True
        threading.Thread(target=self._close, args=(session,), daemon=True).start()

    def _open(self, session: SalesSession) -> None:
        if session.open():
            log.info("office %s: sales session %s is open", self.name, session.session_id)

    def _close(self, session: SalesSession) -> None:
        dropped_before = self._dropped()
        completed = session.close()
        with self._lock:
            self._closed_sessions.add(session.session_id)
        snapshot = session.snapshot(completed, self._dropped() - dropped_before)
        log.info(
            "office %s closed session %s: %s commands applied, digest %s",
            self.name,
            session.session_id,
            len(snapshot.applied),
            snapshot.digest[:12],
        )
        try:
            with AuditorClient(session.report_to) as auditor:
                auditor.submit_snapshot(snapshot)
        except grpc.RpcError as error:
            log.error("could not send snapshot to %s: %s", session.report_to, error.code())

    def _on_datagram(self, data: bytes, address: Address) -> None:
        try:
            packet = decode(data)
        except MalformedPacket as error:
            log.warning("discarding malformed datagram from %s: %s", address, error)
            return
        with self._dispatch_lock:
            with self._lock:
                current = self._current
                stale = packet.round_id in self._closed_sessions
                sender_address = self._members.get(getattr(packet, "sender", -1))
                if not stale and (current is None or packet.round_id != current.session_id):
                    self._hold_early(packet)
                    return
            if current is not None and packet.round_id == current.session_id:
                current.handle(packet)
            elif isinstance(packet, DataPacket) and sender_address is not None:
                ack = AckPacket(packet.round_id, self.office_id, packet.sender, packet.seq)
                self._channel.send(encode(ack), sender_address)

    def _hold_early(self, packet: Packet) -> None:
        if sum(len(packets) for packets in self._early.values()) < EARLY_PACKET_LIMIT:
            self._early.setdefault(packet.round_id, []).append(packet)

    def _dropped(self) -> int:
        return self._faulty.dropped if self._faulty else 0

    def _leave_group(self) -> None:
        if self.office_id < 0:
            return
        try:
            self._group_manager.unregister(self.office_id)
        except grpc.RpcError as error:
            log.warning("could not unregister from group manager: %s", error.code())
        finally:
            self._group_manager.close()

    def _close_transport(self) -> None:
        self._endpoint.close()
        if self._faulty is not None:
            self._faulty.close()


def _choose_sequencer(peers: list[Peer], name: str) -> Peer:
    if not name:
        return min(peers, key=lambda peer: peer.peer_id)
    for peer in peers:
        if peer.name == name:
            return peer
    raise SessionRejected(f"sequencer {name!r} is not a member of the group")
