from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass

from boxoffice.domain.commands import Command, Operation, Outcome
from boxoffice.domain.replica import PendingCommand, Replica, ReplicaView
from boxoffice.domain.seats import SeatMap, Venue
from boxoffice.domain.snapshot import ReplicaSnapshot
from boxoffice.domain.workload import WorkloadGenerator, WorkloadSpec
from mpcomm.group.session import GroupSession, SessionClosed
from mpcomm.model import Address, Endpoint
from mpcomm.ordering.base import Ordering
from mpcomm.transport.channel import Channel
from mpcomm.transport.messages import Packet

log = logging.getLogger(__name__)


class SalesUnavailable(Exception):
    pass


class SessionRejected(Exception):
    pass


@dataclass(frozen=True, slots=True)
class SessionTiming:
    open_timeout: float = 30.0
    close_timeout: float = 120.0
    command_timeout: float = 10.0


class SalesSession:
    """One sales period of an event: a replica of the seat map kept up to date by the
    commands that every office multicasts to the group."""

    def __init__(
        self,
        *,
        session_id: int,
        office_id: int,
        office_name: str,
        members: Mapping[int, Address],
        channel: Channel,
        venue: Venue,
        report_to: Endpoint,
        timing: SessionTiming,
        ordering: Ordering = Ordering.SEQUENCER,
        sequencer: int | None = None,
        sequencer_name: str = "",
    ) -> None:
        self.session_id = session_id
        self.office_id = office_id
        self.office_name = office_name
        self.report_to = report_to
        self.ordering = ordering
        self.sequencer_name = sequencer_name if ordering is Ordering.SEQUENCER else ""
        self._timing = timing
        self._replica = Replica(office_id, SeatMap(venue))
        self._group = GroupSession(
            session_id=session_id,
            self_id=office_id,
            members=members,
            channel=channel,
            deliver=self._replica.apply_delivered,
            ordering=ordering,
            sequencer=sequencer,
        )
        self._opened = threading.Event()
        self._open_succeeded = False

    @property
    def venue(self) -> Venue:
        return self._replica.venue

    @property
    def group_size(self) -> int:
        return self._group.size

    def handle(self, packet: Packet) -> None:
        self._group.handle(packet)

    def open(self) -> bool:
        try:
            self._open_succeeded = self._group.open(self._timing.open_timeout)
        finally:
            self._opened.set()
        return self._open_succeeded

    def execute(
        self, operation: Operation, seats: tuple[str, ...], customer: str
    ) -> tuple[Command, Outcome]:
        pending = self._submit(operation, seats, customer)
        outcome = pending.wait(self._timing.command_timeout)
        if outcome is None:
            raise SalesUnavailable(f"command {pending.command.command_id} was not applied in time")
        return pending.command, outcome

    def run_workload(self, spec: WorkloadSpec) -> tuple[int, int]:
        generator = WorkloadGenerator(self.venue, spec, self.office_id)
        submitted: list[PendingCommand] = []
        for _ in range(spec.commands):
            time.sleep(generator.pause())
            request = generator.next_request(self._replica.view().owners)
            submitted.append(self._submit(request.operation, request.seats, request.customer))
        outcomes = [pending.wait(self._timing.command_timeout) for pending in submitted]
        return len(submitted), outcomes.count(Outcome.ACCEPTED)

    def close(self) -> bool:
        return self._group.close(self._timing.close_timeout)

    def view(self) -> ReplicaView:
        return self._replica.view()

    def snapshot(self, completed: bool, dropped_by_fault_injector: int) -> ReplicaSnapshot:
        view = self._replica.view()
        latency = self._replica.latency()
        return ReplicaSnapshot(
            session_id=self.session_id,
            office_id=self.office_id,
            office_name=self.office_name,
            completed=completed,
            digest=view.digest,
            owners=view.owners,
            applied=self._replica.applied(),
            stats=self._group.stats(dropped_by_fault_injector),
            ordering=self.ordering.value,
            latency_mean_ms=latency.mean_ms,
            latency_p95_ms=latency.p95_ms,
            sequencer=self.sequencer_name,
        )

    def _submit(
        self, operation: Operation, seats: tuple[str, ...], customer: str
    ) -> PendingCommand:
        if not self._opened.wait(self._timing.open_timeout) or not self._open_succeeded:
            raise SalesUnavailable("sales are not open at this office")
        pending = self._replica.prepare(operation, seats, customer)
        try:
            self._group.multicast(pending.command.to_payload())
        except SessionClosed as error:
            self._replica.forget(pending)
            raise SalesUnavailable("sales are closed") from error
        return pending
