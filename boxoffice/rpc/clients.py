from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from boxoffice.domain.commands import Outcome
from boxoffice.domain.seats import Venue
from boxoffice.domain.snapshot import ReplicaSnapshot
from boxoffice.domain.workload import WorkloadSpec
from boxoffice.rpc import boxoffice_pb2 as pb
from boxoffice.rpc import boxoffice_pb2_grpc as rpc
from boxoffice.rpc.convert import (
    outcome_from_proto,
    owners_from_proto,
    snapshot_to_proto,
    venue_from_proto,
    venue_to_proto,
)
from mpcomm.model import Endpoint
from mpcomm.ordering.base import Ordering
from mpcomm.rpc.clients import DEFAULT_TIMEOUT, RpcClient

COMMAND_TIMEOUT = 15.0


@dataclass(frozen=True, slots=True)
class CommandResult:
    command_id: str
    outcome: Outcome
    office_id: int
    office_name: str


@dataclass(frozen=True, slots=True)
class SeatMapSnapshot:
    session_id: int
    venue: Venue
    owners: dict[str, str]
    office_id: int
    office_name: str
    applied: int
    digest: str


class TicketOfficeClient(RpcClient):
    def __init__(self, endpoint: Endpoint, timeout: float = COMMAND_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.TicketOfficeStub(self._channel)

    def reserve(self, seat: str, customer: str) -> CommandResult:
        reply = self._stub.Reserve(
            pb.ReserveRequest(seat=seat, customer=customer), timeout=self._timeout
        )
        return _result(reply)

    def reserve_group(self, seats: Sequence[str], customer: str) -> CommandResult:
        request = pb.ReserveGroupRequest(seats=list(seats), customer=customer)
        return _result(self._stub.ReserveGroup(request, timeout=self._timeout))

    def cancel(self, seat: str, customer: str) -> CommandResult:
        reply = self._stub.Cancel(
            pb.CancelRequest(seat=seat, customer=customer), timeout=self._timeout
        )
        return _result(reply)

    def seat_map(self) -> SeatMapSnapshot:
        reply = self._stub.GetSeatMap(pb.Empty(), timeout=self._timeout)
        return SeatMapSnapshot(
            session_id=reply.session_id,
            venue=venue_from_proto(reply.venue),
            owners=owners_from_proto(reply.owners),
            office_id=reply.office_id,
            office_name=reply.office_name,
            applied=reply.applied,
            digest=reply.digest,
        )


class OfficeControlClient(RpcClient):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.OfficeControlStub(self._channel)

    def open_session(
        self,
        session_id: int,
        venue: Venue,
        report_to: Endpoint,
        ordering: Ordering = Ordering.SEQUENCER,
        sequencer: str = "",
    ) -> int:
        request = pb.OpenSessionRequest(
            session_id=session_id,
            venue=venue_to_proto(venue),
            report_to=str(report_to),
            ordering=ordering.value,
            sequencer=sequencer,
        )
        return self._stub.OpenSession(request, timeout=self._timeout).group_size

    def run_workload(self, session_id: int, spec: WorkloadSpec, timeout: float) -> tuple[int, int]:
        request = pb.WorkloadRequest(
            session_id=session_id,
            commands=spec.commands,
            seed=spec.seed,
            hot_seats=spec.hot_seats,
            hot_probability=spec.hot_probability,
            min_interval_ms=spec.min_interval_ms,
            max_interval_ms=spec.max_interval_ms,
        )
        reply = self._stub.RunWorkload(request, timeout=timeout)
        return reply.issued, reply.accepted

    def close_session(self, session_id: int) -> None:
        self._stub.CloseSession(pb.SessionRef(session_id=session_id), timeout=self._timeout)

    def shutdown(self) -> None:
        self._stub.Shutdown(pb.Empty(), timeout=self._timeout)


class AuditorClient(RpcClient):
    def __init__(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> None:
        super().__init__(endpoint, timeout)
        self._stub = rpc.AuditorStub(self._channel)

    def submit_snapshot(self, snapshot: ReplicaSnapshot) -> None:
        self._stub.SubmitSnapshot(snapshot_to_proto(snapshot), timeout=self._timeout)


def _result(reply: pb.CommandReply) -> CommandResult:
    return CommandResult(
        reply.command_id, outcome_from_proto(reply.outcome), reply.office_id, reply.office_name
    )
