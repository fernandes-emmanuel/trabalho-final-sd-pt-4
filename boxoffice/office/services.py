from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, NoReturn, TypeVar

import grpc

from boxoffice.domain.commands import Command, InvalidCommand, Operation, Outcome
from boxoffice.domain.workload import WorkloadSpec
from boxoffice.office.sales import SalesUnavailable, SessionRejected
from boxoffice.rpc import boxoffice_pb2 as pb
from boxoffice.rpc import boxoffice_pb2_grpc as rpc
from boxoffice.rpc.convert import (
    outcome_to_proto,
    owners_to_proto,
    venue_from_proto,
    venue_to_proto,
)
from mpcomm.model import Endpoint
from mpcomm.ordering.base import Ordering

if TYPE_CHECKING:
    from boxoffice.office.node import OfficeNode

T = TypeVar("T")


def register_services(node: OfficeNode, server: grpc.Server) -> None:
    rpc.add_TicketOfficeServicer_to_server(TicketOfficeService(node), server)
    rpc.add_OfficeControlServicer_to_server(OfficeControlService(node), server)


class TicketOfficeService(rpc.TicketOfficeServicer):
    def __init__(self, node: OfficeNode) -> None:
        self._node = node

    def Reserve(self, request: pb.ReserveRequest, context: grpc.ServicerContext):
        return self._execute(context, Operation.RESERVE, (request.seat,), request.customer)

    def ReserveGroup(self, request: pb.ReserveGroupRequest, context: grpc.ServicerContext):
        return self._execute(
            context, Operation.RESERVE_GROUP, tuple(request.seats), request.customer
        )

    def Cancel(self, request: pb.CancelRequest, context: grpc.ServicerContext):
        return self._execute(context, Operation.CANCEL, (request.seat,), request.customer)

    def GetSeatMap(self, request: pb.Empty, context: grpc.ServicerContext):
        session = _call(context, self._node.session)
        view = session.view()
        return pb.SeatMapReply(
            session_id=session.session_id,
            venue=venue_to_proto(session.venue),
            owners=owners_to_proto(view.owners),
            office_id=session.office_id,
            office_name=session.office_name,
            applied=view.applied,
            digest=view.digest,
        )

    def _execute(
        self,
        context: grpc.ServicerContext,
        operation: Operation,
        seats: tuple[str, ...],
        customer: str,
    ) -> pb.CommandReply:
        session = _call(context, self._node.session)
        command, outcome = _call(context, lambda: session.execute(operation, seats, customer))
        return _reply(command, outcome, session.office_id, session.office_name)


class OfficeControlService(rpc.OfficeControlServicer):
    def __init__(self, node: OfficeNode) -> None:
        self._node = node

    def OpenSession(self, request: pb.OpenSessionRequest, context: grpc.ServicerContext):
        try:
            venue = venue_from_proto(request.venue)
            report_to = Endpoint.parse(request.report_to)
            ordering = Ordering(request.ordering or Ordering.SEQUENCER)
        except ValueError as error:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(error))
        try:
            group_size = self._node.open_session(
                request.session_id, venue, report_to, ordering, request.sequencer
            )
        except SessionRejected as error:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(error))
        except grpc.RpcError as error:
            _abort(
                context, grpc.StatusCode.UNAVAILABLE, f"group manager unreachable: {error.code()}"
            )
        return pb.OpenSessionReply(office_id=self._node.office_id, group_size=group_size)

    def RunWorkload(self, request: pb.WorkloadRequest, context: grpc.ServicerContext):
        try:
            spec = WorkloadSpec(
                commands=request.commands,
                seed=request.seed,
                hot_seats=request.hot_seats,
                hot_probability=request.hot_probability,
                min_interval_ms=request.min_interval_ms,
                max_interval_ms=request.max_interval_ms,
            )
        except ValueError as error:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(error))
        issued, accepted = _call(context, lambda: self._node.run_workload(request.session_id, spec))
        return pb.WorkloadReply(issued=issued, accepted=accepted)

    def CloseSession(self, request: pb.SessionRef, context: grpc.ServicerContext):
        _call(context, lambda: self._node.close_session(request.session_id))
        return pb.Empty()

    def Shutdown(self, request: pb.Empty, context: grpc.ServicerContext):
        self._node.stop()
        return pb.Empty()


def _call(context: grpc.ServicerContext, action: Callable[[], T]) -> T:
    try:
        return action()
    except SalesUnavailable as error:
        _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(error))
    except InvalidCommand as error:
        _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(error))


def _abort(context: grpc.ServicerContext, code: grpc.StatusCode, details: str) -> NoReturn:
    context.abort(code, details)
    raise AssertionError("unreachable")


def _reply(command: Command, outcome: Outcome, office_id: int, office_name: str) -> pb.CommandReply:
    return pb.CommandReply(
        command_id=command.command_id,
        outcome=outcome_to_proto(outcome),
        office_id=office_id,
        office_name=office_name,
    )
