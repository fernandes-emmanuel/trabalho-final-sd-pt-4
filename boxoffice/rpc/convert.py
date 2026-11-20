from collections.abc import Mapping

from boxoffice.domain.commands import AppliedCommand, Command, Operation, Outcome
from boxoffice.domain.seats import Venue
from boxoffice.domain.snapshot import ReplicaSnapshot
from boxoffice.rpc import boxoffice_pb2 as pb
from mpcomm.model import TransportStats


def outcome_to_proto(outcome: Outcome) -> pb.Outcome:
    return pb.Outcome.Value(outcome.name)


def outcome_from_proto(outcome: pb.Outcome) -> Outcome:
    return Outcome[pb.Outcome.Name(outcome)]


def venue_to_proto(venue: Venue) -> pb.Venue:
    return pb.Venue(rows=venue.rows, seats_per_row=venue.seats_per_row)


def venue_from_proto(message: pb.Venue) -> Venue:
    return Venue(message.rows, message.seats_per_row)


def owners_to_proto(owners: Mapping[str, str]) -> list[pb.SeatOwner]:
    return [pb.SeatOwner(seat=seat, customer=customer) for seat, customer in owners.items()]


def owners_from_proto(owners) -> dict[str, str]:
    return {owner.seat: owner.customer for owner in owners}


def snapshot_to_proto(snapshot: ReplicaSnapshot) -> pb.ReplicaSnapshot:
    stats = snapshot.stats
    return pb.ReplicaSnapshot(
        session_id=snapshot.session_id,
        office_id=snapshot.office_id,
        office_name=snapshot.office_name,
        completed=snapshot.completed,
        digest=snapshot.digest,
        owners=owners_to_proto(snapshot.owners),
        applied=[
            pb.AppliedCommand(
                command_id=entry.command.command_id,
                operation=entry.command.operation.value,
                seats=list(entry.command.seats),
                customer=entry.command.customer,
                outcome=outcome_to_proto(entry.outcome),
            )
            for entry in snapshot.applied
        ],
        stats=pb.TransportStats(
            data_sent=stats.data_sent,
            retransmissions=stats.retransmissions,
            acks_sent=stats.acks_sent,
            duplicates_discarded=stats.duplicates_discarded,
            held_back=stats.held_back,
            dropped_by_fault_injector=stats.dropped_by_fault_injector,
            unreachable_peers=list(stats.unreachable_peers),
            order_messages_sent=stats.order_messages_sent,
            order_control_sent=stats.order_control_sent,
            order_queue_peak=stats.order_queue_peak,
        ),
        ordering=snapshot.ordering,
        latency_mean_ms=snapshot.latency_mean_ms,
        latency_p95_ms=snapshot.latency_p95_ms,
        sequencer=snapshot.sequencer,
    )


def snapshot_from_proto(message: pb.ReplicaSnapshot) -> ReplicaSnapshot:
    stats = message.stats
    return ReplicaSnapshot(
        session_id=message.session_id,
        office_id=message.office_id,
        office_name=message.office_name,
        completed=message.completed,
        digest=message.digest,
        owners=owners_from_proto(message.owners),
        applied=tuple(
            AppliedCommand(
                Command(
                    entry.command_id, Operation(entry.operation), tuple(entry.seats), entry.customer
                ),
                outcome_from_proto(entry.outcome),
            )
            for entry in message.applied
        ),
        stats=TransportStats(
            data_sent=stats.data_sent,
            retransmissions=stats.retransmissions,
            acks_sent=stats.acks_sent,
            duplicates_discarded=stats.duplicates_discarded,
            held_back=stats.held_back,
            dropped_by_fault_injector=stats.dropped_by_fault_injector,
            unreachable_peers=tuple(stats.unreachable_peers),
            order_messages_sent=stats.order_messages_sent,
            order_control_sent=stats.order_control_sent,
            order_queue_peak=stats.order_queue_peak,
        ),
        ordering=message.ordering,
        latency_mean_ms=message.latency_mean_ms,
        latency_p95_ms=message.latency_p95_ms,
        sequencer=message.sequencer,
    )
