from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from boxoffice.domain.commands import Command, Outcome
from boxoffice.domain.snapshot import ReplicaSnapshot
from mpcomm.model import TransportStats


@dataclass(frozen=True, slots=True)
class OfficeSummary:
    office_id: int
    office_name: str
    completed: bool
    applied: int
    accepted: int
    digest: str
    stats: TransportStats
    latency_mean_ms: float = 0.0
    latency_p95_ms: float = 0.0

    @property
    def rejected(self) -> int:
        return self.applied - self.accepted


@dataclass(frozen=True, slots=True)
class SeatConflict:
    seat: str
    owners: tuple[tuple[str, str | None], ...]

    @property
    def overbooked(self) -> bool:
        return len({owner for _, owner in self.owners if owner is not None}) > 1


@dataclass(frozen=True, slots=True)
class OutcomeConflict:
    command: Command
    outcomes: tuple[tuple[str, Outcome], ...]


@dataclass(frozen=True, slots=True)
class AuditReport:
    session_id: int
    ordering: str
    sequencer: str
    group: tuple[int, ...]
    offices: tuple[OfficeSummary, ...]
    absent_offices: tuple[int, ...]
    commands_missing: int
    distinct_states: int
    seat_conflicts: tuple[SeatConflict, ...]
    outcome_conflicts: tuple[OutcomeConflict, ...]
    first_divergent_position: int | None
    commands: int = 0
    packets_per_command: float = 0.0
    latency_mean_ms: float = 0.0
    latency_p95_ms: float = 0.0

    @property
    def complete(self) -> bool:
        return (
            not self.absent_offices
            and self.commands_missing == 0
            and all(office.completed for office in self.offices)
        )

    @property
    def consistent(self) -> bool:
        return self.complete and self.distinct_states == 1

    @property
    def overbooked_seats(self) -> int:
        return sum(conflict.overbooked for conflict in self.seat_conflicts)


def audit(
    session_id: int, group: Sequence[int], snapshots: Mapping[int, ReplicaSnapshot]
) -> AuditReport:
    present = [snapshots[office] for office in group if office in snapshots]
    commands = len({entry.command.command_id for s in present for entry in s.applied})
    offices = tuple(_summarize(snapshot) for snapshot in present)
    return AuditReport(
        session_id=session_id,
        ordering=present[0].ordering if present else "",
        sequencer=present[0].sequencer if present else "",
        group=tuple(group),
        offices=offices,
        absent_offices=tuple(office for office in group if office not in snapshots),
        commands_missing=_count_missing_commands(present),
        distinct_states=len({snapshot.digest for snapshot in present}),
        seat_conflicts=_seat_conflicts(present),
        outcome_conflicts=_outcome_conflicts(present),
        first_divergent_position=first_divergent_position(
            [[entry.command.command_id for entry in snapshot.applied] for snapshot in present]
        ),
        commands=commands,
        packets_per_command=_packets_per_command(present, len(group), commands),
        latency_mean_ms=statistics.fmean(o.latency_mean_ms for o in offices) if offices else 0.0,
        latency_p95_ms=max((o.latency_p95_ms for o in offices), default=0.0),
    )


def first_divergent_position(sequences: Sequence[Sequence[str]]) -> int | None:
    if not sequences:
        return None
    longest = max(len(sequence) for sequence in sequences)
    for position in range(longest):
        if len({_at(sequence, position) for sequence in sequences}) > 1:
            return position
    return None


def _summarize(snapshot: ReplicaSnapshot) -> OfficeSummary:
    return OfficeSummary(
        office_id=snapshot.office_id,
        office_name=snapshot.office_name,
        completed=snapshot.completed,
        applied=len(snapshot.applied),
        accepted=sum(entry.outcome is Outcome.ACCEPTED for entry in snapshot.applied),
        digest=snapshot.digest,
        stats=snapshot.stats,
        latency_mean_ms=snapshot.latency_mean_ms,
        latency_p95_ms=snapshot.latency_p95_ms,
    )


def _packets_per_command(
    snapshots: Sequence[ReplicaSnapshot], group_size: int, commands: int
) -> float:
    if commands == 0:
        return 0.0
    packets = sum(
        s.stats.data_sent * group_size + s.stats.retransmissions + s.stats.acks_sent
        for s in snapshots
    )
    return packets / commands


def _count_missing_commands(snapshots: Sequence[ReplicaSnapshot]) -> int:
    applied_sets = [{entry.command.command_id for entry in s.applied} for s in snapshots]
    if not applied_sets:
        return 0
    everything = set().union(*applied_sets)
    return sum(len(everything - applied) for applied in applied_sets)


def _seat_conflicts(snapshots: Sequence[ReplicaSnapshot]) -> tuple[SeatConflict, ...]:
    seats = (
        sorted(set().union(*(s.owners for s in snapshots)), key=_seat_order) if snapshots else []
    )
    conflicts = []
    for seat in seats:
        owners = tuple((s.office_name, s.owners.get(seat)) for s in snapshots)
        if len({owner for _, owner in owners}) > 1:
            conflicts.append(SeatConflict(seat, owners))
    return tuple(conflicts)


def _outcome_conflicts(snapshots: Sequence[ReplicaSnapshot]) -> tuple[OutcomeConflict, ...]:
    outcomes: dict[str, list[tuple[str, Outcome]]] = {}
    commands: dict[str, Command] = {}
    for snapshot in snapshots:
        for entry in snapshot.applied:
            commands[entry.command.command_id] = entry.command
            outcomes.setdefault(entry.command.command_id, []).append(
                (snapshot.office_name, entry.outcome)
            )
    return tuple(
        OutcomeConflict(commands[command_id], tuple(results))
        for command_id, results in sorted(
            outcomes.items(), key=lambda item: _command_order(item[0])
        )
        if len({outcome for _, outcome in results}) > 1
    )


def _at(sequence: Sequence[str], position: int) -> str | None:
    return sequence[position] if position < len(sequence) else None


def _seat_order(seat: str) -> tuple[str, int]:
    return seat[0], int(seat[1:])


def _command_order(command_id: str) -> tuple[int, int]:
    office, _, number = command_id.partition(".")
    return int(office), int(number)
