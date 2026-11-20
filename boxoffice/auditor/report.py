from __future__ import annotations

import dataclasses
from typing import Any

from boxoffice.auditor.analysis import AuditReport, OutcomeConflict, SeatConflict

EXAMPLES = 5
COLUMNS = (
    ("id", 3),
    ("office", 14),
    ("applied", 7),
    ("accepted", 8),
    ("rejected", 8),
    ("retrans", 7),
    ("held", 4),
    ("lat_ms", 6),
    ("digest", 12),
)


def render(report: AuditReport) -> str:
    header = "  ".join(name.ljust(width) for name, width in COLUMNS)
    lines = [
        f"Session {report.session_id}: {len(report.group)} offices, {_ordering(report)}, "
        f"{report.commands} commands",
        header,
        "-" * len(header),
    ]
    for office in report.offices:
        values = (
            office.office_id,
            office.office_name,
            office.applied,
            office.accepted,
            office.rejected,
            office.stats.retransmissions,
            office.stats.held_back,
            f"{office.latency_mean_ms:.0f}",
            office.digest[:12],
        )
        lines.append(
            "  ".join(
                str(value).ljust(width) for value, (_, width) in zip(values, COLUMNS, strict=True)
            )
        )
    if report.absent_offices:
        lines.append(f"No snapshot received from offices {list(report.absent_offices)}")
    divergence = report.first_divergent_position
    lines += [
        "",
        f"Every command reached every replica : {_verdict(report.complete)}",
        f"Distinct replica states             : {report.distinct_states}",
        f"Seats with divergent owners         : {len(report.seat_conflicts)}"
        f" ({report.overbooked_seats} sold to different customers)",
        f"Commands with divergent outcomes    : {len(report.outcome_conflicts)}",
        "Replica logs diverge from position  : "
        + ("never" if divergence is None else str(divergence)),
        f"Replicas consistent                 : {'YES' if report.consistent else 'NO'}",
        f"Command latency (mean / p95)        : {report.latency_mean_ms:.1f} ms"
        f" / {report.latency_p95_ms:.1f} ms",
        f"Network packets per command         : {report.packets_per_command:.1f}",
    ]
    if report.seat_conflicts:
        lines += ["", "Seats with divergent owners:"]
        lines += [f"  {_describe_seat(conflict)}" for conflict in report.seat_conflicts[:EXAMPLES]]
    if report.outcome_conflicts:
        lines += ["", "Commands with divergent outcomes:"]
        lines += [
            f"  {_describe_command(conflict)}" for conflict in report.outcome_conflicts[:EXAMPLES]
        ]
    return "\n".join(lines)


def to_dict(report: AuditReport) -> dict[str, Any]:
    return {
        **dataclasses.asdict(report),
        "complete": report.complete,
        "consistent": report.consistent,
        "overbooked_seats": report.overbooked_seats,
    }


def _describe_seat(conflict: SeatConflict) -> str:
    owners = " | ".join(f"{office}: {owner or '-'}" for office, owner in conflict.owners)
    return f"{conflict.seat:<4} {owners}"


def _describe_command(conflict: OutcomeConflict) -> str:
    command = conflict.command
    outcomes = " | ".join(f"{office}: {outcome.value}" for office, outcome in conflict.outcomes)
    seats = ",".join(command.seats)
    action = f"{command.command_id} {command.operation.value} {seats} for {command.customer}"
    return f"{action} -> {outcomes}"


def _ordering(report: AuditReport) -> str:
    if report.sequencer:
        return f"{report.ordering} order (sequencer: {report.sequencer})"
    return f"{report.ordering} order"


def _verdict(ok: bool) -> str:
    return "OK" if ok else "FAILED"
