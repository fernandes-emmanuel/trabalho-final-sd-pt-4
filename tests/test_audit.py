from boxoffice.auditor.analysis import audit, first_divergent_position
from boxoffice.auditor.report import render, to_dict
from boxoffice.domain.commands import AppliedCommand, Command, Operation, Outcome
from boxoffice.domain.snapshot import ReplicaSnapshot
from mpcomm.model import TransportStats

ANA = Command("0.1", Operation.RESERVE, ("A1",), "ana")
BRUNO = Command("1.1", Operation.RESERVE, ("A1",), "bruno")


def snapshot(office_id, name, applied, owners, digest, completed=True, stats=None):
    return ReplicaSnapshot(
        session_id=1,
        office_id=office_id,
        office_name=name,
        completed=completed,
        digest=digest,
        owners=owners,
        applied=tuple(AppliedCommand(command, outcome) for command, outcome in applied),
        stats=stats or TransportStats(),
        ordering="sequencer",
        sequencer="sao-paulo",
        latency_mean_ms=10.0 * (office_id + 1),
        latency_p95_ms=20.0 * (office_id + 1),
    )


def test_identical_replicas_are_consistent():
    applied = [(ANA, Outcome.ACCEPTED), (BRUNO, Outcome.SEAT_TAKEN)]
    snapshots = {
        0: snapshot(0, "sao-paulo", applied, {"A1": "ana"}, "d1"),
        1: snapshot(1, "rio", applied, {"A1": "ana"}, "d1"),
    }
    report = audit(1, [0, 1], snapshots)

    assert report.complete
    assert report.consistent
    assert report.seat_conflicts == ()
    assert report.outcome_conflicts == ()
    assert report.first_divergent_position is None
    assert "Replicas consistent                 : YES" in render(report)


def test_conflicting_orders_are_reported_as_overbooking():
    snapshots = {
        0: snapshot(0, "sao-paulo", [(ANA, Outcome.ACCEPTED), (BRUNO, Outcome.SEAT_TAKEN)],
                    {"A1": "ana"}, "d1"),
        1: snapshot(1, "lisboa", [(BRUNO, Outcome.ACCEPTED), (ANA, Outcome.SEAT_TAKEN)],
                    {"A1": "bruno"}, "d2"),
    }  # fmt: skip
    report = audit(1, [0, 1], snapshots)

    assert report.complete
    assert not report.consistent
    assert report.distinct_states == 2
    assert report.overbooked_seats == 1
    assert report.seat_conflicts[0].owners == (("sao-paulo", "ana"), ("lisboa", "bruno"))
    assert [conflict.command.command_id for conflict in report.outcome_conflicts] == ["0.1", "1.1"]
    assert report.first_divergent_position == 0

    text = render(report)
    assert "A1   sao-paulo: ana | lisboa: bruno" in text
    assert "0.1 reserve A1 for ana -> sao-paulo: accepted | lisboa: seat_taken" in text
    assert to_dict(report)["overbooked_seats"] == 1


def test_missing_commands_and_absent_replicas_make_the_audit_incomplete():
    snapshots = {
        0: snapshot(0, "sao-paulo", [(ANA, Outcome.ACCEPTED), (BRUNO, Outcome.SEAT_TAKEN)],
                    {"A1": "ana"}, "d1"),
        1: snapshot(1, "rio", [(ANA, Outcome.ACCEPTED)], {"A1": "ana"}, "d1", completed=False),
    }  # fmt: skip
    report = audit(1, [0, 1, 2], snapshots)

    assert report.absent_offices == (2,)
    assert report.commands_missing == 1
    assert not report.complete
    assert not report.consistent


def test_cost_metrics_are_aggregated():
    applied = [(ANA, Outcome.ACCEPTED), (BRUNO, Outcome.SEAT_TAKEN)]
    stats = TransportStats(data_sent=6, retransmissions=2, acks_sent=12)
    snapshots = {
        0: snapshot(0, "sao-paulo", applied, {"A1": "ana"}, "d1", stats=stats),
        1: snapshot(1, "rio", applied, {"A1": "ana"}, "d1", stats=stats),
    }
    report = audit(1, [0, 1], snapshots)

    assert report.ordering == "sequencer"
    assert report.sequencer == "sao-paulo"
    assert "sequencer order (sequencer: sao-paulo)" in render(report)
    assert report.commands == 2
    assert report.packets_per_command == (2 * (6 * 2 + 2 + 12)) / 2
    assert report.latency_mean_ms == 15.0
    assert report.latency_p95_ms == 40.0
    assert "Network packets per command         : 26.0" in render(report)


def test_first_divergent_position():
    assert first_divergent_position([["a", "b"], ["a", "b"]]) is None
    assert first_divergent_position([["a", "b"], ["a", "c"]]) == 1
    assert first_divergent_position([["a"], ["a", "b"]]) == 1
    assert first_divergent_position([]) is None
