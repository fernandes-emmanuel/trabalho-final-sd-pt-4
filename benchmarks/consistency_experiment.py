"""Compares replica consistency and cost of each ordering mechanism as the network degrades."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path

from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from mpcomm.ordering.base import Ordering
from mpcomm.transport.faults import FaultProfile


@dataclass(frozen=True)
class Scenario:
    delay_ms: float
    loss: float

    @property
    def label(self) -> str:
        loss = f", {self.loss:.0%} loss" if self.loss else ""
        return f"{self.delay_ms:.0f} +/- {self.delay_ms:.0f} ms{loss}"


def run(scenario: Scenario, ordering: Ordering, args: argparse.Namespace, workdir: Path) -> dict:
    report_path = workdir / f"{ordering}-{scenario.delay_ms}-{scenario.loss}.json"
    deployment = LocalDeployment(
        offices=args.offices,
        sessions=args.sessions,
        workload=WorkloadSpec(commands=args.commands, seed=args.seed),
        ordering=ordering,
        base_port=args.base_port,
        faults=FaultProfile(
            loss=scenario.loss,
            delay_ms=scenario.delay_ms,
            jitter_ms=scenario.delay_ms,
            seed=args.seed,
        ),
        report=report_path,
        log_level="warning",
    )
    run_local(deployment)
    reports = json.loads(report_path.read_text())
    return {
        "scenario": scenario.label,
        "ordering": ordering.value,
        "sessions": len(reports),
        "complete": sum(r["complete"] for r in reports),
        "consistent": sum(r["consistent"] for r in reports),
        "distinct_states": statistics.fmean(r["distinct_states"] for r in reports),
        "overbooked_seats": statistics.fmean(r["overbooked_seats"] for r in reports),
        "divergent_outcomes": statistics.fmean(len(r["outcome_conflicts"]) for r in reports),
        "latency_mean_ms": statistics.fmean(r["latency_mean_ms"] for r in reports),
        "latency_p95_ms": statistics.fmean(r["latency_p95_ms"] for r in reports),
        "packets_per_command": statistics.fmean(r["packets_per_command"] for r in reports),
    }


def to_markdown(rows: list[dict]) -> str:
    lines = [
        "| Network | Ordering | Complete | Consistent | Distinct states | Overbooked seats "
        "| Divergent outcomes | Latency mean (ms) | Latency p95 (ms) | Packets per command |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {row['scenario']} | {row['ordering']} | {row['complete']}/{row['sessions']} "
        f"| {row['consistent']}/{row['sessions']} | {row['distinct_states']:.1f} "
        f"| {row['overbooked_seats']:.1f} | {row['divergent_outcomes']:.1f} "
        f"| {row['latency_mean_ms']:.1f} | {row['latency_p95_ms']:.1f} "
        f"| {row['packets_per_command']:.1f} |"
        for row in rows
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delays", type=float, nargs="+", default=[0, 5, 20, 60])
    parser.add_argument("--lossy-delay", type=float, default=20)
    parser.add_argument("--loss", type=float, default=0.1)
    parser.add_argument("--offices", type=int, default=5)
    parser.add_argument("--sessions", type=int, default=10)
    parser.add_argument("--commands", type=int, default=40)
    parser.add_argument("--seed", type=int, default=2024)
    parser.add_argument("--base-port", type=int, default=7700)
    parser.add_argument(
        "--orderings", type=Ordering, nargs="+", default=list(Ordering), choices=list(Ordering)
    )
    parser.add_argument("--json", type=Path, help="also write the raw rows as JSON")
    args = parser.parse_args()
    scenarios = [Scenario(delay, 0.0) for delay in args.delays]
    scenarios.append(Scenario(args.lossy_delay, args.loss))
    with tempfile.TemporaryDirectory() as workdir:
        rows = [
            run(scenario, ordering, args, Path(workdir))
            for scenario in scenarios
            for ordering in args.orderings
        ]
    if args.json is not None:
        args.json.write_text(json.dumps(rows, indent=2))
    print(to_markdown(rows))


if __name__ == "__main__":
    main()
