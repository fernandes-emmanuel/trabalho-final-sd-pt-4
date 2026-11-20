"""Measures how the cost of each total ordering mechanism grows with the number of offices."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
from pathlib import Path

from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from mpcomm.ordering.base import Ordering
from mpcomm.transport.faults import FaultProfile

ORDERINGS = (Ordering.TOTAL, Ordering.SEQUENCER)


def expected_packets(ordering: Ordering, offices: int) -> int:
    if ordering is Ordering.TOTAL:
        return 2 * offices * (offices + 1)
    return 4 * offices


def run(offices: int, ordering: Ordering, args: argparse.Namespace, workdir: Path) -> dict:
    report_path = workdir / f"{ordering}-{offices}.json"
    deployment = LocalDeployment(
        offices=offices,
        sessions=args.sessions,
        workload=WorkloadSpec(commands=args.commands, seed=args.seed),
        ordering=ordering,
        base_port=args.base_port,
        faults=FaultProfile(delay_ms=args.delay_ms, jitter_ms=args.delay_ms, seed=args.seed),
        report=report_path,
        log_level="warning",
    )
    run_local(deployment)
    reports = json.loads(report_path.read_text())
    return {
        "offices": offices,
        "ordering": ordering.value,
        "sessions": len(reports),
        "consistent": sum(r["consistent"] for r in reports),
        "expected_packets": expected_packets(ordering, offices),
        "packets_per_command": statistics.fmean(r["packets_per_command"] for r in reports),
        "latency_mean_ms": statistics.fmean(r["latency_mean_ms"] for r in reports),
        "latency_p95_ms": statistics.fmean(r["latency_p95_ms"] for r in reports),
    }


def to_markdown(rows: list[dict]) -> str:
    lines = [
        "| Offices | Ordering | Consistent | Expected packets | Measured packets "
        "| Latency mean (ms) | Latency p95 (ms) |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {row['offices']} | {row['ordering']} | {row['consistent']}/{row['sessions']} "
        f"| {row['expected_packets']} | {row['packets_per_command']:.1f} "
        f"| {row['latency_mean_ms']:.1f} | {row['latency_p95_ms']:.1f} |"
        for row in rows
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offices", type=int, nargs="+", default=[3, 5, 8])
    parser.add_argument("--sessions", type=int, default=5)
    parser.add_argument("--commands", type=int, default=30)
    parser.add_argument("--delay-ms", type=float, default=5)
    parser.add_argument("--seed", type=int, default=2024)
    parser.add_argument("--base-port", type=int, default=7800)
    parser.add_argument("--json", type=Path, help="also write the raw rows as JSON")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as workdir:
        rows = [
            run(offices, ordering, args, Path(workdir))
            for offices in args.offices
            for ordering in ORDERINGS
        ]
    if args.json is not None:
        args.json.write_text(json.dumps(rows, indent=2))
    print(to_markdown(rows))


if __name__ == "__main__":
    main()
