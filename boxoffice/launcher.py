from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from boxoffice.domain.seats import Venue
from boxoffice.domain.workload import WorkloadSpec
from mpcomm.ordering.base import Ordering
from mpcomm.transport.faults import FaultProfile

log = logging.getLogger(__name__)

LOCALHOST = "127.0.0.1"
PROCESS_EXIT_TIMEOUT = 15.0
OFFICE_NAMES = (
    "sao-paulo", "rio", "belo-horizonte", "porto-alegre", "recife",
    "salvador", "curitiba", "lisboa", "toquio", "nova-york",
)  # fmt: skip


def office_name(index: int) -> str:
    if index < len(OFFICE_NAMES):
        return OFFICE_NAMES[index]
    return f"office-{index}"


@dataclass(frozen=True, slots=True)
class LocalDeployment:
    offices: int
    sessions: int
    workload: WorkloadSpec
    venue: Venue = field(default_factory=lambda: Venue(5, 10))
    ordering: Ordering = Ordering.SEQUENCER
    sequencer: str = ""
    base_port: int = 7000
    faults: FaultProfile = field(default_factory=FaultProfile)
    report: Path | None = None
    timeout: float = 600.0
    log_level: str = "info"

    @property
    def group_manager(self) -> str:
        return f"{LOCALHOST}:{self.base_port}"

    @property
    def auditor_port(self) -> int:
        return self.base_port + 1

    def udp_port(self, index: int) -> int:
        return self.base_port + 100 + index

    def control_port(self, index: int) -> int:
        return self.base_port + 200 + index


def run_local(deployment: LocalDeployment) -> int:
    background: list[subprocess.Popen[bytes]] = []
    try:
        background.append(_spawn(deployment, "group-manager", "--listen", deployment.group_manager))
        for index in range(deployment.offices):
            background.append(_spawn(deployment, "office", *_office_arguments(deployment, index)))
        auditor = subprocess.run(
            _command(deployment, "auditor", *_auditor_arguments(deployment)),
            timeout=deployment.timeout,
            check=False,
        )
        for process in background:
            process.wait(PROCESS_EXIT_TIMEOUT)
        return auditor.returncode
    except subprocess.TimeoutExpired:
        log.error("local deployment did not finish in time")
        return 1
    finally:
        for process in background:
            if process.poll() is None:
                process.kill()


def _office_arguments(deployment: LocalDeployment, index: int) -> list[str]:
    faults = deployment.faults
    arguments = [
        "--name", office_name(index),
        "--group-manager", deployment.group_manager,
        "--bind", LOCALHOST,
        "--advertise-host", LOCALHOST,
        "--udp-port", str(deployment.udp_port(index)),
        "--control-port", str(deployment.control_port(index)),
        "--loss", str(faults.loss),
        "--duplication", str(faults.duplication),
        "--delay-ms", str(faults.delay_ms),
        "--jitter-ms", str(faults.jitter_ms),
    ]  # fmt: skip
    if faults.seed is not None:
        arguments += ["--seed", str(faults.seed + index)]
    return arguments


def _auditor_arguments(deployment: LocalDeployment) -> list[str]:
    workload = deployment.workload
    arguments = [
        "--group-manager", deployment.group_manager,
        "--bind", LOCALHOST,
        "--advertise-host", LOCALHOST,
        "--port", str(deployment.auditor_port),
        "--offices", str(deployment.offices),
        "--rows", str(deployment.venue.rows),
        "--seats-per-row", str(deployment.venue.seats_per_row),
        "--sessions", str(deployment.sessions),
        "--ordering", deployment.ordering.value,
        "--commands", str(workload.commands),
        "--hot-seats", str(workload.hot_seats),
        "--hot-probability", str(workload.hot_probability),
        "--min-interval-ms", str(workload.min_interval_ms),
        "--max-interval-ms", str(workload.max_interval_ms),
        "--workload-seed", str(workload.seed),
    ]  # fmt: skip
    if deployment.sequencer:
        arguments += ["--sequencer", deployment.sequencer]
    if deployment.report is not None:
        arguments += ["--report", str(deployment.report)]
    return arguments


def _spawn(deployment: LocalDeployment, *arguments: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(_command(deployment, *arguments, background=True))


def _command(deployment: LocalDeployment, *arguments: str, background: bool = False) -> list[str]:
    level = "warning" if background else deployment.log_level
    return [sys.executable, "-m", "boxoffice", "--log-level", level, *arguments]
