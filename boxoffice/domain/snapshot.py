from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from boxoffice.domain.commands import AppliedCommand
from mpcomm.model import TransportStats


@dataclass(frozen=True, slots=True)
class ReplicaSnapshot:
    session_id: int
    office_id: int
    office_name: str
    completed: bool
    digest: str
    owners: Mapping[str, str]
    applied: tuple[AppliedCommand, ...]
    stats: TransportStats
    ordering: str = "fifo"
    latency_mean_ms: float = 0.0
    latency_p95_ms: float = 0.0
    sequencer: str = ""
