from __future__ import annotations

import itertools
import json
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path

import grpc

from boxoffice.auditor.analysis import AuditReport, audit
from boxoffice.auditor.report import render, to_dict
from boxoffice.domain.seats import Venue
from boxoffice.domain.snapshot import ReplicaSnapshot
from boxoffice.domain.workload import WorkloadSpec
from boxoffice.rpc import boxoffice_pb2 as pb
from boxoffice.rpc import boxoffice_pb2_grpc as rpc
from boxoffice.rpc.clients import OfficeControlClient
from boxoffice.rpc.convert import snapshot_from_proto
from mpcomm.model import Endpoint, Peer
from mpcomm.ordering.base import Ordering
from mpcomm.rpc.clients import GroupManagerClient
from mpcomm.rpc.server import start_server, stop_server

log = logging.getLogger(__name__)

OFFICE_POLL_INTERVAL = 0.5
WORKLOAD_MARGIN = 60.0


class AuditorError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class AuditorSettings:
    listen: Endpoint
    advertise_host: str
    group_manager: Endpoint
    expected_offices: int
    venue: Venue
    ordering: Ordering = Ordering.SEQUENCER
    sequencer: str = ""
    startup_timeout: float = 60.0
    session_timeout: float = 180.0


class SnapshotCollector(rpc.AuditorServicer):
    def __init__(self) -> None:
        self._snapshots: dict[int, dict[int, ReplicaSnapshot]] = defaultdict(dict)
        self._arrived = threading.Condition()

    def SubmitSnapshot(self, request: pb.ReplicaSnapshot, context: grpc.ServicerContext):
        snapshot = snapshot_from_proto(request)
        log.info(
            "received snapshot from %s for session %s", snapshot.office_name, snapshot.session_id
        )
        with self._arrived:
            self._snapshots[snapshot.session_id][snapshot.office_id] = snapshot
            self._arrived.notify_all()
        return pb.Empty()

    def collect(
        self, session_id: int, offices: set[int], timeout: float
    ) -> dict[int, ReplicaSnapshot]:
        with self._arrived:
            self._arrived.wait_for(lambda: offices <= self._snapshots[session_id].keys(), timeout)
            return self._snapshots.pop(session_id, {})


class SalesAuditor:
    def __init__(self, settings: AuditorSettings) -> None:
        self._settings = settings
        self._collector = SnapshotCollector()
        self._group_manager = GroupManagerClient(settings.group_manager)
        self._session_ids = itertools.count(int(time.time()))
        self._server: grpc.Server | None = None
        self._report_to = Endpoint(settings.advertise_host, settings.listen.port)

    def __enter__(self) -> SalesAuditor:
        self._server, port = start_server(
            self._settings.listen,
            lambda s: rpc.add_AuditorServicer_to_server(self._collector, s),
        )
        self._report_to = Endpoint(self._settings.advertise_host, port)
        log.info("auditor collecting snapshots at %s", self._report_to)
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._server is not None:
            stop_server(self._server)
        self._group_manager.close()

    def wait_for_offices(self) -> list[Peer]:
        deadline = time.monotonic() + self._settings.startup_timeout
        offices = self._list_offices()
        while len(offices) < self._settings.expected_offices:
            if time.monotonic() > deadline:
                raise AuditorError(
                    f"only {len(offices)} of {self._settings.expected_offices} offices registered"
                )
            time.sleep(OFFICE_POLL_INTERVAL)
            offices = self._list_offices()
        return offices

    def open_session(self) -> tuple[int, list[Peer]]:
        offices = self.wait_for_offices()
        session_id = next(self._session_ids)
        log.info(
            "opening sales session %s (%s order) at %s",
            session_id,
            self._settings.ordering.value,
            [o.name for o in offices],
        )
        for office in offices:
            try:
                with OfficeControlClient(office.control_endpoint) as control:
                    control.open_session(
                        session_id,
                        self._settings.venue,
                        self._report_to,
                        self._settings.ordering,
                        self._settings.sequencer,
                    )
            except grpc.RpcError as error:
                raise AuditorError(
                    f"office {office.name} refused to open: {error.details() or error.code()}"
                ) from error
        return session_id, offices

    def run_workload(self, session_id: int, offices: list[Peer], spec: WorkloadSpec) -> None:
        timeout = spec.commands * spec.max_interval_ms / 1000 + WORKLOAD_MARGIN

        def drive(office: Peer) -> tuple[int, int]:
            with OfficeControlClient(office.control_endpoint) as control:
                return control.run_workload(session_id, spec, timeout)

        with ThreadPoolExecutor(max_workers=len(offices)) as pool:
            for office, (issued, accepted) in zip(offices, pool.map(drive, offices), strict=True):
                log.info(
                    "%s issued %s commands (%s accepted locally)", office.name, issued, accepted
                )

    def close_and_audit(self, session_id: int, offices: list[Peer]) -> AuditReport:
        for office in offices:
            try:
                with OfficeControlClient(office.control_endpoint) as control:
                    control.close_session(session_id)
            except grpc.RpcError as error:
                log.error("could not close session at %s: %s", office.name, error.code())
        group = [office.peer_id for office in offices]
        snapshots = self._collector.collect(session_id, set(group), self._settings.session_timeout)
        return audit(session_id, group, snapshots)

    def run_session(self, spec: WorkloadSpec) -> AuditReport:
        session_id, offices = self.open_session()
        try:
            self.run_workload(session_id, offices, spec)
        except grpc.RpcError as error:
            log.error("workload failed: %s", error.details() or error.code())
        return self.close_and_audit(session_id, offices)

    def shutdown_group(self) -> None:
        for office in self._list_offices():
            try:
                with OfficeControlClient(office.control_endpoint) as control:
                    control.shutdown()
            except grpc.RpcError as error:
                log.warning("could not stop office %s: %s", office.name, error.code())
        try:
            self._group_manager.shutdown()
        except grpc.RpcError as error:
            log.warning("could not stop group manager: %s", error.code())

    def _list_offices(self) -> list[Peer]:
        try:
            return self._group_manager.list_peers()
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.UNAVAILABLE:
                raise
            return []


def run_batch(
    settings: AuditorSettings,
    spec: WorkloadSpec,
    sessions: int,
    report_path: Path | None,
    keep_group: bool,
) -> int:
    reports: list[AuditReport] = []
    with SalesAuditor(settings) as auditor:
        try:
            for index in range(sessions):
                report = auditor.run_session(replace(spec, seed=spec.seed + index))
                print(render(report), flush=True)
                reports.append(report)
        finally:
            if not keep_group:
                auditor.shutdown_group()
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps([to_dict(r) for r in reports], indent=2, default=str))
    return 0 if all(report.complete for report in reports) else 1


def run_interactive(settings: AuditorSettings) -> int:
    with SalesAuditor(settings) as auditor:
        while True:
            session_id, offices = auditor.open_session()
            print(f"\nSales session {session_id} is open ({_describe_ordering(settings)}) at:")
            for office in offices:
                print(f"  {office.name:<12} {office.host}:{office.control_port}")
            _ask("Press Enter to close the sales and audit the replicas...")
            print(render(auditor.close_and_audit(session_id, offices)), flush=True)
            if _ask("\nOpen a new sales session? [Y/n] ").strip().lower() in ("n", "no"):
                break
        auditor.shutdown_group()
    return 0


def _describe_ordering(settings: AuditorSettings) -> str:
    if settings.ordering is not Ordering.SEQUENCER:
        return f"{settings.ordering} order"
    return f"sequencer order, sequencer: {settings.sequencer or 'lowest office id'}"


def _ask(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:
        return "n"
