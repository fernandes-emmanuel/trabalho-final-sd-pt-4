from __future__ import annotations

import argparse
import logging
import os
import socket
import sys
from collections.abc import Sequence
from pathlib import Path

import grpc

from boxoffice import client
from boxoffice.auditor.server import AuditorError, AuditorSettings, run_batch, run_interactive
from boxoffice.domain.commands import InvalidCommand
from boxoffice.domain.seats import Venue
from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from boxoffice.office.node import OfficeNode, OfficeSettings
from mpcomm.membership.group_manager import run_group_manager
from mpcomm.model import Endpoint
from mpcomm.ordering.base import Ordering
from mpcomm.transport.faults import FaultProfile

DEFAULT_GROUP_MANAGER = "localhost:5680"
DEFAULT_AUDITOR_PORT = 5678
DEFAULT_OFFICE_UDP_PORT = 6789
DEFAULT_OFFICE_CONTROL_PORT = 5679


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    )
    try:
        return args.handler(args)
    except grpc.RpcError as error:
        print(client.describe_rpc_error(error), file=sys.stderr)
        return 2
    except (AuditorError, InvalidCommand, OSError, ValueError) as error:
        logging.getLogger("boxoffice").error("%s", error)
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="boxoffice",
        description="Replicated ticket offices built on reliable FIFO group communication",
    )
    parser.add_argument("--log-level", default=_env("LOG_LEVEL", "info"))
    commands = parser.add_subparsers(dest="command", required=True)
    _add_group_manager_command(commands)
    _add_office_command(commands)
    _add_client_command(commands)
    _add_auditor_command(commands)
    _add_local_command(commands)
    return parser


def _add_group_manager_command(commands) -> None:
    parser = commands.add_parser("group-manager", help="run the membership service")
    parser.add_argument(
        "--listen", type=Endpoint.parse, default=_env("GROUP_MANAGER_LISTEN", "0.0.0.0:5680")
    )
    parser.set_defaults(handler=_run_group_manager)


def _add_office_command(commands) -> None:
    parser = commands.add_parser("office", help="run a ticket office replica")
    parser.add_argument("--name", default=_env("OFFICE_NAME", socket.gethostname()))
    _add_group_manager_option(parser)
    _add_host_options(parser)
    parser.add_argument(
        "--udp-port", type=int, default=int(_env("UDP_PORT", str(DEFAULT_OFFICE_UDP_PORT)))
    )
    parser.add_argument(
        "--control-port",
        type=int,
        default=int(_env("CONTROL_PORT", str(DEFAULT_OFFICE_CONTROL_PORT))),
    )
    _add_fault_options(parser)
    parser.set_defaults(handler=_run_office)


def _add_client_command(commands) -> None:
    parser = commands.add_parser("client", help="buy, cancel and inspect tickets")
    parser.add_argument(
        "--office",
        type=Endpoint.parse,
        default=_env("OFFICE", f"localhost:{DEFAULT_OFFICE_CONTROL_PORT}"),
        help="host:port of the ticket office to talk to",
    )
    actions = parser.add_subparsers(dest="action", required=True)

    reserve = actions.add_parser("reserve", help="reserve one seat")
    reserve.add_argument("seat")
    reserve.add_argument("--customer", required=True)
    reserve.set_defaults(handler=lambda a: client.reserve(a.office, a.seat, a.customer))

    group = actions.add_parser("group", help="reserve several seats, all or nothing")
    group.add_argument("seats", nargs="+")
    group.add_argument("--customer", required=True)
    group.set_defaults(handler=lambda a: client.reserve_group(a.office, a.seats, a.customer))

    cancel = actions.add_parser("cancel", help="cancel a reservation")
    cancel.add_argument("seat")
    cancel.add_argument("--customer", required=True)
    cancel.set_defaults(handler=lambda a: client.cancel(a.office, a.seat, a.customer))

    seat_map = actions.add_parser("map", help="show the seat map as seen by the office")
    seat_map.set_defaults(handler=lambda a: client.show_map(a.office))

    race = actions.add_parser("race", help="request the same seat at several offices at once")
    race.add_argument("seat")
    race.add_argument("--offices", type=Endpoint.parse, nargs="+", required=True)
    race.add_argument("--customers", nargs="+", required=True)
    race.set_defaults(handler=lambda a: client.race(a.seat, a.offices, a.customers))


def _add_auditor_command(commands) -> None:
    parser = commands.add_parser("auditor", help="open sales sessions and audit the replicas")
    _add_group_manager_option(parser)
    _add_host_options(parser)
    parser.add_argument(
        "--port", type=int, default=int(_env("AUDITOR_PORT", str(DEFAULT_AUDITOR_PORT)))
    )
    parser.add_argument("--offices", type=int, default=int(_env("OFFICES", "4")))
    _add_ordering_option(parser)
    _add_venue_options(parser)
    parser.add_argument(
        "--sessions", type=int, help="run this many automatic sessions instead of interactive mode"
    )
    _add_workload_options(parser)
    parser.add_argument("--report", type=Path, help="write the audit reports as JSON")
    parser.add_argument("--session-timeout", type=float, default=180.0)
    parser.add_argument("--startup-timeout", type=float, default=60.0)
    parser.add_argument(
        "--keep-group", action="store_true", help="do not stop offices and group manager at the end"
    )
    parser.set_defaults(handler=_run_auditor)


def _add_local_command(commands) -> None:
    parser = commands.add_parser("local", help="run a complete deployment on localhost")
    parser.add_argument("--offices", type=int, default=4)
    parser.add_argument("--sessions", type=int, default=1)
    parser.add_argument("--base-port", type=int, default=7000)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--timeout", type=float, default=600.0)
    _add_ordering_option(parser)
    _add_venue_options(parser)
    _add_workload_options(parser)
    _add_fault_options(parser)
    parser.set_defaults(handler=_run_local)


def _run_group_manager(args: argparse.Namespace) -> int:
    run_group_manager(args.listen)
    return 0


def _run_office(args: argparse.Namespace) -> int:
    settings = OfficeSettings(
        name=args.name,
        group_manager=args.group_manager,
        bind_host=args.bind,
        advertise_host=args.advertise_host,
        udp_port=args.udp_port,
        control_port=args.control_port,
        faults=_fault_profile(args),
    )
    OfficeNode(settings).run()
    return 0


def _run_auditor(args: argparse.Namespace) -> int:
    settings = AuditorSettings(
        listen=Endpoint(args.bind, args.port),
        advertise_host=args.advertise_host,
        group_manager=args.group_manager,
        expected_offices=args.offices,
        venue=Venue(args.rows, args.seats_per_row),
        ordering=args.ordering,
        sequencer=args.sequencer,
        startup_timeout=args.startup_timeout,
        session_timeout=args.session_timeout,
    )
    if args.sessions is None:
        return run_interactive(settings)
    if args.sessions <= 0:
        raise ValueError("--sessions must be positive")
    return run_batch(settings, _workload(args), args.sessions, args.report, args.keep_group)


def _run_local(args: argparse.Namespace) -> int:
    deployment = LocalDeployment(
        offices=args.offices,
        sessions=args.sessions,
        workload=_workload(args),
        venue=Venue(args.rows, args.seats_per_row),
        ordering=args.ordering,
        sequencer=args.sequencer,
        base_port=args.base_port,
        faults=_fault_profile(args),
        report=args.report,
        timeout=args.timeout,
        log_level=args.log_level,
    )
    return run_local(deployment)


def _add_group_manager_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--group-manager",
        type=Endpoint.parse,
        default=_env("GROUP_MANAGER", DEFAULT_GROUP_MANAGER),
        help="host:port of the group manager",
    )


def _add_host_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bind", default=_env("BIND", "0.0.0.0"), help="local interface to bind")
    parser.add_argument(
        "--advertise-host",
        default=_env("ADVERTISE_HOST", _detect_host()),
        help="address other processes use to reach this one",
    )


def _add_ordering_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ordering",
        type=Ordering,
        choices=list(Ordering),
        default=Ordering(_env("ORDERING", Ordering.SEQUENCER.value)),
        help="message ordering used by the replicas (default: sequencer)",
    )
    parser.add_argument(
        "--sequencer",
        default=_env("SEQUENCER", ""),
        help="name of the office that orders the commands (default: lowest office id)",
    )


def _add_venue_options(parser: argparse.ArgumentParser) -> None:
    venue = parser.add_argument_group("venue")
    venue.add_argument("--rows", type=int, default=5)
    venue.add_argument("--seats-per-row", type=int, default=10)


def _add_workload_options(parser: argparse.ArgumentParser) -> None:
    workload = parser.add_argument_group("automatic workload")
    workload.add_argument("--commands", type=int, default=40, help="commands per office")
    workload.add_argument("--hot-seats", type=int, default=4)
    workload.add_argument("--hot-probability", type=float, default=0.6)
    workload.add_argument("--min-interval-ms", type=int, default=5)
    workload.add_argument("--max-interval-ms", type=int, default=40)
    workload.add_argument("--workload-seed", type=int, default=0)


def _add_fault_options(parser: argparse.ArgumentParser) -> None:
    faults = parser.add_argument_group("fault injection")
    faults.add_argument("--loss", type=float, default=float(_env("LOSS", "0")))
    faults.add_argument("--duplication", type=float, default=float(_env("DUPLICATION", "0")))
    faults.add_argument("--delay-ms", type=float, default=float(_env("DELAY_MS", "0")))
    faults.add_argument("--jitter-ms", type=float, default=float(_env("JITTER_MS", "0")))
    faults.add_argument("--seed", type=int, default=None)


def _workload(args: argparse.Namespace) -> WorkloadSpec:
    return WorkloadSpec(
        commands=args.commands,
        seed=args.workload_seed,
        hot_seats=args.hot_seats,
        hot_probability=args.hot_probability,
        min_interval_ms=args.min_interval_ms,
        max_interval_ms=args.max_interval_ms,
    )


def _fault_profile(args: argparse.Namespace) -> FaultProfile:
    return FaultProfile(
        loss=args.loss,
        duplication=args.duplication,
        delay_ms=args.delay_ms,
        jitter_ms=args.jitter_ms,
        seed=args.seed,
    )


def _env(name: str, default: str) -> str:
    return os.environ.get(f"BOXOFFICE_{name}", default)


def _detect_host() -> str:
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


if __name__ == "__main__":
    sys.exit(main())
