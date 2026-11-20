from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor

import grpc

from boxoffice.domain.commands import Outcome
from boxoffice.rpc.clients import CommandResult, SeatMapSnapshot, TicketOfficeClient
from mpcomm.model import Endpoint

CELL_WIDTH = 8
FREE_SEAT = "."
RACE_SETTLE_TIME = 1.0
CONNECT_TIMEOUT = 10.0


def reserve(office: Endpoint, seat: str, customer: str) -> int:
    with TicketOfficeClient(office) as client:
        return _report(client.reserve(seat, customer), f"seat {seat.upper()} for {customer}")


def reserve_group(office: Endpoint, seats: Sequence[str], customer: str) -> int:
    with TicketOfficeClient(office) as client:
        label = ", ".join(seat.upper() for seat in seats)
        return _report(client.reserve_group(seats, customer), f"seats {label} for {customer}")


def cancel(office: Endpoint, seat: str, customer: str) -> int:
    with TicketOfficeClient(office) as client:
        return _report(client.cancel(seat, customer), f"cancel {seat.upper()} by {customer}")


def show_map(office: Endpoint) -> int:
    with TicketOfficeClient(office) as client:
        print(render_seat_map(client.seat_map()))
    return 0


def race(seat: str, offices: Sequence[Endpoint], customers: Sequence[str]) -> int:
    if len(offices) != len(customers):
        raise ValueError("give one customer per office")
    start = threading.Barrier(len(offices))

    def attempt(office: Endpoint, customer: str) -> CommandResult:
        with TicketOfficeClient(office) as client:
            client.wait_until_connected(CONNECT_TIMEOUT)
            start.wait()
            return client.reserve(seat, customer)

    with ThreadPoolExecutor(max_workers=len(offices)) as pool:
        results = list(pool.map(attempt, offices, customers))
    print(f"Simultaneous requests for seat {seat.upper()}:")
    for customer, result in zip(customers, results, strict=True):
        print(f"  {customer:<10} at {result.office_name:<14} -> {result.outcome.value}")
    time.sleep(RACE_SETTLE_TIME)
    print(f"\nOwner of {seat.upper()} according to each office:")
    for office in offices:
        with TicketOfficeClient(office) as client:
            snapshot = client.seat_map()
        print(f"  {snapshot.office_name:<14} -> {snapshot.owners.get(seat.upper(), 'free')}")
    return 0


def render_seat_map(snapshot: SeatMapSnapshot) -> str:
    venue = snapshot.venue
    lines = [
        f"Office {snapshot.office_name} (id {snapshot.office_id}) | session {snapshot.session_id}"
        f" | {snapshot.applied} commands applied | digest {snapshot.digest[:12]}",
        "",
        "    " + "".join(str(n).ljust(CELL_WIDTH) for n in range(1, venue.seats_per_row + 1)),
    ]
    for row in venue.row_labels:
        cells = (
            snapshot.owners.get(f"{row}{n}", FREE_SEAT)[: CELL_WIDTH - 1].ljust(CELL_WIDTH)
            for n in range(1, venue.seats_per_row + 1)
        )
        lines.append(f"{row}   " + "".join(cells))
    sold = len(snapshot.owners)
    lines += ["", f"{sold} of {venue.capacity} seats sold"]
    return "\n".join(lines)


def describe_rpc_error(error: grpc.RpcError) -> str:
    return f"{error.code().name}: {error.details()}"


def _report(result: CommandResult, description: str) -> int:
    print(
        f"{result.outcome.value.upper()}: {description} "
        f"(office {result.office_name}, command {result.command_id})"
    )
    return 0 if result.outcome is Outcome.ACCEPTED else 3
