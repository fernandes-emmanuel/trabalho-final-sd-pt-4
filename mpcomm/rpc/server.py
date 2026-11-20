from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import grpc

from mpcomm.model import Endpoint

Registration = Callable[[grpc.Server], None]

STOP_POLL_INTERVAL = 0.5
SHUTDOWN_GRACE = 1.0


def start_server(listen: Endpoint, *registrations: Registration) -> tuple[grpc.Server, int]:
    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    for register in registrations:
        register(server)
    try:
        port = server.add_insecure_port(str(listen))
    except RuntimeError as error:
        raise OSError(f"cannot listen on {listen}: {error}") from error
    if port == 0:
        raise OSError(f"cannot listen on {listen}")
    server.start()
    return server, port


def wait_until_stopped(stop: threading.Event) -> None:
    try:
        while not stop.wait(STOP_POLL_INTERVAL):
            pass
    except KeyboardInterrupt:
        stop.set()


def stop_server(server: grpc.Server) -> None:
    server.stop(SHUTDOWN_GRACE).wait()
