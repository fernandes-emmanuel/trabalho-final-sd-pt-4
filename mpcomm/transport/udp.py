from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable

from mpcomm.model import Address

log = logging.getLogger(__name__)

MAX_DATAGRAM_SIZE = 65_507
RECEIVE_BUFFER_SIZE = 4 * 1024 * 1024
POLL_INTERVAL = 0.2

DatagramHandler = Callable[[bytes, Address], None]


class UdpEndpoint:
    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _ignore_windows_connection_reset(self._socket)
        _enlarge_receive_buffer(self._socket)
        self._socket.bind((host, port))
        self._socket.settimeout(POLL_INTERVAL)
        self._stopped = threading.Event()
        self._receiver: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._socket.getsockname()[1]

    def send(self, data: bytes, address: Address) -> None:
        try:
            self._socket.sendto(data, address)
        except OSError as error:
            log.debug("send to %s failed: %s", address, error)

    def serve(self, handler: DatagramHandler) -> None:
        self._receiver = threading.Thread(
            target=self._receive_loop, args=(handler,), name="udp-receiver", daemon=True
        )
        self._receiver.start()

    def close(self) -> None:
        self._stopped.set()
        if self._receiver is not None:
            self._receiver.join()
        self._socket.close()

    def _receive_loop(self, handler: DatagramHandler) -> None:
        while not self._stopped.is_set():
            try:
                data, address = self._socket.recvfrom(MAX_DATAGRAM_SIZE)
            except (TimeoutError, ConnectionResetError):
                continue
            except OSError:
                if not self._stopped.is_set():
                    log.exception("udp receiver stopped unexpectedly")
                return
            try:
                handler(data, address)
            except Exception:
                log.exception("failed to handle datagram from %s", address)


def _enlarge_receive_buffer(sock: socket.socket) -> None:
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECEIVE_BUFFER_SIZE)
    except OSError as error:
        log.debug("could not enlarge the receive buffer: %s", error)


def _ignore_windows_connection_reset(sock: socket.socket) -> None:
    if hasattr(socket, "SIO_UDP_CONNRESET"):
        sock.ioctl(socket.SIO_UDP_CONNRESET, False)
