from typing import Protocol

from mpcomm.model import Address


class Channel(Protocol):
    def send(self, data: bytes, address: Address) -> None: ...
