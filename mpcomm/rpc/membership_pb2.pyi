from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PeerInfo(_message.Message):
    __slots__ = ("peer_id", "name", "host", "udp_port", "control_port")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    UDP_PORT_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    name: str
    host: str
    udp_port: int
    control_port: int
    def __init__(self, peer_id: _Optional[int] = ..., name: _Optional[str] = ..., host: _Optional[str] = ..., udp_port: _Optional[int] = ..., control_port: _Optional[int] = ...) -> None: ...

class RegisterRequest(_message.Message):
    __slots__ = ("name", "host", "udp_port", "control_port")
    NAME_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    UDP_PORT_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    name: str
    host: str
    udp_port: int
    control_port: int
    def __init__(self, name: _Optional[str] = ..., host: _Optional[str] = ..., udp_port: _Optional[int] = ..., control_port: _Optional[int] = ...) -> None: ...

class RegisterReply(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    def __init__(self, peer_id: _Optional[int] = ...) -> None: ...

class UnregisterRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: int
    def __init__(self, peer_id: _Optional[int] = ...) -> None: ...

class PeerList(_message.Message):
    __slots__ = ("peers",)
    PEERS_FIELD_NUMBER: _ClassVar[int]
    peers: _containers.RepeatedCompositeFieldContainer[PeerInfo]
    def __init__(self, peers: _Optional[_Iterable[_Union[PeerInfo, _Mapping]]] = ...) -> None: ...
