from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTCOME_UNSPECIFIED: _ClassVar[Outcome]
    ACCEPTED: _ClassVar[Outcome]
    SEAT_TAKEN: _ClassVar[Outcome]
    NOT_OWNER: _ClassVar[Outcome]
    UNKNOWN_SEAT: _ClassVar[Outcome]
OUTCOME_UNSPECIFIED: Outcome
ACCEPTED: Outcome
SEAT_TAKEN: Outcome
NOT_OWNER: Outcome
UNKNOWN_SEAT: Outcome

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Venue(_message.Message):
    __slots__ = ("rows", "seats_per_row")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    SEATS_PER_ROW_FIELD_NUMBER: _ClassVar[int]
    rows: int
    seats_per_row: int
    def __init__(self, rows: _Optional[int] = ..., seats_per_row: _Optional[int] = ...) -> None: ...

class ReserveRequest(_message.Message):
    __slots__ = ("seat", "customer")
    SEAT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    seat: str
    customer: str
    def __init__(self, seat: _Optional[str] = ..., customer: _Optional[str] = ...) -> None: ...

class ReserveGroupRequest(_message.Message):
    __slots__ = ("seats", "customer")
    SEATS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    seats: _containers.RepeatedScalarFieldContainer[str]
    customer: str
    def __init__(self, seats: _Optional[_Iterable[str]] = ..., customer: _Optional[str] = ...) -> None: ...

class CancelRequest(_message.Message):
    __slots__ = ("seat", "customer")
    SEAT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    seat: str
    customer: str
    def __init__(self, seat: _Optional[str] = ..., customer: _Optional[str] = ...) -> None: ...

class CommandReply(_message.Message):
    __slots__ = ("command_id", "outcome", "office_id", "office_name")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    OFFICE_ID_FIELD_NUMBER: _ClassVar[int]
    OFFICE_NAME_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    outcome: Outcome
    office_id: int
    office_name: str
    def __init__(self, command_id: _Optional[str] = ..., outcome: _Optional[_Union[Outcome, str]] = ..., office_id: _Optional[int] = ..., office_name: _Optional[str] = ...) -> None: ...

class SeatOwner(_message.Message):
    __slots__ = ("seat", "customer")
    SEAT_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    seat: str
    customer: str
    def __init__(self, seat: _Optional[str] = ..., customer: _Optional[str] = ...) -> None: ...

class SeatMapReply(_message.Message):
    __slots__ = ("session_id", "venue", "owners", "office_id", "office_name", "applied", "digest")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    VENUE_FIELD_NUMBER: _ClassVar[int]
    OWNERS_FIELD_NUMBER: _ClassVar[int]
    OFFICE_ID_FIELD_NUMBER: _ClassVar[int]
    OFFICE_NAME_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    venue: Venue
    owners: _containers.RepeatedCompositeFieldContainer[SeatOwner]
    office_id: int
    office_name: str
    applied: int
    digest: str
    def __init__(self, session_id: _Optional[int] = ..., venue: _Optional[_Union[Venue, _Mapping]] = ..., owners: _Optional[_Iterable[_Union[SeatOwner, _Mapping]]] = ..., office_id: _Optional[int] = ..., office_name: _Optional[str] = ..., applied: _Optional[int] = ..., digest: _Optional[str] = ...) -> None: ...

class OpenSessionRequest(_message.Message):
    __slots__ = ("session_id", "venue", "report_to", "ordering", "sequencer")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    VENUE_FIELD_NUMBER: _ClassVar[int]
    REPORT_TO_FIELD_NUMBER: _ClassVar[int]
    ORDERING_FIELD_NUMBER: _ClassVar[int]
    SEQUENCER_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    venue: Venue
    report_to: str
    ordering: str
    sequencer: str
    def __init__(self, session_id: _Optional[int] = ..., venue: _Optional[_Union[Venue, _Mapping]] = ..., report_to: _Optional[str] = ..., ordering: _Optional[str] = ..., sequencer: _Optional[str] = ...) -> None: ...

class OpenSessionReply(_message.Message):
    __slots__ = ("office_id", "group_size")
    OFFICE_ID_FIELD_NUMBER: _ClassVar[int]
    GROUP_SIZE_FIELD_NUMBER: _ClassVar[int]
    office_id: int
    group_size: int
    def __init__(self, office_id: _Optional[int] = ..., group_size: _Optional[int] = ...) -> None: ...

class WorkloadRequest(_message.Message):
    __slots__ = ("session_id", "commands", "seed", "hot_seats", "hot_probability", "min_interval_ms", "max_interval_ms")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    HOT_SEATS_FIELD_NUMBER: _ClassVar[int]
    HOT_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    MIN_INTERVAL_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_INTERVAL_MS_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    commands: int
    seed: int
    hot_seats: int
    hot_probability: float
    min_interval_ms: int
    max_interval_ms: int
    def __init__(self, session_id: _Optional[int] = ..., commands: _Optional[int] = ..., seed: _Optional[int] = ..., hot_seats: _Optional[int] = ..., hot_probability: _Optional[float] = ..., min_interval_ms: _Optional[int] = ..., max_interval_ms: _Optional[int] = ...) -> None: ...

class WorkloadReply(_message.Message):
    __slots__ = ("issued", "accepted")
    ISSUED_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    issued: int
    accepted: int
    def __init__(self, issued: _Optional[int] = ..., accepted: _Optional[int] = ...) -> None: ...

class SessionRef(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    def __init__(self, session_id: _Optional[int] = ...) -> None: ...

class AppliedCommand(_message.Message):
    __slots__ = ("command_id", "operation", "seats", "customer", "outcome")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    SEATS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    operation: str
    seats: _containers.RepeatedScalarFieldContainer[str]
    customer: str
    outcome: Outcome
    def __init__(self, command_id: _Optional[str] = ..., operation: _Optional[str] = ..., seats: _Optional[_Iterable[str]] = ..., customer: _Optional[str] = ..., outcome: _Optional[_Union[Outcome, str]] = ...) -> None: ...

class TransportStats(_message.Message):
    __slots__ = ("data_sent", "retransmissions", "acks_sent", "duplicates_discarded", "held_back", "dropped_by_fault_injector", "unreachable_peers", "order_messages_sent", "order_control_sent", "order_queue_peak")
    DATA_SENT_FIELD_NUMBER: _ClassVar[int]
    RETRANSMISSIONS_FIELD_NUMBER: _ClassVar[int]
    ACKS_SENT_FIELD_NUMBER: _ClassVar[int]
    DUPLICATES_DISCARDED_FIELD_NUMBER: _ClassVar[int]
    HELD_BACK_FIELD_NUMBER: _ClassVar[int]
    DROPPED_BY_FAULT_INJECTOR_FIELD_NUMBER: _ClassVar[int]
    UNREACHABLE_PEERS_FIELD_NUMBER: _ClassVar[int]
    ORDER_MESSAGES_SENT_FIELD_NUMBER: _ClassVar[int]
    ORDER_CONTROL_SENT_FIELD_NUMBER: _ClassVar[int]
    ORDER_QUEUE_PEAK_FIELD_NUMBER: _ClassVar[int]
    data_sent: int
    retransmissions: int
    acks_sent: int
    duplicates_discarded: int
    held_back: int
    dropped_by_fault_injector: int
    unreachable_peers: _containers.RepeatedScalarFieldContainer[int]
    order_messages_sent: int
    order_control_sent: int
    order_queue_peak: int
    def __init__(self, data_sent: _Optional[int] = ..., retransmissions: _Optional[int] = ..., acks_sent: _Optional[int] = ..., duplicates_discarded: _Optional[int] = ..., held_back: _Optional[int] = ..., dropped_by_fault_injector: _Optional[int] = ..., unreachable_peers: _Optional[_Iterable[int]] = ..., order_messages_sent: _Optional[int] = ..., order_control_sent: _Optional[int] = ..., order_queue_peak: _Optional[int] = ...) -> None: ...

class ReplicaSnapshot(_message.Message):
    __slots__ = ("session_id", "office_id", "office_name", "completed", "digest", "owners", "applied", "stats", "ordering", "latency_mean_ms", "latency_p95_ms", "sequencer")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    OFFICE_ID_FIELD_NUMBER: _ClassVar[int]
    OFFICE_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    OWNERS_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    ORDERING_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MEAN_MS_FIELD_NUMBER: _ClassVar[int]
    LATENCY_P95_MS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCER_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    office_id: int
    office_name: str
    completed: bool
    digest: str
    owners: _containers.RepeatedCompositeFieldContainer[SeatOwner]
    applied: _containers.RepeatedCompositeFieldContainer[AppliedCommand]
    stats: TransportStats
    ordering: str
    latency_mean_ms: float
    latency_p95_ms: float
    sequencer: str
    def __init__(self, session_id: _Optional[int] = ..., office_id: _Optional[int] = ..., office_name: _Optional[str] = ..., completed: _Optional[bool] = ..., digest: _Optional[str] = ..., owners: _Optional[_Iterable[_Union[SeatOwner, _Mapping]]] = ..., applied: _Optional[_Iterable[_Union[AppliedCommand, _Mapping]]] = ..., stats: _Optional[_Union[TransportStats, _Mapping]] = ..., ordering: _Optional[str] = ..., latency_mean_ms: _Optional[float] = ..., latency_p95_ms: _Optional[float] = ..., sequencer: _Optional[str] = ...) -> None: ...
