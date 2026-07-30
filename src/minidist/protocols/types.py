"""Shared vocabulary for comparing replication protocols.

Real systems attach different guarantees to similarly named knobs.  These
enums provide a common experiment vocabulary without pretending every
protocol supports every level.  Implementations must reject unsupported
levels rather than silently weakening the requested guarantee.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol, TypeAlias

NodeId: TypeAlias = str


class AckLevel(Enum):
    """How much replication work a client asks to wait for."""

    NONE = auto()
    LEADER = auto()
    QUORUM = auto()
    ALL_ISR = auto()


class ReadLevel(Enum):
    """Where a read may run and what consistency it asks for."""

    LOCAL = auto()
    LEADER = auto()
    LINEARIZABLE = auto()


class UnsupportedLevelError(ValueError):
    """Raised instead of silently pretending a protocol is stronger."""


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Public result of a write accepted by a replication group."""

    accepted: bool
    offset: int
    message: str = ""


@dataclass(frozen=True, slots=True)
class ReadResult:
    """A value together with the node and replication point that served it."""

    value: bytes | None
    node: NodeId
    offset: int


class ReplicationGroup(Protocol):
    """Minimal common driver surface used by cross-protocol experiments."""

    def client_write(self, key: bytes, value: bytes, ack: AckLevel) -> WriteResult: ...

    def client_read(
        self,
        key: bytes,
        level: ReadLevel,
        node: NodeId | None = None,
    ) -> ReadResult: ...

    def tick(self) -> None: ...

    def crash(self, node: NodeId) -> None: ...

    def restart(self, node: NodeId) -> None: ...

    def probe(self) -> object: ...
