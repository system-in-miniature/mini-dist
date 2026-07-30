"""Structured event history for replay assertions and teaching output.

Production tracing systems add timestamps, spans, and exporters.  MiniDist
keeps the essential contract: every meaningful transition gets an ordered,
machine-readable record.  Sequence numbers make equal-tick events unambiguous.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TraceEvent:
    """One stable observation in a simulation run."""

    sequence: int
    tick: int
    kind: str
    details: dict[str, Any]


class Trace:
    """Append-only structured event log."""

    def __init__(self) -> None:
        self._events: list[TraceEvent] = []

    @property
    def events(self) -> tuple[TraceEvent, ...]:
        return tuple(self._events)

    def record(self, tick: int, kind: str, **details: Any) -> TraceEvent:
        event = TraceEvent(
            sequence=len(self._events),
            tick=tick,
            kind=kind,
            details=details,
        )
        self._events.append(event)
        return event

    def as_dicts(self) -> list[dict[str, Any]]:
        """Return value-based snapshots convenient for exact replay checks."""

        return [asdict(event) for event in self._events]
