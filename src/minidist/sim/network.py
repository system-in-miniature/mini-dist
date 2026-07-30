"""Seeded in-process network fault simulation.

``SimNet`` corresponds to the transport fault layer used in deterministic
simulation testing: it can delay, drop, reorder, or partition messages while
keeping every choice reproducible from an explicit seed.  It transports
Python values because serialization is not the lesson in MiniDist.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .clock import SimClock
from .scheduler import Scheduler
from .trace import Trace


@dataclass(frozen=True, slots=True)
class Message:
    """An immutable envelope visible to node handlers."""

    message_id: int
    source: str
    destination: str
    payload: Any


class SimNet:
    """Deterministic message transport with scriptable fault boundaries."""

    def __init__(
        self,
        *,
        seed: int,
        clock: SimClock,
        scheduler: Scheduler,
        trace: Trace,
        min_delay: int = 1,
        max_delay: int | None = None,
        drop_rate: float = 0.0,
        reorder_rate: float = 0.0,
    ) -> None:
        max_delay = min_delay if max_delay is None else max_delay
        if min_delay < 0 or max_delay < min_delay:
            raise ValueError("network delay range is invalid")
        if not 0.0 <= drop_rate <= 1.0:
            raise ValueError("drop_rate must be between 0 and 1")
        if not 0.0 <= reorder_rate <= 1.0:
            raise ValueError("reorder_rate must be between 0 and 1")
        self.clock = clock
        self.scheduler = scheduler
        self.trace = trace
        self._rng = random.Random(seed)
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._drop_rate = drop_rate
        self._reorder_rate = reorder_rate
        self._handlers: dict[str, Callable[[Message], None]] = {}
        self._partitions: set[tuple[str, str]] = set()
        self._next_message_id = 0
        self.trace.record(clock.now, "network_created", seed=seed)

    def register(self, node: str, handler: Callable[[Message], None]) -> None:
        self._handlers[node] = handler

    def partition(
        self, source: str, destination: str, *, bidirectional: bool = False
    ) -> None:
        self._partitions.add((source, destination))
        if bidirectional:
            self._partitions.add((destination, source))
        self.trace.record(
            self.clock.now,
            "partition_added",
            source=source,
            destination=destination,
            bidirectional=bidirectional,
        )

    def heal(
        self, source: str, destination: str, *, bidirectional: bool = False
    ) -> None:
        self._partitions.discard((source, destination))
        if bidirectional:
            self._partitions.discard((destination, source))
        self.trace.record(
            self.clock.now,
            "partition_healed",
            source=source,
            destination=destination,
            bidirectional=bidirectional,
        )

    def send(self, source: str, destination: str, payload: Any) -> Message:
        message = Message(
            message_id=self._next_message_id,
            source=source,
            destination=destination,
            payload=payload,
        )
        self._next_message_id += 1
        self.trace.record(
            self.clock.now,
            "message_sent",
            message_id=message.message_id,
            source=source,
            destination=destination,
            payload=payload,
        )

        if (source, destination) in self._partitions:
            self._record_drop(message, "partition")
            return message
        if self._rng.random() < self._drop_rate:
            self._record_drop(message, "seeded_drop")
            return message

        delay = self._rng.randint(self._min_delay, self._max_delay)
        if self._rng.random() < self._reorder_rate:
            # Extra seeded jitter deliberately lets a later send overtake this
            # one; no host timing participates in that ordering decision.
            delay += self._rng.randint(0, max(1, self._max_delay))
        self.scheduler.schedule(
            delay,
            lambda: self._deliver(message),
            label=f"deliver:{message.message_id}",
        )
        return message

    def _record_drop(self, message: Message, reason: str) -> None:
        self.trace.record(
            self.clock.now,
            "message_dropped",
            message_id=message.message_id,
            source=message.source,
            destination=message.destination,
            reason=reason,
        )

    def _deliver(self, message: Message) -> None:
        if (message.source, message.destination) in self._partitions:
            self._record_drop(message, "partition_at_delivery")
            return
        handler = self._handlers.get(message.destination)
        if handler is None:
            self._record_drop(message, "unknown_destination")
            return
        self.trace.record(
            self.clock.now,
            "message_delivered",
            message_id=message.message_id,
            source=message.source,
            destination=message.destination,
            payload=message.payload,
        )
        handler(message)
