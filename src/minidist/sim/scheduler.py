"""Single-threaded deterministic event scheduling.

This models the event queues underneath discrete-event simulators.  A
monotonic insertion counter breaks ties at the same logical tick; therefore
the result never depends on thread scheduling or heap implementation details.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable
from dataclasses import dataclass, field

from .clock import SimClock
from .trace import Trace


@dataclass(order=True, slots=True)
class _ScheduledEvent:
    due_tick: int
    order: int
    callback: Callable[[], None] = field(compare=False, repr=False)
    label: str = field(compare=False)


class Scheduler:
    """A deterministic priority queue driven one tick at a time."""

    def __init__(self, clock: SimClock, trace: Trace) -> None:
        self.clock = clock
        self.trace = trace
        self._queue: list[_ScheduledEvent] = []
        self._next_order = 0

    @property
    def pending(self) -> int:
        return len(self._queue)

    def schedule(self, delay: int, callback: Callable[[], None], *, label: str) -> None:
        if delay < 0:
            raise ValueError("event delay must not be negative")
        event = _ScheduledEvent(
            due_tick=self.clock.now + delay,
            order=self._next_order,
            callback=callback,
            label=label,
        )
        self._next_order += 1
        heapq.heappush(self._queue, event)
        self.trace.record(
            self.clock.now,
            "event_scheduled",
            label=label,
            due_tick=event.due_tick,
            order=event.order,
        )

    def tick(self) -> int:
        """Advance once, run all now-due events, and return their count."""

        self.clock.advance()
        ran = 0
        while self._queue and self._queue[0].due_tick <= self.clock.now:
            event = heapq.heappop(self._queue)
            self.trace.record(
                self.clock.now,
                "event_started",
                label=event.label,
                due_tick=event.due_tick,
                order=event.order,
            )
            event.callback()
            ran += 1
        return ran

    def run_until_idle(self, *, max_ticks: int = 10_000) -> int:
        """Tick until no work remains, protecting lessons from infinite loops."""

        ticks = 0
        while self._queue:
            if ticks >= max_ticks:
                raise RuntimeError("scheduler did not become idle")
            self.tick()
            ticks += 1
        return ticks
