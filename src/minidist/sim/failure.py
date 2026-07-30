"""Crash/restart ownership boundary for simulated nodes.

Real crash recovery distinguishes memory lost with the process from bytes
already durable on disk.  ``FailureInjector`` makes that distinction visible:
crash clears the node's volatile mapping, leaves its persistent mapping alone,
and marks it unavailable until an explicit restart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .clock import SimClock
from .trace import Trace


@dataclass(slots=True)
class SimNode:
    """Small generic node used to teach volatile versus persistent state."""

    node_id: str
    persistent: dict[str, Any] = field(default_factory=dict)
    volatile: dict[str, Any] = field(default_factory=dict)
    alive: bool = True

    def on_crash(self) -> None:
        self.volatile.clear()

    def on_restart(self) -> None:
        """Hook for richer nodes; durable state already remains available."""


class FailureInjector:
    """Apply explicit lifecycle faults and trace each transition."""

    def __init__(self, *, clock: SimClock, trace: Trace) -> None:
        self.clock = clock
        self.trace = trace
        self._nodes: dict[str, SimNode] = {}
        self._durable: dict[str, dict[str, Any]] = {}

    def register(self, node: SimNode) -> None:
        if node.node_id in self._nodes:
            raise ValueError(f"node already registered: {node.node_id}")
        self._nodes[node.node_id] = node
        self._durable[node.node_id] = dict(node.persistent)

    def stage_persistent(self, node_id: str, key: str, value: Any) -> None:
        """Expose a write that the process has accepted but not fsynced."""

        node = self._node(node_id)
        if not node.alive:
            raise RuntimeError(f"node is crashed: {node_id}")
        node.persistent[key] = value
        self.trace.record(
            self.clock.now,
            "persistent_write_staged",
            node=node_id,
            key=key,
        )

    def fsync(self, node_id: str) -> None:
        """Advance the simulated disk boundary to the node's current state."""

        node = self._node(node_id)
        if not node.alive:
            raise RuntimeError(f"node is crashed: {node_id}")
        self._durable[node_id] = dict(node.persistent)
        self.trace.record(self.clock.now, "node_fsynced", node=node_id)

    def crash(self, node_id: str, *, lose_unfsynced: bool = False) -> None:
        node = self._node(node_id)
        if not node.alive:
            raise ValueError(f"node already crashed: {node_id}")
        if lose_unfsynced:
            # A power-loss fault restores the last complete fsync image.  It is
            # explicit because an ordinary process crash does not imply that
            # the kernel forgot acknowledged page-cache writes.
            node.persistent.clear()
            node.persistent.update(self._durable[node_id])
        # Clear volatile data before publishing the down state so observers can
        # never see a crashed node that still owns in-flight process memory.
        node.on_crash()
        node.alive = False
        self.trace.record(
            self.clock.now,
            "node_crashed",
            node=node_id,
            lost_unfsynced=lose_unfsynced,
        )

    def restart(self, node_id: str) -> None:
        node = self._node(node_id)
        if node.alive:
            raise ValueError(f"node already running: {node_id}")
        node.on_restart()
        node.alive = True
        self.trace.record(self.clock.now, "node_restarted", node=node_id)

    def _node(self, node_id: str) -> SimNode:
        try:
            return self._nodes[node_id]
        except KeyError as error:
            raise KeyError(f"unknown node: {node_id}") from error
