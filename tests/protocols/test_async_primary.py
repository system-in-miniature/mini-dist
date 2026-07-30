from __future__ import annotations

import pytest

from minidist.protocols import AckLevel, ReadLevel, UnsupportedLevelError
from minidist.protocols.async_primary import AsyncPrimaryGroup, SyncMode


def test_write_acks_before_replica_converges() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)

    result = group.client_write(b"course", b"MiniDist", AckLevel.LEADER)

    assert result.accepted
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    group.tick()
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value is None
    group.tick()
    assert group.client_read(b"course", ReadLevel.LOCAL, node="r1").value == b"MiniDist"


@pytest.mark.parametrize("level", [AckLevel.QUORUM, AckLevel.ALL_ISR])
def test_async_primary_rejects_stronger_ack_levels(level: AckLevel) -> None:
    group = AsyncPrimaryGroup()
    with pytest.raises(UnsupportedLevelError, match=level.name):
        group.client_write(b"k", b"v", level)


def test_async_primary_rejects_linearizable_reads() -> None:
    group = AsyncPrimaryGroup()
    with pytest.raises(UnsupportedLevelError, match="LINEARIZABLE"):
        group.client_read(b"k", ReadLevel.LINEARIZABLE)


def test_restart_uses_partial_resync_inside_backlog() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=3, replication_delay=1)
    group.client_write(b"a", b"1", AckLevel.LEADER)
    group.tick()
    group.crash("r1")
    group.client_write(b"b", b"2", AckLevel.LEADER)
    group.client_write(b"c", b"3", AckLevel.LEADER)

    group.restart("r1")
    group.run_until_idle()

    state = group.probe()
    assert state.nodes["r1"].sync_mode is SyncMode.PARTIAL
    assert state.nodes["r1"].data == {b"a": b"1", b"b": b"2", b"c": b"3"}


def test_restart_uses_full_resync_outside_backlog() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), backlog_size=2, replication_delay=1)
    group.crash("r1")
    for number in range(3):
        group.client_write(f"k{number}".encode(), str(number).encode(), AckLevel.LEADER)

    group.restart("r1")
    group.run_until_idle()

    state = group.probe()
    assert state.nodes["r1"].sync_mode is SyncMode.FULL
    assert state.nodes["r1"].data == state.nodes[state.primary].data


def test_manual_promotion_can_lose_acknowledged_write() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    result = group.client_write(b"paid", b"yes", AckLevel.LEADER)

    group.crash("primary")
    group.promote("r1")
    group.run_until_idle()

    assert result.accepted
    assert group.client_read(b"paid", ReadLevel.LEADER).value is None
    assert group.probe().primary == "r1"


def test_queued_old_primary_full_sync_is_rejected_after_promotion() -> None:
    group = AsyncPrimaryGroup(
        replica_ids=("r1", "r2"),
        backlog_size=1,
        replication_delay=2,
    )
    group.crash("r2")
    group.client_write(b"first", b"old-history", AckLevel.LEADER)
    group.client_write(b"second", b"old-history", AckLevel.LEADER)
    group.run_until_idle()

    group.restart("r2")
    group.promote("r1")
    group.run_until_idle()

    rejected = [
        event
        for event in group.trace.events
        if event.kind == "replication_message_rejected"
        and event.details["message_type"] == "full_sync"
        and event.details["source_primary"] == "primary"
    ]
    assert rejected
    assert rejected[0].details["reason"] == "non_current_primary"
    state = group.probe()
    assert state.nodes["r2"].data == state.nodes["r1"].data
    assert state.nodes["r2"].replication_id == state.nodes["r1"].replication_id


def test_promotion_converges_alive_replicas_without_a_new_write() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1", "r2"), replication_delay=1)
    group.isolate("r2")
    group.client_write(b"before-promotion", b"present", AckLevel.LEADER)
    group.run_until_idle()
    group.heal("r2")

    group.promote("r1")
    group.run_until_idle()

    state = group.probe()
    assert state.nodes["r2"].data == state.nodes["r1"].data
    assert state.nodes["r2"].offset == state.nodes["r1"].offset
    assert state.nodes["r2"].replication_id == state.nodes["r1"].replication_id


def test_queued_old_primary_backlog_entry_is_rejected_after_promotion() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    group.client_write(b"old-primary", b"dirty", AckLevel.LEADER)

    group.promote("r1")
    group.run_until_idle()

    rejected = [
        event
        for event in group.trace.events
        if event.kind == "replication_message_rejected"
        and event.details["message_type"] == "entry"
    ]
    assert rejected
    assert rejected[0].details["reason"] == "non_current_primary"


@pytest.mark.parametrize(
    ("generation", "replication_id", "reason"),
    [
        (0, "repl-1", "stale_generation"),
        (1, "obsolete-lineage", "stale_replication_id"),
    ],
)
def test_current_primary_delivery_requires_current_generation_and_replication_id(
    generation: int,
    replication_id: str,
    reason: str,
) -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=1)
    group.network.send(
        "primary",
        "r1",
        {
            "type": "entry",
            "mode": "stream",
            "source_primary": "primary",
            "generation": generation,
            "replication_id": replication_id,
            "offset": 1,
            "key": b"fenced",
            "value": b"dirty",
        },
    )

    group.run_until_idle()

    assert group.probe().nodes["r1"].data == {}
    assert any(
        event.kind == "replication_message_rejected"
        and event.details["reason"] == reason
        for event in group.trace.events
    )


def test_power_loss_discards_acked_but_unfsynced_primary_state() -> None:
    group = AsyncPrimaryGroup(replica_ids=("r1",), replication_delay=2)
    group.client_write(b"durable", b"yes", AckLevel.LEADER)
    group.fsync("primary")
    group.client_write(b"window", b"lost", AckLevel.LEADER)

    group.crash("primary", lose_unfsynced=True)
    group.restart("primary")

    assert group.client_read(b"durable", ReadLevel.LEADER).value == b"yes"
    assert group.client_read(b"window", ReadLevel.LEADER).value is None
    assert group.probe().nodes["primary"].durable_offset == 1
