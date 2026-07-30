from __future__ import annotations

import pytest

from minidist.sim import (
    FailureInjector,
    Scheduler,
    SimClock,
    SimNet,
    SimNode,
    Trace,
)


def test_clock_advances_only_by_positive_manual_steps() -> None:
    clock = SimClock()

    assert clock.now == 0
    assert clock.advance(3) == 3
    with pytest.raises(ValueError, match="positive"):
        clock.advance(0)


def test_scheduler_orders_same_tick_events_by_insertion() -> None:
    clock = SimClock()
    trace = Trace()
    scheduler = Scheduler(clock, trace)
    observed: list[str] = []

    scheduler.schedule(1, lambda: observed.append("first"), label="first")
    scheduler.schedule(1, lambda: observed.append("second"), label="second")
    assert scheduler.tick() == 2

    assert observed == ["first", "second"]
    assert clock.now == 1


def _network_script(seed: int) -> list[dict[str, object]]:
    clock = SimClock()
    trace = Trace()
    scheduler = Scheduler(clock, trace)
    network = SimNet(
        seed=seed,
        clock=clock,
        scheduler=scheduler,
        trace=trace,
        min_delay=1,
        max_delay=3,
        drop_rate=0.2,
        reorder_rate=0.8,
    )
    network.register("b", lambda message: None)
    for number in range(8):
        network.send("a", "b", {"number": number})
    scheduler.run_until_idle()
    return trace.as_dicts()


def test_same_seed_and_script_replay_event_for_event() -> None:
    first = _network_script(1729)
    replay = _network_script(1729)
    other_seed = _network_script(1730)

    assert first == replay
    assert first != other_seed
    dropped = [
        event["details"]["message_id"]
        for event in first
        if event["kind"] == "message_dropped"
    ]
    delivered = [
        event["details"]["message_id"]
        for event in first
        if event["kind"] == "message_delivered"
    ]
    delivery_ticks = [
        event["tick"] for event in first if event["kind"] == "message_delivered"
    ]
    assert dropped == [1, 2]
    assert delivered == [4, 5, 7, 0, 3, 6]
    assert delivery_ticks == [1, 3, 3, 4, 5, 6]


def test_directional_and_bidirectional_partitions() -> None:
    clock = SimClock()
    trace = Trace()
    scheduler = Scheduler(clock, trace)
    network = SimNet(seed=7, clock=clock, scheduler=scheduler, trace=trace, min_delay=1)
    received: list[tuple[str, object]] = []
    network.register("a", lambda message: received.append(("a", message.payload)))
    network.register("b", lambda message: received.append(("b", message.payload)))

    network.partition("a", "b")
    network.send("a", "b", "blocked")
    network.send("b", "a", "allowed")
    scheduler.run_until_idle()
    assert received == [("a", "allowed")]

    network.heal("a", "b")
    network.partition("a", "b", bidirectional=True)
    network.send("a", "b", "blocked")
    network.send("b", "a", "also blocked")
    scheduler.run_until_idle()
    assert received == [("a", "allowed")]


def test_crash_restart_clears_volatile_and_keeps_persistent_state() -> None:
    trace = Trace()
    clock = SimClock()
    node = SimNode(
        node_id="n1",
        persistent={"offset": 4},
        volatile={"inflight": b"write"},
    )
    failures = FailureInjector(clock=clock, trace=trace)
    failures.register(node)

    failures.crash("n1")
    assert not node.alive
    assert node.volatile == {}
    assert node.persistent == {"offset": 4}

    failures.restart("n1")
    assert node.alive
    assert node.persistent == {"offset": 4}
