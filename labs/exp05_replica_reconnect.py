"""Experiment 5: compare incremental and full catch-up triggers."""

import argparse
from dataclasses import dataclass

from minidist.protocols import AckLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.isr import IsrGroup
from minidist.protocols.raft import RaftGroup
from minidist.protocols.wal_shipping import WalShippingGroup


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    protocol: str
    inside_window: str
    outside_window: str


def _write_many(group: object, ack: AckLevel, count: int) -> None:
    for number in range(count):
        group.client_write(f"k{number}".encode(), str(number).encode(), ack)  # type: ignore[attr-defined]


def run_experiment(*, protocol: str = "async", verbose: bool = True) -> ExperimentResult:
    if protocol == "async":
        inside = AsyncPrimaryGroup(replica_ids=("r",), backlog_size=3)
        inside.crash("r")
        _write_many(inside, AckLevel.LEADER, 2)
        inside.restart("r")
        inside.run_until_idle()
        inside_mode = inside.probe().nodes["r"].sync_mode.name.lower().replace("partial", "incremental")

        outside = AsyncPrimaryGroup(replica_ids=("r",), backlog_size=2)
        outside.crash("r")
        _write_many(outside, AckLevel.LEADER, 3)
        outside.restart("r")
        outside.run_until_idle()
        outside_mode = outside.probe().nodes["r"].sync_mode.name.lower()
    elif protocol == "wal":
        inside = WalShippingGroup(standby_ids=("r",), wal_retention=3)
        inside.crash("r")
        _write_many(inside, AckLevel.LEADER, 2)
        inside.restart("r")
        inside.run_until_idle()
        inside_mode = inside.probe().nodes["r"].sync_mode.value

        outside = WalShippingGroup(standby_ids=("r",), wal_retention=2)
        outside.crash("r")
        _write_many(outside, AckLevel.LEADER, 3)
        outside.restart("r")
        outside.run_until_idle()
        outside_mode = outside.probe().nodes["r"].sync_mode.value
    elif protocol == "isr":
        inside = IsrGroup(log_retention=3)
        follower = next(node for node in inside.probe().nodes if node != inside.probe().leader)
        inside.crash(follower)
        _write_many(inside, AckLevel.ALL_ISR, 2)
        inside.restart(follower)
        inside.run_until_idle()
        inside_mode = inside.probe().nodes[follower].catch_up_mode.value

        outside = IsrGroup(log_retention=2)
        follower = next(node for node in outside.probe().nodes if node != outside.probe().leader)
        outside.crash(follower)
        _write_many(outside, AckLevel.ALL_ISR, 3)
        outside.restart(follower)
        outside.run_until_idle()
        outside_mode = outside.probe().nodes[follower].catch_up_mode.value
    elif protocol == "raft":
        def replay(count: int) -> str:
            group = RaftGroup(
                seed=505,
                election_timeout_range=(4, 7),
                heartbeat_interval=1,
            )
            group.run_until_leader()
            follower = next(node for node in group.probe().nodes if node != group.probe().leader)
            group.crash(follower)
            _write_many(group, AckLevel.QUORUM, count)
            group.restart(follower)
            group.run_until_converged()
            return "log replay"

        inside_mode, outside_mode = replay(2), replay(6)
    else:
        raise ValueError("protocol must be async, wal, isr, or raft")

    result = ExperimentResult(protocol, inside_mode, outside_mode)
    if verbose:
        print(
            f"实验 5 [{protocol}]：inside_window={inside_mode}; "
            f"outside_window={outside_mode}"
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--protocol",
        choices=("async", "wal", "isr", "raft", "all"),
        default="all",
    )
    args = parser.parse_args()
    selected = ("async", "wal", "isr", "raft") if args.protocol == "all" else (args.protocol,)
    for name in selected:
        run_experiment(protocol=name)
