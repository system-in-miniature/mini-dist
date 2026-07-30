"""Experiment 4: compare availability while one replica is slow."""

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
    write_available: bool
    slow_replica_stale: bool
    membership_effect: str


def run_experiment(*, protocol: str = "isr", verbose: bool = True) -> ExperimentResult:
    if protocol == "async":
        group = AsyncPrimaryGroup(replica_ids=("slow", "fast"))
        group.isolate("slow")
        write = group.client_write(b"k", b"v", AckLevel.LEADER)
        group.run_until_idle()
        stale = group.probe().nodes["slow"].data.get(b"k") is None
        effect = "replica is not a voter"
    elif protocol == "wal":
        group = WalShippingGroup(
            standby_ids=("slow", "fast"),
            synchronous_standby_ids=(),
        )
        group.isolate("slow")
        write = group.client_write(b"k", b"v", AckLevel.LEADER)
        group.run_until_idle()
        stale = group.probe().nodes["slow"].data.get(b"k") is None
        effect = "async standby does not gate commit"
    elif protocol == "isr":
        group = IsrGroup(replica_lag_timeout=2, min_insync_replicas=2)
        slow_node = next(
            node for node in group.probe().nodes if node != group.probe().leader
        )
        group.set_replica_slow(slow_node, True)
        write = group.client_write(b"k", b"v", AckLevel.ALL_ISR)
        state = group.probe()
        stale = state.nodes[slow_node].data.get(b"k") is None
        effect = "removed from ISR" if slow_node not in state.isr else "still in ISR"
    elif protocol == "raft":
        group = RaftGroup(
            seed=404,
            election_timeout_range=(4, 7),
            heartbeat_interval=1,
        )
        group.run_until_leader()
        slow_node = next(
            node for node in group.probe().nodes if node != group.probe().leader
        )
        group.isolate(slow_node)
        write = group.client_write(b"k", b"v", AckLevel.QUORUM)
        stale = group.probe().nodes[slow_node].data.get(b"k") is None
        effect = "fixed voter; majority unaffected"
    else:
        raise ValueError("protocol must be async, wal, isr, or raft")

    result = ExperimentResult(protocol, write.accepted, stale, effect)
    if verbose:
        print(f"实验 4 [{protocol}]：write_available={result.write_available}")
        print(
            f"slow_replica_stale={result.slow_replica_stale}; "
            f"membership={result.membership_effect}"
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
