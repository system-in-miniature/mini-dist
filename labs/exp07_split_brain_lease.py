"""Experiment 7: stale old-leader reads versus authority guards."""

import argparse
from dataclasses import dataclass

from minidist.protocols import AckLevel, ReadLevel
from minidist.protocols.async_primary import AsyncPrimaryGroup
from minidist.protocols.isr import IsrGroup
from minidist.protocols.raft import RaftGroup
from minidist.protocols.wal_shipping import WalShippingGroup


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    protocol: str
    old_side_local_value: bytes | None
    current_value: bytes | None
    guard: str


def run_experiment(*, protocol: str = "isr", verbose: bool = True) -> ExperimentResult:
    if protocol == "async":
        group = AsyncPrimaryGroup(replica_ids=("r1", "r2"))
        group.client_write(b"state", b"before", AckLevel.LEADER)
        group.run_until_idle()
        old = group.primary
        group.isolate(old)
        group.promote("r1")
        group.client_write(b"state", b"after", AckLevel.LEADER)
        group.run_until_idle()
        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
        current = group.client_read(b"state", ReadLevel.LEADER).value
        guard = "unsupported"
    elif protocol == "wal":
        group = WalShippingGroup(standby_ids=("s1", "s2"))
        group.client_write(b"state", b"before", AckLevel.LEADER)
        group.run_until_idle()
        old = group.primary
        group.isolate(old)
        group.promote("s1")
        group.client_write(b"state", b"after", AckLevel.LEADER)
        group.run_until_idle()
        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
        current = group.client_read(b"state", ReadLevel.LEADER).value
        guard = "unsupported"
    elif protocol == "isr":
        group = IsrGroup()
        group.client_write(b"state", b"before", AckLevel.ALL_ISR)
        old = group.leader
        group.isolate(old)
        new = next(node for node in group.probe().isr if node != old)
        group.controller_elect(new)
        group.client_write(b"state", b"after", AckLevel.ALL_ISR)
        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
        current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
        try:
            group.lease_read(old, b"state")
        except RuntimeError:
            guard = "lease fenced"
        else:
            guard = "lease failed open"
    elif protocol == "raft":
        group = RaftGroup(
            seed=707,
            election_timeout_range=(4, 7),
            heartbeat_interval=1,
        )
        old = group.run_until_leader()
        group.client_write(b"state", b"before", AckLevel.QUORUM)
        group.run_until_converged()
        group.isolate(old)
        group.run_until_leader(exclude=old)
        group.client_write(b"state", b"after", AckLevel.QUORUM)
        old_value = group.client_read(b"state", ReadLevel.LOCAL, node=old).value
        current = group.client_read(b"state", ReadLevel.LINEARIZABLE).value
        guard = "read-index fenced"
    else:
        raise ValueError("protocol must be async, wal, isr, or raft")

    result = ExperimentResult(protocol, old_value, current, guard)
    if verbose:
        print(
            f"实验 7 [{protocol}]：old_LOCAL={old_value!r}; "
            f"current={current!r}; authority_guard={guard}"
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
