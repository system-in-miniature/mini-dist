"""Experiment 3: isolate a Raft leader, replace it, then heal its stale log.

Only public ``RaftGroup`` operations are used.  The old leader remains leader
in its own obsolete term while isolated, but cannot turn a local append into a
QUORUM acknowledgement.  The majority elects a higher-term leader and keeps
serving.  On healing, the higher term fences the old leader and AppendEntries'
prefix check replaces its conflicting suffix.
"""

from dataclasses import dataclass

from minidist.protocols import AckLevel
from minidist.protocols.raft import RaftGroup


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    old_leader: str
    new_leader: str
    old_term: int
    new_term: int
    old_leader_write_committed: bool
    new_leader_write_committed: bool
    old_leader_role_after_heal: str
    logs_converged: bool
    minority_value: bytes | None
    majority_value: bytes | None


def run_experiment(*, verbose: bool = True) -> ExperimentResult:
    """Run the minority-old-leader partition and return assertion-friendly facts."""

    group = RaftGroup(
        node_ids=("node-1", "node-2", "node-3"),
        seed=303,
        election_timeout_range=(4, 7),
        heartbeat_interval=1,
    )
    old_leader = group.run_until_leader()
    old_term = group.probe().nodes[old_leader].current_term
    group.isolate(old_leader)

    minority_write = group.client_write(
        b"minority-only",
        b"must-not-commit",
        AckLevel.QUORUM,
    )
    new_leader = group.run_until_leader(exclude=old_leader)
    majority_write = group.client_write(
        b"majority",
        b"continues",
        AckLevel.QUORUM,
    )
    new_term = group.probe().nodes[new_leader].current_term

    group.heal(old_leader)
    group.run_until_converged()
    state = group.probe()
    reference_log = state.nodes[new_leader].log
    result = ExperimentResult(
        old_leader=old_leader,
        new_leader=new_leader,
        old_term=old_term,
        new_term=new_term,
        old_leader_write_committed=minority_write.accepted,
        new_leader_write_committed=majority_write.accepted,
        old_leader_role_after_heal=state.nodes[old_leader].role.value,
        logs_converged=all(node.log == reference_log for node in state.nodes.values()),
        minority_value=state.nodes[new_leader].data.get(b"minority-only"),
        majority_value=state.nodes[new_leader].data.get(b"majority"),
    )

    if verbose:
        print("实验 3：旧 leader 位于少数派的 Raft 网络分区")
        print(
            f"1) term={old_term} 的旧 leader {old_leader} 被双向隔离；"
            f"其新写 QUORUM ack={minority_write.accepted}。"
        )
        print(
            f"2) 多数派在 term={new_term} 选出 {new_leader}；"
            f"继续写入的 QUORUM ack={majority_write.accepted}。"
        )
        print(
            f"3) 分区愈合后旧 leader 角色={result.old_leader_role_after_heal}；"
            f"日志收敛={result.logs_converged}。"
        )
        print(
            "4) 最终状态："
            f"minority-only={result.minority_value!r}, "
            f"majority={result.majority_value!r}。"
        )
        print("结论：高任期完成 fencing，少数派未提交分叉被新 leader 日志覆盖。")
    return result


if __name__ == "__main__":
    run_experiment()
