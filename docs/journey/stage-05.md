# Stage 05 · Comparable experiments 1–3

### Goal

Convert the first three protocol stories into executable, assertion-friendly experiments that hold the workload constant while varying the replication mechanism.

??? note "Deliverable files"
    - `labs/__init__.py`
    - `labs/exp01_normal_replication.py`
    - `labs/exp02_acked_write_loss.py`
    - `labs/exp03_partition_old_leader.py`
    - `tests/labs/test_experiments.py`

### The problem at this point

Protocol unit tests prove local invariants but do not yet provide one learner-visible comparison surface. Without shared scenarios, it is easy to compare an async happy path with a Raft failure path and attribute workload differences to the protocols.

### Test contract

#### See the failure first

Experiment 2 performs the same write, acknowledgement, immediate leader crash, failover, and read against both protocols. The async result must lose the value while the Raft result retains it. If a lab advances time on only one side, the comparison is contaminated before protocol semantics are observed.

??? note "File diff: tests/labs/test_experiments.py"
    ```diff
    diff --git a/tests/labs/test_experiments.py b/tests/labs/test_experiments.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..eaedcc8ed27ab20a386554ce4428a061576cf763
    --- /dev/null
    +++ b/tests/labs/test_experiments.py
    @@ -0,0 +1,44 @@
    +from labs.exp01_normal_replication import run_experiment as run_normal
    +from labs.exp02_acked_write_loss import run_experiment as run_loss
    +from labs.exp03_partition_old_leader import run_experiment as run_partition
    +
    +
    +def test_experiment_1_observes_delayed_convergence() -> None:
    +    result = run_normal(verbose=False)
    +    assert result.before_tick is None
    +    assert result.after_tick == b"MiniDist"
    +    assert result.replica_offset == result.primary_offset == 1
    +
    +
    +def test_experiment_2_proves_acknowledged_write_can_be_lost() -> None:
    +    result = run_loss(protocol="async", verbose=False)
    +    assert result.acknowledged
    +    assert result.value_before_failover == b"confirmed"
    +    assert result.value_after_failover is None
    +    assert result.new_primary == "replica-1"
    +
    +
    +def test_experiment_1_raft_converges_after_quorum_ack() -> None:
    +    result = run_normal(protocol="raft", verbose=False)
    +    assert result.before_tick is None
    +    assert result.after_tick == b"MiniDist"
    +    assert result.replica_offset == result.primary_offset
    +
    +
    +def test_experiment_2_raft_acknowledged_write_survives_failover() -> None:
    +    result = run_loss(protocol="raft", verbose=False)
    +    assert result.acknowledged
    +    assert result.value_before_failover == b"confirmed"
    +    assert result.value_after_failover == b"confirmed"
    +    assert result.new_primary != result.old_primary
    +
    +
    +def test_experiment_3_old_leader_is_fenced_and_cluster_converges() -> None:
    +    result = run_partition(verbose=False)
    +    assert not result.old_leader_write_committed
    +    assert result.new_leader_write_committed
    +    assert result.new_leader != result.old_leader
    +    assert result.old_leader_role_after_heal == "follower"
    +    assert result.logs_converged
    +    assert result.majority_value == b"continues"
    +    assert result.minority_value is None
    ```

**What this test locks**

Five contracts lock delayed convergence, post-ack failover outcomes for both protocols, and Raft's minority-old-leader fencing result.

**How it constructs the counterexample**

Tests call each experiment with `verbose=False` and assert returned structured facts rather than parsing console prose.

**Key test statement**

```python
assert result.value_before_failover == b"confirmed"
assert result.value_after_failover is None
```

**What a failure means**

The scenario no longer isolates acknowledgement or authority, or the experiment reports a story inconsistent with the protocol's observable state.

### Basic concepts

An experiment has a controlled script, an observation boundary, and a structured result. The independent variable is the protocol; the workload and failure timing stay fixed. Console output is for learners, while dataclass results are the verification API. A comparison is honest only when unsupported guarantees remain visible rather than normalized away.

### Why this mechanism is necessary

The project is a protocol spectrum, not four unrelated implementations. Executable labs connect internal mechanics to externally observable differences and prevent documentation conclusions from drifting away from code.

### Runtime mental model

Each `run_experiment` constructs a deterministic group, issues only public driver calls, captures state exactly at the acknowledgement or partition boundary, advances the simulation when the script says, and returns immutable observations. Optional printing explains those same facts without becoming the source of truth.

### Mechanism blocks

#### Experiments 1–2: convergence and acknowledged-write survival

Run the same observation sequence against async primary and Raft so the acknowledgement boundary, not narration, explains the outcome.

??? note "File diff: labs/exp01_normal_replication.py"
    ```diff
    diff --git a/labs/exp01_normal_replication.py b/labs/exp01_normal_replication.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..596ebcdb30c09f62018761dfc881c8e2efbf65ab
    --- /dev/null
    +++ b/labs/exp01_normal_replication.py
    @@ -0,0 +1,91 @@
    +"""Experiment 1: compare normal async and Raft replication convergence."""
    +
    +import argparse
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel, ReadLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.raft import RaftGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    before_tick: bytes | None
    +    after_tick: bytes | None
    +    primary_offset: int
    +    replica_offset: int
    +
    +
    +def run_experiment(
    +    *,
    +    protocol: str = "async",
    +    verbose: bool = True,
    +) -> ExperimentResult:
    +    """Run one protocol using only its public replication-group API."""
    +
    +    if protocol == "async":
    +        group = AsyncPrimaryGroup(
    +            replica_ids=("replica-1",),
    +            replication_delay=1,
    +            seed=101,
    +        )
    +        write = group.client_write(b"project", b"MiniDist", AckLevel.LEADER)
    +        before = group.client_read(
    +            b"project", ReadLevel.LOCAL, node="replica-1"
    +        ).value
    +        group.tick()
    +        after = group.client_read(
    +            b"project", ReadLevel.LOCAL, node="replica-1"
    +        ).value
    +        state = group.probe()
    +        primary_offset = state.nodes[state.primary].offset
    +        replica_offset = state.nodes["replica-1"].offset
    +        title = "异步主从"
    +        ack_note = "primary 立即 ack，复制仍在途中"
    +    elif protocol == "raft":
    +        group = RaftGroup(
    +            node_ids=("node-1", "node-2", "node-3"),
    +            seed=101,
    +            election_timeout_range=(4, 7),
    +            heartbeat_interval=1,
    +        )
    +        group.run_until_leader()
    +        write = group.client_write(b"project", b"MiniDist", AckLevel.QUORUM)
    +        state = group.probe()
    +        follower = next(node for node in state.nodes if node != state.leader)
    +        before = state.nodes[follower].data.get(b"project")
    +        group.run_until_converged()
    +        state = group.probe()
    +        after = state.nodes[follower].data.get(b"project")
    +        primary_offset = state.nodes[state.leader].commit_index
    +        replica_offset = state.nodes[follower].commit_index
    +        title = "Raft"
    +        ack_note = "多数派复制后才 ack，随后 leaderCommit 传播到全部 follower"
    +    else:
    +        raise ValueError("protocol must be 'async' or 'raft'")
    +
    +    result = ExperimentResult(
    +        before_tick=before,
    +        after_tick=after,
    +        primary_offset=primary_offset,
    +        replica_offset=replica_offset,
    +    )
    +
    +    if verbose:
    +        print(f"实验 1：正常复制（{title}）")
    +        print(f"1) 写入在 offset={write.offset} 返回 ack；{ack_note}。")
    +        print(f"2) ack 边界处，观察 follower 状态机：{before!r}。")
    +        print(f"3) 推进至收敛后，观察 follower 状态机：{after!r}。")
    +        print(
    +            "4) 最终 offset："
    +            f"leader={result.primary_offset}, "
    +            f"follower={result.replica_offset}；副本已收敛。"
    +        )
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument("--protocol", choices=("async", "raft"), default="async")
    +    args = parser.parse_args()
    +    run_experiment(protocol=args.protocol)
    ```

**What it is and why it appears**

Experiment 1 observes a follower at the acknowledgement boundary and after convergence for async primary and Raft.

**Runtime role**

The protocol switch changes group construction and supported ack level while preserving write, observe, advance, observe.

**Key code**

```python
before = group.client_read(
    b"project", ReadLevel.LOCAL, node="replica-1"
).value
group.tick()
```

**Statement understanding**

The first read occurs before the explicit tick, so `None` is evidence of delayed visibility rather than a failed write.

??? note "File diff: labs/exp02_acked_write_loss.py"
    ```diff
    diff --git a/labs/exp02_acked_write_loss.py b/labs/exp02_acked_write_loss.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..692e697615c42ddf681add9b5791a1e6725679c4
    --- /dev/null
    +++ b/labs/exp02_acked_write_loss.py
    @@ -0,0 +1,92 @@
    +"""Experiment 2: contrast async loss with Raft quorum durability."""
    +
    +import argparse
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel, ReadLevel
    +from minidist.protocols.async_primary import AsyncPrimaryGroup
    +from minidist.protocols.raft import RaftGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    acknowledged: bool
    +    value_before_failover: bytes | None
    +    value_after_failover: bytes | None
    +    old_primary: str
    +    new_primary: str
    +
    +
    +def run_experiment(
    +    *,
    +    protocol: str = "async",
    +    verbose: bool = True,
    +) -> ExperimentResult:
    +    """Ack, immediately crash the leader, fail over, then read the same key."""
    +
    +    if protocol == "async":
    +        group = AsyncPrimaryGroup(
    +            replica_ids=("replica-1",),
    +            replication_delay=2,
    +            seed=202,
    +        )
    +        old_primary = group.primary
    +        write = group.client_write(b"order:42", b"confirmed", AckLevel.LEADER)
    +        before = group.client_read(b"order:42", ReadLevel.LEADER).value
    +
    +        # No tick occurs here: the client has an ack while the replica is stale.
    +        group.crash(old_primary)
    +        group.promote("replica-1")
    +        group.run_until_idle()
    +        new_primary = group.primary
    +        after = group.client_read(b"order:42", ReadLevel.LEADER).value
    +        title = "异步主从"
    +        conclusion = "已确认写丢失"
    +    elif protocol == "raft":
    +        group = RaftGroup(
    +            node_ids=("node-1", "node-2", "node-3"),
    +            seed=202,
    +            election_timeout_range=(4, 7),
    +            heartbeat_interval=1,
    +        )
    +        old_primary = group.run_until_leader()
    +        write = group.client_write(b"order:42", b"confirmed", AckLevel.QUORUM)
    +        before = group.client_read(b"order:42", ReadLevel.LEADER).value
    +
    +        # QUORUM returned only after a majority stored the entry.
    +        group.crash(old_primary)
    +        new_primary = group.run_until_leader(exclude=old_primary)
    +        after = group.client_read(b"order:42", ReadLevel.LINEARIZABLE).value
    +        title = "Raft"
    +        conclusion = "多数派确认的写在换主后仍在"
    +    else:
    +        raise ValueError("protocol must be 'async' or 'raft'")
    +
    +    result = ExperimentResult(
    +        acknowledged=write.accepted,
    +        value_before_failover=before,
    +        value_after_failover=after,
    +        old_primary=old_primary,
    +        new_primary=new_primary,
    +    )
    +
    +    if verbose:
    +        print(f"实验 2：ack 后立即 crash leader（{title}）")
    +        print(
    +            "1) client 写入 order:42=confirmed；"
    +            f"leader 在 offset={write.offset} 返回 ack={write.accepted}。"
    +        )
    +        print(f"2) 故障前从 leader 读取：{before!r}。")
    +        print(f"3) ack 后立即 crash {old_primary}，随后完成 failover。")
    +        print(
    +            f"4) 新 leader={result.new_primary}，读取 order:42：{after!r}。"
    +        )
    +        print(f"结论：{conclusion}。")
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    parser = argparse.ArgumentParser()
    +    parser.add_argument("--protocol", choices=("async", "raft"), default="async")
    +    args = parser.parse_args()
    +    run_experiment(protocol=args.protocol)
    ```

**What it is and why it appears**

Experiment 2 pins the failure immediately after acknowledgement and compares what the newly authoritative node can read.

**Runtime role**

Async promotion selects a possibly stale replica; Raft elects from a majority that already stored the committed entry.

**Key code**

```python
group.crash(old_primary)
group.promote("replica-1")
group.run_until_idle()
```

**Statement understanding**

No delivery tick occurs between acknowledgement and crash. That absence creates the async loss window and keeps the comparison causal.

#### Experiment 3: minority authority and log repair

Turn Raft's partition behavior into returned facts about acceptance, terms, roles, logs, and final values.

??? note "File diff: labs/exp03_partition_old_leader.py"
    ```diff
    diff --git a/labs/exp03_partition_old_leader.py b/labs/exp03_partition_old_leader.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e8f871cf0ac204720d04afbf65197dc94902bbeb
    --- /dev/null
    +++ b/labs/exp03_partition_old_leader.py
    @@ -0,0 +1,97 @@
    +"""Experiment 3: isolate a Raft leader, replace it, then heal its stale log.
    +
    +Only public ``RaftGroup`` operations are used.  The old leader remains leader
    +in its own obsolete term while isolated, but cannot turn a local append into a
    +QUORUM acknowledgement.  The majority elects a higher-term leader and keeps
    +serving.  On healing, the higher term fences the old leader and AppendEntries'
    +prefix check replaces its conflicting suffix.
    +"""
    +
    +from dataclasses import dataclass
    +
    +from minidist.protocols import AckLevel
    +from minidist.protocols.raft import RaftGroup
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ExperimentResult:
    +    old_leader: str
    +    new_leader: str
    +    old_term: int
    +    new_term: int
    +    old_leader_write_committed: bool
    +    new_leader_write_committed: bool
    +    old_leader_role_after_heal: str
    +    logs_converged: bool
    +    minority_value: bytes | None
    +    majority_value: bytes | None
    +
    +
    +def run_experiment(*, verbose: bool = True) -> ExperimentResult:
    +    """Run the minority-old-leader partition and return assertion-friendly facts."""
    +
    +    group = RaftGroup(
    +        node_ids=("node-1", "node-2", "node-3"),
    +        seed=303,
    +        election_timeout_range=(4, 7),
    +        heartbeat_interval=1,
    +    )
    +    old_leader = group.run_until_leader()
    +    old_term = group.probe().nodes[old_leader].current_term
    +    group.isolate(old_leader)
    +
    +    minority_write = group.client_write(
    +        b"minority-only",
    +        b"must-not-commit",
    +        AckLevel.QUORUM,
    +    )
    +    new_leader = group.run_until_leader(exclude=old_leader)
    +    majority_write = group.client_write(
    +        b"majority",
    +        b"continues",
    +        AckLevel.QUORUM,
    +    )
    +    new_term = group.probe().nodes[new_leader].current_term
    +
    +    group.heal(old_leader)
    +    group.run_until_converged()
    +    state = group.probe()
    +    reference_log = state.nodes[new_leader].log
    +    result = ExperimentResult(
    +        old_leader=old_leader,
    +        new_leader=new_leader,
    +        old_term=old_term,
    +        new_term=new_term,
    +        old_leader_write_committed=minority_write.accepted,
    +        new_leader_write_committed=majority_write.accepted,
    +        old_leader_role_after_heal=state.nodes[old_leader].role.value,
    +        logs_converged=all(node.log == reference_log for node in state.nodes.values()),
    +        minority_value=state.nodes[new_leader].data.get(b"minority-only"),
    +        majority_value=state.nodes[new_leader].data.get(b"majority"),
    +    )
    +
    +    if verbose:
    +        print("实验 3：旧 leader 位于少数派的 Raft 网络分区")
    +        print(
    +            f"1) term={old_term} 的旧 leader {old_leader} 被双向隔离；"
    +            f"其新写 QUORUM ack={minority_write.accepted}。"
    +        )
    +        print(
    +            f"2) 多数派在 term={new_term} 选出 {new_leader}；"
    +            f"继续写入的 QUORUM ack={majority_write.accepted}。"
    +        )
    +        print(
    +            f"3) 分区愈合后旧 leader 角色={result.old_leader_role_after_heal}；"
    +            f"日志收敛={result.logs_converged}。"
    +        )
    +        print(
    +            "4) 最终状态："
    +            f"minority-only={result.minority_value!r}, "
    +            f"majority={result.majority_value!r}。"
    +        )
    +        print("结论：高任期完成 fencing，少数派未提交分叉被新 leader 日志覆盖。")
    +    return result
    +
    +
    +if __name__ == "__main__":
    +    run_experiment()
    ```

**What it is and why it appears**

Experiment 3 records the difference between local leader identity and majority-backed authority during a partition.

**Runtime role**

It writes on the isolated old leader, elects and writes on the majority side, heals, then returns role, term, log, and value evidence.

**Key code**

```python
group.heal(old_leader)
group.run_until_converged()
state = group.probe()
```

**Statement understanding**

Healing alone is not the conclusion; convergence plus a probe proves the old suffix was repaired and the majority value became shared state.

#### Experiment package scaffold

Keep the package marker as supporting wiring rather than a protocol lesson.

??? note "Supporting file diffs (1 file)"
    **`labs/__init__.py`**

    ```diff
    diff --git a/labs/__init__.py b/labs/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e6262e81ca57c9fcc9ee133acbdb0ea3cc115f7a
    --- /dev/null
    +++ b/labs/__init__.py
    @@ -0,0 +1 @@
    +"""Runnable MiniDist experiments; each lab also exposes a testable function."""
    ```


### Verification evidence

Run `uv run pytest -q $(cat journey/stages/05-spectrum-experiments/tests.txt)`. The five contracts bind the three learner-facing experiments to real protocol observations.

### Durable takeaways

Hold the script fixed, observe at named boundaries, and return structured facts. Different outcomes are meaningful only when the experiment itself is comparable.

### Explain it in your own words

Explain why Experiment 2 is stronger evidence than separately demonstrating “async can lose data” and “Raft can survive a crash.”

### Textbook

[Chapter 8](https://github.com/system-in-miniature/mini-dist/blob/main/docs/tutorial/08-experiments.md)

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-dist/compare/stage-04...stage-05)

After finishing, use `git checkout stage-05` to compare your result.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/05-spectrum-experiments/stage.patch)
