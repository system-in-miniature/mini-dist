# Stage 05 · 可比较实验 1–3

### 目标

把前三个协议故事变成可执行、可断言的实验：保持工作负载不变，只改变复制机制。

??? note "交付文件"
    - `labs/__init__.py`
    - `labs/exp01_normal_replication.py`
    - `labs/exp02_acked_write_loss.py`
    - `labs/exp03_partition_old_leader.py`
    - `tests/labs/test_experiments.py`

### 当前遇到的问题

协议单测证明局部不变量，却还没有学习者可见的统一比较面。没有共享场景时，很容易拿异步复制正常路径对比 Raft 故障路径，再把工作负载差异误归因于协议。

### 测试契约

#### 先看会坏在哪里

实验 2 对两个协议执行相同的写入、确认、立即 Crash Leader、故障转移与读取。异步结果必须丢失值，Raft 结果必须保留值。如果 Lab 只在一侧推进时间，比较在观察协议语义前就已被污染。

??? note "文件差异：tests/labs/test_experiments.py"
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

**测试锁定什么**

五条契约锁定延迟收敛、两个协议在确认后故障转移的结果，以及 Raft 少数侧旧 Leader 的 Fencing 结果。

**如何构造反例**

测试以 `verbose=False` 调用实验并断言返回的结构化事实，不解析控制台文案。

**关键测试语句**

```python
assert result.value_before_failover == b"confirmed"
assert result.value_after_failover is None
```

**失败意味着什么**

场景不再隔离确认或权威，或者实验报告的故事与协议可观察状态不一致。

### 基本概念

实验由受控脚本、观察边界与结构化结果组成。自变量是协议；工作负载和故障时机保持不变。控制台输出服务学习者，Dataclass Result 才是验证 API。只有不支持的保证仍然可见，而非被统一抹平，比较才诚实。

### 为什么需要这个机制

项目是一条协议光谱，不是四个互不相关的实现。可执行 Lab 把内部机制连接到外部差异，并防止文档结论与代码漂移。

### 运行时心智模型

每个 `run_experiment` 创建确定性 Group，只调用公开驱动，在确认或分区边界精确取样，按脚本推进仿真，再返回不可变观察。可选打印解释同一组事实，但不成为事实来源。

### 机制板块

#### 实验 1–2：收敛与已确认写入存活

对异步主从与 Raft 执行相同观察序列，让确认边界而不是叙述解释结果。

??? note "文件差异：labs/exp01_normal_replication.py"
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

**是什么，为什么现在需要**

实验 1 分别在确认边界与收敛后观察异步主从和 Raft 的 Follower。

**在运行时做什么**

Protocol Switch 改变 Group 构造与支持的 Ack Level，但保持写入、观察、推进、再观察的顺序。

**关键代码**

```python
before = group.client_read(
    b"project", ReadLevel.LOCAL, node="replica-1"
).value
group.tick()
```

**关键语句理解**

第一次读取发生在显式 Tick 以前，因此 `None` 是延迟可见性的证据，不是写入失败。

??? note "文件差异：labs/exp02_acked_write_loss.py"
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

**是什么，为什么现在需要**

实验 2 把故障固定在确认之后，比较新权威节点能读到什么。

**在运行时做什么**

异步 Promotion 选择可能落后的 Replica；Raft 从已经保存已提交 Entry 的多数派中选举。

**关键代码**

```python
group.crash(old_primary)
group.promote("replica-1")
group.run_until_idle()
```

**关键语句理解**

确认与 Crash 之间没有 Delivery Tick。正是这段缺失制造异步丢失窗口并保持比较的因果性。

#### 实验 3：少数侧权威与日志修复

把 Raft 分区行为变成有关确认、Term、Role、Log 与最终值的返回事实。

??? note "文件差异：labs/exp03_partition_old_leader.py"
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

**是什么，为什么现在需要**

实验 3 记录分区期间本地 Leader 身份与多数派权威之间的差别。

**在运行时做什么**

它在被隔离旧 Leader 上写入，在多数侧选举并写入，恢复网络后返回 Role、Term、Log 与 Value 证据。

**关键代码**

```python
group.heal(old_leader)
group.run_until_converged()
state = group.probe()
```

**关键语句理解**

仅仅 Heal 不是结论；收敛再 Probe 才能证明旧后缀已修复且多数侧值成为共享状态。

#### 实验包脚手架

把包标记保留为支撑接线，而不是协议课程。

??? note "支撑文件差异（1 个文件）"
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


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-spectrum-experiments/tests.txt)`。五条契约把三个学习者可见实验绑定到真实协议观察。

### 需要真正记住的内容

固定脚本，在命名边界观察，并返回结构化事实。只有实验本身可比较，不同结果才有意义。

### 用自己的话讲清楚

说明为什么实验 2 比分别演示“异步会丢数据”和“Raft 能抗崩溃”提供更强证据。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-dist/blob/main/docs/zh/tutorial/08-experiments.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-dist/compare/stage-04...stage-05)

完成后可运行 `git checkout stage-05` 对照你的结果。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-dist/blob/main/journey/stages/05-spectrum-experiments/stage.patch)
