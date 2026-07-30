# 第 2 章——确定性仿真基座

[English](../../tutorial/02-deterministic-simulation.md)

分布式协议最值得观察的通常是尴尬边界：超时恰逢消息到达；节点在确认后、投递前崩溃；延迟包跨越角色切换。用墙上时钟和线程复现这些调度很困难，因此 MiniDist 把调度、随机性、故障与观察都变成单进程中的显式值。

## 学习目标

完成本章后，你将能够：

1. 从 `SimNet.send` 经 `Scheduler.schedule` 追踪事件到注册的消息处理器；
2. 解释 `SimClock`、调度器插入顺序和私有种子 RNG 如何排除宿主时序的不确定性；
3. 区分发送时丢弃与投递时丢弃、生命周期 crash 与网络 partition；
4. 检查 `Trace` 并逐事件比较两次完整运行；以及
5. 不使用 sleep、线程或真实 socket，设计确定性故障实验。

## 逻辑时间是受控状态

`src/minidist/sim/clock.py` 中的 `SimClock` 只拥有整数 `_now`。`SimClock.advance` 只接受正步数，递增并返回该整数；没有 `time.time`、`time.monotonic` 或后台定时器。因此宿主忙碌时，空闲仿真不会自己变老。

这不只是用小整数代替秒。实验工具拥有因果何时推进的控制权。若实验调用 `client_write` 后在 `tick` 前让 primary crash，任何计划中的复制投递都不能因为操作系统调度了另一个线程而偷偷发生。

`src/minidist/sim/scheduler.py` 中的 `Scheduler.schedule` 把相对延迟转换成 `clock.now + delay`。堆键是 `(due_tick, order)`，其中 `order` 是单调插入计数器。因此两个都在 tick 5 到期的回调按调度顺序运行，不依赖回调身份或堆实现。

`Scheduler.tick` 先推进一次时钟，再依次弹出所有 `due_tick <= clock.now` 的事件；每个回调在同一线程中完整运行。`Scheduler.run_until_idle` 重复该过程直到堆为空，并以 `max_ticks=10_000` 防止课程代码永不空闲。

必须精确理解这个结论：确定性仿真不会枚举所有可能调度，而是让一个选定调度可以重放。覆盖更多行为仍需不同 seed 和故障脚本。

同一 seed 下重复的 trace 只证明可重放，本身不证明正确。协议 bug 也可以完全确定。测试仍需声明目标 invariant，例如陈旧 generation 消息必须被拒绝，再用可重放调度作为证据。确定性使失败可解释、可回归，却不替你判断行为是否安全。

### 设计确定性故障脚本

有效脚本在注入故障前先命名边界。“找个时候 crash leader”太含糊；“调用 `client_write`，不调用 `tick`，然后调用 `crash`”则固定确认与计划投递之间的边界。同理，“发送后、投递前 partition”要求先调度消息，再添加相关有向 partition，之后才推进时钟。

协议动作与观察应分开。write、isolate、tick、heal、promote 会改变仿真世界；`probe` 和 trace 筛选只观察。若脚本修改私有节点字典来制造目标状态，它就不再测试公开协议路径。`tests/labs/test_experiments.py` 有意调用与可运行 lab 相同的公开 group API。

好的确定性测试记录三类信息：固定 setup（node ID、seed、延迟范围）、有序 action script，以及所选观察点的 invariant。失败时保留 seed 与完整 trace。一次改变多个故障参数可能发现 bug，但把案例缩减成最小动作序列，才能使因果边界可教学。

当中间状态正是课程重点时，不要盲目调用 `run_until_idle`。它会推进到没有工作，从而抹掉“已计划”和“已投递”的区别。实验 1 只显式推进一个 tick 来暴露这个区别。反过来，若断言目标是最终收敛，且所有有限排队事件都应结束，则应使用 `run_until_idle`。

## 一条消息有两个故障边界

`src/minidist/sim/network.py` 中的 `SimNet` 组合不可变 `Message`、节点处理器注册表、有向 partition 状态和私有 `random.Random(seed)`。`SimNet.send`：

1. 分配单调递增 message ID；
2. 追加 `message_sent` trace；
3. 若有向边已 partition，则立即丢弃；
4. 否则做带种子的丢弃选择；
5. 采样带种子的延迟与可选乱序抖动；
6. 调度稍后调用 `SimNet._deliver` 的回调。

回调不会假设链路一直健康。`SimNet._deliver` 再次检查 partition。因此，partition 前已接受的消息可能以 `partition_at_delivery` 原因丢弃；目的地没有处理器时原因是 `unknown_destination`；否则网络先记录 `message_delivered`，再调用处理器。

两次检查让故障放置可教学：“发送前 partition”与“传输中 partition”对应不同 trace 证据，而不是依赖时序的症状。partition 默认有向：`SimNet.partition("a", "b")` 只阻断 a→b；`bidirectional=True` 还阻断 b→a。`heal` 删除相同边。

网络运输 Python payload，有意省略 framing、序列化、内核缓冲、重连与 TCP 背压。它适合协议顺序课程，不是网络性能测试。

## 生命周期故障拥有不同状态

`src/minidist/sim/failure.py` 中的 `FailureInjector` 与 `SimNode` 展示通用 crash 模型。`FailureInjector.crash` 调用清空 `volatile` 映射的 `SimNode.on_crash`，再标记节点死亡并记录 `node_crashed`；`persistent` 映射保留。`restart` 调用 hook、恢复存活并记录反向转换。

协议实现可以增加拓扑行为。例如 `src/minidist/protocols/async_primary/group.py` 中的 `AsyncPrimaryGroup.crash` 清空协议节点的 volatile 状态，标记死亡，并双向 partition 所有相邻链路，防止已排队流量在 crash 后应用。

两种模型都没有模拟磁盘。“persistent”只表示被这个进程内教学模型保留，不说明 fsync、扇区撕裂、重启延迟或生产 Redis 会重新载入什么。

## Trace 是重放判据

`src/minidist/sim/trace.py` 中的 `Trace.record` 令 `sequence=len(self._events)`，再把 tick、kind 和结构化 details 存入不可变 `TraceEvent`。sequence 区分同一 tick 的多个事件；`events` 返回 tuple，阻止调用方追加内部列表；`Trace.as_dicts` 通过 `dataclasses.asdict` 生成便于相等比较的值快照。

这比断言“两次最终字典相同”更强。两次执行可能经历不同选举或消息路径后偶然收敛。比较每个 trace 项还会检查 message ID、逻辑 tick、超时选择、丢弃、投递与状态转换。

`tests/sim/test_simulation.py::test_same_seed_and_script_replay_event_for_event` 用 seed 1729 构造两个网络，并用 1730 构造第三个；前两个 trace 必须相等，第三个必须不同，还固定了丢弃和投递的 message ID。协议层的 `tests/protocols/test_raft.py::test_same_seed_replays_election_and_partition_heal_exactly` 重复选举、写、隔离、换主、愈合与收敛，再比较完整 trace。

## 动手实验 1：不 sleep 的调度

运行：

```bash
uv run python -c 'from minidist.sim import SimClock,Scheduler,Trace; c=SimClock(); t=Trace(); s=Scheduler(c,t); out=[]; s.schedule(2,lambda:out.append("later"),label="later"); s.schedule(1,lambda:out.append("first"),label="first"); print("start",c.now,s.pending,out); print("tick1",s.tick(),c.now,out); print("tick2",s.tick(),c.now,out); print([(e.sequence,e.tick,e.kind) for e in t.events])'
```

本仓实测：

```text
start 0 2 []
tick1 1 1 ['first']
tick2 1 2 ['first', 'later']
[(0, 0, 'event_scheduled'), (1, 0, 'event_scheduled'), (2, 1, 'event_started'), (3, 2, 'event_started')]
```

每行 `tick` 后第一个数字是执行的回调数量，不是新时间；下一个数字才是 `clock.now`。两条 schedule 记录都发生在 tick 0。

## 动手实验 2：重放带种子的网络选择

运行：

```bash
uv run python -c 'from minidist.sim import SimClock,Scheduler,SimNet,Trace
def run(seed):
 c=SimClock(); t=Trace(); s=Scheduler(c,t); n=SimNet(seed=seed,clock=c,scheduler=s,trace=t,min_delay=1,max_delay=3,drop_rate=.2,reorder_rate=.8); n.register("b",lambda m:None); [n.send("a","b",{"number":i}) for i in range(8)]; s.run_until_idle(); return t.as_dicts()
a=run(1729); b=run(1729); c=run(1730); print("same-seed",a==b); print("different-seed",a==c); print("dropped",[e["details"]["message_id"] for e in a if e["kind"]=="message_dropped"]); print("delivered",[e["details"]["message_id"] for e in a if e["kind"]=="message_delivered"]); print("ticks",[e["tick"] for e in a if e["kind"]=="message_delivered"])'
```

本仓实测：

```text
same-seed True
different-seed False
dropped [1, 2]
delivered [4, 5, 7, 0, 3, 6]
ticks [1, 3, 3, 4, 5, 6]
```

message 0 晚于 4、5、7 到达，展示带种子的乱序。改变 seed 会改变调度；保持 seed 会重现全部 trace 细节。

## 对照真实确定性仿真系统

生产级确定性仿真框架通常控制 task、timer、storage、RNG、进程重启，有时甚至控制运行时；它们在 CI 中探索大量 seed，并缩减失败历史。真实系统测试框架还可能在故障受控网络后运行真正进程或虚拟机。

MiniDist 只保留本课程需要的机制：

- `SimClock.advance` 提供显式逻辑时间；
- `Scheduler.schedule` 提供稳定事件顺序；
- `SimNet.send` 提供带种子的延迟、丢弃和乱序；
- 有向与双向链路 partition；
- 可见的 volatile/retained 生命周期状态；以及
- `Trace.record` 提供只追加结构化观察。

它不模拟 CPU 并发、真实 socket、重传、磁盘、时钟漂移、带宽、消息序列化或穷举调度。见[实验矩阵的确定性边界](../experiments.md#确定性边界)与 [README Determinism Boundary](https://github.com/system-in-miniature/mini-dist#determinism-boundary)。tick 绝不是延迟指标。

## 练习

### 理解题

1. `Scheduler` 已有 due tick，为什么还需要插入计数器？
2. `message_dropped` 的 `partition` 和 `partition_at_delivery` 各证明什么？

??? note "参考答案"

    1. 多个事件可能在同一 tick 到期；计数器提供显式稳定的 tie-breaker，而不依赖回调比较或宿主调度。
    2. `partition` 表示 `send` 时边已阻断；`partition_at_delivery` 表示消息已计划，但回调执行前链路被阻断。

### 动手题

3. 写一个临时 pytest，按 `"a"`、`"b"` 的顺序调度两个 delay-1 回调，断言二者在 tick 1 按插入顺序运行。

   **验收：**文件不得位于 `src/`；运行 `PYTHONPATH=src:. uv run pytest -q /tmp/test_minidist_ch02.py`，得到 `1 passed`。

??? note "参考答案"

    ```python
    from minidist.sim import SimClock, Scheduler, Trace

    def test_same_tick_uses_insertion_order():
        clock, trace = SimClock(), Trace()
        scheduler = Scheduler(clock, trace)
        seen = []
        scheduler.schedule(1, lambda: seen.append("a"), label="a")
        scheduler.schedule(1, lambda: seen.append("b"), label="b")
        assert scheduler.tick() == 2
        assert clock.now == 1
        assert seen == ["a", "b"]
    ```

4. 只把第二个回调的 delay 改成 2。在运行前写出两个 tick 后各自预期的 `seen`。

   **验收：**加入断言：tick 1 后为 `["a"]`，tick 2 后为 `["a", "b"]`；临时测试通过。

??? note "参考答案"

    tick 1 到期的回调先运行；另一个保留到第二次显式 tick。无需 sleep，宿主负载也无法改变顺序。

## 小结

MiniDist 把看似不确定的竞态变成显式输入：逻辑 tick、堆顺序、seed、有向 partition、生命周期转换和完整结构化 trace。一个 seed 不能证明所有可能执行，但能让一次执行稳定到足以调试和教学。第 3 章将用这个稳定实验室，通过同一词汇比较协议，同时拒绝假装它们的保证可以互换。
