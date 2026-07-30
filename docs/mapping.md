> **Language**: English | [简体中文](zh/mapping.md)

# Mapping Protocols to Real-System Replication Mechanisms

Label meanings:

- **Equivalent**: preserves the key state and fault semantics relevant to the
  experiment.
- **Intentionally simplified**: follows the same direction but removes
  engineering details irrelevant to the current course question.
- **Semantically opposite**: the name or unified interface may imply a stronger
  guarantee, and the implementation explicitly presents a counterexample.

| MiniDist mechanism | Real Redis counterpart | Classification | Boundary notes |
|---|---|---|---|
| The primary immediately acks after writing local state; replicas do not participate in the write decision | Redis uses asynchronous replication by default; the master does not wait for replicas before returning from an ordinary write request | **Equivalent** | This is precisely why an acknowledged write can be lost after failover in experiment 2. |
| Replicas only consume the primary's ordered write stream | A Redis replica receives and applies the master's replication stream | **Equivalent** | A replica is not a quorum voter and does not contribute its own acknowledgement to the client ack. |
| Replication ID + monotonic offset | Redis replication ID and replication offset | **Equivalent** | Together they identify a replication position in one history; comparing only the numeric offset is insufficient. |
| Partial resynchronization within a bounded backlog | Redis `PSYNC`/partial resynchronization and replication backlog | **Equivalent** | A lagging replica requests the missing portion from the backlog without replacing its existing complete state. |
| Replace the entire dict when beyond the backlog or when the replication ID differs | Redis full resynchronization, which transfers an RDB and then continues with the command stream | **Intentionally simplified** | The snapshot atomically replaces state in one simulation event; fork, RDB bytes, socket buffers, and loading time are not simulated. |
| The backlog stores structured SET entries | The Redis backlog stores the replication protocol's byte stream | **Intentionally simplified** | MiniDist focuses on the window and offset; it does not teach RESP encoding or byte boundaries. |
| `promote` is an explicit public method | `REPLICAOF NO ONE`, or a role transition triggered by the Sentinel/Cluster control plane | **Intentionally simplified** | Fault detection, voting, Sentinel configuration epochs, and automatic leader election are omitted so that the data plane can be observed in isolation. |
| A new primary receives a new replication ID and clears the old backlog | Redis promotion creates a new replication history and retains a secondary replication ID to assist partial resynchronization | **Intentionally simplified** | MiniDist retains only one current ID, so an old node performs a full resynchronization after a primary change. |
| Every replication delivery carries its source primary, generation, and replication ID and is accepted only for the current lineage | Redis validates replication lineage with replication IDs, while Sentinel/Cluster control planes fence stale ownership with configuration epochs | **Intentionally simplified** | MiniDist's generation is an in-process manual-promotion epoch, not a complete Sentinel or Cluster configuration-epoch protocol. It prevents queued full-sync and backlog deliveries from an old primary from crossing a promotion. |
| Promotion immediately starts synchronization for every alive replica | Redis replicas are redirected or reconnect to the selected master and synchronize against its history | **Intentionally simplified** | There is no discovery or automatic failover control plane; explicit `promote` owns the topology change and eagerly initiates convergence. A partitioned delivery is dropped and `heal` retries synchronization. |
| A crash clears volatile control state while preserving the educational node's dataset | A Redis process's dataset after restart depends on RDB/AOF and persistence configuration | **Intentionally simplified** | The dataset is treated as persistent here to isolate the variable “replication has not yet reached the new primary”; this is not a power-loss model for Redis without persistence. |
| `AckLevel.LEADER` means the primary has applied the write to its in-memory dict | The unified interface draft describes `LEADER` as the leader having persisted to disk; success of an ordinary Redis write does not imply fsync | **Semantically opposite** | For protocol 1, this level never represents disk durability. Both the returned message and experiment 2 deliberately expose that fact. |
| Reject `AckLevel.QUORUM` / `ALL_ISR` | Redis `WAIT` can wait for replica acks, but it still does not turn Redis into a strongly consistent quorum log | **Intentionally simplified** | The current protocol models only the default asynchronous write path; it does not quietly treat `WAIT` as a Raft commit. |
| Reject `ReadLevel.LINEARIZABLE` | Redis replica reads may be stale by default, and failover provides no read-index-style proof | **Equivalent** | `LOCAL` may read an old value; `LEADER` only routes and makes no claim of consensus linearizability. |

## DIFFERENCES List

This milestone does not implement Redis Sentinel, Cluster, `WAIT`, RDB/AOF
policies, disk fsync loss windows, or real RESP transport. Their support cannot
be inferred from the current tests passing. The provable conclusions for
protocol 1 are limited to asynchronous acks, the replica sink,
offset/backlog-based resynchronization, fenced full replacement, and manual
primary change. MiniDist therefore does not claim the original M1/M2 plan as
complete: disk-fsync loss-window injection remains a planned M3 capability.

# Mapping Protocol 4 to the Raft Paper

“The Raft paper” here refers to Diego Ongaro and John Ousterhout's
*In Search of an Understandable Consensus Algorithm*. The implementation is
organized around the state and RPC rules in Figure 2 rather than disguising
shared-memory state updates as RPCs.

| MiniDist Raft mechanism | Paper section | Classification | Correspondence and experiment boundary |
|---|---|---|---|
| follower / candidate / leader and monotonically increasing terms | §5, §5.1 | **Equivalent** | On receiving a higher-term RequestVote, AppendEntries, or response, the node updates its persistent term and steps down; this fences the old leader in experiment 3. |
| Randomized election timeout, candidate self-vote, and majority election through RequestVote | §5.2, §5.6 | **Equivalent** | The timeout is drawn from the private random source associated with the constructor seed; neither wall-clock time nor module-level random state is read. |
| A candidate's log must be at least as up-to-date as the voter's log | §5.4.1 | **Equivalent** | The `(lastLogTerm, lastLogIndex)` pair is compared lexicographically, preventing election of a leader that lacks committed log entries. |
| AppendEntries heartbeats and log replication | §5.2, §5.3 | **Equivalent** | Every RPC goes through `SimNet`; a heartbeat is AppendEntries with empty entries. |
| `prevLogIndex` / `prevLogTerm` consistency checks, conflicting-suffix deletion, and `nextIndex` fallback and retry | §5.3, Figure 2 | **Equivalent** | The follower returns the start of the conflicting term, and the leader retransmits from that point. When experiment 3 heals, this overwrites the uncommitted fork of the old minority leader. |
| Majority replication advances commitIndex | §5.3 | **Equivalent** | An entry can be committed only when `matchIndex` covers a majority; QUORUM is the only supported write ack. |
| Advance commitIndex by replica count only for “entries from the current term” | §5.4.2 | **Equivalent** | The code's why-comment directly cites the counterexample in this section: an old-term entry cannot be committed directly merely because it is currently stored on a majority; it is committed indirectly as part of the committed prefix of a current-term entry. |
| The leader appends a no-op after election | §8 | **Intentionally simplified** | The no-op provides a committable point in the current term and safely commits earlier prefixes after recovery or a primary change; it does not alter the KV state. Client-session handling from §8 is omitted. |
| `currentTerm`, `votedFor`, and the log are persistent; other state is volatile | §5 / Figure 2 | **Intentionally simplified** | A crash preserves these three fields in an in-process durable mapping; restart rebuilds the node as a follower with commitIndex=0 and an empty state machine, then replays the committed prefix from leaderCommit. No disk/fsync behavior is claimed. |
| Direct `LEADER` read | Client-interaction background in §8 | **Semantically opposite** | This only routes to the currently known leader with the highest term and does not prove that the leader still controls a majority. It deliberately does not provide the linearizable-read guarantee a caller might infer from “leader read.” |
| Simplified read-index barrier for `LINEARIZABLE` reads | Read-only request rules in §8 | **Intentionally simplified** | The leader sends empty AppendEntries in the current term and reads the applied state machine after receiving successful responses from a majority; the read command is not written to the log, and lease/batching/apply-wait machinery is omitted. |

## Differences Between This Implementation and Textbook Raft

- **Intentionally simplified: a static cluster of three or more nodes.** Joint
  consensus membership changes from §6 are not implemented; membership remains
  fixed after `RaftGroup` construction.
- **Intentionally simplified: no snapshot / compaction.** Paper §7 is not
  implemented; the log is unbounded, and a lagging follower always catches up
  through `nextIndex` fallback and replay.
- **Intentionally simplified: the conflict optimization returns only a conflict
  index.** Textbook Raft permits decrementing one entry at a time; this
  implementation returns the first position of the conflicting term to avoid
  repeated ticks, but does not implement the complete conflict-term jump
  optimization.
- **Intentionally simplified: synchronous educational client.** `client_write`
  and linearizable reads drive logical ticks within the call until a majority
  succeeds or a deterministic timeout occurs; there are no futures, retry IDs,
  sessions, or duplicate-request table from paper §8.
- **Intentionally simplified: read-index is a single-round heartbeat barrier.**
  It verifies that a majority is reachable in the current term, but does not
  implement lease reads, batched read-index, or cross-thread apply waits.
- **Equivalent semantics without disk simulation: persistent fields use an
  in-process durable mapping.** Crash/restart semantics match Figure 2, but
  there is no fsync latency, torn write, or real file.
- **Educational observability extensions.** `probe()`, `isolate()`, `heal()`,
  `run_until_leader()`, and `run_until_converged()` are experiment harness APIs,
  not parts of the Raft wire protocol.

This implementation does not support `AckLevel.NONE`, `LEADER`, `ALL_ISR`, or
`ReadLevel.LOCAL`; these combinations explicitly raise
`UnsupportedLevelError` instead of silently weakening the guarantee.
