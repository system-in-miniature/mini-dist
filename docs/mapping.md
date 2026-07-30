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

## Protocol 1 DIFFERENCES List

This milestone does not implement Redis Sentinel, Cluster, `WAIT`, RDB/AOF
policies, or real RESP transport. The M3 persistence boundary is an in-process
fsync image, not those production persistence modes.

# Mapping Protocol 2 to PostgreSQL WAL Shipping

| MiniDist WAL mechanism | PostgreSQL counterpart | Classification | Boundary notes |
|---|---|---|---|
| Ordered logical SET bytes with monotonic LSNs | Ordered WAL bytes identified by LSN | **Intentionally simplified** | Real PostgreSQL WAL is physical; logical SET keeps the shared KV state machine while preserving byte transport and order. |
| `LEADER` flushes primary WAL and returns before asynchronous standby apply | Local commit with asynchronous streaming replication | **Equivalent** | A promoted async standby can lack the acknowledged suffix. |
| `QUORUM` waits for every configured synchronous standby to flush | `synchronous_commit` plus configured synchronous standbys | **Intentionally simplified** | Here `QUORUM` means all configured sync standbys, not a numeric majority or the complete `ANY/FIRST` grammar. |
| Standbys decode/apply strictly in LSN order | WAL receiver/replay follows one ordered stream | **Equivalent** | A gap triggers catch-up instead of applying over an unknown prefix. |
| Retained WAL enables incremental catch-up; an older position triggers a base backup | WAL retention/archive slots versus a new base backup | **Equivalent** | Archive retrieval and transfer cost are omitted; snapshot replacement is one event. |
| Promotion increments timeline and rejects queued old-timeline WAL | Timeline history protects recovery branches after promotion | **Equivalent** | History files and common-ancestor search are simplified to an integer branch fence. |
| `LOCAL` may be stale; `LINEARIZABLE` is rejected | Hot-standby replay can lag; synchronous commit is not a read lease | **Equivalent** | Write durability is not silently presented as read authority. |

## Protocol 2 DIFFERENCES List

- **Intentionally simplified:** logical KV WAL, fixed synchronous standby tuple,
  atomic base backup, and no transactions/checkpoints/archive restore.
- **Equivalent branch safety:** old-timeline records are rejected before apply.
- **Semantically opposite if generalized:** `QUORUM` here is all configured
  synchronous standbys, not a Raft majority.

# Mapping Protocol 3 to Kafka ISR Replication

| MiniDist ISR mechanism | Kafka counterpart | Classification | Boundary notes |
|---|---|---|---|
| In-process controller owns leader, leader epoch, and ISR | Kafka controller/metadata quorum decides partition leadership and ISR | **Intentionally simplified** | Controller availability and metadata replication are outside this data-plane model. |
| Lag timeout removes a follower; reaching HW permits rejoin | ISR shrink/expand from replica lag and catch-up | **Equivalent** | Logical ticks replace wall-clock lag/fetch timing. |
| HW is the minimum log-end offset in current ISR | Kafka partition high watermark | **Equivalent** | Committed reads expose only entries at or below HW. |
| `ALL_ISR` waits for current ISR and enforces `min.insync.replicas` | Producer `acks=all` with `min.insync.replicas` | **Equivalent** | Dynamic ISR is not a Raft majority. |
| `LEADER` returns without waiting for ISR/HW | Kafka `acks=1` | **Equivalent** | A leader read may expose data above HW. |
| Controller election increments leader epoch; old-epoch packets are rejected | Leader-epoch fencing and log truncation | **Equivalent** | Producer and transactional epochs are separate and omitted. |
| Retained log uses incremental catch-up; outside retention uses a full snapshot | Segment retention and replica bootstrap | **Intentionally simplified** | Kafka transfers log segments rather than atomically replacing a KV snapshot. |
| Logical leader lease gates the authority-read lab | Controller ownership plus a pedagogical lease | **Intentionally simplified** | Kafka does not expose MiniDist's `lease_read`; it isolates the stale-old-leader mechanism. |

## Protocol 3 DIFFERENCES List

- **Intentionally simplified:** always-available in-process controller, one
  record per write, and no batches, idempotence, transactions, or segment files.
- **Equivalent:** ISR shrink/rejoin, HW, min.insync, ALL_ISR, and leader-epoch
  rejection preserve their fault semantics.
- **Semantically opposite if generalized:** `ALL_ISR` is dynamic and may cover
  fewer nodes than a majority of the configured assignment.

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
| `currentTerm`, `votedFor`, and the log are persistent; other state is volatile | §5 / Figure 2 | **Intentionally simplified** | A crash preserves these fields at the simulated stable-storage boundary; restart rebuilds volatile state and replays the committed prefix from leaderCommit. |
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
- **Equivalent stable-storage ordering without files:** persistent fields reach
  the simulated fsync boundary before dependent Raft responses. There is no
  fsync latency, torn write, checksum, or real file.
- **Educational observability extensions.** `probe()`, `isolate()`, `heal()`,
  `run_until_leader()`, and `run_until_converged()` are experiment harness APIs,
  not parts of the Raft wire protocol.

This implementation does not support `AckLevel.NONE`, `LEADER`, or `ALL_ISR`.
`ReadLevel.LOCAL` exists specifically for stale-read experiments and makes no
authority claim; `LINEARIZABLE` remains the read-index-protected path.
