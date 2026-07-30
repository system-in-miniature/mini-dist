# M3 Protocol Spectrum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete MiniDist M3 with WAL shipping, ISR/HW/controller replication, an explicit acknowledged-but-unfsynced crash fault, experiments 4–7, and bilingual mapping/milestone documentation.

**Architecture:** Both new groups implement the existing `ReplicationGroup` surface over `SimClock`, `Scheduler`, `SimNet`, and `Trace`, while exposing protocol-specific immutable probe state. Persistence tracks a live accepted position separately from its fsynced position; only the explicit fsync-loss crash injection rolls live state back. Labs drive public protocol APIs and return assertion-friendly dataclasses.

**Tech Stack:** Python 3.12, dataclasses/enums, MiniDist deterministic simulator, pytest, uv, Markdown.

---

### Task 1: Persistent boundary and protocol contracts

**Files:**
- Modify: `src/minidist/sim/failure.py`
- Modify: `src/minidist/protocols/async_primary/group.py`
- Modify: `src/minidist/protocols/raft/group.py`
- Test: `tests/sim/test_simulation.py`
- Test: `tests/protocols/test_async_primary.py`
- Test: `tests/protocols/test_raft.py`

- [ ] Add failing tests proving `FailureInjector.crash(..., lose_unfsynced=True)` removes staged acknowledged state but keeps fsynced state, and that all existing protocol crash paths expose an explicit fsync-loss boundary.
- [ ] Run the focused tests and confirm failures are caused by the missing API/state distinction.
- [ ] Add staged persistent state, `fsync`, and explicit loss-on-crash handling with trace events; preserve ordinary crash behavior.
- [ ] Run the focused tests until green.

### Task 2: Protocol 2, PostgreSQL-style WAL shipping

**Files:**
- Create: `src/minidist/protocols/wal_shipping/__init__.py`
- Create: `src/minidist/protocols/wal_shipping/group.py`
- Test: `tests/protocols/test_wal_shipping.py`

- [ ] Add failing tests for byte WAL ordering, LEADER asynchronous acknowledgement, QUORUM acknowledgement by every configured synchronous standby, timeline promotion fencing, incremental/full catch-up, read levels, fsync rollback, and same-seed trace replay.
- [ ] Run the new test module and confirm import/API failures.
- [ ] Implement `WalShippingGroup`, immutable probe records, public lifecycle/catch-up APIs, and why-comments around WAL apply and timeline rejection.
- [ ] Run the new test module until green.

### Task 3: Protocol 3, Kafka-style ISR/HW/controller

**Files:**
- Create: `src/minidist/protocols/isr/__init__.py`
- Create: `src/minidist/protocols/isr/group.py`
- Test: `tests/protocols/test_isr.py`

- [ ] Add failing tests for ISR shrink/rejoin, HW advancement, min.insync rejection, ALL_ISR acknowledgement, controller election, epoch fencing, incremental/full catch-up, read levels, fsync rollback, and same-seed trace replay.
- [ ] Run the new test module and confirm import/API failures.
- [ ] Implement `IsrGroup`, the in-process controller, immutable probe records, public slow/isolate/heal/elect APIs, and why-comments around ISR/HW/epoch transitions.
- [ ] Run the new test module until green.

### Task 4: Experiments 4–7

**Files:**
- Create: `labs/exp04_slow_replica.py`
- Create: `labs/exp05_replica_reconnect.py`
- Create: `labs/exp06_read_consistency.py`
- Create: `labs/exp07_split_brain_lease.py`
- Modify: `tests/labs/test_experiments.py`

- [ ] Add failing pytest assertions for the four protocol columns: ISR shrink versus Raft majority availability; incremental versus full reconnect; every ReadLevel outcome/staleness cell; split-brain local reads versus authority-checked reads/leases.
- [ ] Run the labs test module and confirm failures are due to missing scripts/behaviors.
- [ ] Implement public-API-only lab runners with deterministic seeds and concise Chinese stdout.
- [ ] Run the labs tests until green and execute each script to capture its output.

### Task 5: Bilingual M3 documentation

**Files:**
- Modify: `docs/experiments.md`
- Modify: `docs/zh/experiments.md`
- Modify: `docs/mapping.md`
- Modify: `docs/zh/mapping.md`
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `docs/tutorial/08-experiments.md`
- Modify: `docs/zh/tutorial/08-experiments.md`

- [ ] Replace all protocol 2/3 and experiment 4–7 placeholders with asserted, four-column conclusions and runnable commands.
- [ ] Add protocol 2/3 three-tier mapping sections and update fsync limitations precisely.
- [ ] Mark M3 complete in both READMEs and update layout/lab coverage.
- [ ] Append only the requested one-sentence protocol 2/3 pointer to each tutorial chapter ending.
- [ ] Search for stale M3/not-implemented claims in the changed non-tutorial documents.

### Task 6: Final verification and report evidence

**Files:**
- Verify all changed files.

- [ ] Run `uv run pytest -q` and record the exact pass count.
- [ ] Run experiments 4–7 and record their concise output summaries.
- [ ] Run `git diff --check`, inspect `git diff --stat`/`git status --short`, and confirm no commit was created.
- [ ] Count source lines under `src/minidist/protocols/wal_shipping/` and `src/minidist/protocols/isr/`, excluding blank lines only if the report labels the counting rule.
