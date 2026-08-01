# MiniDist Polished Learning Surfaces Design

## Objective

MiniDist already has a bilingual mechanism tutorial and a verified reference
implementation. It becomes polished by adding the same three learning modes as
MiniS3 without pretending that its sparse Git history is already a course:

1. **Mechanism Tutorial** — the existing ten topic-oriented chapters.
2. **Self-Guided Rebuild** — ten bilingual browser-native Stages backed by an
   exact cumulative patch chain.
3. **Agent-Guided Rebuild** — direct `开始 Agent 带教 Stage NN` routing from the
   canonical repository, with no learner branch switching.

## Why ten Stages

The repository has only three source-bearing historical boundaries. The root
implementation commit combines the simulator, asynchronous replication, Raft,
and three experiments; the M3 commit combines WAL shipping, ISR, shared failure
semantics, and four more experiments. Rendering those commits directly would
produce two oversized lessons that hide causality.

The Journey therefore reconstructs the historical snapshots by coherent file
ownership, while preserving the two real cross-version fix deltas. Stage count
is content-sized; it is not copied from MiniS3 or MiniRedis.

## Stage map

| Stage | Boundary | Mechanism increment |
|---:|---|---|
| 01 | `86c9886` file slice | Deterministic clock, trace, scheduler, network, failures, and project scaffold |
| 02 | `86c9886` file slice | Shared acknowledgement/read vocabulary and replication-group contract |
| 03 | `86c9886` file slice | Asynchronous primary/replica replication, backlog, restart, and failover |
| 04 | `86c9886` file slice | Seeded Raft election, log replication, quorum commit, and read barrier |
| 05 | `86c9886` file slice | Experiments 1–3 across delayed replication, acknowledged loss, and partitioned leaders |
| 06 | `86c9886..3826aa7` | Generation/lineage fencing and the dual-protocol partition experiment |
| 07 | `3826aa7..HEAD` selected files | fsync-loss simulation plus M3 hooks and the election-timeout liveness bound |
| 08 | current file slice | PostgreSQL-style WAL shipping, synchronous standby acknowledgements, and promotion |
| 09 | current file slice | Kafka-style ISR, high watermark, min.insync, and catch-up modes |
| 10 | current file slice | Experiments 4–7 and final cross-protocol comparison parity |

## Canonical artifacts

Each `journey/stages/NN-slug/` contains:

- `goal.md` — bilingual authored lesson facts;
- `stage.patch` — exact delta from Stage N-1;
- `tests.txt` — cumulative focused evidence;
- `layout.toml` — test-file ownership plus grouped mechanism/supporting blocks.

The checked-in patches are self-contained. `journey/manifest.toml` records the
source commit and owned file slice used to regenerate each boundary.

## Teaching order

Every localized browser page follows this order:

1. current problem;
2. **Test contract**, containing the nested failure preview, collapsed test
   diffs, counterexample construction, critical assertion, and failure meaning;
3. basic concepts and necessity;
4. runtime mental model;
5. grouped mechanism blocks with implementation diffs and key statements;
6. verification evidence, durable takeaways, and learner explanation;
7. relevant tutorial chapter.

Tests never appear inside mechanism blocks. Multiple files that implement one
causal boundary share one block title and may share one explanation. Routine
package markers, lockfiles, and configuration stay in one supporting drawer.

Tests are executable motivation and completion evidence, not a mandatory
test-first teaching narrative.

## Patch and parity boundary

The Journey owns:

- `src/minidist/**`;
- `labs/**`;
- simulation, protocol, lab, and public-type tests;
- `pyproject.toml`, `uv.lock`, and `uv.toml`.

It excludes the tutorial, website assets, and `tests/test_docs_homepage.py`.
After Stage 10, every owned byte must match the current reference tree.

## Agent contract

The root `AGENTS.md` routes the explicit Stage request, prepares or resumes a
Stage-specific internal learner repository, gives a short misconception screen,
teaches concepts before implementation, uses tests as visible evidence, walks
the authored mechanism blocks, and verifies both tests and canonical parity.
The web Agent pages remain short usage guides only.

## Acceptance gates

MiniDist is polished only when all are freshly green:

- full repository tests and compileall;
- all ten cumulative Stage checks;
- final owned-tree byte parity;
- bilingual renderer coverage with no unowned or duplicated diff;
- strict MkDocs build;
- real browser checks for Stages 01, 03, 04, 06, 08, 09, and 10 in both
  languages, collapsed drawers, test-before-concept order, language preservation,
  and Agent routing.

