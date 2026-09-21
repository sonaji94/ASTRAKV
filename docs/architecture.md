# AstraKV Architecture

## Target system

```text
                    Client
                      │
                ┌─────▼─────┐
                │ API / SDK  │
                └─────┬─────┘
                      │
              ┌───────▼────────┐
              │ Query / Router │
              └───────┬────────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Shard 0     Shard 1     Shard 2
       ┌─────┐     ┌─────┐     ┌─────┐
       │ R0  │     │ R0  │     │ R0  │
       │ R1  │     │ R1  │     │ R1  │
       │ R2  │     │ R2  │     │ R2  │
       └─────┘     └─────┘     └─────┘
          │           │           │
       Raft        Raft        Raft
       Group       Group       Group
```

Each physical node runs two layers on top of one storage engine:

```text
                    Node
                     │
          ┌──────────┴──────────┐
          │                     │
      Raft Layer            Storage Layer
          │                     │
   Leader Election              WAL
   Log Replication              │
   Commit Index              MemTable
          │                     │
          └──────────┬──────────┘
                     │
                  LSM Tree
                     │
                  SSTables
```

## Components

### Storage layer (Phases 2–8)
- **WAL** — append-only, fsync'ed log. Source of durability. Replayed on startup.
- **MemTable** — in-memory ordered structure (sorted map / skip list). Buffers writes.
- **SSTable** — immutable, sorted, on-disk files with sparse index, bloom filter, checksums.
- **LSM tree** — MemTable rotation, flush to L0, leveled compaction, tombstones.

Write path: `Client → WAL → MemTable → (flush) → SSTable → (compaction) → larger SSTables`

Read path: `MemTable → immutable MemTables → SSTable indices (skipped by Bloom filter)`

### Distributed layer (Phases 9–15)
- **Raft** — leader election, log replication, commit index, persistent state, catch-up.
- **Sharding** — consistent hashing / shard map; each shard is its own Raft group.
- **Router** — maps key → shard → owning Raft group → leader.
- **Consistency** — explicit, documented guarantees proven by tests (not marketing).

### Transactions (Phase 16)
- Atomic single-shard ops first, then versioning, conflict detection, optimistic concurrency.
- Cross-shard transactions only after the single-shard path is proven.

## Design principles

1. **Understand before code.** Each subsystem is designed and explained before implementation.
2. **Test aggressively.** Unit, integration, property, and chaos tests.
3. **Prove guarantees.** Consistency claims must be demonstrated, not asserted.
4. **Benchmark everything.** Baseline single-node numbers before distribution.

## Failure story

The centerpiece demo:

```text
Node 1 = LEADER, Node 2/3 = FOLLOWER
        💀 KILL NODE 1
        → election → Node 2 becomes LEADER
        → cluster continues writes
        → Node 1 restarts → log catch-up → rejoins
        → consistency verification → 100% expected records
```

## See also

- `docs/raft.md`, `docs/storage.md`, `docs/consistency.md` (added as built)