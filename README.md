# AstraKV

**AstraKV** is a production-inspired, fault-tolerant distributed key-value database built in Go.

It combines a log-structured storage engine (WAL + MemTable + SSTables + LSM compaction) with
Raft consensus, multi-shard routing, replication, and explicit consistency guarantees — built
one layer at a time so every subsystem is understood, tested, and reproducible.

## Status: Milestone 1 — Foundation

Project foundation stage. Empty buildable skeleton; database engine not started yet.

| Milestone | Status |
|---|---|
| Architecture | ⬜ |
| Go foundation | ⬜ |
| Basic KV store | ⬜ |
| WAL | ⬜ |
| MemTable | ⬜ |
| SSTable | ⬜ |
| Index / Bloom filter | ⬜ |
| LSM tree / compaction | ⬜ |
| Benchmarks | ⬜ |
| Networking (gRPC) | ⬜ |
| Multi-node cluster | ⬜ |
| Raft / leader election / replication | ⬜ |
| Chaos testing | ⬜ |
| Sharding | ⬜ |
| Consistency | ⬜ |
| Transactions | ⬜ |
| Observability | ⬜ |
| Docker / CI/CD | ⬜ |
| Final benchmark + docs | ⬜ |

## Repository layout

```text
astrakv/
├── cmd/
│   ├── server/       # starts a node
│   ├── client/       # CLI client
│   └── benchmark/    # benchmark harness
├── raft/             # consensus: elections, replication, log
├── storage/
│   ├── wal/          # write-ahead log (durability)
│   ├── memtable/     # in-memory sorted buffer
│   ├── sstable/      # immutable sorted files
│   ├── lsm/          # LSM tree orchestration
│   └── compaction/   # level compaction
├── shard/            # shard map, router, manager
├── transaction/      # concurrency control
├── replication/
├── network/          # gRPC + protobuf
├── recovery/         # crash recovery
├── chaos/            # fault injection
├── benchmark/
├── tests/
├── docs/             # design & architecture docs
└── go.mod
```

## Getting started

Prerequisites: Go 1.27+, Git.

```bash
go build ./...
go run ./cmd/server
```

## License

Educational project. Inspired by Spanner/Bigtable/RocksDB design principles.