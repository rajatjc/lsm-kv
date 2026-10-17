# lsm-kv

A log-structured merge-tree (LSM) key-value store in Go, built from scratch, with
an HTTP API and a live web UI that shows writes moving through the WAL, the
memtable, the SSTables, and compaction.

It's the same storage design used by Cassandra, RocksDB, LevelDB and ScyllaDB.

## Architecture

```
 PUT/DELETE ──► WAL (append, CRC32) ──► memtable (in memory)
                                             │  full?
                                             ▼
                                   SSTable #N (immutable, sorted)
                                   ├─ data entries
                                   ├─ sparse index (every 16 keys)
                                   ├─ Bloom filter (10 bits/key)
                                   └─ footer
                                             │  ≥ 4 tables?
                                             ▼
                                   compaction: k-way merge,
                                   newest version wins, tombstones dropped

 GET ──► memtable ──► SSTables newest→oldest
                      (Bloom filter skips tables that can't contain the key;
                       sparse index + binary search finds the block)
```

| File | What it does |
| --- | --- |
| [engine/wal.go](engine/wal.go) | Write-ahead log, CRC-checked records, torn-tail tolerant replay |
| [engine/memtable.go](engine/memtable.go) | In-memory write buffer |
| [engine/sstable.go](engine/sstable.go) | On-disk format: writer, reader, point lookup, iterator |
| [engine/bloom.go](engine/bloom.go) | Bloom filter with double hashing |
| [engine/db.go](engine/db.go) | Put/Get/Delete/Scan, flush, compaction, recovery, stats |
| [cmd/server/main.go](cmd/server/main.go) | HTTP API + serves the UI |
| [web/index.html](web/index.html) | Single-file UI, embedded into the binary |

### Durability

Every write is appended to the WAL before it's acknowledged. On startup, the
WAL is replayed into the memtable. A crash in the middle of an append leaves a
torn record, and its failed CRC check marks the end of the log. SSTables are
written to a `.tmp` file, fsynced, and renamed into place, so a crash never
leaves a half-written table.

`SyncWrites` controls whether the WAL is fsynced on every write:

- **Off (default):** writes survive a process crash, because they're in the OS
  page cache, but not a power loss.
- **On:** writes survive power loss, but each write costs about 1,600x more.

## Benchmarks

Apple M3 Pro, `go test ./engine -bench=.`

| Operation | Latency |
| --- | --- |
| Put, buffered WAL | 2.5 µs/op (~400K writes/s) |
| Put, fsync per write | 4.1 ms/op |
| Get, hit from SSTable | 1.4 µs/op |

Bloom filter: on lookups of missing keys, 98% of SSTable checks were skipped
without touching disk.

## Run locally

```sh
go test ./...              # unit tests: CRUD, flush, compaction, tombstones, WAL recovery
go run ./cmd/server        # http://localhost:8080
```

Environment variables: `PORT`, `DATA_DIR` (default `./data`), `SYNC_WRITES=true`,
`CORS_ORIGIN`, `MAX_DATA_MB` (default 100).

## API

| Method | Path | |
| --- | --- | --- |
| `PUT` | `/api/kv/{key}` | body = value (≤ 64 KB), key ≤ 256 bytes |
| `GET` | `/api/kv/{key}` | `404` if missing or deleted |
| `DELETE` | `/api/kv/{key}` | writes a tombstone |
| `GET` | `/api/scan?prefix=user:&limit=100` | sorted prefix scan |
| `GET` | `/api/stats` | memtable, WAL, SSTables, counters |
| `POST` | `/api/flush`, `/api/compact` | force a flush or compaction |
| `POST` | `/api/bulk?n=1000` | write n random keys (demo) |

```sh
curl -X PUT localhost:8080/api/kv/user:1 -d '{"name":"Ada"}'
curl localhost:8080/api/kv/user:1
```

## Deploy (backend + frontend together)

The UI is embedded into the Go binary, so one deployment serves both the API
and the frontend from the same origin, with no CORS setup. The database writes
to disk, so the host must provide a **persistent volume**. On hosts without one
(Vercel, or Render's free tier), all data is wiped on every restart.

### Fly.io (recommended)

```sh
brew install flyctl
fly auth signup                  # or: fly auth login
cd ~/PersonalProjects/lsm-kv
fly launch --no-deploy --copy-config   # keeps fly.toml; pick a unique app name if asked
fly volumes create lsm_data --region yyz --size 1
fly deploy
fly open                         # https://<app>.fly.dev
```

To redeploy, run `fly deploy`. Fly sends SIGTERM first, and the server closes
the database cleanly. Keep exactly **one** machine: this is a single-node
engine, and a volume can only attach to one machine.

### Alternatives

- **Railway:** connect the GitHub repo. It detects the Dockerfile. Add a volume
  mounted at `/data`.
- **Render:** create a Web Service from the repo using Docker, and attach a
  persistent disk at `/data`. Disks need a paid instance.

### Hosting the frontend separately (optional)

If you want the UI on GitHub Pages or Vercel and the API on Fly:

1. Copy `web/index.html` to the static host, and set
   `<meta name="api-base" content="https://<app>.fly.dev">`.
2. Run `fly secrets set CORS_ORIGIN=https://<you>.github.io`.

## Roadmap

These are ordered roughly by how much each one adds:

1. **Concurrent flush:** swap in an immutable memtable and flush it in a
   background goroutine, so writes don't stall during a flush.
2. **Background compaction:** move compaction off the write path.
3. **Leveled compaction:** L0 to Ln with non-overlapping key ranges, like
   LevelDB. Measure write amplification against the current size-tiered
   approach.
4. **Group commit:** batch concurrent writers into a single fsync. The target is
   to close most of the 1,600x gap between the two durability modes.
5. **Skiplist memtable:** sorted inserts, which make range scans cheap.
6. **Merge iterators:** stream `Scan` instead of materializing the result in a
   map.
7. **Block cache:** an LRU cache over SSTable blocks, plus reuse of read buffers
   (`Get` currently allocates 4.7 KB per op).
8. **Manifest file:** an atomic record of which tables are live, instead of
   globbing `*.sst`.
9. **Crash-test harness:** kill the process at random points and verify that no
   acknowledged write is lost.
10. **YCSB-style benchmarks:** compare against Pebble or BadgerDB.
