# lsm-kv

A log-structured merge-tree (LSM) key-value store in Java, built from scratch,
with an HTTP API and a live web UI that shows writes moving through the WAL, the
memtable, the SSTables, and compaction.

It's the storage design used by Cassandra (itself written in Java), RocksDB,
LevelDB and ScyllaDB. The next milestone is replicating it across nodes with
Raft; see the [roadmap](#roadmap).

## Architecture

```
 PUT/DELETE ──► WAL (append, CRC32) ──► memtable (sorted, in memory)
                                             │  full?
                                             ▼
                                   SSTable #N (immutable, sorted, memory-mapped)
                                   ├─ data entries
                                   ├─ sparse index (every 16 keys)
                                   ├─ Bloom filter (10 bits/key)
                                   └─ footer
                                             │  ≥ 4 tables?
                                             ▼
                                   compaction: streaming k-way merge,
                                   newest version wins, tombstones dropped

 GET ──► memtable ──► SSTables newest→oldest
                      (Bloom filter skips tables that can't contain the key;
                       sparse index + binary search finds the block)
```

All storage code is in [src/main/java/io/github/rajatjc/lsmkv/storage/](src/main/java/io/github/rajatjc/lsmkv/storage/):

| File | What it does |
| --- | --- |
| [Wal.java](src/main/java/io/github/rajatjc/lsmkv/storage/Wal.java) | Write-ahead log with CRC-checked records; replay stops cleanly at a torn tail |
| [Memtable.java](src/main/java/io/github/rajatjc/lsmkv/storage/Memtable.java) | Sorted write buffer on `ConcurrentSkipListMap` |
| [SSTable.java](src/main/java/io/github/rajatjc/lsmkv/storage/SSTable.java) | On-disk format: writer, memory-mapped reader, point lookup, seekable iterator |
| [BloomFilter.java](src/main/java/io/github/rajatjc/lsmkv/storage/BloomFilter.java) | Bloom filter with double hashing over FNV-1a |
| [MergeIterator.java](src/main/java/io/github/rajatjc/lsmkv/storage/MergeIterator.java) | Heap-based k-way merge where the newest version wins; used by scans and compaction |
| [LsmStore.java](src/main/java/io/github/rajatjc/lsmkv/storage/LsmStore.java) | Put/Get/Delete/Scan, flush, compaction, recovery, stats |

The HTTP server is [KvServer.java](src/main/java/io/github/rajatjc/lsmkv/server/KvServer.java),
and the UI is a single file, [index.html](src/main/resources/public/index.html).

### Design notes

- **No runtime dependencies.** The server is the JDK's built-in `HttpServer`
  with one virtual thread per request, and JSON output is hand-written. The jar
  is about 40 KB.
- **Sorted memtable.** A `ConcurrentSkipListMap` keeps keys ordered, so a flush
  streams straight to disk and scans need no sort step.
- **Memory-mapped SSTables.** Reads use absolute positions on a shared mapping,
  so concurrent readers need no system calls and no coordination. Keys are
  compared in place, without copying them out.
- **Real range scans.** Each SSTable iterator seeks to the prefix with the
  sparse index, a heap merges the memtable and the tables, and the scan stops at
  the end of the prefix range instead of reading whole tables.
- **Streaming compaction.** The merge iterator feeds the SSTable writer
  directly, so compaction never holds the whole dataset in memory.
- **Concurrency.** A read-write lock lets gets and scans run in parallel. Writes,
  flushes and compactions are exclusive. Moving flush and compaction to the
  background is on the roadmap.

### Durability

Every write is appended to the WAL before it's acknowledged, and the WAL is
replayed into the memtable on startup. A crash in the middle of an append leaves
a torn record. Replay treats a failed CRC check, or a length that runs past the
end of the file, as the end of the log, so a corrupt header can't trigger a huge
allocation.

SSTables are written to a `.tmp` file, fsynced, renamed into place, and then the
directory is fsynced. That makes the new table durable before the WAL or old
tables are deleted. Temp files left by an interrupted write are removed on
startup. Compaction deletes old tables oldest first, so a crash partway through
can't bring a deleted key back.

`SyncWrites` controls whether the WAL is fsynced on every write:

- **Off (default):** writes survive a process crash, because they're in the OS
  page cache, but not a power loss.
- **On:** writes survive power loss, but each write costs about 1,800x more.

The on-disk format is unchanged from the earlier Go version of this project, and
this engine opens data directories the Go version wrote.

## Benchmarks

JMH 1.37 on an Apple M3 Pro, IBM Semeru 21 (OpenJ9 VM), via `./gradlew jmh`:

| Benchmark | Latency |
| --- | --- |
| Put, buffered WAL | 3.0 µs/op |
| Put, fsync per write | ~5.4 ms/op |
| Get, hit from SSTable | 2.4 µs/op |
| Get, miss (answered by Bloom filters) | 0.14 µs/op |

A miss is about 17x cheaper than a hit, because the Bloom filters answer without
reading SSTable data.

## Run locally

Gradle needs JDK 17+ to start. It builds and runs the project on JDK 21, and
downloads that JDK if it isn't installed.

```sh
./gradlew test    # JUnit: CRUD, flush, compaction, tombstones, scans, WAL recovery, torn WAL tail
./gradlew run     # http://localhost:8080
./gradlew jmh     # benchmarks (filter with --args=ReadBenchmark)
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

The UI ships inside the jar, so one deployment serves both the API and the
frontend from the same origin, with no CORS setup. The database writes to disk,
so the host must provide a **persistent volume**. On hosts without one (Vercel,
or Render's free tier), all data is wiped on every restart.

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

[fly.toml](fly.toml) asks for a 512 MB VM, because the JVM needs more headroom
than a 256 MB one gives. Fly sends SIGTERM on redeploy, and a shutdown hook
closes the store cleanly. Keep exactly **one** machine: this is a single-node
engine for now, and a volume can only attach to one machine.

### Alternatives

- **Railway:** connect the GitHub repo. It detects the Dockerfile. Add a volume
  mounted at `/data`.
- **Render:** create a Web Service from the repo using Docker, and attach a
  persistent disk at `/data`. Disks need a paid instance.

### Hosting the frontend separately (optional)

If you want the UI on GitHub Pages or Vercel and the API on Fly:

1. Copy `src/main/resources/public/index.html` to the static host, and set
   `<meta name="api-base" content="https://<app>.fly.dev">`.
2. Run `fly secrets set CORS_ORIGIN=https://<you>.github.io`.

## Roadmap

### Distributed: Raft replication

1. ~~**Single-node LSM engine**~~ (done)
2. **Leader election:** RequestVote RPCs, randomized election timeouts, terms,
   and persisted `currentTerm` and `votedFor`.
3. **Log replication:** AppendEntries, commit index, and applying committed
   commands to the LSM store as the state machine.
4. **Snapshots:** compact the Raft log by snapshotting the store's SSTables, and
   catch up lagging followers with InstallSnapshot.
5. **Client semantics:** redirect clients to the leader, linearizable reads with
   ReadIndex, and safe retries through client request IDs.
6. **Fault injection:** kill nodes, partition the network, and drop or delay
   messages. Record operation histories and check them for linearizability.
7. **Cluster UI:** show nodes, the leader, terms, commit indexes and replication
   lag live.

### Storage engine

- Background flush through an immutable memtable, so writes don't stall
- Background and leveled compaction, with write amplification measured
- Group commit, to batch concurrent writers into one fsync
- Block cache and fewer allocations on the read path
- A manifest file recording live tables, instead of a directory listing
