# lsm-kv

A log-structured merge-tree (LSM) key-value store in Java, built from scratch,
with an HTTP API and a live web UI that shows writes moving through the WAL, the
memtable, the SSTables, and compaction.

**Live demo: [lsm-kv.onrender.com](https://lsm-kv.onrender.com/)**. It runs on a
free tier, so the first load after it has been idle takes about a minute, and
data resets whenever it restarts.

The **Cluster** tab ([direct link](https://lsm-kv.onrender.com/#cluster)) shows
lsm-kv pods running in Kubernetes. Kill one and watch Kubernetes replace it with
its data intact. On the demo site those pods are simulated; to see real ones,
[run it on a local cluster](#run-the-cluster-view-on-a-real-cluster).

It's the storage design used by Cassandra (itself written in Java), RocksDB,
LevelDB and ScyllaDB.

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
`CORS_ORIGIN`, `MAX_DATA_MB` (default 100), `CLUSTER_MODE` (`demo`, `kubernetes`
or `off`; defaults to `kubernetes` inside a cluster and `demo` everywhere else),
and `POD_SELECTOR` (default `app=lsm-kv`).

## Run the Cluster view on a real cluster

This runs three lsm-kv pods in a local Kubernetes cluster with
[kind](https://kind.sigs.k8s.io/), plus a dashboard that watches them. You need
[kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation),
[kubectl](https://kubernetes.io/docs/tasks/tools/), and either Docker or Podman.

**1. Use a separate kubeconfig**, so none of this touches your existing
clusters or your current kubectl context:

```sh
cd lsm-kv
export KUBECONFIG=$PWD/kind.kubeconfig
```

**2. Build the image and create the cluster.**

With Docker:

```sh
docker build -t lsm-kv:dev .
kind create cluster --name lsm-kv
kind load docker-image lsm-kv:dev --name lsm-kv
```

With Podman (start it first with `podman machine start` on macOS or Windows).
The image must be named `docker.io/library/lsm-kv:dev`, because Podman would
otherwise name it `localhost/lsm-kv:dev` and Kubernetes wouldn't find it:

```sh
export KIND_EXPERIMENTAL_PROVIDER=podman
podman build -t docker.io/library/lsm-kv:dev .
kind create cluster --name lsm-kv
podman save -o lsm-kv.tar docker.io/library/lsm-kv:dev
kind load image-archive lsm-kv.tar --name lsm-kv
```

**3. Deploy and open the dashboard:**

```sh
kubectl apply -f k8s/
kubectl -n lsm-kv rollout status statefulset/lsm-kv
kubectl -n lsm-kv port-forward svc/lsm-kv-dashboard 8090:8080
```

Open <http://localhost:8090/#cluster> and kill a pod. In another terminal,
`kubectl -n lsm-kv get pods -w` shows the same thing from the command line.

**4. Clean up** when you're done:

```sh
kind delete cluster --name lsm-kv
rm -f kind.kubeconfig lsm-kv.tar
```

### How the Cluster view works

- [k8s/lsm-kv.yaml](k8s/lsm-kv.yaml) runs the store as a 3-replica
  **StatefulSet**, with a volume per pod, and the website as a separate
  **dashboard** Deployment. Killing a store pod never drops your connection to
  the page.
- The dashboard's service account has a **Role** that can only get, list, watch
  and delete pods in the `lsm-kv` namespace.
- [KubernetesCluster.java](src/main/java/io/github/rajatjc/lsmkv/cluster/KubernetesCluster.java)
  calls the Kubernetes API directly over HTTPS with that service account's
  token. It lists the pods, then follows a **watch** from the list's
  `resourceVersion`, and lists again whenever the watch ends. It needs no
  client library.
- The server pushes each change to the browser with **Server-Sent Events**.
  Every open tab holds one virtual thread.
- Kills are rate-limited to one every 2 seconds, and only pods matching the
  selector can be killed.
- On the demo site,
  [SimulatedCluster.java](src/main/java/io/github/rajatjc/lsmkv/cluster/SimulatedCluster.java)
  plays the same sequence a real StatefulSet goes through: Terminating, gone,
  Pending, ContainerCreating, Running, then ready.

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
| `GET` | `/api/cluster` | cluster mode, namespace and pods |
| `GET` | `/api/cluster/events` | the pod list as a Server-Sent Events stream |
| `POST` | `/api/cluster/pods/{name}/kill` | delete a pod (at most one every 2 seconds) |

```sh
curl -X PUT localhost:8080/api/kv/user:1 -d '{"name":"Ada"}'
curl localhost:8080/api/kv/user:1
```

## Deploy (backend + frontend together)

The UI ships inside the jar, so one deployment serves both the API and the
frontend from the same origin, with no CORS setup.

### Render (how the live demo runs)

The demo is a Docker web service on Render's free tier: **New → Web Service**,
pick this repo, Docker runtime, **Free** instance, and health check path
`/healthz`. Render sets `PORT`, and every push to `main` redeploys. The free tier
has no persistent disk and sleeps after 15 idle minutes, so data resets whenever
the service restarts.

### Fly.io (keeps data)

To keep data across restarts, the host needs a **persistent volume**.
[fly.toml](fly.toml) is set up for Fly.io:

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
engine, and a volume can only attach to one machine.

### Other hosts

- **Railway:** connect the GitHub repo. It detects the Dockerfile. Add a volume
  mounted at `/data`.
- **Render with a disk:** attach a persistent disk at `/data`. Disks need a paid
  instance.

### Hosting the frontend separately (optional)

If you want the UI on GitHub Pages or Vercel and the API on Fly:

1. Copy `src/main/resources/public/index.html` to the static host, and set
   `<meta name="api-base" content="https://<app>.fly.dev">`.
2. Run `fly secrets set CORS_ORIGIN=https://<you>.github.io`.

## Roadmap

- Background flush through an immutable memtable, so writes don't stall
- Background and leveled compaction, with write amplification measured
- Group commit, to batch concurrent writers into one fsync
- Block cache and fewer allocations on the read path
- A manifest file recording live tables, instead of a directory listing
