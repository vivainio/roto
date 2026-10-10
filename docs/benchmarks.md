# Benchmarks

The small benchmark suite measures Roto startup, idle process memory, and a few
AWS API requests. It uses disposable servers and Python's standard library; it
needs no Moto, boto3, Docker, or extra Python packages.

## Measured startup and idle RAM

Measured on October 10, 2026 with an **Apple M3, 8 GiB RAM, macOS 27.0 arm64**,
using `cargo build -p roto-server --release --locked` and Rust 1.98.1.
Each mode has seven fresh-process runs, interleaved across modes.

| Server state | Startup median | Startup observed range | Idle RSS median |
| --- | ---: | ---: | ---: |
| Empty, ephemeral | 9.2 ms | 8.6–514.7 ms | 10.8 MiB |
| Empty, persistent | 19.1 ms | 18.8–20.8 ms | 11.5 MiB |
| Demo seed, ephemeral | 61.4 ms | 60.5–64.2 ms | 45.1 MiB |

The first measured binary launch took **514.7 ms**; the subsequent six ephemeral
launches took 8.6–10.0 ms. No samples were discarded. These are repeated process
starts with normal OS caches, rather than cold-boot or cleared-filesystem-cache
tests. The executable was 28.8 MiB before stripping; this is the file size, not a
container image or download size.

The seeded mode runs `examples/demo/setup.lua` through ordinary service APIs on a
fresh store. It includes resource setup and the fixtures across eleven stateful
services, including Lambda execution results, logs, and notification deliveries.
It does not launch Lambda containers. Persistent mode uses a fresh on-disk store
for every run, with normal SQLite settings and without `--durable`.

Startup starts immediately before spawning the built binary and ends after the
first successful STS `GetCallerIdentity` response with the expected account ID.
It includes process creation, service initialization, any Lua setup, and that
HTTP request. It excludes compilation and benchmark temporary-directory setup.
Readiness is polled at roughly 1 ms intervals, so these are end-to-end readiness
measurements rather than internal initialization timings.

Idle RAM means the server process's **resident set size (RSS)**, sampled five
times at 100 ms intervals after a two-second settling period. Sampling happens
before the request workloads. The table reports the median across per-run RSS
medians. RSS includes resident shared pages and is not the same as total
application memory, macOS physical footprint, or container memory accounting.
The Python client, OS caches, and any external executors are outside that number.

## Launching Roto from test code

The measured startup and idle-memory costs make a fresh Roto process per test or
test group a reasonable default when state isolation matters. Each `--ephemeral`
launch starts empty, so tests can create resources independently without carrying
state from an earlier test. The suite itself exercises 21 disposable launches.

Build the binary once before running tests, choose a free port for each server,
wait for a successful readiness response, and stop the process in fixture cleanup.
Use `--ephemeral --setup examples/demo/setup.lua` when populated state helps;
setup is included in the seeded startup measurement. Async deliveries may continue
after readiness, so wait for the expected delivery result when testing them.
For tests that intentionally share resources, a server per test collection remains
useful. See [integration testing](integration-testing.md) for fixture and trace
assertion guidance.

## S3, DynamoDB, and STS request performance

Each run performs 20 warmup requests followed by 200 measured requests per
operation. The figures below pool the 1,400 measured latency samples per operation
and mode. Requests run sequentially over one persistent loopback HTTP connection.
S3 and STS use the original run; DynamoDB uses a second seven-run pass on the same
machine and exact same release binary. Both raw snapshots are linked below.

### S3: 1 KiB object writes and reads

| Server state | Operation | Median latency | p95 latency | Sequential requests/s |
| --- | --- | ---: | ---: | ---: |
| Empty, ephemeral | PutObject | 0.424 ms | 0.503 ms | 2,286 |
| Empty, ephemeral | GetObject | 0.147 ms | 0.162 ms | 6,831 |
| Empty, persistent | PutObject | 0.446 ms | 0.544 ms | 2,203 |
| Empty, persistent | GetObject | 0.154 ms | 0.169 ms | 6,499 |
| Demo seed | PutObject | 0.423 ms | 0.492 ms | 2,313 |
| Demo seed | GetObject | 0.147 ms | 0.161 ms | 6,861 |

### DynamoDB: item writes and strongly consistent reads

The item has a string partition key and a 1 KiB string payload; the full encoded
item is slightly larger. `PutItem` replaces the same item, and `GetItem` uses
`ConsistentRead = true`. The table has no secondary indexes, and requests have no
conditions or projections. Table creation and the initial item write happen
before the timed workload.

| Server state | Operation | Median latency | p95 latency | Sequential requests/s |
| --- | --- | ---: | ---: | ---: |
| Empty, ephemeral | PutItem | 0.136 ms | 0.144 ms | 7,341 |
| Empty, ephemeral | GetItem | 0.133 ms | 0.140 ms | 7,511 |
| Empty, persistent | PutItem | 0.138 ms | 0.147 ms | 7,214 |
| Empty, persistent | GetItem | 0.133 ms | 0.143 ms | 7,431 |
| Demo seed | PutItem | 0.135 ms | 0.144 ms | 7,370 |
| Demo seed | GetItem | 0.133 ms | 0.141 ms | 7,447 |

### STS: identity lookup

| Server state | Operation | Median latency | p95 latency | Sequential requests/s |
| --- | --- | ---: | ---: | ---: |
| Empty, ephemeral | GetCallerIdentity | 0.136 ms | 0.190 ms | 7,260 |
| Empty, persistent | GetCallerIdentity | 0.130 ms | 0.196 ms | 7,348 |
| Demo seed | GetCallerIdentity | 0.127 ms | 0.189 ms | 7,304 |

Requests/s is the median of per-run rates, including client loop overhead. It is
not a concurrent server capacity measurement. Latencies include the Python HTTP
client, network round trip, response reading, and response validation. The suite
checks successful responses, the STS account ID, S3 read contents, and the
complete returned DynamoDB item. S3 writes
replace the same object; they do not model a growing bucket or large uploads.
The request scope uses fake credentials for routing, with no client-side request
signing. Roto does not validate AWS signatures.

These figures describe this machine and these workloads. They are not a measured
head-to-head comparison with MiniStack, LocalStack, or Moto: platform, service
coverage, state, startup definition, and memory accounting need to match before
comparing numbers.

## Reproduce or extend the suite

From the repository root:

```sh
cargo build -p roto-server --release --locked
python3 scripts/benchmark.py --output benchmark-results.json
```

For a quick check or selected modes:

```sh
python3 scripts/benchmark.py --runs 1 --requests 10 --warmup 2 \
  --settle-seconds 0.2 --output /tmp/roto-benchmark.json
python3 scripts/benchmark.py --modes ephemeral persistent --runs 10 \
  --requests 1000 --output benchmark-results.json
```

Use `--binary /path/to/roto-server` to test another build, and `--help` to see all
options. RSS measurement supports Linux (`/proc/PID/status`) and macOS (`ps`).
Every run uses a free loopback port and a fresh temporary store. Servers are
terminated and state is removed afterward; the suite does not reset an existing
server or use your development data.

The [benchmark script](https://github.com/vivainio/roto/blob/main/scripts/benchmark.py)
saves raw startup times, RSS samples, request latencies, binary size/SHA-256,
toolchain and machine details, and source revision/dirty-state information.
The [original raw run](https://github.com/vivainio/roto/blob/main/benchmarks/results/macos-arm64.json)
and [DynamoDB follow-up run](https://github.com/vivainio/roto/blob/main/benchmarks/results/macos-arm64-dynamodb.json)
were built from revision `83d27b6` with working-tree changes, including the container
executor work. Update the snapshot and these tables together when publishing new
numbers. See the [suite README](https://github.com/vivainio/roto/blob/main/benchmarks/README.md)
for the workload definitions.
