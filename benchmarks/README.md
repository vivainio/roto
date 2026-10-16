# Small server benchmark suite

Build an optimized binary and run from the repository root:

```sh
cargo build -p roto-server --release --locked
python3 scripts/benchmark.py --output benchmark-results.json
```

Python's standard library is sufficient; no Docker, boto3, Moto, or Python packages
are needed. RSS measurement supports Linux and macOS. Run a short check with:

```sh
python3 scripts/benchmark.py --runs 1 --requests 10 --warmup 2 \
  --settle-seconds 0.2 --output /tmp/roto-benchmark.json
```

The default suite interleaves seven fresh processes for each of three modes:

- `ephemeral`: empty in-memory state.
- `persistent`: empty SQLite/filesystem state in a fresh temporary directory.
- `seeded`: fresh ephemeral state populated by `examples/demo/setup.luau`.

It measures spawn-to-valid-STS-response startup time, idle process RSS, and
sequential STS GetCallerIdentity, S3 PutObject/GetObject with a 1 KiB object,
and DynamoDB PutItem/GetItem with a 1 KiB string payload. DynamoDB uses a
string partition key and strongly consistent reads. Idle samples are taken before request workloads. Every response is checked
for success, including the account ID for STS, object contents for S3 reads,
and the complete returned item for DynamoDB reads.
Each process is stopped and its disposable state removed afterward.

Use `--modes ephemeral persistent`, `--runs`, `--requests`, `--warmup`,
`--settle-seconds`, `--memory-samples`, and `--memory-interval` to adjust the suite.
Use `--binary /path/to/roto-server` to measure another build. Timings include the
Python HTTP client and loopback HTTP; sequential requests/second is not a concurrent
capacity test. The suite sends fake credentials for service routing and does not
measure AWS request signing. Run `python3 scripts/benchmark.py --help` for defaults.

The JSON includes raw startup, RSS, and request samples, the binary SHA-256 and
size, toolchain, machine details, and source revision/dirty-state information.
Published results and methodology are in [the benchmark chapter](../docs/benchmarks.md).
Keep machine-specific snapshots in `benchmarks/results/` when updating published
numbers; avoid comparing results from different hosts or measurement methods as
if they were the same test.
