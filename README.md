# roto

An AWS simulator written in Rust: a from-scratch rewrite of [moto](https://github.com/getmoto/moto)'s server mode.
One static binary, AWS wire-protocol compatible, persistent by default (SQLite + real-filesystem S3).

**Status:** under active development. See [STATUS.md](STATUS.md) for current coverage and [PLAN.md](PLAN.md) for the roadmap.

**Documentation:** [The roto book](https://vivainio.github.io/roto/) ([sources](docs/index.md)).

```sh
uv tool install roto-aws
roto-server --port 5070                      # persistent: ./roto-data ; add --ephemeral for in-memory
aws --endpoint-url http://localhost:5070 sts get-caller-identity
scripts/run-moto-tests.sh test_sts              # moto's own tests, run against roto
```

Lambda functions can execute local commands, POST events to an HTTP API, or invoke an existing
Lambda container image through RIE. Configure bindings in Lua with `--setup`; see the
[executor example and contracts](examples/lambda/README.md) and the
[container demo](demos/lambda-container/README.md).
S3 notifications and SQS event-source mappings invoke those same executors.
Captured Lambda output is available through a small [CloudWatch Logs subset](docs/cloudwatch-logs.md).
Use `--setup setup.lua` to declare queues, functions, and wiring with embedded Lua; see the
[Lua setup example](examples/lua/README.md). EventBridge rules can route custom and S3 events
to Lambda or SQS; see the [EventBridge example](examples/eventbridge/README.md).
A [CloudFormation subset](docs/cloudformation.md) manages SQS, SNS, S3, DynamoDB, Kinesis,
IAM, and Lambda resources with persistent stacks, change sets, rollback attempts,
references, outputs, and events.

## Startup and memory

Release-build measurements on an Apple M3 with 8 GiB RAM, macOS 27.0
(seven fresh-process runs per mode):

| Server state | Median startup | Median idle RSS |
| --- | ---: | ---: |
| Empty, ephemeral | 9.2 ms | 10.8 MiB |
| Empty, persistent | 19.1 ms | 11.5 MiB |
| Demo seed, ephemeral | 61.4 ms | 45.1 MiB |

Startup runs from process spawn to a valid STS response. The first measured launch
took 515 ms; all samples are retained. These are repeated starts with normal OS
caches. See [methodology, API latency results, and raw measurements](docs/benchmarks.md).

These startup and idle-memory costs make repeated disposable launches practical
in test code: use a fresh `--ephemeral` process per test or test group for isolated
state. Build the binary once, wait for readiness, and stop it in fixture cleanup.

Run the small suite yourself; it needs only Python's standard library:

```sh
cargo build -p roto-server --release --locked
python3 scripts/benchmark.py --output benchmark-results.json
```

## Documentation

```sh
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r requirements-docs.txt
.venv-docs/bin/zensical serve
```

GitHub Actions builds the book on pull requests and deploys `main` to GitHub Pages.
Set the repository's Pages source to **GitHub Actions**. See
[the documentation guide](docs/documentation.md) for details.

## Python package releases

Install with `uv tool install roto-aws`; the package provides the `roto-server`
command. Upgrade with `uv tool upgrade roto-aws`. For development from source,
run `cargo run -p roto-server -- --port 5070`.

Push a Cargo-compatible version tag such as `v0.0.1` to trigger
`.github/workflows/release.yml`. The shared workflow sets the workspace version
from the tag, builds Linux x64/ARM64, Windows x64, macOS Intel/ARM64 wheels and a
source distribution, then the local publishing job uploads them to PyPI.
The shared components are pinned to a commit in `vivainio/actions`.

Before the first release, create the GitHub environment `pypi` and configure a
PyPI Trusted Publisher (or pending publisher for a new project) with:

- Project: `roto-aws`
- Owner: `vivainio`
- Repository: `roto`
- Workflow filename: `release.yml`
- Environment: `pypi`

Release tags should point to commits that pass the existing CI checks.

Build locally with `uvx maturin build --release --locked --out dist` or
`uvx maturin sdist --out dist`.

To retry uploading already-built distributions, run the release workflow
manually and provide the original release run ID in `artifact-run-id`. This
skips rebuilding and publishes that run's artifacts. Verify the chosen run
belongs to the release you intend to publish.

## License

MIT. See [LICENSE](LICENSE).

Optional Lua services (Organizations, CUR, ACM, Budgets, Config, Cloud Map HTTP,
OpenSearch, RDS, ECR, ECS and Glue)
ship separately as `roto-lua-services-<tag>.zip` on GitHub releases. Load the ZIP
without extraction using `roto-server --lua-services path/to/bundle.zip`, or load
this checkout with `--lua-services services`. No Lua service scripts are embedded
in the binary. See [the bundle README](services/README.md) for scope and packaging.
