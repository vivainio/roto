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

Lambda functions can execute local commands or POST events to an HTTP API. Configure bindings
with `--lambda-executors`; see the [runnable example and contracts](examples/lambda/README.md).
S3 notifications and SQS event-source mappings invoke those same executors.
Use `--setup setup.lua` to declare queues, functions, and wiring with embedded Lua; see the
[Lua setup example](examples/lua/README.md). EventBridge rules can route custom and S3 events
to Lambda or SQS; see the [EventBridge example](examples/eventbridge/README.md).
A [CloudFormation subset](docs/cloudformation.md) manages SQS, SNS, S3, DynamoDB, and IAM
roles with persistent stacks, updates, references, and outputs.

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
