# Development

## Rust checks

Run the checks used by CI:

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
scripts/gen.sh
git diff --exit-code
```

The final command catches generated-code drift and any other uncommitted changes;
inspect the diff when working in a modified tree. Generation covers STS, SQS,
IAM, S3, DynamoDB, SSM, Secrets Manager, SNS, Lambda, EventBridge, CloudFormation,
Kinesis, KMS, and CloudWatch Logs.

## Moto compatibility tests

The repository vendors moto tests unmodified and runs them in server mode against
roto. The pinned version is in `tests/moto/MOTO_VERSION`.

```sh
scripts/run-moto-tests.sh test_sts
scripts/run-moto-tests.sh test_s3
scripts/run-moto-tests.sh test_dynamodb
scripts/run-moto-tests.sh test_awslambda
scripts/run-moto-tests.sh test_events
scripts/run-moto-tests.sh test_kinesis
```

The runner creates `.venv-moto`, installs the test requirements, builds roto, and
starts an ephemeral instance. It adapts upstream port-5000 URLs in a temporary
copy, gives Lambda's IAM role fixture a valid policy document, corrects the old
DynamoDB CloudFormation index schemas and optional-field assertions, and uses a one-second
SQS long poll for EventBridge delivery assertions. Vendored sources
remain unchanged. Tests requiring Docker are skipped; local Lambda executors are
covered by the SDK smokes. The runner cleans up the server afterward. Pass additional pytest arguments after
the service directory:

```sh
scripts/run-moto-tests.sh test_sqs -k fifo
TARGET=moto scripts/run-moto-tests.sh test_sts
```

`TARGET=moto` runs the same tests against moto to distinguish upstream behavior
from roto gaps. `ROTO_TEST_PORT` selects a port other than 5070; `TEST_TIMEOUT`
changes the per-test timeout (default 30 seconds).
CI uses Python 3.12. Use a supported Python version for `.venv-moto`; Python 3.9's
older HTTP dependencies can time out on S3 uploads. `ROTO_MOTO_VENV` can select an
alternative virtual environment with the dependencies from
`tests/moto/requirements.txt`.

## Expected failures

Known failures in `tests/moto/expected_failures/<service>.txt` are deselected in
normal runs. A green run means no regression against that baseline, not complete
service parity. Tests needing moto's in-process internals are ignored through
`tests/moto/not_portable.txt`.
Exclusions match complete test IDs, so excluding one test never suppresses another
test whose name starts with the same text.

Retry just the excluded tests without changing the baseline:

```sh
AUDIT_EXPECTED=1 scripts/run-moto-tests.sh test_dynamodb
```

The audit succeeds when exclusions still fail or skip. It fails and lists tests
that now pass so they can be removed from the baseline. Missing test IDs and
collection errors also fail the audit. The weekly `coverage-audit.yml` workflow
runs this check for every baselined service and supports manual dispatch.

After reviewing coverage changes, regenerate a service baseline with:

```sh
UPDATE_EXPECTED=1 scripts/run-moto-tests.sh test_sqs
```

Review the resulting diff: this records all current failures, so it can also hide
regressions if accepted without inspection.
Collection errors leave the existing baseline intact; parametrized IDs retain
their full text, including spaces.

The DynamoDB SDK smoke checks account and region isolation using identical table
names and item keys, then restarts roto and verifies items, indexes, TTL settings,
and STS credential routing:

```sh
.venv-moto/bin/python tests/smoke-dynamodb.py
```

To refresh vendored suites from the pinned upstream version:

```sh
scripts/sync-moto-tests.sh test_sts test_sqs
```

## Adding behavior

Implement operations in the relevant service crate, add meaningful Rust or
compatibility coverage, regenerate when model/codegen changes require it, and
update the coverage baseline and book when observable behavior changes.

## Minimal CRUD service example

`roto-svc-logs` is the small service example. It implements log-group create,
list, delete, retention updates, tags, and one event query. The protocol types
and dispatcher come from `models/logs/service-2.json`; the handwritten behavior
is in `crates/roto-svc-logs/src/service.rs`, with the SQLite tables and migration
in `schema.rs` and `lib.rs`. Its handlers scope each key by account and region,
use Diesel transactions for mutations, and return generated AWS response types.

Lambda connects to the log service in process and writes captured output to
`/aws/lambda/<function>`. Stream management, direct `PutLogEvents`, Insights,
and subscription features stay outside this example.

## Typed SQLite access

All service database access uses Diesel's SQLite backend. Each service's
`schema.rs` declares database tables; services with stored records use
`models.rs` or local structs derived with `Queryable`, `Selectable` and
`Insertable`. Generated AWS API models remain separate. Handlers use typed
queries inside `DieselDb::transaction`, preserving transaction boundaries.
Path-prefix filters use SQLite substring functions so prefixes remain literal
and case-sensitive.

`Store::diesel_db` runs existing service migrations, configures WAL, foreign keys,
busy timeout and durability, and shares the service lock with the legacy
connection used by inspection. Ephemeral connections share an isolated in-memory
SQLite database. `rusqlite` remains only in the shared store for migrations and
the inspection UI's dynamic schema and row access; service handlers use Diesel.
Add a direct Diesel dependency when using its derives or table macros; the core
crate converts Diesel errors to the existing storage error response.

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

## Luau service contracts in CI

The `lua-services` job has separate matrix entries for CUR, ACM, Budgets, Config,
Cloud Map, OpenSearch, RDS, ECR, ECS and Glue. Each entry builds the server, installs pinned boto3, and runs:

```sh
python3 tests/smoke-lua-services.py --service acm --sdk
```

Replace `acm` with `cur`, `budgets`, `config`, `servicediscovery`, `opensearch`,
`rds`, `ecr`, `ecs`, or `glue`. The runner
starts and cleans up disposable servers on free ports. It checks each service's
supported operations, modeled failures, account/region isolation, persistence
through restart, and reset, using both a directory bundle and a ZIP bundle.
SDK mode also checks model field names/types, serialization and response parsing.
Without `--sdk`, it needs only Python's standard library. Omit `--service` to run
all ten. Build first with `cargo build -p roto-server --locked`.

These focused contracts are adapted from MiniStack tests and cover the implemented
subsets. They are separate from the Moto operation-coverage reports.
