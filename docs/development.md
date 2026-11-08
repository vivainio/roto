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

## Luau service tests

Use Luau tests for service behavior, validation, fixtures and virtual-time scenarios.
Group cases in a shared suite when a common environment makes them cheaper or clearer;
use standalone cases when they need a fresh environment. Moto server-mode tests provide
AWS wire-protocol and SDK compatibility coverage. Keep focused HTTP smoke tests where
they cover routing, protocols or integration behavior not exercised by Moto.

Run Luau cases without an HTTP listener or boto3:

```sh
cargo build -p roto-server --locked
./target/debug/roto-server test tests/luau
./target/debug/roto-server test tests/luau/states.luau --filter "wait fires"
cargo run -p roto-server -- test tests/luau
```

A directory is searched recursively for `.luau` files; explicit files are also
accepted. The runner reports every selected case and returns a nonzero status on
failure, discovery error, or no selected cases. By default it loads `./services`
if present. Use `--lua-services PATH` or `ROTO_LUA_SERVICES` to select another
bundle. Standalone cases run sequentially in one process, each with a fresh ephemeral store,
service environment and frozen clock starting at 2025-01-01T00:00:00Z. The file is executed
once for discovery and again for each selected case or suite, so keep fixture setup inside
cases or suite hooks.

Cases have a 30-second Luau instruction deadline. Since they share a process,
a host crash or a blocking native call can stop the run; the driver runs cases
sequentially because the virtual clock is process-wide. A Luau timeout interrupts
the whole suite, so remaining cases and cleanup hooks in that suite cannot run;
the runner reports the interrupted suite and proceeds to the next environment.

```luau
test.case("a queue starts empty", function()
    local queue = test.call("sqs", "CreateQueue", {QueueName = "work"})
    local ok, err = test.try_call("sqs", "GetQueueUrl", {QueueName = "missing"})
    test.equal(ok, false)
    assert(err.code)
    test.equal(test.now(), 1735689600)
    test.advance(60) -- runs due Luau service jobs, including chains of timers
end)
```

Use a suite when cases should share a fixture, store, service runtimes and virtual clock:

```luau
test.suite("repositories", function(suite)
    suite.before_all(function()
        test.call("ecr", "CreateRepository", {repositoryName = "shared"})
    end)
    suite.before_each(function() -- optional per-case setup
    end)
    suite.case("describes the fixture", function()
        local result = test.call("ecr", "DescribeRepositories", {repositoryNames = {"shared"}})
        test.equal(result.repositories[1].repositoryName, "shared")
    end)
    suite.after_each(function() -- optional per-case cleanup
    end)
    suite.after_all(function() -- optional suite cleanup
    end)
end)
```

Each suite starts in a fresh environment; its selected cases run in registration order and
share their changes. Hooks run in registration order. The runner runs `before_all` once,
`before_each` and `after_each` around each selected case, and `after_all` once. If setup or
a case fails, it reports the error, runs applicable cleanup hooks and continues with the
other cases. A `before_all` failure fails every selected case; an `after_all` failure is
reported separately. `--filter` matches the case name or its suite name, and `before_all`
still runs when only one suite case is selected. A suite's state does not leak to another
suite or standalone case. To avoid order-dependent cases, reset mutable state in the hooks.

`test.call(service, operation, input)` returns a decoded response or raises an
error. `test.try_call` returns `true, response` or `false, error`, where the error
has `code`, `message`, and `status` when available. To assert a particular code:
`test.expect_error("SomeCode", function() return test.try_call(...) end)`.
`test.equal` compares nested values. `test.drain()` runs currently due jobs;
`test.advance(seconds)` advances through each scheduled time and drains jobs.
Both fail if the job execution budget is exhausted. The existing `roto` setup
helpers, account/region controls, and fixture APIs are also available.

For checked request and response shapes, use `test.aws.<service>.<Operation>`.
It calls the same dispatcher as `test.call`:

```luau
--!strict
test.case("typed queue", function()
    local queue = test.aws.sqs.CreateQueue({QueueName = "work"})
    local url = assert(queue.QueueUrl)
    test.aws.sqs.SendMessage({QueueUrl = url, MessageBody = "hello"})
end)
```

Run `roto-server --check tests/luau/*.luau` to check test files with
`luau-lsp`. Typed clients cover the existing `roto.aws` operations plus Roto's
implemented Step Functions, Scheduler and CloudWatch Logs operations. The
string-based `test.call` and `test.try_call` accept dynamic services and remain
untyped. Generate editor definitions with `roto-server --lua-stubs types` and
use `types/roto.d.luau` for both setup scripts and tests. Setup scripts may also
declare cases and suites: a normal setup run validates the names and callbacks and skips
their bodies. The other `test` APIs are available while the test driver runs a file.

The virtual clock affects services that use Roto's clock, including Luau service
jobs. Native workers that use wall time are not advanced by this API. These tests
supplement the HTTP smoke tests, which continue to cover protocol and SDK behavior.

To compare ECR cases and the ported CUR, ACM, Config, Budgets and Cloud Map
workflows with their HTTP equivalents, run `python3 tests/benchmark-luau-services.py 5` after
building the server. It reports warm-run medians for the Luau driver, HTTP with a
fresh server per workflow, and HTTP with one server reset between workflows. The
shared-server mode retains service runtimes, unlike a fresh test environment.

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
