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
IAM, S3, DynamoDB, SSM, Secrets Manager, SNS, Lambda, EventBridge, CloudFormation, and Kinesis.

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
.venv-moto/bin/python scripts/smoke-dynamodb.py
```

To refresh vendored suites from the pinned upstream version:

```sh
scripts/sync-moto-tests.sh test_sts test_sqs
```

## Adding behavior

Implement operations in the relevant service crate, add meaningful Rust or
compatibility coverage, regenerate when model/codegen changes require it, and
update the coverage baseline and book when observable behavior changes.

## Typed SQLite access

IAM, SSM, KMS, STS, Secrets Manager, CloudFormation, EventBridge, Kinesis and SNS use Diesel's SQLite backend. Each service's `schema.rs` declares database tables and
`models.rs` contains stored records derived with `Queryable`, `Selectable` and
`Insertable`; generated AWS API models remain separate. Handlers use typed
queries inside `DieselDb::transaction`, retaining one transaction per API call.
Path-prefix filters use SQLite substring functions so prefixes remain literal
and case-sensitive.

`Store::diesel_db` runs existing service migrations, configures WAL, foreign keys,
busy timeout and durability, and shares the service lock with the legacy
connection used by inspection. Ephemeral connections share an isolated in-memory
SQLite database. Other services still use `rusqlite` and can be ported individually.
Add a direct Diesel dependency when using its derives or table macros; the core
crate converts Diesel errors to the existing storage error response.
