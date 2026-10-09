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
IAM, S3, DynamoDB, SSM, Secrets Manager, and SNS.

## Moto compatibility tests

The repository vendors moto tests unmodified and runs them in server mode against
roto. The pinned version is in `tests/moto/MOTO_VERSION`.

```sh
scripts/run-moto-tests.sh test_sts
scripts/run-moto-tests.sh test_s3
scripts/run-moto-tests.sh test_dynamodb
```

The runner creates `.venv-moto`, installs the test requirements, builds roto, and
starts an ephemeral instance. It adapts upstream port-5000 URLs in a temporary
copy and cleans up the server afterward. Pass additional pytest arguments after
the service directory:

```sh
scripts/run-moto-tests.sh test_sqs -k fifo
TARGET=moto scripts/run-moto-tests.sh test_sts
```

`TARGET=moto` runs the same tests against moto to distinguish upstream behavior
from roto gaps. `ROTO_TEST_PORT` selects a port other than 5070; `TEST_TIMEOUT`
changes the per-test timeout (default 30 seconds).

## Expected failures

Known failures in `tests/moto/expected_failures/<service>.txt` are deselected in
normal runs. A green run means no regression against that baseline, not complete
service parity. Tests needing moto's in-process internals are ignored through
`tests/moto/not_portable.txt`.

After reviewing coverage changes, regenerate a service baseline with:

```sh
UPDATE_EXPECTED=1 scripts/run-moto-tests.sh test_sqs
```

Review the resulting diff: this records all current failures, so it can also hide
regressions if accepted without inspection.

To refresh vendored suites from the pinned upstream version:

```sh
scripts/sync-moto-tests.sh test_sts test_sqs
```

## Adding behavior

Implement operations in the relevant service crate, add meaningful Rust or
compatibility coverage, regenerate when model/codegen changes require it, and
update the coverage baseline and book when observable behavior changes.
