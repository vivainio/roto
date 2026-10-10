# Quick test data

Use the reusable Lua seed at `examples/demo/setup.lua` when you need populated
service state for API development, integration tests, manual exploration, or the
inspection UI. It uses normal service APIs and seeds ten stateful services;
no moto installation or Python packages are needed.

```sh
scripts/demo.sh                 # builds and starts a seeded ephemeral server on 5071
scripts/demo.sh 5072            # choose another port
```

The endpoint is `http://localhost:5071`; browse it at
`http://localhost:5071/roto-api/`. The launcher stays running until Ctrl-C. Data
is disposable; restarting the launcher creates a fresh fixture set. Use a free
port alongside any existing server.

For tests that manage their own server, pass
`--ephemeral --setup examples/demo/setup.lua` to `roto-server`. Run the seed on a
fresh store: resource names are fixed and the seed is not an idempotent migration.
The setup finishes before the listener starts; EventBridge deliveries and S3
notification retries continue afterward.

Fixtures include two CloudFormation stacks with resources and outputs, multi-page bucket contents, object versions and delete markers,
nested DynamoDB items, queue messages, disabled event-source mappings, successful
and intentionally failed Lambda executions with logs, EventBridge deliveries,
parameters and secret versions, IAM resources, and an SNS subscription. The
missing `demo-missing` Lambda destination intentionally demonstrates failed S3
handoffs. Executors use `sh`; credentials and tokens are fake demo values.

Verify the fixture contract after changing it:

```sh
cargo build -p roto-server --locked
python3 scripts/smoke-demo.py    # starts and stops its own disposable server
```

# API coverage from Moto tests

Run the server-mode Moto tests by service and save each run's request trace:

```sh
mkdir -p api-traces
for suite in test_awslambda test_dynamodb test_ecr test_events test_iam test_kinesis \
  test_kms test_s3 test_secretsmanager test_sns test_sqs test_ssm test_sts
do
  ROTO_TRACE_FILE="api-traces/${suite}.jsonl" scripts/run-moto-tests.sh "$suite" \
    || echo "$suite had failures; keeping its trace"
done
```

The runner builds a Moto test environment if needed, starts a fresh ephemeral
Roto server for each suite, and tags calls with the pytest node ID. Expected
failures are excluded from the normal run. To also capture those known failing
cases, run an additional pass with `AUDIT_EXPECTED=1` and distinct trace names.

Build a combined, interactive report against every API operation in botocore:

```sh
.venv-moto/bin/python scripts/report-api-coverage.py \
  --full-spec api-traces/*.jsonl --output api-coverage.html
```

The report distinguishes observed successes, errors, unsupported operations,
and operations not exercised by the selected tests. Unseen operations are not
necessarily unimplemented. Keep separate trace files per run; the runner
truncates its output file when a suite starts. See
[`docs/integration-testing.md`](docs/integration-testing.md) for app-level
assertions using the in-memory trace endpoint.
