# Quick demo/test data

Start a fresh server with useful sample resources for API development, integration
testing, or browsing the inspection UI:

```sh
scripts/demo.sh              # default: http://localhost:5071
scripts/demo.sh 5072         # use another free port
```

The launcher builds the local binary and runs `--ephemeral --setup
examples/demo/setup.lua`. It keeps running until Ctrl-C; restarting creates a
fresh dataset. No moto, boto3, standalone Lua installation, or Python packages
are needed. Local Lambda executors use `sh`.

To manage the server yourself (including a custom account or region):

```sh
roto-server --ephemeral --port 5071 --setup examples/demo/setup.lua
```

The script uses normal service APIs and finishes seeding before the server starts
listening. Use a fresh store: fixed resource names make this a fixture seed, not
an idempotent migration. EventBridge deliveries and S3 retries continue after
startup. Resources follow `--account-id` and `--setup-region`; unsigned history
inspection defaults to `us-east-1`.

| Service | Sample data |
| --- | --- |
| CloudFormation | Messaging and storage stacks, each with two resources and outputs |
| S3 | Four buckets, 68 object records, nested keys, 55 catalog JSON files for pagination, text/CSV/HTML/binary/empty objects, a large preview, versions, a delete marker |
| DynamoDB | Six orders with nested maps/lists, sets, booleans, and nulls; an empty table and a stack-managed inventory table |
| SQS | Orders and dead-letter messages kept for inspection; an event delivery queue |
| Lambda | Echo and intentional failure executors, results and logs, a disabled queue mapping |
| EventBridge | Custom bus, rule, two targets, four deliveries |
| SSM | String, StringList, and SecureString parameters; multiple versions |
| Secrets Manager | Fake database credentials with two versions |
| IAM | Demo user and role |
| SNS | Topic and SQS subscription |

The S3 destination `demo-missing` deliberately does not exist. Its handoff appears
queued, then fails after three attempts, making retry errors available to inspect.
The Lambda `demo-fail` also fails intentionally. All credentials are fake.

Browse at `http://localhost:5071/roto-api/`, use AWS SDKs/CLI against the endpoint,
or verify the fixture contract with the standard-library-only smoke harness:

```sh
cargo build -p roto-server --locked
python3 scripts/smoke-demo.py
```

The harness starts and stops its own server on a free port and checks resource
counts, pagination, version downloads, nested item data, retained queue messages, stack resources/outputs,
Lambda results/logs, and asynchronous deliveries.
