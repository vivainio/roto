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
