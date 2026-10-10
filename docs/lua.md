# Lua setup and SQS → Lambda

Lua declares resources and wires queues to Lambda functions. Functions execute a local
process or POST their event to an HTTP endpoint. Lua does not execute Lambda handlers.
The binary embeds Lua 5.4 through `mlua`; no Lua installation is needed.

```sh
cargo run -p roto-server -- --ephemeral --setup examples/lua/setup.lua
aws --endpoint-url http://localhost:5070 sqs send-message \
  --queue-url http://localhost:5070/123456789012/jobs --message-body 'hello'
curl http://localhost:5070/roto-api/lambda/invocations
```

The shell example requires Python 3. See [setup.lua](https://github.com/vivainio/roto/blob/main/examples/lua/setup.lua) for an optional HTTP binding.
Both executors receive the AWS SQS event with a `Records` array. A shell script must read
stdin and write exactly one JSON result to stdout; logs go to stderr. An HTTP endpoint must
return a 2xx response containing one JSON result. The full contracts, environment variables,
timeouts and output limits are in [the Lambda executor reference](lambda.md).

For a standalone first setup, save this as `setup.lua`:

```lua
local queue = roto.sqs.queue("jobs", {visibility_timeout = 30})
local handler = roto.lambda.function_("echo-job", {
  executor = {command = {"sh", "-c", "cat"}}, timeout = 5
})
roto.lambda.event_source(queue, handler)
```

Start `roto-server --setup setup.lua`, then send a message with the command above.
The handler returns the incoming event as its JSON result.

## Quick test data

Use the reusable [demo seed](https://github.com/vivainio/roto/blob/main/examples/demo/setup.lua)
for API development, integration tests, or the inspection UI:

```sh
scripts/demo.sh                 # seeded ephemeral server on port 5071
# Or manage the server directly:
roto-server --ephemeral --port 5071 --setup examples/demo/setup.lua
```

It creates useful data across all nine stateful services, including multi-page
bucket contents, versions, nested DynamoDB items, retained queue messages,
Lambda results/logs, and delivery history. Use a fresh store; names are fixed.
No moto or Python dependencies are needed. Verify it with
`python3 scripts/smoke-demo.py` after building the server. See the
[demo README](https://github.com/vivainio/roto/blob/main/examples/demo/README.md)
for the fixture inventory and intentional failure cases.

## Setup helpers

```lua
local dead = roto.sqs.queue("dead")
local queue = roto.sqs.queue("jobs", {
  visibility_timeout = 30,
  attributes = { RedrivePolicy = '{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dead","maxReceiveCount":3}' },
  tags = { owner = "local" },
})

local handler = roto.lambda.function_("process-job", {
  timeout = 10,
  memory_size = 128,
  description = "Process local jobs",
  environment = { MODE = "development" },
  executor = { command = { "sh", "./process_jobs.sh" }, env = { DEBUG = "1" } },
})
-- Alternatively: executor = { url = "http://localhost:8080/jobs", headers = {} }

local mapping = roto.lambda.event_source(queue, handler, {
  batch_size = 10,
  enabled = true,
  report_batch_item_failures = true,
})
```

`queue` returns `{name, url, arn}`; `function_` returns `{name, arn}`; `event_source` returns
the AWS mapping configuration, including `UUID`. `function_` avoids Lua's reserved word
`function`. Unknown helper options cause a setup error.

The helpers create missing resources and update existing ones. Repeating setup preserves
queue messages and mapping UUIDs. Queue attributes/tags supplied by the script are merged;
omitted ones remain. Function timeout/memory/description/environment and mapping settings
are reconciled to the supplied values or helper defaults. Omitting a resource from the script
does not delete it.

The script runs once on each startup after service initialization, before the HTTP listener
and delivery workers start. `--account-id` and `--setup-region` (default `us-east-1`) set its
scope. `roto.account_id`, `roto.region`, and `roto.endpoint` expose that scope to Lua.
`require("module")` searches the script directory first. Command `cwd` defaults to that
directory; relative `cwd` is resolved against it. Executables use the normal process PATH.

Setup errors include a source location and stop startup. Changes from earlier successful
operations remain persisted. Setup scripts are trusted local code; executor bindings are
local configuration and cannot be submitted through AWS APIs. Lua bindings for full function
ARNs override the corresponding JSON binding; untouched JSON bindings remain available.

## Other service operations

`roto.call(service, operation, input)` accepts AWS-shaped input and returns decoded JSON.
It supports Lambda, SQS, DynamoDB, SSM, Secrets Manager, and EventBridge (`events` or `eventbridge`):

```lua
roto.call("ssm", "PutParameter", {
  Name = "/app/mode", Value = "development", Type = "String", Overwrite = true,
})
```

Use `roto.array({})` for an explicitly empty JSON array and `roto.null` for JSON null.

`roto.request(service, method, path, options)` calls any installed service through its normal
wire handler, without waiting for the listener. Options are `headers`, `query` (raw query
string), and `body` (a string or a Lua table encoded as JSON). It returns
`{status, headers, body}` with a raw string body. HTTP errors raise a Lua error.

```lua
roto.request("s3", "PUT", "/fixtures")
roto.request("s3", "PUT", "/fixtures/hello.txt", { body = "hello" })
roto.request("sns", "POST", "/", {
  headers = { ["content-type"] = "application/x-www-form-urlencoded" },
  body = "Action=CreateTopic&Version=2010-03-31&Name=events",
})
```

## EventBridge wiring

Use `roto.call("events", "PutRule", input)` and `PutTargets` to route custom or S3
events to these same functions and queues. See [EventBridge](eventbridge.md) for a
complete setup and supported patterns.

## SQS delivery behavior

Mappings are persisted and manageable with the ordinary Lambda event-source mapping APIs.
Supported sources are standard queues in the same account and region, with batches of 1–10
messages and no batching window. Unsupported options (including FIFO sources, filters,
scaling settings, tags, and nonzero batching windows) fail explicitly. One polling worker
processes mappings sequentially; this does not emulate AWS's scaling or polling delays.
The queue visibility timeout must be at least the function timeout.

Messages remain in SQS during execution. Success deletes them using the current receipt
handle; handler failure leaves them hidden until visibility timeout expires. SQS receive
counts and `RedrivePolicy` control retries and dead-letter delivery. SQS invocations do not
use Lambda's separate three-attempt async retry policy. Payloads are capped at 6 MiB,
including event metadata and JSON encoding, by reducing batches when necessary.

With `report_batch_item_failures`, return:

```json
{"batchItemFailures": [{"itemIdentifier": "failed-message-id"}]}
```

Only listed messages are retried. Invalid failure identifiers or a malformed failure list
retry the whole batch. An empty or absent failure list means success. Disabling/deleting a
mapping prevents subsequent polling; an already executing batch may finish. A crash before
acknowledgment can cause duplicates after visibility timeout, so delivery is at least once.
On restart, interrupted SQS invocations are marked failed in history and retry through SQS.

Reset clears resources, mappings and history. Local executor bindings remain; setup is not
automatically rerun after reset.

## Verification

```sh
cargo build -p roto-server
.venv-moto/bin/python scripts/smoke-lua.py
```

The smoke test starts disposable servers, exercises shell and HTTP deliveries and disabled
mappings, and restarts persistent state to verify idempotent setup.
