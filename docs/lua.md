# Lua setup

Lua declares resources and wires queues to Lambda functions. Functions can execute a local
process, POST their event to an HTTP endpoint, or invoke an existing Lambda container image
through RIE, or run an inline Lua placeholder handler.
See [runtime Lua handlers and interception](runtime-hooks.md) for inline functions and request hooks.
This page is a tutorial; the [Lua API reference](lua-api.md) is the single source for every
symbol, option and return value (start with its symbol index).
The binary embeds Lua 5.4 through `mlua`; no Lua installation is needed.

Save this as `setup.lua`:

```lua
local queue = roto.sqs.queue("jobs", {visibility_timeout = 30})
local handler = roto.lambda.fn("echo-job", {
  executor = {command = {"sh", "-c", "cat"}}, timeout = 5
})
roto.lambda.event_source(queue, handler)
```

Start the server in one terminal:

```sh
roto-server --ephemeral --setup setup.lua
```

In another terminal, send a message using the AWS CLI with local credentials
configured as described in [Getting started](getting-started.md):

```sh
aws --endpoint-url http://localhost:5070 sqs send-message \
  --queue-url http://localhost:5070/123456789012/jobs --message-body 'hello'
curl http://localhost:5070/roto-api/lambda/invocations
```

The handler receives an AWS SQS event with a `Records` array and returns that
event as its JSON result. A shell executor reads stdin and writes one JSON result
to stdout; logs go to stderr. See [Lambda](lambda.md) for the full
contract, HTTP and container executors, timeouts, and output limits.

## Setup helpers

This expands the first example with queue settings and function options:

```lua
local dead = roto.sqs.queue("dead")
local queue = roto.sqs.queue("jobs", {
  visibility_timeout = 30,
  attributes = { RedrivePolicy = '{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dead","maxReceiveCount":3}' },
  tags = { owner = "local" },
})

local handler = roto.lambda.fn("process-job", {
  timeout = 10,
  memory_size = 128,
  description = "Process local jobs",
  environment = { MODE = "development" },
  executor = { command = { "sh", "-c", "cat" }, env = { DEBUG = "1" } },
})
-- Alternatively: executor = { url = "http://localhost:8080/jobs", headers = {} }
-- Or invoke an existing Lambda image: executor = { container = "my-worker:latest" }

local mapping = roto.lambda.event_source(queue, handler, {
  batch_size = 10,
  enabled = true,
  report_batch_item_failures = true,
})
```

For a quick placeholder, supply `handler` instead of `executor`:

```lua
roto.lambda.fn("greet", {
  handler = function(event, context)
    return {message = "Hello " .. event.name}
  end,
})
```

The handler receives decoded JSON and returns a JSON-compatible value. Closures
survive startup; Lua errors fail the invocation. Runtime callbacks cannot use
resource setup APIs or `roto.call`/`roto.wire`. See [runtime hooks](runtime-hooks.md)
for context fields, timeout limits, and interception examples.

`queue` returns `{name, url, arn}`; `fn` returns `{name, arn}`; `event_source` returns
the AWS mapping configuration, including `UUID`. `fn` is short for function (`function` is a Lua keyword). Unknown helper options cause a setup error.

The helpers create missing resources and update existing ones. Repeating setup preserves
queue messages and mapping UUIDs. Queue attributes/tags supplied by the script are merged;
omitted ones remain. Function timeout/memory/description/environment and mapping settings
are reconciled to the supplied values or helper defaults. Omitting a resource from the script
does not delete it.

Use `roto.lambda.bind(name, executor)` to bind an executor to function metadata created through
the AWS API. `fn` creates/updates the metadata and binds in one step. Executors may be
commands, HTTP endpoints, or RIE container image names; see [the Lambda reference](lambda.md).

The script runs once on each startup after service initialization, before the HTTP listener
and delivery workers start. `--account-id` and `--setup-region` (default `us-east-1`) set its
scope. `roto.account_id`, `roto.region`, and `roto.endpoint` expose that scope to Lua.
`require("module")` searches the script directory first. Command `cwd` defaults to that
directory; relative `cwd` is resolved against it. Executables use the normal process PATH.

Setup errors include a source location and stop startup. Changes from earlier successful
operations remain persisted. Setup scripts are trusted local code; executor bindings are local
configuration and cannot be submitted through AWS APIs.

## S3 fixtures from host files

`roto.s3.link(bucket, key, file)` and `roto.s3.link_dir(bucket, prefix, dir)` register
existing host files as objects without copying or uploading them, which suits large fixtures
that every start would otherwise re-upload. See the [Lua API reference](lua-api.md#s3)
and [Storage](storage.md).

## Other services

Anything without a helper can be seeded with `roto.call`, using an operation name
and AWS-shaped arguments, or `roto.wire`, using an HTTP method, path, headers, and
body. Both call local service handlers directly. `roto.wire` returns a raw response
body; decode JSON responses with `roto.json_decode`.
See [Service access](lua-api.md#service-access) for supported services and
[the same S3 upload with wire, call, and boto3](lua-api.md#the-same-upload-with-wire-call-and-boto3)
for a comparison including object metadata.

```lua
roto.call("ssm", "PutParameter", {
  Name = "/app/mode", Value = "development", Type = "String", Overwrite = true,
})
roto.call("s3", "CreateBucket", {Bucket = "fixtures"})
roto.wire("s3", "PUT", "/fixtures/hello.txt", { body = "hello" })
```

Use `roto.array({})` for an explicitly empty JSON array and `roto.null` for JSON null.

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

## Running scripts against a live server

With `--enable-lua-api`, `POST /roto-api/lua` runs a script with these same helpers; see the
[Lua API reference](lua-api.md#running-scripts-on-a-live-server).

## Quick test data

To explore populated services in the inspection UI, download the
[example setup script](https://github.com/vivainio/roto/blob/main/examples/demo/setup.lua)
and save it as `demo-setup.lua`. Run it with:

```sh
roto-server --ephemeral --port 5071 --setup demo-setup.lua
```

It creates useful data across eleven stateful services, including multi-page
bucket contents, versions, nested DynamoDB items, retained queue messages,
Lambda results/logs, and delivery history. Use a fresh store; names are fixed.
Open `http://localhost:5071/roto-api/` to inspect the data. See the
[demo README](https://github.com/vivainio/roto/blob/main/examples/demo/README.md)
for the fixture inventory and intentional failure cases.
