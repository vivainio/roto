# Lambda

Save this executor binding as `setup.luau`:

```luau
--!strict
roto.lambda.bind("process-upload", {command = {"sh", "-c", "cat"}})
```

Start Roto with that file:

```sh
roto-server --ephemeral --setup setup.luau
```

Create function metadata through the usual Lambda API (SDK, CLI, or Terraform), using a name
bound by the setup script. `roto.lambda.bind(name, executor)` works for functions created before
or after startup. `roto.lambda.fn` can create/update metadata and bind an executor in one
step. Bindings are local configuration and are never accepted through an AWS API.

For example, with local AWS credentials configured:

```sh
aws --endpoint-url http://localhost:5070 lambda create-function \
  --function-name process-upload --runtime provided.al2023 --handler external \
  --role arn:aws:iam::123456789012:role/local --code '{"ZipFile":""}'
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name process-upload --cli-binary-format raw-in-base64-out \
  --payload '{"hello":"world"}' result.json
```

The [Luau setup chapter](lua.md) covers resource creation and executor bindings. The
[Demos chapter](demos.md#invoke-a-lambda-container-image) shows the RIE image invocation.

## Inline Luau placeholder functions

For a quick local function, provide `handler` instead of `executor` in a setup
script. The event is decoded JSON, so you can examine its fields and return a
custom JSON payload directly:

```luau
--!strict
roto.lambda.fn("greet", {
  timeout = 3,
  handler = function(event: {name: string?}, context)
    return {
      message = "Hello " .. (event.name or "world"),
      received = event,
      requestId = context.request_id,
    }
  end,
})
```

The `event` annotation documents the payload this function expects and lets `--check` catch
a misspelled field. Without it `event` is `any`, because its shape depends on what invokes
the function. `context` is typed by Roto. For events Roto delivers itself, name the type
instead of spelling out its shape: `SqsEvent` for an SQS event source, `S3Event` for an
S3 bucket notification, `EventBridgeEvent` for an EventBridge target and `HttpApiEvent` for an
`--http-routes` route.

Save this as `setup.luau`, start the server, and invoke it:

```sh
roto-server --ephemeral --setup setup.luau
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name greet --cli-binary-format raw-in-base64-out \
  --payload '{"name":"Ada"}' result.json
cat result.json
```

The result contains `message: "Hello Ada"`, the original event, and the invocation
request ID. Context also contains `function_arn`, `region`, `environment`, and
`timeout` in seconds. Return JSON-compatible values; `roto.array({})` represents
an empty array and `roto.null` represents JSON null. Captured locals survive
invocations. Luau errors fail the invocation, and results have a 6 MiB limit.
`print` writes to the server console rather than invocation logs.

For a stateful Luau placeholder, capture a counter outside the callback:

```luau
--!strict
local invocations = 0
roto.lambda.fn("lua-counter", {
  handler = function(event, context)
    invocations = invocations + 1
    return {count = invocations, requestId = context.request_id}
  end,
})
```

This closure retains its state until restart regardless of the RIE `warm` option,
which applies only to container executors. Use `handler` for Luau callbacks; the
serialized `inline` executor ID is internal and is not a user binding option.

Inline functions use the normal invocation and event delivery machinery. Their
bindings are process-local: rerun setup after restarting. Already-persisted jobs
referencing an earlier process's inline handler fail rather than invoke a new
handler accidentally. Callbacks from one setup script execute serially. Luau
deadline checks (at calls and loop iterations) enforce the function timeout for Luau code,
but cannot interrupt a blocking native call. Resource setup
helpers and `roto.call`/`roto.wire` are unavailable inside runtime callbacks.

## Intercept AWS requests from Luau

Setup scripts can also inspect incoming AWS requests and supply custom responses:

```luau
--!strict
roto.hooks.request("lambda", "Invoke", function(request, context)
  if request.input and request.input.preview then
    return {
      status = 200,
      headers = {["content-type"] = "application/json"},
      body = '{"message":"Custom preview response"}',
    }
  end
  return nil -- Invoke the configured function normally.
end)
```

Here `request.input` is the decoded invocation payload and `request.body` is its
raw text. Requests also include service, operation, method, path, raw query, and
headers. Returning `nil` continues normal dispatch. A response requires an HTTP
`status` and string `body`; `headers` is optional. Interception happens before
native validation or execution, so a short-circuit response does not create a
Lambda invocation record. It does appear in the request trace.

Interceptors match an exact service/operation pair, can stub unsupported
operations, and have a one-second Luau instruction deadline. Internal service
calls and setup calls do not trigger them. See [runtime hooks](runtime-hooks.md)
for the full contract and limits, including a setup example you can save locally.

## Command executor

Bind an echo command to function metadata created through the AWS API:

```luau
--!strict
roto.lambda.bind("command-echo", {
  command = {"sh", "-c", "cat"},
})
```

To create the function and configure every command binding option together:

```luau
--!strict
roto.lambda.fn("process-upload", {
  timeout = 10,
  memory_size = 256,
  description = "Run a local Python handler",
  environment = {MODE = "development"},
  executor = {
    command = {"python3", "handler.py"},
    cwd = "./workers", -- Relative to this setup.luau file.
    env = {DEBUG = "1"},
  },
})
```

Place this handler in `workers/handler.py` beside the setup script:

```python
import json
import os
import sys

print("processing upload", file=sys.stderr)
event = json.load(sys.stdin)
json.dump({"received": event, "mode": os.environ["MODE"]}, sys.stdout)
```


`command` is an argv array. Shell syntax requires an explicit `["sh", "-c", "..."]`.
`cwd` is resolved relative to the setup Luau file; it defaults to that file's directory.
Use an absolute executable path when selecting a particular interpreter or local binary.
The example uses `python3` from PATH and has no Python dependencies.

Each invocation starts a new process. It receives one event JSON value on stdin followed by
EOF. Stdout must contain exactly one JSON result (including `null`); stderr contains logs.
Nonzero exit, invalid JSON, excess output, and timeout become Lambda function errors.
The timeout comes from the function configuration (default 3 seconds, range 1–900).
On Unix, timeout/error cleanup kills the process group, including descendants that remain in it.

The process inherits roto's environment, overlaid by the function's `Environment.Variables`,
then the binding's `env`. Roto sets these invocation-specific values last:

- `AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL`, `AWS_LAMBDA_FUNCTION_NAME`
- `ROTO_FUNCTION_ARN`, `ROTO_INVOCATION_ID`
- `ROTO_CLIENT_CONTEXT`, when supplied on synchronous Invoke (base64 as received)

Configure SDK credentials in the inherited environment, function environment, or binding.
Commands execute with roto's OS permissions; this is a local execution backend, with no
container isolation or Lambda memory-limit enforcement.

## HTTP executor

Bind an already running JSON handler:

```luau
--!strict
roto.lambda.bind("http-worker", {
  url = "http://127.0.0.1:8080/invoke",
  headers = {
    ["authorization"] = "Bearer local-demo-token",
    ["x-worker-mode"] = "development",
  },
})
```

Or create the metadata and binding in one step. Headers are optional:

```luau
--!strict
roto.lambda.fn("http-echo", {
  timeout = 10,
  executor = {url = "http://127.0.0.1:8080/echo"},
})
```

The HTTP handler runs separately; this binding does not start it. It receives the
same JSON event that a command reads from stdin.


Roto POSTs the event JSON to `url`, with the configured headers and invocation metadata in
`X-Roto-Invocation-Id`, `X-Roto-Function-Arn`, and `X-Roto-Region`. Synchronous client context,
if supplied, is passed in `X-Roto-Client-Context`. Content-Type is `application/json` unless
explicitly overridden. Redirects are not followed.

A 2xx response must contain one JSON result; an empty body (for example `204 No Content`) is a
`null` result, like a handler that returns nothing. Non-2xx responses, connection errors, invalid
JSON, and timeouts become function errors. Non-2xx response bodies are retained as diagnostic
logs. Stdout, stderr, and HTTP response bodies each have a 6 MiB limit.

## Lambda container images

`container = "my-worker:latest"` names an image already available in your local
Podman or Docker image store. Roto launches that image and sends invocation events
to its Runtime Interface Emulator (RIE). RIE provides the local HTTP invocation
endpoint and connects it to the Lambda runtime/client inside the container. This
binding runs the image locally; it does not deploy a function to AWS.

`warm = true` keeps an idle container available for later invocations, preserving
its initialized runtime and handler state. `warm = false` (the default) starts a
fresh container for each invocation. This option applies only to `container`
executors; HTTP executors use an external server whose lifetime Roto does not manage.

AWS Lambda runtime base images (for example `public.ecr.aws/lambda/python:3.12`)
include the Runtime Interface Emulator (RIE) and Runtime Interface Client (RIC).
AWS OS-only images include RIE but require you to add the runtime/client. Custom
images may be valid for AWS Lambda without including RIE. Roto's `container` executor
requires RIE in the image and an entrypoint that starts it locally on port 8080;
Roto does not inject or mount the emulator. See [AWS's container image requirements](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html).

For a RIE-compatible image, bind its image name:

```luau
--!strict
-- Fresh container for every invocation (the default).
roto.lambda.bind("cold-worker", {container = "my-worker:latest", warm = false})

-- Preserve the runtime process and /tmp between sequential invocations.
roto.lambda.bind("warm-worker", {container = "my-worker:latest", warm = true})

-- Create metadata and a warm binding together.
roto.lambda.fn("image-worker", {
  timeout = 30,
  memory_size = 256,
  environment = {
    AWS_ACCESS_KEY_ID = "testing",
    AWS_SECRET_ACCESS_KEY = "testing",
  },
  executor = {container = "my-worker:latest", warm = true},
})
```

Roto starts the image with Podman when available, or Docker otherwise, publishes container port
8080 to an ephemeral loopback port, discovers that port, and POSTs the event to RIE's
`/2015-03-31/functions/function/invocations` endpoint. With `warm = false` (or omitted),
the container is removed after each invocation. With `warm = true`, an idle container is
reused for the same function and configuration. The image must already exist locally and include a Lambda Runtime Interface Client
and RIE-compatible entrypoint; roto does not build or pull images. Function timeout includes
container startup time, so set a longer timeout for image-based functions. Container stdout and
stderr are returned as invocation logs.

Warm containers retain module globals, initialized SDK clients, and temporary files.
Overlapping invocations use separate containers; each container handles one invocation
at a time. Handler errors can leave the container warm. Timeouts, transport errors,
invalid responses discard the executing container. Rebinding, function
configuration/code updates, deletion, and reset discard affected idle containers;
service shutdown releases the pool. Idle containers expire after five
minutes; at most 32 idle containers are retained across a Lambda service instance.
Warm state is process-local and is not restored after restarting Roto.

Invocation logs include only the new container output. Use the runtime's handler
context for request IDs: `ROTO_INVOCATION_ID` is not injected in warm mode because
process environment variables cannot change between invocations. Client context is
still passed as `ROTO_CLIENT_CONTEXT`; changing it selects a fresh container.
RIE generates the runtime request ID, which can differ from Roto's invocation ID.

### Try warm and cold invocations

Download [setup.luau](https://github.com/vivainio/roto/blob/main/demos/lambda-warm/setup.luau),
[app.py](https://github.com/vivainio/roto/blob/main/demos/lambda-warm/app.py), and
[Containerfile](https://github.com/vivainio/roto/blob/main/demos/lambda-warm/Containerfile)
into an empty directory, then run these commands from that directory. The setup
creates both functions using an image whose Python handler increments a module-level counter:

```sh
podman build -t roto-lambda-warm:latest \
  -f Containerfile .
roto-server --ephemeral --setup setup.luau
```

Use Docker instead of Podman if needed. Invoke each function twice:

```sh
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name warm-counter --cli-binary-format raw-in-base64-out \
  --payload '{"label":"hello"}' result.json
cat result.json
# Repeat: count becomes 2. For cold-counter, count stays 1.
```

The setup is:

```luau
--!strict
roto.lambda.fn("warm-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = true},
})
roto.lambda.fn("cold-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = false},
})
```

See the [container invocation demo](demos.md#invoke-a-lambda-container-image) for a sample
`Containerfile` and Luau setup. If a handler calls AWS APIs against roto, start roto on a host
interface reachable from the container (for example `--host 0.0.0.0`); `AWS_ENDPOINT_URL` is
rewritten to the host alias for the selected container engine.

## Invocation and S3 events

`RequestResponse` waits and returns the JSON result with status 200; execution failures carry
`X-Amz-Function-Error: Unhandled`. `LogType=Tail` returns the final 4 KiB of logs as base64.
`Event` persists a job and returns 202; `DryRun` checks function existence and returns 204.
An invocation requiring execution fails explicitly if no executor is bound.

Use ordinary `PutBucketNotificationConfiguration` with `LambdaFunctionConfigurations` to wire
an S3 bucket. Supported events: object creation by Put, Copy, and CompleteMultipartUpload;
object removal by Delete and DeleteObjects, including delete markers. Prefix/suffix filters
and event wildcards are applied, and keys in the event are URL form-encoded.

S3 persists the event in its own transaction and hands it to Lambda after commit. Both queues
survive restarts in persistent mode. Async Lambda jobs get up to three attempts, with short
local retry delays. One worker processes async invocations sequentially. Delivery is at least
once: a crash between execution and recording the result, or between S3 handoff and removing
its outbox row, can cause duplicates. S3 handoff failures get three attempts and remain visible.

Inspect the latest 100 Lambda invocations (state, attempts, result, logs) and outstanding S3
notifications (including handoff failures):

```sh
curl http://localhost:5070/roto-api/lambda/invocations
curl http://localhost:5070/roto-api/s3/notifications
```

Unsigned inspection uses the default account/us-east-1; signed requests use caller scope.
Reset clears function metadata, queues and invocation history; local executor bindings remain.

Code packages are stored as metadata, not unpacked or executed. Only `$LATEST` is supported;
versions, aliases, Runtime API workers, DLQs/destinations, CloudWatch Logs streams and Insights,
and automatic Kinesis event-source polling are not implemented. Captured executor output is
available through the [limited CloudWatch Logs API](cloudwatch-logs.md). Policies are stored
but not enforced.
Standard SQS queues can invoke these executors through event-source mappings.
[EventBridge rules](eventbridge.md) can also enqueue invocations from custom or S3 events.
Use `--setup setup.luau` for Luau resource setup and executor bindings; see
[Luau setup and SQS delivery](lua.md) for supported options and retry behavior.

## SQS handler configuration in Luau

Create a queue and function together, then enable partial batch failure reporting:

```luau
--!strict
local queue = roto.sqs.queue("orders", {visibility_timeout = 30})
local worker = roto.lambda.fn("process-orders", {
  timeout = 10,
  handler = function(event: SqsEvent, context)
    local failures = {}
    for _, record in ipairs(event.Records) do
      if record.body == "retry" then
        failures[#failures + 1] = {itemIdentifier = record.messageId}
      end
    end
    return {batchItemFailures = roto.array(failures)}
  end,
})
roto.lambda.event_source(queue, worker, {
  batch_size = 5,
  enabled = true,
  report_batch_item_failures = true,
})
```

Messages whose body is `retry` become visible again after the queue visibility
timeout; successful messages are deleted. Replace `handler` with any of the
executor bindings above to use a process, HTTP service, or warm container.
See [SQS delivery behavior](lua.md#sqs-delivery-behavior) for the retry contract.

## Full SDK smoke test

```sh
cargo build -p roto-server
.venv-moto/bin/python tests/smoke-lambda.py
```

This starts a disposable roto and HTTP handler, exercises command and HTTP invocations plus
metadata APIs, then verifies filtered S3 → command → SQS delivery. It requires boto3 (the moto
test virtualenv includes it), and does not change the example config or an existing server.

## Local HTTP API routes

Expose Lambda functions through API Gateway HTTP API v2 proxy events:

Save this function as `setup.luau`:

```luau
--!strict
roto.lambda.fn("process-order", {
  handler = function(event: HttpApiEvent, context)
    return {
      statusCode = 201,
      headers = {["content-type"] = "application/json"},
      body = '{"status":"created"}',
    }
  end,
})
```

Save the following route file as `routes.json` and start
`roto-server --ephemeral --setup setup.luau --http-routes routes.json`.
A `POST /roto-http/orders/42` invokes this handler.

The route file contains a `routes` array. Each route specifies `method`, `path`,
`function` (a Lambda name or full ARN), and optionally `region` (default
`us-east-1`). Functions and executor bindings must already exist; the setup above
provides both. Restart the server to reload the route file.

```json
{"routes":[{"method":"POST","path":"/orders/{id}","function":"process-order"}]}
```

Routes are exposed under `/roto-http`; that prefix is removed from Lambda paths.
Methods include `ANY`. Paths support named parameters (`{id}`) and a terminal
catch-all (`{proxy+}`). Literal paths take precedence over parameter routes,
then non-greedy routes over catch-alls, then specific methods over `ANY`.
Unmatched requests return 404.

The event includes the v2 request context, raw path and query, decoded path
parameters, lowercase headers, comma-joined repeated headers and query values,
cookies, and body. Binary content types and invalid UTF-8 bodies are base64
encoded; text, JSON, XML, and form bodies are strings. The request context uses
local API ID `local`, stage `$default`, and the connection's client IP.

Return `{"statusCode":201,"headers":{"content-type":"text/plain"},"body":"created"}`
for a custom response. `cookies` becomes multiple `Set-Cookie` headers, and
`isBase64Encoded: true` decodes a binary response body. Results without
`statusCode` infer status 200 and content type `application/json`; string results
become the body directly. Lambda invocation failures and malformed proxy responses
return 502 without exposing execution details. Execution history and logs use the
usual Lambda inspection facilities.

This is local route configuration with the
[AWS HTTP API v2 payload contract](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html).
API Gateway management APIs, REST API v1 payloads, deployments, authorizers,
CORS configuration, and custom domains are not implemented. Routes remain loaded
when service state is reset; their functions must be recreated before invocation.
