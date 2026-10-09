# Local Lambda executors

Start roto with local executor bindings:

```sh
cargo run -p roto-server -- --ephemeral --lambda-executors examples/lambda/executors.json
```

Create function metadata through the usual Lambda API (SDK, CLI, or Terraform), using the
same function name as a binding. Bindings may use full function ARNs for account/region-specific
configuration. A full ARN binding takes precedence over a name binding; bare names apply in
every account/region. No function is created implicitly by the executor file. Restart roto to
load changes to that file. The file is local configuration, never accepted through an AWS API.

For example, with local AWS credentials configured:

```sh
aws --endpoint-url http://localhost:5070 lambda create-function \
  --function-name process-upload --runtime provided.al2023 --handler external \
  --role arn:aws:iam::123456789012:role/local --code '{"ZipFile":""}'
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name process-upload --cli-binary-format raw-in-base64-out \
  --payload '{"hello":"world"}' result.json
```

## Command contract

`command` is an argv array. Shell syntax requires an explicit `["sh", "-c", "..."]`.
`cwd` is resolved relative to the executor config file; it defaults to that file's directory.
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

## HTTP contract

Roto POSTs the event JSON to `url`, with the configured headers and invocation metadata in
`X-Roto-Invocation-Id`, `X-Roto-Function-Arn`, and `X-Roto-Region`. Synchronous client context,
if supplied, is passed in `X-Roto-Client-Context`. Content-Type is `application/json` unless
explicitly overridden. Redirects are not followed.

A 2xx response must contain one JSON result. Non-2xx responses, connection errors, invalid
JSON, and timeouts become function errors. Non-2xx response bodies are retained as diagnostic
logs. Stdout, stderr, and HTTP response bodies each have a 6 MiB limit.

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
versions, aliases, Runtime API workers, DLQs/destinations, CloudWatch logging, and automatic
Kinesis event-source polling is not implemented. Policies are stored but not enforced.
Standard SQS queues can invoke these executors through event-source mappings.
Use `--setup setup.lua` for Lua resource setup and executor bindings; see
[Lua setup and SQS delivery](../lua/README.md) for supported options and retry behavior.

## Full SDK smoke test

```sh
cargo build -p roto-server
.venv-moto/bin/python scripts/smoke-lambda.py
```

This starts a disposable roto and HTTP handler, exercises command and HTTP invocations plus
metadata APIs, then verifies filtered S3 → command → SQS delivery. It requires boto3 (the moto
test virtualenv includes it), and does not change the example config or an existing server.
