# EventBridge

Run `cargo run -p roto-server -- --ephemeral --setup examples/eventbridge/setup.lua`.
The setup creates an `uploads` bucket, enables EventBridge notifications, and routes
`aws.s3` / `Object Created` events to an SQS queue and a local shell Lambda.
Upload an object through your usual SDK pointed at `http://localhost:5070`.

The setup script is:

```lua
local queue = roto.sqs.queue("events")
local handler = roto.lambda.function_("events", {
    executor = {command = {"sh", "-c", "cat"}}, timeout = 5
})
roto.call("events", "PutRule", {
    Name = "uploads", EventPattern = '{"source":["aws.s3"],"detail-type":["Object Created"]}'
})
roto.call("events", "PutTargets", {
    Rule = "uploads", Targets = {
        {Id = "queue", Arn = queue.arn},
        {Id = "lambda", Arn = handler.arn}
    }
})
roto.request("s3", "PUT", "/uploads")
roto.request("s3", "PUT", "/uploads", {query = "notification",
    body = "<NotificationConfiguration><EventBridgeConfiguration/></NotificationConfiguration>"})
```

For example, upload and inspect the resulting invocation:

```sh
aws --endpoint-url http://localhost:5070 s3api put-object \
  --bucket uploads --key hello.txt --body ./hello.txt
curl http://localhost:5070/roto-api/lambda/invocations
```

Create `hello.txt` first and configure local SDK credentials and region `us-east-1`.

Use `roto.call("events", operation, input)` (or `"eventbridge"`) for EventBridge APIs.
The default bus always exists; `CreateEventBus` creates custom buses. `PutRule`
accepts JSON event patterns; `PutTargets` accepts same-account, same-region Lambda
and SQS ARNs. `PutEvents` returns per-entry IDs or errors. Rules and deliveries
persist across restarts. Disabled rules receive no new events.

Supported pattern features: exact alternatives, nested fields, event arrays,
`prefix`, `suffix`, `equals-ignore-case`, `anything-but` with scalar/list values,
`numeric`, `exists`, and `$or`. Unsupported operators return an error.
Target `Input` supplies static JSON; `InputPath` selects `$` or dot-separated object
keys. SQS `MessageGroupId` is supported; FIFO queues require content-based deduplication.

Delivery attempts run asynchronously, with at most three attempts and short local
retry delays. `GET /roto-api/events/deliveries` shows the last 100 deliveries in the
current account/region. A successful Lambda delivery means the invocation was
queued; inspect `/roto-api/lambda/invocations` for execution results. Delivery can
repeat after a crash between target handoff and recording success.

This initial implementation excludes schedules, input transformers, permissions,
cross-account/region targets, archives, replays, partner buses and other target
services. S3 emits object creation/deletion events for supported local operations,
using the [AWS S3 EventBridge envelope](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ev-events.html).

Verification: after `cargo build -p roto-server`, run
`.venv-moto/bin/python scripts/smoke-eventbridge.py`.
