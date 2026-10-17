# Quick tour of the Luau API

One script, five services, almost no prose. Each block builds on the one before; the
full file is [`examples/walkthrough/setup.luau`](https://github.com/vivainio/roto/blob/main/examples/walkthrough/setup.luau).

```sh
roto-server --ephemeral --setup setup.luau
```

## What lives where in `roto`

Everything a script can do hangs off one global table, `roto`. It helps to sort its
members into four groups:

| Group | Examples | What it is |
| --- | --- | --- |
| **Helpers**, one per service | `roto.sqs.queue`, `roto.s3.bucket`, `roto.lambda.fn`, `roto.dynamodb.table` | Short, hand-written functions that create a resource and hand back a small table (`name`, `arn`, `url`). Use these first. |
| **`roto.aws`**, every other API call | `roto.aws.ssm.PutParameter`, `roto.aws.events.PutRule` | The real AWS operations, named and shaped exactly as in boto3 or the AWS docs: `roto.aws.<service>.<Operation>(input)`. Use it when no helper exists. |
| **Hooks** | `roto.hooks.request`, `roto.hooks.mqtt_connect` | Register callbacks that run later, while the server is serving traffic. |
| **Context and utilities** | `roto.region`, `roto.vars`, `roto.null`, `roto.array`, `roto.json_decode` | Values and small functions for writing the script itself. |

The difference between the first two is the thing to remember. `roto.sqs.queue("jobs")` is
Roto's own convenience API: lowercase names, sensible defaults, and a result you can pass
to the next helper. `roto.aws.sqs.CreateQueue({ QueueName = "jobs" })` is the AWS API
itself: PascalCase names, nothing defaulted for you, and the response is the AWS
response. Both do the same thing to the same in-process service, so mix them freely.

```luau
--!strict
-- Helper: Roto-shaped, returns { name, url, arn }
local jobs = roto.sqs.queue("jobs", { visibility_timeout = 30 })

-- roto.aws: AWS-shaped, returns what boto3 would return
local param = roto.aws.ssm.PutParameter({ Name = "/app/mode", Type = "String", Value = "dev" })
print(param.Version)
```

`roto.aws` calls run inside the server process, with no HTTP and no credentials. Under
`--!strict` each operation has its own input and output type, so a misspelled field is
caught by `roto-server --check`. Two escape hatches sit beside it: `roto.call("ssm",
"PutParameter", input)` is the untyped form of the same call, and `roto.wire` sends a raw
HTTP request for anything else. See [Service access](lua-api.md#service-access) in the
reference.

## Queue and function

```luau
--!strict
local jobs = roto.sqs.queue("jobs", { visibility_timeout = 30 })

local worker = roto.lambda.fn("worker", {
  timeout = 5,
  handler = function(event, context)
    return { got = event }
  end,
})
roto.lambda.event_source(jobs, worker, { batch_size = 5 })
```

## Bucket notifies the queue

```luau
roto.s3.bucket("uploads", {
  notify = { { queue = jobs, events = { "s3:ObjectCreated:*" }, prefix = "in/" } },
})
roto.s3.put("uploads", "in/hello.txt", "hi")   -- fires the notification
```

## Table

```luau
roto.dynamodb.table("results", { hash = "id" })
roto.dynamodb.put("results", { id = "seed", ok = true })
```

## Config and secrets

```luau
roto.aws.ssm.PutParameter({ Name = "/app/mode", Type = "String", Value = "dev" })
roto.aws.secretsmanager.CreateSecret({ Name = "app/db", SecretString = '{"user":"dev"}' })
```

## EventBridge fan-out

```luau
local audit = roto.sqs.queue("audit")
roto.aws.events.PutRule({ Name = "orders", EventPattern = '{"source":["shop"]}' })
roto.aws.events.PutTargets({ Rule = "orders", Targets = {
  { Id = "audit", Arn = audit.arn },
  { Id = "worker", Arn = worker.arn },
} })
```

## Try it

```sh
curl localhost:5070/roto-api/lambda/invocations   # the upload already ran the worker
aws --endpoint-url http://localhost:5070 events put-events \
  --entries '[{"Source":"shop","DetailType":"x","Detail":"{}"}]'
```

The pattern throughout: **helper** (`roto.sqs.queue`, `roto.s3.bucket`, …) where one exists,
`roto.aws.<service>.<Operation>` for everything else, and pass the returned `arn`/`url`
tables straight into the next call.

Next: [Luau scripting guide](lua.md), [API reference](lua-api.md), [Runtime hooks](runtime-hooks.md).
