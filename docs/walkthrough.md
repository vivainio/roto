# Wiring services in Luau

One script, five services, almost no prose. Each block builds on the one before; the
full file is [`examples/walkthrough/setup.luau`](https://github.com/vivainio/roto/blob/main/examples/walkthrough/setup.luau).

```sh
roto-server --ephemeral --setup setup.luau
```

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

Next: [Luau setup](lua.md), [API reference](lua-api.md), [Runtime hooks](runtime-hooks.md).
