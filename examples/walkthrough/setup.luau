--!strict
local jobs = roto.sqs.queue("jobs", { visibility_timeout = 30 })

roto.dynamodb.table("results", { hash = "id" })

local worker = roto.lambda.fn("worker", {
  timeout = 5,
  handler = function(event, context)
    return { got = event }
  end,
})
roto.lambda.event_source(jobs, worker, { batch_size = 5 })

roto.s3.bucket("uploads", {
  notify = { { queue = jobs, events = { "s3:ObjectCreated:*" }, prefix = "in/" } },
})

roto.aws.ssm.PutParameter({ Name = "/app/mode", Type = "String", Value = "dev" })
roto.aws.secretsmanager.CreateSecret({ Name = "app/db", SecretString = '{"user":"dev"}' })

local audit = roto.sqs.queue("audit")
roto.aws.events.PutRule({ Name = "orders", EventPattern = '{"source":["shop"]}' })
roto.aws.events.PutTargets({ Rule = "orders", Targets = {
  { Id = "audit", Arn = audit.arn }, { Id = "worker", Arn = worker.arn },
} })

roto.dynamodb.put("results", { id = "seed", ok = true })
roto.s3.put("uploads", "in/hello.txt", "hi") -- fires the notification
