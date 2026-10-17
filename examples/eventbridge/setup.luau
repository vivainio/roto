--!strict
local queue = roto.sqs.queue("events")
local handler = roto.lambda.fn("events", {
    executor = {command = {"sh", "-c", "cat"}}, timeout = 5
})
roto.aws.events.PutRule({
    Name = "uploads",
    EventPattern = '{"source":["aws.s3"],"detail-type":["Object Created"]}',
})
roto.aws.events.PutTargets({
    Rule = "uploads",
    Targets = {
        {Id = "queue", Arn = queue.arn},
        {Id = "lambda", Arn = handler.arn},
    },
})
roto.s3.bucket("uploads", {eventbridge = true})
