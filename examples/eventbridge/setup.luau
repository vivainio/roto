--!strict
local queue = roto.sqs.queue("upload-events")
local handler = roto.lambda.fn("upload-handler", {
    executor = {command = {"sh", "-c", "cat"}}, timeout = 5
})
roto.aws.events.PutRule({
    Name = "s3-uploads",
    EventPattern = '{"source":["aws.s3"],"detail-type":["Object Created"]}',
})
roto.aws.events.PutTargets({
    Rule = "s3-uploads",
    Targets = {
        {Id = "queue", Arn = queue.arn},
        {Id = "lambda", Arn = handler.arn},
    },
})
roto.s3.bucket("uploads", {eventbridge = true})
