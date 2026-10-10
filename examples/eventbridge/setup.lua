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
