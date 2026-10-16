-- roto-server --ephemeral --setup examples/hooks/setup.luau
local jobs = roto.sqs.queue("jobs", {visibility_timeout = 5})
local worker = roto.lambda.fn("quick-worker", {
  timeout = 5,
  handler = function(event, context)
    return {ok = true, event = event, requestId = context.request_id}
  end,
})
roto.lambda.event_source(jobs, worker)

-- Return nil to let Roto handle the request normally.
local calls = 0
roto.hooks.request("sqs", "SendMessage", function(request, context)
  calls = calls + 1
  if request.input and request.input.MessageBody == "fail" then
    return {
      status = 503,
      headers = {["content-type"] = "application/x-amz-json-1.0"},
      body = '{"__type":"ServiceUnavailable","message":"Injected failure"}',
    }
  end
  return nil
end)
