-- Run with: roto-server --ephemeral --setup examples/lua/setup.lua
local jobs = roto.sqs.queue("jobs", { visibility_timeout = 30 })
local process_job = roto.lambda.function_("process-job", {
  timeout = 10,
  executor = { command = { "sh", "./process_jobs.sh" } },
})
roto.lambda.event_source(jobs, process_job, { batch_size = 10 })

-- Optional HTTP pipeline: run the endpoint before sending messages.
-- local web_jobs = roto.sqs.queue("web-jobs")
-- local web_handler = roto.lambda.function_("web-handler", {
--   executor = { url = "http://localhost:8080/jobs" },
-- })
-- roto.lambda.event_source(web_jobs, web_handler)
print("Send messages to " .. jobs.url)
