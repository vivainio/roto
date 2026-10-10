-- Disposable test/demo data: roto-server --ephemeral --setup examples/demo/setup.lua
-- Uses the installed services' normal APIs; no database writes or network setup calls.
local tags = { purpose = "demo-fixtures", owner = "local" }
local function s3(method, path, body, query, headers)
  return roto.request("s3", method, path, { body = body, query = query, headers = headers })
end
local function query(service, body)
  return roto.request(service, "POST", "/", {
    headers = { ["content-type"] = "application/x-www-form-urlencoded" }, body = body,
  })
end
local function checked(service, operation, input)
  local result = roto.call(service, operation, input)
  assert(not result.FailedEntryCount or result.FailedEntryCount == 0, operation .. " had failed entries")
  return result
end

-- Browse folders, text, structured data, binary data, empty objects, and multiple pages.
for _, bucket in ipairs({ "demo-assets", "demo-archive", "demo-empty" }) do s3("PUT", "/" .. bucket) end
s3("PUT", "/demo-assets", [[<Tagging><TagSet><Tag><Key>purpose</Key><Value>demo-fixtures</Value></Tag></TagSet></Tagging>]], "tagging")
s3("PUT", "/demo-assets/README.txt", "Welcome to the roto inspection demo.\nTry previews, downloads, search, and pagination.\n")
s3("PUT", "/demo-assets/config/app.json", { app = "demo-shop", region = roto.region, features = { search = true, checkout = false } }, nil, { ["content-type"] = "application/json" })
s3("PUT", "/demo-assets/reports/orders.csv", "order_id,customer,total,status\n1001,Ada,42.50,paid\n1002,Linus,19.99,pending\n", nil, { ["content-type"] = "text/csv" })
s3("PUT", "/demo-assets/pages/index.html", "<!doctype html><h1>Demo shop</h1><p>HTML is previewed as text.</p>", nil, { ["content-type"] = "text/html" })
s3("PUT", "/demo-assets/notes/a%20%2B%20space%20%E2%98%95.txt", "Keys can contain spaces, plus signs, and Unicode.\n")
s3("PUT", "/demo-assets/empty.txt", "")
s3("PUT", "/demo-assets/binary/sample.bin", string.char(0, 1, 2, 127, 128, 254, 255), nil, { ["content-type"] = "application/octet-stream" })
s3("PUT", "/demo-assets/reports/large.txt", string.rep("A bounded preview shows the first 64 KiB only.\n", 2000))
for i = 1, 55 do
  s3("PUT", string.format("/demo-assets/catalog/product-%03d.json", i), {
    id = string.format("SKU-%03d", i), name = "Demo product " .. i, price = i * 3.5, in_stock = i % 4 ~= 0,
  }, nil, { ["content-type"] = "application/json", ["x-amz-meta-fixture"] = "catalog" })
end
s3("PUT", "/demo-archive", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", "versioning")
s3("PUT", "/demo-archive/release.json", { version = 1, status = "draft" })
s3("PUT", "/demo-archive/release.json", { version = 2, status = "published" })
s3("PUT", "/demo-archive/removed.txt", "This previous version is still downloadable.")
s3("DELETE", "/demo-archive/removed.txt")

-- Items include nested documents, lists, sets, booleans, and nulls.
for _, name in ipairs({ "demo-orders", "demo-empty-table" }) do
  roto.call("dynamodb", "CreateTable", {
    TableName = name, BillingMode = "PAY_PER_REQUEST",
    KeySchema = { { AttributeName = "order_id", KeyType = "HASH" } },
    AttributeDefinitions = { { AttributeName = "order_id", AttributeType = "S" } },
    Tags = { { Key = "purpose", Value = "demo-fixtures" } },
  })
end
for i, customer in ipairs({ "Ada", "Linus", "Grace", "Ken", "Margaret", "Barbara" }) do
  roto.call("dynamodb", "PutItem", { TableName = "demo-orders", Item = {
    order_id = { S = "ORDER-" .. (1000 + i) }, customer = { S = customer }, total = { N = tostring(i * 19.5) },
    paid = { BOOL = i % 2 == 0 }, shipping = { M = { city = { S = "Helsinki" }, priority = { BOOL = false } } },
    labels = { SS = { "demo", "fixtures" } }, notes = { NULL = true },
    lines = { L = { { M = { sku = { S = "SKU-001" }, quantity = { N = "2" } } } } },
  } })
end

-- Keep one queue populated; its mapping is disabled so browsing remains useful.
local orders = roto.sqs.queue("demo-orders", { visibility_timeout = 30, tags = tags })
local dead = roto.sqs.queue("demo-dead-letter", { tags = tags })
local events = roto.sqs.queue("demo-events", { tags = tags })
for i = 1, 3 do
  roto.call("sqs", "SendMessage", { QueueUrl = orders.url, MessageBody = string.format('{"order_id":"ORDER-%d","action":"ship"}', 1000 + i),
    MessageAttributes = { source = { DataType = "String", StringValue = "demo-shop" } },
  })
end
roto.call("sqs", "SendMessage", { QueueUrl = dead.url, MessageBody = '{"order_id":"ORDER-0999","error":"payment declined"}' })

-- Real invocations with readable results and logs, including an intentional failure.
local echo = roto.lambda.function_("demo-echo", {
  description = "Echoes events and records a demo log", timeout = 5, environment = { MODE = "demo" },
  executor = { command = { "sh", "-c", "echo 'demo-echo: processing event' >&2; cat" } },
})
local fail = roto.lambda.function_("demo-fail", {
  description = "Intentional failure for inspecting error logs", timeout = 5,
  executor = { command = { "sh", "-c", "echo 'Demo failure: payment service unavailable' >&2; exit 1" } },
})
roto.lambda.event_source(orders, echo, { enabled = false, batch_size = 3 })
roto.request("lambda", "POST", "/2015-03-31/functions/" .. echo.name .. "/invocations", { body = { greeting = "Hello from Lua", orders = 6 } })
roto.request("lambda", "POST", "/2015-03-31/functions/" .. fail.name .. "/invocations", { body = { order_id = "ORDER-0999" } })

-- EventBridge deliveries complete after startup, while the queue retains the events.
roto.call("events", "CreateEventBus", { Name = "demo-shop" })
roto.call("events", "PutRule", { EventBusName = "demo-shop", Name = "demo-orders", Description = "Route demo orders to SQS and Lambda",
  EventPattern = '{"source":["demo.shop"]}', State = "ENABLED" })
checked("events", "PutTargets", { EventBusName = "demo-shop", Rule = "demo-orders", Targets = {
  { Id = "queue", Arn = events.arn }, { Id = "echo", Arn = echo.arn },
} })
checked("events", "PutEvents", { Entries = {
  { EventBusName = "demo-shop", Source = "demo.shop", DetailType = "Order created", Detail = '{"order_id":"ORDER-1001","total":19.5}' },
  { EventBusName = "demo-shop", Source = "demo.shop", DetailType = "Order created", Detail = '{"order_id":"ORDER-1002","total":39}' },
} })
-- Missing destination intentionally leaves a pending, then failed S3 handoff for inspection.
s3("PUT", "/demo-archive", string.format([[<NotificationConfiguration><CloudFunctionConfiguration><Id>demo-missing-target</Id><CloudFunction>arn:aws:lambda:%s:%s:function:demo-missing</CloudFunction><Event>s3:ObjectCreated:*</Event></CloudFunctionConfiguration></NotificationConfiguration>]], roto.region, roto.account_id), "notification")
s3("PUT", "/demo-archive/undelivered.json", { message = "Missing Lambda target demonstrates delivery retries" })

-- Configuration versions and relationships across the remaining services.
roto.call("ssm", "PutParameter", { Name = "/demo/shop/environment", Type = "String", Value = "development" })
roto.call("ssm", "PutParameter", { Name = "/demo/shop/environment", Type = "String", Value = "demo", Overwrite = true })
roto.call("ssm", "PutParameter", { Name = "/demo/shop/features", Type = "StringList", Value = "browse,search,download" })
roto.call("ssm", "PutParameter", { Name = "/demo/shop/token", Type = "SecureString", Value = "fake-demo-token" })
roto.call("secretsmanager", "CreateSecret", { Name = "demo/shop/database", Description = "Fake credentials for UI fixtures",
  SecretString = '{"username":"demo","password":"not-a-real-password"}', Tags = { { Key = "purpose", Value = "demo-fixtures" } } })
roto.call("secretsmanager", "PutSecretValue", { SecretId = "demo/shop/database", SecretString = '{"username":"demo","password":"rotated-demo-password"}' })
query("iam", "Action=CreateUser&Version=2010-05-08&UserName=demo-user&Path=%2Fdemo%2F")
query("iam", "Action=CreateRole&Version=2010-05-08&RoleName=demo-reader&Description=Demo+read+role&AssumeRolePolicyDocument=%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%5D%7D")
query("sns", "Action=CreateTopic&Version=2010-03-31&Name=demo-updates")
query("sns", "Action=Subscribe&Version=2010-03-31&TopicArn=arn%3Aaws%3Asns%3A" .. roto.region .. "%3A" .. roto.account_id .. "%3Ademo-updates&Protocol=sqs&Endpoint=arn%3Aaws%3Asqs%3A" .. roto.region .. "%3A" .. roto.account_id .. "%3Ademo-events")
print("Demo resources ready: " .. roto.endpoint .. "/roto-api/")
