--!strict
-- Disposable test/demo data: roto-server --ephemeral --setup examples/demo/setup.luau
-- Uses the installed services' normal APIs; no database writes or network setup calls.
local tags = { purpose = "demo-fixtures", owner = "local" }
local function checked(service: string, operation: string, input: { [string]: any }): any
  local result = roto.call(service, operation, input)
  assert(not result.FailedEntryCount or result.FailedEntryCount == 0, operation .. " had failed entries")
  return result
end

-- Browse folders, text, structured data, binary data, empty objects, and multiple pages.
for _, bucket in ipairs({ "demo-assets", "demo-archive", "demo-empty" }) do roto.aws.s3.CreateBucket({ Bucket = bucket }) end
roto.aws.s3.PutBucketTagging({ Bucket = "demo-assets", Tagging = { TagSet = { { Key = "purpose", Value = "demo-fixtures" } } } })
roto.s3.put("demo-assets", "README.txt", "Welcome to the roto inspection demo.\nTry previews, downloads, search, and pagination.\n")
roto.s3.put("demo-assets", "config/app.json", string.format('{"app":"demo-shop","region":"%s","features":{"search":true,"checkout":false}}', roto.region), { content_type = "application/json" })
roto.s3.put("demo-assets", "reports/orders.csv", "order_id,customer,total,status\n1001,Ada,42.50,paid\n1002,Linus,19.99,pending\n", { content_type = "text/csv" })
roto.s3.put("demo-assets", "pages/index.html", "<!doctype html><h1>Demo shop</h1><p>HTML is previewed as text.</p>", { content_type = "text/html" })
roto.s3.put("demo-assets", "notes/a + space \u{2615}.txt", "Keys can contain spaces, plus signs, and Unicode.\n")
roto.s3.put("demo-assets", "empty.txt", "")
roto.s3.put("demo-assets", "binary/sample.bin", string.char(0, 1, 2, 127, 128, 254, 255), { content_type = "application/octet-stream" })
roto.s3.put("demo-assets", "reports/large.txt", string.rep("A bounded preview shows the first 64 KiB only.\n", 2000))
for i = 1, 55 do
  roto.s3.put("demo-assets", string.format("catalog/product-%03d.json", i), string.format(
    '{"id":"SKU-%03d","name":"Demo product %d","price":%s,"in_stock":%s}', i, i, tostring(i * 3.5), tostring(i % 4 ~= 0)
  ), { content_type = "application/json", metadata = { fixture = "catalog" } })
end
roto.aws.s3.PutBucketVersioning({ Bucket = "demo-archive", VersioningConfiguration = { Status = "Enabled" } })
roto.s3.put("demo-archive", "release.json", '{"version":1,"status":"draft"}')
roto.s3.put("demo-archive", "release.json", '{"version":2,"status":"published"}')
roto.s3.put("demo-archive", "removed.txt", "This previous version is still downloadable.")
roto.aws.s3.DeleteObject({ Bucket = "demo-archive", Key = "removed.txt" })

-- Items include nested documents, lists, sets, booleans, and nulls.
for _, name in ipairs({ "demo-orders", "demo-empty-table" }) do
  roto.aws.dynamodb.CreateTable({
    TableName = name, BillingMode = "PAY_PER_REQUEST",
    KeySchema = { { AttributeName = "order_id", KeyType = "HASH" } },
    AttributeDefinitions = { { AttributeName = "order_id", AttributeType = "S" } },
    Tags = { { Key = "purpose", Value = "demo-fixtures" } },
  })
end
for i, customer in ipairs({ "Ada", "Linus", "Grace", "Ken", "Margaret", "Barbara" }) do
  roto.aws.dynamodb.PutItem({ TableName = "demo-orders", Item = {
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
  roto.aws.sqs.SendMessage({ QueueUrl = orders.url, MessageBody = string.format('{"order_id":"ORDER-%d","action":"ship"}', 1000 + i),
    MessageAttributes = { source = { DataType = "String", StringValue = "demo-shop" } },
  })
end
roto.aws.sqs.SendMessage({ QueueUrl = dead.url, MessageBody = '{"order_id":"ORDER-0999","error":"payment declined"}' })

-- Real invocations with readable results and logs, including an intentional failure.
local echo = roto.lambda.fn("demo-echo", {
  description = "Echoes events and records a demo log", timeout = 5, environment = { MODE = "demo" },
  executor = { command = { "sh", "-c", "echo 'demo-echo: processing event' >&2; cat" } },
})
local fail = roto.lambda.fn("demo-fail", {
  description = "Intentional failure for inspecting error logs", timeout = 5,
  executor = { command = { "sh", "-c", "echo 'Demo failure: payment service unavailable' >&2; exit 1" } },
})
roto.lambda.event_source(orders, echo, { enabled = false, batch_size = 3 })
roto.aws.lambda.Invoke({ FunctionName = echo.name, Payload = roto.encode_base64('{"greeting":"Hello from Lua","orders":6}') })
roto.aws.lambda.Invoke({ FunctionName = fail.name, Payload = roto.encode_base64('{"order_id":"ORDER-0999"}') })

-- EventBridge deliveries complete after startup, while the queue retains the events.
roto.aws.events.CreateEventBus({ Name = "demo-shop" })
roto.aws.events.PutRule({ EventBusName = "demo-shop", Name = "demo-orders", Description = "Route demo orders to SQS and Lambda",
  EventPattern = '{"source":["demo.shop"]}', State = "ENABLED" })
checked("events", "PutTargets", { EventBusName = "demo-shop", Rule = "demo-orders", Targets = {
  { Id = "queue", Arn = events.arn }, { Id = "echo", Arn = echo.arn },
} })
checked("events", "PutEvents", { Entries = {
  { EventBusName = "demo-shop", Source = "demo.shop", DetailType = "Order created", Detail = '{"order_id":"ORDER-1001","total":19.5}' },
  { EventBusName = "demo-shop", Source = "demo.shop", DetailType = "Order created", Detail = '{"order_id":"ORDER-1002","total":39}' },
} })
-- Missing destination intentionally leaves a pending, then failed S3 handoff for inspection.
roto.s3.bucket("demo-archive", {
  notify = { { lambda = string.format("arn:aws:lambda:%s:%s:function:demo-missing", roto.region, roto.account_id), events = { "s3:ObjectCreated:*" }, id = "demo-missing-target" } },
  skip_validation = true,
})
roto.s3.put("demo-archive", "undelivered.json", '{"message":"Missing Lambda target demonstrates delivery retries"}')

-- Configuration versions and relationships across the remaining services.
roto.aws.ssm.PutParameter({ Name = "/demo/shop/environment", Type = "String", Value = "development" })
roto.aws.ssm.PutParameter({ Name = "/demo/shop/environment", Type = "String", Value = "demo", Overwrite = true })
roto.aws.ssm.PutParameter({ Name = "/demo/shop/features", Type = "StringList", Value = "browse,search,download" })
roto.aws.ssm.PutParameter({ Name = "/demo/shop/token", Type = "SecureString", Value = "fake-demo-token" })
roto.aws.secretsmanager.CreateSecret({ Name = "demo/shop/database", Description = "Fake credentials for UI fixtures",
  SecretString = '{"username":"demo","password":"not-a-real-password"}', Tags = { { Key = "purpose", Value = "demo-fixtures" } } })
roto.aws.secretsmanager.PutSecretValue({ SecretId = "demo/shop/database", SecretString = '{"username":"demo","password":"rotated-demo-password"}' })
roto.call("iam", "CreateUser", { UserName = "demo-user", Path = "/demo/" })
roto.aws.iam.CreateRole({ RoleName = "demo-reader", Description = "Demo read role", AssumeRolePolicyDocument = '{"Version":"2012-10-17","Statement":[]}' })
local updates = roto.call("sns", "CreateTopic", { Name = "demo-updates" })
roto.aws.sns.Subscribe({ TopicArn = updates.TopicArn, Protocol = "sqs", Endpoint = "arn:aws:sqs:" .. roto.region .. ":" .. roto.account_id .. ":demo-events" })
-- CloudFormation owns a second resource set, with relationships and outputs to browse.
local function stack(name: string, template: string)
  roto.aws.cloudformation.CreateStack({ StackName = name, TemplateBody = template, Tags = { { Key = "purpose", Value = "demo-fixtures" } } })
end
stack("demo-messaging", [[{
  "Description": "Demo queue and subscribed topic managed by CloudFormation",
  "Resources": {
    "Queue": { "Type": "AWS::SQS::Queue", "Properties": { "QueueName": "demo-stack-jobs", "VisibilityTimeout": 30 } },
    "Topic": { "Type": "AWS::SNS::Topic", "Properties": {
      "TopicName": "demo-stack-updates", "DisplayName": "Stack updates",
      "Subscription": [{ "Protocol": "sqs", "Endpoint": { "Fn::GetAtt": ["Queue", "Arn"] } }]
    } }
  },
  "Outputs": {
    "QueueURL": { "Value": { "Ref": "Queue" }, "Description": "Send stack jobs here" },
    "TopicARN": { "Value": { "Ref": "Topic" } }
  }
}]])
stack("demo-storage", [[{
  "Description": "Demo versioned bucket and inventory table managed by CloudFormation",
  "Resources": {
    "Bucket": { "Type": "AWS::S3::Bucket", "Properties": {
      "BucketName": "demo-stack-uploads", "VersioningConfiguration": { "Status": "Enabled" }
    } },
    "Inventory": { "Type": "AWS::DynamoDB::Table", "Properties": {
      "TableName": "demo-stack-inventory", "BillingMode": "PAY_PER_REQUEST",
      "KeySchema": [{ "AttributeName": "sku", "KeyType": "HASH" }],
      "AttributeDefinitions": [{ "AttributeName": "sku", "AttributeType": "S" }]
    } }
  },
  "Outputs": {
    "BucketName": { "Value": { "Ref": "Bucket" } },
    "TableName": { "Value": { "Ref": "Inventory" } }
  }
}]])
print("Demo resources ready: " .. roto.endpoint .. "/roto-api/")
