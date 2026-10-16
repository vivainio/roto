# Lua API reference

This chapter is the single reference for every Roto-provided Lua symbol. Roto embeds
Lua 5.4; no separate Lua installation is required. Standard Lua libraries are
available. Scripts are trusted local code running with the server's OS permissions.
For a guided introduction see [Lua setup](lua.md); for worked callback examples see
[Runtime hooks](runtime-hooks.md). Editor completion and checks for every symbol here
are available through the [Lua type stubs](lua-types.md).

There are two separate environments with different `roto` tables:

- **Setup environment**: `--setup setup.lua` and `POST /roto-api/lua`. One shared VM that
  creates resources, seeds services and registers runtime callbacks.
- **Service-module environment**: `--lua-services bundle`. Each module gets its own VM and
  transactional storage. See [Service modules](#service-modules).

Everything before the Service modules section describes the setup environment.

## Symbol index

Availability: **setup** = startup and live scripts only (calling it from a callback
raises an error); **any** = also safe inside runtime callbacks.

| Symbol | Returns | Availability | Section |
| --- | --- | --- | --- |
| `roto.version`, `roto.api_version` | string, integer | any | [Context](#context-and-values) |
| `roto.account_id`, `roto.region`, `roto.endpoint` | strings | any | [Context](#context-and-values) |
| `roto.vars` | table of strings | any | [Context](#context-and-values) |
| `roto.null`, `roto.array(table)` | JSON null, marked array | any | [Context](#context-and-values) |
| `roto.json_decode(text)` | decoded value (raises on bad JSON) | any | [Reading responses](#reading-responses) |
| `roto.encode_base64(text)`, `roto.decode_base64(text)` | base64 string, decoded string (raise on invalid base64) | any | [Context](#context-and-values) |
| `roto.sqs.queue(name, options?)` | `{name, url, arn}` | setup | [SQS](#sqs) |
| `roto.s3.bucket(name, options?)` | `{name, arn}` | setup | [S3](#s3) |
| `roto.s3.put(bucket, key, body, options?)` | `{bucket, key}` | setup | [S3](#s3) |
| `roto.s3.link(bucket, key, file, options?)` | `{bucket, key}` | setup | [S3](#s3) |
| `roto.s3.link_dir(bucket, prefix, dir, options?)` | `{bucket, prefix, count}` | setup | [S3](#s3) |
| `roto.dynamodb.table(name, options)` | `{name, arn}` | setup | [DynamoDB](#dynamodb) |
| `roto.dynamodb.put(table, item)` | `{table}` | setup | [DynamoDB](#dynamodb) |
| `roto.lambda.fn(name, options)` | `{name, arn}` | setup | [Lambda](#lambda) |
| `roto.lambda.bind(name, executor)` | `{name}` | setup | [Lambda](#lambda) |
| `roto.lambda.event_source(queue, function, options?)` | AWS mapping configuration | setup | [Lambda](#lambda) |
| `roto.call(service, operation, input?)` | decoded AWS response | setup | [Service access](#service-access) |
| `roto.wire(service, method, path, options?)` | `{status, headers, body}` | setup | [Service access](#service-access) |
| `roto.hooks.request(service, operation, callback)` | nothing | setup | [Callbacks](#runtime-callbacks) |
| `roto.hooks.mqtt_connect(cb)`, `roto.hooks.mqtt_subscribe(cb)` | nothing | setup | [Callbacks](#runtime-callbacks) |

Return shapes differ by design: the helpers return small lowercase tables, while
`roto.call` and `event_source` return AWS-shaped (PascalCase) responses unchanged.

## Context and values

```sh
roto-server --ephemeral --port 5071 --setup setup.lua
```

Setup runs once on startup, after service initialization and before the listener
and delivery workers start. the script starts in the `--account-id`
account and `us-east-1`. The script is the authority for its scope: call
`roto.set_region("eu-west-1")` and `roto.set_account("123456789012")` to re-scope every
later setup call (`set_account` also forces that account for every request, whatever the
access key says, which suits single-account apps), and read `roto.region` / `roto.account_id` for the current values. To
take a value from the command line, pass `--set REGION=eu-west-1` and use
`roto.set_region(roto.vars.REGION)`. `require("module")` searches beside the
setup script first, including `module.lua` and `module/init.lua`.
`--set KEY=VALUE` (repeatable) passes string parameters to the script as `roto.vars.KEY`.

| Value or function | Meaning |
| --- | --- |
| `roto.version` | Server version string (informational) |
| `roto.api_version` | Integer Lua API level; bumped only on breaking changes (currently 2: version 1 had `roto.lambda.function_`, `roto.request`, `roto.intercept_request` and `roto.mqtt.on_*`, now `roto.lambda.fn`, `roto.wire` and `roto.hooks.*`). Additive changes keep it, so feature-detect new functions with `if roto.s3.link_dir then` |
| `roto.account_id` | Setup account ID string |
| `roto.region` | Setup region string |
| `roto.endpoint` | Server base URL string |
| `roto.vars` | Table of strings from repeatable `--set KEY=VALUE` flags; unset keys are `nil` |
| `roto.null` | Explicit JSON null |
| `roto.array(table)` | Marks and returns the supplied table as a JSON array |
| `roto.json_decode(text)` | Decodes a JSON string; malformed JSON raises an error |
| `roto.encode_base64(text)`, `roto.decode_base64(text)` | Standard base64 of a string, and back; both are binary-safe, and invalid base64 raises an error |

```lua
local settings = {
  region = roto.region,
  optional = roto.null,
  destinations = roto.array({}),
}
```

JSON objects become Lua tables and JSON arrays become marked sequence tables.
Use string keys for objects and consecutive integer keys starting at 1 for
arrays. An unmarked empty table encodes as `{}`; `roto.array({})` encodes as `[]`.
`nil` omits an object field; use `roto.null` to keep a field with a null value.
Return JSON-compatible values, without cycles or functions.

All APIs except `roto.array`, `roto.null`, `roto.json_decode`, `roto.encode_base64`, `roto.decode_base64` and the context values are startup-only. Calling them
inside a Lambda, interceptor, or MQTT callback raises an error. Setup failures
stop startup and include source locations; earlier successful service changes
are not rolled back as a group. Bindings and callbacks are local configuration:
rerun setup after restart. Resource reset retains registrations and bindings,
but does not rerun setup.

## Resource helpers

Helpers create a missing resource or reconcile an existing one, so rerunning setup is
safe. Omitting a resource from the script does not delete it. Unknown options raise
an error. Services without a helper are seeded through [Service access](#service-access).

### SQS
#### `roto.sqs.queue(name, options?)`

Creates a missing queue or updates an existing queue. Returns
`{name = string, url = string, arn = string}`.

| Option | Type | Behavior when omitted |
| --- | --- | --- |
| `visibility_timeout` | Integer seconds | Preserve existing value or use service default on creation |
| `attributes` | Table of string values | No attribute changes |
| `tags` | Table of string values | No tag changes |

`visibility_timeout` overrides `attributes.VisibilityTimeout` when both are
supplied. Attributes and tags are merged; existing messages remain. Unknown
options raise an error.

```lua
local jobs = roto.sqs.queue("jobs", {
  visibility_timeout = 30,
  attributes = {MessageRetentionPeriod = "86400"},
  tags = {owner = "development"},
})
roto.call("sqs", "SendMessage", {
  QueueUrl = jobs.url, MessageBody = "hello",
})
```

### S3

Feature-detect the bucket and put helpers with `if roto.s3.bucket then`.

#### `roto.s3.bucket(name, options?)`

Creates the bucket if it is missing and applies the given configuration. Running setup
again is safe: an existing bucket is kept, and each option that you pass replaces that
configuration. An option you leave out is left unchanged. Returns `{name, arn}`.

| Option | Meaning |
| --- | --- |
| `cors` | List of rules `{methods, origins, headers?, expose?, max_age?, id?}`. Replaces the CORS configuration; `{}` removes it. |
| `notify` | List of destinations, see below. Replaces the whole notification configuration, so one call must list every destination; `{}` removes it. |
| `skip_validation` | `true` skips the check that queue destinations exist (it sends `x-amz-skip-destination-validation`). |

A `notify` entry is `{queue | topic | lambda, events, prefix?, suffix?, id?}`. The
destination is an ARN string or a helper table with an `arn` field, such as the result of
`roto.sqs.queue`. Give exactly one destination per entry and at least one event. A queue
must exist when the bucket is configured unless `skip_validation` is set. Queue and
Lambda destinations are delivered; topic destinations are stored but not delivered. See
[Object notifications](s3.md#object-notifications).

```lua
local events = roto.sqs.queue("s3-events")
local scanner = roto.sqs.queue("scanner")
roto.s3.bucket("attachments", {
  cors = {{ methods = {"GET", "PUT"}, origins = {"*"}, headers = {"*"}, max_age = 3500 }},
  notify = {
    { queue = events, events = {"s3:ObjectTagging:Put"} },
    { queue = scanner, events = {"s3:ObjectCreated:*"}, prefix = "unscanned/" },
  },
})
```

#### `roto.s3.put(bucket, key, body, options?)`

Stores `body` (a Lua string, so binary-safe) as a new version of `key`, creating the
bucket if needed. It is the inline counterpart of `link`: the bytes are copied into the
store, versioning and notifications apply as for a normal `PutObject`, and rerunning
setup writes a new version. Options are the same as for `link`: `content_type`,
`metadata` and `headers`. Returns `{bucket, key}`.

```lua
roto.s3.put("attachments", "tenant1/Invoice.pdf", pdf_bytes, {content_type = "application/pdf"})
```

#### `roto.s3.link(bucket, key, file, options?)`

Registers `key` as a reference to a host file. The body is not copied; GET, HEAD, Range
and listings are served from `file`. Relative paths resolve against the setup script's
directory. The bucket is created if missing. Running setup again refreshes an existing
link to the same file. A missing file is a setup error. Linking only stats the file; the
real MD5 ETag is computed the first time it is needed (a `HEAD`, `GET` or listing) and
remembered until the file's size or modification time changes.

Options: `content_type` (otherwise guessed from the extension; `.gz` and `.tgz` stay
`binary/octet-stream`), `metadata` (user metadata table) and `headers` (`cache_control`,
`content_disposition`, `content_encoding`, `content_language`, `expires`,
`website_redirect_location`). Returns `{bucket = string, key = string}`.

```lua
roto.s3.link("assets", "demodata/rows.csv.gz", "Assets/demodata/rows.csv.gz")
```

#### `roto.s3.link_dir(bucket, prefix, dir, options?)`

Links every regular file below `dir`, recursively, as `prefix` plus the relative path.
Returns `{bucket, prefix, count}`, where `count` is the number of files linked. An empty
directory is fine; a missing one is an error.

Besides the `link` options, `include` and `exclude` take lists of globs to choose files. A
file is linked when it matches any `include` glob (or `include` is empty or omitted) and no
`exclude` glob. A glob without a `/` matches the file name at any depth; one with a `/`
matches the path relative to `dir`. `*` and `?` do not cross a `/`; `**` does, and `**/`
also matches no directory at all.

```lua
local linked = roto.s3.link_dir("assets", "static/", "public")  -- static/<relative path>
print(linked.count .. " files linked")

-- only the compressed CSVs and SQL, skipping drafts
roto.s3.link_dir("assets", "demodata/", "Assets/demodata", {
  include = {"*.csv.gz", "*.sql"},
  exclude = {"draft_*"},
})
```

### DynamoDB

Feature-detect with `if roto.dynamodb then`.

#### `roto.dynamodb.table(name, options)`

Creates the table if it is missing. An existing table is kept as is, so rerunning setup
is safe; indexes are not reconciled in place. Tables are pay-per-request. Returns
`{name, arn}`.

| Option | Meaning |
| --- | --- |
| `hash` | Hash key attribute name. Required. |
| `range` | Range key attribute name. Omit for a hash-only table. |
| `types` | Table of attribute name to `"S"`, `"N"` or `"B"`. Every key attribute defaults to `"S"`. |
| `gsis` | List of global secondary indexes `{name, hash, range?, projection?}`. `projection` defaults to `"ALL"`. |

Key attribute definitions are derived from `hash`, `range` and the index keys.

```lua
roto.dynamodb.table("admin", {
  hash = "PK", range = "SK",
  gsis = {{ name = "GSI1", hash = "GSI1PK", range = "GSI1SK" }},
})
roto.dynamodb.table("sessions", { hash = "PK" })
```

#### `roto.dynamodb.put(table, item)`

Writes an item given as plain Lua values, so there are no `{S = ...}` wrappers. Strings,
numbers, booleans, `roto.null`, `roto.array` lists and nested tables become `S`, `N`,
`BOOL`, `NULL`, `L` and `M`. An unmarked empty table is an empty map. Binary and set types
need `roto.call("dynamodb", "PutItem", ...)`. Returns `{table}`.

```lua
roto.dynamodb.put("admin", {
  PK = "TENANTS", SK = "TENANT#acme", Enabled = true, Seats = 25,
  Tags = roto.array({"pilot"}), Contact = { Region = "DUB" },
})
```

### Lambda

#### `roto.lambda.fn(name, options)`

Creates or updates function metadata and binds its local implementation. Returns
`{name = string, arn = string}`. The name is `fn` because `function` is a Lua
keyword. Supply exactly one of `executor` or `handler`.

| Option | Type | Default |
| --- | --- | --- |
| `timeout` | Integer seconds, 1–900 | `3` |
| `memory_size` | Integer MiB | `128` |
| `description` | String | `""` |
| `environment` | Table of string values | Empty environment |
| `executor` | Executor table below | Required unless `handler` is supplied |
| `handler` | `function(event, context)` | Required unless `executor` is supplied |

On every setup run, metadata is reconciled to supplied values or these defaults,
including replacing the function environment. Unknown helper options fail.
New metadata uses runtime `provided.al2023`, handler `external`, and a local
placeholder role; uploaded packages are not executed by these bindings.

#### Executor tables

Choose one executor form:

| Form | Fields |
| --- | --- |
| Command | `command`: nonempty argv array; optional `cwd`: directory; optional `env`: string map |
| HTTP | `url`: HTTP/HTTPS URL; optional `headers`: string map |
| Container | `container`: local RIE image name; optional `warm`: boolean, default `false` |

```lua
local worker = roto.lambda.fn("echo", {
  timeout = 5,
  executor = {command = {"sh", "-c", "cat"}, env = {MODE = "demo"}},
})
-- Other executor forms:
-- executor = {url = "http://127.0.0.1:8080/invoke", headers = {}}
-- executor = {container = "my-worker:latest", warm = true}
```

A command's `cwd` defaults to the setup script directory; relative paths resolve
against that directory. Executables use PATH. Commands read event JSON on stdin
and must emit exactly one JSON value on stdout; stderr carries logs. HTTP
executors POST event JSON and require a 2xx JSON response. Containers must already
contain RIE. See [Lambda execution](lambda.md) for environment precedence,
output limits, container lifecycle, and timeout behavior.

#### `roto.lambda.bind(name, executor)`

Binds an executor to existing function metadata without creating or updating
that metadata. Accepts the same executor forms and path resolution as above;
returns `{name = string}`.

```lua
-- The function must already exist, for example through an AWS SDK call.
roto.lambda.bind("existing-worker", {command = {"sh", "-c", "cat"}})
```

#### `roto.lambda.event_source(queue, function, options?)`

Accepts the helper return tables (or tables with their `arn` fields). Creates or
updates the SQS mapping for that source/function pair and returns the AWS mapping
configuration, including `UUID`. Repeated setup preserves the mapping UUID.

| Option | Type | Default |
| --- | --- | --- |
| `batch_size` | Integer, 1–10 | `10` |
| `enabled` | Boolean | `true` |
| `report_batch_item_failures` | Boolean | `false` |

```lua
local queue = roto.sqs.queue("tasks", {visibility_timeout = 30})
local worker = roto.lambda.fn("task-worker", {
  handler = function(event, context)
    local failures = roto.array({})
    for _, record in ipairs(event.Records) do
      if record.body == "retry" then
        table.insert(failures, {itemIdentifier = record.messageId})
      end
    end
    return {batchItemFailures = failures}
  end,
})
local mapping = roto.lambda.event_source(queue, worker, {
  batch_size = 5, report_batch_item_failures = true,
})
print(mapping.UUID)
```

Sources must be standard queues in the same account and region. Queue visibility
timeout must be at least the function timeout. Unknown options fail. See
[SQS delivery behavior](lua.md#sqs-delivery-behavior) for retry and deletion rules.

## Service access

Three tiers reach a service, from most to least convenient:

| Need | Use |
| --- | --- |
| Create an SQS queue, Lambda function or mapping, or link S3 files | [Resource helpers](#resource-helpers) |
| Call an operation of any supported service with a boto3-shaped table | `roto.call` |
| Raw wire access for anything else (custom paths, odd payloads) | `roto.wire` |

`roto.call` services: `sqs`, `lambda`, `dynamodb`, `ssm`, `secretsmanager`, `kinesis`,
`kms`, `logs` (CloudWatch Logs) and `events` (alias `eventbridge`), plus the XML-protocol
services `s3`, `sns`, `iam`, `sts` and `cloudformation`. Feature-detect the XML services
with `pcall`; other services need `roto.wire`.

For the XML-protocol services `roto.call` works like boto3: it reads the botocore model
for the operation, routes each input member to the URI, query string, headers or XML/form
body, and converts the XML response back to a PascalCase table. Lists are Lua arrays,
maps are tables, booleans and numbers are native. Timestamps and blobs are strings
(blobs are base64-encoded for you, so pass raw bytes; a `GetObject` `Body` comes back
as a UTF-8 string with replacement, so use `roto.wire` for binary objects). Unknown
input members raise an error naming the member.

### The same upload with wire, call, and boto3

Each example uploads `hello.txt` to an existing `fixtures` bucket with the same
contents, content type, and custom metadata. Run any one of them; they perform
the same S3 `PutObject` operation.

**Lua: `roto.wire`** spells out the HTTP request. It calls Roto's service handler
directly, without a network connection. Metadata becomes `x-amz-meta-*` headers:

```lua
roto.wire("s3", "PUT", "/fixtures/hello.txt", {
  headers = {
    ["content-type"] = "text/plain",
    ["x-amz-meta-owner"] = "demo",
    ["x-amz-meta-purpose"] = "greeting",
  },
  body = "hello",
})
```

**Lua: `roto.call`** takes the operation name and AWS-shaped arguments. Roto
translates the bucket and key into the path, and the metadata into headers:

```lua
roto.call("s3", "PutObject", {
  Bucket = "fixtures",
  Key = "hello.txt",
  Body = "hello",
  ContentType = "text/plain",
  Metadata = {owner = "demo", purpose = "greeting"},
})
```

**Python: boto3** takes equivalent arguments and sends the request over HTTP to
the running Roto server (here on the default port, 5070):

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
    aws_access_key_id="demo",
    aws_secret_access_key="demo",
)
s3.put_object(
    Bucket="fixtures",
    Key="hello.txt",
    Body=b"hello",
    ContentType="text/plain",
    Metadata={"owner": "demo", "purpose": "greeting"},
)
```

`wire` means using the service's HTTP request format; it executes a request,
rather than configuring a connection between resources. Prefer `roto.call`
when you have an operation name and structured arguments.

### `roto.call(service, operation, input?)`

Calls a supported service operation with AWS-shaped input and returns decoded
JSON. Omitted input becomes `{}`. Supported service names are listed above.
Service errors raise Lua errors; operation coverage follows the service's
implementation.

```lua
roto.call("ssm", "PutParameter", {
  Name = "/app/mode", Value = "development", Type = "String", Overwrite = true,
})
roto.call("s3", "CreateBucket", {Bucket = "assets"})
roto.call("s3", "PutBucketCors", {Bucket = "assets", CORSConfiguration = {CORSRules = {
  {AllowedMethods = {"GET", "PUT"}, AllowedOrigins = {"*"}, MaxAgeSeconds = 3500},
}}})
local parameter = roto.call("ssm", "GetParameter", {Name = "/app/mode"})
assert(parameter.Parameter.Value == "development")

local ok, err = pcall(function()
  return roto.call("ssm", "GetParameter", {Name = "/missing"})
end)
if not ok then print(tostring(err)) end
```

### `roto.wire(service, method, path, options?)`

Calls any installed service directly through its wire handler, before the HTTP
listener starts. Use the registered service name; the `eventbridge` alias belongs
only to `roto.call`. Returns `{status, headers, body}`, where `body` is a raw
string (response bytes are converted to UTF-8 with replacement for invalid bytes).
Handler errors and HTTP status codes of 400 or higher raise Lua errors.

| Option | Type | Default |
| --- | --- | --- |
| `headers` | String map; names are lowercased | Empty |
| `query` | Raw query string without `?` | `""` |
| `body` | String bytes or JSON-compatible Lua value | Empty body |

```lua
roto.wire("s3", "PUT", "/fixtures")
roto.wire("s3", "PUT", "/fixtures/hello.txt", {body = "hello"})
local object = roto.wire("s3", "GET", "/fixtures/hello.txt")
assert(object.status == 200 and object.body == "hello")

roto.wire("sns", "POST", "/", {
  headers = {["content-type"] = "application/x-www-form-urlencoded"},
  body = "Action=CreateTopic&Version=2010-03-31&Name=updates",
})
```

Supply protocol headers, escaping, and payload shape yourself. Table bodies are
JSON encoded; strings are sent unchanged. Neither call API invokes request
interceptors. For a larger runnable seed, use
[the demo fixtures](lua.md#quick-test-data).

### Reading responses

`roto.wire` bodies are raw strings. Decode JSON ones with `roto.json_decode`:

```lua
local fn = roto.lambda.fn("greeter", {handler = function(e) return {hi = e.name} end})
local out = roto.wire("lambda", "POST",
  "/2015-03-31/functions/" .. fn.name .. "/invocations", {body = {name = "ada"}})
assert(roto.json_decode(out.body).hi == "ada")
```

### Seeding a service without a helper

```lua
-- KMS and Logs go through roto.call
local key = roto.call("kms", "CreateKey", {Description = "demo"})
roto.call("logs", "CreateLogGroup", {logGroupName = "/app/web"})
-- S3 objects and SNS topics go through roto.wire
roto.wire("s3", "PUT", "/assets")
roto.wire("s3", "PUT", "/assets/hello.txt", {body = "hello"})
```

For a full seed across eleven services, see the
[demo fixtures](lua.md#quick-test-data).

## Running scripts on a live server

Start roto with `--enable-lua-api` to accept scripts at `POST /roto-api/lua`. The body is
the Lua source. The script gets the same `roto.*` setup API as `--setup` (including
`roto.s3.link`), starting in `us-east-1` and with any `--set` variables. Relative paths
and `require` resolve against the server's working directory.

```sh
curl -X POST localhost:5070/roto-api/lua --data-binary @seed.lua
curl -X POST localhost:5070/roto-api/lua --data-binary 'return roto.call("sqs", "ListQueues")'
```

Success returns `{"result": <return value>, "output": [<print lines>]}`. A failure returns
status 400 with `{"error": "...name:line: message...", "output": [...]}`. Query `name=`
labels the chunk in errors and `timeout=` sets the Lua time limit in seconds (default 30,
at most 600). Scripts run one at a time. `roto.hooks.request` hooks a script registers
stay active.

This executes arbitrary code as the server's user, so it is off by default, answers only
loopback peers, and rejects any request carrying an `Origin` header (browsers).

## Runtime callbacks

### Inline Lambda `handler(event, context)`

`event` is decoded JSON. `context` contains `function_arn`, `request_id`, `region`,
`environment`, and `timeout` in seconds. Return a JSON-compatible value; Lua errors
fail the invocation. Captured locals persist across invocations. Results are
limited to 6 MiB. `print` writes to the server console, not Lambda invocation logs.

```lua
local invocations = 0
roto.lambda.fn("greet", {
  handler = function(event, context)
    invocations = invocations + 1
    return {message = "Hello " .. event.name, count = invocations,
            region = context.region}
  end,
})
```

### `roto.hooks.request(service, operation, callback)`

Registers `callback(request, context)` for an exact service/operation pair;
a later registration replaces the earlier one. Returns no value. The callback
returns `nil` for normal dispatch or `{status, headers?, body}` to supply a wire
response. `status` must be 100–599; `body` must be a string.

`request` contains `service`, `operation`, `method`, `path`, raw `query`, `headers`,
raw string `body`, and `input` when the body parses as JSON. Authorization is
omitted from headers. `context` contains `account_id`, `region`, and `request_id`.

```lua
roto.hooks.request("sqs", "SendMessage", function(request, context)
  if request.input and request.input.MessageBody == "fail" then
    return {status = 503,
      headers = {["content-type"] = "application/x-amz-json-1.0"},
      body = '{"__type":"ServiceUnavailable","message":"Injected failure"}'}
  end
  return nil
end)
```

Interception applies to external AWS HTTP requests. Internal service calls bypass
it. Callback failures produce an internal AWS error. See
[runtime interception](runtime-hooks.md#request-interception) for tracing details.

### `roto.hooks.rds_data_connection(callback)`

Register `callback(request, context)` to choose the PostgreSQL server behind RDS Data
API calls (`aws rds-data ...`, signing name `rds-data`). Return a connection string
or `nil` to reject the request with `BadRequestException`. The request has
`resource_arn`, `secret_arn` and `database`; the context has `account_id` and `region`.
A later registration replaces the earlier one. Connections use plain TCP (no TLS).

```lua
roto.hooks.rds_data_connection(function(request, context)
  return "postgres://postgres:pw@localhost:5432/app"
end)
```

`:name` parameters are inlined as escaped literals and results use the text protocol,
so `typeHint` (`DATE`, `DECIMAL`, `JSON`, `TIME`, `TIMESTAMP`, `UUID`) only adds a cast.
Transactions hold a dedicated connection and expire after three minutes. The Data API
`database` argument is passed to the hook but not applied; put the database in the URL.

### `roto.hooks.mqtt_connect(callback)` and `roto.hooks.mqtt_subscribe(callback)`

Register `callback(request, context)` for MQTT lifecycle decisions; return no
value. Later registration replaces the same hook. Callback `nil` applies normal
behavior. Connect may return `{return_code = code}`: 0 accepts; 1–5 refuse
(unsupported version, rejected client ID, unavailable, bad credentials, or not
authorized). Subscribe returns `{return_codes = roto.array(codes)}` with one
entry per filter: 0 or 1 grants up to the requested QoS; 128 rejects.

```lua
roto.hooks.mqtt_connect(function(request, context)
  if request.connect.client_id == "blocked" then return {return_code = 5} end
end)
roto.hooks.mqtt_subscribe(function(request, context)
  local codes = {}
  for i, filter in ipairs(request.filters) do
    codes[i] = filter.topic == "private/#" and 128 or filter.qos
  end
  return {return_codes = roto.array(codes)}
end)
```

Both requests contain `transport`, `operation`, `path`, raw WebSocket `query`,
`headers` without Authorization, `source_ip`, `source_port`, `connection_id`, and
`connect`. The latter contains `protocol_version`, `client_id`, `clean_session`,
`keep_alive`, `username`, `password`, `password_base64`, and optional `will`
(`topic`, `qos`, `retain`, `payload_base64`). Absent optional values are JSON null.
Subscribe also includes `packet_id` and `filters` (`topic`, `qos`). Context has
`account_id`, `region`, and `request_id`. Credentials are visible to trusted hooks.

Invalid decisions and errors fail closed. Hooks cannot accept invalid filters or
QoS 2. See [MQTT hooks](runtime-hooks.md#mqtt-connection-and-subscription-hooks)
for connection closure and repeat-subscription behavior.

### Callback limits

All callbacks from one setup script execute serially. Lua deadline checks run
every 1,000 instructions: one second for interceptors and MQTT hooks, and the
configured timeout for Lambdas. Blocking native calls such as file I/O or
`os.execute` cannot be interrupted by these checks. Runtime callbacks may use
`roto.array` and `roto.null`, but cannot call setup APIs or register more hooks.

## Service modules

Service modules return an operation table. Each operation receives
`(context, input, db)` and returns a JSON-compatible response object. Context
contains `account_id`, `region`, and `request_id`; input is the decoded AWS
request. Missing handlers return 501 `NotImplemented`.

Each operation runs in one SQLite transaction. Success commits; errors and
five-second instruction-hook timeouts roll back database changes. The `db`
functions are valid only during that operation; do not retain them in closures.
Durable state belongs in `db`, since mutable module globals are not rolled back.

| Function | Result or behavior |
| --- | --- |
| `db.get(namespace, id)` | Stored JSON value, or `nil` when absent |
| `db.put(namespace, id, value)` | Insert or replace a JSON document; no return value |
| `db.delete(namespace, id)` | Remove a document, including an absent one; no return value |
| `db.list(namespace)` | JSON array of documents ordered by document ID |
| `roto.array(table?)` | Marks and returns the supplied table as a JSON array, or a new empty array |
| `roto.null` | Explicit JSON null |
| `roto.error(code, message, status?)` | Raise AWS client error; default HTTP status `400` |
| `roto.id(prefix)` | Prefix plus 12 hexadecimal characters from a generated request ID |
| `roto.uuid()` | Generated UUID string |
| `roto.sha256(text)` | Lowercase hexadecimal SHA-256 digest |
| `roto.decode_base64(text)` | Standard base64 decoded to UTF-8; invalid UTF-8 is replaced; invalid base64 raises an error |
| `roto.json_decode(text, error_code?)` | Decoded JSON; malformed JSON raises `error_code` or `InvalidParameterException` |

Namespace and ID arguments are strings. Namespaces are not automatically scoped:
include account and region when isolation is required. Setup APIs are not
exposed here.

```lua
-- service.lua
return {
  PutNote = function(ctx, input, db)
    if type(input.Id) ~= "string" or input.Id == "" then
      roto.error("InvalidInputException", "Id is required")
    end
    local ns = ctx.account_id .. "/" .. ctx.region .. "/notes"
    local note = {Id = input.Id, Text = input.Text, Digest = roto.sha256(input.Text)}
    db.put(ns, input.Id, note)
    return {Note = note}
  end,
  ListNotes = function(ctx, input, db)
    local ns = ctx.account_id .. "/" .. ctx.region .. "/notes"
    return {Notes = db.list(ns)}
  end,
}
```

For bundle manifests, protocol routing, and a runnable CRUD example, see
[the service bundle guide](https://github.com/vivainio/roto/blob/main/services/README.md)
and [widgets example](https://github.com/vivainio/roto/tree/main/examples/lua-services).
Service VMs load lazily and serialize requests per service.

### Shared helpers from `services/common.lua`

These functions are Lua helpers supplied by the bundled `common.lua`, rather
than built-in bindings. They are available when the manifest includes that file
as `common`; custom bundles can omit or replace it.

| Function | Behavior |
| --- | --- |
| `roto.required(input, key, code?)` | Return a nonblank string field, or raise `code` (default `InvalidInput`) |
| `roto.object(value, code?, label?)` | Validate and return an object table; label defaults to `Value` |
| `roto.list(value, code?, label?)` | Validate and return a marked array; label defaults to `Value` |
| `roto.scope(ctx, collection, global?)` | Account/region namespace; truthy `global` omits region |
| `roto.contains(values, target)` | Boolean membership check over a sequence |
| `roto.page(input, values, key, default?, cap?, code?)` | Response with array under `key` and optional `NextToken`; defaults and cap are 100 |
| `roto.tags(existing, incoming, code?)` | Merge arrays of `{Key, Value}` tags, validate incoming entries, and sort by key |
| `roto.untag(existing, keys, code?)` | Return tags with the listed string keys removed |
| `roto.copy_fields(source, destination, fields)` | Copy non-nil named fields; return destination |

`roto.page` reads `input.MaxResults` or `input.Limit`, validates integer sizes
against the cap, and uses a decimal offset `NextToken`. Its default error code is
`InvalidInput`. It preserves the supplied sequence order; callers choose sorting.

```lua
-- In an operation in a bundle with common.lua:
local id = roto.required(input, "Id", "InvalidInputException")
local ns = roto.scope(ctx, "notes")
db.put(ns, id, {Id = id, Text = input.Text})
return roto.page(input, db.list(ns), "Notes", 20, 100)
```
