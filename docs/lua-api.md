# Lua API reference

This chapter covers every Roto-provided Lua entry point: startup setup, runtime
callbacks, and optional service modules. Roto embeds Lua 5.4; no separate Lua
installation is required. Standard Lua libraries are available. Scripts are
trusted local code running with the server's OS permissions.

There are two separate environments. `--setup setup.lua` creates resources and
registers runtime callbacks in one shared VM. `--lua-services bundle` loads each
service module into its own VM, with a different `roto` table and transactional
storage. Functions from one environment are not available in the other.

## Setup lifecycle and values

```sh
roto-server --ephemeral --port 5071 --setup setup.lua
```

Setup runs once on startup, after service initialization and before the listener
and delivery workers start. `--account-id` and `--setup-region` select its scope
(the default region is `us-east-1`). `require("module")` searches beside the
setup script first, including `module.lua` and `module/init.lua`.
`--set KEY=VALUE` (repeatable) passes string parameters to the script as `roto.vars.KEY`.

| Value or function | Meaning |
| --- | --- |
| `roto.version` | Server version string (informational) |
| `roto.api_version` | Integer Lua API level; bumped only on breaking changes. Additive changes keep it, so feature-detect new functions with `if roto.s3.link_dir then` |
| `roto.account_id` | Setup account ID string |
| `roto.region` | Setup region string |
| `roto.endpoint` | Server base URL string |
| `roto.vars` | Table of strings from repeatable `--set KEY=VALUE` flags; unset keys are `nil` |
| `roto.null` | Explicit JSON null |
| `roto.array(table)` | Marks and returns the supplied table as a JSON array |

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

All APIs below except `roto.array` and `roto.null` are startup-only. Calling them
inside a Lambda, interceptor, or MQTT callback raises an error. Setup failures
stop startup and include source locations; earlier successful service changes
are not rolled back as a group. Bindings and callbacks are local configuration:
rerun setup after restart. Resource reset retains registrations and bindings,
but does not rerun setup.

## Queue helper

### `roto.sqs.queue(name, options?)`

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

## S3 helpers

### `roto.s3.link(bucket, key, file, options?)`

Registers `key` as a reference to a host file. The body is not copied; GET, HEAD, Range
and listings are served from `file`. Relative paths resolve against the setup script's
directory. The bucket is created if missing. Running setup again refreshes an existing
link to the same file. A missing file is a setup error.

Options: `content_type` (otherwise guessed from the extension; `.gz` and `.tgz` stay
`binary/octet-stream`), `metadata` (user metadata table) and `headers` (`cache_control`,
`content_disposition`, `content_encoding`, `content_language`, `expires`,
`website_redirect_location`).

```lua
roto.s3.link("assets", "demodata/rows.csv.gz", "Assets/demodata/rows.csv.gz")
```

### `roto.s3.link_dir(bucket, prefix, dir, options?)`

Links every regular file below `dir`, recursively, as `prefix` plus the relative path.
Returns the number of files linked. An empty directory is fine; a missing one is an error.

## Lambda helpers

### `roto.lambda.function_(name, options)`

Creates or updates function metadata and binds its local implementation. Returns
`{name = string, arn = string}`. The trailing underscore avoids Lua's reserved
word `function`. Supply exactly one of `executor` or `handler`.

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

### Executor tables

Choose one executor form:

| Form | Fields |
| --- | --- |
| Command | `command`: nonempty argv array; optional `cwd`: directory; optional `env`: string map |
| HTTP | `url`: HTTP/HTTPS URL; optional `headers`: string map |
| Container | `container`: local RIE image name; optional `warm`: boolean, default `false` |

```lua
local worker = roto.lambda.function_("echo", {
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

### `roto.lambda.bind(name, executor)`

Binds an executor to existing function metadata without creating or updating
that metadata. Accepts the same executor forms and path resolution as above;
returns `{name = string}`.

```lua
-- The function must already exist, for example through an AWS SDK call.
roto.lambda.bind("existing-worker", {command = {"sh", "-c", "cat"}})
```

### `roto.lambda.event_source(queue, function, options?)`

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
local worker = roto.lambda.function_("task-worker", {
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

## Running scripts on a live server

Start roto with `--enable-lua-api` to accept scripts at `POST /roto-api/lua`. The body is
the Lua source. The script gets the same `roto.*` setup API as `--setup` (including
`roto.s3.link`), with the `--setup-region` region and any `--set` variables. Relative paths
and `require` resolve against the server's working directory.

```sh
curl -X POST localhost:5070/roto-api/lua --data-binary @seed.lua
curl -X POST localhost:5070/roto-api/lua --data-binary 'return roto.call("sqs", "ListQueues")'
```

Success returns `{"result": <return value>, "output": [<print lines>]}`. A failure returns
status 400 with `{"error": "...name:line: message...", "output": [...]}`. Query `name=`
labels the chunk in errors and `timeout=` sets the Lua time limit in seconds (default 30,
at most 600). Scripts run one at a time. `roto.intercept_request` hooks a script registers
stay active.

This executes arbitrary code as the server's user, so it is off by default, answers only
loopback peers, and rejects any request carrying an `Origin` header (browsers).

## Service calls

### `roto.call(service, operation, input?)`

Calls a supported service operation with AWS-shaped input and returns decoded
JSON. Omitted input becomes `{}`. Supported service names are `sqs`, `lambda`,
`dynamodb`, `ssm`, `secretsmanager`, `events` (alias `eventbridge`), and `kinesis`.
Service errors raise Lua errors; operation coverage follows the service's
implementation.

```lua
roto.call("ssm", "PutParameter", {
  Name = "/app/mode", Value = "development", Type = "String", Overwrite = true,
})
local parameter = roto.call("ssm", "GetParameter", {Name = "/app/mode"})
assert(parameter.Parameter.Value == "development")

local ok, err = pcall(function()
  return roto.call("ssm", "GetParameter", {Name = "/missing"})
end)
if not ok then print(tostring(err)) end
```

### `roto.request(service, method, path, options?)`

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
roto.request("s3", "PUT", "/fixtures")
roto.request("s3", "PUT", "/fixtures/hello.txt", {body = "hello"})
local object = roto.request("s3", "GET", "/fixtures/hello.txt")
assert(object.status == 200 and object.body == "hello")

roto.request("sns", "POST", "/", {
  headers = {["content-type"] = "application/x-www-form-urlencoded"},
  body = "Action=CreateTopic&Version=2010-03-31&Name=updates",
})
```

Supply protocol headers, escaping, and payload shape yourself. Table bodies are
JSON encoded; strings are sent unchanged. Neither call API invokes request
interceptors. For a larger runnable seed, use
[the demo fixtures](lua.md#quick-test-data).

## Runtime callbacks

### Inline Lambda `handler(event, context)`

`event` is decoded JSON. `context` contains `function_arn`, `request_id`, `region`,
`environment`, and `timeout` in seconds. Return a JSON-compatible value; Lua errors
fail the invocation. Captured locals persist across invocations. Results are
limited to 6 MiB. `print` writes to the server console, not Lambda invocation logs.

```lua
local invocations = 0
roto.lambda.function_("greet", {
  handler = function(event, context)
    invocations = invocations + 1
    return {message = "Hello " .. event.name, count = invocations,
            region = context.region}
  end,
})
```

### `roto.intercept_request(service, operation, callback)`

Registers `callback(request, context)` for an exact service/operation pair;
a later registration replaces the earlier one. Returns no value. The callback
returns `nil` for normal dispatch or `{status, headers?, body}` to supply a wire
response. `status` must be 100–599; `body` must be a string.

`request` contains `service`, `operation`, `method`, `path`, raw `query`, `headers`,
raw string `body`, and `input` when the body parses as JSON. Authorization is
omitted from headers. `context` contains `account_id`, `region`, and `request_id`.

```lua
roto.intercept_request("sqs", "SendMessage", function(request, context)
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

### `roto.mqtt.on_connect(callback)` and `roto.mqtt.on_subscribe(callback)`

Register `callback(request, context)` for MQTT lifecycle decisions; return no
value. Later registration replaces the same hook. Callback `nil` applies normal
behavior. Connect may return `{return_code = code}`: 0 accepts; 1–5 refuse
(unsupported version, rejected client ID, unavailable, bad credentials, or not
authorized). Subscribe returns `{return_codes = roto.array(codes)}` with one
entry per filter: 0 or 1 grants up to the requested QoS; 128 rejects.

```lua
roto.mqtt.on_connect(function(request, context)
  if request.connect.client_id == "blocked" then return {return_code = 5} end
end)
roto.mqtt.on_subscribe(function(request, context)
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

## Optional service module API

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
| `roto.array()` | New marked empty JSON array; **does not accept a table to populate it** |
| `roto.error(code, message, status?)` | Raise AWS client error; default HTTP status `400` |
| `roto.id(prefix)` | Prefix plus 12 hexadecimal characters from a generated request ID |
| `roto.uuid()` | Generated UUID string |
| `roto.sha256(text)` | Lowercase hexadecimal SHA-256 digest |
| `roto.decode_base64(text)` | Standard base64 decoded to UTF-8; invalid UTF-8 is replaced; invalid base64 raises an error |
| `roto.json_decode(text, error_code?)` | Decoded JSON; malformed JSON raises `error_code` or `InvalidParameterException` |

Namespace and ID arguments are strings. Namespaces are not automatically scoped:
include account and region when isolation is required. Setup APIs and
`roto.null` are not exposed here. To obtain explicit null in a module, use
`roto.json_decode("null")`.

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
