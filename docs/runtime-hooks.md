# Runtime Luau handlers and request interception

A `--setup` script can register inline Lambda handlers and exact-operation request
interceptors. Roto retains their Luau closures after startup. See
[Luau scripting guide](lua.md) for resource creation and executor bindings, and the
[Luau API reference](lua-api.md) for every binding and callback contract.

## Inline placeholder Lambdas

```luau
--!strict
roto.lambda.fn("greet", {
  timeout = 3,
  handler = function(event: {name: string}, context: LambdaContext)
    return {message = "Hello " .. event.name}
  end,
})
```

Use `handler` instead of `executor`. The function receives a decoded JSON event
and a context table with `function_arn`, `request_id`, `region`, `environment`,
and `timeout` (seconds). Return a JSON-compatible value; use `roto.array({})` and
`roto.null` for empty arrays and null. Captured Luau locals survive invocations.

These functions use the normal Lambda invocation machinery, including direct
Invoke, asynchronous delivery, SQS mappings, S3 notifications, EventBridge, and
invocation history. Luau errors become failed Lambda invocations. Results are
limited to 6 MiB. Inline handlers are local configuration; rerun setup after
restarting the server. They do not execute uploaded Lambda packages or emulate
an AWS Luau runtime. `print` writes to the server console, not invocation logs.

## Request interception

```luau
--!strict
roto.hooks.request("sqs", "SendMessage", function(request: HookRequest, context: HookContext)
  if request.input and request.input.MessageBody == "fail" then
    return {
      status = 503,
      headers = {["content-type"] = "application/x-amz-json-1.0"},
      body = '{"__type":"ServiceUnavailable","message":"Injected failure"}',
    }
  end
  return nil
end)
```

Registration matches an exact service name and operation name. A later
registration for the same pair replaces the earlier callback. Returning `nil`
continues normal dispatch. Returning `{status, headers, body}` short-circuits
before service dispatch; `status` must be 100–599, `headers` is an optional table
of string values, and `body` is a required string. Scripts provide the AWS wire
format, including error bodies and protocol headers. This also allows stubbing
unsupported operations when their service and operation can be identified.

The request table contains `service`, `operation`, `method`, `path`, raw `query`,
`headers`, raw string `body`, and `input` when the body parses as JSON. The
Authorization header is omitted. Context contains `account_id`, `region`, and
`request_id`. Scripts are trusted local code and may see other credential data
in query parameters and headers; do not log those indiscriminately.

Interceptors run for external AWS HTTP requests, including IoT Publish, and
are recorded in request traces. Internal service-to-service calls and startup
setup calls do not trigger them. Requests without a matching registration do
not enter Luau. Callback errors and invalid responses return an internal AWS
error. There is no global interceptor or missing-support fallback registration.

## MQTT connection and subscription hooks

Register `roto.hooks.mqtt_connect(callback)` or
`roto.hooks.mqtt_subscribe(callback)` to simulate policies.
These MQTT lifecycle hooks are separate from AWS service request interception.
They receive decoded MQTT dictionaries and return MQTT decisions. They share
the Luau VM, serialization lock, and execution limits with service interceptors.
A later registration for the same MQTT hook replaces the previous callback.

For example:

```luau
--!strict
roto.hooks.mqtt_connect(function(request: MqttRequest, context: HookContext)
  if request.connect.client_id == "blocked" then
    return {return_code = 5} -- Not authorized.
  end
  return nil
end)

roto.hooks.mqtt_subscribe(function(request: MqttRequest, context: HookContext)
  local codes: {number} = {}
  for i, filter in ipairs(request.filters or {}) do
    codes[i] = filter.topic == "private/#" and 128 or filter.qos
  end
  return {return_codes = roto.array(codes)}
end)
```

Both request dictionaries include `transport` (`mqtt`), `operation`, `path`, raw
WebSocket `query`, `headers` (Authorization omitted), actual peer `source_ip`
and `source_port`, and Roto's `connection_id`. The nested `connect` table
contains `protocol_version` (4), `client_id`, `clean_session`, `keep_alive`,
`username`, `password` (when valid UTF-8), `password_base64` (preserving binary
passwords), and `will` with `topic`, `qos`, `retain`, and `payload_base64`.
Absent optional values are null. The Subscribe request additionally contains
`packet_id` and an ordered `filters` array of `{topic, qos}` dictionaries.
Context contains `account_id`, `region` (local default `us-east-1`), and
`request_id`. These trusted hooks can see MQTT credentials; avoid logging them.

Returning `nil` applies normal behavior. Connect results use `return_code`
0 (accept), 1 (unsupported version), 2 (client ID rejected), 3 (unavailable),
4 (bad credentials), or 5 (not authorized). Refusal sends CONNACK then closes.
Subscribe results require one `return_codes` entry per filter: 0 or 1 to grant
at most the requested QoS, or 128 to reject. Invalid filters and QoS 2 cannot be
accepted by a hook. Accepted filters enter the shared topic registry; rejected
filters do not. Rejecting a repeat subscription does not revoke an existing
subscription; use unsubscribe to remove it.

Errors, timeouts, and invalid results fail closed: CONNECT receives code 3,
and SUBSCRIBE receives failure codes for every filter. The hooks share the Luau
serialization and execution limits below, run after the WebSocket upgrade,
and are not included in the AWS HTTP request trace. Without hooks, valid
connections and QoS 0/1 subscriptions are accepted. This simulates policy
outcomes; it does not implement AWS IoT IAM policies or signature validation.

See `demos/mqtt/setup.luau` and the browser demo's Luau denial checks.

## Execution limits

Callbacks from one setup script execute serially. Luau deadline checks run
at function calls and loop iterations: one second for an interceptor, and the
configured function timeout for a Lambda. These checks interrupt Luau loops;
they cannot interrupt a blocking native call.
Use command or HTTP executors for workloads that need external processes.

Resource setup helpers, `roto.call`, `roto.wire`, and hook registration are
startup-only and fail when called from a runtime callback. This prevents nested
Lambda invocations from deadlocking the shared VM. Runtime callbacks may use
`roto.array` and `roto.null`. Bindings and registrations remain after resource
reset; reset does not rerun setup.

Save the example as `setup.luau` and run it with:

```sh
roto-server --ephemeral --setup setup.luau
```

Post-create resource callbacks and a catch-all fallback remain future work.
