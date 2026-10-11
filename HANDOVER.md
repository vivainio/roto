# Handover: minimal MQTT-over-WebSocket for IoT

Implemented 2026-10-11 on `feat/iot-mqtt-websocket`.

- `/mqtt` negotiates the `mqtt` WebSocket subprotocol and accepts MQTT 3.1.1.
- `crates/roto-mqtt` is a dependency-free minimal wire codec. It handles partial
  and combined packets, remaining-length encoding, and a 1 MiB packet limit.
- `crates/roto-server/src/websocket/mqtt.rs` handles CONNECT/CONNACK,
  SUBSCRIBE/SUBACK (QoS 0/1), UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, and
  DISCONNECT using the existing shared connection/topic registry.
- HTTP IoT Data Plane Publish delivers raw payloads as MQTT QoS 0 PUBLISH.
  Client publishes are dropped; QoS 1 receives PUBACK. Authentication, sessions,
  wills, retained messages, MQTT 5, QoS 2, and offline delivery are unsupported.
- Lua `roto.mqtt.on_connect(callback)` / `roto.mqtt.on_subscribe(callback)` hooks
  receive decoded client metadata and ordered topic/QoS filters. CONNECT
  refusals return CONNACK codes; SUBSCRIBE can reject individual filters.
  Callback errors or invalid results fail closed. See `docs/runtime-hooks.md`.
- `demos/mqtt` is a plain HTML app loading MQTT.js 5.16.0 from unpkg. Its
  Python standard-library server proxies HTTP publishes. No Node or npm is needed.
  MQTT.js is the reference client and acceptance test. See its README for
  launch commands and browser acceptance checks; `--setup demos/mqtt/setup.lua`
  plus the demo server's `--hooks` option enables policy denial checks.

Verified with `agent-browser` against disposable servers: MQTT.js connection,
subprotocol, QoS grants, exact/wildcard and health-topic delivery, 20 KB binary
payload, PUBACK without client publish fan-out, unsubscribe, keepalive,
disconnect, reconnect without sessions, and resubscribe. The page displayed
`PASS: all MQTT.js browser acceptance checks including Lua denials`.
Additional checks cover explicit refusal, hook errors/invalid results, mixed
subscription grants, no delivery to denied filters, and continued delivery to
accepted filters.

`cargo build --workspace --locked`, `cargo test --workspace --locked`, codec
unit tests, and `tests/smoke-websocket.py` pass. The latter now tolerates a null
Lambda result while polling disconnect history. Documentation covers the
endpoint and limits in `docs/iot.md` and the service coverage table.

---

# IoT WebSocket and API Gateway v2 handover

Recorded 2026-10-10 and updated after implementing the API Gateway v2 WebSocket
subset. A native, non-Moto integration smoke test covers the local WebSocket
topic channel, API creation and route execution, Lambda integrations, and
Management API callbacks.

## Current implementation

- `crates/roto-server/src/websocket.rs` owns the shared connection hub.
- Roto exposes `ws://localhost:5070/roto-api/ws` and the alias
  `ws://localhost:5070/roto-api/iot/ws`. Clients receive a connection ID and
  use JSON `subscribe` / `unsubscribe` messages. Topic filters support `+` and
  `#`.
- IoT Data Plane `Publish` delivers UTF-8 and base64-wrapped binary payloads to
  connected matching subscribers. Delivery is local and best-effort; retained messages and durable offline
  delivery are not implemented. The minimal MQTT endpoint is described above.
- API Gateway Management API `PostToConnection` uses the same connection hub
  and sends text or binary WebSocket frames. The API ID and stage are not
  configured for the custom local sockets.
- API Gateway v2 control-plane calls under `/v2/apis` create WebSocket APIs,
  Lambda proxy integrations, routes, `$default` route responses, stages, and
  deployments. Configuration is in memory and is cleared on reset or restart.
- API Gateway WebSocket clients connect to `/{apiId}/{stage}` (or `/{apiId}` for
  `$default`). `$connect`, `$disconnect`, and selected message routes invoke the
  configured Lambda integration. A `$default` route response sends the Lambda
  payload to the client; Management API callback paths check API ID and stage.
- `docs/iot.md` documents the current endpoints and protocol.

## Native coverage

`tests/smoke-websocket.py` uses only Python's standard library. It starts an
ephemeral Roto server and exercises two custom WebSocket clients, wildcard topic
delivery, unsubscribe, binary payloads, custom Management API text/binary
callbacks, closed-connection `GoneException`, and invalid QoS handling. It also
creates a WebSocket API through `/v2/apis`, configures a Lambda proxy route and
auto-deploy stage, connects through the API ID/stage path, checks route selection
and the Lambda event, and posts a callback through the API-specific Management
API path. It does not use Moto or an AWS SDK.

Verified on this worktree:

```sh
cargo build -p roto-server --locked
python3 tests/smoke-websocket.py
python3 -m py_compile tests/smoke-websocket.py
git diff --check
```

All four commands passed; the smoke test printed:
`PASS: IoT topic sockets and API Gateway v2 routes, Lambda integrations, and callbacks`.

Remaining API Gateway gaps are HTTP APIs, custom domains, authorizers, request
models, non-Lambda integrations, automatic Lambda permissions, integration
responses beyond the `$default` route response, and `GetConnection`/
`DeleteConnection`. The data plane currently supports route-selection
expressions of the form `$request.body.field`; API configuration is in memory.
