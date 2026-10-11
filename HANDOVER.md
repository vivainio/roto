# Handover: minimal MQTT-over-WebSocket for IoT (proposed plan)

Proposed 2026-10-11, not started. Goal: let applications that talk to AWS IoT
through an MQTT client library run against roto locally instead of the real
IoT endpoint.

## Why

A typical app publishes through the IoT Data Plane `Publish` API on the server
and has browsers subscribe with **mqtt.js v5**, MQTT 3.1.1 (`protocolVersion: 4`)
over `wss`, QoS 1, exact topics (no wildcards), no retained messages, and a
health ping to `$aws/things/ping`. Without a local broker, such apps must use a
real AWS IoT endpoint and credentials even for local development.

Roto already accepts iot-data `Publish` and fans out to subscribers on its custom
JSON WebSocket (`docs/iot.md`, `/roto-api/ws`). mqtt.js cannot talk to that, so
roto needs an MQTT listener on top of the existing topic registry
(`crates/roto-server/src/websocket.rs`, described in the IoT section below).

## Reference client

**mqtt.js is the reference client.** Behaviour is defined by what mqtt.js
(`mqtt@5`) needs in order to connect, subscribe, receive and stay alive. When the
spec and mqtt.js disagree on something optional, follow mqtt.js. The acceptance
test is a real mqtt.js client, not a hand-rolled packet test.

## Scope: super minimal

Accept connections; do only what keeps mqtt.js connected and delivers messages.

Handle:
- WebSocket upgrade on an MQTT path (e.g. `/mqtt`) echoing subprotocol `mqtt`.
- CONNECT -> CONNACK accepted. Ignore client id, credentials, clean flag,
  keepalive, will.
- SUBSCRIBE -> register filters in the existing registry, SUBACK granting the
  requested QoS (0/1).
- UNSUBSCRIBE -> remove filters, UNSUBACK.
- PINGREQ -> PINGRESP.
- DISCONNECT -> close.
- Delivery: iot-data `Publish` matching a subscription -> PUBLISH packet to the
  client, always at QoS 0 (no packet ids or retransmission).

Ignore on purpose:
- Client PUBLISH: accept and drop (send PUBACK for QoS 1 so mqtt.js does not stall).
- Sessions, retained messages, will, authentication, MQTT 5, QoS 2, `$SYS`.
- Messages published while a client is disconnected are lost (best effort, as in
  `docs/iot.md`). Clients resubscribe on reconnect.

## Steps

1. Test first: a mqtt.js client (connect, subscribe, iot-data publish, receive,
   unsubscribe, ping, QoS 1 client publish gets PUBACK) against a roto server,
   in the style of `tests/smoke-websocket.py`. Needs node + `mqtt@5`; keep it out
   of the default cargo test run if node is unavailable.
2. Implement the listener: packet parse/encode (variable-length remaining length,
   topic and packet-id fields), wired to the existing connection/topic registry.
3. Update `docs/iot.md` and the IoT row in `docs/services.md`; run the full
   build/tests; commit to `main`.

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
  connected matching subscribers. Delivery is local and best-effort; MQTT,
  retained messages, durable offline delivery, and MQTT acknowledgements are
  not implemented.
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
