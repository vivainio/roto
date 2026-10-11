# MQTT.js browser demo

**MQTT.js is the reference client and acceptance test** for Roto's minimal
MQTT listener. This plain HTML app uses MQTT.js 5.16.0 from unpkg with MQTT
3.1.1 (`protocolVersion: 4`). No Node installation or npm build.
The CDN needs an internet connection. Start a disposable Roto server:

```sh
cargo build -p roto-server --locked
target/debug/roto-server --ephemeral --port 5071
```

In another terminal, run the Python standard-library demo server:

```sh
python3 demos/mqtt/serve.py
```

Open <http://127.0.0.1:5072/>. Connect and subscribe, then publish a message.
The demo server proxies publishes to Roto's IoT HTTP API; subscriptions use
MQTT over WebSocket directly. It does not change Roto's CORS configuration.
Use `--port` and `--roto-port` to select free ports alongside existing servers.

**Run acceptance checks** exercises the real browser MQTT.js client: CONNECT,
subprotocol negotiation, QoS 0/1 SUBACK, exact and wildcard subscriptions,
binary HTTP publish delivery, `$aws/things/ping`, client QoS 1 PUBACK without
fan-out, UNSUBACK, keepalive PINGRESP, disconnect, and reconnect/resubscribe.
The page displays a PASS or FAIL result and individual checks.

This is a minimal MQTT 3.1.1 listener, not a complete broker. Delivery is always
QoS 0. Authentication, retained messages, sessions, wills, MQTT 5 and QoS 2
are not implemented. Client publishes are acknowledged when necessary and dropped.

## Simulate connection and subscription denials

Start Roto with `--setup demos/mqtt/setup.lua` and add `--hooks` to the Python
server command. The acceptance checks additionally exercise CONNECT refusal,
hook errors and invalid results, per-filter SUBACK failure, and verify that
refused filters receive no messages while accepted filters still work.

The example inspects client credentials, clean-session/keepalive settings, will
metadata, peer address, WebSocket headers, and connection/packet IDs. See
[the runtime hooks documentation](../../docs/runtime-hooks.md) for the contract.

## Run the browser checks with agent-browser

With `agent-browser` installed and both servers running as above, use a named
browser session. The browser still loads the real MQTT.js CDN bundle:

```sh
agent-browser --session roto-mqtt-check open http://127.0.0.1:5072/
agent-browser --session roto-mqtt-check snapshot -i
agent-browser --session roto-mqtt-check find role button click --name "Run acceptance checks"
agent-browser --session roto-mqtt-check wait --text "PASS: all MQTT.js browser acceptance checks"
agent-browser --session roto-mqtt-check get text '#status'
agent-browser --session roto-mqtt-check get text '#log'
agent-browser --session roto-mqtt-check close
```

For the Lua denial checks, start the two servers with these commands instead
(in separate terminals), then run the same browser commands:

```sh
target/debug/roto-server --ephemeral --port 5071 --setup demos/mqtt/setup.lua
python3 demos/mqtt/serve.py --hooks
```

The final status should be `PASS: all MQTT.js browser acceptance checks including
Lua denials`. If the wait times out, inspect `#status` and `#log`, or run
`agent-browser --session roto-mqtt-check errors` and
`agent-browser --session roto-mqtt-check console`. Close the browser session
when finished and stop the disposable servers with Ctrl-C. If you selected
another demo port, adjust the browser URL accordingly.

## Run the MQTT.js integration checks with Bun

The CI integration test runs MQTT.js 5.16.0 under Bun 1.4.3, including the Lua
policy denial checks. It starts a fresh ephemeral Roto server on a free port and
stops it afterward; no browser or Python demo server is needed.

```sh
cargo build -p roto-server --locked
cd tests/mqtt
bun install --frozen-lockfile
bun test
```

The test uses `forceNativeWebSocket: true` because Bun does not implement the
`ws.createWebSocketStream` used by MQTT.js's default Node transport. Dependencies
are pinned in `tests/mqtt/bun.lock`. Set `ROTO_SERVER` to an absolute binary path
if using a different Cargo target directory. The browser checks remain useful
for verifying the real browser transport.
