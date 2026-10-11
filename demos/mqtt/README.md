# MQTT.js browser demo

Plain HTML using MQTT.js 5.16.0 from unpkg. No Node installation or npm build.
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
