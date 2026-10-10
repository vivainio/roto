# IoT topics and local WebSockets

Roto accepts the IoT Data Plane `Publish` HTTP request and delivers its payload
to matching clients connected to Roto's local WebSocket endpoint. This supports
browser clients without an MQTT library or MQTT wire protocol.

Connect to `ws://localhost:5070/roto-api/ws` (or the alias
`ws://localhost:5070/roto-api/iot/ws`). On connection, the server sends a JSON
message containing a `connectionId`:

```json
{"type":"ready","connectionId":"..."}
```

Send JSON text frames to subscribe and unsubscribe. Topic filters support MQTT
`+` and `#` wildcards:

```json
{"action":"subscribe","topic":"devices/+/state"}
{"action":"unsubscribe","topic":"devices/+/state"}
```

The server acknowledges each command. Matching publishes arrive as JSON text
frames:

```json
{
  "type":"message",
  "topic":"devices/123/state",
  "qos":0,
  "payload":{"encoding":"utf-8","data":"online"}
}
```

Payloads that are not valid UTF-8 use `{"encoding":"base64","data":"..."}`.
The payload is otherwise unchanged; JSON payloads are not parsed. A minimal
browser client can use `new WebSocket("ws://localhost:5070/roto-api/ws")`, wait
for `open`, then send the subscribe JSON above.

Publish through the AWS SDK's IoT Data Plane client, pointed at Roto:

```python
import boto3

iot = boto3.client("iot-data", endpoint_url="http://localhost:5070",
                   region_name="us-east-1")
iot.publish(topic="devices/123/state", qos=0, payload=b"online")
```

Publishes are delivered to currently connected subscribers only. Retained
messages are not supported; `retain=true` returns an `InvalidRequestException`.
QoS 0 and 1 values are accepted, but delivery is local and best-effort: there is
no durable queue, replay, session state, or MQTT acknowledgement protocol.

Run the native end-to-end checks without Moto or AWS SDK packages:

```sh
cargo build -p roto-server --locked
python3 tests/smoke-websocket.py
```

The smoke test opens two real WebSocket connections, checks topic filtering and
unsubscribe behavior, publishes UTF-8 and binary IoT payloads, exercises
Management API text and binary callbacks, and verifies that a closed connection
returns `GoneException`.

## API Gateway Management API

The same connection registry supports `apigatewaymanagementapi.PostToConnection`.
Use the `connectionId` from the WebSocket `ready` message and set the management
client's endpoint URL to Roto with the API stage path, for example:

```python
management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url="http://localhost:5070/dev",
    region_name="us-east-1",
)
management.post_to_connection(ConnectionId=connection_id, Data=b"hello")
```

The callback sends a text WebSocket frame when `Data` is UTF-8 and a binary
frame otherwise. The local endpoint accepts `POST /{stage}/@connections/{id}`;
API IDs and stages are not separately configured or isolated. Expired IDs return
a `GoneException`. Roto does not create WebSocket APIs or implement their
`$connect`, `$disconnect`, and message routes; this is the Management API's
targeted-send mechanism over Roto's local WebSocket endpoint.
