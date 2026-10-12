# IoT topics and local WebSockets

Roto accepts the IoT Data Plane `Publish` HTTP request and delivers its payload
to matching clients connected to Roto's local WebSocket endpoint. This supports
browser clients using MQTT.js or Roto's custom JSON protocol.

## MQTT over WebSocket

**MQTT.js is the reference client and acceptance test** for this subset. The
browser demo uses MQTT.js v5 with MQTT 3.1.1 (`protocolVersion: 4`); library
version 5 does not require the MQTT 5 wire protocol.

Connect to `ws://localhost:5070/mqtt` using MQTT 3.1.1 (`protocolVersion: 4`
in MQTT.js). The upgrade negotiates the `mqtt` subprotocol. For example, with
MQTT.js loaded in the browser:

```javascript
const client = mqtt.connect("ws://localhost:5070/mqtt", { protocolVersion: 4 });
client.on("connect", () => client.subscribe("devices/123/state", { qos: 1 }));
client.on("message", (topic, payload) => console.log(topic, payload.toString()));
```

Send a message to that subscriber with boto3's IoT Data Plane client, pointed at
Roto's HTTP endpoint (not the WebSocket):

```python
import boto3

iot = boto3.client("iot-data", endpoint_url="http://localhost:5070",
                   region_name="us-east-1")
iot.publish(topic="devices/123/state", qos=0, payload=b"online")
```

The MQTT.js client above receives this as a PUBLISH on `devices/123/state`.

Subscriptions support exact topics and `+`/`#` filters through the same registry
as the JSON endpoint. HTTP IoT publishes arrive as MQTT PUBLISH packets with
unchanged payload bytes, always at QoS 0. SUBACK grants requested QoS 0/1;
QoS 2 and invalid filters receive a failure code. UNSUBSCRIBE, PINGREQ, and
DISCONNECT are supported. Client PUBLISH payloads are dropped; QoS 1 receives
PUBACK so the client does not stall.

Client IDs, credentials, clean session, keepalive settings, and wills do not
create persistent state or authentication. Optional [Lua hooks](runtime-hooks.md#mqtt-connection-and-subscription-hooks)
(`roto.hooks.mqtt_connect` and `roto.hooks.mqtt_subscribe`) receive this metadata and
can refuse connections or individual subscription filters. MQTT 5, QoS 2, retained messages,
and offline delivery are unsupported. Clients must resubscribe on reconnect.
The local packet size limit is 1 MiB. For `wss`, put a TLS proxy in front of Roto.

The [browser demo](https://github.com/vivainio/roto/tree/main/demos/mqtt)
loads MQTT.js from a CDN and includes acceptance checks; it requires no Node
installation. See `demos/mqtt/README.md` for launch instructions. Its **Run
acceptance checks** button verifies connection, QoS grants, topic filtering,
binary payload delivery, unsubscribe, keepalive, and reconnect. Start Roto with
`--setup demos/mqtt/setup.lua` and the demo server with `--hooks` to additionally
verify Lua connection refusals and per-filter subscription denial.

## Custom JSON WebSocket

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
no durable queue, replay, or session state. The JSON endpoint uses the command
acknowledgements shown above.

Run the native end-to-end checks without Moto or AWS SDK packages:

```sh
cargo build -p roto-server --locked
python3 tests/smoke-websocket.py
```

The smoke test opens two real WebSocket connections, checks topic filtering and
unsubscribe behavior, publishes UTF-8 and binary IoT payloads, exercises
Management API text and binary callbacks, and verifies that a closed connection
returns `GoneException`.

## API Gateway v2 WebSocket APIs

Roto implements a local subset of API Gateway v2's control plane and WebSocket
data plane. Control-plane requests use the ordinary API Gateway v2 paths under
`/v2/apis`. The current subset creates and lists WebSocket APIs, integrations,
routes, route responses, stages, and deployments, and supports reading or
deleting those resources. Configuration is held in memory and is cleared by
`/roto-api/reset` or a server restart.

Create a WebSocket API with the AWS SDK pointed at Roto:

```python
import boto3

control = boto3.client(
    "apigatewayv2",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
)
api = control.create_api(
    Name="local-chat",
    ProtocolType="WEBSOCKET",
    RouteSelectionExpression="$request.body.action",
)
```

Add an `AWS_PROXY` Lambda integration, routes such as `$connect`,
`$disconnect`, and `sendmessage`, then create a stage with `AutoDeploy=True`.
The integration URI uses API Gateway's normal Lambda form:

```text
arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:local-chat/invocations
```

Connect to `ws://localhost:5070/{apiId}/{stage}`. The `$default` stage uses
`ws://localhost:5070/{apiId}`. The local endpoint sends the same
`{"type":"ready","connectionId":"..."}` greeting as Roto's custom socket
endpoint so local clients can exercise `PostToConnection` directly; AWS does
not send this greeting. Connect and disconnect integrations receive API
Gateway-style events. Text messages select routes using expressions of the
form `$request.body.field`; `$default` handles unmatched or binary messages.
The Lambda integration receives a WebSocket event with `requestContext` values
such as `apiId`, `stage`, `routeKey`, `eventType`, and `connectionId`.

WebSocket Lambda results are discarded unless the route has a `$default` route
response. With that route response configured, Roto sends the Lambda payload
back to the client. The Management API can also send callbacks:

```python
management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=f"http://localhost:5070/{api['ApiId']}/dev",
    region_name="us-east-1",
)
management.post_to_connection(ConnectionId=connection_id, Data=b"hello")
```

API ID and stage are checked against the live connection for API-specific
Management API paths. Expired or mismatched connections return
`GoneException`.

The implementation supports Lambda proxy integrations and a single route
response key (`$default`). It does not implement HTTP APIs, custom domains,
authorizers, request models, non-Lambda integrations, IAM authorization, or
Lambda permission enforcement. Stage auto-deploy and explicit deployments
control which route configuration is active.

## API Gateway Management API

The same connection registry supports `apigatewaymanagementapi.PostToConnection`
for both custom local sockets and API Gateway v2 sockets. For the custom socket,
use the `connectionId` from its `ready` message and set the management client's
endpoint URL to Roto with a stage path, for example:

```python
management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url="http://localhost:5070/dev",
    region_name="us-east-1",
)
management.post_to_connection(ConnectionId=connection_id, Data=b"hello")
```

The callback sends a text WebSocket frame when `Data` is UTF-8 and a binary
frame otherwise. The custom local endpoint accepts
`POST /{stage}/@connections/{id}`. API Gateway v2 sockets accept
`POST /{apiId}/{stage}/@connections/{id}` and check the API and stage against
the connection.
