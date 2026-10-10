# Server reference

Pass options to the installed `roto-server` command:

```sh
roto-server --data-dir ./test-data --durable
```

| Option | Default | Behavior |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Listen address |
| `--port` | `5070` | Port shared by all services |
| `--data-dir` | `roto-data` | Persistent storage directory; also `ROTO_DATA_DIR` |
| `--ephemeral` | Off | In-memory databases and temporary S3 files |
| `--durable` | Off | SQLite `synchronous=FULL`, syncing each commit |
| `--lambda-executors` | None | JSON bindings to local command or HTTP Lambda executors |
| `--setup` | None | Lua resource setup and executor bindings before listening or processing events |
| `--setup-region` | `us-east-1` | Region used by Lua setup |
| `--account-id` | `123456789012` | Default account; also `ROTO_ACCOUNT_ID` |

Use `--help` for the executable's current options. `RUST_LOG` controls tracing output:

```sh
RUST_LOG=debug roto-server --ephemeral
```

## Administrative endpoints

| Method | Path | Result |
| --- | --- | --- |
| `GET` | `/roto-api/` | HTML resource and history browser (also `/roto-api`) |
| `GET` | `/roto-api/resources` | Resource collection catalog |
| `GET` | `/roto-api/resources/{service}/{collection}` | Resource records, 50 per page |
| `GET` | `/roto-api/s3/object` | Download an object or read a bounded preview |
| `GET` | `/roto-api/health` | Returns `ok` |
| `GET` | `/roto-api/unsupported` | Deduplicated unsupported HTTP calls and counts |
| `POST` | `/roto-api/reset` | Clears state across services |
| `GET` | `/roto-api/lambda/invocations` | Latest 100 invocations, results and logs |
| `GET` | `/roto-api/s3/notifications` | Pending/failed S3 handoffs |
| `GET` | `/roto-api/events/deliveries` | Latest 100 EventBridge deliveries |
| `POST` | `/moto-api/reset` | Alias used by moto's server-mode tests |

Open `http://localhost:5070/roto-api/` to browse created resources and their data:

- S3 buckets, object versions, bucket settings, and multipart uploads. Open a
  bucket to browse its objects, preview up to 64 KiB as text, or download a version.
- DynamoDB tables and items, including backups; SQS queues and messages.
  Browsing messages does not receive, acknowledge, or change their visibility.
- CloudFormation stacks, including templates, resources, and outputs.
- Lambda functions and event-source mappings; EventBridge buses, rules, and targets;
  SNS topics and subscriptions; IAM resources; SSM parameter versions; secrets
  and their versions.
- Lambda invocation results and logs, S3 notification handoffs, and EventBridge
  delivery history.

The page includes search, expandable JSON details, manual refresh, optional
refresh every five seconds, and pagination. Resource lists include **all stored
accounts and regions**, with 50 records per page. History tabs use unsigned
inspection in the default server account and `us-east-1`, limited to the API's
latest 100 records. Search applies to the loaded page. The UI is embedded in the
server binary and requires no frontend build or external assets.

Resource JSON supports `offset` (default `0`) and an optional exact-match
`field`/`value` filter, for example:

```sh
curl 'http://localhost:5070/roto-api/resources/s3/objects?field=bucket&value=uploads&offset=0'
curl 'http://localhost:5070/roto-api/s3/object?bucket=uploads&key=hello.txt&preview=true'
```

Responses contain `records`, `total`, `offset`, and `limit`. The object endpoint
accepts `bucket`, `key`, an optional `version`, and `preview=true` for the first
64 KiB. Downloads are served as attachments. These inspection endpoints only
read resources; they do not modify service state.

Reset deletes the instance's service state, including persistent state:

```sh
curl --fail -X POST http://localhost:5070/roto-api/reset
```

## Finding unsupported API calls

Run your app against roto, then inspect the calls that need implementation or a
local stub:

```sh
curl --fail http://localhost:5070/roto-api/unsupported
```

The server logs each unsupported call to **stderr** at warning level. `RUST_LOG`
can filter these warnings; the discovery endpoint records them regardless of the
log level. Normal validation errors, missing resources, and successful requests
do not appear in the list. Unsupported calls keep their existing AWS error
responses; recording a call does not make it succeed.

For example, a boto3 `iot-data.publish` request currently produces an entry like:

```json
{
  "calls": [{
    "service": "iotdata",
    "operation": null,
    "method": "POST",
    "path": "/topics/devices%2F123",
    "account_id": "123456789012",
    "region": "us-east-1",
    "reason": "unroutable",
    "count": 1
  }],
  "dropped_calls": 0
}
```

`service` is the SigV4 signing service or the selected handler's service name.
For boto3's `iot-data` client, the signing name is `iotdata`. `operation` comes
from a JSON target, a Query `Action`, or the operation named by a native
unsupported-operation error. Unknown REST routes can have a null operation;
use their service, method, and path to identify the call. A null service means
the request could not be assigned a service at all.

Reasons are `unroutable` (no service handler), `not_implemented` (a service reports
unsupported behavior), and `unknown_operation` (a service rejects the operation
or route). Counts group by all displayed fields except `count`, including the
account and region. SDK retries count as additional requests.

The list is kept in memory and cleared on restart or either reset endpoint.
It holds up to 1,000 distinct entries; existing entries keep counting after that
limit, while `dropped_calls` counts requests for additional distinct entries.
The recorder stores no request bodies, query strings, or headers. Paths remain
visible, including resource names supplied in the URL. Payload capture and Lua
fallback handlers are not implemented yet.

Future fallback handlers will also need a response contract: either raw HTTP
status, headers, and body, or an AWS-shaped result serialized by a model-aware
adapter. Different APIs have different responses; IoT `Publish`, for example,
returns an empty HTTP 200 response on success. The discovery list identifies
calls, but does not describe their input/output schemas.

## Accounts and regions

The server reads the service, region, and access key from the SigV4 credential
scope, including presigned URL credentials. Host names provide a service fallback;
handlers can claim supported unsigned requests, such as STS web-identity requests.
For unsigned requests, the user agent can supply a region; otherwise the region
falls back to `us-east-1`.

IAM user access keys and STS sessions resolve to their owning account. Assuming
a role in another account switches the request account. Unknown keys use the
default account. These mechanisms route requests; they do not verify signatures
or enforce IAM, ACL, bucket-policy, or trust-policy authorization.

## Lua setup

Lua can declare queues, Lambda functions backed by shell scripts or HTTP endpoints, and SQS
event-source mappings. See [Lua setup](lua.md), [Lambda executors](lambda.md), and [EventBridge wiring](eventbridge.md).

```sh
roto-server --ephemeral --setup examples/lua/setup.lua
```
