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
| `GET` | `/roto-api/health` | Returns `ok` |
| `POST` | `/roto-api/reset` | Clears state across services |
| `GET` | `/roto-api/lambda/invocations` | Latest 100 invocations, results and logs |
| `GET` | `/roto-api/s3/notifications` | Pending/failed S3 handoffs |
| `GET` | `/roto-api/events/deliveries` | Latest 100 EventBridge deliveries |
| `POST` | `/moto-api/reset` | Alias used by moto's server-mode tests |

Reset deletes the instance's service state, including persistent state:

```sh
curl --fail -X POST http://localhost:5070/roto-api/reset
```

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
