# Handover: S3 object links (serve bodies from their original file)

Written 2026-10-12 and implemented the same day (see "As built" below). Requested by the dh-core
project, which seeds its buckets from `Assets/demodata/*` (including a 1M-row
`.csv.gz`) and `output/dbscripts/dbscripts.tgz` and currently has to upload, so copy,
every file on each start.

## Goal

Register an object by reference: roto stores only the metadata row and a pointer to a
host file, and serves the body from that file. No copy into `roto-data/s3`, no upload
step, and edits to the file show up on the next read.

Lua API, startup-only like the rest of `roto.*` setup:

```lua
roto.s3.link("dh-assets-bucket", "demodata/demo_data_1k.csv.gz", "Assets/demodata/demo_data_1k.csv.gz")
roto.s3.link_dir("dh-assets-bucket", "demodata/", "Assets/demodata")  -- recursive, every file
```

- Relative host paths resolve against the setup script's directory, like `roto.lambda.bind`
  `cwd` does (`bind_parent` in `crates/roto-server/src/setup.rs`).
- The bucket is created if missing (same `CreateBucket` behaviour as the other helpers).
- Optional third-arg table on `link`: `content_type`, `metadata`, `headers`. Otherwise
  guess content type from the extension; `.gz` and `.tgz` stay `binary/octet-stream`.
- Setup runs on every start and persisted state survives, so `link` must be idempotent:
  find the current row for `(bucket, key)`; if it is already a link to the same path,
  refresh size/etag/mtime; if it is a normal object, replace it with the link.
- Missing file: `link` raises a Lua error with the setup.lua location. `link_dir` on a
  missing directory also errors; an empty directory is fine.

## Where bodies are handled today

`crates/roto-svc-s3/src/blobs.rs` stores bodies at `<root>/<bucket>/<escaped key>` and the
`objects.path` column holds that path relative to `<root>`. An empty `path` means no body
(delete markers, folder keys). Every body touch point in `service.rs`:

| Site | What it does with `o.path` |
| --- | --- |
| `read_body` (~341) | `blobs.read` for GET and internal reads |
| `get_object` body (~691-698) | `blobs.read` or `blobs.read_range` |
| `drop_row` (~252) | `blobs.remove` and prunes empty dirs |
| `retire_latest` (~239) | `blobs.move_rel` into `.roto/<bucket>/versions/...` |
| `promote_newest` (~274) | `blobs.relocate_live` back to the live tree |
| `store_object` (~307) | `blobs.write_live`, then inserts the row |
| `copy_object` (~1112) | reads the source body and stores a new object |
| bucket delete (~39, ~843) | `remove_bucket` deletes the bucket directory |

Multipart parts live under `.roto/<bucket>/uploads` and never reference a link.

## Design

1. **Marker.** Keep `objects.path` as a `String`. A link is `link:<absolute host path>`.
   No schema migration, and `path.is_empty()` checks keep working. Add helpers in
   `blobs.rs`: `is_link(&str)`, `link_target(&str) -> Option<PathBuf>`.
2. **Reads.** `Blobs::read` and `read_range` check for the marker and open the host
   file instead of `self.abs(rel)`. Everything above them is unchanged, so GET, HEAD,
   Range and listings work.
3. **Never mutate the host file.** This is the one rule that matters:
   - `drop_row`: for a link, delete only the row. Do not call `blobs.remove`.
   - `retire_latest` (versioning, or an overwrite): a link has nothing to move.
     Keep the pointer on the retired row. The non-current version still serves from
     the host file.
   - `promote_newest`: a link has nothing to relocate; keep the pointer as is.
   - `store_object` (PUT over a linked key): works unchanged. It writes a new body file
     and a new row, so the link is replaced, not written through.
   - `remove_bucket`: removes the directory tree, which holds no linked bodies, so it
     is already safe. Add a test.
4. **Stale links.** Size and ETag are captured at link time. On every read, `stat` the
   file. If it is gone, return `InternalError` (not 404) with a message naming the path,
   since a 404 hides a broken fixture. If the size changed, serve the current bytes and
   do not rewrite the row (v1). `HEAD` and listings report the stored size, so document
   that editing a linked file requires restarting roto, or relinking.
5. **ETag.** v1 uses md5 of the file computed at link time, streamed in chunks. The
   1M-row gz costs a fraction of a second, acceptable at startup. If that ever hurts,
   switch to a lazy md5 on first HEAD or GET, or a size+mtime pseudo-ETag. Most SDKs do
   not verify a GET ETag. Multipart-style ETags are not needed.
6. **Lua binding.** Add `roto.s3.link` and `roto.s3.link_dir` in
   `crates/roto-server/src/setup.rs`, next to `sqs_api`/`lambda_api` (new `s3_api`
   table). The S3 service handle needs passing into `setup::run` the way `sqs` and
   `lambda` are. Reuse `setup_only(lua)?` and `options(&opts, &[...])` for validation.
   S3 core gets a new method, `S3::link_object(&self, ctx, bucket, key, host_path, attrs)`,
   in `service.rs`. It does the same transaction as `store_object` minus the body write.
7. **Ephemeral mode.** Works the same, since linked files are outside the temp data dir.
   Make sure the temp-dir cleanup never follows a link.

## Tests

- Unit (`blobs.rs`): marker round-trip, read and range through a link, missing target.
- Service (`service.rs`): link, GET, HEAD, Range, list; delete the key and assert the host
  file still exists; PUT over a link and assert the host file is unchanged; versioning on:
  overwrite a link and read the old version; delete the bucket and assert the host files.
- Setup (`setup.rs`, follow `setup_can_seed_other_services_and_load_relative_modules`):
  `link` with a relative path, `link_dir` recursion and prefix, rerun is idempotent,
  missing file error has `setup.lua:N`.
- Smoke: add a link to `tests/smoke-*.py` or a small demo using `aws s3 cp` and `aws s3 ls`.

## Docs

`docs/storage.md` currently says copying files into a bucket directory does not register
objects; add a section pointing to `roto.s3.link` as the supported way. Add the two
functions to `docs/lua-api.md` and a note in `docs/s3.md`.

## Open questions

- Windows: `link:` plus drive-letter paths. Probably fine as an opaque string, but test.
- Should `link_dir` skip dotfiles and obey a glob filter? dh-core needs neither yet.
- Should linked objects show a header like `x-roto-linked: true` in `/roto-api`
  inspection output? Cheap and helpful when debugging.

## dh-core usage once this lands

`dolib/roto-setup.lua` in dh-core would end with:

```lua
roto.s3.link_dir("dh-assets-bucket", "demodata/", "../Assets/demodata")
roto.s3.link("dh-assets-bucket", "dbscripts.tgz", "../output/dbscripts/dbscripts.tgz")
```

Until then dh-core uses `io.open` + `roto.request("s3", "PUT", ...)` in setup.

## As built

- `roto.s3.link` / `link_dir` are in `crates/roto-server/src/setup.rs`; `S3::link_object`,
  `LinkAttrs` and the `Body::Link` path are in `crates/roto-svc-s3/src/service.rs`; the
  `link:` marker helpers and link-aware reads are in `blobs.rs`.
- **ETag differs from the design:** size, plus an md5 of path+size+mtime, taken from `stat`;
  the file is never read at link time. Size/ETag are still not refreshed on HEAD.
- `setup::run` now takes an `Arc<S3>`.
- Not done: the `x-roto-linked` inspection header, dotfile/glob filters, Windows testing,
  and the `tests/smoke-*.py` link case.
- Also added: `--enable-lua-api` and `POST /roto-api/lua` (`crates/roto-server/src/lua_api.rs`)
  to run setup-API scripts against a live server; loopback only, rejects `Origin` requests.

---

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
