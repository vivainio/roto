# Roto: an AWS simulator in Rust (moto rewrite)

## Goals / non-goals
- Single static binary, one port, speaks AWS wire protocols; SDKs/CLI/Terraform point at it via endpoint_url.
- Fast startup, low memory, persistent by default.
- Non-goals: moto's in-process `@mock_aws` decorator mode; full parity with moto's ~150 services.
- Reference: Python moto 5.0.11 (installed locally) for differential testing. Moto and botocore models are Apache-2.0.

## Architecture (Cargo workspace)
| Crate | Responsibility |
|---|---|
| `roto-server` | Binary. axum/hyper listener, routing, CLI, health + reset endpoints |
| `roto-core` | Store layer (SQLite + migrations), account/region scoping, ARNs, errors, ID/time generators, SigV4 scope parsing (no verification by default) |
| `roto-protocol` | Codecs: query, json 1.0/1.1, rest-json, rest-xml, ec2-query; per-protocol error serialisation |
| `roto-codegen` | Generates typed input/output structs, validation, (de)serialisation from botocore `service-2.json` |
| `roto-svc-*` | One crate per service implementing a handler trait over generated types |
| `roto-tests` | Differential + conformance suite against moto |

Key decision: generate types from botocore models; services only implement business logic.

Request flow: route (SigV4 credential scope / X-Amz-Target / host / path) -> decode -> handler against store -> encode output or error.

## Storage (persistent)
- SQLite (rusqlite, bundled) for metadata and structured state; one DB file per service, `account_id` + `region` key columns.
- WAL, `synchronous=NORMAL` (`--durable` for FULL), busy_timeout; dedicated writer thread / spawn_blocking so tokio never blocks.
- Typed tables per service (e.g. SQS `queues`, `messages(visible_at, ...)`), not generic KV.
- Versioned migrations per DB. On-disk format unstable until v1.0 (explicit wipe-on-incompatible option).
- One SQLite transaction per API operation; cross-service effects (SNS->SQS) via transactional outbox.
- Time-based behaviour stored as absolute timestamps; background reaper handles expiry (survives restarts).
- Modes: `--data-dir <path>` (default, persistent), `--ephemeral` (in-memory/tmpfs). Same code via `Store` trait.

### S3 on a real filesystem layout
```
$ROTO_DATA_DIR/s3/
  <bucket>/<key path>         # current version of each object as a real file
  .roto/<bucket>/versions/<key-hash>/<version-id>   # noncurrent versions, delete markers
  .roto/<bucket>/uploads/<upload-id>/<part-n>       # in-flight multipart parts
  .roto/tmp/                                        # staging for atomic writes
```
- SQLite is the source of truth for metadata (ETag, content-type, user metadata, tags, ACLs, version IDs) and for all listings. Never answer ListObjects via readdir.
- Key -> path rules (`KeyPath` module, property-tested: lossless round-trip, no two keys collide):
  - `a` and `a/b` coexist: if `a` is a directory, object `a` lives at `a/.roto-self`; promotion/demotion happens with the metadata change, rename last.
  - Folder placeholder objects (`photos/`): directory + marker row.
  - Unsafe segments (`.`, `..`, empty, NUL, leading `/`): deterministic reversible percent-escape.
  - Segments > ~200 bytes: `<prefix>~<hash>`; real key in DB.
  - Detect case-insensitive / normalising filesystems at startup; use collision-safe encoding there, literal on ext4/XFS.
- Writes: temp file in `.roto/tmp` -> fsync -> atomic rename -> commit SQLite. Startup recovery reconciles orphans/mismatches.
- Multipart: parts staged, concatenated into tmp, renamed on complete.
- No dedup (accepted cost of real layout).
- **External edits: opt-in rescan.** Default is read-only browsing of the tree. `roto s3 rescan [bucket]` (explicit command) walks the tree and reconciles with the DB: new files become objects (compute ETag, size, content-type guess), deleted files drop their rows, changed files (mtime/size) get new ETag. No always-on watcher initially (possible later `--watch`). Lets users seed buckets with `cp -r fixtures/ $DATA/s3/mybucket/ && roto s3 rescan`.

## Phases
0. **Foundation (1-2 wk):** workspace, CI, moto reference setup, server skeleton, SigV4 scope parser, router, `Store` layer + migrations, codegen pipeline proven on STS `GetCallerIdentity`, error model, `/roto-api/reset`.
1. **Core quartet (4-6 wk):** STS+IAM; SQS; S3 (buckets, objects, multipart, versioning, presigned URLs, tags/ACL/policy, path+virtual-host style, rescan); DynamoDB (own expression-parser crate; GSI/LSI, query/scan, transactions).
2. **Common services (6-8 wk):** SNS, Secrets Manager, SSM, KMS, CloudWatch Logs/metrics, EventBridge, Kinesis, Lambda (API/state), CloudFormation subset.
3. **Infrastructure:** EC2 (metadata only), ELBv2, Route53, ECS, ECR, Step Functions, API Gateway, Cognito, ACM.
4. **Long tail + hardening:** usage-driven services, IAM policy enforcement, fault injection, Docker image.

## Testing
1. Differential tests: same boto3 scripts vs moto_server and roto; diff normalised responses (mask IDs/timestamps/ordering).
2. **Moto's own test suite, vendored unmodified** (`tests/moto/tests/<service>`, pinned by `MOTO_VERSION`, Apache-2.0, see NOTICE) and run in moto's `TEST_SERVER_MODE` against roto on port 5070 (roto also serves `POST /moto-api/reset`).
   - `scripts/sync-moto-tests.sh <test_dir>...` vendors services; `scripts/run-moto-tests.sh <test_dir>` runs them.
   - Tests that drive moto internals in-process (Flask test client, backends) cannot target another server; the sync script lists them in `tests/moto/not_portable.txt` and they are ignored. Their intent gets re-implemented as native Rust tests instead.
   - `tests/moto/expected_failures/<service>.txt` is the baseline of known failures (deselected). Goal per service: empty list. CI fails on regressions; `UPDATE_EXPECTED=1` re-baselines after progress.
   - Server-mode caveats: tests relying on `freeze_time` or on `settings.TEST_SERVER_MODE`-guarded branches behave as upstream's server mode does.
3. SDK smoke: AWS CLI, aws-sdk-rust, aws-sdk-js v3, Terraform AWS provider.
4. Property/fuzz: codecs, expression parsers, `KeyPath`.
5. Protocol corpus from botocore's `tests/unit/protocols`.
6. Persistence: kill -9 mid-operation + restart consistency (incl. between S3 rename and SQLite commit); migration test per schema version; rescan tests.
7. Auto-generated coverage matrix (implemented ops vs botocore ops per service). A service is "done" only when matrix % and differential tests pass; unsupported ops return an explicit error, never silent wrong answers.

## Risks
- S3/DynamoDB long-tail fidelity -> differential testing.
- Protocol edge cases (aws-chunked uploads, Expect: 100-continue, XML quirks) -> botocore protocol corpus.
- SQLite single-writer throughput -> batching for BatchWriteItem / bulk S3 ops.
- Filesystem key mapping edge cases -> KeyPath property tests, capability detection.
- Scope creep -> coverage-matrix gate.

## Open items
- Compatibility target: exact moto error messages/IDs vs "good enough for SDKs".
- Default libs: tokio, hyper/axum, serde, quick-xml, rusqlite, dashmap-free (state is in SQLite).
- Next step: Phase 0 remainder, then Phase 1 (IAM, SQS, S3, DynamoDB), each gated on its vendored moto tests.

## Target services (what the user's stack actually uses)
`s3, sqs, kms, kinesis, dynamodb, secretsmanager, lambda, sns, ssm, iam, events, iot, iot-data` (plus STS, which clients need for identity).
Everything else in moto is out of scope. Order after DynamoDB: **SNS, SSM, Secrets Manager, KMS, Kinesis**, then
**IAM completion** (managed policies, groups, instance profiles). Lambda now has a local execution
backend: command argv or HTTP POST bindings, configured separately from AWS function metadata,
with synchronous/asynchronous Invoke and S3 notifications. Embedded Lua (`mlua`) provides startup resource setup over the same service operations, including
standard SQS event-source mappings. EventBridge now routes custom/S3 events to Lambda and SQS
through persisted pattern rules and delivery retries; schedules and input transformers remain future work. Packaged runtimes, FIFO polling and Kinesis polling remain future work.
Deprioritised on request: DynamoDB backups/PartiQL/import-table, and chasing exact error wording in long tails.

### Generic local API hooks (planned; IoT publish first)

Unsupported-call discovery is implemented: server warnings on stderr and
`GET /roto-api/unsupported` provide deduplicated HTTP calls and counts without
capturing payloads. See the book's server reference for limits and reset behavior.

The immediate IoT use case is an app calling boto3 `iot-data.publish` over HTTP.
A local MQTT broker is not required to exercise that publisher. Start with a
shared local hook mechanism, configured by trusted Lua startup setup, that can
forward requests to an HTTP endpoint or command executor.

- **Fallback API handlers:** explicitly registered handlers for unsupported
  service/operation pairs. Execute synchronously and supply a response or error;
  unregistered operations continue to return explicit unsupported errors.
  Native implemented operations retain precedence.
- **Event hooks:** observers of successful supported operations, with an
  explicit asynchronous delivery/retry contract. These are distinct from
  fallback handlers, which must produce the calling SDK's response.
- **Shared executor contract:** reuse command argv/stdin/stdout and HTTP POST
  execution infrastructure. Requests include account, region, service,
  operation, method, path, query, headers and a base64 body for binary payloads.
  Responses specify status, headers and a base64 body. Omit credentials from
  forwarded headers. Executors have bounded timeouts and output sizes.
- **Protocol adapters:** identify operations using JSON targets, Query actions,
  or model-generated REST routes. The generic hook transport does not remove
  the need for service-specific HTTP bindings or AWS-compatible response
  serialization. Unsupported services need routing before fallback dispatch.
- **First adapter:** `iotdata` (the SigV4 signing name for boto3 `iot-data`),
  `Publish`, using the botocore REST-JSON model. A Lua-configured HTTP or command
  handler consumes the topic and payload and returns the expected empty 200
  response after successful execution. This provides local app integration,
  without claiming MQTT subscriber delivery, retained state, or rules support.
- **Validation:** real boto3 publish to both executor types, binary payloads,
  account/region context, executor errors/timeouts, native-handler precedence,
  and unchanged unsupported errors when no hook is configured.

IoT APIs and generic hooks are not implemented yet. Endpoint discovery, device
shadows, MQTT transports, retained messages, and IoT rules remain follow-ups
based on actual usage.

## Status
- Phase 0 done: workspace, `roto-core` (store/migrations/SigV4 scope), `roto-protocol` (query+XML), `roto-codegen` (botocore model -> typed code), `roto-svc-sts` (GetCallerIdentity, GetAccessKeyInfo), `roto-server`, vendored moto STS tests, CI.
- Phase 1 started: JSON 1.0/1.1 codec + codegen (maps, blobs, required members as plain values); **SQS** service on SQLite (queues, messages, receipt-handle history, DLQ redrive, FIFO + dedup, long polling, batch ops, permissions, tags) - 131/139 of moto's `test_sqs` pass against roto; the 8 failures need CloudFormation (7) and STS AssumeRole (1) and are in `tests/moto/expected_failures/test_sqs.txt`.
- **REST-XML codec + codegen** (S3 first; reusable for Route 53 / CloudFront, and the binding layer carries over to rest-json): routes are generated from `requestUri` plus required query-string members (this is what separates `UploadPart` from `PutObject`), deprecated twins lose ties, header-keyed variants (`x-amz-copy-source`) are listed in the generator.
- **S3 on SQLite + real files** (as designed above): `keypath` (injective key -> path mapping, `a` and `a/b` coexist via `.roto-self`), `blobs` (atomic temp+rename writes, version and multipart staging under `.roto/`), `aws-chunked` decoding, virtual-host and path-style addressing. Implemented: buckets, objects (put/get/head/delete/copy, ranges, conditionals, versioning with delete markers, tagging, ACLs), listing (v1/v2/versions, delimiters, url-encoding), multipart (create/upload/copy-part/complete/abort/list), and generic bucket sub-resources (cors, lifecycle, website, encryption, replication, ownership, public-access-block, logging, notification, accelerate, request-payment, tagging, policy). Not yet: object lock/retention/legal hold, restore, select, inventory/analytics/metrics configs, CORS/website serving, event notifications.
- `TARGET=moto scripts/run-moto-tests.sh <dir>` runs the same suite against real moto, to separate upstream quirks from roto gaps (moto itself is the oracle for ambiguous behaviour).
- Planned: a second codegen input, AWS's Smithy models (`aws/api-models-aws`), for typed errors (`awsQueryError`, `httpError`) and protocol test cases.
- Not yet: rest-json/rest-xml/ec2 codecs, state-backed services using the store, reset of persisted state, coverage-matrix report.
