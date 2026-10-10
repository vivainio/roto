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

### Runtime Lua hooks (planned; IoT publish first)

Unsupported-call discovery is implemented: server warnings on stderr and
`GET /roto-api/unsupported` provide deduplicated HTTP calls and counts without
capturing payloads. See the book's server reference for limits and reset behavior.

The primary lifecycle feature is a post-create callback registered from
startup Lua, such as `hooks.created("lambda", "worker", callback)`. When a
matching resource is successfully created, the callback receives the resulting
resource metadata and can apply local configuration—for example, bind a command
or HTTP executor to a Lambda function created through the Lambda API. Resource
matching across logical IDs, physical names and ARNs, update behavior, duplicate
deliveries, and callback error handling need to be defined.

Hooks registered in startup Lua are intended to remain active while the server
handles later AWS requests. Request hooks are a separate capability:

- **`intercept_request`:** registered for an exact service operation (for
  example, `s3.PutObject`); it runs before native dispatch and may continue,
  return a simulated response/error, or implement that operation itself.
- **`missing_support`:** a single catch-all fallback after native dispatch
  cannot handle a request. It can inspect the service and operation and provide
  a response/error; declining preserves Roto's current unsupported response.

Normal validation and resource errors from implemented operations do not invoke
the fallback. Setup-time helper calls do not trigger runtime hooks. A no-hook
request should continue directly to native dispatch. The book's
[runtime-hooks plan](docs/runtime-hooks.md) tracks lifecycle, request/response,
concurrency, timeout, and validation decisions.

The first fallback use case is boto3 `iot-data.publish`. It should accept the
botocore REST-JSON request and return the empty HTTP 200 response expected on
success, without claiming MQTT subscriber delivery, retained state, or rules
support. Command/HTTP forwarding may implement this use case after the Lua hook
contract is established. IoT endpoint discovery, device shadows, MQTT
transports, retained messages, and IoT rules remain follow-ups based on actual
usage.

## Status
- Phase 0 done: workspace, `roto-core` (store/migrations/SigV4 scope), `roto-protocol` (query+XML), `roto-codegen` (botocore model -> typed code), `roto-svc-sts` (GetCallerIdentity, GetAccessKeyInfo), `roto-server`, vendored moto STS tests, CI.
- Phase 1 started: JSON 1.0/1.1 codec + codegen (maps, blobs, required members as plain values); **SQS** service on SQLite (queues, messages, receipt-handle history, DLQ redrive, FIFO + dedup, long polling, batch ops, permissions, tags) - 131/139 of moto's `test_sqs` pass against roto; the 8 failures need CloudFormation (7) and STS AssumeRole (1) and are in `tests/moto/expected_failures/test_sqs.txt`.
- **REST-XML codec + codegen** (S3 first; reusable for Route 53 / CloudFront, and the binding layer carries over to rest-json): routes are generated from `requestUri` plus required query-string members (this is what separates `UploadPart` from `PutObject`), deprecated twins lose ties, header-keyed variants (`x-amz-copy-source`) are listed in the generator.
- **S3 on SQLite + real files** (as designed above): `keypath` (injective key -> path mapping, `a` and `a/b` coexist via `.roto-self`), `blobs` (atomic temp+rename writes, version and multipart staging under `.roto/`), `aws-chunked` decoding, virtual-host and path-style addressing. Implemented: buckets, objects (put/get/head/delete/copy, ranges, conditionals, versioning with delete markers, tagging, ACLs), listing (v1/v2/versions, delimiters, url-encoding), multipart (create/upload/copy-part/complete/abort/list), and generic bucket sub-resources (cors, lifecycle, website, encryption, replication, ownership, public-access-block, logging, notification, accelerate, request-payment, tagging, policy). Not yet: object lock/retention/legal hold, restore, select, inventory/analytics/metrics configs, CORS/website serving, event notifications.
- `TARGET=moto scripts/run-moto-tests.sh <dir>` runs the same suite against real moto, to separate upstream quirks from roto gaps (moto itself is the oracle for ambiguous behaviour).
- Planned: a second codegen input, AWS's Smithy models (`aws/api-models-aws`), for typed errors (`awsQueryError`, `httpError`) and protocol test cases.
- Not yet: rest-json/rest-xml/ec2 codecs, state-backed services using the store, reset of persisted state, coverage-matrix report.

## Kinesis implementation (2026-10-10)

Kinesis now implements 28/39 model operations and passes all 76 portable Moto 5.0.11 tests.
Streams, shard lineage, records and cursors persist in SQLite. CloudFormation supports
Kinesis streams, including shard-count/retention/tag updates and YAML intrinsic short tags.
Native checks cover expiry, sequence continuity, batch validation, cursor isolation and
closed-shard draining; the SDK smoke covers restart and account/region isolation.
Enhanced fan-out streaming, Lambda Kinesis polling, throughput enforcement and resource
policies remain future work. Encryption and monitoring APIs store settings only.

KMS design agreed: persist key metadata and encode a versioned JSON envelope as base64
for simulated ciphertext (key ID, encryption context, base64 plaintext). No per-message
plaintext/ciphertext store or real encryption. Decode checks key state and context;
data-key generation still returns random bytes of the requested length. Asymmetric
cryptographic operations remain unsupported until separately implemented.

KMS core simulation implemented: CreateKey/DescribeKey, CreateAlias, EnableKey/DisableKey,
Encrypt/Decrypt/ReEncrypt, GenerateDataKey and GenerateDataKeyWithoutPlaintext.
Keys and aliases persist; native tests cover context/state/key checks, isolation,
binary payloads, data-key lengths, restart and reset. Metadata listing, policies,
grants, rotation and deletion remain future work.


## IAM customer-managed policies (2026-10-10)

IAM now implements 47/180 operations and passes 107/349 portable Moto tests
(13 additional tests skipped). Customer-managed policies support CRUD, up to five
versions, default-version selection, tags, user/role attachments, attachment counts,
and paginated policy, version, attachment and entity listings. Policies and versions
persist across restart and are scoped by account; attached policies and non-default
versions block policy deletion. Native checks cover persistence, pagination, version
limits and IDs, attachment idempotency, account isolation, deletion conflicts and reset.
The AWS-managed policy catalog, full policy-document validation, groups and instance
profiles remain follow-ups. Policy documents are stored without evaluating permissions.


## IAM groups (2026-10-10)

IAM group CRUD, membership, inline policies and customer-managed policy attachments
are implemented, bringing coverage to 62/180 operations and 127/349 portable Moto
tests passing. Group renames preserve memberships and both kinds of policies;
deletion requires removing users and policies first. Native checks cover account
isolation, idempotent membership, pagination and renamed relationships across restart.


## IAM instance profiles (2026-10-10)

Instance profiles now support create/get/list/delete, add/remove a role, listing by
role, and tags, bringing IAM to 72/180 operations and 133/349 portable Moto tests
passing (13 skipped). Profiles enforce a one-role limit, block deletion while a role
is attached, and prevent deleting an attached role. Native checks cover account
isolation, case-insensitive names, role/tag persistence, scoped and paginated listings,
cleanup/reset, tag replacement at capacity and rollback of invalid creation. The shared
tag updater now counts new keys rather than replacements toward the 50-tag limit.
Some remaining Moto profile tests use invalid paths or non-JSON trust policies that
Roto deliberately rejects. CloudFormation instance-profile resources remain unsupported.

## Diesel adoption (2026-10-10)

The shared store now provides typed Diesel SQLite connections, sharing the service
lock and database with existing migrations and read-only inspection. IAM's entire
implemented handler surface uses Diesel queries, with table declarations and stored
row models separate from generated AWS models. Persistence, rollback, ephemeral-store
isolation and inspection access have native coverage. The IAM Moto baseline remains
133 passing, 13 skipped and 216 excluded; the seeded demo and workspace tests pass.
Remaining services retain `rusqlite` for incremental conversion.


## SSM Diesel port (2026-10-10)

Parameter Store now uses typed Diesel records and queries for parameter versions,
labels, resource tags and scoped latest-version selection. The Moto baseline stays
75 passing, 2 skipped and 81 excluded. Native tests cover restart persistence,
account/region isolation with identical names, SecureString reads, label moves,
tag updates, history pagination, reset and the labeled-oldest-version pruning guard.
Secrets Manager and KMS are the next small CRUD candidates; S3, DynamoDB, SQS and
Lambda need more care around storage and delivery behavior.

## KMS and STS Diesel port (2026-10-10)

Key metadata, aliases and role sessions now use typed Diesel queries. Scoped alias
joins and credential account routing retain their existing behavior. Native tests
and service Clippy pass; Moto remains 152 passing for KMS and 25 for STS.

## Secrets Manager Diesel port (2026-10-10)

Secret metadata and version records now use named Diesel row models. All CRUD,
version-stage updates, deletion, filters and batch reads use typed queries.
Moto remains 104 passing, 2 skipped and 30 excluded; service Clippy passes.

## CloudFormation and EventBridge Diesel port (2026-10-10)

Stack snapshots and EventBridge buses, rules, targets and delivery queues now use
typed queries. Delivery retry ordering remains SQLite rowid order. CloudFormation
has 22 passing native tests (one existing ignored); EventBridge keeps its 23-pass,
5-skip Moto baseline. Service Clippy and the seeded demo smoke pass.

## Kinesis Diesel port (2026-10-10)

Streams, records, shard sequences and iterator tokens now use typed queries,
including record retention and stream token invalidation. All 76 Moto tests,
five native tests and service Clippy pass.

## SNS Diesel port (2026-10-10)

Topics and subscriptions use named Diesel rows and typed CRUD, preserving
creation order and fan-out behavior. Moto remains 126 passing, 1 skipped and
58 excluded. Native tests and service Clippy pass.

## SQS Diesel port (2026-10-10)

Queues, messages and receipt history use typed queries. FIFO blocking uses a
correlated alias; inserted messages return their sequence directly. Dead-letter
moves and receipt tombstones retain their behavior. All 139 Moto tests (6 skipped),
eight native tests and service Clippy pass.
