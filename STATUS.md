# Status

What works today, measured against moto's own test suite (vendored unmodified, run in moto's
server mode against roto; see `scripts/run-moto-tests.sh`). Numbers are from the last run on
2026-10-10 against moto 5.0.11's tests with Python 3.12. The IAM policy update was checked locally with Python 3.9. "Known failing" tests are listed per service in
`tests/moto/expected_failures/`; a normal run is green against that baseline, so any new failure
is a regression.

| Service | API coverage | moto tests passing | Notes |
|---|---|---|---|
| **STS** | 7 / 8 operations | 25 / 25 | DynamoDB multi-account integration passes. No `DecodeAuthorizationMessage`. |
| **SQS** | 19 / 23 | 139 / 139 (6 skipped upstream) | CloudFormation queue integration passes. Not done: message-move tasks, `ListDeadLetterSourceQueues`. |
| **S3** | 66 / 116 | 241 / 368 (59 skipped) | See [S3](#s3) below. |
| **IAM** | 47 / 180 | 107 / 349 (13 skipped) | Users, roles, access keys, tags, account aliases, customer-managed policies and versions, user/role attachments and entity listings, and inline role policies. CloudFormation supports IAM roles and `AWS::IAM::Policy`. IAM policy evaluation is intentionally absent. Missing: AWS-managed policy catalog, full policy-document validation, groups, instance profiles, providers. |
| **DynamoDB** | 47 / 57 | 414 / 529 | Tables, items, condition/update/projection expressions, query/scan, GSI/LSI (computed at query time), batch, transactions, tags, TTL, backups. Missing: PartiQL, ImportTable, streams. |
| **SSM** | 13 / 152 | 75 / 156 | Parameter Store complete for normal use (versions, labels, history, hierarchy, tags, SecureString, filters). Not done: documents, commands, maintenance windows, patch baselines, public AMI/service parameters. |
| **Secrets Manager** | 20 / 23 | 104 / 134 (2 skipped) | Secrets, versions and staging labels, deletion/restore, tags, resource policies, rotation bookkeeping (no Lambda invocation), listing with filters, random passwords, batch get. Missing: cross-region replication, rotation via Lambda. |
| **SNS** | 19 / 42 | 126 / 184 (1 skipped) | Topics, subscriptions (SQS fan-out in-process, raw delivery, filter policies on attributes or body), publish/batch, FIFO checks, tags, permissions. Not done: platform applications/endpoints, SMS attributes, HTTP/Lambda/email delivery. |
| **Lambda** | 19 / 85 | 8 / 125 (30 skipped) | Function metadata/code bookkeeping, tags, stored permissions, Invoke (sync/async/dry-run), local command and HTTP executors, persistent jobs/results/logs. S3 notifications and standard SQS event-source mappings supported; embedded Lua startup setup. Native executor/restart tests and SDK smoke; no versions/aliases, packaged-runtime execution, FIFO or Kinesis polling. |
| **EventBridge** | 15 / 57 | 23 / 136 (5 skipped) | Default/custom buses, pattern rules, Lambda/SQS targets, persistent delivery/retries, S3 bucket events, Lua setup. No schedules, input transformers, archives/replays, cross-account targets or permission APIs. |
| **CloudFormation** | 16 / 90 | 25 resource integration tests | Synchronous stacks for SQS/SNS/S3/DynamoDB/Kinesis/IAM/Lambda, change sets, events, templates, refs/conditions, tags, outputs, dependency ordering, persistence, rollback attempts, retention, stack policies, termination protection. Missing: nested stacks, imports, transforms, rollback triggers, Lambda configuration updates, full policy semantics. |
| **Kinesis** | 28 / 39 | 76 / 76 | Persisted streams/shards/records, hash routing, iterators, split/merge/scaling, retention, tags, consumer registration, stored encryption/monitoring settings, CloudFormation streams. No enhanced fan-out streaming, Lambda polling, throughput enforcement, resource policies or actual encryption. |
| **KMS** | 27 / 54 | 152 / 225 Moto tests | Persisted symmetric keys and aliases, alias/key listing and updates, tags, key policies, rotation status, deletion scheduling, Encrypt/Decrypt/ReEncrypt and random data keys. 73 known Moto failures remain; mostly asymmetric operations, grants, multi-region behavior and policy enforcement. Ciphertext uses a reversible base64 JSON envelope. |

Credentials are issued and tracked but **never enforced**: no signature verification, IAM policy
evaluation, bucket policies, ACL checks or trust-policy checks. This is deliberate.

## How requests are handled

* One binary, one port (default 5070). `--ephemeral` keeps everything in
  memory/temp files; otherwise state persists under `--data-dir` (default `./roto-data`).
* Requests are routed by the SigV4 credential scope (service + region + access key), falling back
  to the host name, then to a service "claiming" an unsigned request (STS web-identity / SAML).
* Multi-account: an access key resolves to the account that owns it (IAM user keys, STS sessions),
  so `AssumeRole` into another account really switches accounts. The default account is
  `123456789012` (`ROTO_ACCOUNT_ID`).
* `POST /moto-api/reset` and `POST /roto-api/reset` clear all state; `GET /roto-api/health`.

## Wire protocols

| Protocol | Status | Used by |
|---|---|---|
| `query` (form request, XML response; lists, maps, blobs) | done | STS, IAM, SNS, CloudFormation |
| `json` 1.0 / 1.1 with query-compatible errors | done | SQS |
| `rest-xml` (URI/query/header/payload bindings, XML bodies, route table) | done | S3 |
| `rest-json` | initial model-generated bindings | Lambda (JSON, URI/query/header, raw payload and status bindings) |
| `ec2` query | not started | EC2, … |

Types, (de)serialisation, routing tables and operation lists are **generated from botocore's
service models** (`models/*/service-2.json`) by `roto-codegen`, so a service crate contains only
behaviour. `scripts/gen.sh` regenerates; CI fails if generated code is stale.

## Storage

* One SQLite database per service (WAL, one transaction per API call, versioned migrations).
* **S3 object bodies are real files** mirroring the bucket layout:
  `<data>/s3/<bucket>/<key path>` holds the current version of every object, so the tree can be
  browsed or `rsync`ed. Non-current versions and in-flight multipart parts live under
  `<data>/s3/.roto/`. Writes are temp-file + atomic rename. A key that is also a prefix
  (`a` and `a/b`) is stored as `a/.roto-self`; unsafe key segments (`..`, empty, control
  characters, over-long) are escaped. SQLite is the source of truth for metadata and all listings.
* Not yet: `roto s3 rescan` (re-importing files edited by hand), crash-recovery reconciliation,
  and fsync controls beyond SQLite's `--durable`.

## S3

Working: buckets (create/head/delete/location/list, us-east-1 re-create semantics); objects
(put/get/head/delete/delete-many/copy, range and conditional requests, metadata, storage class,
tagging); versioning with delete markers and version-specific
reads/deletes; multipart (create, upload part, upload part copy, list parts/uploads, complete with
ordering/size/etag validation, abort); ACLs for buckets and objects (canned, header grants and
bodies; stored, not enforced); listing v1/v2/versions with prefix, delimiter, paging and
url-encoding; `GetObjectAttributes`; bucket sub-resources stored and returned verbatim (CORS,
lifecycle, website, encryption, replication, ownership controls, public-access-block, logging,
notification, accelerate, request-payment, tagging, policy); path-style and virtual-host
addressing; `aws-chunked` uploads. S3 → Lambda notifications for Put/Copy/multipart completion
and single/batch delete, with prefix/suffix filters and a persistent transactional outbox.
Buckets with `EventBridgeConfiguration` emit object creation/deletion events to the default bus.

Not working yet (the bulk of the 132 known failures): object lock / retention / legal hold,
`RestoreObject`, `SelectObjectContent`, response checksums (CRC32/SHA1/SHA256), KMS actual encryption and bucket-default encryption behavior,
inventory/analytics/metrics configurations, direct event notifications to SNS/SQS,
server access logging delivery, lifecycle execution, website/CORS serving, and anything that
depends on enforcement (anonymous access, bucket policies, presigned-URL auth).

## Tests and tooling

* `cargo test` - unit tests (codecs, key paths, blob store, chunked decoding, ACLs, store).
* `scripts/run-moto-tests.sh <test_dir>` - moto's tests against roto; `TARGET=moto` runs them
  against real moto to separate upstream quirks from roto gaps; `UPDATE_EXPECTED=1` re-baselines.
  Exclusions match exact test IDs. `AUDIT_EXPECTED=1` retries exclusions and reports newly passing
  tests; the weekly/manual `coverage-audit.yml` workflow runs this for every service.
  The runner adapts upstream's port 5000 URLs, Lambda's IAM fixture, DynamoDB's CloudFormation fixtures, and EventBridge delivery
  polling in a temporary copy. Docker tests are skipped; local executors have SDK smoke coverage.
* `scripts/smoke-cloudformation.py` - SDK checks for stack wiring, updates, DynamoDB index queries,
  restart, account/region isolation, validation, failed-resource cleanup, and reset.
* `scripts/smoke-kinesis.py` - SDK checks for account/region isolation, scoped cursors and paging, stream/record/tag persistence across restart, split-shard routing, closed-shard draining, and reset.
* `scripts/smoke-dynamodb.py` - SDK checks for account/region isolation with identical table names
  and item keys, persisted GSI/TTL/item state, and STS credentials across server restart.
* `scripts/sync-moto-tests.sh <test_dir>...` - vendors tests from the pinned moto tag; modules
  that need moto's in-process internals are listed in `tests/moto/not_portable.txt`.
* CI (`.github/workflows/ci.yml`): fmt, clippy `-D warnings`, unit tests, generated-code drift,
  vendored moto suites, Lua/EventBridge SDK smokes, and `scripts/smoke-lambda.py` (SDK command/HTTP Invoke and S3 → command → SQS).

## Local Lambda execution

`--lambda-executors <file.json>` binds function names or full ARNs to command argv arrays or
HTTP URLs. AWS APIs create/update the function metadata; local bindings select execution.
See [examples/lambda/README.md](examples/lambda/README.md) for configuration and contracts.
`--setup <file.lua>` declares resources, command/HTTP bindings, and SQS mappings using embedded
Lua. See [examples/lua/README.md](examples/lua/README.md). SQS failures retry through visibility
timeout and queue DLQ redrive, with optional partial batch responses.
Async jobs retry up to three attempts and persist across restart. Invocation history is at
`GET /roto-api/lambda/invocations`; pending/failed S3 handoffs at `GET /roto-api/s3/notifications`.
Delivery is at least once. Commands run with roto's OS permissions, without container isolation.

## Next

Extend KMS asymmetric operations and grants as needed, then IAM groups/instance profiles; extend Lambda integrations as needed. See `PLAN.md`.
