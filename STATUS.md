# Status

What works today, measured against moto's own test suite (vendored unmodified, run in moto's
server mode against roto; see `scripts/run-moto-tests.sh`). Numbers are from the last run on
2026-10-09 against moto 5.0.11's tests. "Known failing" tests are listed per service in
`tests/moto/expected_failures/`; a normal run is green against that baseline, so any new failure
is a regression.

| Service | API coverage | moto tests passing | Notes |
|---|---|---|---|
| **STS** | 7 / 8 operations | 24 / 25 | Last failure needs DynamoDB. No `DecodeAuthorizationMessage`. |
| **SQS** | 19 / 23 | 132 / 139 (6 skipped upstream) | 7 failures need CloudFormation. Not done: message-move tasks, `ListDeadLetterSourceQueues`. |
| **S3** | 66 / 116 | 235 / 368 (59 skipped) | See [S3](#s3) below. |
| **IAM** | 28 / 180 | 17 / 349 | Users, roles, access keys, tags, account aliases. Missing: managed policies, groups, instance profiles, providers. |
| DynamoDB, Lambda, SNS, … | not started | – | See `PLAN.md`. |

Credentials are issued and tracked but **never enforced**: no signature verification, IAM policy
evaluation, bucket policies, ACL checks or trust-policy checks. This is deliberate.

## How requests are handled

* One binary, one port (default 5000, same as `moto_server`). `--ephemeral` keeps everything in
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
| `query` (form request, XML response) | done | STS, IAM |
| `json` 1.0 / 1.1 with query-compatible errors | done | SQS |
| `rest-xml` (URI/query/header/payload bindings, XML bodies, route table) | done | S3 |
| `rest-json`, `ec2` query | not started | Lambda, API Gateway, EC2, … |

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
tagging, SSE echo of the requested mode); versioning with delete markers and version-specific
reads/deletes; multipart (create, upload part, upload part copy, list parts/uploads, complete with
ordering/size/etag validation, abort); ACLs for buckets and objects (canned, header grants and
bodies; stored, not enforced); listing v1/v2/versions with prefix, delimiter, paging and
url-encoding; `GetObjectAttributes`; bucket sub-resources stored and returned verbatim (CORS,
lifecycle, website, encryption, replication, ownership controls, public-access-block, logging,
notification, accelerate, request-payment, tagging, policy); path-style and virtual-host
addressing; `aws-chunked` uploads.

Not working yet (the bulk of the 133 known failures): object lock / retention / legal hold,
`RestoreObject`, `SelectObjectContent`, response checksums (CRC32/SHA1/SHA256), KMS/SSE details,
inventory/analytics/metrics configurations, event notifications (EventBridge/SNS/SQS/Lambda),
server access logging delivery, lifecycle execution, website/CORS serving, and anything that
depends on enforcement (anonymous access, bucket policies, presigned-URL auth).

## Tests and tooling

* `cargo test` - unit tests (codecs, key paths, blob store, chunked decoding, ACLs, store).
* `scripts/run-moto-tests.sh <test_dir>` - moto's tests against roto; `TARGET=moto` runs them
  against real moto to separate upstream quirks from roto gaps; `UPDATE_EXPECTED=1` re-baselines.
  Moto's tests hardcode `localhost:5000`, so run on the default port.
* `scripts/sync-moto-tests.sh <test_dir>...` - vendors tests from the pinned moto tag; modules
  that need moto's in-process internals are listed in `tests/moto/not_portable.txt`.
* CI (`.github/workflows/ci.yml`): fmt, clippy `-D warnings`, unit tests, generated-code drift,
  and the STS/SQS/S3 moto suites.

## Next

DynamoDB (JSON 1.0, expression parser), then the long tails: IAM managed policies and groups,
S3 object lock and checksums. See `PLAN.md`.
