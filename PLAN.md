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
2. Port moto's own per-service tests, run via boto3 against roto.
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
- Next step: scaffold Phase 0.
