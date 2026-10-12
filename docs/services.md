# Service coverage

This overview describes the implementation on **2026-10-10**. See
[STATUS.md](https://github.com/vivainio/roto/blob/main/STATUS.md) for operation counts,
moto test results, and detailed gaps. A service being listed here does not mean
full AWS compatibility.

## Core services: first-class experience

The core services below are hand-tuned implementations in Rust, built into the
server binary. These are the services where roto aims to offer a **first-class
local development experience**, with working service behavior, persistence, and
cross-service integration. They are the focus of compatibility and behavior
improvements; the gaps listed below still apply.

**Most users should start with the core services.** Typical AWS applications use
services such as S3, SQS, DynamoDB, Lambda, SNS, and EventBridge for data storage,
messaging, and event-driven processing. The optional Lua bundle is mainly useful
when your local environment also needs service administration or configuration
APIs.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| STS | Caller identity, sessions, role assumption | Authorization-message decoding |
| SQS | Queues, messages, batches, FIFO, deduplication, long polling, DLQ redrive, tags | Message-move tasks, dead-letter source listing |
| S3 | Buckets, objects, listing, versioning, multipart, tagging, browser rendering and directory listings, stored ACLs and bucket configuration | Object lock, restore, select, checksum details, SNS/SQS notification delivery, policy enforcement |
| IAM | Users, roles, access keys, tags, account aliases, customer-managed policies and versions, user/role/group attachments and entity listings, groups and membership, inline role/group policies, instance profiles and role relationships; CloudFormation roles and `AWS::IAM::Policy` | AWS-managed policy catalog, full policy-document validation, user inline policies, providers; policy evaluation is not performed |
| DynamoDB | Tables, items, expressions, query/scan, GSI/LSI, batch, transactions, tags, TTL, backups | PartiQL, import, streams |
| SSM | Parameter Store, versions, labels, history, hierarchy, tags, SecureString | Documents, commands, maintenance windows, patch baselines, public parameters |
| Secrets Manager | Secrets, versions, staging labels, deletion/restore, tags, policies, rotation bookkeeping, batch get | Cross-region replication, Lambda rotation execution |
| SNS | Topics, subscriptions, publishing, SQS fan-out, filter policies, FIFO checks, tags | Platform endpoints, SMS attributes, HTTP/Lambda/email delivery |
| Lambda | Function metadata, local command/HTTP execution, S3 notifications, standard SQS mappings, Lua startup setup | Packaged runtimes, versions/aliases, FIFO/Kinesis polling |
| CloudWatch Logs | Log group CRUD, retention and tags, basic event filtering, local Lambda output | Log streams API, direct `PutLogEvents`, Insights, subscriptions, advanced filter patterns, retention enforcement |
| EventBridge | Default/custom buses, pattern rules, Lambda/SQS targets, S3 events, persistent delivery | Schedules, input transformers, archives/replays, cross-account targets, permissions |
| IoT Data Plane | `Publish` to local JSON or MQTT 3.1.1 WebSocket subscribers; QoS 0 delivery, `+` and `#` filters, subscribe/unsubscribe, keepalive | MQTT 5, QoS 2, client publish fan-out, authentication, retained messages, durable delivery, IoT Rules, shadows |
| API Gateway v2 WebSocket APIs and Management API | In-memory WebSocket API, Lambda proxy integrations, routes and route responses, stages/deployments, route selection, `PostToConnection` | HTTP APIs, custom domains, authorizers, non-Lambda integrations, `GetConnection`, `DeleteConnection`, IAM/permission enforcement |
| Kinesis | Streams, shards, records, polling, expiring iterators, resharding, retention, tags, consumer registration, encryption/monitoring metadata | Enhanced fan-out streaming, Lambda polling, throughput enforcement, resource policies, actual encryption |
| KMS | Persisted symmetric keys and aliases, listing, tags, key policies, rotation status, deletion scheduling, Encrypt/Decrypt, ReEncrypt, random data keys | Real encryption, asymmetric operations, grants, multi-region keys, policy enforcement |
| CloudFormation | Synchronous stacks for SQS/SNS/S3/DynamoDB/Kinesis/IAM/Lambda, recursive nested stacks from S3 templates, cross-stack exports/imports, refs, outputs, tags, change sets, events, rollback attempts, retention, stack policies, termination protection, persistence | Nested change sets, imports of existing resources, transforms, rollback triggers, Lambda configuration updates, full policy semantics |


KMS uses a versioned base64 JSON envelope for simulated ciphertext; it provides no
cryptographic protection. See [KMS](kms.md).

## Optional Lua services: completeness coverage

Lua services are provided for completeness, with mostly simple resource CRUD,
metadata storage, and configuration simulation. They have a narrower scope than
the core Rust services and do not carry the same first-class experience goal.
For example, RDS does not run a SQL database, OpenSearch does not run a search
backend, and ECS does not execute containers. A successful configuration API call
does not mean the underlying AWS service behavior is available.

**These services require a separate download.** They are not included in the
server binary or the `roto-aws` installation. Download the
`roto-lua-services-<tag>.zip` asset from the matching
[GitHub release](https://github.com/vivainio/roto/releases), then load it explicitly:

```sh
roto-server --lua-services ./roto-lua-services-vX.Y.Z.zip
```

Use the actual downloaded filename in place of `vX.Y.Z`; no extraction or
separate Lua runtime is needed. See [Getting started](getting-started.md#add-optional-lua-services)
and the [server reference](server.md#optional-lua-service-bundle) for loading options.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| CUR | Lua report definition CRUD, tags, pagination; account-scoped persistence | Billing report generation |
| ACM | Lua certificate metadata, synthetic requests, PEM imports/retrieval, tags and options | X.509 parsing/issuance, export, renewal/email delivery, advanced list filters |
| Budgets | Lua budgets, notifications/subscribers, tags, pagination and time periods | Billing engine, alert delivery, budget actions |
| Config | Lua rules, recorders, delivery channels, local start/stop and status; unevaluated compliance reads | Recording, delivery and evaluation engines |
| ECR | Lua repository CRUD, settings, repository/lifecycle-policy storage, tags and pagination | Image/layer APIs, registry V2, authorization, scanning/lifecycle engines and policy enforcement |
| ECS | Lua cluster configuration, task-definition revisions/deregistration, service configuration, tags and pagination | Container instances/tasks, deployment reconciliation, networking and logs; running/pending counts remain zero |
| Glue | Lua databases/tables, storage descriptors, version history/concurrency, partitions/batches/filtered reads, database tags | Crawlers/jobs/workflows, Lake Formation, federated catalogs, transactions, Iceberg commits and full expression language |
| OpenSearch | Lua domain configuration CRUD, revisions, multi-describe, engine-filtered listing and tags; REST/JSON | Configuration simulation only; no search backend/data endpoint, VPC provisioning, packages or upgrades |
| RDS | Lua instance/cluster configuration, membership, deletion protection, subnet/parameter groups, parameter overrides, tags and pagination; Query/XML | Configuration simulation only; no SQL backend/endpoint, snapshots, restore, DocumentDB or RDS Data API |
| Cloud Map | Lua HTTP namespaces, services/instances, discovery, custom health, revisions, attributes, operations and tags | DNS namespaces/Route 53, asynchronous workers, cross-account discovery, idempotency replay |
| Organizations | Lua organization, root and OU hierarchy, account reads and tags | Account creation/moving, handshakes, policies, delegated administrators and IAM side effects |

## S3 details

S3 supports path-style and virtual-host addressing, range and conditional reads,
copy operations, delete markers, multipart uploads and `aws-chunked` uploads.
Listings use SQLite metadata. Current object bodies are visible in the
[storage directory](storage.md). Lua setup can also register host files as
objects without copying them (`roto.s3.link`; see [Lua API](lua-api.md#s3-helpers)).

Many bucket sub-resources are stored and returned without executing their
behavior. For example, storing lifecycle or CORS settings does not implement
lifecycle execution or CORS serving. Website settings apply to the browser view. S3 Lambda and EventBridge notifications execute supported object events.
ACLs and policies are stored without enforcement. See [S3](s3.md) for API access,
browser rendering, directory listings, website configuration, and deployment examples.

## Cross-service behavior

SNS can deliver to SQS in-process, including raw delivery and filters on message
attributes or bodies. STS and IAM share account credential information.
The [CloudFormation subset](cloudformation.md) creates and updates SQS queues, SNS
topics, S3 buckets, DynamoDB tables, Kinesis streams, IAM roles, and inline IAM
policies through those same handlers. It also creates and deletes Lambda functions;
Lambda configuration updates, EventBridge, and SSM resources remain unsupported.

Lua startup setup and execution contracts are covered in [Lua setup](lua.md),
[local Lambda execution](lambda.md), and [EventBridge](eventbridge.md).

Lua services require an explicit `--lua-services DIR_OR_ZIP` option (or
`ROTO_LUA_SERVICES`); they are not embedded in the binary. Lua service scope,
optional release bundle packaging and per-service CI commands are documented in
[the service bundle README](https://github.com/vivainio/roto/blob/main/services/README.md).
