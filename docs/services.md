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
messaging, and event-driven processing. The optional Luau bundle is mainly useful
when your local environment also needs service administration or configuration
APIs.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| STS | Caller identity, sessions, role assumption | Authorization-message decoding |
| SQS | Queues, messages, batches, FIFO, deduplication, long polling, DLQ redrive, tags | Message-move tasks, dead-letter source listing |
| S3 | Buckets, objects, listing, versioning, multipart, tagging, browser rendering and directory listings, stored ACLs and bucket configuration | Object lock, restore, select, checksum details, SNS/SQS notification delivery, policy enforcement |
| IAM | Users, roles, access keys, tags, account aliases, customer-managed policies and versions, user/role/group attachments and entity listings, groups and membership, inline role/group policies, instance profiles and role relationships; CloudFormation roles and `AWS::IAM::Policy` | AWS-managed policy catalog, full policy-document validation, user inline policies, providers; policy evaluation is not performed |
| DynamoDB | Tables, items, expressions, query/scan, GSI/LSI, batch, transactions, tags, TTL, backups | PartiQL, import, streams |
| SSM | Parameter Store, versions, labels, history, hierarchy, tags, SecureString; Run Command records (`SendCommand`, invocations, cancel) that a fixture settles | Documents, maintenance windows, patch baselines, public parameters |
| Secrets Manager | Secrets, versions, staging labels, deletion/restore, tags, policies, rotation bookkeeping, batch get | Cross-region replication, Lambda rotation execution |
| SNS | Topics, subscriptions, publishing, SQS fan-out, filter policies, FIFO checks, tags | Platform endpoints, SMS attributes, HTTP/Lambda/email delivery |
| Lambda | Function metadata, local command/HTTP execution, S3 notifications, standard/FIFO SQS mappings, Luau startup setup | Packaged runtimes, versions/aliases, Kinesis polling |
| CloudWatch Logs | Log group CRUD, retention and tags, basic event filtering, local Lambda output | Log streams API, direct `PutLogEvents`, Insights, subscriptions, advanced filter patterns, retention enforcement |
| EventBridge | Default/custom buses, pattern rules, Lambda/SQS targets, S3 events, persistent delivery | Rule schedules, input transformers, archives/replays, cross-account targets, permissions |
| [EventBridge Scheduler](scheduler.md) | Schedule groups and tags, schedules with `rate`, `cron` and `at` expressions, time zones, start/end dates, SQS/Lambda targets, persistent firing without backfill | Flexible-window jitter, other target types, retry policy and dead-letter handling |
| IoT Data Plane | `Publish` to local JSON or MQTT 3.1.1 WebSocket subscribers; QoS 0 delivery, `+` and `#` filters, subscribe/unsubscribe, keepalive, client MQTT publish fan-out | MQTT 5, QoS 2, authentication, retained messages, durable delivery, IoT Rules, shadows |
| API Gateway v2 WebSocket APIs and Management API | In-memory WebSocket API, Lambda proxy integrations, routes and route responses, stages/deployments, route selection, `PostToConnection` | HTTP APIs, custom domains, authorizers, non-Lambda integrations, `GetConnection`, `DeleteConnection`, IAM/permission enforcement |
| Kinesis | Streams, shards, records, polling, expiring iterators, resharding, retention, tags, consumer registration, encryption/monitoring metadata | Enhanced fan-out streaming, Lambda polling, throughput enforcement, resource policies, actual encryption |
| KMS | Persisted symmetric keys and aliases, listing, tags, key policies, rotation status, deletion scheduling, Encrypt/Decrypt, ReEncrypt, random data keys | Real encryption, asymmetric operations, grants, multi-region keys, policy enforcement |
| CloudFormation | Synchronous stacks for SQS/SNS/S3/DynamoDB/Kinesis/IAM/Lambda, inert records for any other resource type (opt-in), recursive nested stacks from S3 templates, cross-stack exports/imports, refs, outputs, tags, change sets, events, rollback attempts, retention, stack policies, termination protection, persistence | Nested change sets, imports of existing resources, transforms, rollback triggers, Lambda configuration updates, full policy semantics |
| [RDS Data API](rds-data.md) | Proxy to a local PostgreSQL chosen by `roto.hooks.rds_data_connection`: ExecuteStatement, BatchExecuteStatement, transactions, typed `:name` parameters, result metadata, `formatRecordsAs: JSON` | PostgreSQL only; array parameters/results, secret validation, `database` override and Aurora-specific behavior |


KMS uses a versioned base64 JSON envelope for simulated ciphertext; it provides no
cryptographic protection. See [KMS](kms.md).

## Optional Luau services: completeness coverage

Luau services are provided for completeness, with mostly simple resource CRUD,
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
separate Luau runtime is needed. See [Getting started](getting-started.md#add-optional-luau-services)
and the [server reference](server.md#optional-luau-service-bundle) for loading options.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| CUR | Luau report definition CRUD, tags, pagination; account-scoped persistence | Billing report generation |
| ACM | Luau certificate metadata, synthetic requests, PEM imports/retrieval, tags and options | X.509 parsing/issuance, export, renewal/email delivery, advanced list filters |
| Budgets | Luau budgets, notifications/subscribers, tags, pagination and time periods | Billing engine, alert delivery, budget actions |
| Config | Luau rules, recorders, delivery channels, local start/stop and status; unevaluated compliance reads | Recording, delivery and evaluation engines |
| ECR | Luau repository CRUD, settings, repository/lifecycle-policy storage, tags and pagination | Image/layer APIs, registry V2, authorization, scanning/lifecycle engines and policy enforcement |
| ECS | Luau cluster configuration, task-definition revisions/deregistration, service configuration, tags and pagination; `RunTask`, `DescribeTasks`, `ListTasks`, `StopTask` records with settable status and exit code | Containers, container instances, deployment reconciliation, networking and logs; running/pending counts remain zero |
| Glue | Luau databases/tables, storage descriptors, version history/concurrency, partitions/batches/filtered reads, database tags | Crawlers/jobs/workflows, Lake Formation, federated catalogs, transactions, Iceberg commits and full expression language |
| WAFv2 | Luau web ACLs, IP sets and rule groups with lock tokens, rule-document validation and cross-references, regional web ACL associations, tags and pagination; `CLOUDFRONT` scope via us-east-1 | Request inspection or enforcement, capacity accounting, managed rule groups, logging, permission policies, API keys, regex sets, resource-existence checks |
| OpenSearch | Luau domain configuration CRUD, revisions, multi-describe, engine-filtered listing and tags; REST/JSON | Configuration simulation only; no search backend/data endpoint, VPC provisioning, packages or upgrades |
| [Elastic Load Balancing v2](elbv2.md) | Luau application load balancers, target groups, listeners, rules (path, host, header, method, query string, source IP), target registration, attributes and tags; Query/XML. With `--alb-port`, a forwarding proxy and health checker for `ip`/`instance` targets | Network and gateway load balancers (configuration only), TLS termination, Lambda and ALB targets, authentication actions, sticky sessions, trust stores, access logs |
| Classic Elastic Load Balancing | Luau load balancers, listeners as stored, instance registration, settable instance health, tags; Query/XML sharing the `elasticloadbalancing` signing name with v2 | Traffic, health checks, policies, attributes, availability-zone and subnet operations |
| EC2 | Luau instances (run, describe with filters and tags, start/stop/reboot/terminate with a state machine), security groups and tags; `ec2` protocol | Anything that is not instances, security groups and tags: no volumes, images, VPC resources, rules on groups, or execution |
| Auto Scaling | Luau groups, stored instance membership and health, detach/attach, process suspension, tags and scheduled actions | Launching or terminating instances, scaling policies, lifecycle hooks, warm pools, instance refresh |
| Step Functions | Luau state machines and executions: `ValidateStateMachineDefinition` diagnostics across independent states (including structural locations, `INVALID_RESOURCE`/`MISSING_TRANSITION_TARGET`/`MISSING_END_STATE` errors and `NO_DOLLAR`/`NO_PATH`/`PASS_RESULT_IS_STATIC` warnings), Pass, Task (Lambda and SDK integrations; Request Response, `.waitForTaskToken` and activities, with `SendTask*` and heartbeats), Parallel and Map (threads per branch/item, `$$.Map.Item.Index/Value/Source`, so Waits and callbacks work inside; `MaxConcurrency`/`MaxConcurrencyPath` with the 40-iteration inline cap), nested executions (`states:startExecution`, `.sync`, `.sync:2`), Choice, Wait, Succeed and Fail states are interpreted, with Retry/Catch on Tasks (`MaxDelaySeconds` and `JitterStrategy: FULL` supported), `GetExecutionHistory` (including Lambda Request Response events, `includeExecutionData=false`, reverse order and request-bound 24-hour pagination tokens), the 256 KiB execution/state/connector payload limit with `States.DataLimitExceeded`, `$$.Execution.RedriveCount`, `UpdateStateMachine`, tags on state machines and activities, `TestState` for each state type (inline Map/Parallel mock results and Distributed Map mocks for array input, exported results, item batching, failure thresholds, and supplied JSON/JSONL/CSV ItemReader data (Parquet and manifests are not supported), supplied context for mocks, DEBUG/TRACE data-flow inspection for Pass and Task states and inline Map/Parallel results, state selection within complete definitions, and Retry/Catch mock errors with named Map/Parallel error sources; Task mock results support STRICT/PRESENT shape, enum, length/range and supported regex pattern validation for vendored and bundle-provided service models, including ECS (regex uses Rust syntax and Unicode semantics; unsupported constructs and services without models require NONE); without mocks, branches run inline and callback/activity tasks cannot wait), Task `TimeoutSecondsPath`/`HeartbeatSecondsPath` on callback and activity Tasks, wall-clock `TimeoutSeconds`/`TimeoutSecondsPath` on Lambda and ordinary SDK Request Response Tasks, machine `TimeoutSeconds`, virtual-clock timers, published definition versions (explicitly or with `publish=true` on create/update) and aliases with weighted `StartExecution` routing, including nested starts | JSONata definitions/fields are rejected; Distributed Map, `.sync` integrations other than `states:startExecution` and `ecs:runTask`, long-polling `GetActivityTask`, ECS and nested-execution Request Response `TimeoutSeconds`, Express sync execution, Express execution-history retrieval |
| RDS | Luau instance/cluster configuration, membership, deletion protection, subnet/parameter groups, parameter overrides, tags and pagination; Query/XML | Configuration simulation only; no SQL backend/endpoint, snapshots, restore or DocumentDB (see [RDS Data API](rds-data.md)) |
| Cloud Map | Luau HTTP namespaces, services/instances, discovery, custom health, revisions, attributes, operations and tags | DNS namespaces/Route 53, asynchronous workers, cross-account discovery, idempotency replay |
| Organizations | Luau organization, root and OU hierarchy, account reads and tags | Account creation/moving, handshakes, policies, delegated administrators and IAM side effects |

## S3 details

S3 supports path-style and virtual-host addressing, range and conditional reads,
copy operations, delete markers, multipart uploads and `aws-chunked` uploads.
Listings use SQLite metadata. Current object bodies are visible in the
[storage directory](storage.md). Luau setup can also register host files as
objects without copying them (`roto.s3.link`; see [Luau API](lua-api.md#s3)).

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

Luau startup setup and execution contracts are covered in [Luau scripting guide](lua.md),
[Lambda](lambda.md), and [EventBridge](eventbridge.md).

Luau services require an explicit `--lua-services DIR_OR_ZIP` option (or
`ROTO_LUA_SERVICES`); they are not embedded in the binary. Luau service scope,
optional release bundle packaging and per-service CI commands are documented in
[the service bundle README](https://github.com/vivainio/roto/blob/main/services/README.md).
