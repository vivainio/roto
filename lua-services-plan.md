# Luau services implementation plan

Working backlog, started 2026-10-10. This is the authoritative plan for growing
`roto-lua-services`. Check items off as implemented and verified; keep unfinished
slices and their dependencies visible. A listed service is a candidate, not a
claim of AWS compatibility or a commitment to implement every operation.

## How we work through this

1. Take the next ready slice in the queue below. Record it under **Current work**.
2. Inspect MiniStack code/tests where available and the installed botocore model.
   Choose an exact operation set, persistence model, scope and error contract.
3. Implement the Luau module and required routing entry. Add focused contract
   checks, documentation and per-service CI in the same change.
4. Run the checks in **Definition of done**. Record evidence and remaining gaps,
   then check off the slice and select the next ready item.
5. When a slice needs runtime support, implement its prerequisite as a separate
   change rather than hiding it in service code.

Priorities reflect usefulness and fit with Roto's current architecture; there
are no time estimates. Prefer persistent resource behavior and real integrations
where they are practical. Successful job/delivery responses must represent work
that actually occurred or an explicitly documented simulation contract.

## Current work

- Active implementation: initial ECR/ECS/Glue slices complete; verification and shipping.
- Selected priority: S39 OpenSearch → S82 RDS → **S01 ECR → S09 ECS → S02 Glue**.
- Completed initial slices: OpenSearch domain configuration/tags; RDS
  instances/clusters, subnet/parameter groups, parameter overrides and tags;
  ECR repository configuration; ECS resource configuration; Glue catalog/partitions.
- Follow-up: RDS group attachments/parent constraints and cluster parameter groups;
  real OpenSearch search/indexing and RDS SQL adapters remain R05/R09 work.
- Remaining ready queue: S03 WAFv2 → S04 CodeBuild →
  S05 OpenSearch Serverless (separate from managed OpenSearch) → S06 Transfer.
- Existing-service improvements E01–E06 can be selected when needed by a concrete
  application. Runtime work R01–R09 unlocks the later batches.

## Starting point and evidence

The bundle registers ACM, Budgets, Config, CUR, Organizations, Cloud Map,
managed OpenSearch (`es`), RDS, ECR, ECS and Glue. Luau handlers receive account/region/request context, run in a SQLite
transaction, and have document CRUD plus ID/UUID/hash helpers. AWS JSON 1.0/1.1
requests require `X-Amz-Target`. Each service owns its VM, mutex and database.

Model-backed REST/JSON scalar bindings and Query/XML codecs now support the
OpenSearch/RDS configuration slices. Binary responses, cross-service calls,
resource enumeration and durable background work remain unsupported. Normal Luau
libraries are available, but blocking native calls escape the instruction-hook
deadline and external effects cannot be rolled back with SQLite. Do not use
request-local subprocess/HTTP tricks as the architecture for delivery engines.

Sources:

- [Bundle interface and limits](services/README.md), [manifest](services/manifest.json),
  [loader](crates/roto-server/src/lua_service.rs), [shared helpers](services/common.luau).
- [HTTP/SDK checks](tests/smoke-lua-services.py),
  [packaging checks](tests/test_package_lua_services.py), [.github/workflows/ci.yml](.github/workflows/ci.yml).
- Local inspiration: `~/r/MiniStack/ministack/services/` and its `tests/` directory.
- Protocols/signing names below were inspected using botocore in `.venv-moto`.
  MiniStack source and SDK models are evidence of existing code and wire shapes,
  not real AWS validation. New candidates absent from MiniStack need their own
  behavioral evidence before implementation; their slices below are proposals.

## Definition of done for every service slice

- [ ] Exact exported operations and intentionally unsupported behavior documented.
- [ ] Tenant/region/global scope, identifiers, parent relationships, deletion and
      update semantics defined; durable state stored in SQLite, not module globals.
- [ ] Manifest includes the correct signing identity, target prefix and JSON
      version, or the appropriate new protocol descriptor.
- [ ] SDK request/response field names, arrays, nulls, timestamps, blobs, errors,
      pagination and partial batch failures checked where applicable.
- [ ] Directory and ZIP loading, persistence across restart, reset of loaded and
      unopened stores, isolation and rollback verified through HTTP.
- [ ] Relevant service-specific checks added; per-service CI entry added.
- [ ] Bundle docs and `docs/services.md` updated with implemented scope and gaps.
- [ ] MiniStack notices retained for adapted code and its license packaged.
- [ ] Plan updated with completed tasks, validation evidence and next slice.

Typical validation after registering a new JSON service:

```sh
cargo build -p roto-server --locked
python3 tests/smoke-lua-services.py --service <service>
.venv-moto/bin/python tests/smoke-lua-services.py --service <service> --sdk
python3 tests/test_package_lua_services.py
```

Extend the smoke runner before using a new service name. Runtime changes also
need `cargo test -p roto-server --locked` and any affected crate checks. Add
relevant Moto runs with distinct `ROTO_TRACE_FILE` paths when applicable; record
unsupported/known failures explicitly. If changing `examples/demo/setup.luau`,
run `python3 tests/smoke-demo.py` on its fresh disposable server.

## Runtime prerequisites

### R01 — REST/JSON protocol support

- [x] Add model-derived method/path dispatch for OpenSearch, including encoded
      identifiers and scalar URI/query/header bindings.
- [ ] Extend REST codecs to payload/list bindings and broader service contracts.
- [ ] Preserve modeled response status codes, headers, body field names and errors.
- [ ] Handle unsigned routing and ambiguity with existing native service routes.
- [ ] Version the manifest extension; retain compatibility with existing bundles.
- [ ] Check SDK round trips for at least two services with different route shapes.

Unlocks most of batch B, especially AppConfig, Scheduler and SESv2.

### R02 — Binary bodies, blobs and streaming boundaries

- [ ] Provide byte-safe blob helpers; current base64 decoding is UTF-8 only.
- [ ] Support modeled binary request/response bodies, content types and headers.
- [ ] Set payload limits and define which streaming/event-stream operations are
      supported; avoid buffering unbounded streams.
- [ ] Verify non-UTF-8 round trips, binary errors and bundle reload behavior.

Needed by AppConfigData, registry layers, artifacts and model runtimes.

### R03 — Service identities, aliases and shared storage

- [ ] Separate SDK endpoint identity, SigV4 signing name and target/path dispatch.
- [ ] Define shared storage/VM ownership for cooperating service endpoints.
- [ ] Resolve target-prefix collisions without weakening conflict validation.
- [ ] Verify signed and unsigned routing, reset and persistence for aliases.

Examples: OpenSearch Serverless signs as `aoss`; Cloud Control as
`cloudcontrolapi`; DynamoDB Streams shares DynamoDB signing scope; AppConfig and
AppConfigData need shared configuration state. Timestream Write and Query share
signing name and target prefix, so the current prefix-only descriptor cannot
register them as independent modules unchanged.

### R04 — Calls between services

- [ ] Expose supported Rust/Luau service calls with explicit caller account/region.
- [ ] Define lock ordering, recursion limits and errors for reentrant calls.
- [ ] Define transaction boundaries; do not imply cross-service atomic rollback.
- [ ] Preserve traces and prevent credentials leaking into persisted job payloads.
- [ ] Verify real S3/Lambda/SQS interactions and downstream failures.

Needed by Firehose, Step Functions, Cloud Control adapters and resource writers.

### R05 — Durable workers and timers

- [ ] Persist jobs/outbox records, attempts, next-run times and ownership leases.
- [ ] Implement cancellation, retries/backoff, deadlines and idempotent effects.
- [ ] Define crash recovery, restart behavior and reset/shutdown coordination.
- [ ] Execute outside the request transaction; bound concurrency and queue sizes.
- [ ] Verify restart mid-job, repeated delivery, downstream failure and cancellation.

Needed by Scheduler, Pipes, builds, workflows, migrations and job services.

### R06 — Resource and tag registry

- [ ] Enumerate actual Rust/Luau resources through scoped adapters.
- [ ] Route tag writes to the owning service; report partial errors per ARN.
- [ ] Define deleted/previously tagged resource visibility and pagination.
- [ ] Verify that direct service tagging and aggregate reads stay consistent.

Needed by Resource Groups Tagging API and query-based Resource Groups.

### R07 — Audit capture and change feeds

- [ ] Capture gateway calls with account/region, event IDs, times and resource IDs.
- [ ] Define redaction, retention and whether tracing and audit share infrastructure.
- [ ] Persist DynamoDB item changes with ordered sequence IDs and stream views.
- [ ] Verify rollback produces no committed change records and audit scope is correct.

Gateway audit and DynamoDB change feeds are separate subprojects; both need
producer-side work rather than a Luau API that invents events on reads.

### R08 — Auth, token and identity endpoints

- [ ] Provide signing/verification and persistent signing-key rotation where needed.
- [ ] Add JWKS/OIDC routes and modeled auth errors; define local credentials.
- [ ] Integrate identity credentials with STS and explicit local authorization scope.
- [ ] Verify expiry, issuer/audience, tenant isolation and restart behavior.

Needed by usable Cognito flows and identity-backed sessions.

### R09 — Other protocols and execution adapters

- [x] Initial RDS Query/XML codec: nested structures/lists, scalar inputs,
      modeled XML names/envelopes, timestamps and errors; HTTP/boto3 verified.
- [ ] Broader Query/EC2 codecs, maps and database/compute runtime adapters.

- [ ] Add query/EC2 decoding and XML responses before EC2/RDS/ELB families.
- [ ] Add REST/XML support before Route 53/CloudFront management APIs.
- [ ] Inspect CBOR SDK generations before CloudWatch/Compute Optimizer support.
- [ ] Define executor adapters for containers, SQL, Git, SSH/SFTP and inference
      only as selected slices need them; these are independent work items.

## Batch A — JSON services and bounded control planes

These can largely use today's loader. Resolve R03 identity details where noted.

### S01 — ECR (`ecr`) · repository slice complete · MiniStack: `ecr.py`, `test_ecr.py`

- [x] Repository create/describe/delete, settings, tags and pagination.
- [x] Repository-policy set/get/delete and lifecycle-policy put/get/delete.
- [x] Duplicate names, ARN/scope validation and policy-document round trips.
- [ ] Second slice: image manifests, digest/tag relationships, mutability,
      image list/get/delete and nonempty repository deletion.
- [ ] Later: R01/R02 registry V2 push/pull, layer storage and authorization;
      scanning/lifecycle execution require their own engines.

First slice has no Docker dependency. Register correct SDK scope and trace
unsupported image/layer APIs; repository metadata is not a working registry.

### S02 — Glue (`glue`) · MiniStack: `glue.py`, `test_glue.py`

- [x] Database/table CRUD and listing; preserve storage descriptors and parameters.
- [x] Table versions, version reads, update concurrency and batch-delete errors.
- [x] Partition CRUD/batches, unique partition keys and parent/deletion behavior.
- [x] Explicit comparison/AND partition-expression subset with numeric and quoted-string tests.
- [ ] Later: connections/security configurations/classifiers as separate slices;
      crawlers, ETL jobs and workflows need R04/R05 and execution adapters.

Catalog/account isolation and regional behavior need checks. Do not port the
entire MiniStack Spark/subprocess runner as part of catalog storage.

### S03 — WAFv2 (`wafv2`) · MiniStack: `waf.py`, `test_waf.py`

- [ ] Web ACL, IP set and rule-group CRUD/list; preserve nested rule documents.
- [ ] Lock-token validation/rotation and stale-token update/delete errors.
- [ ] Tags, local associations and resource-to-ACL lookup.
- [ ] REGIONAL versus CLOUDFRONT scope and endpoint-region rules.
- [ ] Later: R04 resource-existence checks; managed rules, capacity and enforcement
      only with implemented behavior.

### S04 — CodeBuild (`codebuild`) · MiniStack: `codebuild.py`, `test_codebuild.py`

- [ ] Project create/update/delete, batch get and sorted/paginated listing.
- [ ] Preserve source/environment/artifacts/tags/timeouts and missing-project IDs.
- [ ] Later: R04/R05 build execution, logs/artifacts, stop and history.
- [ ] Verify cancellation, timeout and restart before claiming executed builds.

MiniStack's default simulated build immediately reports `SUCCEEDED`. Keep that
separate from actual execution; do not imply that commands/tests ran.

### S05 — OpenSearch Serverless (`opensearchserverless`)

MiniStack: `opensearchserverless.py`, `test_opensearchserverless.py`.

- [ ] R03 registration using SigV4 `aoss` and JSON 1.0 target `OpenSearchServerless`.
- [ ] Collection create/update/delete, batch get and listing.
- [ ] Security/access policy CRUD, policy versions, tags and name/type constraints.
- [ ] Later: VPC endpoint configuration; real indexing/search needs a data-plane
      adapter, lifecycle handling and reachable endpoints.

Metadata endpoints must not advertise a functional search backend by accident.

### S06 — Transfer Family (`transfer`) · MiniStack: `transfer.py`, `test_transfer.py`

- [ ] Server CRUD/list and configuration state.
- [ ] Users, home-directory mappings, roles, SSH public keys and tags.
- [ ] Parent constraints, duplicate keys and tenant/region isolation.
- [ ] Later: R04/R09 real SFTP listener, persistent host keys and S3-backed I/O.

Online/offline metadata does not establish that an SFTP endpoint is listening.

### S07 — CloudTrail (`cloudtrail`) · MiniStack: `cloudtrail.py`, `test_cloudtrail.py`

- [ ] Trail CRUD/list, event selectors, tags and logging configuration.
- [ ] Home-region/global trail visibility and ARN mutation rules.
- [ ] R07 gateway capture and `LookupEvents` with filters, time bounds and pages.
- [ ] Later: R04/R05 S3 delivery, retention and failure reporting.

A trail's enabled configuration alone must not claim delivered audit data.

### S08 — Cognito Identity (`cognito-identity`)

MiniStack: `cognito.py`, selected `test_cognito.py` cases.

- [ ] Identity-pool CRUD/list, tags, role mappings and provider configuration.
- [ ] Identity lookup/merge/delete with deliberate provider-login semantics.
- [ ] R04/R08 credentials through STS and account-scoped auth checks.
- [ ] Keep User Pools and token issuance separate from this pool metadata slice.

### S09 — ECS (`ecs`) · MiniStack: `ecs.py`, `test_ecs.py`

- [x] Cluster CRUD/list, settings and tags.
- [x] Task-definition registration/revisions, describe/list/deregister.
- [x] Service configuration: records desired state only; no tasks/deployment reconciliation.
- [ ] R04/R05/R09 task execution, networking, logs and stop/restart semantics.

### S10 — EMR (`emr`) · MiniStack: `emr.py`, `test_emr.py`

- [ ] Cluster/job-flow configuration, lists, tags and termination protection.
- [ ] Instance-group/fleet metadata and bootstrap-action reads.
- [ ] Persist submitted steps with explicit state transitions and cancellation.
- [ ] Later: R04/R05/R09 Spark/Hadoop execution; do not report executed steps
      merely because configuration was accepted.

### S11 — Cloud Control (`cloudcontrol`)

MiniStack: `cloudcontrol.py`, `test_cloudcontrol.py`; R03 signing `cloudcontrolapi`.

- [ ] Select supported type adapters for actual Roto resources through R04.
- [ ] Create/get/list/update/delete, JSON-string properties and patch semantics.
- [ ] Persist progress events/request IDs; get/list/cancel request behavior.
- [ ] Verify created resources appear in their owning service and deletion is real.

MiniStack's generic document store is useful inspiration for shapes but does
not supply resource provisioning. Unknown types should remain unsupported.

### S12 — Service Quotas (`service-quotas`) · model-first candidate

- [ ] Versioned local quota definitions and service/default/applied-quota reads.
- [ ] Increase-request create/list/get with explicit local status transitions.
- [ ] Templates/tags only if needed; test units, context and pagination.
- [ ] Document which quotas are enforced by Roto versus informational metadata.

### S13 — Identity Store (`identitystore`) · model-first candidate

- [ ] User/group CRUD and listing, external IDs and supported attribute updates.
- [ ] Membership CRUD/list/lookups and parent/duplicate constraints.
- [ ] Store-ID tenancy and filtered reads; reject unsupported filters explicitly.
- [ ] Verify deletion cleanup, persistent references and batch membership checks.

### S14 — IAM Identity Center admin (`sso-admin`) · model-first candidate

- [ ] R03 signing `sso`; instances and permission-set CRUD/list/tags.
- [ ] Inline/managed/customer-managed policy references and boundaries.
- [ ] Assignment records tied to S13 principals and E01 account membership.
- [ ] Provisioning status and reads with explicit local semantics; actual IAM
      effects/session credentials require R04/R08.

### S15 — Network Firewall (`network-firewall`) · model-first candidate

- [ ] Rule-group/firewall-policy CRUD, update tokens, tags and references.
- [ ] Firewall configuration and subnet/zone association metadata.
- [ ] Validate stateful/stateless rule-document shapes and deletion constraints.
- [ ] Keep packet inspection and network endpoint provisioning as R09 work.

### S16 — CodePipeline (`codepipeline`) · model-first candidate

- [ ] Pipeline create/get/update/delete/list, versions, stage/action configuration.
- [ ] Tags, stage-transition settings and webhook configuration.
- [ ] R04/R05 execution state, action jobs, approvals and history.
- [ ] Use S04 for build actions when execution exists; unsupported actions fail
      explicitly rather than producing successful pipeline runs.

### S17 — CodeDeploy (`codedeploy`) · model-first candidate

- [ ] Applications, deployment groups/configurations, revisions and tags.
- [ ] Resolve group/application relationships and revision identity/listing.
- [ ] R04/R05 deployment execution, stop, failure, rollback and target status.

### S18 — CodeCommit (`codecommit`) · model-first candidate

- [ ] Repository CRUD/list/batch reads, descriptions and tags.
- [ ] Select a consistent Git object/branch/commit model before file APIs.
- [ ] R02/R09 Git/file/blob integration and reachable transport endpoints.
- [ ] Verify actual content and branch references; metadata alone is not Git hosting.

### S19 — App Runner (`apprunner`) · model-first candidate

- [ ] Service configuration, tags and versioned autoscaling configuration.
- [ ] VPC connectors, observability configuration and relationship constraints.
- [ ] Operation history with explicit local lifecycle semantics.
- [ ] R04/R05/R09 deployment, health checks and real application endpoints.

### S20 — Service Catalog (`servicecatalog`) · model-first candidate

- [ ] Portfolios/products/provisioning artifacts and associations.
- [ ] Constraints, principals, tags and artifact-version relationships.
- [ ] R04/R05 provisioning through CloudFormation with real resource results.
- [ ] Distinguish catalog metadata from a provisioned product.

## Batch B — REST/JSON services unlocked by R01

### S21 — AppConfig + AppConfigData

MiniStack: `appconfig.py`, `test_appconfig.py`; requires R01/R02/R03.

- [ ] Applications/environments/profiles and hosted version CRUD/list/tags.
- [ ] Validators, strategies, deployments and version selection.
- [ ] Shared configuration state across control/data endpoints.
- [ ] Session tokens, polling intervals and binary configuration responses.
- [ ] R05 gradual deployments/rollback when selected; begin with a documented
      bounded deployment strategy.

### S22 — EventBridge Scheduler (`scheduler`)

MiniStack: `scheduler.py`, `test_scheduler.py`; requires R01 for metadata.

- [ ] Schedule/group CRUD/list/tags and expression/target validation.
- [ ] R04/R05 one-shot delivery to Lambda/SQS, disabled state and persistence.
- [ ] Recurring rate/cron expressions, time zones, windows, retries and DLQs.
- [ ] Verify missed-run/restart policy and avoid duplicate deliveries.

### S23 — EventBridge Pipes (`pipes`)

MiniStack: `pipes.py`, `test_pipes.py`; requires R01/R04/R05.

- [ ] Pipe configuration CRUD/list/tags and desired/current status.
- [ ] Start with one real source/target pair, such as SQS → Lambda.
- [ ] Filters, transforms, batching, checkpoints, retries and stop/reset semantics.
- [ ] Add S29 DynamoDB Streams sources after change-feed support exists.

MiniStack's initial runtime uses DynamoDB Streams → SNS/Step Functions; choose
Roto's first integration based on already functional sources and targets.

### S24 — SESv2 (`sesv2`) · MiniStack: `ses_v2.py`, `test_ses_v2.py`

- [ ] Email identities, configuration sets, templates, tags and local verification.
- [ ] SendEmail/SendBulkEmail with actual persisted outbox content and per-entry errors.
- [ ] R02 raw/MIME payloads; R04/R05 optional SMTP or event delivery.
- [ ] Explicit local recipient/identity policy; no claim of internet email delivery.

### S25 — Resource Groups (`resource-groups`)

MiniStack: `resource_groups.py`, `test_resource_groups.py`; requires R01/R06.

- [ ] Group CRUD, configuration, queries and tags.
- [ ] Explicit memberships, grouping status and deletion constraints.
- [ ] Query-backed membership/search against actual resources through R06.

### S26 — Account (`account`) · MiniStack: `account.py`, `test_account.py`

- [ ] Account/contact information reads and supported updates with ownership checks.
- [ ] Local region catalog, opt status and enable/disable transitions.
- [ ] E01 membership for delegated account operations; document bootstrap defaults.

### S27 — EKS (`eks`) · MiniStack: `eks.py`, `test_eks.py`

- [ ] Cluster/node-group configuration, tags, versions and update records.
- [ ] Access entries/policies and parent/version constraints as selected slices.
- [ ] R09 Kubernetes runtime only if needed; do not advertise a usable API server
      from metadata-only cluster creation.

### S28 — MSK (`kafka`) · MiniStack: `msk.py`, `test_msk.py`

- [ ] Cluster/configuration CRUD/list/revisions and tags.
- [ ] Broker/security/SCRAM configuration and coherent version updates.
- [ ] R09 Kafka runtime and bootstrap endpoints; metadata must not imply a listener.

### S29 — DynamoDB Streams (`dynamodbstreams`)

MiniStack: `dynamodb_streams.py`, `test_dynamodb_streams.py`; JSON, requires R03/R07.

- [ ] Capture changes in Rust DynamoDB transactions with configured stream views.
- [ ] List/describe streams, shard iterators, ordered get-records and expiry.
- [ ] Stream enable/disable lifecycle, retention and account/region isolation.
- [ ] R04/R05 consumers: Pipes and Lambda; verify checkpoints and retries.

This is JSON-compatible but cannot be implemented correctly as independent Luau
CRUD. Keep it here with integrations rather than in the immediately ready queue.

### S30 — Backup (`backup`) · MiniStack: `backup.py`, `test_backup.py`

- [ ] Vaults/plans/versions/selections and tags with relationship constraints.
- [ ] R04/R05 backup jobs backed by actual supported resource snapshots.
- [ ] Recovery points and restore jobs with verified restoration, retention and failures.

### S31 — Inspector2 (`inspector2`)

MiniStack: `inspector2.py`, `test_inspector2.py`.

- [ ] Account enablement/configuration and filter CRUD/tags.
- [ ] Explicitly seeded findings with filtering, aggregation and pagination.
- [ ] Later scanner integration through R04/R05; synthetic findings must be labeled.

### S32 — S3 Tables (`s3tables`) · MiniStack: `s3tables.py`, `test_s3tables.py`

- [ ] Table buckets/namespaces/tables, policies, tags and parent constraints.
- [ ] Metadata locations/version tokens with R04 actual S3 metadata storage.
- [ ] Separate Iceberg REST catalog adapters and commit concurrency checks.
- [ ] Verify downstream catalog/table data reads before claiming Iceberg support.

### S33 — S3 Files (`s3files`) · MiniStack: `s3files.py`, `test_s3files.py`

- [ ] File systems, mount targets, access points, policies, tags and sync configuration.
- [ ] Validate parent references and endpoint/lifecycle metadata.
- [ ] R04/R05/R09 actual file access/synchronization as separate integrations.

### S34 — CloudFront KeyValueStore (`cloudfront-keyvaluestore`)

MiniStack: `cloudfront_keyvaluestore.py`; requires R01 and R03 shared ownership.

- [ ] Store describe/key get/list/put/delete and atomic multi-key updates.
- [ ] ETag preconditions, key/value bounds and correct errors/headers.
- [ ] Store creation/ownership through CloudFront or an explicit fixture contract.
- [ ] Runtime edge-function integration is separate; this is a useful real KV slice.

### S35 — Signer (`signer`) · MiniStack: `signer.py`, `test_signer.py`

- [ ] Signing profiles/versions, permissions and cancellation.
- [ ] R04/R05 signing jobs and S3 artifact references with history/status.
- [ ] Define actual signing or a labeled simulation; verify output artifacts.

### S36 — AWS Batch (`batch`) · MiniStack: `batch.py`, `test_batch.py`

- [ ] Compute environments, queues, job-definition revisions and tags.
- [ ] Dependency/parent validation and scheduling configuration.
- [ ] R04/R05/R09 submit/execute/cancel/terminate jobs and logs.
- [ ] Keep MiniStack's immediate-success simulation separate from actual execution.

### S37 — Amazon MQ (`mq`) · MiniStack: `mq.py`, `test_mq.py`

- [ ] Broker/configuration/revision/user CRUD, tags and engine-specific validation.
- [ ] R05/R09 RabbitMQ/ActiveMQ adapter for reachable broker endpoints.
- [ ] Verify lifecycle recovery and auth before claiming messaging support.

### S38 — MWAA (`mwaa`) · MiniStack: `mwaa.py`, `test_mwaa.py`

- [ ] Environment CRUD/list/tags, source paths and version configuration.
- [ ] R04/R05/R09 Airflow provisioning and S3 DAG synchronization.
- [ ] Token/login APIs only against a real or explicitly simulated session backend.

### S39 — OpenSearch (`opensearch`) · MiniStack: `opensearch.py`, `test_opensearch.py`

- [x] Domain configuration create/read/update/delete, tags and stored policy documents.
- [x] REST/JSON model routes with `es` signing identity, persistence, scope,
      rollback, ZIP packaging, HTTP/boto3 checks and per-service CI.
- [ ] VPC/security provisioning, packages, upgrades and full option validation.
- [ ] R05/R09 domain runtime/proxy and real search/index APIs.
- [ ] Keep domain metadata and S05 serverless collections as distinct surfaces.

### S40 — Aurora DSQL (`dsql`) · MiniStack: `dsql.py`, `test_dsql.py`

- [ ] Cluster configuration, policies, tags and deletion protection.
- [ ] R09 SQL backend/proxy and local authentication.
- [ ] Explicit SQL subset/transaction semantics and R07 change capture if selected.

### S41 — EFS (`efs`) · MiniStack: `efs.py`, `test_efs.py`

- [ ] File systems, mount targets, access points, policies and tags.
- [ ] Regional references and mount-target deletion constraints.
- [ ] R09 reachable filesystem protocol/backend as separate work.

### S42 — IoT control/jobs/wireless

MiniStack: `iot.py`, `iot_jobs_data.py`, `iotwireless.py` and matching tests.

- [ ] Thing/type/group registry, policies and principal attachments.
- [ ] R02/R08 certificate/key handling and auth semantics.
- [ ] R04 integration with existing Roto IoT publishing and rules destinations.
- [ ] Jobs/executions and device update flows through R05.
- [ ] Wireless device/destination metadata as a separate bounded module.
- [ ] Shadows/MQTT listeners are additional protocol/state projects, not registry CRUD.

### S43 — Amazon Location (`location`) · MiniStack: `location.py`, `test_location.py`

- [ ] R03 signing `geo`; trackers, tags and scoped device identities.
- [ ] Position writes/current reads/history, timestamp handling and pagination.
- [ ] Optional geofence evaluation and EventBridge events via R04/R05.
- [ ] Maps/search/routing require an explicit backend and separate scope.

### S44 — MediaConnect (`mediaconnect`)

MiniStack: `mediaconnect.py`, `test_mediaconnect.py`.

- [ ] Flow/source/output configuration and tags with parent constraints.
- [ ] Supported lifecycle mutations and persisted status.
- [ ] Media transport/inspection requires R09; avoid invented source thumbnails.

### S45 — Bedrock control plane and agents

MiniStack: `bedrock.py`, `bedrock_agent.py`, `bedrock_agentcore.py` and tests.

- [ ] Select useful subsets: guardrails/versions, inference profiles, prompts,
      agent definitions/aliases/action groups and tags.
- [ ] Separate knowledge-base/data-source configuration from actual ingestion.
- [ ] R04/R05 ingestion/agent tool execution and failure recovery.
- [ ] R02/R09 InvokeModel/Converse/event streams with an explicit inference backend
      or a labeled deterministic fixture contract.
- [ ] AgentCore runtime/session resources only after lifecycle/auth needs are defined.

No need to mirror MiniStack's whole model catalog or return fake trained models.

## Batch C — Data movement, workflow and query integrations

### S46 — Firehose (`firehose`) · MiniStack: `firehose.py`, `test_firehose.py`

JSON; requires R02/R04 for a useful DirectPut slice, R05 for buffering.

- [ ] Delivery-stream configuration, destination versions and tags.
- [ ] Real PutRecord/PutRecordBatch → S3 delivery with byte-safe payloads.
- [ ] Per-record failures, retries, buffering, prefix handling and restart recovery.
- [ ] Later Lambda transforms, Kinesis sources and catalog/table destinations.

### S47 — Step Functions (`stepfunctions`)

MiniStack: `stepfunctions.py`, `test_stepfunctions.py`.

- [ ] State-machine CRUD, definition validation, tags and execution/history storage.
- [ ] Bounded ASL interpreter: Pass/Choice/Succeed/Fail and explicit unsupported states.
- [ ] R04 Lambda/SQS task integrations and modeled failures.
- [ ] R05 Wait/retry/catch, parallel/map, cancellation and restart.
- [ ] Activities/task tokens, versions/aliases and sync execution as later slices.

### S48 — Athena (`athena`) · MiniStack: `athena.py`, `test_athena.py`

- [ ] Workgroups/catalogs/named queries and tags.
- [ ] R04 Glue/S3 bindings and R09 SQL backend with a stated SQL subset.
- [ ] R05 query execution/cancel/status, results, errors and pagination.
- [ ] Verify output objects and query results rather than returning empty successes.

### S49 — Translate / Transcribe

MiniStack: `translate.py`, `transcribe.py` and corresponding tests.

- [ ] Select explicit deterministic fixture or real text/audio backend contracts.
- [ ] Translate terminology/settings and Transcribe vocabulary configuration.
- [ ] R02/R04 documents/media inputs and actual output artifacts.
- [ ] R05 jobs, cancel/status/history and event delivery with restart checks.

### S50 — Resource Groups Tagging API (`resourcegroupstaggingapi`)

MiniStack: `tagging.py`, `test_tagging.py`; JSON, requires R06.

- [ ] GetResources/GetTagKeys/GetTagValues from actual scoped resources.
- [ ] TagResources/UntagResources routed to owners with per-ARN failure maps.
- [ ] Filters, pagination and consistency after direct service mutations/deletion.
- [ ] Inspect signing identity through R03; do not build a disconnected tag database.

## Batch D — Additional model-first candidates

These broaden the backlog beyond MiniStack ports. Before promoting an entry,
identify a concrete consumer and inspect its SDK model and behavioral evidence.
Each row is an unchecked implementation slice; job/security recommendations must
not be represented by invented successful outcomes. Protocols reflect the local
installed models, not all historical SDK versions.

| Done | ID | Candidate | Protocol / prerequisite | Proposed work |
| --- | --- | --- | --- | --- |
| [ ] | S51 | DMS (`dms`) | json; today’s JSON loader | Endpoints, replication instances/configurations, tasks and tags. Define parent/state transitions; real migration needs R04/R05/R09 database adapters. |
| [ ] | S52 | DataSync (`datasync`) | json; today’s JSON loader | Agents, locations, task options and tags. Transfer execution needs R04/R05/R09 source/destination adapters and verified bytes. |
| [ ] | S53 | Directory Service (`ds`) | json; today’s JSON loader | Directories, aliases, trusts and tags. Domain/LDAP provisioning and auth require R08/R09; metadata endpoints must be labeled. |
| [ ] | S54 | SSM Contacts (`ssm-contacts`) | json; today’s JSON loader | Contacts/channels, plans and rotations. Paging/engagement needs R05 plus a real or labeled local notification backend. |
| [ ] | S55 | Firewall Manager (`fms`) | json; today’s JSON loader | Policies, app/protocol lists and resource sets. Compliance and enforcement require E01, R06 and S03/S15 adapters. |
| [ ] | S56 | Shield (`shield`) | json; today’s JSON loader | Protections, groups, subscription settings and tags. Seeded attack history only under a fixture contract; no DDoS protection claim. |
| [ ] | S57 | Support (`support`) | json; today’s JSON loader | Cases, communications and attachment storage. Define local service/severity catalog and explicit local resolution; binary attachments need R02. |
| [ ] | S58 | Health (`health`) | json; today’s JSON loader | Explicitly seeded events/entities, filter/detail/aggregate reads and pagination. Define fixture ingestion; do not invent live AWS incidents. |
| [ ] | S59 | Cost Explorer (`ce`) | json; today’s JSON loader | Explicit billing fixtures, time/group/filter query semantics and cost aggregation. CUR/Budgets integration requires a shared billing model; no arbitrary zero-cost successes. |
| [ ] | S60 | SageMaker (`sagemaker`) | json; today’s JSON loader | Model/endpoint-config metadata, tags and relationships. Training/processing/endpoint serving require R04/R05/R09 and SageMaker Runtime routing. |
| [ ] | S61 | Personalize (`personalize`) | json; today’s JSON loader | Dataset groups/schemas/datasets/solutions and tags. Imports/training/inference require R04/R05/R09 with observable results. |
| [ ] | S62 | AppStream (`appstream`) | json; today’s JSON loader | Stacks/fleets/applications and associations. Streaming sessions and actual desktops require R08/R09. |
| [ ] | S63 | WorkSpaces (`workspaces`) | json; today’s JSON loader | Directories/bundles/workspaces, tags and lifecycle records. Actual desktop provisioning and access require R08/R09. |
| [ ] | S64 | Redshift Data API (`redshift-data`) | json; today’s JSON loader | SQL execution/cancel/status/results through R09 backend and R05 jobs. Preserve typed result cells and batch/transaction behavior. |
| [ ] | S65 | Timestream Write/Query (`timestream-write`) | json; today’s JSON loader | R03 shared target/signing routing; databases/tables/retention, real writes and bounded queries. Query companion is timestream-query; agree storage/time/type semantics before ingestion. |
| [ ] | S66 | Lake Formation (`lakeformation`) | rest-json; R01 | Data-lake settings, resource registration, LF tags and permission records. Enforcement requires R04/R06 integration with Glue/S3. |
| [ ] | S67 | RAM (`ram`) | rest-json; R01 | Resource shares, principals/resources, invitations and permission versions. Real cross-account resource access needs E01/R04/R06. |
| [ ] | S68 | Access Analyzer (`accessanalyzer`) | rest-json; R01 | Analyzers, archive rules and fixture findings. Real policy analysis requires a defined evaluator; do not fabricate security conclusions. |
| [ ] | S69 | GuardDuty (`guardduty`) | rest-json; R01 | Detectors, members, filters and labeled fixture findings. Real detection requires audit/event inputs and an implemented detector. |
| [ ] | S70 | Macie (`macie2`) | rest-json; R01 | Session/configuration, classification-job records and fixture findings. Classification requires R04/R05/R09 with actual object inspection. |
| [ ] | S71 | Image Builder (`imagebuilder`) | rest-json; R01 | Recipes/components/images/pipelines and tags. Actual image creation needs R05/R09 build adapters and artifact verification. |
| [ ] | S72 | CodeArtifact (`codeartifact`) | rest-json; R01 | Domains/repositories/permissions and tags. Package publish/fetch and authorization need R02/R08/R09; protocols vary by package ecosystem. |
| [ ] | S73 | Amplify (`amplify`) | rest-json; R01 | Apps/branches/domain configuration and tags. Builds/deployment need R04/R05/R09 and actual hosting/artifacts. |
| [ ] | S74 | Synthetics (`synthetics`) | rest-json; R01 | Canary/group configuration, tags and version records. Real checks need R05/R09 runner and captured artifacts/results. |
| [ ] | S75 | X-Ray (`xray`) | rest-json; R01 | Persist trace segments and implement selected trace/service-graph reads. Define validation, retention, tenant scope and gateway/Lambda instrumentation. |
| [ ] | S76 | SSM Incidents (`ssm-incidents`) | rest-json; R01 | Response plans, incident records/timeline/relations. Integrate contacts through S54 and R04/R05; model local incident state changes. |
| [ ] | S77 | Rekognition / Textract / Comprehend (`rekognition`) | json; today’s JSON loader | Choose exact fixture-based inference APIs or real backends. Preserve typed outputs and byte-safe inputs; R02/R04/R05 for S3/job paths. Inspect all three models separately. |
| [ ] | S78 | Compute Optimizer (`compute-optimizer`) | smithy-rpc-v2-cbor; R09 | Seeded resource/recommendation fixture contract. Inspect CBOR support in R09 before routing; real recommendations need telemetry and an implemented model. |

## Batch E — Broader protocol and infrastructure families

These are large families, not immediate Luau ports. Split by resource family and
choose metadata or real runtime scope explicitly before starting.

| Done | ID | Candidate / MiniStack reference | Work and dependencies |
| --- | --- | --- | --- |
| [ ] | S79 | EC2 — `ec2.py` | R09 EC2/query protocol; VPC/subnet/route/security-group metadata first, then instances/images/volumes. Real networking/compute is separate; check Terraform-shaped read/update/delete flows. |
| [ ] | S80 | Auto Scaling — `autoscaling.py` | R09 query protocol; groups/launch configuration/policies/tags. R04/R05 EC2 reconciliation and metrics-driven scaling, with observable instance changes. |
| [ ] | S81 | ELB/ELBv2 — `alb.py` | R09 query protocol; load balancers/listeners/rules/target groups and tags. Separate proxy/health-check runtime with target routing and TLS behavior. |
| [ ] | S82 | RDS / DocumentDB — `rds.py`, `documentdb.py` | Initial RDS configuration slice implemented: Query/XML; instance/cluster CRUD and membership; subnet/parameter groups, overrides, tags, pagination and deletion protection. Remaining: group attachments, cluster parameter groups, snapshots/restore, DocumentDB, real SQL/backend/auth/lifecycle/backup verification. |
| [ ] | S83 | RDS Data API — `rds_data.py` | R01/R02 modeled SQL/parameter/result payloads; R04 Secrets Manager and R09 SQL backend. Transactions, errors, pagination and timeout behavior need real tests. |
| [ ] | S84 | ElastiCache — `elasticache.py` | R09 query protocol; caches/subnet/parameter/user groups and tags. Redis/Valkey backend, auth, lifecycle and reachable endpoints are separate runtime work. |
| [ ] | S85 | Route 53 — `route53.py` | R09 REST/XML; hosted zones, record sets, changes and tags with atomic batch validation. Real local DNS listener and Cloud Map DNS namespace adapters follow. |
| [ ] | S86 | CloudFront — `cloudfront.py` | R09 REST/XML; distributions/origins/cache policies/functions and tags, ETags and concurrency. R04/R09 actual proxy/cache/TLS and S34 KV integration later. |
| [ ] | S87 | CloudWatch — `cloudwatch.py` | R09 SDK protocol compatibility, including local model’s CBOR. Persist metrics/dimensions and meaningful aggregation; R04/R05 alarm evaluation and SNS actions later. |
| [ ] | S88 | SES classic — `ses.py` | R09 query protocol; identities/templates/configuration and R02 raw MIME. Share real outbox/backend state with S24 through R03 rather than duplicating email semantics. |
| [ ] | S89 | AppSync / Events — `appsync.py`, `appsync_events.py` | R01 API/schema/resolver configuration; R04 GraphQL DynamoDB/Lambda execution. Define supported GraphQL syntax; subscriptions/events require streaming connection lifecycle. |
| [ ] | S90 | Cognito User Pools — `cognito.py` | JSON metadata can start now: pools/clients/users/groups. R08 password/challenge/token/JWKS flows; tenant/client/provider validation and real auth checks before integration claims. |
| [ ] | S91 | Bedrock Agent Runtime / AgentCore runtime — `bedrock_agent_runtime.py`, `bedrock_agentcore.py` | R01/R02/R04/R05/R08/R09 streaming, session lifecycle, tools and inference backend. Keep control-plane configuration S45 separate from usable runtime calls. |
| [ ] | S92 | WAF classic/regional — `waf_v1.py` | JSON-compatible but lower priority than S03; change tokens, sets/rules/ACLs and associations. Select only when an existing application needs these older API surfaces. |

## Existing Luau service upgrades

### E01 — Organizations

MiniStack: `organizations.py`, `test_organizations.py`.

- [ ] Account creation/status and membership ownership model.
- [ ] MoveAccount and source/destination/root constraints.
- [ ] Policy CRUD, attachments and inherited policy listings.
- [ ] Enabled policy types, delegated admins and handshakes as later slices.
- [ ] Keep SCP storage distinct from actual IAM policy evaluation.

### E02 — ACM

- [ ] Implement useful list Includes/sort filters and certificate-state validation.
- [ ] R02 proper PEM/X.509/private-key parsing and safe key persistence if required.
- [ ] Renewal/export only with implemented crypto/issuance semantics.
- [ ] R04 consumer integrations with endpoint/TLS behavior as separate slices.

### E03 — Cloud Map

- [ ] Idempotency-token replay, revisions and more precise discovery filters.
- [ ] R04/S85 DNS namespaces and records with cleanup/rollback behavior.
- [ ] R05 asynchronous operation progression if needed; preserve existing HTTP APIs.

### E04 — Config

- [ ] R06 resource inventory and R07 change recording.
- [ ] R04 delivery channels writing real snapshots/history to S3.
- [ ] Selected rule evaluators with actual compliance results and evaluation details.
- [ ] R05 evaluation scheduling and restart/reset behavior.

### E05 — Budgets

- [ ] Shared explicit billing/usage fixtures with S59 Cost Explorer and E06 CUR.
- [ ] Budget calculations and thresholds against those inputs.
- [ ] R04/R05 real notification delivery, then selected budget actions.

### E06 — CUR

- [ ] Report generation from shared billing fixtures with defined schema/periods.
- [ ] R04 real S3 output, versioning/overwrite semantics and report manifest.
- [ ] R05 scheduled delivery, retries and idempotency.

## Separate integration backlog

- [ ] I01: CloudFormation adapters for selected new resource types; registering a
      Luau service alone does not add stack provisioning support.
- [ ] I02: inspection UI support for Luau document namespaces and resource summaries.
- [ ] I03: expand the disposable demo seed with selected new service fixtures and
      validate its contract with `tests/smoke-demo.py`.
- [ ] I04: run selected MiniStack HTTP tests against Roto; adapt direct-Python-state
      tests instead of treating them as portable compatibility evidence.
- [ ] I05: extend API operation coverage reports for new services and save separate
      traces per run, including explicitly selected expected failures.
- [ ] I06: common helper improvements only when two slices need them: ARN parsing,
      service-specific pagination/sorting, timestamp/blob handling and validation.

## Decision rules and dependencies

- User-selected completed slices: S39 OpenSearch → S82 RDS → S01 ECR → S09 ECS → S02 Glue.
  Remaining initial ready queue: S03 → S04 → S05 → S06. S05 needs the limited
  R03 signing-alias slice first; verify the existing router rather than assuming
  endpoint/signing names match.
- High-value runtime track: R01/R02/R03 → S21 AppConfig; R04/R05 → S46 Firehose
  and S22 Scheduler; R07 → S29 Streams → S23 Pipes.
- Resource discovery track: R06 → S50 Tagging → S25 Resource Groups → E04 Config.
- Identity track: E01 + S13 → S14; R08 → S08/S90 usable identity flows.
- Data catalog track: S02 → S48 SQL integration or S32 Iceberg catalog integration.
- Security metadata track: S03/S15 → S55 Firewall Manager; policy records alone
  do not imply enforcement.
- Distinct endpoints with shared ownership need R03. Do not weaken tenant scope
  or manufacture a second resource store just to make routing pass.
- Avoid completing a job with fake success, returning empty filtered results
  without evaluating the filter, or advertising a data endpoint that cannot
  perform the promised operation. Unsupported behavior stays explicit.

## Progress log

| Date | Slice | Outcome | Validation / remaining work |
| --- | --- | --- | --- |
| 2026-10-10 | Planning | Broad backlog created from local MiniStack source/tests and botocore models | Initial backlog snapshot; S39/S82 subsequently selected below. |
| 2026-10-10 | S39 → S82 configuration | Added 10 OpenSearch and 21 RDS Luau operations, model-backed REST/JSON and Query/XML, signing-aware routing, trace operation names, packaging and per-service CI | HTTP/boto3 contracts for directory/ZIP, persistence/reset/isolation/rollback; 36 server/core tests and 2 packaging tests passed. Real search/SQL backends remain unsupported; R01/R09 are partial prerequisites. |
| 2026-10-10 | S01 → S09 → S02 | Added 14 ECR repository, 19 ECS configuration and 26 Glue catalog operations; shared JSON-policy decoder, directory/ZIP packaging and per-service CI | HTTP/boto3 checks cover every export, numeric revisions, table concurrency/archive semantics, partition filters/batches/cascades, rollback, scope, persistence/reset. 32 selected MiniStack HTTP tests passed (10 ECR, 9 ECS, 13 Glue), with separate request traces. Image registry, container execution and Glue jobs remain unsupported. |
