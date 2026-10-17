# Optional Luau service bundle

Luau service scripts are external files; none are embedded in the server binary.
Without a configured bundle the server registers only its Rust services. Activate
the bundle explicitly using a directory or a ZIP file:

```sh
roto-server --ephemeral --lua-services ./services
roto-server --ephemeral --lua-services ./roto-lua-services-vX.Y.Z.zip
# Equivalent environment setting:
ROTO_LUA_SERVICES=./roto-lua-services-vX.Y.Z.zip roto-server --ephemeral
```

The CLI flag takes precedence over the environment variable. There is no implicit
search of the working directory or installation directories. A configured missing
or invalid bundle fails startup. ZIP files are read directly, without extraction.
Only stored and deflated ZIP compression is supported; encrypted archives are not.

## Root manifest

A bundle has one `manifest.json` at its root. For example:

```json
{
  "version": 1,
  "common": "common.luau",
  "services": [
    {
      "service": "cur",
      "script": "cur/service.luau",
      "target_prefix": "AWSOrigamiServiceGatewayService.",
      "json_version": "1.1"
    }
  ]
}
```

`common` is optional and names a helper script executed before each service
module. The supplied bundle uses it for validation, pagination and tag helpers.
Manifests describe routing; they execute no code. Script files are read only on
the first request for their service. Successfully loaded modules remain loaded
until restart. Failed directory module initialization can be retried after
repairing the file. An open ZIP keeps its index for the server lifetime: replace
archives only while the server is stopped, then restart.

The loader limits each manifest/script to 1 MiB and rejects unsafe
ZIP entry names. These are trusted executable Luau modules with normal libraries.

## Packaging and GitHub releases

```sh
python3 scripts/package-lua-services.py --output dist/roto-lua-services.zip
roto-server --ephemeral --lua-services dist/roto-lua-services.zip
```

The packaging script includes the root manifest, its referenced Luau files and
license/documentation files. It produces a reproducible ZIP with `manifest.json`
as the first local entry and first central-directory entry, followed by the
common helper and service scripts. No parent directory wraps the bundle. The
loader finds the manifest by name, so compatible ZIPs from other tools also work.

The tagged release workflow attaches `roto-lua-services-<tag>.zip` to the matching
GitHub release. This asset is separate from the Python wheels/server binary and
is downloaded only when Luau services are wanted.

## Lazy dispatch

At startup the server reads manifests and registers lightweight handler descriptors. Signed
requests select it using the SigV4 service name `organizations`. Unsigned requests
select it using the `AWSOrganizationsV20161128.` prefix of `X-Amz-Target`.
Neither lookup opens a database nor executes Luau. The first valid request loads
the module, opens the service database (for example, `organizations.db`), and applies the generic document migration.
Initialization is serialized and subsequent calls reuse that service's VM.
Resetting an unused service does not initialize it. Existing persisted databases
are cleared on reset without loading their Luau modules.

Each loaded service owns a VM and mutex. Each operation runs within one SQLite
transaction, committed on success and rolled back on AWS errors, Luau failures, or
timeouts. Runtime Luau code has a five-second instruction-hook deadline. These are
trusted modules with normal Luau libraries; the deadline does not interrupt
blocking native calls. The scripts should keep durable state in SQLite and avoid
mutable module globals, which SQLite rollback cannot undo.

## User-defined services

Add an entry to the root `manifest.json` and create its `script` file. Service
names use lowercase letters, digits and hyphens. Target prefixes end with a dot
and cannot overlap. Script paths are relative to the bundle root, use forward
slashes and cannot contain `..`, absolute paths or drive prefixes. Unknown
manifest fields, unsupported manifest/JSON versions and conflicting service names
fail at startup. Unlisted files do not register services.

The working CRUD example in `examples/lua-services` has one root manifest and a
`widgets/service.luau` script:

```sh
cargo run -p roto-server --locked -- --ephemeral --lua-services examples/lua-services
curl http://localhost:5070/ -H 'Content-Type: application/x-amz-json-1.1' \
  -H 'X-Amz-Target: Widgets_20261010.CreateWidget' \
  -d '{"Id":"hello","Value":{"enabled":true}}'
```

Use `Widgets_20261010.GetWidget` with `{"Id":"hello"}` to read it. For signed
requests use `widgets` as the SigV4 service name. Custom services do not install
SDK service models; callers can use HTTP or supply their own model.

## Module interface

A module returns a table mapping operation names to functions:

```luau
return {
    DescribeThing = function(ctx, input, db)
        local thing = db.get(ctx.account_id .. "/things", input.Id)
        if not thing then roto.error("ResourceNotFoundException", "Unknown thing") end
        return {Thing = thing}
    end,
}
```

`ctx` includes `account_id`, `region`, and `request_id`. Rust decodes/encodes AWS
JSON and records existing request traces. Missing operation handlers return
`NotImplemented` (501) and appear in unsupported-operation reports.

Storage functions use bound SQLite parameters and are valid only during the
request transaction:

- `db.get(namespace, id)` returns a JSON document or `nil`.
- `db.put(namespace, id, document)` inserts or replaces it.
- `db.delete(namespace, id)` removes it.
- `db.list(namespace)` returns documents ordered by ID.

Namespaces are chosen by the service, allowing regional, per-account, or shared
organization state. The SQLite table is `documents(namespace, id, body)`; the
inspection UI does not yet expose it.

`roto.error(code, message, status)` raises a modeled AWS client error and rolls back.
`status` defaults to 400. `roto.id(prefix)` generates a short identifier and
`roto.uuid()` generates a UUID. `roto.sha256(text)` returns a hexadecimal digest;
`roto.decode_base64(text)` decodes a base64 UTF-8 blob.
`roto.json_decode(text, error_code)` parses JSON policy documents into Luau values
and raises the specified AWS error code on malformed JSON. `roto.array()` constructs an explicitly
marked JSON array, including empty arrays. Plain empty tables encode as objects;
JSON null values use mlua's serde null representation. Successful operations
return JSON objects.

## Organizations scope

Implemented operations:

- `CreateOrganization`, `DescribeOrganization`, `DeleteOrganization`, `ListRoots`
- `CreateOrganizationalUnit`, `DescribeOrganizationalUnit`,
  `UpdateOrganizationalUnit`, `DeleteOrganizationalUnit`,
  `ListOrganizationalUnitsForParent`, `ListParents`
- `ListAccounts`, `DescribeAccount`, `ListAccountsForParent`
- `TagResource`, `UntagResource`, `ListTagsForResource`

Creation establishes the management account and root. OUs enforce existing
parents, unique sibling names, and deletion only when empty. List operations use
stable ID ordering and offset pagination. State is shared across regions, and
membership links callers to their organization; management operations check the
calling account.

This is a prototype, not full AWS Organizations parity. Account creation/moving,
handshakes, policies, delegated administrators, IAM side effects, and full AWS
limits/validation are not implemented. Management account names/emails and join
timestamps are placeholders. `ALL` advertises the default SCP policy type but
policy objects and policy enforcement are not implemented. Pagination tokens are
simple offsets and assume the collection is unchanged between calls.

Verification: `cargo test -p roto-server --locked` covers lazy loading, concurrent
initialization, transaction rollback, hierarchy errors, pagination, empty arrays,
region independence, persistence, separate bundles, signed/unsigned HTTP routing,
tracing, unsupported operations, and reset.

## Optional service bundle

These modules are adapted from MiniStack under its MIT license; see
[LICENSE-MiniStack](LICENSE-MiniStack). Their behavior is supported by existing
MiniStack code/tests and local botocore model inspection, not real AWS validation.

| Service | Operations | Implemented scope | Limits |
| --- | ---: | --- | --- |
| CUR (`cur`) | 7 | Report definition create/modify/delete/list and tags, account scoped | No billing reports generated; lists paginate in pages of five |
| ACM (`acm`) | 10 | Request/import/get/describe/list/delete certificates, tags and options, account + region scoped | Requested certificates are synthetic; imports store supplied PEM/chain without parsing X.509; private keys are validated as blobs but not persisted; renewal, export and email delivery unsupported |
| Budgets (`budgets`) | 16 | Budget CRUD, notifications/subscribers, tags, pagination and UTC period defaults, account scoped | AccountId must match caller; spend is zero without a billing engine; no alert delivery or budget actions |
| Config (`config`) | 14 | Rules, recorders, delivery channels, start/stop, status and compliance reads, account + region scoped | No recording/delivery/evaluation engine: compliance is INSUFFICIENT_DATA and evaluation results are empty; recorder status describes its local enabled state |
| ECR (`ecr`) | 14 | Repository CRUD, settings, policies, lifecycle-policy storage, tags and pagination; account + region scoped | No images/layers, authorization, registry V2, policy enforcement, scanning or lifecycle execution; AES256 configuration only |
| ECS (`ecs`) | 19 | Clusters, task-definition revisions/deregistration, service configuration, tags, filtering and pagination; account + region scoped | No tasks/container instances, deployment reconciliation or networking; running/pending counts remain zero |
| Glue (`glue`) | 26 | Database/table CRUD, table versions/concurrency, partition CRUD/batches/filtering, database tags and pagination; account/catalog + region scoped | No crawlers/jobs/workflows, Lake Formation, federated/resource links, transaction snapshots or Iceberg commits |
| OpenSearch (`es` signing scope) | 10 | Domain configuration CRUD, multi-describe, engine-filtered listing and tags; account + region scoped | Configuration simulation; no search backend, data endpoint, VPC provisioning, advanced security, packages or upgrades |
| RDS (`rds`) | 21 | Instances/clusters, subnet groups, parameter groups and values, tags, cluster membership, deletion protection and pagination; account + region scoped | Synchronous configuration simulation; no SQL server, endpoint, credential management, snapshots, restore, DocumentDB or Data API |
| Cloud Map (`servicediscovery`) | 26 | HTTP namespaces, service/instance CRUD, discovery, health, revisions, service attributes, operations and tags, account + region scoped | No DNS namespaces/Route 53, asynchronous workers, cross-account discovery or idempotency-token replay; mutations complete synchronously |

Each `service.luau` lists its exported operations. Missing handlers return 501
`NotImplemented`. List pagination tokens are offsets over stable ID ordering;
concurrent changes between pages can affect results. Cloud Map discovery supports
`QueryParameters` and health selection; optional attribute ranking is not modeled.
ACM list sorting and `Includes` filters are not modeled. For boto3 Cloud Map
clients pointed at localhost, use `botocore.config.Config(inject_host_prefix=False)`:
its discovery operations otherwise prepend `data-` to the endpoint hostname.

## Per-service CI checks

Build once, then run a service independently:

```sh
cargo build -p roto-server --locked
python3 tests/smoke-lua-services.py --service cur
python3 tests/smoke-lua-services.py --service acm
python3 tests/smoke-lua-services.py --service budgets
python3 tests/smoke-lua-services.py --service config
python3 tests/smoke-lua-services.py --service servicediscovery
python3 tests/smoke-lua-services.py --service opensearch
python3 tests/smoke-lua-services.py --service rds
python3 tests/smoke-lua-services.py --service ecr
python3 tests/smoke-lua-services.py --service ecs
python3 tests/smoke-lua-services.py --service glue
```

Omit `--service` to run all ten. These checks need only Python's standard
library and start disposable servers on free ports. Every service runs against
both directory loading and ZIP loading, with HTTP round trips, modeled
errors, empty arrays, account/region scope, restart persistence and reset of both
loaded and unopened databases. Service-specific checks cover pagination, tags,
notification/subscriber relationships, certificate import blobs, recorder state,
and instance health/revisions.

Add `--sdk` with boto3 installed to validate response field names/types against
botocore models and run real SDK serialization, pagination, timestamp/blob and
error round trips. The `lua-services` CI matrix runs each service separately with
`--sdk` and pinned boto3, so each service has its own pass/fail result. These are
local emulator contracts; they do not establish full AWS compatibility or a Moto
coverage score.

## Model-backed REST/JSON and Query modules

The version 1 manifest also accepts `protocol` and `model` in place of
`target_prefix` and `json_version`. For example:

```json
{"service":"es","script":"opensearch/service.luau","protocol":"rest-json","model":"opensearch/model.json"}
```

`service` is the signing identity: managed OpenSearch SDK clients sign as `es`.
OpenSearch Serverless is a separate, currently unsupported service. Models are
read at discovery to identify method/path/URI/query/header bindings and operations;
Luau code and SQLite remain lazy. Model files are packaged in directory and ZIP
bundles. Unknown modeled operations return `NotImplemented` (501). Model-derived
operation names are recorded in request traces, including REST requests.

REST JSON currently supports object bodies and scalar URI/query/header bindings.
Query supports form or URL parameters, nested structures, nonflattened lists,
scalar types and XML response envelopes with modeled list member names and ISO
timestamps. Query maps and binary/streaming/payload REST bindings are not
implemented. Required members and basic types are checked; full AWS limits,
patterns, enums and service-specific provisioning constraints are not validated.
Unsigned Query routing requires the matching API version and known action.
Use signed SDK requests for unambiguous service/region/account selection.

Regenerate the stripped wire descriptors with
`python3 scripts/generate-lua-service-models.py` using the CI-pinned botocore/boto3
installation. Descriptors include unsupported operations for routing and tracing;
the Luau exports define the implemented subset. No runtime botocore dependency is
required. Configuration-only behavior is implemented directly in Luau; the local
MiniStack modules/tests informed the selection of operations.

### OpenSearch slice

Exports: `CreateDomain`, `DescribeDomain`, `DescribeDomains`, `DeleteDomain`,
`ListDomainNames`, `DescribeDomainConfig`, `UpdateDomainConfig`, `AddTags`,
`RemoveTags`, `ListTags`.

Supported configuration fields are listed in `opensearch/service.luau`. Config
updates replace the supplied option objects and increment their local revision;
creation/update timestamps describe local changes. `Active`/`Created` describe
configuration state only. No `Endpoint` is returned. Access policy strings are
stored without enforcement. Unsupported provisioning settings return 501.
Domain deletion is immediate and removes its tags.

### RDS slice

Exports: `CreateDBInstance`, `DescribeDBInstances`, `ModifyDBInstance`,
`DeleteDBInstance`, `CreateDBCluster`, `DescribeDBClusters`, `ModifyDBCluster`,
`DeleteDBCluster`, `CreateDBSubnetGroup`, `DescribeDBSubnetGroups`,
`ModifyDBSubnetGroup`, `DeleteDBSubnetGroup`, `CreateDBParameterGroup`,
`DescribeDBParameterGroups`, `DeleteDBParameterGroup`, `ModifyDBParameterGroup`,
`ResetDBParameterGroup`, `DescribeDBParameters`, `AddTagsToResource`,
`RemoveTagsFromResource`, `ListTagsForResource`.

`available` denotes an immediately committed configuration record, with no SQL
backend or advertised endpoint. Creation passwords are checked for presence but
are never stored or returned; password modification is unsupported. Instances
can reference existing clusters with the same engine. Member lists are updated
on create/delete; clusters cannot be deleted while members remain. Deletion
protection is enforced, and instance/cluster deletion requires
`SkipFinalSnapshot=true`; snapshots are unsupported. Subnet IDs are recorded
without EC2 validation; subnet/parameter group attachment and parameter effects
on databases are not implemented. Parameter reset/list operates on explicitly
stored overrides, with no engine-default parameter catalog. Unimplemented
settings and filters return 501. Pagination is stable ID order with offset
`Marker` tokens; this local slice allows page sizes 1–100. Tags are separate
resource metadata, read through `ListTagsForResource`.


### ECR repository slice

Exports: `CreateRepository`, `DescribeRepositories`, `DeleteRepository`,
`PutImageTagMutability`, `PutImageScanningConfiguration`, `SetRepositoryPolicy`,
`GetRepositoryPolicy`, `DeleteRepositoryPolicy`, `PutLifecyclePolicy`,
`GetLifecyclePolicy`, `DeleteLifecyclePolicy`, `TagResource`, `UntagResource`,
`ListTagsForResource`.

Names, duplicate repositories, registry IDs and caller-region ARNs are checked.
Policies retain the supplied JSON text after basic object/Statement or rules
validation; no permissions or lifecycle engine executes them. Mutability modes,
including exclusion-filter configuration, are stored; there are no image APIs to
apply those settings to. Scan-on-push is configuration only. AES256 configuration
is supported; KMS configuration returns 501. `repositoryUri` is standard AWS
metadata, not a local registry endpoint. `GetAuthorizationToken`, image/layer
operations and registry V2 are unsupported. Deletion removes policies and tags
with the repository. Describe pagination uses `maxResults` (1–1000) and offset
`nextToken`; explicit repository-name selection cannot be mixed with pagination.

### ECS configuration slice

Exports: `CreateCluster`, `DescribeClusters`, `ListClusters`, `UpdateCluster`,
`UpdateClusterSettings`, `DeleteCluster`, `RegisterTaskDefinition`,
`DescribeTaskDefinition`, `ListTaskDefinitions`, `ListTaskDefinitionFamilies`,
`DeregisterTaskDefinition`, `CreateService`, `DescribeServices`, `ListServices`,
`UpdateService`, `DeleteService`, `TagResource`, `UntagResource`,
`ListTagsForResource`.

Cluster configuration/settings and tags persist. Task definitions retain nested
container fields, increment revisions per family, and remain describable by
explicit revision after deregistration. Family-only resolution selects the latest
active revision. Lists filter by prefix/status and sort revisions numerically.
Clusters and services accept scoped names or ARNs. Missing entries in batch
describe calls appear in `failures`. Cluster deletion requires no active services.
Service deletion requires zero desired count or `force`; inactive records remain
describable until cluster deletion. Recreating an inactive service stores a new
configuration record. Service configuration references an existing active task
definition. Role, networking, load-balancer and registry settings are stored
without provisioning or IAM validation. Tags support clusters, services and task
definitions; describe responses include tags when requested.

`ACTIVE` describes a local configuration record. Desired count records intent;
running/pending task counts remain zero, and deployments/events are empty. No
container instances, network resources, task execution, reconciliation or logs
are produced. Nonempty capacity providers, service-connect/volume/Lattice
provisioning, external deployment controllers, DAEMON scheduling, idempotency-token
replay and `forceNewDeployment=true` return 501. Lists use page sizes 1–100 and
offset `nextToken` tokens. AWS sizing limits and full nested validation are not
implemented.

### Glue catalog slice

Exports: `CreateDatabase`, `GetDatabase`, `GetDatabases`, `UpdateDatabase`,
`DeleteDatabase`, `CreateTable`, `GetTable`, `GetTables`, `UpdateTable`,
`DeleteTable`, `BatchDeleteTable`, `GetTableVersion`, `GetTableVersions`,
`DeleteTableVersion`, `BatchDeleteTableVersion`, `CreatePartition`,
`GetPartition`, `GetPartitions`, `UpdatePartition`, `DeletePartition`,
`BatchCreatePartition`, `BatchGetPartition`, `BatchDeletePartition`,
`TagResource`, `UntagResource`, `GetTags`.

Catalog IDs must match the caller account. This slice accepts ASCII names using
letters, digits, underscores, dots and hyphens and normalizes database/table names
to lowercase. Database/table updates merge supplied mutable fields; supplied
nested descriptors replace their previous values. Storage descriptors, nested
column types, parameters and view text round-trip without a data/query engine.
Table versions increment monotonically; `VersionId` guards updates against stale
writes. `SkipArchive` removes the immediately previous snapshot while retaining
older archives. Version lists sort numerically; deleting the live version is
rejected. Database/table deletion cascades to versions and partitions.

Partition values must match unique, ordered table partition keys. Values use
length-framed identifiers to avoid delimiter collisions. Updating values checks
for collisions; changing partition keys while partitions exist is rejected.
Batch create/delete return per-entry errors and retain successful entries. Invalid
request shapes abort the whole request transaction. Batch get returns missing
values in `UnprocessedKeys`, matching the
[AWS response contract](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html).
Database tags accept maps through create/get/tag/untag APIs with scoped ARNs;
other resource types are unsupported.

Partition `Expression` supports key comparisons (`=`, `!=`, `<>`, `<`, `<=`, `>`,
`>=`) joined by `AND`. Quoted strings support doubled apostrophes; numeric
partition types compare as numbers. Unsupported grammar/keys/literals raise
`InvalidInputException` rather than return an unfiltered result. OR, IN, LIKE,
parentheses, functions and the broader SQL expression language are unsupported.
`ExcludeColumnSchema` removes columns only from the returned descriptor.
`GetTables` regex filtering and attribute projection return 501. Table/database/
version pages allow 1–100 results; partition pages allow 1–1000, using offset
`NextToken` tokens. Federated/resource links, structured views, partition indexes,
transactions, historical query contexts, Lake Formation, crawlers/jobs/workflows,
and Iceberg table commits are unsupported.
