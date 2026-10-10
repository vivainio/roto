# Architecture

roto is a Cargo workspace with shared protocol and storage infrastructure and a
handler crate for each service.

| Crate | Responsibility |
| --- | --- |
| `roto-server` | Axum listener, CLI, routing, health and reset endpoints |
| `roto-core` | Store and migrations, request context, IDs, errors, credential-scope parsing |
| `roto-protocol` | Shared wire-protocol codecs |
| `roto-codegen` | Generate types, serialization, routing tables, and operation lists from botocore models |
| `roto-svc-*` | Service behavior over generated types |

## Request flow

1. The server collects the HTTP method, path, query, headers, and body.
2. Credential scope or host name identifies the service; supported unsigned
   requests can be claimed by a handler.
3. The access key resolves the account and the credential scope provides the region.
4. The service decodes the request, runs its behavior against the store, and encodes a response.
5. The server emits the HTTP response and request ID.

Synchronous service work runs through `spawn_blocking` so it does not block the
async listener's executor.

## Models and protocols

Botocore `models/<service>/service-2.json` files feed `roto-codegen`. Generated
`src/generated.rs` files are checked into each service crate. Change the generator
and regenerate rather than editing these files by hand.

Query/XML serves STS, IAM, SNS, and CloudFormation. JSON 1.0/1.1 and its error handling support
services such as SQS; REST-XML bindings and generated routes support S3.
REST-JSON and EC2 query are future work.

The [project plan](https://github.com/vivainio/roto/blob/main/PLAN.md) records design
directions. Use source code and the current coverage baseline to distinguish
implemented behavior from planned infrastructure.

CloudFormation resolves template dependencies and calls existing service handlers
in-process with the caller's account and region. Stack/resource progress is saved
in its own SQLite database; service databases retain the actual resource state.
Stack mutations are serialized, but resource changes span service transactions
and are not atomic. Failed stack updates attempt to reconcile the previous
template; rollback can itself fail and does not restore deleted resource data.
