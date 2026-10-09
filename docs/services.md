# Service coverage

This overview describes the implementation on **2026-10-09**. See
[STATUS.md](https://github.com/vivainio/roto/blob/main/STATUS.md) for operation counts,
moto test results, and detailed gaps. A service being listed here does not mean
full AWS compatibility.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| STS | Caller identity, sessions, role assumption | Authorization-message decoding |
| SQS | Queues, messages, batches, FIFO, deduplication, long polling, DLQ redrive, tags | Message-move tasks, dead-letter source listing, CloudFormation integration |
| S3 | Buckets, objects, listing, versioning, multipart, tagging, stored ACLs and bucket configuration | Object lock, restore, select, checksum details, event delivery, policy enforcement |
| IAM | Users, roles, access keys, tags, account aliases | Managed policies, groups, instance profiles, providers |
| DynamoDB | Tables, items, expressions, query/scan, GSI/LSI, batch, transactions, tags, TTL, backups | PartiQL, import, streams |
| SSM | Parameter Store, versions, labels, history, hierarchy, tags, SecureString | Documents, commands, maintenance windows, patch baselines, public parameters |
| Secrets Manager | Secrets, versions, staging labels, deletion/restore, tags, policies, rotation bookkeeping, batch get | Cross-region replication, Lambda rotation execution |
| SNS | Topics, subscriptions, publishing, SQS fan-out, filter policies, FIFO checks, tags | Platform endpoints, SMS attributes, HTTP/Lambda/email delivery |

KMS, Kinesis, and Lambda have not been started.

## S3 details

S3 supports path-style and virtual-host addressing, range and conditional reads,
copy operations, delete markers, multipart uploads and `aws-chunked` uploads.
Listings use SQLite metadata. Current object bodies are visible in the
[storage directory](storage.md).

Many bucket sub-resources are stored and returned without executing their
behavior. For example, storing lifecycle, website, CORS, or notification settings
does not implement lifecycle execution, website/CORS serving, or notification delivery.
ACLs and policies are stored without enforcement.

## Cross-service behavior

SNS can deliver to SQS in-process, including raw delivery and filters on message
attributes or bodies. STS and IAM share account credential information.
CloudFormation is not implemented, so tests requiring it remain outside the
passing baseline.
