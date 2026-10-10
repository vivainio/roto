# Service coverage

This overview describes the implementation on **2026-10-10**. See
[STATUS.md](https://github.com/vivainio/roto/blob/main/STATUS.md) for operation counts,
moto test results, and detailed gaps. A service being listed here does not mean
full AWS compatibility.

| Service | Available behavior | Main gaps |
| --- | --- | --- |
| STS | Caller identity, sessions, role assumption | Authorization-message decoding |
| SQS | Queues, messages, batches, FIFO, deduplication, long polling, DLQ redrive, tags | Message-move tasks, dead-letter source listing |
| S3 | Buckets, objects, listing, versioning, multipart, tagging, stored ACLs and bucket configuration | Object lock, restore, select, checksum details, SNS/SQS notification delivery, policy enforcement |
| IAM | Users, roles, access keys, tags, account aliases, customer-managed policies and versions, user/role/group attachments and entity listings, groups and membership, inline role/group policies; CloudFormation roles and `AWS::IAM::Policy` | AWS-managed policy catalog, full policy-document validation, instance profiles, providers; policy evaluation is not performed |
| DynamoDB | Tables, items, expressions, query/scan, GSI/LSI, batch, transactions, tags, TTL, backups | PartiQL, import, streams |
| SSM | Parameter Store, versions, labels, history, hierarchy, tags, SecureString | Documents, commands, maintenance windows, patch baselines, public parameters |
| Secrets Manager | Secrets, versions, staging labels, deletion/restore, tags, policies, rotation bookkeeping, batch get | Cross-region replication, Lambda rotation execution |
| SNS | Topics, subscriptions, publishing, SQS fan-out, filter policies, FIFO checks, tags | Platform endpoints, SMS attributes, HTTP/Lambda/email delivery |
| Lambda | Function metadata, local command/HTTP execution, S3 notifications, standard SQS mappings, Lua startup setup | Packaged runtimes, versions/aliases, FIFO/Kinesis polling |
| EventBridge | Default/custom buses, pattern rules, Lambda/SQS targets, S3 events, persistent delivery | Schedules, input transformers, archives/replays, cross-account targets, permissions |
| Kinesis | Streams, shards, records, polling, expiring iterators, resharding, retention, tags, consumer registration, encryption/monitoring metadata | Enhanced fan-out streaming, Lambda polling, throughput enforcement, resource policies, actual encryption |
| KMS | Persisted symmetric keys and aliases, listing, tags, key policies, rotation status, deletion scheduling, Encrypt/Decrypt, ReEncrypt, random data keys | Real encryption, asymmetric operations, grants, multi-region keys, policy enforcement |
| CloudFormation | Synchronous stacks for SQS/SNS/S3/DynamoDB/Kinesis/IAM/Lambda, refs, outputs, tags, change sets, events, rollback attempts, retention, stack policies, termination protection, persistence | Nested stacks, imports, transforms, rollback triggers, Lambda configuration updates, full policy semantics |

KMS uses a versioned base64 JSON envelope for simulated ciphertext; it provides no
cryptographic protection. See [KMS](kms.md).

## S3 details

S3 supports path-style and virtual-host addressing, range and conditional reads,
copy operations, delete markers, multipart uploads and `aws-chunked` uploads.
Listings use SQLite metadata. Current object bodies are visible in the
[storage directory](storage.md).

Many bucket sub-resources are stored and returned without executing their
behavior. For example, storing lifecycle, website, CORS, or notification settings
does not implement lifecycle execution or website/CORS serving. S3 Lambda and EventBridge notifications execute supported object events.
ACLs and policies are stored without enforcement.

## Cross-service behavior

SNS can deliver to SQS in-process, including raw delivery and filters on message
attributes or bodies. STS and IAM share account credential information.
The [CloudFormation subset](cloudformation.md) creates and updates SQS queues, SNS
topics, S3 buckets, DynamoDB tables, Kinesis streams, IAM roles, and inline IAM
policies through those same handlers. It also creates and deletes Lambda functions;
Lambda configuration updates, EventBridge, and SSM resources remain unsupported.

Lua startup setup and execution contracts are covered in [Lua setup](lua.md),
[local Lambda execution](lambda.md), and [EventBridge](eventbridge.md).
