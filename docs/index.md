# The roto book

roto is an AWS simulator written in Rust: a from-scratch rewrite of
[moto](https://github.com/getmoto/moto)'s server mode. A single binary serves AWS
wire protocols on one port, with persistent SQLite state and S3 object bodies
stored as real files.

roto is intended as a replacement for **LocalStack** for local AWS development,
with persistent resources that survive server restarts. Point your AWS clients
at its local endpoint, create your development environment once, and keep using
its buckets, queues, tables, and other supported resources across sessions.

## Core services and optional Lua services

roto's **core services** are hand-tuned in Rust and built into the server binary.
These are where we aim to offer a **first-class local development experience**,
with useful service behavior, persistence, and cross-service integration.
Most users will start here: typical AWS applications are built on services such
as S3, SQS, DynamoDB, Lambda, SNS, and EventBridge. These support everyday
application workloads such as storing data, processing messages, and running
event-driven code. See
[core service coverage](services.md#core-services-first-class-experience)
for the supported behavior and remaining gaps.

The optional **Lua services** are provided for completeness and mostly offer
simple CRUD, metadata storage, and configuration simulation, mainly for service
administration rather than everyday application workloads. They have a narrower
scope and do not carry the same first-class experience goal. Adding them requires
downloading a **separate service bundle** from a GitHub release and passing
`--lua-services` when starting the server; installing `roto-aws` alone does not
include them. See [optional Lua service coverage](services.md#optional-lua-services-completeness-coverage)
and [bundle installation](getting-started.md#add-optional-lua-services).

## Why an open-source LocalStack alternative

LocalStack for AWS is now distributed as commercial software. In March 2026,
LocalStack replaced its maintained Community edition with a unified distribution
requiring an account and authentication token. Commercial use requires a
commercial license; free access remains available for non-commercial use and
eligible open-source projects. See LocalStack's
[distribution announcement](https://blog.localstack.cloud/the-road-ahead-for-localstack/)
and [licensing documentation](https://docs.localstack.cloud/aws/licensing/).

For many organizations that want to avoid subscription procurement, license
management, and account or token administration for local development and CI,
that makes LocalStack an impractical choice. roto is intended to offer an
MIT-licensed alternative that runs locally without a vendor account, license
key, or subscription.

[MiniStack](https://ministack.org/) is another open-source LocalStack alternative,
with much broader [AWS service coverage](https://ministack.org/docs/services/)
than roto. MiniStack is also committed to remaining free and open source under
the MIT license. roto aims to offer solid support for core AWS services in a
dependency-free Rust binary you can drop into a development environment or CI
without Docker containers or language runtimes. Check roto's
[service coverage](services.md) to see whether it supports your workload.
An embedded Lua interpreter lets you [create and wire services](lua.md), define
inline Lambda handlers, and [intercept AWS requests](runtime-hooks.md), without
installing a separate Lua runtime.

**roto will be free forever.** This project has no revenue goal and no interest
in monetization. There is no plan for paid tiers, license fees, or a commercial
edition. The MIT license lets you use, modify, and redistribute the code,
including in commercial projects.

roto is maintained with AI assistance, with the aim of keeping maintenance costs
effectively zero. That supports keeping the project free without a revenue
model. Changes are still checked through Rust tests and moto compatibility suites.

## Why persistence comes first

Moto focuses on quick, isolated integration tests with disposable mock state.
Its [testing documentation](https://docs.getmoto.org/en/latest/docs/getting_started.html)
describes state being reset between test methods, and the request for
[server-state persistence](https://github.com/getmoto/moto/issues/9755) was closed
as not planned. A persistent local AWS environment is outside that focus.

roto takes moto's server-mode API behavior as a compatibility reference and makes
persistence a core part of the design: SQLite stores service state, and S3 object
bodies live in real files. The goal is a lightweight local AWS environment you
can stop and resume, serving the development role often filled by LocalStack.
LocalStack also [supports persistence](https://docs.localstack.cloud/user-guide/state-management/persistence/);
roto provides its own persistent implementation in a single Rust binary.

For disposable integration tests, roto also offers `--ephemeral` and reset
endpoints. It does not provide moto's in-process `@mock_aws` mode. Its current
service coverage is narrower than LocalStack's; check
[service coverage](services.md) before migrating a workload.

Start with [Getting started](getting-started.md), check [service coverage](services.md),
or read [Architecture](architecture.md) to work on the implementation.

See [Performance](benchmarks.md) for measured startup, idle RSS, and API latency,
with a reproducible suite and raw results.

The book also covers [Lua setup](lua.md), the [full Lua API reference](lua-api.md), runnable [demos](demos.md), [integration testing](integration-testing.md),
[server options and inspection](server.md), and [persistent storage](storage.md).
Core service guides cover [S3](s3.md), [Lambda](lambda.md),
[CloudWatch Logs](cloudwatch-logs.md), [EventBridge](eventbridge.md), and
[Kinesis](kinesis.md). Other service guides cover
[RDS Data API](rds-data.md), [CloudFormation](cloudformation.md), [KMS](kms.md), and
[IoT topics and WebSockets](iot.md).
The [runtime Lua handler and interception guide](runtime-hooks.md) covers inline
Lambda handlers and request interceptors.

## Project maturity

roto is under active development. It implements a growing set of services, with
substantial differences in coverage. Credentials are tracked for account routing,
but signatures and IAM policies are not enforced. Stored policies and ACLs do
not imply access checks.

The repository's [STATUS.md](https://github.com/vivainio/roto/blob/main/STATUS.md)
is the detailed coverage baseline. [PLAN.md](https://github.com/vivainio/roto/blob/main/PLAN.md)
contains design goals and future work; planned features are not necessarily implemented.

## License

roto is MIT licensed. Vendored moto tests and botocore service models carry
Apache-2.0 notices; see the repository's
[NOTICE](https://github.com/vivainio/roto/blob/main/NOTICE).
