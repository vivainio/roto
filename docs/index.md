# The roto book

roto is an AWS simulator written in Rust: a from-scratch rewrite of
[moto](https://github.com/getmoto/moto)'s server mode. A single binary serves AWS
wire protocols on one port, with persistent SQLite state and S3 object bodies
stored as real files.

roto is intended as a replacement for **LocalStack** for local AWS development,
with persistent resources that survive server restarts. Point your AWS clients
at its local endpoint, create your development environment once, and keep using
its buckets, queues, tables, and other supported resources across sessions.

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

The book also covers [Lua setup](lua.md), runnable [demos](demos.md), [integration testing](integration-testing.md),
[server options and inspection](server.md), and [persistent storage](storage.md).
Service guides explain [CloudFormation](cloudformation.md), [EventBridge](eventbridge.md),
[Kinesis](kinesis.md), [KMS](kms.md), [IoT topics and WebSockets](iot.md), and
[local Lambda execution](lambda.md).
The planned [runtime Lua hook system](runtime-hooks.md) describes future request
interception and missing-operation fallbacks.

## Project maturity

roto is under active development. Eight services have implementations, with
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
