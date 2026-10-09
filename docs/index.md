# The roto book

roto is an AWS simulator written in Rust: a from-scratch rewrite of
[moto](https://github.com/getmoto/moto)'s server mode. A single binary serves AWS
wire protocols on one port, with persistent SQLite state and S3 object bodies
stored as real files.

Use it to run local application and integration tests with AWS clients pointed
at a local endpoint. It does not provide moto's in-process `@mock_aws` mode.

Start with [Getting started](getting-started.md), check [service coverage](services.md),
or read [Architecture](architecture.md) to work on the implementation.

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
