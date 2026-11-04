# roto

An MIT-licensed AWS simulator for local development and CI. One Rust binary
serves AWS APIs on one port, with persistent SQLite state and S3 objects stored
as real files. No Docker, vendor account, or license key required.

```sh
uv tool install roto-aws
roto-server --port 5070
```

Point your AWS SDK or CLI at `http://localhost:5070`. Resources survive restarts;
use `--ephemeral` for disposable test environments.

**[Get started](getting-started.md)** · **[Check service coverage](services.md)** ·
**[Compare AWS simulators](comparisons.md)**

## What’s included

- **Core AWS services** built into the binary, including S3, SQS, DynamoDB,
  Lambda, SNS, and EventBridge. See [supported behavior and gaps](services.md#core-services-first-class-experience).
- **Embedded Luau** for [setup scripts](lua.md), inline Lambda handlers, and
  [request hooks](runtime-hooks.md).
- **Optional Luau services** for basic administration and configuration APIs.
  These require a [separate service bundle](getting-started.md#add-optional-luau-services).
- **An inspection UI** for browsing local resources. See [server options](server.md)
  and [runnable demos](demos.md).

## Before you use it

roto is under active development, and coverage varies by service. Credentials
route requests to accounts; signatures and IAM policies are not enforced.
Check [service coverage](services.md) before migrating a workload.

For implementation details, see [Architecture](architecture.md),
[Performance](benchmarks.md), and [Integration testing](integration-testing.md).
[STATUS.md](https://github.com/vivainio/roto/blob/main/STATUS.md) tracks coverage;
[PLAN.md](https://github.com/vivainio/roto/blob/main/PLAN.md) describes future work.

roto is [MIT licensed](https://github.com/vivainio/roto/blob/main/LICENSE).
Vendored tests and service models carry [Apache-2.0 notices](https://github.com/vivainio/roto/blob/main/NOTICE).
