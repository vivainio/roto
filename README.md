# roto

An MIT-licensed AWS simulator written in Rust, with persistent SQLite state and
S3 objects stored as real files. One binary serves AWS wire protocols on one port.
Runs directly on your machine; no container image or Docker daemon is needed.

```sh
uv tool install roto-aws
roto-server --port 5070
```

**[Read the roto book](https://vivainio.github.io/roto/)** for
[getting started](https://vivainio.github.io/roto/getting-started/),
[service coverage](https://vivainio.github.io/roto/services/),
[performance results](https://vivainio.github.io/roto/benchmarks/), and
[development](https://vivainio.github.io/roto/development/).

The hand-tuned Rust core services are where we aim to offer a first-class
experience for typical AWS applications. Many less central simulators are
implemented entirely in Luau. These optional services provide basic
administration and configuration APIs and require a separate bundle download;
see the book's [installation guide](https://vivainio.github.io/roto/getting-started/#add-optional-luau-services).

Under active development. See [STATUS.md](STATUS.md) for detailed coverage,
[PLAN.md](PLAN.md) for the design notes, and [LICENSE](LICENSE) for the MIT license.

## Other AWS simulators

See the book's [comparisons chapter](https://vivainio.github.io/roto/comparisons/)
for implementation details, setup and mocking workflows, and guidance on choosing
a simulator for your application.

Newer projects include:

- [Floci](https://floci.io/aws/) is built with Quarkus Native and targets
  lightweight local AWS emulation and migration from LocalStack. It supports
  LocalStack-compatible init scripts and memory, persistent, hybrid, and WAL
  storage modes.
- [MiniStack](https://github.com/ministackorg/ministack) is an MIT-licensed
  Python emulator, runnable through pip or Docker. It supports shell/Python init
  scripts, optional state snapshots, local-process Lambda execution, and real
  database containers for services such as RDS and ElastiCache.
- [Rustack](https://github.com/tyrchen/rustack) is an MIT-licensed Rust binary
  with selectable service adapters and an explicit capability endpoint. Its
  documentation distinguishes routed API operations from execution, delivery,
  and persistence support; Lambda execution is disabled by default.
- [laws](https://github.com/huseyinbabal/laws) is a standalone Rust binary with
  a broad catalog of AWS service APIs and no Docker or Python runtime requirement.
  It embeds a dashboard; its [persistence implementation](https://github.com/huseyinbabal/laws/blob/main/src/persistence.rs)
  provides optional SQLite-backed maps alongside in-memory storage.
- [fakecloud](https://github.com/faiscadev/fakecloud) is an AGPL-licensed
  Rust binary emphasizing Smithy-model conformance tests, cross-service
  integrations, and test-assertion SDKs. It supports optional disk persistence
  and configurable Bedrock responses/failures; Lambda and database execution use
  container backends. The server itself runs without Docker.
- [LocalEmu](https://github.com/localemu/localemu) is an Apache-licensed fork of
  LocalStack with a Python core, installable through pip. It offers optional
  persistence, IAM enforcement, throttling, and latency simulation. Most services
  run without Docker; Lambda and other real-engine workloads use containers.

These descriptions summarize each project's stated focus; service counts and
API compatibility vary, so check the operations your application needs.

Established and service-specific options include:

- [LocalStack](https://docs.localstack.cloud/aws/getting-started/) provides a
  broad local AWS environment in a container, with support for infrastructure
  tools such as Terraform and CDK.
- [Moto](https://docs.getmoto.org/en/latest/docs/getting_started.html) provides
  AWS mocks for Python tests through decorators and context managers. Its
  [standalone server](https://docs.getmoto.org/en/latest/docs/server_mode.html)
  also works with SDKs in other languages.
- [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
  is AWS's own local DynamoDB implementation, available as a Java download,
  Maven dependency, or Docker image. It focuses on one service.
- [ElasticMQ](https://github.com/softwaremill/elasticmq) is a message queue with
  an SQS-compatible interface, usable standalone or embedded. It is a focused
  option for testing queue-based applications.

Roto combines a native Rust server, persistent SQLite state, and embedded
[Luau scripting](docs/lua.md) for quick, repeatable setup across services.
The server and Luau setup run in one process, without pulling a container image
or installing a separate scripting runtime. Lambda functions can use inline
Luau handlers or local processes; [container execution](docs/lambda.md#lambda-container-images)
is an optional mode that requires Podman or Docker.
A `--setup` script can create resources, seed data, and wire event sources before
the server starts; see the [demo seed](examples/demo/setup.luau).
The same script can register [inline Lambda handlers and request hooks](docs/runtime-hooks.md)
to return custom responses or inject failures for specific AWS operations.
This lets tests combine stateful service simulation with targeted mocks in one
local server. See [service coverage](STATUS.md) when choosing a simulator for
your application.
