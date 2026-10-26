# Comparing AWS simulators

Roto is a good fit when you want a persistent local AWS environment that starts
as a native Rust binary, with embedded Luau for resource setup and targeted
mocking. Its core services aim to support application behavior such as storing
objects, processing queue messages, and delivering events between services.
Check [service coverage](services.md) against your workload before choosing it.

There are several credible alternatives, including other standalone Rust
implementations. Running without Docker is a practical benefit of Roto, but it
is shared by several projects. The more specific reason to choose Roto is the
combination of persistent state, embedded setup scripts, inline Lambda handlers,
and request interception in one local server.

This comparison was reviewed in October 2026 using the linked project
documentation and selected implementation files. It is not a head-to-head
compatibility or performance benchmark. Features described below are documented
by their projects; service counts alone do not establish behavioral fidelity.

## Start with the workload

| What you need | Options to investigate |
| --- | --- |
| Persistent buckets, tables, queues, and event flows with scripted fixtures | Roto; verify the required operations and integrations in its coverage guide |
| In-process mocks inside Python tests | Moto's decorators and context managers |
| Broad AWS and infrastructure-as-code workflows | LocalStack, Floci, MiniStack, fakecloud, and LocalEmu; test your actual templates and deployment paths |
| A native Rust server without a container image | Roto, Rustack, laws, and fakecloud |
| Real database engines behind AWS-shaped control APIs | Projects with database container backends, including Floci, MiniStack, fakecloud, and LocalEmu |
| Only DynamoDB or SQS | DynamoDB Local or ElasticMQ may be sufficient |

For an event-driven application, test the whole path. Creating an S3 notification
configuration is different from delivering an object event to Lambda; creating
an event-source mapping is different from polling a queue, invoking a function,
and retrying failures. The distinction matters more than a service appearing in
a supported-services list.

## Where Roto differs

### A native server with embedded setup

Roto's Rust core services, SQLite storage, and Luau interpreter run in one
process. The server needs no container image, Docker daemon, Python environment,
or separate Lua installation. Many less central service simulators are
implemented entirely in Luau and downloaded separately as an
[optional service bundle](getting-started.md#add-optional-luau-services).
These primarily cover administration, metadata, and configuration APIs, where
basic simulation is useful but a full Rust implementation would offer less
value for typical application tests. They have a narrower behavioral scope than
the Rust core services and are not included in the base installation. The
embedded interpreter runs them without a separate Luau runtime.

A setup script can create resources, populate data, and connect event sources
before the listener starts. Tests can start a fresh environment with:

```sh
roto-server --ephemeral --setup examples/demo/setup.luau
```

Run this repository example on a fresh store: its fixed resource names make it
a fixture seed rather than an idempotent migration. See the
[Luau scripting guide](lua.md) for smaller scripts to adapt to your application.

Other simulators also provide initialization mechanisms. Floci and MiniStack,
for example, support LocalStack-compatible startup scripts. Roto's convenience
is that its setup language and service helpers are embedded, so fixture setup
does not require launching an AWS CLI or installing an SDK runtime.

### Stateful simulation with targeted mocks

The same script can register inline Luau Lambda handlers and hooks for specific
AWS service operations. A hook can let a request proceed normally or return a
custom wire response, making it possible to inject a failure for a particular
input while keeping normal service behavior for other requests.

That is useful for testing retries, handling unavailable dependencies, or
replacing one part of an otherwise stateful integration test. See
[runtime hooks](runtime-hooks.md) for examples and limits. Hooks apply to external
AWS HTTP requests; internal service-to-service calls and startup setup calls do
not pass through them. Scripts must construct the appropriate AWS response
format. Inline handlers are local placeholders rather than execution of uploaded
Lambda packages.

Mocking and fault injection are not exclusive to Roto. LocalEmu documents
throttling and latency simulation, and fakecloud documents configurable Bedrock
responses and failures. Roto offers a programmable operation-level mechanism
through the same Luau environment used for setup.

### Vision: deeper control over internal state

The longer-term vision is to let Luau make “deep tweaks” to the simulator's
internal service data and behavior. A test could put a resource into an unusual
state, adjust timing or retry metadata, or arrange a partial failure that is
hard to reproduce through public AWS APIs. Subsequent requests could then
exercise the service implementation against that state.

This would extend Luau from resource setup and response interception into a
programmable test environment with access below the AWS API surface. It is a
future direction, not a currently supported general-purpose internal-state API.
Today's setup helpers call service APIs, and request hooks can replace external
responses; they do not provide arbitrary access to Rust service internals.

### Persistence as the normal development workflow

Roto stores service state in SQLite and S3 object bodies in files. You can keep a
local environment across server restarts, or use `--ephemeral` and reset
endpoints for disposable tests. See [storage](storage.md) and
[integration testing](integration-testing.md).

Persistence is also available in several alternatives. Compare what survives a
restart, when writes reach disk, and whether execution state and object bodies
are included. A shutdown snapshot and a database-backed mutation have different
failure characteristics; neither a persistence flag nor the word “WAL” alone
establishes equivalent durability across implementations.

## Other native implementations

### Rustack

[Rustack](https://github.com/tyrchen/rustack) is an MIT-licensed Rust server with
service adapters selectable at build time and startup. It can run directly or
in Docker. Its capability endpoint and
[runtime guide](https://github.com/tyrchen/rustack/blob/master/docs/runtime-hardening.md)
help distinguish routed operations from execution, delivery, and persistence
support. Lambda execution is disabled by default and must be selected explicitly.

Rustack is worth evaluating alongside Roto when a native executable and control
over the included service set matter. Use its capability information to check
your event flows, rather than treating its operation count as a compatibility
guarantee.

### laws

[laws](https://github.com/huseyinbabal/laws) is an MIT-licensed Rust binary with an
embedded dashboard and a broad catalog of AWS APIs. Its README describes
data-driven protocol handling and concurrent in-memory storage through DashMap.
Its [persistence implementation](https://github.com/huseyinbabal/laws/blob/main/src/persistence.rs)
also provides optional SQLite-backed maps with write-through insertion and
removal and rehydration at construction.

That makes laws a closer implementation comparison than its brief README might
suggest. Check persistence and execution support for each service you use; the
existence of a shared storage module does not establish that every service uses
it for every mutation. Roto's Luau setup and request-hook workflow is a concrete
feature to compare when evaluating the two.

### fakecloud

[fakecloud](https://github.com/faiscadev/fakecloud) is an AGPL-3.0 Rust emulator.
Its [architecture](https://fakecloud.dev/docs/about/architecture/) uses a Cargo
workspace with separate service crates and a shared dispatcher. The server runs
without Docker; Lambda and real database workloads use container backends.

The project emphasizes cross-service integrations, Smithy-model conformance
testing, and SDKs for assertions against recorded activity. It also documents
configurable Bedrock responses and failures. Optional
[persistent mode](https://fakecloud.dev/docs/reference/persistence/) writes
service snapshots on mutations and stores S3 object bodies on disk.

fakecloud is another direct alternative for a native server. Its license differs
from Roto's MIT license. Evaluate its conformance claims alongside its behavioral
parity documentation: passing model-derived cases is useful evidence, but does
not by itself establish every production AWS behavior.

## Broader local cloud environments

### Floci

[Floci](https://floci.io/aws/) uses Java and Quarkus Native rather than Rust or a
Python interpreter. Its [repository](https://github.com/skyiron/floci-aws-emulator)
documents LocalStack-compatible configuration and init scripts, SDK and IaC
compatibility tests, and memory, persistent, hybrid, and write-ahead-log storage
modes. Real-engine services use container images.

Floci is particularly relevant when migrating an existing LocalStack setup:
configuration translation and compatible init paths may reduce changes to CI.
Compare that migration convenience with Roto's embedded Luau fixtures and the
operations your application actually needs.

### MiniStack

[MiniStack](https://github.com/ministackorg/ministack) is an MIT-licensed Python
emulator that can run through pip or Docker. It supports shell and Python init
scripts, including LocalStack-compatible paths, and optional state snapshots
saved on shutdown and restored at startup.

Lambda can execute in local subprocesses or containers; RDS and ElastiCache can
use real database containers. MiniStack therefore does not require Docker for
every workload. It is worth investigating for broad service coverage and
existing Python or shell fixtures. Roto offers a native server and an embedded
setup runtime, with a smaller service scope to validate against your application.

### LocalEmu

[LocalEmu](https://github.com/localemu/localemu) is an Apache-2.0-licensed
LocalStack fork with a Python core, installable through pip. Most services run
without Docker; workloads such as Lambda and real database engines use sidecar
containers. Persistence is optional, with shutdown, scheduled, and manual
snapshot strategies.

It also documents opt-in IAM enforcement, throttling, and latency simulation.
Those are relevant when permission and resilience testing are central to your
workflow. Roto currently tracks credentials for account routing but does not
enforce IAM policies or verify signatures; storing a policy is not an access
check.

### LocalStack

[LocalStack](https://docs.localstack.cloud/aws/getting-started/) provides a broad
container-based AWS environment with infrastructure tooling support. It remains
a relevant choice for teams whose SDK, Terraform, CDK, and deployment workflows
are already validated against it.

Its maintained AWS distribution requires an account and authentication token;
consult the current [licensing terms](https://docs.localstack.cloud/aws/licensing/)
for your use case. Roto uses the MIT license and runs without a vendor account,
license key, or subscription. Its coverage is narrower, so migration requires
checking both API operations and service integrations.

## Python mocks and focused service emulators

[Moto](https://docs.getmoto.org/en/latest/docs/getting_started.html) is especially
convenient for Python unit and integration tests: decorators and context managers
provide disposable AWS mock state inside the test process. Its
[server mode](https://docs.getmoto.org/en/latest/docs/server_mode.html) also serves
SDKs in other languages. Roto takes Moto's server-mode behavior as a compatibility
reference but provides a separate persistent Rust implementation. It does not
replace Moto's in-process `@mock_aws` interface.

If you only need one service, a focused implementation may be simpler.
[DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
is AWS's downloadable DynamoDB implementation, available through Java, Maven,
or Docker. [ElasticMQ](https://github.com/softwaremill/elasticmq) provides an
SQS-compatible queue interface and runs standalone or embedded. Neither aims to
provide a complete environment for interactions between different AWS services.

## Evaluate with a representative test

Start with one application flow, including its failure path. Create resources,
write real application data, trigger an event, and assert the resulting state.
If persistence matters, restart the server and repeat the reads. If infrastructure
tooling matters, apply the actual template and check its outputs and updates.

For Roto, use [service coverage](services.md) to identify gaps, the
[demo fixtures](https://github.com/vivainio/roto/blob/main/examples/demo/setup.luau)
to explore populated state, and [request traces](integration-testing.md) to see
what the application called. For performance comparisons, use the reproducible
[benchmark suite](benchmarks.md) on the same machine and workload. Keep real-AWS
checks for behaviors your local tests do not establish, especially permissions
and managed runtime behavior.
