# Luau types

Setup scripts run on [Luau](https://luau.org), a gradually typed Lua. Type annotations and
checking are optional and happen **before** you start the server: Roto does not check types
at runtime. You run a separate checker over the script, and it tells you about a
misspelled option, a missing field or a wrong value type without starting anything.

Roto provides the part the checker cannot know on its own: a *definitions file*,
`roto.d.luau`, that describes everything the `roto` table offers.

**Download: [roto.d.luau](roto.d.luau)**. The server generates the same file with
`roto-server --lua-stubs DIR`, so the file always matches the version you run.

## Check a script

Start the script with `--!strict`. Without it Luau checks loosely, and most mistakes
pass. Then run:

```sh
roto-server --check setup.luau
```

This writes the definitions that match your server version to a temporary directory and runs
[luau-lsp](https://github.com/JohnnyMorganz/luau-lsp) over the script. It prints the
checker's findings and exits non-zero on a type error, so it works in CI. Roto's own CI checks
every script under `examples/` and `demos/` this way (`scripts/check-luau-types.sh`).

`luau-lsp` must be installed and on your `PATH` (or set `ROTO_LUAU_LSP` to its path). If it is
missing, `--check` stops and points you to [Install luau-lsp](#install-luau-lsp). It also warns
about a file without `--!strict`.

To run the checker yourself, write the definitions file and pass it explicitly:

```sh
roto-server --lua-stubs types          # writes types/roto.d.luau
luau-lsp analyze --platform=standard --flag:LuauSolverV2=true \
  --definitions=types/roto.d.luau setup.luau
```

For completion, hover and live errors in an editor (VS Code, Neovim, Zed and others),
install the luau-lsp extension or plugin and point it at the same file:

```json
{
  "luau-lsp.types.definitionFiles": { "roto": "types/roto.d.luau" },
  "luau-lsp.fflags.enableNewSolver": true
}
```

The definitions work with both Luau type solvers. The new solver (`LuauSolverV2`) gives
better diagnostics and is the one to use.

## Install luau-lsp

`luau-lsp` is a single binary. Homebrew and most Linux package managers do not carry it, so
download it from the [releases page](https://github.com/JohnnyMorganz/luau-lsp/releases):

| Platform | Asset |
| --- | --- |
| macOS (Apple silicon and Intel) | `luau-lsp-macos.zip` |
| Linux x86-64 | `luau-lsp-linux-x86_64.zip` |
| Linux arm64 | `luau-lsp-linux-arm64.zip` |
| Windows | `luau-lsp-win64.zip` |

Unzip it and put the `luau-lsp` binary on your `PATH`, for example on macOS or Linux:

```sh
unzip luau-lsp-macos.zip -d ~/.local/bin
chmod +x ~/.local/bin/luau-lsp
luau-lsp --version
```

On macOS, if Gatekeeper blocks the downloaded binary, clear the quarantine flag with
`xattr -d com.apple.quarantine ~/.local/bin/luau-lsp`. If you would rather not change your
`PATH`, leave the binary anywhere and set `ROTO_LUAU_LSP=/full/path/to/luau-lsp`.

## What you see

```luau
--!strict
local queue = roto.sqs.queue("jobs", { visibility_timeout = "30" })
roto.aws.sqs.SendMessage({ QueueUrl = queue.url })
roto.aws.s3.PutBucketCors({ Bucket = 1 })
local url: number = roto.aws.sqs.GetQueueUrl({ QueueName = "jobs" }).QueueUrl
roto.aws.logs.CreateLogGroup({ logGroupName = "x" })
```

The checker reports:

```text
bad.luau(2,61): TypeError: Expected this to be 'number?', but got 'string'
bad.luau(3,26): TypeError: Table type '{ QueueUrl: string }' not compatible with type
  'aws_sqs_SendMessageRequest' because the former is missing field 'MessageBody'
bad.luau(4,38): TypeError: Expected this to be 'string', but got 'number'
bad.luau(4,27): TypeError: Table type '{ Bucket: number }' not compatible with type
  'aws_s3_PutBucketCorsRequest' because the former is missing field 'CORSConfiguration'
bad.luau(5,21): TypeError: Expected this to be 'number', but got 'string?'
bad.luau(6,1): TypeError: Key 'logs' not found in table '{ cloudformation: ...
```

Line 2 is a wrong option type, line 3 a missing required AWS member, line 4 both, line 5 a
result used as the wrong type (a field the service may omit is `string?`), and line 6 a
service that has no typed client.

## How it works

**Two kinds of definitions.** The helpers (`roto.s3.bucket`, `roto.sqs.queue`,
`roto.lambda.fn`, `roto.hooks.*` and so on, with their option tables, return tables and
callback arguments) are written by hand in the server's source. The AWS part is generated
from the botocore service models that Roto already embeds, so a request or response type is
exactly the shape the AWS API documents.

**`roto.aws` is the typed door to `roto.call`.** `roto.aws.<service>.<Operation>(input)` does
exactly what `roto.call("<service>", "<Operation>", input)` does, including tracing, but
the definitions give each operation its own input and output type:

```luau
--!strict
local queue = roto.sqs.queue("jobs")
roto.aws.sqs.SendMessage({ QueueUrl = queue.url, MessageBody = "{}" })   -- checked
local out = roto.aws.sqs.GetQueueUrl({ QueueName = "jobs" })
print(out.QueueUrl)                                                       -- string
```

`roto.call(service, operation, input)` stays available and untyped (`any`). It is the way to
reach an operation or service that has no typed client.

**Nothing changes at runtime.** The definitions only describe the API. A call the checker
would reject still runs, and the server still returns the AWS error for bad input. Roto never
refuses a call because of a type.

## Which operations are typed

Typing every operation of every service would make a very large definitions file, so it
covers what setup scripts use:

| Services | Typed operations |
| --- | --- |
| `s3`, `sqs`, `sns`, `dynamodb`, `lambda` | every operation Roto implements |
| `iam`, `sts`, `ssm`, `secretsmanager`, `kms`, `events` (also `eventbridge`), `kinesis`, `cloudformation` | the operations scripts commonly use |
| anything else | untyped, through `roto.call` |

The choice is in `crates/roto-server/typed-operations.json`: a service is either
`"implemented"` or a list of operation names. "Implemented" is not a hand-kept list. Each
service implements its operations on a generated trait, and a small attribute on the
`impl` block (`#[implements_operations]`) records exactly the methods it defines, so the
typed set cannot drift from the code. A test fails if a listed operation is not
implemented.

To type another operation, add its name to the service's list in that file; to type a whole
service, set it to `"implemented"`.

## Limits of the checker

- Luau tables are open, so an unknown extra field in an option table is not always flagged.
  A missing required field and a wrong value type are.
- HTTP methods in `roto.wire` and `cors.methods` are literal unions, so a misspelled method is an
  error. Luau has no numeric literal types, so MQTT QoS and return codes stay `number`.
- AWS enum members are typed as `string`, not as a union of the allowed values. (Literal
  unions are rejected inside nested tables by the old Luau solver.)
- Most definition types (`QueueOptions`, `Queue`, ...) cannot be named in your own
  annotations: Luau definition files do not export type aliases. Annotate with
  `typeof(...)` or let inference carry the types. The exceptions are the Lambda event
  types, which are declared as external types so you can write `event: SqsEvent`,
  `S3Event`, `EventBridgeEvent` or `HttpApiEvent` on an inline handler (see [Lambda](lambda.md#inline-luau-placeholder-functions)).
- Setup-only helpers are not distinguished from runtime-callback code; calling one inside a
  callback is still a runtime error.
- The old solver reports a false error for some deeply nested DynamoDB items. Use the new
  solver.
- Luau service modules (`services/*/service.luau`) use a different `roto` environment and
  are not covered by these definitions.

## The helper definitions

This is the part of the file that describes the setup helpers, options and callbacks.

```luau
--8<-- "docs/roto.d.luau:helpers"
```
