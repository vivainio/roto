# Luau types

Setup scripts run on [Luau](https://luau.org), a gradually typed Lua. Roto publishes type
definitions for everything it provides, so the Luau type checker flags a misspelled option,
a missing field or a wrong value type before you start the server. They cover:

- every `roto.*` helper: option tables, return tables and callback signatures
- every operation of the AWS services Roto implements, through typed clients
  (`roto.aws.s3.PutBucketCors({ ... })`): the input and output of each operation come
  from the AWS service models
- the shapes passed to request and MQTT hooks, and to inline Lambda handlers

Nothing is checked at runtime by the definitions; the server still rejects unknown
options itself. A script without `--!strict` is checked loosely or not at all.

**Download: [roto.d.luau](roto.d.luau)**. The file is generated from the server's source by
`roto-server --lua-stubs DIR` each time the book is built, so it always matches this version.

## Checking a script

Start the script with `--!strict`, then check it with
[luau-lsp](https://github.com/JohnnyMorganz/luau-lsp), which also gives editors (VS Code,
Neovim, Zed and others) completion and hover for the definitions:

```sh
roto-server --lua-stubs types          # writes types/roto.d.luau
luau-lsp analyze --platform=standard \
  --flag:LuauTarjanChildLimit=1000000 \
  --definitions=types/roto.d.luau setup.lua
```

The AWS models are large, so raise Luau's `LuauTarjanChildLimit` as shown; without it the
checker gives up with "Code is too complex to typecheck". In an editor, set the same in the
luau-lsp settings:

```json
{
  "luau-lsp.types.definitionFiles": { "roto": "types/roto.d.luau" },
  "luau-lsp.fflags.override": { "LuauTarjanChildLimit": "1000000" }
}
```

The definitions work with both Luau type solvers; the new solver
(`"luau-lsp.fflags.enableNewSolver": true`) gives the better diagnostics.

## Typed AWS clients

`roto.aws.<service>.<Operation>(input)` is `roto.call("<service>", "<Operation>", input)`
with checked tables. Services: `cloudformation`, `dynamodb`, `events` (also
`eventbridge`), `iam`, `kinesis`, `kms`, `lambda`, `logs`, `s3`, `secretsmanager`, `sns`,
`sqs`, `ssm` and `sts`.

```lua
--!strict
local queue = roto.sqs.queue("jobs")
roto.aws.sqs.SendMessage({ QueueUrl = queue.url, MessageBody = "{}" })

local url = roto.aws.sqs.GetQueueUrl({ QueueName = "jobs" })
print(url.QueueUrl)            -- string
roto.aws.s3.PutBucketCors({ Bucket = 1 })  -- error: Bucket must be a string, CORSConfiguration is missing
```

`roto.call(service, operation, input)` stays available and untyped.

## Limits of the checker

- Luau tables are open, so an unknown extra field in an option table is not always flagged;
  a missing required field and a wrong value type are.
- AWS enum members are typed as `string`.
- The definition types (`QueueOptions`, `Queue`, ...) cannot be named in your own
  annotations: Luau definition files do not export type aliases. Annotate with
  `typeof(...)` or let inference carry the types.
- Setup-only helpers are not distinguished from runtime-callback code.

## The helper definitions

This is the part of the file that describes the setup helpers, options and callbacks.

```lua
--8<-- "docs/roto.d.luau:helpers"
```
