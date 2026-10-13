# Lua type stubs

Roto publishes type stubs for its setup Lua API, so editors that use
[lua-language-server](https://luals.github.io/) (VS Code, Neovim, Zed and others) can
complete and check `roto.*` calls. They describe the option tables of the helpers, the
shapes they return, and the callback signatures. Nothing is checked at runtime by the
stubs; the server still rejects unknown options itself.

**Download: [roto.d.lua](roto.d.lua)**. The file is generated from the server's source by
`roto-server --lua-stubs` each time the book is built, so it always matches this version.

## Setup

Save the file somewhere in your project, for example `types/roto.d.lua`, and point
`.luarc.json` at the folder:

```json
{
  "workspace.library": ["types"],
  "diagnostics.globals": ["roto"]
}
```

Then a call such as `roto.s3.bucket("assets", { colors = {} })` is flagged while you
type, and hovering a function shows its options and return table.

## `roto.call`

For `s3`, `sns`, `iam`, `sts` and `cloudformation` every operation and its input and
output tables are typed from the AWS service models, so `roto.call("s3", "PutBucketCors", {...})`
completes the members of the request and flags unknown operations. For the other services
`roto.call` takes any table; use the [AWS API reference](https://docs.aws.amazon.com/) for
the members of each operation. These generated declarations are several thousand lines
and are only in the download.

## The helper stubs

This is the part of the file that describes the setup helpers, options and callbacks.

```lua
--8<-- "docs/roto.d.lua:helpers"
```
