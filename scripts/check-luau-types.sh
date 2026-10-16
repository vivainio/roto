#!/bin/sh
# Type-checks the Lua examples against the generated Luau definitions (needs luau-lsp on PATH).
set -eu
cd "$(dirname "$0")/.."
types=$(mktemp -d)
trap 'rm -rf "$types"' EXIT
cargo run -q -p roto-server --locked -- --lua-stubs "$types"
luau-lsp analyze --platform=standard  --flag:LuauSolverV2=true --definitions="$types/roto.d.luau" \
  examples/demo/setup.lua examples/lua/setup.lua
