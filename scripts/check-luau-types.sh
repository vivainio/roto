#!/bin/sh
# Type-checks the Lua examples against the generated Luau definitions (needs luau-lsp on PATH).
set -eu
cd "$(dirname "$0")/.."
exec cargo run -q -p roto-server --locked -- --check examples/*/setup.luau demos/*/setup.luau
