#!/bin/sh
# Type-checks the Luau setup scripts and the service modules that have opted in with `--!strict`
# (needs luau-lsp on PATH).
set -eu
cd "$(dirname "$0")/.."
cargo run -q -p roto-server --locked -- --check examples/*/setup.luau demos/*/setup.luau
strict=$(grep -l '^--!strict' services/common.luau services/*/service.luau || true)
# shellcheck disable=SC2086
exec cargo run -q -p roto-server --locked -- --check-services $strict
