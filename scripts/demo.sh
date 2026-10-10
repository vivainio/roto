#!/usr/bin/env bash
# Usage: scripts/demo.sh [port]; Ctrl-C stops the disposable demo.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
port="${1:-5071}"
cargo build -q --locked --manifest-path "$root/Cargo.toml" -p roto-server
exec "$root/target/debug/roto-server" --ephemeral --port "$port" --setup "$root/examples/demo/setup.lua"
