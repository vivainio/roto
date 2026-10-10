#!/usr/bin/env bash
# Vendor moto's test suite for the given services into tests/moto/ at a pinned tag.
# Usage: scripts/sync-moto-tests.sh test_sts test_sqs ...
set -euo pipefail
MOTO_TAG="${MOTO_TAG:-5.0.11}"
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"   # downloaded content is untrusted: never execute anything from it
trap 'rm -rf "$work"' EXIT
git clone -q --depth 1 --branch "$MOTO_TAG" https://github.com/getmoto/moto.git "$work/moto"
top="$root/tests/moto"        # NOT a package: a dir named moto with __init__.py would shadow real moto
dest="$top/tests"           # upstream layout, so `from tests import ...` keeps working
mkdir -p "$dest"
cp "$work/moto/LICENSE" "$top/LICENSE-moto"
cp "$work/moto/tests/__init__.py" "$work/moto/tests/markers.py" "$dest/"
for svc in "$@"; do
  rm -rf "$dest/$svc"
  cp -r "$work/moto/tests/$svc" "$dest/$svc"
  find "$dest/$svc" -name __pycache__ -prune -exec rm -rf {} +
done
# Lambda's main suite imports this pure manifest fixture; no ECR tests are collected.
if [[ -d "$dest/test_awslambda" ]]; then
  mkdir -p "$dest/test_ecr"
  cp "$work/moto/tests/test_ecr/__init__.py" "$work/moto/tests/test_ecr/test_ecr_helpers.py" "$dest/test_ecr/"
fi
echo "$MOTO_TAG" > "$top/MOTO_VERSION"
# Tests that drive moto's Python internals in-process, or import another service's tests,
# cannot be run against a different server.
: >"$top/not_portable.txt"
for svc_dir in "$dest"/test_*; do
  svc="$(basename "$svc_dir")"
  (cd "$dest" && {
    # Only modules that import the in-process server are unusable as a whole; single tests that poke
    # at backends fail individually and are tracked in expected_failures.
    grep -rlE '^(from|import) moto(\.server| import server)|create_backend_app|ThreadedMotoServer' --include='*.py' "$svc" || true
    grep -rE "^(from|import) tests\.test_" --include='*.py' "$svc" | grep -vE "tests\.${svc}\b|tests\.test_ecr\.test_ecr_helpers" | cut -d: -f1 || true
  }) >>"$top/not_portable.txt"
done
if [[ -d "$dest/test_awslambda" ]]; then
  # This module tests moto's Python Policy class, not a server API.
  echo "test_awslambda/test_policy.py" >>"$top/not_portable.txt"
fi
sort -u "$top/not_portable.txt" -o "$top/not_portable.txt"
echo "synced: $* (moto $MOTO_TAG)"
