#!/usr/bin/env bash
# Regenerate service code from the botocore models in models/.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
for svc in sts sqs iam s3 dynamodb; do
  cargo run -q --manifest-path "$root/Cargo.toml" -p roto-codegen -- \
    "$root/models/$svc/service-2.json" "$root/crates/roto-svc-$svc/src/generated.rs"
  rustfmt --edition 2024 "$root/crates/roto-svc-$svc/src/generated.rs"
done
