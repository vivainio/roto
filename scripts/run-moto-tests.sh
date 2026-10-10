#!/usr/bin/env bash
# Run vendored moto tests against a locally built roto in moto "server mode".
# Usage: scripts/run-moto-tests.sh test_sts [extra pytest args]
# Tests listed in tests/moto/expected_failures/<service>.txt (one node id per line) are
# deselected; the list shrinks as roto gains coverage.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
svc="${1:?service dir, e.g. test_sts}"; shift || true
port="${ROTO_TEST_PORT:-5070}"
venv="${ROTO_MOTO_VENV:-$root/.venv-moto}"
if [[ ! -x "$venv/bin/python" ]]; then
  python3 -m venv "$venv" && "$venv/bin/pip" install -q -r "$root/tests/moto/requirements.txt"
fi
cargo build -q --manifest-path "$root/Cargo.toml" -p roto-server
data="$(mktemp -d)"
if [[ "${TARGET:-roto}" == moto ]]; then
  # Reference run: the same tests against real moto, to tell upstream quirks from roto gaps.
  "$venv/bin/moto_server" -p "$port" >"$data/server.log" 2>&1 &
else
  "$root/target/debug/roto-server" --port "$port" --ephemeral >"$data/server.log" 2>&1 &
fi
pid=$!
trap 'kill $pid 2>/dev/null || true; wait $pid 2>/dev/null || true; rm -rf "$data"' EXIT
health_path="/roto-api/health"
[[ "${TARGET:-roto}" == moto ]] && health_path="/"
ready=0
for _ in $(seq 100); do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  if curl --fail -s -o /dev/null "http://localhost:$port$health_path"; then ready=1; break; fi
  sleep 0.1
done
if [[ "$ready" != 1 ]]; then cat "$data/server.log"; exit 1; fi
xf="$root/tests/moto/expected_failures/$svc.txt"
ignore=()
[[ -f "$root/tests/moto/not_portable.txt" ]] && while IFS= read -r l; do [[ -n "$l" ]] && ignore+=(--ignore "tests/$l"); done <"$root/tests/moto/not_portable.txt"
# Adapt upstream endpoints and Lambda's permissive IAM fixture in a temporary copy.
cp -R "$root/tests/moto/tests" "$data/tests"
cp "$root/tests/moto/pytest.ini" "$data/pytest.ini"
"$venv/bin/python" - "$data/tests" "$port" <<'PYPORT'
from pathlib import Path
import json
import sys
for path in Path(sys.argv[1]).rglob("*.py"):
    source = path.read_text()
    adapted = source.replace("localhost:5000", f"localhost:{sys.argv[2]}")
    if path.as_posix().endswith("test_awslambda/utilities.py"):
        # Moto accepts this non-JSON placeholder; roto validates policy documents.
        # The helper retries forever on rejection, obscuring every Lambda API test.
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                           "Action": "sts:AssumeRole"}],
        })
        adapted = adapted.replace(
            'AssumeRolePolicyDocument="some policy"',
            f"AssumeRolePolicyDocument={policy!r}",
        )
    if path.as_posix().endswith("test_events/test_events_integration.py"):
        # Roto delivers through a background outbox; wait for the message while
        # retaining upstream's assertions about the delivered event.
        adapted = adapted.replace(
            "client_sqs.receive_message(QueueUrl=queue_url)",
            "client_sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)",
        )
    if path.as_posix().endswith("test_dynamodb/test_dynamodb_cloudformation.py"):
        # These old Moto fixtures contain invalid index schemas and assume
        # optional DescribeTable fields are always present (and timestamps absent).
        adapted = adapted.replace(
            'template["Resources"]["table"]["Properties"]["GlobalSecondaryIndexes"] = [',
            'template["Resources"]["table"]["Properties"]["AttributeDefinitions"] += '
            '[{"AttributeName": "gsipk", "AttributeType": "S"}, '
            '{"AttributeName": "lsipk", "AttributeType": "S"}]\n'
            '    template["Resources"]["table"]["Properties"]["GlobalSecondaryIndexes"] = [',
        ).replace(
            '{"AttributeName": "gsipk", "KeyType": "S"}',
            '{"AttributeName": "gsipk", "KeyType": "HASH"}',
        ).replace(
            '{"AttributeName": "lsipk", "KeyType": "S"}',
            '{"AttributeName": "Name", "KeyType": "HASH"}, '
            '{"AttributeName": "lsipk", "KeyType": "RANGE"}',
        ).replace(
            'assert table["BillingModeSummary"] == {"BillingMode": "PAY_PER_REQUEST"}',
            'assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"',
        ).replace(
            'assert table["BillingModeSummary"] == {"BillingMode": "PROVISIONED"}',
            'assert table.get("BillingModeSummary", {"BillingMode": "PROVISIONED"})'
            '["BillingMode"] == "PROVISIONED"',
        ).replace('table["LocalSecondaryIndexes"] == []', 'table.get("LocalSecondaryIndexes", []) == []'
        ).replace('table["GlobalSecondaryIndexes"] == []', 'table.get("GlobalSecondaryIndexes", []) == []')
    if adapted != source:
        path.write_text(adapted)
PYPORT
cd "$data"
if [[ "${AUDIT_EXPECTED:-}" == 1 ]]; then
  TEST_SERVER_MODE=true TEST_SERVER_MODE_ENDPOINT="http://localhost:$port" \
    AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing TESTS_SKIP_REQUIRES_DOCKER=1 \
    "$venv/bin/python" "$root/scripts/audit-moto-tests.py" "$xf" \
    --timeout="${TEST_TIMEOUT:-30}" ${ignore[@]+"${ignore[@]}"} "$@"
  exit $?
fi
if [[ "${UPDATE_EXPECTED:-}" == 1 ]]; then
  # Collection/usage errors must not overwrite the existing baseline.
  TEST_SERVER_MODE=true TEST_SERVER_MODE_ENDPOINT="http://localhost:$port" \
    AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing TESTS_SKIP_REQUIRES_DOCKER=1 \
    "$venv/bin/python" "$root/scripts/audit-moto-tests.py" --update "$xf" \
    --timeout="${TEST_TIMEOUT:-30}" ${ignore[@]+"${ignore[@]}"} "$@"
  exit $?

fi
TEST_SERVER_MODE=true TEST_SERVER_MODE_ENDPOINT="http://localhost:$port" \
  AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing TESTS_SKIP_REQUIRES_DOCKER=1 \
  "$venv/bin/python" "$root/scripts/audit-moto-tests.py" --run "$xf" --timeout="${TEST_TIMEOUT:-30}" ${ignore[@]+"${ignore[@]}"} "$@"
