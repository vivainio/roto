#!/usr/bin/env bash
# Run vendored moto tests against a locally built roto in moto "server mode".
# Usage: scripts/run-moto-tests.sh test_sts [extra pytest args]
# Tests listed in tests/moto/expected_failures/<service>.txt (one node id per line) are
# deselected; the list shrinks as roto gains coverage.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
svc="${1:?service dir, e.g. test_sts}"; shift || true
port="${ROTO_TEST_PORT:-5070}"
venv="$root/.venv-moto"
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
trap 'kill $pid 2>/dev/null; rm -rf "$data"' EXIT
health_path="/roto-api/health"
[[ "${TARGET:-roto}" == moto ]] && health_path="/"
for _ in $(seq 100); do curl --fail -s -o /dev/null "http://localhost:$port$health_path" && break; sleep 0.1; done
deselect=()
xf="$root/tests/moto/expected_failures/$svc.txt"
[[ -f "$xf" ]] && while IFS= read -r l; do [[ -n "$l" && "$l" != \#* ]] && deselect+=(--deselect "$l"); done <"$xf"
ignore=()
[[ -f "$root/tests/moto/not_portable.txt" ]] && while IFS= read -r l; do [[ -n "$l" ]] && ignore+=(--ignore "tests/$l"); done <"$root/tests/moto/not_portable.txt"
# Adapt hardcoded upstream endpoint URLs without modifying vendored sources.
cp -R "$root/tests/moto/tests" "$data/tests"
cp "$root/tests/moto/pytest.ini" "$data/pytest.ini"
"$venv/bin/python" - "$data/tests" "$port" <<'PYPORT'
from pathlib import Path
import sys
for path in Path(sys.argv[1]).rglob("*.py"):
    source = path.read_text()
    adapted = source.replace("localhost:5000", f"localhost:{sys.argv[2]}")
    if adapted != source:
        path.write_text(adapted)
PYPORT
cd "$data"
if [[ "${UPDATE_EXPECTED:-}" == 1 ]]; then
  # Re-baseline: record every currently failing test as an expected failure.
  mkdir -p "$(dirname "$xf")"
  out="$(TEST_SERVER_MODE=true TEST_SERVER_MODE_ENDPOINT="http://localhost:$port" AWS_ACCESS_KEY_ID=testing \
    AWS_SECRET_ACCESS_KEY=testing "$venv/bin/python" -m pytest "tests/$svc" -q -p no:cacheprovider --timeout="${TEST_TIMEOUT:-30}" \
    --tb=no -rfE "${ignore[@]}" || true)"
  { echo "# Known failures for $svc (regenerate: UPDATE_EXPECTED=1 scripts/run-moto-tests.sh $svc)"
    sed -nE 's/^(FAILED|ERROR) (\S+).*/\2/p' <<<"$out" | sed 's/^tests\///' | sed 's/^/tests\//' | sort -u; } >"$xf"
  tail -1 <<<"$out"; echo "wrote $xf"; exit 0
fi
TEST_SERVER_MODE=true TEST_SERVER_MODE_ENDPOINT="http://localhost:$port" \
  AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing \
  "$venv/bin/python" -m pytest "tests/$svc" -q -p no:cacheprovider --timeout="${TEST_TIMEOUT:-30}" "${deselect[@]}" "${ignore[@]}" "$@"
