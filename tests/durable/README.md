# Durable Lambda SDK tests

Tests use Python 3.14, as do the other Python test jobs. From the repository root:

```sh
cargo build -p roto-server --locked
uv venv --python 3.14 /tmp/roto-durable-tests
uv pip install --python /tmp/roto-durable-tests/bin/python -r tests/durable/requirements.txt
/tmp/roto-durable-tests/bin/python -m maturin build --profile dev --locked --out dist
uv pip install --python /tmp/roto-durable-tests/bin/python --no-deps dist/*.whl
/tmp/roto-durable-tests/bin/python -m pytest -q tests/durable
```

`handler.py` uses the real AWS durable execution SDK for a named step, a
five-second durable wait, and a second named step. It optionally fails the
first step to exercise retries and terminal errors. The audit file records
handler/step entries to prove replay occurs without repeating a completed
step body; it is test instrumentation, not workflow state.

- `test_roto.py` starts disposable Roto servers and runs the handler through
  the installed `roto_aws.lambda_runtime` module and Roto's Runtime API. The real
  execution SDK sends checkpoints to Roto through boto3. No AWS testing runner
  or in-memory service replacement participates in these integration tests.
  They cover synchronous results, suspension, replay, persisted checkpoints
  across server restart, named-start idempotency, step retries, terminal errors,
  cancellation, and execution timeouts. Clock advancement makes long waits fast.
- `test_handler.py` separately validates the fixture against AWS's reference
  local testing runner with virtual time.
- `test_runtime.py` checks the installed adapter's context, logs, warm process,
  handler/import errors, serialization errors, and timeout handling.

The Moto runner also requires Python 3.14 and rejects older existing virtual
environments. To migrate an old `.venv-moto`, move it aside, create a new one
with `python3.14 -m venv .venv-moto`, and install
`tests/moto/requirements.txt` with its Python's `-m pip install -r` option.
