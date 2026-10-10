# S3 Moto HTTP timeout handover

Recorded 2026-10-10 after the service Diesel conversion (`40c736a`). The bug is
still unresolved. It also reproduces with the pre-port S3 implementation;
do not treat it as a Diesel regression or add it to expected failures to hide it.

## Reproduce

From the repository root, use a free port and a distinct trace filename:

```sh
mkdir -p api-traces
ROTO_TEST_PORT=5073 \
  ROTO_TRACE_FILE="$PWD/api-traces/s3-timeout-focused.jsonl" \
  scripts/run-moto-tests.sh test_s3 -k test_list_object_versions_with_delimiter
```

The selector also matches another passing test. The failing node is:

```text
tests/test_s3/test_s3_list_object_versions.py::test_list_object_versions_with_delimiter
```

For the full baseline:

```sh
ROTO_TEST_PORT=5073 \
  ROTO_TRACE_FILE="$PWD/api-traces/s3-timeout-full.jsonl" \
  scripts/run-moto-tests.sh test_s3
```

The runner builds the server and starts a fresh ephemeral store. It truncates the
selected trace file. Its temporary server log is removed on exit; preserve that
log or launch the server separately if investigating wire behavior.

## Observed failure

The test alternates nonempty PUTs (`Body=b"data-1"`, etc.) and empty PUTs
(`Body=b""`) on one boto3 client with bucket versioning enabled. It fails during
setup at a nonempty `put_object`, before the version-listing assertions.

The client waits in `socket.recv_into` until pytest-timeout's 30-second limit.
Its captured log reports:

```text
MissingHeaderBodySeparatorDefect()
urllib3.exceptions.HeaderParsingError
unparsed data: 'HTTP/1.1 200 OK\r\n...content-length: 0\r\n...\r\n\r\n'
```

A complete final response status line appears inside what the client is parsing
as headers. The server trace records the affected PUT as successful (HTTP 200,
duration 0 ms); this points toward response parsing/framing rather than a blocked
storage transaction, but the root cause has not been established.

Two full post-port runs each produced **1 failed, 240 passed, 59 skipped,
127 deselected, 2 xfailed**. A focused comparison temporarily restored the
tracked S3 files from `7a13ed6` (before the S3 port), leaving the other services
converted, and produced **1 failed, 1 passed, 427 deselected**, with the same
timeout and header-parsing symptom. The converted files were restored afterward.
All 15 native S3 tests, service Clippy, workspace tests and demo smoke passed.

## Local environment and evidence

The reproducing environment was macOS with Python 3.9.6 in `.venv-moto`:

| Package | Version |
| --- | --- |
| boto3 / botocore | 1.42.97 |
| urllib3 | 1.26.20 |
| Moto | 5.0.11 |
| pytest | 8.4.2 |
| pytest-timeout | 2.4.0 |

Artifacts from this session (local only, not committed):

- `/tmp/s3-diesel-moto.log` and `/tmp/s3-diesel-moto-repeat.log`: full failures.
- `/tmp/s3-before-port-moto.log`: focused pre-port comparison.
- `api-traces/s3-diesel.jsonl` and `api-traces/s3-diesel-repeat.jsonl`: full traces.
- `api-traces/s3-before-port.jsonl`: focused pre-port trace.

Temporary files may disappear; the commands above regenerate the evidence.

## Next investigation

1. Reduce to repeated alternating empty/nonempty PUTs on one persistent boto3
   connection. Capture client debug output and raw HTTP traffic, especially the
   response to an empty PUT and the following nonempty PUT.
2. Inspect `Expect: 100-continue` handling. Botocore's
   `.venv-moto/lib/python3.9/site-packages/botocore/awsrequest.py` implements
   `_send_output`, `_handle_expect_response`, `_consume_headers`, and a custom
   `AWSHTTPResponse`. It changes `response_class` after an early final response;
   check whether that state survives into a later request. This is a hypothesis,
   not a confirmed cause.
3. Compare with a fresh connection per request, disabling `Expect` as a diagnostic,
   and a current supported Python environment. Record the package versions for
   each comparison. Do not silently change the runner or baseline exclusions.
4. On the server side, `crates/roto-server/src/main.rs` uses `axum::serve`, reads
   the request body in `handle`, and constructs responses in `into_response`.
   Check informational-response ordering and empty-body framing at that boundary.
5. Once the cause is established, add a focused regression at the responsible
   layer, then rerun the focused test, full S3 Moto suite and demo smoke. Keep the
   original version-listing assertions intact.
