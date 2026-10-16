#!/usr/bin/env python3
"""Measure a local Roto binary with disposable state; Python standard library only."""
import argparse
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
MODES = ("ephemeral", "persistent", "seeded")
STS_BODY = b"Action=GetCallerIdentity&Version=2011-06-15"
STS_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
# Roto uses the credential scope for routing and does not validate signatures.
# These are dummy credentials, not a signing-cost benchmark.
S3_HEADERS = {"Authorization": "AWS4-HMAC-SHA256 Credential=benchmark/"
              + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
              + "/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=" + "0" * 64}
PAYLOAD = b"x" * 1024
BUCKET = "/roto-benchmark"
OBJECT = BUCKET + "/payload/data.bin"
DYNAMODB_TABLE = "roto-benchmark"
DYNAMODB_ITEM = {"id": {"S": "benchmark"}, "payload": {"S": "x" * 1024}}


def dynamodb_request(operation, body):
    return (json.dumps(body).encode(), {"Content-Type": "application/x-amz-json-1.0",
                                       "X-Amz-Target": "DynamoDB_20120810." + operation,
                                       "Authorization": S3_HEADERS["Authorization"].replace("/s3/", "/dynamodb/")})



def command_output(*args):
    result = subprocess.run(args, capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def summary(values):
    ordered = sorted(values)
    return {"min": min(values), "median": statistics.median(values),
            "p95": ordered[math.ceil(len(ordered) * .95) - 1], "max": max(values)}


def rss_bytes(pid):
    if platform.system() == "Linux":
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        raise RuntimeError("process has no VmRSS")
    value = command_output("ps", "-o", "rss=", "-p", str(pid))
    if not value:
        raise RuntimeError(f"cannot read RSS for process {pid}")
    return int(value) * 1024


def request(conn, method, path, body=None, headers=None, expected=None):
    conn.request(method, path, body=body, headers=headers or {})
    response = conn.getresponse()
    data = response.read()
    if not 200 <= response.status < 300:
        raise RuntimeError(f"{method} {path}: HTTP {response.status}: {data[:500]!r}")
    valid = expected(data) if callable(expected) else expected is None or expected in data
    if not valid:
        raise RuntimeError(f"{method} {path}: unexpected response {data[:500]!r}")
    return data


def wait_ready(proc, port, timeout):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with status {proc.returncode}")
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=.2)
        try:
            request(conn, "POST", "/", STS_BODY, STS_HEADERS, b"<Account>123456789012</Account>")
            return
        except (OSError, http.client.HTTPException):
            time.sleep(.001)
        finally:
            conn.close()
    raise TimeoutError(f"server did not answer STS within {timeout}s")


def measure_requests(port, count, warmup):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        request(conn, "PUT", BUCKET, headers=S3_HEADERS)
        request(conn, "PUT", OBJECT, PAYLOAD, S3_HEADERS)
        create_body, create_headers = dynamodb_request("CreateTable", {
            "TableName": DYNAMODB_TABLE,
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        })
        request(conn, "POST", "/", create_body, create_headers)
        put_body, put_headers = dynamodb_request("PutItem", {
            "TableName": DYNAMODB_TABLE, "Item": DYNAMODB_ITEM,
        })
        get_body, get_headers = dynamodb_request("GetItem", {
            "TableName": DYNAMODB_TABLE, "Key": {"id": {"S": "benchmark"}}, "ConsistentRead": True,
        })
        request(conn, "POST", "/", put_body, put_headers)
        cases = [
            ("sts_get_caller_identity", "POST", "/", STS_BODY, STS_HEADERS, b"<Account>123456789012</Account>"),
            ("s3_put_object_1kib", "PUT", OBJECT, PAYLOAD, S3_HEADERS, None),
            ("s3_get_object_1kib", "GET", OBJECT, None, S3_HEADERS, PAYLOAD),
            ("dynamodb_put_item_1kib", "POST", "/", put_body, put_headers, None),
            ("dynamodb_get_item_1kib", "POST", "/", get_body, get_headers,
             lambda data: json.loads(data).get("Item") == DYNAMODB_ITEM),
        ]
        measurements = {}
        for name, method, path, body, headers, expected in cases:
            for _ in range(warmup):
                request(conn, method, path, body, headers, expected)
            samples = []
            started = time.perf_counter()
            for _ in range(count):
                before = time.perf_counter_ns()
                request(conn, method, path, body, headers, expected)
                samples.append((time.perf_counter_ns() - before) / 1_000_000)
            elapsed = time.perf_counter() - started
            measurements[name] = {"latency_ms": summary(samples),
                                  "requests_per_second": count / elapsed,
                                  "samples_ms": samples}
        return measurements
    finally:
        conn.close()


def measure_run(binary, mode, args):
    with tempfile.TemporaryDirectory(prefix="roto-benchmark-") as temp:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        argv = [str(binary), "--host", "127.0.0.1", "--port", str(port),
                "--account-id", "123456789012", "--data-dir", str(Path(temp) / "state")]
        if mode != "persistent":
            argv.append("--ephemeral")
        if mode == "seeded":
            argv.extend(["--setup", str(ROOT / "examples/demo/setup.lua")])
        env = dict(os.environ, RUST_LOG="warn")
        with tempfile.TemporaryFile(mode="w+b") as logs:
            started = time.perf_counter_ns()
            proc = subprocess.Popen(argv, stdout=logs, stderr=logs, env=env)
            try:
                wait_ready(proc, port, args.timeout)
                startup_ms = (time.perf_counter_ns() - started) / 1_000_000
                # Let worker startup and demo deliveries finish before idle sampling.
                time.sleep(args.settle_seconds)
                memory = []
                for index in range(args.memory_samples):
                    if proc.poll() is not None:
                        raise RuntimeError("server exited during idle sampling")
                    memory.append(rss_bytes(proc.pid))
                    if index + 1 < args.memory_samples:
                        time.sleep(args.memory_interval)
                workloads = measure_requests(port, args.requests, args.warmup)
                return {"startup_ms": startup_ms, "idle_rss_bytes": memory, "requests": workloads}
            except Exception as error:
                logs.seek(0)
                raise RuntimeError(f"{mode}: {error}\n{logs.read().decode(errors='replace')[-4000:]}") from error
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/roto-server")
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--settle-seconds", type=float, default=2)
    parser.add_argument("--memory-samples", type=int, default=5)
    parser.add_argument("--memory-interval", type=float, default=.1)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results.json"))
    args = parser.parse_args()
    args.modes = list(dict.fromkeys(args.modes))
    if platform.system() not in ("Darwin", "Linux"):
        parser.error("RSS measurement currently supports macOS and Linux")
    if min(args.runs, args.requests, args.memory_samples) < 1 or args.warmup < 0:
        parser.error("runs, requests, and memory-samples must be positive; warmup must be nonnegative")
    if min(args.settle_seconds, args.memory_interval) < 0 or args.timeout <= 0:
        parser.error("delays must be nonnegative and timeout must be positive")
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"missing binary: {binary}; run cargo build -p roto-server --release --locked")
    results = {"schema_version": 1, "measured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "environment": {"os": platform.platform(), "architecture": platform.machine(),
                               "cpu": command_output("sysctl", "-n", "machdep.cpu.brand_string") if platform.system() == "Darwin" else platform.processor(),
                               "ram_bytes": command_output("sysctl", "-n", "hw.memsize") if platform.system() == "Darwin" else None,
                               "python": platform.python_version(), "rustc": command_output("rustc", "--version"),
                               "revision": command_output("git", "-C", str(ROOT), "rev-parse", "HEAD"),
                               "working_tree_dirty": bool(command_output("git", "-C", str(ROOT), "status", "--porcelain")),
                               "binary_bytes": binary.stat().st_size, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()},
               "settings": {key: value for key, value in vars(args).items() if key not in ("binary", "output")},
               "modes": {mode: {"runs": []} for mode in args.modes}}
    # Interleave modes to reduce bias from a changing machine load.
    for index in range(args.runs):
        for mode in args.modes:
            run = measure_run(binary, mode, args)
            results["modes"][mode]["runs"].append(run)
            print(f"{mode:10s} {index + 1}/{args.runs}: {run['startup_ms']:.1f} ms startup, "
                  f"{statistics.median(run['idle_rss_bytes']) / 1048576:.1f} MiB idle RSS", flush=True)
    for data in results["modes"].values():
        data["startup_ms"] = summary([run["startup_ms"] for run in data["runs"]])
        data["idle_rss_mib"] = summary([statistics.median(run["idle_rss_bytes"]) / 1048576 for run in data["runs"]])
        data["requests"] = {}
        for name in data["runs"][0]["requests"]:
            samples = [sample for run in data["runs"] for sample in run["requests"][name]["samples_ms"]]
            data["requests"][name] = {"latency_ms": summary(samples), "median_requests_per_second":
                                     statistics.median(run["requests"][name]["requests_per_second"] for run in data["runs"])}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    print("\n| Mode | Startup median / p95 (ms) | Idle RSS median (MiB) |")
    print("| --- | ---: | ---: |")
    for mode, data in results["modes"].items():
        print(f"| {mode} | {data['startup_ms']['median']:.1f} / {data['startup_ms']['p95']:.1f} | {data['idle_rss_mib']['median']:.1f} |")
    print(f"\nRaw measurements: {args.output}")


if __name__ == "__main__":
    main()
