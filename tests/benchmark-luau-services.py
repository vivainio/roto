#!/usr/bin/env python3
"""Compare ten Luau service cases against equivalent HTTP smoke checks.

All timings include runner/server startup. The HTTP fresh mode starts a new ephemeral
server for each workflow. HTTP shared mode starts one ephemeral server and resets its
service state between workflows; it retains the service runtimes. The Luau runner
creates a fresh in-process service environment per case, plus one for discovery.

Run after `cargo build -p roto-server --locked`:
    python3 tests/benchmark-luau-services.py [repetitions]
"""
import importlib.util
import json
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "target/debug/roto-server"
BUNDLE = ROOT / "services"
CASES = (ROOT / "tests/luau/ecr.luau", ROOT / "tests/luau/ported_services.luau")


def load_smoke():
    spec = importlib.util.spec_from_file_location("smoke_lua_services", ROOT / "tests/smoke-lua-services.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SMOKE = load_smoke()
spec = importlib.util.spec_from_file_location("benchmark_ecr", ROOT / "tests/benchmark-ecr.py")
ECR = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ECR)


def ecr_scenario(check):
    def scenario(server):
        def call(op, data, expected_error=None):
            return server.call("ecr", op, data, status=400 if expected_error else 200, error=expected_error)
        check(call)
    return scenario


SCENARIOS = tuple(ecr_scenario(check) for check in (ECR.crud, ECR.errors, ECR.rollback, ECR.tags, ECR.pages)) + (
    SMOKE.cur, SMOKE.acm, SMOKE.config, SMOKE.budgets, SMOKE.cloudmap)


class HttpServer:
    def __init__(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        self.calls = 0
        self.process = subprocess.Popen([str(SERVER), "--ephemeral", "--port", str(port), "--lua-services", str(BUNDLE)],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        try:
            while True:
                if self.process.poll() is not None:
                    raise RuntimeError("server exited before becoming ready")
                try:
                    urllib.request.urlopen(self.url + "/roto-api/health", timeout=0.5).close()
                    break
                except (OSError, urllib.error.URLError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError("server did not become ready")
                    time.sleep(0.001)
        except BaseException:
            self.close()
            raise

    def call(self, service, op, data, status=200, error=None, account=SMOKE.ACCOUNT, region="us-east-1", unsigned=False):
        self.calls += 1
        headers = {"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": f"{SMOKE.TARGETS[service]}.{op}"}
        if not unsigned:
            headers["Authorization"] = f"AWS4-HMAC-SHA256 Credential={account}/20261010/{region}/{service}/aws4_request, SignedHeaders=host, Signature=fake"
        request = urllib.request.Request(self.url, data=json.dumps(data).encode(), headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            value = json.load(response)
            assert response.status == status, (service, op, response.status, value)
            if error:
                assert value["__type"].split("#")[-1] == error, (op, value)
            return value

    def reset(self):
        request = urllib.request.Request(self.url + "/roto-api/reset", data=b"{}")
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 200

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


def luau():
    result = subprocess.run([str(SERVER), "test", *map(str, CASES), "--lua-services", str(BUNDLE)],
                            cwd=ROOT, check=True, capture_output=True, text=True)
    assert f"{len(SCENARIOS)} passed; 0 failed" in result.stdout, result.stdout
    return None


def http_fresh():
    counts = []
    for scenario in SCENARIOS:
        server = HttpServer()
        try:
            scenario(server)
            counts.append(server.calls)
        finally:
            server.close()
    return counts


def http_shared():
    server = HttpServer()
    try:
        counts = []
        for index, scenario in enumerate(SCENARIOS):
            if index:
                server.reset()
            before = server.calls
            scenario(server)
            counts.append(server.calls - before)
        return counts
    finally:
        server.close()


def luau_call_counts():
    # Instrument an unmeasured copy, leaving the checked-in test and timed runs untouched.
    # A deliberate error after each successful case lets the driver return its count.
    prefix = '''local calls = 0
local originalTry = test.try_call
local originalCase = test.case
test.try_call = function(...) calls += 1; return originalTry(...) end
test.case = function(name, run)
    originalCase(name, function() run(); error("BENCH_CALL_COUNT:" .. calls) end)
end
'''
    with tempfile.TemporaryDirectory() as directory:
        paths = [Path(directory) / path.name for path in CASES]
        for source, path in zip(CASES, paths):
            path.write_text(prefix + source.read_text())
        result = subprocess.run([str(SERVER), "test", *map(str, paths), "--lua-services", str(BUNDLE)],
                                cwd=ROOT, capture_output=True, text=True)
    counts = [int(value) for value in re.findall(r"BENCH_CALL_COUNT:(\d+)", result.stderr)]
    assert result.returncode != 0 and len(counts) == len(SCENARIOS), result.stderr
    return counts


def main():
    repetitions = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    modes = [("Luau fresh environments", luau), ("HTTP fresh servers", http_fresh), ("HTTP shared server + reset", http_shared)]
    expected_counts = luau_call_counts()
    for name, workload in modes:
        counts = workload()  # warm caches and validate the scenarios before measuring
        if counts is not None:
            assert counts == expected_counts, (name, counts, expected_counts)
    timings = {name: [] for name, _ in modes}
    for iteration in range(repetitions):
        # Rotate measurement order to avoid always giving one mode the same position.
        for offset in range(len(modes)):
            name, workload = modes[(iteration + offset) % len(modes)]
            start = time.perf_counter()
            counts = workload()
            timings[name].append(time.perf_counter() - start)
            if counts is not None:
                assert counts == expected_counts, (name, counts, expected_counts)
    print(f"{len(SCENARIOS)} cases; {sum(expected_counts)} service calls per run in each mode; {repetitions} measured repetitions")
    for name, times in timings.items():
        print(f"{name}: median {statistics.median(times):.3f}s; runs " + ", ".join(f"{t:.3f}s" for t in times))
    baseline = statistics.median(timings["Luau fresh environments"])
    for name in ("HTTP fresh servers", "HTTP shared server + reset"):
        print(f"{name} / Luau: {statistics.median(timings[name]) / baseline:.2f}x")


if __name__ == "__main__":
    main()
