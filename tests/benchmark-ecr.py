#!/usr/bin/env python3
"""Compare isolated and shared-suite ECR cases with equivalent raw HTTP checks.

All measurements include startup. Standalone cases and HTTP use fresh stores per case.
The suite shares one store and deletes repositories after each case, adding cleanup calls.
Run after `cargo build -p roto-server --locked`: python3 tests/benchmark-ecr.py [repetitions].
"""
import json
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
PREFIX = "AmazonEC2ContainerRegistry_V20150921."


def run_http(check):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen([str(SERVER), "--ephemeral", "--port", str(port), "--lua-services", str(BUNDLE)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 10
        while True:
            if process.poll() is not None:
                raise RuntimeError("server exited before becoming ready")
            try:
                urllib.request.urlopen(url + "/roto-api/health", timeout=0.5).close()
                break
            except (OSError, urllib.error.URLError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("server did not become ready")
                time.sleep(0.001)

        def call(operation, data, expected_error=None):
            request = urllib.request.Request(url, data=json.dumps(data).encode(), method="POST",
                                             headers={"Content-Type": "application/x-amz-json-1.1",
                                                      "X-Amz-Target": PREFIX + operation})
            try:
                response = urllib.request.urlopen(request, timeout=5)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = json.load(response)
                if expected_error:
                    assert response.status >= 400 and body["__type"].split("#")[-1] == expected_error, body
                else:
                    assert response.status == 200, body
                return body
        check(call)
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def crud(call):
    created = call("CreateRepository", {"repositoryName": "team/app"})["repository"]
    assert created["repositoryName"] == "team/app" and created["imageTagMutability"] == "MUTABLE"
    assert created["imageScanningConfiguration"]["scanOnPush"] is False
    assert call("DescribeRepositories", {"repositoryNames": ["team/app"]})["repositories"][0]["repositoryArn"] == created["repositoryArn"]
    assert call("DeleteRepository", {"repositoryName": "team/app"})["repository"]["repositoryName"] == "team/app"
    call("DescribeRepositories", {"repositoryNames": ["team/app"]}, "RepositoryNotFoundException")


def errors(call):
    call("CreateRepository", {"repositoryName": "app"})
    call("CreateRepository", {"repositoryName": "app"}, "RepositoryAlreadyExistsException")
    call("DeleteRepository", {"repositoryName": "missing"}, "RepositoryNotFoundException")
    assert len(call("DescribeRepositories", {})["repositories"]) == 1


def rollback(call):
    call("CreateRepository", {"repositoryName": "app", "imageTagMutability": "IMMUTABLE"})
    call("PutImageTagMutability", {"repositoryName": "app", "imageTagMutability": "MUTABLE_WITH_EXCLUSION"}, "InvalidParameterException")
    assert call("DescribeRepositories", {"repositoryNames": ["app"]})["repositories"][0]["imageTagMutability"] == "IMMUTABLE"


def tags(call):
    arn = call("CreateRepository", {"repositoryName": "app", "tags": [{"Key": "team", "Value": "one"}]})["repository"]["repositoryArn"]
    call("TagResource", {"resourceArn": arn, "tags": [{"Key": "team", "Value": "two"}, {"Key": "env", "Value": "test"}]})
    call("UntagResource", {"resourceArn": arn, "tagKeys": ["env"]})
    assert call("ListTagsForResource", {"resourceArn": arn})["tags"] == [{"Key": "team", "Value": "two"}]


def pages(call):
    for name in ("aa", "bb", "cc"):
        call("CreateRepository", {"repositoryName": name})
    first = call("DescribeRepositories", {"maxResults": 2})
    assert len(first["repositories"]) == 2 and first.get("nextToken")
    second = call("DescribeRepositories", {"maxResults": 2, "nextToken": first["nextToken"]})
    assert len(second["repositories"]) == 1 and not second.get("nextToken")
    assert {repo["repositoryName"] for repo in first["repositories"] + second["repositories"]} == {"aa", "bb", "cc"}


def main():
    repetitions = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    timings = {"Luau standalone": [], "Luau suite": [], "HTTP": []}
    original = ROOT / "tests/luau/ecr.luau"
    with tempfile.TemporaryDirectory() as directory:
        suite = Path(directory) / "ecr_suite.luau"
        cleanup = '''suite.after_each(function()
    for _, repo in test.call("ecr", "DescribeRepositories", {}).repositories do
        test.call("ecr", "DeleteRepository", {repositoryName = repo.repositoryName})
    end
end)
'''
        suite.write_text('test.suite("ECR", function(suite)\n' + cleanup +
                         original.read_text().replace('test.case(', 'suite.case(') + '\nend)\n')
        for _ in range(repetitions):
            for name, path in (("Luau standalone", original), ("Luau suite", suite)):
                start = time.perf_counter()
                result = subprocess.run([str(SERVER), "test", str(path), "--lua-services", str(BUNDLE)],
                                        cwd=ROOT, check=True, capture_output=True, text=True)
                assert "5 passed; 0 failed" in result.stdout, result.stdout
                timings[name].append(time.perf_counter() - start)
            start = time.perf_counter()
            for check in (crud, errors, rollback, tags, pages):
                run_http(check)
            timings["HTTP"].append(time.perf_counter() - start)
    for name, times in timings.items():
        print(f"{name}: median {statistics.median(times):.3f}s; runs " + ", ".join(f"{t:.3f}s" for t in times))
    print(f"Standalone / suite median: {statistics.median(timings['Luau standalone']) / statistics.median(timings['Luau suite']):.2f}x")


if __name__ == "__main__":
    main()
