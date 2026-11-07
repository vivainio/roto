"""Exercise the installed Python adapter against Roto's real Runtime API."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from test_roto import Server


APP = '''
import sys
import time
count = 0

def handler(event, context):
    global count
    count += 1
    print("handler stdout", flush=True)
    print("handler stderr", file=sys.stderr, flush=True)
    context.log("context log")
    if event.get("fail"):
        raise ValueError("intentional failure")
    if event.get("invalid"):
        return {1, 2}
    if event.get("sleep"):
        time.sleep(event["sleep"])
    return {
        "count": count,
        "name": context.function_name,
        "version": context.function_version,
        "memory": context.memory_limit_in_mb,
        "arn": context.invoked_function_arn,
        "request_id": context.aws_request_id,
        "remaining": context.get_remaining_time_in_millis(),
        "client": context.client_context.custom if context.client_context else None,
    }
'''


@pytest.fixture
def runtime(tmp_path):
    (tmp_path / "app.py").write_text(APP)
    server = Server(tmp_path, handler_name="app.handler", handler_cwd=tmp_path)
    try:
        server.start()
        server.client.create_function(
            FunctionName="durable-test", Runtime="provided.al2023", Handler="app.handler",
            Role="arn:aws:iam::123456789012:role/local", Code={"ZipFile": b""},
            Timeout=2, MemorySize=256,
        )
        yield server
    finally:
        server.stop()


def invoke(server, event, **options):
    response = server.client.invoke(
        FunctionName="durable-test", Payload=json.dumps(event).encode(), **options,
    )
    return response, json.load(response["Payload"])


def test_context_logs_and_warm_process_survives_handler_errors(runtime):
    client_context = base64.b64encode(json.dumps({"custom": {"hello": "world"}}).encode()).decode()
    response, result = invoke(runtime, {}, ClientContext=client_context, LogType="Tail")
    assert "FunctionError" not in response, result
    assert result["count"] == 1
    assert result["name"] == "durable-test"
    assert result["version"] == "$LATEST"
    assert result["memory"] == "256"
    assert result["arn"].endswith(":function:durable-test")
    assert result["request_id"]
    assert 0 < result["remaining"] <= 2000
    assert result["client"] == {"hello": "world"}
    logs = base64.b64decode(response["LogResult"]).decode()
    assert "handler stdout" in logs and "handler stderr" in logs and "context log" in logs

    response, error = invoke(runtime, {"fail": True})
    assert response["FunctionError"] == "Unhandled"
    assert error["errorType"] == "ValueError"
    assert error["errorMessage"] == "intentional failure"
    assert error["stackTrace"]
    response, error = invoke(runtime, {"invalid": True})
    assert response["FunctionError"] == "Unhandled"
    assert error["errorType"] == "TypeError"
    response, result = invoke(runtime, {})
    assert "FunctionError" not in response, result
    assert result["count"] == 4
    assert result["client"] is None


def test_timeout_discards_python_process(runtime):
    response, error = invoke(runtime, {"sleep": 4})
    assert response["FunctionError"] == "Unhandled"
    assert "timed out" in error["errorMessage"]
    response, result = invoke(runtime, {})
    assert "FunctionError" not in response, result
    assert result["count"] == 1


@pytest.mark.parametrize(("handler_name", "error_type"), [
    ("not_a_handler", "ValueError"),
    ("missing_module.handler", "ModuleNotFoundError"),
])
def test_import_errors_are_reported_through_runtime_api(tmp_path, handler_name, error_type):
    server = Server(tmp_path, handler_name=handler_name, handler_cwd=tmp_path)
    try:
        server.start()
        server.client.create_function(
            FunctionName="durable-test", Runtime="provided.al2023", Handler=handler_name,
            Role="arn:aws:iam::123456789012:role/local", Code={"ZipFile": b""}, Timeout=5,
        )
        response, error = invoke(server, {})
        assert response["FunctionError"] == "Unhandled"
        assert error["errorType"] == error_type, error
    finally:
        server.stop()


def test_installed_entry_points_and_missing_runtime_endpoint():
    environment = dict(os.environ)
    environment.pop("AWS_LAMBDA_RUNTIME_API", None)
    response = subprocess.run(
        [sys.executable, "-m", "roto_aws.lambda_runtime", "app.handler"],
        env=environment, capture_output=True, text=True, timeout=5,
    )
    assert response.returncode == 2
    assert "AWS_LAMBDA_RUNTIME_API is missing" in response.stderr
    scripts = Path(sys.executable).parent
    for name in ("roto-server",):
        response = subprocess.run(
            [str(scripts / name), "--help"], capture_output=True, text=True, timeout=5,
        )
        assert response.returncode == 0, response.stderr
