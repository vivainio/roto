#!/usr/bin/env python3
"""Discover unsupported calls through real boto3 requests and verify stderr logging."""
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory() as temp:
        stdout_path, stderr_path = Path(temp) / "stdout", Path(temp) / "stderr"
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            proc = subprocess.Popen(
                [str(root / "target/debug/roto-server"), "--port", str(port), "--ephemeral"],
                stdout=stdout, stderr=stderr,
                env={**os.environ, "RUST_LOG": "warn"},
            )
            try:
                deadline = time.monotonic() + 10
                while True:
                    try:
                        urllib.request.urlopen(endpoint + "/roto-api/health").close()
                        break
                    except OSError:
                        if proc.poll() is not None or time.monotonic() >= deadline:
                            raise AssertionError("Server did not start: " + stderr_path.read_text())
                        time.sleep(.05)

                def client(service):
                    return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1",
                                        aws_access_key_id="test", aws_secret_access_key="test",
                                        config=Config(retries={"max_attempts": 0}))

                def expect_error(call, code):
                    try:
                        call()
                    except ClientError as error:
                        assert error.response["Error"]["Code"] == code, error.response
                    else:
                        raise AssertionError("Expected " + code)

                def snapshot():
                    with urllib.request.urlopen(endpoint + "/roto-api/unsupported") as response:
                        assert response.headers.get_content_type() == "application/json"
                        return json.load(response)

                assert snapshot() == {"calls": [], "dropped_calls": 0}
                sts, iot, dynamodb, lambda_ = (client(s) for s in ("sts", "iot-data", "dynamodb", "lambda"))
                assert sts.get_caller_identity()["Account"] == "123456789012"
                expect_error(lambda: dynamodb.describe_table(TableName="missing-table"), "ResourceNotFoundException")
                assert snapshot()["calls"] == []
                for _ in range(2):
                    expect_error(lambda: iot.publish(topic="devices/123", qos=1, payload=b"private-payload"), "UnrecognizedClientException")
                expect_error(lambda: sts.decode_authorization_message(EncodedMessage="private-token"), "NotImplemented")
                expect_error(lambda: lambda_.get_alias(FunctionName="demo", Name="live"), "NotImplemented")
                expect_error(lambda: dynamodb.execute_statement(Statement="SELECT * FROM missing"), "NotImplemented")
                calls = {c["service"]: c for c in snapshot()["calls"]}
                assert len(calls) == 4, calls
                assert calls["iotdata"]["count"] == 2 and calls["iotdata"]["operation"] is None
                assert calls["iotdata"]["path"] == "/topics/devices%2F123", calls
                assert calls["sts"]["operation"] == "DecodeAuthorizationMessage"
                assert calls["lambda"]["operation"] == "GetAlias"
                assert calls["dynamodb"]["operation"] == "ExecuteStatement"
                captured = json.dumps(calls) + stderr_path.read_text()
                assert "private-payload" not in captured and "private-token" not in captured
                assert "Unsupported AWS call" in stderr_path.read_text()
                assert stdout_path.read_text() == ""
                for path in ("/roto-api/reset", "/moto-api/reset"):
                    request = urllib.request.Request(endpoint + path, data=b"", method="POST")
                    urllib.request.urlopen(request).close()
                    assert snapshot() == {"calls": [], "dropped_calls": 0}
                    if path == "/roto-api/reset":
                        expect_error(lambda: iot.publish(topic="devices/123", payload=b""), "UnrecognizedClientException")
            finally:
                proc.terminate()
                proc.wait(timeout=10)
    print("Unsupported-call discovery SDK smoke passed")


if __name__ == "__main__":
    smoke()
