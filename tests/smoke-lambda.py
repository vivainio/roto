#!/usr/bin/env python3
"""SDK smoke: command + HTTP Invoke, and filtered S3 -> command -> SQS.

Run after cargo build -p roto-server:
    .venv-moto/bin/python tests/smoke-lambda.py
Uses only disposable server state. --handler is the command executor entry point.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3


def client(service, endpoint):
    return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id="testing", aws_secret_access_key="testing")


def handler():
    event = json.load(sys.stdin)
    keys = [record["s3"]["object"]["key"] for record in event.get("Records", [])]
    if keys:
        client("sqs", os.environ["AWS_ENDPOINT_URL"]).send_message(
            QueueUrl=os.environ["QUEUE_URL"], MessageBody=json.dumps({"keys": keys}))
    print("processed invocation " + os.environ["ROTO_INVOCATION_ID"], file=sys.stderr)
    json.dump({"keys": keys, "event": event}, sys.stdout)


class HttpHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.headers.get("X-Roto-Invocation-Id")
        assert self.headers.get("X-Roto-Function-Arn")
        event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        body = json.dumps({"received": event}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    http = ThreadingHTTPServer(("127.0.0.1", 0), HttpHandler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="roto-lambda-smoke-") as temp:
            setup = Path(temp) / "setup.lua"
            command = ", ".join(json.dumps(arg) for arg in
                                  [sys.executable, str(Path(__file__).resolve()), "--handler"])
            url = json.dumps(f"http://127.0.0.1:{http.server_port}/invoke")
            setup.write_text(
                f'roto.lambda.bind("process-upload", {{command = {{{command}}}}})\n'
                f'roto.lambda.bind("http-handler", {{url = {url}}})\n'
            )
            with (Path(temp) / "server.log").open("w+") as log:
                process = subprocess.Popen([str(root / "target/debug/roto-server"), "--ephemeral",
                                            "--port", str(port), "--setup", str(setup)],
                                           stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1).close()
                            break
                        except OSError:
                            if process.poll() is not None or time.monotonic() > deadline:
                                log.seek(0)
                                raise RuntimeError(log.read())
                            time.sleep(0.05)
                    lam, s3, sqs = [client(s, endpoint) for s in ("lambda", "s3", "sqs")]
                    queue = sqs.create_queue(QueueName="processed")["QueueUrl"]
                    role = "arn:aws:iam::123456789012:role/local"
                    function = lam.create_function(FunctionName="process-upload", Role=role,
                                                   Runtime="provided.al2023", Handler="external",
                                                   Code={"ZipFile": b""}, Timeout=5,
                                                   Environment={"Variables": {"QUEUE_URL": queue}},
                                                   Tags={"local": "true"})
                    lam.create_function(FunctionName="http-handler", Role=role, Code={"ZipFile": b""})
                    response = lam.invoke(FunctionName="process-upload", Payload=b'{"direct":true}', LogType="Tail")
                    assert response["StatusCode"] == 200 and "FunctionError" not in response
                    assert json.load(response["Payload"])["event"] == {"direct": True}
                    assert response["LogResult"]
                    response = lam.invoke(FunctionName="http-handler", Payload=b'{"http":true}')
                    assert json.load(response["Payload"]) == {"received": {"http": True}}
                    assert lam.invoke(FunctionName="process-upload", InvocationType="DryRun")["StatusCode"] == 204
                    lam.tag_resource(Resource=function["FunctionArn"], Tags={"remove": "me"})
                    lam.untag_resource(Resource=function["FunctionArn"], TagKeys=["remove", "local"])
                    assert lam.list_tags(Resource=function["FunctionArn"])["Tags"] == {}
                    lam.update_function_configuration(FunctionName="http-handler", Description="HTTP backend")
                    assert lam.get_function(FunctionName="http-handler")["Configuration"]["Description"] == "HTTP backend"
                    assert len(lam.list_functions()["Functions"]) == 2
                    lam.add_permission(FunctionName="process-upload", StatementId="s3", Action="lambda:InvokeFunction",
                                       Principal="s3.amazonaws.com", SourceArn="arn:aws:s3:::uploads")
                    assert json.loads(lam.get_policy(FunctionName="process-upload")["Policy"])["Statement"][0]["Sid"] == "s3"
                    s3.create_bucket(Bucket="uploads")
                    s3.put_bucket_notification_configuration(Bucket="uploads", NotificationConfiguration={
                        "LambdaFunctionConfigurations": [{"Id": "incoming-json", "LambdaFunctionArn": function["FunctionArn"],
                                                          "Events": ["s3:ObjectCreated:*"],
                                                          "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "incoming/"},
                                                                                           {"Name": "suffix", "Value": ".json"}]}}}]
                    })
                    s3.put_object(Bucket="uploads", Key="ignored.json", Body=b"ignored")
                    s3.put_object(Bucket="uploads", Key="incoming/a b.json", Body=b"{}")
                    deadline = time.monotonic() + 8
                    while True:
                        messages = sqs.receive_message(QueueUrl=queue).get("Messages", [])
                        if messages:
                            assert json.loads(messages[0]["Body"]) == {"keys": ["incoming%2Fa+b.json"]}
                            break
                        assert time.monotonic() < deadline, "S3 -> command -> SQS delivery timed out"
                        time.sleep(0.05)
                    history = json.load(urllib.request.urlopen(endpoint + "/roto-api/lambda/invocations"))
                    assert len(history["invocations"]) == 3
                    lam.remove_permission(FunctionName="process-upload", StatementId="s3")
                    lam.delete_function(FunctionName="http-handler")
                    print("PASS: command/HTTP Invoke, metadata, tags/policies, filtered S3 -> command -> SQS")
                finally:
                    process.terminate()
                    process.wait(timeout=5)
    finally:
        http.shutdown()
        http.server_close()


if __name__ == "__main__":
    if sys.argv[1:] == ["--handler"]:
        handler()
    else:
        smoke()
