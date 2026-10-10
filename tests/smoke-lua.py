#!/usr/bin/env python3
"""SDK smoke: Lua setup, SQS -> shell/HTTP, disabled mappings, and persisted restart.

Run after cargo build -p roto-server with .venv-moto/bin/python tests/smoke-lua.py.
"""
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert self.headers.get("X-Roto-Invocation-Id")
        self.server.events.append(event)
        body = json.dumps({"processed": len(event["Records"])}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def eventually(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for pipeline")


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    http.events = []
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="roto-lua-smoke-") as temp:
            temp = Path(temp)
            script = temp / "setup.lua"
            command = root / "examples/lua/process_jobs.sh"
            script.write_text(f'''
local jobs = roto.sqs.queue("jobs", {{visibility_timeout=30}})
local shell = roto.lambda.function_("shell", {{timeout=5, executor={{command={{"sh", {json.dumps(str(command))}}}}}}})
roto.lambda.event_source(jobs, shell, {{enabled=false}})
local web = roto.sqs.queue("web-jobs")
local handler = roto.lambda.function_("http", {{executor={{url="http://127.0.0.1:{http.server_port}/jobs"}}}})
roto.lambda.event_source(web, handler)
''')
            clients = {service: boto3.client(service, endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="testing", aws_secret_access_key="testing")
                       for service in ("sqs", "lambda")}
            sqs, lam = clients["sqs"], clients["lambda"]
            previous_ids = None
            for restart in range(2):
                with (temp / "server.log").open("w+") as log:
                    process = subprocess.Popen([str(root / "target/debug/roto-server"), "--port", str(port),
                                                "--data-dir", str(temp / "state"), "--setup", str(script)],
                                               stdout=log, stderr=log)
                    try:
                        def ready():
                            if process.poll() is not None:
                                log.seek(0)
                                raise RuntimeError(log.read())
                            try:
                                urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1).close()
                                return True
                            except OSError:
                                return False
                        eventually(ready)
                        mappings = lam.list_event_source_mappings()["EventSourceMappings"]
                        ids = {m["FunctionArn"].split(":")[-1]: m["UUID"] for m in mappings}
                        assert len(ids) == 2
                        if previous_ids:
                            assert ids == previous_ids, "Setup duplicated persisted mappings"
                        previous_ids = ids
                        shell_mapping = lam.get_event_source_mapping(UUID=ids["shell"])
                        assert shell_mapping["State"] == "Disabled"
                        jobs = sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
                        web = sqs.get_queue_url(QueueName="web-jobs")["QueueUrl"]
                        sqs.send_message(QueueUrl=jobs, MessageBody=f"shell-{restart}")
                        time.sleep(0.15)
                        count = sqs.get_queue_attributes(QueueUrl=jobs, AttributeNames=["ApproximateNumberOfMessages"])
                        assert count["Attributes"]["ApproximateNumberOfMessages"] == "1"
                        lam.update_event_source_mapping(UUID=ids["shell"], Enabled=True)
                        sqs.send_message(QueueUrl=web, MessageBody=f"http-{restart}")
                        def consumed(url):
                            attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
                            return attrs["ApproximateNumberOfMessages"] == "0" and attrs["ApproximateNumberOfMessagesNotVisible"] == "0"
                        eventually(lambda: consumed(jobs) and consumed(web))
                        eventually(lambda: len(http.events) == restart + 1)
                        assert http.events[-1]["Records"][0]["body"] == f"http-{restart}"
                        with urllib.request.urlopen(endpoint + "/roto-api/lambda/invocations") as response:
                            history = json.load(response)["invocations"]
                        assert all(i["state"] == "succeeded" for i in history)
                        shell_history = [i for i in history if i["functionArn"].endswith(":shell")]
                        assert len(shell_history) == restart + 1
                        assert shell_history[0]["result"] == {"processed": 1}
                    except BaseException:
                        log.seek(0)
                        print(log.read())
                        raise
                    finally:
                        process.terminate()
                        process.wait(timeout=10)
            print("Lua setup / SQS -> shell + HTTP / persisted restart: passed")
    finally:
        http.shutdown()
        http.server_close()


if __name__ == "__main__":
    smoke()
