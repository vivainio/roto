"""Run a Python handler against the Lambda Runtime API served by Roto.

Usage: python -m roto_aws.lambda_runtime app.handler
Uses only the standard library; application dependencies belong in this interpreter.
"""

import argparse
import base64
import http.client
import importlib
import json
import os
import sys
import time
import traceback
from types import SimpleNamespace
from urllib.parse import quote


API = "/2018-06-01/runtime/"


def _client_context(encoded):
    if not encoded:
        return None
    value = json.loads(base64.b64decode(encoded, validate=True))
    client = value.get("client")
    return SimpleNamespace(
        client=SimpleNamespace(**client) if client is not None else None,
        custom=value.get("custom"),
        env=value.get("env"),
    )


class LambdaContext:
    """Invocation metadata supplied by the runtime endpoint and function environment."""

    def __init__(self, headers):
        self.aws_request_id = headers["Lambda-Runtime-Aws-Request-Id"]
        self.invoked_function_arn = headers.get("Lambda-Runtime-Invoked-Function-Arn")
        self.function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        self.function_version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        self.memory_limit_in_mb = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        self.log_group_name = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME")
        self.log_stream_name = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME")
        self.client_context = _client_context(headers.get("Lambda-Runtime-Client-Context"))
        identity = headers.get("Lambda-Runtime-Cognito-Identity")
        self.identity = SimpleNamespace(**json.loads(identity)) if identity else None
        self.tenant_id = headers.get("Lambda-Runtime-Aws-Tenant-Id")
        remaining = max(0, int(headers["Lambda-Runtime-Deadline-Ms"]) - time.time() * 1000)
        self._deadline = time.monotonic() + remaining / 1000

    def get_remaining_time_in_millis(self):
        return max(0, int((self._deadline - time.monotonic()) * 1000))

    def log(self, message):
        print(message, flush=True)


def _error(error):
    return {
        "errorType": type(error).__name__,
        "errorMessage": str(error),
        "stackTrace": traceback.format_tb(error.__traceback__),
    }


def _post(connection, path, body):
    connection.request(
        "POST", API + path, body=body,
        headers={"Content-Type": "application/json"},
    )
    response = connection.getresponse()
    response.read()
    if response.status != 202:
        raise RuntimeError(f"Runtime API {path} returned HTTP {response.status}")


def run(handler_name, endpoint):
    """Load the handler once and process successive invocations on a warm process."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    connection = http.client.HTTPConnection(endpoint, timeout=None)
    try:
        try:
            module_name, separator, attribute = handler_name.rpartition(".")
            if not separator or not module_name or not attribute:
                raise ValueError("Handler must have the form module.function, for example app.handler")
            handler = getattr(importlib.import_module(module_name), attribute)
            if not callable(handler):
                raise TypeError(f"Handler {handler_name!r} is not callable")
        except Exception as error:
            traceback.print_exc()
            _post(connection, "init/error", json.dumps(_error(error)).encode())
            return 1

        while True:
            connection.request("GET", API + "invocation/next")
            response = connection.getresponse()
            event_body = response.read()
            if response.status != 200:
                raise RuntimeError(f"Runtime API invocation/next returned HTTP {response.status}")
            request_id = response.headers.get("Lambda-Runtime-Aws-Request-Id")
            if not request_id:
                raise RuntimeError("Runtime API response is missing Lambda-Runtime-Aws-Request-Id")
            trace = response.headers.get("Lambda-Runtime-Trace-Id")
            if trace:
                os.environ["_X_AMZN_TRACE_ID"] = trace
            else:
                os.environ.pop("_X_AMZN_TRACE_ID", None)
            try:
                context = LambdaContext(response.headers)
                result = handler(json.loads(event_body), context)
                output = json.dumps(result, allow_nan=False).encode()
                suffix = "response"
            except Exception as error:
                traceback.print_exc()
                output = json.dumps(_error(error)).encode()
                suffix = "error"
            sys.stdout.flush()
            sys.stderr.flush()
            _post(connection, f"invocation/{quote(request_id, safe='')}/{suffix}", output)
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("handler", help="Python handler to import, for example app.handler")
    args = parser.parse_args(argv)
    endpoint = os.environ.get("AWS_LAMBDA_RUNTIME_API")
    if not endpoint:
        parser.error("AWS_LAMBDA_RUNTIME_API is missing; launch this module with a Roto runtime binding")
    try:
        return run(args.handler, endpoint)
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
