"""Command executor adapter; checkpoint requests go to Roto via the real SDK."""
import json
import os
import sys
import time
from types import SimpleNamespace

from handler import handler


if __name__ == "__main__":
    if "--plain" in sys.argv:
        event = json.load(sys.stdin)
        json.dump({"value": event["value"] * 10}, sys.stdout)
        sys.exit(0)
    deadline = time.monotonic() + 30
    context = SimpleNamespace(
        aws_request_id=os.environ["ROTO_INVOCATION_ID"],
        function_name=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
        function_version=os.environ["AWS_LAMBDA_FUNCTION_VERSION"],
        invoked_function_arn=os.environ["ROTO_FUNCTION_ARN"] + (":$LATEST" if len(os.environ["ROTO_FUNCTION_ARN"].split(":"))==7 else ""),
        memory_limit_in_mb="128",
        log_group_name="/aws/lambda/durable-test",
        log_stream_name="local",
        get_remaining_time_in_millis=lambda: max(0, int((deadline - time.monotonic()) * 1000)),
    )
    json.dump(handler(json.load(sys.stdin), context), sys.stdout)
