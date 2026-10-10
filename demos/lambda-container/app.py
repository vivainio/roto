import boto3
import os


def handler(event, context):
    print(f"container invocation: {context.aws_request_id}")
    sts = boto3.client(
        "sts",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name=os.environ["AWS_REGION"],
    )
    identity = sts.get_caller_identity()
    return {"received": event, "identity": identity}
