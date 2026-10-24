#!/usr/bin/env python3
"""SDK smoke for EventBridge Scheduler: CRUD, validation, and firing into SQS."""
import json
import socket
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


def eventually(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError("scheduler delivery timed out")


def code(call):
    try:
        call()
    except ClientError as e:
        return e.response["Error"]["Code"]
    raise AssertionError("expected an error")


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory() as temp:
        proc = subprocess.Popen([str(root / "target/debug/roto-server"), "--port", str(port), "--data-dir", temp], stdout=subprocess.DEVNULL)
        try:
            def healthy():
                try:
                    return urllib.request.urlopen(endpoint + "/roto-api/health").status == 200
                except OSError:
                    return False
            eventually(healthy)
            def client(service):
                return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
            sqs, scheduler = client("sqs"), client("scheduler")
            url = sqs.create_queue(QueueName="work")["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            role = "arn:aws:iam::123456789012:role/r"
            target = {"Arn": arn, "RoleArn": role, "Input": json.dumps({"tenant": "acme", "tick": "<aws.scheduler.scheduled-time>"})}
            window = {"Mode": "OFF"}

            # Validation and CRUD.
            assert code(lambda: scheduler.create_schedule(Name="bad", ScheduleExpression="rate(1 minutes)", ScheduleExpressionTimezone="Mars/Base", FlexibleTimeWindow=window, Target=target)) == "ValidationException"
            scheduler.create_schedule(Name="poll", ScheduleExpression="rate(1 minutes)", FlexibleTimeWindow=window, Target=target)
            assert code(lambda: scheduler.create_schedule(Name="poll", ScheduleExpression="rate(1 minutes)", FlexibleTimeWindow=window, Target=target)) == "ConflictException"
            assert code(lambda: scheduler.get_schedule(Name="missing")) == "ResourceNotFoundException"
            scheduler.create_schedule_group(Name="tenants", Tags=[{"Key": "k", "Value": "v"}])
            scheduler.create_schedule(Name="nightly", GroupName="tenants", ScheduleExpression="cron(0 2 * * ? *)", ScheduleExpressionTimezone="Europe/Helsinki",
                                      State="DISABLED", FlexibleTimeWindow=window, Target=target)
            got = scheduler.get_schedule(Name="nightly", GroupName="tenants")
            assert got["State"] == "DISABLED" and got["ScheduleExpressionTimezone"] == "Europe/Helsinki" and got["Target"]["Input"] == target["Input"], got
            assert [s["Name"] for s in scheduler.list_schedules(GroupName="tenants")["Schedules"]] == ["nightly"]
            assert [s["Name"] for s in scheduler.list_schedules(State="ENABLED")["Schedules"]] == ["poll"]
            assert [s["Name"] for s in scheduler.list_schedules(NamePrefix="ni")["Schedules"]] == ["nightly"]
            page = scheduler.list_schedules(MaxResults=1)
            assert len(page["Schedules"]) == 1 and "NextToken" in page
            assert {g["Name"] for g in scheduler.list_schedule_groups()["ScheduleGroups"]} == {"default", "tenants"}
            assert scheduler.list_tags_for_resource(ResourceArn=scheduler.get_schedule_group(Name="tenants")["Arn"])["Tags"] == [{"Key": "k", "Value": "v"}]
            scheduler.update_schedule(Name="poll", ScheduleExpression="rate(5 minutes)", FlexibleTimeWindow=window, Target=target)
            assert scheduler.get_schedule(Name="poll")["ScheduleExpression"] == "rate(5 minutes)"
            scheduler.delete_schedule_group(Name="tenants")
            assert code(lambda: scheduler.get_schedule(Name="nightly", GroupName="tenants")) == "ResourceNotFoundException"
            scheduler.delete_schedule(Name="poll")

            # A one-time schedule fires on time with the scheduled time substituted.
            at = (datetime.now(timezone.utc) + timedelta(seconds=3)).replace(microsecond=0)
            scheduler.create_schedule(Name="once", ScheduleExpression=f"at({at:%Y-%m-%dT%H:%M:%S})", ActionAfterCompletion="DELETE", FlexibleTimeWindow=window, Target=target)
            def receive():
                messages = sqs.receive_message(QueueUrl=url).get("Messages", [])
                if messages:
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=messages[0]["ReceiptHandle"])
                    return json.loads(messages[0]["Body"])
            body = eventually(receive)
            assert body == {"tenant": "acme", "tick": f"{at:%Y-%m-%dT%H:%M:%S}Z"}, body
            eventually(lambda: code(lambda: scheduler.get_schedule(Name="once")) == "ResourceNotFoundException")
            time.sleep(.5)
            assert receive() is None
            history = json.load(urllib.request.urlopen(endpoint + "/roto-api/scheduler/deliveries"))["deliveries"]
            assert [d["state"] for d in history] == ["succeeded"], history
            print("Scheduler SDK smoke passed")
        finally:
            proc.terminate()
            proc.wait(timeout=10)


if __name__ == "__main__":
    smoke()
