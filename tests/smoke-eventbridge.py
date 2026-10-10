#!/usr/bin/env python3
"""SDK smoke for Lua setup, EventBridge routing, transforms and S3 events."""
import json
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path
import boto3


def eventually(predicate):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise AssertionError("EventBridge delivery timed out")


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory() as temp:
        proc = subprocess.Popen([str(root / "target/debug/roto-server"), "--port", str(port), "--data-dir", temp, "--setup", str(root / "examples/eventbridge/setup.lua")], stdout=subprocess.DEVNULL)
        try:
            def healthy():
                try:
                    return urllib.request.urlopen(endpoint + "/roto-api/health").status == 200
                except OSError:
                    return False
            eventually(healthy)
            def client(service):
                return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
            sqs, events, s3 = client("sqs"), client("events"), client("s3")
            url = sqs.get_queue_url(QueueName="events")["QueueUrl"]
            def receive():
                messages = sqs.receive_message(QueueUrl=url).get("Messages", [])
                if messages:
                    sqs.delete_message(QueueUrl=url, ReceiptHandle=messages[0]["ReceiptHandle"])
                    return json.loads(messages[0]["Body"])
            s3.put_object(Bucket="uploads", Key="a b.json", Body=b"hello")
            event = eventually(receive)
            assert event["source"] == "aws.s3" and event["detail"]["object"]["key"] == "a b.json", event
            assert event["detail"]["object"]["size"] == 5
            eventually(lambda: any(i["state"] == "succeeded" for i in json.load(urllib.request.urlopen(endpoint + "/roto-api/lambda/invocations"))["invocations"]))
            events.create_event_bus(Name="app")
            pattern = json.dumps({"source":["app"],"detail":{"count":[{"numeric":[">",2]}]}})
            events.put_rule(Name="jobs", EventBusName="app", EventPattern=pattern)
            assert events.test_event_pattern(EventPattern=pattern, Event=json.dumps({"source":"app","detail":{"count":3}}))["Result"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            assert events.put_targets(Rule="jobs", EventBusName="app", Targets=[{"Id":"q","Arn":arn,"InputPath":"$.detail"}])["FailedEntryCount"] == 0
            def publish(count):
                return events.put_events(Entries=[{"EventBusName":"app","Source":"app","DetailType":"Job","Detail":json.dumps({"count":count})}])
            assert publish(3)["FailedEntryCount"] == 0
            assert eventually(receive) == {"count":3}
            events.disable_rule(Name="jobs", EventBusName="app")
            publish(4)
            time.sleep(.2)
            assert receive() is None
            result = events.put_events(Entries=[{"Source":"app","DetailType":"Job","Detail":"invalid"}])
            assert result["FailedEntryCount"] == 1
            assert len(events.list_rules(EventBusName="app")["Rules"]) == 1
            events.remove_targets(Rule="jobs", EventBusName="app", Ids=["q"])
            events.delete_rule(Name="jobs", EventBusName="app")
            events.delete_event_bus(Name="app")
            print("EventBridge SDK smoke passed")
        finally:
            proc.terminate()
            proc.wait(timeout=10)


if __name__ == "__main__":
    smoke()
