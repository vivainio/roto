#!/usr/bin/env python3
"""SDK coverage for CloudFormation wiring, persistence, isolation and failure cleanup."""

import copy
import json
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"

    def client(service, region="us-east-1", credentials=None):
        credentials = credentials or {"AccessKeyId": "testing", "SecretAccessKey": "testing"}
        return boto3.client(
            service, endpoint_url=endpoint, region_name=region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials.get("SessionToken"),
        )

    def start(directory):
        proc = subprocess.Popen(
            [str(root / "target/debug/roto-server"), "--port", str(port), "--data-dir", directory],
            stdout=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise AssertionError("CloudFormation smoke server exited")
                try:
                    with urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1):
                        return proc
                except OSError:
                    time.sleep(.05)
            raise AssertionError("CloudFormation smoke server did not become healthy")
        except BaseException:
            proc.terminate()
            proc.wait(timeout=10)
            raise

    def error(call, code):
        try:
            call()
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == code, exc
        else:
            raise AssertionError(f"Expected {code}")

    with tempfile.TemporaryDirectory() as directory:
        proc = start(directory)
        try:
            cf, sqs, sns, ddb, s3 = [client(service) for service in ("cloudformation", "sqs", "sns", "dynamodb", "s3")]
            template = {
                "Parameters": {"TopicLabel": {"Type": "String", "Default": "notifications"}},
                "Resources": {
                    "ATopic": {"Type": "AWS::SNS::Topic", "Properties": {
                        "DisplayName": {"Ref": "TopicLabel"},
                        "Subscription": [{"Protocol": "sqs", "Endpoint": {"Fn::GetAtt": "ZQueue.Arn"}}],
                    }},
                    "ZQueue": {"Type": "AWS::SQS::Queue", "Properties": {
                        "VisibilityTimeout": 45, "Tags": [{"Key": "custom", "Value": "remove-me"}],
                        "RedrivePolicy": {"deadLetterTargetArn": {"Fn::GetAtt": ["DeadLetter", "Arn"]}, "maxReceiveCount": 3},
                    }},
                    "DeadLetter": {"Type": "AWS::SQS::Queue"},
                    "Bucket": {"Type": "AWS::S3::Bucket", "Properties": {
                        "VersioningConfiguration": {"Status": "Enabled"},
                        "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
                    }},
                    "Table": {"Type": "AWS::DynamoDB::Table", "Properties": {
                        "BillingMode": "PAY_PER_REQUEST",
                        "AttributeDefinitions": [{"AttributeName": name, "AttributeType": "S"} for name in ("pk", "sk", "group")],
                        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                        "GlobalSecondaryIndexes": [{"IndexName": "by-group", "KeySchema": [{"AttributeName": "group", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}],
                        "LocalSecondaryIndexes": [{"IndexName": "local-group", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "group", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}],
                        "TimeToLiveSpecification": {"AttributeName": "expires", "Enabled": True},
                    }},
                },
                "Outputs": {
                    "Queue": {"Value": {"Ref": "ZQueue"}}, "Topic": {"Value": {"Ref": "ATopic"}},
                    "Table": {"Value": {"Ref": "Table"}}, "Bucket": {"Value": {"Ref": "Bucket"}},
                    "Label": {"Value": {"Fn::Sub": ["${AWS::StackName}:${label}:${ATopic.TopicName}:${!literal}", {"label": {"Ref": "TopicLabel"}}]}, "Export": {"Name": {"Fn::Sub": "${AWS::StackName}:label"}}},
                },
            }
            stack_id = cf.create_stack(
                StackName="wired", TemplateBody=json.dumps(template),
                Parameters=[{"ParameterKey": "TopicLabel", "ParameterValue": "work"}],
                Tags=[{"Key": "environment", "Value": "smoke"}],
            )["StackId"]

            def stack():
                return cf.describe_stacks(StackName=stack_id)["Stacks"][0]

            def outputs():
                return {o["OutputKey"]: o["OutputValue"] for o in stack()["Outputs"]}

            original = outputs()
            assert stack()["StackStatus"] == "CREATE_COMPLETE"
            assert original["Label"].startswith("wired:work:") and original["Label"].endswith(":${literal}")
            assert sqs.list_queue_tags(QueueUrl=original["Queue"])["Tags"] == {"custom": "remove-me", "environment": "smoke"}
            sns.publish(TopicArn=original["Topic"], Message="via-stack")
            assert json.loads(sqs.receive_message(QueueUrl=original["Queue"], WaitTimeSeconds=1)["Messages"][0]["Body"])["Message"] == "via-stack"
            s3.head_bucket(Bucket=original["Bucket"])
            assert s3.get_bucket_versioning(Bucket=original["Bucket"])["Status"] == "Enabled"
            item = {"pk": {"S": "pk"}, "sk": {"S": "sk"}, "group": {"S": "group"}}
            ddb.put_item(TableName=original["Table"], Item=item)

            def verify_indexes():
                for index in ("by-group", "local-group"):
                    expr = "#g = :g" if index == "by-group" else "pk = :pk AND #g = :g"
                    values = {":g": {"S": "group"}}
                    if index == "local-group": values[":pk"] = {"S": "pk"}
                    assert ddb.query(TableName=original["Table"], IndexName=index,
                                     KeyConditionExpression=expr, ExpressionAttributeNames={"#g": "group"},
                                     ExpressionAttributeValues=values)["Items"] == [item]

            verify_indexes()
            changed = copy.deepcopy(template)
            changed["Resources"]["ZQueue"]["Properties"]["VisibilityTimeout"] = 90
            changed["Resources"]["ZQueue"]["Properties"].pop("Tags")
            cf.update_stack(StackName=stack_id, TemplateBody=json.dumps(changed))
            assert outputs() == original
            assert sqs.get_queue_attributes(QueueUrl=original["Queue"], AttributeNames=["VisibilityTimeout"])["Attributes"]["VisibilityTimeout"] == "90"
            assert sqs.list_queue_tags(QueueUrl=original["Queue"])["Tags"] == {"environment": "smoke"}
            error(lambda: cf.update_stack(StackName=stack_id, TemplateBody=json.dumps(changed)), "ValidationError")

            changed["Resources"]["ZQueue"]["Properties"].pop("VisibilityTimeout")
            cf.update_stack(StackName=stack_id, TemplateBody=json.dumps(changed))
            assert sqs.get_queue_attributes(QueueUrl=original["Queue"], AttributeNames=["VisibilityTimeout"])["Attributes"]["VisibilityTimeout"] == "30"

            changed["Resources"]["ZQueue"]["Properties"]["QueueName"] = "replacement-" + uuid4().hex[:12]
            cf.update_stack(StackName=stack_id, TemplateBody=json.dumps(changed))
            current = outputs()
            assert current["Queue"] != original["Queue"] and current["Table"] == original["Table"]
            error(lambda: sqs.get_queue_attributes(QueueUrl=original["Queue"], AttributeNames=["All"]), "AWS.SimpleQueueService.NonExistentQueue")
            subscriptions = sns.list_subscriptions_by_topic(TopicArn=current["Topic"])["Subscriptions"]
            assert len(subscriptions) == 1
            assert subscriptions[0]["Endpoint"] == sqs.get_queue_attributes(QueueUrl=current["Queue"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

            credentials = client("sts").assume_role(RoleArn="arn:aws:iam::111111111111:role/cf-smoke", RoleSessionName="isolation")["Credentials"]
            for other in (client("cloudformation", "us-west-2"), client("cloudformation", credentials=credentials)):
                error(lambda: other.describe_stacks(StackName=stack_id), "ValidationError")
                other.create_stack(StackName="wired", TemplateBody='{"Resources":{}}')
                assert other.describe_stacks(StackName="wired")["Stacks"][0]["StackId"] != stack_id

            proc.terminate()
            proc.wait(timeout=10)
            proc = start(directory)
            assert outputs() == current and stack()["StackStatus"] == "UPDATE_COMPLETE"
            assert cf.list_stack_resources(StackName=stack_id)["StackResourceSummaries"]
            verify_indexes()
            assert ddb.describe_time_to_live(TableName=current["Table"])["TimeToLiveDescription"]["AttributeName"] == "expires"
            assert client("cloudformation", credentials=credentials).describe_stacks(StackName="wired")["Stacks"]

            for invalid in (
                {"Resources": {"Q": {"Type": "AWS::EC2::Instance"}}},
                {"Resources": {"Q": {"Type": "AWS::SQS::Queue", "DependsOn": "Q"}}},
                {"Resources": {"Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": {"Ref": "Missing"}}}}},
            ):
                error(lambda: cf.create_stack(StackName="invalid", TemplateBody=json.dumps(invalid)), "ValidationError")
            error(lambda: cf.describe_stacks(StackName="invalid"), "ValidationError")

            invalid_index = copy.deepcopy(template["Resources"]["Table"])
            invalid_index["Properties"]["GlobalSecondaryIndexes"][0]["KeySchema"][0]["KeyType"] = "S"
            # Like AWS, a failed resource does not fail CreateStack; the default
            # OnFailure=ROLLBACK ends the stack in ROLLBACK_COMPLETE.
            cf.create_stack(StackName="invalid-ddb", TemplateBody=json.dumps({"Resources": {"Table": invalid_index}}))
            assert cf.describe_stacks(StackName="invalid-ddb")["Stacks"][0]["StackStatus"] == "ROLLBACK_COMPLETE"
            cf.delete_stack(StackName="invalid-ddb")

            # Configuration failure leaves a tracked resource that can be deleted.
            # DisableRollback keeps the failed resource tracked, as before.
            cf.create_stack(StackName="failed", DisableRollback=True, TemplateBody=json.dumps({
                "Resources": {"Topic": {"Type": "AWS::SNS::Topic", "Properties": {
                    "Subscription": [{"Protocol": "invalid", "Endpoint": "invalid"}],
                }}},
            }))
            failed = cf.describe_stacks(StackName="failed")["Stacks"][0]
            assert failed["StackStatus"] == "CREATE_FAILED" and failed["StackStatusReason"]
            failed_resources = cf.list_stack_resources(StackName="failed")["StackResourceSummaries"]
            assert len(failed_resources) == 1 and failed_resources[0]["ResourceStatus"] == "CREATE_FAILED"
            cf.delete_stack(StackName="failed")

            cf.delete_stack(StackName=stack_id)
            assert cf.describe_stacks(StackName=stack_id)["Stacks"][0]["StackStatus"] == "DELETE_COMPLETE"
            error(lambda: cf.describe_stacks(StackName="wired"), "ValidationError")
            error(lambda: ddb.describe_table(TableName=current["Table"]), "ResourceNotFoundException")
            error(lambda: s3.head_bucket(Bucket=current["Bucket"]), "404")
            cf.create_stack(StackName="wired", TemplateBody='{"Resources":{}}')
            urllib.request.urlopen(urllib.request.Request(endpoint + "/roto-api/reset", method="POST"))
            assert cf.describe_stacks()["Stacks"] == []
            print("CloudFormation SDK smoke passed (wiring, updates, indexes, restart, isolation, errors, cleanup)")
        finally:
            proc.terminate()
            proc.wait(timeout=10)


if __name__ == "__main__":
    smoke()
