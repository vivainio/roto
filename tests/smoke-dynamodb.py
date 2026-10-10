#!/usr/bin/env python3
"""Verify DynamoDB account/region isolation and persistent items/indexes via the SDK."""

import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"

    def client(service, region="us-east-1", credentials=None):
        credentials = credentials or {
            "AccessKeyId": "testing", "SecretAccessKey": "testing"
        }
        return boto3.client(
            service, endpoint_url=endpoint, region_name=region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials.get("SessionToken"),
        )

    def start(directory):
        proc = subprocess.Popen(
            [str(root / "target/debug/roto-server"), "--port", str(port),
             "--data-dir", directory], stdout=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise AssertionError("DynamoDB smoke server exited")
                try:
                    with urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1):
                        return proc
                except OSError:
                    time.sleep(.05)
            raise AssertionError("DynamoDB smoke server did not become healthy")
        except BaseException:
            proc.terminate()
            proc.wait(timeout=10)
            raise

    with tempfile.TemporaryDirectory() as directory:
        proc = start(directory)
        try:
            credentials = client("sts").assume_role(
                RoleArn="arn:aws:iam::111111111111:role/ddb-smoke",
                RoleSessionName="isolation",
            )["Credentials"]
            identities = [
                ("us-east-1", None, "123456789012", "default"),
                ("us-west-2", None, "123456789012", "other-region"),
                ("us-east-1", credentials, "111111111111", "other-account"),
            ]
            table_name = "isolation-smoke"
            for region, creds, account, value in identities:
                ddb = client("dynamodb", region, creds)
                assert ddb.list_tables()["TableNames"] == []
                table = ddb.create_table(
                    TableName=table_name, BillingMode="PAY_PER_REQUEST",
                    AttributeDefinitions=[
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "group", "AttributeType": "S"},
                    ],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    GlobalSecondaryIndexes=[{
                        "IndexName": "by-group",
                        "KeySchema": [{"AttributeName": "group", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }],
                )["TableDescription"]
                assert table["TableArn"] == f"arn:aws:dynamodb:{region}:{account}:table/{table_name}"
                ddb.put_item(TableName=table_name, Item={
                    "pk": {"S": "same-key"}, "group": {"S": "shared-group"},
                    "value": {"S": value},
                })
                ddb.update_time_to_live(TableName=table_name, TimeToLiveSpecification={
                    "Enabled": True, "AttributeName": "expires",
                })

            def verify():
                for region, creds, account, value in identities:
                    ddb = client("dynamodb", region, creds)
                    assert ddb.list_tables()["TableNames"] == [table_name]
                    item = ddb.get_item(
                        TableName=table_name, Key={"pk": {"S": "same-key"}},
                        ConsistentRead=True,
                    )["Item"]
                    assert item["value"] == {"S": value}
                    assert ddb.scan(TableName=table_name)["Items"] == [item]
                    result = ddb.query(
                        TableName=table_name, IndexName="by-group",
                        KeyConditionExpression="#g = :g",
                        ExpressionAttributeNames={"#g": "group"},
                        ExpressionAttributeValues={":g": {"S": "shared-group"}},
                    )
                    assert result["Items"] == [item]
                    assert ddb.describe_time_to_live(TableName=table_name)[
                        "TimeToLiveDescription"
                    ]["AttributeName"] == "expires"

            verify()
            proc.terminate()
            proc.wait(timeout=10)
            proc = start(directory)
            verify()
            client("dynamodb", credentials=credentials).delete_table(TableName=table_name)
            assert client("dynamodb").get_item(
                TableName=table_name, Key={"pk": {"S": "same-key"}}
            )["Item"]["value"] == {"S": "default"}
            print("DynamoDB SDK smoke passed (isolation, indexes, TTL, restart)")
        finally:
            proc.terminate()
            proc.wait(timeout=10)


if __name__ == "__main__":
    smoke()
