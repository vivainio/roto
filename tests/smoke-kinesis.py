#!/usr/bin/env python3
"""SDK checks for Kinesis persistence, scoped cursors, resharding and reset."""
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

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
                    raise AssertionError("Kinesis smoke server exited")
                try:
                    with urllib.request.urlopen(endpoint + "/roto-api/health", timeout=1):
                        return proc
                except OSError:
                    time.sleep(.05)
            raise AssertionError("Kinesis smoke server did not become healthy")
        except BaseException:
            proc.terminate()
            proc.wait(timeout=10)
            raise

    def fails(fn, code):
        try:
            fn()
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == code, exc
        else:
            raise AssertionError(f"Expected {code}")

    with tempfile.TemporaryDirectory() as directory:
        proc = start(directory)
        try:
            credentials = client("sts").assume_role(
                RoleArn="arn:aws:iam::111111111111:role/kinesis-smoke", RoleSessionName="isolation",
            )["Credentials"]
            identities = [
                ("us-east-1", None, "123456789012", b"default"),
                ("us-west-2", None, "123456789012", b"other-region"),
                ("us-east-1", credentials, "111111111111", b"other-account"),
            ]
            cursors = []
            stream_name = "isolation-smoke"
            shard_id = "shardId-000000000000"
            for region, creds, account, payload in identities:
                k = client("kinesis", region, creds)
                k.create_stream(StreamName=stream_name, ShardCount=2)
                desc = k.describe_stream(StreamName=stream_name)["StreamDescription"]
                assert desc["StreamARN"] == f"arn:aws:kinesis:{region}:{account}:stream/{stream_name}"
                k.add_tags_to_stream(StreamName=stream_name, Tags={"scope": payload.decode()})
                k.increase_stream_retention_period(StreamName=stream_name, RetentionPeriodHours=48)
                first = k.put_record(StreamName=stream_name, Data=payload, PartitionKey="same", ExplicitHashKey="0")
                assert first["ShardId"] == shard_id and first["SequenceNumber"] == "1"
                cursor = k.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
                cursors.append(cursor)
                page = k.list_shards(StreamName=stream_name, MaxResults=1)
                assert len(page["Shards"]) == 1
                assert len(k.list_shards(NextToken=page["NextToken"])["Shards"]) == 1

            k = client("kinesis")
            for index, (region, creds, _, _) in enumerate(identities[1:], 1):
                fails(lambda: client("kinesis", region, creds).get_records(ShardIterator=cursors[0]), "InvalidArgumentException")
                fails(lambda: k.get_records(ShardIterator=cursors[index]), "InvalidArgumentException")
            k.split_shard(StreamName=stream_name, ShardToSplit=shard_id, NewStartingHashKey="100")
            child = k.put_record(StreamName=stream_name, Data=b"child", PartitionKey="same", ExplicitHashKey="0")
            assert child["ShardId"] == "shardId-000000000002"
            proc.terminate()
            proc.wait(timeout=10)
            proc = start(directory)

            for index, (region, creds, _, payload) in enumerate(identities):
                k = client("kinesis", region, creds)
                desc = k.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
                assert desc["RetentionPeriodHours"] == 48
                assert desc["OpenShardCount"] == (3 if index == 0 else 2)
                assert k.list_tags_for_stream(StreamName=stream_name)["Tags"] == [{"Key": "scope", "Value": payload.decode()}]
                records = k.get_records(ShardIterator=cursors[index])
                assert [record["Data"] for record in records["Records"]] == [payload]
                if index == 0:
                    assert "NextShardIterator" not in records
                else:
                    second = k.put_record(StreamName=stream_name, Data=b"second", PartitionKey="same", ExplicitHashKey="0")
                    assert second["SequenceNumber"] == "2"
                    assert k.get_records(ShardIterator=records["NextShardIterator"])["Records"][0]["Data"] == b"second"

            req = urllib.request.Request(endpoint + "/roto-api/reset", data=b"", method="POST")
            with urllib.request.urlopen(req):
                pass
            assert client("kinesis").list_streams()["StreamNames"] == []
            fails(lambda: client("kinesis").get_records(ShardIterator=cursors[0]), "InvalidArgumentException")
        finally:
            proc.terminate()
            proc.wait(timeout=10)
    print("Kinesis isolation, cursors, restart, resharding and reset passed")


if __name__ == "__main__":
    smoke()
