# Kinesis Data Streams

Kinesis supports persisted streams, shards and records, scoped by account and
region. Records use MD5 partition-key routing or `ExplicitHashKey`, with monotonic
sequence numbers per shard. Data and stream metadata survive server restart.

```python
import boto3

kinesis = boto3.client(
    "kinesis", endpoint_url="http://localhost:5070", region_name="us-east-1",
    aws_access_key_id="testing", aws_secret_access_key="testing",
)
kinesis.create_stream(StreamName="events", ShardCount=1)
kinesis.put_record(StreamName="events", Data=b"hello", PartitionKey="customer-1")
shard = kinesis.list_shards(StreamName="events")["Shards"][0]["ShardId"]
cursor = kinesis.get_shard_iterator(
    StreamName="events", ShardId=shard, ShardIteratorType="TRIM_HORIZON",
)["ShardIterator"]
page = kinesis.get_records(ShardIterator=cursor)
assert page["Records"][0]["Data"] == b"hello"
cursor = page["NextShardIterator"]
```

All five iterator types are supported: `TRIM_HORIZON`, `LATEST`,
`AT_SEQUENCE_NUMBER`, `AFTER_SEQUENCE_NUMBER`, and `AT_TIMESTAMP`.
Iterators expire after five minutes and are tied to the caller's account and
region. Reads are non-destructive; continue using `NextShardIterator`.
Closed shards remain readable until drained, then omit the next iterator.

`SplitShard`, `MergeShards`, and `UpdateShardCount` preserve closed parents and
create child shards. New writes route to open shards. Provisioned and on-demand
streams are supported, with synchronous state transitions. Retention defaults to
24 hours and can be changed between 24 and 8760 hours. Expired records are
filtered and removed lazily; extending retention does not revive expired data.

Stream/shard/tag/consumer listings support pagination. Tags and registered
consumer metadata persist. Registration does not implement enhanced fan-out
streaming. Encryption and enhanced-monitoring APIs store settings; records are
not encrypted and no CloudWatch metrics are emitted.

`AWS::Kinesis::Stream` is supported by [CloudFormation](cloudformation.md), including
names, shard count, retention, tags, mode and encryption settings. Kinesis APIs
also work through Lua startup setup. Streams and records are available in the
inspection resource browser.

## Compatibility and remaining gaps

The implementation covers 28 of the 39 operations in the vendored botocore model
and passes all 76 portable Moto 5.0.11 tests. This includes stream lifecycle,
records, iterators, resharding, tags, monitoring/encryption metadata, registered
consumers, and CloudFormation integration.

Not implemented: `SubscribeToShard` event streaming, Lambda Kinesis polling,
resource-policy APIs, resource-oriented tag APIs, account settings, warm
throughput, configurable maximum record size, and time-based `ListShards`
filters. Unsupported operations return explicit errors. Rate/throughput limits
and real encryption are not simulated. Writes retain the pinned suite's 1 MiB
per-record and 5 MiB/500-record batch limits.

```sh
scripts/run-moto-tests.sh test_kinesis
.venv-moto/bin/python scripts/smoke-kinesis.py
```

The SDK smoke additionally verifies persisted cursors, records and tags across
restart, account/region isolation, reshard routing, closed-shard draining and
reset. Native tests cover expiry, sequence continuity, batch validation and
hash-range boundaries.
