# CloudFormation

roto supports synchronous CloudFormation stacks for five resource types:

| Resource | Supported properties |
| --- | --- |
| `AWS::SQS::Queue` | Name, tags, visibility/retention/delay settings, long polling, FIFO/deduplication, redrive and encryption attributes |
| `AWS::SNS::Topic` | Name, tags, display name, FIFO/deduplication, KMS key attribute, inline subscriptions |
| `AWS::S3::Bucket` | Name, tags, encryption, versioning, public access block configuration |
| `AWS::DynamoDB::Table` | Name, tags, keys/attribute definitions, billing/throughput, GSI/LSI, stream/SSE metadata, table class, deletion protection, TTL, point-in-time recovery configuration |
| `AWS::Kinesis::Stream` | Name, shard count, retention, tags, stream mode, stored encryption settings |

Resources use the existing service handlers and storage, so their behavior and
limitations match resources created directly with the AWS SDK.

## Stack APIs

`CreateStack`, `UpdateStack`, `DeleteStack`, `DescribeStacks`,
`ListStackResources`, and `DescribeStackResources` are implemented. Successful
mutations finish before the API responds, with `CREATE_COMPLETE`,
`UPDATE_COMPLETE`, or `DELETE_COMPLETE` status.

Stack state is scoped by account and region and persists in `cloudformation.db`.
Resources and outputs remain available after server restart. Resource descriptions
include logical IDs, physical IDs, types, status, and timestamps. Stack name and
stack ARN can be used for lookups. A deleted stack can be described by ARN until
its name is reused; name lookup and listing omit deleted stacks.

Templates use inline `TemplateBody`. JSON and ordinary YAML mappings are accepted;
YAML short tags `!Ref`, `!GetAtt`, and `!Sub` are accepted. Available template features are:

- `Resources`, `Outputs`, `Description`, and `Metadata`.
- String `Parameters`, defaults, explicit values, and `UsePreviousValue` on updates.
- `Ref`, `Fn::GetAtt` (list or dotted-string form), and `Fn::Sub`, including variable maps and literal escapes.
- `AWS::AccountId`, `AWS::Region`, `AWS::StackName`, `AWS::StackId`, `AWS::Partition`, and `AWS::URLSuffix` references.
- Explicit `DependsOn` and implicit dependencies from references. Forward references are ordered before resource creation; missing dependencies and cycles are rejected.
- Stack tags merged into resource tags; a resource tag takes precedence when both use the same key.

Output export names are returned in stack descriptions. Cross-stack export lookup
and `Fn::ImportValue` are not implemented.

## Example

Create a queue and an SNS topic subscribed to it:

```python
import boto3
import json

cf = boto3.client(
    "cloudformation",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)

template = {
    "Resources": {
        "Queue": {"Type": "AWS::SQS::Queue"},
        "Topic": {
            "Type": "AWS::SNS::Topic",
            "Properties": {
                "Subscription": [
                    {"Protocol": "sqs", "Endpoint": {"Fn::GetAtt": ["Queue", "Arn"]}}
                ]
            },
        },
    },
    "Outputs": {
        "QueueURL": {"Value": {"Ref": "Queue"}},
        "TopicARN": {"Value": {"Ref": "Topic"}},
    },
}

stack_id = cf.create_stack(
    StackName="notifications",
    TemplateBody=json.dumps(template),
)["StackId"]
print(cf.describe_stacks(StackName=stack_id)["Stacks"][0]["Outputs"])
```

Resources without explicit names receive generated names. `Ref` returns the queue
URL, topic ARN, bucket name, or table name, depending on resource type. SQS physical
IDs use the AWS queue URL format; SDK clients still need roto's endpoint URL.

## Updates and failures

Updates can add/remove resources, change supported properties in place, and replace
resources when a name, FIFO mode, or DynamoDB key/index definition changes.
Replacement creates the new resource before deleting the old one. A named
resource replacement needs a different explicit name. DynamoDB data is preserved
for in-place updates and is removed with its table on replacement or deletion.
Changing an SNS inline subscription reconciles subscriptions owned by the stack.

Unsupported resource types, properties, template sections, and intrinsic
functions fail explicitly. IAM, Lambda, EventBridge, and SSM resource types,
change sets, conditions, transforms, nested stacks, template URLs, deletion/retention
policies, termination protection, stack policies, pagination, and automatic
rollback are not implemented. Some property removals also fail explicitly when
the underlying service needs an update that this subset does not support.

If a resource operation fails, the stack and affected resource record a failed
status and reason. Completed work stays tracked so `DeleteStack` can clean it up.
An update failure may leave partial changes; automatic rollback is not performed.
A process interruption leaves its last saved progress visible, and the stack can
be deleted before recreating it. Delete failures retain the remaining resources
and can be retried after fixing the cause, such as emptying a nonempty S3 bucket.

The implementation is checked against 25 existing Moto integration tests across
SQS, SNS, S3, DynamoDB, and Kinesis. The runner corrects invalid schemas and overly strict
optional-field assertions in Moto's old DynamoDB CloudFormation fixtures in a
temporary copy; vendored tests remain unchanged and DynamoDB validation stays
strict. Additional SDK checks cover wiring, index queries, updates, restart,
account/region isolation, failure state, cleanup, and reset:

```sh
.venv-moto/bin/python scripts/smoke-cloudformation.py
```
