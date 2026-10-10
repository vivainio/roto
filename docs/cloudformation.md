# CloudFormation

CloudFormation turns a template into service resources and gives clients one
place to create, inspect, update, and delete them. Roto implements this as a
useful subset: stack operations are synchronous, and templates call the same
service handlers used by direct SQS, SNS, S3, DynamoDB, Kinesis, IAM, and Lambda
requests.

## Run the end-to-end demo

Start a seeded local server in one terminal, then run the AWS CLI demo in
another. The CLI uses fake credentials and targets Roto on port 5071.

```sh
scripts/demo.sh
```

```sh
cd demos/cloudformation
./deploy-local.sh
```

The demo creates an SQS queue and an SNS topic subscribed to it, plus a nested
stack that creates a second queue. It then creates a consumer stack that imports
an export from the first stack. The templates show parameters, references,
outputs, and `Fn::Sub`. Set `ROTO_ENDPOINT` if the server uses another port.

The script uploads the nested template into a local S3 bucket because
`AWS::CloudFormation::Stack` loads its template through `TemplateURL`. It prints
both stacks' outputs and the nested stack's parent/root IDs. Delete the consumer
stack first so it releases its import, then delete the main stack; that removes
the nested stack and its queue. The shared template bucket remains until the
ephemeral server is restarted.

## Supported resources

Roto supports synchronous CloudFormation stacks for nine resource types:

| Resource | Supported properties |
| --- | --- |
| `AWS::SQS::Queue` | Name, tags, visibility/retention/delay settings, long polling, FIFO/deduplication, redrive and encryption attributes |
| `AWS::SNS::Topic` | Name, tags, display name, FIFO/deduplication, KMS key attribute, inline subscriptions |
| `AWS::S3::Bucket` | Name, tags, encryption, versioning, public access block configuration |
| `AWS::DynamoDB::Table` | Name, tags, keys/attribute definitions, billing/throughput, GSI/LSI, stream/SSE metadata, table class, deletion protection, TTL, point-in-time recovery configuration |
| `AWS::Kinesis::Stream` | Name, shard count, retention, tags, stream mode, stored encryption settings |
| `AWS::IAM::Role` | Name, path, trust policy, description, session duration, tags, managed policy ARN attachments |
| `AWS::IAM::Policy` | Inline policy document attached to one or more roles |
| `AWS::Lambda::Function` | Creation/deletion with name, inline or S3 code, role, runtime, handler, description, timeout, memory, environment, architectures, ephemeral storage, tracing, and tags; configuration updates are unsupported |
| `AWS::CloudFormation::Stack` | Nested stacks loaded from an S3 `TemplateURL`, with parameter and tag propagation, recursive create/update/delete, and child outputs |

Resources use the existing service handlers and storage. IAM role policy documents
and managed-policy attachments are stored as configuration; they do not grant or
deny requests because IAM policy evaluation is not implemented.

## Stack APIs

`CreateStack`, `UpdateStack`, `DeleteStack`, `DescribeStacks`, `ListStacks`,
`ListExports`,
`ListStackResources`, `DescribeStackResources`, `DescribeStackEvents`, and
`GetTemplate` are implemented. Change-set APIs include `CreateChangeSet`,
`DescribeChangeSet`, `ExecuteChangeSet`, `DeleteChangeSet`, and `ListChangeSets`.
`SetStackPolicy`, `GetStackPolicy`, and `UpdateTerminationProtection` are also
implemented. Successful
mutations finish before the API responds, with `CREATE_COMPLETE`,
`UPDATE_COMPLETE`, or `DELETE_COMPLETE` status.

Stack state is scoped by account and region and persists in `cloudformation.db`.
Resources and outputs remain available after server restart. Resource descriptions
include logical IDs, physical IDs, types, status, and timestamps. Stack name and
stack ARN can be used for lookups. A deleted stack can be described by ARN until
its name is reused; name lookup and listing omit deleted stacks.

Known IAM access keys and STS session keys select their owning account. For local
multi-account work without provisioning IAM credentials, an otherwise unknown
12-digit `AWS_ACCESS_KEY_ID` selects that account directly. For example, these
commands list separate CloudFormation namespaces:

```sh
AWS_ACCESS_KEY_ID=111122223333 AWS_SECRET_ACCESS_KEY=demo \
  aws --endpoint-url http://localhost:5071 cloudformation list-stacks
AWS_ACCESS_KEY_ID=444455556666 AWS_SECRET_ACCESS_KEY=demo \
  aws --endpoint-url http://localhost:5071 cloudformation list-stacks
```

Non-numeric or other unknown credentials use `--account-id` / `ROTO_ACCOUNT_ID`
(default `123456789012`). Region still comes from the SigV4 credential scope.

Templates use inline `TemplateBody`. JSON and ordinary YAML mappings are accepted;
YAML short tags `!Ref`, `!GetAtt`, `!Sub`, and `!ImportValue` are accepted. Available template features are:

- `Resources`, `Outputs`, `Description`, `Metadata`, and `Conditions`.
- String `Parameters`, defaults, explicit values, and `UsePreviousValue` on updates.
- `Ref`, `Fn::GetAtt` (list or dotted-string form), `Fn::Sub`, including variable maps and literal escapes, `Fn::Join`, and `Fn::ImportValue`.
- `Fn::If` with named conditions using `Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`, and condition references. Cyclic condition references are rejected. Resource/output `Condition` fields are unsupported.
- `AWS::NoValue` omits a property or list item, including when selected by `Fn::If`.
- `DeletionPolicy` and `UpdateReplacePolicy` with `Delete` or `Retain`.
- `AWS::AccountId`, `AWS::Region`, `AWS::StackName`, `AWS::StackId`, `AWS::Partition`, and `AWS::URLSuffix` references.
- Explicit `DependsOn` and implicit dependencies from references. Forward references are ordered before resource creation; missing dependencies and cycles are rejected.
- Stack tags merged into resource tags; a resource tag takes precedence when both use the same key.
- Nested `AWS::CloudFormation::Stack` resources load `TemplateURL` objects from Roto's S3 service. Nested parameters, tags, outputs, and parent/root stack IDs are tracked, and nested stacks can themselves contain nested stacks.

Export names are returned in stack descriptions and `ListExports`. `Fn::ImportValue`
resolves exports from another stack in the same account and region. Export names
must be unique there, and an export cannot be changed or removed while another
stack imports it. The exporter also cannot be deleted while an import is active;
delete the importing stack first. `ListExports` returns pages of up to 100
exports.

For example, an exporting stack can publish a resource attribute:

```yaml
Outputs:
  QueueArn:
    Value:
      Fn::GetAtt: [Queue, Arn]
    Export:
      Name:
        Fn::Sub: "${AWS::StackName}-QueueArn"
```

A separate stack can accept the export name as a parameter and import it:

```yaml
Parameters:
  QueueArnExportName:
    Type: String

Outputs:
  SharedQueueArn:
    Value:
      Fn::ImportValue:
        Ref: QueueArnExportName
```

The export name is resolved when the first stack is created. The importing
stack's output then contains the exported value; inspect available names with
`aws --endpoint-url http://localhost:5071 cloudformation list-exports`.

## Nested stacks

An `AWS::CloudFormation::Stack` resource creates a child stack from a template
object in Roto's S3 service. Its `Parameters` and `Tags` are passed to the child.
The parent can read child outputs through `Fn::GetAtt`, using attributes such as
`Outputs.QueueUrl`. `Ref` on the nested resource returns the child stack ID.
Child records expose `ParentId` and `RootId`, and nested stacks can contain more
nested stacks up to a maximum depth of 16.

Creating or updating the parent creates or updates the child in the same
account and region. Deleting the parent recursively deletes the child. A child
deletion failure leaves the failed child and remaining resources visible so the
delete can be retried. Nested change sets are not supported.

## Change sets and protection

Create/update change sets store a target template, parameters, tags, and a resource
change preview. `DescribeChangeSet` reports additions, removals, modifications,
and replacement estimates; unresolved properties can produce a conditional
replacement estimate. A change set with no resource changes is marked `FAILED`
and cannot be executed. Executing a create change set creates its
`REVIEW_IN_PROGRESS` stack; executing an update uses the stack update path.

Stack policies support `Allow` and `Deny` statements with `Action` and `Resource`.
The current evaluator enforces matching `Deny` statements for `Update:Modify`,
`Update:Replace`, and `Update:Delete`; it does not implement full AWS policy
semantics. `StackPolicyDuringUpdateBody` can override the policy for an update.
Termination protection blocks `DeleteStack` until disabled.

`DescribeStackEvents` returns persisted events newest first in pages of 100;
`ListExports` returns pages of up to 100. Other stack/resource/change-set listing
APIs do not paginate. `GetTemplate` returns the stored stack template as JSON;
fetching a change-set template is unsupported.

## Template specification checks

On the first template check, Roto loads pinned resource specification version
`267.0.0` from its cache or downloads it with a ten-second timeout. Cached and
downloaded bytes are checked against a pinned SHA-256. The specification checks
top-level property names, required properties, and scalar types; Roto's built-in
checks still determine which resource types and properties it can execute.

Set `ROTO_CFN_SPEC=offline` to skip downloading. `ROTO_CACHE_DIR` chooses the cache
directory, which defaults to `~/.cache/roto`. If the specification is unavailable,
Roto logs a warning and uses its built-in template checks.

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
functions fail explicitly. IAM users, groups, and managed policy resources,
EventBridge and SSM resources, transforms, nested change sets, imports of existing
resources, top-level template URLs, snapshot retention policies, and rollback triggers remain
unsupported. Some property removals also fail when the underlying service needs
an update that this subset does not support. Lambda configuration updates are
unsupported; creating a function does not attach a local executor automatically.

Resource-operation failures are recorded in stack status and reason; the API
call can succeed even when the resulting stack fails. Create failures default to
rolling back created resources. `OnFailure` can select `ROLLBACK`, `DELETE`, or
`DO_NOTHING`; `DisableRollback=true` selects `DO_NOTHING` and cannot be combined
with `OnFailure`. Update failures attempt to reconcile the previous template,
parameters, and tags, resulting in `UPDATE_ROLLBACK_COMPLETE` or
`UPDATE_ROLLBACK_FAILED`. This is a reconciliation attempt, not a transactional
restore of deleted resource data. `ExecuteChangeSet` also accepts
`DisableRollback`.

A process interruption leaves its last saved progress visible. Delete failures
retain remaining resources and can be retried after fixing the cause, such as
emptying a nonempty S3 bucket. `RetainResources` leaves named logical resources
in place while dropping them from the stack. `DeletionMode=FORCE_DELETE_STACK`
also drops resources whose deletion fails. Template retention policies apply to
deletion and replacement.

The implementation is checked against 25 existing Moto integration tests across
SQS, SNS, S3, DynamoDB, and Kinesis. The runner corrects invalid schemas and overly strict
optional-field assertions in Moto's old DynamoDB CloudFormation fixtures in a
temporary copy; vendored tests remain unchanged and DynamoDB validation stays
strict. Additional SDK checks cover wiring, index queries, updates, restart,
account/region isolation, failure state, cleanup, and reset:

```sh
.venv-moto/bin/python tests/smoke-cloudformation.py
```
