# Agent quickstart

roto is an MIT-licensed local AWS simulator: one Rust binary, one endpoint,
persistent state, no Docker or AWS account required. Use normal AWS SDKs and CLI
commands against it. Core services include S3, SQS, DynamoDB, Lambda, SNS, and
EventBridge; check [coverage](https://vivainio.github.io/roto/services/) for supported operations.

## Start a server

```sh
uv tool install roto-aws
roto-server --port 5070 --ephemeral
```

Keep the server running in a separate terminal or managed background process.
`--ephemeral` discards state when the process stops. For persistent development,
replace it with `--data-dir ./roto-data` and reuse that directory on each start.
Use a free port if another server is running.

```sh
curl --fail http://localhost:5070/roto-api/health
```

## Configure clients

Set these in the shell that runs your application or AWS CLI:

```sh
export AWS_ENDPOINT_URL=http://localhost:5070
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
unset AWS_SESSION_TOKEN AWS_PROFILE
```

These credentials are local placeholders. The default account is `123456789012`.
For SDKs that do not honor `AWS_ENDPOINT_URL`, pass the endpoint explicitly.
For example, with boto3 installed:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)
s3.create_bucket(Bucket="agent-demo")
s3.put_object(Bucket="agent-demo", Key="hello.txt", Body=b"hello")
assert s3.get_object(Bucket="agent-demo", Key="hello.txt")["Body"].read() == b"hello"
```

## Use and inspect it

With AWS CLI v2 installed and the environment above configured:

```sh
aws sts get-caller-identity
aws sqs create-queue --queue-name agent-demo
aws sqs list-queues
```

Open `http://localhost:5070/roto-api/` to inspect resources and request history.
For a single JSON summary of stored collections across accounts and regions:

```sh
curl --fail http://localhost:5070/roto-api/summary
```

The summary includes counts and identifying fields for built-in and installed
Luau-service collections. Counts are stored rows and can include versions and
internal documents; resource bodies and secret values are omitted.

For debugging, query recent calls and unsupported operations:

```sh
curl --fail http://localhost:5070/roto-api/trace
curl --fail http://localhost:5070/roto-api/unsupported
```

For repeatable fixtures, start a fresh server with
`roto-server --ephemeral --setup setup.luau`; the [Luau setup script](https://vivainio.github.io/roto/lua/)
runs before the listener starts. In a roto checkout, `scripts/demo.sh 5071`
builds and starts a populated disposable server on port 5071.

## Constraints

Coverage varies by service. Signatures and IAM policies are not enforced, so
roto does not validate authorization behavior. Optional Luau services require a
[separate bundle](https://vivainio.github.io/roto/getting-started/#add-optional-luau-services). Lambda execution
requires [local handler configuration](https://vivainio.github.io/roto/lambda/).

See [Getting started](https://vivainio.github.io/roto/getting-started/) for a persistent CLI profile and
[Server reference](https://vivainio.github.io/roto/server/) for options, reset endpoints, and request tracing.
