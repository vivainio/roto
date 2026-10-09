# Getting started

## Install and start

With [uv](https://docs.astral.sh/uv/) installed, install roto as a command-line tool:

```sh
uv tool install roto-aws
roto-server --port 5070
```

The package is named `roto-aws`; the installed command is `roto-server`.
The server listens on `127.0.0.1:5070` and persists state under `./roto-data`.
Leave it running and use another terminal for client commands.

To upgrade:

```sh
uv tool upgrade roto-aws
```

## Build from source

For development, install a current stable Rust toolchain supporting edition 2024,
then clone the repository:

```sh
git clone https://github.com/vivainio/roto.git
cd roto
cargo run -p roto-server -- --port 5070
```

For a release build:

```sh
cargo build --release -p roto-server
./target/release/roto-server --port 5070
```

## Connect the AWS CLI

With a current AWS CLI v2, add a named profile to `~/.aws/config`:

```ini
[profile roto]
region = us-east-1
endpoint_url = http://localhost:5070
aws_access_key_id = testing
aws_secret_access_key = testing
output = json
```

These are local test credentials; they need not exist in AWS. The default roto
account is `123456789012`. The AWS CLI supports both credentials and the endpoint
in the [shared config file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
The profile's endpoint applies to all services.

Select the profile when running AWS commands:

```sh
aws --profile roto sts get-caller-identity
aws --profile roto sqs list-queues
aws --profile roto dynamodb list-tables
```

For a terminal session, you can also select it with `AWS_PROFILE`:

```sh
export AWS_PROFILE=roto
aws sts get-caller-identity
```

`--profile roto` makes the selected environment explicit in scripts. If endpoint
overrides such as `AWS_ENDPOINT_URL` are already set in your shell, they take
precedence over the config file; unset them to use the profile's endpoint.
See AWS's [endpoint configuration rules](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-endpoints.html).

Try an S3 round trip:

```sh
aws --profile roto s3 mb s3://demo-bucket
printf 'hello from roto\n' > /tmp/roto-hello.txt
aws --profile roto s3 cp /tmp/roto-hello.txt s3://demo-bucket/hello.txt
aws --profile roto s3 cp s3://demo-bucket/hello.txt -
```

## Connect boto3

With boto3 installed, create clients using the same endpoint:

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)
queue = sqs.create_queue(QueueName="demo")
sqs.send_message(QueueUrl=queue["QueueUrl"], MessageBody="hello")
print(sqs.receive_message(QueueUrl=queue["QueueUrl"]))
```

## Disposable test state

```sh
roto-server --ephemeral
curl --fail http://localhost:5070/roto-api/health
```

Ephemeral mode uses in-memory databases and temporary files. Restarting discards
state. To clear all services on a running instance, use the
[reset endpoint](server.md#administrative-endpoints).
