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

Provide local test credentials and a region so the client can sign and route requests:

```sh
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:5070 sts get-caller-identity
```

The default account is `123456789012`. Test credentials need not exist in AWS.
Set the endpoint explicitly on each AWS command.

Try an S3 round trip:

```sh
aws --endpoint-url http://localhost:5070 s3 mb s3://demo-bucket
printf 'hello from roto\n' > /tmp/roto-hello.txt
aws --endpoint-url http://localhost:5070 s3 cp /tmp/roto-hello.txt s3://demo-bucket/hello.txt
aws --endpoint-url http://localhost:5070 s3 cp s3://demo-bucket/hello.txt -
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
