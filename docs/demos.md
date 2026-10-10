# Demos

The repository's `demos/` directory contains runnable examples that connect Roto to
external tools and runtimes. This chapter walks through invoking a Lambda container image
with RIE. Its code blocks include the demo's actual source files, so they stay in sync with
the runnable example.

## Invoke a Lambda container image

This demo builds a small Python Lambda image. Roto receives only the image name in Lua,
starts the image with Podman or Docker, discovers the host port mapped to container port
8080, sends the event to RIE, and removes the container after the invocation.

Build the image from the repository root:

```sh
podman build -t roto-lambda-container:latest \
  -f demos/lambda-container/Containerfile demos/lambda-container
```

Use `docker build` with the same arguments if Docker is your container engine.

The `Containerfile` uses the AWS Lambda Python base image. Its entrypoint starts the
Runtime Interface Emulator for local invocations:

```dockerfile title="demos/lambda-container/Containerfile"
--8<-- "demos/lambda-container/Containerfile"
```

The handler calls boto3 STS against the `AWS_ENDPOINT_URL` supplied by Roto, then returns
that identity alongside the original event:

```python title="demos/lambda-container/app.py"
--8<-- "demos/lambda-container/app.py"
```

The Lua setup creates function metadata and binds the local image name:

```lua title="demos/lambda-container/setup.lua"
--8<-- "demos/lambda-container/setup.lua"
```

Start Roto with the setup file:

```sh
cargo run -p roto-server -- --ephemeral --host 0.0.0.0 \
  --setup demos/lambda-container/setup.lua
```

The non-loopback listen address lets the container reach Roto through the container engine's
host alias. The executor rewrites `AWS_ENDPOINT_URL` to `host.containers.internal` for Podman
or `host.docker.internal` for Docker. The setup supplies fake local credentials for the
signed STS request.

Invoke the function with AWS CLI:

```sh
AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name container-echo \
  --cli-binary-format raw-in-base64-out \
  --payload '{"hello":"container"}' result.json
cat result.json
```

The result contains `received` with the event and `identity` with `Account`, `Arn`, and
`UserId` from Roto's STS response. The request ID in boto3's response metadata changes on
each invocation.

The function timeout includes image startup. The image must already be present locally;
Roto does not build or pull it. See the [Lambda execution chapter](lambda.md) for executor
behavior and limits, and the [demo README](https://github.com/vivainio/roto/blob/main/demos/lambda-container/README.md)
for the same run instructions.

## Other runnable examples

- [CloudFormation demo](https://github.com/vivainio/roto/tree/main/demos/cloudformation):
  create queues and a topic, use a nested stack, then import an exported value.
- [CDK demo](https://github.com/vivainio/roto/tree/main/demos/cdk): synthesize a TypeScript
  app and deploy its stack against a local Roto endpoint.
