# Demos

This example uses the installed `roto-server` command and Podman or Docker.
Create an empty directory and save the three files shown below there:
`Containerfile`, `app.py`, and `setup.luau`. No repository checkout is needed.

## Invoke a Lambda container image

This demo builds a small Python Lambda image. Roto receives only the image name in Luau,
starts the image with Podman or Docker, discovers the host port mapped to container port
8080, sends the event to RIE, and removes the container after the invocation.
Set `warm = true` in the executor binding to reuse idle containers; see the
[warm counter example](lambda.md#try-warm-and-cold-invocations).

The `Containerfile` uses the AWS Lambda Python base image. Its entrypoint starts the
Runtime Interface Emulator (RIE) for local invocations. RIE and the runtime client
are already included in this base image, so the Containerfile does not need to
install them. The `container` binding requires RIE to be available and started on
port 8080; arbitrary custom images may need to add it:

```dockerfile title="Containerfile"
--8<-- "demos/lambda-container/Containerfile"
```

The handler calls boto3 STS against the `AWS_ENDPOINT_URL` supplied by Roto, then returns
that identity alongside the original event:

```python title="app.py"
--8<-- "demos/lambda-container/app.py"
```

The Luau setup creates function metadata and binds the local image name:

```luau title="setup.luau"
--8<-- "demos/lambda-container/setup.luau"
```

After saving all three files, build the image from that directory:

```sh
podman build -t roto-lambda-container:latest -f Containerfile .
```

Use `docker build` with the same arguments if Docker is your container engine.
Then start Roto:

```sh
roto-server --ephemeral --host 0.0.0.0 --setup setup.luau
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

## Further examples

These source examples can be downloaded separately with their supporting files:

- [CloudFormation demo](https://github.com/vivainio/roto/tree/main/demos/cloudformation):
  create queues and a topic, use a nested stack, then import an exported value.
- [CDK demo](https://github.com/vivainio/roto/tree/main/demos/cdk): synthesize a TypeScript
  app and deploy its stack against a local Roto endpoint.
