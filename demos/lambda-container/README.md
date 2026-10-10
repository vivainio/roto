# Lambda container invocation

This demo builds a small AWS Lambda Python image, then has Roto invoke it through the
Runtime Interface Emulator (RIE). Roto only needs the image name in Lua; it does not build
or pull the image.

Build the image with Podman or Docker:

```sh
podman build -t roto-lambda-container:latest \
  -f demos/lambda-container/Containerfile demos/lambda-container
```

Start Roto with the Lua setup:

```sh
cargo run -p roto-server -- --ephemeral --host 0.0.0.0 \
  --setup demos/lambda-container/setup.lua
```

Invoke the function:

```sh
aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name container-echo \
  --cli-binary-format raw-in-base64-out \
  --payload '{"hello":"container"}' result.json
cat result.json
```

The result includes the event and the response from `sts:GetCallerIdentity`, called by boto3
inside the container through `AWS_ENDPOINT_URL`. Roto prefers Podman when it is on `PATH`, and
falls back to Docker. It starts one container for each invocation, publishes container port 8080
on an ephemeral loopback port, calls RIE's invocation endpoint, reads the container logs, and
removes the container.

The AWS Lambda Python base image supplies boto3, the runtime client, and RIE-compatible entrypoint.
The executor assumes that the image is already present locally. Increase the function timeout
if image startup takes longer on your machine. The `0.0.0.0` listen address lets the container
reach Roto through the host alias that the executor uses for `AWS_ENDPOINT_URL`.
