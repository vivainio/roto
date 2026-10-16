# Warm and cold Lambda containers

Build the image, then start a disposable Roto server:

```sh
podman build -t roto-lambda-warm:latest -f demos/lambda-warm/Containerfile demos/lambda-warm
cargo run -p roto-server -- --ephemeral --setup demos/lambda-warm/setup.luau
```

Docker works with the same build arguments. The image uses the Python Lambda base
image with RIE and needs no additional Python packages.

In another terminal, invoke `warm-counter` twice with local AWS credentials:

```sh
AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name warm-counter --cli-binary-format raw-in-base64-out \
  --payload '{"label":"hello"}' result.json
cat result.json
```

The `count` increases from 1 to 2 because the handler's module global survives.
Invoke `cold-counter` instead and each invocation returns `count: 1`.

Use `{"fail":true}` to trigger a handler error while retaining warm state, or
`{"sleep":31}` to trigger the configured 30-second timeout and discard the container.
Restarting Roto clears the warm pool. See [Lambda executors](../../docs/lambda.md)
for pool limits, lifecycle, invocation metadata, and other executor examples.

After building this image, run the optional real-container regression test:

```sh
cargo test -p roto-svc-lambda rie_reuses -- --ignored
```
