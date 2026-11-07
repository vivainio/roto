# Durable functions

Run your SDK-decorated Lambda handler against Roto's local Lambda service.
Roto stores checkpoints, suspends waits, and invokes the handler again when work
is ready. The durable SDK handles replay and returns saved results for completed
operations. A container is optional.

## Choose how to run the handler

| Option | Where it runs | What you install | Use it when |
| --- | --- | --- | --- |
| Roto's packaged Python adapter | Native macOS or Linux process | `roto-aws` and your function dependencies in its virtual environment | You want local Python development without a container or a copied adapter script. |
| AWS Python Runtime Interface Client | Native Linux process | `awslambdaric` and your function dependencies | You want to use AWS's runtime client on Linux. |
| Lambda container image | Linux container through Podman or Docker | A local image containing your handler, runtime, durable SDK, and Runtime Interface Emulator (RIE) | You want the function's runtime and dependencies packaged together. |
| Another custom runtime | Local executable | Your runtime and its dependencies | You already have a bootstrap that speaks the Lambda Runtime API. |

Each option uses the same Lambda create/invoke APIs and Roto checkpoint store.
Roto's server is Rust. The Python adapter is a standard-library-only module in
the `roto-aws` PyPI package; it runs in your selected Python interpreter.

## Native Python with the packaged adapter

Install Roto and the durable SDK in the function's environment:

```sh
python3.14 -m venv .venv
.venv/bin/python -m pip install roto-aws aws-durable-execution-sdk-python
```

If you installed the server with `uv tool install roto-aws`, also install
`roto-aws` in this virtual environment. The adapter needs access to the same
dependencies as your handler; the tool environment is separate.

Save the function as `app.py`:

```python
from aws_durable_execution_sdk_python import durable_execution, durable_step
from aws_durable_execution_sdk_python.config import Duration


@durable_step
def double(context, value):
    print(f"Doubling {value}")
    return value * 2


@durable_execution
def handler(event, context):
    first = context.step(double(event["value"]), name="first")
    context.wait(Duration.from_seconds(5), name="pause")
    second = context.step(double(first), name="second")
    return {"value": second}
```

Save this binding beside it as `setup.luau`:

```luau
--!strict
roto.lambda.bind("workflow", {
  runtime = {".venv/bin/python", "-m", "roto_aws.lambda_runtime", "app.handler"},
})
```

The working directory defaults to the setup file's directory. Use `cwd` for a
different project directory and an absolute interpreter path when the virtual
environment lives elsewhere. `app.handler` means the callable named `handler`
in the importable module `app`.

Start Roto in one terminal:

```sh
.venv/bin/roto-server --ephemeral --setup setup.luau
```

The adapter loads the handler once per worker process, builds the Lambda context
from Roto's Runtime API, and serves successive invocations. Handler errors are
reported through that API; stdout, stderr, and tracebacks become invocation logs.
The context includes the request ID, function name/version, memory setting,
client context, and a remaining-time method based on the invocation deadline.
Roto enforces the timeout and discards a timed-out worker.

### Create and invoke the durable function

In another terminal, create the function using the AWS CLI or your normal AWS
SDK deployment code. The binding above selects the local executable;
`DurableConfig` enables durable execution in the function metadata.

```sh
export AWS_ACCESS_KEY_ID=testing
export AWS_SECRET_ACCESS_KEY=testing
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:5070 lambda create-function \
  --function-name workflow \
  --runtime provided.al2023 --handler app.handler \
  --role arn:aws:iam::123456789012:role/local \
  --code '{"ZipFile":""}' --timeout 30 \
  --durable-config '{"ExecutionTimeout":120,"RetentionPeriodInDays":1}'

aws --endpoint-url http://localhost:5070 lambda invoke \
  --function-name 'workflow:$LATEST' \
  --cli-binary-format raw-in-base64-out \
  --payload '{"value":7}' result.json
cat result.json
# {"value": 28}
```

The call waits through the five-second pause. Use `InvocationType="Event"` in
your SDK to start asynchronously; the response includes a `DurableExecutionArn`.
Poll `GetDurableExecution` to retrieve its status and result, or use the callback
APIs for workflows waiting on external work. Published versions and aliases can
replace `$LATEST` in the qualified function name.

Use a persistent store by omitting `--ephemeral` when you want checkpoints to
survive restarting Roto. Reapply the executor binding through the setup file at
startup. The first completed step is replayed from its stored result after the
wait. Side effects inside a step can still repeat if the process fails before
its result is checkpointed.

## AWS's Python runtime client on Linux

Install `awslambdaric` in the function's virtual environment and replace the
binding with:

```luau
--!strict
roto.lambda.bind("workflow", {
  runtime = {".venv/bin/python", "-m", "awslambdaric", "app.handler"},
})
```

Roto supplies `AWS_LAMBDA_RUNTIME_API`, function environment variables, invocation
headers, and the service endpoint. AWS's client supplies the Python context and
handler loop. Its required native module is built only on Linux, so installing
the package on macOS alone does not provide a working native runtime. See
[AWS's runtime client](https://github.com/aws/aws-lambda-python-runtime-interface-client).

## A Lambda container image

For a Python image, one starting point is an
[AWS Lambda base image](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html),
which includes the runtime client and RIE:

```dockerfile
FROM public.ecr.aws/lambda/python:3.14
RUN pip install aws-durable-execution-sdk-python
COPY app.py ${LAMBDA_TASK_ROOT}/app.py
CMD ["app.handler"]
```

Build it locally, then bind the image name:

```sh
podman build -t workflow:local .
```

```luau
--!strict
roto.lambda.bind("workflow", {
  container = "workflow:local",
  warm = true,
})
```

Roto uses Podman when available, or Docker otherwise. The image must already
exist locally and start RIE on port 8080; Roto does not build or pull it.
The same durable function metadata and invocation flow shown above apply to
this local executor. See [Lambda container images](lambda.md#lambda-container-images)
for details, including warm workers and image requirements.

## Other custom runtimes

Bind any executable that implements the Lambda Runtime API with
`runtime = {"/path/to/bootstrap"}`. It must pass the durable invocation envelope
to its language's durable SDK and report that SDK's result to the Runtime API.
See [Custom runtime binaries](lambda.md#custom-runtime-binaries) for the process
lifecycle and environment contract.
