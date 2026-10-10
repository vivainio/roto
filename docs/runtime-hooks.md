# Runtime Lua hooks (planned)

This proposal extends Lua setup from a startup-only resource seeder into a
runtime extension point. A setup script registers hooks before the server
starts; Roto retains the Lua runtime and calls registered hooks during later
resource creation and AWS request dispatch.

The primary resource use case is configuring a resource immediately after an
application creates it—for example, attaching a local executor to a Lambda
function created through an AWS API or CloudFormation. Request hooks are a
separate use case for failure injection and small local implementations of
unsupported APIs. This is not intended to emulate Lambda's packaged runtimes.
Existing command and HTTP Lambda executors remain the way to run a function
when Lambda Invoke is called.

## Post-create resource configuration

Setup registers a callback for a resource by creation event, service, and name.
For example, hooks.created("lambda", "worker", callback) registers a
callback for creation of the Lambda function named worker. When a matching
resource is successfully created at runtime, Roto invokes the callback with its type,
logical/name identifiers, physical identifier or ARN when available, account,
region, and creation properties. The callback can perform follow-up local
configuration, such as binding a command or HTTP executor to a newly created
Lambda function.

Illustrative syntax:

\`\`\`lua
hooks.created("lambda", "worker", function(resource)
  roto.lambda.bind(resource.arn, {
    command = {"sh", "./worker.sh"}
  })
end)
\`\`\`

The callback runs after the resource creation succeeds; it does not replace the
create operation or its AWS response. Creation callbacks are distinct from
interceptors, which run before a service operation. Callback failure semantics
need to be defined because the resource may already be persisted when the
callback runs.

The initial design needs to settle how resource names match across creation
paths, such as CloudFormation logical IDs, physical names, and ARNs; whether
callbacks run for creation only or also for updates; and how duplicate
deliveries/retries are handled. A callback can only attach an executor after
the Lambda service has created its function record. CloudFormation now supports creating and deleting `AWS::Lambda::Function`;
invoking a post-create callback from that path still requires runtime hook support.

## Two hook types

### intercept_request

Register an interceptor for one exact service operation, for example
s3.PutObject or dynamodb.PutItem. It runs before the native service
handler. An interceptor may inspect the request, return a simulated AWS
response or error, or continue to normal dispatch. This supports failure
injection on both implemented and not-yet-implemented operations.

Registration is operation-specific; there is no global interceptor in the
initial design. Calls that do not match a registered service operation go
through normal dispatch without running a Lua callback.

### missing_support

Register one catch-all fallback for calls the native dispatcher cannot handle.
It receives service and operation information when available, along with the
request context, and may return a response or error. If it declines the call,
Roto returns the existing unsupported-operation error.

This hook is not called for normal service errors such as validation failures,
missing resources, or conflicts. Those errors come from an operation Roto
already handles. A fallback can branch on service and operation itself; this
keeps the registration API small while allowing many local stubs.

## Dispatch order

1. Resolve the incoming AWS request to a service and operation.
2. Run a matching intercept_request hook, if registered. It may short-circuit
   with a response or continue.
3. Run the native service handler.
4. If native dispatch cannot handle the call, run missing_support, if
   registered. Use its response when it handles the call; otherwise preserve
   Roto's current unsupported response.
5. After a native resource creation succeeds and its state is committed, run
   matching post-create config callbacks before returning the AWS response.

An interceptor that implements an operation short-circuits before native
dispatch, so the catch-all is not called. Setup-time helper calls such as
roto.call are configuration work and do not trigger runtime hooks.

## Lua lifetime and execution

The current setup implementation creates a Lua VM, runs the setup file, and
drops the VM before listening. Runtime hooks require retaining the VM or
compiling registrations into a runtime-safe representation. With no matching
hooks, request dispatch should proceed directly to the native handler without
entering Lua.

Before implementation, settle how callbacks interact with concurrent requests:
serialize Lua callback execution or use a dedicated hook worker. Also define
callback time limits, error conversion, and whether a timed-out or failed hook
returns an AWS error or falls through. Hooks should not hold service locks while
running Lua or waiting on an external executor.

## Request and response contract

The callback needs enough context to make decisions: service, operation,
account, region, HTTP method and path, headers, query parameters, and body.
Hook access to signed credential data must not expose the signing secret. The
interface should distinguish an unhandled result from an intentional empty
response.

There are two viable response contracts:

- Return raw HTTP status, headers, and body. This can represent any AWS wire
  protocol, but scripts must serialize protocol-specific AWS responses.
- Return an AWS-shaped success value or error and let Roto serialize it using
  the service operation's protocol model. This is easier to use but requires
  model metadata and typed response support for operations Roto does not
  currently implement.

Choose the contract before implementing missing_support; it determines
whether a Lua fallback can support an arbitrary unknown operation or only
operations with known protocol metadata.

## Lambda function creation

Lua can already bind command or HTTP executors to Lambda function metadata
during startup. A future intercept_request for lambda.CreateFunction could
also bind an executor when an application creates a function through the
Lambda API. CloudFormation now supports creating `AWS::Lambda::Function`;
runtime post-create hooks would let the same binding pattern work for
CloudFormation-created functions.

## Initial implementation sequence

1. Define config callback matching keys and payloads, callback names, handled/
   continue values, and raw-versus-model-aware response behavior.
2. Retain or safely compile setup registrations into a runtime registry. Keep
   the no-hook dispatch path direct.
3. Add post-create callbacks for supported resource creation paths. Verify
   callbacks see committed resource metadata and can register Lambda executors.
4. Add exact-operation interception before native dispatch, including
   pass-through and deterministic response/error short-circuiting.
5. Add the catch-all missing_support fallback after native dispatch reports
   an unsupported or unroutable call. Preserve the existing AWS error if it
   declines.
6. Add a Lua hook example for iotdata.Publish, then one for injecting a
   controlled failure into an implemented operation.
7. Verify hook ordering, unsupported fallthrough, ordinary service errors,
   operation identification across protocols, concurrent requests, timeout
   behavior, and that hook-free calls retain their existing behavior.

This plan describes future work; runtime Lua hooks are not implemented.
See the [project plan](https://github.com/vivainio/roto/blob/main/PLAN.md).
