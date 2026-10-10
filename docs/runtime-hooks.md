# Runtime Lua hooks (planned)

This proposal extends Lua setup from a startup-only resource seeder into a
runtime extension point. A setup script registers hooks before the server
starts; Roto retains the Lua runtime and consults the registered hooks while
dispatching later AWS requests.

The first use cases are local failure injection for implemented APIs and
providing small local implementations for unsupported APIs. This is not
intended to emulate Lambda's packaged runtimes. Existing command and HTTP Lambda
executors remain the way to run a function when Lambda Invoke is called.

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
4. If no native handler supports the call, run missing_support, if
   registered. Use its response when it handles the call; otherwise preserve
   Roto's current unsupported response.

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
Lambda API. CloudFormation currently does not support AWS::Lambda::Function;
that resource would need separate support before the same pattern works for
CloudFormation-created functions.

## Initial implementation sequence

1. Define Lua registration names, callback arguments, handled/continue values,
   and raw-versus-model-aware response behavior.
2. Retain or safely compile setup registrations into a runtime registry. Keep
   the no-hook dispatch path direct.
3. Add exact-operation interception before native dispatch, including
   pass-through and deterministic response/error short-circuiting.
4. Add the catch-all missing_support fallback after native dispatch reports
   an unsupported or unroutable call. Preserve the existing AWS error if it
   declines.
5. Add a Lua hook example for iotdata.Publish, then one for injecting a
   controlled failure into an implemented operation.
6. Verify hook ordering, unsupported fallthrough, ordinary service errors,
   operation identification across protocols, concurrent requests, timeout
   behavior, and that hook-free calls retain their existing behavior.

This plan describes future work; runtime Lua hooks are not implemented.
See the [project plan](https://github.com/vivainio/roto/blob/main/PLAN.md).
