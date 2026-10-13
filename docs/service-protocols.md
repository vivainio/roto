# Service protocols

AWS services use four wire protocols. Roto handles each in both directions:
the server side decodes requests and encodes responses for real SDKs, and
`roto.call` does the opposite for setup Lua.

| Protocol | Services | Request | Response | Server codec | `roto.call` |
| --- | --- | --- | --- | --- | --- |
| `json` 1.0/1.1 | DynamoDB, SSM, Secrets Manager, Kinesis, KMS, Logs, EventBridge | POST `/`, `x-amz-target`, JSON body | JSON | `roto-protocol::json` | `JSON_SERVICES` |
| `query` | IAM, SNS, STS, CloudFormation | POST `/`, form body with `Action` | XML | `roto-protocol::query` | model-driven |
| `rest-xml` | S3 | method and path by operation, XML body | XML | `roto-protocol::restxml` | model-driven |
| `rest-json` | Lambda, API Gateway | method and path by operation, JSON body | JSON | `roto-protocol::restjson` | `roto.wire` only |

SQS is `json` on the wire but is called through its own dispatcher, as is Lambda.

## Server side

`roto-codegen` reads a botocore model and emits typed request and response
structs, parsing and serialisation, and an operation dispatcher into the
service crate's `generated.rs`. Handwritten code implements the operations on
those types. The protocol crate holds the shared pieces: the XML writer, query
parameter parsing, base64 and timestamp formats, and error envelopes.

The server picks a handler from the SigV4 credential scope (the service name),
falling back to `claims_unsigned` for unsigned calls.

## `roto.call` (client side)

`crates/roto-server/src/aws_client.rs` is a runtime interpreter of the same
botocore models, going the other way: Lua table in, `RawRequest` out, and the
`RawResponse` back into a PascalCase table. It never touches the network; it
calls the handler's `handle()` directly.

**JSON services** need no model. The table is the body and the operation name
goes in `x-amz-target`. Member names are not validated.

**`rest-xml`** (S3) routes every input member by its `location`:

- `uri`: substituted into `requestUri`; a `{Key+}` label keeps slashes.
- `querystring`: query parameters, lists repeated, maps expanded.
- `header` and `headers`: request headers (`headers` is a prefix map).
- everything else: the XML body, rooted at the `payload` member if the shape
  names one, otherwise at the input shape. Blob and string payloads are sent raw.

A literal query in `requestUri` such as `?notification` is kept.

**`query`** (IAM, SNS, STS, CloudFormation) flattens the input into
`Action`, `Version` and dotted keys:

- structures: `Parent.Member`
- lists: `Name.member.N`, or `Name.N` when flattened
- maps: `Name.entry.N.key` and `Name.entry.N.value`

**Responses** are read with the output shape. For `query`, the result sits under
the operation's `resultWrapper`. For `rest-xml`, `header` and `statusCode`
members come from the response metadata. Lists, maps, integers, booleans and
doubles are converted; timestamps and blobs stay strings.

Unknown input members raise an error naming the member, so typos fail at setup.

## Known limits

- Timestamps must be strings, and binary S3 bodies come back as lossy UTF-8
  (use `roto.wire` for those).
- `rest-json` has no client encoder yet.
- Operations with unusual serialisation (ACL grantee `xsi:type`, checksum
  headers) are not specially handled.
