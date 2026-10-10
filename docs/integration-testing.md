# Integration testing with request traces

An integration test can check both its result and the AWS API calls that
produced it. Give the scenario a unique fake access key beginning with
`ROTO`, run the AWS clients against roto, then query
`/roto-api/trace?trace_id=<access-key>` and assert the expected operations.
Roto keeps the latest 10,000 requests in memory, so this works without
`--trace` or a trace file.

For example, this xUnit test checks that an S3 upload succeeds and that roto
observed a successful `PutObject` request:

```csharp
using System.Net.Http.Json;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Xunit;

public class UploadTests
{
    [Fact]
    public async Task Upload_is_sent_to_s3()
    {
        var traceId = $"ROTO_upload_{Guid.NewGuid():N}";
        var credentials = new BasicAWSCredentials(traceId, "local-secret");
        var config = new AmazonS3Config
        {
            ServiceURL = "http://localhost:5070",
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1"
        };

        using var s3 = new AmazonS3Client(credentials, config);
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = "integration-tests",
            Key = "hello.txt",
            ContentBody = "hello"
        });

        using var http = new HttpClient();
        var url = "http://localhost:5070/roto-api/trace?trace_id=" +
                  Uri.EscapeDataString(traceId);
        using var trace = await http.GetFromJsonAsync<JsonDocument>(url);
        Assert.NotNull(trace);

        var calls = trace.RootElement.GetProperty("entries").EnumerateArray();
        Assert.Contains(calls, call =>
            call.GetProperty("service").GetString() == "s3" &&
            call.GetProperty("operation").GetString() == "PutObject" &&
            call.GetProperty("outcome").GetString() == "success");
    }
}
```

Start a disposable server before running the test:

```sh
roto-server --ephemeral --port 5070
```

In a test fixture, start the server once for the test collection and stop it
afterward; wait for `/roto-api/health` before creating clients. A unique trace
ID keeps parallel tests' calls separate. The trace assertion checks that the
SDK made the expected successful API request; keep ordinary state or response
assertions too when the test needs to verify the resulting resource behavior.

For longer investigations, add `--trace calls.jsonl` to preserve the full
history on disk. The in-memory endpoint remains available and is useful for
assertions even without that option. See [Server reference](server.md#finding-unsupported-api-calls)
for the trace fields, filtering, and coverage report.

## Lua service contracts in CI

The `lua-services` job has separate matrix entries for CUR, ACM, Budgets, Config,
Cloud Map, OpenSearch, RDS, ECR, ECS and Glue. Each entry builds the server, installs pinned boto3, and runs:

```sh
python3 tests/smoke-lua-services.py --service acm --sdk
```

Replace `acm` with `cur`, `budgets`, `config`, `servicediscovery`, `opensearch`,
`rds`, `ecr`, `ecs`, or `glue`. The runner
starts and cleans up disposable servers on free ports. It checks each service's
supported operations, modeled failures, account/region isolation, persistence
through restart, and reset, using both a directory bundle and a ZIP bundle.
SDK mode also checks model field names/types, serialization and response parsing.
Without `--sdk`, it needs only Python's standard library. Omit `--service` to run
all ten. Build first with `cargo build -p roto-server --locked`.

These focused contracts are adapted from MiniStack tests and cover the implemented
subsets. They are separate from the Moto operation-coverage reports.
