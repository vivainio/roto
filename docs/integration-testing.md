# Integration testing with request traces

An integration test can check both its result and the AWS API calls that
produced it. Give the scenario a unique fake access key beginning with
`ROTO`, run the AWS clients against roto, then query
`/roto-api/trace?trace_id=<access-key>` and assert the expected operations.
Roto keeps the latest 10,000 requests in memory, so this works without
`--trace` or a trace file.

Start a disposable server before running the test:

```sh
roto-server --ephemeral --port 5070
```

The example assumes a .NET test project with xUnit and the `AWSSDK.S3` package.
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
        await s3.PutBucketAsync(new PutBucketRequest { BucketName = "integration-tests" });
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

In a test fixture, start a fresh `--ephemeral` server for each test or test group
when you want isolated state. Roto's measured [startup time and idle memory](benchmarks.md)
make repeated launches practical. Use the installed `roto-server` binary, choose a free port for
each process, and stop it in fixture cleanup. A server per test collection also
works when tests intentionally share resources. Wait for `/roto-api/health`
before creating clients. A unique trace
ID keeps parallel tests' calls separate. The trace assertion checks that the
SDK made the expected successful API request; keep ordinary state or response
assertions too when the test needs to verify the resulting resource behavior.

For longer investigations, add `--trace calls.jsonl` to preserve the full
history on disk. The in-memory endpoint remains available and is useful for
assertions even without that option. See [Server reference](server.md#finding-unsupported-api-calls)
for the trace fields, filtering, and coverage report.
