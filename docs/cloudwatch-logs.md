# CloudWatch Logs subset

Roto includes a small CloudWatch Logs API as an example of a simple persistent
resource service. It supports log-group create, list, and delete; retention
updates; tags; and basic `FilterLogEvents` queries. Lambda executor output is
written to `/aws/lambda/<function-name>` automatically.

Create a group and set its retention:

```sh
aws --endpoint-url http://localhost:5070 logs create-log-group \
  --log-group-name /demo/worker
aws --endpoint-url http://localhost:5070 logs put-retention-policy \
  --log-group-name /demo/worker --retention-in-days 14
aws --endpoint-url http://localhost:5070 logs describe-log-groups \
  --log-group-name-prefix /demo/
```

After invoking a locally bound Lambda, query its captured output:

```sh
aws --endpoint-url http://localhost:5070 logs filter-log-events \
  --log-group-name /aws/lambda/echo
```

`filterPattern` is treated as a literal substring, optionally wrapped in
quotes. Roto does not evaluate the full CloudWatch filter pattern language.
Retention days and tags are stored and returned, but retention does not delete
old events. Group and event state is isolated by account and region and persists
with the normal Roto data directory.

The implementation supports `CreateLogGroup`, `DescribeLogGroups`,
`DeleteLogGroup`, `PutRetentionPolicy`, `DeleteRetentionPolicy`,
`TagResource`, `UntagResource`, `ListTagsForResource`, and `FilterLogEvents`.
Log streams, direct `PutLogEvents`, CloudWatch Logs Insights, subscriptions,
KMS encryption, and advanced filter patterns are outside this slice. See the
[minimal CRUD development note](development.md#minimal-crud-service-example)
for the service's Rust structure.
