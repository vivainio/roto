# RDS Data API

Roto's RDS Data API runs SQL against a PostgreSQL server you provide. Applications
use the usual `rds-data` client pointed at Roto; a Luau hook selects the PostgreSQL
connection. The Data API is built into `roto-server` and needs no optional service
bundle.

This is separate from the optional RDS service, which stores instance and cluster
configuration. Creating an RDS instance in Roto does not start a database, and
you do not need to create one to use the Data API.

## Connect PostgreSQL

You need a running PostgreSQL server and an existing database. Save this as
`setup.luau`, replacing the connection string with your database's details:

```luau
--!strict
roto.hooks.rds_data_connection(function(request: RdsDataConnectionRequest, context: HookContext)
  return "postgres://postgres:pw@localhost:5432/app"
end)
```

Start Roto:

```sh
roto-server --ephemeral --setup setup.luau
```

The hook receives `resource_arn`, `secret_arn`, and `database` in `request`, and
`account_id` and `region` in `context`. It can select different databases for
different requests. Return `nil` to reject a request. This example sends all
requests to the same database; the supplied ARNs are local placeholders.

## Execute a statement

With boto3 installed, run:

```python
import boto3

data = boto3.client(
    "rds-data",
    endpoint_url="http://localhost:5070",
    region_name="us-east-1",
    aws_access_key_id="testing",
    aws_secret_access_key="testing",
)
result = data.execute_statement(
    resourceArn="arn:aws:rds:us-east-1:123456789012:cluster:local",
    secretArn="arn:aws:secretsmanager:us-east-1:123456789012:secret:local",
    sql="SELECT :name AS greeting",
    parameters=[{"name": "name", "value": {"stringValue": "hello"}}],
    includeResultMetadata=True,
)
print(result["records"])
```

The result contains `hello` as a string field. Statements execute against the
configured PostgreSQL database, so SQL writes change that database even when
Roto uses `--ephemeral`.

## Supported behavior and limits

Supported operations include `ExecuteStatement`, `BatchExecuteStatement`,
`BeginTransaction`, `CommitTransaction`, and `RollbackTransaction`. Statements
accept named parameters, typed values, result metadata, and JSON-formatted
results. Pass a transaction ID to execute statements in a transaction;
transactions expire after three minutes.

Connections use plain TCP without TLS. Roto does not validate the secret ARN or
load credentials from Secrets Manager. The `database` argument is passed to the
hook; choose the actual database in the connection string. Array parameters and
results and Aurora-specific behavior are unsupported. See the
[connection hook reference](lua-api.md#rotohooksrds_data_connectioncallback)
for parameter conversion details.
