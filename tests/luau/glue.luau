--!strict
local function call(op, input) return test.call("glue", op, input) end
local function fails(op, input, code)
    test.expect_error(code, function() return test.try_call("glue", op, input) end)
end
local function tableInput(name)
    return {Name = name, StorageDescriptor = {Columns = {{Name = "id", Type = "bigint"}, {Name = "payload", Type = "struct<x:string,y:array<int>>"}},
        Location = "s3://demo/data", InputFormat = "parquet", OutputFormat = "parquet", SerdeInfo = {SerializationLibrary = "demo", Parameters = {k = "v"}}},
        PartitionKeys = {{Name = "region", Type = "string"}, {Name = "year", Type = "int"}}, Parameters = {classification = "parquet"}, TableType = "EXTERNAL_TABLE"}
end

test.suite("Glue databases", function(suite)
    suite.before_all(function()
        call("CreateDatabase", {DatabaseInput = {Name = "Demo", Description = "catalog", Parameters = {source = "test"}}, Tags = {env = "test"}})
    end)
    suite.case("normalizes names, updates fields and checks catalog", function()
        test.equal(call("GetDatabase", {Name = "demo"}).Database.Name, "demo")
        fails("CreateDatabase", {DatabaseInput = {Name = "demo"}}, "AlreadyExistsException")
        call("UpdateDatabase", {Name = "demo", DatabaseInput = {Name = "demo", Description = "updated"}})
        test.equal(call("GetDatabase", {Name = "demo"}).Database.Description, "updated")
        fails("GetDatabases", {CatalogId = "999999999999"}, "InvalidInputException")
    end)
    suite.case("paginates databases", function()
        call("CreateDatabase", {DatabaseInput = {Name = "paged"}})
        local page = call("GetDatabases", {MaxResults = 1}); assert(page.NextToken)
        test.equal(#call("GetDatabases", {MaxResults = 1, NextToken = page.NextToken}).DatabaseList, 1)
        call("DeleteDatabase", {Name = "paged"})
    end)
    suite.case("tags resources with account validation", function()
        local arn = "arn:aws:glue:us-east-1:123456789012:database/demo"
        call("TagResource", {ResourceArn = arn, TagsToAdd = {env = "updated", drop = "x"}})
        call("UntagResource", {ResourceArn = arn, TagsToRemove = {"drop"}})
        test.equal(call("GetTags", {ResourceArn = arn}).Tags, {env = "updated"})
        fails("GetTags", {ResourceArn = "arn:aws:glue:us-east-1:999999999999:database/demo"}, "InvalidInputException")
        fails("StartJobRun", {JobName = "demo"}, "NotImplemented")
    end)
end)

test.suite("Glue tables", function(suite)
    suite.before_all(function() call("CreateDatabase", {DatabaseInput = {Name = "demo"}}) end)
    suite.case("versions, stale writes, deletion and numeric ordering", function()
        local ti = tableInput("versions")
        call("CreateTable", {DatabaseName = "demo", TableInput = ti})
        fails("CreateTable", {DatabaseName = "missing", TableInput = ti}, "EntityNotFoundException")
        fails("CreateTable", {DatabaseName = "demo", TableInput = ti}, "AlreadyExistsException")
        local current = call("GetTable", {DatabaseName = "demo", Name = "versions"}).Table
        test.equal(current.StorageDescriptor, ti.StorageDescriptor); test.equal(current.VersionId, "1")
        local updated: any = table.clone(ti); updated.Description = "version two"
        call("UpdateTable", {DatabaseName = "demo", TableInput = updated, VersionId = "1"})
        fails("UpdateTable", {DatabaseName = "demo", TableInput = updated, VersionId = "1"}, "ConcurrentModificationException")
        test.equal(call("GetTable", {DatabaseName = "demo", Name = "versions"}).Table.Description, "version two")
        assert(call("GetTableVersion", {DatabaseName = "demo", TableName = "versions", VersionId = "1"}).TableVersion.Table.Description == nil)
        local page = call("GetTableVersions", {DatabaseName = "demo", TableName = "versions", MaxResults = 1}); assert(page.NextToken)
        test.equal(call("GetTableVersions", {DatabaseName = "demo", TableName = "versions", NextToken = page.NextToken}).TableVersions[1].VersionId, "2")
        call("UpdateTable", {DatabaseName = "demo", TableInput = updated, VersionId = "2"})
        call("DeleteTableVersion", {DatabaseName = "demo", TableName = "versions", VersionId = "1"})
        local errors = call("BatchDeleteTableVersion", {DatabaseName = "demo", TableName = "versions", VersionIds = {"2", "999", "3"}}).Errors
        test.equal({errors[1].VersionId, errors[2].VersionId}, {"999", "3"})
        for _ = 1, 9 do call("UpdateTable", {DatabaseName = "demo", TableInput = {Name = "versions", Description = "persist"}}) end
        local versions = call("GetTableVersions", {DatabaseName = "demo", TableName = "versions"}).TableVersions
        test.equal(#versions, 10)
        for index, version in versions do test.equal(version.VersionId, tostring(index + 2)) end
        call("UpdateTable", {DatabaseName = "demo", TableInput = {Name = "versions"}, SkipArchive = true})
        fails("GetTableVersion", {DatabaseName = "demo", TableName = "versions", VersionId = "12"}, "EntityNotFoundException")
        fails("DeleteTableVersion", {DatabaseName = "demo", TableName = "versions", VersionId = "13"}, "InvalidInputException")
    end)
    suite.case("batch delete reports missing tables", function()
        call("CreateTable", {DatabaseName = "demo", TableInput = {Name = "temporary"}})
        local result = call("BatchDeleteTable", {DatabaseName = "demo", TablesToDelete = {"temporary", "missing"}})
        test.equal(result.Errors[1].ErrorDetail.ErrorCode, "EntityNotFoundException")
        fails("GetTable", {DatabaseName = "demo", Name = "temporary"}, "EntityNotFoundException")
    end)
    suite.case("deleting a table cascades to partitions", function()
        local ti = {Name = "cascade", PartitionKeys = {{Name = "p", Type = "string"}}}
        call("CreateTable", {DatabaseName = "demo", TableInput = ti})
        call("CreatePartition", {DatabaseName = "demo", TableName = "cascade", PartitionInput = {Values = {"a"}}})
        call("DeleteTable", {DatabaseName = "demo", Name = "cascade"})
        call("CreateTable", {DatabaseName = "demo", TableInput = ti})
        test.equal(call("GetPartitions", {DatabaseName = "demo", TableName = "cascade"}).Partitions, {})
    end)
end)

test.suite("Glue partitions", function(suite)
    local sd = tableInput("events").StorageDescriptor
    local function request(fields)
        local input = {DatabaseName = "demo", TableName = "events"}
        for key, value in fields do input[key] = value end
        return input
    end
    suite.before_all(function()
        call("CreateDatabase", {DatabaseInput = {Name = "demo"}})
        call("CreateTable", {DatabaseName = "demo", TableInput = tableInput("events")})
    end)
    suite.before_each(function()
        call("CreatePartition", request({PartitionInput = {Values = {"eu", "2025"}, StorageDescriptor = sd, Parameters = {batch = "one"}}}))
        call("CreatePartition", request({PartitionInput = {Values = {"us", "2026"}}}))
    end)
    suite.after_each(function()
        for _, part in call("GetPartitions", request({})).Partitions do
            call("DeletePartition", request({PartitionValues = part.Values}))
        end
    end)
    suite.case("batch creation returns item errors and rolls back malformed input", function()
        local result = call("BatchCreatePartition", request({PartitionInputList = {{Values = {"ap", "2026"}}, {Values = {"eu", "2025"}}, {Values = {"bad"}}}}))
        test.equal({result.Errors[1].ErrorDetail.ErrorCode, result.Errors[2].ErrorDetail.ErrorCode}, {"AlreadyExistsException", "InvalidInputException"})
        fails("BatchCreatePartition", request({PartitionInputList = {{Values = {"rollback", "2026"}}, {Values = "invalid"}}}), "InvalidInputException")
        local partitions = call("GetPartitions", request({})).Partitions
        test.equal(#partitions, 3)
        for _, part in partitions do assert(part.Values[1] ~= "rollback") end
    end)
    suite.case("filters, paginates and reads partitions", function()
        test.equal(call("GetPartitions", request({Expression = "region = 'eu' AND year >= 2025"})).Partitions[1].Values, {"eu", "2025"})
        test.equal(call("GetPartitions", request({Expression = "year > 2025"})).Partitions[1].Values, {"us", "2026"})
        test.equal(call("GetPartitions", request({Expression = "region = 'none'"})).Partitions, {})
        fails("GetPartitions", request({Expression = "region = 'eu' OR year = 2026"}), "InvalidInputException")
        fails("GetPartitions", request({Expression = "unknown = 'x'"}), "InvalidInputException")
        local page = call("GetPartitions", request({MaxResults = 1})); assert(page.NextToken)
        assert(#call("GetPartitions", request({NextToken = page.NextToken})).Partitions > 0)
        test.equal(call("GetPartition", request({PartitionValues = {"eu", "2025"}})).Partition.StorageDescriptor, sd)
    end)
    suite.case("updates and batch retrieves or deletes partitions", function()
        call("UpdatePartition", request({PartitionValueList = {"eu", "2025"}, PartitionInput = {Values = {"eu", "2026"}, Parameters = {batch = "updated"}}}))
        fails("UpdatePartition", request({PartitionValueList = {"eu", "2026"}, PartitionInput = {Values = {"us", "2026"}}}), "AlreadyExistsException")
        local got = call("BatchGetPartition", request({PartitionsToGet = {{Values = {"eu", "2026"}}, {Values = {"none", "2026"}}}}))
        test.equal(#got.Partitions, 1); test.equal(got.UnprocessedKeys, {{Values = {"none", "2026"}}})
        call("CreatePartition", request({PartitionInput = {Values = {"drop", "2026"}}}))
        call("DeletePartition", request({PartitionValues = {"drop", "2026"}}))
        local result = call("BatchDeletePartition", request({PartitionsToDelete = {{Values = {"us", "2026"}}, {Values = {"none", "2026"}}}}))
        test.equal(result.Errors[1].ErrorDetail.ErrorCode, "EntityNotFoundException")
        fails("UpdateTable", {DatabaseName = "demo", TableInput = {Name = "events", PartitionKeys = roto.array({})}}, "InvalidInputException")
    end)
    suite.case("parses quoted string values and numeric comparisons", function()
        call("CreatePartition", request({PartitionInput = {Values = {"O'Reilly AND Co", "10"}}}))
        call("CreatePartition", request({PartitionInput = {Values = {"number", "2"}}}))
        test.equal(call("GetPartitions", request({Expression = "region = 'O''Reilly AND Co' AND year > 9"})).Partitions[1].Values, {"O'Reilly AND Co", "10"})
        test.equal(#call("GetPartitions", request({Expression = "year > 9"})).Partitions, 3)
    end)
end)
