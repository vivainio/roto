--!strict
-- In-process checks of the Luau ECR repository service.
local function call(operation, input)
    return test.call("ecr", operation, input)
end

test.case("ECR repository CRUD returns defaults and removes the repository", function()
    local created = call("CreateRepository", {repositoryName = "team/app"}).repository
    test.equal(created.repositoryName, "team/app")
    test.equal(created.imageTagMutability, "MUTABLE")
    test.equal(created.imageScanningConfiguration.scanOnPush, false)
    test.equal(call("DescribeRepositories", {repositoryNames = {"team/app"}}).repositories[1].repositoryArn, created.repositoryArn)
    test.equal(call("DeleteRepository", {repositoryName = "team/app"}).repository.repositoryName, "team/app")
    test.expect_error("RepositoryNotFoundException", function()
        return test.try_call("ecr", "DescribeRepositories", {repositoryNames = {"team/app"}})
    end)
end)

test.case("ECR duplicate and missing repositories return structured errors", function()
    call("CreateRepository", {repositoryName = "app"})
    test.expect_error("RepositoryAlreadyExistsException", function()
        return test.try_call("ecr", "CreateRepository", {repositoryName = "app"})
    end)
    test.expect_error("RepositoryNotFoundException", function()
        return test.try_call("ecr", "DeleteRepository", {repositoryName = "missing"})
    end)
    test.equal(#call("DescribeRepositories", {}).repositories, 1)
end)

test.case("ECR rejects invalid mutability without changing configuration", function()
    call("CreateRepository", {repositoryName = "app", imageTagMutability = "IMMUTABLE"})
    test.expect_error("InvalidParameterException", function()
        return test.try_call("ecr", "PutImageTagMutability", {repositoryName = "app", imageTagMutability = "MUTABLE_WITH_EXCLUSION"})
    end)
    test.equal(call("DescribeRepositories", {repositoryNames = {"app"}}).repositories[1].imageTagMutability, "IMMUTABLE")
end)

test.case("ECR tags can be updated and removed", function()
    local arn = call("CreateRepository", {repositoryName = "app", tags = {{Key = "team", Value = "one"}}}).repository.repositoryArn
    call("TagResource", {resourceArn = arn, tags = {{Key = "team", Value = "two"}, {Key = "env", Value = "test"}}})
    call("UntagResource", {resourceArn = arn, tagKeys = {"env"}})
    test.equal(call("ListTagsForResource", {resourceArn = arn}).tags, {{Key = "team", Value = "two"}})
end)

test.case("ECR lists repositories across pages", function()
    for _, name in {"aa", "bb", "cc"} do call("CreateRepository", {repositoryName = name}) end
    local first = call("DescribeRepositories", {maxResults = 2})
    test.equal(#first.repositories, 2)
    assert(first.nextToken)
    local second = call("DescribeRepositories", {maxResults = 2, nextToken = first.nextToken})
    test.equal(#second.repositories, 1)
    assert(second.nextToken == nil)
    local names = {}
    for _, repo in first.repositories do names[repo.repositoryName] = true end
    for _, repo in second.repositories do names[repo.repositoryName] = true end
    test.equal(names, {aa = true, bb = true, cc = true})
end)
