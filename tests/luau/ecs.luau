--!strict
local function call(op, input) return test.call("ecs", op, input) end
local function fails(op, input, code)
    test.expect_error(code, function() return test.try_call("ecs", op, input) end)
end
local function definition(family)
    return {family = family, containerDefinitions = {{name = "web", image = "demo:latest", environment = {{name = "MODE", value = "test"}}, portMappings = {{containerPort = 8080}}}},
        networkMode = "awsvpc", requiresCompatibilities = {"FARGATE"}, cpu = "256", memory = "512", tags = {{key = "env", value = "test"}}}
end

test.suite("ECS clusters", function(suite)
    local arn = ""
    suite.before_all(function()
        local cluster = call("CreateCluster", {clusterName = "demo", tags = {{key = "env", value = "test"}}}).cluster
        arn = cluster.clusterArn
        test.equal(cluster.runningTasksCount, 0); test.equal(cluster.pendingTasksCount, 0)
    end)
    suite.case("updates configuration and reports missing clusters", function()
        call("UpdateCluster", {cluster = "demo", configuration = {executeCommandConfiguration = {logging = "NONE"}}, settings = {{name = "containerInsights", value = "disabled"}}})
        test.equal(call("DescribeClusters", {clusters = {"demo"}}).clusters[1].settings[1].value, "disabled")
        call("UpdateClusterSettings", {cluster = arn, settings = {{name = "containerInsights", value = "enabled"}}})
        test.equal(call("DescribeClusters", {clusters = {"demo", "missing"}, include = {"TAGS"}}).failures, {{arn = "missing", reason = "MISSING"}})
    end)
    suite.case("paginates and deletes clusters", function()
        call("CreateCluster", {clusterName = "temporary"})
        local page = call("ListClusters", {maxResults = 1}); assert(page.nextToken)
        assert(#call("ListClusters", {nextToken = page.nextToken}).clusterArns > 0)
        call("DeleteCluster", {cluster = "temporary"})
    end)
    suite.case("updates cluster tags and checks account scope", function()
        call("TagResource", {resourceArn = arn, tags = {{key = "env", value = "updated"}, {key = "drop", value = "x"}}})
        call("UntagResource", {resourceArn = arn, tagKeys = {"drop"}})
        test.equal(call("ListTagsForResource", {resourceArn = arn}).tags, {{key = "env", value = "updated"}})
        fails("ListTagsForResource", {resourceArn = string.gsub(arn, "123456789012", "999999999999")}, "InvalidParameterException")
    end)
end)

test.suite("ECS task definitions", function(suite)
    local first = ""
    suite.before_all(function()
        local d1 = call("RegisterTaskDefinition", definition("web")).taskDefinition
        local d2 = call("RegisterTaskDefinition", definition("web")).taskDefinition
        first = d1.taskDefinitionArn
        test.equal(d1.revision, 1); test.equal(d2.revision, 2)
    end)
    suite.case("describes latest and tagged revisions", function()
        test.equal(call("DescribeTaskDefinition", {taskDefinition = "web"}).taskDefinition.revision, 2)
        test.equal(call("DescribeTaskDefinition", {taskDefinition = first, include = {"TAGS"}}).tags, {{key = "env", value = "test"}})
        test.equal(#call("ListTaskDefinitions", {familyPrefix = "we"}).taskDefinitionArns, 2)
        test.equal(call("ListTaskDefinitionFamilies", {status = "ACTIVE"}).families, {"web"})
    end)
    suite.case("sorts revisions numerically", function()
        for _ = 1, 12 do call("RegisterTaskDefinition", {family = "ordered", containerDefinitions = {{name = "main", image = "demo"}}}) end
        local revisions = call("ListTaskDefinitions", {familyPrefix = "ordered", sort = "DESC"}).taskDefinitionArns
        assert(string.match(revisions[1], ":12$")); assert(string.match(revisions[#revisions], ":1$"))
    end)
    suite.case("deregisters a revision without removing it", function()
        call("DeregisterTaskDefinition", {taskDefinition = "web:1"})
        test.equal(call("ListTaskDefinitions", {status = "INACTIVE"}).taskDefinitionArns, {first})
        test.equal(call("DescribeTaskDefinition", {taskDefinition = "web:1"}).taskDefinition.status, "INACTIVE")
    end)
end)

test.suite("ECS services and tasks", function(suite)
    local clusterArn, definitionArn = "", ""
    suite.before_all(function()
        clusterArn = call("CreateCluster", {clusterName = "demo", tags = {{key = "env", value = "test"}}}).cluster.clusterArn
        call("RegisterTaskDefinition", definition("web"))
        definitionArn = call("RegisterTaskDefinition", definition("web")).taskDefinition.taskDefinitionArn
        call("DeregisterTaskDefinition", {taskDefinition = "web:1"})
    end)
    suite.case("validates service dependencies and manages service state", function()
        fails("CreateService", {cluster = "missing", serviceName = "api", taskDefinition = "web"}, "ClusterNotFoundException")
        fails("CreateService", {cluster = "demo", serviceName = "api", taskDefinition = "web:1"}, "ClientException")
        local service = call("CreateService", {cluster = "demo", serviceName = "api", taskDefinition = "web", desiredCount = 2, launchType = "FARGATE", role = "service-role",
            networkConfiguration = {awsvpcConfiguration = {subnets = {"subnet-demo"}, assignPublicIp = "DISABLED"}}}).service
        test.equal(service.runningCount, 0); test.equal(service.pendingCount, 0); test.equal(service.deployments, {})
        test.equal(service.roleArn, "arn:aws:iam::123456789012:role/service-role")
        test.equal(call("DescribeClusters", {clusters = {"demo"}}).clusters[1].activeServicesCount, 1)
        test.equal(call("ListServices", {cluster = "demo", launchType = "FARGATE"}).serviceArns, {service.serviceArn})
        test.equal(call("DescribeServices", {cluster = "demo", services = {"api", "missing"}}).failures, {{arn = "missing", reason = "MISSING"}})
        fails("DeleteCluster", {cluster = "demo"}, "ClusterContainsServicesException")
        fails("DeleteService", {cluster = "demo", service = "api"}, "InvalidParameterException")
        call("UpdateService", {cluster = "demo", service = "api", desiredCount = 1})
        fails("UpdateService", {cluster = "demo", service = "api", forceNewDeployment = true}, "NotImplemented")
        for _, resource in {clusterArn, definitionArn, service.serviceArn} do
            call("TagResource", {resourceArn = resource, tags = {{key = "env", value = "updated"}, {key = "drop", value = "x"}}})
            call("UntagResource", {resourceArn = resource, tagKeys = {"drop"}})
            test.equal(call("ListTagsForResource", {resourceArn = resource}).tags, {{key = "env", value = "updated"}})
        end
        call("DeleteService", {cluster = "demo", service = "api", force = true})
        test.equal(call("DescribeServices", {cluster = "demo", services = {"api"}}).services[1].status, "INACTIVE")
        test.equal(call("ListServices", {cluster = "demo"}).serviceArns, {})
        test.equal(call("CreateService", {cluster = "demo", serviceName = "api", taskDefinition = "web:2", desiredCount = 3}).service.desiredCount, 3)
    end)
    suite.case("runs and stops tasks", function()
        local task = call("RunTask", {cluster = "demo", taskDefinition = "web"}).tasks[1]
        test.equal(task.lastStatus, "PROVISIONING")
        test.equal(call("DescribeTasks", {cluster = "demo", tasks = {task.taskArn, "missing"}}).failures[1].reason, "MISSING")
        test.equal(call("ListTasks", {cluster = "demo"}).taskArns, {task.taskArn})
        test.equal(call("StopTask", {cluster = "demo", task = task.taskArn, reason = "done"}).task.lastStatus, "STOPPING")
        call("RotoSetTaskStatus", {cluster = "demo", task = task.taskArn, lastStatus = "STOPPED", exitCode = 2})
        test.equal(call("DescribeTasks", {cluster = "demo", tasks = {task.taskArn}}).tasks[1].containers[1].exitCode, 2)
        test.equal(call("ListTasks", {cluster = "demo"}).taskArns, {})
        fails("StartTask", {cluster = "demo", taskDefinition = "web", containerInstances = {"ci"}}, "NotImplemented")
    end)
end)
