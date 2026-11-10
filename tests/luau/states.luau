--!strict
-- The runner supplies `test` and `roto`.
local function machine(name, definition)
    return test.call("states", "CreateStateMachine", {
        name = name,
        roleArn = "arn:aws:iam::123456789012:role/test",
        definition = roto.json_encode(definition),
    }).stateMachineArn
end

test.case("pass evaluates paths and intrinsics", function()
    local arn = machine("paths", {StartAt = "Shape", States = {
        Shape = {Type = "Pass", Parameters = { ["greeting.$"] = "States.Format('hello {}', $.name)" }, End = true},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn, input = '{"name":"Ada"}'})
    local result = test.call("states", "DescribeExecution", {executionArn = run.executionArn})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.output), {greeting = "hello Ada"})
end)

test.case("wait fires at virtual time", function()
    local arn = machine("wait", {StartAt = "Wait", States = {
        Wait = {Type = "Wait", Seconds = 60, Next = "Done"},
        Done = {Type = "Succeed"},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn})
    test.equal(test.call("states", "DescribeExecution", {executionArn = run.executionArn}).status, "RUNNING")
    test.advance(60)
    test.equal(test.call("states", "DescribeExecution", {executionArn = run.executionArn}).status, "SUCCEEDED")
end)

test.case("each case starts with an empty store", function()
    test.equal(#test.call("states", "ListStateMachines", {}).stateMachines, 0)
    test.equal(test.now(), 1735689600)
end)

test.case("parallel and map execute branches", function()
    local arn = machine("fanout", {StartAt = "Both", States = {
        Both = {Type = "Parallel", Next = "Each", ResultPath = "$.both", Branches = {
            {StartAt = "A", States = {A = {Type = "Pass", Result = "a", End = true}}},
            {StartAt = "B", States = {B = {Type = "Pass", Result = "b", End = true}}},
        }},
        Each = {Type = "Map", ItemsPath = "$.items", ResultPath = "$.mapped", ItemProcessor = {
            StartAt = "Double", States = {Double = {Type = "Pass", Parameters = { ["n.$"] = "States.MathAdd($, $)" }, End = true}},
        }, End = true},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn, input = '{"items":[1,2]}'})
    local result = test.call("states", "DescribeExecution", {executionArn = run.executionArn})
    test.equal(result.status, "SUCCEEDED")
    local output = roto.json_decode(result.output)
    test.equal(output.both, {"a", "b"})
    test.equal(output.mapped, {{n = 2}, {n = 4}})
end)

test.case("retry and catch record execution history", function()
    local arn = machine("recover", {StartAt = "Boom", States = {
        Boom = {Type = "Task", Resource = "arn:aws:lambda:us-east-1:123456789012:function:missing",
            Retry = {{ErrorEquals = {"States.ALL"}, IntervalSeconds = 10, MaxAttempts = 1}},
            Catch = {{ErrorEquals = {"States.ALL"}, ResultPath = "$.caught", Next = "Done"}}, End = true},
        Done = {Type = "Succeed"},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn})
    test.equal(test.call("states", "DescribeExecution", {executionArn = run.executionArn}).status, "RUNNING")
    test.advance(10)
    local result = test.call("states", "DescribeExecution", {executionArn = run.executionArn})
    test.equal(result.status, "SUCCEEDED")
    assert(roto.json_decode(result.output).caught.Error)
    local history = test.call("states", "GetExecutionHistory", {executionArn = run.executionArn}).events
    local failed = 0
    for _, event in history do if event.type == "LambdaFunctionFailed" then failed += 1 end end
    test.equal(failed, 2)
end)

test.case("sqs task token callback completes execution", function()
    local queue = test.call("sqs", "CreateQueue", {QueueName = "jobs"}).QueueUrl
    local arn = machine("callback", {StartAt = "Ask", States = {
        Ask = {Type = "Task", Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            ResultPath = "$.answer", Parameters = {QueueUrl = queue, ["MessageBody.$"] = "$$.Task.Token"}, End = true},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn, input = '{"id":1}'})
    local msg = test.call("sqs", "ReceiveMessage", {QueueUrl = queue}).Messages[1]
    test.equal(test.call("states", "DescribeExecution", {executionArn = run.executionArn}).status, "RUNNING")
    test.call("states", "SendTaskSuccess", {taskToken = msg.Body, output = '{"ok":true}'})
    local result = test.call("states", "DescribeExecution", {executionArn = run.executionArn})
    test.equal(roto.json_decode(result.output), {id = 1, answer = {ok = true}})
    test.expect_error("TaskTimedOut", function()
        return test.try_call("states", "SendTaskSuccess", {taskToken = msg.Body, output = "{}"})
    end)
end)

test.case("built in service errors have structured codes", function()
    test.expect_error("AWS.SimpleQueueService.NonExistentQueue", function()
        return test.try_call("sqs", "GetQueueUrl", {QueueName = "missing"})
    end)
end)

test.case("scheduler CRUD shares the case store", function()
    test.call("scheduler", "CreateScheduleGroup", {Name = "test-group"})
    test.equal(test.call("scheduler", "GetScheduleGroup", {Name = "test-group"}).Name, "test-group")
end)

test.case("modeled bundle operations validate requests", function()
    test.expect_error("ValidationException", function()
        return test.try_call("es", "AcceptInboundConnection", {})
    end)
end)

test.case("account and region scope service calls", function()
    test.call("sqs", "CreateQueue", {QueueName = "scoped"})
    roto.set_region("eu-west-1")
    test.expect_error("AWS.SimpleQueueService.NonExistentQueue", function()
        return test.try_call("sqs", "GetQueueUrl", {QueueName = "scoped"})
    end)
    roto.set_region("us-east-1")
    assert(test.call("sqs", "GetQueueUrl", {QueueName = "scoped"}).QueueUrl)
    roto.set_account("999999999999")
    test.expect_error("AWS.SimpleQueueService.NonExistentQueue", function()
        return test.try_call("sqs", "GetQueueUrl", {QueueName = "scoped"})
    end)
end)

test.case("rds data operations use the built in handler", function()
    local ok, err = test.try_call("rds-data", "CommitTransaction", {})
    test.equal(ok, false)
    assert(err.code and err.status)
end)

test.case("TestState mocks Parallel output without running branches", function()
    local state = {Type = "Parallel", Branches = {
        {StartAt = "Fail", States = {Fail = {Type = "Fail", Error = "must not execute"}}},
        {StartAt = "Fail", States = {Fail = {Type = "Fail", Error = "must not execute"}}},
    }, ResultSelector = { ["first.$"] = "$[0].value", ["second.$"] = "$[1]" },
        ResultPath = "$.result", Next = "Next"}
    local result = test.call("states", "TestState", {definition = roto.json_encode(state), input = '{"keep":true}',
        mock = {result = '[{"value":7},"ok"]'}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(result.nextState, "Next")
    test.equal(roto.json_decode(result.output), {keep = true, result = {first = 7, second = "ok"}})
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = roto.json_encode(state), mock = {result = '[1]'}})
    end)
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = roto.json_encode(state), mock = {result = '{}'}})
    end)
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = roto.json_encode(state), mock = {result = '[1,2]', fieldValidationMode = 'INVALID'}})
    end)
end)

test.case("TestState mocks Map output and validates input selection", function()
    local state = {Type = "Map", ItemsPath = "$.items", ItemSelector = { ["value.$"] = "$$.Map.Item.Value" },
        ItemProcessor = {StartAt = "Fail", States = {Fail = {Type = "Fail", Error = "must not execute"}}},
        ResultPath = "$.results", OutputPath = "$.results", End = true}
    local result = test.call("states", "TestState", {definition = roto.json_encode(state), input = '{"items":[1,2]}',
        mock = {result = '[{"done":1},{"done":2}]'}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.output), {{done = 1}, {done = 2}})
    local invalid = test.call("states", "TestState", {definition = roto.json_encode(state), input = '{"items":3}',
        mock = {result = '[]'}})
    test.equal(invalid.status, "FAILED")
    test.equal(invalid.error, "States.Runtime")
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = roto.json_encode(state), mock = {result = '{}'}})
    end)
end)

test.case("TestState handles mocked Parallel errors with retry and catch", function()
    local state = {Type = "Parallel", Branches = {{StartAt = "S", States = {S = {Type = "Succeed"}}}},
        Retry = {{ErrorEquals = {"Oops"}, IntervalSeconds = 3, MaxAttempts = 1}},
        Catch = {{ErrorEquals = {"Oops"}, ResultPath = "$.caught", Next = "Recover"}}, End = true}
    local request = {definition = roto.json_encode(state), input = '{"x":1}',
        mock = {errorOutput = {error = "Oops", cause = "bad"}}, inspectionLevel = "DEBUG"}
    local retry = test.call("states", "TestState", request)
    test.equal(retry.status, "RETRIABLE")
    test.equal(retry.inspectionData.errorDetails.retryBackoffIntervalSeconds, 3)
    request.stateConfiguration = {retrierRetryCount = 1}
    local caught = test.call("states", "TestState", request)
    test.equal(caught.status, "CAUGHT_ERROR")
    test.equal(caught.nextState, "Recover")
    test.equal(roto.json_decode(caught.output), {x = 1, caught = {Error = "Oops", Cause = "bad"}})
end)

test.case("TestState mock errors respect retry limits and catch ordering", function()
    local state = {Type = "Task", Resource = "arn:aws:states:::sqs:sendMessage",
        Retry = {{ErrorEquals = {"Transient"}, IntervalSeconds = 4, BackoffRate = 2, MaxAttempts = 3, MaxDelaySeconds = 5}},
        Catch = {{ErrorEquals = {"Transient"}, ResultPath = "$.failure", Next = "Recover"},
            {ErrorEquals = {"States.ALL"}, Next = "Fallback"}}, End = true}
    local function run(count)
        return test.call("states", "TestState", {definition = roto.json_encode(state), input = '{"keep":true}',
            mock = {errorOutput = {error = "Transient", cause = "retry me"}},
            stateConfiguration = {retrierRetryCount = count}, inspectionLevel = "DEBUG"})
    end
    local first, second, caught = run(0), run(1), run(3)
    test.equal(first.status, "RETRIABLE")
    test.equal(first.inspectionData.errorDetails.retryBackoffIntervalSeconds, 4)
    test.equal(second.inspectionData.errorDetails.retryBackoffIntervalSeconds, 5)
    test.equal(caught.status, "CAUGHT_ERROR")
    test.equal(caught.nextState, "Recover")
    test.equal(caught.inspectionData.errorDetails.catchIndex, 0)
    test.equal(roto.json_decode(caught.output), {keep = true, failure = {Error = "Transient", Cause = "retry me"}})
end)

test.case("TestState States.TaskFailed does not catch timeout", function()
    local state = {Type = "Task", Resource = "arn:aws:states:::sqs:sendMessage",
        Catch = {{ErrorEquals = {"States.TaskFailed"}, Next = "Recover"}}, End = true}
    local result = test.call("states", "TestState", {definition = roto.json_encode(state),
        mock = {errorOutput = {error = "States.Timeout", cause = "timeout"}}})
    test.equal(result.status, "FAILED")
    test.equal(result.error, "States.Timeout")
end)

test.case("TestState validates Task mock results with STRICT and PRESENT", function()
    local state = {Type = "Task", Resource = "arn:aws:states:::aws-sdk:sqs:changeMessageVisibilityBatch", End = true}
    local definition = roto.json_encode(state)
    local function request(result, mode)
        return {definition = definition, mock = {result = result, fieldValidationMode = mode}}
    end
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request('{}', nil)) end)
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request('{}', 'STRICT')) end)
    local present = test.call("states", "TestState", request('{}', 'PRESENT'))
    test.equal(present.status, "SUCCEEDED")
    local valid = test.call("states", "TestState", request('{"Successful":[],"Failed":[],"Extra":true}', 'STRICT'))
    test.equal(valid.status, "SUCCEEDED")
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request('{"Successful":"wrong"}', 'PRESENT')) end)
    local unchecked = test.call("states", "TestState", request('{"Successful":"wrong"}', 'NONE'))
    test.equal(unchecked.status, "SUCCEEDED")
end)

test.case("TestState accepts decoded optimized Lambda payloads", function()
    local definition = roto.json_encode({Type = "Task", Resource = "arn:aws:states:::lambda:invoke", End = true})
    local result = test.call("states", "TestState", {definition = definition,
        mock = {result = '{"StatusCode":200,"Payload":{"ok":true}}'}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.output).Payload, {ok = true})
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = definition, mock = {result = '{"StatusCode":"bad"}'}})
    end)
    local direct = roto.json_encode({Type = "Task", Resource = "arn:aws:lambda:us-east-1:123456789012:function:demo", End = true})
    local raw = test.call("states", "TestState", {definition = direct, mock = {result = '42'}})
    test.equal(roto.json_decode(raw.output), 42)
end)

test.case("TestState validates mocked responses from modeled bundle services", function()
    local definition = roto.json_encode({Type = "Task", Resource = "arn:aws:states:::aws-sdk:autoscaling:describeAutoScalingGroups", End = true})
    local function request(result, mode)
        return {definition = definition, mock = {result = result, fieldValidationMode = mode}}
    end
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request('{}', 'STRICT')) end)
    test.equal(test.call("states", "TestState", request('{}', 'PRESENT')).status, "SUCCEEDED")
    test.equal(test.call("states", "TestState", request('{"AutoScalingGroups":[]}', 'STRICT')).status, "SUCCEEDED")
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request('{"AutoScalingGroups":"wrong"}', 'PRESENT')) end)
end)

test.case("TestState validates ECS task response mocks", function()
    for _, resource in {"arn:aws:states:::ecs:runTask", "arn:aws:states:::ecs:runTask.sync"} do
        local definition = roto.json_encode({Type = "Task", Resource = resource, End = true})
        local function request(result)
            return {definition = definition, mock = {result = result}}
        end
        local valid = test.call("states", "TestState", request('{"Tasks":[{"TaskArn":"arn:task","Containers":[{"Name":"app"}]}],"Extra":true}'))
        test.equal(valid.status, "SUCCEEDED")
        test.expect_error("ValidationException", function()
            return test.try_call("states", "TestState", request('{"Tasks":[{"TaskArn":12}]}'))
        end)
        test.expect_error("ValidationException", function()
            return test.try_call("states", "TestState", request('{"Tasks":"bad"}'))
        end)
    end
end)

test.case("TestState validates nested execution response mocks", function()
    local function request(suffix: string, result: string)
        return {definition = roto.json_encode({Type = "Task", Resource = "arn:aws:states:::states:startExecution" .. suffix, End = true}),
            mock = {result = result}}
    end
    local started = '{"ExecutionArn":"arn:aws:states:us-east-1:123456789012:execution:machine:name","StartDate":123}'
    test.equal(test.call("states", "TestState", request("", started)).status, "SUCCEEDED")
    local done = '{"ExecutionArn":"arn:aws:states:us-east-1:123456789012:execution:machine:name","StateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:machine","Status":"SUCCEEDED","StartDate":123,"Output":"{}"}'
    test.equal(test.call("states", "TestState", request(".sync", done)).status, "SUCCEEDED")
    local decoded = string.gsub(done, '"Output":"{}"', '"Output":{"ok":true}')
    test.equal(test.call("states", "TestState", request(".sync:2", decoded)).status, "SUCCEEDED")
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request(".sync", decoded)) end)
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request("", '{}')) end)
end)
