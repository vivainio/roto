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

test.case("empty map execution returns a JSON array", function()
    local arn = machine("empty-map", {StartAt = "Each", States = {
        Each = {Type = "Map", ItemProcessor = {StartAt = "Done", States = {Done = {Type = "Succeed"}}}, End = true},
    }})
    local run = test.call("states", "StartExecution", {stateMachineArn = arn, input = "[]"})
    local result = test.call("states", "DescribeExecution", {executionArn = run.executionArn})
    test.equal(result.output, "[]")
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
        mock = {errorOutput = {error = "Oops", cause = "bad"}}, inspectionLevel = "DEBUG",
        stateConfiguration = {errorCausedByState = "S", retrierRetryCount = 0}}
    local retry = test.call("states", "TestState", request)
    test.equal(retry.status, "RETRIABLE")
    test.equal(retry.inspectionData.errorDetails.retryBackoffIntervalSeconds, 3)
    request.stateConfiguration = {errorCausedByState = "S", retrierRetryCount = 1}
    local caught = test.call("states", "TestState", request)
    test.equal(caught.status, "CAUGHT_ERROR")
    test.equal(caught.nextState, "Recover")
    test.equal(roto.json_decode(caught.output), {x = 1, caught = {Error = "Oops", Cause = "bad"}})
end)

test.case("TestState validates the source of mocked fan-out errors", function()
    local state = {Type = "Map", ItemProcessor = {StartAt = "Group", States = {
        Group = {Type = "Parallel", Branches = {{StartAt = "Work", States = {
            Work = {Type = "Task", Resource = "arn:aws:states:::sqs:sendMessage", End = true},
        }}}, End = true},
    }}, Catch = {{ErrorEquals = {"States.TaskFailed"}, Next = "Recover"}}, End = true}
    local request: any = {definition = roto.json_encode(state), input = '[1]',
        mock = {errorOutput = {error = "SomeService.Error"}},
        stateConfiguration = {errorCausedByState = "Work"}, inspectionLevel = "DEBUG"}
    local result = test.call("states", "TestState", request)
    test.equal(result.status, "CAUGHT_ERROR")
    test.equal(result.nextState, "Recover")
    request.stateConfiguration.errorCausedByState = "Missing"
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    request.stateConfiguration = nil
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    request.stateConfiguration = {errorCausedByState = "Work"}
    request.mock = {result = '[]'}
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
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

test.case("TestState checks modeled patterns in task mocks", function()
    local definition = roto.json_encode({Type = "Task", Resource = "arn:aws:states:::aws-sdk:dynamodb:listTables", End = true})
    local function request(result: string, mode: string)
        return {definition = definition, mock = {result = result, fieldValidationMode = mode}}
    end
    test.equal(test.call("states", "TestState", request('{"LastEvaluatedTableName":"abc"}', "STRICT")).status, "SUCCEEDED")
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", request('{"LastEvaluatedTableName":"!!!"}', "PRESENT"))
    end)
    test.equal(test.call("states", "TestState", request('{"LastEvaluatedTableName":"!!!"}', "NONE")).status, "SUCCEEDED")
end)

test.case("TestState ignores unknown fields for operations without modeled output", function()
    local definition = roto.json_encode({Type = "Task", Resource = "arn:aws:states:::aws-sdk:sqs:deleteMessage", End = true})
    for _, mode in {"STRICT", "PRESENT"} do
        local result = test.call("states", "TestState", {definition = definition,
            mock = {result = '{"extra":{"value":1}}', fieldValidationMode = mode}})
        test.equal(result.status, "SUCCEEDED")
        test.expect_error("ValidationException", function()
            return test.try_call("states", "TestState", {definition = definition,
                mock = {result = '42', fieldValidationMode = mode}})
        end)
    end
end)

test.case("TestState uses supplied context for mocked tasks", function()
    local state = {Type = "Task", Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
        Parameters = { ["MessageBody.$"] = "$$.Task.Token" },
        ResultSelector = { ["id.$"] = "$$.Execution.Id", ["name.$"] = "$$.State.Name",
            ["token.$"] = "$$.Task.Token", ["value.$"] = "$.value" }, End = true}
    local definition = roto.json_encode(state)
    local result = test.call("states", "TestState", {definition = definition,
        context = '{"Execution":{"Id":"custom-execution"},"State":{"Name":"custom-state"},"Task":{"Token":"custom-token"}}',
        mock = {result = '{"value":7}', fieldValidationMode = "NONE"}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.output), {id = "custom-execution", name = "custom-state", token = "custom-token", value = 7})
    local failed = test.call("states", "TestState", {definition = definition,
        context = '{"Task":{"Token":"custom-token"}}', mock = {errorOutput = {error = "Oops"}}})
    test.equal(failed.error, "Oops")
    local map = {Type = "Map", ItemsPath = "$.items", ItemSelector = { ["id.$"] = "$$.Execution.Id" },
        ItemProcessor = {StartAt = "Done", States = {Done = {Type = "Succeed"}}},
        ResultSelector = { ["id.$"] = "$$.Execution.Id", ["first.$"] = "$[0]" }, End = true}
    local mapped = test.call("states", "TestState", {definition = roto.json_encode(map), input = '{"items":[1]}',
        context = '{"Execution":{"Id":"map-execution"}}', mock = {result = '[3]'}})
    test.equal(roto.json_decode(mapped.output), {id = "map-execution", first = 3})
    for _, context in {"[1]", "null", "not-json"} do
        test.expect_error("ValidationException", function()
            return test.try_call("states", "TestState", {definition = definition, context = context, mock = {result = '{}', fieldValidationMode = "NONE"}})
        end)
    end
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = definition, context = '{}'})
    end)
end)

test.case("TestState selects named states from complete definitions", function()
    local definition = roto.json_encode({StartAt = "Outer", States = {
        Outer = {Type = "Map", ItemProcessor = {StartAt = "Nested", States = {
            Nested = {Type = "Pass", Parameters = { ["name.$"] = "$$.State.Name", ["index.$"] = "$$.Map.Item.Index",
                ["value.$"] = "$$.Map.Item.Value" }, End = true},
        }}, End = true},
        Other = {Type = "Pass", Result = "selected", Next = "Outer"},
    }})
    local nested = test.call("states", "TestState", {definition = definition, stateName = "Nested", input = '{"id":3}'})
    test.equal(roto.json_decode(nested.output), {name = "Nested", index = 0, value = {id = 3}})
    local other = test.call("states", "TestState", {definition = definition, stateName = "Other"})
    test.equal(roto.json_decode(other.output), "selected")
    test.equal(other.nextState, "Outer")
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = definition, stateName = "Missing"})
    end)
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = '{"Type":"Succeed"}', stateName = "Missing"})
    end)
    local ambiguous = roto.json_encode({StartAt = "Branch", States = {
        Branch = {Type = "Parallel", Branches = {
            {StartAt = "Same", States = {Same = {Type = "Succeed"}}},
            {StartAt = "Same", States = {Same = {Type = "Succeed"}}},
        }, End = true},
    }})
    test.expect_error("ValidationException", function()
        return test.try_call("states", "TestState", {definition = ambiguous, stateName = "Same"})
    end)
end)

test.case("TestState inspects mocked task and map transformations", function()
    local task = {Type = "Task", Resource = "arn:aws:states:::lambda:invoke", InputPath = "$.payload",
        Parameters = { ["number.$"] = "$.n" }, ResultSelector = { ["answer.$"] = "$.answer" },
        ResultPath = "$.result", OutputPath = "$.result", End = true}
    local request = {definition = roto.json_encode(task), input = '{"payload":{"n":2},"keep":true}',
        mock = {result = '{"answer":3}', fieldValidationMode = "NONE"}, inspectionLevel = "DEBUG"}
    local response = test.call("states", "TestState", request)
    local inspection = response.inspectionData
    test.equal(roto.json_decode(inspection.input), {payload = {n = 2}, keep = true})
    test.equal(roto.json_decode(inspection.afterInputPath), {n = 2})
    test.equal(roto.json_decode(inspection.afterParameters), {number = 2})
    test.equal(roto.json_decode(inspection.result), {answer = 3})
    test.equal(roto.json_decode(inspection.afterResultSelector), {answer = 3})
    test.equal(roto.json_decode(inspection.afterResultPath), {payload = {n = 2}, keep = true, result = {answer = 3}})
    test.equal(roto.json_decode(response.output), {answer = 3})
    request.inspectionLevel = "INFO"
    test.equal(test.call("states", "TestState", request).inspectionData, nil)
    request.inspectionLevel = "INVALID"
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    local map = {Type = "Map", ItemsPath = "$.items", ItemSelector = { ["value.$"] = "$$.Map.Item.Value" },
        ItemProcessor = {StartAt = "Done", States = {Done = {Type = "Succeed"}}}, End = true}
    local mapped = test.call("states", "TestState", {definition = roto.json_encode(map), input = '{"items":[1,2]}',
        mock = {result = '[3,4]'}, inspectionLevel = "TRACE"})
    test.equal(roto.json_decode(mapped.inspectionData.afterItemsPath), {1, 2})
    test.equal(roto.json_decode(mapped.inspectionData.afterItemSelector), {{value = 1}, {value = 2}})
    test.equal(roto.json_decode(mapped.inspectionData.result), {3, 4})
end)

test.case("TestState inspects unmocked pass and task transformations", function()
    local pass = {Type = "Pass", InputPath = "$.payload", Parameters = { ["n.$"] = "$.number" },
        ResultPath = "$.result", OutputPath = "$.result", End = true}
    local response = test.call("states", "TestState", {definition = roto.json_encode(pass),
        input = '{"payload":{"number":2},"keep":true}', inspectionLevel = "DEBUG"})
    test.equal(roto.json_decode(response.inspectionData.afterInputPath), {number = 2})
    test.equal(roto.json_decode(response.inspectionData.afterParameters), {n = 2})
    test.equal(roto.json_decode(response.inspectionData.afterResultPath), {payload = {number = 2}, keep = true, result = {n = 2}})
    test.equal(roto.json_decode(response.output), {n = 2})
    local queue = test.call("sqs", "CreateQueue", {QueueName = "inspect"}).QueueUrl
    local task = {Type = "Task", Resource = "arn:aws:states:::aws-sdk:sqs:getQueueUrl",
        Parameters = {QueueName = "inspect"}, End = true}
    local task_result = test.call("states", "TestState", {definition = roto.json_encode(task), inspectionLevel = "TRACE"})
    test.equal(roto.json_decode(task_result.inspectionData.afterParameters), {QueueName = "inspect"})
    test.equal(roto.json_decode(task_result.inspectionData.result).QueueUrl, queue)
    test.equal(roto.json_decode(task_result.output).QueueUrl, queue)
end)

test.case("TestState mocks Distributed Map exports and failure thresholds", function()
    local map: any = {Type = "Map", ItemsPath = "$.items", ItemProcessor = {
        ProcessorConfig = {Mode = "DISTRIBUTED", ExecutionType = "STANDARD"},
        StartAt = "Never", States = {Never = {Type = "Fail", Error = "must not run"}}},
        ToleratedFailureCountPath = "$.allowed", ToleratedFailurePercentage = 50,
        ResultWriter = {Resource = "arn:aws:states:::s3:putObject", Parameters = {Bucket = "unused"}},
        ResultPath = "$.export", End = true}
    local definition = roto.json_encode(map)
    local request = {definition = definition, input = '{"items":[1,2,3,4],"allowed":2}',
        mock = {result = '{"MapRunArn":"arn:map","ResultWriterDetails":{"Bucket":"b","Key":"manifest"}}'},
        stateConfiguration = {mapIterationFailureCount = 2}, inspectionLevel = "DEBUG"}
    local success = test.call("states", "TestState", request)
    test.equal(success.status, "SUCCEEDED")
    test.equal(roto.json_decode(success.output).export.ResultWriterDetails.Key, "manifest")
    test.equal(success.inspectionData.toleratedFailureCount, 2)
    test.equal(success.inspectionData.toleratedFailurePercentage, 50)
    request.stateConfiguration.mapIterationFailureCount = 3
    local failed = test.call("states", "TestState", request)
    test.equal(failed.status, "FAILED")
    test.equal(failed.error, "States.ExceedToleratedFailureThreshold")
    map.Catch = {{ErrorEquals = {"States.ExceedToleratedFailureThreshold"}, ResultPath = "$.error", Next = "Recover"}}
    request.definition = roto.json_encode(map)
    local caught = test.call("states", "TestState", request)
    test.equal(caught.status, "CAUGHT_ERROR")
    test.equal(caught.nextState, "Recover")
    request.stateConfiguration.mapIterationFailureCount = 5
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    request.stateConfiguration.mapIterationFailureCount = 0
    request.mock.result = '[]'
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    map.ResultWriter = nil
    request.definition = roto.json_encode(map)
    local array = test.call("states", "TestState", request)
    test.equal(array.status, "SUCCEEDED")
    test.equal(roto.json_decode(array.output).export, {})
end)

test.case("TestState inspects Distributed Map item batches", function()
    local map: any = {Type = "Map", ItemsPath = "$.items", ItemSelector = { ["value.$"] = "$$.Map.Item.Value" },
        ItemBatcher = {MaxItemsPerBatchPath = "$.limit", BatchInput = { ["job.$"] = "$.job" }},
        ItemProcessor = {ProcessorConfig = {Mode = "DISTRIBUTED", ExecutionType = "STANDARD"},
            StartAt = "Done", States = {Done = {Type = "Succeed"}}},
        ToleratedFailureCount = 2, End = true}
    local request = {definition = roto.json_encode(map), input = '{"items":[1,2,3],"limit":2,"job":"job1"}',
        mock = {result = '["a","b"]'}, stateConfiguration = {mapIterationFailureCount = 2}, inspectionLevel = "DEBUG"}
    local result = test.call("states", "TestState", request)
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector), {{value = 1}, {value = 2}, {value = 3}})
    test.equal(roto.json_decode(result.inspectionData.afterItemBatcher), {
        {Items = {{value = 1}, {value = 2}}, BatchInput = {job = "job1"}},
        {Items = {{value = 3}}, BatchInput = {job = "job1"}},
    })
    map.ItemBatcher = {MaxInputBytesPerBatch = 25}
    request.definition = roto.json_encode(map)
    local bounded = test.call("states", "TestState", request)
    test.equal(#roto.json_decode(bounded.inspectionData.afterItemBatcher), 3)
    map.ItemBatcher = {MaxInputBytesPerBatch = 5}
    request.definition = roto.json_encode(map)
    local too_large = test.call("states", "TestState", request)
    test.equal(too_large.error, "States.DataLimitExceeded")
end)

test.case("TestState reads supplied JSON and JSONL ItemReader data", function()
    local map: any = {Type = "Map", ItemReader = {Resource = "arn:aws:states:::s3:getObject",
        ReaderConfig = {InputType = "JSON", ItemsPointer = "/orders/a~1b", MaxItemsPath = "$.limit"}},
        ItemSelector = { ["value.$"] = "$$.Map.Item.Value", ["source.$"] = "$$.Map.Item.Source" },
        ItemBatcher = {MaxItemsPerBatch = 2}, ItemProcessor = {
            ProcessorConfig = {Mode = "DISTRIBUTED", ExecutionType = "STANDARD"},
            StartAt = "Done", States = {Done = {Type = "Succeed"}}}, End = true}
    local request: any = {definition = roto.json_encode(map), input = '{"limit":2}',
        stateConfiguration = {mapItemReaderData = '{"orders":{"a/b":[1,2,3]}}', mapIterationFailureCount = 0},
        mock = {result = '["done"]'}, inspectionLevel = "DEBUG"}
    local result = test.call("states", "TestState", request)
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.inspectionData.afterItemsPointer), {1, 2})
    test.equal(roto.json_decode(result.inspectionData.afterItemBatcher), {
        {Items = {{value = 1, source = "JSON"}, {value = 2, source = "JSON"}}},
    })
    map.ItemReader.ReaderConfig = {InputType = "JSONL"}
    request.definition = roto.json_encode(map)
    request.stateConfiguration.mapItemReaderData = '{"n":1}\n{"n":2}\n'
    result = test.call("states", "TestState", request)
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector), {
        {value = {n = 1}, source = "JSONL"}, {value = {n = 2}, source = "JSONL"},
    })
    request.stateConfiguration.mapItemReaderData = '{bad}\n'
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    map.ItemReader.ReaderConfig.InputType = "PARQUET"
    request.definition = roto.json_encode(map)
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
end)

test.case("TestState reads supplied CSV ItemReader data", function()
    local map: any = {Type = "Map", ItemReader = {Resource = "arn:aws:states:::s3:getObject",
        ReaderConfig = {InputType = "CSV", CSVHeaderLocation = "FIRST_ROW", CSVDelimiter = "PIPE"}},
        ItemSelector = { ["row.$"] = "$$.Map.Item.Value", ["source.$"] = "$$.Map.Item.Source" },
        ItemProcessor = {ProcessorConfig = {Mode = "DISTRIBUTED", ExecutionType = "STANDARD"},
            StartAt = "Done", States = {Done = {Type = "Succeed"}}}, End = true}
    local request: any = {definition = roto.json_encode(map), input = '{}', mock = {result = '[]'},
        stateConfiguration = {mapItemReaderData = 'name|note|count\r\n"Ada|A"|"line 1\nline ""2"""|4\r\nBob|short\r\n'},
        inspectionLevel = "DEBUG"}
    local result = test.call("states", "TestState", request)
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector), {
        {row = {name = "Ada|A", note = 'line 1\nline "2"', count = "4"}, source = "CSV"},
        {row = {name = "Bob", note = "short", count = ""}, source = "CSV"},
    })
    map.ItemReader.ReaderConfig = {InputType = "CSV", CSVHeaderLocation = "GIVEN", CSVHeaders = {"a", "b"}}
    request.definition = roto.json_encode(map)
    request.stateConfiguration.mapItemReaderData = 'one\\,two,three,ignored\n'
    result = test.call("states", "TestState", request)
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector)[1].row, {a = "one,two", b = "three"})
    request.stateConfiguration.mapItemReaderData = '"unclosed'
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
end)

test.case("TestState selects keys from Distributed Map object input", function()
    local map: any = {Type = "Map", ItemsPath = "$.items",
        ItemSelector = { ["key.$"] = "$$.Map.Item.Key", ["value.$"] = "$$.Map.Item.Value" },
        ItemProcessor = {ProcessorConfig = {Mode = "DISTRIBUTED", ExecutionType = "STANDARD"},
            StartAt = "Done", States = {Done = {Type = "Succeed"}}},
        ToleratedFailureCount = 1, End = true}
    local request: any = {definition = roto.json_encode(map), input = '{"items":{"beta":2,"alpha":1}}',
        mock = {result = '["one","two"]'}, stateConfiguration = {mapIterationFailureCount = 1}, inspectionLevel = "DEBUG"}
    local result = test.call("states", "TestState", request)
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector), {{key = "alpha", value = 1}, {key = "beta", value = 2}})
    map.ItemsPath = nil
    map.ItemReader = {Resource = "arn:aws:states:::s3:getObject", ReaderConfig = {InputType = "JSON", ItemsPointer = "/items", MaxItems = 1}}
    request.definition = roto.json_encode(map)
    request.input = '{}'
    request.stateConfiguration = {mapItemReaderData = '{"items":{"beta":2,"alpha":1}}', mapIterationFailureCount = 1}
    result = test.call("states", "TestState", request)
    test.equal(roto.json_decode(result.inspectionData.afterItemsPointer), {alpha = 1})
    test.equal(roto.json_decode(result.inspectionData.afterItemSelector), {{key = "alpha", value = 1}})
    request.stateConfiguration.mapIterationFailureCount = 2
    test.expect_error("ValidationException", function() return test.try_call("states", "TestState", request) end)
    map.ItemReader = nil
    map.ItemProcessor.ProcessorConfig.Mode = "INLINE"
    request.definition = roto.json_encode(map)
    request.input = '{"a":1}'
    request.stateConfiguration = nil
    local inline = test.call("states", "TestState", request)
    test.equal(inline.error, "States.Runtime")
end)
