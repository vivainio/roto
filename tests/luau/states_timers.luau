--!strict
local function machine(name, definition)
    return test.aws.states.CreateStateMachine({name = name,
        roleArn = "arn:aws:iam::123456789012:role/test", definition = roto.json_encode(definition)}).stateMachineArn
end
local function start(arn, input)
    return test.aws.states.StartExecution({stateMachineArn = arn, input = roto.json_encode(input)}).executionArn
end
local function status(arn)
    return test.aws.states.DescribeExecution({executionArn = arn})
end
local function history(arn)
    return test.aws.states.GetExecutionHistory({executionArn = arn}).events
end
local function types(arn)
    local out = {}
    for _, event in history(arn) do table.insert(out, event.type) end
    return out
end

test.case("chained waits fire at their scheduled time in one advance", function()
    local arn = machine("chain", {StartAt = "First", States = {
        First = {Type = "Wait", Seconds = 10, Next = "Second"},
        Second = {Type = "Wait", SecondsPath = "$.delay", Next = "Done"},
        Done = {Type = "Pass", Result = "finished", End = true},
    }})
    local run = start(arn, {delay = 20})
    test.advance(29)
    test.equal(status(run).status, "RUNNING")
    test.advance(1)
    local result = status(run)
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(result.output)), "finished")
    local events = history(run)
    test.equal(types(run), {"ExecutionStarted", "WaitStateEntered", "WaitStateExited", "WaitStateEntered",
        "WaitStateExited", "PassStateEntered", "PassStateExited", "ExecutionSucceeded"})
    local started, first, second = events[2].timestamp, events[3].timestamp, events[5].timestamp
    local entered = events[4].timestamp
    assert(type(started) == "number" and type(first) == "number" and type(entered) == "number" and type(second) == "number")
    test.equal(first - started, 10)
    test.equal(second - entered, 20)
end)

test.case("machine timeout and stop cancel waiting jobs", function()
    local arn = machine("timeout", {StartAt = "Wait", TimeoutSeconds = 60, States = {
        Wait = {Type = "Wait", Seconds = 100, End = true},
    }})
    local timed = start(arn, {})
    local stopped = start(arn, {})
    test.aws.states.StopExecution({executionArn = stopped, error = "ManualStop", cause = "cancelled"})
    test.advance(59)
    test.equal(status(timed).status, "RUNNING")
    test.advance(1)
    test.equal(status(timed).status, "TIMED_OUT")
    test.equal(status(timed).error, "States.Timeout")
    test.advance(500)
    test.equal(status(stopped).status, "ABORTED")
    test.equal(status(stopped).cause, "cancelled")
    test.equal(types(timed)[#types(timed)], "ExecutionTimedOut")
    test.equal(types(stopped)[#types(stopped)], "ExecutionAborted")
end)

test.case("activity heartbeat begins after pickup and can extend deadline", function()
    local activity = test.aws.states.CreateActivity({name = "work"}).activityArn
    local arn = machine("activity", {StartAt = "Work", States = {
        Work = {Type = "Task", Resource = activity, TimeoutSeconds = 100, HeartbeatSeconds = 10,
            ResultPath = "$.answer", End = true},
    }})
    local run = start(arn, {job = 1})
    test.advance(25)
    test.equal(status(run).status, "RUNNING")
    local task = test.aws.states.GetActivityTask({activityArn = activity, workerName = "worker"})
    local token = assert(task.taskToken)
    test.equal(roto.json_decode(assert(task.input)), {job = 1})
    test.advance(9)
    test.aws.states.SendTaskHeartbeat({taskToken = token})
    test.advance(9)
    test.equal(status(run).status, "RUNNING")
    test.aws.states.SendTaskSuccess({taskToken = token, output = '"done"'})
    test.equal(roto.json_decode(assert(status(run).output)), {job = 1, answer = "done"})
    test.advance(200)
    test.equal(status(run).status, "SUCCEEDED")
    test.expect_error("TaskTimedOut", function()
        return test.try_call("states", "SendTaskSuccess", {taskToken = token, output = '{}'})
    end)
end)

test.case("activity heartbeat timeout catches and invalidates the token", function()
    local activity = test.aws.states.CreateActivity({name = "timeout"}).activityArn
    local arn = machine("activity-timeout", {StartAt = "Work", States = {
        Work = {Type = "Task", Resource = activity, TimeoutSeconds = 100, HeartbeatSeconds = 10,
            Catch = {{ErrorEquals = {"States.Timeout"}, ResultPath = "$.caught", Next = "Done"}}, End = true},
        Done = {Type = "Succeed"},
    }})
    local run = start(arn, {})
    local token = assert(test.aws.states.GetActivityTask({activityArn = activity}).taskToken)
    test.advance(10)
    local result = status(run)
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(result.output)).caught.Error, "States.Timeout")
    local seen = false
    for _, event in history(run) do if event.type == "ActivityTimedOut" then seen = true end end
    assert(seen)
    test.expect_error("TaskTimedOut", function()
        return test.try_call("states", "SendTaskHeartbeat", {taskToken = token})
    end)
end)

test.case("a callback in a parallel branch survives a sibling wait", function()
    local queue = assert(test.aws.sqs.CreateQueue({QueueName = "callbacks"}).QueueUrl)
    local arn = machine("parallel-wait", {StartAt = "Both", States = {
        Both = {Type = "Parallel", End = true, Branches = {
            {StartAt = "Wait", States = {Wait = {Type = "Wait", Seconds = 10, Next = "Done"},
                Done = {Type = "Pass", Result = "timer", End = true}}},
            {StartAt = "Ask", States = {Ask = {Type = "Task",
                Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                Parameters = {QueueUrl = queue, ["MessageBody.$"] = "$$.Task.Token"}, End = true}}},
        }},
    }})
    local run = start(arn, {})
    local token = assert(assert(test.aws.sqs.ReceiveMessage({QueueUrl = queue}).Messages)[1].Body)
    test.advance(10)
    test.equal(status(run).status, "RUNNING")
    test.aws.states.SendTaskSuccess({taskToken = token, output = '"callback"'})
    test.equal(roto.json_decode(assert(status(run).output)), {"timer", "callback"})
    local events: {[string]: any} = {}
    for _, event in history(run) do events[event.type] = event end
    test.equal(events.WaitStateEntered.previousEventId, events.ParallelStateStarted.id)
    test.equal(events.TaskStateEntered.previousEventId, events.ParallelStateStarted.id)
    test.equal(events.WaitStateExited.previousEventId, events.WaitStateEntered.id)
    test.equal(events.TaskSucceeded.previousEventId, events.TaskStarted.id)
end)

test.case("Map preserves item order when callbacks finish out of order", function()
    local queue = assert(test.aws.sqs.CreateQueue({QueueName = "map"}).QueueUrl)
    local arn = machine("map-callbacks", {StartAt = "Map", States = {Map = {
        Type = "Map", ItemsPath = "$.items", End = true,
        ItemSelector = { ["item.$"] = "$$.Map.Item.Value" },
        ItemProcessor = {StartAt = "Ask", States = {Ask = {Type = "Task",
            Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            Parameters = {QueueUrl = queue, ["MessageBody.$"] = "States.JsonToString($)",
                MessageAttributes = {token = {DataType = "String", ["StringValue.$"] = "$$.Task.Token"}}},
            End = true}}},
    }}})
    local run = start(arn, {items = {"x", "y", "z"}})
    local received = test.call("sqs", "ReceiveMessage", {QueueUrl = queue, MaxNumberOfMessages = 10, MessageAttributeNames = {"All"}})
    local tokens: {[string]: string} = {}
    for _, message in received.Messages do tokens[roto.json_decode(message.Body).item] = message.MessageAttributes.token.StringValue end
    assert(tokens.x and tokens.y and tokens.z)
    for _, item in {"z", "x", "y"} do
        test.equal(status(run).status, "RUNNING")
        test.aws.states.SendTaskSuccess({taskToken = tokens[item], output = roto.json_encode(string.upper(item))})
    end
    test.equal(roto.json_decode(assert(status(run).output)), {"X", "Y", "Z"})
    local events = history(run)
    local started = 0
    local succeeded = 0
    for _, event in events do
        if event.type == "MapIterationStarted" then started += 1 end
        if event.type == "MapIterationSucceeded" then succeeded += 1 end
    end
    test.equal(started, 3)
    test.equal(succeeded, 3)
end)

test.case("parallel failure cancels sibling callbacks and timers", function()
    local queue = assert(test.aws.sqs.CreateQueue({QueueName = "orphans"}).QueueUrl)
    local arn = machine("abort", {StartAt = "Both", States = {Both = {Type = "Parallel", End = true, Branches = {
        {StartAt = "Ask", States = {Ask = {Type = "Task",
            Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken", TimeoutSeconds = 30,
            Parameters = {QueueUrl = queue, ["MessageBody.$"] = "$$.Task.Token"}, End = true}}},
        {StartAt = "Fail", States = {Fail = {Type = "Fail", Error = "Oops", Cause = "failed"}}},
    }}}})
    local run = start(arn, {})
    test.equal(status(run).status, "FAILED")
    test.equal(status(run).error, "Oops")
    local token = assert(assert(test.aws.sqs.ReceiveMessage({QueueUrl = queue}).Messages)[1].Body)
    test.expect_error("TaskTimedOut", function()
        return test.try_call("states", "SendTaskSuccess", {taskToken = token, output = '{}'})
    end)
    test.advance(100)
    test.equal(status(run).status, "FAILED")
    test.equal(types(run)[#types(run)], "ExecutionFailed")
end)

test.case("callback retries preserve state entry time and increment retry count", function()
    local queue = assert(test.aws.sqs.CreateQueue({QueueName = "retry"}).QueueUrl)
    local arn = machine("retry-context", {StartAt = "Ask", States = {Ask = {Type = "Task",
        Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
        Parameters = {QueueUrl = queue, ["MessageBody.$"] = "States.JsonToString($$.State)",
            MessageAttributes = {token = {DataType = "String", ["StringValue.$"] = "$$.Task.Token"}}},
        Retry = {{ErrorEquals = {"Transient"}, IntervalSeconds = 5, BackoffRate = 2, MaxAttempts = 2}},
        End = true,
    }}})
    local run = start(arn, {})
    local function receive()
        local response = test.call("sqs", "ReceiveMessage", {QueueUrl = queue, MessageAttributeNames = {"All"}})
        local message = response.Messages[1]
        test.call("sqs", "DeleteMessage", {QueueUrl = queue, ReceiptHandle = message.ReceiptHandle})
        return roto.json_decode(message.Body), message.MessageAttributes.token.StringValue
    end
    local first, token = receive()
    test.equal(first.RetryCount, 0)
    test.aws.states.SendTaskFailure({taskToken = token, error = "Transient"})
    test.advance(4)
    test.equal(status(run).status, "RUNNING")
    test.advance(1)
    local second, secondToken = receive()
    test.equal(second.RetryCount, 1)
    test.equal(second.EnteredTime, first.EnteredTime)
    test.aws.states.SendTaskFailure({taskToken = secondToken, error = "Transient"})
    test.advance(10)
    local third, thirdToken = receive()
    test.equal(third.RetryCount, 2)
    test.equal(third.EnteredTime, first.EnteredTime)
    test.aws.states.SendTaskSuccess({taskToken = thirdToken, output = '"done"'})
    test.equal(status(run).status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(status(run).output)), "done")
    local entered, exited = 0, 0
    for _, event in history(run) do
        if event.type == "TaskStateEntered" then entered += 1 end
        if event.type == "TaskStateExited" then exited += 1 end
    end
    test.equal(entered, 1)
    test.equal(exited, 1)
end)

test.case("States.ALL does not catch an oversized callback result", function()
    local queue = assert(test.aws.sqs.CreateQueue({QueueName = "large"}).QueueUrl)
    local arn = machine("payload-limit", {StartAt = "Ask", States = {Ask = {Type = "Task",
        Resource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken", ResultPath = "$.response",
        Parameters = {QueueUrl = queue, ["MessageBody.$"] = "$$.Task.Token"},
        Catch = {{ErrorEquals = {"States.ALL"}, ResultPath = "$.caught", Next = "Recovered"}}, End = true},
        Recovered = {Type = "Succeed"},
    }})
    local run = start(arn, {padding = string.rep("a", 140000)})
    local token = assert(assert(test.aws.sqs.ReceiveMessage({QueueUrl = queue}).Messages)[1].Body)
    test.aws.states.SendTaskSuccess({taskToken = token, output = roto.json_encode({padding = string.rep("b", 140000)})})
    local result = status(run)
    test.equal(result.status, "FAILED")
    test.equal(result.error, "States.DataLimitExceeded")
end)
