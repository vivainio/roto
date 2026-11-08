--!strict
local role = "arn:aws:iam::123456789012:role/test"
local function machine(name, definition)
    return test.aws.states.CreateStateMachine({name = name, roleArn = role,
        definition = roto.json_encode(definition)}).stateMachineArn
end
local function start(arn, input)
    return test.aws.states.StartExecution({stateMachineArn = arn, input = roto.json_encode(input)}).executionArn
end
local function status(arn)
    return test.aws.states.DescribeExecution({executionArn = arn})
end

test.case("ResultPath null discards a result and OutputPath selects input", function()
    local arn = machine("discard", {StartAt = "Discard", States = {
        Discard = {Type = "Pass", Result = 42, ResultPath = roto.null, Next = "Select"},
        Select = {Type = "Pass", OutputPath = "$.keep", End = true},
    }})
    local done = status(start(arn, {keep = {nested = 1}, other = true}))
    test.equal(done.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(done.output)), {nested = 1})
end)

test.case("missing input path fails without executing the next state", function()
    local arn = machine("missing", {StartAt = "Input", States = {
        Input = {Type = "Pass", InputPath = "$.missing", Next = "Never"},
        Never = {Type = "Fail", Error = "ShouldNotExecute"},
    }})
    local done = status(start(arn, {}))
    test.equal(done.status, "FAILED")
    test.equal(done.error, "States.Runtime")
end)

test.case("Choice evaluates guards, string patterns, timestamps and default", function()
    local arn = machine("choice", {StartAt = "Choose", States = {
        Choose = {Type = "Choice", Choices = {
            {And = {{Variable = "$.text", IsPresent = true}, {Variable = "$.text", StringMatches = "log-*"}}, Next = "Log"},
            {And = {{Variable = "$.time", IsPresent = true}, {Variable = "$.time", TimestampLessThan = "2030-01-01T00:00:00Z"}}, Next = "Old"},
            {Variable = "$.flag", IsPresent = false, Next = "Missing"},
        }, Default = "Other"},
        Log = {Type = "Pass", Result = "log", End = true},
        Old = {Type = "Pass", Result = "old", End = true},
        Missing = {Type = "Pass", Result = "missing", End = true},
        Other = {Type = "Fail", Error = "Rejected", Cause = "no match"},
    }})
    for _, item in {
        {input = {text = "log-1"}, output = "log"},
        {input = {time = "2029-06-01T10:00:00+02:00"}, output = "old"},
        {input = {}, output = "missing"},
    } do
        local done = status(start(arn, item.input))
        test.equal(done.status, "SUCCEEDED")
        test.equal(roto.json_decode(assert(done.output)), item.output)
    end
    local failed = status(start(arn, {flag = false}))
    test.equal(failed.status, "FAILED")
    test.equal(failed.error, "Rejected")
    test.equal(failed.cause, "no match")
end)

test.case("Choice without a matching rule or default fails", function()
    local arn = machine("no-default", {StartAt = "Choose", States = {
        Choose = {Type = "Choice", Choices = {{Variable = "$.n", NumericEquals = 1, Next = "Done"}}},
        Done = {Type = "Succeed"},
    }})
    test.equal(status(start(arn, {n = 1})).status, "SUCCEEDED")
    local failed = status(start(arn, {n = 2}))
    test.equal(failed.status, "FAILED")
    test.equal(failed.error, "States.NoChoiceMatched")
end)

test.case("invalid definitions report diagnostics and cannot be created", function()
    local bad = {StartAt = "Missing", States = {A = {Type = "Pass", Next = "Unknown"}}}
    local definition = roto.json_encode(bad)
    local validation = test.aws.states.ValidateStateMachineDefinition({definition = definition})
    test.equal(validation.result, "FAIL")
    assert(#validation.diagnostics > 0)
    test.expect_error("InvalidDefinition", function()
        return test.try_call("states", "CreateStateMachine", {name = "bad", roleArn = role, definition = definition})
    end)
    test.equal(#test.aws.states.ListStateMachines({}).stateMachines, 0)
end)

test.case("running executions are idempotent only for the same input", function()
    local arn = machine("idempotent", {StartAt = "Wait", States = {Wait = {Type = "Wait", Seconds = 60, End = true}}})
    local first = test.aws.states.StartExecution({stateMachineArn = arn, name = "same", input = '{"n":1}'}).executionArn
    test.equal(test.aws.states.StartExecution({stateMachineArn = arn, name = "same", input = '{"n":1}'}).executionArn, first)
    test.expect_error("ExecutionAlreadyExists", function()
        return test.try_call("states", "StartExecution", {stateMachineArn = arn, name = "same", input = '{"n":2}'})
    end)
    test.advance(60)
    test.equal(status(first).status, "SUCCEEDED")
    test.expect_error("ExecutionAlreadyExists", function()
        return test.try_call("states", "StartExecution", {stateMachineArn = arn, name = "same", input = '{"n":1}'})
    end)
end)

test.case("state machine updates do not change running executions", function()
    local arn = machine("update", {StartAt = "Wait", States = {
        Wait = {Type = "Wait", Seconds = 10, Next = "Result"},
        Result = {Type = "Pass", Result = "old", End = true},
    }})
    local old = start(arn, {})
    local new = {StartAt = "Result", States = {Result = {Type = "Pass", Result = "new", End = true}}}
    test.aws.states.UpdateStateMachine({stateMachineArn = arn, definition = roto.json_encode(new)})
    test.equal(roto.json_decode(assert(status(start(arn, {})).output)), "new")
    test.advance(10)
    test.equal(roto.json_decode(assert(status(old).output)), "old")
    local snapshot = test.aws.states.DescribeStateMachineForExecution({executionArn = old})
    test.equal(roto.json_decode(snapshot.definition).States.Result.Result, "old")
end)

test.case("history pages and reverse order preserve event ids", function()
    local arn = machine("pages", {StartAt = "A", States = {
        A = {Type = "Pass", Next = "B"}, B = {Type = "Pass", Next = "C"}, C = {Type = "Succeed"},
    }})
    local run = start(arn, {})
    local ids = {}
    local token: string? = nil
    repeat
        local page = test.aws.states.GetExecutionHistory({executionArn = run, maxResults = 3, nextToken = token})
        for _, event in page.events do table.insert(ids, event.id) end
        token = page.nextToken
    until token == nil
    assert(#ids > 3)
    for index, id in ids do test.equal(id, index) end
    local reverse = test.aws.states.GetExecutionHistory({executionArn = run, reverseOrder = true}).events
    for index, event in reverse do test.equal(event.id, ids[#ids - index + 1]) end
    test.equal(reverse[#reverse].previousEventId, 0)
end)
