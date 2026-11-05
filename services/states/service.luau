--!strict
-- AWS Step Functions (JSON 1.0, SigV4 name `states`, target prefix `AWSStepFunctions.`). Record only:
-- state machines are stored with their definition, which is never evaluated. StartExecution keeps
-- the input and leaves the execution RUNNING; StopExecution marks it ABORTED. RotoCompleteExecution
-- (a Roto extension) settles a running execution as SUCCEEDED, FAILED or TIMED_OUT for fixtures.
--
-- Document namespaces (per account and region): states/machine keyed by ARN, states/execution keyed
-- by execution ARN.
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "states/" .. kind)
end

local function machine_arn(ctx: Ctx, name: string): string
    return "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":stateMachine:" .. name
end

local function machine(ctx: Ctx, db: Db, arn: any): Doc
    local key = roto.required({ arn = arn }, "arn", "InvalidArn")
    local doc = db.get(ns(ctx, "machine"), key)
    if not doc then roto.error("StateMachineDoesNotExist", "State Machine Does Not Exist: '" .. key .. "'", 400) end
    return doc :: Doc
end

local function execution(ctx: Ctx, db: Db, arn: any): Doc
    local key = roto.required({ arn = arn }, "arn", "InvalidArn")
    local doc = db.get(ns(ctx, "execution"), key)
    if not doc then roto.error("ExecutionDoesNotExist", "Execution Does Not Exist: '" .. key .. "'", 400) end
    return doc :: Doc
end

local function valid_json(text: any, label: string): string
    if type(text) ~= "string" then roto.error("InvalidDefinition", label .. " must be a JSON string") end
    roto.json_decode(text, "InvalidDefinition")
    return text
end

function M.CreateStateMachine(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "name", "InvalidName")
    if #name > 80 or string.find(name, "[%s<>{}%[%]%?%*\"#%%\\^|~`$&,;:/]") then
        roto.error("InvalidName", "Invalid Name: '" .. name .. "'")
    end
    roto.required(input, "roleArn", "InvalidArn")
    valid_json(input.definition, "definition")
    local arn = machine_arn(ctx, name)
    if db.get(ns(ctx, "machine"), arn) then
        roto.error("StateMachineAlreadyExists", "State Machine Already Exists: '" .. arn .. "'")
    end
    local now = os.time()
    db.put(ns(ctx, "machine"), arn, {
        stateMachineArn = arn,
        name = name,
        definition = input.definition,
        roleArn = input.roleArn,
        type = input.type or "STANDARD",
        creationDate = now,
        status = "ACTIVE",
        loggingConfiguration = input.loggingConfiguration or { level = "OFF", includeExecutionData = false, destinations = roto.array() },
        tracingConfiguration = input.tracingConfiguration or { enabled = false },
    })
    return { stateMachineArn = arn, creationDate = now }
end

function M.DeleteStateMachine(ctx: Ctx, input: any, db: Db)
    -- AWS deletes asynchronously and succeeds for an unknown ARN; executions stay readable.
    db.delete(ns(ctx, "machine"), roto.required(input, "stateMachineArn", "InvalidArn"))
    return {}
end

function M.DescribeStateMachine(ctx: Ctx, input: any, db: Db)
    local doc = machine(ctx, db, input.stateMachineArn)
    return doc
end

function M.ListStateMachines(ctx: Ctx, input: any, db: Db)
    local rows: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "machine"))) do
        table.insert(rows, { stateMachineArn = doc.stateMachineArn, name = doc.name, type = doc.type, creationDate = doc.creationDate })
    end
    local out = roto.page({ MaxResults = input.maxResults, NextToken = input.nextToken }, rows, "stateMachines", 100, 1000, "InvalidToken")
    out.nextToken = out.NextToken
    out.NextToken = nil
    return out
end

function M.StartExecution(ctx: Ctx, input: any, db: Db)
    local sm = machine(ctx, db, input.stateMachineArn)
    local payload = input.input or "{}"
    valid_json(payload, "input")
    local name = input.name or roto.uuid()
    local arn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":execution:" .. sm.name .. ":" .. name
    local existing = db.get(ns(ctx, "execution"), arn) :: Doc?
    if existing then
        -- AWS treats a repeated name with identical input as idempotent while running.
        if existing.status == "RUNNING" and existing.input == payload then
            return { executionArn = arn, startDate = existing.startDate }
        end
        roto.error("ExecutionAlreadyExists", "Execution Already Exists: '" .. arn .. "'")
    end
    local now = os.time()
    db.put(ns(ctx, "execution"), arn, {
        executionArn = arn,
        stateMachineArn = sm.stateMachineArn,
        name = name,
        status = "RUNNING",
        startDate = now,
        input = payload,
        inputDetails = { included = true },
        traceHeader = input.traceHeader,
    })
    return { executionArn = arn, startDate = now }
end

function M.StopExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    if doc.status == "RUNNING" then
        doc.status = "ABORTED"
        doc.stopDate = os.time()
        doc.error = input.error
        doc.cause = input.cause
        db.put(ns(ctx, "execution"), doc.executionArn, doc)
    end
    return { stopDate = doc.stopDate or os.time() }
end

function M.DescribeExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    local out: Doc = {}
    for key, value in pairs(doc) do out[key] = value end
    if doc.status ~= "RUNNING" then out.stopDate = doc.stopDate end
    return out
end

function M.ListExecutions(ctx: Ctx, input: any, db: Db)
    local sm = machine(ctx, db, input.stateMachineArn)
    local rows: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "execution"))) do
        if doc.stateMachineArn == sm.stateMachineArn and (input.statusFilter == nil or doc.status == input.statusFilter) then
            table.insert(rows, {
                executionArn = doc.executionArn, stateMachineArn = doc.stateMachineArn, name = doc.name,
                status = doc.status, startDate = doc.startDate, stopDate = doc.stopDate,
            })
        end
    end
    table.sort(rows, function(a: any, b: any) return a.startDate > b.startDate end)
    local out = roto.page({ MaxResults = input.maxResults, NextToken = input.nextToken }, rows, "executions", 100, 1000, "InvalidToken")
    out.nextToken = out.NextToken
    out.NextToken = nil
    return out
end

function M.RotoCompleteExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    local status = input.status
    if status ~= "SUCCEEDED" and status ~= "FAILED" and status ~= "TIMED_OUT" then
        roto.error("ValidationException", "status must be SUCCEEDED, FAILED or TIMED_OUT")
    end
    doc.status = status
    doc.stopDate = os.time()
    doc.output = input.output
    doc.error = input.error
    doc.cause = input.cause
    db.put(ns(ctx, "execution"), doc.executionArn, doc)
    return {}
end

return M
