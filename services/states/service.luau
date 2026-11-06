--!strict
-- AWS Step Functions (JSON 1.0, SigV4 name `states`, target prefix `AWSStepFunctions.`).
--
-- State machines are stored with their definition and executions are interpreted: Pass, Choice,
-- Wait, Succeed and Fail states run with the full path/parameter pipeline (InputPath, Parameters,
-- Result, ResultPath, OutputPath), JSONPath, `$$` context, intrinsic functions and Choice
-- operators, and every step is recorded in the execution history. Wait states and the
-- machine-level TimeoutSeconds use service timers (`db.schedule`), so they follow the virtual
-- clock (`POST /roto-api/clock/advance`).
--
-- Task states call other services in-process (Lambda functions, and SDK integrations such as
-- `arn:aws:states:::sqs:sendMessage`) with Retry and Catch; only the Request Response pattern is
-- supported. Retry backoffs run on timers. The callee commits in its own transaction, so a Task's
-- effect is not rolled back with the execution.
--
-- Callback Tasks (`.waitForTaskToken`) and activity Tasks park the execution with a task token until
-- SendTaskSuccess/SendTaskFailure, TimeoutSeconds or a missed HeartbeatSeconds settles them. An
-- activity Task queues its input under `states/activity-queue/<activity ARN>` for GetActivityTask.
--
-- `arn:aws:states:::states:startExecution` starts a nested execution in-process (a service cannot
-- call itself through roto.call); its `.sync`/`.sync:2` forms park the Task on a token that the child
-- delivers when it settles (see `notify_parent`).
--
-- `ecs:*` Tasks go to the ECS Luau service as raw requests; `ecs:runTask.sync` polls DescribeTasks on
-- a timer (virtual seconds) until the tasks stop.
--
-- Parallel and Map states fan out into threads: each branch or Map item is a thread with its own
-- position, retry counters, timers and task token, so Waits, Retry backoffs and callback or
-- activity Tasks work inside them and siblings make progress independently. Threads advance
-- round-robin, one transition at a time, within the request or job transaction. The parent
-- continues once every child has finished (results in branch/item order); a child failure that
-- the branch does not handle cancels its siblings and fails the Parallel/Map state, which then
-- applies its own Retry and Catch. A Map's MaxConcurrency limits how many items run at once;
-- ToleratedFailure belongs to distributed Map, which is rejected (an inline Map fails on its first
-- failed item, as in AWS).
-- RotoCompleteExecution (a Roto extension) settles a running execution as SUCCEEDED, FAILED or
-- TIMED_OUT for fixtures.
--
-- Document namespaces (per account and region): states/machine keyed by ARN; states/execution and
-- states/cursor (the execution's threads and the definition it started with) keyed by
-- execution ARN; states/history/<execution ARN> (one document per event) and states/history-count.
-- Timers: `resume/<arn>` wakes a waiting execution, `timeout/<arn>` enforces TimeoutSeconds.
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local ARRAY = getmetatable(roto.array())
local NULL = roto.null
-- Transitions one request or job may run before yielding to a follow-up job, which keeps a
-- looping machine from monopolizing the transaction.
local MAX_STEPS = 200
-- AWS fails an execution whose history would pass this many events.
local MAX_EVENTS = 25000

local function is_activity(resource: string): boolean
    return string.match(resource, "^arn:[^:]*:states:[^:]*:[^:]*:activity:.") ~= nil
end

local function timer_key(kind: string, arn: string, tid: string): string
    return kind .. "/" .. arn .. "/" .. tid
end

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

---------------------------------------------------------------------------------------------------
-- Values

local function is_array(value: any): boolean
    return type(value) == "table" and getmetatable(value) == ARRAY
end

local function is_object(value: any): boolean
    return type(value) == "table" and getmetatable(value) ~= ARRAY
end

local function copy(value: any): any
    if type(value) ~= "table" then return value end
    local out: any = {}
    for k, v in pairs(value) do out[k] = copy(v) end
    if is_array(value) then return roto.array(out) end
    return out
end

local function sorted_keys(value: any): { string }
    local keys: { string } = {}
    for k in pairs(value) do table.insert(keys, k) end
    table.sort(keys)
    return keys
end

-- Failures inside the interpreter are raised as tables and become the execution's error.
local function fail(code: string, cause: string): never
    error({ asl_error = code, cause = cause })
end

local function days_from_civil(year: number, month: number, day: number): number
    local y = if month <= 2 then year - 1 else year
    local era = math.floor(y / 400)
    local yoe = y - era * 400
    local mp = (month + 9) % 12
    local doy = math.floor((153 * mp + 2) / 5) + day - 1
    local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
    return era * 146097 + doe - 719468
end

-- RFC 3339 timestamp to Unix seconds, or nil.
local function parse_timestamp(text: any): number?
    if type(text) ~= "string" then return nil end
    local y, mo, d, h, mi, sec, frac, zone = string.match(text, "^(%d%d%d%d)%-(%d%d)%-(%d%d)[Tt](%d%d):(%d%d):(%d%d)(%.?%d*)(.*)$")
    if not y then return nil end
    local zone_text: string = zone or ""
    local fraction: string = frac or ""
    local offset = 0
    if zone_text == "Z" or zone_text == "z" then
        offset = 0
    else
        local sign, oh, om = string.match(zone_text, "^([%+%-])(%d%d):(%d%d)$")
        if not sign then return nil end
        offset = ((tonumber(oh) :: number) * 3600 + (tonumber(om) :: number) * 60) * (if sign == "-" then -1 else 1)
    end
    local seconds = days_from_civil(tonumber(y) :: number, tonumber(mo) :: number, tonumber(d) :: number) * 86400
        + (tonumber(h) :: number) * 3600 + (tonumber(mi) :: number) * 60 + (tonumber(sec) :: number) - offset
    if fraction ~= "" and fraction ~= "." then seconds += tonumber("0" .. fraction) :: number end
    return seconds
end

local function format_timestamp(seconds: number): string
    return os.date("!%Y-%m-%dT%H:%M:%SZ", math.floor(seconds)) :: string
end

---------------------------------------------------------------------------------------------------
-- JSONPath (the subset Step Functions documents: `$`, `.name`, `['name']`, `[n]`, `[*]`, `.*`)

type Step = { kind: string, key: string?, index: number? }

local function parse_path(path: string): { Step }
    if string.sub(path, 1, 1) ~= "$" then fail("States.Runtime", "Invalid path '" .. path .. "': must start with $") end
    local steps: { Step } = {}
    local pos = 2
    while pos <= #path do
        local rest = string.sub(path, pos)
        local name = string.match(rest, "^%.([%w_%-]+)")
        local quoted = string.match(rest, "^%['([^']*)'%]")
        local index = string.match(rest, "^%[(%d+)%]")
        if name then
            table.insert(steps, { kind = "key", key = name })
            pos += #name + 1
        elseif quoted then
            table.insert(steps, { kind = "key", key = quoted })
            pos += #quoted + 4
        elseif index then
            table.insert(steps, { kind = "index", index = tonumber(index) })
            pos += #index + 2
        elseif string.match(rest, "^%[%*%]") then
            table.insert(steps, { kind = "wild" })
            pos += 3
        elseif string.match(rest, "^%.%*") then
            table.insert(steps, { kind = "wild" })
            pos += 2
        else
            fail("States.Runtime", "Unsupported JSONPath syntax in '" .. path .. "'")
        end
    end
    return steps
end

local function walk(node: any, steps: { Step }, i: number): (any, boolean)
    if i > #steps then return node, true end
    local step = steps[i]
    if step.kind == "key" then
        if is_object(node) and node[step.key :: string] ~= nil then return walk(node[step.key :: string], steps, i + 1) end
        return nil, false
    elseif step.kind == "index" then
        if is_array(node) and node[(step.index :: number) + 1] ~= nil then return walk(node[(step.index :: number) + 1], steps, i + 1) end
        return nil, false
    end
    if not is_array(node) and not is_object(node) then return nil, false end
    local out = roto.array()
    local children: { any } = {}
    if is_array(node) then
        children = node
    else
        for _, key in ipairs(sorted_keys(node)) do table.insert(children, node[key]) end
    end
    for _, child in ipairs(children) do
        local value, found = walk(child, steps, i + 1)
        if found then table.insert(out, value) end
    end
    return out, true
end

type Scope = { data: any, context: any }

-- Resolves `$...` against the state input or `$$...` against the context object.
local function lookup(path: string, scope: Scope): (any, boolean)
    if string.sub(path, 1, 2) == "$$" then
        return walk(scope.context, parse_path(string.sub(path, 2)), 1)
    end
    return walk(scope.data, parse_path(path), 1)
end

local function resolve(path: string, scope: Scope): any
    local value, found = lookup(path, scope)
    if not found then fail("States.Runtime", "The JSONPath '" .. path .. "' could not be found in the input '" .. roto.json_encode(scope.data) .. "'") end
    return value
end

-- Writes `value` at a `$.a.b` path of a copy of `input`.
local function set_path(input: any, path: string, value: any): any
    local steps = parse_path(path)
    if #steps == 0 then return value end
    local root = copy(input)
    if not is_object(root) then fail("States.ResultPathMatchFailure", "Unable to apply ResultPath '" .. path .. "' to a non-object input") end
    local node: any = root
    for i, step in ipairs(steps) do
        if step.kind ~= "key" then fail("States.Runtime", "ResultPath '" .. path .. "' must use only field names") end
        local key = step.key :: string
        if i == #steps then
            node[key] = value
        else
            if node[key] == nil then node[key] = {} end
            node = node[key]
            if not is_object(node) then fail("States.ResultPathMatchFailure", "Unable to apply ResultPath '" .. path .. "'") end
        end
    end
    return root
end

---------------------------------------------------------------------------------------------------
-- Intrinsic functions and payload templates

local INTRINSICS: { [string]: (any, { any }) -> any } = {}

local function expect_array(value: any, name: string): any
    if not is_array(value) then fail("States.Runtime", name .. " expects an array") end
    return value
end

local function expect_number(value: any, name: string): number
    if type(value) ~= "number" then fail("States.Runtime", name .. " expects a number") end
    return value
end

local function expect_string(value: any, name: string): string
    if type(value) ~= "string" then fail("States.Runtime", name .. " expects a string") end
    return value
end

INTRINSICS["States.Format"] = function(_, args)
    local template = expect_string(args[1], "States.Format")
    local index = 1
    local out = string.gsub(template, "{}", function()
        index += 1
        local value = args[index]
        if value == nil then fail("States.Runtime", "States.Format has more placeholders than arguments") end
        if type(value) == "string" then return value end
        return roto.json_encode(value)
    end)
    return out
end
INTRINSICS["States.StringToJson"] = function(_, args)
    return roto.json_decode(expect_string(args[1], "States.StringToJson"), "States.Runtime")
end
INTRINSICS["States.JsonToString"] = function(_, args) return roto.json_encode(args[1]) end
INTRINSICS["States.Array"] = function(_, args)
    local out = roto.array()
    for i = 1, #args do table.insert(out, args[i]) end
    return out
end
INTRINSICS["States.ArrayLength"] = function(_, args) return #expect_array(args[1], "States.ArrayLength") end
INTRINSICS["States.ArrayGetItem"] = function(_, args)
    local array = expect_array(args[1], "States.ArrayGetItem")
    local value = array[expect_number(args[2], "States.ArrayGetItem") + 1]
    if value == nil then fail("States.Runtime", "States.ArrayGetItem index out of range") end
    return value
end
INTRINSICS["States.ArrayContains"] = function(_, args)
    local needle = roto.json_encode(args[2])
    for _, item in ipairs(expect_array(args[1], "States.ArrayContains")) do
        if roto.json_encode(item) == needle then return true end
    end
    return false
end
INTRINSICS["States.ArrayUnique"] = function(_, args)
    local seen: { [string]: boolean } = {}
    local out = roto.array()
    for _, item in ipairs(expect_array(args[1], "States.ArrayUnique")) do
        local key = roto.json_encode(item)
        if not seen[key] then
            seen[key] = true
            table.insert(out, item)
        end
    end
    return out
end
INTRINSICS["States.ArrayRange"] = function(_, args)
    local first, last, step = expect_number(args[1], "States.ArrayRange"), expect_number(args[2], "States.ArrayRange"), expect_number(args[3], "States.ArrayRange")
    if step == 0 or (step > 0 and first > last) or (step < 0 and first < last) then
        fail("States.Runtime", "States.ArrayRange has an empty or invalid range")
    end
    local out = roto.array()
    local value = first
    while (step > 0 and value <= last) or (step < 0 and value >= last) do
        table.insert(out, value)
        value += step
        if #out > 1000 then fail("States.Runtime", "States.ArrayRange is limited to 1000 items") end
    end
    return out
end
INTRINSICS["States.ArrayPartition"] = function(_, args)
    local array = expect_array(args[1], "States.ArrayPartition")
    local size = expect_number(args[2], "States.ArrayPartition")
    if size < 1 then fail("States.Runtime", "States.ArrayPartition needs a positive chunk size") end
    local out = roto.array()
    for i = 1, #array, size do
        local chunk = roto.array()
        for j = i, math.min(i + size - 1, #array) do table.insert(chunk, array[j]) end
        table.insert(out, chunk)
    end
    return out
end
INTRINSICS["States.Base64Encode"] = function(_, args) return roto.encode_base64(expect_string(args[1], "States.Base64Encode")) end
INTRINSICS["States.Base64Decode"] = function(_, args)
    local ok, decoded = pcall(roto.decode_base64, expect_string(args[1], "States.Base64Decode"))
    if not ok then fail("States.Runtime", "States.Base64Decode got invalid base64") end
    return decoded
end
INTRINSICS["States.Hash"] = function(_, args)
    local algorithm = expect_string(args[2], "States.Hash")
    if algorithm ~= "SHA-256" then fail("States.Runtime", "States.Hash supports only SHA-256 here, not " .. algorithm) end
    return roto.sha256(expect_string(args[1], "States.Hash"))
end
INTRINSICS["States.JsonMerge"] = function(_, args)
    if args[3] ~= false then fail("States.Runtime", "States.JsonMerge supports only shallow merges (third argument false)") end
    if not is_object(args[1]) or not is_object(args[2]) then fail("States.Runtime", "States.JsonMerge expects two objects") end
    local out: any = copy(args[1])
    for k, v in pairs(args[2]) do out[k] = copy(v) end
    return out
end
INTRINSICS["States.MathAdd"] = function(_, args) return expect_number(args[1], "States.MathAdd") + expect_number(args[2], "States.MathAdd") end
INTRINSICS["States.MathRandom"] = function(_, args)
    local low, high = expect_number(args[1], "States.MathRandom"), expect_number(args[2], "States.MathRandom")
    if low > high then fail("States.Runtime", "States.MathRandom start must not exceed end") end
    return math.random(low, high)
end
INTRINSICS["States.StringSplit"] = function(_, args)
    local text, delimiters = expect_string(args[1], "States.StringSplit"), expect_string(args[2], "States.StringSplit")
    local out = roto.array()
    local current = ""
    for i = 1, #text do
        local ch = string.sub(text, i, i)
        if string.find(delimiters, ch, 1, true) then
            table.insert(out, current)
            current = ""
        else
            current ..= ch
        end
    end
    table.insert(out, current)
    return out
end
INTRINSICS["States.UUID"] = function(_, _) return roto.uuid() end

-- Evaluates `States.Name(arg, ...)`; arguments are 'strings', numbers, true/false/null, paths and
-- nested calls.
local function evaluate_intrinsic(source: string, scope: Scope): any
    local pos = 1
    local function skip()
        pos = string.find(source, "%S", pos) or (#source + 1)
    end
    local parse_value: () -> any
    local function parse_call(): any
        local matched = string.match(source, "^States%.%w+", pos)
        if not matched then return fail("States.Runtime", "Invalid intrinsic function: " .. source) end
        local name: string = matched
        pos += #name
        skip()
        if string.sub(source, pos, pos) ~= "(" then fail("States.Runtime", "Invalid intrinsic function: " .. source) end
        pos += 1
        local args: { any } = {}
        skip()
        if string.sub(source, pos, pos) == ")" then
            pos += 1
        else
            while true do
                table.insert(args, parse_value())
                skip()
                local ch = string.sub(source, pos, pos)
                pos += 1
                if ch == ")" then break end
                if ch ~= "," then fail("States.Runtime", "Invalid intrinsic function: " .. source) end
            end
        end
        local implementation = INTRINSICS[name]
        if not implementation then fail("States.Runtime", "Unsupported intrinsic function " .. name) end
        return implementation(scope, args)
    end
    parse_value = function(): any
        skip()
        local ch = string.sub(source, pos, pos)
        if ch == "'" then
            local out = ""
            pos += 1
            while pos <= #source do
                local c = string.sub(source, pos, pos)
                if c == "\\" then
                    out ..= string.sub(source, pos + 1, pos + 1)
                    pos += 2
                elseif c == "'" then
                    pos += 1
                    return out
                else
                    out ..= c
                    pos += 1
                end
            end
            return fail("States.Runtime", "Unterminated string in intrinsic function: " .. source)
        elseif ch == "$" then
            local path = string.match(source, "^%$%$?[%w_%.%[%]%*'%-]*", pos) :: string
            pos += #path
            return resolve(path, scope)
        elseif string.match(source, "^States%.", pos) then
            return parse_call()
        end
        local number = string.match(source, "^%-?%d+%.?%d*", pos)
        if number then
            pos += #number
            return tonumber(number)
        end
        if string.sub(source, pos, pos + 3) == "true" then
            pos += 4
            return true
        elseif string.sub(source, pos, pos + 4) == "false" then
            pos += 5
            return false
        end
        if string.sub(source, pos, pos + 3) == "null" then
            pos += 4
            return NULL
        end
        return fail("States.Runtime", "Invalid intrinsic function: " .. source)
    end
    local value = parse_call()
    skip()
    if pos <= #source then fail("States.Runtime", "Invalid intrinsic function: " .. source) end
    return value
end

-- A `.$` field: an intrinsic function or a path.
local function evaluate_reference(reference: any, scope: Scope): any
    if type(reference) ~= "string" then fail("States.Runtime", "A '.$' field must be a string") end
    if string.sub(reference, 1, 7) == "States." then return evaluate_intrinsic(reference, scope) end
    return resolve(reference, scope)
end

local function apply_template(template: any, scope: Scope): any
    if is_array(template) then
        local out = roto.array()
        for _, item in ipairs(template) do table.insert(out, apply_template(item, scope)) end
        return out
    elseif is_object(template) then
        local out: any = {}
        for key, value in pairs(template) do
            if string.sub(key, -2) == ".$" then
                out[string.sub(key, 1, -3)] = evaluate_reference(value, scope)
            else
                out[key] = apply_template(value, scope)
            end
        end
        return out
    end
    return template
end

---------------------------------------------------------------------------------------------------
-- Input/output paths

local function apply_input_path(state: Doc, data: any, context: any): any
    local path = state.InputPath
    if path == nil then return data end
    if path == NULL then return {} end
    return resolve(path, { data = data, context = context })
end

local function apply_output_path(state: Doc, data: any, context: any): any
    local path = state.OutputPath
    if path == nil then return data end
    if path == NULL then return {} end
    return resolve(path, { data = data, context = context })
end

local function apply_result_path(state: Doc, input: any, result: any): any
    local path = state.ResultPath
    if path == nil then return result end
    if path == NULL then return input end
    return set_path(input, path, result)
end

---------------------------------------------------------------------------------------------------
-- Choice rules

type Comparator = { kind: string, op: string, path: boolean }
local COMPARATORS: { [string]: Comparator } = {}
for _, kind in ipairs({ "String", "Numeric", "Boolean", "Timestamp" }) do
    for _, op in ipairs({ "Equals", "LessThan", "GreaterThan", "LessThanEquals", "GreaterThanEquals" }) do
        if (kind ~= "Boolean") or op == "Equals" then
            COMPARATORS[kind .. op] = { kind = kind, op = op, path = false }
            COMPARATORS[kind .. op .. "Path"] = { kind = kind, op = op, path = true }
        end
    end
end
COMPARATORS["StringMatches"] = { kind = "String", op = "Matches", path = false }

local function glob_matches(pattern: string, text: string): boolean
    local lua_pattern = "^"
    local i = 1
    while i <= #pattern do
        local ch = string.sub(pattern, i, i)
        if ch == "\\" then
            i += 1
            lua_pattern ..= "%" .. string.sub(pattern, i, i)
        elseif ch == "*" then
            lua_pattern ..= ".*"
        elseif string.find(ch, "%w") then
            lua_pattern ..= ch
        else
            lua_pattern ..= "%" .. ch
        end
        i += 1
    end
    return string.find(text, lua_pattern .. "$") ~= nil
end

local function compare(kind: string, op: string, value: any, operand: any): boolean
    local left, right = value, operand
    if kind == "String" then
        if type(left) ~= "string" or type(right) ~= "string" then return false end
        if op == "Matches" then return glob_matches(right, left) end
    elseif kind == "Numeric" then
        if type(left) ~= "number" or type(right) ~= "number" then return false end
    elseif kind == "Boolean" then
        if type(left) ~= "boolean" or type(right) ~= "boolean" then return false end
    else
        left, right = parse_timestamp(left), parse_timestamp(right)
        if left == nil or right == nil then return false end
    end
    if op == "Equals" then return left == right end
    if kind == "Boolean" then return false end
    if op == "LessThan" then return left < right end
    if op == "GreaterThan" then return left > right end
    if op == "LessThanEquals" then return left <= right end
    return left >= right
end

local function eval_rule(rule: any, scope: Scope): boolean
    if type(rule) ~= "table" then fail("States.Runtime", "A Choice rule must be an object") end
    if rule.And ~= nil then
        for _, inner in ipairs(rule.And) do
            if not eval_rule(inner, scope) then return false end
        end
        return true
    elseif rule.Or ~= nil then
        for _, inner in ipairs(rule.Or) do
            if eval_rule(inner, scope) then return true end
        end
        return false
    elseif rule.Not ~= nil then
        return not eval_rule(rule.Not, scope)
    end
    if type(rule.Variable) ~= "string" then fail("States.Runtime", "A Choice rule needs a Variable") end
    local value, found = lookup(rule.Variable, scope)
    local present: Doc = rule
    for key, operand in pairs(present) do
        if key == "IsPresent" then
            return found == operand
        end
    end
    if not found then fail("States.Runtime", "Invalid path '" .. rule.Variable .. "': The choice state's condition path references an invalid value.") end
    local fields: Doc = rule
    for key, operand in pairs(fields) do
        local comparator = COMPARATORS[key]
        if comparator then
            if comparator.path then operand = resolve(operand :: string, scope) end
            return compare(comparator.kind, comparator.op, value, operand)
        elseif key == "IsNull" then
            return (value == NULL) == operand
        elseif key == "IsNumeric" then
            return (type(value) == "number") == operand
        elseif key == "IsString" then
            return (type(value) == "string") == operand
        elseif key == "IsBoolean" then
            return (type(value) == "boolean") == operand
        elseif key == "IsTimestamp" then
            return (parse_timestamp(value) ~= nil) == operand
        end
    end
    return fail("States.Runtime", "A Choice rule has no supported comparison operator")
end

---------------------------------------------------------------------------------------------------
-- Definition validation

local STATE_TYPES: { [string]: boolean } = { Pass = true, Task = true, Choice = true, Wait = true, Succeed = true, Fail = true, Parallel = true, Map = true }

local function validate_definition(text: any): string
    valid_json(text, "definition")
    local function invalid(message: string): never
        return roto.error("InvalidDefinition", "Invalid State Machine Definition: " .. message)
    end
    local function check(definition: any)
        if not is_object(definition) or type(definition.StartAt) ~= "string" or not is_object(definition.States) then
            invalid("a definition needs StartAt and States")
        end
        local states: Doc = definition.States
        if next(states) == nil then invalid("States must not be empty") end
        if states[definition.StartAt] == nil then invalid("StartAt '" .. definition.StartAt .. "' is not a state") end
        local function target(name: any, from: string)
            if type(name) ~= "string" or states[name] == nil then invalid("state '" .. from .. "' transitions to unknown state '" .. tostring(name) .. "'") end
        end
        for name, state in pairs(states) do
            if not is_object(state) or not STATE_TYPES[state.Type] then invalid("state '" .. name .. "' has an unknown Type") end
            local kind = state.Type
            if kind == "Choice" then
                if not is_array(state.Choices) or #state.Choices == 0 then invalid("Choice state '" .. name .. "' needs Choices") end
                for _, rule in ipairs(state.Choices) do target(rule.Next, name) end
                if state.Default ~= nil then target(state.Default, name) end
            elseif kind ~= "Succeed" and kind ~= "Fail" then
                if state.Next ~= nil then
                    target(state.Next, name)
                elseif state.End ~= true then
                    invalid("state '" .. name .. "' needs Next or End")
                end
            end
            if kind == "Task" or kind == "Parallel" or kind == "Map" then
                local catches: { Doc } = state.Catch or {}
                for _, rule in ipairs(catches) do target(rule.Next, name) end
                local retries: { Doc } = state.Retry or {}
                for _, rule in ipairs(retries) do
                    if not is_array(rule.ErrorEquals) then invalid("a Retry rule in '" .. name .. "' needs ErrorEquals") end
                end
            end
            if kind == "Task" and type(state.Resource) ~= "string" then invalid("Task state '" .. name .. "' needs a Resource") end
            if kind == "Parallel" then
                if not is_array(state.Branches) then invalid("Parallel state '" .. name .. "' needs Branches") end
                for _, branch in ipairs(state.Branches) do check(branch) end
            elseif kind == "Map" then
                local processor = state.ItemProcessor or state.Iterator
                if processor == nil then invalid("Map state '" .. name .. "' needs an ItemProcessor") end
                local config: any = (processor :: Doc).ProcessorConfig
                if state.ItemReader ~= nil or state.ResultWriter ~= nil or (type(config) == "table" and config.Mode == "DISTRIBUTED") then
                    invalid("Map state '" .. name .. "' is a distributed Map (ItemReader, ResultWriter or Mode DISTRIBUTED), which is not supported")
                end
                if state.MaxConcurrency ~= nil and (type(state.MaxConcurrency) ~= "number" or state.MaxConcurrency < 0 or state.MaxConcurrency % 1 ~= 0) then
                    invalid("Map state '" .. name .. "' needs a non-negative integer MaxConcurrency")
                end
                check(processor)
            end
            if kind == "Wait" then
                if state.Seconds == nil and state.Timestamp == nil and state.SecondsPath == nil and state.TimestampPath == nil then
                    invalid("Wait state '" .. name .. "' needs Seconds, Timestamp, SecondsPath or TimestampPath")
                end
            end
        end
    end
    check(roto.json_decode(text :: string, "InvalidDefinition"))
    return text :: string
end

---------------------------------------------------------------------------------------------------
-- History and settling

-- `events` holds the events added since the last save; `count` is every event so far. Each event
-- is its own document, so a run writes only what it added.
type History = { events: { any }, count: number }

local function emit(history: History, event_type: string, details_key: string?, details: any)
    history.count += 1
    local id = history.count
    local event: Doc = { id = id, timestamp = roto.now(), type = event_type, previousEventId = id - 1 }
    if details_key then event[details_key] = details end
    table.insert(history.events, event)
end

local function history_space(ctx: Ctx, arn: string): string
    return ns(ctx, "history") .. "/" .. arn
end

local function load_history(ctx: Ctx, db: Db, arn: string): History
    local meta = db.get(ns(ctx, "history-count"), arn) :: Doc?
    return { events = {}, count = if meta then meta.count else 0 }
end

local function save_history(ctx: Ctx, db: Db, arn: string, history: History)
    for _, event in ipairs(history.events) do
        db.put(history_space(ctx, arn), string.format("%08d", event.id), event)
    end
    history.events = {}
    db.put(ns(ctx, "history-count"), arn, { count = history.count })
end

-- Cancels every timer and task token of an execution. `cursor` is the in-memory cursor when the
-- caller has one that is newer than the stored copy.
local function cancel_timers(ctx: Ctx, db: Db, arn: string, cursor: Cursor?): { string }
    db.cancel("timeout/" .. arn)
    db.cancel("continue/" .. arn)
    local current = cursor or (db.get(ns(ctx, "cursor"), arn) :: Cursor?)
    local children: { string } = {}
    if current then
        for tid, thread in pairs((current :: Cursor).threads) do
            db.cancel(timer_key("resume", arn, tid))
            db.cancel(timer_key("task", arn, tid))
            db.cancel(timer_key("heartbeat", arn, tid))
            db.cancel(timer_key("poll", arn, tid))
            if thread.waiting then
                db.delete(ns(ctx, "token"), thread.waiting.token)
                if thread.waiting.child then table.insert(children, thread.waiting.child) end
            end
        end
    end
    return children
end

local SETTLED_EVENTS: { [string]: { string } } = {
    SUCCEEDED = { "ExecutionSucceeded", "executionSucceededEventDetails" },
    FAILED = { "ExecutionFailed", "executionFailedEventDetails" },
    ABORTED = { "ExecutionAborted", "executionAbortedEventDetails" },
    TIMED_OUT = { "ExecutionTimedOut", "executionTimedOutEventDetails" },
}

local notify_parent: ((Ctx, Db, Doc) -> ())? = nil
local abort_nested: (Ctx, Db, string) -> () = function() end

local function settle(ctx: Ctx, db: Db, exec: Doc, history: History, status: string, output: any, err: any, cause: any, cursor: Cursor?)
    exec.status = status
    exec.stopDate = os.time()
    if status == "SUCCEEDED" then
        exec.output = output
        exec.outputDetails = { included = true }
    else
        exec.error = err
        exec.cause = cause
    end
    local event = SETTLED_EVENTS[status]
    if status == "SUCCEEDED" then
        emit(history, event[1], event[2], { output = output, outputDetails = { truncated = false } })
    else
        emit(history, event[1], event[2], { error = err, cause = cause })
    end
    db.put(ns(ctx, "execution"), exec.executionArn, exec)
    local children = cancel_timers(ctx, db, exec.executionArn, cursor)
    db.delete(ns(ctx, "cursor"), exec.executionArn)
    if notify_parent then notify_parent(ctx, db, exec) end
    -- Nested executions this one was waiting for (`.sync`) are stopped with it.
    for _, child_arn in ipairs(children) do abort_nested(ctx, db, child_arn) end
end

-- Stops a nested execution that is still running because the Task waiting for it went away.
abort_nested = function(ctx: Ctx, db: Db, child_arn: string)
    local child = db.get(ns(ctx, "execution"), child_arn) :: Doc?
    if child and (child :: Doc).status == "RUNNING" then
        local child_history = load_history(ctx, db, child_arn)
        settle(ctx, db, child :: Doc, child_history, "ABORTED", nil, "States.Aborted", "The parent execution was stopped")
        save_history(ctx, db, child_arn, child_history)
    end
end

---------------------------------------------------------------------------------------------------
-- Interpreter

type Outcome = { output: any, next: string?, wait: number?, error: string?, cause: string?, waiting: string?, queued: any, spawn: { Spawn }? }

local function step_pass(state: Doc, data: any, scope: Scope): any
    local effective = apply_input_path(state, data, scope.context)
    local result: any
    if state.Result ~= nil then
        result = copy(state.Result)
    elseif state.Parameters ~= nil then
        result = apply_template(state.Parameters, { data = effective, context = scope.context })
    else
        result = effective
    end
    return apply_output_path(state, apply_result_path(state, data, result), scope.context)
end

local function step_wait(state: Doc, data: any, scope: Scope): (any, number)
    local effective = apply_input_path(state, data, scope.context)
    local inner: Scope = { data = effective, context = scope.context }
    local seconds: number
    if state.Seconds ~= nil then
        seconds = state.Seconds
    elseif state.SecondsPath ~= nil then
        local value = resolve(state.SecondsPath, inner)
        if type(value) ~= "number" then fail("States.Runtime", "SecondsPath must resolve to a number") end
        seconds = value
    else
        local stamp = state.Timestamp
        if state.TimestampPath ~= nil then stamp = resolve(state.TimestampPath, inner) end
        local at = parse_timestamp(stamp)
        if at == nil then fail("States.Runtime", "Wait timestamp must be an RFC 3339 timestamp") end
        seconds = (at :: number) - roto.now()
    end
    if type(seconds) ~= "number" or seconds ~= seconds then fail("States.Runtime", "Wait Seconds must be a number") end
    return apply_output_path(state, effective, scope.context), math.max(seconds, 0)
end

-- The error name and cause of something raised while evaluating a state.
local function failure_of(failure: any): (string, string)
    if type(failure) == "table" and failure.asl_error then return failure.asl_error, failure.cause end
    return "States.Runtime", tostring(failure)
end

-- Task states call other services in-process (`roto.call`/`roto.try_wire`) while the execution's
-- transaction is open: the callee commits on its own and is not rolled back if this execution's
-- own writes are later rolled back.
local SERVICE_NAMES: { [string]: string } = {
    dynamodb = "DynamoDb", sqs = "Sqs", sns = "Sns", secretsmanager = "SecretsManager", ssm = "Ssm",
    kms = "Kms", kinesis = "Kinesis", events = "Events", logs = "Logs", s3 = "S3", lambda = "Lambda",
}

local function capitalize(text: string): string
    return string.upper(string.sub(text, 1, 1)) .. string.sub(text, 2)
end

-- The call a resource ARN stands for: { service, operation } for an SDK integration or "lambda"
-- for a function (a bare function ARN, or `arn:aws:states:::lambda:invoke`).
-- "v1" for `.sync`, "v2" for `.sync:2` (the child's output parsed as JSON), nil otherwise.
local function sync_mode(resource: string): string?
    if string.match(resource, "%.sync:2$") then return "v2" end
    if string.match(resource, "%.sync$") then return "v1" end
    return nil
end

-- Set by `run` for the duration of a run: Task states reach the execution's own service (nested
-- executions) through it, because a service cannot call itself through `roto.call`.
local active: { ctx: Ctx, db: Db }? = nil
local start_execution: (Ctx, any, Db, { token: string, v2: boolean }?) -> any

local function task_target(resource: any): (string, string)
    if type(resource) ~= "string" then fail("States.Runtime", "A Task state needs a Resource") end
    local text: string = string.gsub(resource, "%.waitForTaskToken$", "")
    local synchronous = string.match(text, "%.sync:?2?$") ~= nil
    if synchronous then text = string.gsub(text, "%.sync:?2?$", "") end
    if synchronous and not (string.match(text, ":states:::states:startExecution$") or string.match(text, ":states:::ecs:runTask$")) then
        fail("States.Runtime", "The service integration pattern of '" .. resource .. "' is not supported; .sync works only for states:startExecution and ecs:runTask")
    end
    if string.match(text, "^arn:[^:]*:lambda:[^:]*:[^:]*:function:.") then return "lambda", text end
    if is_activity(text) then return "activity", text end
    local integration = string.match(text, "^arn:[^:]*:states:::(.+)$")
    if integration == nil then fail("States.Runtime", "Unsupported Task resource '" .. text .. "'") end
    integration = string.gsub(integration :: string, "^aws%-sdk:", "")
    if integration == "lambda:invoke" then return "lambda", "" end
    local service, action = string.match(integration :: string, "^([%w%-]+):(%w+)$")
    if service == nil then fail("States.Runtime", "Unsupported Task resource '" .. text .. "'") end
    return service :: string, capitalize(action :: string)
end

local function function_name(arn: string): string
    return string.match(arn, "function:([^:]+)") or arn
end

-- Calls the integration; returns the result or raises the Step Functions error for the failure.
local function describe_doc(exec: Doc, v2: boolean): Doc
    local out: Doc = {
        ExecutionArn = exec.executionArn, StateMachineArn = exec.stateMachineArn, Name = exec.name,
        Status = exec.status, StartDate = exec.startDate, StopDate = exec.stopDate, Input = exec.input,
        Error = exec.error, Cause = exec.cause,
    }
    if exec.output ~= nil then out.Output = if v2 then roto.json_decode(exec.output) else exec.output end
    return out
end

-- ECS speaks lowerCamel in its API and PascalCase in Step Functions parameters and results.
local function recase(value: any, to_upper: boolean): any
    if type(value) ~= "table" then return value end
    if is_array(value) then
        local out = {}
        for index, item in ipairs(value) do out[index] = recase(item, to_upper) end
        return roto.array(out)
    end
    local out: Doc = {}
    for key, item in pairs(value) do
        local name = tostring(key)
        local first = string.sub(name, 1, 1)
        out[(if to_upper then string.upper(first) else string.lower(first)) .. string.sub(name, 2)] = recase(item, to_upper)
    end
    return out
end

-- ECS is a Luau service, which `roto.call` does not reach, so it is called as a raw request.
local function ecs_call(operation: string, body: any): any
    local response = roto.try_wire("ecs", "POST", "/", {
        headers = { ["x-amz-target"] = "AmazonEC2ContainerServiceV20141113." .. operation, ["content-type"] = "application/x-amz-json-1.1" },
        body = roto.json_encode(body),
    })
    if not response.ok then fail("Ecs." .. (response.code or "Error"), response.message or "") end
    return roto.json_decode(response.body :: string)
end

-- How often a `.sync` integration that has to be polled looks at its target, in virtual seconds.
local POLL_SECONDS = 5

-- Looks at the tasks of an `ecs:runTask.sync` Task: nil while any is still running, otherwise the
-- DescribeTasks result and whether a container exited with a failure.
local function poll_ecs(poll: any): (any, boolean)
    local described = ecs_call("DescribeTasks", { cluster = poll.cluster, tasks = poll.tasks })
    local failed = false
    for _, task in ipairs(described.tasks) do
        if task.lastStatus ~= "STOPPED" then return nil, false end
        local containers: { Doc } = task.containers or {}
        for _, container in ipairs(containers) do
            if container.exitCode ~= nil and container.exitCode ~= 0 then failed = true end
        end
    end
    return recase(described, true), failed
end

local function task_call(resource: string, parameters: any, token: string?): any
    local service, operation = task_target(resource)
    local label = SERVICE_NAMES[service] or capitalize(service)
    if service == "states" and operation == "StartExecution" then
        if active == nil then fail("States.Runtime", "states:startExecution is not available here") end
        local params: any = parameters or {}
        local input: any = params.Input
        local started = (start_execution :: (Ctx, any, Db, { token: string, v2: boolean }?) -> any)(
            (active :: { ctx: Ctx, db: Db }).ctx,
            { stateMachineArn = params.StateMachineArn, name = params.Name, input = if type(input) == "string" then input else roto.json_encode(if input == nil then {} else input) },
            (active :: { ctx: Ctx, db: Db }).db,
            if token ~= nil and sync_mode(resource) ~= nil then { token = token, v2 = sync_mode(resource) == "v2" } else nil
        )
        local exec = (active :: { ctx: Ctx, db: Db }).db.get(ns((active :: { ctx: Ctx, db: Db }).ctx, "execution"), started.executionArn) :: Doc
        if token ~= nil and sync_mode(resource) ~= nil then
            if exec.status == "RUNNING" then return { Pending = started.executionArn } end
            return describe_doc(exec, sync_mode(resource) == "v2")
        end
        return { ExecutionArn = started.executionArn, StartDate = started.startDate }
    end
    if service == "ecs" then
        local started = recase(ecs_call(operation, recase(parameters or {}, false)), true)
        if sync_mode(resource) ~= nil and operation == "RunTask" then
            local arns = {}
            local tasks: { Doc } = started.Tasks or {}
            for _, task in ipairs(tasks) do table.insert(arns, task.TaskArn) end
            if #arns == 0 then fail("Ecs.TaskFailed", roto.json_encode(started.Failures or {})) end
            return { Pending = arns[1], Poll = { kind = "ecs", cluster = (started.Tasks[1] :: Doc).ClusterArn, tasks = arns } }
        end
        return started
    end
    if service == "lambda" then
        local name, payload = operation, parameters
        if name == "" then
            name = (parameters or {}).FunctionName
            payload = (parameters or {}).Payload
            if type(name) ~= "string" then fail("States.Runtime", "lambda:invoke needs a FunctionName parameter") end
            name = function_name(name)
        else
            name = function_name(name)
        end
        local response = roto.try_wire("lambda", "POST", "/2015-03-31/functions/" .. name .. "/invocations", {
            headers = { ["x-amz-invocation-type"] = "RequestResponse", ["content-type"] = "application/json" },
            body = roto.json_encode(if payload == nil then {} else payload),
        })
        if not response.ok then
            fail("Lambda." .. (if response.code == "ResourceNotFoundException" then "ResourceNotFoundException" else "ServiceException"), (response.message or ""))
        end
        local text = response.body :: string
        local decoded: any = if text == "" then NULL else roto.json_decode(text)
        if response.headers and response.headers["x-amz-function-error"] then
            local detail: any = if type(decoded) == "table" then decoded else {}
            fail(detail.errorType or "Lambda.Unknown", text)
        end
        if resource ~= "" and string.sub(resource, 1, 4) == "arn:" and string.find(resource, ":states:::", 1, true) then
            return { ExecutedVersion = "$LATEST", Payload = decoded, StatusCode = response.status }
        end
        return decoded
    end
    local response = roto.try_call(service, operation, parameters or {})
    if not response.ok then
        fail(label .. "." .. (response.code or "Error"), response.message or "")
    end
    return response.value
end

local function resource_type(resource: string): string
    return string.match(resource, "^arn:[^:]*:states:::([^:]+)") or "lambda"
end

local function waits_for_token(resource: string): boolean
    return is_activity(resource) or sync_mode(resource) ~= nil or string.find(resource, ".waitForTaskToken", 1, true) ~= nil
end

-- Applies ResultSelector, ResultPath and OutputPath to a task's raw result.
local function finish_task(state: Doc, data: any, scope: Scope, result: any): any
    if state.ResultSelector ~= nil then
        result = apply_template(state.ResultSelector, { data = result, context = scope.context })
    end
    return apply_output_path(state, apply_result_path(state, data, result), scope.context)
end

-- Returns the state's output, or (input, token) when the task is a callback (`.waitForTaskToken`)
-- that now waits for SendTaskSuccess/SendTaskFailure.
local function step_task(state: Doc, data: any, scope: Scope, history: History): (any, string?, any)
    local effective = apply_input_path(state, data, scope.context)
    local resource: string = state.Resource
    local token: string? = nil
    local context = scope.context
    if waits_for_token(resource) then
        token = roto.uuid() .. roto.uuid()
        context = table.clone(context)
        context.Task = { Token = token }
    end
    local inner: Scope = { data = effective, context = context }
    local parameters: any = effective
    if state.Parameters ~= nil then parameters = apply_template(state.Parameters, inner) end
    if is_activity(resource) then
        -- The worker fetches the input with GetActivityTask; nothing is called here.
        emit(history, "ActivityScheduled", "activityScheduledEventDetails", {
            resource = resource, input = roto.json_encode(parameters), inputDetails = { truncated = false },
            timeoutInSeconds = state.TimeoutSeconds, heartbeatInSeconds = state.HeartbeatSeconds,
        })
        return data, token, { activity = resource, input = roto.json_encode(parameters) }
    end
    emit(history, "TaskScheduled", "taskScheduledEventDetails", {
        resourceType = resource_type(resource), resource = resource,
        region = "", parameters = roto.json_encode(parameters),
    })
    emit(history, "TaskStarted", "taskStartedEventDetails", { resourceType = resource_type(resource), resource = resource })
    local ok, result = pcall(task_call, resource, parameters, token)
    if not ok then
        local code, cause = failure_of(result)
        emit(history, "TaskFailed", "taskFailedEventDetails", { resourceType = resource_type(resource), resource = resource, error = code, cause = cause })
        error(result, 0)
    end
    if sync_mode(resource) ~= nil and type(result) == "table" and result.Pending == nil then
        -- The child execution already finished inside the call: no need to wait for it.
        token = nil
        if result.Status ~= "SUCCEEDED" then
            local cause = roto.json_encode(result)
            emit(history, "TaskFailed", "taskFailedEventDetails", { resourceType = resource_type(resource), resource = resource, error = "States.TaskFailed", cause = cause })
            fail("States.TaskFailed", cause)
        end
    end
    if token ~= nil then
        return data, token, if type(result) == "table" and result.Pending then { child = if result.Poll then nil else result.Pending, poll = result.Poll } else nil
    end
    emit(history, "TaskSucceeded", "taskSucceededEventDetails", { resourceType = resource_type(resource), resource = resource, output = roto.json_encode(result), outputDetails = { truncated = false } })
    return finish_task(state, data, scope, result), nil, nil
end

local function step_choice(name: string, state: Doc, data: any, scope: Scope): (any, string)
    local effective = apply_input_path(state, data, scope.context)
    local inner: Scope = { data = effective, context = scope.context }
    local chosen: string? = nil
    for _, rule in ipairs(state.Choices) do
        if eval_rule(rule, inner) then
            chosen = rule.Next
            break
        end
    end
    if chosen == nil then chosen = state.Default end
    if chosen == nil then
        fail("States.NoChoiceMatched", "An error occurred while executing the state '" .. name .. "'. No Choices matched and no Default is configured.")
    end
    return apply_output_path(state, effective, scope.context), chosen :: string
end

local function step_fail(state: Doc, data: any, scope: Scope): (any, any)
    local inner: Scope = { data = data, context = scope.context }
    local err, cause = state.Error, state.Cause
    if state.ErrorPath ~= nil then err = resolve(state.ErrorPath, inner) end
    if state.CausePath ~= nil then cause = resolve(state.CausePath, inner) end
    return err, cause
end

local function context_object(exec: Doc, name: string, input: any): any
    local arn: string = exec.stateMachineArn
    return {
        Execution = {
            Id = exec.executionArn, Input = input, Name = exec.name, RoleArn = exec.roleArn,
            StartTime = format_timestamp(exec.startDate),
        },
        State = { Name = name, EnteredTime = format_timestamp(roto.now()), RetryCount = 0 },
        StateMachine = { Id = arn, Name = string.match(arn, "([^:]+)$") },
    }
end

-- An execution is a set of threads: the root, plus one per Parallel branch or Map item that is
-- running. A thread has its own position, retry counters, timers and task token. A thread that
-- reached a Parallel/Map state is blocked on `fan` until all of its children have finished.
type Thread = {
    id: string, parent: string?, slot: number?,
    -- Where the thread's definition is, relative to the machine definition (the root has none).
    path: { any },
    state: string?, data: any,
    map: { Index: number, Value: any }?,
    -- A Wait or retry backoff in progress (a timer wakes the thread); `resumed` is the Wait to
    -- close with an exit event when it does.
    sleeping: boolean?,
    resumed: { name: string, kind: string, output: any }?,
    -- Retry attempts per rule for the state the thread is on, and whether the state is being
    -- run again (so it is not entered a second time).
    retries: { [string]: number }?, retrying: boolean?,
    -- A callback or activity Task waiting for its token, and a result or failure delivered for it
    -- that the next step applies to the Task state instead of calling the service.
    waiting: { token: string, heartbeat: number?, child: string?, poll: any? }?,
    delivered: { output: any?, error: string?, cause: string? }?,
    -- Results are keyed by branch/item number (a sparse list would not survive JSON).
    -- `pending` counts children not yet finished, including ones still queued behind MaxConcurrency.
    fan: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string }?,
}

type Cursor = {
    definition: string, input: any, threads: { [string]: Thread }, next_tid: number,
    done: boolean?, output: any?,
}

local function schedule_resume(db: Db, arn: string, tid: string, delay: number)
    db.schedule(timer_key("resume", arn, tid), { delay = delay, payload = { arn = arn, kind = "resume", tid = tid } })
end

local function exit_state(history: History, kind: string, name: string, output: any)
    emit(history, kind .. "StateExited", "stateExitedEventDetails", { name = name, output = roto.json_encode(output), outputDetails = { truncated = false } })
end

-- Whether a Retry/Catch rule's ErrorEquals names this error. `States.ALL` matches everything
-- except `States.DataLimitExceeded`; `States.TaskFailed` matches any task error but a timeout.
local function error_matches(names: any, code: string): boolean
    if code == "States.Runtime" then return false end -- never catchable, as in AWS
    for _, name in ipairs(names or {}) do
        if name == code or name == "States.ALL" then return true end
        if name == "States.TaskFailed" and code ~= "States.Timeout" then return true end
    end
    return false
end

-- Backoff in seconds before the Task is run again, or nil when no Retry rule applies (or its
-- attempts are used up). Counts the attempt against the rule in `cursor.retries`.
local function retry_delay(state: Doc, cursor: { retries: { [string]: number }? }, code: string): number?
    local rules: { Doc } = state.Retry or {}
    for index, rule in ipairs(rules) do
        if error_matches(rule.ErrorEquals, code) then
            local retries = cursor.retries or {}
            cursor.retries = retries
            local key = tostring(index)
            local used = retries[key] or 0
            if used >= (rule.MaxAttempts or 3) then return nil end
            retries[key] = used + 1
            local interval: number = rule.IntervalSeconds or 1
            local rate: number = rule.BackoffRate or 2
            return interval * rate ^ used
        end
    end
    return nil
end

local function find_catcher(state: Doc, code: string): Doc?
    local rules: { Doc } = state.Catch or {}
    for _, rule in ipairs(rules) do
        if error_matches(rule.ErrorEquals, code) then return rule end
    end
    return nil
end

-- What a Parallel or Map state starts: one child thread per branch or item. `key` locates the
-- child's definition inside the state; `data` is its input.
type Spawn = { key: { any }, data: any, map: { Index: number, Value: any }? }

local function finish_fanout(state: Doc, data: any, scope: Scope, results: { any }): any
    local result: any = roto.array(results)
    if state.ResultSelector ~= nil then
        result = apply_template(state.ResultSelector, { data = result, context = scope.context })
    end
    return apply_output_path(state, apply_result_path(state, data, result), scope.context)
end

local function spawn_parallel(state: Doc, data: any, scope: Scope): { Spawn }
    local effective = apply_input_path(state, data, scope.context)
    local spawns: { Spawn } = {}
    for index = 1, #state.Branches do
        table.insert(spawns, { key = { "Branches", index }, data = effective })
    end
    return spawns
end

local function spawn_map(state: Doc, data: any, scope: Scope): { Spawn }
    local effective = apply_input_path(state, data, scope.context)
    local inner: Scope = { data = effective, context = scope.context }
    local items: any = effective
    if state.ItemsPath ~= nil then items = resolve(state.ItemsPath, inner) end
    if not is_array(items) then fail("States.Runtime", "The Map state's items must be an array") end
    local key = if state.ItemProcessor ~= nil then "ItemProcessor" else "Iterator"
    local selector = state.ItemSelector or state.Parameters
    local spawns: { Spawn } = {}
    for index, item in ipairs(items :: { any }) do
        local map = { Index = index - 1, Value = item }
        local context = table.clone(scope.context)
        context.Map = { Item = map }
        local input = item
        if selector ~= nil then input = apply_template(selector, { data = effective, context = context }) end
        table.insert(spawns, { key = { key }, data = input, map = map })
    end
    return spawns
end

-- One state's effect on its input, without touching storage.
local function evaluate_state(name: string, state: Doc, data: any, scope: Scope, history: History): Outcome
    local kind: string = state.Type
    if kind == "Parallel" then
        return { output = data, spawn = spawn_parallel(state, data, scope) }
    elseif kind == "Map" then
        return { output = data, spawn = spawn_map(state, data, scope) }
    elseif kind == "Task" then
        local output, token, queued = step_task(state, data, scope, history)
        return { output = output, next = state.Next, waiting = token, queued = queued }
    elseif kind == "Pass" then
        return { output = step_pass(state, data, scope), next = state.Next }
    elseif kind == "Wait" then
        local output, seconds = step_wait(state, data, scope)
        return { output = output, next = state.Next, wait = seconds }
    elseif kind == "Choice" then
        local output, chosen = step_choice(name, state, data, scope)
        return { output = output, next = chosen }
    elseif kind == "Succeed" then
        return { output = apply_output_path(state, apply_input_path(state, data, scope.context), scope.context) }
    end
    local err, cause = step_fail(state, data, scope)
    return { output = data, error = err or "", cause = cause }
end

local function thread_ids(cursor: Cursor): { string }
    local ids: { string } = {}
    for tid in pairs(cursor.threads) do table.insert(ids, tid) end
    table.sort(ids, function(x: string, y: string) return (tonumber(string.sub(x, 2)) or 0) < (tonumber(string.sub(y, 2)) or 0) end)
    return ids
end

local function runnable(thread: Thread): boolean
    if thread.sleeping or thread.waiting then return false end
    return thread.fan == nil or thread.fan.pending <= 0
end

-- Runs the execution's threads, one state transition at a time and round-robin, until the
-- execution settles or every thread is waiting (on a timer, a task token, or its children), or the
-- step allowance is used up (a follow-up job continues).
local function run_threads(ctx: Ctx, db: Db, exec: Doc, cursor: Cursor, history: History)
    local arn: string = exec.executionArn
    local root: Doc = roto.json_decode(cursor.definition)
    local failure: { code: string, cause: string }? = nil

    local function persist()
        db.put(ns(ctx, "cursor"), arn, cursor)
        save_history(ctx, db, arn, history)
    end

    local function definition_of(thread: Thread): Doc
        local def: any = root
        for _, key in ipairs(thread.path) do def = def[key] end
        return def
    end

    -- Removes everything a thread's children left behind: threads, timers and task tokens.
    local function drop_children(thread: Thread)
        local fan = thread.fan
        if not fan then return end
        for _, cid in ipairs(fan.children) do
            local child = cursor.threads[cid]
            if child then
                drop_children(child)
                db.cancel(timer_key("resume", arn, cid))
                db.cancel(timer_key("task", arn, cid))
                db.cancel(timer_key("heartbeat", arn, cid))
                db.cancel(timer_key("poll", arn, cid))
                if child.waiting then
                    db.delete(ns(ctx, "token"), child.waiting.token)
                    if child.waiting.child then abort_nested(ctx, db, child.waiting.child) end
                end
                cursor.threads[cid] = nil
            end
        end
        thread.fan = nil
    end

    -- A state failed. Retry rules first (a backoff on a timer), then Catch rules; an error nobody
    -- handles fails the enclosing Parallel/Map state, or the execution.
    local function fail_state(thread: Thread, name: string, state: Doc, code: string, cause: string)
        local kind: string = state.Type
        if kind == "Task" or kind == "Parallel" or kind == "Map" then
            local delay = retry_delay(state, thread, code)
            if delay ~= nil then
                thread.retrying = true
                thread.sleeping = true
                schedule_resume(db, arn, thread.id, delay)
                return
            end
            local catcher = find_catcher(state, code)
            if catcher ~= nil then
                local caught = apply_result_path(catcher, thread.data, { Error = code, Cause = cause })
                exit_state(history, kind, name, caught)
                thread.data = caught
                thread.state = catcher.Next
                thread.retries = nil
                return
            end
        end
        if thread.parent then
            local parent = cursor.threads[thread.parent :: string]
            drop_children(parent)
            local parent_name = parent.state :: string
            fail_state(parent, parent_name, definition_of(parent).States[parent_name], code, cause)
        else
            failure = { code = code, cause = cause }
        end
    end

    -- Starts up to `count` of the fan-out's queued branches or items as child threads.
    local function launch(thread: Thread, count: number)
        local fan = thread.fan :: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string }
        for _ = 1, count do
            local spawn: Spawn? = table.remove(fan.queue, 1)
            if spawn == nil then return end
            fan.started += 1
            local id = "t" .. cursor.next_tid
            cursor.next_tid += 1
            local path = table.clone(thread.path)
            table.insert(path, "States")
            table.insert(path, fan.name)
            for _, key in ipairs(spawn.key) do table.insert(path, key) end
            local child: Thread = { id = id, parent = thread.id, slot = fan.started, path = path, state = nil, data = spawn.data, map = spawn.map }
            child.state = definition_of(child).StartAt
            cursor.threads[id] = child
            table.insert(fan.children, id)
        end
    end

    local function complete(thread: Thread)
        if thread.parent == nil then
            cursor.done = true
            cursor.output = thread.data
            return
        end
        local parent = cursor.threads[thread.parent :: string]
        local fan = parent.fan :: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string }
        fan.results[tostring(thread.slot)] = thread.data
        fan.pending -= 1
        cursor.threads[thread.id] = nil
        launch(parent, 1) -- a MaxConcurrency slot is free
    end

    -- `limit` is MaxConcurrency: how many children may run at once (0 or nil means all).
    local function start_children(thread: Thread, name: string, spawns: { Spawn }, limit: number?)
        thread.fan = { pending = #spawns, total = #spawns, results = {}, children = {}, queue = spawns, started = 0, name = name }
        local first = #spawns
        if limit ~= nil and limit > 0 and limit < first then first = limit end
        launch(thread, first)
    end

    local function advance(thread: Thread)
        if thread.resumed then
            exit_state(history, thread.resumed.kind, thread.resumed.name, thread.resumed.output)
            thread.resumed = nil
        end
        local name = thread.state
        if name == nil then
            complete(thread)
            return
        end
        local state: Doc = definition_of(thread).States[name]
        local kind: string = state.Type
        if not thread.retrying and thread.fan == nil then
            emit(history, kind .. "StateEntered", "stateEnteredEventDetails", { name = name, input = roto.json_encode(thread.data), inputDetails = { truncated = false } })
        end
        thread.retrying = nil
        local context = context_object(exec, name, cursor.input)
        if thread.map then context.Map = { Item = thread.map } end
        local scope: Scope = { data = thread.data, context = context }
        local attempts = 0
        for _, count in pairs(thread.retries or {}) do attempts += count end
        context.State.RetryCount = attempts
        local ok, outcome
        local delivered = thread.delivered
        thread.delivered = nil
        local fan = thread.fan
        if fan ~= nil then
            -- Every child finished: gather their outputs in order.
            thread.fan = nil
            ok, outcome = pcall(function()
                local results = {}
                for slot = 1, fan.total do table.insert(results, fan.results[tostring(slot)]) end
                return { output = finish_fanout(state, thread.data, scope, results), next = state.Next }
            end)
        elseif delivered ~= nil and delivered.error ~= nil then
            if is_activity(state.Resource) then
                local timed_out = delivered.error == "States.Timeout"
                emit(history, if timed_out then "ActivityTimedOut" else "ActivityFailed", if timed_out then "activityTimedOutEventDetails" else "activityFailedEventDetails", { error = delivered.error, cause = delivered.cause or "" })
            else
                emit(history, "TaskFailed", "taskFailedEventDetails", { resourceType = resource_type(state.Resource), resource = state.Resource, error = delivered.error, cause = delivered.cause or "" })
            end
            ok, outcome = false, { asl_error = delivered.error, cause = delivered.cause or "" }
        elseif delivered ~= nil then
            if is_activity(state.Resource) then
                emit(history, "ActivitySucceeded", "activitySucceededEventDetails", { output = roto.json_encode(delivered.output), outputDetails = { truncated = false } })
            else
                emit(history, "TaskSucceeded", "taskSucceededEventDetails", { resourceType = resource_type(state.Resource), resource = state.Resource, output = roto.json_encode(delivered.output), outputDetails = { truncated = false } })
            end
            local result = finish_task(state, thread.data, scope, delivered.output)
            ok, outcome = true, { output = result, next = state.Next }
        else
            ok, outcome = pcall(evaluate_state, name, state, thread.data, scope, history)
        end
        if not ok then
            local code, cause = failure_of(outcome)
            fail_state(thread, name, state, code, cause)
            return
        end
        local result: Outcome = outcome :: Outcome
        if kind == "Fail" then
            fail_state(thread, name, state, result.error or "", result.cause or "")
            return
        end
        if result.spawn then
            start_children(thread, name, result.spawn, if kind == "Map" and type(state.MaxConcurrency) == "number" then state.MaxConcurrency else nil)
            return
        end
        if result.waiting then
            -- A callback or activity Task: park the thread until SendTaskSuccess/SendTaskFailure,
            -- the Task's TimeoutSeconds or a missed heartbeat delivers an outcome.
            local heartbeat: number? = if type(state.HeartbeatSeconds) == "number" then state.HeartbeatSeconds else nil
            thread.waiting = { token = result.waiting, heartbeat = heartbeat, child = if result.queued then result.queued.child else nil, poll = if result.queued then result.queued.poll else nil }
            db.put(ns(ctx, "token"), result.waiting, { arn = arn, tid = thread.id })
            if type(state.TimeoutSeconds) == "number" then
                db.schedule(timer_key("task", arn, thread.id), { delay = state.TimeoutSeconds, payload = { arn = arn, kind = "task_timeout", tid = thread.id } })
            end
            if heartbeat ~= nil then
                db.schedule(timer_key("heartbeat", arn, thread.id), { delay = heartbeat, payload = { arn = arn, kind = "task_timeout", tid = thread.id } })
            end
            if result.queued and result.queued.poll then
                db.schedule(timer_key("poll", arn, thread.id), { delay = POLL_SECONDS, payload = { arn = arn, kind = "poll", tid = thread.id } })
            end
            if result.queued and result.queued.activity then
                -- Ordered by the virtual clock, then by token for a stable tie-break.
                local id = string.format("%016d", math.floor(roto.now() * 1000)) .. result.waiting
                db.put(ns(ctx, "activity-queue") .. "/" .. result.queued.activity, id, { id = id, token = result.waiting, input = result.queued.input, arn = arn })
            end
            return
        end
        if kind == "Wait" and (result.wait :: number) > 0 then
            -- Wait on a timer; the exit event and the next state happen when it fires.
            thread.data = result.output
            thread.state = result.next
            thread.resumed = { name = name, kind = kind, output = result.output }
            thread.sleeping = true
            schedule_resume(db, arn, thread.id, result.wait :: number)
            return
        end
        exit_state(history, kind, name, result.output)
        thread.data = result.output
        thread.state = result.next
        thread.retries = nil
    end

    local steps = 0
    while failure == nil and not cursor.done do
        local progressed = false
        for _, tid in ipairs(thread_ids(cursor)) do
            local thread = cursor.threads[tid]
            if thread and runnable(thread) then
                if steps >= MAX_STEPS then
                    db.schedule("continue/" .. arn, { delay = 0, payload = { arn = arn, kind = "continue" } })
                    persist()
                    return
                end
                if history.count + 2 > MAX_EVENTS then
                    settle(ctx, db, exec, history, "FAILED", nil, "States.Runtime", "The execution reached the maximum number of history events (" .. MAX_EVENTS .. ")", cursor)
                    save_history(ctx, db, arn, history)
                    return
                end
                steps += 1
                advance(thread)
                progressed = true
                if failure ~= nil or cursor.done then break end
            end
        end
        if not progressed then break end
    end
    if failure ~= nil then
        settle(ctx, db, exec, history, "FAILED", nil, failure.code, failure.cause, cursor)
        save_history(ctx, db, arn, history)
    elseif cursor.done then
        settle(ctx, db, exec, history, "SUCCEEDED", roto.json_encode(cursor.output), nil, nil, cursor)
        save_history(ctx, db, arn, history)
    else
        persist()
    end
end

local function run(ctx: Ctx, db: Db, exec: Doc, cursor: Cursor, history: History)
    local previous = active
    active = { ctx = ctx, db = db }
    local ok, err = pcall(run_threads, ctx, db, exec, cursor, history)
    active = previous
    if not ok then error(err, 0) end
end

---------------------------------------------------------------------------------------------------
-- API

-- Tags are stored as a name-to-value map; the API uses lower-case `key`/`value` pairs.
local function tags_from(list: any): { [string]: string }
    local tags: { [string]: string } = {}
    if list == nil then return tags end
    for _, tag in ipairs(roto.list(list, "InvalidTags", "tags")) do
        if type(tag) ~= "table" or type(tag.key) ~= "string" or type(tag.value) ~= "string" then
            roto.error("InvalidTags", "Each tag needs a string key and value")
        end
        tags[tag.key] = tag.value
    end
    return tags
end

local function tag_list(tags: { [string]: string }?): { any }
    local out = roto.array()
    for _, key in ipairs(sorted_keys(tags or {})) do table.insert(out, { key = key, value = (tags :: any)[key] }) end
    return out
end

function M.CreateStateMachine(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "name", "InvalidName")
    if #name > 80 or string.find(name, "[%s<>{}%[%]%?%*\"#%%\\^|~`$&,;:/]") then
        roto.error("InvalidName", "Invalid Name: '" .. name .. "'")
    end
    roto.required(input, "roleArn", "InvalidArn")
    validate_definition(input.definition)
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
        revisionId = roto.uuid(),
        tags = tags_from(input.tags),
    })
    return { stateMachineArn = arn, creationDate = now }
end

function M.UpdateStateMachine(ctx: Ctx, input: any, db: Db)
    local doc = machine(ctx, db, input.stateMachineArn)
    if input.definition == nil and input.roleArn == nil and input.loggingConfiguration == nil and input.tracingConfiguration == nil then
        roto.error("MissingRequiredParameter", "Either the definition, the role ARN, the LoggingConfiguration, or the TracingConfiguration must be specified")
    end
    if input.definition ~= nil then doc.definition = validate_definition(input.definition) end
    doc.roleArn = input.roleArn or doc.roleArn
    doc.loggingConfiguration = input.loggingConfiguration or doc.loggingConfiguration
    doc.tracingConfiguration = input.tracingConfiguration or doc.tracingConfiguration
    doc.updateDate = os.time()
    doc.revisionId = roto.uuid()
    db.put(ns(ctx, "machine"), doc.stateMachineArn, doc)
    return { updateDate = doc.updateDate, revisionId = doc.revisionId }
end

local function tagged_machine(ctx: Ctx, db: Db, arn: any): Doc
    local key = roto.required({ arn = arn }, "arn", "InvalidArn")
    local doc = db.get(ns(ctx, "machine"), key) :: Doc?
    if not doc then roto.error("ResourceNotFound", "Resource not found: '" .. key .. "'", 404) end
    return doc :: Doc
end

function M.TagResource(ctx: Ctx, input: any, db: Db)
    local doc = tagged_machine(ctx, db, input.resourceArn)
    local tags: { [string]: string } = doc.tags or {}
    for key, value in pairs(tags_from(input.tags)) do tags[key] = value end
    doc.tags = tags
    db.put(ns(ctx, "machine"), doc.stateMachineArn, doc)
    return {}
end

function M.UntagResource(ctx: Ctx, input: any, db: Db)
    local doc = tagged_machine(ctx, db, input.resourceArn)
    local tags: { [string]: string } = doc.tags or {}
    for _, key in ipairs(roto.list(input.tagKeys, "InvalidTags", "tagKeys")) do tags[key] = nil end
    doc.tags = tags
    db.put(ns(ctx, "machine"), doc.stateMachineArn, doc)
    return {}
end

function M.ListTagsForResource(ctx: Ctx, input: any, db: Db)
    return { tags = tag_list(tagged_machine(ctx, db, input.resourceArn).tags) }
end

function M.DeleteStateMachine(ctx: Ctx, input: any, db: Db)
    -- AWS deletes asynchronously and succeeds for an unknown ARN; executions stay readable.
    db.delete(ns(ctx, "machine"), roto.required(input, "stateMachineArn", "InvalidArn"))
    return {}
end

function M.DescribeStateMachine(ctx: Ctx, input: any, db: Db)
    local doc = machine(ctx, db, input.stateMachineArn)
    local out: Doc = {}
    for key, value in pairs(doc) do out[key] = value end
    out.tags = nil -- not part of DescribeStateMachine; use ListTagsForResource
    return out
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

start_execution = function(ctx: Ctx, input: any, db: Db, parent: { token: string, v2: boolean }?): any
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
    local exec: Doc = {
        executionArn = arn,
        stateMachineArn = sm.stateMachineArn,
        name = name,
        status = "RUNNING",
        startDate = now,
        input = payload,
        inputDetails = { included = true },
        roleArn = sm.roleArn,
        traceHeader = input.traceHeader,
    }
    local history: History = { events = {}, count = 0 }
    emit(history, "ExecutionStarted", "executionStartedEventDetails", { input = payload, inputDetails = { truncated = false }, roleArn = sm.roleArn })
    local definition = roto.json_decode(sm.definition)
    local data = roto.json_decode(payload)
    local timeout = definition.TimeoutSeconds
    if type(timeout) == "number" then
        db.schedule("timeout/" .. arn, { delay = timeout, payload = { arn = arn, kind = "timeout" } })
    end
    db.put(ns(ctx, "execution"), arn, exec)
    if parent then db.put(ns(ctx, "parent"), arn, { token = parent.token, v2 = parent.v2 }) end
    local root: Thread = { id = "t0", path = {}, state = definition.StartAt, data = data }
    run(ctx, db, exec, { definition = sm.definition, input = data, threads = { t0 = root }, next_tid = 1 }, history)
    return { executionArn = arn, startDate = now }
end

function M.StartExecution(ctx: Ctx, input: any, db: Db)
    return start_execution(ctx, input, db, nil)
end

-- Timers: a Wait that finished, a looping machine that yielded, or TimeoutSeconds expiring.
function M.__job(ctx: Ctx, job: any, db: Db)
    local arn: string = job.payload.arn
    local exec = db.get(ns(ctx, "execution"), arn) :: Doc?
    if not exec or exec.status ~= "RUNNING" then return {} end
    local history = load_history(ctx, db, arn)
    if job.payload.kind == "timeout" then
        settle(ctx, db, exec, history, "TIMED_OUT", nil, "States.Timeout", nil)
        save_history(ctx, db, arn, history)
        return {}
    end
    local cursor = db.get(ns(ctx, "cursor"), arn) :: Cursor?
    if not cursor then return {} end
    local kind = job.payload.kind
    local thread = cursor.threads[job.payload.tid or ""]
    if kind == "resume" then
        if not thread then return {} end
        thread.sleeping = nil
    elseif kind == "poll" then
        if not thread or not thread.waiting or not thread.waiting.poll then return {} end
        local ok, result, failed = pcall(poll_ecs, thread.waiting.poll)
        if ok and result == nil then
            -- Still running: look again later.
            db.schedule(timer_key("poll", arn, thread.id), { delay = POLL_SECONDS, payload = { arn = arn, kind = "poll", tid = thread.id } })
            return {}
        end
        db.delete(ns(ctx, "token"), thread.waiting.token)
        db.cancel(timer_key("task", arn, thread.id))
        db.cancel(timer_key("heartbeat", arn, thread.id))
        thread.waiting = nil
        thread.retrying = true
        if not ok then
            local code, cause = failure_of(result)
            thread.delivered = { error = code, cause = cause }
        elseif failed then
            thread.delivered = { error = "States.TaskFailed", cause = roto.json_encode(result) }
        else
            thread.delivered = { output = result }
        end
    elseif kind == "task_timeout" then
        if not thread or not thread.waiting then return {} end
        db.delete(ns(ctx, "token"), thread.waiting.token)
        db.cancel(timer_key("task", arn, thread.id))
        db.cancel(timer_key("heartbeat", arn, thread.id))
        db.cancel(timer_key("poll", arn, thread.id))
        thread.waiting = nil
        thread.retrying = true
        thread.delivered = { error = "States.Timeout", cause = "The task timed out" }
    end
    run(ctx, db, exec, cursor, history)
    return {}
end

-- The execution, cursor, history and thread a task token belongs to, or the AWS error for a bad token.
local function waiting_task(ctx: Ctx, db: Db, token: any): (Doc, Cursor, History, Thread)
    if type(token) ~= "string" or token == "" or #token > 1024 then
        roto.error("InvalidToken", "Invalid Token: 'Invalid token'")
    end
    local entry = db.get(ns(ctx, "token"), token) :: Doc?
    if not entry then roto.error("TaskDoesNotExist", "Task Does Not Exist: '" .. token .. "'") end
    local arn: string = (entry :: Doc).arn
    local exec = db.get(ns(ctx, "execution"), arn) :: Doc?
    local cursor = db.get(ns(ctx, "cursor"), arn) :: Cursor?
    local thread = if cursor then cursor.threads[(entry :: Doc).tid or ""] else nil
    if not exec or exec.status ~= "RUNNING" or not cursor or not thread or not thread.waiting or thread.waiting.token ~= token then
        roto.error("TaskTimedOut", "Task Timed Out: '" .. token .. "'")
    end
    return exec :: Doc, cursor :: Cursor, load_history(ctx, db, arn), thread :: Thread
end

local function deliver(ctx: Ctx, db: Db, input: any, delivered: { output: any?, error: string?, cause: string? })
    local exec, cursor, history, thread = waiting_task(ctx, db, input.taskToken)
    local arn: string = exec.executionArn
    db.delete(ns(ctx, "token"), (thread.waiting :: { token: string, heartbeat: number?, child: string?, poll: any? }).token)
    db.cancel(timer_key("task", arn, thread.id))
    db.cancel(timer_key("heartbeat", arn, thread.id))
    db.cancel(timer_key("poll", arn, thread.id))
    thread.waiting = nil
    thread.retrying = true
    thread.delivered = delivered
    run(ctx, db, exec, cursor, history)
end

-- A nested execution started by `states:startExecution.sync` finished: hand its outcome to the
-- parent Task as if a worker had called SendTaskSuccess/SendTaskFailure.
notify_parent = function(ctx: Ctx, db: Db, exec: Doc)
    local link = db.get(ns(ctx, "parent"), exec.executionArn) :: Doc?
    if not link then return end
    db.delete(ns(ctx, "parent"), exec.executionArn)
    local token = (link :: Doc).token
    if not db.get(ns(ctx, "token"), token) then return end -- the parent has not parked yet
    local described = describe_doc(exec, (link :: Doc).v2 == true)
    if exec.status == "SUCCEEDED" then
        deliver(ctx, db, { taskToken = token }, { output = described })
    else
        deliver(ctx, db, { taskToken = token }, { error = "States.TaskFailed", cause = roto.json_encode(described) })
    end
end

function M.SendTaskSuccess(ctx: Ctx, input: any, db: Db)
    valid_json(input.output, "output")
    deliver(ctx, db, input, { output = roto.json_decode(input.output) })
    return {}
end

function M.SendTaskFailure(ctx: Ctx, input: any, db: Db)
    deliver(ctx, db, input, { error = input.error or "", cause = input.cause or "" })
    return {}
end

function M.SendTaskHeartbeat(ctx: Ctx, input: any, db: Db)
    local exec, _, _, thread = waiting_task(ctx, db, input.taskToken)
    local heartbeat = (thread.waiting :: { token: string, heartbeat: number?, child: string?, poll: any? }).heartbeat
    if heartbeat ~= nil then
        local arn: string = exec.executionArn
        db.schedule(timer_key("heartbeat", arn, thread.id), { delay = heartbeat, payload = { arn = arn, kind = "task_timeout", tid = thread.id } })
    end
    return {}
end

---------------------------------------------------------------------------------------------------
-- Activities

local function activity_arn(ctx: Ctx, name: string): string
    return "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":activity:" .. name
end

local function activity(ctx: Ctx, db: Db, arn: any): Doc
    if type(arn) ~= "string" or not is_activity(arn) then roto.error("InvalidArn", "Invalid Arn: '" .. tostring(arn) .. "'") end
    local doc = db.get(ns(ctx, "activity"), arn) :: Doc?
    if not doc then roto.error("ActivityDoesNotExist", "Activity Does Not Exist: '" .. arn .. "'") end
    return doc :: Doc
end

function M.CreateActivity(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "name", "InvalidName")
    if #name > 80 or string.find(name, '[<>{}%[%]?*"#%%\\^|~`$&,;:/%c ]') then
        roto.error("InvalidName", "Invalid Name: '" .. name .. "'")
    end
    local arn = activity_arn(ctx, name)
    local existing = db.get(ns(ctx, "activity"), arn) :: Doc?
    if existing then return { activityArn = arn, creationDate = (existing :: Doc).creationDate } end
    local doc: Doc = { activityArn = arn, name = name, creationDate = os.time() }
    if input.tags ~= nil then doc.tags = tags_from(input.tags) end
    db.put(ns(ctx, "activity"), arn, doc)
    return { activityArn = arn, creationDate = doc.creationDate }
end

function M.DescribeActivity(ctx: Ctx, input: any, db: Db)
    local doc = activity(ctx, db, input.activityArn)
    return { activityArn = doc.activityArn, name = doc.name, creationDate = doc.creationDate }
end

function M.DeleteActivity(ctx: Ctx, input: any, db: Db)
    local doc = activity(ctx, db, input.activityArn)
    db.delete(ns(ctx, "activity"), doc.activityArn)
    return {}
end

function M.ListActivities(ctx: Ctx, input: any, db: Db)
    local docs = db.list(ns(ctx, "activity")) :: { Doc }
    local out = {}
    for _, doc in ipairs(docs) do
        table.insert(out, { activityArn = doc.activityArn, name = doc.name, creationDate = doc.creationDate })
    end
    return roto.page(input, out, "activities", 100, 1000)
end

-- Hands the oldest queued task to a worker. AWS holds the request open for up to 60 seconds; with
-- nothing queued this answers at once with no task token, which is what a poller sees on timeout.
function M.GetActivityTask(ctx: Ctx, input: any, db: Db)
    local doc = activity(ctx, db, input.activityArn)
    local space = ns(ctx, "activity-queue") .. "/" .. doc.activityArn
    for _, task in ipairs(db.list(space) :: { Doc }) do
        db.delete(space, task.id)
        -- A task whose execution already moved on (timeout, stop) has no token any more: skip it.
        if db.get(ns(ctx, "token"), task.token) then
            local exec = db.get(ns(ctx, "execution"), task.arn) :: Doc?
            if exec then
                local history = load_history(ctx, db, task.arn)
                emit(history, "ActivityStarted", "activityStartedEventDetails", { workerName = input.workerName })
                save_history(ctx, db, task.arn, history)
            end
            return { taskToken = task.token, input = task.input }
        end
    end
    return {}
end

function M.StopExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    if doc.status == "RUNNING" then
        local history = load_history(ctx, db, doc.executionArn)
        settle(ctx, db, doc, history, "ABORTED", nil, input.error, input.cause)
        save_history(ctx, db, doc.executionArn, history)
    end
    return { stopDate = doc.stopDate or os.time() }
end

function M.DescribeExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    local out: Doc = {}
    for key, value in pairs(doc) do out[key] = value end
    out.roleArn = nil -- kept for the history only; DescribeExecution does not return it
    if doc.status ~= "RUNNING" then out.stopDate = doc.stopDate end
    return out
end

function M.DescribeStateMachineForExecution(ctx: Ctx, input: any, db: Db)
    local exec = execution(ctx, db, input.executionArn)
    local doc = db.get(ns(ctx, "machine"), exec.stateMachineArn) :: Doc?
    if not doc then roto.error("StateMachineDoesNotExist", "State Machine Does Not Exist: '" .. exec.stateMachineArn .. "'") end
    local sm = doc :: Doc
    return {
        stateMachineArn = sm.stateMachineArn, name = sm.name, definition = sm.definition, roleArn = sm.roleArn,
        updateDate = sm.updateDate or sm.creationDate, loggingConfiguration = sm.loggingConfiguration,
        tracingConfiguration = sm.tracingConfiguration, revisionId = sm.revisionId,
    }
end

-- Runs one state definition against an input, without creating an execution. Wait does not wait.
function M.TestState(ctx: Ctx, input: any, db: Db)
    valid_json(input.definition, "definition")
    local state = roto.json_decode(input.definition)
    if not is_object(state) or not STATE_TYPES[state.Type] then
        roto.error("ValidationException", "Invalid State Machine Definition: the state needs a known Type")
    end
    if state.Type == "Parallel" or state.Type == "Map" then
        roto.error("ValidationException", state.Type .. " states cannot be tested here yet")
    end
    local payload = input.input or "{}"
    valid_json(payload, "input")
    local data = roto.json_decode(payload)
    local fake: Doc = {
        executionArn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":execution:TestState:" .. roto.uuid(),
        stateMachineArn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":stateMachine:TestState",
        name = "TestState", roleArn = input.roleArn, startDate = os.time(),
    }
    local scope: Scope = { data = data, context = context_object(fake, "TestState", data) }
    local ok, outcome = pcall(evaluate_state, "TestState", state, data, scope, { events = {}, count = 0 })
    if not ok then
        local code, cause = failure_of(outcome)
        return { status = "FAILED", error = code, cause = cause }
    end
    local result: Outcome = outcome :: Outcome
    if state.Type == "Fail" then
        return { status = "FAILED", error = result.error, cause = result.cause }
    end
    return { status = "SUCCEEDED", output = roto.json_encode(result.output), nextState = result.next }
end

function M.GetExecutionHistory(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    local events: { any } = db.list(history_space(ctx, doc.executionArn))
    if input.reverseOrder == true then
        local reversed: { any } = {}
        for i = #events, 1, -1 do table.insert(reversed, events[i]) end
        events = reversed
    end
    local out = roto.page({ MaxResults = input.maxResults, NextToken = input.nextToken }, events, "events", 100, 1000, "InvalidToken")
    out.nextToken = out.NextToken
    out.NextToken = nil
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
    local history = load_history(ctx, db, doc.executionArn)
    settle(ctx, db, doc, history, status, input.output, input.error, input.cause)
    save_history(ctx, db, doc.executionArn, history)
    return {}
end

return M
