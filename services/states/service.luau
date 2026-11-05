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
-- Not interpreted yet: Task, Parallel and Map states (they need calls to other services). An
-- execution that reaches one stays RUNNING at that state until it is stopped or settled with
-- RotoCompleteExecution (a Roto extension that settles a running execution as SUCCEEDED, FAILED
-- or TIMED_OUT for fixtures). Retry and Catch only apply to those states, so they are not used.
--
-- Document namespaces (per account and region): states/machine keyed by ARN; states/execution and
-- states/cursor (where the execution is, its data and the definition it started with) keyed by
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
local INTERPRETED: { [string]: boolean } = { Pass = true, Choice = true, Wait = true, Succeed = true, Fail = true }

local function validate_definition(text: any): string
    valid_json(text, "definition")
    local definition = roto.json_decode(text :: string, "InvalidDefinition")
    local function invalid(message: string): never
        return roto.error("InvalidDefinition", "Invalid State Machine Definition: " .. message)
    end
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
        if kind == "Wait" and state.Seconds == nil and state.Timestamp == nil and state.SecondsPath == nil and state.TimestampPath == nil then
            invalid("Wait state '" .. name .. "' needs Seconds, Timestamp, SecondsPath or TimestampPath")
        end
    end
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

local function cancel_timers(db: Db, arn: string)
    db.cancel("resume/" .. arn)
    db.cancel("timeout/" .. arn)
end

local SETTLED_EVENTS: { [string]: { string } } = {
    SUCCEEDED = { "ExecutionSucceeded", "executionSucceededEventDetails" },
    FAILED = { "ExecutionFailed", "executionFailedEventDetails" },
    ABORTED = { "ExecutionAborted", "executionAbortedEventDetails" },
    TIMED_OUT = { "ExecutionTimedOut", "executionTimedOutEventDetails" },
}

local function settle(ctx: Ctx, db: Db, exec: Doc, history: History, status: string, output: any, err: any, cause: any)
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
    db.delete(ns(ctx, "cursor"), exec.executionArn)
    cancel_timers(db, exec.executionArn)
end

---------------------------------------------------------------------------------------------------
-- Interpreter

type Outcome = { output: any, next: string?, wait: number?, error: string?, cause: string? }

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

type Cursor = { definition: string, input: any, state: string?, data: any, resumed: { name: string, kind: string, output: any }? }

local function schedule_resume(db: Db, arn: string, delay: number)
    db.schedule("resume/" .. arn, { delay = delay, payload = { arn = arn, kind = "resume" } })
end

local function exit_state(history: History, kind: string, name: string, output: any)
    emit(history, kind .. "StateExited", "stateExitedEventDetails", { name = name, output = roto.json_encode(output), outputDetails = { truncated = false } })
end

-- One state's effect on its input, without touching storage.
local function evaluate_state(name: string, state: Doc, data: any, scope: Scope): Outcome
    local kind: string = state.Type
    if kind == "Pass" then
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

-- The error name and cause of something raised while evaluating a state.
local function failure_of(failure: any): (string, string)
    if type(failure) == "table" and failure.asl_error then return failure.asl_error, failure.cause end
    return "States.Runtime", tostring(failure)
end

-- Runs the execution from its cursor until it settles, waits, reaches a state that is not
-- interpreted, or has used its step allowance.
local function run(ctx: Ctx, db: Db, exec: Doc, cursor: Cursor, history: History)
    local arn: string = exec.executionArn
    local definition = roto.json_decode(cursor.definition)
    local function persist()
        db.put(ns(ctx, "cursor"), arn, cursor)
        save_history(ctx, db, arn, history)
    end
    if cursor.resumed then
        exit_state(history, cursor.resumed.kind, cursor.resumed.name, cursor.resumed.output)
        cursor.resumed = nil
    end
    local steps = 0
    while true do
        local name = cursor.state
        if name == nil then
            settle(ctx, db, exec, history, "SUCCEEDED", roto.json_encode(cursor.data), nil, nil)
            save_history(ctx, db, arn, history)
            return
        end
        local state: Doc = definition.States[name]
        local kind: string = state.Type
        if not INTERPRETED[kind] then
            persist() -- stays RUNNING here: see the header comment
            return
        end
        if steps >= MAX_STEPS then
            schedule_resume(db, arn, 0)
            persist()
            return
        end
        if history.count + 2 > MAX_EVENTS then
            settle(ctx, db, exec, history, "FAILED", nil, "States.Runtime", "The execution reached the maximum number of history events (" .. MAX_EVENTS .. ")")
            save_history(ctx, db, arn, history)
            return
        end
        steps += 1
        emit(history, kind .. "StateEntered", "stateEnteredEventDetails", { name = name, input = roto.json_encode(cursor.data), inputDetails = { truncated = false } })
        local scope: Scope = { data = cursor.data, context = context_object(exec, name, cursor.input) }
        local ok, outcome = pcall(evaluate_state, name, state, cursor.data, scope)
        if not ok then
            local code, cause = failure_of(outcome)
            settle(ctx, db, exec, history, "FAILED", nil, code, cause)
            save_history(ctx, db, arn, history)
            return
        end
        local result: Outcome = outcome :: Outcome
        if kind == "Fail" then
            settle(ctx, db, exec, history, "FAILED", nil, result.error, result.cause)
            save_history(ctx, db, arn, history)
            return
        end
        if kind == "Wait" and (result.wait :: number) > 0 then
            -- Wait on a timer; the exit event and the next state happen when it fires.
            cursor.data = result.output
            cursor.state = result.next
            cursor.resumed = { name = name, kind = kind, output = result.output }
            schedule_resume(db, arn, result.wait :: number)
            persist()
            return
        end
        exit_state(history, kind, name, result.output)
        cursor.data = result.output
        cursor.state = result.next
    end
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
    run(ctx, db, exec, { definition = sm.definition, input = data, state = definition.StartAt, data = data }, history)
    return { executionArn = arn, startDate = now }
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
    run(ctx, db, exec, cursor, history)
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
    if not INTERPRETED[state.Type] then
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
    local ok, outcome = pcall(evaluate_state, "TestState", state, data, scope)
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
