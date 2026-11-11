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
local MAX_PAYLOAD_BYTES = 262144

local function is_activity(resource: string): boolean
    return string.match(resource, "^arn:[^:]*:states:[^:]*:[^:]*:activity:.") ~= nil
end

local function timer_key(kind: string, arn: string, tid: string): string
    return kind .. "/" .. arn .. "/" .. tid
end

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "states/" .. kind)
end

local function validate_page_size(value: any)
    if value ~= nil and (type(value) ~= "number" or value % 1 ~= 0 or value < 0 or value > 1000) then
        roto.error("ValidationException", "maxResults must be an integer from 0 to 1000")
    end
end

local function states_page(ctx: Ctx, db: Db, input: any, rows: { any }, key: string): any
    validate_page_size(input.maxResults)
    local fingerprint = roto.json_encode(roto.array({
        key, if input.maxResults == nil then NULL else input.maxResults,
        if input.stateMachineArn == nil then NULL else input.stateMachineArn,
        if input.statusFilter == nil then NULL else input.statusFilter,
        if input.mapRunArn == nil then NULL else input.mapRunArn,
        if input.redriveFilter == nil then NULL else input.redriveFilter,
    }))
    local space = ns(ctx, "list-token")
    local now = roto.now()
    for _, token in ipairs(db.list(space) :: { Doc }) do
        if token.expiresAt <= now then db.delete(space, token.id) end
    end
    local offset = "0"
    if input.nextToken ~= nil then
        if type(input.nextToken) ~= "string" then roto.error("InvalidToken", "Invalid NextToken") end
        local token = db.get(space, input.nextToken) :: Doc?
        if not token or token.expiresAt <= now or token.request ~= fingerprint then
            roto.error("InvalidToken", "Invalid NextToken")
        end
        offset = token.offset
    end
    local max_results = if input.maxResults == 0 then nil else input.maxResults
    local out = roto.page({ MaxResults = max_results, NextToken = offset }, rows, key, 100, 1000, "InvalidToken")
    if out.NextToken ~= nil then
        local token = roto.uuid() .. roto.uuid()
        db.put(space, token, { id = token, offset = out.NextToken, request = fingerprint, expiresAt = now + 86400 })
        out.nextToken = token
    end
    out.NextToken = nil
    return out
end

-- Keep closed tokens so SendTask* can distinguish an expired token from a completely
-- unknown token, matching AWS's TaskTimedOut versus InvalidToken responses.
local function close_task_token(ctx: Ctx, db: Db, token: string)
    local entry = db.get(ns(ctx, "token"), token) :: Doc?
    if not entry then return end
    entry.closed = true
    db.put(ns(ctx, "token"), token, entry)
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

local function check_payload(value: any, label: string)
    local size = #roto.json_encode(value)
    if size > MAX_PAYLOAD_BYTES then
        fail("States.DataLimitExceeded", label .. " exceeded the 256 KiB payload limit")
    end
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
    local y, mo, d, h, mi, sec, frac, zone = string.match(text, "^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)(%.?%d*)(.*)$")
    if not y then return nil end
    local zone_text: string = zone or ""
    local fraction: string = frac or ""
    local year, month, day = tonumber(y) :: number, tonumber(mo) :: number, tonumber(d) :: number
    local hour, minute, second = tonumber(h) :: number, tonumber(mi) :: number, tonumber(sec) :: number
    local days = { 31, if year % 4 == 0 and (year % 100 ~= 0 or year % 400 == 0) then 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
    if month < 1 or month > 12 or day < 1 or day > days[month] or hour > 23 or minute > 59 or second > 60 then return nil end
    if fraction == "." then return nil end
    local offset = 0
    if zone_text == "Z" then
        offset = 0
    else
        local sign, oh, om = string.match(zone_text, "^([%+%-])(%d%d):(%d%d)$")
        if not sign then return nil end
        if (tonumber(oh) :: number) > 23 or (tonumber(om) :: number) > 59 then return nil end
        offset = ((tonumber(oh) :: number) * 3600 + (tonumber(om) :: number) * 60) * (if sign == "-" then -1 else 1)
    end
    local seconds = days_from_civil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second - offset
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
local INTRINSIC_ARITY: { [string]: { number } } = {
    ["States.Format"] = { 1 }, ["States.StringToJson"] = { 1, 1 }, ["States.JsonToString"] = { 1, 1 },
    ["States.Array"] = { 0 }, ["States.ArrayLength"] = { 1, 1 }, ["States.ArrayGetItem"] = { 2, 2 },
    ["States.ArrayContains"] = { 2, 2 }, ["States.ArrayUnique"] = { 1, 1 }, ["States.ArrayRange"] = { 3, 3 },
    ["States.ArrayPartition"] = { 2, 2 }, ["States.Base64Encode"] = { 1, 1 }, ["States.Base64Decode"] = { 1, 1 },
    ["States.Hash"] = { 2, 2 }, ["States.JsonMerge"] = { 3, 3 }, ["States.MathAdd"] = { 2, 2 },
    ["States.MathRandom"] = { 2, 3 }, ["States.StringSplit"] = { 2, 2 }, ["States.UUID"] = { 0, 0 },
}
local ESCAPED_OPEN_BRACE = "\1ROTO_INTRINSIC_OPEN\1"
local ESCAPED_CLOSE_BRACE = "\1ROTO_INTRINSIC_CLOSE\1"

local function unescape_intrinsic_braces(value: any): any
    if type(value) ~= "string" then return value end
    return string.gsub(string.gsub(value, ESCAPED_OPEN_BRACE, "{"), ESCAPED_CLOSE_BRACE, "}")
end

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
    if index < #args then fail("States.Runtime", "States.Format has more arguments than placeholders") end
    out = unescape_intrinsic_braces(out)
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
local function json_equal(left: any, right: any): boolean
    if left == NULL or right == NULL then return left == right end
    if type(left) ~= type(right) then return false end
    if type(left) ~= "table" then return left == right end
    local left_array, right_array = is_array(left), is_array(right)
    if left_array ~= right_array then return false end
    if left_array then
        if #left ~= #right then return false end
        for index = 1, #left do
            if not json_equal(left[index], right[index]) then return false end
        end
        return true
    end
    for key, value in pairs(left) do
        if right[key] == nil or not json_equal(value, right[key]) then return false end
    end
    for key in pairs(right) do
        if left[key] == nil then return false end
    end
    return true
end
local function canonical_json(value: any): string
    if value == NULL then return "null" end
    if is_array(value) then
        local encoded = {}
        for _, item in ipairs(value) do table.insert(encoded, canonical_json(item)) end
        return "[" .. table.concat(encoded, ",") .. "]"
    end
    if is_object(value) then
        local encoded = {}
        for _, key in ipairs(sorted_keys(value)) do
            table.insert(encoded, roto.json_encode(key) .. ":" .. canonical_json(value[key]))
        end
        return "{" .. table.concat(encoded, ",") .. "}"
    end
    return roto.json_encode(value)
end
INTRINSICS["States.ArrayContains"] = function(_, args)
    for _, item in ipairs(expect_array(args[1], "States.ArrayContains")) do
        if json_equal(item, args[2]) then return true end
    end
    return false
end
INTRINSICS["States.ArrayUnique"] = function(_, args)
    local seen: { [string]: boolean } = {}
    local out = roto.array()
    for _, item in ipairs(expect_array(args[1], "States.ArrayUnique")) do
        local key = canonical_json(item)
        if not seen[key] then
            seen[key] = true
            table.insert(out, item)
        end
    end
    return out
end
INTRINSICS["States.ArrayRange"] = function(_, args)
    local first, last, step = expect_number(args[1], "States.ArrayRange"), expect_number(args[2], "States.ArrayRange"), expect_number(args[3], "States.ArrayRange")
    first, last, step = math.floor(first + 0.5), math.floor(last + 0.5), math.floor(step + 0.5)
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
    size = math.floor(size + 0.5)
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
    local text = expect_string(args[1], "States.Hash")
    if #text > 10000 then fail("States.Runtime", "States.Hash input must be no longer than 10000 characters") end
    if algorithm ~= "MD5" and algorithm ~= "SHA-1" and algorithm ~= "SHA-256" and algorithm ~= "SHA-384" and algorithm ~= "SHA-512" then
        fail("States.Runtime", "States.Hash does not support algorithm " .. algorithm)
    end
    return roto.hash(text, algorithm)
end
INTRINSICS["States.JsonMerge"] = function(_, args)
    if args[3] ~= false then fail("States.Runtime", "States.JsonMerge supports only shallow merges (third argument false)") end
    if not is_object(args[1]) or not is_object(args[2]) then fail("States.Runtime", "States.JsonMerge expects two objects") end
    local out: any = copy(args[1])
    for k, v in pairs(args[2]) do out[k] = copy(v) end
    return out
end
INTRINSICS["States.MathAdd"] = function(_, args)
    local left, right = expect_number(args[1], "States.MathAdd"), expect_number(args[2], "States.MathAdd")
    left, right = math.floor(left + 0.5), math.floor(right + 0.5)
    if left < -2147483648 or left > 2147483647 or right < -2147483648 or right > 2147483647 then
        fail("States.Runtime", "States.MathAdd arguments must be in the signed 32-bit integer range")
    end
    local result = left + right
    if result < -2147483648 or result > 2147483647 then
        fail("States.Runtime", "States.MathAdd result is outside the signed 32-bit integer range")
    end
    return result
end
INTRINSICS["States.MathRandom"] = function(_, args)
    local low, high = expect_number(args[1], "States.MathRandom"), expect_number(args[2], "States.MathRandom")
    -- AWS rounds fractional bounds to the nearest integer; the upper bound is exclusive.
    low, high = math.floor(low + 0.5), math.floor(high + 0.5)
    if low >= high then fail("States.Runtime", "States.MathRandom start must be less than end") end
    if args[3] ~= nil then
        local seed = expect_number(args[3], "States.MathRandom")
        if seed % 1 ~= 0 then fail("States.Runtime", "States.MathRandom seed must be an integer") end
        -- A local digest makes seeded results repeatable without changing Luau's global RNG.
        local hex = string.sub(roto.sha256(tostring(seed) .. ":" .. tostring(low) .. ":" .. tostring(high)), 1, 8)
        local value = tonumber(hex, 16) :: number
        return low + (value % (high - low))
    end
    return math.random(low, high - 1)
end
INTRINSICS["States.StringSplit"] = function(_, args)
    local text, delimiters = expect_string(args[1], "States.StringSplit"), expect_string(args[2], "States.StringSplit")
    local out = roto.array()
    local function char_width(value: string, index: number): number
        local first = string.byte(value, index)
        if first < 0x80 then return 1 end
        if first < 0xE0 then return 2 end
        if first < 0xF0 then return 3 end
        return 4
    end
    local separator_set: { [string]: boolean } = {}
    local delimiter_index = 1
    while delimiter_index <= #delimiters do
        local width = char_width(delimiters, delimiter_index)
        separator_set[string.sub(delimiters, delimiter_index, delimiter_index + width - 1)] = true
        delimiter_index += width
    end
    local current = ""
    local text_index = 1
    while text_index <= #text do
        local width = char_width(text, text_index)
        local ch = string.sub(text, text_index, text_index + width - 1)
        if separator_set[ch] then
            table.insert(out, current)
            current = ""
        else
            current ..= ch
        end
        text_index += width
    end
    table.insert(out, current)
    return out
end
INTRINSICS["States.UUID"] = function(_, _) return roto.uuid() end

-- Evaluates `States.Name(arg, ...)`; arguments are 'strings', numbers, true/false/null, paths and
-- nested calls.
local function evaluate_intrinsic(source: string, scope: Scope): any
    local pos = 1
    local depth = 0
    local function skip()
        pos = string.find(source, "%S", pos) or (#source + 1)
    end
    local parse_value: () -> any
    local function parse_call(): any
        depth += 1
        if depth > 10 then fail("States.Runtime", "Intrinsic function nesting exceeds 10 levels") end
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
        local arity = INTRINSIC_ARITY[name]
        if arity ~= nil and (#args < arity[1] or (arity[2] ~= nil and #args > arity[2])) then
            fail("States.Runtime", name .. " received an invalid number of arguments")
        end
        if name == "States.Format" then
            for index = 2, #args do args[index] = unescape_intrinsic_braces(args[index]) end
        else
            for index, value in ipairs(args) do args[index] = unescape_intrinsic_braces(value) end
        end
        local result = implementation(scope, args)
        depth -= 1
        return result
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
                    local escaped = string.sub(source, pos + 1, pos + 1)
                    if escaped ~= "'" and escaped ~= "{" and escaped ~= "}" and escaped ~= "\\" then
                        fail("States.Runtime", "Invalid escape in intrinsic function: " .. source)
                    end
                    if escaped == "{" then
                        out ..= ESCAPED_OPEN_BRACE
                    elseif escaped == "}" then
                        out ..= ESCAPED_CLOSE_BRACE
                    else
                        out ..= escaped
                    end
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
    local function render(value: any): any
        if is_array(value) then
            local out = roto.array()
            for _, item in ipairs(value) do table.insert(out, render(item)) end
            return out
        elseif is_object(value) then
            local out: any = {}
            for key, item in pairs(value) do
                if string.sub(key, -2) == ".$" then
                    out[string.sub(key, 1, -3)] = evaluate_reference(item, scope)
                else
                    out[key] = render(item)
                end
            end
            return out
        end
        return value
    end
    local result = render(template)
    check_payload(result, "Transformed state payload")
    return result
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
    local tokens: { { value: string, wildcard: boolean } } = {}
    local i = 1
    while i <= #pattern do
        local ch = string.sub(pattern, i, i)
        if ch == "\\" then
            local next_char = string.sub(pattern, i + 1, i + 1)
            if next_char == "*" or next_char == "\\" then
                table.insert(tokens, { value = next_char, wildcard = false })
                i += 2
            else
                table.insert(tokens, { value = "\\", wildcard = false })
                i += 1
            end
        elseif ch == "*" then
            table.insert(tokens, { value = "", wildcard = true })
            i += 1
        else
            table.insert(tokens, { value = ch, wildcard = false })
            i += 1
        end
    end

    -- Greedy wildcard matching, comparing all non-wildcard bytes literally. This avoids leaking
    -- Lua pattern syntax into ASL, where `*` is the only special character.
    local token_index, text_index = 1, 1
    local last_wildcard: number? = nil
    local wildcard_text_index = 0
    while text_index <= #text do
        local token = tokens[token_index]
        if token and not token.wildcard and token.value == string.sub(text, text_index, text_index) then
            token_index += 1
            text_index += 1
        elseif token and token.wildcard then
            last_wildcard = token_index
            wildcard_text_index = text_index
            token_index += 1
        elseif last_wildcard ~= nil then
            token_index = last_wildcard + 1
            wildcard_text_index += 1
            text_index = wildcard_text_index
        else
            return false
        end
    end
    while tokens[token_index] and tokens[token_index].wildcard do token_index += 1 end
    return token_index > #tokens
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

local function validate_definition(text: any, diagnostics: boolean?, collected: { any }?, workflow_type: string?): string
    valid_json(text, "definition")
    local current_location: string? = nil
    local function invalid(message: string, location: string?, diagnostic_code: string?): never
        if diagnostics then
            error({ validationMessage = message, validationCode = diagnostic_code, location = location or current_location }, 0)
        end
        return roto.error("InvalidDefinition", "Invalid State Machine Definition: " .. message)
    end
    local function escape_pointer(part: string): string
        return (string.gsub(string.gsub(part, "~", "~0"), "/", "~1"))
    end
    local function check(definition: any, path: string, processor_config_allowed: boolean?)
        current_location = if path == "" then nil else path
        if not is_object(definition) or type(definition.StartAt) ~= "string" or not is_object(definition.States) then
            invalid("a definition needs StartAt and States")
        end
        local definition_fields = { StartAt = true, States = true, Version = true, Comment = true, TimeoutSeconds = true, QueryLanguage = true }
        if processor_config_allowed then definition_fields.ProcessorConfig = true end
        for field in pairs(definition) do
            if not definition_fields[field] then invalid("definition has unsupported field " .. tostring(field), path .. "/" .. tostring(field)) end
        end
        if definition.Comment ~= nil and type(definition.Comment) ~= "string" then invalid("Comment must be a string", path .. "/Comment") end
        if definition.Version ~= nil and definition.Version ~= "1.0" then invalid("Version must be 1.0", path .. "/Version") end
        if definition.QueryLanguage ~= nil and definition.QueryLanguage ~= "JSONPath" then
            invalid("QueryLanguage must be JSONPath; JSONata is not supported")
        end
        if definition.TimeoutSeconds ~= nil and (type(definition.TimeoutSeconds) ~= "number" or definition.TimeoutSeconds <= 0 or definition.TimeoutSeconds % 1 ~= 0) then
            invalid("TimeoutSeconds must be a positive integer")
        end
        local states: Doc = definition.States
        if next(states) == nil then invalid("States must not be empty") end
        if states[definition.StartAt] == nil then
            invalid("StartAt '" .. definition.StartAt .. "' is not a state", path .. "/StartAt", "MISSING_TRANSITION_TARGET")
        end
        local has_terminal = false
        for _, state in pairs(states) do
            if type(state) == "table" and (state.End == true or state.Type == "Succeed" or state.Type == "Fail") then
                has_terminal = true
                break
            end
        end
        if not has_terminal then
            local location = path .. "/States"
            if diagnostics then
                table.insert(collected :: { any }, {
                    code = "MISSING_END_STATE", severity = "ERROR",
                    message = "The state machine does not contain a terminal state.", location = location,
                })
            else
                invalid("The state machine does not contain a terminal state", location, "MISSING_END_STATE")
            end
        end
        local function target(name: any, from: string, location: string?)
            if type(name) ~= "string" or states[name] == nil then
                invalid("state '" .. from .. "' transitions to unknown state '" .. tostring(name) .. "'", location, "MISSING_TRANSITION_TARGET")
            end
        end
        local function validate_path(path_value: any, location: string, reference_only: boolean, allow_context: boolean)
            if type(path_value) ~= "string" then invalid("path must be a JSONPath string", location) end
            local path_text = path_value :: string
            if string.sub(path_text, 1, 2) == "$$" then
                if not allow_context then invalid("path must refer to state input", location) end
                path_text = string.sub(path_text, 2)
            end
            local ok, steps = pcall(parse_path, path_text)
            if not ok then invalid("path '" .. (path_value :: string) .. "' uses unsupported JSONPath syntax", location) end
            if reference_only then
                for _, step in ipairs(steps :: { Step }) do
                    if step.kind == "wild" then invalid("reference paths cannot contain wildcards", location) end
                end
            end
        end
        local function choice_path(path_value: any, location: string)
            validate_path(path_value, location, true, true)
        end
        local function validate_choice_rule(rule: any, location: string, nested: boolean)
            if not is_object(rule) then invalid("Choice rules must be objects", location) end
            if nested and rule.Next ~= nil then invalid("nested Choice rules cannot have Next", location .. "/Next") end
            local logical: { string } = {}
            for _, field in ipairs({ "And", "Or", "Not" }) do
                if rule[field] ~= nil then table.insert(logical, field) end
            end
            if #logical > 0 then
                if #logical ~= 1 or rule.Variable ~= nil then invalid("Choice rules must contain one logical operator or one variable comparison", location) end
                local operator = logical[1]
                for field in pairs(rule) do
                    if field ~= operator and not (field == "Next" and not nested) then
                        invalid("Choice logical rule has unsupported field " .. tostring(field), location .. "/" .. tostring(field))
                    end
                end
                local children = rule[operator]
                if operator == "Not" then
                    validate_choice_rule(children, location .. "/Not", true)
                else
                    if not is_array(children) or #children == 0 then invalid("Choice " .. operator .. " must contain at least one rule", location .. "/" .. operator) end
                    for index, child in ipairs(children) do
                        validate_choice_rule(child, location .. "/" .. operator .. "/" .. tostring(index - 1), true)
                    end
                end
                if not nested then target(rule.Next, name, location .. "/Next") end
                return
            end
            if type(rule.Variable) ~= "string" then invalid("a Choice comparison needs a Variable path", location .. "/Variable") end
            choice_path(rule.Variable, location .. "/Variable")
            local comparisons = 0
            local allowed_fields: { [string]: boolean } = { Variable = true, Next = true }
            for field in pairs(COMPARATORS) do allowed_fields[field] = true end
            for _, field in ipairs({ "IsPresent", "IsNull", "IsNumeric", "IsString", "IsBoolean", "IsTimestamp" }) do
                allowed_fields[field] = true
            end
            for field, operand in pairs(rule) do
                if not allowed_fields[field] or (nested and field == "Next") then
                    invalid("Choice comparison has unsupported field " .. tostring(field), location .. "/" .. tostring(field))
                end
                local comparator = COMPARATORS[field]
                if comparator then
                    comparisons += 1
                    if comparator.path then
                        choice_path(operand, location .. "/" .. field)
                    elseif comparator.kind == "String" or comparator.kind == "Timestamp" then
                        if type(operand) ~= "string" then invalid("Choice " .. field .. " must have a string value", location .. "/" .. field) end
                        if comparator.kind == "Timestamp" and parse_timestamp(operand) == nil then invalid("Choice " .. field .. " must have a valid timestamp", location .. "/" .. field) end
                    elseif comparator.kind == "Numeric" then
                        if type(operand) ~= "number" then invalid("Choice " .. field .. " must have a numeric value", location .. "/" .. field) end
                    elseif type(operand) ~= "boolean" then
                        invalid("Choice " .. field .. " must have a boolean value", location .. "/" .. field)
                    end
                elseif field == "IsPresent" or field == "IsNull" or field == "IsNumeric" or field == "IsString" or field == "IsBoolean" or field == "IsTimestamp" then
                    comparisons += 1
                    if type(operand) ~= "boolean" then invalid("Choice " .. field .. " must have a boolean value", location .. "/" .. field) end
                end
            end
            if comparisons ~= 1 then invalid("a Choice comparison needs exactly one comparison operator", location) end
            if not nested then target(rule.Next, name, location .. "/Next") end
        end
        for name, state in pairs(states) do
            local state_location = path .. "/States/" .. escape_pointer(name)
            current_location = state_location
            local function validate_state()
                if not is_object(state) or not STATE_TYPES[state.Type] then invalid("state '" .. name .. "' has an unknown Type") end
                local kind = state.Type
                local state_fields: { [string]: boolean } = { Type = true, Comment = true, QueryLanguage = true }
                local function allow_fields(fields: { string })
                    for _, field in ipairs(fields) do state_fields[field] = true end
                end
                if kind ~= "Fail" then allow_fields({ "InputPath", "OutputPath" }) end
                if kind == "Pass" then allow_fields({ "Next", "End", "Result", "Parameters", "ResultPath" })
                elseif kind == "Task" then allow_fields({ "Next", "End", "Resource", "Parameters", "ResultSelector", "ResultPath", "TimeoutSeconds", "TimeoutSecondsPath", "HeartbeatSeconds", "HeartbeatSecondsPath", "Retry", "Catch" })
                elseif kind == "Choice" then allow_fields({ "Choices", "Default" })
                elseif kind == "Wait" then allow_fields({ "Next", "End", "Seconds", "Timestamp", "SecondsPath", "TimestampPath" })
                elseif kind == "Succeed" then allow_fields({})
                elseif kind == "Fail" then allow_fields({ "Error", "ErrorPath", "Cause", "CausePath" })
                elseif kind == "Parallel" then allow_fields({ "Next", "End", "Branches", "Parameters", "ResultSelector", "ResultPath", "Retry", "Catch" })
                elseif kind == "Map" then allow_fields({ "Next", "End", "ItemProcessor", "Iterator", "ItemReader", "ResultWriter", "ItemBatcher", "ToleratedFailureCount", "ToleratedFailurePercentage", "Label", "Items", "ItemsPath", "MaxConcurrency", "MaxConcurrencyPath", "ItemSelector", "Parameters", "ResultSelector", "ResultPath", "Retry", "Catch" }) end
                for field in pairs(state) do
                    if not state_fields[field] then invalid("state '" .. name .. "' has unsupported field " .. tostring(field), state_location .. "/" .. tostring(field)) end
                end
                if state.Comment ~= nil and type(state.Comment) ~= "string" then invalid("state Comment must be a string", state_location .. "/Comment") end
                if kind ~= "Map" then
                    for _, field in ipairs({ "ItemProcessor", "Iterator", "ItemReader", "ResultWriter", "Items", "ItemsPath", "MaxConcurrency", "MaxConcurrencyPath", "ItemBatcher", "ToleratedFailureCount", "ToleratedFailurePercentage", "Label" }) do
                        if state[field] ~= nil then invalid("state '" .. name .. "' does not support Map field " .. field, state_location .. "/" .. field) end
                    end
                end
                if kind ~= "Parallel" and state.Branches ~= nil then
                    invalid("state '" .. name .. "' does not support Branches", state_location .. "/Branches")
                end
                if state.QueryLanguage ~= nil and state.QueryLanguage ~= "JSONPath" then
                    invalid("state '" .. name .. "' must use JSONPath; JSONata is not supported", state_location .. "/QueryLanguage")
                end
                for _, field in ipairs({ "Assign", "Arguments", "Output" }) do
                    if state[field] ~= nil then
                        invalid("state '" .. name .. "' uses unsupported JSONata field " .. field, state_location .. "/" .. field)
                    end
                end
                if kind == "Map" and state.Items ~= nil then
                    invalid("Map state '" .. name .. "' uses unsupported JSONata Items", state_location .. "/Items")
                end
                for _, field in ipairs({ "InputPath", "OutputPath" }) do
                    local value = state[field]
                    if value ~= nil and value ~= NULL then validate_path(value, state_location .. "/" .. field, false, false) end
                end
                if state.ResultPath ~= nil then
                    if state.Type ~= "Pass" and state.Type ~= "Task" and state.Type ~= "Parallel" and state.Type ~= "Map" then
                        invalid("state '" .. name .. "' does not support ResultPath", state_location .. "/ResultPath")
                    end
                    if state.ResultPath ~= NULL then validate_path(state.ResultPath, state_location .. "/ResultPath", true, false) end
                end
                if state.Parameters ~= nil then
                    if state.Type ~= "Pass" and state.Type ~= "Task" and state.Type ~= "Parallel" and state.Type ~= "Map" then
                        invalid("state '" .. name .. "' does not support Parameters", state_location .. "/Parameters")
                    end
                    if not is_object(state.Parameters) then invalid("Parameters must be an object", state_location .. "/Parameters") end
                end
                if state.ResultSelector ~= nil then
                    if state.Type ~= "Task" and state.Type ~= "Parallel" and state.Type ~= "Map" then
                        invalid("state '" .. name .. "' does not support ResultSelector", state_location .. "/ResultSelector")
                    end
                    if not is_object(state.ResultSelector) then invalid("ResultSelector must be an object", state_location .. "/ResultSelector") end
                end
                if state.ItemSelector ~= nil then
                    if state.Type ~= "Map" then invalid("ItemSelector is only supported in Map states", state_location .. "/ItemSelector") end
                    if not is_object(state.ItemSelector) then invalid("ItemSelector must be an object", state_location .. "/ItemSelector") end
                end
                if kind == "Choice" then
                    for index, rule in ipairs(state.Choices or {}) do
                        if is_object(rule) and rule.Condition ~= nil then
                            invalid("Choice state '" .. name .. "' uses unsupported JSONata Condition", state_location .. "/Choices/" .. tostring(index - 1) .. "/Condition")
                        end
                    end
                end
                if kind == "Choice" then
                    if not is_array(state.Choices) or #state.Choices == 0 then invalid("Choice state '" .. name .. "' needs Choices") end
                    for index, rule in ipairs(state.Choices) do
                        validate_choice_rule(rule, state_location .. "/Choices/" .. tostring(index - 1), false)
                    end
                    if state.Default ~= nil then target(state.Default, name, state_location .. "/Default") end
                elseif kind ~= "Succeed" and kind ~= "Fail" then
                    if state.Next ~= nil and state.End ~= nil then
                        invalid("state '" .. name .. "' cannot set both Next and End", state_location)
                    end
                    if state.End ~= nil and type(state.End) ~= "boolean" then
                        invalid("state End must be a boolean", state_location .. "/End")
                    end
                    if state.Next ~= nil then
                        target(state.Next, name, state_location .. "/Next")
                    elseif state.End ~= true then
                        invalid("state '" .. name .. "' needs Next or End")
                    end
                end
                if kind == "Task" or kind == "Parallel" or kind == "Map" then
                    if state.Catch ~= nil and not is_array(state.Catch) then invalid("Catch in state '" .. name .. "' must be an array") end
                    if state.Retry ~= nil and not is_array(state.Retry) then invalid("Retry in state '" .. name .. "' must be an array") end
                    local catches: { Doc } = state.Catch or {}
                    for index, rule in ipairs(catches) do
                        local rule_location = state_location .. "/Catch/" .. tostring(index - 1)
                        if not is_object(rule) then invalid("Catch entries in state '" .. name .. "' must be objects", rule_location) end
                        local catch_fields = { Next = true, ErrorEquals = true, ResultPath = true }
                        for field in pairs(rule) do
                            if not catch_fields[field] then invalid("Catch rule has unsupported field " .. tostring(field), rule_location .. "/" .. tostring(field)) end
                        end
                        target(rule.Next, name, rule_location .. "/Next")
                        if not is_array(rule.ErrorEquals) or #rule.ErrorEquals == 0 then
                            invalid("a Catch rule in '" .. name .. "' needs a non-empty ErrorEquals array", rule_location .. "/ErrorEquals")
                        end
                        for _, error_name in ipairs(rule.ErrorEquals) do
                            if type(error_name) ~= "string" or error_name == "" then invalid("Catch ErrorEquals values must be non-empty strings", rule_location .. "/ErrorEquals") end
                        end
                        for _, error_name in ipairs(rule.ErrorEquals) do
                            if error_name == "States.ALL" and (#rule.ErrorEquals ~= 1 or index ~= #catches) then
                                invalid("States.ALL must be the only Catch ErrorEquals entry and must be in the last Catch rule", rule_location .. "/ErrorEquals")
                            end
                        end
                        if rule.ResultPath ~= nil and rule.ResultPath ~= NULL then
                            validate_path(rule.ResultPath, rule_location .. "/ResultPath", true, false)
                        end
                    end
                    local retries: { Doc } = state.Retry or {}
                    for index, rule in ipairs(retries) do
                        local rule_location = state_location .. "/Retry/" .. tostring(index - 1)
                        if not is_object(rule) then invalid("Retry entries in state '" .. name .. "' must be objects", rule_location) end
                        local retry_fields = {
                            ErrorEquals = true, IntervalSeconds = true, MaxAttempts = true, BackoffRate = true,
                            MaxDelaySeconds = true, JitterStrategy = true,
                        }
                        for field in pairs(rule) do
                            if not retry_fields[field] then invalid("Retry rule has unsupported field " .. tostring(field), rule_location .. "/" .. tostring(field)) end
                        end
                        if not is_array(rule.ErrorEquals) or #rule.ErrorEquals == 0 then
                            invalid("a Retry rule in '" .. name .. "' needs a non-empty ErrorEquals array", rule_location .. "/ErrorEquals")
                        end
                        for _, error_name in ipairs(rule.ErrorEquals) do
                            if type(error_name) ~= "string" or error_name == "" then invalid("Retry ErrorEquals values must be non-empty strings", rule_location .. "/ErrorEquals") end
                        end
                        for _, error_name in ipairs(rule.ErrorEquals) do
                            if error_name == "States.ALL" and (#rule.ErrorEquals ~= 1 or index ~= #retries) then
                                invalid("States.ALL must be the only Retry ErrorEquals entry and must be in the last Retry rule", rule_location .. "/ErrorEquals")
                            end
                        end
                        for _, field in ipairs({ "IntervalSeconds", "MaxAttempts", "MaxDelaySeconds" }) do
                            local value = rule[field]
                            local minimum = if field == "MaxAttempts" then 0 else 1
                            local maximum = if field == "MaxDelaySeconds" then 31622400 else 99999999
                            if value ~= nil and (type(value) ~= "number" or value % 1 ~= 0 or value < minimum or value > maximum) then
                                invalid("Retry " .. field .. " in state '" .. name .. "' must be an integer from " .. tostring(minimum) .. " through " .. tostring(maximum), rule_location .. "/" .. field)
                            end
                        end
                        if rule.BackoffRate ~= nil and (type(rule.BackoffRate) ~= "number" or rule.BackoffRate < 1) then
                            invalid("Retry BackoffRate in state '" .. name .. "' must be a number greater than or equal to 1", rule_location .. "/BackoffRate")
                        end
                        if rule.JitterStrategy ~= nil and rule.JitterStrategy ~= "FULL" and rule.JitterStrategy ~= "NONE" then
                            invalid("Retry JitterStrategy in state '" .. name .. "' must be FULL or NONE", rule_location .. "/JitterStrategy")
                        end
                    end
                end
                if kind == "Task" and type(state.Resource) ~= "string" then
                    invalid("Task state '" .. name .. "' needs a Resource", state_location .. "/Resource", "INVALID_RESOURCE")
                end
                if kind == "Task" then
                    if workflow_type == "EXPRESS" and type(state.Resource) == "string" then
                        local resource: string = state.Resource
                        local integration = string.find(resource, ":states:::", 1, true) ~= nil
                        if is_activity(resource) or (integration and (string.match(resource, "%.sync:?2?$") or string.match(resource, "%.waitForTaskToken$"))) then
                            invalid("Express workflows do not support activity, .sync, or .waitForTaskToken Tasks", state_location .. "/Resource", "INVALID_RESOURCE")
                        end
                    end
                    for _, field in ipairs({ "TimeoutSeconds", "HeartbeatSeconds" }) do
                        local value = state[field]
                        if value ~= nil and (type(value) ~= "number" or value <= 0 or value % 1 ~= 0) then
                            invalid("Task state '" .. name .. "' has an invalid " .. field .. "; it must be a positive integer")
                        end
                    end
                    for _, field in ipairs({ "TimeoutSecondsPath", "HeartbeatSecondsPath" }) do
                        local value = state[field]
                        if value ~= nil and type(value) ~= "string" then
                            invalid("Task state '" .. name .. "' has an invalid " .. field .. "; it must be a JSONPath string")
                        end
                        if value ~= nil then validate_path(value, state_location .. "/" .. field, true, false) end
                    end
                    if state.TimeoutSeconds ~= nil and state.TimeoutSecondsPath ~= nil then
                        invalid("Task state '" .. name .. "' cannot set both TimeoutSeconds and TimeoutSecondsPath")
                    end
                    if state.HeartbeatSeconds ~= nil and state.HeartbeatSecondsPath ~= nil then
                        invalid("Task state '" .. name .. "' cannot set both HeartbeatSeconds and HeartbeatSecondsPath")
                    end
                    local heartbeat_set = state.HeartbeatSeconds ~= nil or state.HeartbeatSecondsPath ~= nil
                    local timeout_set = state.TimeoutSeconds ~= nil or state.TimeoutSecondsPath ~= nil
                    if heartbeat_set and not timeout_set then
                        invalid("Task state '" .. name .. "' must set HeartbeatSeconds lower than TimeoutSeconds")
                    end
                    if state.HeartbeatSeconds ~= nil and state.TimeoutSeconds ~= nil and state.HeartbeatSeconds >= state.TimeoutSeconds then
                        invalid("Task state '" .. name .. "' must set HeartbeatSeconds lower than TimeoutSeconds")
                    end
                end
                if kind == "Parallel" then
                    if not is_array(state.Branches) or #state.Branches == 0 then invalid("Parallel state '" .. name .. "' needs at least one Branch", state_location .. "/Branches") end
                    for index, branch in ipairs(state.Branches) do check(branch, state_location .. "/Branches/" .. tostring(index - 1)) end
                elseif kind == "Map" then
                    if state.ItemProcessor ~= nil and state.Iterator ~= nil then
                        invalid("Map state '" .. name .. "' cannot set both ItemProcessor and Iterator", state_location .. "/ItemProcessor")
                    end
                    if state.ItemSelector ~= nil and state.Parameters ~= nil then
                        invalid("Map state '" .. name .. "' cannot set both ItemSelector and Parameters", state_location .. "/ItemSelector")
                    end
                    for _, field in ipairs({ "ItemBatcher", "ToleratedFailureCount", "ToleratedFailurePercentage", "Label" }) do
                        if state[field] ~= nil then
                            invalid("Map state '" .. name .. "' uses unsupported distributed Map field " .. field, state_location .. "/" .. field)
                        end
                    end
                    local processor = state.ItemProcessor or state.Iterator
                    if processor == nil then invalid("Map state '" .. name .. "' needs an ItemProcessor") end
                    if not is_object(processor) then invalid("Map ItemProcessor must be an object", state_location .. "/ItemProcessor") end
                    local config: any = (processor :: Doc).ProcessorConfig
                    if state.ItemReader ~= nil or state.ResultWriter ~= nil then
                        invalid("Map state '" .. name .. "' uses unsupported distributed Map ItemReader or ResultWriter", state_location .. (if state.ItemReader ~= nil then "/ItemReader" else "/ResultWriter"))
                    end
                    if config ~= nil then
                        if not is_object(config) then invalid("ProcessorConfig must be an object", state_location .. "/ItemProcessor/ProcessorConfig") end
                        for field in pairs(config) do
                            if field ~= "Mode" then invalid("ProcessorConfig has unsupported field " .. tostring(field), state_location .. "/ItemProcessor/ProcessorConfig/" .. tostring(field)) end
                        end
                        if config.Mode ~= nil and config.Mode ~= "INLINE" and config.Mode ~= "DISTRIBUTED" then
                            invalid("ProcessorConfig Mode must be INLINE or DISTRIBUTED", state_location .. "/ItemProcessor/ProcessorConfig/Mode")
                        end
                        if config.Mode == "DISTRIBUTED" then
                            invalid("Distributed Map is not supported", state_location .. "/ItemProcessor/ProcessorConfig/Mode")
                        end
                    end
                    if state.MaxConcurrency ~= nil and (type(state.MaxConcurrency) ~= "number" or state.MaxConcurrency < 0 or state.MaxConcurrency % 1 ~= 0) then
                        invalid("Map state '" .. name .. "' needs a non-negative integer MaxConcurrency")
                    end
                    if state.MaxConcurrencyPath ~= nil and type(state.MaxConcurrencyPath) ~= "string" then
                        invalid("Map state '" .. name .. "' needs a JSONPath string for MaxConcurrencyPath")
                    end
                    if state.MaxConcurrencyPath ~= nil then validate_path(state.MaxConcurrencyPath, state_location .. "/MaxConcurrencyPath", true, false) end
                    if state.ItemsPath ~= nil then validate_path(state.ItemsPath, state_location .. "/ItemsPath", true, false) end
                    if state.MaxConcurrency ~= nil and state.MaxConcurrencyPath ~= nil then
                        invalid("Map state '" .. name .. "' cannot set both MaxConcurrency and MaxConcurrencyPath")
                    end
                    check(processor, state_location .. "/" .. (if state.ItemProcessor ~= nil then "ItemProcessor" else "Iterator"), state.ItemProcessor ~= nil)
                end
                if kind == "Wait" then
                    local wait_fields = { "Seconds", "Timestamp", "SecondsPath", "TimestampPath" }
                    local selected: { string } = {}
                    for _, field in ipairs(wait_fields) do
                        if state[field] ~= nil then table.insert(selected, field) end
                    end
                    if #selected ~= 1 then
                        invalid("Wait state '" .. name .. "' must specify exactly one of Seconds, Timestamp, SecondsPath or TimestampPath")
                    end
                    local field = selected[1]
                    if field == "Seconds" then
                        local value = state.Seconds
                        if type(value) ~= "number" or value % 1 ~= 0 or value < 0 or value > 99999999 then
                            invalid("Wait Seconds must be an integer from 0 through 99999999", state_location .. "/Seconds")
                        end
                    elseif field == "Timestamp" then
                        if type(state.Timestamp) ~= "string" or parse_timestamp(state.Timestamp) == nil then
                            invalid("Wait Timestamp must be a valid RFC 3339 timestamp", state_location .. "/Timestamp")
                        end
                    elseif field == "SecondsPath" or field == "TimestampPath" then
                        validate_path(state[field], state_location .. "/" .. field, true, false)
                    end
                end
                if kind == "Fail" then
                    if state.Next ~= nil or state.End ~= nil then
                        invalid("Fail state '" .. name .. "' cannot have Next or End", state_location)
                    end
                    for _, field in ipairs({ "Error", "Cause" }) do
                        if state[field] ~= nil and type(state[field]) ~= "string" then
                            invalid("Fail state " .. field .. " must be a string", state_location .. "/" .. field)
                        end
                    end
                    for _, pair in ipairs({ { "Error", "ErrorPath" }, { "Cause", "CausePath" } }) do
                        local value_field, path_field = pair[1], pair[2]
                        if state[value_field] ~= nil and state[path_field] ~= nil then
                            invalid("Fail state cannot set both " .. value_field .. " and " .. path_field, state_location .. "/" .. path_field)
                        end
                        if state[path_field] ~= nil then
                            local path_value = state[path_field]
                            if type(path_value) ~= "string" then
                                invalid("Fail state " .. path_field .. " must be a reference path or supported intrinsic", state_location .. "/" .. path_field)
                            elseif string.sub(path_value, 1, 7) == "States." then
                                local allowed_intrinsics = {
                                    ["States.Format"] = true, ["States.JsonToString"] = true, ["States.ArrayGetItem"] = true,
                                    ["States.Base64Encode"] = true, ["States.Base64Decode"] = true, ["States.Hash"] = true,
                                    ["States.UUID"] = true,
                                }
                                local intrinsic = string.match(path_value, "^(States%.[%w]+)%(")
                                if intrinsic == nil or not allowed_intrinsics[intrinsic] then
                                    invalid("Fail state " .. path_field .. " uses an unsupported intrinsic", state_location .. "/" .. path_field)
                                end
                            else
                                choice_path(path_value, state_location .. "/" .. path_field)
                            end
                        end
                    end
                    local allowed_fields = { Type = true, Comment = true, QueryLanguage = true, Error = true, ErrorPath = true, Cause = true, CausePath = true }
                    for field in pairs(state) do
                        if not allowed_fields[field] then invalid("Fail state has unsupported field " .. tostring(field), state_location .. "/" .. tostring(field)) end
                    end
                end
            end
            if diagnostics then
                local valid_state, issue = pcall(validate_state)
                if not valid_state then
                    local detail: any = issue
                    table.insert(collected :: { any }, {
                        code = if type(detail) == "table" and type(detail.validationCode) == "string" then detail.validationCode else "SCHEMA_VALIDATION_FAILED",
                        severity = "ERROR",
                        message = if type(detail) == "table" and type(detail.validationMessage) == "string" then detail.validationMessage else tostring(issue),
                        location = if type(detail) == "table" then detail.location else state_location,
                    })
                end
            else
                validate_state()
            end
            if diagnostics then
                local function warning(code: string, message: string, location: string)
                    table.insert(collected :: { any }, { code = code, severity = "WARNING", message = message, location = location })
                end
                local function looks_dynamic(value: any): boolean
                    return type(value) == "string" and (string.sub(value, 1, 1) == "$" or string.match(value, "^States%.[%w]+%(") ~= nil)
                end
                local function scan_template(value: any, location: string)
                    if type(value) ~= "table" then return end
                    for key, item in pairs(value) do
                        local name = tostring(key)
                        if string.sub(name, -2) ~= ".$" and looks_dynamic(item) then
                            warning("NO_DOLLAR", "The value of '" .. name .. "' looks like a JSONPath or intrinsic function. Use '" .. name .. ".$' to evaluate it at runtime.", location)
                        else
                            scan_template(item, location)
                        end
                    end
                end
                for _, field in ipairs({ "Parameters", "ResultSelector", "ItemSelector" }) do
                    if state[field] ~= nil then scan_template(state[field], state_location .. "/" .. field) end
                end
                for _, pair in ipairs({ { "Error", "ErrorPath" }, { "Cause", "CausePath" } }) do
                    local field, path_field = pair[1], pair[2]
                    if state[path_field] == nil and looks_dynamic(state[field]) then
                        warning("NO_PATH", "The value of '" .. field .. "' looks like a JSONPath. Use '" .. path_field .. "' to evaluate it at runtime.", state_location .. "/" .. field)
                    end
                end
                if state.Type == "Pass" and state.Result ~= nil then
                    local dynamic_field: string? = nil
                    local function find_result_path(value: any)
                        if dynamic_field ~= nil then return end
                        if type(value) == "table" then
                            for key, item in pairs(value) do
                                local name = tostring(key)
                                if string.sub(name, -2) == ".$" or looks_dynamic(item) then
                                    dynamic_field = name
                                    return
                                end
                                find_result_path(item)
                            end
                        elseif looks_dynamic(value) then
                            dynamic_field = "Result"
                        end
                    end
                    find_result_path(state.Result)
                    if dynamic_field ~= nil then
                        warning("PASS_RESULT_IS_STATIC", "The value of '" .. dynamic_field .. "' will not evaluate as a JSONPath or intrinsic function at runtime. Use Parameters instead of Result to evaluate it.", state_location .. "/Result")
                    end
                end
            end
        end
    end
    check(roto.json_decode(text :: string, "InvalidDefinition"), "")
    return text :: string
end

function M.ValidateStateMachineDefinition(_ctx: Ctx, input: any, _db: Db)
    local definition = roto.required(input, "definition", "ValidationException")
    if type(definition) ~= "string" or #definition < 1 or #definition > 1048576 then
        roto.error("ValidationException", "definition must be a non-empty string no longer than 1048576 characters")
    end
    if input.type ~= nil and input.type ~= "STANDARD" and input.type ~= "EXPRESS" then
        roto.error("ValidationException", "type must be STANDARD or EXPRESS")
    end
    local severity = input.severity or "ERROR"
    if severity ~= "ERROR" and severity ~= "WARNING" then
        roto.error("ValidationException", "severity must be ERROR or WARNING")
    end
    local max_results = input.maxResults or 100
    if type(max_results) ~= "number" or max_results < 0 or max_results > 100 or max_results % 1 ~= 0 then
        roto.error("ValidationException", "maxResults must be an integer from 0 to 100")
    end
    if max_results == 0 then max_results = 100 end

    local diagnostics: { any } = {}
    local parsed, parse_error = pcall(roto.json_decode, definition)
    if not parsed then
        table.insert(diagnostics, {
            code = "INVALID_JSON_DESCRIPTION", severity = "ERROR",
            message = tostring(parse_error),
        })
    else
        local valid, validation_error = pcall(validate_definition, definition, true, diagnostics, input.type)
        if not valid then
            local detail: any = validation_error
            local message = if type(detail) == "table" and type(detail.validationMessage) == "string" then detail.validationMessage else tostring(validation_error)
            local code = if type(detail) == "table" and type(detail.validationCode) == "string" then detail.validationCode else "SCHEMA_VALIDATION_FAILED"
            local location = if type(detail) == "table" then detail.location else nil
            table.insert(diagnostics, {
                code = code, severity = "ERROR",
                message = message, location = location,
            })
        end
    end

    local result = "OK"
    local included: { any } = {}
    for _, diagnostic in ipairs(diagnostics) do
        if diagnostic.severity == "ERROR" then result = "FAIL" end
        if severity == "WARNING" or diagnostic.severity == "ERROR" then table.insert(included, diagnostic) end
    end
    local truncated = #included > max_results
    while #included > max_results do table.remove(included) end
    return { result = result, diagnostics = roto.array(included), truncated = truncated }
end

---------------------------------------------------------------------------------------------------
-- History and settling

-- `events` holds the events added since the last save; `count` is every event so far. Each event
-- is its own document, so a run writes only what it added.
type History = { events: { any }, count: number, current: any? }

local function emit(history: History, event_type: string, details_key: string?, details: any)
    history.count += 1
    local id = history.count
    local current = history.current
    local previous = if current and current.lastEventId ~= nil then current.lastEventId else id - 1
    local event: Doc = { id = id, timestamp = roto.now(), type = event_type, previousEventId = previous }
    if current then current.lastEventId = id end
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
local stop_ecs: (any) -> () = function() end
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
                close_task_token(ctx, db, thread.waiting.token)
                if thread.waiting.child then table.insert(children, thread.waiting.child) end
                if thread.waiting.poll then stop_ecs(thread.waiting.poll) end
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
    exec.stopDate = roto.now()
    if status == "SUCCEEDED" then
        exec.output = output
        exec.outputDetails = { included = true }
    else
        exec.error = err
        exec.cause = cause
    end
    local event = SETTLED_EVENTS[status]
    local current_cursor = cursor or (db.get(ns(ctx, "cursor"), exec.executionArn) :: Cursor?)
    history.current = if current_cursor then current_cursor.threads.t0 else nil
    if status == "SUCCEEDED" then
        emit(history, event[1], event[2], { output = output, outputDetails = { truncated = false } })
    else
        emit(history, event[1], event[2], { error = err, cause = cause })
    end
    history.current = nil
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

type Outcome = { output: any, next: string?, wait: number?, error: string?, cause: string?, waiting: string?, queued: any, spawn: { Spawn }?, maxConcurrency: number? }

local function step_pass(state: Doc, data: any, scope: Scope, inspection: Doc?): any
    local effective = apply_input_path(state, data, scope.context)
    if inspection then inspection.afterInputPath = roto.json_encode(effective) end
    local result: any
    if state.Result ~= nil then
        result = copy(state.Result)
    elseif state.Parameters ~= nil then
        result = apply_template(state.Parameters, { data = effective, context = scope.context })
    else
        result = effective
    end
    if inspection then
        inspection.afterParameters = roto.json_encode(if state.Parameters ~= nil then result else effective)
        inspection.result = roto.json_encode(result)
        inspection.afterResultSelector = roto.json_encode(result)
    end
    local merged = apply_result_path(state, data, result)
    if inspection then inspection.afterResultPath = roto.json_encode(merged) end
    return apply_output_path(state, merged, scope.context)
end

local function step_wait(state: Doc, data: any, scope: Scope): (any, number)
    local effective = apply_input_path(state, data, scope.context)
    local inner: Scope = { data = effective, context = scope.context }
    local seconds: number
    if state.Seconds ~= nil then
        if type(state.Seconds) ~= "number" or state.Seconds % 1 ~= 0 or state.Seconds < 0 or state.Seconds > 99999999 then
            fail("States.Runtime", "Seconds must be an integer from 0 through 99999999")
        end
        seconds = state.Seconds
    elseif state.SecondsPath ~= nil then
        local value = resolve(state.SecondsPath, inner)
        if type(value) ~= "number" or value % 1 ~= 0 or value < 0 or value > 99999999 then
            fail("States.Runtime", "SecondsPath must resolve to an integer from 0 through 99999999")
        end
        seconds = value
    else
        local stamp = state.Timestamp
        if state.TimestampPath ~= nil then stamp = resolve(state.TimestampPath, inner) end
        local at = parse_timestamp(stamp)
        if at == nil then fail("States.Runtime", "Wait timestamp must be an RFC 3339 timestamp") end
        -- Wait state timestamps are truncated to whole seconds, even if the RFC3339 input
        -- includes fractional seconds.
        seconds = math.floor(at :: number) - roto.now()
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
    if not string.find(resource, ":states:::", 1, true) then return nil end
    if string.match(resource, "%.sync:2$") then return "v2" end
    if string.match(resource, "%.sync$") then return "v1" end
    return nil
end

-- Set by `run` for the duration of a run: Task states reach the execution's own service (nested
-- executions) through it, because a service cannot call itself through `roto.call`.
local active: { ctx: Ctx, db: Db }? = nil
local start_execution: (Ctx, any, Db, { token: string, v2: boolean, express: boolean? }?, boolean?) -> any

local function task_target(resource: any): (string, string)
    if type(resource) ~= "string" then fail("States.Runtime", "A Task state needs a Resource") end
    local is_integration = string.find(resource, ":states:::", 1, true) ~= nil
    local text: string = if is_integration then string.gsub(resource, "%.waitForTaskToken$", "") else resource
    local synchronous = is_integration and string.match(text, "%.sync:?2?$") ~= nil
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
    return if service == "sfn" then "states" else service :: string, capitalize(action :: string)
end

local function function_name(arn: string): string
    return string.match(arn, "function:(.+)") or arn
end

-- Calls the integration; returns the result or raises the Step Functions error for the failure.
local function describe_doc(exec: Doc, v2: boolean): Doc
    local out: Doc = {
        ExecutionArn = exec.executionArn, StateMachineArn = exec.stateMachineArn, Name = exec.name,
        Status = exec.status, StartDate = exec.startDate, StopDate = exec.stopDate, Input = exec.input,
        Error = exec.error, Cause = exec.cause,
    }
    if exec.stateMachineVersionArn ~= nil then out.StateMachineVersionArn = exec.stateMachineVersionArn end
    if exec.stateMachineAliasArn ~= nil then out.StateMachineAliasArn = exec.stateMachineAliasArn end
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
local function ecs_call(operation: string, body: any, timeout: number?): any
    local options = {
        headers = { ["x-amz-target"] = "AmazonEC2ContainerServiceV20141113." .. operation, ["content-type"] = "application/x-amz-json-1.1" },
        body = roto.json_encode(body),
    }
    local response = if timeout ~= nil then roto.try_wire_timed("ecs", "POST", "/", options, timeout)
        else roto.try_wire("ecs", "POST", "/", options)
    if not response.ok then
        if response.code == "States.Timeout" then fail("States.Timeout", response.message or "The task timed out") end
        fail("Ecs." .. (response.code or "Error"), response.message or "")
    end
    return roto.json_decode(response.body :: string)
end

-- Cancellation is best effort: a failed StopTask must not mask the original error.
stop_ecs = function(poll: any)
    if poll.kind ~= "ecs" or not is_array(poll.tasks) then return end
    for _, task in ipairs(poll.tasks) do
        pcall(ecs_call, "StopTask", { cluster = poll.cluster, task = task, reason = "Step Functions task cancelled" }, 1)
    end
end

-- How often a `.sync` integration that has to be polled looks at its target, in virtual seconds.
local POLL_SECONDS = 5
local INLINE_MAP_MAX_CONCURRENCY = 40

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

local function task_call(resource: string, parameters: any, token: string?, timeout: number?): any
    local service, operation = task_target(resource)
    local label = SERVICE_NAMES[service] or capitalize(service)
    if service == "states" and operation == "StartExecution" then
        if active == nil then fail("States.Runtime", "states:startExecution is not available here") end
        local params: any = parameters or {}
        local input: any = params.Input
        local started = (start_execution :: (Ctx, any, Db, { token: string, v2: boolean, express: boolean? }?, boolean?) -> any)(
            (active :: { ctx: Ctx, db: Db }).ctx,
            { stateMachineArn = params.StateMachineArn, name = params.Name, input = if type(input) == "string" then input else roto.json_encode(if input == nil then {} else input) },
            (active :: { ctx: Ctx, db: Db }).db,
            if token ~= nil and sync_mode(resource) ~= nil then { token = token, v2 = sync_mode(resource) == "v2" } else nil,
            true
        )
        local exec = (active :: { ctx: Ctx, db: Db }).db.get(ns((active :: { ctx: Ctx, db: Db }).ctx, "execution"), started.executionArn) :: Doc
        if token ~= nil and sync_mode(resource) ~= nil then
            if exec.status == "RUNNING" then return { Pending = started.executionArn } end
            return describe_doc(exec, sync_mode(resource) == "v2")
        end
        return { ExecutionArn = started.executionArn, StartDate = started.startDate }
    end
    if service == "states" and operation == "StartSyncExecution" then
        if active == nil or token == nil then fail("States.Runtime", "StartSyncExecution requires a running workflow") end
        local params: any = parameters or {}
        if type(params.StateMachineArn) ~= "string" then fail("Sfn.InvalidArn", "StateMachineArn is required") end
        if params.Input ~= nil and type(params.Input) ~= "string" then fail("Sfn.InvalidExecutionInput", "Input must be a JSON string") end
        if params.IncludedData ~= nil and params.IncludedData ~= "ALL_DATA" and params.IncludedData ~= "METADATA_ONLY" then
            fail("Sfn.ValidationException", "Invalid IncludedData")
        end
        local current = active :: { ctx: Ctx, db: Db }
        local alias = current.db.get(ns(current.ctx, "alias"), params.StateMachineArn) :: Doc?
        local version = current.db.get(ns(current.ctx, "version"), params.StateMachineArn) :: Doc?
        local arn = if alias then alias.stateMachineArn elseif version then version.stateMachineArn else params.StateMachineArn
        local target = current.db.get(ns(current.ctx, "machine"), arn) :: Doc?
        if not target then fail("Sfn.StateMachineDoesNotExist", "State machine does not exist") end
        if target.type ~= "EXPRESS" then fail("Sfn.StateMachineTypeNotSupported", "StartSyncExecution requires an EXPRESS state machine") end
        local started = start_execution(current.ctx, { stateMachineArn = params.StateMachineArn, name = params.Name,
            input = params.Input, traceHeader = params.TraceHeader }, current.db, { token = token, v2 = false, express = true }, true)
        return { Pending = started.executionArn }
    end
    if service == "ecs" then
        local started = recase(ecs_call(operation, recase(parameters or {}, false), timeout), true)
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
        local options = {
            headers = { ["x-amz-invocation-type"] = "RequestResponse", ["content-type"] = "application/json" },
            body = roto.json_encode(if payload == nil then {} else payload),
        }
        local path = "/2015-03-31/functions/" .. name .. "/invocations"
        local response = if timeout ~= nil and token == nil then roto.try_wire_timed("lambda", "POST", path, options, timeout)
            else roto.try_wire("lambda", "POST", path, options)
        if not response.ok then
            if response.code == "States.Timeout" then fail("States.Timeout", response.message or "The task timed out") end
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
    local response = if timeout ~= nil and token == nil then roto.try_call_timed(service, operation, parameters or {}, timeout)
        else roto.try_call(service, operation, parameters or {})
    if not response.ok then
        if response.code == "States.Timeout" then fail("States.Timeout", response.message or "The task timed out") end
        fail(label .. "." .. (response.code or "Error"), response.message or "")
    end
    return response.value
end

local function resource_type(resource: string): string
    return string.match(resource, "^arn:[^:]*:states:::([^:]+)") or "lambda"
end

local function waits_for_token(resource: string): boolean
    return is_activity(resource) or sync_mode(resource) ~= nil
        or string.match(resource, "^arn:[^:]*:states:::aws%-sdk:sfn:startSyncExecution$") ~= nil
        or (string.find(resource, ":states:::", 1, true) ~= nil and string.match(resource, "%.waitForTaskToken$") ~= nil)
end

local function lambda_arn(execution_arn: string, function_name_value: string): string
    if string.sub(function_name_value, 1, 4) == "arn:" then return function_name_value end
    local partition, region, account = string.match(execution_arn, "^arn:([^:]+):states:([^:]+):([^:]+):execution:")
    if partition == nil then partition, region, account = string.match(execution_arn, "^arn:([^:]+):states:([^:]+):([^:]+):express:") end
    local partial_account, partial_name = string.match(function_name_value, "^(%d+):function:(.+)$")
    if partial_account then account = partial_account; function_name_value = partial_name :: string end
    return "arn:" .. (partition or "aws") .. ":lambda:" .. (region or "") .. ":" .. (account or "") .. ":function:" .. function_name_value
end

local function emit_task_event(execution_arn: string, history: History, resource: string, phase: string, details: Doc)
    if resource_type(resource) == "lambda" and not waits_for_token(resource) then
        local event_names = {
            Scheduled = "LambdaFunctionScheduled", Started = "LambdaFunctionStarted",
            Succeeded = "LambdaFunctionSucceeded", Failed = "LambdaFunctionFailed", TimedOut = "LambdaFunctionTimedOut",
        }
        local detail_keys = {
            Scheduled = "lambdaFunctionScheduledEventDetails", Started = "lambdaFunctionStartedEventDetails",
            Succeeded = "lambdaFunctionSucceededEventDetails", Failed = "lambdaFunctionFailedEventDetails", TimedOut = "lambdaFunctionTimedOutEventDetails",
        }
        local lambda_details: Doc = {}
        local parameters: any = details.parametersValue or {}
        local lambda_resource = if string.find(resource, ":states:::lambda:invoke", 1, true)
            and type(parameters.FunctionName) == "string" then lambda_arn(execution_arn, parameters.FunctionName) else resource
        if phase == "Scheduled" then
            local lambda_input = details.parameters
            if string.find(resource, ":states:::lambda:invoke", 1, true) then
                lambda_input = roto.json_encode(if parameters.Payload == nil then {} else parameters.Payload)
            end
            lambda_details = { resource = lambda_resource, input = lambda_input, inputDetails = { truncated = false } }
            if details.timeoutInSeconds ~= nil then lambda_details.timeoutInSeconds = details.timeoutInSeconds end
        elseif phase == "Started" then
            lambda_details = { resource = lambda_resource }
        elseif phase == "Succeeded" then
            lambda_details = { output = details.lambdaOutput, outputDetails = { truncated = false } }
        else
            lambda_details = { error = details.error, cause = details.cause or "" }
        end
        emit(history, event_names[phase] :: string, detail_keys[phase] :: string, lambda_details)
        return
    end
    details.parametersValue = nil
    details.lambdaOutput = nil
    local event_name = "Task" .. phase
    local detail_key = string.lower(string.sub(event_name, 1, 1)) .. string.sub(event_name, 2) .. "EventDetails"
    emit(history, event_name, detail_key, details)
end

-- Applies ResultSelector, ResultPath and OutputPath to a task's raw result.
local function finish_task(state: Doc, data: any, scope: Scope, result: any, inspection: Doc?): any
    if inspection then inspection.result = roto.json_encode(result) end
    if state.ResultSelector ~= nil then
        result = apply_template(state.ResultSelector, { data = result, context = scope.context })
    end
    if inspection then inspection.afterResultSelector = roto.json_encode(result) end
    local merged = apply_result_path(state, data, result)
    if inspection then inspection.afterResultPath = roto.json_encode(merged) end
    return apply_output_path(state, merged, scope.context)
end

-- Returns the state's output, or (input, token) when the task is a callback (`.waitForTaskToken`)
-- that now waits for SendTaskSuccess/SendTaskFailure.
local function step_task(state: Doc, data: any, scope: Scope, history: History, execution_arn: string, inspection: Doc?): (any, string?, any)
    local effective = apply_input_path(state, data, scope.context)
    if inspection then inspection.afterInputPath = roto.json_encode(effective) end
    local resource: string = state.Resource
    local token: string? = nil
    local context = scope.context
    if waits_for_token(resource) then
        token = roto.uuid() .. roto.uuid()
        context = table.clone(context)
        context.Task = { Token = token }
    end
    local inner: Scope = { data = effective, context = context }
    local function resolve_duration(field: string, path_field: string): number?
        local value = state[field]
        if state[path_field] ~= nil then value = resolve(state[path_field], inner) end
        if value == nil then return nil end
        if type(value) ~= "number" or value <= 0 or value % 1 ~= 0 then
            fail("States.Runtime", path_field .. " or " .. field .. " must resolve to a positive integer")
        end
        return value
    end
    local timeout = resolve_duration("TimeoutSeconds", "TimeoutSecondsPath")
    local heartbeat = resolve_duration("HeartbeatSeconds", "HeartbeatSecondsPath")
    if heartbeat ~= nil and timeout ~= nil and heartbeat >= timeout then
        fail("States.Runtime", "HeartbeatSeconds must be lower than TimeoutSeconds")
    end
    local parameters: any = effective
    if state.Parameters ~= nil then parameters = apply_template(state.Parameters, inner) end
    if inspection then inspection.afterParameters = roto.json_encode(parameters) end
    if is_activity(resource) then
        -- The worker fetches the input with GetActivityTask; nothing is called here.
        emit(history, "ActivityScheduled", "activityScheduledEventDetails", {
            resource = resource, input = roto.json_encode(parameters), inputDetails = { truncated = false },
            timeoutInSeconds = timeout, heartbeatInSeconds = heartbeat,
        })
        return data, token, { activity = resource, input = roto.json_encode(parameters), timeout = timeout, heartbeat = heartbeat }
    end
    emit_task_event(execution_arn, history, resource, "Scheduled", {
        resourceType = resource_type(resource), resource = resource,
        region = "", parameters = roto.json_encode(parameters),
        parametersValue = parameters, timeoutInSeconds = timeout,
    })
    emit_task_event(execution_arn, history, resource, "Started", {
        resourceType = resource_type(resource), resource = resource, parametersValue = parameters,
    })
    local task_started = roto.now()
    local ok, result = pcall(task_call, resource, parameters, token, timeout)
    if not ok then
        local code, cause = failure_of(result)
        emit_task_event(execution_arn, history, resource, if code == "States.Timeout" then "TimedOut" else "Failed", { resourceType = resource_type(resource), resource = resource, error = code, cause = cause })
        error(result, 0)
    end
    if #roto.json_encode(result) > MAX_PAYLOAD_BYTES then
        local cause = "Task connector output exceeded the 256 KiB payload limit"
        emit_task_event(execution_arn, history, resource, "Failed", { resourceType = resource_type(resource), resource = resource, error = "States.DataLimitExceeded", cause = cause })
        fail("States.DataLimitExceeded", cause)
    end
    if sync_mode(resource) ~= nil and type(result) == "table" and result.Pending == nil then
        -- The child execution already finished inside the call: no need to wait for it.
        token = nil
        if result.Status ~= "SUCCEEDED" then
            local cause = roto.json_encode(result)
            emit_task_event(execution_arn, history, resource, "Failed", { resourceType = resource_type(resource), resource = resource, error = "States.TaskFailed", cause = cause })
            fail("States.TaskFailed", cause)
        end
    end
    if token ~= nil then
        local queued: any = if type(result) == "table" and result.Pending then { child = if result.Poll then nil else result.Pending, poll = result.Poll } else {}
        queued.timeout = if timeout ~= nil and sync_mode(resource) ~= nil then math.max(0, timeout - (roto.now() - task_started)) else timeout
        queued.heartbeat = heartbeat
        return data, token, queued
    end
    emit_task_event(execution_arn, history, resource, "Succeeded", {
        resourceType = resource_type(resource), resource = resource,
        output = roto.json_encode(result), lambdaOutput = if string.find(resource, ":states:::lambda:invoke", 1, true) and type(result) == "table" then roto.json_encode(result.Payload) else roto.json_encode(result),
        outputDetails = { truncated = false },
    })
    return finish_task(state, data, scope, result, inspection), nil, nil
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

local function step_fail(state: Doc, data: any, scope: Scope): (string, string)
    local inner: Scope = { data = data, context = scope.context }
    local err: any = state.Error or "States.Failed"
    local cause: any = state.Cause or ""
    if state.ErrorPath ~= nil then err = evaluate_reference(state.ErrorPath, inner) end
    if state.CausePath ~= nil then cause = evaluate_reference(state.CausePath, inner) end
    if type(err) ~= "string" then fail("States.Runtime", "Fail ErrorPath must resolve to a string") end
    if type(cause) ~= "string" then fail("States.Runtime", "Fail CausePath must resolve to a string") end
    return err, cause
end

local function context_object(exec: Doc, name: string, input: any, entered_time: string?): any
    local arn: string = exec.stateMachineArn
    return {
        Execution = {
            Id = exec.executionArn, Input = input, Name = exec.name, RoleArn = exec.roleArn,
            StartTime = format_timestamp(exec.startDate), RedriveCount = exec.redriveCount or 0,
        },
        State = { Name = name, EnteredTime = entered_time or format_timestamp(roto.now()), RetryCount = 0 },
        StateMachine = { Id = arn, Name = string.match(arn, "([^:]+)$") },
    }
end

-- An execution is a set of threads: the root, plus one per Parallel branch or Map item that is
-- running. A thread has its own position, retry counters, timers and task token. A thread that
-- reached a Parallel/Map state is blocked on `fan` until all of its children have finished.
type Thread = {
    id: string, parent: string?, slot: number?,
    lastEventId: number?,
    -- Where the thread's definition is, relative to the machine definition (the root has none).
    path: { any },
    state: string?, data: any,
    enteredTime: string?,
    map: { Index: number, Value: any, Source: string }?,
    -- A Wait or retry backoff in progress (a timer wakes the thread); `resumed` is the Wait to
    -- close with an exit event when it does.
    sleeping: boolean?,
    resumed: { name: string, kind: string, output: any }?,
    -- Retry attempts per rule for the state the thread is on, and whether the state is being
    -- run again (so it is not entered a second time).
    retries: { [string]: number }?, retrying: boolean?,
    -- A callback or activity Task waiting for its token, and a result or failure delivered for it
    -- that the next step applies to the Task state instead of calling the service.
    waiting: { token: string, timeout: number?, heartbeat: number?, child: string?, poll: any?, activity: string? }?,
    delivered: { output: any?, error: string?, cause: string? }?,
    -- Results are keyed by branch/item number (a sparse list would not survive JSON).
    -- `pending` counts children not yet finished, including ones still queued behind MaxConcurrency.
    fan: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string, kind: string }?,
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
        if name == code then return true end
        if name == "States.ALL" and code ~= "States.DataLimitExceeded" then return true end
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
            local delay = interval * rate ^ used
            if rule.MaxDelaySeconds ~= nil then delay = math.min(delay, rule.MaxDelaySeconds) end
            if rule.JitterStrategy == "FULL" then delay *= math.random() end
            return delay
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
type MapItem = { Index: number, Value: any, Source: string, Key: string? }
type Spawn = { key: { any }, data: any, map: MapItem? }

local function finish_fanout(state: Doc, data: any, scope: Scope, results: any, inspection: Doc?): any
    -- Child execution results are collected in ordinary Luau tables, including an
    -- empty table for zero iterations. Only an exported Map mock is an object.
    local exported = state.Type == "Map" and is_object(state.ResultWriter)
        and state.ResultWriter.Resource ~= nil and not is_array(results)
    local result: any = if exported then results else roto.array(results)
    if inspection then inspection.result = roto.json_encode(result) end
    if state.ResultSelector ~= nil then
        result = apply_template(state.ResultSelector, { data = result, context = scope.context })
    end
    if inspection then inspection.afterResultSelector = roto.json_encode(result) end
    local merged = apply_result_path(state, data, result)
    if inspection then inspection.afterResultPath = roto.json_encode(merged) end
    return apply_output_path(state, merged, scope.context)
end

local function spawn_parallel(state: Doc, data: any, scope: Scope, inspection: Doc?): { Spawn }
    local effective = apply_input_path(state, data, scope.context)
    if inspection then inspection.afterInputPath = roto.json_encode(effective) end
    local spawns: { Spawn } = {}
    for index = 1, #state.Branches do
        table.insert(spawns, { key = { "Branches", index }, data = effective })
    end
    return spawns
end

local function spawn_map(state: Doc, data: any, scope: Scope, inspection: Doc?, reader: any?): ({ Spawn }, number?, number)
    local effective = apply_input_path(state, data, scope.context)
    if inspection then inspection.afterInputPath = roto.json_encode(effective) end
    local inner: Scope = { data = effective, context = scope.context }
    local max_concurrency = state.MaxConcurrency
    if state.MaxConcurrencyPath ~= nil then max_concurrency = resolve(state.MaxConcurrencyPath, inner) end
    if max_concurrency ~= nil and (type(max_concurrency) ~= "number" or max_concurrency < 0 or max_concurrency % 1 ~= 0) then
        fail("States.Runtime", "MaxConcurrencyPath must resolve to a non-negative integer")
    end
    local items: any = if reader ~= nil then reader.items else effective
    if reader == nil and state.ItemsPath ~= nil then items = resolve(state.ItemsPath :: string, inner) end
    local distributed = is_object(state.ItemProcessor) and is_object(state.ItemProcessor.ProcessorConfig)
        and state.ItemProcessor.ProcessorConfig.Mode == "DISTRIBUTED"
    if not is_array(items) and not (distributed and is_object(items)) then
        fail("States.Runtime", "The Map state's items must be an array or a Distributed Map object")
    end
    local entries: { any } = {}
    if is_array(items) then
        for _, value in ipairs(items) do table.insert(entries, { value = value }) end
    else
        local keys = {}
        for item_key in pairs(items) do table.insert(keys, item_key) end
        table.sort(keys)
        for _, item_key in ipairs(keys) do table.insert(entries, { key = item_key, value = items[item_key] }) end
    end
    if reader ~= nil and (reader.maxItems ~= nil or reader.maxItemsPath ~= nil) then
        local max_items = if reader.maxItemsPath then resolve(reader.maxItemsPath, inner) else reader.maxItems
        if type(max_items) ~= "number" or max_items < 1 or max_items % 1 ~= 0 then
            fail("States.Runtime", "MaxItems must resolve to a positive integer")
        end
        while #entries > max_items do table.remove(entries) end
        if is_array(items) then
            local limited = {}
            for _, entry in ipairs(entries) do table.insert(limited, entry.value) end
            items = roto.array(limited)
        else
            local limited = {}
            for _, entry in ipairs(entries) do limited[entry.key] = entry.value end
            items = limited
        end
    end
    if inspection then
        if reader ~= nil and reader.pointer ~= nil then inspection.afterItemsPointer = roto.json_encode(items)
        else inspection.afterItemsPath = roto.json_encode(items) end
        inspection.maxConcurrency = max_concurrency
    end
    local key = if state.ItemProcessor ~= nil then "ItemProcessor" else "Iterator"
    local selector = state.ItemSelector or state.Parameters
    local spawns: { Spawn } = {}
    for index, entry in ipairs(entries) do
        local item = entry.value
        local map: MapItem = { Index = index - 1, Value = item, Key = entry.key, Source = if reader ~= nil then reader.source else "STATE_DATA" }
        local context = table.clone(scope.context)
        context.Map = { Item = map }
        local input = if entry.key ~= nil then { [entry.key] = item } else item
        if selector ~= nil then input = apply_template(selector, { data = effective, context = context }) end
        table.insert(spawns, { key = { key }, data = input, map = map })
    end
    if inspection then
        local inputs = {}
        for _, spawn in ipairs(spawns) do table.insert(inputs, spawn.data) end
        inspection.afterItemSelector = roto.json_encode(roto.array(inputs))
    end
    if state.ItemBatcher ~= nil then
        local batcher = state.ItemBatcher
        if not is_object(batcher) then fail("States.Runtime", "ItemBatcher must be an object") end
        if (batcher.MaxItemsPerBatch ~= nil and batcher.MaxItemsPerBatchPath ~= nil)
            or (batcher.MaxInputBytesPerBatch ~= nil and batcher.MaxInputBytesPerBatchPath ~= nil) then
            fail("States.Runtime", "ItemBatcher limits cannot specify both a value and a path")
        end
        local max_items = if batcher.MaxItemsPerBatchPath ~= nil then resolve(batcher.MaxItemsPerBatchPath, inner) else batcher.MaxItemsPerBatch
        local max_bytes = if batcher.MaxInputBytesPerBatchPath ~= nil then resolve(batcher.MaxInputBytesPerBatchPath, inner) else batcher.MaxInputBytesPerBatch
        if max_items == nil and max_bytes == nil then fail("States.Runtime", "ItemBatcher needs a batch limit") end
        if max_items ~= nil and (type(max_items) ~= "number" or max_items < 1 or max_items % 1 ~= 0) then
            fail("States.Runtime", "MaxItemsPerBatch must resolve to a positive integer")
        end
        if max_bytes ~= nil and (type(max_bytes) ~= "number" or max_bytes < 1 or max_bytes > MAX_PAYLOAD_BYTES or max_bytes % 1 ~= 0) then
            fail("States.Runtime", "MaxInputBytesPerBatch must resolve to an integer from 1 to 262144")
        end
        max_bytes = max_bytes or MAX_PAYLOAD_BYTES
        local batch_input = if batcher.BatchInput ~= nil then apply_template(batcher.BatchInput, inner) else nil
        local batches: { Spawn } = {}
        local batch_items: { any } = {}
        local function batch_value(): any
            return { Items = roto.array(table.clone(batch_items)), BatchInput = batch_input }
        end
        local function flush()
            if #batch_items == 0 then return end
            table.insert(batches, { key = { key }, data = batch_value() })
            batch_items = {}
        end
        for _, spawn in ipairs(spawns) do
            if max_items ~= nil and #batch_items >= max_items then flush() end
            table.insert(batch_items, spawn.data)
            if #roto.json_encode(batch_value()) > max_bytes then
                table.remove(batch_items)
                flush()
                table.insert(batch_items, spawn.data)
                if #roto.json_encode(batch_value()) > max_bytes then
                    fail("States.DataLimitExceeded", "An item exceeds MaxInputBytesPerBatch")
                end
            end
        end
        flush()
        spawns = batches
        if inspection then
            local values = {}
            for _, batch in ipairs(batches) do table.insert(values, batch.data) end
            inspection.afterItemBatcher = roto.json_encode(roto.array(values))
        end
    end
    return spawns, max_concurrency, #entries
end

-- One state's effect on its input, without touching storage.
local function evaluate_state(name: string, state: Doc, data: any, scope: Scope, history: History, execution_arn: string, inspection: Doc?): Outcome
    local kind: string = state.Type
    if kind == "Parallel" then
        return { output = data, spawn = spawn_parallel(state, data, scope, inspection) }
    elseif kind == "Map" then
        local spawns, max_concurrency = spawn_map(state, data, scope, inspection)
        return { output = data, spawn = spawns, maxConcurrency = max_concurrency }
    elseif kind == "Task" then
        local output, token, queued = step_task(state, data, scope, history, execution_arn, inspection)
        return { output = output, next = state.Next, waiting = token, queued = queued }
    elseif kind == "Pass" then
        return { output = step_pass(state, data, scope, inspection), next = state.Next }
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

-- TestState executes Parallel/Map children without creating persisted executions or threads.
-- Waits advance immediately, matching the single-state TestState behavior.
local function test_branch(definition: Doc, data: any, exec: Doc, history: History, map_item: MapItem?): any
    local name: string? = definition.StartAt
    local value = data
    for _ = 1, MAX_STEPS do
        if name == nil then return value end
        local state: Doc = definition.States[name]
        if state.Type == "Task" and type(state.Resource) == "string" and waits_for_token(state.Resource) then
            fail("States.Runtime", "TestState cannot wait for callback, activity, or synchronous Tasks")
        end
        local context = context_object(exec, name, data)
        if map_item ~= nil then context.Map = { Item = map_item } end
        local scope: Scope = { data = value, context = context }
        local outcome = evaluate_state(name, state, value, scope, history, exec.executionArn)
        if outcome.error ~= nil then fail(outcome.error, outcome.cause or "") end
        if outcome.spawn ~= nil then
            local results = {}
            for index, spawn in ipairs(outcome.spawn) do
                local child_def: Doc
                if state.Type == "Parallel" then
                    child_def = state.Branches[index]
                else
                    child_def = state.ItemProcessor or state.Iterator
                end
                table.insert(results, test_branch(child_def, spawn.data, exec, history, spawn.map))
            end
            value = finish_fanout(state, value, scope, results)
        elseif outcome.waiting ~= nil then
            fail("States.Runtime", "TestState cannot wait for callback or activity Tasks")
        else
            value = outcome.output
        end
        name = state.Next
    end
    fail("States.Runtime", "TestState branch exceeded the transition limit")
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
    local function drop_children(thread: Thread, failed_child: string?)
        local fan = thread.fan
        if not fan then return end
        for _, cid in ipairs(fan.children) do
            local child = cursor.threads[cid]
            if child then
                if fan.kind == "Map" and child.map and cid ~= failed_child then
                    local previous = history.current
                    history.current = child
                    emit(history, "MapIterationAborted", "mapIterationAbortedEventDetails", {
                        name = fan.name, index = child.map.Index,
                    })
                    history.current = previous
                end
                drop_children(child)
                db.cancel(timer_key("resume", arn, cid))
                db.cancel(timer_key("task", arn, cid))
                db.cancel(timer_key("heartbeat", arn, cid))
                db.cancel(timer_key("poll", arn, cid))
                if child.waiting then
                    close_task_token(ctx, db, child.waiting.token)
                    if child.waiting.child then abort_nested(ctx, db, child.waiting.child) end
                    if child.waiting.poll then stop_ecs(child.waiting.poll) end
                end
                cursor.threads[cid] = nil
            end
        end
        thread.fan = nil
    end

    -- A state failed. Retry rules first (a backoff on a timer), then Catch rules; an error nobody
    -- handles fails the enclosing Parallel/Map state, or the execution.
    local function fail_state(thread: Thread, name: string, state: Doc, code: string, cause: string, skip_catch: boolean?)
        local kind: string = state.Type
        if kind == "Map" or kind == "Parallel" then emit(history, kind .. "StateFailed", nil, nil) end
        if kind == "Task" or kind == "Parallel" or kind == "Map" then
            local delay = retry_delay(state, thread, code)
            if delay ~= nil then
                thread.retrying = true
                thread.sleeping = true
                schedule_resume(db, arn, thread.id, delay)
                return
            end
            local catcher = if skip_catch then nil else find_catcher(state, code)
            if catcher ~= nil then
                local caught = apply_result_path(catcher, thread.data, { Error = code, Cause = cause })
                local payload_ok, payload_error = pcall(check_payload, caught, "Caught state output")
                if not payload_ok then
                    local payload_code, payload_cause = failure_of(payload_error)
                    fail_state(thread, name, state, payload_code, payload_cause, true)
                    return
                end
                exit_state(history, kind, name, caught)
                thread.data = caught
                thread.state = catcher.Next
                thread.enteredTime = nil
                thread.retries = nil
                return
            end
        end
        if thread.parent then
            local parent = cursor.threads[thread.parent :: string]
            local parent_fan = parent.fan
            if parent_fan and parent_fan.kind == "Map" and thread.map then
                emit(history, "MapIterationFailed", "mapIterationFailedEventDetails", {
                    name = parent_fan.name, index = thread.map.Index,
                })
            end
            drop_children(parent, thread.id)
            local parent_name = parent.state :: string
            history.current = parent
            fail_state(parent, parent_name, definition_of(parent).States[parent_name], code, cause)
        else
            failure = { code = code, cause = cause }
        end
    end

    -- Starts up to `count` of the fan-out's queued branches or items as child threads.
    local function launch(thread: Thread, count: number)
        local fan = thread.fan :: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string, kind: string }
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
            local child: Thread = { id = id, parent = thread.id, slot = fan.started, path = path, state = nil, data = spawn.data, map = spawn.map or thread.map, lastEventId = thread.lastEventId }
            child.state = definition_of(child).StartAt
            cursor.threads[id] = child
            table.insert(fan.children, id)
            if fan.kind == "Map" and child.map then
                local previous = history.current
                history.current = child
                emit(history, "MapIterationStarted", "mapIterationStartedEventDetails", {
                    name = fan.name, index = child.map.Index,
                })
                history.current = previous
            end
        end
    end

    local function complete(thread: Thread)
        if thread.parent == nil then
            cursor.done = true
            cursor.output = thread.data
            return
        end
        local parent = cursor.threads[thread.parent :: string]
        local fan = parent.fan :: { pending: number, total: number, results: { [string]: any }, children: { string }, queue: { any }, started: number, name: string, kind: string }
        if fan.kind == "Map" and thread.map then
            emit(history, "MapIterationSucceeded", "mapIterationSucceededEventDetails", {
                name = fan.name, index = thread.map.Index,
            })
        end
        fan.results[tostring(thread.slot)] = thread.data
        fan.pending -= 1
        cursor.threads[thread.id] = nil
        launch(parent, 1) -- a MaxConcurrency slot is free
    end

    -- `limit` is MaxConcurrency: how many children may run at once (0 or nil means all).
    local function start_children(thread: Thread, name: string, kind: string, spawns: { Spawn }, limit: number?)
        thread.fan = { pending = #spawns, total = #spawns, results = {}, children = {}, queue = spawns, started = 0, name = name, kind = kind }
        if kind == "Map" then
            emit(history, "MapStateStarted", "mapStateStartedEventDetails", { length = #spawns })
        else
            emit(history, "ParallelStateStarted", nil, nil)
        end
        local first = #spawns
        if limit ~= nil and limit > 0 and limit < first then first = limit end
        launch(thread, first)
    end

    local function advance(thread: Thread)
        if thread.resumed then
            exit_state(history, thread.resumed.kind, thread.resumed.name, thread.resumed.output)
            thread.resumed = nil
            thread.enteredTime = nil
        end
        local name = thread.state
        if name == nil then
            complete(thread)
            return
        end
        local state: Doc = definition_of(thread).States[name]
        local kind: string = state.Type
        local input_ok, input_error = pcall(check_payload, thread.data, "State input")
        if not input_ok then
            local code, cause = failure_of(input_error)
            fail_state(thread, name, state, code, cause)
            return
        end
        if thread.enteredTime == nil then thread.enteredTime = format_timestamp(roto.now()) end
        if not thread.retrying and thread.fan == nil then
            emit(history, kind .. "StateEntered", "stateEnteredEventDetails", { name = name, input = roto.json_encode(thread.data), inputDetails = { truncated = false } })
        end
        thread.retrying = nil
        local context = context_object(exec, name, cursor.input, thread.enteredTime)
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
            emit(history, fan.kind .. "StateSucceeded", nil, nil)
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
                local phase = if delivered.error == "States.Timeout" then "TimedOut" else "Failed"
                emit_task_event(arn, history, state.Resource, phase, {
                    resourceType = resource_type(state.Resource), resource = state.Resource,
                    error = delivered.error, cause = delivered.cause or "",
                })
            end
            ok, outcome = false, { asl_error = delivered.error, cause = delivered.cause or "" }
        elseif delivered ~= nil then
            local payload_ok, payload_error = pcall(check_payload, delivered.output, "Task connector output")
            if not payload_ok then
                local code, cause = failure_of(payload_error)
                if is_activity(state.Resource) then
                    emit(history, "ActivityFailed", "activityFailedEventDetails", { error = code, cause = cause })
                else
                    emit(history, "TaskFailed", "taskFailedEventDetails", { resourceType = resource_type(state.Resource), resource = state.Resource, error = code, cause = cause })
                end
                ok, outcome = false, { asl_error = code, cause = cause }
            else
                if is_activity(state.Resource) then
                    emit(history, "ActivitySucceeded", "activitySucceededEventDetails", { output = roto.json_encode(delivered.output), outputDetails = { truncated = false } })
                else
                    emit(history, "TaskSucceeded", "taskSucceededEventDetails", { resourceType = resource_type(state.Resource), resource = state.Resource, output = roto.json_encode(delivered.output), outputDetails = { truncated = false } })
                end
                local result = finish_task(state, thread.data, scope, delivered.output)
                ok, outcome = true, { output = result, next = state.Next }
            end
        else
            ok, outcome = pcall(evaluate_state, name, state, thread.data, scope, history, arn)
        end
        if not ok then
            local code, cause = failure_of(outcome)
            fail_state(thread, name, state, code, cause)
            return
        end
        local result: Outcome = outcome :: Outcome
        if result.output ~= nil then
            local output_ok, output_error = pcall(check_payload, result.output, "State output")
            if not output_ok then
                local code, cause = failure_of(output_error)
                fail_state(thread, name, state, code, cause)
                return
            end
        end
        if kind == "Fail" then
            fail_state(thread, name, state, result.error or "", result.cause or "")
            return
        end
        if result.spawn then
            local limit = result.maxConcurrency
            if kind == "Map" then
                limit = if limit == nil or limit == 0 then INLINE_MAP_MAX_CONCURRENCY else math.min(limit, INLINE_MAP_MAX_CONCURRENCY)
            end
            start_children(thread, name, kind, result.spawn, limit)
            return
        end
        if result.waiting then
            -- A callback or activity Task: park the thread until SendTaskSuccess/SendTaskFailure,
            -- the Task's TimeoutSeconds or a missed heartbeat delivers an outcome.
            local queued: any = result.queued or {}
            local heartbeat: number? = queued.heartbeat
            thread.waiting = {
                token = result.waiting, timeout = queued.timeout, heartbeat = heartbeat,
                child = if result.queued then result.queued.child else nil,
                poll = if result.queued then result.queued.poll else nil,
                activity = if result.queued then result.queued.activity else nil,
            }
            local activity_wait = result.queued ~= nil and result.queued.activity ~= nil
            db.put(ns(ctx, "token"), result.waiting, { arn = arn, tid = thread.id,
                deadline = if (queued.child ~= nil or queued.poll ~= nil) and type(queued.timeout) == "number" then roto.now() + queued.timeout else nil })
            if type(queued.timeout) == "number" and not activity_wait then
                db.schedule(timer_key("task", arn, thread.id), { delay = queued.timeout, payload = { arn = arn, kind = "task_timeout", tid = thread.id } })
            end
            if heartbeat ~= nil and not activity_wait then
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
        thread.enteredTime = nil
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
                history.current = thread
                advance(thread)
                history.current = nil
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
    local values = roto.list(list, "InvalidTags", "tags")
    if #values > 50 then roto.error("TooManyTags", "A resource can have at most 50 tags") end
    local function valid_tag_text(value: any, field: string, max_length: number): string
        if type(value) ~= "string" then
            roto.error("InvalidTags", "Each tag needs a string key and value")
        end
        local character_count = utf8.len(value)
        if character_count == nil or character_count > max_length then
            roto.error("InvalidTags", field .. " must be valid UTF-8 and no longer than " .. tostring(max_length) .. " characters")
        end
        -- Step Functions tags allow Unicode letters, digits and whitespace, plus this
        -- ASCII punctuation set. UTF-8 continuation bytes are validated by utf8.len above.
        for index = 1, #value do
            local byte = string.byte(value, index)
            if byte < 0x80 and not (string.match(string.char(byte), "^[%w%s%._:/=+%-@]$") ~= nil) then
                roto.error("InvalidTags", field .. " contains an unsupported character")
            end
        end
        if string.sub(string.lower(value), 1, 4) == "aws:" then
            roto.error("InvalidTags", field .. " cannot use the reserved aws: prefix")
        end
        return value
    end
    for _, tag in ipairs(values) do
        if type(tag) ~= "table" then
            roto.error("InvalidTags", "Each tag needs a string key and value")
        end
        local key = valid_tag_text(tag.key, "Tag key", 128)
        local value = valid_tag_text(tag.value, "Tag value", 256)
        if #key == 0 then roto.error("InvalidTags", "Tag key must not be empty") end
        tags[key] = value
    end
    return tags
end

local function check_tag_quota(tags: { [string]: string })
    local count = 0
    for _ in pairs(tags) do count += 1 end
    if count > 50 then roto.error("TooManyTags", "A resource can have at most 50 tags") end
end

local function tag_list(tags: { [string]: string }?): { any }
    local out = roto.array()
    for _, key in ipairs(sorted_keys(tags or {})) do table.insert(out, { key = key, value = (tags :: any)[key] }) end
    return out
end

local function publish_version(ctx: Ctx, db: Db, sm: Doc, description: string?): Doc
    local highest = 0
    local count = 0
    local versions = db.list(ns(ctx, "version"))
    for _, version in ipairs(versions) do
        if version.stateMachineArn == sm.stateMachineArn then
            count += 1
            if type(version.version) == "number" then highest = math.max(highest, version.version) end
            if version.revisionId == sm.revisionId then return version end
        end
    end
    if count >= 1000 then roto.error("ServiceQuotaExceededException", "A state machine can have at most 1000 versions", 402) end
    local counter = db.get(ns(ctx, "version-counter"), sm.stateMachineArn) :: Doc?
    local number = math.max(highest, if counter then counter.lastVersion else 0) + 1
    db.put(ns(ctx, "version-counter"), sm.stateMachineArn, { lastVersion = number })
    local arn = sm.stateMachineArn .. ":" .. tostring(number)
    local version = {
        stateMachineArn = sm.stateMachineArn, stateMachineVersionArn = arn,
        version = number, name = sm.name, status = sm.status, definition = sm.definition, roleArn = sm.roleArn, type = sm.type,
        creationDate = os.time(), revisionId = sm.revisionId, description = description or "",
        loggingConfiguration = sm.loggingConfiguration, tracingConfiguration = sm.tracingConfiguration,
    }
    db.put(ns(ctx, "version"), arn, version)
    return version
end

local function state_machine_config(logging_value: any, tracing_value: any): (Doc, Doc)
    local logging: any = logging_value or {}
    local tracing: any = tracing_value or {}
    roto.object(logging, "ValidationException", "loggingConfiguration")
    roto.object(tracing, "ValidationException", "tracingConfiguration")

    local level = logging.level or "OFF"
    if level ~= "ALL" and level ~= "ERROR" and level ~= "FATAL" and level ~= "OFF" then
        roto.error("ValidationException", "loggingConfiguration.level must be ALL, ERROR, FATAL, or OFF")
    end
    local include_data = if logging.includeExecutionData == nil then false else logging.includeExecutionData
    if type(include_data) ~= "boolean" then
        roto.error("ValidationException", "loggingConfiguration.includeExecutionData must be a boolean")
    end
    local destinations: any = if logging.destinations == nil then roto.array() else logging.destinations
    roto.list(destinations, "ValidationException", "loggingConfiguration.destinations")
    if #destinations > 1 then
        roto.error("ValidationException", "loggingConfiguration.destinations can contain at most one destination")
    end
    if level ~= "OFF" and #destinations == 0 then
        roto.error("ValidationException", "loggingConfiguration.destinations is required when logging is enabled")
    end
    for _, destination in ipairs(destinations) do
        roto.object(destination, "ValidationException", "logging destination")
        local cloudwatch = destination.cloudWatchLogsLogGroup
        roto.object(cloudwatch, "ValidationException", "cloudWatchLogsLogGroup")
        local arn = cloudwatch.logGroupArn
        if type(arn) ~= "string" or #arn < 1 or #arn > 256 or string.sub(arn, -2) ~= ":*" then
            roto.error("ValidationException", "cloudWatchLogsLogGroup.logGroupArn must be an ARN ending in :*")
        end
    end

    local enabled = if tracing.enabled == nil then false else tracing.enabled
    if type(enabled) ~= "boolean" then
        roto.error("ValidationException", "tracingConfiguration.enabled must be a boolean")
    end
    return { level = level, includeExecutionData = include_data, destinations = destinations }, { enabled = enabled }
end

function M.CreateStateMachine(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "name", "InvalidName")
    if #name > 80 or string.find(name, "[%s<>{}%[%]%?%*\"#%%\\^|~`$&,;:/]") then
        roto.error("InvalidName", "Invalid Name: '" .. name .. "'")
    end
    if input.publish ~= nil and type(input.publish) ~= "boolean" then
        roto.error("ValidationException", "publish must be a boolean")
    end
    if input.versionDescription ~= nil and (input.publish ~= true or type(input.versionDescription) ~= "string" or #input.versionDescription > 256) then
        roto.error("ValidationException", "versionDescription requires publish=true and must be at most 256 characters")
    end
    roto.required(input, "roleArn", "InvalidArn")
    if input.type ~= nil and input.type ~= "STANDARD" and input.type ~= "EXPRESS" then
        roto.error("StateMachineTypeNotSupported", "State machine type must be STANDARD or EXPRESS")
    end
    validate_definition(input.definition, nil, nil, input.type)
    local arn = machine_arn(ctx, name)
    local state_type = input.type or "STANDARD"
    local logging, tracing = state_machine_config(input.loggingConfiguration, input.tracingConfiguration)
    local existing = db.get(ns(ctx, "machine"), arn) :: Doc?
    if existing then
        local same = existing.definition == input.definition and existing.type == state_type
            and roto.json_encode(existing.loggingConfiguration) == roto.json_encode(logging)
            and roto.json_encode(existing.tracingConfiguration) == roto.json_encode(tracing)
            and existing.createPublish == (input.publish == true)
            and existing.createVersionDescription == (input.versionDescription or "")
        if same then
            local version = if input.publish == true then publish_version(ctx, db, existing, input.versionDescription) else nil
            return { stateMachineArn = arn, creationDate = existing.creationDate,
                stateMachineVersionArn = if version then version.stateMachineVersionArn else nil }
        end
        roto.error("StateMachineAlreadyExists", "State Machine Already Exists: '" .. arn .. "'")
    end
    local now = os.time()
    local doc = {
        stateMachineArn = arn,
        name = name,
        definition = input.definition,
        roleArn = input.roleArn,
        type = state_type,
        creationDate = now,
        status = "ACTIVE",
        loggingConfiguration = logging,
        tracingConfiguration = tracing,
        createPublish = input.publish == true,
        createVersionDescription = input.versionDescription or "",
        revisionId = roto.uuid(),
        tags = tags_from(input.tags),
    }
    db.put(ns(ctx, "machine"), arn, doc)
    local version = if input.publish == true then publish_version(ctx, db, doc, input.versionDescription) else nil
    return { stateMachineArn = arn, creationDate = now,
        stateMachineVersionArn = if version then version.stateMachineVersionArn else nil }
end

function M.UpdateStateMachine(ctx: Ctx, input: any, db: Db)
    local doc = machine(ctx, db, input.stateMachineArn)
    if input.publish ~= nil and type(input.publish) ~= "boolean" then
        roto.error("ValidationException", "publish must be a boolean")
    end
    if input.versionDescription ~= nil and (input.publish ~= true or type(input.versionDescription) ~= "string" or #input.versionDescription > 256) then
        roto.error("ValidationException", "versionDescription requires publish=true and must be at most 256 characters")
    end
    if input.definition == nil and input.roleArn == nil then
        roto.error("MissingRequiredParameter", "Either definition or roleArn must be specified")
    end
    if input.loggingConfiguration ~= nil or input.tracingConfiguration ~= nil then
        local logging, tracing = state_machine_config(
            input.loggingConfiguration or doc.loggingConfiguration,
            input.tracingConfiguration or doc.tracingConfiguration
        )
        doc.loggingConfiguration = logging
        doc.tracingConfiguration = tracing
    end
    if input.definition ~= nil then doc.definition = validate_definition(input.definition, nil, nil, doc.type) end
    doc.roleArn = input.roleArn or doc.roleArn
    doc.updateDate = os.time()
    doc.revisionId = roto.uuid()
    db.put(ns(ctx, "machine"), doc.stateMachineArn, doc)
    local version = if input.publish == true then publish_version(ctx, db, doc, input.versionDescription) else nil
    return { updateDate = doc.updateDate, revisionId = doc.revisionId,
        stateMachineVersionArn = if version then version.stateMachineVersionArn else nil }
end

local function tagged_resource(ctx: Ctx, db: Db, arn: any): (Doc, string, string)
    local key = roto.required({ arn = arn }, "arn", "InvalidArn")
    local machine_space = ns(ctx, "machine")
    local machine_doc = db.get(machine_space, key) :: Doc?
    if machine_doc then return machine_doc, machine_space, key end
    if is_activity(key) then
        local activity_space = ns(ctx, "activity")
        local activity_doc = db.get(activity_space, key) :: Doc?
        if activity_doc then return activity_doc, activity_space, key end
    end
    roto.error("ResourceNotFound", "Resource not found: '" .. key .. "'", 404)
end

function M.TagResource(ctx: Ctx, input: any, db: Db)
    local doc, space, key = tagged_resource(ctx, db, input.resourceArn)
    local tags: { [string]: string } = doc.tags or {}
    for key, value in pairs(tags_from(input.tags)) do tags[key] = value end
    check_tag_quota(tags)
    doc.tags = tags
    db.put(space, key, doc)
    return {}
end

function M.UntagResource(ctx: Ctx, input: any, db: Db)
    local doc, space, key = tagged_resource(ctx, db, input.resourceArn)
    local tags: { [string]: string } = doc.tags or {}
    for _, key in ipairs(roto.list(input.tagKeys, "InvalidTags", "tagKeys")) do tags[key] = nil end
    doc.tags = tags
    db.put(space, key, doc)
    return {}
end

function M.ListTagsForResource(ctx: Ctx, input: any, db: Db)
    local doc = tagged_resource(ctx, db, input.resourceArn)
    return { tags = tag_list(doc.tags) }
end

function M.DeleteStateMachine(ctx: Ctx, input: any, db: Db)
    -- AWS removes every published version and alias with the unqualified machine.
    local arn = roto.required(input, "stateMachineArn", "InvalidArn")
    local base = string.match(arn, "^(.*:stateMachine:[^:/]+)") or arn
    db.delete(ns(ctx, "machine"), base)
    db.delete(ns(ctx, "version-counter"), base)
    for _, alias in ipairs(db.list(ns(ctx, "alias"))) do
        if alias.stateMachineArn == base then db.delete(ns(ctx, "alias"), alias.stateMachineAliasArn) end
    end
    for _, version in ipairs(db.list(ns(ctx, "version"))) do
        if version.stateMachineArn == base then db.delete(ns(ctx, "version"), version.stateMachineVersionArn) end
    end
    return {}
end

function M.DescribeStateMachine(ctx: Ctx, input: any, db: Db)
    local arn = roto.required(input, "stateMachineArn", "InvalidArn")
    local version = db.get(ns(ctx, "version"), arn) :: Doc?
    local alias = db.get(ns(ctx, "alias"), arn) :: Doc?
    local doc = if version then version elseif alias then machine(ctx, db, alias.stateMachineArn) else machine(ctx, db, arn)
    local out: Doc = {}
    for key, value in pairs(doc :: Doc) do out[key] = value end
    if version then out.stateMachineArn = version.stateMachineVersionArn; out.version = nil end
    out.createPublish = nil
    out.createVersionDescription = nil
    out.tags = nil -- not part of DescribeStateMachine; use ListTagsForResource
    return out
end

function M.ListStateMachines(ctx: Ctx, input: any, db: Db)
    local rows: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "machine"))) do
        table.insert(rows, { stateMachineArn = doc.stateMachineArn, name = doc.name, type = doc.type, creationDate = doc.creationDate })
    end
    return states_page(ctx, db, input, rows, "stateMachines")
end

-- Publish an immutable snapshot. Version numbers are monotonic within a machine.
function M.PublishStateMachineVersion(ctx: Ctx, input: any, db: Db)
    local sm = machine(ctx, db, input.stateMachineArn)
    if input.description ~= nil and (type(input.description) ~= "string" or #input.description > 256) then
        roto.error("ValidationException", "Version description must be at most 256 characters")
    end
    if input.revisionId ~= nil and input.revisionId ~= sm.revisionId then
        roto.error("ConflictException", "The state machine revision changed before the version was published", 409)
    end
    local version = publish_version(ctx, db, sm, input.description)
    return { stateMachineVersionArn = version.stateMachineVersionArn, creationDate = version.creationDate }
end

function M.DeleteStateMachineVersion(ctx: Ctx, input: any, db: Db)
    local arn = roto.required(input, "stateMachineVersionArn", "InvalidArn")
    if not db.get(ns(ctx, "version"), arn) then roto.error("ResourceNotFound", "State machine version not found: '" .. arn .. "'", 404) end
    for _, alias in ipairs(db.list(ns(ctx, "alias"))) do
        for _, target in ipairs(alias.routingConfiguration or {}) do
            if target.stateMachineVersionArn == arn then
                roto.error("ConflictException", "State machine version is referenced by alias '" .. alias.stateMachineAliasArn .. "'", 409)
            end
        end
    end
    db.delete(ns(ctx, "version"), arn)
    return {}
end

function M.ListStateMachineVersions(ctx: Ctx, input: any, db: Db)
    local sm = machine(ctx, db, input.stateMachineArn)
    local rows = {}
    for _, version in ipairs(db.list(ns(ctx, "version"))) do
        if version.stateMachineArn == sm.stateMachineArn then
            table.insert(rows, { stateMachineVersionArn = version.stateMachineVersionArn, creationDate = version.creationDate })
        end
    end
    table.sort(rows, function(a, b)
        if a.creationDate ~= b.creationDate then return a.creationDate > b.creationDate end
        return (tonumber(string.match(a.stateMachineVersionArn, "([^:]+)$")) or 0)
            > (tonumber(string.match(b.stateMachineVersionArn, "([^:]+)$")) or 0)
    end)
    return states_page(ctx, db, input, rows, "stateMachineVersions")
end

local function alias_arn(sm_arn: string, name: string): string
    return sm_arn .. ":alias:" .. name
end

local function validate_routing(ctx: Ctx, db: Db, routing: any, machine_arn_value: string): any
    local entries = roto.list(routing, "ValidationException", "routingConfiguration")
    if #entries < 1 or #entries > 2 then
        roto.error("ValidationException", "routingConfiguration must contain one or two versions")
    end
    local total = 0
    local seen: { [string]: boolean } = {}
    for _, item in ipairs(entries) do
        roto.object(item, "ValidationException", "routing entry")
        if type(item.stateMachineVersionArn) ~= "string" or item.stateMachineVersionArn == ""
            or type(item.weight) ~= "number" or item.weight < 0 or item.weight > 100 or item.weight % 1 ~= 0 then
            roto.error("ValidationException", "Each routing entry needs a version ARN and weight from 0 to 100")
        end
        if seen[item.stateMachineVersionArn] then roto.error("ValidationException", "A version may appear only once in routingConfiguration") end
        seen[item.stateMachineVersionArn] = true
        local version = db.get(ns(ctx, "version"), item.stateMachineVersionArn)
        if not version then roto.error("ResourceNotFound", "State machine version not found: '" .. item.stateMachineVersionArn .. "'", 404) end
        if version.stateMachineArn ~= machine_arn_value then roto.error("ValidationException", "Alias routing versions must belong to the aliased state machine") end
        total += item.weight
    end
    if total ~= 100 then roto.error("ValidationException", "Routing weights must add up to 100") end
    return routing
end

function M.CreateStateMachineAlias(ctx: Ctx, input: any, db: Db)
    local sm = machine(ctx, db, input.stateMachineArn)
    local name = roto.required(input, "name", "InvalidName")
    if #name < 1 or #name > 80 or string.find(name, "[^%w_.%-]") or string.match(name, "^%d+$") then roto.error("InvalidName", "Invalid alias name") end
    if input.description ~= nil and (type(input.description) ~= "string" or #input.description > 256) then
        roto.error("ValidationException", "Alias description must be a string of at most 256 characters")
    end
    local arn = alias_arn(sm.stateMachineArn, name)
    local routing = validate_routing(ctx, db, input.routingConfiguration, sm.stateMachineArn)
    local existing = db.get(ns(ctx, "alias"), arn) :: Doc?
    if existing then
        if existing.description == (input.description or "") and roto.json_encode(existing.routingConfiguration) == roto.json_encode(routing) then
            return { stateMachineAliasArn = arn, creationDate = existing.creationDate }
        end
        roto.error("StateMachineAlreadyExists", "Alias already exists: '" .. arn .. "'")
    end
    local count = 0
    for _, alias in ipairs(db.list(ns(ctx, "alias"))) do
        if alias.stateMachineArn == sm.stateMachineArn then count += 1 end
    end
    if count >= 100 then roto.error("ServiceQuotaExceededException", "A state machine can have at most 100 aliases", 402) end
    local doc = { stateMachineAliasArn = arn, stateMachineArn = sm.stateMachineArn, name = name,
        routingConfiguration = routing, description = input.description or "", creationDate = os.time(), updateDate = os.time() }
    db.put(ns(ctx, "alias"), arn, doc)
    return { stateMachineAliasArn = arn, creationDate = doc.creationDate }
end

function M.DescribeStateMachineAlias(ctx: Ctx, input: any, db: Db)
    local doc = db.get(ns(ctx, "alias"), roto.required(input, "stateMachineAliasArn", "InvalidArn"))
    if not doc then roto.error("ResourceNotFound", "State machine alias not found", 404) end
    return doc
end

function M.UpdateStateMachineAlias(ctx: Ctx, input: any, db: Db)
    local arn = roto.required(input, "stateMachineAliasArn", "InvalidArn")
    local doc = db.get(ns(ctx, "alias"), arn) :: Doc?
    if not doc then roto.error("ResourceNotFound", "State machine alias not found", 404) end
    if input.routingConfiguration == nil and input.description == nil then
        roto.error("MissingRequiredParameter", "routingConfiguration or description must be specified")
    end
    if input.description ~= nil and (type(input.description) ~= "string" or #input.description > 256) then
        roto.error("ValidationException", "Alias description must be a string of at most 256 characters")
    end
    if input.routingConfiguration ~= nil then (doc :: Doc).routingConfiguration = validate_routing(ctx, db, input.routingConfiguration, (doc :: Doc).stateMachineArn) end
    if input.description ~= nil then (doc :: Doc).description = input.description end
    (doc :: Doc).updateDate = os.time()
    db.put(ns(ctx, "alias"), arn, doc)
    return { stateMachineAliasArn = (doc :: Doc).stateMachineAliasArn,
        creationDate = (doc :: Doc).creationDate, updateDate = (doc :: Doc).updateDate }
end

function M.DeleteStateMachineAlias(ctx: Ctx, input: any, db: Db)
    local arn = roto.required(input, "stateMachineAliasArn", "InvalidArn")
    if not db.get(ns(ctx, "alias"), arn) then roto.error("ResourceNotFound", "State machine alias not found", 404) end
    db.delete(ns(ctx, "alias"), arn)
    return {}
end

function M.ListStateMachineAliases(ctx: Ctx, input: any, db: Db)
    local arn = roto.required(input, "stateMachineArn", "InvalidArn")
    local requested_version = db.get(ns(ctx, "version"), arn) :: Doc?
    local sm = if requested_version then machine(ctx, db, requested_version.stateMachineArn) else machine(ctx, db, arn)
    local rows = {}
    for _, alias in ipairs(db.list(ns(ctx, "alias"))) do
        if alias.stateMachineArn == sm.stateMachineArn then
            local references_version = false
            for _, target in ipairs(alias.routingConfiguration or {}) do
                if target.stateMachineVersionArn == arn then references_version = true end
            end
            if not requested_version or references_version then
                table.insert(rows, { stateMachineAliasArn = alias.stateMachineAliasArn, creationDate = alias.creationDate })
            end
        end
    end
    table.sort(rows, function(a, b)
        if a.creationDate ~= b.creationDate then return a.creationDate > b.creationDate end
        return a.stateMachineAliasArn > b.stateMachineAliasArn
    end)
    return states_page(ctx, db, input, rows, "stateMachineAliases")
end

start_execution = function(ctx: Ctx, input: any, db: Db, parent: { token: string, v2: boolean, express: boolean? }?, defer: boolean?): any
    local requested_arn = roto.required(input, "stateMachineArn", "InvalidArn")
    local alias = db.get(ns(ctx, "alias"), requested_arn) :: Doc?
    local version = db.get(ns(ctx, "version"), requested_arn) :: Doc?
    if alias then
        local point = math.random(1, 100)
        local total = 0
        for _, target in ipairs(alias.routingConfiguration) do
            total += target.weight
            if point <= total then version = db.get(ns(ctx, "version"), target.stateMachineVersionArn) :: Doc?; break end
        end
    end
    local sm = if version then machine(ctx, db, version.stateMachineArn) else machine(ctx, db, requested_arn)
    local definition_text = if version then version.definition else sm.definition
    local execution_role = if version then version.roleArn else sm.roleArn
    local payload = if input.input == nil then "{}" else input.input
    if type(payload) ~= "string" or #payload > MAX_PAYLOAD_BYTES then
        roto.error("InvalidExecutionInput", "Execution input must be a JSON string no larger than 256 KiB")
    end
    local valid_input = pcall(roto.json_decode, payload)
    if not valid_input then roto.error("InvalidExecutionInput", "Execution input must be valid JSON") end
    local name = if input.name == nil then roto.uuid() else input.name
    if type(name) ~= "string" or #name < 1 or #name > 80 or string.find(name, "[%s<>{}%[%]%?%*\"#%%\\%^|~`$&,;:/]") then
        roto.error("InvalidName", "Execution name is invalid")
    end
    if input.traceHeader ~= nil then
        if type(input.traceHeader) ~= "string" or #input.traceHeader > 256 then
            roto.error("ValidationException", "traceHeader must be an ASCII string no longer than 256 characters")
        end
        for index = 1, #input.traceHeader do
            local byte = string.byte(input.traceHeader, index)
            if byte > 0x7f then
                roto.error("ValidationException", "traceHeader must contain only ASCII characters")
            end
        end
    end
    local arn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id
        .. (if sm.type == "EXPRESS" then ":express:" else ":execution:") .. sm.name .. ":" .. name
        .. (if sm.type == "EXPRESS" then ":" .. roto.uuid() else "")
    local existing = db.get(ns(ctx, "execution"), arn) :: Doc?
    if existing then
        -- Standard executions are idempotent while running. Express executions use unique ARNs.
        if sm.type == "STANDARD" and existing.status == "RUNNING" and existing.input == payload then
            return { executionArn = arn, startDate = existing.startDate }
        end
        roto.error("ExecutionAlreadyExists", "Execution Already Exists: '" .. arn .. "'")
    end
    local now = roto.now()
    local exec: Doc = {
        executionArn = arn,
        stateMachineArn = sm.stateMachineArn,
        stateMachineVersionArn = if version then version.stateMachineVersionArn else nil,
        stateMachineAliasArn = if alias then alias.stateMachineAliasArn else nil,
        name = name,
        status = "RUNNING",
        startDate = now,
        input = payload,
        inputDetails = { included = true },
        roleArn = execution_role,
        stateMachineSnapshot = {
            name = sm.name, type = if version then version.type else sm.type, definition = definition_text,
            roleArn = if version then version.roleArn else sm.roleArn,
            loggingConfiguration = if version then version.loggingConfiguration else sm.loggingConfiguration,
            tracingConfiguration = if version then version.tracingConfiguration else sm.tracingConfiguration,
            revisionId = if version then version.revisionId else sm.revisionId,
            updateDate = if version then version.creationDate else sm.updateDate or sm.creationDate,
        },
        traceHeader = input.traceHeader,
    }
    local history: History = { events = {}, count = 0 }
    emit(history, "ExecutionStarted", "executionStartedEventDetails", {
        input = payload, inputDetails = { truncated = false }, roleArn = execution_role,
        stateMachineVersionArn = exec.stateMachineVersionArn,
        stateMachineAliasArn = exec.stateMachineAliasArn,
    })
    local definition = roto.json_decode(definition_text)
    local data = roto.json_decode(payload)
    local timeout = definition.TimeoutSeconds
    if sm.type == "EXPRESS" then timeout = math.min(timeout or 300, 300) end
    if type(timeout) == "number" then
        db.schedule("timeout/" .. arn, { delay = timeout, payload = { arn = arn, kind = "timeout" } })
    end
    db.put(ns(ctx, "execution"), arn, exec)
    if parent then db.put(ns(ctx, "parent"), arn, { token = parent.token, v2 = parent.v2, express = parent.express }) end
    local root: Thread = { id = "t0", path = {}, state = definition.StartAt, data = data, lastEventId = history.count }
    local cursor = { definition = definition_text, input = data, threads = { t0 = root }, next_tid = 1 }
    if defer then
        db.put(ns(ctx, "cursor"), arn, cursor)
        save_history(ctx, db, arn, history)
        db.schedule("continue/" .. arn, { delay = 0, payload = { arn = arn, kind = "continue" } })
    else
        run(ctx, db, exec, cursor, history)
    end
    return { executionArn = arn, startDate = now }
end

function M.StartExecution(ctx: Ctx, input: any, db: Db)
    return start_execution(ctx, input, db, nil)
end

-- HTTP dispatch waits outside the service transaction for this deferred execution.
function M.RotoStartSyncExecution(ctx: Ctx, input: any, db: Db)
    local requested = roto.required(input, "stateMachineArn", "InvalidArn")
    local alias = db.get(ns(ctx, "alias"), requested) :: Doc?
    local version = db.get(ns(ctx, "version"), requested) :: Doc?
    local arn = if alias then alias.stateMachineArn elseif version then version.stateMachineArn else requested
    local sm = machine(ctx, db, arn)
    if sm.type ~= "EXPRESS" then roto.error("StateMachineTypeNotSupported", "StartSyncExecution requires an EXPRESS state machine") end
    if input.includedData ~= nil and input.includedData ~= "ALL_DATA" and input.includedData ~= "METADATA_ONLY" then
        roto.error("ValidationException", "Invalid includedData")
    end
    return start_execution(ctx, input, db, nil, true)
end

function M.StartSyncExecution(_ctx: Ctx, _input: any, _db: Db)
    roto.error("ValidationException", "StartSyncExecution requires HTTP dispatch")
end

function M.RotoGetSyncExecution(ctx: Ctx, input: any, db: Db)
    local exec = execution(ctx, db, input.executionArn)
    if exec.status == "RUNNING" then return { pending = true } end
    return {
        executionArn = exec.executionArn, stateMachineArn = exec.stateMachineArn, name = exec.name,
        status = exec.status, startDate = exec.startDate, stopDate = exec.stopDate,
        input = exec.input, inputDetails = exec.inputDetails, output = exec.output, outputDetails = exec.outputDetails,
        error = exec.error, cause = exec.cause, traceHeader = exec.traceHeader,
    }
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
    local poll_deadline: number? = nil
    if kind == "poll" and thread and thread.waiting then
        local token = db.get(ns(ctx, "token"), thread.waiting.token) :: Doc?
        if token and type(token.deadline) == "number" then
            poll_deadline = token.deadline
            if roto.now() >= token.deadline then kind = "task_timeout" end
        end
    end
    if kind == "resume" then
        if not thread then return {} end
        thread.sleeping = nil
    elseif kind == "poll" then
        if not thread or not thread.waiting or not thread.waiting.poll then return {} end
        local ok, result, failed = pcall(poll_ecs, thread.waiting.poll)
        local expired = poll_deadline ~= nil and roto.now() >= poll_deadline
        if ok and result == nil and not expired then
            -- Still running: look again later.
            db.schedule(timer_key("poll", arn, thread.id), { delay = POLL_SECONDS, payload = { arn = arn, kind = "poll", tid = thread.id } })
            return {}
        end
        close_task_token(ctx, db, thread.waiting.token)
        db.cancel(timer_key("task", arn, thread.id))
        db.cancel(timer_key("heartbeat", arn, thread.id))
        if expired then stop_ecs(thread.waiting.poll) end
        thread.waiting = nil
        thread.retrying = true
        if expired then
            thread.delivered = { error = "States.Timeout", cause = "The task timed out" }
        elseif not ok then
            local code, cause = failure_of(result)
            thread.delivered = { error = code, cause = cause }
        elseif failed then
            thread.delivered = { error = "States.TaskFailed", cause = roto.json_encode(result) }
        else
            thread.delivered = { output = result }
        end
    elseif kind == "task_timeout" then
        if not thread or not thread.waiting then return {} end
        close_task_token(ctx, db, thread.waiting.token)
        if thread.waiting.child then abort_nested(ctx, db, thread.waiting.child) end
        if thread.waiting.poll then stop_ecs(thread.waiting.poll) end
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
    if type(token) ~= "string" or token == "" or #token > 2048 then
        roto.error("InvalidToken", "Invalid Token: 'Invalid token'")
    end
    local entry = db.get(ns(ctx, "token"), token) :: Doc?
    if not entry then roto.error("InvalidToken", "Invalid Token: '" .. token .. "'") end
    if (entry :: Doc).closed then roto.error("TaskTimedOut", "Task Timed Out: '" .. token .. "'") end
    local arn: string = (entry :: Doc).arn
    local exec = db.get(ns(ctx, "execution"), arn) :: Doc?
    local cursor = db.get(ns(ctx, "cursor"), arn) :: Cursor?
    local thread = if cursor then cursor.threads[(entry :: Doc).tid or ""] else nil
    if thread and thread.waiting and thread.waiting.activity ~= nil
        and not db.get(ns(ctx, "activity"), thread.waiting.activity) then
        roto.error("TaskDoesNotExist", "Activity does not exist for task token: '" .. token .. "'")
    end
    if not exec or exec.status ~= "RUNNING" or not cursor or not thread or not thread.waiting or thread.waiting.token ~= token then
        roto.error("TaskTimedOut", "Task Timed Out: '" .. token .. "'")
    end
    return exec :: Doc, cursor :: Cursor, load_history(ctx, db, arn), thread :: Thread
end

local function deliver(ctx: Ctx, db: Db, input: any, delivered: { output: any?, error: string?, cause: string? })
    local exec, cursor, history, thread = waiting_task(ctx, db, input.taskToken)
    local arn: string = exec.executionArn
    close_task_token(ctx, db, (thread.waiting :: { token: string, heartbeat: number?, child: string?, poll: any? }).token)
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
    local token_entry = db.get(ns(ctx, "token"), token) :: Doc?
    if not token_entry or token_entry.closed then return end -- the parent has not parked, or it has closed
    -- A child may finish after its parent's timer became due while this service was
    -- busy running the child. Do not let that late completion cancel the timeout.
    if type(token_entry.deadline) == "number" and roto.now() >= token_entry.deadline then
        deliver(ctx, db, { taskToken = token }, { error = "States.Timeout", cause = "The task timed out" })
        return
    end
    if link.express then
        deliver(ctx, db, { taskToken = token }, { output = {
            ExecutionArn = exec.executionArn, StateMachineArn = exec.stateMachineArn, Name = exec.name,
            Status = exec.status, StartDate = exec.startDate, StopDate = exec.stopDate,
            Input = exec.input, InputDetails = exec.inputDetails and { Included = exec.inputDetails.included },
            Output = exec.output, OutputDetails = exec.outputDetails and { Included = exec.outputDetails.included },
            Error = exec.error, Cause = exec.cause, TraceHeader = exec.traceHeader,
        } })
        return
    end
    local described = describe_doc(exec, (link :: Doc).v2 == true)
    if exec.status == "SUCCEEDED" then
        deliver(ctx, db, { taskToken = token }, { output = described })
    else
        deliver(ctx, db, { taskToken = token }, { error = "States.TaskFailed", cause = roto.json_encode(described) })
    end
end

function M.SendTaskSuccess(ctx: Ctx, input: any, db: Db)
    if type(input.output) ~= "string" or #input.output > MAX_PAYLOAD_BYTES then
        roto.error("InvalidOutput", "Task output must be a JSON string no larger than 256 KiB")
    end
    local ok, output = pcall(roto.json_decode, input.output)
    if not ok then roto.error("InvalidOutput", "Task output must be valid JSON") end
    deliver(ctx, db, input, { output = output })
    return {}
end

function M.SendTaskFailure(ctx: Ctx, input: any, db: Db)
    local function failure_field(value: any, name: string, limit: number): string
        if value == nil or value == NULL then return "" end
        if type(value) ~= "string" then
            roto.error("ValidationException", name .. " must be a string")
        end
        local length = utf8.len(value)
        if length == nil or length > limit then
            roto.error("ValidationException", name .. " must be valid UTF-8 and at most " .. tostring(limit) .. " characters")
        end
        return value
    end
    local code = failure_field(input.error, "error", 256)
    local cause = failure_field(input.cause, "cause", 32768)
    deliver(ctx, db, input, { error = code, cause = cause })
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
    if input.tags ~= nil then
        doc.tags = tags_from(input.tags)
        check_tag_quota(doc.tags)
    end
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
    return states_page(ctx, db, input, out, "activities")
end

-- Hands the oldest queued task to a worker. HTTP long polling repeats this short transaction
-- outside the Luau runtime; in-process calls and the test runner perform one immediate poll.
function M.GetActivityTask(ctx: Ctx, input: any, db: Db)
    local doc = activity(ctx, db, input.activityArn)
    if input.workerName ~= nil and (type(input.workerName) ~= "string" or #input.workerName < 1 or #input.workerName > 80) then
        roto.error("ValidationException", "workerName must contain between 1 and 80 characters")
    end
    local space = ns(ctx, "activity-queue") .. "/" .. doc.activityArn
    for _, task in ipairs(db.list(space) :: { Doc }) do
        db.delete(space, task.id)
        -- A task whose execution already moved on (timeout, stop) has no token any more: skip it.
        local token_entry = db.get(ns(ctx, "token"), task.token) :: Doc?
        if token_entry and not token_entry.closed then
            local exec = db.get(ns(ctx, "execution"), task.arn) :: Doc?
            if exec then
                local history = load_history(ctx, db, task.arn)
                local cursor = db.get(ns(ctx, "cursor"), task.arn) :: Cursor?
                local thread = if token_entry and cursor then cursor.threads[token_entry.tid] else nil
                history.current = thread
                emit(history, "ActivityStarted", "activityStartedEventDetails", { workerName = input.workerName })
                history.current = nil
                if thread and thread.waiting then
                    if thread.waiting.timeout ~= nil then
                        db.schedule(timer_key("task", task.arn, thread.id), { delay = thread.waiting.timeout, payload = { arn = task.arn, kind = "task_timeout", tid = thread.id } })
                    end
                    if thread.waiting.heartbeat ~= nil then
                        db.schedule(timer_key("heartbeat", task.arn, thread.id), { delay = thread.waiting.heartbeat, payload = { arn = task.arn, kind = "task_timeout", tid = thread.id } })
                    end
                end
                if cursor then db.put(ns(ctx, "cursor"), task.arn, cursor) end
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
    return { stopDate = doc.stopDate or roto.now() }
end

function M.DescribeExecution(ctx: Ctx, input: any, db: Db)
    local doc = execution(ctx, db, input.executionArn)
    local out: Doc = {}
    for key, value in pairs(doc) do out[key] = value end
    out.roleArn = nil -- kept for the history only; DescribeExecution does not return it
    out.stateMachineSnapshot = nil -- internal pinned definition/configuration
    if doc.status ~= "RUNNING" then out.stopDate = doc.stopDate end
    return out
end

function M.DescribeStateMachineForExecution(ctx: Ctx, input: any, db: Db)
    local exec = execution(ctx, db, input.executionArn)
    local snapshot = exec.stateMachineSnapshot :: Doc?
    if snapshot then
        if snapshot.type == "EXPRESS" then roto.error("ValidationException", "DescribeStateMachineForExecution is not supported for EXPRESS state machines") end
        return {
            stateMachineArn = exec.stateMachineArn, name = snapshot.name, type = snapshot.type,
            definition = snapshot.definition, roleArn = snapshot.roleArn,
            updateDate = snapshot.updateDate,
            loggingConfiguration = snapshot.loggingConfiguration,
            tracingConfiguration = snapshot.tracingConfiguration,
            revisionId = snapshot.revisionId,
        }
    end
    local doc = db.get(ns(ctx, "machine"), exec.stateMachineArn) :: Doc?
    if not doc then roto.error("StateMachineDoesNotExist", "State Machine Does Not Exist: '" .. exec.stateMachineArn .. "'") end
    local sm = doc :: Doc
    if sm.type == "EXPRESS" then roto.error("ValidationException", "DescribeStateMachineForExecution is not supported for EXPRESS state machines") end
    local version = if exec.stateMachineVersionArn then db.get(ns(ctx, "version"), exec.stateMachineVersionArn) :: Doc? else nil
    return {
        stateMachineArn = sm.stateMachineArn, name = sm.name, type = if version then version.type else sm.type,
        definition = if version then version.definition else sm.definition, roleArn = if version then version.roleArn else sm.roleArn,
        updateDate = if version then version.creationDate else sm.updateDate or sm.creationDate,
        loggingConfiguration = if version then version.loggingConfiguration else sm.loggingConfiguration,
        tracingConfiguration = if version then version.tracingConfiguration else sm.tracingConfiguration,
        revisionId = if version then version.revisionId else sm.revisionId,
    }
end

-- Parse text-delimited input, preserving quoted newlines, doubled quotes and escapes.
local function test_csv(raw: string, config: any): any
    local delimiters = { COMMA = ",", PIPE = "|", SEMICOLON = ";", SPACE = " ", TAB = "\t" }
    local delimiter = delimiters[config.CSVDelimiter or "COMMA"]
    if delimiter == nil then roto.error("ValidationException", "Invalid CSVDelimiter") end
    local records: { { string } } = {}
    local record: { string } = {}
    local field: { string } = {}
    local quoted, closed, started = false, false, false
    local i = 1
    local function end_field()
        table.insert(record, table.concat(field)); field = {}; closed = false; started = false
    end
    local function end_record()
        end_field(); table.insert(records, record); record = {}
    end
    while i <= #raw do
        local c = string.sub(raw, i, i)
        local next_c = string.sub(raw, i + 1, i + 1)
        if c == "\\" then
            if next_c == "" then roto.error("ValidationException", "CSV ends with an escape") end
            table.insert(field, next_c); started = true; i += 1
        elseif quoted then
            if c == '"' then
                if next_c == '"' then table.insert(field, '"'); i += 1 else quoted = false; closed = true end
            else table.insert(field, c) end
        elseif c == delimiter then end_field()
        elseif c == "\n" or c == "\r" then
            end_record()
            if c == "\r" and next_c == "\n" then i += 1 end
        elseif c == '"' and not started and not closed then quoted = true; started = true
        elseif closed or c == '"' then roto.error("ValidationException", "Invalid CSV quotation")
        else table.insert(field, c); started = true end
        i += 1
    end
    if quoted then roto.error("ValidationException", "Unterminated CSV quotation") end
    if started or closed or #record > 0 then end_record() end
    local headers: any
    if config.CSVHeaderLocation == "FIRST_ROW" then
        if config.CSVHeaders ~= nil or #records == 0 then roto.error("ValidationException", "CSV requires a first-row header") end
        headers = table.remove(records, 1)
    elseif config.CSVHeaderLocation == "GIVEN" then
        headers = config.CSVHeaders
        if not is_array(headers) or #headers == 0 then roto.error("ValidationException", "CSVHeaders must be a nonempty array") end
    else roto.error("ValidationException", "CSVHeaderLocation must be FIRST_ROW or GIVEN") end
    local seen = {}
    for _, header in ipairs(headers) do
        if type(header) ~= "string" or header == "" or seen[header] then
            roto.error("ValidationException", "CSV headers must be distinct nonempty strings")
        end
        seen[header] = true
    end
    local items = {}
    for _, row in ipairs(records) do
        local item = {}
        for index, header in ipairs(headers) do item[header] = row[index] or "" end
        table.insert(items, item)
    end
    return roto.array(items)
end

-- Decode a supplied ItemReader fixture without calling S3.
local function test_reader(state: Doc, raw: any): any
    if type(raw) ~= "string" or #raw > MAX_PAYLOAD_BYTES then
        roto.error("ValidationException", "mapItemReaderData must be a string no larger than 256 KiB")
    end
    local reader = state.ItemReader
    if not is_object(reader) or type(reader.Resource) ~= "string" or not string.match(reader.Resource, ":s3:getObject$") then
        roto.error("ValidationException", "mapItemReaderData requires an S3 GetObject ItemReader")
    end
    local config = reader.ReaderConfig or {}
    if not is_object(config) then roto.error("ValidationException", "ReaderConfig must be an object") end
    local kind = config.InputType
    if kind ~= "JSON" and kind ~= "JSONL" and kind ~= "CSV" then
        roto.error("ValidationException", "mapItemReaderData supports JSON, JSONL and CSV ItemReaders")
    end
    if config.ManifestType ~= nil then roto.error("ValidationException", "ItemReader manifests are not supported") end
    if state.ItemsPath ~= nil then roto.error("ValidationException", "ItemsPath cannot be combined with an S3 ItemReader") end
    local items: any
    if kind == "JSON" then
        local ok, decoded = pcall(roto.json_decode, raw)
        if not ok then roto.error("ValidationException", "mapItemReaderData must contain valid JSON") end
        items = decoded
        local pointer = config.ItemsPointer
        if pointer ~= nil then
            if type(pointer) ~= "string" or (pointer ~= "" and string.sub(pointer, 1, 1) ~= "/") then
                roto.error("ValidationException", "ItemsPointer must be a JSON pointer")
            end
            for token in string.gmatch(pointer, "/([^/]*)") do
                if string.find(token, "~[^01]") or string.sub(token, -1) == "~" then
                    roto.error("ValidationException", "ItemsPointer contains an invalid escape")
                end
                token = string.gsub(string.gsub(token, "~1", "/"), "~0", "~")
                if is_array(items) then
                    if not string.match(token, "^%d+$") or (#token > 1 and string.sub(token, 1, 1) == "0") then
                        roto.error("ValidationException", "ItemsPointer has an invalid array index")
                    end
                    items = items[(tonumber(token) :: number) + 1]
                elseif is_object(items) then items = items[token]
                else items = nil end
                if items == nil then roto.error("ValidationException", "ItemsPointer does not exist in mapItemReaderData") end
            end
        end
    elseif kind == "JSONL" then
        if config.ItemsPointer ~= nil then roto.error("ValidationException", "ItemsPointer requires JSON input") end
        local rows = {}
        for line in string.gmatch(raw .. "\n", "([^\n]*)\n") do
            if string.match(line, "%S") then
                local ok, value = pcall(roto.json_decode, line, "ValidationException")
                if not ok then roto.error("ValidationException", "mapItemReaderData contains invalid JSONL") end
                table.insert(rows, value)
            end
        end
        items = roto.array(rows)
    else
        if config.ItemsPointer ~= nil then roto.error("ValidationException", "ItemsPointer requires JSON input") end
        items = test_csv(raw, config)
    end
    if not is_array(items) and not is_object(items) then roto.error("ValidationException", "ItemReader data must select a JSON array or object") end
    if config.MaxItems ~= nil and config.MaxItemsPath ~= nil then
        roto.error("ValidationException", "ItemReader cannot specify both MaxItems and MaxItemsPath")
    end
    return { items = items, source = kind, pointer = config.ItemsPointer,
        maxItems = config.MaxItems, maxItemsPath = config.MaxItemsPath }
end

-- Runs one state definition against an input, without creating an execution. Wait does not wait.
function M.TestState(ctx: Ctx, input: any, db: Db)
    if input.inspectionLevel ~= nil and input.inspectionLevel ~= "INFO" and input.inspectionLevel ~= "DEBUG" and input.inspectionLevel ~= "TRACE" then
        roto.error("ValidationException", "Invalid inspectionLevel")
    end
    valid_json(input.definition, "definition")
    local state = roto.json_decode(input.definition)
    local selected_name = "TestState"
    local in_map = false
    if input.stateName ~= nil then
        if type(input.stateName) ~= "string" or #input.stateName < 1 or #input.stateName > 80 then
            roto.error("ValidationException", "stateName must be between 1 and 80 characters")
        end
        if not is_object(state) or not is_object(state.States) or type(state.StartAt) ~= "string" then
            roto.error("ValidationException", "stateName requires a state machine definition")
        end
        selected_name = input.stateName
        local found: any = nil
        local matches = 0
        local function search(machine: any, map_parent: boolean, depth: number)
            if depth > 64 or not is_object(machine) or not is_object(machine.States) then return end
            for state_key, candidate in pairs(machine.States) do
                if is_object(candidate) then
                    if state_key == selected_name then found = candidate; in_map = map_parent; matches += 1 end
                    if candidate.Type == "Map" then
                        search(candidate.ItemProcessor or candidate.Iterator, true, depth + 1)
                    elseif candidate.Type == "Parallel" and is_array(candidate.Branches) then
                        for _, branch in ipairs(candidate.Branches) do search(branch, map_parent, depth + 1) end
                    end
                end
            end
        end
        search(state, false, 0)
        if matches == 0 then roto.error("ValidationException", "stateName was not found in the definition") end
        if matches > 1 then roto.error("ValidationException", "stateName is ambiguous in the definition") end
        state = found
    end
    if not is_object(state) or not STATE_TYPES[state.Type] then
        roto.error("ValidationException", "Invalid State Machine Definition: the state needs a known Type")
    end
    if input.context ~= nil and input.mock == nil then roto.error("ValidationException", "context requires a mock") end
    if input.mock == nil and state.Type == "Task" and type(state.Resource) == "string" and waits_for_token(state.Resource) then
        return { status = "FAILED", error = "States.Runtime", cause = "TestState cannot wait for callback, activity, or synchronous Tasks" }
    end
    local payload = input.input or "{}"
    valid_json(payload, "input")
    local data = roto.json_decode(payload)
    local fake: Doc = {
        executionArn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":execution:TestState:" .. roto.uuid(),
        stateMachineArn = "arn:aws:states:" .. ctx.region .. ":" .. ctx.account_id .. ":stateMachine:TestState",
        name = "TestState", roleArn = input.roleArn, startDate = os.time(),
    }
    local context = context_object(fake, selected_name, data)
    if in_map and input.context == nil then
        context.Map = { Item = { Index = 0, Value = data, Source = "STATE_DATA" } }
    end
    if input.context ~= nil then
        if type(input.context) ~= "string" or #input.context > MAX_PAYLOAD_BYTES then
            roto.error("ValidationException", "context must be a JSON object no larger than 256 KiB")
        end
        local valid, supplied = pcall(roto.json_decode, input.context)
        if not valid or not is_object(supplied) then
            roto.error("ValidationException", "context must be a JSON object")
        end
        -- Keep generated defaults for omitted context fields, while allowing callers to
        -- replace individual Execution, State, StateMachine, Task, and Map values.
        for key, value in pairs(supplied) do
            if is_object(value) and is_object(context[key]) then
                local merged = table.clone(context[key])
                for field, item in pairs(value) do merged[field] = item end
                context[key] = merged
            else
                context[key] = value
            end
        end
    end
    local scope: Scope = { data = data, context = context }
    if input.mock ~= nil then
        local inspection: Doc? = if input.inspectionLevel == "DEBUG" or input.inspectionLevel == "TRACE" then { input = payload } else nil
        local mock = roto.object(input.mock, "ValidationException", "mock")
        if state.Type ~= "Task" and state.Type ~= "Parallel" and state.Type ~= "Map" then
            roto.error("ValidationException", "Mock results require a Task, Parallel, or Map state")
        end
        if mock.fieldValidationMode ~= nil and mock.fieldValidationMode ~= "NONE" and mock.fieldValidationMode ~= "STRICT" and mock.fieldValidationMode ~= "PRESENT" then
            roto.error("ValidationException", "Invalid mock.fieldValidationMode")
        end
        if mock.result ~= nil and mock.errorOutput ~= nil then
            roto.error("ValidationException", "mock.result and mock.errorOutput are mutually exclusive")
        end
        local config = if input.stateConfiguration == nil then {} else roto.object(input.stateConfiguration, "ValidationException", "stateConfiguration")
        local distributed = state.Type == "Map" and is_object(state.ItemProcessor)
            and is_object(state.ItemProcessor.ProcessorConfig) and state.ItemProcessor.ProcessorConfig.Mode == "DISTRIBUTED"
        if config.errorCausedByState ~= nil then
            local source = config.errorCausedByState
            if (state.Type ~= "Map" and state.Type ~= "Parallel") or mock.errorOutput == nil
                or type(source) ~= "string" or #source < 1 or #source > 80 then
                roto.error("ValidationException", "errorCausedByState requires a mocked Map or Parallel error and a valid state name")
            end
            local matches = 0
            local function search_children(machine: any, depth: number)
                if depth > 64 or not is_object(machine) or not is_object(machine.States) then return end
                for child_name, child in pairs(machine.States) do
                    if is_object(child) then
                        if child_name == source then matches += 1 end
                        if child.Type == "Map" then search_children(child.ItemProcessor or child.Iterator, depth + 1)
                        elseif child.Type == "Parallel" and is_array(child.Branches) then
                            for _, branch in ipairs(child.Branches) do search_children(branch, depth + 1) end
                        end
                    end
                end
            end
            if state.Type == "Map" then search_children(state.ItemProcessor or state.Iterator, 0)
            elseif is_array(state.Branches) then
                for _, branch in ipairs(state.Branches) do search_children(branch, 0) end
            end
            if matches == 0 then roto.error("ValidationException", "errorCausedByState was not found inside the Map or Parallel state") end
            if matches > 1 then roto.error("ValidationException", "errorCausedByState is ambiguous") end
        elseif mock.errorOutput ~= nil and (state.Type == "Map" or state.Type == "Parallel") then
            roto.error("ValidationException", "A mocked Map or Parallel error requires errorCausedByState")
        end
        local reader: any = nil
        local map_prepared = false
        if config.mapItemReaderData ~= nil then
            if not distributed then roto.error("ValidationException", "mapItemReaderData requires a Distributed Map") end
            reader = test_reader(state, config.mapItemReaderData)
        end
        if config.mapIterationFailureCount ~= nil then
            local count = config.mapIterationFailureCount
            if not distributed or type(count) ~= "number" or count < 0 or count % 1 ~= 0 then
                roto.error("ValidationException", "mapIterationFailureCount requires a Distributed Map and a nonnegative integer")
            end
            local ok, spawns, _, item_count = pcall(spawn_map, state, data, scope, inspection, reader)
            if not ok then
                local code, cause = failure_of(spawns)
                return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
            end
            map_prepared = true
            if count > item_count then roto.error("ValidationException", "mapIterationFailureCount exceeds the item count") end
            local effective = apply_input_path(state, data, scope.context)
            local inner = { data = effective, context = scope.context }
            local limit_count = if state.ToleratedFailureCountPath ~= nil then resolve(state.ToleratedFailureCountPath, inner) else state.ToleratedFailureCount
            local limit_percent = if state.ToleratedFailurePercentagePath ~= nil then resolve(state.ToleratedFailurePercentagePath, inner) else state.ToleratedFailurePercentage
            if limit_count ~= nil and (type(limit_count) ~= "number" or limit_count < 0 or limit_count % 1 ~= 0) then
                roto.error("ValidationException", "ToleratedFailureCount must resolve to a nonnegative integer")
            end
            if limit_percent ~= nil and (type(limit_percent) ~= "number" or limit_percent < 0 or limit_percent > 100) then
                roto.error("ValidationException", "ToleratedFailurePercentage must resolve to a number between 0 and 100")
            end
            if inspection then inspection.toleratedFailureCount = limit_count; inspection.toleratedFailurePercentage = limit_percent end
            local exceeded = (limit_count ~= nil and count > limit_count)
                or (limit_percent ~= nil and item_count > 0 and count * 100 / item_count > limit_percent)
                or (limit_count == nil and limit_percent == nil and count > 0)
            if exceeded and mock.result ~= nil then
                if type(mock.result) ~= "string" or #mock.result > MAX_PAYLOAD_BYTES or not pcall(roto.json_decode, mock.result) then
                    roto.error("ValidationException", "mock.result must be valid JSON no larger than 256 KiB")
                end
                mock.result = nil
                mock.errorOutput = { error = "States.ExceedToleratedFailureThreshold", cause = "The Map state exceeded its tolerated failure threshold" }
            end
        end
        if mock.errorOutput ~= nil then
            local error_output = roto.object(mock.errorOutput, "ValidationException", "mock.errorOutput")
            local error_length = if type(error_output.error) == "string" then utf8.len(error_output.error) else nil
            local cause_length = if type(error_output.cause) == "string" then utf8.len(error_output.cause) else nil
            if error_length == nil or error_length > 256
                or (error_output.cause ~= nil and (cause_length == nil or cause_length > 32768)) then
                roto.error("ValidationException", "mock.errorOutput requires an error of at most 256 characters and an optional cause of at most 32768 characters")
            end
            for key in pairs(config) do
                if key ~= "retrierRetryCount" and key ~= "mapIterationFailureCount" and key ~= "mapItemReaderData" and key ~= "errorCausedByState" then roto.error("ValidationException", "Unsupported stateConfiguration field: " .. tostring(key)) end
            end
            local used = config.retrierRetryCount or 0
            if type(used) ~= "number" or used < 0 or used % 1 ~= 0 then
                roto.error("ValidationException", "retrierRetryCount must be a nonnegative integer")
            end
            local code: string = error_output.error
            local cause: string = error_output.cause or ""
            local prepared, preparation_error = pcall(function()
                if state.Type == "Parallel" then spawn_parallel(state, data, scope, inspection); return end
                if state.Type == "Map" then
                    if not map_prepared then spawn_map(state, data, scope, inspection, reader) end
                    return
                end
                local effective = apply_input_path(state, data, scope.context)
                if inspection then inspection.afterInputPath = roto.json_encode(effective) end
                local context = scope.context
                if type(state.Resource) == "string" and waits_for_token(state.Resource) then
                    context = table.clone(context)
                    if not is_object(context.Task) or context.Task.Token == nil then
                        local task: any = if is_object(context.Task) then table.clone(context.Task) else {}
                        task.Token = roto.uuid() .. roto.uuid()
                        context.Task = task
                    end
                end
                local parameters = if state.Parameters ~= nil then apply_template(state.Parameters, { data = effective, context = context }) else effective
                if inspection then inspection.afterParameters = roto.json_encode(parameters) end
            end)
            if not prepared then
                local prepare_code, prepare_cause = failure_of(preparation_error)
                return { status = "FAILED", error = prepare_code, cause = prepare_cause, inspectionData = inspection }
            end
            for index, rule in ipairs(state.Retry or {}) do
                if error_matches(rule.ErrorEquals, code) then
                    if used < (rule.MaxAttempts or 3) then
                        local delay = (rule.IntervalSeconds or 1) * (rule.BackoffRate or 2) ^ used
                        if rule.MaxDelaySeconds ~= nil then delay = math.min(delay, rule.MaxDelaySeconds) end
                        if rule.JitterStrategy == "FULL" then delay *= math.random() end
                        if inspection then inspection.errorDetails = { retryIndex = index - 1, retryBackoffIntervalSeconds = delay } end
                        return { status = "RETRIABLE", error = code, cause = cause, inspectionData = inspection }
                    end
                    break
                end
            end
            for index, catcher in ipairs(state.Catch or {}) do
                if error_matches(catcher.ErrorEquals, code) then
                    local ok, caught = pcall(apply_result_path, catcher, data, { Error = code, Cause = cause })
                    if not ok then
                        local path_code, path_cause = failure_of(caught)
                        return { status = "FAILED", error = path_code, cause = path_cause, inspectionData = inspection }
                    end
                    if inspection then inspection.errorDetails = { catchIndex = index - 1 } end
                    return { status = "CAUGHT_ERROR", error = code, cause = cause, nextState = catcher.Next,
                        output = roto.json_encode(caught), inspectionData = inspection }
                end
            end
            return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
        end
        if type(mock.result) ~= "string" or #mock.result > MAX_PAYLOAD_BYTES then
            roto.error("ValidationException", "mock.result must be a JSON string no larger than 256 KiB")
        end
        local decoded, mock_result = pcall(roto.json_decode, mock.result)
        if not decoded then roto.error("ValidationException", "mock.result must be valid JSON") end
        if state.Type == "Task" and mock.fieldValidationMode ~= "NONE" then
            local target_ok, service, operation = pcall(task_target, state.Resource)
            if not target_ok then roto.error("ValidationException", "Cannot resolve the mocked Task resource") end
            if service == "activity" then
                roto.error("ValidationException", "Response model validation is unavailable for activity tasks; use fieldValidationMode NONE")
            end
            -- Direct Lambda ARNs return the function's arbitrary JSON payload rather than
            -- the Invoke response envelope, so there is no service response shape to check.
            if not (service == "lambda" and operation ~= "") then
                if service == "lambda" then operation = "Invoke" end
                -- The synchronous ECS integration returns the final DescribeTasks response.
                if service == "ecs" and operation == "RunTask" and sync_mode(state.Resource) ~= nil then
                    operation = "DescribeTasks"
                end
                if service == "states" and operation == "StartExecution" and sync_mode(state.Resource) ~= nil then
                    operation = if sync_mode(state.Resource) == "v2" then "DescribeExecutionSync2" else "DescribeExecution"
                end
                local validation_error = roto.validate_mock_result(service, operation, mock_result, mock.fieldValidationMode ~= "PRESENT")
                if validation_error ~= nil then roto.error("ValidationException", validation_error) end
            end
        end
        if state.Type == "Parallel" or state.Type == "Map" then
            local exported = distributed and is_object(state.ResultWriter) and state.ResultWriter.Resource ~= nil
            if not is_array(mock_result) and not (exported and is_object(mock_result)) then
                roto.error("ValidationException", "mock.result must be a JSON array for an inline Map or Parallel state")
            end
            if exported and not is_object(mock_result) then
                roto.error("ValidationException", "mock.result must be a JSON object for an exporting ResultWriter")
            end
            if state.Type == "Parallel" and (not is_array(state.Branches) or #mock_result ~= #state.Branches) then
                roto.error("ValidationException", "mock.result must contain one element for each Parallel branch")
            end
            local ok, result = pcall(function()
                if state.Type == "Parallel" then spawn_parallel(state, data, scope, inspection)
                elseif not map_prepared then spawn_map(state, data, scope, inspection, reader) end
                return finish_fanout(state, data, scope, mock_result, inspection)
            end)
            if not ok then
                local code, cause = failure_of(result)
                return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
            end
            return { status = "SUCCEEDED", output = roto.json_encode(result), nextState = state.Next, inspectionData = inspection }
        end
        local ok, result = pcall(function()
            local effective = apply_input_path(state, data, scope.context)
            if inspection then inspection.afterInputPath = roto.json_encode(effective) end
            local context = scope.context
            if type(state.Resource) == "string" and waits_for_token(state.Resource) then
                context = table.clone(context)
                if not is_object(context.Task) or context.Task.Token == nil then
                    local task: any = if is_object(context.Task) then table.clone(context.Task) else {}
                    task.Token = roto.uuid() .. roto.uuid()
                    context.Task = task
                end
            end
            local parameters = if state.Parameters ~= nil then apply_template(state.Parameters, { data = effective, context = context }) else effective
            if inspection then inspection.afterParameters = roto.json_encode(parameters) end
            return finish_task(state, data, { data = data, context = context }, mock_result, inspection)
        end)
        if not ok then
            local code, cause = failure_of(result)
            return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
        end
        return { status = "SUCCEEDED", output = roto.json_encode(result), nextState = state.Next, inspectionData = inspection }
    end
    local inspection: Doc? = if input.inspectionLevel == "DEBUG" or input.inspectionLevel == "TRACE" then { input = payload } else nil
    local previous = active
    active = { ctx = ctx, db = db }
    local ok, outcome = pcall(evaluate_state, selected_name, state, data, scope, { events = {}, count = 0 }, fake.executionArn, inspection)
    active = previous
    if not ok then
        local code, cause = failure_of(outcome)
        return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
    end
    local result: Outcome = outcome :: Outcome
    if result.spawn ~= nil then
        local result_values = {}
        for index, spawn in ipairs(result.spawn) do
            local child_def: Doc
            if state.Type == "Parallel" then child_def = state.Branches[index]
            else child_def = state.ItemProcessor or state.Iterator end
            previous = active
            active = { ctx = ctx, db = db }
            local branch_ok, value = pcall(test_branch, child_def, spawn.data, fake, { events = {}, count = 0 }, spawn.map)
            active = previous
            if not branch_ok then
                local code, cause = failure_of(value)
                return { status = "FAILED", error = code, cause = cause, inspectionData = inspection }
            end
            table.insert(result_values, value)
        end
        result.output = finish_fanout(state, data, scope, result_values, inspection)
    elseif result.waiting ~= nil then
        return { status = "FAILED", error = "States.Runtime", cause = "TestState cannot wait for callback or activity Tasks" }
    end
    if state.Type == "Fail" then
        return { status = "FAILED", error = result.error, cause = result.cause, inspectionData = inspection }
    end
    return { status = "SUCCEEDED", output = roto.json_encode(result.output), nextState = result.next, inspectionData = inspection }
end

function M.GetExecutionHistory(ctx: Ctx, input: any, db: Db)
    validate_page_size(input.maxResults)
    for _, field in ipairs({ "reverseOrder", "includeExecutionData" }) do
        if input[field] ~= nil and type(input[field]) ~= "boolean" then
            roto.error("ValidationException", field .. " must be a boolean")
        end
    end
    local doc = execution(ctx, db, input.executionArn)
    local snapshot = doc.stateMachineSnapshot :: Doc?
    if snapshot and snapshot.type == "EXPRESS" then
        roto.error("ValidationException", "GetExecutionHistory is not supported for EXPRESS state machines")
    end
    local events: { any } = db.list(history_space(ctx, doc.executionArn))
    if input.includeExecutionData == false then
        -- Filter a response copy: stored history remains complete for later calls that include data.
        local filtered: { any } = {}
        for _, event in ipairs(events) do
            local item = copy(event)
            for key, details in pairs(item) do
                if string.match(key, "EventDetails$") and type(details) == "table" then
                    details.input = nil
                    details.output = nil
                    -- TaskScheduled calls the task input `parameters` in its history shape.
                    details.parameters = nil
                end
            end
            table.insert(filtered, item)
        end
        events = filtered
    end
    if input.reverseOrder == true then
        local reversed: { any } = {}
        for i = #events, 1, -1 do table.insert(reversed, events[i]) end
        events = reversed
    end
    -- AWS tokens are opaque, expire after 24 hours, and are valid only with the same request
    -- arguments. Keep the paginator's numeric offset internal to this operation.
    local request: Doc = { executionArn = input.executionArn }
    for _, key in ipairs({ "maxResults", "reverseOrder", "includeExecutionData" }) do
        if input[key] ~= nil then request[key] = input[key] end
    end
    local fingerprint = roto.json_encode(request)
    local token_space = ns(ctx, "history-token")
    local now = roto.now()
    for _, old_token in ipairs(db.list(token_space) :: { Doc }) do
        if old_token.expiresAt <= now then db.delete(token_space, old_token.id) end
    end
    local offset = "0"
    if input.nextToken ~= nil then
        if type(input.nextToken) ~= "string" then roto.error("InvalidToken", "Invalid NextToken") end
        local token = db.get(token_space, input.nextToken) :: Doc?
        if not token or token.expiresAt <= now or token.request ~= fingerprint then
            roto.error("InvalidToken", "Invalid NextToken")
        end
        offset = token.offset
    end
    -- Step Functions accepts zero as the default page size for this operation.
    local max_results = if input.maxResults == 0 then nil else input.maxResults
    local out = roto.page({ MaxResults = max_results, NextToken = offset }, events, "events", 100, 1000, "InvalidToken")
    if out.NextToken ~= nil then
        local next_token = roto.uuid() .. roto.uuid()
        db.put(token_space, next_token, {
            id = next_token, executionArn = doc.executionArn,
            offset = out.NextToken, request = fingerprint, expiresAt = now + 86400,
        })
        out.nextToken = next_token
    end
    out.NextToken = nil
    return out
end

function M.ListExecutions(ctx: Ctx, input: any, db: Db)
    if (input.stateMachineArn == nil) == (input.mapRunArn == nil) then
        roto.error("ValidationException", "Specify exactly one of stateMachineArn or mapRunArn")
    end
    local valid_status = { RUNNING = true, SUCCEEDED = true, FAILED = true, TIMED_OUT = true, ABORTED = true, PENDING_REDRIVE = true }
    if input.statusFilter ~= nil and (type(input.statusFilter) ~= "string" or not valid_status[input.statusFilter]) then
        roto.error("ValidationException", "Invalid statusFilter")
    end
    if input.redriveFilter ~= nil and input.redriveFilter ~= "REDRIVEN" and input.redriveFilter ~= "NOT_REDRIVEN" then
        roto.error("ValidationException", "Invalid redriveFilter")
    end
    if input.mapRunArn ~= nil then
        if type(input.mapRunArn) ~= "string" or not string.match(input.mapRunArn, "^arn:[^:]+:states:[^:]+:[^:]+:mapRun:.+") then
            roto.error("InvalidArn", "Invalid Map Run ARN")
        end
        -- Distributed Map is not implemented, so there are no Map Runs to list.
        roto.error("ResourceNotFound", "Map Run not found", 400)
    end
    if input.redriveFilter ~= nil or input.statusFilter == "PENDING_REDRIVE" then
        roto.error("ValidationException", "redriveFilter and PENDING_REDRIVE require mapRunArn")
    end
    local arn = roto.required(input, "stateMachineArn", "InvalidArn")
    local requested_version = db.get(ns(ctx, "version"), arn) :: Doc?
    local requested_alias = db.get(ns(ctx, "alias"), arn) :: Doc?
    local sm = if requested_version then machine(ctx, db, requested_version.stateMachineArn)
        elseif requested_alias then machine(ctx, db, requested_alias.stateMachineArn)
        else machine(ctx, db, arn)
    if sm.type == "EXPRESS" then roto.error("StateMachineTypeNotSupported", "ListExecutions is not supported for EXPRESS state machines") end
    local rows: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "execution"))) do
        local matches_qualified = (not requested_version or doc.stateMachineVersionArn == requested_version.stateMachineVersionArn)
            and (not requested_alias or doc.stateMachineAliasArn == requested_alias.stateMachineAliasArn)
        if doc.stateMachineArn == sm.stateMachineArn and matches_qualified and (input.statusFilter == nil or doc.status == input.statusFilter) then
            table.insert(rows, {
                executionArn = doc.executionArn, stateMachineArn = doc.stateMachineArn, name = doc.name,
                stateMachineVersionArn = doc.stateMachineVersionArn,
                stateMachineAliasArn = doc.stateMachineAliasArn,
                status = doc.status, startDate = doc.startDate, stopDate = doc.stopDate,
            })
        end
    end
    table.sort(rows, function(a: any, b: any)
        local a_time = if a.status == "RUNNING" then a.startDate else a.stopDate or a.startDate
        local b_time = if b.status == "RUNNING" then b.startDate else b.stopDate or b.startDate
        if a_time ~= b_time then return a_time > b_time end
        return a.executionArn > b.executionArn
    end)
    return states_page(ctx, db, input, rows, "executions")
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
