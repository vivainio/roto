--!strict
-- Shared helpers for trusted Luau service modules. No durable module state.
-- Signatures are declared in luau_service_stubs.d.luau; the annotations here must match them.
function roto.required(input: any, key: string, code: string?): string
    local value = input[key]
    if type(value) ~= "string" or not string.match(value, "%S") then
        roto.error(code or "InvalidInput", key .. " must be a nonempty string")
    end
    return value
end
function roto.object(value: any, code: string?, label: string?): { [string]: any }
    if type(value) ~= "table" or getmetatable(value) == getmetatable(roto.array()) then
        roto.error(code or "InvalidInput", (label or "Value") .. " must be an object")
    end
    return value
end
function roto.list(value: any, code: string?, label: string?): { any }
    if type(value) ~= "table" or getmetatable(value) ~= getmetatable(roto.array()) then
        roto.error(code or "InvalidInput", (label or "Value") .. " must be a list")
    end
    return value
end
function roto.scope(ctx: Ctx, collection: string, global: boolean?): string
    return ctx.account_id .. "/" .. (global and "" or ctx.region .. "/") .. collection
end
function roto.contains(values: { any }?, target: any): boolean
    for _, value in ipairs(values or {}) do if value == target then return true end end
    return false
end
function roto.page(input: any, values: { any }, key: string, default: number?, cap: number?, code: string?): { [string]: any }
    local limit: any = input.MaxResults or input.Limit or default or 100
    local max: number = cap or 100
    code = code or "InvalidInput"
    if type(limit) ~= "number" or limit % 1 ~= 0 or limit < 1 or limit > max then
        roto.error(code, "Page size must be between 1 and " .. max)
    end
    local offset: number = 0
    if input.NextToken ~= nil then
        if type(input.NextToken) ~= "string" or not string.match(input.NextToken, "^%d+$") then
            roto.error(code, "Invalid NextToken")
        end
        offset = tonumber(input.NextToken) :: number
        if offset > #values then roto.error(code, "Invalid NextToken") end
    end
    local result: { [string]: any } = {[key]=roto.array()}
    for i = offset+1, math.min(offset+limit,#values) do table.insert(result[key],values[i]) end
    if offset+limit < #values then result.NextToken = tostring(offset+limit) end
    return result
end
function roto.tags(existing: { { Key: string, Value: string } }?, incoming: any, code: string?): { { Key: string, Value: string } }
    local tags: { [string]: string } = {}
    for _, tag in ipairs(existing or ({} :: { { Key: string, Value: string } })) do tags[tag.Key] = tag.Value end
    for _, tag in ipairs(roto.list(incoming,code,"Tags")) do
        roto.object(tag,code,"Tag")
        local key = roto.required(tag,"Key",code)
        if type(tag.Value) ~= "string" then roto.error(code or "InvalidInput", "Tag Value must be a string") end
        tags[key] = tag.Value
    end
    local keys: { string } = {}
    for key in pairs(tags) do table.insert(keys,key) end
    table.sort(keys)
    local result: { { Key: string, Value: string } } = roto.array()
    for _, key in ipairs(keys) do table.insert(result,{Key=key,Value=tags[key]}) end
    return result
end
function roto.untag(existing: { { Key: string, Value: string } }?, keys: any, code: string?): { { Key: string, Value: string } }
    local names = roto.list(keys,code,"Tag keys")
    for _, key in ipairs(names) do if type(key) ~= "string" then roto.error(code or "InvalidInput","Tag key must be a string") end end
    local result: { { Key: string, Value: string } } = roto.array()
    for _, tag in ipairs(existing or ({} :: { { Key: string, Value: string } })) do if not roto.contains(names,tag.Key) then table.insert(result,tag) end end
    return result
end
function roto.copy_fields(source: any, destination: any, fields: { string }): any
    for _, key in ipairs(fields) do if source[key] ~= nil then destination[key] = source[key] end end
    return destination
end
