-- Shared helpers for trusted Lua service modules. No durable module state.
function roto.required(input, key, code)
    local value = input[key]
    if type(value) ~= "string" or not value:match("%S") then
        roto.error(code or "InvalidInput", key .. " must be a nonempty string")
    end
    return value
end
function roto.object(value, code, label)
    if type(value) ~= "table" or getmetatable(value) == getmetatable(roto.array()) then
        roto.error(code or "InvalidInput", (label or "Value") .. " must be an object")
    end
    return value
end
function roto.list(value, code, label)
    if type(value) ~= "table" or getmetatable(value) ~= getmetatable(roto.array()) then
        roto.error(code or "InvalidInput", (label or "Value") .. " must be a list")
    end
    return value
end
function roto.scope(ctx, collection, global)
    return ctx.account_id .. "/" .. (global and "" or ctx.region .. "/") .. collection
end
function roto.contains(values, target)
    for _, value in ipairs(values or {}) do if value == target then return true end end
    return false
end
function roto.page(input, values, key, default, cap, code)
    local limit = input.MaxResults or input.Limit or default or 100
    cap = cap or 100
    code = code or "InvalidInput"
    if type(limit) ~= "number" or limit % 1 ~= 0 or limit < 1 or limit > cap then
        roto.error(code, "Page size must be between 1 and " .. cap)
    end
    local offset = 0
    if input.NextToken ~= nil then
        if type(input.NextToken) ~= "string" or not input.NextToken:match("^%d+$") then
            roto.error(code, "Invalid NextToken")
        end
        offset = tonumber(input.NextToken)
        if offset > #values then roto.error(code, "Invalid NextToken") end
    end
    local result = {[key]=roto.array()}
    for i = offset+1, math.min(offset+limit,#values) do table.insert(result[key],values[i]) end
    if offset+limit < #values then result.NextToken = tostring(offset+limit) end
    return result
end
function roto.tags(existing, incoming, code)
    local tags = {}
    for _, tag in ipairs(existing or {}) do tags[tag.Key] = tag.Value end
    for _, tag in ipairs(roto.list(incoming,code,"Tags")) do
        roto.object(tag,code,"Tag")
        local key = roto.required(tag,"Key",code)
        if type(tag.Value) ~= "string" then roto.error(code or "InvalidInput", "Tag Value must be a string") end
        tags[key] = tag.Value
    end
    local keys = {}
    for key in pairs(tags) do table.insert(keys,key) end
    table.sort(keys)
    local result = roto.array()
    for _, key in ipairs(keys) do table.insert(result,{Key=key,Value=tags[key]}) end
    return result
end
function roto.untag(existing, keys, code)
    roto.list(keys,code,"Tag keys")
    for _, key in ipairs(keys) do if type(key) ~= "string" then roto.error(code or "InvalidInput","Tag key must be a string") end end
    local result = roto.array()
    for _, tag in ipairs(existing or {}) do if not roto.contains(keys,tag.Key) then table.insert(result,tag) end end
    return result
end
function roto.copy_fields(source, destination, fields)
    for _, key in ipairs(fields) do if source[key] ~= nil then destination[key] = source[key] end end
    return destination
end
