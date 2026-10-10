-- Trusted service module. Account membership is global, never partitioned by region.
local ops = {}
local function required(input, key)
    local value = input[key]
    if type(value) ~= "string" or value == "" then
        roto.error("InvalidInputException", key .. " must be a nonempty string")
    end
    return value
end
local function organization(ctx, db)
    local membership = db.get("memberships", ctx.account_id)
    local org = membership and db.get("organizations", membership.OrganizationId)
    if not org then roto.error("AWSOrganizationsNotInUseException", "Account is not in an organization") end
    return org
end
local function management(ctx, db)
    local org = organization(ctx, db)
    if org.MasterAccountId ~= ctx.account_id then
        roto.error("AccessDeniedException", "Operation requires the management account")
    end
    return org
end
local function namespace(org, collection) return org.Id .. "/" .. collection end
local function arn(org, kind, id)
    return "arn:aws:organizations::" .. org.MasterAccountId .. ":" .. kind .. "/" .. org.Id .. "/" .. id
end
local function page(input, values, key)
    local limit = input.MaxResults or 20
    if type(limit) ~= "number" or limit % 1 ~= 0 or limit < 1 or limit > 20 then
        roto.error("InvalidInputException", "MaxResults must be between 1 and 20")
    end
    local offset = 0
    if input.NextToken ~= nil then
        if type(input.NextToken) ~= "string" or not input.NextToken:match("^offset:%d+$") then
            roto.error("InvalidInputException", "Invalid NextToken")
        end
        offset = tonumber(input.NextToken:sub(8))
        if offset > #values then roto.error("InvalidInputException", "Invalid NextToken") end
    end
    local result = {[key] = roto.array()}
    for i = offset + 1, math.min(offset + limit, #values) do
        table.insert(result[key], values[i])
    end
    if offset + limit < #values then result.NextToken = "offset:" .. (offset + limit) end
    return result
end
local function parent(org, db, id)
    local value = db.get(namespace(org, "roots"), id) or db.get(namespace(org, "ous"), id)
    if not value then roto.error("ParentNotFoundException", "Unknown parent " .. id) end
    return value
end
local function ou(org, db, id)
    local value = db.get(namespace(org, "ous"), id)
    if not value then roto.error("OrganizationalUnitNotFoundException", "Unknown OU " .. id) end
    return value
end
local function public_ou(value) return {Id=value.Id, Arn=value.Arn, Name=value.Name} end
local function resource(org, db, id)
    local value = db.get(namespace(org, "roots"), id)
        or db.get(namespace(org, "ous"), id) or db.get(namespace(org, "accounts"), id)
    if not value then roto.error("TargetNotFoundException", "Unknown resource " .. id) end
    return value
end
local function set_tags(org, db, id, tags)
    local existing = db.get(namespace(org, "tags"), id) or {}
    if type(tags) ~= "table" then roto.error("InvalidInputException", "Tags must be a list") end
    for _, tag in ipairs(tags) do
        local key = required(tag, "Key")
        if type(tag.Value) ~= "string" then roto.error("InvalidInputException", "Tag Value must be a string") end
        existing[key] = tag.Value
    end
    db.put(namespace(org, "tags"), id, existing)
end

function ops.CreateOrganization(ctx, input, db)
    if db.get("memberships", ctx.account_id) then
        roto.error("AlreadyInOrganizationException", "Account already belongs to an organization")
    end
    local feature = input.FeatureSet or "ALL"
    if feature ~= "ALL" and feature ~= "CONSOLIDATED_BILLING" then
        roto.error("InvalidInputException", "Invalid FeatureSet")
    end
    local id = roto.id("o-")
    local org = {Id=id, Arn="arn:aws:organizations::" .. ctx.account_id .. ":organization/" .. id,
        FeatureSet=feature, MasterAccountId=ctx.account_id,
        MasterAccountArn="arn:aws:organizations::" .. ctx.account_id .. ":account/" .. id .. "/" .. ctx.account_id,
        MasterAccountEmail=ctx.account_id .. "@example.com", AvailablePolicyTypes=roto.array()}
    local policy_types = roto.array()
    if feature == "ALL" then
        table.insert(policy_types, {Type="SERVICE_CONTROL_POLICY", Status="ENABLED"})
        table.insert(org.AvailablePolicyTypes, {Type="SERVICE_CONTROL_POLICY", Status="ENABLED"})
    end
    db.put("organizations", id, org)
    db.put("memberships", ctx.account_id, {OrganizationId=id})
    local root = roto.id("r-")
    db.put(namespace(org, "roots"), root, {Id=root, Arn=arn(org,"root",root), Name="Root", PolicyTypes=policy_types})
    db.put(namespace(org, "accounts"), ctx.account_id, {Id=ctx.account_id, Arn=org.MasterAccountArn,
        Email=org.MasterAccountEmail, Name="Management account", Status="ACTIVE", State="ACTIVE",
        JoinedMethod="INVITED", JoinedTimestamp=0, ParentId=root})
    return {Organization=org}
end
function ops.DescribeOrganization(ctx, _, db) return {Organization=organization(ctx,db)} end
function ops.ListRoots(ctx, input, db)
    return page(input, db.list(namespace(management(ctx,db), "roots")), "Roots")
end
function ops.DeleteOrganization(ctx, _, db)
    local org = management(ctx,db)
    if #db.list(namespace(org,"accounts")) > 1 then
        roto.error("OrganizationNotEmptyException", "Remove member accounts first")
    end
    for _, collection in ipairs({"roots","ous","accounts","tags"}) do
        -- Delete by stored IDs; tag documents are removed alongside their resources below.
        if collection ~= "tags" then
            for _, value in ipairs(db.list(namespace(org,collection))) do
                db.delete(namespace(org,"tags"), value.Id)
                db.delete(namespace(org,collection), value.Id)
            end
        end
    end
    db.delete("memberships", ctx.account_id)
    db.delete("organizations", org.Id)
    return {}
end
function ops.CreateOrganizationalUnit(ctx, input, db)
    local org = management(ctx,db)
    local name, parent_id = required(input,"Name"), required(input,"ParentId")
    parent(org,db,parent_id)
    for _, value in ipairs(db.list(namespace(org,"ous"))) do
        if value.ParentId == parent_id and value.Name == name then
            roto.error("DuplicateOrganizationalUnitException", "OU name already exists under this parent")
        end
    end
    local root_segment = parent_id:match("^r%-(.+)$") or parent_id:match("^ou%-([^-]+)%-")
    local id = roto.id("ou-" .. root_segment .. "-")
    local value = {Id=id, Arn=arn(org,"ou",id), Name=name, ParentId=parent_id}
    db.put(namespace(org,"ous"), id, value)
    if input.Tags then set_tags(org,db,id,input.Tags) end
    return {OrganizationalUnit=public_ou(value)}
end
function ops.DescribeOrganizationalUnit(ctx, input, db)
    return {OrganizationalUnit=public_ou(ou(management(ctx,db),db,required(input,"OrganizationalUnitId")))}
end
function ops.UpdateOrganizationalUnit(ctx, input, db)
    local org = management(ctx,db)
    local value = ou(org,db,required(input,"OrganizationalUnitId"))
    local name = required(input,"Name")
    for _, other in ipairs(db.list(namespace(org,"ous"))) do
        if other.Id ~= value.Id and other.ParentId == value.ParentId and other.Name == name then
            roto.error("DuplicateOrganizationalUnitException", "OU name already exists under this parent")
        end
    end
    value.Name = name
    db.put(namespace(org,"ous"), value.Id, value)
    return {OrganizationalUnit=public_ou(value)}
end
function ops.DeleteOrganizationalUnit(ctx, input, db)
    local org = management(ctx,db)
    local value = ou(org,db,required(input,"OrganizationalUnitId"))
    for _, collection in ipairs({"ous","accounts"}) do
        for _, child in ipairs(db.list(namespace(org,collection))) do
            if child.ParentId == value.Id then roto.error("OrganizationalUnitNotEmptyException", "OU has children") end
        end
    end
    db.delete(namespace(org,"tags"), value.Id)
    db.delete(namespace(org,"ous"), value.Id)
    return {}
end
function ops.ListOrganizationalUnitsForParent(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ParentId")
    parent(org,db,id)
    local values = roto.array()
    for _, value in ipairs(db.list(namespace(org,"ous"))) do
        if value.ParentId == id then table.insert(values,public_ou(value)) end
    end
    return page(input,values,"OrganizationalUnits")
end
local function public_account(value)
    local result = {}
    for key, item in pairs(value) do if key ~= "ParentId" then result[key] = item end end
    return result
end
function ops.ListAccounts(ctx, input, db)
    local org = management(ctx,db)
    local values = roto.array()
    for _, value in ipairs(db.list(namespace(org,"accounts"))) do table.insert(values,public_account(value)) end
    return page(input,values,"Accounts")
end
function ops.DescribeAccount(ctx, input, db)
    local org = management(ctx,db)
    local value = db.get(namespace(org,"accounts"),required(input,"AccountId"))
    if not value then roto.error("AccountNotFoundException", "Unknown account") end
    return {Account=public_account(value)}
end
function ops.ListAccountsForParent(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ParentId")
    parent(org,db,id)
    local values = roto.array()
    for _, value in ipairs(db.list(namespace(org,"accounts"))) do
        if value.ParentId == id then table.insert(values,public_account(value)) end
    end
    return page(input,values,"Accounts")
end
function ops.ListParents(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ChildId")
    local value = db.get(namespace(org,"ous"),id) or db.get(namespace(org,"accounts"),id)
    if not value then roto.error("ChildNotFoundException", "Unknown child") end
    local values = roto.array()
    table.insert(values, {Id=value.ParentId, Type=value.ParentId:sub(1,2)=="r-" and "ROOT" or "ORGANIZATIONAL_UNIT"})
    return page(input,values,"Parents")
end
function ops.TagResource(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ResourceId")
    resource(org,db,id)
    if input.Tags == nil then roto.error("InvalidInputException", "Tags is required") end
    set_tags(org,db,id,input.Tags)
    return {}
end
function ops.UntagResource(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ResourceId")
    resource(org,db,id)
    if type(input.TagKeys) ~= "table" then roto.error("InvalidInputException", "TagKeys is required") end
    local tags = db.get(namespace(org,"tags"),id) or {}
    for _, key in ipairs(input.TagKeys) do
        if type(key) ~= "string" then roto.error("InvalidInputException", "TagKeys must contain strings") end
        tags[key] = nil
    end
    db.put(namespace(org,"tags"),id,tags)
    return {}
end
function ops.ListTagsForResource(ctx, input, db)
    local org = management(ctx,db)
    local id = required(input,"ResourceId")
    resource(org,db,id)
    local values = roto.array()
    for key, value in pairs(db.get(namespace(org,"tags"),id) or {}) do table.insert(values,{Key=key,Value=value}) end
    table.sort(values,function(a,b) return a.Key < b.Key end)
    return page(input,values,"Tags")
end
return ops
