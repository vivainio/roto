--!strict
-- Amazon EC2 (protocol `ec2`, API 2016-11-15): instances, security groups and tags. Nothing runs;
-- instances only change state. Operations: RunInstances, DescribeInstances, DescribeInstanceStatus,
-- Start/Stop/Reboot/TerminateInstances, Create/Delete/DescribeSecurityGroups and
-- Create/Delete/DescribeTags. Other operations answer InvalidAction.
--
-- Lifecycle. Run and Start report `pending`, Stop reports `stopping`, Terminate reports
-- `shutting-down`; the next request that reads the instance settles it as `running`, `stopped` or
-- `terminated`. RotoSetInstanceState (a Roto extension) forces a state and holds it, so a fixture
-- can keep an instance in a transitional state or put one straight into `stopped`. A terminated
-- instance stays visible for an hour, as in AWS.
--
-- Document namespaces (per account and region): ec2/instance, ec2/sg, ec2/tags (keyed by resource
-- id), ec2/meta (address counter).
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local CODES: { [string]: number } = {
    pending = 0, running = 16, ["shutting-down"] = 32, terminated = 48, stopping = 64, stopped = 80,
}
local SETTLES: { [string]: string } = { pending = "running", stopping = "stopped", ["shutting-down"] = "terminated" }
local TERMINATED_VISIBLE_SECONDS = 3600

local function items(value: any): { any }
    return if type(value) == "table" then value :: { any } else {}
end

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "ec2/" .. kind)
end

local function hex(length: number): string
    return string.sub(roto.sha256(roto.uuid()), 1, length)
end

local function public(doc: Doc): Doc
    local out: Doc = {}
    for key, value in pairs(doc) do
        if string.sub(tostring(key), 1, 1) ~= "_" then out[key] = value end
    end
    return out
end

local function check_dry_run(input: any)
    if input.DryRun == true then
        roto.error("DryRunOperation", "Request would have succeeded, but DryRun flag is set.", 412)
    end
end

local function state(name: string): Doc
    return { Code = CODES[name], Name = name }
end

local function glob_match(value: string, pattern: string): boolean
    local escaped = string.gsub(pattern, "[%^%$%(%)%%%.%[%]%+%-]", "%%%0")
    escaped = string.gsub(escaped, "%*", ".*")
    escaped = string.gsub(escaped, "%?", ".")
    return string.match(value, "^" .. escaped .. "$") ~= nil
end

local function tags_of(ctx: Ctx, db: Db, id: string): { any }
    local doc = db.get(ns(ctx, "tags"), id) :: Doc?
    return if doc then doc.Tags else roto.array()
end

local function add_tags(ctx: Ctx, db: Db, id: string, incoming: any)
    local doc: Doc = (db.get(ns(ctx, "tags"), id) :: Doc?) or { ResourceId = id, Tags = roto.array() }
    local merged = roto.array()
    for _, tag in ipairs(doc.Tags) do
        local replaced = false
        for _, new in ipairs(incoming) do if new.Key == tag.Key then replaced = true end end
        if not replaced then table.insert(merged, tag) end
    end
    for _, new in ipairs(incoming) do table.insert(merged, { Key = new.Key, Value = new.Value or "" }) end
    doc.Tags = merged
    db.put(ns(ctx, "tags"), id, doc)
end

local function spec_tags(specs: any, resource_type: string): { any }
    local tags = roto.array()
    for _, spec in ipairs(items(specs)) do
        if spec.ResourceType == resource_type then
            for _, tag in ipairs(items(spec.Tags)) do table.insert(tags, tag) end
        end
    end
    return tags
end

local function resource_type(id: string): string
    local prefix = string.match(id, "^(%a+)%-") or ""
    local types: { [string]: string } = {
        i = "instance", sg = "security-group", vpc = "vpc", subnet = "subnet", ami = "image", vol = "volume",
        eni = "network-interface", snap = "snapshot", r = "reservation",
    }
    return types[prefix] or prefix
end

-- Filters ----------------------------------------------------------------------------------------

local function check_filters(filters: any, known: { [string]: (Doc, { any }) -> { string } }): { any }
    local checked: { any } = {}
    for _, filter in ipairs(items(filters)) do
        local name = filter.Name
        if type(name) ~= "string" or not (known[name] or string.match(name, "^tag:.+")) then
            roto.error("InvalidParameterValue", "The filter '" .. tostring(name) .. "' is invalid")
        end
        table.insert(checked, filter)
    end
    return checked
end

local function matches(doc: Doc, tags: { any }, filters: { any }, known: { [string]: (Doc, { any }) -> { string } }): boolean
    for _, filter in ipairs(filters) do
        local candidates: { string } = {}
        local key = string.match(filter.Name, "^tag:(.+)")
        if key then
            for _, tag in ipairs(tags) do if tag.Key == key then table.insert(candidates, tag.Value) end end
        else
            candidates = known[filter.Name](doc, tags)
        end
        local hit = false
        for _, candidate in ipairs(candidates) do
            for _, pattern in ipairs(items(filter.Values)) do
                if glob_match(candidate, pattern) then hit = true end
            end
        end
        if not hit then return false end
    end
    return true
end

local function tag_keys(_doc: Doc, tags: { any }): { string }
    local out: { string } = {}
    for _, tag in ipairs(tags) do table.insert(out, tag.Key) end
    return out
end

local function tag_values(_doc: Doc, tags: { any }): { string }
    local out: { string } = {}
    for _, tag in ipairs(tags) do table.insert(out, tag.Value) end
    return out
end

local function one(field: string): (Doc, { any }) -> { string }
    return function(doc: Doc, _tags: { any }): { string }
        local value = doc[field]
        return if value == nil then {} else { tostring(value) }
    end
end

local INSTANCE_FILTERS: { [string]: (Doc, { any }) -> { string } } = {
    ["instance-id"] = one("InstanceId"),
    ["instance-type"] = one("InstanceType"),
    ["image-id"] = one("ImageId"),
    ["key-name"] = one("KeyName"),
    ["subnet-id"] = one("SubnetId"),
    ["vpc-id"] = one("VpcId"),
    ["private-ip-address"] = one("PrivateIpAddress"),
    ["ip-address"] = one("PublicIpAddress"),
    ["reservation-id"] = one("_ReservationId"),
    ["instance-state-name"] = function(doc) return { doc.State.Name } end,
    ["instance-state-code"] = function(doc) return { tostring(doc.State.Code) } end,
    ["availability-zone"] = function(doc) return { doc.Placement.AvailabilityZone } end,
    ["tag-key"] = tag_keys,
    ["tag-value"] = tag_values,
    ["instance.group-id"] = function(doc)
        local out: { string } = {}
        for _, group in ipairs(doc.SecurityGroups) do table.insert(out, group.GroupId) end
        return out
    end,
    ["instance.group-name"] = function(doc)
        local out: { string } = {}
        for _, group in ipairs(doc.SecurityGroups) do table.insert(out, group.GroupName) end
        return out
    end,
}

local SG_FILTERS: { [string]: (Doc, { any }) -> { string } } = {
    ["group-id"] = one("GroupId"),
    ["group-name"] = one("GroupName"),
    ["vpc-id"] = one("VpcId"),
    ["description"] = one("Description"),
    ["owner-id"] = one("OwnerId"),
    ["tag-key"] = tag_keys,
    ["tag-value"] = tag_values,
}

-- Instances --------------------------------------------------------------------------------------

local function instance_ids(raw: any): { string }
    local ids: { string } = {}
    for _, id in ipairs(items(raw)) do
        if type(id) ~= "string" or not string.match(id, "^i%-[%x]+$") then
            roto.error("InvalidInstanceID.Malformed", "Invalid id: \"" .. tostring(id) .. "\"")
        end
        table.insert(ids, id)
    end
    return ids
end

local function visible(doc: Doc): boolean
    return doc.State.Name ~= "terminated" or os.time() - (doc._TerminatedAt or 0) < TERMINATED_VISIBLE_SECONDS
end

local function settle(ctx: Ctx, db: Db, doc: Doc): Doc
    local next_state = SETTLES[doc.State.Name]
    if next_state and not doc._Hold and doc._Request ~= ctx.request_id then
        doc.State = state(next_state)
        if next_state == "terminated" then doc._TerminatedAt = os.time() end
        db.put(ns(ctx, "instance"), doc.InstanceId, doc)
    end
    return doc
end

local function load_instance(ctx: Ctx, db: Db, id: string): Doc
    local doc = db.get(ns(ctx, "instance"), id) :: Doc?
    if not doc then roto.error("InvalidInstanceID.NotFound", "The instance ID '" .. id .. "' does not exist") end
    doc = settle(ctx, db, doc :: Doc)
    if not visible(doc) then roto.error("InvalidInstanceID.NotFound", "The instance ID '" .. id .. "' does not exist") end
    return doc
end

local function selected_instances(ctx: Ctx, db: Db, raw: any): { Doc }
    local docs: { Doc } = {}
    local ids = instance_ids(raw)
    if #ids > 0 then
        for _, id in ipairs(ids) do table.insert(docs, load_instance(ctx, db, id)) end
    else
        for _, doc in ipairs(db.list(ns(ctx, "instance"))) do
            doc = settle(ctx, db, doc)
            if visible(doc) then table.insert(docs, doc) end
        end
        table.sort(docs, function(a: any, b: any)
            if a.LaunchTime ~= b.LaunchTime then return a.LaunchTime < b.LaunchTime end
            return a.InstanceId < b.InstanceId
        end)
    end
    return docs
end

local function present(ctx: Ctx, db: Db, doc: Doc): Doc
    local out = public(doc)
    out.Tags = tags_of(ctx, db, doc.InstanceId)
    return out
end

local function reservations(ctx: Ctx, db: Db, docs: { Doc }): { any }
    local order: { string } = {}
    local by_id: { [string]: Doc } = {}
    for _, doc in ipairs(docs) do
        local id: string = doc._ReservationId
        if not by_id[id] then
            by_id[id] = { ReservationId = id, OwnerId = ctx.account_id, Groups = roto.array(), Instances = roto.array() }
            table.insert(order, id)
        end
        table.insert(by_id[id].Instances, present(ctx, db, doc))
    end
    local out = roto.array()
    for _, id in ipairs(order) do table.insert(out, by_id[id]) end
    return out
end

local function next_address(ctx: Ctx, db: Db): string
    local meta: Doc = (db.get(ns(ctx, "meta"), "address") :: Doc?) or { Next = 4 }
    local n: number = meta.Next
    meta.Next = n + 1
    db.put(ns(ctx, "meta"), "address", meta)
    return "10.0." .. tostring(math.floor(n / 250) % 250) .. "." .. tostring(n % 250 + 4)
end

local function group_identifiers(ctx: Ctx, db: Db, ids: any): { any }
    local out = roto.array()
    for _, id in ipairs(items(ids)) do
        local group = db.get(ns(ctx, "sg"), id) :: Doc?
        table.insert(out, { GroupId = id, GroupName = group and group.GroupName or "default" })
    end
    return out
end

function M.RunInstances(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    local count: number = input.MaxCount
    if input.MinCount < 1 or input.MaxCount < input.MinCount then
        roto.error("InvalidParameterValue", "Invalid value for MinCount/MaxCount")
    end
    local reservation = "r-" .. hex(17)
    local zone = (input.Placement and input.Placement.AvailabilityZone) or (ctx.region .. "a")
    local subnet: string? = input.SubnetId
    local docs: { Doc } = {}
    for index = 1, count do
        local id = "i-" .. hex(17)
        local address = next_address(ctx, db)
        local private_name = "ip-" .. string.gsub(address, "%.", "-")
        local doc: Doc = {
            InstanceId = id,
            ImageId = input.ImageId,
            InstanceType = input.InstanceType or "m1.small",
            KeyName = input.KeyName,
            LaunchTime = os.time(),
            AmiLaunchIndex = index - 1,
            State = state("pending"),
            StateTransitionReason = "",
            PrivateDnsName = private_name .. "." .. ctx.region .. ".compute.internal",
            PrivateIpAddress = address,
            Placement = { AvailabilityZone = zone, Tenancy = "default", GroupName = "" },
            Monitoring = { State = if input.Monitoring and input.Monitoring.Enabled then "enabled" else "disabled" },
            SubnetId = subnet,
            VpcId = subnet and ("vpc-" .. string.sub(roto.sha256(subnet), 1, 8)) or nil,
            SecurityGroups = group_identifiers(ctx, db, input.SecurityGroupIds),
            Architecture = "x86_64",
            RootDeviceType = "ebs",
            RootDeviceName = "/dev/xvda",
            Hypervisor = "xen",
            VirtualizationType = "hvm",
            EbsOptimized = input.EbsOptimized or false,
            ClientToken = input.ClientToken or "",
            ProductCodes = roto.array(),
            BlockDeviceMappings = roto.array(),
            NetworkInterfaces = roto.array(),
            _ReservationId = reservation,
            _Request = ctx.request_id,
        }
        if input.IamInstanceProfile and input.IamInstanceProfile.Arn then
            doc.IamInstanceProfile = { Arn = input.IamInstanceProfile.Arn, Id = "AIPA" .. string.upper(hex(17)) }
        end
        db.put(ns(ctx, "instance"), id, doc)
        local tags = spec_tags(input.TagSpecifications, "instance")
        if #tags > 0 then add_tags(ctx, db, id, tags) end
        table.insert(docs, doc)
    end
    local result = reservations(ctx, db, docs)[1]
    return result
end

function M.DescribeInstances(ctx: Ctx, input: any, db: Db)
    local filters = check_filters(input.Filters, INSTANCE_FILTERS)
    local rows: { any } = {}
    for _, doc in ipairs(selected_instances(ctx, db, input.InstanceIds)) do
        if matches(doc, tags_of(ctx, db, doc.InstanceId), filters, INSTANCE_FILTERS) then table.insert(rows, doc) end
    end
    local size = input.MaxResults
    if size ~= nil and (size < 5 or size > 1000) then
        roto.error("InvalidParameterValue", "Value (" .. tostring(size) .. ") for parameter maxResults is invalid. Expecting a value between 5 and 1000.")
    end
    if size ~= nil and input.InstanceIds ~= nil and #input.InstanceIds > 0 then
        roto.error("InvalidParameterCombination", "The parameter instancesSet cannot be used with the parameter maxResults")
    end
    local paged = roto.page({ MaxResults = size, NextToken = input.NextToken }, rows, "Rows", 1000, 1000, "InvalidParameterValue")
    local out: Doc = { Reservations = reservations(ctx, db, paged.Rows) }
    out.NextToken = paged.NextToken
    return out
end

local SUMMARY_OK = { Status = "ok", Details = roto.array({ { Name = "reachability", Status = "passed" } }) }
local SUMMARY_NA = { Status = "not-applicable", Details = roto.array() }

function M.DescribeInstanceStatus(ctx: Ctx, input: any, db: Db)
    local filters = check_filters(input.Filters, INSTANCE_FILTERS)
    local rows: { any } = {}
    for _, doc in ipairs(selected_instances(ctx, db, input.InstanceIds)) do
        local running = doc.State.Name == "running"
        if (running or input.IncludeAllInstances == true) and matches(doc, tags_of(ctx, db, doc.InstanceId), filters, INSTANCE_FILTERS) then
            table.insert(rows, {
                InstanceId = doc.InstanceId,
                AvailabilityZone = doc.Placement.AvailabilityZone,
                InstanceState = doc.State,
                InstanceStatus = if running then SUMMARY_OK else SUMMARY_NA,
                SystemStatus = if running then SUMMARY_OK else SUMMARY_NA,
                Events = roto.array(),
            })
        end
    end
    local paged = roto.page({ MaxResults = input.MaxResults, NextToken = input.NextToken }, rows, "InstanceStatuses", 1000, 1000, "InvalidParameterValue")
    return paged
end

local function transition(ctx: Ctx, input: any, db: Db, allowed: { string }, keep: { string }, target: string, verb: string): { any }
    check_dry_run(input)
    local ids = instance_ids(input.InstanceIds)
    if #ids == 0 then roto.error("MissingParameter", "The request must contain the parameter InstanceId") end
    local docs: { Doc } = {}
    for _, id in ipairs(ids) do
        local doc = load_instance(ctx, db, id)
        if not roto.contains(allowed, doc.State.Name) then
            roto.error("IncorrectInstanceState", "The instance '" .. id .. "' is not in a state from which it can be " .. verb .. ".")
        end
        table.insert(docs, doc)
    end
    local changes = roto.array()
    for _, doc in ipairs(docs) do
        local previous = doc.State
        if not roto.contains(keep, doc.State.Name) then
            doc.State = state(target)
            doc._Request = ctx.request_id
            doc._Hold = nil
            db.put(ns(ctx, "instance"), doc.InstanceId, doc)
        end
        table.insert(changes, { InstanceId = doc.InstanceId, CurrentState = doc.State, PreviousState = previous })
    end
    return changes
end

function M.StartInstances(ctx: Ctx, input: any, db: Db)
    return { StartingInstances = transition(ctx, input, db, { "stopped", "pending", "running" }, { "pending", "running" }, "pending", "started") }
end

function M.StopInstances(ctx: Ctx, input: any, db: Db)
    return { StoppingInstances = transition(ctx, input, db, { "running", "pending", "stopping", "stopped" }, { "stopping", "stopped" }, "stopping", "stopped") }
end

function M.TerminateInstances(ctx: Ctx, input: any, db: Db)
    return { TerminatingInstances = transition(ctx, input, db, { "pending", "running", "stopping", "stopped", "shutting-down", "terminated" }, { "shutting-down", "terminated" }, "shutting-down", "terminated") }
end

function M.RebootInstances(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    for _, id in ipairs(instance_ids(input.InstanceIds)) do
        local doc = load_instance(ctx, db, id)
        if doc.State.Name ~= "running" then
            roto.error("IncorrectInstanceState", "The instance '" .. id .. "' is not in a state from which it can be rebooted.")
        end
    end
    return {}
end

-- Roto extension (not an EC2 operation): force instances into a state and hold it there.
function M.RotoSetInstanceState(ctx: Ctx, input: any, db: Db)
    if CODES[input.State] == nil then
        roto.error("InvalidParameterValue", "State must be one of: pending, running, shutting-down, terminated, stopping, stopped")
    end
    for _, id in ipairs(instance_ids(input.InstanceIds)) do
        local doc = db.get(ns(ctx, "instance"), id) :: Doc?
        if not doc then roto.error("InvalidInstanceID.NotFound", "The instance ID '" .. id .. "' does not exist") end
        local target = doc :: Doc
        target.State = state(input.State)
        target._Hold = true
        target._Request = ctx.request_id
        if input.State == "terminated" then target._TerminatedAt = os.time() end
        db.put(ns(ctx, "instance"), id, target)
    end
    return {}
end

-- Security groups --------------------------------------------------------------------------------

function M.CreateSecurityGroup(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    local name = roto.required(input, "GroupName", "MissingParameter")
    roto.required(input, "Description", "MissingParameter")
    for _, group in ipairs(db.list(ns(ctx, "sg"))) do
        if group.GroupName == name and group.VpcId == input.VpcId then
            roto.error("InvalidGroup.Duplicate", "The security group '" .. name .. "' already exists" .. (input.VpcId and (" for VPC '" .. input.VpcId .. "'") or ""))
        end
    end
    local id = "sg-" .. hex(17)
    db.put(ns(ctx, "sg"), id, {
        GroupId = id,
        GroupName = name,
        Description = input.Description,
        VpcId = input.VpcId,
        OwnerId = ctx.account_id,
        SecurityGroupArn = "arn:aws:ec2:" .. ctx.region .. ":" .. ctx.account_id .. ":security-group/" .. id,
        IpPermissions = roto.array(),
        IpPermissionsEgress = roto.array({ { IpProtocol = "-1", IpRanges = roto.array({ { CidrIp = "0.0.0.0/0" } }) } }),
    })
    local tags = spec_tags(input.TagSpecifications, "security-group")
    if #tags > 0 then add_tags(ctx, db, id, tags) end
    return { GroupId = id, SecurityGroupArn = "arn:aws:ec2:" .. ctx.region .. ":" .. ctx.account_id .. ":security-group/" .. id, Tags = tags }
end

function M.DeleteSecurityGroup(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    local id: string? = input.GroupId
    if id == nil then
        for _, group in ipairs(db.list(ns(ctx, "sg"))) do
            if group.GroupName == input.GroupName then id = group.GroupId end
        end
    end
    if id == nil or not db.get(ns(ctx, "sg"), id) then
        roto.error("InvalidGroup.NotFound", "The security group '" .. tostring(input.GroupId or input.GroupName) .. "' does not exist")
    end
    for _, instance in ipairs(db.list(ns(ctx, "instance"))) do
        if instance.State.Name ~= "terminated" then
            for _, group in ipairs(items(instance.SecurityGroups)) do
                if group.GroupId == id then roto.error("DependencyViolation", "resource " .. id .. " has a dependent object") end
            end
        end
    end
    db.delete(ns(ctx, "sg"), id :: string)
    db.delete(ns(ctx, "tags"), id :: string)
    return {}
end

function M.DescribeSecurityGroups(ctx: Ctx, input: any, db: Db)
    local filters = check_filters(input.Filters, SG_FILTERS)
    local all = db.list(ns(ctx, "sg"))
    for _, id in ipairs(items(input.GroupIds)) do
        local found = false
        for _, group in ipairs(all) do if group.GroupId == id then found = true end end
        if not found then roto.error("InvalidGroup.NotFound", "The security group '" .. tostring(id) .. "' does not exist") end
    end
    for _, name in ipairs(items(input.GroupNames)) do
        local found = false
        for _, group in ipairs(all) do if group.GroupName == name then found = true end end
        if not found then roto.error("InvalidGroup.NotFound", "The security group '" .. tostring(name) .. "' does not exist") end
    end
    local rows: { any } = {}
    for _, group in ipairs(all) do
        local wanted = (input.GroupIds == nil or #input.GroupIds == 0 or roto.contains(input.GroupIds, group.GroupId))
            and (input.GroupNames == nil or #input.GroupNames == 0 or roto.contains(input.GroupNames, group.GroupName))
        local tags = tags_of(ctx, db, group.GroupId)
        if wanted and matches(group, tags, filters, SG_FILTERS) then
            local row = public(group)
            row.Tags = tags
            table.insert(rows, row)
        end
    end
    return roto.page({ MaxResults = input.MaxResults, NextToken = input.NextToken }, rows, "SecurityGroups", 1000, 1000, "InvalidParameterValue")
end

-- Tags -------------------------------------------------------------------------------------------

function M.CreateTags(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    local resources = roto.list(input.Resources, "MissingParameter", "Resources")
    if #resources == 0 then roto.error("MissingParameter", "The request must contain the parameter resourceIdSet") end
    for _, tag in ipairs(roto.list(input.Tags, "MissingParameter", "Tags")) do
        if type(tag.Key) ~= "string" or tag.Key == "" or #tag.Key > 128 or string.sub(tag.Key, 1, 4) == "aws:" then
            roto.error("InvalidParameterValue", "Invalid tag key")
        end
    end
    for _, id in ipairs(resources) do add_tags(ctx, db, id, input.Tags) end
    return {}
end

function M.DeleteTags(ctx: Ctx, input: any, db: Db)
    check_dry_run(input)
    for _, id in ipairs(roto.list(input.Resources, "MissingParameter", "Resources")) do
        local doc = db.get(ns(ctx, "tags"), id) :: Doc?
        if doc then
            local kept = roto.array()
            for _, tag in ipairs(doc.Tags) do
                local removed = false
                for _, drop in ipairs(items(input.Tags)) do
                    if drop.Key == tag.Key and (drop.Value == nil or drop.Value == tag.Value) then removed = true end
                end
                if not removed then table.insert(kept, tag) end
            end
            doc.Tags = kept
            db.put(ns(ctx, "tags"), id, doc)
        end
    end
    return {}
end

local TAG_FILTERS: { [string]: (Doc, { any }) -> { string } } = {
    ["resource-id"] = one("ResourceId"),
    ["resource-type"] = one("ResourceType"),
    ["key"] = one("Key"),
    ["value"] = one("Value"),
}

function M.DescribeTags(ctx: Ctx, input: any, db: Db)
    local filters: { any } = {}
    for _, filter in ipairs(items(input.Filters)) do
        if not TAG_FILTERS[filter.Name] then roto.error("InvalidParameterValue", "The filter '" .. tostring(filter.Name) .. "' is invalid") end
        table.insert(filters, filter)
    end
    local rows: { any } = {}
    local ids: { string } = {}
    for _, doc in ipairs(db.list(ns(ctx, "tags"))) do
        -- db.list returns bodies only, so each tags document carries its own ResourceId.
        table.insert(ids, doc.ResourceId)
    end
    for _, id in ipairs(ids) do
        for _, tag in ipairs(tags_of(ctx, db, id)) do
            local row = { ResourceId = id, ResourceType = resource_type(id), Key = tag.Key, Value = tag.Value }
            if matches(row, {}, filters, TAG_FILTERS) then table.insert(rows, row) end
        end
    end
    table.sort(rows, function(a: any, b: any) if a.ResourceId ~= b.ResourceId then return a.ResourceId < b.ResourceId end return a.Key < b.Key end)
    return roto.page({ MaxResults = input.MaxResults, NextToken = input.NextToken }, rows, "Tags", 1000, 1000, "InvalidParameterValue")
end

return M
