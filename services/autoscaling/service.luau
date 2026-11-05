--!strict
-- EC2 Auto Scaling, API version 2011-01-01. Stores groups, instance membership, tags and scheduled
-- actions as given; it never launches or terminates instances and runs no scaling policies. Instance
-- membership does not have to exist in EC2 (Luau services cannot call each other), so a fixture
-- registers the same instance IDs in both services.
--
-- Document namespaces (per account and region): autoscaling/group keyed by name,
-- autoscaling/schedule keyed by `<group>|<action>`.
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local GROUP_FIELDS = {
    "LaunchConfigurationName", "LaunchTemplate", "MixedInstancesPolicy", "DefaultCooldown", "AvailabilityZones",
    "AvailabilityZoneIds", "LoadBalancerNames", "TargetGroupARNs", "HealthCheckType", "HealthCheckGracePeriod",
    "PlacementGroup", "VPCZoneIdentifier", "TerminationPolicies", "NewInstancesProtectedFromScaleIn",
    "ServiceLinkedRoleARN", "MaxInstanceLifetime", "CapacityRebalance", "DefaultInstanceWarmup",
    "DeletionProtection", "TrafficSources", "InstanceMaintenancePolicy",
}

local function items(value: any): { any }
    return if type(value) == "table" then value :: { any } else {}
end

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "autoscaling/" .. kind)
end

local function public(doc: Doc): Doc
    local out: Doc = {}
    for key, value in pairs(doc) do
        if string.sub(tostring(key), 1, 1) ~= "_" then out[key] = value end
    end
    return out
end

local function group(ctx: Ctx, db: Db, name: any): Doc
    local key = roto.required({ name = name }, "name", "ValidationError")
    local doc = db.get(ns(ctx, "group"), key)
    if not doc then roto.error("ValidationError", "AutoScalingGroup name not found - " .. key, 400) end
    return doc :: Doc
end

local function group_arn(ctx: Ctx, name: string, id: string): string
    return "arn:aws:autoscaling:" .. ctx.region .. ":" .. ctx.account_id .. ":autoScalingGroup:" .. id .. ":autoScalingGroupName/" .. name
end

local function check_sizes(min: any, max: any, desired: any)
    if type(min) ~= "number" or type(max) ~= "number" then roto.error("ValidationError", "MinSize and MaxSize are required") end
    if min > max then roto.error("ValidationError", "Desired capacity:" .. tostring(desired) .. " must be between the specified min size:" .. tostring(min) .. " and max size:" .. tostring(max)) end
    if type(desired) ~= "number" or desired < min or desired > max then
        roto.error("ValidationError", "Desired capacity:" .. tostring(desired) .. " must be between the specified min size:" .. tostring(min) .. " and max size:" .. tostring(max))
    end
end

local function activity(ctx: Ctx, name: string, description: string, cause: string): Doc
    local now = os.time()
    return {
        ActivityId = roto.uuid(),
        AutoScalingGroupName = name,
        Description = description,
        Cause = cause,
        StartTime = now,
        EndTime = now,
        StatusCode = "Successful",
        Progress = 100,
    }
end

local function tag_rows(name: string, tags: any): { Doc }
    local rows: { Doc } = {}
    for _, tag in ipairs(roto.list(tags or roto.array(), "ValidationError", "Tags")) do
        local key = roto.required(roto.object(tag, "ValidationError", "Tag"), "Key", "ValidationError")
        local row: Doc = {
            ResourceId = tag.ResourceId or name,
            ResourceType = tag.ResourceType or "auto-scaling-group",
            Key = key,
            Value = tag.Value or "",
            PropagateAtLaunch = tag.PropagateAtLaunch or false,
        }
        table.insert(rows, row)
    end
    return rows
end

local function merge_tags(existing: { any }, incoming: { Doc }): { any }
    local out = roto.array()
    for _, row in ipairs(existing) do
        local replaced = false
        for _, new in ipairs(incoming) do
            if new.ResourceId == row.ResourceId and new.Key == row.Key then replaced = true end
        end
        if not replaced then table.insert(out, row) end
    end
    for _, new in ipairs(incoming) do table.insert(out, new) end
    return out
end

function M.CreateAutoScalingGroup(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "AutoScalingGroupName", "ValidationError")
    if db.get(ns(ctx, "group"), name) then
        roto.error("AlreadyExists", "AutoScalingGroup by this name already exists - A group with the name " .. name .. " already exists", 400)
    end
    local desired = input.DesiredCapacity or input.MinSize
    check_sizes(input.MinSize, input.MaxSize, desired)
    local doc: Doc = {
        AutoScalingGroupName = name,
        AutoScalingGroupARN = group_arn(ctx, name, roto.uuid()),
        MinSize = input.MinSize,
        MaxSize = input.MaxSize,
        DesiredCapacity = desired,
        DefaultCooldown = input.DefaultCooldown or 300,
        AvailabilityZones = input.AvailabilityZones or roto.array(),
        LoadBalancerNames = roto.array(),
        TargetGroupARNs = roto.array(),
        HealthCheckType = input.HealthCheckType or "EC2",
        Instances = roto.array(),
        CreatedTime = os.time(),
        SuspendedProcesses = roto.array(),
        EnabledMetrics = roto.array(),
        Status = nil,
        Tags = merge_tags(roto.array(), tag_rows(name, input.Tags)),
        TerminationPolicies = roto.array({ "Default" }),
        NewInstancesProtectedFromScaleIn = false,
    }
    roto.copy_fields(input, doc, GROUP_FIELDS)
    db.put(ns(ctx, "group"), name, doc)
    return {}
end

function M.DeleteAutoScalingGroup(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    if #doc.Instances > 0 and not input.ForceDelete then
        roto.error("ScalingActivityInProgress", "You cannot delete an AutoScalingGroup while there are instances or pending Spot instance request(s) still in the group.", 400)
    end
    for _, action in ipairs(db.list(ns(ctx, "schedule"))) do
        if action.AutoScalingGroupName == doc.AutoScalingGroupName then
            db.delete(ns(ctx, "schedule"), doc.AutoScalingGroupName .. "|" .. action.ScheduledActionName)
        end
    end
    db.delete(ns(ctx, "group"), doc.AutoScalingGroupName)
    return {}
end

function M.UpdateAutoScalingGroup(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local min = if input.MinSize ~= nil then input.MinSize else doc.MinSize
    local max = if input.MaxSize ~= nil then input.MaxSize else doc.MaxSize
    local desired = if input.DesiredCapacity ~= nil then input.DesiredCapacity else doc.DesiredCapacity
    -- Like AWS, lowering the maximum or raising the minimum drags the desired capacity along
    -- unless it was given explicitly.
    if input.DesiredCapacity == nil then
        if desired < min then desired = min end
        if desired > max then desired = max end
    end
    check_sizes(min, max, desired)
    doc.MinSize, doc.MaxSize, doc.DesiredCapacity = min, max, desired
    roto.copy_fields(input, doc, GROUP_FIELDS)
    db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    return {}
end

local function with_instances(doc: Doc, include: any): Doc
    local out = public(doc)
    if include == false then out.Instances = roto.array() end
    return out
end

function M.DescribeAutoScalingGroups(ctx: Ctx, input: any, db: Db)
    local names = input.AutoScalingGroupNames
    local found: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "group"))) do
        local wanted = names == nil or #names == 0 or roto.contains(names, doc.AutoScalingGroupName)
        for _, filter in ipairs(items(input.Filters)) do
            if filter.Name == "tag-key" then
                local hit = false
                for _, tag in ipairs(doc.Tags) do if roto.contains(filter.Values, tag.Key) then hit = true end end
                wanted = wanted and hit
            elseif filter.Name == "tag-value" then
                local hit = false
                for _, tag in ipairs(doc.Tags) do if roto.contains(filter.Values, tag.Value) then hit = true end end
                wanted = wanted and hit
            end
        end
        if wanted then table.insert(found, with_instances(doc, input.IncludeInstances)) end
    end
    return roto.page({ MaxResults = input.MaxRecords, NextToken = input.NextToken }, found, "AutoScalingGroups", 50, 100, "ValidationError")
end

function M.DescribeAutoScalingInstances(ctx: Ctx, input: any, db: Db)
    local ids = input.InstanceIds
    local found: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "group"))) do
        for _, instance in ipairs(doc.Instances) do
            if ids == nil or #ids == 0 or roto.contains(ids, instance.InstanceId) then
                local row = table.clone(instance)
                row.AutoScalingGroupName = doc.AutoScalingGroupName
                table.insert(found, row)
            end
        end
    end
    return roto.page({ MaxResults = input.MaxRecords, NextToken = input.NextToken }, found, "AutoScalingInstances", 50, 50, "ValidationError")
end

function M.AttachInstances(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    for _, id in ipairs(roto.list(input.InstanceIds or roto.array(), "ValidationError", "InstanceIds")) do
        local member = false
        for _, instance in ipairs(doc.Instances) do
            if instance.InstanceId == id then member = true end
        end
        if not member then
            table.insert(doc.Instances, {
                InstanceId = id,
                AvailabilityZone = (doc.AvailabilityZones[1] or ctx.region .. "a"),
                LifecycleState = "InService",
                HealthStatus = "Healthy",
                ProtectedFromScaleIn = false,
            })
            doc.DesiredCapacity += 1
        end
    end
    if doc.DesiredCapacity > doc.MaxSize then doc.MaxSize = doc.DesiredCapacity end
    db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    return {}
end

function M.DetachInstances(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local detached = roto.array()
    for _, id in ipairs(roto.list(input.InstanceIds or roto.array(), "ValidationError", "InstanceIds")) do
        local kept = roto.array()
        local found = false
        for _, instance in ipairs(doc.Instances) do
            if instance.InstanceId == id then found = true else table.insert(kept, instance) end
        end
        if not found then roto.error("ValidationError", "The instance " .. id .. " is not part of Auto Scaling group " .. doc.AutoScalingGroupName .. ".", 400) end
        doc.Instances = kept
        if input.ShouldDecrementDesiredCapacity then doc.DesiredCapacity = math.max(doc.MinSize, doc.DesiredCapacity - 1) end
        table.insert(detached, activity(ctx, doc.AutoScalingGroupName, "Detaching EC2 instance: " .. id, "At " .. os.date("!%Y-%m-%dT%H:%M:%SZ") .. " instance " .. id .. " was detached in response to a user request."))
    end
    db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    return { Activities = detached }
end

function M.SetInstanceHealth(ctx: Ctx, input: any, db: Db)
    local id = roto.required(input, "InstanceId", "ValidationError")
    local status = roto.required(input, "HealthStatus", "ValidationError")
    if status ~= "Healthy" and status ~= "Unhealthy" then roto.error("ValidationError", "Health status must be Healthy or Unhealthy") end
    for _, doc in ipairs(db.list(ns(ctx, "group"))) do
        for _, instance in ipairs(doc.Instances) do
            if instance.InstanceId == id then
                instance.HealthStatus = status
                db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
                return {}
            end
        end
    end
    roto.error("ValidationError", "Instance Id not found - No managed instance found for instance ID '" .. id .. "'", 400)
    return {}
end

local PROCESSES = {
    "Launch", "Terminate", "AddToLoadBalancer", "AlarmNotification", "AZRebalance", "HealthCheck",
    "InstanceRefresh", "ReplaceUnhealthy", "ScheduledActions", "StandbyInstances",
}

local function processes(input: any): { string }
    local names: { string } = {}
    for _, name in ipairs(input.ScalingProcesses or roto.array()) do
        if not roto.contains(PROCESSES, name) then roto.error("ValidationError", "Invalid scaling process name: " .. tostring(name)) end
        table.insert(names, name)
    end
    if #names == 0 then names = PROCESSES end
    return names
end

function M.SuspendProcesses(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    for _, name in ipairs(processes(input)) do
        local present = false
        for _, existing in ipairs(doc.SuspendedProcesses) do if existing.ProcessName == name then present = true end end
        if not present then table.insert(doc.SuspendedProcesses, { ProcessName = name, SuspensionReason = "User suspended at " .. os.date("!%Y-%m-%dT%H:%M:%SZ") }) end
    end
    db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    return {}
end

function M.ResumeProcesses(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local resumed = processes(input)
    local kept = roto.array()
    for _, existing in ipairs(doc.SuspendedProcesses) do
        if not roto.contains(resumed, existing.ProcessName) then table.insert(kept, existing) end
    end
    doc.SuspendedProcesses = kept
    db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    return {}
end

-- Tags --------------------------------------------------------------------------------------------

function M.CreateOrUpdateTags(ctx: Ctx, input: any, db: Db)
    local by_group: { [string]: { Doc } } = {}
    for _, row in ipairs(tag_rows("", input.Tags)) do
        local owner = row.ResourceId
        if owner == "" then roto.error("ValidationError", "ResourceId is required for each tag") end
        by_group[owner] = by_group[owner] or {}
        table.insert(by_group[owner], row)
    end
    for owner, rows in pairs(by_group) do
        local doc = group(ctx, db, owner)
        doc.Tags = merge_tags(doc.Tags, rows)
        db.put(ns(ctx, "group"), owner, doc)
    end
    return {}
end

function M.DeleteTags(ctx: Ctx, input: any, db: Db)
    for _, row in ipairs(tag_rows("", input.Tags)) do
        local doc = group(ctx, db, row.ResourceId)
        local kept = roto.array()
        for _, tag in ipairs(doc.Tags) do if tag.Key ~= row.Key then table.insert(kept, tag) end end
        doc.Tags = kept
        db.put(ns(ctx, "group"), doc.AutoScalingGroupName, doc)
    end
    return {}
end

function M.DescribeTags(ctx: Ctx, input: any, db: Db)
    local rows: { any } = {}
    for _, doc in ipairs(db.list(ns(ctx, "group"))) do
        for _, tag in ipairs(doc.Tags) do
            local keep = true
            for _, filter in ipairs(items(input.Filters)) do
                local value: any = nil
                if filter.Name == "auto-scaling-group" then value = tag.ResourceId
                elseif filter.Name == "key" then value = tag.Key
                elseif filter.Name == "value" then value = tag.Value
                elseif filter.Name == "propagate-at-launch" then value = tostring(tag.PropagateAtLaunch) end
                if value == nil or not roto.contains(filter.Values, value) then keep = false end
            end
            if keep then table.insert(rows, tag) end
        end
    end
    table.sort(rows, function(a: any, b: any) if a.ResourceId ~= b.ResourceId then return a.ResourceId < b.ResourceId end return a.Key < b.Key end)
    return roto.page({ MaxResults = input.MaxRecords, NextToken = input.NextToken }, rows, "Tags", 100, 100, "ValidationError")
end

-- Scheduled actions -------------------------------------------------------------------------------

local function schedule_key(name: string, action: string): string
    return name .. "|" .. action
end

function M.PutScheduledUpdateGroupAction(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local action = roto.required(input, "ScheduledActionName", "ValidationError")
    if input.MinSize == nil and input.MaxSize == nil and input.DesiredCapacity == nil then
        roto.error("ValidationError", "Scheduled action must have at least one of MinSize, MaxSize or DesiredCapacity")
    end
    local stored: Doc = (db.get(ns(ctx, "schedule"), schedule_key(doc.AutoScalingGroupName, action)) :: Doc?) or {}
    stored.AutoScalingGroupName = doc.AutoScalingGroupName
    stored.ScheduledActionName = action
    stored.ScheduledActionARN = "arn:aws:autoscaling:" .. ctx.region .. ":" .. ctx.account_id .. ":scheduledUpdateGroupAction:" .. roto.uuid() .. ":autoScalingGroupName/" .. doc.AutoScalingGroupName .. ":scheduledActionName/" .. action
    roto.copy_fields(input, stored, { "Time", "StartTime", "EndTime", "Recurrence", "MinSize", "MaxSize", "DesiredCapacity", "TimeZone" })
    if input.Time ~= nil then stored.StartTime = input.Time end
    db.put(ns(ctx, "schedule"), schedule_key(doc.AutoScalingGroupName, action), stored)
    return {}
end

function M.DescribeScheduledActions(ctx: Ctx, input: any, db: Db)
    local rows: { any } = {}
    for _, action in ipairs(db.list(ns(ctx, "schedule"))) do
        local wanted = input.AutoScalingGroupName == nil or action.AutoScalingGroupName == input.AutoScalingGroupName
        if input.ScheduledActionNames ~= nil and #input.ScheduledActionNames > 0 then
            wanted = wanted and roto.contains(input.ScheduledActionNames, action.ScheduledActionName)
        end
        if input.StartTime ~= nil and action.StartTime ~= nil and action.StartTime < input.StartTime then wanted = false end
        if input.EndTime ~= nil and action.StartTime ~= nil and action.StartTime > input.EndTime then wanted = false end
        if wanted then table.insert(rows, action) end
    end
    return roto.page({ MaxResults = input.MaxRecords, NextToken = input.NextToken }, rows, "ScheduledUpdateGroupActions", 50, 100, "ValidationError")
end

function M.DeleteScheduledAction(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local action = roto.required(input, "ScheduledActionName", "ValidationError")
    if not db.get(ns(ctx, "schedule"), schedule_key(doc.AutoScalingGroupName, action)) then
        roto.error("ValidationError", "Scheduled action name not found - No scheduled update group action found for group '" .. doc.AutoScalingGroupName .. "' with name '" .. action .. "'")
    end
    db.delete(ns(ctx, "schedule"), schedule_key(doc.AutoScalingGroupName, action))
    return {}
end

function M.BatchDeleteScheduledAction(ctx: Ctx, input: any, db: Db)
    local doc = group(ctx, db, input.AutoScalingGroupName)
    local failed = roto.array()
    for _, action in ipairs(roto.list(input.ScheduledActionNames, "ValidationError", "ScheduledActionNames")) do
        local key = schedule_key(doc.AutoScalingGroupName, action)
        if db.get(ns(ctx, "schedule"), key) then
            db.delete(ns(ctx, "schedule"), key)
        else
            table.insert(failed, { ScheduledActionName = action, ErrorCode = "ScheduledActionNotFound", ErrorMessage = "The scheduled action '" .. action .. "' was not found." })
        end
    end
    return { FailedScheduledActions = failed }
end

return M
