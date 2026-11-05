--!strict
-- Elastic Load Balancing, classic API version 2012-06-01 (`boto3.client("elb")`). Control plane only:
-- no listener forwards traffic and no health check runs. Instance health is whatever the fixture
-- sets with RotoSetInstanceHealth (a Roto extension); a registered instance is `InService` until then.
--
-- Document namespaces (per account and region): elb/lb keyed by name, elb/health keyed by
-- `<name>|<instance id>`. Shares the `elasticloadbalancing` signing name with elbv2; the query
-- `Version` selects this module.
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local STATES = { InService = true, OutOfService = true, Unknown = true }

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "elb/" .. kind)
end

local function public(doc: Doc): Doc
    local out: Doc = {}
    for key, value in pairs(doc) do
        if string.sub(tostring(key), 1, 1) ~= "_" then out[key] = value end
    end
    return out
end

local function balancer(ctx: Ctx, db: Db, name: any): Doc
    local key = roto.required({ name = name }, "name", "ValidationError")
    local doc = db.get(ns(ctx, "lb"), key)
    if not doc then roto.error("LoadBalancerNotFound", "There is no ACTIVE Load Balancer named '" .. key .. "'", 400) end
    return doc :: Doc
end

local function instance_ids(raw: any): { string }
    local ids: { string } = {}
    for _, item in ipairs(roto.list(raw, "ValidationError", "Instances")) do
        table.insert(ids, roto.required(roto.object(item, "ValidationError", "Instance"), "InstanceId", "ValidationError"))
    end
    return ids
end

local function health_key(name: string, id: string): string
    return name .. "|" .. id
end

local function registered(lb: Doc, id: string): boolean
    for _, instance in ipairs(lb.Instances) do
        if instance.InstanceId == id then return true end
    end
    return false
end

function M.CreateLoadBalancer(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "LoadBalancerName", "ValidationError")
    if not string.match(name, "^[%w%-]+$") or #name > 32 then
        roto.error("ValidationError", "Load balancer names may contain only letters, digits and hyphens, up to 32 characters")
    end
    if db.get(ns(ctx, "lb"), name) then
        roto.error("DuplicateLoadBalancerName", "A load balancer with name '" .. name .. "' already exists", 400)
    end
    local listeners = roto.array()
    for _, listener in ipairs(roto.list(input.Listeners, "ValidationError", "Listeners")) do
        table.insert(listeners, {
            Listener = {
                Protocol = listener.Protocol,
                LoadBalancerPort = listener.LoadBalancerPort,
                InstanceProtocol = listener.InstanceProtocol or listener.Protocol,
                InstancePort = listener.InstancePort,
                SSLCertificateId = listener.SSLCertificateId,
            },
            PolicyNames = roto.array(),
        })
    end
    local dns = string.lower(name) .. "-" .. string.sub(roto.sha256(roto.uuid()), 1, 10) .. "." .. ctx.region .. ".elb.amazonaws.com"
    local doc: Doc = {
        LoadBalancerName = name,
        DNSName = dns,
        CanonicalHostedZoneName = dns,
        CanonicalHostedZoneNameID = "Z35SXDOTRQ7X7K",
        ListenerDescriptions = listeners,
        Policies = { AppCookieStickinessPolicies = roto.array(), LBCookieStickinessPolicies = roto.array(), OtherPolicies = roto.array() },
        BackendServerDescriptions = roto.array(),
        AvailabilityZones = input.AvailabilityZones or roto.array(),
        Subnets = input.Subnets or roto.array(),
        VPCId = input.Subnets and "vpc-" .. string.sub(roto.sha256(roto.uuid()), 1, 8) or nil,
        Instances = roto.array(),
        HealthCheck = { Target = "TCP:80", Interval = 30, Timeout = 5, UnhealthyThreshold = 2, HealthyThreshold = 10 },
        SecurityGroups = input.SecurityGroups or roto.array(),
        CreatedTime = os.time(),
        Scheme = input.Scheme or "internet-facing",
        _Tags = roto.tags(nil, input.Tags or roto.array(), "ValidationError"),
    }
    db.put(ns(ctx, "lb"), name, doc)
    return { DNSName = dns }
end

function M.DeleteLoadBalancer(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "LoadBalancerName", "ValidationError")
    -- Deleting an absent load balancer succeeds, as in AWS.
    local lb = db.get(ns(ctx, "lb"), name) :: Doc?
    if lb then
        for _, instance in ipairs(lb.Instances) do db.delete(ns(ctx, "health"), health_key(name, instance.InstanceId)) end
        db.delete(ns(ctx, "lb"), name)
    end
    return {}
end

function M.DescribeLoadBalancers(ctx: Ctx, input: any, db: Db)
    local found: { any } = {}
    if input.LoadBalancerNames ~= nil and #input.LoadBalancerNames > 0 then
        for _, name in ipairs(roto.list(input.LoadBalancerNames, "ValidationError", "LoadBalancerNames")) do
            table.insert(found, public(balancer(ctx, db, name)))
        end
    else
        for _, doc in ipairs(db.list(ns(ctx, "lb"))) do table.insert(found, public(doc)) end
    end
    local size = input.PageSize
    if size ~= nil and (type(size) ~= "number" or size < 1 or size > 400) then
        roto.error("ValidationError", "PageSize must be between 1 and 400")
    end
    local out = roto.page({ MaxResults = size, NextToken = input.Marker }, found, "LoadBalancerDescriptions", 400, 400, "ValidationError")
    out.NextMarker = out.NextToken
    out.NextToken = nil
    return out
end

function M.RegisterInstancesWithLoadBalancer(ctx: Ctx, input: any, db: Db)
    local lb = balancer(ctx, db, input.LoadBalancerName)
    for _, id in ipairs(instance_ids(input.Instances)) do
        if not registered(lb, id) then table.insert(lb.Instances, { InstanceId = id }) end
    end
    db.put(ns(ctx, "lb"), lb.LoadBalancerName, lb)
    return { Instances = lb.Instances }
end

function M.DeregisterInstancesFromLoadBalancer(ctx: Ctx, input: any, db: Db)
    local lb = balancer(ctx, db, input.LoadBalancerName)
    for _, id in ipairs(instance_ids(input.Instances)) do
        local kept = roto.array()
        for _, instance in ipairs(lb.Instances) do
            if instance.InstanceId ~= id then table.insert(kept, instance) end
        end
        lb.Instances = kept
        db.delete(ns(ctx, "health"), health_key(lb.LoadBalancerName, id))
    end
    db.put(ns(ctx, "lb"), lb.LoadBalancerName, lb)
    return { Instances = lb.Instances }
end

function M.DescribeInstanceHealth(ctx: Ctx, input: any, db: Db)
    local lb = balancer(ctx, db, input.LoadBalancerName)
    local ids: { string } = {}
    if input.Instances ~= nil and #input.Instances > 0 then
        ids = instance_ids(input.Instances)
        for _, id in ipairs(ids) do
            if not registered(lb, id) then
                roto.error("InvalidInstance", "Instance '" .. id .. "' is not registered with load balancer '" .. lb.LoadBalancerName .. "'", 400)
            end
        end
    else
        for _, instance in ipairs(lb.Instances) do table.insert(ids, instance.InstanceId) end
    end
    local states = roto.array()
    for _, id in ipairs(ids) do
        local state = (db.get(ns(ctx, "health"), health_key(lb.LoadBalancerName, id)) :: Doc?)
            or { State = "InService", ReasonCode = "N/A", Description = "N/A" }
        table.insert(states, { InstanceId = id, State = state.State, ReasonCode = state.ReasonCode, Description = state.Description })
    end
    return { InstanceStates = states }
end

-- Roto extension (not an AWS operation): choose the health DescribeInstanceHealth reports. With no
-- Instances it applies to every registered instance.
function M.RotoSetInstanceHealth(ctx: Ctx, input: any, db: Db)
    local lb = balancer(ctx, db, input.LoadBalancerName)
    if not STATES[input.State] then roto.error("ValidationError", "State must be InService, OutOfService or Unknown") end
    local ids: { string } = {}
    if input.Instances ~= nil and #input.Instances > 0 then
        ids = instance_ids(input.Instances)
    else
        for _, instance in ipairs(lb.Instances) do table.insert(ids, instance.InstanceId) end
    end
    local out_of_service = input.State == "OutOfService"
    for _, id in ipairs(ids) do
        if not registered(lb, id) then
            roto.error("InvalidInstance", "Instance '" .. id .. "' is not registered with load balancer '" .. lb.LoadBalancerName .. "'", 400)
        end
        db.put(ns(ctx, "health"), health_key(lb.LoadBalancerName, id), {
            State = input.State,
            ReasonCode = input.ReasonCode or (if out_of_service then "Instance" else "N/A"),
            Description = input.Description or (if out_of_service then "Instance has failed at least the UnhealthyThreshold number of health checks consecutively." else "N/A"),
        })
    end
    return {}
end

local function tag_descriptions(ctx: Ctx, db: Db, names: any): { any }
    local out = roto.array()
    for _, name in ipairs(roto.list(names, "ValidationError", "LoadBalancerNames")) do
        local lb = balancer(ctx, db, name)
        table.insert(out, { LoadBalancerName = lb.LoadBalancerName, Tags = lb._Tags })
    end
    return out
end

function M.AddTags(ctx: Ctx, input: any, db: Db)
    for _, name in ipairs(roto.list(input.LoadBalancerNames, "ValidationError", "LoadBalancerNames")) do
        local lb = balancer(ctx, db, name)
        lb._Tags = roto.tags(lb._Tags, input.Tags, "ValidationError")
        db.put(ns(ctx, "lb"), lb.LoadBalancerName, lb)
    end
    return {}
end

function M.RemoveTags(ctx: Ctx, input: any, db: Db)
    local keys = roto.array()
    for _, tag in ipairs(roto.list(input.Tags, "ValidationError", "Tags")) do table.insert(keys, tag.Key) end
    for _, name in ipairs(roto.list(input.LoadBalancerNames, "ValidationError", "LoadBalancerNames")) do
        local lb = balancer(ctx, db, name)
        lb._Tags = roto.untag(lb._Tags, keys, "ValidationError")
        db.put(ns(ctx, "lb"), lb.LoadBalancerName, lb)
    end
    return {}
end

function M.DescribeTags(ctx: Ctx, input: any, db: Db)
    return { TagDescriptions = tag_descriptions(ctx, db, input.LoadBalancerNames) }
end

return M
