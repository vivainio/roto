--!strict
-- Elastic Load Balancing v2 (application load balancers): load balancers, target groups, listeners,
-- rules, target registration and tags. This module is the control plane only. Request forwarding and
-- target health checks run in the server (`roto-server --alb-port`), which reads the documents written
-- here and writes `elbv2/health` documents back; DescribeTargetHealth reports them.
--
-- Document namespaces (per account and region): elbv2/lb, elbv2/tg, elbv2/listener, elbv2/rule,
-- elbv2/health. The server's proxy depends on this layout; keep both sides in step.
local M: { [string]: (Ctx, any, Db) -> any } = {}

type Doc = { [string]: any }

local TAG_ERRORS: { [string]: string } = {
    loadbalancer = "LoadBalancerNotFound",
    targetgroup = "TargetGroupNotFound",
    listener = "ListenerNotFound",
    ["listener-rule"] = "RuleNotFound",
}

local LB_ATTRIBUTES: { [string]: string } = {
    ["access_logs.s3.enabled"] = "false",
    ["access_logs.s3.bucket"] = "",
    ["access_logs.s3.prefix"] = "",
    ["deletion_protection.enabled"] = "false",
    ["idle_timeout.timeout_seconds"] = "60",
    ["routing.http.desync_mitigation_mode"] = "defensive",
    ["routing.http.drop_invalid_header_fields.enabled"] = "false",
    ["routing.http.preserve_host_header.enabled"] = "false",
    ["routing.http.x_amzn_tls_version_and_cipher_suite.enabled"] = "false",
    ["routing.http.xff_client_port.enabled"] = "false",
    ["routing.http.xff_header_processing.mode"] = "append",
    ["routing.http2.enabled"] = "true",
    ["waf.fail_open.enabled"] = "false",
}

local TG_ATTRIBUTES: { [string]: string } = {
    ["deregistration_delay.timeout_seconds"] = "300",
    ["load_balancing.algorithm.type"] = "round_robin",
    ["load_balancing.cross_zone.enabled"] = "use_load_balancer_configuration",
    ["slow_start.duration_seconds"] = "0",
    ["stickiness.enabled"] = "false",
    ["stickiness.lb_cookie.duration_seconds"] = "86400",
    ["stickiness.type"] = "lb_cookie",
    ["target_group_health.dns_failover.minimum_healthy_targets.count"] = "off",
    ["target_group_health.unhealthy_state_routing.minimum_healthy_targets.count"] = "1",
}

local CONDITION_CONFIGS: { [string]: string } = {
    ["path-pattern"] = "PathPatternConfig",
    ["host-header"] = "HostHeaderConfig",
    ["http-header"] = "HttpHeaderConfig",
    ["http-request-method"] = "HttpRequestMethodConfig",
    ["query-string"] = "QueryStringConfig",
    ["source-ip"] = "SourceIpConfig",
}

local FIXED_CONTENT_TYPES = { "text/plain", "text/css", "text/html", "application/javascript", "application/json" }

local function ns(ctx: Ctx, kind: string): string
    return roto.scope(ctx, "elbv2/" .. kind)
end

local function public(doc: Doc): Doc
    local out: Doc = {}
    for key, value in pairs(doc) do
        if string.sub(tostring(key), 1, 1) ~= "_" then out[key] = value end
    end
    return out
end

local function sorted_keys(map: { [string]: any }): { string }
    local keys: { string } = {}
    for key in pairs(map) do table.insert(keys, key) end
    table.sort(keys)
    return keys
end

local function hex16(): string
    return string.sub(roto.sha256(roto.uuid()), 1, 16)
end

local function arn(ctx: Ctx, resource: string): string
    return "arn:aws:elasticloadbalancing:" .. ctx.region .. ":" .. ctx.account_id .. ":" .. resource
end

local function type_slug(kind: string): string
    if kind == "network" then return "net" elseif kind == "gateway" then return "gwy" end
    return "app"
end

local function get(ctx: Ctx, db: Db, kind: string, key: string, code: string, message: string): Doc
    local doc = db.get(ns(ctx, kind), key)
    if not doc then roto.error(code, message, 400) end
    return doc :: Doc
end

local function exists(ctx: Ctx, db: Db, kind: string, key: string): boolean
    return db.get(ns(ctx, kind), key) ~= nil
end

local function strings(value: any, label: string): { string }
    local out: { string } = {}
    for _, item in ipairs(roto.list(value, "ValidationError", label)) do
        if type(item) ~= "string" then roto.error("ValidationError", label .. " must contain strings") end
        table.insert(out, item)
    end
    return out
end

local function page(input: any, values: { any }, key: string): Doc
    local size = input.PageSize
    if size ~= nil and (type(size) ~= "number" or size < 1 or size > 400) then
        roto.error("ValidationError", "PageSize must be between 1 and 400")
    end
    local out = roto.page({ MaxResults = size, NextToken = input.Marker }, values, key, 400, 400, "ValidationError")
    out.NextMarker = out.NextToken
    out.NextToken = nil
    return out
end

local function valid_name(name: string, label: string)
    if #name > 32 or not string.match(name, "^[%w][%w%-]*$") or string.sub(name, -1) == "-" then
        roto.error("ValidationError", label .. " '" .. name .. "' cannot be longer than 32 characters, must be alphanumeric or hyphen and cannot begin or end with a hyphen")
    end
end

local function port_number(value: any, label: string): number
    if type(value) ~= "number" or value % 1 ~= 0 or value < 1 or value > 65535 then
        roto.error("ValidationError", label .. " must be between 1 and 65535")
    end
    return value
end

local function to_any(values: { string }): { any }
    local out: { any } = {}
    for _, value in ipairs(values) do table.insert(out, value) end
    return out
end

local function attribute_list(defaults: { [string]: string }, overrides: { [string]: string }?): { { Key: string, Value: string } }
    local merged: { [string]: string } = {}
    for key, value in pairs(defaults) do merged[key] = value end
    for key, value in pairs(overrides or {}) do merged[key] = value end
    local out: { { Key: string, Value: string } } = roto.array()
    for _, key in ipairs(sorted_keys(merged)) do table.insert(out, { Key = key, Value = merged[key] }) end
    return out
end

local function modify_attributes(defaults: { [string]: string }, doc: Doc, input: any): { { Key: string, Value: string } }
    local current: { [string]: string } = doc._Attributes or {}
    for _, attribute in ipairs(roto.list(input.Attributes, "ValidationError", "Attributes")) do
        local key = roto.required(attribute, "Key", "ValidationError")
        if defaults[key] == nil then roto.error("ValidationError", "Unknown attribute key: " .. key) end
        if type(attribute.Value) ~= "string" then roto.error("ValidationError", "Attribute value must be a string") end
        current[key] = attribute.Value
    end
    doc._Attributes = current
    return attribute_list(defaults, current)
end

local function attribute(doc: Doc, key: string, defaults: { [string]: string }): string
    local overrides: { [string]: string } = doc._Attributes or {}
    return overrides[key] or defaults[key] or ""
end

-- Actions ----------------------------------------------------------------------------------------

local function check_target_group(ctx: Ctx, db: Db, group_arn: any): string
    if type(group_arn) ~= "string" or not exists(ctx, db, "tg", group_arn) then
        roto.error("TargetGroupNotFound", "Target group '" .. tostring(group_arn) .. "' not found")
    end
    return group_arn :: string
end

local function normalize_actions(ctx: Ctx, db: Db, raw: any): { any }
    local list = roto.list(raw, "ValidationError", "Actions")
    if #list == 0 then roto.error("ValidationError", "Actions must contain at least one action") end
    local out = roto.array()
    for index, item in ipairs(list) do
        local action = roto.object(item, "ValidationError", "Action")
        local kind = action.Type
        local normalized: Doc = { Type = kind, Order = action.Order or index }
        if kind == "forward" then
            local config = action.ForwardConfig
            if config ~= nil then
                local groups = roto.list(roto.object(config, "ValidationError", "ForwardConfig").TargetGroups, "ValidationError", "TargetGroups")
                if #groups == 0 then roto.error("ValidationError", "ForwardConfig needs at least one target group") end
                local checked: { any } = roto.array()
                for _, entry in ipairs(groups) do
                    local weight = entry.Weight or 1
                    if type(weight) ~= "number" or weight < 0 or weight > 999 then roto.error("ValidationError", "Weight must be between 0 and 999") end
                    table.insert(checked, { TargetGroupArn = check_target_group(ctx, db, entry.TargetGroupArn), Weight = weight })
                end
                normalized.ForwardConfig = { TargetGroups = checked, TargetGroupStickinessConfig = config.TargetGroupStickinessConfig or { Enabled = false } }
                if #checked == 1 then normalized.TargetGroupArn = (checked[1] :: any).TargetGroupArn end
            else
                local group = check_target_group(ctx, db, action.TargetGroupArn)
                normalized.TargetGroupArn = group
                normalized.ForwardConfig = {
                    TargetGroups = roto.array({ { TargetGroupArn = group, Weight = 1 } }),
                    TargetGroupStickinessConfig = { Enabled = false },
                }
            end
        elseif kind == "fixed-response" then
            local config = roto.object(action.FixedResponseConfig, "ValidationError", "FixedResponseConfig")
            local status = roto.required(config, "StatusCode", "ValidationError")
            if not string.match(status, "^[245]%d%d$") then roto.error("ValidationError", "StatusCode must be 2XX, 4XX or 5XX") end
            if config.ContentType ~= nil and not roto.contains(FIXED_CONTENT_TYPES, config.ContentType) then
                roto.error("ValidationError", "Unsupported ContentType: " .. tostring(config.ContentType))
            end
            if config.MessageBody ~= nil and (type(config.MessageBody) ~= "string" or #config.MessageBody > 1024) then
                roto.error("ValidationError", "MessageBody must be at most 1024 characters")
            end
            normalized.FixedResponseConfig = roto.copy_fields(config, {}, { "StatusCode", "ContentType", "MessageBody" })
        elseif kind == "redirect" then
            local config = roto.object(action.RedirectConfig, "ValidationError", "RedirectConfig")
            if config.StatusCode ~= "HTTP_301" and config.StatusCode ~= "HTTP_302" then
                roto.error("ValidationError", "RedirectConfig.StatusCode must be HTTP_301 or HTTP_302")
            end
            normalized.RedirectConfig = {
                Protocol = config.Protocol or "#{protocol}",
                Port = config.Port or "#{port}",
                Host = config.Host or "#{host}",
                Path = config.Path or "/#{path}",
                Query = config.Query or "#{query}",
                StatusCode = config.StatusCode,
            }
        elseif kind == "authenticate-oidc" or kind == "authenticate-cognito" then
            roto.copy_fields(action, normalized, { "AuthenticateOidcConfig", "AuthenticateCognitoConfig" })
        else
            roto.error("ValidationError", "Unsupported action type: " .. tostring(kind))
        end
        table.insert(out, normalized)
    end
    return out
end

-- Conditions -------------------------------------------------------------------------------------

local function condition_values(config: any, label: string): { string }
    local values = strings(roto.object(config, "ValidationError", label).Values, label .. ".Values")
    if #values == 0 then roto.error("ValidationError", label .. " needs at least one value") end
    return values
end

local function normalize_conditions(raw: any): { any }
    local out = roto.array()
    for _, item in ipairs(roto.list(raw, "ValidationError", "Conditions")) do
        local condition = roto.object(item, "ValidationError", "Condition")
        local field = roto.required(condition, "Field", "ValidationError")
        local config_name = CONDITION_CONFIGS[field]
        if not config_name then roto.error("ValidationError", "Condition field '" .. field .. "' is not supported") end
        local normalized: Doc = { Field = field }
        local config: any = condition[config_name]
        if config == nil and condition.Values ~= nil and (field == "path-pattern" or field == "host-header") then
            config = { Values = condition.Values }
        end
        if config == nil then roto.error("ValidationError", "Condition " .. field .. " needs " .. config_name) end
        if field == "http-header" then
            roto.required(config, "HttpHeaderName", "ValidationError")
            normalized[config_name] = { HttpHeaderName = config.HttpHeaderName, Values = condition_values(config, config_name) }
        elseif field == "query-string" then
            local pairs_out = roto.array()
            for _, pair in ipairs(roto.list(roto.object(config, "ValidationError", config_name).Values, "ValidationError", "QueryStringConfig.Values")) do
                roto.required(pair, "Value", "ValidationError")
                table.insert(pairs_out, roto.copy_fields(pair, {}, { "Key", "Value" }))
            end
            if #pairs_out == 0 then roto.error("ValidationError", "QueryStringConfig needs at least one value") end
            normalized[config_name] = { Values = pairs_out }
        else
            local values = condition_values(config, config_name)
            normalized[config_name] = { Values = values }
            if field == "path-pattern" or field == "host-header" then normalized.Values = values end
        end
        table.insert(out, normalized)
    end
    return out
end

-- Lookups ----------------------------------------------------------------------------------------

local function listener_rules(ctx: Ctx, db: Db, listener_arn: string): { Doc }
    local rules: { Doc } = {}
    for _, rule in ipairs(db.list(ns(ctx, "rule"))) do
        if rule._ListenerArn == listener_arn then table.insert(rules, rule) end
    end
    table.sort(rules, function(a: Doc, b: Doc): boolean
        local left: number = tonumber(a.Priority) or 0
        local right: number = tonumber(b.Priority) or 0
        return left < right
    end)
    return rules
end

local function default_rule(listener: Doc): Doc
    return {
        RuleArn = string.gsub(listener.ListenerArn, ":listener/", ":listener-rule/", 1) .. "/" .. listener._DefaultRuleId,
        Priority = "default",
        Conditions = roto.array(),
        Actions = listener.DefaultActions,
        IsDefault = true,
    }
end

local function action_groups(actions: { any }): { string }
    local found: { string } = {}
    for _, action in ipairs(actions) do
        local entries: { any } = action.ForwardConfig and action.ForwardConfig.TargetGroups or {}
        for _, entry in ipairs(entries) do
            table.insert(found, entry.TargetGroupArn)
        end
    end
    return found
end

-- Load balancer ARNs that send traffic to the target group.
local function group_balancers(ctx: Ctx, db: Db, group_arn: string): { string }
    local found: { string } = {}
    local function note(listener_arn: string, actions: { any })
        if not roto.contains(action_groups(actions), group_arn) then return end
        local listener = db.get(ns(ctx, "listener"), listener_arn) :: Doc?
        if listener and not roto.contains(found, listener.LoadBalancerArn) then table.insert(found, listener.LoadBalancerArn) end
    end
    for _, listener in ipairs(db.list(ns(ctx, "listener"))) do note(listener.ListenerArn, listener.DefaultActions) end
    for _, rule in ipairs(db.list(ns(ctx, "rule"))) do note(rule._ListenerArn, rule.Actions) end
    table.sort(found)
    return found
end

local function group_in_use(ctx: Ctx, db: Db, group_arn: string): boolean
    return #group_balancers(ctx, db, group_arn) > 0
end

local function balancer_listeners(ctx: Ctx, db: Db, balancer_arn: string): { Doc }
    local found: { Doc } = {}
    for _, listener in ipairs(db.list(ns(ctx, "listener"))) do
        if listener.LoadBalancerArn == balancer_arn then table.insert(found, listener) end
    end
    return found
end

local function public_group(ctx: Ctx, db: Db, group: Doc): Doc
    local out = public(group)
    out.LoadBalancerArns = roto.array(to_any(group_balancers(ctx, db, group.TargetGroupArn)))
    return out
end

local function default_health_port(group: Doc): string
    return group.HealthCheckPort or "traffic-port"
end

-- Load balancers ---------------------------------------------------------------------------------

function M.CreateLoadBalancer(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "Name", "ValidationError")
    valid_name(name, "Load balancer name")
    local kind = input.Type or "application"
    if kind ~= "application" and kind ~= "network" and kind ~= "gateway" then
        roto.error("ValidationError", "Type must be application, network or gateway")
    end
    local scheme = input.Scheme or "internet-facing"
    if scheme ~= "internet-facing" and scheme ~= "internal" then roto.error("ValidationError", "Invalid scheme: " .. tostring(scheme)) end
    for _, existing in ipairs(db.list(ns(ctx, "lb"))) do
        if existing.LoadBalancerName == name then
            roto.error("DuplicateLoadBalancerName", "A load balancer with the same name '" .. name .. "' exists, but with different settings")
        end
    end
    local subnets = input.Subnets ~= nil and strings(input.Subnets, "Subnets") or {}
    local mappings: { any } = input.SubnetMappings or {}
    for _, mapping in ipairs(mappings) do table.insert(subnets, mapping.SubnetId) end
    if #subnets == 0 then roto.error("ValidationError", "At least one subnet must be specified") end
    local id = hex16()
    local zones = roto.array()
    for index, subnet in ipairs(subnets) do
        table.insert(zones, { ZoneName = ctx.region .. string.char(96 + ((index - 1) % 6) + 1), SubnetId = subnet, LoadBalancerAddresses = roto.array() })
    end
    local doc: Doc = {
        LoadBalancerArn = arn(ctx, "loadbalancer/" .. type_slug(kind) .. "/" .. name .. "/" .. id),
        DNSName = name .. "-" .. tostring(math.random(100000000, 999999999)) .. "." .. ctx.region .. ".elb.localhost",
        CanonicalHostedZoneId = "Z35SXDOTRQ7X7K",
        CreatedTime = os.time(),
        LoadBalancerName = name,
        Scheme = scheme,
        VpcId = "vpc-" .. string.sub(roto.sha256(table.concat(subnets, ",")), 1, 8),
        State = { Code = "active" },
        Type = kind,
        AvailabilityZones = zones,
        SecurityGroups = roto.array(to_any(input.SecurityGroups ~= nil and strings(input.SecurityGroups, "SecurityGroups") or {})),
        IpAddressType = input.IpAddressType or "ipv4",
        _Attributes = {},
        _Tags = roto.tags({}, input.Tags or roto.array(), "ValidationError"),
    }
    db.put(ns(ctx, "lb"), doc.LoadBalancerArn, doc)
    return { LoadBalancers = roto.array({ public(doc) }) }
end

function M.DescribeLoadBalancers(ctx: Ctx, input: any, db: Db)
    local arns = input.LoadBalancerArns ~= nil and strings(input.LoadBalancerArns, "LoadBalancerArns") or nil
    local names = input.Names ~= nil and strings(input.Names, "Names") or nil
    local values = roto.array()
    if arns then
        for _, balancer_arn in ipairs(arns) do
            table.insert(values, public(get(ctx, db, "lb", balancer_arn, "LoadBalancerNotFound", "One or more load balancers not found")))
        end
    else
        local matched: { [string]: boolean } = {}
        for _, doc in ipairs(db.list(ns(ctx, "lb"))) do
            if not names or roto.contains(names, doc.LoadBalancerName) then
                matched[doc.LoadBalancerName] = true
                table.insert(values, public(doc))
            end
        end
        for _, name in ipairs(names or {}) do
            if not matched[name] then roto.error("LoadBalancerNotFound", "One or more load balancers not found") end
        end
    end
    return page(input, values, "LoadBalancers")
end

function M.DeleteLoadBalancer(ctx: Ctx, input: any, db: Db)
    local balancer_arn = roto.required(input, "LoadBalancerArn", "ValidationError")
    local doc = get(ctx, db, "lb", balancer_arn, "LoadBalancerNotFound", "Load balancer '" .. balancer_arn .. "' not found")
    if attribute(doc, "deletion_protection.enabled", LB_ATTRIBUTES) == "true" then
        roto.error("OperationNotPermitted", "Load balancer '" .. balancer_arn .. "' cannot be deleted because deletion protection is enabled")
    end
    for _, listener in ipairs(balancer_listeners(ctx, db, balancer_arn)) do
        for _, rule in ipairs(listener_rules(ctx, db, listener.ListenerArn)) do db.delete(ns(ctx, "rule"), rule.RuleArn) end
        db.delete(ns(ctx, "listener"), listener.ListenerArn)
    end
    db.delete(ns(ctx, "lb"), balancer_arn)
    return {}
end

function M.DescribeLoadBalancerAttributes(ctx: Ctx, input: any, db: Db)
    local doc = get(ctx, db, "lb", roto.required(input, "LoadBalancerArn", "ValidationError"), "LoadBalancerNotFound", "Load balancer not found")
    return { Attributes = attribute_list(LB_ATTRIBUTES, doc._Attributes) }
end

function M.ModifyLoadBalancerAttributes(ctx: Ctx, input: any, db: Db)
    local doc = get(ctx, db, "lb", roto.required(input, "LoadBalancerArn", "ValidationError"), "LoadBalancerNotFound", "Load balancer not found")
    local attributes = modify_attributes(LB_ATTRIBUTES, doc, input)
    db.put(ns(ctx, "lb"), doc.LoadBalancerArn, doc)
    return { Attributes = attributes }
end

function M.SetSecurityGroups(ctx: Ctx, input: any, db: Db)
    local doc = get(ctx, db, "lb", roto.required(input, "LoadBalancerArn", "ValidationError"), "LoadBalancerNotFound", "Load balancer not found")
    doc.SecurityGroups = roto.array(to_any(strings(input.SecurityGroups, "SecurityGroups")))
    db.put(ns(ctx, "lb"), doc.LoadBalancerArn, doc)
    return { SecurityGroupIds = doc.SecurityGroups }
end

function M.SetSubnets(ctx: Ctx, input: any, db: Db)
    local doc = get(ctx, db, "lb", roto.required(input, "LoadBalancerArn", "ValidationError"), "LoadBalancerNotFound", "Load balancer not found")
    local subnets = input.Subnets ~= nil and strings(input.Subnets, "Subnets") or {}
    local mappings: { any } = input.SubnetMappings or {}
    for _, mapping in ipairs(mappings) do table.insert(subnets, mapping.SubnetId) end
    if #subnets == 0 then roto.error("ValidationError", "At least one subnet must be specified") end
    local zones = roto.array()
    for index, subnet in ipairs(subnets) do
        table.insert(zones, { ZoneName = ctx.region .. string.char(96 + ((index - 1) % 6) + 1), SubnetId = subnet, LoadBalancerAddresses = roto.array() })
    end
    doc.AvailabilityZones = zones
    db.put(ns(ctx, "lb"), doc.LoadBalancerArn, doc)
    return { AvailabilityZones = zones, IpAddressType = doc.IpAddressType }
end

function M.SetIpAddressType(ctx: Ctx, input: any, db: Db)
    local doc = get(ctx, db, "lb", roto.required(input, "LoadBalancerArn", "ValidationError"), "LoadBalancerNotFound", "Load balancer not found")
    local kind = roto.required(input, "IpAddressType", "ValidationError")
    if kind ~= "ipv4" and kind ~= "dualstack" and kind ~= "dualstack-without-public-ipv4" then
        roto.error("ValidationError", "Invalid IpAddressType: " .. kind)
    end
    doc.IpAddressType = kind
    db.put(ns(ctx, "lb"), doc.LoadBalancerArn, doc)
    return { IpAddressType = kind }
end

-- Target groups ----------------------------------------------------------------------------------

local HEALTH_FIELDS = {
    "HealthCheckProtocol", "HealthCheckPort", "HealthCheckEnabled", "HealthCheckPath",
    "HealthCheckIntervalSeconds", "HealthCheckTimeoutSeconds", "HealthyThresholdCount", "UnhealthyThresholdCount",
}

local function check_health(group: Doc)
    local interval, timeout = group.HealthCheckIntervalSeconds, group.HealthCheckTimeoutSeconds
    if type(interval) ~= "number" or interval < 5 or interval > 300 then
        roto.error("ValidationError", "HealthCheckIntervalSeconds must be between 5 and 300")
    end
    if type(timeout) ~= "number" or timeout < 2 or timeout > 120 then
        roto.error("ValidationError", "HealthCheckTimeoutSeconds must be between 2 and 120")
    end
    if timeout >= interval then roto.error("ValidationError", "Health check timeout must be smaller than the interval") end
    for _, field in ipairs({ "HealthyThresholdCount", "UnhealthyThresholdCount" }) do
        local value = group[field]
        if type(value) ~= "number" or value < 2 or value > 10 then roto.error("ValidationError", field .. " must be between 2 and 10") end
    end
    local port = group.HealthCheckPort
    if port ~= "traffic-port" then
        local number = tonumber(port)
        if not number then roto.error("ValidationError", "HealthCheckPort must be 'traffic-port' or a port number") end
        port_number(number, "HealthCheckPort")
    end
end

local function matcher(input: any, current: Doc?): Doc
    local given = input
    if given == nil then return current or { HttpCode = "200" } end
    local result: Doc = {}
    result.HttpCode = given.HttpCode or (current and current.HttpCode) or "200"
    result.GrpcCode = given.GrpcCode or (current and current.GrpcCode)
    return result
end

function M.CreateTargetGroup(ctx: Ctx, input: any, db: Db)
    local name = roto.required(input, "Name", "ValidationError")
    valid_name(name, "Target group name")
    for _, existing in ipairs(db.list(ns(ctx, "tg"))) do
        if existing.TargetGroupName == name then
            roto.error("DuplicateTargetGroupName", "A target group with the same name '" .. name .. "' exists, but with different attributes")
        end
    end
    local kind = input.TargetType or "instance"
    if kind ~= "instance" and kind ~= "ip" and kind ~= "lambda" and kind ~= "alb" then
        roto.error("ValidationError", "Invalid target type: " .. tostring(kind))
    end
    local protocol = input.Protocol
    local port = input.Port
    if kind ~= "lambda" then
        if protocol == nil or port == nil then roto.error("ValidationError", "Protocol and Port are required for target type " .. kind) end
        port_number(port, "Port")
        local allowed = { "HTTP", "HTTPS", "TCP", "TLS", "UDP", "TCP_UDP", "GENEVE" }
        if not roto.contains(allowed, protocol) then roto.error("ValidationError", "Invalid protocol: " .. tostring(protocol)) end
        roto.required(input, "VpcId", "ValidationError")
    end
    local web = protocol == "HTTP" or protocol == "HTTPS"
    local group: Doc = {
        TargetGroupArn = arn(ctx, "targetgroup/" .. name .. "/" .. hex16()),
        TargetGroupName = name,
        Protocol = protocol,
        Port = port,
        VpcId = input.VpcId,
        HealthCheckProtocol = input.HealthCheckProtocol or (kind == "lambda" and "HTTP" or protocol),
        HealthCheckPort = input.HealthCheckPort or "traffic-port",
        HealthCheckEnabled = input.HealthCheckEnabled == nil and true or input.HealthCheckEnabled,
        HealthCheckIntervalSeconds = input.HealthCheckIntervalSeconds or (kind == "lambda" and 35 or 30),
        HealthCheckTimeoutSeconds = input.HealthCheckTimeoutSeconds or (kind == "lambda" and 30 or 5),
        HealthyThresholdCount = input.HealthyThresholdCount or (kind == "lambda" and 5 or 5),
        UnhealthyThresholdCount = input.UnhealthyThresholdCount or 2,
        HealthCheckPath = input.HealthCheckPath or ((web or kind == "lambda") and "/" or nil),
        Matcher = (web or kind == "lambda") and matcher(input.Matcher, nil) or nil,
        TargetType = kind,
        ProtocolVersion = web and (input.ProtocolVersion or "HTTP1") or nil,
        IpAddressType = input.IpAddressType or "ipv4",
        _Targets = {},
        _Attributes = {},
        _Tags = roto.tags({}, input.Tags or roto.array(), "ValidationError"),
    }
    check_health(group)
    db.put(ns(ctx, "tg"), group.TargetGroupArn, group)
    return { TargetGroups = roto.array({ public_group(ctx, db, group) }) }
end

function M.DescribeTargetGroups(ctx: Ctx, input: any, db: Db)
    local arns = input.TargetGroupArns ~= nil and strings(input.TargetGroupArns, "TargetGroupArns") or nil
    local names = input.Names ~= nil and strings(input.Names, "Names") or nil
    local values = roto.array()
    if arns then
        for _, group_arn in ipairs(arns) do
            table.insert(values, public_group(ctx, db, get(ctx, db, "tg", group_arn, "TargetGroupNotFound", "One or more target groups not found")))
        end
    else
        local matched: { [string]: boolean } = {}
        for _, group in ipairs(db.list(ns(ctx, "tg"))) do
            local balancer_ok = true
            if input.LoadBalancerArn ~= nil then
                balancer_ok = roto.contains(group_balancers(ctx, db, group.TargetGroupArn), input.LoadBalancerArn)
            end
            if balancer_ok and (not names or roto.contains(names, group.TargetGroupName)) then
                matched[group.TargetGroupName] = true
                table.insert(values, public_group(ctx, db, group))
            end
        end
        for _, name in ipairs(names or {}) do
            if not matched[name] then roto.error("TargetGroupNotFound", "One or more target groups not found") end
        end
    end
    return page(input, values, "TargetGroups")
end

function M.ModifyTargetGroup(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    roto.copy_fields(input, group, HEALTH_FIELDS)
    if input.Matcher ~= nil then group.Matcher = matcher(input.Matcher, group.Matcher) end
    check_health(group)
    db.put(ns(ctx, "tg"), group.TargetGroupArn, group)
    return { TargetGroups = roto.array({ public_group(ctx, db, group) }) }
end

function M.DeleteTargetGroup(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    if group_in_use(ctx, db, group.TargetGroupArn) then
        roto.error("ResourceInUse", "Target group '" .. group.TargetGroupArn .. "' is currently in use by a listener or a rule")
    end
    for _, target in ipairs(group._Targets) do db.delete(ns(ctx, "health"), group.TargetGroupArn .. "|" .. target.Id .. ":" .. tostring(target.Port)) end
    db.delete(ns(ctx, "tg"), group.TargetGroupArn)
    return {}
end

function M.DescribeTargetGroupAttributes(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    return { Attributes = attribute_list(TG_ATTRIBUTES, group._Attributes) }
end

function M.ModifyTargetGroupAttributes(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    local attributes = modify_attributes(TG_ATTRIBUTES, group, input)
    db.put(ns(ctx, "tg"), group.TargetGroupArn, group)
    return { Attributes = attributes }
end

-- Targets ----------------------------------------------------------------------------------------

local function target_key(group_arn: string, id: string, port: number): string
    return group_arn .. "|" .. id .. ":" .. tostring(port)
end

local function parse_targets(group: Doc, raw: any): { Doc }
    local targets: { Doc } = {}
    local list = roto.list(raw, "ValidationError", "Targets")
    if #list == 0 then roto.error("ValidationError", "Targets must contain at least one target") end
    for _, item in ipairs(list) do
        local id = roto.required(roto.object(item, "ValidationError", "Target"), "Id", "ValidationError")
        local port = item.Port or group.Port
        if group.TargetType == "lambda" then port = 0 else port_number(port, "Target port") end
        local target: Doc = { Id = id, Port = port, AvailabilityZone = item.AvailabilityZone }
        table.insert(targets, target)
    end
    return targets
end

function M.RegisterTargets(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    for _, target in ipairs(parse_targets(group, input.Targets)) do
        local present = false
        for _, existing in ipairs(group._Targets) do
            if existing.Id == target.Id and existing.Port == target.Port then present = true end
        end
        if not present then
            table.insert(group._Targets, target)
            db.delete(ns(ctx, "health"), target_key(group.TargetGroupArn, target.Id, target.Port))
        end
    end
    db.put(ns(ctx, "tg"), group.TargetGroupArn, group)
    return {}
end

function M.DeregisterTargets(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    for _, target in ipairs(parse_targets(group, input.Targets)) do
        local kept: { Doc } = {}
        for _, existing in ipairs(group._Targets) do
            if existing.Id ~= target.Id or existing.Port ~= target.Port then table.insert(kept, existing) end
        end
        group._Targets = kept
        db.delete(ns(ctx, "health"), target_key(group.TargetGroupArn, target.Id, target.Port))
    end
    db.put(ns(ctx, "tg"), group.TargetGroupArn, group)
    return {}
end

function M.DescribeTargetHealth(ctx: Ctx, input: any, db: Db)
    local group = get(ctx, db, "tg", roto.required(input, "TargetGroupArn", "ValidationError"), "TargetGroupNotFound", "Target group not found")
    local selected: { any } = group._Targets
    if input.Targets ~= nil then
        selected = {}
        for _, target in ipairs(parse_targets(group, input.Targets)) do
            local found = false
            for _, existing in ipairs(group._Targets) do
                if existing.Id == target.Id and existing.Port == target.Port then found = true end
            end
            if not found then roto.error("InvalidTarget", "Target '" .. target.Id .. "' is not registered to target group '" .. group.TargetGroupArn .. "'") end
            table.insert(selected, target)
        end
    end
    local in_use = group_in_use(ctx, db, group.TargetGroupArn)
    local out = roto.array()
    for _, target in ipairs(selected) do
        local health: Doc
        if not in_use then
            health = { State = "unused", Reason = "Target.NotInUse", Description = "Target group is not configured to receive traffic from the load balancer" }
        else
            health = (db.get(ns(ctx, "health"), target_key(group.TargetGroupArn, target.Id, target.Port)) :: Doc?)
                or { State = "initial", Reason = "Elb.RegistrationInProgress", Description = "Target registration is in progress" }
        end
        local reported: Doc = { State = health.State }
        reported.Reason = health.Reason
        reported.Description = health.Description
        table.insert(out, {
            Target = { Id = target.Id, Port = target.Port ~= 0 and target.Port or nil },
            HealthCheckPort = default_health_port(group) == "traffic-port" and tostring(target.Port) or default_health_port(group),
            TargetHealth = reported,
        })
    end
    return { TargetHealthDescriptions = out }
end

-- Listeners --------------------------------------------------------------------------------------

local function check_listener(ctx: Ctx, db: Db, input: any, balancer: Doc, current: Doc?): (string, number)
    local protocol: any = input.Protocol or (current and current.Protocol)
    local port: any = input.Port or (current and current.Port)
    if balancer.Type == "application" and protocol ~= "HTTP" and protocol ~= "HTTPS" then
        roto.error("UnsupportedProtocol", "Protocol '" .. tostring(protocol) .. "' is not supported for application load balancers")
    end
    port_number(port, "Port")
    for _, other in ipairs(balancer_listeners(ctx, db, balancer.LoadBalancerArn)) do
        if other.Port == port and (not current or other.ListenerArn ~= current.ListenerArn) then
            roto.error("DuplicateListener", "A listener with the specified port already exists")
        end
    end
    if protocol == "HTTPS" then
        local certificates = input.Certificates or (current and current.Certificates)
        if not certificates or #certificates == 0 then roto.error("ValidationError", "A certificate must be specified for HTTPS listeners") end
    end
    return protocol :: string, port :: number
end

function M.CreateListener(ctx: Ctx, input: any, db: Db)
    local balancer_arn = roto.required(input, "LoadBalancerArn", "ValidationError")
    local balancer = get(ctx, db, "lb", balancer_arn, "LoadBalancerNotFound", "Load balancer '" .. balancer_arn .. "' not found")
    local protocol, port = check_listener(ctx, db, input, balancer, nil)
    local actions = normalize_actions(ctx, db, input.DefaultActions)
    local id = hex16()
    local listener: Doc = {
        ListenerArn = string.gsub(balancer_arn, ":loadbalancer/", ":listener/", 1) .. "/" .. id,
        LoadBalancerArn = balancer_arn,
        Port = port,
        Protocol = protocol,
        DefaultActions = actions,
        _DefaultRuleId = hex16(),
        _Attributes = {},
        _Tags = roto.tags({}, input.Tags or roto.array(), "ValidationError"),
    }
    if protocol == "HTTPS" then
        listener.Certificates = input.Certificates
        listener.SslPolicy = input.SslPolicy or "ELBSecurityPolicy-2016-08"
    end
    db.put(ns(ctx, "listener"), listener.ListenerArn, listener)
    return { Listeners = roto.array({ public(listener) }) }
end

function M.DescribeListeners(ctx: Ctx, input: any, db: Db)
    local values = roto.array()
    if input.ListenerArns ~= nil then
        for _, listener_arn in ipairs(strings(input.ListenerArns, "ListenerArns")) do
            table.insert(values, public(get(ctx, db, "listener", listener_arn, "ListenerNotFound", "One or more listeners not found")))
        end
    elseif input.LoadBalancerArn ~= nil then
        get(ctx, db, "lb", input.LoadBalancerArn, "LoadBalancerNotFound", "Load balancer not found")
        local listeners = balancer_listeners(ctx, db, input.LoadBalancerArn)
        table.sort(listeners, function(a: Doc, b: Doc) return a.Port < b.Port end)
        for _, listener in ipairs(listeners) do table.insert(values, public(listener)) end
    else
        roto.error("ValidationError", "You must specify either listener ARNs or a load balancer ARN")
    end
    return page(input, values, "Listeners")
end

function M.ModifyListener(ctx: Ctx, input: any, db: Db)
    local listener = get(ctx, db, "listener", roto.required(input, "ListenerArn", "ValidationError"), "ListenerNotFound", "Listener not found")
    local balancer = get(ctx, db, "lb", listener.LoadBalancerArn, "LoadBalancerNotFound", "Load balancer not found")
    local protocol, port = check_listener(ctx, db, input, balancer, listener)
    listener.Protocol, listener.Port = protocol, port
    if input.DefaultActions ~= nil then listener.DefaultActions = normalize_actions(ctx, db, input.DefaultActions) end
    if input.Certificates ~= nil then listener.Certificates = input.Certificates end
    if input.SslPolicy ~= nil then listener.SslPolicy = input.SslPolicy end
    if protocol == "HTTP" then
        listener.Certificates = nil
        listener.SslPolicy = nil
    end
    db.put(ns(ctx, "listener"), listener.ListenerArn, listener)
    return { Listeners = roto.array({ public(listener) }) }
end

function M.DeleteListener(ctx: Ctx, input: any, db: Db)
    local listener = get(ctx, db, "listener", roto.required(input, "ListenerArn", "ValidationError"), "ListenerNotFound", "Listener not found")
    for _, rule in ipairs(listener_rules(ctx, db, listener.ListenerArn)) do db.delete(ns(ctx, "rule"), rule.RuleArn) end
    db.delete(ns(ctx, "listener"), listener.ListenerArn)
    return {}
end

function M.DescribeListenerAttributes(ctx: Ctx, input: any, db: Db)
    get(ctx, db, "listener", roto.required(input, "ListenerArn", "ValidationError"), "ListenerNotFound", "Listener not found")
    return { Attributes = roto.array() }
end

function M.DescribeListenerCertificates(ctx: Ctx, input: any, db: Db)
    local listener = get(ctx, db, "listener", roto.required(input, "ListenerArn", "ValidationError"), "ListenerNotFound", "Listener not found")
    return { Certificates = roto.array(listener.Certificates or {}) }
end

-- Rules ------------------------------------------------------------------------------------------

local function priority_of(value: any): number
    if type(value) ~= "number" or value % 1 ~= 0 or value < 1 or value > 50000 then
        roto.error("ValidationError", "Priority must be between 1 and 50000")
    end
    return value
end

local function priority_free(ctx: Ctx, db: Db, listener_arn: string, priority: number, except: string?)
    for _, rule in ipairs(listener_rules(ctx, db, listener_arn)) do
        if tonumber(rule.Priority) == priority and rule.RuleArn ~= except then
            roto.error("PriorityInUse", "The provided priority '" .. tostring(priority) .. "' is already in use")
        end
    end
end

function M.CreateRule(ctx: Ctx, input: any, db: Db)
    local listener_arn = roto.required(input, "ListenerArn", "ValidationError")
    local listener = get(ctx, db, "listener", listener_arn, "ListenerNotFound", "Listener '" .. listener_arn .. "' not found")
    local priority = priority_of(input.Priority)
    priority_free(ctx, db, listener_arn, priority, nil)
    local conditions = normalize_conditions(input.Conditions)
    local actions = normalize_actions(ctx, db, input.Actions)
    local rule: Doc = {
        RuleArn = string.gsub(listener.ListenerArn, ":listener/", ":listener-rule/", 1) .. "/" .. hex16(),
        Priority = tostring(priority),
        Conditions = conditions,
        Actions = actions,
        IsDefault = false,
        _ListenerArn = listener_arn,
        _Tags = roto.tags({}, input.Tags or roto.array(), "ValidationError"),
    }
    db.put(ns(ctx, "rule"), rule.RuleArn, rule)
    return { Rules = roto.array({ public(rule) }) }
end

function M.DescribeRules(ctx: Ctx, input: any, db: Db)
    local values = roto.array()
    if input.RuleArns ~= nil then
        for _, rule_arn in ipairs(strings(input.RuleArns, "RuleArns")) do
            local found = db.get(ns(ctx, "rule"), rule_arn) :: Doc?
            if found then
                table.insert(values, public(found))
            else
                local matched = false
                for _, listener in ipairs(db.list(ns(ctx, "listener"))) do
                    if default_rule(listener).RuleArn == rule_arn then
                        table.insert(values, default_rule(listener))
                        matched = true
                    end
                end
                if not matched then roto.error("RuleNotFound", "One or more rules not found") end
            end
        end
    elseif input.ListenerArn ~= nil then
        local listener = get(ctx, db, "listener", input.ListenerArn, "ListenerNotFound", "Listener not found")
        for _, rule in ipairs(listener_rules(ctx, db, listener.ListenerArn)) do table.insert(values, public(rule)) end
        table.insert(values, default_rule(listener))
    else
        roto.error("ValidationError", "You must specify either listener ARN or rule ARNs")
    end
    return page(input, values, "Rules")
end

function M.ModifyRule(ctx: Ctx, input: any, db: Db)
    local rule = get(ctx, db, "rule", roto.required(input, "RuleArn", "ValidationError"), "RuleNotFound", "Rule not found")
    if input.Conditions ~= nil then rule.Conditions = normalize_conditions(input.Conditions) end
    if input.Actions ~= nil then rule.Actions = normalize_actions(ctx, db, input.Actions) end
    db.put(ns(ctx, "rule"), rule.RuleArn, rule)
    return { Rules = roto.array({ public(rule) }) }
end

function M.DeleteRule(ctx: Ctx, input: any, db: Db)
    local rule_arn = roto.required(input, "RuleArn", "ValidationError")
    local rule = db.get(ns(ctx, "rule"), rule_arn)
    if not rule then
        for _, listener in ipairs(db.list(ns(ctx, "listener"))) do
            if default_rule(listener).RuleArn == rule_arn then roto.error("OperationNotPermitted", "Default rule cannot be deleted") end
        end
        roto.error("RuleNotFound", "Rule '" .. rule_arn .. "' not found")
    end
    db.delete(ns(ctx, "rule"), rule_arn)
    return {}
end

function M.SetRulePriorities(ctx: Ctx, input: any, db: Db)
    local requested: { [string]: number } = {}
    local rules: { Doc } = {}
    local seen: { [number]: boolean } = {}
    for _, entry in ipairs(roto.list(input.RulePriorities, "ValidationError", "RulePriorities")) do
        local rule = get(ctx, db, "rule", roto.required(entry, "RuleArn", "ValidationError"), "RuleNotFound", "Rule not found")
        local priority = priority_of(entry.Priority)
        if seen[priority] then roto.error("PriorityInUse", "The provided priority '" .. tostring(priority) .. "' is already in use") end
        seen[priority] = true
        requested[rule.RuleArn] = priority
        table.insert(rules, rule)
    end
    -- A swap is legal, so priorities are checked against the rules that are not being moved.
    for _, rule in ipairs(rules) do
        for _, other in ipairs(listener_rules(ctx, db, rule._ListenerArn)) do
            if requested[other.RuleArn] == nil and tonumber(other.Priority) == requested[rule.RuleArn] then
                roto.error("PriorityInUse", "The provided priority '" .. tostring(requested[rule.RuleArn]) .. "' is already in use")
            end
        end
    end
    local out = roto.array()
    for _, rule in ipairs(rules) do
        rule.Priority = tostring(requested[rule.RuleArn])
        db.put(ns(ctx, "rule"), rule.RuleArn, rule)
        table.insert(out, public(rule))
    end
    return { Rules = out }
end

-- Tags -------------------------------------------------------------------------------------------

local function tagged(ctx: Ctx, db: Db, resource_arn: string): (string, Doc)
    local resource = string.match(resource_arn, "^arn:aws:elasticloadbalancing:[^:]*:[^:]*:([%w%-]+)/")
    local kind = ({ loadbalancer = "lb", targetgroup = "tg", listener = "listener", ["listener-rule"] = "rule" })[resource or ""]
    if not kind then roto.error("ValidationError", "Invalid resource ARN: " .. resource_arn) end
    return kind, get(ctx, db, kind, resource_arn, TAG_ERRORS[resource :: string], "Resource '" .. resource_arn .. "' not found")
end

function M.AddTags(ctx: Ctx, input: any, db: Db)
    local arns = strings(input.ResourceArns, "ResourceArns")
    if #arns == 0 then roto.error("ValidationError", "ResourceArns must contain at least one ARN") end
    for _, resource_arn in ipairs(arns) do
        local kind, doc = tagged(ctx, db, resource_arn)
        doc._Tags = roto.tags(doc._Tags, input.Tags, "ValidationError")
        db.put(ns(ctx, kind), resource_arn, doc)
    end
    return {}
end

function M.RemoveTags(ctx: Ctx, input: any, db: Db)
    for _, resource_arn in ipairs(strings(input.ResourceArns, "ResourceArns")) do
        local kind, doc = tagged(ctx, db, resource_arn)
        doc._Tags = roto.untag(doc._Tags, input.TagKeys, "ValidationError")
        db.put(ns(ctx, kind), resource_arn, doc)
    end
    return {}
end

function M.DescribeTags(ctx: Ctx, input: any, db: Db)
    local out = roto.array()
    for _, resource_arn in ipairs(strings(input.ResourceArns, "ResourceArns")) do
        local _, doc = tagged(ctx, db, resource_arn)
        table.insert(out, { ResourceArn = resource_arn, Tags = doc._Tags or roto.array() })
    end
    return { TagDescriptions = out }
end

-- Static reference data --------------------------------------------------------------------------

function M.DescribeSSLPolicies(_ctx: Ctx, _input: any, _db: Db)
    return {
        SslPolicies = roto.array({
            { Name = "ELBSecurityPolicy-2016-08", SslProtocols = roto.array({ "TLSv1", "TLSv1.1", "TLSv1.2" }), Ciphers = roto.array() },
            { Name = "ELBSecurityPolicy-TLS13-1-2-2021-06", SslProtocols = roto.array({ "TLSv1.2", "TLSv1.3" }), Ciphers = roto.array() },
        }),
    }
end

function M.DescribeAccountLimits(_ctx: Ctx, _input: any, _db: Db)
    local function limit(name: string, max: string): { Name: string, Max: string } return { Name = name, Max = max } end
    return {
        Limits = roto.array({
            limit("application-load-balancers", "50"),
            limit("target-groups", "3000"),
            limit("targets-per-application-load-balancer", "1000"),
            limit("listeners-per-application-load-balancer", "50"),
            limit("rules-per-application-load-balancer", "100"),
        }),
    }
end

return M
