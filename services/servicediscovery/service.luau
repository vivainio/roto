--!strict
-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack Cloud Map. HTTP namespaces only; operations finish synchronously.
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type FiltersInput = { Filters: any }
type CreateHttpNamespaceInput = { Description: any, Name: any, Tags: any }
type IdInput = { Id: any }
type PagedFilterInput = { NextToken: any, Filters: any }
type UpdateHttpNamespaceInput = { Id: any, Namespace: any }
type CreateServiceInput = { Description: any, DnsConfig: any, HealthCheckConfig: any, HealthCheckCustomConfig: any, Name: any, NamespaceId: any, Tags: any }
type UpdateServiceInput = { Id: any, Service: any }
type RegisterInstanceInput = { Attributes: any, InstanceId: any, ServiceId: any }
type DeregisterInstanceInput = { InstanceId: any, ServiceId: any }
type ListInstancesInput = { NextToken: any, ServiceId: any }
type NamedInput = { NamespaceName: any, ServiceName: any }
type DiscoverInstancesInput = { HealthStatus: any, MaxResults: any, NamespaceName: any, QueryParameters: any, ServiceName: any }
type UpdateInstanceCustomHealthStatusInput = { InstanceId: any, ServiceId: any, Status: any }
type GetInstancesHealthStatusInput = { Instances: any, NextToken: any, ServiceId: any }
type GetOperationInput = { OperationId: any }
type GetServiceAttributesInput = { ServiceId: any }
type UpdateServiceAttributesInput = { Attributes: any, ServiceId: any }
type TaggedInput = { ResourceARN: any }
type TagResourceInput = { ResourceARN: any, Tags: any }
type UntagResourceInput = { ResourceARN: any, TagKeys: any }
local ops = {}
local function ns(ctx: Ctx, collection: any) return roto.scope(ctx,collection) end
local function arn(ctx: Ctx, kind: any, id: any) return "arn:aws:servicediscovery:" .. ctx.region .. ":" .. ctx.account_id .. ":" .. kind .. "/" .. id end
local function get(ctx: Ctx, db: Db, collection: any, id: any, code: any)
    if type(id) ~= "string" or id == "" then roto.error("InvalidInput","Resource ID is required") end
    local value = db.get(ns(ctx,collection),id)
    if not value then roto.error(code,"Resource not found: " .. id,404) end
    return value
end
local function namespace(ctx: Ctx, db: Db, id: any) return get(ctx,db,"namespaces",id,"NamespaceNotFound") end
local function service(ctx: Ctx, db: Db, id: any) return get(ctx,db,"services",id,"ServiceNotFound") end
local function instances(ctx: Ctx, id: any) return ns(ctx,"instances/" .. id) end
local function operation(ctx: Ctx, db: Db, kind: any, targets: any)
    local id,now = roto.uuid(),os.time()
    db.put(ns(ctx,"operations"),id,{Id=id,Status="SUCCESS",Type=kind,Targets=targets,CreateDate=now,UpdateDate=now})
    return {OperationId=id}
end
local function public(value: any)
    value.Tags,value.Revision,value.Attributes = nil,nil,nil
    return value
end
local function service_shape(ctx: Ctx, db: Db, value: any)
    value.InstanceCount = #db.list(instances(ctx,value.Id))
    return public(value)
end
local function filters(values: any, input: FiltersInput, mapping: any): any
    for _, filter in ipairs(roto.list(input.Filters or roto.array(),"InvalidInput","Filters")) do
        local key = mapping[filter.Name]
        if not key then roto.error("InvalidInput","Unsupported filter") end
        local wanted = roto.list(filter.Values,"InvalidInput","Filter.Values")
        if #wanted == 0 then roto.error("InvalidInput","Filter.Values must be nonempty") end
        local condition = filter.Condition or "EQ"
        if condition ~= "EQ" and condition ~= "IN" and condition ~= "BEGINS_WITH" then roto.error("InvalidInput","Unsupported filter condition") end
        local matched = roto.array()
        for _, value in ipairs(values) do
            local actual = if typeof(key) == "function" then (key :: (any) -> any)(value) else value[key]
            local ok = false
            for _, expected in ipairs(wanted) do
                if actual == expected or (condition == "BEGINS_WITH" and type(actual) == "string" and type(expected) == "string" and string.sub(actual,1,#expected) == expected) then ok=true end
            end
            if ok then table.insert(matched,value) end
        end
        values=matched
    end
    return values
end
function ops.CreateHttpNamespace(ctx: Ctx, input: CreateHttpNamespaceInput, db: Db)
    local name = roto.required(input,"Name")
    for _, value in ipairs(db.list(ns(ctx,"namespaces"))) do
        if value.Name == name then roto.error("NamespaceAlreadyExists","Namespace already exists",409) end
    end
    local id = roto.id("ns-")
    local value = {Id=id,Arn=arn(ctx,"namespace",id),Name=name,Type="HTTP",Description=input.Description,
        CreateDate=os.time(),Properties={HttpProperties={HttpName=name}},
        Tags=roto.tags(nil,input.Tags or roto.array())}
    db.put(ns(ctx,"namespaces"),id,value)
    return operation(ctx,db,"CREATE_NAMESPACE",{NAMESPACE=id})
end
function ops.GetNamespace(ctx: Ctx, input: IdInput, db: Db)
    local value = namespace(ctx,db,input.Id)
    value.ServiceCount=0
    for _, s in ipairs(db.list(ns(ctx,"services"))) do if s.NamespaceId == value.Id then value.ServiceCount=value.ServiceCount+1 end end
    return {Namespace=public(value)}
end
function ops.ListNamespaces(ctx: Ctx, input: PagedFilterInput, db: Db)
    local values=filters(db.list(ns(ctx,"namespaces")),input,{TYPE="Type",NAME="Name",HTTP_NAME=function(v) return v.Properties.HttpProperties.HttpName end})
    for _, value in ipairs(values) do
        value.ServiceCount=0
        for _, s in ipairs(db.list(ns(ctx,"services"))) do if s.NamespaceId == value.Id then value.ServiceCount=value.ServiceCount+1 end end
        public(value)
    end
    return roto.page(input,values,"Namespaces",100,100)
end
function ops.UpdateHttpNamespace(ctx: Ctx, input: UpdateHttpNamespaceInput, db: Db)
    local value = namespace(ctx,db,input.Id)
    local update = roto.object(input.Namespace,"InvalidInput","Namespace")
    if update.Description ~= nil then value.Description=update.Description end
    db.put(ns(ctx,"namespaces"),value.Id,value)
    return operation(ctx,db,"UPDATE_NAMESPACE",{NAMESPACE=value.Id})
end
function ops.DeleteNamespace(ctx: Ctx, input: IdInput, db: Db)
    local value = namespace(ctx,db,input.Id)
    for _, s in ipairs(db.list(ns(ctx,"services"))) do if s.NamespaceId == value.Id then roto.error("ResourceInUse","Namespace has associated services") end end
    db.delete(ns(ctx,"namespaces"),value.Id)
    return operation(ctx,db,"DELETE_NAMESPACE",{NAMESPACE=value.Id})
end
function ops.CreateService(ctx: Ctx, input: CreateServiceInput, db: Db)
    namespace(ctx,db,input.NamespaceId)
    local name = roto.required(input,"Name")
    if input.DnsConfig or input.HealthCheckConfig then roto.error("InvalidInput","HTTP namespaces support custom health checks, not DNS configuration") end
    for _, value in ipairs(db.list(ns(ctx,"services"))) do
        if value.NamespaceId == input.NamespaceId and value.Name == name then roto.error("ServiceAlreadyExists","Service already exists",409) end
    end
    local id = roto.id("srv-")
    local value = {Id=id,Arn=arn(ctx,"service",id),Name=name,NamespaceId=input.NamespaceId,Type="HTTP",
        Description=input.Description,HealthCheckCustomConfig=input.HealthCheckCustomConfig,CreateDate=os.time(),
        Tags=roto.tags(nil,input.Tags or roto.array()),Revision=1,Attributes={}}
    db.put(ns(ctx,"services"),id,value)
    return {Service=service_shape(ctx,db,value)}
end
function ops.GetService(ctx: Ctx, input: IdInput, db: Db) return {Service=service_shape(ctx,db,service(ctx,db,input.Id))} end
function ops.ListServices(ctx: Ctx, input: PagedFilterInput, db: Db)
    local values=filters(db.list(ns(ctx,"services")),input,{NAMESPACE_ID="NamespaceId"})
    for _, value in ipairs(values) do service_shape(ctx,db,value); value.NamespaceId=nil end
    return roto.page(input,values,"Services",100,100)
end
function ops.UpdateService(ctx: Ctx, input: UpdateServiceInput, db: Db)
    local value = service(ctx,db,input.Id)
    local update = roto.object(input.Service,"InvalidInput","Service")
    if update.DnsConfig or update.HealthCheckConfig then roto.error("InvalidInput","DNS configuration is unsupported for HTTP namespaces") end
    roto.copy_fields(update,value,{"Description","HealthCheckCustomConfig"})
    db.put(ns(ctx,"services"),value.Id,value)
    return operation(ctx,db,"UPDATE_SERVICE",{SERVICE=value.Id})
end
function ops.DeleteService(ctx: Ctx, input: IdInput, db: Db)
    local value = service(ctx,db,input.Id)
    if #db.list(instances(ctx,value.Id)) > 0 then roto.error("ResourceInUse","Deregister instances before deleting the service") end
    db.delete(ns(ctx,"services"),value.Id)
    return {}
end
local function touch(ctx: Ctx, db: Db, value: any)
    value.Revision=value.Revision+1
    db.put(ns(ctx,"services"),value.Id,value)
end
function ops.RegisterInstance(ctx: Ctx, input: RegisterInstanceInput, db: Db)
    local value = service(ctx,db,input.ServiceId)
    local id = roto.required(input,"InstanceId")
    local attributes = roto.object(input.Attributes,"InvalidInput","Attributes")
    for k,v in pairs(attributes) do if type(k) ~= "string" or type(v) ~= "string" then roto.error("InvalidInput","Instance attributes must be strings") end end
    local health = attributes.AWS_INIT_HEALTH_STATUS or "HEALTHY"
    if health ~= "HEALTHY" and health ~= "UNHEALTHY" then roto.error("InvalidInput","Invalid initial health status") end
    db.put(instances(ctx,value.Id),id,{Id=id,Attributes=attributes,HealthStatus=health})
    touch(ctx,db,value)
    return operation(ctx,db,"REGISTER_INSTANCE",{SERVICE=value.Id,INSTANCE=id})
end
function ops.DeregisterInstance(ctx: Ctx, input: DeregisterInstanceInput, db: Db)
    local value = service(ctx,db,input.ServiceId)
    local id = roto.required(input,"InstanceId")
    get(ctx,db,"instances/" .. value.Id,id,"InstanceNotFound")
    db.delete(instances(ctx,value.Id),id)
    touch(ctx,db,value)
    return operation(ctx,db,"DEREGISTER_INSTANCE",{SERVICE=value.Id,INSTANCE=id})
end
function ops.GetInstance(ctx: Ctx, input: DeregisterInstanceInput, db: Db)
    service(ctx,db,input.ServiceId)
    local value = get(ctx,db,"instances/" .. input.ServiceId,input.InstanceId,"InstanceNotFound")
    value.HealthStatus=nil
    return {Instance=value}
end
function ops.ListInstances(ctx: Ctx, input: ListInstancesInput, db: Db)
    service(ctx,db,input.ServiceId)
    local values=db.list(instances(ctx,input.ServiceId))
    for _, value in ipairs(values) do value.HealthStatus=nil end
    return roto.page(input,values,"Instances",100,100)
end
local function named(ctx: Ctx, input: NamedInput, db: Db): any
    local nname,sname=roto.required(input,"NamespaceName"),roto.required(input,"ServiceName")
    local found: any
    for _, value in ipairs(db.list(ns(ctx,"namespaces"))) do if value.Name == nname then found=value; break end end
    if not found then roto.error("NamespaceNotFound","Namespace not found",404) end
    for _, value in ipairs(db.list(ns(ctx,"services"))) do if value.NamespaceId == found.Id and value.Name == sname then return value end end
    return roto.error("ServiceNotFound","Service not found",404)
end
function ops.DiscoverInstances(ctx: Ctx, input: DiscoverInstancesInput, db: Db)
    local value = named(ctx,input,db)
    local health = input.HealthStatus or "HEALTHY"
    if not roto.contains({"HEALTHY","UNHEALTHY","ALL","HEALTHY_OR_ELSE_ALL"},health) then roto.error("InvalidInput","Invalid HealthStatus") end
    local values,healthy=roto.array(),roto.array()
    local query=roto.object(input.QueryParameters or {},"InvalidInput","QueryParameters")
    for _, inst in ipairs(db.list(instances(ctx,value.Id))) do
        local match=true
        for k,v in pairs(query) do if inst.Attributes[k] ~= v then match=false end end
        if match then
            local out={InstanceId=inst.Id,NamespaceName=input.NamespaceName,ServiceName=input.ServiceName,
                Attributes=inst.Attributes,HealthStatus=inst.HealthStatus}
            if inst.HealthStatus == "HEALTHY" then table.insert(healthy,out) end
            if health == "ALL" or health == "HEALTHY_OR_ELSE_ALL" or health == inst.HealthStatus then table.insert(values,out) end
        end
    end
    if health == "HEALTHY_OR_ELSE_ALL" and #healthy > 0 then values=healthy end
    local max=input.MaxResults or 100
    if type(max) ~= "number" or max%1 ~= 0 or max<1 or max>1000 then roto.error("InvalidInput","Invalid MaxResults") end
    while #values>max do table.remove(values) end
    return {Instances=values,InstancesRevision=value.Revision}
end
function ops.DiscoverInstancesRevision(ctx: Ctx, input: NamedInput, db: Db) return {InstancesRevision=named(ctx,input,db).Revision} end
function ops.UpdateInstanceCustomHealthStatus(ctx: Ctx, input: UpdateInstanceCustomHealthStatusInput, db: Db)
    local value=service(ctx,db,input.ServiceId)
    local inst=get(ctx,db,"instances/" .. value.Id,input.InstanceId,"InstanceNotFound")
    if input.Status ~= "HEALTHY" and input.Status ~= "UNHEALTHY" then roto.error("InvalidInput","Invalid Status") end
    inst.HealthStatus=input.Status
    db.put(instances(ctx,value.Id),inst.Id,inst)
    touch(ctx,db,value)
    return {}
end
function ops.GetInstancesHealthStatus(ctx: Ctx, input: GetInstancesHealthStatusInput, db: Db)
    service(ctx,db,input.ServiceId)
    local values=roto.array()
    local ids=input.Instances
    if ids ~= nil then
        for _, id in ipairs(roto.list(ids,"InvalidInput","Instances")) do
            if type(id) ~= "string" then roto.error("InvalidInput","Instance ID must be a string") end
            local inst=db.get(instances(ctx,input.ServiceId),id)
            table.insert(values,{Id=id,HealthStatus=inst and inst.HealthStatus or "UNKNOWN"})
        end
    else values=db.list(instances(ctx,input.ServiceId)) end
    local page=roto.page(input,values,"Items",100,100)
    local result={Status={},NextToken=page.NextToken}
    for _, inst in ipairs(page.Items) do result.Status[inst.Id]=inst.HealthStatus end
    return result
end
function ops.GetOperation(ctx: Ctx, input: GetOperationInput, db: Db) return {Operation=get(ctx,db,"operations",input.OperationId,"OperationNotFound")} end
function ops.ListOperations(ctx: Ctx, input: PagedFilterInput, db: Db)
    local values=filters(db.list(ns(ctx,"operations")),input,{STATUS="Status",TYPE="Type",
        NAMESPACE_ID=function(v) return v.Targets.NAMESPACE end,SERVICE_ID=function(v) return v.Targets.SERVICE end})
    local result=roto.page(input,values,"Operations",100,100)
    for _, value in ipairs(result.Operations) do
        for key in pairs(value) do if key ~= "Id" and key ~= "Status" then value[key]=nil end end
    end
    return result
end
function ops.GetServiceAttributes(ctx: Ctx, input: GetServiceAttributesInput, db: Db)
    local value=service(ctx,db,input.ServiceId)
    return {ServiceAttributes={ServiceArn=value.Arn,ResourceOwner=ctx.account_id,Attributes=value.Attributes}}
end
function ops.UpdateServiceAttributes(ctx: Ctx, input: UpdateServiceAttributesInput, db: Db)
    local value=service(ctx,db,input.ServiceId)
    local attrs=roto.object(input.Attributes,"InvalidInput","Attributes")
    for k,v in pairs(attrs) do if type(v) ~= "string" then roto.error("InvalidInput","Service attributes must be strings") end; value.Attributes[k]=v end
    db.put(ns(ctx,"services"),value.Id,value)
    return {}
end
function ops.DeleteServiceAttributes(ctx: Ctx, input: UpdateServiceAttributesInput, db: Db)
    local value=service(ctx,db,input.ServiceId)
    for _, key in ipairs(roto.list(input.Attributes,"InvalidInput","Attributes")) do
        if type(key) ~= "string" then roto.error("InvalidInput","Attribute key must be a string") end
        value.Attributes[key]=nil
    end
    db.put(ns(ctx,"services"),value.Id,value)
    return {}
end
local function tagged(ctx: Ctx, input: TaggedInput, db: Db): (any, string)
    local resource=roto.required(input,"ResourceARN")
    local prefix="arn:aws:servicediscovery:" .. ctx.region .. ":" .. ctx.account_id .. ":"
    if resource:sub(1,#prefix) ~= prefix then roto.error("InvalidInput","Invalid ResourceARN") end
    local kind,id=resource:sub(#prefix+1):match("^([^/]+)/([^/]+)$")
    if kind == "namespace" then return namespace(ctx,db,id),"namespaces"
    elseif kind == "service" then return service(ctx,db,id),"services" end
    return roto.error("InvalidInput","Invalid ResourceARN")
end
function ops.TagResource(ctx: Ctx, input: TagResourceInput, db: Db)
    local value,collection=tagged(ctx,input,db)
    value.Tags=roto.tags(value.Tags,input.Tags)
    db.put(ns(ctx,collection),value.Id,value)
    return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagResourceInput, db: Db)
    local value,collection=tagged(ctx,input,db)
    value.Tags=roto.untag(value.Tags,input.TagKeys)
    db.put(ns(ctx,collection),value.Id,value)
    return {}
end
function ops.ListTagsForResource(ctx: Ctx, input: TaggedInput, db: Db) return {Tags=tagged(ctx,input,db).Tags} end
return ops
