--!strict
-- WAFv2 web ACL, IP set and rule-group configuration, tags and web ACL associations.
-- Rule documents are stored and returned, never evaluated: no request inspection, capacity
-- accounting, managed rule-group catalog, logging or resource-existence checks.
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type Doc = { [string]: any }
type Fields = { [string]: any }
type TaggedInput = { ResourceARN: any }
type TagResourceInput = { ResourceARN: any, Tags: any }
type UntagResourceInput = { ResourceARN: any, TagKeys: any }
type ListTagsInput = { ResourceARN: any, Limit: any, NextMarker: any }
type ListInput = { Scope: any, Limit: any, NextMarker: any }
type AssociateInput = { WebACLArn: any, ResourceArn: any }
type ResourceInput = { ResourceArn: any }
type ListResourcesInput = { WebACLArn: any, ResourceType: any }
type Kind = {
    path: string, ns: string, label: string?,
    createRequired: { string }, updateRequired: { string },
    createFields: { string }, updateFields: { string }, immutable: { string },
    validate: (ctx: Ctx, global: boolean, input: Fields, db: Db) -> (),
}
local ops = {}
local kinds: { [string]: Kind }={}
local code = 'WAFInvalidParameterException'
local missing = 'WAFNonexistentItemException'

local function visibility(value: any, label: string)
    roto.object(value,code,label)
    local metric=roto.required(value,'MetricName',code)
    if #metric>128 or not metric:match('^[%w_#:%.%-/]+$') then roto.error(code,'Invalid MetricName') end
    if type(value.SampledRequestsEnabled)~='boolean' or type(value.CloudWatchMetricsEnabled)~='boolean' then
        roto.error(code,label..' requires boolean SampledRequestsEnabled and CloudWatchMetricsEnabled')
    end
end
local function default_action(value: any)
    roto.object(value,code,'DefaultAction')
    if (value.Allow==nil)==(value.Block==nil) then roto.error(code,'DefaultAction must specify exactly one of Allow or Block') end
end

-- Parsed WAFv2 ARN: arn:aws:wafv2:<region>:<account>:<regional|global>/<path>/<name>/<id>
type Parsed = { region: string, account: string, global: boolean, path: string, name: string, id: string }
local function parse_arn(value: any): Parsed?
    if type(value)~='string' then return nil end
    local arn: string=value
    local region,account,scope,path,name,id=string.match(arn,'^arn:aws:wafv2:([^:]*):([^:]*):(%a+)/(%a+)/([%w_%-]+)/([%w%-]+)$')
    if not region or not account or not scope or not path or not name or not id or (scope~='regional' and scope~='global') then return nil end
    return {region=region,account=account,global=scope=='global',path=path,name=name,id=id}
end
-- Parses an ARN that must belong to the caller's account and region and name a known kind.
local function resolve(ctx: Ctx, value: any, label: string): (Parsed, Kind)
    local parsed=parse_arn(value)
    if not parsed then roto.error(code,label..' must be a WAFv2 ARN') end
    local kind=kinds[(parsed :: Parsed).path]
    if not kind then roto.error(code,label..' names an unknown WAFv2 entity type') end
    local arn=parsed :: Parsed
    if arn.region~=ctx.region or arn.account~=ctx.account_id then roto.error(code,label..' must match the caller account and region') end
    return arn,kind :: Kind
end
local function ns(ctx: Ctx, kind: Kind, global: boolean) return roto.scope(ctx,kind.ns,global) end
local function is_global(ctx: Ctx, input: { Scope: any }): boolean
    local scope=input.Scope
    if scope~='REGIONAL' and scope~='CLOUDFRONT' then roto.error(code,'Scope must be REGIONAL or CLOUDFRONT') end
    if scope=='CLOUDFRONT' and ctx.region~='us-east-1' then roto.error(code,'CLOUDFRONT scope requires the us-east-1 endpoint') end
    return scope=='CLOUDFRONT'
end
local function make_arn(ctx: Ctx, kind: Kind, global: boolean, name: string, id: string): string
    return 'arn:aws:wafv2:'..ctx.region..':'..ctx.account_id..':'..(global and 'global' or 'regional')..'/'..kind.path..'/'..name..'/'..id
end
local function public(doc: Doc): Doc
    local out: Doc={};for k,v in pairs(doc) do if tostring(k):sub(1,1)~='_' then out[k]=v end end;return out
end
local function summary(doc: Doc): Doc
    return {Name=doc.Name,Id=doc.Id,Description=doc.Description,LockToken=doc._LockToken,ARN=doc.ARN}
end
local function nonexistent(kind: Kind, name: any): never
    return roto.error(missing,'WAF '..kind.path..' not found: '..tostring(name))
end

-- Statement references to other entities, collected recursively.
local function references(node: any, out: { string })
    if type(node)~='table' then return end
    for key,value in pairs(node) do
        if (key=='IPSetReferenceStatement' or key=='RuleGroupReferenceStatement') and type(value)=='table' then
            local ref: Fields=value :: any
            if type(ref.ARN)=='string' then table.insert(out,ref.ARN) end
        end
        references(value,out)
    end
end
local function rules(ctx: Ctx, global: boolean, input: Fields, db: Db, kinds: { [string]: Kind }, rule_group: boolean)
    if input.Rules==nil then return end
    local seen_names: { [string]: boolean }={};local seen_priorities: { [number]: boolean }={}
    for _,rule in ipairs(roto.list(input.Rules,code,'Rules')) do
        roto.object(rule,code,'Rule')
        local name=roto.required(rule,'Name',code)
        if seen_names[name] then roto.error(code,'Duplicate rule name: '..name) end
        seen_names[name]=true
        local priority=rule.Priority
        if type(priority)~='number' or priority%1~=0 or priority<0 then roto.error(code,'Rule Priority must be a non-negative integer') end
        if seen_priorities[priority] then roto.error(code,'Duplicate rule priority: '..priority) end
        seen_priorities[priority]=true
        roto.object(rule.Statement,code,'Statement')
        visibility(rule.VisibilityConfig,'Rule VisibilityConfig')
        if rule_group then
            if rule.Action==nil then roto.error(code,'Rule group rules require Action') end
        elseif (rule.Action==nil)==(rule.OverrideAction==nil) then roto.error(code,'Rules require exactly one of Action or OverrideAction') end
    end
    local refs: { string }={};references(input.Rules,refs)
    for _,arn in ipairs(refs) do
        local parsed,kind=resolve(ctx,arn,'Reference ARN')
        if kind.path=='webacl' or parsed.global~=global then roto.error(code,'Invalid reference ARN for this scope: '..arn) end
        local doc: Doc?=db.get(ns(ctx,kind,global),parsed.id)
        if not doc or doc.Name~=parsed.name then roto.error(missing,'Referenced item not found: '..arn) end
    end
end

local function ip_valid(version: string, address: any): boolean
    if type(address)~='string' then return false end
    if version=='IPV4' then
        local a,b,c,d,len=string.match(address,'^(%d+)%.(%d+)%.(%d+)%.(%d+)/(%d+)$')
        if not a or not b or not c or not d or not len then return false end
        for _,octet in ipairs({a,b,c,d}) do if (tonumber(octet) :: number)>255 then return false end end
        local bits=tonumber(len) :: number
        return bits>=1 and bits<=32
    end
    local host,len=string.match(address,'^([%x:]+)/(%d+)$')
    if not host or not len or not host:find(':',1,true) or select(2,host:gsub('::','')) > 1 then return false end
    local bits=tonumber(len) :: number
    return bits>=1 and bits<=128
end
kinds.webacl={
    path='webacl',ns='webacls',label='webacl',
    createRequired={'DefaultAction','VisibilityConfig'},updateRequired={'DefaultAction','VisibilityConfig'},
    createFields={'DefaultAction','Description','Rules','VisibilityConfig','DataProtectionConfig','CustomResponseBodies','CaptchaConfig','ChallengeConfig','TokenDomains','AssociationConfig','OnSourceDDoSProtectionConfig','ApplicationConfig'},
    updateFields={'DefaultAction','Description','Rules','VisibilityConfig','DataProtectionConfig','CustomResponseBodies','CaptchaConfig','ChallengeConfig','TokenDomains','AssociationConfig','OnSourceDDoSProtectionConfig','ApplicationConfig'},
    immutable={'LabelNamespace'},
    validate=function(ctx: Ctx, global: boolean, input: Fields, db: Db)
        default_action(input.DefaultAction);visibility(input.VisibilityConfig,'VisibilityConfig')
        rules(ctx,global,input,db,kinds,false)
    end,
}
kinds.ipset={
    path='ipset',ns='ipsets',
    createRequired={'IPAddressVersion','Addresses'},updateRequired={'Addresses'},
    createFields={'Description','IPAddressVersion','Addresses'},updateFields={'Description','Addresses'},immutable={'IPAddressVersion'},
    validate=function(ctx: Ctx, global: boolean, input: Fields, db: Db)
        local version=input.IPAddressVersion
        if version==nil then
            local old: Doc?=db.get(ns(ctx,kinds.ipset,global),tostring(input.Id))
            version=old and old.IPAddressVersion
        end
        if version~='IPV4' and version~='IPV6' then roto.error(code,'IPAddressVersion must be IPV4 or IPV6') end
        local addresses=roto.list(input.Addresses,code,'Addresses')
        if #addresses>10000 then roto.error('WAFLimitsExceededException','An IP set holds at most 10000 addresses') end
        for _,address in ipairs(addresses) do
            if not ip_valid(version,address) then roto.error(code,'Invalid '..version..' CIDR: '..tostring(address)) end
        end
    end,
}
kinds.rulegroup={
    path='rulegroup',ns='rulegroups',label='rulegroup',
    createRequired={'Capacity','VisibilityConfig'},updateRequired={'VisibilityConfig'},
    createFields={'Capacity','Description','Rules','VisibilityConfig','CustomResponseBodies'},updateFields={'Description','Rules','VisibilityConfig','CustomResponseBodies'},
    immutable={'Capacity','LabelNamespace'},
    validate=function(ctx: Ctx, global: boolean, input: Fields, db: Db)
        if input.Capacity~=nil and (type(input.Capacity)~='number' or input.Capacity%1~=0 or input.Capacity<1 or input.Capacity>5000) then
            roto.error(code,'Capacity must be an integer between 1 and 5000')
        end
        visibility(input.VisibilityConfig,'VisibilityConfig')
        rules(ctx,global,input,db,kinds,true)
    end,
}

local function sorted(ctx: Ctx, kind: Kind, global: boolean, db: Db): { Doc }
    local docs: { Doc }={}
    for _,doc in ipairs(db.list(ns(ctx,kind,global))) do table.insert(docs,doc) end
    table.sort(docs,function(a: Doc, b: Doc) if a.Name~=b.Name then return a.Name<b.Name end; return a.Id<b.Id end)
    return docs
end
local function find(ctx: Ctx, kind: Kind, input: Fields, db: Db, allow_arn: boolean): (Doc, boolean)
    if allow_arn and input.ARN~=nil and input.Name==nil and input.Id==nil then
        local parsed,found=resolve(ctx,input.ARN,'ARN')
        if found.path~=kind.path then roto.error(code,'Invalid ARN for '..kind.path) end
        local doc: Doc?=db.get(ns(ctx,kind,parsed.global),parsed.id)
        if not doc or doc.Name~=parsed.name then nonexistent(kind,input.ARN) end
        return doc :: Doc,parsed.global
    end
    local global=is_global(ctx,input)
    local name=roto.required(input,'Name',code);local id=roto.required(input,'Id',code)
    local doc: Doc?=db.get(ns(ctx,kind,global),id)
    if not doc or doc.Name~=name then nonexistent(kind,name) end
    return doc :: Doc,global
end
local function check_lock(doc: Doc, input: Fields)
    local token=roto.required(input,'LockToken',code)
    if token~=doc._LockToken then roto.error('WAFOptimisticLockException','The lock token is stale; read the item again and retry') end
end
-- Associated-item protection: ACLs referenced by associations, sets/groups referenced by rules.
local function ensure_unreferenced(ctx: Ctx, kind: Kind, global: boolean, doc: Doc, db: Db)
    if kind.path=='webacl' then
        for _,assoc in ipairs(db.list(roto.scope(ctx,'associations'))) do
            if assoc.WebACLArn==doc.ARN then roto.error('WAFAssociatedItemException','Web ACL is associated with a resource') end
        end
        return
    end
    for _,owner in ipairs({kinds.webacl,kinds.rulegroup}) do
        for _,other in ipairs(db.list(ns(ctx,owner,global))) do
            local refs: { string }={};references(other.Rules,refs)
            if roto.contains(refs,doc.ARN) then roto.error('WAFAssociatedItemException','Item is referenced by '..tostring(other.Name)) end
        end
    end
end

local function create(ctx: Ctx, kind: Kind, input: Fields, db: Db): Doc
    local global=is_global(ctx,input)
    local name=roto.required(input,'Name',code)
    if #name>128 or not name:match('^[%w_%-]+$') then roto.error(code,'Invalid Name') end
    for _,field in ipairs(kind.createRequired) do if input[field]==nil then roto.error(code,field..' is required') end end
    for _,existing in ipairs(db.list(ns(ctx,kind,global))) do
        if existing.Name==name then roto.error('WAFDuplicateItemException','An item named '..name..' already exists') end
    end
    kind.validate(ctx,global,input,db)
    local id=roto.uuid()
    local doc: Doc={Name=name,Id=id,ARN=make_arn(ctx,kind,global,name,id)}
    if kind.label then doc.LabelNamespace='awswaf:'..ctx.account_id..':'..kind.label..':'..name..':' end
    roto.copy_fields(input,doc,kind.createFields)
    doc._LockToken=roto.uuid();doc._Tags=roto.tags({},input.Tags or roto.array(),code)
    db.put(ns(ctx,kind,global),id,doc)
    return doc
end
local function update(ctx: Ctx, kind: Kind, input: Fields, db: Db): Doc
    local old,global=find(ctx,kind,input,db,false)
    check_lock(old,input)
    for _,field in ipairs(kind.updateRequired) do if input[field]==nil then roto.error(code,field..' is required') end end
    kind.validate(ctx,global,input,db)
    -- Updates replace the whole definition: omitted optional fields are removed.
    local doc: Doc={Name=old.Name,Id=old.Id,ARN=old.ARN}
    roto.copy_fields(old,doc,kind.immutable)
    roto.copy_fields(input,doc,kind.updateFields)
    doc._Tags=old._Tags;doc._LockToken=roto.uuid()
    db.put(ns(ctx,kind,global),old.Id,doc)
    return doc
end
local function delete(ctx: Ctx, kind: Kind, input: Fields, db: Db)
    local doc,global=find(ctx,kind,input,db,false)
    check_lock(doc,input)
    ensure_unreferenced(ctx,kind,global,doc,db)
    db.delete(ns(ctx,kind,global),doc.Id)
end
local function list(ctx: Ctx, kind: Kind, key: string, input: ListInput, db: Db): Doc
    local global=is_global(ctx,input)
    local values: { any }=roto.array()
    for _,doc in ipairs(sorted(ctx,kind,global,db)) do table.insert(values,summary(doc)) end
    local out=roto.page({Limit=input.Limit,NextToken=input.NextMarker},values,key,100,100,code)
    out.NextMarker=out.NextToken;out.NextToken=nil;return out
end

function ops.CreateWebACL(ctx: Ctx, input: Fields, db: Db) return {Summary=summary(create(ctx,kinds.webacl,input,db))} end
function ops.GetWebACL(ctx: Ctx, input: Fields, db: Db)
    local doc=find(ctx,kinds.webacl,input,db,true);return {WebACL=public(doc),LockToken=doc._LockToken}
end
function ops.UpdateWebACL(ctx: Ctx, input: Fields, db: Db) return {NextLockToken=update(ctx,kinds.webacl,input,db)._LockToken} end
function ops.DeleteWebACL(ctx: Ctx, input: Fields, db: Db) delete(ctx,kinds.webacl,input,db);return {} end
function ops.ListWebACLs(ctx: Ctx, input: ListInput, db: Db) return list(ctx,kinds.webacl,'WebACLs',input,db) end

function ops.CreateIPSet(ctx: Ctx, input: Fields, db: Db) return {Summary=summary(create(ctx,kinds.ipset,input,db))} end
function ops.GetIPSet(ctx: Ctx, input: Fields, db: Db)
    local doc=find(ctx,kinds.ipset,input,db,false);return {IPSet=public(doc),LockToken=doc._LockToken}
end
function ops.UpdateIPSet(ctx: Ctx, input: Fields, db: Db) return {NextLockToken=update(ctx,kinds.ipset,input,db)._LockToken} end
function ops.DeleteIPSet(ctx: Ctx, input: Fields, db: Db) delete(ctx,kinds.ipset,input,db);return {} end
function ops.ListIPSets(ctx: Ctx, input: ListInput, db: Db) return list(ctx,kinds.ipset,'IPSets',input,db) end

function ops.CreateRuleGroup(ctx: Ctx, input: Fields, db: Db) return {Summary=summary(create(ctx,kinds.rulegroup,input,db))} end
function ops.GetRuleGroup(ctx: Ctx, input: Fields, db: Db)
    local doc=find(ctx,kinds.rulegroup,input,db,true);return {RuleGroup=public(doc),LockToken=doc._LockToken}
end
function ops.UpdateRuleGroup(ctx: Ctx, input: Fields, db: Db) return {NextLockToken=update(ctx,kinds.rulegroup,input,db)._LockToken} end
function ops.DeleteRuleGroup(ctx: Ctx, input: Fields, db: Db) delete(ctx,kinds.rulegroup,input,db);return {} end
function ops.ListRuleGroups(ctx: Ctx, input: ListInput, db: Db) return list(ctx,kinds.rulegroup,'RuleGroups',input,db) end

-- Associations are regional only. Resource ARNs are checked for shape, region and account,
-- not for existence, since the owning services are not consulted.
local function resource_type(ctx: Ctx, arn: any): string
    if type(arn)~='string' then roto.error(code,'ResourceArn must be a string') end
    local at='arn:aws:%s:'..ctx.region..':'..ctx.account_id..':'
    local function prefix(service: string, tail: string): string return at:format(service)..tail end
    if arn:sub(1,#prefix('elasticloadbalancing','loadbalancer/app/'))==prefix('elasticloadbalancing','loadbalancer/app/') then return 'APPLICATION_LOAD_BALANCER' end
    if arn:sub(1,#prefix('appsync','apis/'))==prefix('appsync','apis/') then return 'APPSYNC' end
    if arn:sub(1,#prefix('cognito-idp','userpool/'))==prefix('cognito-idp','userpool/') then return 'COGNITO_USER_POOL' end
    if arn:sub(1,#prefix('apprunner','service/'))==prefix('apprunner','service/') then return 'APP_RUNNER_SERVICE' end
    if arn:sub(1,#prefix('ec2','verified-access-instance/'))==prefix('ec2','verified-access-instance/') then return 'VERIFIED_ACCESS_INSTANCE' end
    if arn:sub(1,#prefix('amplify','apps/'))==prefix('amplify','apps/') then return 'AMPLIFY' end
    local gateway='arn:aws:apigateway:'..ctx.region..'::/restapis/'
    if arn:sub(1,#gateway)==gateway and arn:sub(#gateway+1):match('^[^/]+/stages/[^/]+$') then return 'API_GATEWAY' end
    return roto.error(code,'Unsupported or out-of-scope resource ARN: '..arn)
end
local function regional_acl(ctx: Ctx, arn: any, db: Db): Doc
    local parsed=resolve(ctx,arn,'WebACLArn')
    if parsed.path~='webacl' or parsed.global then roto.error(code,'WebACLArn must be a regional web ACL') end
    local doc: Doc?=db.get(ns(ctx,kinds.webacl,false),parsed.id)
    if not doc or doc.Name~=parsed.name then nonexistent(kinds.webacl,arn) end
    return doc :: Doc
end
function ops.AssociateWebACL(ctx: Ctx, input: AssociateInput, db: Db)
    resource_type(ctx,input.ResourceArn)
    local acl=regional_acl(ctx,input.WebACLArn,db)
    db.put(roto.scope(ctx,'associations'),input.ResourceArn,{ResourceArn=input.ResourceArn,WebACLArn=acl.ARN})
    return {}
end
function ops.DisassociateWebACL(ctx: Ctx, input: ResourceInput, db: Db)
    resource_type(ctx,input.ResourceArn);db.delete(roto.scope(ctx,'associations'),input.ResourceArn);return {}
end
function ops.GetWebACLForResource(ctx: Ctx, input: ResourceInput, db: Db)
    resource_type(ctx,input.ResourceArn)
    local assoc: Doc?=db.get(roto.scope(ctx,'associations'),input.ResourceArn)
    if not assoc then return {} end
    return {WebACL=public(regional_acl(ctx,assoc.WebACLArn,db))}
end
function ops.ListResourcesForWebACL(ctx: Ctx, input: ListResourcesInput, db: Db)
    local acl=regional_acl(ctx,input.WebACLArn,db)
    local wanted=input.ResourceType
    if wanted~=nil and not roto.contains({'APPLICATION_LOAD_BALANCER','API_GATEWAY','APPSYNC','COGNITO_USER_POOL','APP_RUNNER_SERVICE','VERIFIED_ACCESS_INSTANCE','AMPLIFY'},wanted) then
        roto.error(code,'Invalid ResourceType')
    end
    local arns: { string }={}
    for _,assoc in ipairs(db.list(roto.scope(ctx,'associations'))) do
        if assoc.WebACLArn==acl.ARN and (wanted==nil or resource_type(ctx,assoc.ResourceArn)==wanted) then table.insert(arns,assoc.ResourceArn) end
    end
    table.sort(arns)
    return {ResourceArns=roto.array(arns)}
end

local function tagged(ctx: Ctx, input: TaggedInput, db: Db): (Doc, Kind, boolean)
    local parsed,kind=resolve(ctx,input.ResourceARN,'ResourceARN')
    local doc: Doc?=db.get(ns(ctx,kind,parsed.global),parsed.id)
    if not doc or doc.Name~=parsed.name then roto.error(missing,'Resource not found: '..tostring(input.ResourceARN)) end
    return doc :: Doc,kind,parsed.global
end
function ops.TagResource(ctx: Ctx, input: TagResourceInput, db: Db)
    local doc,kind,global=tagged(ctx,input,db);doc._Tags=roto.tags(doc._Tags,input.Tags,code);db.put(ns(ctx,kind,global),doc.Id,doc);return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagResourceInput, db: Db)
    local doc,kind,global=tagged(ctx,input,db);doc._Tags=roto.untag(doc._Tags,input.TagKeys,code);db.put(ns(ctx,kind,global),doc.Id,doc);return {}
end
function ops.ListTagsForResource(ctx: Ctx, input: ListTagsInput, db: Db)
    local doc=tagged(ctx,input,db)
    local out=roto.page({Limit=input.Limit,NextToken=input.NextMarker},doc._Tags,'TagList',100,100,code)
    local info: Doc={ResourceARN=doc.ARN,TagList=out.TagList}
    return {NextMarker=out.NextToken,TagInfoForResource=info}
end
return ops
