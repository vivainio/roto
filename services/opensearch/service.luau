--!strict
-- Domain configuration only; no search backend or advertised data endpoint.
type Tag = { Key: string, Value: string }
-- The stored domain document. The settings in `fields` are kept as the caller supplied them.
type Domain = {
    DomainName: string, DomainId: string, ARN: string, CreatedAt: number, UpdatedAt: number,
    Revision: number, EngineVersion: string, ClusterConfig: { [string]: any }, Tags: { Tag },
    [string]: any,
}
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type DomainRef = { DomainName: any }
type DomainSettings = {
    DomainName: any, TagList: any, EngineVersion: any, ClusterConfig: any, EBSOptions: any,
    AccessPolicies: any, IPAddressType: any, AdvancedOptions: any, EncryptionAtRestOptions: any,
    NodeToNodeEncryptionOptions: any, DomainEndpointOptions: any, SnapshotOptions: any,
    LogPublishingOptions: any, SoftwareUpdateOptions: any,
}
type DescribeDomainsInput = { DomainNames: any }
type ListDomainNamesInput = { EngineType: any }
type ArnRef = { ARN: any }
type AddTagsInput = { ARN: any, TagList: any }
type RemoveTagsInput = { ARN: any, TagKeys: any }
local M = {}
local fields = {'EngineVersion','ClusterConfig','EBSOptions','AccessPolicies','IPAddressType','AdvancedOptions','EncryptionAtRestOptions','NodeToNodeEncryptionOptions','DomainEndpointOptions','SnapshotOptions','LogPublishingOptions','SoftwareUpdateOptions'}
local function ns(ctx: Ctx): string return roto.scope(ctx,'domains') end
local function get(ctx: Ctx, input: DomainRef, db: Db): Domain
    local name=roto.required(input,'DomainName','ValidationException')
    local domain: Domain? = db.get(ns(ctx),name)
    if not domain then roto.error('ResourceNotFoundException','Domain not found: '..name,404) end
    return domain :: Domain -- roto.error above does not narrow the checker
end
local function check(input: DomainSettings)
    for k in pairs(input :: any) do
        if k ~= 'DomainName' and k ~= 'TagList' and not roto.contains(fields,k) then
            roto.error('NotImplemented','Unsupported domain setting: '..tostring(k),501)
        end
    end
    if input.ClusterConfig and input.ClusterConfig.InstanceCount and input.ClusterConfig.InstanceCount<1 then roto.error('ValidationException','InstanceCount must be positive') end
    if input.DomainEndpointOptions and input.DomainEndpointOptions.CustomEndpointEnabled then
        roto.error('NotImplemented','Custom endpoint provisioning is unsupported',501)
    end
end
local function status(d: Domain): { [string]: any }
    local out={DomainId=d.DomainId,DomainName=d.DomainName,ARN=d.ARN,Created=true,Deleted=false,Processing=false,UpgradeProcessing=false}
    roto.copy_fields(d,out,fields)
    return out
end
local function config(d: Domain): { [string]: any }
    local out={}
    for _,key in ipairs(fields) do
        if d[key] ~= nil then out[key]={Options=d[key],Status={CreationDate=d.CreatedAt,UpdateDate=d.UpdatedAt,UpdateVersion=d.Revision,State='Active',PendingDeletion=false}} end
    end
    return out
end
function M.CreateDomain(ctx: Ctx, input: DomainSettings, db: Db)
    check(input)
    local name=roto.required(input,'DomainName','ValidationException')
    if #name<3 or #name>28 or not name:match('^[a-z][a-z0-9%-]+$') then roto.error('ValidationException','Invalid domain name') end
    if db.get(ns(ctx),name) then roto.error('ResourceAlreadyExistsException','Domain exists') end
    local now=os.time()
    local d={DomainName=name,DomainId=ctx.account_id..'/'..name,ARN='arn:aws:es:'..ctx.region..':'..ctx.account_id..':domain/'..name,EngineVersion='OpenSearch_2.11',ClusterConfig={InstanceType='m6g.large.search',InstanceCount=1,DedicatedMasterEnabled=false,ZoneAwarenessEnabled=false},CreatedAt=now,UpdatedAt=now,Revision=1}
    roto.copy_fields(input,d,fields)
    d.Tags=roto.tags({},input.TagList or roto.array(),'ValidationException')
    db.put(ns(ctx),name,d)
    return {DomainStatus=status(d)}
end
function M.DescribeDomain(ctx: Ctx, input: DomainRef, db: Db) return {DomainStatus=status(get(ctx,input,db))} end
function M.DescribeDomains(ctx: Ctx, input: DescribeDomainsInput, db: Db)
    local out=roto.array()
    for _,name in ipairs(roto.list(input.DomainNames,'ValidationException','DomainNames')) do table.insert(out,status(get(ctx,{DomainName=name},db))) end
    return {DomainStatusList=out}
end
function M.ListDomainNames(ctx: Ctx, input: ListDomainNamesInput, db: Db)
    local out=roto.array()
    if input.EngineType and input.EngineType~='OpenSearch' and input.EngineType~='Elasticsearch' then roto.error('ValidationException','Invalid EngineType') end
    local domains: { Domain } = db.list(ns(ctx))
    for _,d in ipairs(domains) do
        local engine=d.EngineVersion:match('^Elasticsearch') and 'Elasticsearch' or 'OpenSearch'
        if not input.EngineType or input.EngineType==engine then table.insert(out,{DomainName=d.DomainName,EngineType=engine}) end
    end
    return {DomainNames=out}
end
function M.DescribeDomainConfig(ctx: Ctx, input: DomainRef, db: Db) return {DomainConfig=config(get(ctx,input,db))} end
function M.UpdateDomainConfig(ctx: Ctx, input: DomainSettings, db: Db)
    check(input)
    local d=get(ctx,input,db)
    roto.copy_fields(input,d,fields)
    d.UpdatedAt=os.time();d.Revision=d.Revision+1
    db.put(ns(ctx),d.DomainName,d)
    return {DomainConfig=config(d)}
end
function M.DeleteDomain(ctx: Ctx, input: DomainRef, db: Db)
    local d=get(ctx,input,db);db.delete(ns(ctx),d.DomainName)
    local out=status(d);out.Deleted=true
    return {DomainStatus=out}
end
local function tagged(ctx: Ctx, input: ArnRef, db: Db): Domain
    local arn=roto.required(input,'ARN','ValidationException')
    local domains: { Domain } = db.list(ns(ctx))
    for _,d in ipairs(domains) do if d.ARN==arn then return d end end
    return roto.error('ResourceNotFoundException','Domain ARN not found',404)
end
function M.AddTags(ctx: Ctx, input: AddTagsInput, db: Db)
    local d=tagged(ctx,input,db);d.Tags=roto.tags(d.Tags,input.TagList,'ValidationException');db.put(ns(ctx),d.DomainName,d);return {}
end
function M.RemoveTags(ctx: Ctx, input: RemoveTagsInput, db: Db)
    local d=tagged(ctx,input,db);d.Tags=roto.untag(d.Tags,input.TagKeys,'ValidationException');db.put(ns(ctx),d.DomainName,d);return {}
end
function M.ListTags(ctx: Ctx, input: ArnRef, db: Db) return {TagList=tagged(ctx,input,db).Tags} end
return M
