--!strict
-- ECR repository configuration. No image registry, scan or lifecycle engine.
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type RegistryInput = { registryId: any }
type GetInput = { registryId: any, repositoryName: any }
type MutabilityInput = { imageTagMutability: any, imageTagMutabilityExclusionFilters: any }
type ScanningInput = { imageScanningConfiguration: any }
type CreateRepositoryInput = { encryptionConfiguration: any, imageScanningConfiguration: any, imageTagMutability: any, imageTagMutabilityExclusionFilters: any, registryId: any, repositoryName: any, tags: any }
type DescribeRepositoriesInput = { maxResults: any, nextToken: any, registryId: any, repositoryNames: any }
type PutImageTagMutabilityInput = { imageTagMutability: any, imageTagMutabilityExclusionFilters: any, registryId: any, repositoryName: any }
type PutImageScanningConfigurationInput = { imageScanningConfiguration: any, registryId: any, repositoryName: any }
type TaggedInput = { resourceArn: any }
type TagResourceInput = { resourceArn: any, tags: any }
type UntagResourceInput = { resourceArn: any, tagKeys: any }
local ops = {}
local code = 'InvalidParameterException'
local function ns(ctx: Ctx) return roto.scope(ctx,'repositories') end
local function registry(ctx: Ctx, input: RegistryInput)
    if input.registryId and input.registryId ~= ctx.account_id then roto.error(code,'registryId must match the caller account') end
end
local function get(ctx: Ctx, input: GetInput, db: Db)
    registry(ctx,input)
    local name=roto.required(input,'repositoryName',code)
    local repo=db.get(ns(ctx),name)
    if not repo then roto.error('RepositoryNotFoundException','Repository not found: '..name) end
    return repo
end
local function public(repo: { [string]: any }): { [string]: any }
    local out={};for k,v in pairs(repo) do if tostring(k):sub(1,1)~='_' then out[k]=v end end;return out
end
local function mutability(input: MutabilityInput)
    local value: string=input.imageTagMutability or 'MUTABLE'
    if not roto.contains({'MUTABLE','IMMUTABLE','MUTABLE_WITH_EXCLUSION','IMMUTABLE_WITH_EXCLUSION'},value) then roto.error(code,'Invalid imageTagMutability') end
    local filters=input.imageTagMutabilityExclusionFilters
    if value:match('_WITH_EXCLUSION$') then
        roto.list(filters,code,'imageTagMutabilityExclusionFilters')
        if #filters<1 or #filters>5 then roto.error(code,'Exclusion modes require 1–5 filters') end
        for _,filter in ipairs(filters) do
            roto.object(filter,code,'Filter')
            if filter.filterType~='WILDCARD' then roto.error(code,'Unsupported filter type') end
            roto.required(filter,'filter',code)
        end
    elseif filters then roto.error(code,'Exclusion filters require an exclusion mutability mode') end
    return value,filters
end
local function scanning(input: ScanningInput)
    local value=input.imageScanningConfiguration or {scanOnPush=false}
    roto.object(value,code,'imageScanningConfiguration')
    if value.scanOnPush~=nil and type(value.scanOnPush)~='boolean' then roto.error(code,'scanOnPush must be boolean') end
    return {scanOnPush=value.scanOnPush or false}
end
function ops.CreateRepository(ctx: Ctx, input: CreateRepositoryInput, db: Db)
    registry(ctx,input)
    local name=roto.required(input,'repositoryName',code)
    if #name<2 or #name>256 then roto.error(code,'Invalid repository name') end
    for segment in (name..'/'):gmatch('(.-)/') do
        if not segment:match('^[a-z0-9][a-z0-9._%-]*$') or not segment:match('[a-z0-9]$') or segment:find('%.%.',1) then roto.error(code,'Invalid repository name') end
    end
    if db.get(ns(ctx),name) then roto.error('RepositoryAlreadyExistsException','Repository exists') end
    if input.encryptionConfiguration then
        roto.object(input.encryptionConfiguration,code,'encryptionConfiguration')
        if input.encryptionConfiguration.encryptionType~='AES256' then roto.error('NotImplemented','KMS repository encryption is unsupported',501) end
        if input.encryptionConfiguration.kmsKey then roto.error(code,'AES256 cannot specify a KMS key') end
    end
    local mode,filters=mutability(input)
    local repo={repositoryName=name,registryId=ctx.account_id,repositoryArn='arn:aws:ecr:'..ctx.region..':'..ctx.account_id..':repository/'..name,repositoryUri=ctx.account_id..'.dkr.ecr.'..ctx.region..'.amazonaws.com/'..name,createdAt=os.time(),imageTagMutability=mode,imageTagMutabilityExclusionFilters=filters,imageScanningConfiguration=scanning(input),encryptionConfiguration={encryptionType='AES256'},_Tags=roto.tags({},input.tags or roto.array(),code)}
    db.put(ns(ctx),name,repo);return {repository=public(repo)}
end
function ops.DescribeRepositories(ctx: Ctx, input: DescribeRepositoriesInput, db: Db)
    registry(ctx,input)
    local values=roto.array()
    if input.repositoryNames then
        roto.list(input.repositoryNames,code,'repositoryNames')
        if #input.repositoryNames==0 or #input.repositoryNames>100 then roto.error(code,'Specify 1–100 repository names') end
        if input.nextToken or input.maxResults then roto.error(code,'Pagination cannot be combined with repositoryNames') end
        for _,name in ipairs(input.repositoryNames) do table.insert(values,public(get(ctx,{repositoryName=name},db))) end
        return {repositories=values}
    end
    for _,repo in ipairs(db.list(ns(ctx))) do table.insert(values,public(repo)) end
    local out=roto.page({MaxResults=input.maxResults,NextToken=input.nextToken},values,'repositories',100,1000,code)
    out.nextToken=out.NextToken;out.NextToken=nil;return out
end
function ops.DeleteRepository(ctx: Ctx, input: GetInput, db: Db)
    local repo=get(ctx,input,db);db.delete(ns(ctx),repo.repositoryName);return {repository=public(repo)}
end
function ops.PutImageTagMutability(ctx: Ctx, input: PutImageTagMutabilityInput, db: Db)
    roto.required(input,'imageTagMutability',code)
    local repo=get(ctx,input,db);repo.imageTagMutability,repo.imageTagMutabilityExclusionFilters=mutability(input)
    db.put(ns(ctx),repo.repositoryName,repo)
    return {registryId=ctx.account_id,repositoryName=repo.repositoryName,imageTagMutability=repo.imageTagMutability,imageTagMutabilityExclusionFilters=repo.imageTagMutabilityExclusionFilters}
end
function ops.PutImageScanningConfiguration(ctx: Ctx, input: PutImageScanningConfigurationInput, db: Db)
    roto.object(input.imageScanningConfiguration,code,'imageScanningConfiguration')
    local repo=get(ctx,input,db);repo.imageScanningConfiguration=scanning(input);db.put(ns(ctx),repo.repositoryName,repo)
    return {registryId=ctx.account_id,repositoryName=repo.repositoryName,imageScanningConfiguration=repo.imageScanningConfiguration}
end
local function policy(ctx: Ctx, input: GetInput, db: Db, key, field, missing, action)
    local repo=get(ctx,input,db)
    if action=='put' then
        local text=roto.required(input,field,code)
        local value=roto.object(roto.json_decode(text,code),code,'Policy')
        if key=='_Lifecycle' then
            local rules=roto.list(value.rules,code,'rules');if #rules==0 then roto.error(code,'Lifecycle policy rules cannot be empty') end
        elseif type(value.Statement)~='table' then roto.error(code,'Repository policy must contain Statement') end
        repo[key]=text
    elseif not repo[key] then roto.error(missing,'Policy not found') end
    local result={registryId=ctx.account_id,repositoryName=repo.repositoryName,[field]=repo[key]}
    if action=='delete' then repo[key]=nil end
    if action~='get' then db.put(ns(ctx),repo.repositoryName,repo) end
    return result
end
function ops.SetRepositoryPolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Policy','policyText','RepositoryPolicyNotFoundException','put') end
function ops.GetRepositoryPolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Policy','policyText','RepositoryPolicyNotFoundException','get') end
function ops.DeleteRepositoryPolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Policy','policyText','RepositoryPolicyNotFoundException','delete') end
function ops.PutLifecyclePolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Lifecycle','lifecyclePolicyText','LifecyclePolicyNotFoundException','put') end
function ops.GetLifecyclePolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Lifecycle','lifecyclePolicyText','LifecyclePolicyNotFoundException','get') end
function ops.DeleteLifecyclePolicy(ctx: Ctx, input: GetInput, db: Db) return policy(ctx,input,db,'_Lifecycle','lifecyclePolicyText','LifecyclePolicyNotFoundException','delete') end
local function tagged(ctx: Ctx, input: TaggedInput, db: Db)
    local arn=roto.required(input,'resourceArn',code)
    local prefix='arn:aws:ecr:'..ctx.region..':'..ctx.account_id..':repository/'
    if arn:sub(1,#prefix)~=prefix then roto.error(code,'Resource ARN must match caller account and region') end
    return get(ctx,{repositoryName=arn:sub(#prefix+1)},db)
end
function ops.TagResource(ctx: Ctx, input: TagResourceInput, db: Db)
    local repo=tagged(ctx,input,db);repo._Tags=roto.tags(repo._Tags,input.tags,code);db.put(ns(ctx),repo.repositoryName,repo);return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagResourceInput, db: Db)
    local repo=tagged(ctx,input,db);repo._Tags=roto.untag(repo._Tags,input.tagKeys,code);db.put(ns(ctx),repo.repositoryName,repo);return {}
end
function ops.ListTagsForResource(ctx: Ctx, input: TaggedInput, db: Db) return {tags=tagged(ctx,input,db)._Tags} end
return ops
