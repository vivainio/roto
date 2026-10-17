--!strict
-- Synchronous configuration simulation. No SQL server, credentials or endpoints.
local M: { [string]: (Ctx, any, Db) -> any }={}
-- Per-kind settings (identifier key, ARN key, error codes, accepted fields), shared by the generated operations.
type Spec = { [string]: any }
local specs: { [string]: Spec }={
    DBInstance={id='DBInstanceIdentifier',plural='DBInstances',arn='DBInstanceArn',kind='db',missing='DBInstanceNotFound',exists='DBInstanceAlreadyExists',fields={'Engine','EngineVersion','DBInstanceClass','AllocatedStorage','StorageType','MultiAZ','PubliclyAccessible','BackupRetentionPeriod','DBName','MasterUsername','DBClusterIdentifier','DeletionProtection','Port'}},
    DBCluster={id='DBClusterIdentifier',plural='DBClusters',arn='DBClusterArn',kind='cluster',missing='DBClusterNotFoundFault',exists='DBClusterAlreadyExistsFault',fields={'Engine','EngineVersion','DatabaseName','MasterUsername','BackupRetentionPeriod','StorageEncrypted','DeletionProtection','Port'}},
    DBSubnetGroup={id='DBSubnetGroupName',plural='DBSubnetGroups',arn='DBSubnetGroupArn',kind='subgrp',missing='DBSubnetGroupNotFoundFault',exists='DBSubnetGroupAlreadyExistsFault',fields={'DBSubnetGroupDescription'}},
    DBParameterGroup={id='DBParameterGroupName',plural='DBParameterGroups',arn='DBParameterGroupArn',kind='pg',missing='DBParameterGroupNotFound',exists='DBParameterGroupAlreadyExists',fields={'DBParameterGroupFamily','Description'}},
}
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type DescribeDBParametersInput = { Filters: any, Marker: any, MaxRecords: any, Source: any }
type TaggedInput = { ResourceName: any }
type AddTagsToResourceInput = { ResourceName: any, Tags: any }
type RemoveTagsFromResourceInput = { ResourceName: any, TagKeys: any }
local function space(ctx: Ctx, kind: string) return roto.scope(ctx,kind) end
local function identifier(input: any, s: any)
    local name=roto.required(input,s.id,'InvalidParameterValue'):lower()
    if #name>63 or not name:match('^[a-z][a-z0-9%-]*$') or name:match('%-%-') or name:match('%-$') then roto.error('InvalidParameterValue','Invalid resource identifier') end
    return name
end
local function get(ctx: Ctx, input: any, db: Db, kind: string): any
    local s=specs[kind];local id=identifier(input,s);local d=db.get(space(ctx,kind),id)
    if not d then roto.error(s.missing,'Resource not found: '..id,404) end
    return d
end
local function public(d: any): any
    local out={};for k,v in pairs(d) do if tostring(k):sub(1,1)~='_' then out[k]=v end end;return out
end
local function check(input: any, s: any, extra: any)
    for k in pairs(input) do
        if k~=s.id and k~='Tags' and not roto.contains(s.fields,k) and not roto.contains(extra or {},k) then
            roto.error('NotImplemented','Unsupported setting: '..k,501)
        end
    end
end
local function list(ctx: Ctx, input: any, db: Db, kind: string)
    local s=specs[kind];local values=roto.array()
    if input.Filters then roto.error('NotImplemented','Describe filters are unsupported',501) end
    if input[s.id] then table.insert(values,public(get(ctx,input,db,kind)))
    else for _,d in ipairs(db.list(space(ctx,kind))) do table.insert(values,public(d)) end end
    return roto.page({MaxResults=input.MaxRecords,NextToken=input.Marker},values,s.plural,100,100,'InvalidParameterValue')
end
for kind,s in pairs(specs) do
    M['Create'..kind]=function(ctx: Ctx, input: any, db: Db)
        check(input,s,(kind=='DBInstance' or kind=='DBCluster') and {'MasterUserPassword'} or (kind=='DBSubnetGroup' and {'SubnetIds'} or {}))
        local id=identifier(input,s)
        if db.get(space(ctx,kind),id) then roto.error(s.exists,'Resource already exists') end
        local d={[s.id]=id,[s.arn]='arn:aws:rds:'..ctx.region..':'..ctx.account_id..':'..s.kind..':'..id}
        roto.copy_fields(input,d,s.fields)
        if kind=='DBInstance' then
            if not input.DBClusterIdentifier then
                roto.required(input,'MasterUsername','InvalidParameterValue');roto.required(input,'MasterUserPassword','InvalidParameterValue')
            else
                local cluster=get(ctx,{DBClusterIdentifier=input.DBClusterIdentifier},db,'DBCluster')
                if cluster.Engine~=input.Engine then roto.error('InvalidParameterValue','Cluster engine mismatch') end
                d.DBClusterIdentifier=cluster.DBClusterIdentifier
            end
            d.DBInstanceStatus='available';d.DBInstanceClass=input.DBInstanceClass
            d.DBParameterGroups=roto.array();d.VpcSecurityGroups=roto.array();d.DBInstanceIdentifier=id
            d.DbiResourceId=roto.id('db-');d.InstanceCreateTime=os.time()
            d.DBInstanceArn=d[s.arn];d.DeletionProtection=input.DeletionProtection or false
        elseif kind=='DBCluster' then
            roto.required(input,'MasterUsername','InvalidParameterValue');roto.required(input,'MasterUserPassword','InvalidParameterValue')
            d.Status='available';d.DBClusterMembers=roto.array();d.ClusterCreateTime=os.time();d.DeletionProtection=input.DeletionProtection or false
        elseif kind=='DBSubnetGroup' then
            local ids=roto.list(input.SubnetIds,'InvalidParameterValue','SubnetIds')
            if #ids==0 then roto.error('InvalidParameterValue','At least one subnet is required') end
            d.Subnets=roto.array();for _,sid in ipairs(ids) do if type(sid)~='string' then roto.error('InvalidParameterValue','Invalid subnet ID') end;table.insert(d.Subnets,{SubnetIdentifier=sid,SubnetStatus='Active'}) end
            d.SubnetGroupStatus='Complete'
        else d._Parameters={} end
        if d.AllocatedStorage and d.AllocatedStorage<1 then roto.error('InvalidParameterValue','AllocatedStorage must be positive') end
        d._Tags=roto.tags({},input.Tags or roto.array(),'InvalidParameterValue')
        db.put(space(ctx,kind),id,d)
        if kind=='DBInstance' and d.DBClusterIdentifier then
            local c=get(ctx,{DBClusterIdentifier=d.DBClusterIdentifier},db,'DBCluster')
            table.insert(c.DBClusterMembers,{DBInstanceIdentifier=id,IsClusterWriter=#c.DBClusterMembers==0,PromotionTier=1})
            db.put(space(ctx,'DBCluster'),c.DBClusterIdentifier,c)
        end
        if kind=='DBParameterGroup' then return {DBParameterGroup=public(d)} end
        return {[kind]=public(d)}
    end
    M['Describe'..s.plural]=function(ctx: Ctx, input: any, db: Db)
        local out=list(ctx,input,db,kind);out.Marker=out.NextToken;out.NextToken=nil;return out
    end
    M['Delete'..kind]=function(ctx: Ctx, input: any, db: Db)
        check(input,s,(kind=='DBInstance' or kind=='DBCluster') and {'SkipFinalSnapshot','FinalDBSnapshotIdentifier'} or {})
        local d=get(ctx,input,db,kind)
        if d.DeletionProtection then roto.error('InvalidParameterCombination','Deletion protection is enabled') end
        if kind=='DBInstance' or kind=='DBCluster' then
            if input.SkipFinalSnapshot~=true or input.FinalDBSnapshotIdentifier then roto.error('NotImplemented','Final snapshots are unsupported; use SkipFinalSnapshot',501) end
        end
        if kind=='DBCluster' and #d.DBClusterMembers>0 then roto.error('InvalidDBClusterStateFault','Cluster still has instances') end
        db.delete(space(ctx,kind),d[s.id])
        if kind=='DBInstance' and d.DBClusterIdentifier then
            local c=get(ctx,{DBClusterIdentifier=d.DBClusterIdentifier},db,'DBCluster');local members=roto.array()
            for _,member in ipairs(c.DBClusterMembers) do if member.DBInstanceIdentifier~=d.DBInstanceIdentifier then table.insert(members,member) end end
            if members[1] then members[1].IsClusterWriter=true end
            c.DBClusterMembers=members;db.put(space(ctx,'DBCluster'),c.DBClusterIdentifier,c)
        end
        if kind=='DBSubnetGroup' or kind=='DBParameterGroup' then return {} end
        if kind=='DBInstance' then d.DBInstanceStatus='deleting' else d.Status='deleting' end
        return {[kind]=public(d)}
    end
    if kind~='DBParameterGroup' then
        M['Modify'..kind]=function(ctx: Ctx, input: any, db: Db)
            check(input,s,{'ApplyImmediately','MasterUserPassword','SubnetIds'})
            if input.MasterUserPassword then roto.error('NotImplemented','Credential changes are unsupported',501) end
            local d=get(ctx,input,db,kind)
            if input.DBClusterIdentifier and input.DBClusterIdentifier~=d.DBClusterIdentifier then roto.error('NotImplemented','Moving instances between clusters is unsupported',501) end
            roto.copy_fields(input,d,s.fields)
            if kind=='DBSubnetGroup' and input.SubnetIds then
                local ids=roto.list(input.SubnetIds,'InvalidParameterValue','SubnetIds');if #ids==0 then roto.error('InvalidParameterValue','At least one subnet is required') end
                d.Subnets=roto.array();for _,id in ipairs(ids) do table.insert(d.Subnets,{SubnetIdentifier=id,SubnetStatus='Active'}) end
            end
            db.put(space(ctx,kind),d[s.id],d);return {[kind]=public(d)}
        end
    end
end
function M.ModifyDBParameterGroup(ctx: Ctx, input: any, db: Db)
    local d=get(ctx,input,db,'DBParameterGroup')
    for _,p in ipairs(roto.list(input.Parameters,'InvalidParameterValue','Parameters')) do
        local name=roto.required(p,'ParameterName','InvalidParameterValue');roto.required(p,'ParameterValue','InvalidParameterValue')
        if p.ApplyMethod~='immediate' and p.ApplyMethod~='pending-reboot' then roto.error('InvalidParameterValue','Invalid ApplyMethod') end
        d._Parameters[name]={ParameterName=name,ParameterValue=p.ParameterValue,ApplyMethod=p.ApplyMethod,Source='user',IsModifiable=true}
    end
    db.put(space(ctx,'DBParameterGroup'),d.DBParameterGroupName,d);return {DBParameterGroupName=d.DBParameterGroupName}
end
function M.ResetDBParameterGroup(ctx: Ctx, input: any, db: Db)
    local d=get(ctx,input,db,'DBParameterGroup')
    if input.ResetAllParameters then d._Parameters={}
    else for _,p in ipairs(roto.list(input.Parameters,'InvalidParameterValue','Parameters')) do d._Parameters[roto.required(p,'ParameterName','InvalidParameterValue')]=nil end end
    db.put(space(ctx,'DBParameterGroup'),d.DBParameterGroupName,d);return {DBParameterGroupName=d.DBParameterGroupName}
end
function M.DescribeDBParameters(ctx: Ctx, input: DescribeDBParametersInput, db: Db)
    local d=get(ctx,input,db,'DBParameterGroup');local values=roto.array();local keys={}
    if input.Filters then roto.error('NotImplemented','Parameter filters are unsupported',501) end
    for key in pairs(d._Parameters) do table.insert(keys,key) end;table.sort(keys)
    for _,key in ipairs(keys) do local p=d._Parameters[key];if not input.Source or input.Source==p.Source then table.insert(values,p) end end
    local out=roto.page({MaxResults=input.MaxRecords,NextToken=input.Marker},values,'Parameters',100,100,'InvalidParameterValue');out.Marker=out.NextToken;out.NextToken=nil;return out
end
local function tagged(ctx: Ctx, input: TaggedInput, db: Db): (any, string, Spec)
    local arn=roto.required(input,'ResourceName','InvalidParameterValue')
    for kind,s in pairs(specs) do for _,d in ipairs(db.list(space(ctx,kind))) do if d[s.arn]==arn then return d,kind,s end end end
    return roto.error('DBInstanceNotFound','Resource ARN not found',404)
end
function M.AddTagsToResource(ctx: Ctx, input: AddTagsToResourceInput, db: Db)
    local d,kind,s=tagged(ctx,input,db);d._Tags=roto.tags(d._Tags,input.Tags,'InvalidParameterValue');db.put(space(ctx,kind),d[s.id],d);return {}
end
function M.RemoveTagsFromResource(ctx: Ctx, input: RemoveTagsFromResourceInput, db: Db)
    local d,kind,s=tagged(ctx,input,db);d._Tags=roto.untag(d._Tags,input.TagKeys,'InvalidParameterValue');db.put(space(ctx,kind),d[s.id],d);return {}
end
function M.ListTagsForResource(ctx: Ctx, input: TaggedInput, db: Db) return {TagList=tagged(ctx,input,db)._Tags} end
return M
