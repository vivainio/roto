--!strict
-- ECS configuration only. No tasks, container instances or deployment worker.
local ops={}
local code='InvalidParameterException'
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type PageInput = { maxResults: any, nextToken: any }
type CreateClusterInput = { capacityProviders: any, clusterName: any, defaultCapacityProviderStrategy: any, settings: any, tags: any }
type DescribeClustersInput = { clusters: any, include: any }
type UpdateClusterInput = { cluster: any, settings: any }
type DeleteClusterInput = { cluster: any }
type RegisterTaskDefinitionInput = { containerDefinitions: any, family: any, networkMode: any, placementConstraints: any, tags: any, volumes: any }
type DescribeTaskDefinitionInput = { include: any, taskDefinition: any }
type DeregisterTaskDefinitionInput = { taskDefinition: any }
type ListTaskDefinitionsInput = { familyPrefix: any, maxResults: any, nextToken: any, sort: any, status: any }
type ListTaskDefinitionFamiliesInput = { familyPrefix: any, maxResults: any, nextToken: any, status: any }
type ServiceInput = { cluster: any, service: any }
type ServicecheckInput = { capacityProviderStrategy: any, deploymentController: any, desiredCount: any, forceNewDeployment: any, launchType: any, schedulingStrategy: any, serviceConnectConfiguration: any, volumeConfigurations: any, vpcLatticeConfigurations: any }
type CreateServiceInput = { capacityProviderStrategy: any, clientToken: any, cluster: any, deploymentController: any, desiredCount: any, forceNewDeployment: any, launchType: any, role: any, schedulingStrategy: any, serviceConnectConfiguration: any, serviceName: any, tags: any, taskDefinition: any, volumeConfigurations: any, vpcLatticeConfigurations: any }
type DescribeServicesInput = { cluster: any, include: any, services: any }
type ListServicesInput = { cluster: any, launchType: any, maxResults: any, nextToken: any, schedulingStrategy: any }
type UpdateServiceInput = { capacityProviderStrategy: any, cluster: any, deploymentController: any, desiredCount: any, forceNewDeployment: any, launchType: any, schedulingStrategy: any, service: any, serviceConnectConfiguration: any, taskDefinition: any, volumeConfigurations: any, vpcLatticeConfigurations: any }
type DeleteServiceInput = { cluster: any, force: any, service: any }
type TaggedInput = { resourceArn: any }
type TagResourceInput = { resourceArn: any, tags: any }
type UntagResourceInput = { resourceArn: any, tagKeys: any }
local function ns(ctx: Ctx, kind: any) return roto.scope(ctx,kind) end
local function arn(ctx: Ctx, kind: any, id: any) return 'arn:aws:ecs:'..ctx.region..':'..ctx.account_id..':'..kind..'/'..id end
local function name(value: any, label: string): string
    if type(value)~='string' or #value<1 or #value>255 or not string.match(value, '^[%w_%-]+$') then roto.error(code,'Invalid '..label) end
    return value :: string
end
local function id(ctx: Ctx, value: any, kind: any)
    if type(value)~='string' then roto.error(code,'Invalid resource identifier') end
    if value:sub(1,4)=='arn:' then
        local prefix=arn(ctx,kind,'')
        if value:sub(1,#prefix)~=prefix then roto.error(code,'Resource ARN must match caller account and region') end
        return value:sub(#prefix+1)
    end
    return value
end
local function public(doc: any, include_tags: any)
    local out={};for k,v in pairs(doc) do if k:sub(1,1)~='_' and (k~='tags' or include_tags) then out[k]=v end end;return out
end
local function tags(existing: any, incoming: any)
    local upper=roto.array()
    for _,tag in ipairs(roto.list(incoming,code,'tags')) do
        roto.object(tag,code,'Tag');local value=tag.value;if value==nil then value='' end;table.insert(upper,{Key=tag.key,Value=value})
    end
    local prior=roto.array();for _,tag in ipairs((existing or {}) :: { any }) do table.insert(prior,{Key=tag.key,Value=tag.value}) end
    local out=roto.array();for _,tag in ipairs(roto.tags(prior,upper,code)) do table.insert(out,{key=tag.Key,value=tag.Value}) end;return out
end
local function page(input: PageInput, values: any, key: any)
    local out=roto.page({MaxResults=input.maxResults,NextToken=input.nextToken},values,key,100,100,code)
    out.nextToken=out.NextToken;out.NextToken=nil;return out
end
local function cluster(ctx: Ctx, value: any, db: Db)
    local key=id(ctx,value or 'default','cluster');name(key,'cluster')
    local c=db.get(ns(ctx,'clusters'),key)
    if not c or c.status~='ACTIVE' then roto.error('ClusterNotFoundException','Cluster not found: '..key) end
    return c
end
local function cluster_public(ctx: Ctx, c: any, db: Db, include_tags: any)
    local out=public(c,include_tags);out.activeServicesCount=0
    for _,s in ipairs(db.list(ns(ctx,'services/'..c.clusterName))) do if s.status=='ACTIVE' then out.activeServicesCount=out.activeServicesCount+1 end end
    return out
end
local function settings(values: any)
    roto.list(values,code,'settings')
    for _,setting in ipairs(values) do
        if setting.name~='containerInsights' or not roto.contains({'enabled','disabled','enhanced'},setting.value) then roto.error(code,'Invalid cluster setting') end
    end
    return values
end
function ops.CreateCluster(ctx: Ctx, input: CreateClusterInput, db: Db)
    local key=name(input.clusterName or 'default','clusterName')
    local c=db.get(ns(ctx,'clusters'),key)
    if c and c.status=='ACTIVE' then return {cluster=cluster_public(ctx,c,db,true)} end
    if input.capacityProviders and #input.capacityProviders>0 or input.defaultCapacityProviderStrategy and #input.defaultCapacityProviderStrategy>0 then roto.error('NotImplemented','Capacity providers are unsupported',501) end
    c={clusterName=key,clusterArn=arn(ctx,'cluster',key),status='ACTIVE',registeredContainerInstancesCount=0,runningTasksCount=0,pendingTasksCount=0,activeServicesCount=0,settings=settings(input.settings or roto.array()),tags=tags({},input.tags or roto.array()),capacityProviders=roto.array(),defaultCapacityProviderStrategy=roto.array()}
    roto.copy_fields(input,c,{'configuration','serviceConnectDefaults'})
    db.put(ns(ctx,'clusters'),key,c);return {cluster=public(c,true)}
end
function ops.ListClusters(ctx: Ctx, input: PageInput, db: Db)
    local out=roto.array();for _,c in ipairs(db.list(ns(ctx,'clusters'))) do if c.status=='ACTIVE' then table.insert(out,c.clusterArn) end end
    return page(input,out,'clusterArns')
end
function ops.DescribeClusters(ctx: Ctx, input: DescribeClustersInput, db: Db)
    local out={clusters=roto.array(),failures=roto.array()}
    local values=input.clusters or roto.array();if not input.clusters then table.insert(values,'default') end
    for _,value in ipairs(roto.list(values,code,'clusters')) do
        local key=id(ctx,value,'cluster');local c=db.get(ns(ctx,'clusters'),key)
        if c then table.insert(out.clusters,cluster_public(ctx,c,db,roto.contains(input.include,'TAGS')))
        else table.insert(out.failures,{arn=value,reason='MISSING'}) end
    end
    return out
end
function ops.UpdateCluster(ctx: Ctx, input: UpdateClusterInput, db: Db)
    local c=cluster(ctx,input.cluster,db);roto.copy_fields(input,c,{'configuration','serviceConnectDefaults'})
    if input.settings then c.settings=settings(input.settings) end
    db.put(ns(ctx,'clusters'),c.clusterName,c);return {cluster=cluster_public(ctx,c,db,true)}
end
function ops.UpdateClusterSettings(ctx: Ctx, input: UpdateClusterInput, db: Db)
    local c=cluster(ctx,input.cluster,db);c.settings=settings(input.settings);db.put(ns(ctx,'clusters'),c.clusterName,c)
    return {cluster=cluster_public(ctx,c,db,true)}
end
function ops.DeleteCluster(ctx: Ctx, input: DeleteClusterInput, db: Db)
    local c=cluster(ctx,input.cluster,db)
    for _,s in ipairs(db.list(ns(ctx,'services/'..c.clusterName))) do
        if s.status=='ACTIVE' then roto.error('ClusterContainsServicesException','Cluster still contains services') end
        db.delete(ns(ctx,'services/'..c.clusterName),s.serviceName)
    end
    c.status='INACTIVE';db.put(ns(ctx,'clusters'),c.clusterName,c);return {cluster=cluster_public(ctx,c,db,true)}
end
local tdfields={'containerDefinitions','taskRoleArn','executionRoleArn','networkMode','volumes','placementConstraints','requiresCompatibilities','cpu','memory','inferenceAccelerators','pidMode','ipcMode','proxyConfiguration','runtimePlatform','ephemeralStorage','enableFaultInjection'}
local function task(ctx: Ctx, value: any, db: Db, allow_inactive: any): any
    local key=id(ctx,roto.required({value=value},'value',code),'task-definition')
    local family,revision=key:match('^([%w_%-]+):(%d+)$')
    if family then
        local d=db.get(ns(ctx,'task-definitions'),family..':'..(tonumber(revision) :: number))
        if d and (allow_inactive or d.status=='ACTIVE') then return d end
    else
        name(key,'taskDefinition')
        local latest
        for _,d in ipairs(db.list(ns(ctx,'task-definitions'))) do if d.family==key and d.status=='ACTIVE' and (not latest or d.revision>latest.revision) then latest=d end end
        if latest then return latest end
    end
    return roto.error('ClientException','Task definition not found')
end
function ops.RegisterTaskDefinition(ctx: Ctx, input: RegisterTaskDefinitionInput, db: Db)
    local family=name(input.family,'family');local containers=roto.list(input.containerDefinitions,code,'containerDefinitions')
    if #containers==0 then roto.error(code,'At least one container is required') end
    local seen={}
    for _,c in ipairs(containers) do
        roto.object(c,code,'Container');local key=name(c.name,'container name');roto.required(c,'image',code)
        if seen[key] then roto.error(code,'Duplicate container name') end;seen[key]=true
    end
    if input.networkMode and not roto.contains({'bridge','host','awsvpc','none'},input.networkMode) then roto.error(code,'Invalid networkMode') end
    local counter=db.get(ns(ctx,'families'),family) or {family=family,revision=0};counter.revision=counter.revision+1
    local d={family=family,revision=counter.revision,status='ACTIVE',taskDefinitionArn=arn(ctx,'task-definition',family..':'..counter.revision),networkMode=input.networkMode or 'bridge',volumes=input.volumes or roto.array(),placementConstraints=input.placementConstraints or roto.array(),registeredAt=os.time(),_Tags=tags({},input.tags or roto.array())}
    roto.copy_fields(input,d,tdfields)
    db.put(ns(ctx,'families'),family,counter);db.put(ns(ctx,'task-definitions'),family..':'..d.revision,d)
    return {taskDefinition=public(d),tags=d._Tags}
end
function ops.DescribeTaskDefinition(ctx: Ctx, input: DescribeTaskDefinitionInput, db: Db)
    local d=task(ctx,input.taskDefinition,db,true);local out={taskDefinition=public(d)}
    if roto.contains(input.include,'TAGS') then out.tags=d._Tags end;return out
end
function ops.DeregisterTaskDefinition(ctx: Ctx, input: DeregisterTaskDefinitionInput, db: Db)
    local d=task(ctx,input.taskDefinition,db,true);d.status='INACTIVE';d.deregisteredAt=os.time()
    db.put(ns(ctx,'task-definitions'),d.family..':'..d.revision,d);return {taskDefinition=public(d)}
end
function ops.ListTaskDefinitions(ctx: Ctx, input: ListTaskDefinitionsInput, db: Db)
    local status=input.status or 'ACTIVE';if not roto.contains({'ACTIVE','INACTIVE'},status) then roto.error(code,'Invalid status') end
    if input.sort and not roto.contains({'ASC','DESC'},input.sort) then roto.error(code,'Invalid sort') end
    local docs: { any }={};for _,d in ipairs(db.list(ns(ctx,'task-definitions'))) do if d.status==status and (not input.familyPrefix or d.family:sub(1,#input.familyPrefix)==input.familyPrefix) then table.insert(docs,d) end end
    table.sort(docs,function(a: any, b: any) if a.family==b.family then return a.revision<b.revision end;return a.family<b.family end)
    local out=roto.array();if input.sort=='DESC' then for i=#docs,1,-1 do table.insert(out,docs[i].taskDefinitionArn) end else for _,d in ipairs(docs) do table.insert(out,d.taskDefinitionArn) end end
    return page(input,out,'taskDefinitionArns')
end
function ops.ListTaskDefinitionFamilies(ctx: Ctx, input: ListTaskDefinitionFamiliesInput, db: Db)
    local status=input.status or 'ALL';if not roto.contains({'ACTIVE','INACTIVE','ALL'},status) then roto.error(code,'Invalid status') end
    local values={};for _,d in ipairs(db.list(ns(ctx,'task-definitions'))) do if (status=='ALL' or d.status==status) and (not input.familyPrefix or d.family:sub(1,#input.familyPrefix)==input.familyPrefix) then values[d.family]=true end end
    local out=roto.array();for family in pairs(values) do table.insert(out,family) end;table.sort(out);return page(input,out,'families')
end
local servicefields={'desiredCount','launchType','platformVersion','deploymentConfiguration','placementConstraints','placementStrategy','networkConfiguration','healthCheckGracePeriodSeconds','schedulingStrategy','deploymentController','enableECSManagedTags','propagateTags','enableExecuteCommand','availabilityZoneRebalancing','loadBalancers','serviceRegistries'}
local function service(ctx: Ctx, input: ServiceInput, db: Db, c: any)
    c=c or cluster(ctx,input.cluster,db)
    local key=id(ctx,roto.required(input,'service',code),'service')
    local prefix=c.clusterName..'/'
    if key:sub(1,#prefix)==prefix then key=key:sub(#prefix+1) end
    name(key,'service');local s=db.get(ns(ctx,'services/'..c.clusterName),key)
    if not s then roto.error('ServiceNotFoundException','Service not found') end;return s,c
end
local function servicecheck(input: ServicecheckInput)
    if input.desiredCount~=nil and (type(input.desiredCount)~='number' or input.desiredCount<0 or input.desiredCount%1~=0) then roto.error(code,'desiredCount must be a nonnegative integer') end
    if input.capacityProviderStrategy or input.serviceConnectConfiguration or input.volumeConfigurations or input.vpcLatticeConfigurations then roto.error('NotImplemented','Capacity-provider, service-connect, volume and lattice provisioning are unsupported',501) end
    if input.launchType and not roto.contains({'EC2','FARGATE','EXTERNAL'},input.launchType) then roto.error(code,'Invalid launchType') end
    if input.schedulingStrategy and input.schedulingStrategy~='REPLICA' then roto.error('NotImplemented','DAEMON reconciliation is unsupported',501) end
    if input.deploymentController and input.deploymentController.type~='ECS' then roto.error('NotImplemented','External deployment controllers are unsupported',501) end
    if input.forceNewDeployment then roto.error('NotImplemented','Deployment execution is unsupported',501) end
end
function ops.CreateService(ctx: Ctx, input: CreateServiceInput, db: Db)
    servicecheck(input)
    local c=cluster(ctx,input.cluster,db);local key=name(input.serviceName,'serviceName')
    local old=db.get(ns(ctx,'services/'..c.clusterName),key)
    if old and old.status=='ACTIVE' then roto.error('InvalidParameterException','Service already exists') end
    if input.clientToken then roto.error('NotImplemented','Service idempotency-token replay is unsupported',501) end
    local d=task(ctx,input.taskDefinition,db,false)
    local s={serviceName=key,serviceArn=arn(ctx,'service',c.clusterName..'/'..key),clusterArn=c.clusterArn,taskDefinition=d.taskDefinitionArn,status='ACTIVE',desiredCount=input.desiredCount or 0,launchType='EC2',runningCount=0,pendingCount=0,createdAt=os.time(),deployments=roto.array(),events=roto.array(),loadBalancers=roto.array(),serviceRegistries=roto.array(),schedulingStrategy='REPLICA',tags=tags({},input.tags or roto.array())}
    roto.copy_fields(input,s,servicefields)
    if input.role then local role=roto.required(input,'role',code);s.roleArn=role:sub(1,4)=='arn:' and role or 'arn:aws:iam::'..ctx.account_id..':role/'..role end
    db.put(ns(ctx,'services/'..c.clusterName),key,s);return {service=public(s,true)}
end
function ops.DescribeServices(ctx: Ctx, input: DescribeServicesInput, db: Db)
    local c=cluster(ctx,input.cluster,db);local out={services=roto.array(),failures=roto.array()}
    for _,value in ipairs(roto.list(input.services,code,'services')) do
        local key=id(ctx,value,'service');local prefix=c.clusterName..'/';if key:sub(1,#prefix)==prefix then key=key:sub(#prefix+1) end
        local s=db.get(ns(ctx,'services/'..c.clusterName),key)
        if s then table.insert(out.services,public(s,roto.contains(input.include,'TAGS'))) else table.insert(out.failures,{arn=value,reason='MISSING'}) end
    end;return out
end
function ops.ListServices(ctx: Ctx, input: ListServicesInput, db: Db)
    local c=cluster(ctx,input.cluster,db);local out=roto.array()
    for _,s in ipairs(db.list(ns(ctx,'services/'..c.clusterName))) do if s.status=='ACTIVE' and (not input.launchType or s.launchType==input.launchType) and (not input.schedulingStrategy or s.schedulingStrategy==input.schedulingStrategy) then table.insert(out,s.serviceArn) end end
    return page(input,out,'serviceArns')
end
function ops.UpdateService(ctx: Ctx, input: UpdateServiceInput, db: Db)
    servicecheck(input);local s,c=service(ctx,input,db)
    if s.status~='ACTIVE' then roto.error('ServiceNotActiveException','Service is inactive') end
    if input.taskDefinition then s.taskDefinition=task(ctx,input.taskDefinition,db,false).taskDefinitionArn end
    roto.copy_fields(input,s,servicefields);db.put(ns(ctx,'services/'..c.clusterName),s.serviceName,s);return {service=public(s,true)}
end
function ops.DeleteService(ctx: Ctx, input: DeleteServiceInput, db: Db)
    local s,c=service(ctx,input,db)
    if s.desiredCount>0 and not input.force then roto.error(code,'Set desiredCount to zero or use force') end
    s.status='INACTIVE';s.desiredCount=0;db.put(ns(ctx,'services/'..c.clusterName),s.serviceName,s);return {service=public(s,true)}
end
local function tagged(ctx: Ctx, input: TaggedInput, db: Db): (any, string, string, string)
    local resource=roto.required(input,'resourceArn',code)
    if resource:find(':cluster/',1,true) then local c=cluster(ctx,resource,db);return c,ns(ctx,'clusters'),c.clusterName,'tags' end
    if resource:find(':task-definition/',1,true) then local d=task(ctx,resource,db,true);return d,ns(ctx,'task-definitions'),d.family..':'..d.revision,'_Tags' end
    if resource:find(':service/',1,true) then
        local key=id(ctx,resource,'service');local cluster_name,service_name=key:match('^([^/]+)/([^/]+)$')
        if not cluster_name then roto.error(code,'Invalid service ARN') end
        local s,c=service(ctx,{cluster=cluster_name,service=service_name},db);return s,ns(ctx,'services/'..c.clusterName),s.serviceName,'tags'
    end
    return roto.error(code,'Unsupported resource ARN')
end
function ops.TagResource(ctx: Ctx, input: TagResourceInput, db: Db)
    local d,space,key,field=tagged(ctx,input,db);d[field]=tags(d[field],input.tags);db.put(space,key,d);return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagResourceInput, db: Db)
    local d,space,key,field=tagged(ctx,input,db);roto.list(input.tagKeys,code,'tagKeys')
    for _,tagkey in ipairs(input.tagKeys) do if type(tagkey)~='string' then roto.error(code,'Tag keys must be strings') end end
    local out=roto.array();for _,t in ipairs(d[field]) do if not roto.contains(input.tagKeys,t.key) then table.insert(out,t) end end
    d[field]=out;db.put(space,key,d);return {}
end
function ops.ListTagsForResource(ctx: Ctx, input: TaggedInput, db: Db) local d,_,_,field=tagged(ctx,input,db);return {tags=d[field]} end
return ops
