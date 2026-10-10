-- Glue catalog storage; no crawlers, ETL jobs, Lake Formation or Iceberg commits.
local ops={}
local code='InvalidInputException'
local dbfields={'Description','LocationUri','Parameters','CreateTableDefaultPermissions'}
local tablefields={'Description','Owner','LastAccessTime','LastAnalyzedTime','Retention','StorageDescriptor','PartitionKeys','ViewOriginalText','ViewExpandedText','TableType','Parameters'}
local partfields={'LastAccessTime','LastAnalyzedTime','StorageDescriptor','Parameters'}
local function catalog(ctx,input)
    if input.CatalogId and input.CatalogId~=ctx.account_id then roto.error(code,'CatalogId must match caller account') end
    for _,key in ipairs({'TransactionId','QueryAsOfTime','AuditContext','OpenTableFormatInput','UpdateOpenTableFormatInput','ViewDefinition','PartitionIndexes','Segment'}) do
        if input[key]~=nil then roto.error('NotImplemented','Unsupported catalog feature: '..key,501) end
    end
end
local function ns(ctx,kind) return roto.scope(ctx,kind) end
local function name(value,label)
    if type(value)~='string' or #value<1 or #value>255 or not value:match('^[%w_.%-]+$') then roto.error(code,'Invalid '..label) end
    return value:lower()
end
local function database(ctx,input,db,key)
    catalog(ctx,input);local id=name(input[key or 'DatabaseName'],key or 'DatabaseName')
    local d=db.get(ns(ctx,'databases'),id)
    if not d then roto.error('EntityNotFoundException','Database not found: '..id) end;return d
end
local function tabledoc(ctx,input,db,key)
    local d=database(ctx,input,db);local id=name(input[key or 'TableName'],key or 'TableName')
    local t=db.get(ns(ctx,'tables/'..d.Name),id)
    if not t then roto.error('EntityNotFoundException','Table not found: '..id) end;return t
end
local function public(d)
    local out={};for k,v in pairs(d) do if k:sub(1,1)~='_' then out[k]=v end end;return out
end
local function checkkeys(input)
    local keys=input.PartitionKeys or roto.array();roto.list(keys,code,'PartitionKeys')
    local seen={};for _,key in ipairs(keys) do
        roto.object(key,code,'Partition key');local n=name(key.Name,'partition key')
        if seen[n] then roto.error(code,'Duplicate partition key') end;seen[n]=true
    end
end
local function tagmap(value)
    roto.object(value,code,'Tags')
    for k,v in pairs(value) do if type(k)~='string' or #k==0 or type(v)~='string' then roto.error(code,'Invalid tag') end end
    return value
end
local function tableinput(input)
    local t=roto.object(input.TableInput,code,'TableInput');name(t.Name,'TableInput.Name');checkkeys(t)
    for _,key in ipairs({'TargetTable','ViewDefinition'}) do if t[key] then roto.error('NotImplemented','Federated/resource-link and structured views are unsupported',501) end end
    return t
end
function ops.CreateDatabase(ctx,input,db)
    catalog(ctx,input);local value=roto.object(input.DatabaseInput,code,'DatabaseInput');local id=name(value.Name,'DatabaseInput.Name')
    if value.TargetDatabase or value.FederatedDatabase then roto.error('NotImplemented','Federated and resource-link databases are unsupported',501) end
    if db.get(ns(ctx,'databases'),id) then roto.error('AlreadyExistsException','Database exists') end
    local d={Name=id,CatalogId=ctx.account_id,CreateTime=os.time(),Parameters={},CreateTableDefaultPermissions=roto.array(),_Tags=tagmap(input.Tags or {})}
    roto.copy_fields(value,d,dbfields);db.put(ns(ctx,'databases'),id,d);return {}
end
function ops.GetDatabase(ctx,input,db) return {Database=public(database(ctx,input,db,'Name'))} end
function ops.GetDatabases(ctx,input,db)
    catalog(ctx,input)
    if input.ResourceShareType and input.ResourceShareType~='FOREIGN' and input.ResourceShareType~='ALL' then
        if input.ResourceShareType~='FEDERATED' then roto.error(code,'Invalid ResourceShareType') end
    end
    if input.ResourceShareType=='FOREIGN' or input.ResourceShareType=='FEDERATED' then return {DatabaseList=roto.array()} end
    if input.AttributesToGet then roto.error('NotImplemented','Attribute projection is unsupported',501) end
    local out=roto.array();for _,d in ipairs(db.list(ns(ctx,'databases'))) do table.insert(out,public(d)) end
    return roto.page(input,out,'DatabaseList',100,100,code)
end
function ops.UpdateDatabase(ctx,input,db)
    local d=database(ctx,input,db,'Name');local value=roto.object(input.DatabaseInput,code,'DatabaseInput')
    if name(value.Name,'DatabaseInput.Name')~=d.Name then roto.error('NotImplemented','Database renaming is unsupported',501) end
    if value.TargetDatabase or value.FederatedDatabase then roto.error('NotImplemented','Federated databases are unsupported',501) end
    roto.copy_fields(value,d,dbfields);db.put(ns(ctx,'databases'),d.Name,d);return {}
end
local function clear(db,space) for _,d in ipairs(db.list(space)) do db.delete(space,d._Id or d.Name or d.VersionId) end end
local function delete_table(ctx,t,db)
    clear(db,ns(ctx,'partitions/'..t.DatabaseName..'/'..t.Name))
    clear(db,ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name))
    db.delete(ns(ctx,'tables/'..t.DatabaseName),t.Name)
end
function ops.DeleteDatabase(ctx,input,db)
    local d=database(ctx,input,db,'Name')
    for _,t in ipairs(db.list(ns(ctx,'tables/'..d.Name))) do delete_table(ctx,t,db) end
    db.delete(ns(ctx,'databases'),d.Name);return {}
end
function ops.CreateTable(ctx,input,db)
    local d=database(ctx,input,db);local value=tableinput(input);local id=name(value.Name,'TableInput.Name')
    if input.Name and name(input.Name,'Name')~=id then roto.error(code,'Name must match TableInput.Name') end
    if db.get(ns(ctx,'tables/'..d.Name),id) then roto.error('AlreadyExistsException','Table exists') end
    local t={Name=id,DatabaseName=d.Name,CatalogId=ctx.account_id,CreateTime=os.time(),UpdateTime=os.time(),VersionId='1',PartitionKeys=roto.array(),Parameters={}}
    roto.copy_fields(value,t,tablefields)
    db.put(ns(ctx,'tables/'..d.Name),id,t);db.put(ns(ctx,'versions/'..d.Name..'/'..id),'1',{VersionId='1',Table=t});return {}
end
function ops.GetTable(ctx,input,db) return {Table=public(tabledoc(ctx,input,db,'Name'))} end
function ops.GetTables(ctx,input,db)
    local d=database(ctx,input,db)
    if input.Expression or input.AttributesToGet then roto.error('NotImplemented','Table regex filtering and attribute projection are unsupported',501) end
    local out=roto.array();for _,t in ipairs(db.list(ns(ctx,'tables/'..d.Name))) do table.insert(out,public(t)) end
    return roto.page(input,out,'TableList',100,100,code)
end
function ops.UpdateTable(ctx,input,db)
    catalog(ctx,input);local value=tableinput(input);local t=tabledoc(ctx,{CatalogId=input.CatalogId,DatabaseName=input.DatabaseName,TableName=input.Name or value.Name},db)
    if name(value.Name,'TableInput.Name')~=t.Name then roto.error('NotImplemented','Table renaming is unsupported',501) end
    if input.VersionId and input.VersionId~=t.VersionId then roto.error('ConcurrentModificationException','Stale table version') end
    local parts=db.list(ns(ctx,'partitions/'..t.DatabaseName..'/'..t.Name))
    if value.PartitionKeys and #parts>0 then
        if #value.PartitionKeys~=#t.PartitionKeys then roto.error(code,'Cannot change partition keys with existing partitions') end
        for i,key in ipairs(value.PartitionKeys) do if key.Name~=t.PartitionKeys[i].Name or key.Type~=t.PartitionKeys[i].Type then roto.error(code,'Cannot change partition keys with existing partitions') end end
    end
    local old=t.VersionId;t.VersionId=tostring(tonumber(old)+1);t.UpdateTime=os.time();roto.copy_fields(value,t,tablefields)
    if input.SkipArchive then db.delete(ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name),old) end
    db.put(ns(ctx,'tables/'..t.DatabaseName),t.Name,t);db.put(ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name),t.VersionId,{VersionId=t.VersionId,Table=t});return {}
end
function ops.DeleteTable(ctx,input,db) delete_table(ctx,tabledoc(ctx,input,db,'Name'),db);return {} end
local function detail(code_,message) return {ErrorCode=code_,ErrorMessage=message} end
function ops.BatchDeleteTable(ctx,input,db)
    local d=database(ctx,input,db);local out={Errors=roto.array()}
    for _,value in ipairs(roto.list(input.TablesToDelete,code,'TablesToDelete')) do
        local id=name(value,'TableName');local t=db.get(ns(ctx,'tables/'..d.Name),id)
        if t then delete_table(ctx,t,db) else table.insert(out.Errors,{TableName=value,ErrorDetail=detail('EntityNotFoundException','Table not found')}) end
    end;return out
end
local function version(ctx,t,db,value)
    local key=value or t.VersionId
    if type(key)~='string' or not key:match('^%d+$') then roto.error(code,'Invalid VersionId') end
    local v=db.get(ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name),key)
    if not v then roto.error('EntityNotFoundException','Table version not found') end;return v
end
function ops.GetTableVersion(ctx,input,db) local t=tabledoc(ctx,input,db);return {TableVersion=version(ctx,t,db,input.VersionId)} end
function ops.GetTableVersions(ctx,input,db)
    local t=tabledoc(ctx,input,db);local values=db.list(ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name))
    table.sort(values,function(a,b) return tonumber(a.VersionId)<tonumber(b.VersionId) end)
    return roto.page(input,values,'TableVersions',100,100,code)
end
function ops.DeleteTableVersion(ctx,input,db)
    local t=tabledoc(ctx,input,db);local v=version(ctx,t,db,roto.required(input,'VersionId',code))
    if v.VersionId==t.VersionId then roto.error(code,'Cannot delete the current table version') end
    db.delete(ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name),v.VersionId);return {}
end
function ops.BatchDeleteTableVersion(ctx,input,db)
    local t=tabledoc(ctx,input,db);local space=ns(ctx,'versions/'..t.DatabaseName..'/'..t.Name);local out={Errors=roto.array()}
    for _,key in ipairs(roto.list(input.VersionIds,code,'VersionIds')) do
        local err
        if key==t.VersionId then err=detail(code,'Cannot delete the current table version')
        elseif not db.get(space,key) then err=detail('EntityNotFoundException','Table version not found') end
        if err then table.insert(out.Errors,{TableName=t.Name,VersionId=key,ErrorDetail=err}) else db.delete(space,key) end
    end;return out
end
local function values(t,vs)
    roto.list(vs,code,'Values')
    if #t.PartitionKeys==0 or #vs~=#t.PartitionKeys then return nil,'Values must match the table partition keys' end
    local key='';for _,v in ipairs(vs) do if type(v)~='string' then return nil,'Partition values must be strings' end;key=key..#v..':'..v end
    return key
end
local function partspace(ctx,t) return ns(ctx,'partitions/'..t.DatabaseName..'/'..t.Name) end
local function partget(ctx,t,db,vs)
    local key,err=values(t,vs);if err then roto.error(code,err) end
    local p=db.get(partspace(ctx,t),key);if not p then roto.error('EntityNotFoundException','Partition not found') end;return p
end
local function partcreate(ctx,t,db,value)
    roto.object(value,code,'PartitionInput');local key,err=values(t,value.Values)
    if err then return code,err end
    if db.get(partspace(ctx,t),key) then return 'AlreadyExistsException','Partition exists' end
    local p={_Id=key,Values=value.Values,DatabaseName=t.DatabaseName,TableName=t.Name,CatalogId=ctx.account_id,CreationTime=os.time(),Parameters={}}
    roto.copy_fields(value,p,partfields);db.put(partspace(ctx,t),key,p)
end
function ops.CreatePartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local err,msg=partcreate(ctx,t,db,input.PartitionInput);if err then roto.error(err,msg) end;return {}
end
function ops.GetPartition(ctx,input,db) local t=tabledoc(ctx,input,db);return {Partition=public(partget(ctx,t,db,input.PartitionValues))} end
function ops.DeletePartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local p=partget(ctx,t,db,input.PartitionValues);db.delete(partspace(ctx,t),p._Id);return {}
end
function ops.UpdatePartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local p=partget(ctx,t,db,input.PartitionValueList);local value=roto.object(input.PartitionInput,code,'PartitionInput')
    local key,err=values(t,value.Values);if err then roto.error(code,err) end
    if key~=p._Id and db.get(partspace(ctx,t),key) then roto.error('AlreadyExistsException','Partition exists') end
    db.delete(partspace(ctx,t),p._Id);p._Id=key;p.Values=value.Values;roto.copy_fields(value,p,partfields);db.put(partspace(ctx,t),key,p);return {}
end
function ops.BatchCreatePartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local out={Errors=roto.array()}
    for _,value in ipairs(roto.list(input.PartitionInputList,code,'PartitionInputList')) do
        local err,msg=partcreate(ctx,t,db,value);if err then table.insert(out.Errors,{PartitionValues=value.Values,ErrorDetail=detail(err,msg)}) end
    end;return out
end
function ops.BatchGetPartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local out={Partitions=roto.array(),UnprocessedKeys=roto.array()}
    for _,item in ipairs(roto.list(input.PartitionsToGet,code,'PartitionsToGet')) do
        local key,err=values(t,item.Values);if err then roto.error(code,err) end
        local p=db.get(partspace(ctx,t),key);if p then table.insert(out.Partitions,public(p)) else table.insert(out.UnprocessedKeys,{Values=item.Values}) end
    end;return out
end
function ops.BatchDeletePartition(ctx,input,db)
    local t=tabledoc(ctx,input,db);local out={Errors=roto.array()}
    for _,item in ipairs(roto.list(input.PartitionsToDelete,code,'PartitionsToDelete')) do
        local key,err=values(t,item.Values);local p=key and db.get(partspace(ctx,t),key)
        if err or not p then table.insert(out.Errors,{PartitionValues=item.Values,ErrorDetail=detail(err and code or 'EntityNotFoundException',err or 'Partition not found')})
        else db.delete(partspace(ctx,t),key) end
    end;return out
end
-- A small SQL predicate grammar: key op literal [AND key op literal ...].
local function predicates(t,expression)
    if not expression or expression=='' then return {} end
    if type(expression)~='string' then roto.error(code,'Expression must be a string') end
    local columns={};for i,key in ipairs(t.PartitionKeys) do columns[key.Name:lower()]={index=i,type=(key.Type or 'string'):lower()} end
    local out={};local rest=expression
    while true do
        local key,op,pos=rest:match('^%s*([%w_]+)%s*([<>=!]+)%s*()')
        if not key or not roto.contains({'=','!=','<>','<','<=','>','>='},op) then roto.error(code,'Unsupported partition expression; expected key comparison literal joined by AND') end
        local column=columns[key:lower()];if not column then roto.error(code,'Unknown partition key: '..key) end
        rest=rest:sub(pos);local literal
        if rest:sub(1,1)=="'" then
            local i=2;local chars={};local closed=false
            while i<=#rest do
                local c=rest:sub(i,i)
                if c=="'" then
                    if rest:sub(i+1,i+1)=="'" then table.insert(chars,"'");i=i+2 else i=i+1;closed=true;break end
                else table.insert(chars,c);i=i+1 end
            end
            if not closed then roto.error(code,'Unterminated string literal') end
            literal=table.concat(chars);rest=rest:sub(i)
        else
            literal,pos=rest:match('^([+%-]?%d+%.?%d*)()')
            if not literal then roto.error(code,'Unsupported expression literal') end;rest=rest:sub(pos)
        end
        local numeric=column.type:match('^decimal') or roto.contains({'int','integer','bigint','smallint','tinyint','float','double'},column.type)
        if numeric then literal=tonumber(literal);if not literal then roto.error(code,'Numeric partition key requires a numeric literal') end end
        table.insert(out,{index=column.index,op=op,literal=literal,numeric=numeric})
        rest=rest:match('^%s*(.-)%s*$')
        if rest=='' then break end
        if rest:sub(1,3):upper()~='AND' or not rest:sub(4,4):match('%s') then roto.error(code,'Only AND conjunctions are supported') end
        rest=rest:sub(4)
    end;return out
end
local function matches(p,rules)
    for _,rule in ipairs(rules) do
        local v=p.Values[rule.index];if rule.numeric then v=tonumber(v) end
        if v==nil then return false end
        local x=rule.literal;local op=rule.op
        local valid=(op=='=' and v==x) or ((op=='!=' or op=='<>') and v~=x) or (op=='<' and v<x) or (op=='<=' and v<=x) or (op=='>' and v>x) or (op=='>=' and v>=x)
        if not valid then return false end
    end;return true
end
function ops.GetPartitions(ctx,input,db)
    local t=tabledoc(ctx,input,db);local rules=predicates(t,input.Expression);local out=roto.array()
    for _,p in ipairs(db.list(partspace(ctx,t))) do
        if matches(p,rules) then
            local value=public(p)
            if input.ExcludeColumnSchema and value.StorageDescriptor then value.StorageDescriptor.Columns=nil end
            table.insert(out,value)
        end
    end;return roto.page(input,out,'Partitions',100,1000,code)
end
local function tagged(ctx,input,db)
    local resource=roto.required(input,'ResourceArn',code);local prefix='arn:aws:glue:'..ctx.region..':'..ctx.account_id..':database/'
    if resource:sub(1,#prefix)~=prefix then roto.error(code,'Only caller-region database ARNs are supported') end
    return database(ctx,{Name=resource:sub(#prefix+1)},db,'Name')
end
function ops.TagResource(ctx,input,db)
    local d=tagged(ctx,input,db);for key,value in pairs(tagmap(input.TagsToAdd)) do d._Tags[key]=value end
    db.put(ns(ctx,'databases'),d.Name,d);return {}
end
function ops.UntagResource(ctx,input,db)
    local d=tagged(ctx,input,db);for _,key in ipairs(roto.list(input.TagsToRemove,code,'TagsToRemove')) do if type(key)~='string' then roto.error(code,'Invalid tag key') end;d._Tags[key]=nil end
    db.put(ns(ctx,'databases'),d.Name,d);return {}
end
function ops.GetTags(ctx,input,db) return {Tags=tagged(ctx,input,db)._Tags} end
return ops
