-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack's CUR service; retains report definitions, not billing output.
local ops = {}
local function ns(ctx) return roto.scope(ctx,"reports",true) end
local function report(ctx,input,db,code)
    local name = roto.required(input,"ReportName","ValidationException")
    local value = db.get(ns(ctx),name)
    if not value then roto.error(code or "ValidationException","Report definition not found: " .. name) end
    return value,name
end
function ops.PutReportDefinition(ctx,input,db)
    local definition = roto.object(input.ReportDefinition,"ValidationException","ReportDefinition")
    local name = roto.required(definition,"ReportName","ValidationException")
    if db.get(ns(ctx),name) then roto.error("DuplicateReportNameException","Report definition already exists: " .. name) end
    db.put(ns(ctx),name,{Definition=definition,Tags=roto.array()})
    return {}
end
function ops.ModifyReportDefinition(ctx,input,db)
    local value,name = report(ctx,input,db)
    value.Definition = roto.object(input.ReportDefinition,"ValidationException","ReportDefinition")
    value.Definition.ReportName = name
    db.put(ns(ctx),name,value)
    return {}
end
function ops.DeleteReportDefinition(ctx,input,db)
    local _,name = report(ctx,input,db)
    db.delete(ns(ctx),name)
    return {}
end
function ops.DescribeReportDefinitions(ctx,input,db)
    local values = roto.array()
    for _, value in ipairs(db.list(ns(ctx))) do table.insert(values,value.Definition) end
    return roto.page(input,values,"ReportDefinitions",5,5,"ValidationException")
end
function ops.ListTagsForResource(ctx,input,db)
    return {Tags=report(ctx,input,db,"ResourceNotFoundException").Tags}
end
function ops.TagResource(ctx,input,db)
    local value,name = report(ctx,input,db,"ResourceNotFoundException")
    value.Tags = roto.tags(value.Tags,input.Tags,"ValidationException")
    db.put(ns(ctx),name,value)
    return {}
end
function ops.UntagResource(ctx,input,db)
    local value,name = report(ctx,input,db,"ResourceNotFoundException")
    value.Tags = roto.untag(value.Tags,input.TagKeys,"ValidationException")
    db.put(ns(ctx),name,value)
    return {}
end
return ops
