--!strict
-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack's CUR service; retains report definitions, not billing output.
type Tag = { Key: string, Value: string }
type Report = { Definition: { [string]: any }, Tags: { Tag } }
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type ReportRef = { ReportName: any }
type PutInput = { ReportDefinition: any }
type ModifyInput = { ReportName: any, ReportDefinition: any }
type DescribeInput = { MaxResults: any, NextToken: any }
type TagInput = { ReportName: any, Tags: any }
type UntagInput = { ReportName: any, TagKeys: any }
local ops = {}
local function ns(ctx: Ctx): string return roto.scope(ctx,"reports",true) end
local function report(ctx: Ctx, input: ReportRef, db: Db, code: string?): (Report, string)
    local name = roto.required(input,"ReportName","ValidationException")
    local value: Report? = db.get(ns(ctx),name)
    if not value then roto.error(code or "ValidationException","Report definition not found: " .. name) end
    return value :: Report, name -- roto.error above does not narrow the checker
end
function ops.PutReportDefinition(ctx: Ctx, input: PutInput, db: Db)
    local definition = roto.object(input.ReportDefinition,"ValidationException","ReportDefinition")
    local name = roto.required(definition,"ReportName","ValidationException")
    if db.get(ns(ctx),name) then roto.error("DuplicateReportNameException","Report definition already exists: " .. name) end
    db.put(ns(ctx),name,{Definition=definition,Tags=roto.array()})
    return {}
end
function ops.ModifyReportDefinition(ctx: Ctx, input: ModifyInput, db: Db)
    local value,name = report(ctx,input,db)
    value.Definition = roto.object(input.ReportDefinition,"ValidationException","ReportDefinition")
    value.Definition.ReportName = name
    db.put(ns(ctx),name,value)
    return {}
end
function ops.DeleteReportDefinition(ctx: Ctx, input: ReportRef, db: Db)
    local _,name = report(ctx,input,db)
    db.delete(ns(ctx),name)
    return {}
end
function ops.DescribeReportDefinitions(ctx: Ctx, input: DescribeInput, db: Db)
    local values = roto.array()
    local reports: { Report } = db.list(ns(ctx))
    for _, value in ipairs(reports) do table.insert(values,value.Definition) end
    return roto.page(input,values,"ReportDefinitions",5,5,"ValidationException")
end
function ops.ListTagsForResource(ctx: Ctx, input: ReportRef, db: Db)
    return {Tags=report(ctx,input,db,"ResourceNotFoundException").Tags}
end
function ops.TagResource(ctx: Ctx, input: TagInput, db: Db)
    local value,name = report(ctx,input,db,"ResourceNotFoundException")
    value.Tags = roto.tags(value.Tags,input.Tags,"ValidationException")
    db.put(ns(ctx),name,value)
    return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagInput, db: Db)
    local value,name = report(ctx,input,db,"ResourceNotFoundException")
    value.Tags = roto.untag(value.Tags,input.TagKeys,"ValidationException")
    db.put(ns(ctx),name,value)
    return {}
end
return ops
