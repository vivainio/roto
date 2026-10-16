-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack Config: configuration management, no evaluation engine.
local ops = {}
local function ns(ctx,collection) return roto.scope(ctx,collection) end
local function get(ctx,db,collection,name,code)
    if type(name) ~= "string" or name == "" then roto.error(code,"Name is required") end
    local value = db.get(ns(ctx,collection),name)
    if not value then roto.error(code,"Unknown " .. collection .. ": " .. name) end
    return value
end
local function selected(ctx,input,db,collection,key,code)
    local names = input[key]
    if names == nil or #roto.list(names,"InvalidParameterValueException",key) == 0 then return db.list(ns(ctx,collection)) end
    local values = roto.array()
    for _, name in ipairs(names) do table.insert(values,get(ctx,db,collection,name,code)) end
    return values
end
function ops.PutConfigRule(ctx,input,db)
    local rule = roto.object(input.ConfigRule,"InvalidParameterValueException","ConfigRule")
    local name = roto.required(rule,"ConfigRuleName","InvalidParameterValueException")
    local old = db.get(ns(ctx,"rules"),name)
    local id = old and old.ConfigRuleId or roto.id("config-rule-")
    local stored = {ConfigRuleName=name,ConfigRuleId=id,
        ConfigRuleArn="arn:aws:config:" .. ctx.region .. ":" .. ctx.account_id .. ":config-rule/" .. id,
        Source=rule.Source or {},ConfigRuleState=rule.ConfigRuleState or "ACTIVE"}
    roto.copy_fields(rule,stored,{"Description","Scope","InputParameters","MaximumExecutionFrequency","CreatedBy","EvaluationModes"})
    db.put(ns(ctx,"rules"),name,stored)
    return {}
end
function ops.DescribeConfigRules(ctx,input,db)
    return roto.page(input,selected(ctx,input,db,"rules","ConfigRuleNames","NoSuchConfigRuleException"),"ConfigRules",25,25,"InvalidNextTokenException")
end
function ops.DeleteConfigRule(ctx,input,db)
    get(ctx,db,"rules",input.ConfigRuleName,"NoSuchConfigRuleException")
    db.delete(ns(ctx,"rules"),input.ConfigRuleName)
    return {}
end
function ops.PutConfigurationRecorder(ctx,input,db)
    local rec = roto.object(input.ConfigurationRecorder,"InvalidConfigurationRecorderNameException","ConfigurationRecorder")
    local name = rec.name or "default"
    roto.required({name=name},"name","InvalidConfigurationRecorderNameException")
    local stored = db.get(ns(ctx,"recorders"),name) or {recording=false,lastStatus="Pending",lastStatusChangeTime=os.time()}
    stored.recorder = roto.copy_fields(rec,{name=name},{"roleARN","recordingGroup","recordingMode"})
    db.put(ns(ctx,"recorders"),name,stored)
    return {}
end
function ops.DescribeConfigurationRecorders(ctx,input,db)
    local values = roto.array()
    for _, rec in ipairs(selected(ctx,input,db,"recorders","ConfigurationRecorderNames","NoSuchConfigurationRecorderException")) do table.insert(values,rec.recorder) end
    return {ConfigurationRecorders=values}
end
function ops.DescribeConfigurationRecorderStatus(ctx,input,db)
    local values = roto.array()
    for _, rec in ipairs(selected(ctx,input,db,"recorders","ConfigurationRecorderNames","NoSuchConfigurationRecorderException")) do
        table.insert(values,roto.copy_fields(rec,{name=rec.recorder.name,recording=rec.recording,lastStatus=rec.lastStatus},
            {"lastStartTime","lastStopTime","lastStatusChangeTime"}))
    end
    return {ConfigurationRecordersStatus=values}
end
local function recording(ctx,input,db,enabled)
    local rec = get(ctx,db,"recorders",input.ConfigurationRecorderName,"NoSuchConfigurationRecorderException")
    rec.recording,rec.lastStatus,rec.lastStatusChangeTime = enabled,"Success",os.time()
    rec[enabled and "lastStartTime" or "lastStopTime"] = rec.lastStatusChangeTime
    db.put(ns(ctx,"recorders"),input.ConfigurationRecorderName,rec)
    return {}
end
function ops.StartConfigurationRecorder(ctx,input,db) return recording(ctx,input,db,true) end
function ops.StopConfigurationRecorder(ctx,input,db) return recording(ctx,input,db,false) end
function ops.PutDeliveryChannel(ctx,input,db)
    local channel = roto.object(input.DeliveryChannel,"InvalidDeliveryChannelNameException","DeliveryChannel")
    local name = channel.name or "default"
    roto.required({name=name},"name","InvalidDeliveryChannelNameException")
    db.put(ns(ctx,"channels"),name,roto.copy_fields(channel,{name=name},
        {"s3BucketName","s3KeyPrefix","s3KmsKeyArn","snsTopicARN","configSnapshotDeliveryProperties"}))
    return {}
end
function ops.DescribeDeliveryChannels(ctx,input,db)
    return {DeliveryChannels=selected(ctx,input,db,"channels","DeliveryChannelNames","NoSuchDeliveryChannelException")}
end
function ops.DescribeDeliveryChannelStatus(ctx,input,db)
    local values = roto.array()
    for _, channel in ipairs(selected(ctx,input,db,"channels","DeliveryChannelNames","NoSuchDeliveryChannelException")) do
        table.insert(values,{name=channel.name})
    end
    return {DeliveryChannelsStatus=values}
end
function ops.DescribeComplianceByConfigRule(ctx,input,db)
    local values = roto.array()
    for _, rule in ipairs(selected(ctx,input,db,"rules","ConfigRuleNames","NoSuchConfigRuleException")) do
        if not input.ComplianceTypes or roto.contains(input.ComplianceTypes,"INSUFFICIENT_DATA") then
            table.insert(values,{ConfigRuleName=rule.ConfigRuleName,Compliance={ComplianceType="INSUFFICIENT_DATA"}})
        end
    end
    return roto.page(input,values,"ComplianceByConfigRules",25,25,"InvalidNextTokenException")
end
function ops.GetComplianceDetailsByConfigRule(ctx,input,db)
    get(ctx,db,"rules",input.ConfigRuleName,"NoSuchConfigRuleException")
    return {EvaluationResults=roto.array()}
end
function ops.DescribeConfigRuleEvaluationStatus(ctx,input,db)
    local values = roto.array()
    for _, rule in ipairs(selected(ctx,input,db,"rules","ConfigRuleNames","NoSuchConfigRuleException")) do
        table.insert(values,{ConfigRuleName=rule.ConfigRuleName,ConfigRuleArn=rule.ConfigRuleArn,
            ConfigRuleId=rule.ConfigRuleId,FirstEvaluationStarted=false})
    end
    return roto.page(input,values,"ConfigRulesEvaluationStatus",25,25,"InvalidNextTokenException")
end
-- No evaluations or deliveries have run: status reads expose that absence.
return ops
