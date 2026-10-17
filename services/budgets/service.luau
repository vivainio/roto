--!strict
-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack Budgets. Metadata only; no billing or alert delivery engine.
-- Request fields are untrusted JSON, so each is `any` and validated where it is used.
type Check_accountInput = { AccountId: any }
type GetInput = { AccountId: any, BudgetName: any }
type PublicInput = { ShowFilterExpression: any }
type CreateBudgetInput = { AccountId: any, Budget: any, NotificationsWithSubscribers: any, ResourceTags: any }
type DescribeBudgetInput = { AccountId: any, BudgetName: any, ShowFilterExpression: any }
type DescribeBudgetsInput = { AccountId: any, NextToken: any, ShowFilterExpression: any }
type UpdateBudgetInput = { AccountId: any, BudgetName: any, NewBudget: any }
type CreateNotificationInput = { AccountId: any, BudgetName: any, Notification: any, Subscribers: any }
type DeleteNotificationInput = { AccountId: any, BudgetName: any, Notification: any }
type UpdateNotificationInput = { AccountId: any, BudgetName: any, NewNotification: any, OldNotification: any }
type DescribeNotificationsForBudgetInput = { AccountId: any, BudgetName: any, NextToken: any }
type CreateSubscriberInput = { AccountId: any, BudgetName: any, Notification: any, Subscriber: any }
type UpdateSubscriberInput = { AccountId: any, BudgetName: any, NewSubscriber: any, Notification: any, OldSubscriber: any }
type DescribeSubscribersForNotificationInput = { AccountId: any, BudgetName: any, NextToken: any, Notification: any }
type TaggedInput = { ResourceARN: any }
type TagResourceInput = { ResourceARN: any, ResourceTags: any }
type UntagResourceInput = { ResourceARN: any, ResourceTagKeys: any }
-- A stored budget: the budget itself, its notification entries and tags.
type Entry = { Notification: any, Subscribers: any }
type Stored = { Budget: { [string]: any }, Entries: { Entry }, Tags: any }
local ops = {}
local invalid = "InvalidParameterException"
local function ns(ctx: Ctx): string return roto.scope(ctx,"budgets",true) end
local function check_account(ctx: Ctx, input: Check_accountInput)
    if input.AccountId ~= ctx.account_id then roto.error(invalid,"AccountId must match the calling account") end
end
local function get(ctx: Ctx, input: GetInput, db: Db): Stored
    check_account(ctx,input)
    local name = roto.required(input,"BudgetName",invalid)
    local value: Stored? = db.get(ns(ctx),name)
    if not value then roto.error("NotFoundException","Budget not found: " .. name) end
    return value :: Stored -- roto.error above does not narrow the checker
end
local function save(ctx: Ctx, db: Db, value: Stored) db.put(ns(ctx),value.Budget.BudgetName,value) end
local function enum(value: any, values: { string }, label: string)
    if not roto.contains(values,value) then roto.error(invalid,"Invalid " .. label) end
end
local function notification(value: any): any
    roto.object(value,invalid,"Notification")
    enum(value.NotificationType,{"ACTUAL","FORECASTED"},"NotificationType")
    enum(value.ComparisonOperator,{"GREATER_THAN","LESS_THAN","EQUAL_TO"},"ComparisonOperator")
    if type(value.Threshold) ~= "number" or value.Threshold < 0 or value.Threshold > 15000000000000 then roto.error(invalid,"Invalid Threshold") end
    value.ThresholdType = value.ThresholdType or "PERCENTAGE"
    value.NotificationState = value.NotificationState or "OK"
    enum(value.ThresholdType,{"PERCENTAGE","ABSOLUTE_VALUE"},"ThresholdType")
    enum(value.NotificationState,{"OK","ALARM"},"NotificationState")
    return value
end
local function subscriber(value: any): any
    roto.object(value,invalid,"Subscriber")
    enum(value.SubscriptionType,{"SNS","EMAIL"},"SubscriptionType")
    roto.required(value,"Address",invalid)
    return value
end
local function same_notification(a: any, b: any): boolean
    if type(b) ~= "table" then return false end
    return a.NotificationType == b.NotificationType and a.ComparisonOperator == b.ComparisonOperator
        and a.Threshold == b.Threshold and (a.ThresholdType or "PERCENTAGE") == (b.ThresholdType or "PERCENTAGE")
end
local function same_subscriber(a: any, b: any): boolean
    return type(b) == "table" and a.SubscriptionType == b.SubscriptionType and a.Address == b.Address
end
local function find(value: Stored, n: any): (Entry?, number?)
    for i,entry in ipairs(value.Entries) do if same_notification(entry.Notification,n) then return entry,i end end
    return nil, nil
end
local function entry(value: Stored, n: any): (Entry, number)
    local found,index = find(value,n)
    if not found then roto.error("NotFoundException","Notification not found") end
    return found :: Entry, index :: number
end
local function add_notification(value: Stored, n: any, subs: any)
    n = notification(n)
    roto.list(subs,invalid,"Subscribers")
    if #subs == 0 then roto.error(invalid,"Subscribers must be nonempty") end
    if #subs > 11 or #value.Entries >= 10 then roto.error("CreationLimitExceededException","Notification/subscriber limit exceeded") end
    if find(value,n) then roto.error("DuplicateRecordException","Notification already exists") end
    for i,sub in ipairs(subs) do
        subscriber(sub)
        for j=1,i-1 do if same_subscriber(subs[j],sub) then roto.error("DuplicateRecordException","Subscriber already exists") end end
    end
    table.insert(value.Entries,{Notification=n,Subscribers=subs})
end
local function period_start(unit: any): number?
    local now = os.date("!*t")
    now.hour,now.min,now.sec,now.isdst = 0,0,0,false
    if unit == "MONTHLY" then now.day=1
    elseif unit == "QUARTERLY" then now.day=1; now.month=math.floor((now.month-1)/3)*3+1
    elseif unit == "ANNUALLY" then now.day=1; now.month=1
    elseif unit ~= "DAILY" then return nil end
    -- Convert UTC calendar fields without depending on the server's local timezone.
    local y,m,d = now.year,now.month,now.day
    if m <= 2 then y=y-1 end
    local era = math.floor(y/400)
    local yoe = y-era*400
    local mp = m+(m>2 and -3 or 9)
    local doy = math.floor((153*mp+2)/5)+d-1
    local doe = yoe*365+math.floor(yoe/4)-math.floor(yoe/100)+doy
    return (era*146097+doe-719468)*86400
end
local function budget(value: any, old: any): any
    roto.object(value,invalid,"Budget")
    local name = roto.required(value,"BudgetName",invalid)
    if #name > 100 or name:find(":") or name:find("\\") then roto.error(invalid,"Invalid BudgetName") end
    enum(value.TimeUnit,{"DAILY","MONTHLY","QUARTERLY","ANNUALLY","CUSTOM"},"TimeUnit")
    enum(value.BudgetType,{"USAGE","COST","RI_UTILIZATION","RI_COVERAGE","SAVINGS_PLANS_UTILIZATION","SAVINGS_PLANS_COVERAGE"},"BudgetType")
    value.TimePeriod = value.TimePeriod or (old and old.TimePeriod) or {}
    roto.object(value.TimePeriod,invalid,"TimePeriod")
    value.TimePeriod.Start = value.TimePeriod.Start or period_start(value.TimeUnit)
    value.TimePeriod.End = value.TimePeriod.End or 3706473600 -- 2087-06-15 UTC
    local unit = value.BudgetLimit and value.BudgetLimit.Unit
    if not unit and value.PlannedBudgetLimits then for _, limit: any in pairs(value.PlannedBudgetLimits) do unit=limit.Unit; break end end
    value.CalculatedSpend = {ActualSpend={Amount="0",Unit=unit or "USD"}}
    value.LastUpdatedTime = os.time()
    return value
end
local function public(value: Stored, input: PublicInput): { [string]: any }
    local b = value.Budget
    if not input.ShowFilterExpression then b.FilterExpression,b.Metrics = nil,nil end
    return b
end
function ops.CreateBudget(ctx: Ctx, input: CreateBudgetInput, db: Db)
    check_account(ctx,input)
    local b = budget(input.Budget)
    if db.get(ns(ctx),b.BudgetName) then roto.error("DuplicateRecordException","Budget already exists") end
    local value = {Budget=b,Entries=roto.array(),Tags=roto.tags(nil,input.ResourceTags or roto.array(),invalid)}
    for _, e in ipairs(roto.list(input.NotificationsWithSubscribers or roto.array(),invalid,"NotificationsWithSubscribers")) do
        roto.object(e,invalid,"Notification entry")
        add_notification(value,e.Notification,e.Subscribers)
    end
    save(ctx,db,value)
    return {}
end
function ops.DescribeBudget(ctx: Ctx, input: DescribeBudgetInput, db: Db) return {Budget=public(get(ctx,input,db),input)} end
function ops.DescribeBudgets(ctx: Ctx, input: DescribeBudgetsInput, db: Db)
    check_account(ctx,input)
    local values = roto.array()
    for _, value in ipairs(db.list(ns(ctx))) do table.insert(values,public(value,input)) end
    return roto.page(input,values,"Budgets",100,1000,invalid)
end
function ops.UpdateBudget(ctx: Ctx, input: UpdateBudgetInput, db: Db)
    check_account(ctx,input)
    local b = roto.object(input.NewBudget,invalid,"NewBudget")
    local value = get(ctx,{AccountId=input.AccountId,BudgetName=b.BudgetName},db)
    value.Budget = budget(b,value.Budget)
    save(ctx,db,value)
    return {}
end
function ops.DeleteBudget(ctx: Ctx, input: GetInput, db: Db)
    local value = get(ctx,input,db)
    db.delete(ns(ctx),value.Budget.BudgetName)
    return {}
end
function ops.CreateNotification(ctx: Ctx, input: CreateNotificationInput, db: Db)
    local value = get(ctx,input,db)
    add_notification(value,input.Notification,input.Subscribers)
    save(ctx,db,value)
    return {}
end
function ops.DeleteNotification(ctx: Ctx, input: DeleteNotificationInput, db: Db)
    local value = get(ctx,input,db)
    local _,index = entry(value,input.Notification)
    table.remove(value.Entries,index)
    save(ctx,db,value)
    return {}
end
function ops.UpdateNotification(ctx: Ctx, input: UpdateNotificationInput, db: Db)
    local value = get(ctx,input,db)
    local e = entry(value,input.OldNotification)
    local n = notification(input.NewNotification)
    local other = find(value,n)
    if other and other ~= e then roto.error("DuplicateRecordException","Notification already exists") end
    e.Notification=n
    save(ctx,db,value)
    return {}
end
function ops.DescribeNotificationsForBudget(ctx: Ctx, input: DescribeNotificationsForBudgetInput, db: Db)
    local value = get(ctx,input,db)
    local values = roto.array()
    for _, e in ipairs(value.Entries) do table.insert(values,e.Notification) end
    return roto.page(input,values,"Notifications",100,100,invalid)
end
function ops.CreateSubscriber(ctx: Ctx, input: CreateSubscriberInput, db: Db)
    local value = get(ctx,input,db)
    local e = entry(value,input.Notification)
    local sub = subscriber(input.Subscriber)
    if #e.Subscribers >= 11 then roto.error("CreationLimitExceededException","Subscriber limit exceeded") end
    for _, existing in ipairs(e.Subscribers) do if same_subscriber(existing,sub) then roto.error("DuplicateRecordException","Subscriber already exists") end end
    table.insert(e.Subscribers,sub)
    save(ctx,db,value)
    return {}
end
function ops.DeleteSubscriber(ctx: Ctx, input: CreateSubscriberInput, db: Db)
    local value = get(ctx,input,db)
    local e,index = entry(value,input.Notification)
    local found
    for i,sub in ipairs(e.Subscribers) do if same_subscriber(sub,input.Subscriber) then found=i; break end end
    if not found then roto.error("NotFoundException","Subscriber not found") end
    table.remove(e.Subscribers,found)
    if #e.Subscribers == 0 then table.remove(value.Entries,index) end
    save(ctx,db,value)
    return {}
end
function ops.UpdateSubscriber(ctx: Ctx, input: UpdateSubscriberInput, db: Db)
    local value = get(ctx,input,db)
    local e = entry(value,input.Notification)
    local new = subscriber(input.NewSubscriber)
    local found
    for i,sub in ipairs(e.Subscribers) do
        if same_subscriber(sub,input.OldSubscriber) then found=i
        elseif same_subscriber(sub,new) then roto.error("DuplicateRecordException","Subscriber already exists") end
    end
    if not found then roto.error("NotFoundException","Subscriber not found") end
    e.Subscribers[found]=new
    save(ctx,db,value)
    return {}
end
function ops.DescribeSubscribersForNotification(ctx: Ctx, input: DescribeSubscribersForNotificationInput, db: Db)
    local value = get(ctx,input,db)
    return roto.page(input,entry(value,input.Notification).Subscribers,"Subscribers",100,100,invalid)
end
local function tagged(ctx: Ctx, input: TaggedInput, db: Db)
    local arn = roto.required(input,"ResourceARN",invalid)
    local prefix = "arn:aws:budgets::" .. ctx.account_id .. ":budget/"
    if arn:sub(1,#prefix) ~= prefix then roto.error("NotFoundException","Budget not found") end
    return get(ctx,{AccountId=ctx.account_id,BudgetName=arn:sub(#prefix+1)},db)
end
function ops.TagResource(ctx: Ctx, input: TagResourceInput, db: Db)
    local value = tagged(ctx,input,db)
    if #roto.list(input.ResourceTags,invalid,"ResourceTags") == 0 then roto.error(invalid,"ResourceTags must be nonempty") end
    value.Tags = roto.tags(value.Tags,input.ResourceTags,invalid)
    save(ctx,db,value)
    return {}
end
function ops.UntagResource(ctx: Ctx, input: UntagResourceInput, db: Db)
    local value = tagged(ctx,input,db)
    value.Tags = roto.untag(value.Tags,input.ResourceTagKeys,invalid)
    save(ctx,db,value)
    return {}
end
function ops.ListTagsForResource(ctx: Ctx, input: TaggedInput, db: Db) return {ResourceTags=tagged(ctx,input,db).Tags} end
return ops
