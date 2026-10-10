-- A custom service with per-account, per-region document storage.
local function namespace(ctx) return ctx.account_id .. "/" .. ctx.region .. "/widgets" end
local function id(input)
    if type(input.Id) ~= "string" or input.Id == "" then
        roto.error("InvalidInputException", "Id must be a nonempty string")
    end
    return input.Id
end
local function get(ctx, input, db)
    local value = db.get(namespace(ctx), id(input))
    if not value then roto.error("ResourceNotFoundException", "Unknown widget") end
    return value
end
return {
    CreateWidget = function(ctx, input, db)
        local key = id(input)
        if db.get(namespace(ctx), key) then roto.error("AlreadyExistsException", "Widget already exists") end
        local widget = {Id=key, Value=input.Value}
        db.put(namespace(ctx), key, widget)
        return {Widget=widget}
    end,
    GetWidget = function(ctx, input, db) return {Widget=get(ctx,input,db)} end,
    UpdateWidget = function(ctx, input, db)
        local widget = get(ctx,input,db)
        widget.Value = input.Value
        db.put(namespace(ctx),widget.Id,widget)
        return {Widget=widget}
    end,
    DeleteWidget = function(ctx, input, db)
        get(ctx,input,db)
        db.delete(namespace(ctx),input.Id)
        return {}
    end,
    ListWidgets = function(ctx, _, db) return {Widgets=db.list(namespace(ctx))} end,
}
