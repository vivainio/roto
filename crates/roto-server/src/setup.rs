//! Trusted local Lua setup, executed before listeners and delivery workers start.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value as LuaValue};
use roto_core::{RawRequest, RequestContext, ServiceHandler};
use serde_json::{Value, json};

fn options(table: &Table, allowed: &[&str]) -> mlua::Result<()> {
    for pair in table.clone().pairs::<String, LuaValue>() {
        let (key, _) = pair?;
        if !allowed.contains(&key.as_str()) {
            return Err(mlua::Error::external(format!(
                "Unknown setup option: {key}"
            )));
        }
    }
    Ok(())
}
fn decode(response: roto_core::RawResponse) -> mlua::Result<Value> {
    if response.status >= 400 {
        return Err(mlua::Error::external(format!(
            "AWS request failed ({}): {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        )));
    }
    if response.body.is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_slice(&response.body).map_err(mlua::Error::external)
    }
}
fn sqs_call(
    sqs: &roto_svc_sqs::Sqs,
    ctx: &RequestContext,
    operation: &str,
    input: Value,
) -> mlua::Result<Value> {
    decode(roto_svc_sqs::dispatch(sqs, ctx, operation, &input).map_err(mlua::Error::external)?)
}
fn lambda_call(
    lambda: &roto_svc_lambda::Lambda,
    ctx: &RequestContext,
    operation: &str,
    input: Value,
) -> mlua::Result<Value> {
    decode(
        roto_svc_lambda::dispatch(lambda, ctx, operation, &input).map_err(mlua::Error::external)?,
    )
}

pub fn run(
    path: &Path,
    ctx: RequestContext,
    sqs: Arc<roto_svc_sqs::Sqs>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
) -> mlua::Result<()> {
    let path = std::fs::canonicalize(path).map_err(mlua::Error::external)?;
    let parent = path.parent().unwrap().to_path_buf();
    let lua = Lua::new();
    let roto = lua.create_table()?;
    roto.set("account_id", ctx.account_id.clone())?;
    roto.set("region", ctx.region.clone())?;
    roto.set("endpoint", ctx.base_url.clone())?;
    roto.set("null", lua.null())?;
    roto.set(
        "array",
        lua.create_function(|lua, table: Table| {
            table.set_metatable(Some(lua.array_metatable()))?;
            Ok(table)
        })?,
    )?;
    // Make require("module") resolve beside the setup script before the default search paths.
    let package: Table = lua.globals().get("package")?;
    let search: String = package.get("path")?;
    package.set(
        "path",
        format!(
            "{}/?.lua;{}/?/init.lua;{search}",
            parent.display(),
            parent.display()
        ),
    )?;
    let sqs_api = lua.create_table()?;
    let lambda_api = lua.create_table()?;

    let (q_service, q_ctx) = (sqs.clone(), ctx.clone());
    sqs_api.set("queue",lua.create_function(move |lua,(name,opts): (String,Option<Table>)| {
        let opts = opts.unwrap_or(lua.create_table()?);
        options(&opts,&["visibility_timeout","attributes","tags"])?;
        let mut attributes: std::collections::BTreeMap<String,String> = match opts.get::<LuaValue>("attributes")? {
            LuaValue::Nil => Default::default(), value => lua.from_value(value)?,
        };
        if let Some(timeout) = opts.get::<Option<i32>>("visibility_timeout")? { attributes.insert("VisibilityTimeout".into(),timeout.to_string()); }
        let tags: std::collections::BTreeMap<String,String> = match opts.get::<LuaValue>("tags")? {
            LuaValue::Nil => Default::default(), value => lua.from_value(value)?,
        };
        // Get first so changing attributes does not conflict with CreateQueue on persisted state.
        let url = match roto_svc_sqs::dispatch(q_service.as_ref(),&q_ctx,"GetQueueUrl",&json!({"QueueName":name})) {
            Ok(r) => decode(r)?,
            Err(e) if e.code == "AWS.SimpleQueueService.NonExistentQueue" => sqs_call(&q_service,&q_ctx,"CreateQueue",json!({"QueueName":name,"Attributes":attributes,"tags":tags}))?,
            Err(e) => return Err(mlua::Error::external(e)),
        };
        let url = url["QueueUrl"].as_str().ok_or_else(|| mlua::Error::external("Missing queue URL"))?;
        if !attributes.is_empty() { sqs_call(&q_service,&q_ctx,"SetQueueAttributes",json!({"QueueUrl":url,"Attributes":attributes}))?; }
        if !tags.is_empty() { sqs_call(&q_service,&q_ctx,"TagQueue",json!({"QueueUrl":url,"Tags":tags}))?; }
        lua.to_value(&json!({"name":name,"url":url,"arn":format!("arn:aws:sqs:{}:{}:{name}",q_ctx.region,q_ctx.account_id)}))
    })?)?;

    let (l_service, l_ctx) = (lambda.clone(), ctx.clone());
    lambda_api.set("function_",lua.create_function(move |lua,(name,opts): (String,Table)| {
        options(&opts,&["timeout","executor","environment","description","memory_size"])?;
        let mut executor: roto_svc_lambda::Executor = lua.from_value(opts.get::<LuaValue>("executor")?)?;
        executor.validate().map_err(mlua::Error::external)?;
        if let roto_svc_lambda::Executor::Command { cwd,.. } = &mut executor {
            *cwd = Some(cwd.take().map_or_else(|| parent.clone(),|p| parent.join(p)));
        }
        let timeout = opts.get::<Option<u32>>("timeout")?.unwrap_or(3);
        let memory = opts.get::<Option<u32>>("memory_size")?.unwrap_or(128);
        let description = opts.get::<Option<String>>("description")?.unwrap_or_default();
        let environment: std::collections::BTreeMap<String,String> = match opts.get::<LuaValue>("environment")? {
            LuaValue::Nil => Default::default(), value => lua.from_value(value)?,
        };
        let mut input = json!({"FunctionName":name,"Timeout":timeout,"MemorySize":memory,"Description":description,"Environment":{"Variables":environment}});
        let exists = roto_svc_lambda::dispatch(l_service.as_ref(),&l_ctx,"GetFunctionConfiguration",&json!({"FunctionName":name}));
        let result = match exists {
            Ok(_) => lambda_call(&l_service,&l_ctx,"UpdateFunctionConfiguration",input)?,
            Err(e) if e.code == "ResourceNotFoundException" => {
                input["Runtime"] = json!("provided.al2023"); input["Handler"] = json!("external");
                input["Role"] = json!(format!("arn:aws:iam::{}:role/roto-local",l_ctx.account_id));
                input["Code"] = json!({"ZipFile":""});
                lambda_call(&l_service,&l_ctx,"CreateFunction",input)?
            },
            Err(e) => return Err(mlua::Error::external(e)),
        };
        l_service.bind_executor(&l_ctx,&name,executor).map_err(mlua::Error::external)?;
        lua.to_value(&json!({"name":name,"arn":result["FunctionArn"]}))
    })?)?;

    let (m_service, m_ctx) = (lambda.clone(), ctx.clone());
    lambda_api.set("event_source",lua.create_function(move |lua,(queue,function,opts): (Table,Table,Option<Table>)| {
        let opts = opts.unwrap_or(lua.create_table()?);
        options(&opts,&["batch_size","enabled","report_batch_item_failures"])?;
        let source: String = queue.get("arn")?;
        let function: String = function.get("arn")?;
        let mut input = json!({"EventSourceArn":source,"FunctionName":function,"BatchSize":opts.get::<Option<u32>>("batch_size")?.unwrap_or(10),"Enabled":opts.get::<Option<bool>>("enabled")?.unwrap_or(true)});
        if opts.get::<Option<bool>>("report_batch_item_failures")?.unwrap_or(false) { input["FunctionResponseTypes"] = json!(["ReportBatchItemFailures"]); }
        let existing = lambda_call(&m_service,&m_ctx,"ListEventSourceMappings",json!({"EventSourceArn":source,"FunctionName":function}))?;
        let result = if let Some(mapping) = existing["EventSourceMappings"].as_array().and_then(|a| a.first()) {
            input.as_object_mut().unwrap().remove("EventSourceArn");
            input["UUID"] = mapping["UUID"].clone();
            // Explicitly reconcile partial responses, including turning the option off.
            {
                input["FunctionResponseTypes"] = if opts.get::<Option<bool>>("report_batch_item_failures")?.unwrap_or(false) { json!(["ReportBatchItemFailures"]) } else { json!([]) };
                lambda_call(&m_service,&m_ctx,"UpdateEventSourceMapping",input)?
            }
        } else { lambda_call(&m_service,&m_ctx,"CreateEventSourceMapping",input)? };
        lua.to_value(&result)
    })?)?;

    let call_services = services.clone();
    let (call_sqs, call_lambda, call_ctx) = (sqs, lambda, ctx.clone());
    roto.set(
        "call",
        lua.create_function(
            move |lua, (service, operation, input): (String, String, LuaValue)| {
                let input: Value = if input.is_nil() {
                    json!({})
                } else {
                    lua.from_value(input)?
                };
                let result = match service.as_str() {
                    "sqs" => sqs_call(&call_sqs, &call_ctx, &operation, input)?,
                    "lambda" => lambda_call(&call_lambda, &call_ctx, &operation, input)?,
                    "dynamodb" | "ssm" | "secretsmanager" | "events" | "eventbridge"
                    | "kinesis" => {
                        let service = if service == "eventbridge" {
                            "events"
                        } else {
                            service.as_str()
                        };
                        let handler = call_services.get(service).ok_or_else(|| {
                            mlua::Error::external(format!("Service unavailable: {service}"))
                        })?;
                        decode(
                            handler
                                .handle(
                                    &call_ctx,
                                    &RawRequest {
                                        method: "POST".into(),
                                        path: "/".into(),
                                        headers: vec![("x-amz-target".into(), operation)],
                                        body: serde_json::to_vec(&input)
                                            .map_err(mlua::Error::external)?,
                                        ..Default::default()
                                    },
                                )
                                .map_err(mlua::Error::external)?,
                        )?
                    }
                    _ => {
                        return Err(mlua::Error::external(
                            "Use roto.request for services using Query/XML protocols",
                        ));
                    }
                };
                lua.to_value(&result)
            },
        )?,
    )?;
    // Escape hatch for every AWS service/protocol, without requiring the listener to be running.
    roto.set("request",lua.create_function(move |lua,(service,method,path,opts): (String,String,String,Option<Table>)| {
        let opts = opts.unwrap_or(lua.create_table()?);
        options(&opts,&["headers","body","query"])?;
        let headers: std::collections::BTreeMap<String,String> = match opts.get::<LuaValue>("headers")? {
            LuaValue::Nil => Default::default(), value => lua.from_value(value)?,
        };
        let body = match opts.get::<LuaValue>("body")? {
            LuaValue::Nil => Vec::new(), LuaValue::String(s) => s.as_bytes().to_vec(),
            value => serde_json::to_vec(&lua.from_value::<Value>(value)?).map_err(mlua::Error::external)?,
        };
        let handler = services.get(service.as_str()).ok_or_else(|| mlua::Error::external(format!("Unknown service: {service}")))?;
        let response = handler.handle(&ctx,&RawRequest {method,path,query:opts.get::<Option<String>>("query")?.unwrap_or_default(),headers:headers.into_iter().map(|(k,v)| (k.to_ascii_lowercase(),v)).collect(),body}).map_err(mlua::Error::external)?;
        if response.status >= 400 { return Err(mlua::Error::external(format!("AWS request failed ({}): {}",response.status,String::from_utf8_lossy(&response.body)))); }
        lua.to_value(&json!({"status":response.status,"headers":response.headers,"body":String::from_utf8_lossy(&response.body)}))
    })?)?;
    roto.set("sqs", sqs_api)?;
    roto.set("lambda", lambda_api)?;
    lua.globals().set("roto", roto)?;
    let source = std::fs::read_to_string(&path).map_err(mlua::Error::external)?;
    lua.load(&source)
        .set_name(format!("@{}", path.display()))
        .exec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use roto_core::store::Store;

    fn context() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "setup-test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn script(source: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("roto-lua-{}", roto_core::ids::request_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("setup.lua");
        std::fs::write(&path, source).unwrap();
        path
    }
    #[test]
    fn setup_reconciles_resources_bindings_and_mappings_without_duplicates() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(
            roto_svc_lambda::Lambda::new(&store, Default::default())
                .unwrap()
                .with_sqs(sqs.clone()),
        );
        let path = script(
            r#"
local queue = roto.sqs.queue("jobs", {visibility_timeout=30, tags={owner="lua"}})
assert(queue.url == roto.endpoint .. "/" .. roto.account_id .. "/jobs")
local handler = roto.lambda.function_("consume", {timeout=3, executor={command={"sh", "-c", "cat"}},environment={SOURCE="lua"}})
local mapping = roto.lambda.event_source(queue, handler, {batch_size=2,enabled=false,report_batch_item_failures=true})
assert(mapping.State == "Disabled")
roto.lambda.event_source(queue, handler, {batch_size=1,enabled=false})
local http = roto.lambda.function_("http", {executor={url="http://localhost:8080/jobs",headers={Authorization="local"}}})
assert(http.name == "http")
local result = roto.call("lambda", "ListEventSourceMappings", {})
assert(#result.EventSourceMappings == 1)
"#,
        );
        for _ in 0..2 {
            run(
                &path,
                context(),
                sqs.clone(),
                lambda.clone(),
                Default::default(),
            )
            .unwrap();
        }
        let mappings =
            lambda_call(&lambda, &context(), "ListEventSourceMappings", json!({})).unwrap();
        assert_eq!(mappings["EventSourceMappings"].as_array().unwrap().len(), 1);
        assert_eq!(mappings["EventSourceMappings"][0]["BatchSize"], 1);
        assert!(
            mappings["EventSourceMappings"][0]
                .get("FunctionResponseTypes")
                .is_none()
        );
        let cfg = lambda_call(
            &lambda,
            &context(),
            "GetFunctionConfiguration",
            json!({"FunctionName":"consume"}),
        )
        .unwrap();
        assert_eq!(cfg["Environment"]["Variables"]["SOURCE"], "lua");
        // Invoke on this ordinary thread, outside a Tokio runtime.
        let result = roto_svc_lambda::dispatch(
            &lambda,
            &context(),
            "Invoke",
            &json!({"FunctionName":"consume","Payload":"eyJzZXR1cCI6dHJ1ZX0="}),
        )
        .unwrap();
        let result: Value = serde_json::from_slice(&result.body).unwrap();
        assert!(result.get("FunctionError").is_none());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
    #[test]
    fn setup_errors_have_source_locations_and_do_not_start_workers() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(
            roto_svc_lambda::Lambda::new(&store, Default::default())
                .unwrap()
                .with_sqs(sqs.clone()),
        );
        let path =
            script("roto.sqs.queue('created')\nroto.sqs.queue('bad', {visiblity_timeout=30})\n");
        let error = run(
            &path,
            context(),
            sqs.clone(),
            lambda.clone(),
            Default::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("setup.lua:2"), "{error}");
        assert!(error.contains("visiblity_timeout"));
        // Earlier operations remain persisted and rerunning setup is possible.
        assert!(
            sqs_call(
                &sqs,
                &context(),
                "GetQueueUrl",
                json!({"QueueName":"created"})
            )
            .is_ok()
        );
        assert_eq!(
            lambda.history(&context()).unwrap()["invocations"],
            json!([])
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
    #[test]
    fn setup_can_seed_other_services_and_load_relative_modules() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(
            roto_svc_lambda::Lambda::new(&store, Default::default())
                .unwrap()
                .with_sqs(sqs.clone()),
        );
        let services: HashMap<&'static str, Arc<dyn ServiceHandler>> = HashMap::from([
            (
                "ssm",
                Arc::new(roto_svc_ssm::SsmHandler::new(&store).unwrap()) as Arc<dyn ServiceHandler>,
            ),
            (
                "s3",
                Arc::new(roto_svc_s3::S3Handler::new(&store).unwrap()) as Arc<dyn ServiceHandler>,
            ),
            (
                "sns",
                Arc::new(roto_svc_sns::SnsHandler::new(&store, sqs.clone()).unwrap())
                    as Arc<dyn ServiceHandler>,
            ),
        ]);
        let path = script(
            r#"
assert(require("fixture").value == "hello")
roto.call("ssm", "PutParameter", {Name="/app/mode",Value="development",Type="String"})
assert(roto.call("ssm", "GetParameter", {Name="/app/mode"}).Parameter.Value == "development")
roto.request("s3", "PUT", "/fixtures")
roto.request("s3", "PUT", "/fixtures/hello.txt", {body="hello",headers={["Content-Type"]="text/plain"}})
assert(roto.request("s3", "GET", "/fixtures/hello.txt").body == "hello")
local topic = roto.request("sns", "POST", "/", {headers={["Content-Type"]="application/x-www-form-urlencoded"},body="Action=CreateTopic&Version=2010-03-31&Name=events"})
assert(topic.body:find("arn:aws:sns:us%-east%-1:123456789012:events"))
local queue = roto.sqs.queue("empty")
roto.call("sqs", "ReceiveMessage", {QueueUrl=queue.url, MessageAttributeNames=roto.array({})})
"#,
        );
        std::fs::write(
            path.parent().unwrap().join("fixture.lua"),
            "return { value = 'hello' }",
        )
        .unwrap();
        run(&path, context(), sqs, lambda, services).unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
