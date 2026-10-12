//! Trusted local Lua setup, executed before listeners and delivery workers start.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value as LuaValue};
use roto_core::{RawRequest, RequestContext, ServiceHandler};
use serde_json::{Value, json};

/// Lua API level, exposed as `roto.api_version`; bump only on a breaking change.
pub(crate) const LUA_API_VERSION: u32 = 2;

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

const LINK_HEADERS: &[&str] = &[
    "cache_control",
    "content_disposition",
    "content_encoding",
    "content_language",
    "expires",
    "website_redirect_location",
];

fn string_map(
    lua: &Lua,
    table: &Table,
    name: &str,
) -> mlua::Result<std::collections::BTreeMap<String, String>> {
    match table.get::<LuaValue>(name)? {
        LuaValue::Nil => Ok(Default::default()),
        value => lua.from_value(value),
    }
}

fn link_attrs(lua: &Lua, opts: Option<Table>) -> mlua::Result<roto_svc_s3::LinkAttrs> {
    let Some(opts) = opts else {
        return Ok(Default::default());
    };
    options(&opts, &["content_type", "metadata", "headers"])?;
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in string_map(lua, &opts, "headers")? {
        let normalized = name.to_ascii_lowercase().replace('-', "_");
        if !LINK_HEADERS.contains(&normalized.as_str()) {
            return Err(mlua::Error::external(format!(
                "Unknown link header: {name}"
            )));
        }
        headers.insert(normalized, value);
    }
    Ok(roto_svc_s3::LinkAttrs {
        content_type: opts.get("content_type")?,
        metadata: string_map(lua, &opts, "metadata")?,
        headers,
    })
}

/// Every regular file below `dir` as `(path relative to dir, absolute path)`, sorted.
fn walk_files(
    dir: &Path,
    rel: &str,
    out: &mut Vec<(String, std::path::PathBuf)>,
) -> std::io::Result<()> {
    let mut entries = std::fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        // Follow links to files, but not to directories, so a cycle cannot recurse forever.
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk_files(&path, &format!("{rel}{name}/"), out)?;
        } else if kind.is_file() || (kind.is_symlink() && std::fs::metadata(&path)?.is_file()) {
            out.push((format!("{rel}{name}"), path));
        }
    }
    Ok(())
}

#[derive(Clone, Default, Debug)]
pub struct Hooks(Arc<std::sync::Mutex<HashMap<HookKey, Callback>>>);
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum HookKey {
    Service(String, String),
    Mqtt(String),
}
#[derive(Clone, Debug)]
struct Callback {
    lua: Lua,
    function: mlua::Function,
    lock: Arc<std::sync::Mutex<()>>,
}

fn timed_call(
    lua: &Lua,
    function: &mlua::Function,
    args: (Value, Value),
    timeout: u64,
) -> mlua::Result<Value> {
    lua.set_named_registry_value("roto_runtime_callback", true)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout);
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(1000),
        move |_, _| {
            if std::time::Instant::now() >= deadline {
                return Err(mlua::Error::external("Lua callback timed out"));
            }
            Ok(mlua::VmState::Continue)
        },
    )?;
    let result = (|| {
        let value: LuaValue = function.call((lua.to_value(&args.0)?, lua.to_value(&args.1)?))?;
        lua.from_value(value)
    })();
    lua.remove_hook();
    lua.set_named_registry_value("roto_runtime_callback", false)?;
    result
}

impl Hooks {
    /// Adds the hooks of `other`, replacing registrations for the same service and operation.
    pub fn extend(&self, other: Hooks) {
        let added = other.0.lock().unwrap().clone();
        self.0.lock().unwrap().extend(added);
    }

    pub fn matches(&self, service: &str, operation: Option<&str>) -> bool {
        operation.is_some_and(|operation| {
            self.0
                .lock()
                .unwrap()
                .contains_key(&HookKey::Service(service.into(), operation.into()))
        })
    }

    pub fn matches_mqtt(&self, operation: &str) -> bool {
        self.0
            .lock()
            .unwrap()
            .contains_key(&HookKey::Mqtt(operation.into()))
    }

    /// Invoke a protocol-specific hook using the same VM, lock and deadline as HTTP hooks.
    pub fn mqtt(&self, operation: &str, request: Value, context: Value) -> Result<Value, String> {
        let callback = self
            .0
            .lock()
            .unwrap()
            .get(&HookKey::Mqtt(operation.into()))
            .cloned();
        let Some(callback) = callback else {
            return Ok(Value::Null);
        };
        let _guard = callback.lock.lock().map_err(|error| error.to_string())?;
        timed_call(&callback.lua, &callback.function, (request, context), 1)
            .map_err(|error| error.to_string())
    }

    pub fn intercept(
        &self,
        service: &str,
        operation: Option<&str>,
        ctx: &RequestContext,
        req: &RawRequest,
    ) -> Result<Option<roto_core::RawResponse>, roto_core::AwsError> {
        let Some(operation) = operation else {
            return Ok(None);
        };
        let callback = self
            .0
            .lock()
            .unwrap()
            .get(&HookKey::Service(service.into(), operation.into()))
            .cloned();
        let Some(callback) = callback else {
            return Ok(None);
        };
        let _guard = callback
            .lock
            .lock()
            .map_err(|e| roto_core::AwsError::internal(e.to_string()))?;
        let request = json!({"service":service,"operation":operation,"method":req.method,"path":req.path,"query":req.query,"headers":req.headers.iter().filter(|(name,_)| !name.eq_ignore_ascii_case("authorization")).cloned().collect::<HashMap<_,_>>(),"body":String::from_utf8_lossy(&req.body),"input":serde_json::from_slice::<Value>(&req.body).ok()});
        let context =
            json!({"account_id":ctx.account_id,"region":ctx.region,"request_id":ctx.request_id});
        let value = timed_call(&callback.lua, &callback.function, (request, context), 1)
            .map_err(|e| roto_core::AwsError::internal(e.to_string()))?;
        if value.is_null() {
            return Ok(None);
        }
        let status = value["status"]
            .as_u64()
            .filter(|s| (100..=599).contains(s))
            .ok_or_else(|| {
                roto_core::AwsError::internal("Lua interceptor requires status between 100 and 599")
            })? as u16;
        let body = value["body"]
            .as_str()
            .ok_or_else(|| roto_core::AwsError::internal("Lua interceptor requires a string body"))?
            .as_bytes()
            .to_vec();
        let headers = match value.get("headers") {
            None => Vec::new(),
            Some(value) => value
                .as_object()
                .ok_or_else(|| {
                    roto_core::AwsError::internal("Lua response headers must be a table")
                })?
                .iter()
                .map(|(k, v)| {
                    v.as_str().map(|v| (k.clone(), v.into())).ok_or_else(|| {
                        roto_core::AwsError::internal("Lua response header values must be strings")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(Some(roto_core::RawResponse {
            status,
            headers,
            body,
        }))
    }
}

fn setup_only(lua: &Lua) -> mlua::Result<()> {
    if lua
        .named_registry_value::<bool>("roto_runtime_callback")
        .unwrap_or(false)
    {
        return Err(mlua::Error::external(
            "Roto setup APIs cannot be called from runtime callbacks",
        ));
    }
    Ok(())
}

/// Extra controls for a script posted to a running server rather than run at startup.
pub struct Posted {
    /// Wall-clock limit for Lua instructions.
    pub limit: std::time::Duration,
    /// Receives each `print(...)` line.
    pub output: Arc<std::sync::Mutex<Vec<String>>>,
}

/// A Lua chunk to run with the setup API. `dir` anchors relative paths and `require`.
pub struct Source {
    pub name: String,
    pub code: String,
    pub dir: std::path::PathBuf,
    pub posted: Option<Posted>,
}

pub fn run(
    path: &Path,
    ctx: RequestContext,
    vars: &[(String, String)],
    sqs: Arc<roto_svc_sqs::Sqs>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    s3: Arc<roto_svc_s3::S3>,
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
) -> mlua::Result<Hooks> {
    let path = std::fs::canonicalize(path).map_err(mlua::Error::external)?;
    let source = Source {
        name: format!("@{}", path.display()),
        code: std::fs::read_to_string(&path).map_err(mlua::Error::external)?,
        dir: path.parent().unwrap().to_path_buf(),
        posted: None,
    };
    run_source(source, ctx, vars, sqs, lambda, s3, services).map(|(hooks, _)| hooks)
}

/// Runs `source`; returns the hooks it registered and, for posted scripts, its return value.
pub fn run_source(
    source: Source,
    ctx: RequestContext,
    vars: &[(String, String)],
    sqs: Arc<roto_svc_sqs::Sqs>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    s3: Arc<roto_svc_s3::S3>,
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
) -> mlua::Result<(Hooks, Value)> {
    let parent = source.dir.clone();
    let lua = Lua::new();
    let hooks = Hooks::default();
    let runtime_lock = Arc::new(std::sync::Mutex::new(()));
    let roto = lua.create_table()?;
    roto.set("version", env!("CARGO_PKG_VERSION"))?;
    roto.set("api_version", LUA_API_VERSION)?;
    roto.set("account_id", ctx.account_id.clone())?;
    roto.set("region", ctx.region.clone())?;
    roto.set("endpoint", ctx.base_url.clone())?;
    let roto_vars = lua.create_table()?;
    for (key, value) in vars {
        roto_vars.set(key.as_str(), value.as_str())?;
    }
    roto.set("vars", roto_vars)?;
    roto.set("null", lua.null())?;
    roto.set(
        "array",
        lua.create_function(|lua, table: Table| {
            table.set_metatable(Some(lua.array_metatable()))?;
            Ok(table)
        })?,
    )?;
    roto.set(
        "json_decode",
        lua.create_function(|lua, text: String| {
            let value: Value = serde_json::from_str(&text).map_err(mlua::Error::external)?;
            lua.to_value(&value)
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
        setup_only(lua)?;
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

    let s3_api = lua.create_table()?;
    let (link_s3, link_ctx, link_parent) = (s3.clone(), ctx.clone(), parent.clone());
    s3_api.set(
        "link",
        lua.create_function(
            move |lua, (bucket, key, file, opts): (String, String, String, Option<Table>)| {
                setup_only(lua)?;
                let attrs = link_attrs(lua, opts)?;
                link_s3
                    .link_object(&link_ctx, &bucket, &key, &link_parent.join(&file), attrs)
                    .map_err(mlua::Error::external)?;
                lua.to_value(&json!({"bucket":bucket,"key":key}))
            },
        )?,
    )?;
    let (link_s3, link_ctx, link_parent) = (s3, ctx.clone(), parent.clone());
    s3_api.set(
        "link_dir",
        lua.create_function(
            move |lua, (bucket, prefix, dir, opts): (String, String, String, Option<Table>)| {
                setup_only(lua)?;
                let attrs = link_attrs(lua, opts)?;
                let dir = link_parent.join(&dir);
                let mut files = Vec::new();
                if !dir.is_dir() {
                    return Err(mlua::Error::external(format!(
                        "Cannot link directory {}: not a directory",
                        dir.display()
                    )));
                }
                walk_files(&dir, "", &mut files).map_err(mlua::Error::external)?;
                for (rel, file) in &files {
                    link_s3
                        .link_object(
                            &link_ctx,
                            &bucket,
                            &format!("{prefix}{rel}"),
                            file,
                            attrs.clone(),
                        )
                        .map_err(mlua::Error::external)?;
                }
                lua.to_value(&json!({"bucket":bucket,"prefix":prefix,"count":files.len()}))
            },
        )?,
    )?;

    let (l_service, l_ctx, bind_parent) = (lambda.clone(), ctx.clone(), parent.clone());
    lambda_api.set(
        "bind",
        lua.create_function(move |lua, (name, value): (String, LuaValue)| {
            setup_only(lua)?;
            let mut executor: roto_svc_lambda::Executor = lua.from_value(value)?;
            executor.validate().map_err(mlua::Error::external)?;
            if let roto_svc_lambda::Executor::Command { cwd, .. } = &mut executor {
                *cwd = Some(
                    cwd.take()
                        .map_or_else(|| bind_parent.clone(), |p| bind_parent.join(p)),
                );
            }
            l_service
                .bind_executor(&l_ctx, &name, executor)
                .map_err(mlua::Error::external)?;
            lua.to_value(&json!({"name":name}))
        })?,
    )?;

    let (l_service, l_ctx) = (lambda.clone(), ctx.clone());
    let handler_lock = runtime_lock.clone();
    lambda_api.set("fn",lua.create_function(move |lua,(name,opts): (String,Table)| {
        setup_only(lua)?;
        options(&opts,&["timeout","executor","handler","environment","description","memory_size"])?;
        let mut executor = match opts.get::<Option<mlua::Function>>("handler")? {
            Some(function) => {
                if !matches!(opts.get::<LuaValue>("executor")?, LuaValue::Nil) { return Err(mlua::Error::external("Specify handler or executor, not both")); }
                let runtime = lua.clone();
                let lock = handler_lock.clone();
                roto_svc_lambda::inline_executor(move |event, context| {
                    let timeout = context["timeout"].as_u64().unwrap_or(3);
                    let _guard = lock.lock().map_err(|e| e.to_string())?;
                    timed_call(&runtime, &function, (event, context), timeout).map_err(|e| e.to_string())
                })
            }
            None => lua.from_value(opts.get::<LuaValue>("executor")?)?,
        };
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
        setup_only(lua)?;
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
                setup_only(lua)?;
                let result = match service.as_str() {
                    "sqs" => sqs_call(&call_sqs, &call_ctx, &operation, input)?,
                    "lambda" => lambda_call(&call_lambda, &call_ctx, &operation, input)?,
                    "dynamodb" | "ssm" | "secretsmanager" | "events" | "eventbridge"
                    | "kinesis" | "kms" | "logs" => {
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
                            "Use roto.wire for services using Query/XML protocols",
                        ));
                    }
                };
                lua.to_value(&result)
            },
        )?,
    )?;
    // Escape hatch for every AWS service/protocol, without requiring the listener to be running.
    roto.set("wire",lua.create_function(move |lua,(service,method,path,opts): (String,String,String,Option<Table>)| {
        setup_only(lua)?;
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
    let registrations = hooks.clone();
    let hook_lock = runtime_lock.clone();
    let hooks_api = lua.create_table()?;
    hooks_api.set(
        "request",
        lua.create_function(
            move |lua, (service, operation, callback): (String, String, mlua::Function)| {
                setup_only(lua)?;
                registrations.0.lock().unwrap().insert(
                    HookKey::Service(service, operation),
                    Callback {
                        lua: lua.clone(),
                        function: callback,
                        lock: hook_lock.clone(),
                    },
                );
                Ok(())
            },
        )?,
    )?;
    for (name, operation) in [("mqtt_connect", "Connect"), ("mqtt_subscribe", "Subscribe")] {
        let registrations = hooks.clone();
        let hook_lock = runtime_lock.clone();
        hooks_api.set(
            name,
            lua.create_function(move |lua, callback: mlua::Function| {
                setup_only(lua)?;
                registrations.0.lock().unwrap().insert(
                    HookKey::Mqtt(operation.into()),
                    Callback {
                        lua: lua.clone(),
                        function: callback,
                        lock: hook_lock.clone(),
                    },
                );
                Ok(())
            })?,
        )?;
    }
    roto.set("hooks", hooks_api)?;
    roto.set("sqs", sqs_api)?;
    roto.set("lambda", lambda_api)?;
    roto.set("s3", s3_api)?;
    lua.globals().set("roto", roto)?;
    let chunk = lua.load(&source.code).set_name(source.name);
    let Some(posted) = source.posted else {
        chunk.exec()?;
        return Ok((hooks, Value::Null));
    };
    let output = posted.output;
    lua.globals().set(
        "print",
        lua.create_function(move |lua, args: mlua::Variadic<LuaValue>| {
            let tostring: mlua::Function = lua.globals().get("tostring")?;
            let line = args
                .into_iter()
                .map(|value| tostring.call::<String>(value))
                .collect::<mlua::Result<Vec<_>>>()?
                .join("\t");
            output.lock().unwrap().push(line);
            Ok(())
        })?,
    )?;
    let deadline = std::time::Instant::now() + posted.limit;
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(1000),
        move |_, _| {
            if std::time::Instant::now() >= deadline {
                return Err(mlua::Error::external("Lua script timed out"));
            }
            Ok(mlua::VmState::Continue)
        },
    )?;
    let result: LuaValue = chunk.eval()?;
    lua.remove_hook();
    Ok((hooks, lua.from_value(result).unwrap_or(Value::Null)))
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
    fn inline_handlers_and_interceptors_retain_closures_and_report_errors() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(roto_svc_lambda::Lambda::new(&store, Default::default()).unwrap());
        let path = script(
            r#"
local count = 0
roto.lambda.fn("inline", {timeout=1, handler=function(event, ctx)
  count = count + 1
  if event.fail then error("placeholder failure") end
  if event.loop then while true do end end
  if event.reenter then roto.call("lambda", "ListFunctions", {}) end
  return {count=count, name=event.name, region=ctx.region}
end})
roto.hooks.request("sqs", "SendMessage", function(req, ctx)
  if req.input.pass then return nil end
  return {status=503, headers={["content-type"]="application/json"}, body='{"message":"injected"}'}
end)
roto.hooks.request("sqs", "BadHook", function() return {status=999, body=""} end)
roto.hooks.request("sqs", "Loop", function() while true do end end)
"#,
        );
        let hooks = run(
            &path,
            context(),
            &[],
            sqs,
            lambda.clone(),
            Arc::new(roto_svc_s3::S3::new(&store).unwrap()),
            Default::default(),
        )
        .unwrap();
        let handler = roto_svc_lambda::LambdaHandler(lambda.clone());
        let invoke = |body: &str| {
            handler
                .handle(
                    &context(),
                    &RawRequest {
                        method: "POST".into(),
                        path: "/2015-03-31/functions/inline/invocations".into(),
                        body: body.as_bytes().to_vec(),
                        ..Default::default()
                    },
                )
                .unwrap()
        };
        for count in 1..=2 {
            let response = invoke(r#"{"name":"Ada"}"#);
            assert_eq!(response.status, 200);
            assert_eq!(
                serde_json::from_slice::<Value>(&response.body).unwrap(),
                json!({"count":count,"name":"Ada","region":"us-east-1"})
            );
        }
        for body in [
            r#"{"fail":true}"#,
            r#"{"reenter":true}"#,
            r#"{"loop":true}"#,
        ] {
            let response = invoke(body);
            assert!(
                response
                    .headers
                    .iter()
                    .any(|(k, _)| k == "x-amz-function-error")
            );
        }
        let mut request = RawRequest {
            body: b"{}".to_vec(),
            ..Default::default()
        };
        assert_eq!(
            hooks
                .intercept("sqs", Some("SendMessage"), &context(), &request)
                .unwrap()
                .unwrap()
                .status,
            503
        );
        request.body = br#"{"pass":true}"#.to_vec();
        assert!(
            hooks
                .intercept("sqs", Some("SendMessage"), &context(), &request)
                .unwrap()
                .is_none()
        );
        assert!(
            hooks
                .intercept("sqs", Some("Other"), &context(), &request)
                .unwrap()
                .is_none()
        );
        assert!(
            hooks
                .intercept("sqs", Some("BadHook"), &context(), &request)
                .is_err()
        );
        assert!(
            hooks
                .intercept("sqs", Some("Loop"), &context(), &request)
                .is_err()
        );
        // A timed-out callback must leave the VM usable.
        assert_eq!(invoke("{}").status, 200);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
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
local handler = roto.lambda.fn("consume", {timeout=3, executor={command={"sh", "-c", "cat"}},environment={SOURCE="lua"}})
local mapping = roto.lambda.event_source(queue, handler, {batch_size=2,enabled=false,report_batch_item_failures=true})
assert(mapping.State == "Disabled")
roto.lambda.event_source(queue, handler, {batch_size=1,enabled=false})
local http = roto.lambda.fn("http", {executor={url="http://localhost:8080/jobs",headers={Authorization="local"}}})
assert(http.name == "http")
local result = roto.call("lambda", "ListEventSourceMappings", {})
assert(#result.EventSourceMappings == 1)
"#,
        );
        for _ in 0..2 {
            run(
                &path,
                context(),
                &[],
                sqs.clone(),
                lambda.clone(),
                Arc::new(roto_svc_s3::S3::new(&store).unwrap()),
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
    fn setup_exposes_set_variables_as_roto_vars() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(roto_svc_lambda::Lambda::new(&store, Default::default()).unwrap());
        let path = script(
            r#"
assert(roto.vars.mode == "devhost")
assert(roto.vars.url == "http://x/?a=b")
assert(roto.vars.empty == "")
assert(roto.vars.missing == nil)
roto.sqs.queue(roto.vars.mode .. "-queue")
"#,
        );
        let vars = [
            ("mode".to_string(), "devhost".to_string()),
            ("url".to_string(), "http://x/?a=b".to_string()),
            ("empty".to_string(), String::new()),
        ];
        run(
            &path,
            context(),
            &vars,
            sqs.clone(),
            lambda,
            Arc::new(roto_svc_s3::S3::new(&store).unwrap()),
            Default::default(),
        )
        .unwrap();
        assert!(
            sqs_call(
                &sqs,
                &context(),
                "GetQueueUrl",
                json!({"QueueName":"devhost-queue"})
            )
            .is_ok()
        );
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
            &[],
            sqs.clone(),
            lambda.clone(),
            Arc::new(roto_svc_s3::S3::new(&store).unwrap()),
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
            (
                "kms",
                Arc::new(roto_svc_kms::KmsHandler::new(&store).unwrap()) as Arc<dyn ServiceHandler>,
            ),
            (
                "logs",
                Arc::new(roto_svc_logs::CloudWatchLogsHandler::new(&store).unwrap())
                    as Arc<dyn ServiceHandler>,
            ),
        ]);
        let path = script(
            r#"
assert(require("fixture").value == "hello")
roto.call("ssm", "PutParameter", {Name="/app/mode",Value="development",Type="String"})
assert(roto.call("ssm", "GetParameter", {Name="/app/mode"}).Parameter.Value == "development")
roto.wire("s3", "PUT", "/fixtures")
roto.wire("s3", "PUT", "/fixtures/hello.txt", {body="hello",headers={["Content-Type"]="text/plain"}})
assert(roto.wire("s3", "GET", "/fixtures/hello.txt").body == "hello")
local topic = roto.wire("sns", "POST", "/", {headers={["Content-Type"]="application/x-www-form-urlencoded"},body="Action=CreateTopic&Version=2010-03-31&Name=events"})
assert(topic.body:find("arn:aws:sns:us%-east%-1:123456789012:events"))
local queue = roto.sqs.queue("empty")
roto.call("sqs", "ReceiveMessage", {QueueUrl=queue.url, MessageAttributeNames=roto.array({})})
assert(roto.call("kms", "CreateKey", {}).KeyMetadata.KeyId)
roto.call("logs", "CreateLogGroup", {logGroupName = "/app"})
assert(roto.json_decode('{"a":[1,2]}').a[2] == 2)
assert(not pcall(roto.json_decode, "{"))
"#,
        );
        std::fs::write(
            path.parent().unwrap().join("fixture.lua"),
            "return { value = 'hello' }",
        )
        .unwrap();
        run(
            &path,
            context(),
            &[],
            sqs,
            lambda,
            Arc::new(roto_svc_s3::S3::new(&store).unwrap()),
            services,
        )
        .unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn setup_links_host_files_into_s3() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(
            roto_svc_lambda::Lambda::new(&store, Default::default())
                .unwrap()
                .with_sqs(sqs.clone()),
        );
        let s3 = Arc::new(roto_svc_s3::S3::new(&store).unwrap());
        let handler = roto_svc_s3::S3Handler(s3.clone());
        let path = script(
            r#"
assert(roto.s3.link("links", "one.txt", "data/one.txt").key == "one.txt")
assert(roto.s3.link_dir("links", "demo/", "data").count == 3)
roto.s3.link("links", "typed", "data/one.txt", {content_type="text/x-t", headers={["Cache-Control"]="no-cache"}})
assert(roto.wire("s3", "GET", "/links/demo/sub/two.txt").body == "two")
"#,
        );
        let dir = path.parent().unwrap().to_path_buf();
        std::fs::create_dir_all(dir.join("data/sub")).unwrap();
        std::fs::write(dir.join("data/one.txt"), "one").unwrap();
        std::fs::write(dir.join("data/sub/two.txt"), "two").unwrap();
        std::fs::write(dir.join("data/three.bin"), "three").unwrap();
        let services: HashMap<&'static str, Arc<dyn ServiceHandler>> =
            HashMap::from([("s3", Arc::new(handler) as Arc<dyn ServiceHandler>)]);
        for _ in 0..2 {
            run(
                &path,
                context(),
                &[],
                sqs.clone(),
                lambda.clone(),
                s3.clone(),
                services.clone(),
            )
            .unwrap();
        }
        let list = services["s3"]
            .handle(
                &context(),
                &RawRequest {
                    method: "GET".into(),
                    path: "/links".into(),
                    query: "list-type=2".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        let list = String::from_utf8_lossy(&list.body);
        assert_eq!(list.matches("<Key>").count(), 5, "{list}");
        assert!(list.contains("<Key>demo/three.bin</Key>"), "{list}");

        for (source, expected) in [
            (
                "roto.s3.link('links', 'x', 'data/missing.txt')\n",
                "data/missing.txt",
            ),
            ("roto.s3.link_dir('links', 'x/', 'nodir')\n", "nodir"),
            (
                "roto.s3.link('links', 'x', 'data/one.txt', {bogus=1})\n",
                "bogus",
            ),
        ] {
            let path = script(source);
            let error = run(
                &path,
                context(),
                &[],
                sqs.clone(),
                lambda.clone(),
                s3.clone(),
                services.clone(),
            )
            .unwrap_err()
            .to_string();
            assert!(
                error.contains("setup.lua:1") && error.contains(expected),
                "{error}"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn posted_scripts_return_values_capture_output_and_time_out() {
        let store = Store::ephemeral();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let lambda = Arc::new(roto_svc_lambda::Lambda::new(&store, Default::default()).unwrap());
        let s3 = Arc::new(roto_svc_s3::S3::new(&store).unwrap());
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let source = |code: &str, limit| Source {
            name: "=posted.lua".into(),
            code: code.into(),
            dir: std::env::temp_dir(),
            posted: Some(Posted {
                limit: std::time::Duration::from_millis(limit),
                output: output.clone(),
            }),
        };
        let call = |source| {
            run_source(
                source,
                context(),
                &[],
                sqs.clone(),
                lambda.clone(),
                s3.clone(),
                Default::default(),
            )
        };
        let (_, value) = call(source(
            "print('a', 1)\nreturn {n = roto.sqs.queue('posted').name}",
            5000,
        ))
        .unwrap();
        assert_eq!(value, json!({"n": "posted"}));
        assert_eq!(*output.lock().unwrap(), vec!["a\t1".to_string()]);
        let error = call(source("while true do end", 100))
            .unwrap_err()
            .to_string();
        assert!(error.contains("timed out"), "{error}");
        let error = call(source("\nerror('boom')", 5000))
            .unwrap_err()
            .to_string();
        assert!(error.contains("posted.lua:2"), "{error}");
    }
}
