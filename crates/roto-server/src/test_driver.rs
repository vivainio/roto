//! Sequential Luau tests with a fresh in-process service environment for every case.
use clap::Parser;
use roto_core::store::Store;
use roto_core::{RequestContext, ServiceHandler};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "roto-server test", about = "Run Luau service tests")]
pub struct Options {
    /// Luau test files or directories (directories are searched recursively).
    #[arg(required = true, num_args = 1..)]
    pub paths: Vec<PathBuf>,
    /// Run cases whose names or suite names contain this text.
    #[arg(long)]
    pub filter: Option<String>,
    /// Service bundle directory or ZIP. Defaults to ./services when present.
    #[arg(long, env = "ROTO_LUA_SERVICES")]
    pub lua_services: Option<PathBuf>,
}

fn collect(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    if path.is_dir() {
        let entries = std::fs::read_dir(path).map_err(|e| format!("{}: {e}", path.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let ty = entry.file_type().map_err(|e| e.to_string())?;
            if ty.is_dir()
                || (ty.is_file() && entry.path().extension().is_some_and(|e| e == "luau"))
            {
                collect(&entry.path(), files)?;
            }
        }
    } else if path.is_file() {
        files.push(path.to_path_buf());
    } else {
        return Err(format!("{}: no such file or directory", path.display()));
    }
    Ok(())
}

// Built without listeners or background workers. Recreated for discovery and each case.
struct Environment {
    _store: Arc<Store>,
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    jobs: Arc<crate::lua_service::Jobs>,
    sqs: Arc<roto_svc_sqs::Sqs>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    s3: Arc<roto_svc_s3::S3>,
    scheduler: Arc<roto_svc_scheduler::Scheduler>,
}

impl Environment {
    fn new(bundle: Option<&Path>) -> Result<Self, String> {
        let store = Arc::new(Store::ephemeral());
        let sqs = roto_svc_sqs::SqsHandler::new(&store).map_err(|e| e.to_string())?;
        let iam = roto_svc_iam::IamHandler::new(&store).map_err(|e| e.to_string())?;
        let sts =
            roto_svc_sts::StsHandler::new(&store, iam.0.clone()).map_err(|e| e.to_string())?;
        let logs = roto_svc_logs::CloudWatchLogsHandler::new(&store).map_err(|e| e.to_string())?;
        let lambda =
            roto_svc_lambda::LambdaHandler::unstarted(&store, Default::default(), sqs.0.clone())
                .map_err(|e| e.to_string())?;
        lambda.0.set_logs(logs.0.clone());
        let events =
            roto_svc_eventbridge::EventBridgeHandler::new(&store, lambda.0.clone(), sqs.0.clone())
                .map_err(|e| e.to_string())?;
        let scheduler =
            roto_svc_scheduler::SchedulerHandler::new(&store, lambda.0.clone(), sqs.0.clone())
                .map_err(|e| e.to_string())?;
        let s3 = roto_svc_s3::S3Handler::new(&store).map_err(|e| e.to_string())?;
        s3.0.attach_sqs(sqs.0.clone());
        let dynamodb =
            roto_svc_dynamodb::DynamoDbHandler::new(&store).map_err(|e| e.to_string())?;
        let ssm = roto_svc_ssm::SsmHandler::new(&store).map_err(|e| e.to_string())?;
        let secrets = roto_svc_secretsmanager::SecretsManagerHandler::new(&store)
            .map_err(|e| e.to_string())?;
        let sns =
            roto_svc_sns::SnsHandler::new(&store, sqs.0.clone()).map_err(|e| e.to_string())?;
        let kinesis = roto_svc_kinesis::KinesisHandler::new(&store).map_err(|e| e.to_string())?;
        let kms = roto_svc_kms::KmsHandler::new(&store).map_err(|e| e.to_string())?;
        let rds = roto_svc_rdsdata::RdsDataHandler::new();
        let setup_sqs = sqs.0.clone();
        let setup_lambda = lambda.0.clone();
        let setup_s3 = s3.0.clone();
        let setup_scheduler = scheduler.0.clone();
        let handlers: Vec<Arc<dyn ServiceHandler>> = vec![
            Arc::new(sts),
            Arc::new(sqs),
            Arc::new(iam),
            Arc::new(s3),
            Arc::new(dynamodb),
            Arc::new(ssm),
            Arc::new(secrets),
            Arc::new(sns),
            Arc::new(kinesis),
            Arc::new(kms),
            Arc::new(rds),
            Arc::new(logs),
            Arc::new(lambda),
            Arc::new(events),
            Arc::new(scheduler),
        ];
        let mut services: HashMap<_, _> = handlers.into_iter().map(|h| (h.service(), h)).collect();
        let lua = crate::lua_service::discover(store.clone(), bundle.map(Path::to_path_buf))
            .map_err(|e| e.to_string())?;
        let (lua_handlers, jobs) = crate::lua_service::into_handlers(lua);
        for handler in lua_handlers {
            if services.contains_key(handler.service())
                || matches!(
                    handler.service(),
                    "cloudformation" | "iotdata" | "execute-api" | "apigatewayv2"
                )
            {
                return Err(format!(
                    "Lua service {} conflicts with a Rust service",
                    handler.service()
                ));
            }
            services.insert(handler.service(), handler);
        }
        let cloudformation =
            roto_svc_cloudformation::CloudFormationHandler::new(&store, services.clone())
                .map_err(|e| e.to_string())?;
        services.insert("cloudformation", Arc::new(cloudformation));
        jobs.connect(services.clone());
        Ok(Self {
            _store: store,
            services,
            jobs,
            sqs: setup_sqs,
            lambda: setup_lambda,
            s3: setup_s3,
            scheduler: setup_scheduler,
        })
    }
}

// Reset even if setup or a case raises. The driver owns this process and runs cases sequentially.
struct ClockGuard;
impl ClockGuard {
    fn new() -> Self {
        roto_core::clock::reset();
        roto_core::clock::freeze_ms(1_735_689_600_000).expect("valid test clock");
        Self
    }
}
impl Drop for ClockGuard {
    fn drop(&mut self) {
        roto_core::clock::reset();
    }
}

fn execute(
    file: &Path,
    selection: &str,
    bundle: Option<&Path>,
) -> Result<serde_json::Value, String> {
    let _clock = ClockGuard::new();
    let env = Environment::new(bundle)?;
    let source = source(file, selection, env.jobs.clone(), env.scheduler.clone())?;
    let output = source.posted.as_ref().expect("test output").output.clone();
    let context = RequestContext {
        account_id: std::env::var("ROTO_ACCOUNT_ID").unwrap_or_else(|_| "123456789012".into()),
        region: std::env::var("ROTO_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key: None,
        request_id: roto_core::ids::request_id(),
        base_url: "http://127.0.0.1:5070".into(),
    };
    let result = crate::setup::run_source(
        source,
        context,
        &[],
        env.sqs.clone(),
        env.lambda.clone(),
        env.s3.clone(),
        env.services.clone(),
    );
    // Hooks may retain a Lua VM and callbacks. Drop them before the environment and its databases.
    match result {
        Ok((hooks, mut value)) => {
            drop(hooks);
            if value["results"]
                .as_array()
                .is_some_and(|results| results.iter().any(|r| r["ok"] != true))
                || value["after_all_error"].is_string()
            {
                let lines = output.lock().unwrap().join("\n");
                if !lines.is_empty() {
                    value["captured_output"] = lines.into();
                }
            }
            Ok(value)
        }
        Err(error) => {
            let lines = output.lock().unwrap().join("\n");
            Err(if lines.is_empty() {
                error.to_string()
            } else {
                format!("{lines}\n{error}")
            })
        }
    }
}

pub fn run(options: &Options) -> i32 {
    let mut files = Vec::new();
    for path in &options.paths {
        if let Err(e) = collect(path, &mut files) {
            eprintln!("error: {e}");
            return 1;
        }
    }
    files.sort();
    files.dedup();
    if files.is_empty() {
        eprintln!("error: no Luau test files found");
        return 1;
    }
    let bundle = options.lua_services.as_deref().or_else(|| {
        Path::new("services")
            .is_dir()
            .then_some(Path::new("services"))
    });
    let (mut passed, mut failed) = (0, 0);
    for file in files {
        let names = match execute(&file, "discover", bundle) {
            Ok(serde_json::Value::Array(names)) => names,
            Ok(_) => {
                eprintln!("{}: invalid case list", file.display());
                failed += 1;
                continue;
            }
            Err(e) => {
                eprintln!("{}: {e}", file.display());
                failed += 1;
                continue;
            }
        };
        for (index, entry) in names.iter().enumerate() {
            if let Some(name) = entry.as_str() {
                if options.filter.as_ref().is_some_and(|f| !name.contains(f)) {
                    continue;
                }
                match execute(&file, &index.to_string(), bundle) {
                    Ok(_) => {
                        println!("PASS {} :: {name}", file.display());
                        passed += 1;
                    }
                    Err(e) => {
                        eprintln!("FAIL {} :: {name}\n{e}", file.display());
                        failed += 1;
                    }
                }
            } else if let (Some(suite), Some(cases)) =
                (entry["suite"].as_str(), entry["cases"].as_array())
            {
                let selected: Vec<_> = cases
                    .iter()
                    .enumerate()
                    .filter_map(|(case_index, value)| {
                        let name = value.as_str()?;
                        let full = format!("{suite} :: {name}");
                        (!options.filter.as_ref().is_some_and(|f| !full.contains(f)))
                            .then_some((case_index, name))
                    })
                    .collect();
                if selected.is_empty() {
                    continue;
                }
                let ids = selected
                    .iter()
                    .map(|(i, _)| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                match execute(&file, &format!("suite:{index}:{ids}"), bundle) {
                    Ok(value) => {
                        let results = value["results"].as_array();
                        if results.is_none_or(|r| r.len() != selected.len()) {
                            eprintln!("FAIL {} :: {suite}\nInvalid suite result", file.display());
                            failed += 1;
                            continue;
                        }
                        for ((_, name), result) in selected.iter().zip(results.unwrap()) {
                            if result["name"].as_str() != Some(name) {
                                eprintln!(
                                    "FAIL {} :: {suite} :: {name}\nCase changed during discovery",
                                    file.display()
                                );
                                failed += 1;
                            } else if result["ok"] == true {
                                println!("PASS {} :: {suite} :: {name}", file.display());
                                passed += 1;
                            } else {
                                eprintln!(
                                    "FAIL {} :: {suite} :: {name}\n{}",
                                    file.display(),
                                    result["error"].as_str().unwrap_or("Unknown failure")
                                );
                                failed += 1;
                            }
                        }
                        if let Some(error) = value["after_all_error"].as_str() {
                            eprintln!("FAIL {} :: {suite} :: after_all\n{error}", file.display());
                            failed += 1;
                        }
                        if let Some(output) =
                            value["captured_output"].as_str().filter(|s| !s.is_empty())
                        {
                            eprintln!("{output}");
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "FAIL {} :: {suite}\nSuite interrupted; individual results are unavailable.\n{e}",
                            file.display()
                        );
                        failed += 1;
                    }
                }
            } else {
                eprintln!("{}: invalid case entry", file.display());
                failed += 1;
            }
        }
    }
    println!("{passed} passed; {failed} failed");
    if failed != 0 || passed == 0 { 1 } else { 0 }
}

/// Run a file once for discovery and again in a fresh environment for each registered case.
pub fn source(
    path: &Path,
    selection: &str,
    jobs: std::sync::Arc<crate::lua_service::Jobs>,
    scheduler: std::sync::Arc<roto_svc_scheduler::Scheduler>,
) -> Result<crate::setup::Source, String> {
    let path = std::fs::canonicalize(path).map_err(|e| e.to_string())?;
    let code = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let (index, selected_cases) = if selection == "discover" {
        ("nil".into(), "nil".into())
    } else if let Some(rest) = selection.strip_prefix("suite:") {
        let (group, indices) = rest.split_once(':').ok_or("invalid suite selection")?;
        let group = group.parse::<usize>().map_err(|e| e.to_string())? + 1;
        let indices = indices
            .split(',')
            .map(|part| {
                part.parse::<usize>()
                    .map(|n| format!("[{}]=true", n + 1))
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        (group.to_string(), format!("{{{}}}", indices.join(",")))
    } else {
        (
            (selection.parse::<usize>().map_err(|e| e.to_string())? + 1).to_string(),
            "nil".into(),
        )
    };
    let prefix = r#"
local cases = {}
local names = roto.array({})
local top_names = {}
test = {}
local function validate_case(name, fn)
    assert(type(name) == 'string' and name ~= '', 'case name must be a nonempty string')
    assert(type(fn) == 'function', 'case must be a function')
end
function test.case(name, fn)
    validate_case(name, fn)
    assert(not top_names[name], 'duplicate case: ' .. name)
    top_names[name] = true
    table.insert(names, name)
    table.insert(cases, fn)
end
function test.suite(name, define)
    validate_case(name, define)
    assert(not top_names[name], 'duplicate case or suite: ' .. name)
    top_names[name] = true
    local group = {cases = {}, names = roto.array({}), before_all = {}, before_each = {}, after_each = {}, after_all = {}}
    local suite = {}
    local used = {}
    function suite.case(case_name, fn)
        validate_case(case_name, fn)
        assert(not used[case_name], 'duplicate case: ' .. case_name)
        used[case_name] = true
        table.insert(group.names, case_name)
        table.insert(group.cases, fn)
    end
    for _, kind in {'before_all', 'before_each', 'after_each', 'after_all'} do
        suite[kind] = function(fn)
            assert(type(fn) == 'function', kind .. ' must be a function')
            table.insert(group[kind], fn)
        end
    end
    define(suite)
    table.insert(names, {suite = name, cases = group.names})
    table.insert(cases, group)
end
local function run_suite(group, selected)
    local function protected(fn)
        __roto_test.reset_deadline()
        return xpcall(fn, function(err) return debug.traceback(tostring(err), 2) end)
    end
    local function hooks(functions, continue_on_error)
        local errors = {}
        for _, fn in functions do
            local ok, err = protected(fn)
            if not ok then
                table.insert(errors, err)
                if not continue_on_error then break end
            end
        end
        if #errors > 0 then return table.concat(errors, '\n') end
        return nil
    end
    local before_error = hooks(group.before_all)
    local results = roto.array({})
    for i, fn in group.cases do
        if selected[i] then
            local err = before_error and ('before_all: ' .. before_error) or nil
            if not err then
                local setup_error = hooks(group.before_each)
                if setup_error then err = 'before_each: ' .. setup_error
                else
                    local ok, case_error = protected(fn)
                    if not ok then err = case_error end
                end
                local cleanup_error = hooks(group.after_each, true)
                if cleanup_error then err = (err and (err .. '\n') or '') .. 'after_each: ' .. cleanup_error end
            end
            table.insert(results, {name = group.names[i], ok = err == nil, error = err})
        end
    end
    local after_error = hooks(group.after_all, true)
    return {results = results, after_all_error = after_error}
end
local function equal(a, b, seen)
    if type(a) ~= type(b) then return false end
    if type(a) ~= 'table' then return a == b end
    if seen[a] == b then return true end
    seen[a] = b
    for k, v in a do if not equal(v, b[k], seen) then return false end end
    for k in b do if a[k] == nil then return false end end
    return true
end
function test.equal(actual, expected)
    assert(equal(actual, expected, {}), 'values differ: actual=' .. roto.json_encode(actual) .. ' expected=' .. roto.json_encode(expected))
end
function test.try_call(service, operation, input)
    if __roto_test.is_bundle(service) then return __roto_test.try_bundle_call(service, operation, input) end
    return __roto_test.try_builtin_call(service, operation, input)
end
function test.call(service, operation, input)
    local ok, result = test.try_call(service, operation, input)
    if not ok then error((result.code or "Error") .. ": " .. (result.message or ""), 2) end
    return result
end
-- The typed client facade uses the same decoded calls as test.call.
test.aws = setmetatable({}, {__index = function(clients, service)
    local client = setmetatable({}, {__index = function(operations, operation)
        local call = function(input) return test.call(service, operation, input) end
        rawset(operations, operation, call)
        return call
    end})
    rawset(clients, service, client)
    return client
end})
function test.expect_error(code, fn)
    local ok, err = fn()
    assert(not ok, 'expected error ' .. code)
    assert(type(err) == 'table' and err.code == code, 'expected error ' .. code .. ', got ' .. (if type(err) == 'table' then roto.json_encode(err) else tostring(err)))
    return err
end
test.advance = __roto_test.advance
test.drain = __roto_test.drain
test.now = __roto_test.now
"#;
    let suffix = format!(
        "\nlocal selected = {index}\nlocal selected_cases = {selected_cases}\nif selected then assert(cases[selected], 'case disappeared during discovery'); if selected_cases then return run_suite(cases[selected], selected_cases) else cases[selected]() end end\nreturn names\n"
    );
    Ok(crate::setup::Source {
        name: format!("@{}", path.display()),
        code: format!("{prefix}\n{code}\n{suffix}"),
        dir: path.parent().unwrap().to_path_buf(),
        posted: Some(crate::setup::Posted {
            limit: std::time::Duration::from_secs(30),
            output: Default::default(),
        }),
        trace: None,
        test_jobs: Some(jobs),
        test_scheduler: Some(scheduler),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_releases_store_after_loading_bundle_service() {
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services");
        let env = Environment::new(Some(&bundle)).unwrap();
        let store = Arc::downgrade(&env._store);
        let jobs = Arc::downgrade(&env.jobs);
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://127.0.0.1:5070".into(),
        };
        env.jobs
            .call_decoded("states", &ctx, "ListStateMachines", serde_json::json!({}))
            .expect("states loaded")
            .expect("states call succeeded");
        drop(env);
        assert!(
            jobs.upgrade().is_none(),
            "job scheduler retained after case"
        );
        assert!(store.upgrade().is_none(), "store retained after case");
    }

    #[test]
    fn test_environment_releases_store_after_running_a_case() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let env = Environment::new(Some(&root.join("services"))).unwrap();
        let store = Arc::downgrade(&env._store);
        let source = source(
            &root.join("tests/luau/states.luau"),
            "0",
            env.jobs.clone(),
            env.scheduler.clone(),
        )
        .unwrap();
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://127.0.0.1:5070".into(),
        };
        let (hooks, _) = crate::setup::run_source(
            source,
            ctx,
            &[],
            env.sqs.clone(),
            env.lambda.clone(),
            env.s3.clone(),
            env.services.clone(),
        )
        .unwrap();
        drop(hooks);
        drop(env);
        assert!(store.upgrade().is_none(), "a case retained its store");
    }

    #[test]
    fn suite_timeout_interrupts_the_suite() {
        let path = std::env::temp_dir().join(format!(
            "roto-suite-timeout-{}.luau",
            roto_core::ids::request_id()
        ));
        std::fs::write(
            &path,
            r#"
test.suite('timeout', function(suite)
    suite.case('loop', function() while true do end end)
    suite.case('next', function() end)
end)
test.suite('later suite', function(suite)
    suite.case('still runs', function() end)
end)
"#,
        )
        .unwrap();
        let env = Environment::new(None).unwrap();
        let mut script = source(
            &path,
            "suite:0:0,1",
            env.jobs.clone(),
            env.scheduler.clone(),
        )
        .unwrap();
        script.posted.as_mut().unwrap().limit = std::time::Duration::from_millis(25);
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://127.0.0.1:5070".into(),
        };
        let result = crate::setup::run_source(
            script,
            ctx,
            &[],
            env.sqs.clone(),
            env.lambda.clone(),
            env.s3.clone(),
            env.services.clone(),
        );
        assert!(result.err().unwrap().to_string().contains("timed out"));
        let next_env = Environment::new(None).unwrap();
        let next = source(
            &path,
            "suite:1:0",
            next_env.jobs.clone(),
            next_env.scheduler.clone(),
        )
        .unwrap();
        let next_ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://127.0.0.1:5070".into(),
        };
        let (_, value) = crate::setup::run_source(
            next,
            next_ctx,
            &[],
            next_env.sqs.clone(),
            next_env.lambda.clone(),
            next_env.s3.clone(),
            next_env.services.clone(),
        )
        .unwrap();
        assert_eq!(value["results"][0]["ok"], true);
        std::fs::remove_file(path).unwrap();
    }
}
