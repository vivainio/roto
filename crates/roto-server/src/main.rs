mod alb;
mod apigateway_v2;
mod aws_client;
mod gateway;
mod inspection;
mod lua_api;
mod lua_service;
mod lua_wire;
mod luau_stubs;
mod prune;
mod setup;
mod supported;
mod trace;
mod unsupported;
mod websocket;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post};
use clap::Parser;
use roto_core::sigv4::CredentialScope;
use roto_core::store::{Store, StoreOptions};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler, ids};

/// Parses `KEY=VALUE` for `--set`; the value may be empty or contain further `=`.
fn parse_set_var(raw: &str) -> Result<(String, String), String> {
    match raw.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err(format!("expected KEY=VALUE, got `{raw}`")),
    }
}

#[derive(Parser)]
#[command(version, about = "roto: an AWS simulator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Write the Luau type definitions (`roto.d.luau`) for editors into DIR and exit.
    #[arg(long, value_name = "DIR")]
    lua_stubs: Option<PathBuf>,
    /// Type-check the given Luau setup scripts (needs luau-lsp on PATH) and exit.
    #[arg(long, value_name = "FILE", num_args = 1.., conflicts_with = "lua_stubs")]
    check: Vec<PathBuf>,
    /// Type-check Luau service modules (and their common.luau) and exit.
    #[arg(long, value_name = "FILE", num_args = 1.., conflicts_with_all = ["lua_stubs", "check"])]
    check_services: Vec<PathBuf>,
    /// Port for the local AWS simulator.
    #[arg(long, default_value_t = 5070)]
    port: u16,
    #[arg(long, default_value = "roto-data", env = "ROTO_DATA_DIR")]
    data_dir: PathBuf,
    /// Keep everything in memory; nothing is written to disk.
    #[arg(long)]
    ephemeral: bool,
    /// fsync on every commit (SQLite synchronous=FULL).
    #[arg(long)]
    durable: bool,
    #[arg(long, default_value = "123456789012", env = "ROTO_ACCOUNT_ID")]
    account_id: String,
    /// Local HTTP API v2 routes, exposed under /roto-http/.
    #[arg(long)]
    http_routes: Option<PathBuf>,
    /// Trusted Lua resource setup, run before listening or processing events.
    #[arg(long)]
    setup: Option<PathBuf>,
    /// String variable exposed to the Lua setup script as `roto.vars.KEY`; repeatable.
    #[arg(long = "set", value_name = "KEY=VALUE", value_parser = parse_set_var)]
    set: Vec<(String, String)>,
    /// Optional Lua service bundle directory or ZIP with a root manifest.json.
    #[arg(long, env = "ROTO_LUA_SERVICES")]
    lua_services: Option<PathBuf>,
    /// Accept Lua scripts at POST /roto-api/lua from loopback clients (runs code on this host).
    #[arg(long)]
    enable_lua_api: bool,
    /// Write one JSON object per AWS HTTP request; request bodies and credentials are omitted.
    #[arg(long)]
    trace: Option<PathBuf>,
    /// Serve application load balancers created through the Luau `elasticloadbalancing` service on
    /// this port: forwards requests to registered targets and runs their health checks.
    #[arg(long, value_name = "PORT")]
    alb_port: Option<u16>,
}

impl App {
    /// Set by the setup script's `roto.set_account`: every request then runs in this account.
    fn forced_account(&self) -> Option<String> {
        self.hooks.lock().unwrap().forced_account()
    }

    /// Account for server-originated work (setup, inspection, MQTT).
    fn account_id(&self) -> String {
        self.forced_account()
            .unwrap_or_else(|| self.default_account.clone())
    }
}

struct App {
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    #[allow(dead_code)] // handed to stateful services as they are added
    store: Arc<Store>,
    /// Account for requests whose key names none; a script's `roto.set_account` overrides it.
    default_account: String,
    unsupported: unsupported::Unsupported,
    trace: trace::Trace,
    gateway: gateway::Gateway,
    api_gateway_v2: apigateway_v2::Registry,
    websockets: websocket::Hub,
    hooks: std::sync::Mutex<setup::Hooks>,
    lua_api: Option<lua_api::LuaApi>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    if !args.check.is_empty() {
        std::process::exit(luau_stubs::check(&args.check));
    }
    if !args.check_services.is_empty() {
        std::process::exit(luau_stubs::check_services(&args.check_services));
    }
    if let Some(dir) = &args.lua_stubs {
        std::fs::create_dir_all(dir)
            .and_then(|()| std::fs::write(dir.join("roto.d.luau"), luau_stubs::render()))
            .and_then(|()| {
                std::fs::write(dir.join("roto-service.d.luau"), luau_stubs::render_service())
            })
            .unwrap_or_else(|e| {
                eprintln!("error: cannot write Lua stubs to {}: {e}", dir.display());
                std::process::exit(1);
            });
        return;
    }
    let trace = trace::Trace::create(args.trace.as_deref()).unwrap_or_else(|e| {
        eprintln!("error opening trace file: {e}");
        std::process::exit(1);
    });

    let gateway = args
        .http_routes
        .as_deref()
        .map(gateway::Gateway::load)
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("error loading HTTP routes: {e}");
            std::process::exit(1);
        })
        .unwrap_or_default();

    let store = Arc::new(if args.ephemeral {
        Store::ephemeral()
    } else {
        Store::open(
            &args.data_dir,
            StoreOptions {
                durable: args.durable,
            },
        )
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        })
    });

    let sqs = roto_svc_sqs::SqsHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let iam = roto_svc_iam::IamHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let sts = roto_svc_sts::StsHandler::new(&store, iam.0.clone()).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let logs = roto_svc_logs::CloudWatchLogsHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let lambda =
        roto_svc_lambda::LambdaHandler::unstarted(&store, Default::default(), sqs.0.clone())
            .unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            });
    lambda.0.set_logs(logs.0.clone());
    let events = Arc::new(
        roto_svc_eventbridge::EventBridgeHandler::new(&store, lambda.0.clone(), sqs.0.clone())
            .unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            }),
    );
    let s3 = roto_svc_s3::S3Handler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let dynamodb = roto_svc_dynamodb::DynamoDbHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let ssm = roto_svc_ssm::SsmHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let secretsmanager = roto_svc_secretsmanager::SecretsManagerHandler::new(&store)
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    let sns = roto_svc_sns::SnsHandler::new(&store, sqs.0.clone()).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let kinesis = roto_svc_kinesis::KinesisHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let kms = roto_svc_kms::KmsHandler::new(&store).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let rds_data = roto_svc_rdsdata::RdsDataHandler::new();
    let rds_data_backend = rds_data.0.clone();
    let setup_sqs = sqs.0.clone();
    let sqs_for_s3 = sqs.0.clone();
    s3.0.attach_sqs(sqs.0.clone());
    let setup_lambda = lambda.0.clone();
    let setup_s3 = s3.0.clone();
    let lambda = Arc::new(lambda);
    let s3 = Arc::new(s3);
    let handlers: Vec<Arc<dyn ServiceHandler>> = vec![
        Arc::new(sts),
        Arc::new(sqs),
        Arc::new(iam),
        s3.clone(),
        Arc::new(dynamodb),
        Arc::new(ssm),
        Arc::new(secretsmanager),
        Arc::new(sns),
        Arc::new(kinesis),
        Arc::new(kms),
        Arc::new(rds_data),
        Arc::new(logs),
        lambda.clone(),
        events.clone(),
    ];
    let mut services: HashMap<_, _> = handlers.into_iter().map(|h| (h.service(), h)).collect();
    let lua_services =
        lua_service::discover(store.clone(), args.lua_services).unwrap_or_else(|e| {
            eprintln!("error discovering Lua services: {e}");
            std::process::exit(1);
        });
    for handler in lua_services {
        if services.contains_key(handler.service())
            || matches!(
                handler.service(),
                "cloudformation" | "iotdata" | "execute-api" | "apigatewayv2"
            )
        {
            eprintln!(
                "error: Lua service {} conflicts with a Rust service",
                handler.service()
            );
            std::process::exit(1);
        }
        services.insert(handler.service(), Arc::new(handler));
    }
    let cloudformation =
        roto_svc_cloudformation::CloudFormationHandler::new(&store, services.clone())
            .unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            });
    services.insert("cloudformation", Arc::new(cloudformation));
    let endpoint = format!(
        "http://{}:{}",
        if args.host == "0.0.0.0" {
            "127.0.0.1"
        } else {
            &args.host
        },
        args.port
    );
    let alb_store = store.clone();
    let lua_api = args.enable_lua_api.then(|| lua_api::LuaApi {
        region: "us-east-1".into(),
        endpoint: endpoint.clone(),
        vars: args.set.clone(),
        sqs: setup_sqs.clone(),
        lambda: setup_lambda.clone(),
        s3: setup_s3.clone(),
        running: Default::default(),
    });
    let app = Arc::new(App {
        services,
        store,
        default_account: args.account_id,
        unsupported: Default::default(),
        trace,
        gateway,
        api_gateway_v2: apigateway_v2::Registry::default(),
        websockets: websocket::Hub::default(),
        hooks: Default::default(),
        lua_api,
    });

    {
        let app = Arc::downgrade(&app);
        rds_data_backend.set_resolver(Arc::new(move |request| {
            let app = app.upgrade().ok_or("server is shutting down")?;
            let hooks = app.hooks.lock().unwrap().clone();
            hooks.rds_data_connection(
                serde_json::json!({
                    "resource_arn": request.resource_arn,
                    "secret_arn": request.secret_arn,
                    "database": request.database,
                }),
                serde_json::json!({
                    "account_id": request.account_id,
                    "region": request.region,
                }),
            )
        }));
    }

    if let Some(path) = args.setup {
        let set_vars = args.set;
        let ctx = RequestContext {
            account_id: app.account_id(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: ids::request_id(),
            base_url: endpoint.clone(),
        };
        let services = app.services.clone();
        let sink: setup::TraceSink = {
            let app = app.clone();
            Arc::new(move |entry| app.trace.record(entry))
        };
        let hooks = tokio::task::spawn_blocking(move || {
            setup::run_traced(
                &path,
                ctx,
                &set_vars,
                setup_sqs,
                setup_lambda,
                setup_s3,
                services,
                Some(sink),
            )
            .map_err(|e| e.to_string())
        })
        .await
        .expect("setup task failed")
        .unwrap_or_else(|e| {
            eprintln!("error in Lua setup: {e}");
            std::process::exit(1);
        });
        *app.hooks.lock().unwrap() = hooks;
    }
    let router = Router::new()
        .route("/roto-api", get(inspection_ui))
        .route("/roto-api/", get(inspection_ui))
        .route("/roto-api/resources", get(inspection::catalog))
        .route("/roto-api/sqs/messages", get(inspection::sqs_messages))
        .route(
            "/roto-api/sns/sms-messages",
            get(inspection::sns_sms_messages),
        )
        .route(
            "/roto-api/resources/{service}/{table}",
            get(inspection::records),
        )
        .route("/roto-api/dynamodb/query", post(inspection::dynamodb_query))
        .route("/roto-api/s3/object", get(inspection::object))
        .route("/roto-api/health", get(inspection::health))
        .route("/mqtt", get(websocket::mqtt::websocket))
        .route("/roto-api/ws", get(websocket::websocket))
        .route("/roto-api/iot/ws", get(websocket::websocket))
        .route("/v2/apis", any(handle))
        .route("/v2/apis/{api_id}", any(handle))
        .route(
            "/{api_id}",
            get(websocket::apigateway_default_websocket).fallback(handle),
        )
        .route(
            "/{api_id}/{stage_name}",
            get(websocket::apigateway_websocket).fallback(handle),
        )
        .route("/roto-api/trace", get(request_trace))
        .route("/roto-api/unsupported", get(unsupported_calls))
        .route("/roto-api/reset", post(reset))
        .route("/roto-api/lua", post(lua_api::run))
        // moto's server-mode test harness resets state through this path.
        .route("/moto-api/reset", post(reset))
        .fallback(handle)
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            s3_browser,
        ))
        .with_state(app);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("invalid host/port");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: cannot bind {addr}: {e}");
            std::process::exit(1);
        });
    if let Some(port) = args.alb_port {
        let alb_addr: SocketAddr = format!("{}:{}", args.host, port)
            .parse()
            .expect("invalid host/alb-port");
        match alb::start(alb_store, alb_addr).await {
            Ok(bound) => tracing::info!("load balancer proxy listening on http://{bound}"),
            Err(e) => {
                eprintln!("error: cannot bind load balancer proxy {alb_addr}: {e}");
                std::process::exit(1);
            }
        }
    }
    lambda.start(endpoint);
    events.start();
    s3.start_notifications_with_queues(lambda.0.clone(), events.0.clone(), sqs_for_s3);
    tracing::info!(
        "roto listening on http://{addr} ({})",
        if args.ephemeral {
            "ephemeral".into()
        } else {
            format!("data: {}", args.data_dir.display())
        }
    );
    let shutdown_lambda = lambda.0.clone();
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown_lambda.shutdown_executors();
    })
    .await
    .unwrap();
}

async fn inspection_ui() -> Html<&'static str> {
    Html(include_str!("inspection.html"))
}

async fn reset(State(app): State<Arc<App>>) -> Response {
    let app2 = app.clone();
    let res =
        tokio::task::spawn_blocking(move || app2.services.values().try_for_each(|s| s.reset()))
            .await
            .unwrap();
    match res {
        Ok(()) => {
            app.unsupported.reset();
            app.api_gateway_v2.reset();
            (StatusCode::OK, r#"{"status":"ok"}"#).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn unsupported_calls(State(app): State<Arc<App>>) -> axum::Json<serde_json::Value> {
    axum::Json(app.unsupported.snapshot())
}

async fn request_trace(
    State(app): State<Arc<App>>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::Json<serde_json::Value> {
    let trace_id = params.get("trace_id").map(String::as_str);
    axum::Json(app.trace.snapshot(trace_id))
}

// Intercept browser routes before API and inspection routes on website hosts.
async fn s3_browser(
    State(app): State<Arc<App>>,
    mut req: Request<Body>,
    next: axum::middleware::Next,
) -> Response {
    let host_bucket = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(roto_svc_s3::website::bucket_from_host);
    let path = req.uri().path().to_owned();
    let target = if let Some(bucket) = host_bucket {
        Some((bucket, path, String::new()))
    } else if let Some(rest) = path.strip_prefix("/s3/").filter(|_| {
        !req.headers().contains_key("authorization")
            && !req
                .uri()
                .query()
                .is_some_and(|q| q.contains("X-Amz-Algorithm="))
    }) {
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            None
        } else {
            if !rest.contains('/') {
                let location = format!(
                    "{path}/{}",
                    req.uri()
                        .query()
                        .map(|q| format!("?{q}"))
                        .unwrap_or_default()
                );
                return (StatusCode::TEMPORARY_REDIRECT, [("location", location)]).into_response();
            }
            Some((
                roto_protocol::restxml::percent_decode_path(bucket),
                format!("/{key}"),
                format!("/s3/{bucket}"),
            ))
        }
    } else {
        None
    };
    let Some((bucket, path, prefix)) = target else {
        return next.run(req).await;
    };
    let Some(handler) = app.services.get("s3").cloned() else {
        return (StatusCode::NOT_FOUND, "S3 is not initialized").into_response();
    };
    req.headers_mut().remove("x-roto-website-prefix");
    let mut headers: Vec<_> = req
        .headers()
        .iter()
        .filter(|(n, _)| n.as_str() != "host")
        .filter_map(|(n, v)| Some((n.as_str().to_owned(), v.to_str().ok()?.to_owned())))
        .collect();
    headers.retain(|(name, _)| name != "x-roto-website-authority");
    headers.push((
        "x-roto-website-authority".into(),
        req.headers()
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("localhost")
            .to_owned(),
    ));
    headers.push(("host".into(), format!("{bucket}.s3-website.localhost")));
    headers.push(("x-roto-website-prefix".into(), prefix));
    let raw = RawRequest {
        method: req.method().to_string(),
        path,
        query: req.uri().query().unwrap_or("").into(),
        headers,
        ..Default::default()
    };
    let ctx = RequestContext {
        account_id: app.account_id(),
        region: "us-east-1".into(),
        access_key: None,
        request_id: ids::request_id(),
        base_url: String::new(),
    };
    match tokio::task::spawn_blocking(move || handler.handle(&ctx, &raw)).await {
        Ok(Ok(raw)) => into_response(raw),
        Ok(Err(e)) => (
            StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.message,
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "S3 browser failed").into_response(),
    }
}

async fn handle(State(app): State<Arc<App>>, req: Request<Body>) -> Response {
    let (parts, body) = req.into_parts();
    let body: Bytes = match axum::body::to_bytes(body, 512 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "request body too large").into_response(),
    };
    let headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_string(), v.to_str().ok()?.to_string())))
        .collect();
    let raw = RawRequest {
        method: parts.method.as_str().to_string(),
        path: parts.uri.path().to_string(),
        query: parts.uri.query().unwrap_or("").to_string(),
        headers,
        body: body.to_vec(),
    };

    let request_id = ids::request_id();
    let scope = scope_of(&raw);
    let service = scope
        .as_ref()
        .map(|s| s.service.clone())
        .or_else(|| host_service(&raw));
    let access_key = scope.as_ref().map(|s| s.access_key.clone());
    let account_id = app.forced_account().unwrap_or_else(|| {
        access_key
            .as_deref()
            .and_then(|k| app.services.values().find_map(|svc| svc.resolve_account(k)))
            .or_else(|| access_key.as_deref().and_then(account_id_from_access_key))
            .unwrap_or_else(|| app.default_account.clone())
    });
    let ctx = RequestContext {
        account_id,
        region: scope
            .as_ref()
            .map(|s| s.region.clone())
            .or_else(|| region_from_user_agent(&raw))
            .unwrap_or_else(|| "us-east-1".into()),
        access_key,
        request_id: request_id.clone(),
        base_url: raw
            .header("host")
            .map(|h| format!("http://{h}"))
            .unwrap_or_else(|| "http://localhost:5070".into()),
    };

    let hook_service = service.clone().or_else(|| {
        (ctx.access_key.is_none())
            .then(|| {
                app.services
                    .values()
                    .find(|h| h.claims_unsigned(&raw))
                    .map(|h| h.service().to_owned())
            })
            .flatten()
    });
    if let Some(hook_service) = hook_service {
        let hooks = app.hooks.lock().unwrap().clone();
        let operation = if hook_service == "iotdata"
            && raw.method == "POST"
            && raw.path.starts_with("/topics/")
        {
            Some("Publish".to_owned())
        } else {
            trace::operation(Some(&hook_service), &raw)
        };
        if hooks.matches(&hook_service, operation.as_deref()) {
            let context = ctx.clone();
            let request = raw.clone();
            let callback_operation = operation.clone();
            let callback_service = hook_service.clone();
            let started = std::time::Instant::now();
            let intercepted = tokio::task::spawn_blocking(move || {
                hooks.intercept(
                    &callback_service,
                    callback_operation.as_deref(),
                    &context,
                    &request,
                )
            })
            .await;
            let response = match intercepted {
                Ok(Ok(None)) => None,
                Ok(Ok(Some(response))) => Some(response),
                Ok(Err(error)) => Some(plain_error(&error, &request_id)),
                Err(_) => Some(plain_error(
                    &AwsError::internal("Lua interceptor panicked"),
                    &request_id,
                )),
            };
            if let Some(response) = response {
                let error_code = trace::response_error_code(&response);
                app.trace.record(trace::entry(
                    &raw,
                    &ctx,
                    Some(&hook_service),
                    operation,
                    response.status,
                    if response.status < 400 {
                        "success"
                    } else {
                        "error"
                    },
                    error_code.as_deref(),
                    started.elapsed().as_millis(),
                ));
                return into_request_response(response, &raw);
            }
        }
    }

    if raw.path == "/roto-http" || raw.path.starts_with("/roto-http/") {
        let source_ip = parts
            .extensions
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .map(|peer| peer.0.ip().to_string())
            .unwrap_or_else(|| "127.0.0.1".into());
        let protocol = format!("{:?}", parts.version);
        let mut ctx = ctx;
        ctx.account_id = app.account_id();
        ctx.access_key = None;
        let app = app.clone();
        let response_request = raw.clone();
        return match tokio::task::spawn_blocking(move || {
            app.gateway.handle(
                app.services["lambda"].as_ref(),
                &ctx,
                &raw,
                &source_ip,
                &protocol,
            )
        })
        .await
        {
            Ok(response) => finish_request_response(into_response(response), &response_request),
            Err(_) => finish_request_response(
                (StatusCode::BAD_GATEWAY, "Internal Server Error").into_response(),
                &response_request,
            ),
        };
    }

    if service.as_deref() == Some("iotdata")
        && raw.method == "POST"
        && raw.path.starts_with("/topics/")
    {
        let started = std::time::Instant::now();
        let result = app.websockets.publish_iot(&raw);
        let response = match result {
            Ok(()) => RawResponse {
                status: 200,
                headers: Vec::new(),
                body: Vec::new(),
            },
            Err(error) => plain_error(&error, &request_id),
        };
        let outcome = if response.status < 400 {
            "success"
        } else {
            "error"
        };
        let error_code = trace::response_error_code(&response);
        app.trace.record(trace::entry(
            &raw,
            &ctx,
            Some("iotdata"),
            Some("Publish".into()),
            response.status,
            outcome,
            error_code.as_deref(),
            started.elapsed().as_millis(),
        ));
        return finish_request_response(into_response(response), &raw);
    }

    if raw.path == "/v2/apis" || raw.path.starts_with("/v2/apis/") {
        let started = std::time::Instant::now();
        let response = app.api_gateway_v2.handle(&raw);
        let outcome = if response.status < 400 {
            "success"
        } else {
            "error"
        };
        let error_code = trace::response_error_code(&response);
        app.trace.record(trace::entry(
            &raw,
            &ctx,
            Some("apigatewayv2"),
            apigateway_v2::operation(&raw),
            response.status,
            outcome,
            error_code.as_deref(),
            started.elapsed().as_millis(),
        ));
        return finish_request_response(into_response(response), &raw);
    }

    if service.as_deref() == Some("execute-api")
        && raw.method == "POST"
        && raw.path.contains("/@connections/")
    {
        let started = std::time::Instant::now();
        let result = websocket::handle_management_post(&app.websockets, &raw);
        let response = match result {
            Ok(()) => RawResponse {
                status: 200,
                headers: Vec::new(),
                body: Vec::new(),
            },
            Err(error) => plain_error(&error, &request_id),
        };
        let outcome = if response.status < 400 {
            "success"
        } else {
            "error"
        };
        let error_code = trace::response_error_code(&response);
        app.trace.record(trace::entry(
            &raw,
            &ctx,
            Some("execute-api"),
            Some("PostToConnection".into()),
            response.status,
            outcome,
            error_code.as_deref(),
            started.elapsed().as_millis(),
        ));
        return finish_request_response(into_response(response), &raw);
    }

    let handler = service
        .as_deref()
        .and_then(|s| app.services.get(s))
        .cloned()
        .or_else(|| {
            // Unsigned requests carry no credential scope; let a service claim them.
            (ctx.access_key.is_none())
                .then(|| {
                    app.services
                        .values()
                        .find(|h| h.claims_unsigned(&raw))
                        .cloned()
                })
                .flatten()
        });
    let Some(handler) = handler else {
        app.trace.record(trace::entry(
            &raw,
            &ctx,
            service.as_deref(),
            trace::operation(service.as_deref(), &raw),
            400,
            "unroutable",
            Some("UnrecognizedClientException"),
            0,
        ));
        app.unsupported.record(unsupported::Call::new(
            service.as_deref(),
            &raw,
            &ctx,
            "unroutable",
            None,
        ));
        let msg = format!(
            "roto cannot route this request to a service ({} {})",
            raw.method, raw.path
        );
        return into_request_response(
            plain_error(
                &AwsError::sender(400, "UnrecognizedClientException", msg),
                &request_id,
            ),
            &raw,
        );
    };
    let mut call =
        unsupported::Call::new(Some(handler.service()), &raw, &ctx, "not_implemented", None);
    let service_name = handler.service();
    let mut operation = handler
        .operation_name(&raw)
        .or_else(|| trace::operation(Some(service_name), &raw));
    call.operation = operation.clone();
    let started = std::time::Instant::now();
    let request = raw.clone();
    let context = ctx.clone();
    let result = tokio::task::spawn_blocking(move || handler.handle(&context, &request)).await;
    let unsupported = match &result {
        Ok(Ok(response)) => unsupported::response(response),
        Ok(Err(error)) => unsupported::error(error),
        Err(_) => None,
    };
    if let Some((reason, reported_operation)) = unsupported {
        call.reason = reason;
        if reported_operation.is_some() {
            call.operation = reported_operation.clone();
            operation = reported_operation;
        }
        app.unsupported.record(call);
    }
    {
        let (status, outcome, error_code) = match &result {
            Ok(Ok(response)) if response.status < 400 => (response.status, "success", None),
            Ok(Ok(response)) => {
                let is_unsupported = unsupported::response(response).is_some();
                (
                    response.status,
                    if is_unsupported {
                        "unsupported"
                    } else {
                        "error"
                    },
                    trace::response_error_code(response),
                )
            }
            Ok(Err(error)) => (
                error.status,
                if unsupported::error(error).is_some() {
                    "unsupported"
                } else {
                    "error"
                },
                Some(error.code.clone()),
            ),
            Err(_) => (500, "handler_panic", Some("HandlerPanic".to_owned())),
        };
        app.trace.record(trace::entry(
            &raw,
            &ctx,
            Some(service_name),
            operation,
            status,
            outcome,
            error_code.as_deref(),
            started.elapsed().as_millis(),
        ));
    }
    let response = match result {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => plain_error(&e, &request_id),
        Err(_) => plain_error(&AwsError::internal("handler panicked"), &request_id),
    };
    into_request_response(response, &raw)
}

/// SigV4 scope from the `Authorization` header or a presigned URL's `X-Amz-Credential`.
fn scope_of(req: &RawRequest) -> Option<CredentialScope> {
    if let Some(s) = req
        .header("authorization")
        .and_then(CredentialScope::from_authorization)
    {
        return Some(s);
    }
    let credential = req
        .query
        .split('&')
        .find_map(|p| p.strip_prefix("X-Amz-Credential="))?;
    CredentialScope::parse(&credential.replace("%2F", "/").replace("%2f", "/"))
}

/// `sts.us-east-1.amazonaws.com` / `sts.localhost` → `sts`.
fn host_service(req: &RawRequest) -> Option<String> {
    let host = req.header("host")?;
    let first = host.split(['.', ':']).next()?;
    (!first.is_empty() && first != "localhost" && first.parse::<u8>().is_err())
        .then(|| first.to_string())
}

fn plain_error(e: &AwsError, request_id: &str) -> RawResponse {
    let body = format!("{{\"__type\":{:?},\"message\":{:?}}}", e.code, e.message);
    RawResponse {
        status: e.status,
        headers: vec![
            ("content-type".into(), "application/json".into()),
            ("x-amzn-requestid".into(), request_id.into()),
        ],
        body: body.into_bytes(),
    }
}

fn into_response(r: RawResponse) -> Response {
    let mut resp = Response::new(Body::from(r.body));
    *resp.status_mut() =
        StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (k, v) in r.headers {
        if let (Ok(k), Ok(v)) = (HeaderName::try_from(k), HeaderValue::try_from(v)) {
            resp.headers_mut().append(k, v);
        }
    }
    resp
}

/// Treat a 12-digit access key as an account selector for local multi-account use.
fn account_id_from_access_key(access_key: &str) -> Option<String> {
    (access_key.len() == 12 && access_key.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| access_key.to_owned())
}

fn into_request_response(r: RawResponse, req: &RawRequest) -> Response {
    finish_request_response(into_response(r), req)
}

fn finish_request_response(mut response: Response, req: &RawRequest) -> Response {
    let close_after_response = req.body.is_empty()
        && req
            .header("expect")
            .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"));
    if close_after_response {
        // Hyper can finish a zero-length expected body without sending 100 Continue.
        // Botocore leaves its early-response parser on a pooled connection, so close
        // this connection to make it reset that parser before the next request.
        response
            .headers_mut()
            .insert("connection", HeaderValue::from_static("close"));
    }
    response
}

/// Unsigned requests have no credential scope; moto's server-mode harness (and some SDK setups)
/// advertise the region as `region/<name>` in the user agent.
fn region_from_user_agent(req: &RawRequest) -> Option<String> {
    let ua = req.header("user-agent")?;
    let rest = ua.split("region/").nth(1)?;
    let region: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    (!region.is_empty()).then_some(region)
}

#[cfg(test)]
mod discovery_tests {
    use super::*;
    use serde_json::json;

    fn app() -> Arc<App> {
        let store = Arc::new(Store::ephemeral());
        let iam = roto_svc_iam::IamHandler::new(&store).unwrap();
        let sts = roto_svc_sts::StsHandler::new(&store, iam.0.clone()).unwrap();
        let sqs = roto_svc_sqs::SqsHandler::new(&store).unwrap();
        let lambda =
            roto_svc_lambda::LambdaHandler::unstarted(&store, Default::default(), sqs.0.clone())
                .unwrap();
        let handlers: Vec<Arc<dyn ServiceHandler>> = vec![
            Arc::new(sts),
            Arc::new(sqs),
            Arc::new(lambda),
            Arc::new(lua_service::test_service(
                store.clone(),
                "organizations",
                None,
            )),
        ];
        Arc::new(App {
            services: handlers.into_iter().map(|h| (h.service(), h)).collect(),
            store,
            default_account: "123456789012".into(),
            unsupported: Default::default(),
            trace: trace::Trace::create(None).unwrap(),
            gateway: Default::default(),
            websockets: Default::default(),
            api_gateway_v2: Default::default(),
            hooks: Default::default(),
            lua_api: None,
        })
    }

    async fn request(
        app: &Arc<App>,
        service: &str,
        path: &str,
        body: &str,
        content_type: &str,
    ) -> Response {
        let request = Request::builder().method("POST").uri(path)
            .header("authorization", format!("AWS4-HMAC-SHA256 Credential=test/20261010/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test"))
            .header("content-type", content_type)
            .body(Body::from(body.to_owned())).unwrap();
        handle(State(app.clone()), request).await
    }

    #[tokio::test]
    async fn lua_service_routes_signed_and_unsigned_requests_and_records_trace() {
        let app = app();
        assert!(app.store.existing_db("organizations").is_none());
        for (operation, signed) in [
            ("CreateOrganization", false),
            ("DescribeOrganization", true),
            ("CreateAccount", true),
        ] {
            let mut builder = Request::builder()
                .method("POST")
                .uri("/")
                .header(
                    "x-amz-target",
                    format!("AWSOrganizationsV20161128.{operation}"),
                )
                .header("content-type", "application/x-amz-json-1.1");
            if signed {
                builder = builder.header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261010/us-east-1/organizations/aws4_request, SignedHeaders=host, Signature=test");
            }
            let response =
                handle(State(app.clone()), builder.body(Body::from("{}")).unwrap()).await;
            assert_eq!(
                response.status().as_u16(),
                if operation == "CreateAccount" {
                    501
                } else {
                    200
                }
            );
        }
        let trace = app.trace.snapshot(None);
        assert_eq!(trace["entries"][0]["service"], "organizations");
        assert_eq!(trace["entries"][0]["operation"], "CreateOrganization");
        assert_eq!(trace["entries"][2]["outcome"], "unsupported");
        assert_eq!(reset(State(app.clone())).await.status(), StatusCode::OK);
        let handler = &app.services["organizations"];
        let response = handler
            .handle(
                &RequestContext {
                    account_id: app.account_id(),
                    region: "us-east-1".into(),
                    access_key: None,
                    request_id: "reset".into(),
                    base_url: "http://localhost".into(),
                },
                &RawRequest {
                    method: "POST".into(),
                    path: "/".into(),
                    headers: vec![(
                        "x-amz-target".into(),
                        "AWSOrganizationsV20161128.DescribeOrganization".into(),
                    )],
                    body: b"{}".to_vec(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(response.status, 400);
    }

    #[tokio::test]
    async fn lua_interception_precedes_dispatch_and_traces_stubbed_operations() {
        let app = app();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&app.store).unwrap());
        let lambda =
            Arc::new(roto_svc_lambda::Lambda::new(&app.store, Default::default()).unwrap());
        let s3 = Arc::new(roto_svc_s3::S3::new(&app.store).unwrap());
        let path = std::env::temp_dir().join(format!("roto-hooks-{}.luau", ids::request_id()));
        std::fs::write(
            &path,
            r#"
roto.hooks.request("sts", "DecodeAuthorizationMessage", function(request)
  assert(request.headers.authorization == nil)
  return {status=200, headers={["content-type"]="text/xml"}, body="stubbed"}
end)
roto.hooks.request("sts", "GetCallerIdentity", function() return nil end)
roto.hooks.request("iotdata", "Publish", function() return {status=429, body="throttled"} end)
"#,
        )
        .unwrap();
        let ctx = RequestContext {
            account_id: app.account_id(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "setup".into(),
            base_url: "http://localhost:5070".into(),
        };
        let services = app.services.clone();
        let script = path.clone();
        let hooks = tokio::task::spawn_blocking(move || {
            setup::run(&script, ctx, &[], sqs, lambda, s3, services)
        })
        .await
        .unwrap()
        .unwrap();
        *app.hooks.lock().unwrap() = hooks;
        let response = request(
            &app,
            "sts",
            "/",
            "Action=DecodeAuthorizationMessage",
            "application/x-www-form-urlencoded",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1000)
                .await
                .unwrap()
                .as_ref(),
            b"stubbed"
        );
        let response = request(
            &app,
            "sts",
            "/",
            "Action=GetCallerIdentity",
            "application/x-www-form-urlencoded",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = request(&app, "iotdata", "/topics/demo", "{}", "application/json").await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            app.unsupported.snapshot()["calls"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            app.trace.snapshot(None)["entries"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn records_unroutable_and_serialized_unsupported_calls_without_changing_responses() {
        let app = app();
        for _ in 0..2 {
            let response = request(
                &app,
                "unregistered",
                "/unregistered",
                "private-payload",
                "application/octet-stream",
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        let response = request(
            &app,
            "sts",
            "/",
            "Action=DecodeAuthorizationMessage&EncodedMessage=secret",
            "application/x-www-form-urlencoded",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("<Code>NotImplemented</Code>"));

        let request = Request::builder().uri("/2015-03-31/functions/demo/aliases/live")
            .header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261010/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=test")
            .body(Body::empty()).unwrap();
        assert_eq!(
            handle(State(app.clone()), request).await.status(),
            StatusCode::NOT_IMPLEMENTED
        );
        let snapshot = unsupported_calls(State(app.clone())).await.0;
        assert_eq!(snapshot["calls"].as_array().unwrap().len(), 3);
        let calls = snapshot["calls"].as_array().unwrap();
        let unroutable = calls
            .iter()
            .find(|c| c["service"] == "unregistered")
            .unwrap();
        assert_eq!(unroutable["count"], 2);
        assert_eq!(unroutable["operation"], serde_json::Value::Null);
        let lambda = calls.iter().find(|c| c["service"] == "lambda").unwrap();
        assert_eq!(lambda["operation"], "GetAlias");
        let sts = calls.iter().find(|c| c["service"] == "sts").unwrap();
        assert_eq!(sts["operation"], "DecodeAuthorizationMessage");
        assert!(!snapshot.to_string().contains("private-payload"));
        assert!(!snapshot.to_string().contains("secret"));
        assert_eq!(reset(State(app.clone())).await.status(), StatusCode::OK);
        assert_eq!(
            unsupported_calls(State(app)).await.0,
            json!({"calls": [], "dropped_calls": 0})
        );
    }

    #[tokio::test]
    async fn normal_service_errors_and_successes_are_not_discovery_entries() {
        let app = app();
        let response = request(
            &app,
            "iotdata",
            "/topics/devices%2F123",
            "private-payload",
            "application/octet-stream",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let trace = app.trace.snapshot(None);
        assert_eq!(trace["entries"][0]["service"], "iotdata");
        assert_eq!(trace["entries"][0]["operation"], "Publish");
        assert_eq!(trace["entries"][0]["outcome"], "success");
        let response = request(
            &app,
            "sts",
            "/",
            "Action=GetCallerIdentity",
            "application/x-www-form-urlencoded",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let req = Request::builder()
            .uri("/2015-03-31/functions/missing")
            .header("authorization", "AWS4-HMAC-SHA256 Credential=test/20261010/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=test")
            .body(Body::empty()).unwrap();
        let response = handle(State(app.clone()), req).await;
        assert!(response.status().is_client_error());
        assert_eq!(app.unsupported.snapshot()["calls"], json!([]));
    }
}
