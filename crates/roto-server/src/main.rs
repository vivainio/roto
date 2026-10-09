mod setup;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use clap::Parser;
use roto_core::sigv4::CredentialScope;
use roto_core::store::{Store, StoreOptions};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler, ids};

#[derive(Parser)]
#[command(version, about = "roto: an AWS simulator")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
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
    /// JSON bindings from Lambda function names/ARNs to local command or HTTP executors.
    #[arg(long)]
    lambda_executors: Option<PathBuf>,
    /// Trusted Lua resource setup, run before listening or processing events.
    #[arg(long)]
    setup: Option<PathBuf>,
    /// Region used by the Lua setup script.
    #[arg(long, default_value = "us-east-1")]
    setup_region: String,
}

struct App {
    services: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    #[allow(dead_code)] // handed to stateful services as they are added
    store: Arc<Store>,
    account_id: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

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
    let executors = args
        .lambda_executors
        .as_deref()
        .map(roto_svc_lambda::Executors::load)
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("error loading Lambda executors: {e}");
            std::process::exit(1);
        })
        .unwrap_or_default();
    let lambda = roto_svc_lambda::LambdaHandler::unstarted(&store, executors, sqs.0.clone())
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
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
    let setup_sqs = sqs.0.clone();
    let setup_lambda = lambda.0.clone();
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
        lambda.clone(),
    ];
    let app = Arc::new(App {
        services: handlers.into_iter().map(|h| (h.service(), h)).collect(),
        store,
        account_id: args.account_id,
    });

    let endpoint = format!(
        "http://{}:{}",
        if args.host == "0.0.0.0" {
            "127.0.0.1"
        } else {
            &args.host
        },
        args.port
    );
    if let Some(path) = args.setup {
        let ctx = RequestContext {
            account_id: app.account_id.clone(),
            region: args.setup_region,
            access_key: None,
            request_id: ids::request_id(),
            base_url: endpoint.clone(),
        };
        let services = app.services.clone();
        tokio::task::spawn_blocking(move || {
            setup::run(&path, ctx, setup_sqs, setup_lambda, services).map_err(|e| e.to_string())
        })
        .await
        .expect("setup task failed")
        .unwrap_or_else(|e| {
            eprintln!("error in Lua setup: {e}");
            std::process::exit(1);
        });
    }
    let router = Router::new()
        .route("/roto-api/health", get(|| async { "ok" }))
        .route("/roto-api/reset", post(reset))
        // moto's server-mode test harness resets state through this path.
        .route("/moto-api/reset", post(reset))
        .fallback(handle)
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
    lambda.start(endpoint);
    s3.start_notifications(lambda.0.clone());
    tracing::info!(
        "roto listening on http://{addr} ({})",
        if args.ephemeral {
            "ephemeral".into()
        } else {
            format!("data: {}", args.data_dir.display())
        }
    );
    axum::serve(listener, router).await.unwrap();
}

async fn reset(State(app): State<Arc<App>>) -> Response {
    let app2 = app.clone();
    let res =
        tokio::task::spawn_blocking(move || app2.services.values().try_for_each(|s| s.reset()))
            .await
            .unwrap();
    match res {
        Ok(()) => (StatusCode::OK, r#"{"status":"ok"}"#).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
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
    let account_id = access_key
        .as_deref()
        .and_then(|k| app.services.values().find_map(|svc| svc.resolve_account(k)))
        .unwrap_or_else(|| app.account_id.clone());
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
        let msg = format!(
            "roto cannot route this request to a service ({} {})",
            raw.method, raw.path
        );
        return into_response(plain_error(
            &AwsError::sender(400, "UnrecognizedClientException", msg),
            &request_id,
        ));
    };
    let result = tokio::task::spawn_blocking(move || handler.handle(&ctx, &raw)).await;
    match result {
        Ok(Ok(resp)) => into_response(resp),
        Ok(Err(e)) => into_response(plain_error(&e, &request_id)),
        Err(_) => into_response(plain_error(
            &AwsError::internal("handler panicked"),
            &request_id,
        )),
    }
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
            resp.headers_mut().insert(k, v);
        }
    }
    resp
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
