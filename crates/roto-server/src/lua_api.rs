//! `POST /roto-api/lua`: run a Lua script against the live server with the setup API.
//!
//! The endpoint is arbitrary code execution on the host, so it exists only with
//! `--enable-lua-api`, answers loopback peers only, and refuses browser requests (anything
//! carrying an `Origin` header), which closes cross-site and DNS-rebinding posts.
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use roto_core::{RequestContext, ids};
use serde_json::{Value, json};

use crate::{App, setup};

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_TIMEOUT_SECS: u64 = 600;

pub struct LuaApi {
    pub region: String,
    pub endpoint: String,
    pub vars: Vec<(String, String)>,
    pub sqs: Arc<roto_svc_sqs::Sqs>,
    pub lambda: Arc<roto_svc_lambda::Lambda>,
    pub s3: Arc<roto_svc_s3::S3>,
    /// Scripts run one at a time, like setup.
    pub running: Mutex<()>,
}

fn reply(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

/// Query: `name` labels the chunk in error messages (default `script.lua`); `timeout` is the
/// Lua instruction time limit in seconds (default 30, at most 600).
pub async fn run(
    State(app): State<Arc<App>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if app.lua_api.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !peer.ip().is_loopback() || headers.contains_key("origin") {
        return reply(
            StatusCode::FORBIDDEN,
            json!({"error": "the Lua API accepts only non-browser requests from loopback"}),
        );
    }
    let Ok(code) = String::from_utf8(body.to_vec()) else {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({"error": "script must be UTF-8"}),
        );
    };
    let name = params
        .get("name")
        .cloned()
        .unwrap_or_else(|| "script.lua".into());
    let limit = params
        .get("timeout")
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(DEFAULT_TIMEOUT_SECS)
        .clamp(1, MAX_TIMEOUT_SECS);
    let output = Arc::new(Mutex::new(Vec::<String>::new()));
    let task_output = output.clone();
    let task_app = app.clone();
    let result = tokio::task::spawn_blocking(move || {
        let api = task_app.lua_api.as_ref().expect("checked above");
        let _running = api.running.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::current_dir().map_err(|e| e.to_string())?;
        let ctx = RequestContext {
            account_id: task_app.account_id.clone(),
            region: api.region.clone(),
            access_key: None,
            request_id: ids::request_id(),
            base_url: api.endpoint.clone(),
        };
        let source = setup::Source {
            name: format!("={name}"),
            code,
            dir,
            posted: Some(setup::Posted {
                limit: Duration::from_secs(limit),
                output: task_output,
            }),
        };
        setup::run_source(
            source,
            ctx,
            &api.vars,
            api.sqs.clone(),
            api.lambda.clone(),
            api.s3.clone(),
            task_app.services.clone(),
        )
        .map_err(|e| e.to_string())
    })
    .await
    .unwrap_or_else(|e| Err(e.to_string()));
    let output = output.lock().unwrap().clone();
    match result {
        Ok((hooks, value)) => {
            app.hooks.lock().unwrap().extend(hooks);
            reply(StatusCode::OK, json!({"result": value, "output": output}))
        }
        Err(error) => reply(
            StatusCode::BAD_REQUEST,
            json!({"error": error, "output": output}),
        ),
    }
}
