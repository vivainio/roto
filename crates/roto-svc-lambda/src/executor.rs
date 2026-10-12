use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

const OUTPUT_LIMIT: usize = 6 * 1024 * 1024;

/// Exactly one executor variant. Commands are argv arrays, never implicitly shell-expanded.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Executor {
    Command {
        command: Vec<String>,
        #[serde(default)]
        cwd: Option<PathBuf>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    Inline {
        inline: String,
    },
    Container {
        container: String,
        #[serde(default)]
        warm: bool,
    },
}

type InlineHandler = dyn Fn(Value, Value) -> Result<Value, String> + Send + Sync;
static INLINE: std::sync::OnceLock<
    std::sync::Mutex<BTreeMap<String, std::sync::Arc<InlineHandler>>>,
> = std::sync::OnceLock::new();

/// Registers a process-local handler. Its binding cannot be restored without setup.
pub fn inline_executor(
    handler: impl Fn(Value, Value) -> Result<Value, String> + Send + Sync + 'static,
) -> Executor {
    let id = roto_core::ids::request_id();
    INLINE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .insert(id.clone(), std::sync::Arc::new(handler));
    Executor::Inline { inline: id }
}

#[derive(Clone, Debug, Default)]
pub struct Executors {
    pub functions: BTreeMap<String, Executor>,
}

impl Executor {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Command { command, env, .. } => {
                if command.is_empty()
                    || command[0].is_empty()
                    || command.iter().any(|s| s.contains('\0'))
                {
                    return Err("command must be a nonempty argv array without NULs".into());
                }
                if env
                    .iter()
                    .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
                {
                    return Err("invalid command environment".into());
                }
            }
            Self::Http { url, headers } => {
                let url = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err("executor URL must use http or https".into());
                }
                for (k, v) in headers {
                    reqwest::header::HeaderName::from_bytes(k.as_bytes())
                        .map_err(|e| e.to_string())?;
                    reqwest::header::HeaderValue::from_str(v).map_err(|e| e.to_string())?;
                }
            }
            Self::Inline { .. } => {}
            Self::Container { container, .. } => {
                if container.trim().is_empty() || container.contains('\0') {
                    return Err("RIE executor requires a nonempty container image name".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Job {
    pub executor: Executor,
    pub arn: String,
    pub request_id: String,
    #[serde(default)]
    pub revision: String,
    pub region: String,
    pub endpoint: String,
    pub environment: BTreeMap<String, String>,
    pub timeout: u64,
    #[serde(default = "default_memory_size")]
    pub memory_size: u64,
    pub event: Value,
    pub client_context: Option<String>,
}

fn default_memory_size() -> u64 {
    128
}

pub(crate) struct Outcome {
    pub payload: Value,
    pub logs: String,
    pub failed: bool,
}

impl Outcome {
    fn error(message: impl Into<String>, logs: String) -> Self {
        Self {
            payload: json!({"errorType": "ExecutorError", "errorMessage": message.into()}),
            logs,
            failed: true,
        }
    }
}

async fn bounded_read(mut stream: impl AsyncRead + Unpin) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = stream.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if count == 0 {
            return Ok(output);
        }
        if output.len() + count > OUTPUT_LIMIT {
            return Err("executor output exceeds 6 MiB".into());
        }
        output.extend_from_slice(&buffer[..count]);
    }
}

pub(crate) async fn execute(job: &Job, pool: &RiePool) -> Outcome {
    let result =
        tokio::time::timeout(Duration::from_secs(job.timeout), execute_inner(job, pool)).await;
    match result {
        Ok(outcome) => outcome,
        Err(_) => Outcome::error(
            format!("Function timed out after {} seconds", job.timeout),
            String::new(),
        ),
    }
}

async fn execute_inner(job: &Job, pool: &RiePool) -> Outcome {
    let payload = serde_json::to_vec(&job.event).unwrap();
    match &job.executor {
        Executor::Inline { inline } => {
            let handler = INLINE
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .get(inline)
                .cloned();
            let Some(handler) = handler else {
                return Outcome::error(
                    "Inline handler unavailable; rerun Lua setup",
                    String::new(),
                );
            };
            let event = job.event.clone();
            let context = json!({"function_arn": job.arn, "request_id": job.request_id, "region": job.region, "environment": job.environment, "timeout": job.timeout});
            match tokio::task::spawn_blocking(move || handler(event, context)).await {
                Ok(Ok(payload))
                    if serde_json::to_vec(&payload).is_ok_and(|p| p.len() <= OUTPUT_LIMIT) =>
                {
                    Outcome {
                        payload,
                        logs: String::new(),
                        failed: false,
                    }
                }
                Ok(Ok(_)) => Outcome::error("executor output exceeds 6 MiB", String::new()),
                Ok(Err(error)) => Outcome::error(error, String::new()),
                Err(error) => Outcome::error(error.to_string(), String::new()),
            }
        }
        Executor::Command { command, cwd, env } => {
            let mut cmd = tokio::process::Command::new(&command[0]);
            cmd.args(&command[1..])
                .envs(&job.environment)
                .envs(env)
                .env("AWS_REGION", &job.region)
                .env("AWS_DEFAULT_REGION", &job.region)
                .env("AWS_ENDPOINT_URL", &job.endpoint)
                .env(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    job.arn.rsplit(':').next().unwrap_or(""),
                )
                .env("ROTO_INVOCATION_ID", &job.request_id)
                .env("ROTO_FUNCTION_ARN", &job.arn)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            // A process group lets timeout/error cleanup also kill spawned descendants.
            #[cfg(unix)]
            cmd.process_group(0);
            if let Some(context) = &job.client_context {
                cmd.env("ROTO_CLIENT_CONTEXT", context);
            } else {
                cmd.env_remove("ROTO_CLIENT_CONTEXT");
            }
            if let Some(cwd) = cwd {
                cmd.current_dir(cwd);
            }
            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => return Outcome::error(e.to_string(), String::new()),
            };
            #[cfg(unix)]
            let _group = ProcessGroup(child.id().unwrap() as i32);
            let mut stdin = child.stdin.take().unwrap();
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();
            let io = async {
                // A command may exit without reading stdin; its exit status reports that.
                let ignore_closed = |r: std::io::Result<()>| match r {
                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
                    other => other.map_err(|e| e.to_string()),
                };
                ignore_closed(stdin.write_all(&payload).await)?;
                ignore_closed(stdin.shutdown().await)?;
                drop(stdin);
                Ok::<_, String>(())
            };
            let result = tokio::try_join!(io, bounded_read(stdout), bounded_read(stderr), async {
                child.wait().await.map_err(|e| e.to_string())
            });
            match result {
                Ok(((), output, logs, status)) => {
                    let logs = String::from_utf8_lossy(&logs).into_owned();
                    if !status.success() {
                        return Outcome::error(format!("Command exited with {status}"), logs);
                    }
                    match serde_json::from_slice(&output) {
                        Ok(payload) => Outcome {
                            payload,
                            logs,
                            failed: false,
                        },
                        Err(e) => {
                            Outcome::error(format!("stdout must contain one JSON value: {e}"), logs)
                        }
                    }
                }
                Err(e) => Outcome::error(e, String::new()),
            }
        }
        Executor::Http { url, headers } => {
            let client = match reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
            {
                Ok(c) => c,
                Err(e) => return Outcome::error(e.to_string(), String::new()),
            };
            let mut request = client.post(url).header("content-type", "application/json");
            for (k, v) in headers {
                request = request.header(k, v);
            }
            request = request
                .header("x-roto-invocation-id", &job.request_id)
                .header("x-roto-function-arn", &job.arn)
                .header("x-roto-region", &job.region);
            if let Some(context) = &job.client_context {
                request = request.header("x-roto-client-context", context);
            }
            let mut response = match request.body(payload).send().await {
                Ok(r) => r,
                Err(e) => return Outcome::error(e.to_string(), String::new()),
            };
            let status = response.status();
            let mut body = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) if body.len() + chunk.len() <= OUTPUT_LIMIT => {
                        body.extend_from_slice(&chunk)
                    }
                    Ok(Some(_)) => {
                        return Outcome::error("HTTP response exceeds 6 MiB", String::new());
                    }
                    Ok(None) => break,
                    Err(e) => return Outcome::error(e.to_string(), String::new()),
                }
            }
            if !status.is_success() {
                return Outcome::error(
                    format!("HTTP executor returned {status}"),
                    String::from_utf8_lossy(&body).into_owned(),
                );
            }
            // A worker that sends no body (e.g. 204) returned nothing, like a handler
            // returning None; that is a null result rather than malformed JSON.
            if body.iter().all(u8::is_ascii_whitespace) {
                return Outcome {
                    payload: Value::Null,
                    logs: String::new(),
                    failed: false,
                };
            }
            match serde_json::from_slice(&body) {
                Ok(payload) => Outcome {
                    payload,
                    logs: String::new(),
                    failed: false,
                },
                Err(e) => Outcome::error(
                    format!("HTTP response must contain one JSON value: {e}"),
                    String::new(),
                ),
            }
        }
        Executor::Container { container, warm } => {
            execute_rie(job, container, *warm, payload, pool).await
        }
    }
}

// Only idle containers live in the pool. An invocation owns its container, so
// cancellation (including timeout) drops and removes it rather than returning it.
#[derive(Default)]
pub(crate) struct RiePool(std::sync::Mutex<RiePoolState>);
#[derive(Default)]
struct RiePoolState {
    idle: Vec<IdleContainer>,
    closed: bool,
}
struct IdleContainer {
    arn: String,
    key: String,
    used: std::time::Instant,
    container: ContainerGuard,
}
impl RiePool {
    fn take(&self, arn: &str, key: &str) -> Option<ContainerGuard> {
        let mut state = self.0.lock().unwrap();
        let idle = &mut state.idle;
        idle.retain(|entry| {
            entry.used.elapsed() < Duration::from_secs(300)
                && (entry.arn != arn || entry.key == key)
        });
        let index = idle
            .iter()
            .position(|entry| entry.arn == arn && entry.key == key)?;
        Some(idle.remove(index).container)
    }
    fn put(&self, arn: &str, key: String, container: ContainerGuard) {
        let mut state = self.0.lock().unwrap();
        if state.closed {
            return;
        }
        let idle = &mut state.idle;
        if idle.len() >= 32 {
            idle.remove(0);
        }
        idle.push(IdleContainer {
            arn: arn.into(),
            key,
            used: std::time::Instant::now(),
            container,
        });
    }
    pub(crate) fn close(&self) {
        let mut state = self.0.lock().unwrap();
        state.closed = true;
        state.idle.clear();
    }
    pub(crate) fn expire(&self) {
        self.0
            .lock()
            .unwrap()
            .idle
            .retain(|entry| entry.used.elapsed() < Duration::from_secs(300));
    }
    pub(crate) fn clear(&self, arn: Option<&str>) {
        self.0
            .lock()
            .unwrap()
            .idle
            .retain(|entry| arn.is_some_and(|arn| entry.arn != arn));
    }
}
fn warm_key(job: &Job, image: &str) -> String {
    serde_json::to_string(&(
        image,
        &job.revision,
        &job.region,
        &job.endpoint,
        &job.environment,
        job.timeout,
        job.memory_size,
        &job.client_context,
    ))
    .unwrap()
}

#[derive(Clone, Copy)]
enum ContainerEngine {
    Podman,
    Docker,
}

impl ContainerEngine {
    fn program(self) -> &'static str {
        match self {
            Self::Podman => "podman",
            Self::Docker => "docker",
        }
    }

    fn host_name(self) -> &'static str {
        match self {
            Self::Podman => "host.containers.internal",
            Self::Docker => "host.docker.internal",
        }
    }
}

async fn process_output(
    program: &str,
    args: &[String],
) -> Result<(ExitStatus, Vec<u8>, Vec<u8>), String> {
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|e| format!("{program}: {e}"))?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let wait = async { child.wait().await.map_err(|e| e.to_string()) };
    let (stdout, stderr, status) =
        tokio::try_join!(bounded_read(stdout), bounded_read(stderr), wait)?;
    Ok((status, stdout, stderr))
}

async fn engine_candidates() -> Vec<ContainerEngine> {
    let mut engines = Vec::new();
    for engine in [ContainerEngine::Podman, ContainerEngine::Docker] {
        let args = vec!["--version".to_owned()];
        if process_output(engine.program(), &args)
            .await
            .is_ok_and(|(status, _, _)| status.success())
        {
            engines.push(engine);
        }
    }
    engines
}

fn container_environment(job: &Job, engine: ContainerEngine) -> BTreeMap<String, String> {
    let mut env = job.environment.clone();
    env.insert("AWS_REGION".into(), job.region.clone());
    env.insert("AWS_DEFAULT_REGION".into(), job.region.clone());
    env.insert(
        "AWS_LAMBDA_FUNCTION_NAME".into(),
        job.arn.rsplit(':').next().unwrap_or("").to_owned(),
    );
    env.insert(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE".into(),
        job.memory_size.to_string(),
    );
    env.insert(
        "AWS_LAMBDA_FUNCTION_TIMEOUT".into(),
        job.timeout.to_string(),
    );
    env.insert(
        "AWS_ENDPOINT_URL".into(),
        container_endpoint(&job.endpoint, engine),
    );
    // A warm process cannot receive changing invocation metadata through env.
    if !matches!(job.executor, Executor::Container { warm: true, .. }) {
        env.insert("ROTO_INVOCATION_ID".into(), job.request_id.clone());
    }
    env.insert("ROTO_FUNCTION_ARN".into(), job.arn.clone());
    if let Some(context) = &job.client_context {
        env.insert("ROTO_CLIENT_CONTEXT".into(), context.clone());
    }
    env
}

fn container_endpoint(endpoint: &str, engine: ContainerEngine) -> String {
    let Ok(mut url) = reqwest::Url::parse(endpoint) else {
        return endpoint.to_owned();
    };
    if url
        .host_str()
        .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1" | "0.0.0.0"))
    {
        let _ = url.set_host(Some(engine.host_name()));
    }
    url.to_string()
}

struct ContainerGuard {
    engine: ContainerEngine,
    id: String,
    log_offsets: (usize, usize),
    port: Option<u16>,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let mut command = std::process::Command::new(self.engine.program());
        command
            .args(["rm", "--force", &self.id])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let _ = command.spawn();
    }
}

async fn start_container(
    engine: ContainerEngine,
    image: &str,
    job: &Job,
) -> Result<ContainerGuard, String> {
    let engine_name = match engine {
        ContainerEngine::Podman => "podman",
        ContainerEngine::Docker => "docker",
    };
    let container_name = format!(
        "roto-lambda-{engine_name}-{}",
        roto_core::ids::request_id().replace('-', "")
    );
    let container = ContainerGuard {
        engine,
        id: container_name.clone(),
        log_offsets: (0, 0),
        port: None,
    };
    let mut args = vec![
        "run".into(),
        "--detach".into(),
        "--pull=never".into(),
        "--name".into(),
        container_name,
        "--publish".into(),
        "127.0.0.1::8080".into(),
    ];
    if matches!(engine, ContainerEngine::Docker) {
        args.extend([
            "--add-host".into(),
            "host.docker.internal:host-gateway".into(),
        ]);
    }
    for (key, value) in container_environment(job, engine) {
        args.push("--env".into());
        args.push(format!("{key}={value}"));
    }
    args.push(image.to_owned());
    let (status, _stdout, stderr) = process_output(engine.program(), &args).await?;
    if !status.success() {
        return Err(format!(
            "{} could not start image {image}: {}",
            engine.program(),
            String::from_utf8_lossy(&stderr).trim()
        ));
    }
    Ok(container)
}

async fn published_port(container: &ContainerGuard) -> Result<u16, String> {
    let args = vec!["port".into(), container.id.clone(), "8080/tcp".into()];
    let (status, stdout, stderr) = process_output(container.engine.program(), &args).await?;
    if !status.success() {
        return Err(format!(
            "{} could not inspect container port: {}",
            container.engine.program(),
            String::from_utf8_lossy(&stderr).trim()
        ));
    }
    let mapping = String::from_utf8_lossy(&stdout);
    mapping
        .lines()
        .find_map(|line| line.trim().rsplit(':').next()?.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| {
            format!(
                "{} returned an invalid port mapping: {mapping}",
                container.engine.program()
            )
        })
}

// Drain old output without keeping it in memory or counting it toward the
// invocation output limit. Separate offsets preserve stdout/stderr ordering.
async fn new_logs(
    mut stream: impl AsyncRead + Unpin,
    skip: usize,
) -> Result<(Vec<u8>, usize), String> {
    let mut output = Vec::new();
    let mut total = 0;
    let mut buffer = [0; 8192];
    loop {
        let count = stream.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if count == 0 {
            return Ok((output, total));
        }
        let start = skip.saturating_sub(total).min(count);
        total += count;
        if output.len() + count - start > OUTPUT_LIMIT {
            return Err("executor output exceeds 6 MiB".into());
        }
        output.extend_from_slice(&buffer[start..count]);
    }
}
async fn container_logs(container: &mut ContainerGuard) -> String {
    let result = async {
        let mut child = tokio::process::Command::new(container.engine.program())
            .args(["logs", &container.id])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| e.to_string())?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (out, err, _) = tokio::try_join!(
            new_logs(stdout, container.log_offsets.0),
            new_logs(stderr, container.log_offsets.1),
            async { child.wait().await.map_err(|e| e.to_string()) }
        )?;
        container.log_offsets = (out.1, err.1);
        let mut logs = String::from_utf8_lossy(&out.0).into_owned();
        logs.push_str(&String::from_utf8_lossy(&err.0));
        Ok::<_, String>(logs)
    }
    .await;
    result.unwrap_or_else(|error| format!("could not read container logs: {error}"))
}

async fn execute_rie(
    job: &Job,
    image: &str,
    warm: bool,
    payload: Vec<u8>,
    pool: &RiePool,
) -> Outcome {
    let key = warm_key(job, image);
    let cached = if warm {
        pool.take(&job.arn, &key)
    } else {
        None
    };
    let container = if let Some(container) = cached {
        container
    } else {
        let engines = engine_candidates().await;
        if engines.is_empty() {
            return Outcome::error(
                "RIE executor requires podman or docker on PATH",
                String::new(),
            );
        }
        let mut started = None;
        let mut errors = Vec::new();
        for engine in engines {
            match start_container(engine, image, job).await {
                Ok(container) => {
                    started = Some(container);
                    break;
                }
                Err(error) => errors.push(error),
            }
        }
        let Some(container) = started else {
            return Outcome::error(errors.join("; "), String::new());
        };
        container
    };
    let mut container = container;
    let port = if let Some(port) = container.port {
        port
    } else {
        match published_port(&container).await {
            Ok(port) => {
                container.port = Some(port);
                port
            }
            Err(error) => return Outcome::error(error, container_logs(&mut container).await),
        }
    };
    let url = format!("http://127.0.0.1:{port}/2015-03-31/functions/function/invocations");
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return Outcome::error(error.to_string(), container_logs(&mut container).await);
        }
    };
    let response = loop {
        match client
            .post(&url)
            .header("content-type", "application/json")
            .body(payload.clone())
            .send()
            .await
        {
            Ok(response) => break Ok(response),
            Err(error) if error.is_connect() => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(error) => break Err(error),
        }
    };
    let mut response = match response {
        Ok(response) => response,
        Err(error) => {
            return Outcome::error(error.to_string(), container_logs(&mut container).await);
        }
    };
    let failed_header = response
        .headers()
        .get("x-amz-function-error")
        .is_some_and(|value| !value.as_bytes().is_empty());
    let status = response.status();
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len() + chunk.len() <= OUTPUT_LIMIT => {
                body.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => {
                return Outcome::error(
                    "RIE response exceeds 6 MiB",
                    container_logs(&mut container).await,
                );
            }
            Ok(None) => break,
            Err(error) => {
                return Outcome::error(error.to_string(), container_logs(&mut container).await);
            }
        }
    }
    let logs = container_logs(&mut container).await;
    if !status.is_success() {
        return Outcome::error(
            format!("RIE returned {status}"),
            format!("{logs}{}", String::from_utf8_lossy(&body)),
        );
    }
    match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => {
            // Some RIE runtimes return the standard error envelope without an
            // error header (and with HTTP 200).
            let runtime_error = payload["errorType"].is_string()
                && payload["errorMessage"].is_string()
                && payload["requestId"].is_string()
                && payload["stackTrace"].is_array();
            if warm {
                pool.put(&job.arn, key, container);
            }
            Outcome {
                payload,
                logs,
                failed: failed_header || runtime_error,
            }
        }
        Err(error) => Outcome::error(
            format!("RIE response must contain one JSON value: {error}"),
            logs,
        ),
    }
}

#[cfg(unix)]
struct ProcessGroup(i32);
#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // The child was spawned in its own group; never targets roto's process group.
        unsafe {
            libc::kill(-self.0, libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod rie_tests {
    use super::*;

    #[test]
    fn warm_binding_is_opt_in_and_validated() {
        for (input, expected) in [
            (json!({"container": "worker:latest"}), false),
            (json!({"container": "worker:latest", "warm": true}), true),
            (json!({"container": "worker:latest", "warm": false}), false),
        ] {
            let executor: Executor = serde_json::from_value(input).unwrap();
            executor.validate().unwrap();
            assert!(matches!(executor, Executor::Container { warm, .. } if warm == expected));
        }
        assert!(
            serde_json::from_value::<Executor>(json!({"container": "worker", "warm": "yes"}))
                .is_err()
        );
        assert!(serde_json::from_value::<Executor>(json!({"rie": "worker:latest"})).is_err());
        assert!(
            serde_json::from_value::<Executor>(
                json!({"url": "http://localhost:8080", "warm": true})
            )
            .is_err()
        );
    }

    // Build demos/lambda-warm/Containerfile as roto-lambda-warm:latest first.
    #[tokio::test]
    #[ignore = "requires a running container engine and the lambda-warm demo image"]
    async fn rie_reuses_state_separates_logs_and_discards_timed_out_containers() {
        let pool = RiePool::default();
        let mut job = Job {
            executor: Executor::Container {
                container: "roto-lambda-warm:latest".into(),
                warm: true,
            },
            arn: format!(
                "arn:aws:lambda:us-east-1:123456789012:function:test-{}",
                roto_core::ids::request_id()
            ),
            request_id: roto_core::ids::request_id(),
            revision: "one".into(),
            region: "us-east-1".into(),
            endpoint: "http://localhost:5070".into(),
            environment: BTreeMap::new(),
            timeout: 30,
            memory_size: 128,
            event: json!({"label": "first"}),
            client_context: None,
        };
        let first = execute(&job, &pool).await;
        assert!(!first.failed, "{} {}", first.payload, first.logs);
        assert_eq!(first.payload["count"], 1);
        assert!(first.logs.contains("warm-demo: first"), "{}", first.logs);
        let id = pool.0.lock().unwrap().idle[0].container.id.clone();
        job.request_id = roto_core::ids::request_id();
        job.event = json!({"label": "second"});
        let second = execute(&job, &pool).await;
        assert!(!second.failed, "{} {}", second.payload, second.logs);
        assert_eq!(second.payload["count"], 2);
        assert_ne!(first.payload["requestId"], second.payload["requestId"]);
        assert!(second.logs.contains("warm-demo: second"));
        assert!(!second.logs.contains("warm-demo: first"));
        assert_eq!(pool.0.lock().unwrap().idle[0].container.id, id);
        job.event = json!({"fail": true});
        assert!(execute(&job, &pool).await.failed);
        job.event = json!({});
        assert_eq!(execute(&job, &pool).await.payload["count"], 4);
        job.revision = "two".into();
        assert_eq!(execute(&job, &pool).await.payload["count"], 1);
        // Cancel the invocation while it owns a warm container.
        job.event = json!({"sleep": 2});
        assert!(
            tokio::time::timeout(Duration::from_millis(500), execute(&job, &pool))
                .await
                .is_err()
        );
        assert!(pool.0.lock().unwrap().idle.is_empty());
        job.event = json!({});
        assert_eq!(execute(&job, &pool).await.payload["count"], 1);
        // Overlapping calls must not share the same runtime.
        job.event = json!({"sleep": 0.5});
        let mut other = job.clone();
        other.request_id = roto_core::ids::request_id();
        let (left, right) = tokio::join!(execute(&job, &pool), execute(&other, &pool));
        assert!(!left.failed && !right.failed);
        assert_ne!(left.payload["count"], right.payload["count"]);
        assert_eq!(pool.0.lock().unwrap().idle.len(), 2);
        for entry in pool.0.lock().unwrap().idle.iter_mut() {
            entry.used = std::time::Instant::now() - Duration::from_secs(301);
        }
        pool.expire();
        assert!(pool.0.lock().unwrap().idle.is_empty());
        job.event = json!({});
        job.executor = Executor::Container {
            container: "roto-lambda-warm:latest".into(),
            warm: false,
        };
        assert_eq!(execute(&job, &pool).await.payload["count"], 1);
        assert_eq!(execute(&job, &pool).await.payload["count"], 1);
        assert!(pool.0.lock().unwrap().idle.is_empty());
    }
}
