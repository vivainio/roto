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
    Rie {
        rie: String,
    },
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
            Self::Rie { rie } => {
                if rie.trim().is_empty() || rie.contains('\0') {
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

pub(crate) async fn execute(job: &Job) -> Outcome {
    let result = tokio::time::timeout(Duration::from_secs(job.timeout), execute_inner(job)).await;
    match result {
        Ok(outcome) => outcome,
        Err(_) => Outcome::error(
            format!("Function timed out after {} seconds", job.timeout),
            String::new(),
        ),
    }
}

async fn execute_inner(job: &Job) -> Outcome {
    let payload = serde_json::to_vec(&job.event).unwrap();
    match &job.executor {
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
                stdin.write_all(&payload).await.map_err(|e| e.to_string())?;
                stdin.shutdown().await.map_err(|e| e.to_string())?;
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
        Executor::Rie { rie } => execute_rie(job, rie, payload).await,
    }
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
    env.insert("ROTO_INVOCATION_ID".into(), job.request_id.clone());
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
        job.request_id.replace('-', "")
    );
    let container = ContainerGuard {
        engine,
        id: container_name.clone(),
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

async fn container_logs(container: &ContainerGuard) -> String {
    let args = vec!["logs".into(), container.id.clone()];
    match process_output(container.engine.program(), &args).await {
        Ok((_, stdout, stderr)) => {
            let mut logs = String::from_utf8_lossy(&stdout).into_owned();
            logs.push_str(&String::from_utf8_lossy(&stderr));
            logs
        }
        Err(error) => format!("could not read container logs: {error}"),
    }
}

async fn execute_rie(job: &Job, image: &str, payload: Vec<u8>) -> Outcome {
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
    let port = match published_port(&container).await {
        Ok(port) => port,
        Err(error) => return Outcome::error(error, container_logs(&container).await),
    };
    let url = format!("http://127.0.0.1:{port}/2015-03-31/functions/function/invocations");
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(error) => return Outcome::error(error.to_string(), container_logs(&container).await),
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
            return Outcome::error(error.to_string(), container_logs(&container).await);
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
                    container_logs(&container).await,
                );
            }
            Ok(None) => break,
            Err(error) => {
                return Outcome::error(error.to_string(), container_logs(&container).await);
            }
        }
    }
    let logs = container_logs(&container).await;
    if !status.is_success() {
        return Outcome::error(
            format!("RIE returned {status}"),
            format!("{logs}{}", String::from_utf8_lossy(&body)),
        );
    }
    match serde_json::from_slice(&body) {
        Ok(payload) => Outcome {
            payload,
            logs,
            failed: failed_header,
        },
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
