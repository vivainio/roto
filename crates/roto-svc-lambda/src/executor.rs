use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
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
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Executors {
    pub functions: BTreeMap<String, Executor>,
}

impl Executors {
    pub fn load(path: &Path) -> Result<Self, String> {
        let source = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let mut config: Self = serde_json::from_str(&source).map_err(|e| e.to_string())?;
        let parent = std::fs::canonicalize(path)
            .map_err(|e| e.to_string())?
            .parent()
            .unwrap()
            .to_path_buf();
        for executor in config.functions.values_mut() {
            executor.validate()?;
            if let Executor::Command { cwd, .. } = executor {
                *cwd = Some(match cwd.take() {
                    Some(p) => parent.join(p),
                    None => parent.clone(),
                });
            }
        }
        Ok(config)
    }
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
    pub event: Value,
    pub client_context: Option<String>,
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
