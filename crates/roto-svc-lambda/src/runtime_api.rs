//! Warm Runtime API workers: custom-runtime binaries (`provided.al2023` `bootstrap`s) that poll
//! the Lambda Runtime API for work.
//!
//! A worker is one child process plus a private Runtime API listener on a loopback port, handed to
//! the process through `AWS_LAMBDA_RUNTIME_API`. Like a real execution environment it stays alive
//! between invocations (it lives in the pool while idle) and serves one invocation at a time;
//! overlapping invocations of one function use separate workers. A timed-out, cancelled or crashed
//! invocation kills its worker. A handler error reported through the API does not.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::executor::{Job, OUTPUT_LIMIT, Outcome};

const API: &str = "/2018-06-01/runtime/";
/// Captured stdout/stderr kept per worker between invocations; the oldest output is dropped.
const LOG_LIMIT: usize = 1024 * 1024;

struct Invocation {
    id: String,
    arn: String,
    deadline_ms: u64,
    body: Vec<u8>,
    client_context: Option<String>,
}

enum Reply {
    Response(Vec<u8>),
    Error(Vec<u8>),
    InitError(Vec<u8>),
}

/// The state the Runtime API connection tasks share.
struct Api {
    next: tokio::sync::Mutex<mpsc::Receiver<Invocation>>,
    replies: mpsc::Sender<(String, Reply)>,
}

pub(crate) struct Worker {
    child: tokio::process::Child,
    #[cfg(unix)]
    _group: crate::executor::ProcessGroup,
    invoke: mpsc::Sender<Invocation>,
    replies: mpsc::Receiver<(String, Reply)>,
    logs: Arc<Mutex<Vec<u8>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Aborting the server drops its JoinSet, which aborts every open connection with it.
        self.server.abort();
    }
}

impl Worker {
    /// Whether the process is still running (an idle worker may have crashed or exited).
    pub(crate) fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    fn take_logs(&self) -> String {
        let mut logs = self.logs.lock().unwrap();
        String::from_utf8_lossy(&std::mem::take(&mut *logs)).into_owned()
    }
}

fn capture(mut stream: impl AsyncRead + Unpin + Send + 'static, logs: Arc<Mutex<Vec<u8>>>) {
    tokio::spawn(async move {
        let mut buffer = [0; 8192];
        while let Ok(count) = stream.read(&mut buffer).await {
            if count == 0 {
                break;
            }
            let mut logs = logs.lock().unwrap();
            logs.extend_from_slice(&buffer[..count]);
            if logs.len() > LOG_LIMIT {
                let excess = logs.len() - LOG_LIMIT;
                logs.drain(..excess);
            }
        }
    });
}

pub(crate) async fn spawn(
    job: &Job,
    argv: &[String],
    cwd: &Option<PathBuf>,
    env: &BTreeMap<String, String>,
) -> Result<Worker, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("runtime API listener: {e}"))?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let (invoke, invocations) = mpsc::channel(1);
    let (reply_tx, replies) = mpsc::channel(4);
    let api = Arc::new(Api {
        next: tokio::sync::Mutex::new(invocations),
        replies: reply_tx,
    });
    let server = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            connections.spawn(connection(socket, api.clone()));
        }
    });

    let name = job.arn.rsplit(':').next().unwrap_or("").to_owned();
    let mut cmd = tokio::process::Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .envs(&job.environment)
        .envs(env)
        .env("AWS_REGION", &job.region)
        .env("AWS_DEFAULT_REGION", &job.region)
        .env("AWS_ENDPOINT_URL", &job.endpoint)
        .env("AWS_LAMBDA_FUNCTION_NAME", &name)
        .env("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        .env(
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
            job.memory_size.to_string(),
        )
        .env("AWS_LAMBDA_LOG_GROUP_NAME", format!("/aws/lambda/{name}"))
        .env("AWS_EXECUTION_ENV", "AWS_Lambda_provided.al2023")
        .env("_HANDLER", "external")
        .env("AWS_LAMBDA_RUNTIME_API", format!("127.0.0.1:{port}"))
        .env("ROTO_FUNCTION_ARN", &job.arn)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Real Lambda hands the function credentials; a local SDK client needs some to sign requests.
    for (key, value) in [
        ("AWS_ACCESS_KEY_ID", "roto"),
        ("AWS_SECRET_ACCESS_KEY", "roto"),
    ] {
        if !job.environment.contains_key(key)
            && !env.contains_key(key)
            && std::env::var_os(key).is_none()
        {
            cmd.env(key, value);
        }
    }
    #[cfg(unix)]
    cmd.process_group(0);
    if let Some(cwd) = cwd {
        cmd.env("LAMBDA_TASK_ROOT", cwd).current_dir(cwd);
    }
    let mut child = cmd.spawn().map_err(|e| {
        server.abort();
        format!("{}: {e}", argv[0])
    })?;
    #[cfg(unix)]
    let group = crate::executor::ProcessGroup(child.id().unwrap() as i32);
    let logs = Arc::new(Mutex::new(Vec::new()));
    capture(child.stdout.take().unwrap(), logs.clone());
    capture(child.stderr.take().unwrap(), logs.clone());
    Ok(Worker {
        child,
        #[cfg(unix)]
        _group: group,
        invoke,
        replies,
        logs,
        server,
    })
}

/// Runs one invocation on `worker`. On success the worker is handed back for reuse.
pub(crate) async fn invoke(mut worker: Worker, job: &Job) -> (Outcome, Option<Worker>) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    let invocation = Invocation {
        id: job.request_id.clone(),
        arn: job.arn.clone(),
        deadline_ms: now_ms + job.timeout * 1000,
        body: serde_json::to_vec(&job.event).unwrap(),
        client_context: job.client_context.clone(),
    };
    if worker.invoke.send(invocation).await.is_err() {
        return (
            Outcome::error("runtime worker is gone", worker.take_logs()),
            None,
        );
    }
    let reply = loop {
        tokio::select! {
            reply = worker.replies.recv() => match reply {
                // A reply for another request belongs to an earlier, abandoned invocation.
                Some((id, reply)) if id == job.request_id || id.is_empty() => break reply,
                Some(_) => continue,
                None => return (Outcome::error("runtime API stopped", worker.take_logs()), None),
            },
            status = worker.child.wait() => {
                // Output the process wrote just before exiting may still be in flight.
                tokio::task::yield_now().await;
                let message = match status {
                    Ok(status) => format!("Runtime exited with {status} before replying"),
                    Err(error) => format!("Runtime exited before replying: {error}"),
                };
                return (Outcome::error(message, worker.take_logs()), None);
            }
        }
    };
    let logs = worker.take_logs();
    match reply {
        Reply::Response(body) => match parse_body(&body) {
            Ok(payload) => (
                Outcome {
                    payload,
                    logs,
                    failed: false,
                },
                Some(worker),
            ),
            Err(error) => (
                Outcome::error(
                    format!("runtime response must be one JSON value: {error}"),
                    logs,
                ),
                Some(worker),
            ),
        },
        // A handler error: the environment survives it, as on Lambda.
        Reply::Error(body) => {
            let payload = parse_body(&body).unwrap_or_else(|_| {
                json!({"errorType": "Runtime.Error", "errorMessage": String::from_utf8_lossy(&body)})
            });
            (
                Outcome {
                    payload,
                    logs,
                    failed: true,
                },
                Some(worker),
            )
        }
        Reply::InitError(body) => {
            let payload = parse_body(&body).unwrap_or_else(|_| {
                json!({"errorType": "Runtime.InitError", "errorMessage": String::from_utf8_lossy(&body)})
            });
            (
                Outcome {
                    payload,
                    logs,
                    failed: true,
                },
                None,
            )
        }
    }
}

fn parse_body(body: &[u8]) -> Result<Value, String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body).map_err(|e| e.to_string())
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

async fn connection(socket: TcpStream, api: Arc<Api>) {
    let mut reader = BufReader::new(socket);
    while let Ok(Some(request)) = read_request(&mut reader).await {
        if handle(reader.get_mut(), &api, request).await.is_err() {
            break;
        }
    }
}

async fn handle(socket: &mut TcpStream, api: &Api, request: Request) -> std::io::Result<()> {
    let Some(route) = request.path.strip_prefix(API) else {
        return respond(socket, 404, &[], b"{}").await;
    };
    match (request.method.as_str(), route) {
        ("GET", "invocation/next") => {
            let Some(invocation) = api.next.lock().await.recv().await else {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            };
            let mut headers = vec![
                ("Content-Type", "application/json".to_owned()),
                ("Lambda-Runtime-Aws-Request-Id", invocation.id),
                (
                    "Lambda-Runtime-Deadline-Ms",
                    invocation.deadline_ms.to_string(),
                ),
                ("Lambda-Runtime-Invoked-Function-Arn", invocation.arn),
            ];
            if let Some(context) = invocation.client_context {
                headers.push(("Lambda-Runtime-Client-Context", context));
            }
            respond(socket, 200, &headers, &invocation.body).await
        }
        ("POST", "init/error") => {
            let _ = api
                .replies
                .send((String::new(), Reply::InitError(request.body)))
                .await;
            respond(socket, 202, &[], br#"{"status":"OK"}"#).await
        }
        ("POST", route) => {
            let rest = route.strip_prefix("invocation/");
            let reply = match rest.and_then(|r| r.split_once('/')) {
                Some((id, "response")) => Some((id, Reply::Response(request.body))),
                Some((id, "error")) => Some((id, Reply::Error(request.body))),
                _ => None,
            };
            match reply {
                Some((id, reply)) => {
                    let _ = api.replies.send((id.to_owned(), reply)).await;
                    respond(socket, 202, &[], br#"{"status":"OK"}"#).await
                }
                None => respond(socket, 404, &[], b"{}").await,
            }
        }
        _ => respond(socket, 404, &[], b"{}").await,
    }
}

async fn respond(
    socket: &mut TcpStream,
    status: u16,
    headers: &[(&str, String)],
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        _ => "Not Found",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body).await?;
    socket.flush().await
}

fn too_big() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "request exceeds 6 MiB")
}

/// Reads one HTTP/1.1 request (content-length or chunked body); `None` when the peer closed.
async fn read_request(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_owned();
    let path = parts.next().unwrap_or("").to_owned();
    let (mut length, mut chunked) = (0usize, false);
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        let header = line.trim().to_ascii_lowercase();
        if header.is_empty() {
            break;
        }
        if let Some(value) = header.strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        } else if let Some(value) = header.strip_prefix("transfer-encoding:") {
            chunked = value.contains("chunked");
        }
    }
    let mut body = Vec::new();
    if chunked {
        loop {
            line.clear();
            reader.read_line(&mut line).await?;
            let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
                .map_err(|_| std::io::ErrorKind::InvalidData)?;
            if size == 0 {
                // Trailers, then the blank line that ends the body.
                loop {
                    line.clear();
                    reader.read_line(&mut line).await?;
                    if line.trim().is_empty() {
                        break;
                    }
                }
                break;
            }
            if body.len() + size > OUTPUT_LIMIT {
                return Err(too_big());
            }
            let start = body.len();
            body.resize(start + size, 0);
            reader.read_exact(&mut body[start..]).await?;
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf).await?;
        }
    } else {
        if length > OUTPUT_LIMIT {
            return Err(too_big());
        }
        body.resize(length, 0);
        reader.read_exact(&mut body).await?;
    }
    Ok(Some(Request { method, path, body }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Executor, RiePool, execute};
    use std::time::Duration;

    // A minimal custom runtime: polls the Runtime API like `lambda_runtime` does and keeps a counter,
    // so a warm worker is visible as a count that goes on rising.
    const RUNTIME: &str = r#"
import json, os, sys, urllib.request as u
api = "http://" + os.environ["AWS_LAMBDA_RUNTIME_API"] + "/2018-06-01/runtime/invocation/"
n = 0
while True:
    r = u.urlopen(api + "next")
    rid = r.headers["Lambda-Runtime-Aws-Request-Id"]
    ev = json.load(r)
    n += 1
    print("handling", rid, flush=True)
    if ev.get("exit"):
        sys.exit(3)
    if ev.get("sleep"):
        import time; time.sleep(ev["sleep"])
    if ev.get("fail"):
        out = u.Request(api + rid + "/error", json.dumps({"errorType": "Boom", "errorMessage": "x"}).encode(), method="POST")
    else:
        body = {"count": n, "pid": os.getpid(), "echo": ev, "deadline": r.headers["Lambda-Runtime-Deadline-Ms"],
                "arn": r.headers["Lambda-Runtime-Invoked-Function-Arn"], "region": os.environ["AWS_REGION"]}
        out = u.Request(api + rid + "/response", json.dumps(body).encode(), method="POST")
    u.urlopen(out).read()
"#;

    fn job(event: Value) -> Job {
        Job {
            executor: Executor::Runtime {
                runtime: vec!["python3".into(), "-c".into(), RUNTIME.into()],
                cwd: None,
                env: BTreeMap::new(),
            },
            arn: "arn:aws:lambda:us-east-1:123456789012:function:rt-test".into(),
            request_id: roto_core::ids::request_id(),
            revision: "one".into(),
            region: "us-east-1".into(),
            endpoint: "http://localhost:5070".into(),
            environment: BTreeMap::new(),
            timeout: 10,
            memory_size: 128,
            event,
            client_context: None,
        }
    }

    #[tokio::test]
    async fn runtime_worker_is_warm_reports_errors_and_restarts_after_a_crash() {
        if std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: python3 not available");
            return;
        }
        let pool = RiePool::default();
        let first = execute(&job(json!({"a": 1})), &pool).await;
        assert!(!first.failed, "{} {}", first.payload, first.logs);
        assert_eq!(first.payload["count"], 1);
        assert_eq!(first.payload["echo"], json!({"a": 1}));
        assert_eq!(first.payload["region"], "us-east-1");
        assert!(
            first.payload["deadline"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert!(first.logs.contains("handling"), "{}", first.logs);

        // The same process serves the next invocation; logs hold only that invocation's output.
        let second = execute(&job(json!({})), &pool).await;
        assert_eq!(second.payload["count"], 2);
        assert_eq!(second.payload["pid"], first.payload["pid"]);
        assert_eq!(
            second.logs.matches("handling").count(),
            1,
            "{}",
            second.logs
        );

        // A handler error is a failed invocation, and the worker survives it.
        let failed = execute(&job(json!({"fail": true})), &pool).await;
        assert!(failed.failed);
        assert_eq!(failed.payload["errorType"], "Boom");
        assert_eq!(execute(&job(json!({})), &pool).await.payload["count"], 4);

        // A crash fails that invocation; the next one starts a fresh process.
        let crashed = execute(&job(json!({"exit": true})), &pool).await;
        assert!(
            crashed.failed
                && crashed.payload["errorMessage"]
                    .as_str()
                    .unwrap()
                    .contains("exited")
        );
        let fresh = execute(&job(json!({})), &pool).await;
        assert_eq!(fresh.payload["count"], 1);
        assert_ne!(fresh.payload["pid"], first.payload["pid"]);

        // A changed configuration revision does not reuse the old worker.
        let mut changed = job(json!({}));
        changed.revision = "two".into();
        assert_eq!(execute(&changed, &pool).await.payload["count"], 1);

        // A cancelled (timed out) invocation kills its worker instead of pooling it.
        let slow = job(json!({"sleep": 3}));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), execute(&slow, &pool))
                .await
                .is_err()
        );
        assert_eq!(execute(&job(json!({})), &pool).await.payload["count"], 1);
    }

    #[test]
    fn runtime_executor_parses_and_validates() {
        let ok: Executor =
            serde_json::from_value(json!({"runtime": ["./bootstrap"], "env": {"A": "b"}})).unwrap();
        ok.validate().unwrap();
        assert!(matches!(ok, Executor::Runtime { .. }));
        let empty: Executor = serde_json::from_value(json!({"runtime": []})).unwrap();
        assert!(empty.validate().is_err());
        assert!(
            serde_json::from_value::<Executor>(json!({"runtime": ["x"], "command": ["y"]}))
                .is_err()
        );
    }
}
