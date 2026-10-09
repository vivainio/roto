use super::*;
use roto_core::store::StoreOptions;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

fn ctx() -> RequestContext {
    RequestContext {
        account_id: "123456789012".into(),
        region: "us-east-1".into(),
        access_key: None,
        request_id: "test-request".into(),
        base_url: "http://localhost:5070".into(),
    }
}
fn request(
    handler: &LambdaHandler,
    method: &str,
    path: &str,
    body: &[u8],
    headers: Vec<(String, String)>,
) -> RawResponse {
    handler
        .handle(
            &ctx(),
            &RawRequest {
                method: method.into(),
                path: path.into(),
                body: body.to_vec(),
                headers,
                ..Default::default()
            },
        )
        .unwrap()
}
fn create(handler: &LambdaHandler, name: &str, timeout: u64) -> RawResponse {
    request(handler,"POST","/2015-03-31/functions",json!({"FunctionName":name,"Role":"arn:aws:iam::123456789012:role/test","Runtime":"provided.al2023","Handler":"external","Code":{"ZipFile":""},"Timeout":timeout}).to_string().as_bytes(),vec![])
}
fn invoke(handler: &LambdaHandler, name: &str, body: &[u8], kind: &str) -> RawResponse {
    request(
        handler,
        "POST",
        &format!("/2015-03-31/functions/{name}/invocations"),
        body,
        vec![
            ("x-amz-invocation-type".into(), kind.into()),
            ("x-amz-log-type".into(), "Tail".into()),
        ],
    )
}
fn command(args: &[&str]) -> Executor {
    Executor::Command {
        command: args.iter().map(|s| s.to_string()).collect(),
        cwd: None,
        env: BTreeMap::new(),
    }
}
fn configured(name: &str, executor: Executor) -> LambdaHandler {
    LambdaHandler::new(
        &Store::ephemeral(),
        Executors {
            functions: BTreeMap::from([(name.into(), executor)]),
        },
    )
    .unwrap()
}
fn wait_for(lambda: &Lambda, state: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let history = lambda.history(&ctx()).unwrap();
        if history["invocations"][0]["state"] == state {
            return history;
        }
        assert!(Instant::now() < deadline, "{history}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn command_json_logs_failures_and_dry_run() {
    let handler = configured("echo", command(&["sh", "-c", "echo diagnostic >&2; cat"]));
    assert_eq!(create(&handler, "echo", 3).status, 201);
    let response = invoke(&handler, "echo", br#"{"hello":"world"}"#, "RequestResponse");
    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        json!({"hello":"world"})
    );
    let logs = response
        .headers
        .iter()
        .find(|(k, _)| k == "x-amz-log-result")
        .unwrap();
    assert_eq!(
        roto_protocol::base64::decode(&logs.1).unwrap(),
        b"diagnostic\n"
    );
    assert_eq!(invoke(&handler, "echo", b"invalid", "DryRun").status, 204);
    assert_eq!(
        handler.0.history(&ctx()).unwrap()["invocations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        invoke(&handler, "missing", b"{}", "RequestResponse").status,
        404
    );
    assert_eq!(
        invoke(&handler, "echo", b"invalid", "RequestResponse").status,
        400
    );

    let failure = configured("fail", command(&["sh", "-c", "echo broke >&2; exit 7"]));
    create(&failure, "fail", 3);
    let response = invoke(&failure, "fail", b"{}", "RequestResponse");
    assert_eq!(response.status, 200);
    assert!(
        response
            .headers
            .iter()
            .any(|(k, v)| k == "x-amz-function-error" && v == "Unhandled")
    );
    assert!(String::from_utf8_lossy(&response.body).contains("7"));
    assert_eq!(
        failure.0.history(&ctx()).unwrap()["invocations"][0]["state"],
        "failed"
    );
}

#[test]
fn command_drains_pipes_while_writing_large_input() {
    let handler = configured("large", command(&["sh", "-c", "cat"]));
    create(&handler, "large", 3);
    let payload = json!({"data":"x".repeat(256*1024)}).to_string();
    let response = invoke(&handler, "large", payload.as_bytes(), "RequestResponse");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::from_str::<Value>(&payload).unwrap()
    );
}

#[test]
fn timeout_kills_command_descendants() {
    let marker = std::env::temp_dir().join(format!("roto-child-{}", roto_core::ids::request_id()));
    let script = format!("(sleep 2; echo escaped > '{}') & wait", marker.display());
    let handler = configured("slow", command(&["sh", "-c", &script]));
    create(&handler, "slow", 1);
    let started = Instant::now();
    let response = invoke(&handler, "slow", b"{}", "RequestResponse");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(String::from_utf8_lossy(&response.body).contains("timed out"));
    std::thread::sleep(Duration::from_millis(1300));
    assert!(!marker.exists(), "descendant survived timeout");
}

#[test]
fn http_posts_event_and_metadata_and_records_error_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for index in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buffer = Vec::new();
            let mut part = [0; 1024];
            loop {
                let n = stream.read(&mut part).unwrap();
                assert!(n > 0);
                buffer.extend_from_slice(&part[..n]);
                if let Some(start) = buffer.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buffer[..start]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.parse().unwrap())
                        })
                        .unwrap();
                    if buffer.len() >= start + 4 + length {
                        assert!(headers.contains("POST /handler"));
                        assert!(headers.contains("x-roto-function-arn:"));
                        assert!(headers.contains("authorization: Bearer test"));
                        assert_eq!(
                            serde_json::from_slice::<Value>(&buffer[start + 4..]).unwrap(),
                            json!({"event":true})
                        );
                        break;
                    }
                }
            }
            let (status, body) = if index == 0 {
                ("200 OK", r#"{"received":true}"#)
            } else {
                ("503 Service Unavailable", "unavailable")
            };
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    let handler = configured(
        "http",
        Executor::Http {
            url: format!("http://{addr}/handler"),
            headers: BTreeMap::from([("Authorization".into(), "Bearer test".into())]),
        },
    );
    create(&handler, "http", 3);
    let success = invoke(&handler, "http", br#"{"event":true}"#, "RequestResponse");
    assert_eq!(
        serde_json::from_slice::<Value>(&success.body).unwrap(),
        json!({"received":true})
    );
    let error = invoke(&handler, "http", br#"{"event":true}"#, "RequestResponse");
    assert!(
        error
            .headers
            .iter()
            .any(|(k, _)| k == "x-amz-function-error")
    );
    assert!(String::from_utf8_lossy(&error.body).contains("503"));
    server.join().unwrap();
}

#[test]
fn async_jobs_retry_and_survive_restart() {
    let dir = std::env::temp_dir().join(format!(
        "roto-lambda-persistence-{}",
        roto_core::ids::request_id()
    ));
    let executors = Executors {
        functions: BTreeMap::from([("echo".into(), command(&["sh", "-c", "cat"]))]),
    };
    {
        let store = Store::open(&dir, StoreOptions::default()).unwrap();
        // Deliberately do not start a worker until the database is reopened.
        let lambda = Arc::new(Lambda::new(&store, executors.clone()).unwrap());
        let handler = LambdaHandler(lambda);
        create(&handler, "echo", 3);
        assert_eq!(
            invoke(&handler, "echo", br#"{"persisted":true}"#, "Event").status,
            202
        );
    }
    let store = Store::open(&dir, StoreOptions::default()).unwrap();
    let handler = LambdaHandler::new(&store, executors).unwrap();
    let history = wait_for(&handler.0, "succeeded");
    assert_eq!(
        history["invocations"][0]["result"],
        json!({"persisted":true})
    );
    drop(handler);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();

    let handler = configured("fail", command(&["sh", "-c", "exit 1"]));
    create(&handler, "fail", 3);
    assert_eq!(invoke(&handler, "fail", b"{}", "Event").status, 202);
    assert_eq!(
        wait_for(&handler.0, "failed")["invocations"][0]["attempts"],
        3
    );
}

#[test]
fn metadata_scope_and_executor_configuration_validation() {
    let handler = configured("echo", command(&["sh", "-c", "cat"]));
    create(&handler, "echo", 3);
    assert_eq!(create(&handler, "echo", 3).status, 409);
    assert_eq!(create(&handler, "bad", 0).status, 400);
    let mut other = ctx();
    other.account_id = "999999999999".into();
    let response = handler
        .handle(
            &other,
            &RawRequest {
                method: "GET".into(),
                path: "/2015-03-31/functions/echo/configuration".into(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(response.status, 404);
    assert!(
        serde_json::from_str::<Executors>(
            r#"{"functions":{"bad":{"command":["echo"],"url":"http://localhost"}}}"#
        )
        .is_err()
    );
    assert!(command(&[]).validate().is_err());
    assert!(
        Executor::Http {
            url: "file:///tmp/foo".into(),
            headers: BTreeMap::new()
        }
        .validate()
        .is_err()
    );
}
