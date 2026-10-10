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
        serde_json::from_str::<Executor>(r#"{"command":["echo"],"url":"http://localhost"}"#)
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

fn sqs_api(sqs: &roto_svc_sqs::Sqs, op: &str, input: Value) -> Value {
    let r = roto_svc_sqs::dispatch(sqs, &ctx(), op, &input).unwrap();
    serde_json::from_slice(&r.body).unwrap()
}
fn lambda_api(lambda: &Lambda, op: &str, input: Value) -> Value {
    let r = dispatch(lambda, &ctx(), op, &input).unwrap();
    serde_json::from_slice(&r.body).unwrap()
}
fn queue(sqs: &roto_svc_sqs::Sqs, name: &str) -> String {
    sqs_api(
        sqs,
        "CreateQueue",
        json!({"QueueName":name,"Attributes":{"VisibilityTimeout":"1"}}),
    )["QueueUrl"]
        .as_str()
        .unwrap()
        .into()
}
fn mapping_for(lambda: &Lambda, name: &str, partial: bool) -> String {
    lambda_api(lambda,"CreateEventSourceMapping",json!({"FunctionName":"consume","EventSourceArn":format!("arn:aws:sqs:us-east-1:123456789012:{name}"),"FunctionResponseTypes":if partial {vec!["ReportBatchItemFailures"]} else {vec![]}}))["UUID"].as_str().unwrap().into()
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn sqs_lambda(store: &Store, executor: Executor) -> (Arc<roto_svc_sqs::Sqs>, LambdaHandler) {
    let sqs = Arc::new(roto_svc_sqs::Sqs::new(store).unwrap());
    let handler = LambdaHandler::unstarted(
        store,
        Executors {
            functions: BTreeMap::from([("consume".into(), executor)]),
        },
        sqs.clone(),
    )
    .unwrap();
    assert_eq!(create(&handler, "consume", 1).status, 201);
    (sqs, handler)
}

#[test]
fn sqs_success_event_shape_disabled_mapping_and_scoping() {
    let (sqs, handler) = sqs_lambda(&Store::ephemeral(), command(&["sh", "-c", "cat"]));
    let url = queue(&sqs, "jobs");
    let id = mapping_for(&handler.0, "jobs", false);
    lambda_api(
        &handler.0,
        "UpdateEventSourceMapping",
        json!({"UUID":id,"Enabled":false}),
    );
    sqs_api(
        &sqs,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"hello","MessageAttributes":{"label":{"DataType":"String","StringValue":"test"},"binary":{"DataType":"Binary","BinaryValue":"aGk="}}}),
    );
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    assert_eq!(handler.0.history(&ctx()).unwrap()["invocations"], json!([]));
    let mut other = ctx();
    other.account_id = "999999999999".into();
    assert_eq!(
        handler.0.get_mapping(&other, &id).unwrap_err().code,
        "ResourceNotFoundException"
    );
    lambda_api(
        &handler.0,
        "UpdateEventSourceMapping",
        json!({"UUID":id,"Enabled":true}),
    );
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    let history = handler.0.history(&ctx()).unwrap();
    let record = &history["invocations"][0]["result"]["Records"][0];
    assert_eq!(record["body"], "hello");
    assert_eq!(record["eventSource"], "aws:sqs");
    assert_eq!(record["attributes"]["ApproximateReceiveCount"], "1");
    assert_eq!(record["messageAttributes"]["label"]["stringValue"], "test");
    assert_eq!(record["messageAttributes"]["binary"]["binaryValue"], "aGk=");
    assert!(
        sqs_api(&sqs, "ReceiveMessage", json!({"QueueUrl":url}))
            .get("Messages")
            .is_none()
    );
    lambda_api(&handler.0, "DeleteEventSourceMapping", json!({"UUID":id}));
    assert!(handler.0.get_mapping(&ctx(), &id).is_err());
}

#[test]
fn sqs_partial_failure_retries_only_failed_message_and_can_be_cleared() {
    let code = "import json,sys; e=json.load(sys.stdin); print(json.dumps({'batchItemFailures':[{'itemIdentifier':r['messageId']} for r in e['Records'] if r['body']=='fail']}))";
    let (sqs, handler) = sqs_lambda(&Store::ephemeral(), command(&["python3", "-c", code]));
    let url = queue(&sqs, "jobs");
    let id = mapping_for(&handler.0, "jobs", true);
    for body in ["ok", "fail"] {
        sqs_api(
            &sqs,
            "SendMessage",
            json!({"QueueUrl":url,"MessageBody":body}),
        );
    }
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    let received = sqs_api(
        &sqs,
        "ReceiveMessage",
        json!({"QueueUrl":url,"AttributeNames":["All"],"MaxNumberOfMessages":10}),
    );
    assert_eq!(received["Messages"].as_array().unwrap().len(), 1);
    assert_eq!(received["Messages"][0]["Body"], "fail");
    assert_eq!(
        received["Messages"][0]["Attributes"]["ApproximateReceiveCount"],
        "2"
    );
    lambda_api(
        &handler.0,
        "UpdateEventSourceMapping",
        json!({"UUID":id,"Enabled":false}),
    );
    assert_eq!(
        handler
            .0
            .get_mapping(&ctx(), &id)
            .unwrap()
            .function_response_types,
        vec!["ReportBatchItemFailures"]
    );
    let response = request(
        &handler,
        "PUT",
        &format!("/2015-03-31/event-source-mappings/{id}"),
        br#"{"FunctionResponseTypes":[]}"#,
        vec![],
    );
    assert_eq!(response.status, 202);
    assert!(
        handler
            .0
            .get_mapping(&ctx(), &id)
            .unwrap()
            .function_response_types
            .is_empty()
    );
}

#[test]
fn sqs_handler_failure_uses_queue_redrive_not_async_lambda_retries() {
    let (sqs, handler) = sqs_lambda(&Store::ephemeral(), command(&["sh", "-c", "exit 1"]));
    let url = queue(&sqs, "jobs");
    let dlq = queue(&sqs, "dead");
    sqs_api(
        &sqs,
        "SetQueueAttributes",
        json!({"QueueUrl":url,"Attributes":{"RedrivePolicy":json!({"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dead","maxReceiveCount":1}).to_string()}}),
    );
    let id = mapping_for(&handler.0, "jobs", false);
    sqs_api(
        &sqs,
        "SendMessage",
        json!({"QueueUrl":url,"MessageBody":"fail"}),
    );
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    let history = handler.0.history(&ctx()).unwrap();
    assert_eq!(history["invocations"][0]["state"], "failed");
    assert_eq!(history["invocations"][0]["attempts"], 1);
    std::thread::sleep(Duration::from_millis(1100));
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    assert_eq!(
        handler.0.history(&ctx()).unwrap()["invocations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sqs_api(&sqs, "ReceiveMessage", json!({"QueueUrl":dlq}))["Messages"][0]["Body"],
        "fail"
    );
}

#[test]
fn sqs_mapping_and_inflight_message_survive_restart() {
    let dir =
        std::env::temp_dir().join(format!("roto-sqs-lambda-{}", roto_core::ids::request_id()));
    let id;
    {
        let store = Store::open(&dir, StoreOptions::default()).unwrap();
        let (sqs, handler) = sqs_lambda(&store, command(&["sh", "-c", "cat"]));
        let url = queue(&sqs, "jobs");
        id = mapping_for(&handler.0, "jobs", false);
        sqs_api(
            &sqs,
            "SendMessage",
            json!({"QueueUrl":url,"MessageBody":"persisted"}),
        );
        // Simulate a crash after receiving, before invoking/acknowledging.
        let event = sqs
            .receive_event_batch(&ctx(), "arn:aws:sqs:us-east-1:123456789012:jobs", 10)
            .unwrap();
        assert_eq!(event["Records"].as_array().unwrap().len(), 1);
        let job = handler.0.job(&ctx(), "consume", event, None).unwrap();
        handler.0.record(&job, "running").unwrap();
    }
    std::thread::sleep(Duration::from_millis(1100));
    {
        let store = Store::open(&dir, StoreOptions::default()).unwrap();
        let sqs = Arc::new(roto_svc_sqs::Sqs::new(&store).unwrap());
        let handler = LambdaHandler::unstarted(
            &store,
            Executors {
                functions: BTreeMap::from([("consume".into(), command(&["sh", "-c", "cat"]))]),
            },
            sqs,
        )
        .unwrap();
        let interrupted = handler.0.history(&ctx()).unwrap();
        assert_eq!(interrupted["invocations"][0]["state"], "failed");
        handler
            .0
            .process_sqs_batch(&ctx(), &id, &runtime())
            .unwrap();
        let event = handler.0.history(&ctx()).unwrap();
        assert_eq!(
            event["invocations"][0]["result"]["Records"][0]["body"],
            "persisted"
        );
        assert_eq!(
            event["invocations"][0]["result"]["Records"][0]["attributes"]["ApproximateReceiveCount"],
            "2"
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn sqs_mapping_rejects_unsupported_settings_and_paginates() {
    let (sqs, handler) = sqs_lambda(&Store::ephemeral(), command(&["sh", "-c", "cat"]));
    queue(&sqs, "jobs");
    queue(&sqs, "jobs2");
    let source = "arn:aws:sqs:us-east-1:123456789012:jobs";
    for extra in [
        json!({"BatchSize":11}),
        json!({"MaximumBatchingWindowInSeconds":1}),
        json!({"FilterCriteria":{"Filters":[{"Pattern":"{}"}]}}),
        json!({"EventSourceArn":"arn:aws:sqs:us-west-2:123456789012:jobs"}),
    ] {
        let mut input = json!({"FunctionName":"consume","EventSourceArn":source});
        input
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(dispatch(&handler.0, &ctx(), "CreateEventSourceMapping", &input).is_err());
    }
    let id = mapping_for(&handler.0, "jobs", false);
    mapping_for(&handler.0, "jobs2", false);
    assert!(
        dispatch(
            &handler.0,
            &ctx(),
            "CreateEventSourceMapping",
            &json!({"FunctionName":"consume","EventSourceArn":source})
        )
        .is_err()
    );
    let first = lambda_api(&handler.0, "ListEventSourceMappings", json!({"MaxItems":1}));
    assert_eq!(first["EventSourceMappings"].as_array().unwrap().len(), 1);
    let second = lambda_api(
        &handler.0,
        "ListEventSourceMappings",
        json!({"MaxItems":1,"Marker":first["NextMarker"]}),
    );
    assert_eq!(second["EventSourceMappings"].as_array().unwrap().len(), 1);
    assert_ne!(
        first["EventSourceMappings"][0]["UUID"],
        second["EventSourceMappings"][0]["UUID"]
    );
    handler.0.reset().unwrap();
    assert!(handler.0.get_mapping(&ctx(), &id).is_err());
}

#[test]
fn sqs_invalid_partial_response_retries_whole_batch() {
    let (sqs, handler) = sqs_lambda(
        &Store::ephemeral(),
        command(&[
            "sh",
            "-c",
            "cat >/dev/null; printf '%s' '{\"batchItemFailures\":[{\"itemIdentifier\":\"unknown\"}]}'",
        ]),
    );
    let url = queue(&sqs, "jobs");
    let id = mapping_for(&handler.0, "jobs", true);
    for body in ["one", "two"] {
        sqs_api(
            &sqs,
            "SendMessage",
            json!({"QueueUrl":url,"MessageBody":body}),
        );
    }
    handler
        .0
        .process_sqs_batch(&ctx(), &id, &runtime())
        .unwrap();
    assert!(
        handler
            .0
            .get_mapping(&ctx(), &id)
            .unwrap()
            .last_processing_result
            .unwrap()
            .contains("Invalid partial batch response")
    );
    std::thread::sleep(Duration::from_millis(1100));
    let received = sqs_api(
        &sqs,
        "ReceiveMessage",
        json!({"QueueUrl":url,"MaxNumberOfMessages":10}),
    );
    assert_eq!(received["Messages"].as_array().unwrap().len(), 2);
}

#[test]
fn sqs_event_batch_respects_payload_limit_and_stale_receipts() {
    let sqs = roto_svc_sqs::Sqs::new(&Store::ephemeral()).unwrap();
    let url = queue(&sqs, "large");
    sqs_api(
        &sqs,
        "SetQueueAttributes",
        json!({"QueueUrl":url,"Attributes":{"VisibilityTimeout":"2"}}),
    );
    for _ in 0..10 {
        sqs_api(
            &sqs,
            "SendMessage",
            json!({"QueueUrl":url,"MessageBody":"\u{001f}".repeat(250_000)}),
        );
    }
    let source = "arn:aws:sqs:us-east-1:123456789012:large";
    let first = sqs.receive_event_batch(&ctx(), source, 10).unwrap();
    assert!(serde_json::to_vec(&first).unwrap().len() <= 6 * 1024 * 1024);
    assert_eq!(first["Records"].as_array().unwrap().len(), 4);
    assert!(
        first["Records"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["attributes"]["ApproximateReceiveCount"] == "1")
    );
    std::thread::sleep(Duration::from_millis(2100));
    let second = sqs.receive_event_batch(&ctx(), source, 1).unwrap();
    assert_eq!(
        first["Records"][0]["messageId"],
        second["Records"][0]["messageId"]
    );
    sqs.acknowledge_event(
        &ctx(),
        source,
        first["Records"][0]["receiptHandle"].as_str().unwrap(),
    )
    .unwrap();
    let attrs = sqs_api(
        &sqs,
        "GetQueueAttributes",
        json!({"QueueUrl":url,"AttributeNames":["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"]}),
    );
    let visible: usize = attrs["Attributes"]["ApproximateNumberOfMessages"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let hidden: usize = attrs["Attributes"]["ApproximateNumberOfMessagesNotVisible"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        visible + hidden,
        10,
        "Stale receipt must not acknowledge a newer delivery"
    );
}
