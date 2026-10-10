//! In-memory discovery of unsupported HTTP calls; never captures bodies or credentials.
use std::collections::BTreeMap;
use std::sync::Mutex;

use roto_core::{AwsError, RawRequest, RawResponse, RequestContext};
use roto_protocol::QueryParams;
use serde_json::{Value, json};

const MAX_CALLS: usize = 1000;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Call {
    pub service: Option<String>,
    pub operation: Option<String>,
    pub method: String,
    pub path: String,
    pub account_id: String,
    pub region: String,
    pub reason: &'static str,
}

impl Call {
    pub fn new(
        service: Option<&str>,
        req: &RawRequest,
        ctx: &RequestContext,
        reason: &'static str,
        operation: Option<String>,
    ) -> Self {
        Self {
            service: service.map(str::to_owned),
            operation: operation.or_else(|| request_operation(req)),
            method: req.method.clone(),
            path: req.path.clone(),
            account_id: ctx.account_id.clone(),
            region: ctx.region.clone(),
            reason,
        }
    }
}

#[derive(Default)]
struct Calls {
    entries: BTreeMap<Call, u64>,
    dropped_calls: u64,
}

#[derive(Default)]
pub struct Unsupported(Mutex<Calls>);

impl Unsupported {
    pub fn record(&self, call: Call) {
        tracing::warn!(
            service = call.service.as_deref().unwrap_or("unknown"),
            operation = call.operation.as_deref().unwrap_or("unknown"),
            method = %call.method,
            path = %call.path,
            account_id = %call.account_id,
            region = %call.region,
            reason = call.reason,
            "Unsupported AWS call"
        );
        let mut calls = self.0.lock().unwrap();
        if let Some(count) = calls.entries.get_mut(&call) {
            *count += 1;
        } else if calls.entries.len() < MAX_CALLS {
            calls.entries.insert(call, 1);
        } else {
            calls.dropped_calls += 1;
        }
    }

    pub fn snapshot(&self) -> Value {
        let calls = self.0.lock().unwrap();
        let entries: Vec<_> = calls
            .entries
            .iter()
            .map(|(call, count)| {
                json!({
                    "service": call.service, "operation": call.operation,
                    "method": call.method, "path": call.path,
                    "account_id": call.account_id, "region": call.region,
                    "reason": call.reason, "count": count,
                })
            })
            .collect();
        json!({"calls": entries, "dropped_calls": calls.dropped_calls})
    }

    pub fn reset(&self) {
        *self.0.lock().unwrap() = Calls::default();
    }
}

fn request_operation(req: &RawRequest) -> Option<String> {
    if let Some(target) = req.header("x-amz-target") {
        return target
            .rsplit('.')
            .next()
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
    }
    let mut query = QueryParams::parse(&req.query);
    // Only Query protocol bodies carry Action. Never interpret arbitrary binary/JSON bodies.
    if req
        .header("content-type")
        .is_some_and(|s| s.starts_with("application/x-www-form-urlencoded"))
    {
        query.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
    }
    query
        .get("Action")
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn classify(code: &str, message: &str) -> Option<(&'static str, Option<String>)> {
    let code = code.split(':').next().unwrap_or(code);
    let code = code.rsplit('#').next().unwrap_or(code);
    match code {
        "NotImplemented" | "NotImplementedException" => {
            // Generated defaults name REST operations even when the request has no Action/target.
            let operation = message
                .strip_prefix("roto does not implement ")
                .and_then(|s| s.strip_suffix(" yet"))
                .and_then(|s| s.split_once(':'))
                .map(|(_, operation)| operation.to_owned());
            Some(("not_implemented", operation))
        }
        "InvalidAction" | "UnknownOperationException" => Some(("unknown_operation", None)),
        "ResourceNotFoundException" if message == "Unknown operation" => {
            Some(("unknown_operation", None))
        }
        _ => None,
    }
}

pub fn error(error: &AwsError) -> Option<(&'static str, Option<String>)> {
    classify(&error.code, &error.message)
}

pub fn response(response: &RawResponse) -> Option<(&'static str, Option<String>)> {
    if response.status < 400 {
        return None;
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&response.body) {
        let code = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-amzn-errortype"))
            .map(|(_, value)| value.as_str())
            .or_else(|| value.get("__type").and_then(Value::as_str))
            .or_else(|| value.get("code").and_then(Value::as_str))
            .unwrap_or("");
        let message = value
            .get("message")
            .or_else(|| value.get("Message"))
            .and_then(Value::as_str)
            .unwrap_or("");
        return classify(code, message);
    }
    // Query and REST-XML error serializers use these exact elements.
    let xml = std::str::from_utf8(&response.body).ok()?;
    fn element<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
        xml.split_once(&format!("<{tag}>"))?
            .1
            .split_once(&format!("</{tag}>"))
            .map(|(value, _)| value)
    }
    classify(element(xml, "Code")?, element(xml, "Message").unwrap_or(""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost".into(),
        }
    }

    #[test]
    fn deduplicates_scopes_and_resets_without_capturing_secrets() {
        let recorder = Unsupported::default();
        let req = RawRequest {
            method: "POST".into(),
            path: "/".into(),
            query: "token=secret".into(),
            headers: vec![
                (
                    "x-amz-target".into(),
                    "DynamoDB_20120810.ExecuteStatement".into(),
                ),
                ("authorization".into(), "secret".into()),
            ],
            body: b"secret".to_vec(),
        };
        let call = Call::new(Some("dynamodb"), &req, &context(), "not_implemented", None);
        recorder.record(call.clone());
        recorder.record(call);
        let mut ctx = context();
        ctx.region = "eu-west-1".into();
        recorder.record(Call::new(
            Some("dynamodb"),
            &req,
            &ctx,
            "not_implemented",
            None,
        ));
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot["calls"].as_array().unwrap().len(), 2);
        assert_eq!(snapshot["calls"][1]["count"], 2);
        assert!(!snapshot.to_string().contains("secret"));
        recorder.reset();
        assert_eq!(
            recorder.snapshot(),
            json!({"calls": [], "dropped_calls": 0})
        );
    }

    #[test]
    fn reads_query_action_but_not_arbitrary_payloads() {
        let mut req = RawRequest {
            body: b"Action=Create%54hing".to_vec(),
            ..Default::default()
        };
        assert_eq!(request_operation(&req), None);
        req.headers.push((
            "content-type".into(),
            "application/x-www-form-urlencoded; charset=utf-8".into(),
        ));
        assert_eq!(request_operation(&req).as_deref(), Some("CreateThing"));
        req.body.clear();
        req.query = "Action=ListThings".into();
        assert_eq!(request_operation(&req).as_deref(), Some("ListThings"));
    }

    #[test]
    fn classifies_json_xml_and_direct_errors_but_not_normal_failures() {
        let unsupported = AwsError::not_implemented("s3", "RestoreObject");
        assert_eq!(
            error(&unsupported),
            Some(("not_implemented", Some("RestoreObject".into())))
        );
        let json = RawResponse { status: 501, headers: vec![], body: br#"{"__type":"com.aws#NotImplemented","message":"roto does not implement lambda:GetAlias yet"}"#.to_vec() };
        assert_eq!(
            response(&json),
            Some(("not_implemented", Some("GetAlias".into())))
        );
        let header_error = RawResponse {
            status: 501,
            headers: vec![(
                "x-amzn-errortype".into(),
                "NotImplementedException:detail".into(),
            )],
            body: b"{}".to_vec(),
        };
        assert_eq!(response(&header_error), Some(("not_implemented", None)));
        let xml = roto_protocol::query_error("urn:test", &unsupported, "test");
        assert_eq!(response(&xml), error(&unsupported));
        let mut normal = json.clone();
        normal.status = 200;
        assert_eq!(response(&normal), None);
        for code in [
            "ResourceNotFoundException",
            "ValidationException",
            "InternalFailure",
            "AccessDeniedException",
        ] {
            assert_eq!(error(&AwsError::sender(400, code, "ordinary error")), None);
        }
        assert_eq!(
            error(&AwsError::sender(
                404,
                "ResourceNotFoundException",
                "Unknown operation"
            )),
            Some(("unknown_operation", None))
        );
    }

    #[test]
    fn bounds_unique_entries_and_keeps_counting_existing_calls() {
        let recorder = Unsupported::default();
        let mut req = RawRequest::default();
        for i in 0..MAX_CALLS + 2 {
            req.path = format!("/{i}");
            recorder.record(Call::new(None, &req, &context(), "unroutable", None));
        }
        req.path = "/0".into();
        recorder.record(Call::new(None, &req, &context(), "unroutable", None));
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot["calls"].as_array().unwrap().len(), MAX_CALLS);
        assert_eq!(snapshot["dropped_calls"], 2);
        assert_eq!(snapshot["calls"][0]["count"], 2);
    }
}
