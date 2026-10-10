//! Opt-in per-request JSONL trace. Request bodies, query strings, and credentials are excluded.
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use roto_core::{RawRequest, RequestContext};

const MAX_RECENT_ENTRIES: usize = 10_000;

struct TraceState {
    writer: Option<BufWriter<File>>,
    recent: VecDeque<serde_json::Value>,
    dropped_entries: u64,
}

pub struct Trace(Mutex<TraceState>);

impl Trace {
    pub fn create(path: Option<&Path>) -> std::io::Result<Self> {
        let writer = path
            .map(|path| {
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)
                    .map(BufWriter::new)
            })
            .transpose()?;
        Ok(Self(Mutex::new(TraceState {
            writer,
            recent: VecDeque::new(),
            dropped_entries: 0,
        })))
    }

    pub fn record(&self, entry: serde_json::Value) {
        let Ok(mut writer) = self.0.lock() else {
            return;
        };
        if let Some(file) = &mut writer.writer {
            if let Err(error) = serde_json::to_writer(&mut *file, &entry) {
                tracing::error!(%error, "Could not write request trace entry");
            } else if let Err(error) = file.write_all(b"\n").and_then(|()| file.flush()) {
                tracing::error!(%error, "Could not flush request trace entry");
            }
        }
        if writer.recent.len() == MAX_RECENT_ENTRIES {
            writer.recent.pop_front();
            writer.dropped_entries += 1;
        }
        writer.recent.push_back(entry);
    }

    pub fn snapshot(&self, trace_id: Option<&str>) -> serde_json::Value {
        let Ok(state) = self.0.lock() else {
            return serde_json::json!({"entries": [], "dropped_entries": 0});
        };
        let entries: Vec<_> = state
            .recent
            .iter()
            .filter(|entry| trace_id.is_none_or(|id| entry["trace_id"].as_str() == Some(id)))
            .cloned()
            .collect();
        serde_json::json!({
            "entries": entries,
            "dropped_entries": state.dropped_entries,
            "max_recent_entries": MAX_RECENT_ENTRIES,
        })
    }
}

pub fn started_at_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

pub fn operation(service: Option<&str>, req: &RawRequest) -> Option<String> {
    if let Some(target) = req.header("x-amz-target") {
        return target
            .rsplit('.')
            .next()
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
    }
    if service == Some("s3") {
        return roto_svc_s3::operation_name(req).map(str::to_owned);
    }
    if service == Some("lambda") {
        return roto_svc_lambda::ROUTES.iter().find_map(|route| {
            if route.method != req.method {
                return None;
            }
            let template: Vec<_> = route.path.trim_start_matches('/').split('/').collect();
            let path: Vec<_> = req.path.trim_start_matches('/').split('/').collect();
            (template.len() == path.len()
                && template
                    .iter()
                    .zip(path)
                    .all(|(a, b)| (a.starts_with('{') && a.ends_with('}')) || *a == b))
            .then(|| route.operation.to_owned())
        });
    }
    let mut query = roto_protocol::QueryParams::parse(&req.query);
    if req
        .header("content-type")
        .is_some_and(|v| v.starts_with("application/x-www-form-urlencoded"))
    {
        query.extend(roto_protocol::QueryParams::parse(&String::from_utf8_lossy(
            &req.body,
        )));
    }
    query
        .get("Action")
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
}

pub fn trace_id(req: &RawRequest) -> Option<String> {
    req.header("x-roto-trace-id")
        .filter(|id| !id.is_empty())
        .map(|id| id.chars().take(256).collect())
        .or_else(|| {
            let authorization = req.header("authorization")?;
            let scope = roto_core::sigv4::CredentialScope::from_authorization(authorization)?;
            scope
                .access_key
                .starts_with("ROTO")
                .then(|| scope.access_key.chars().take(256).collect())
        })
}

pub fn response_error_code(response: &roto_core::RawResponse) -> Option<String> {
    if let Some((_, value)) = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-amzn-errortype"))
    {
        return Some(value.split(':').next().unwrap_or(value).to_owned());
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&response.body) {
        return value
            .get("__type")
            .or_else(|| value.get("code"))
            .or_else(|| value.get("Code"))
            .and_then(serde_json::Value::as_str)
            .map(|code| code.rsplit('#').next().unwrap_or(code).to_owned());
    }
    roto_protocol::restxml::parse_xml(&response.body)
        .ok()?
        .descendants()
        .find(|node| node.has_tag_name("Code"))
        .and_then(|node| node.text())
        .map(str::to_owned)
}

pub fn entry(
    req: &RawRequest,
    ctx: &RequestContext,
    service: Option<&str>,
    operation: Option<String>,
    status: u16,
    outcome: &str,
    error_code: Option<&str>,
    duration_ms: u128,
) -> serde_json::Value {
    serde_json::json!({
        "timestamp_ms": started_at_ms(), "duration_ms": duration_ms,
        "request_id": ctx.request_id, "trace_id": trace_id(req), "service": service,
        "operation": operation, "method": req.method, "path": req.path,
        "account_id": ctx.account_id, "region": ctx.region, "status": status,
        "outcome": outcome, "error_code": error_code,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(access_key: &str) -> RawRequest {
        RawRequest {
            headers: vec![(
                "authorization".into(),
                format!(
                    "AWS4-HMAC-SHA256 Credential={access_key}/20261010/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=secret"
                ),
            )],
            ..Default::default()
        }
    }

    #[test]
    fn uses_full_roto_access_key_as_trace_id_and_ignores_other_keys() {
        assert_eq!(
            trace_id(&request("ROTO_checkout-42")).as_deref(),
            Some("ROTO_checkout-42")
        );
        assert_eq!(trace_id(&request("testing")), None);
    }

    #[test]
    fn explicit_trace_header_takes_precedence_over_roto_access_key() {
        let mut request = request("ROTO_checkout-42");
        request
            .headers
            .push(("x-roto-trace-id".into(), "explicit-id".into()));
        assert_eq!(trace_id(&request).as_deref(), Some("explicit-id"));
    }

    #[test]
    fn in_memory_trace_can_be_queried_without_a_file() {
        let trace = Trace::create(None).unwrap();
        trace.record(serde_json::json!({"trace_id": "ROTO_checkout-42", "operation": "PutObject"}));
        trace.record(serde_json::json!({"trace_id": "ROTO_other", "operation": "ListBuckets"}));

        let result = trace.snapshot(Some("ROTO_checkout-42"));
        assert_eq!(result["entries"].as_array().unwrap().len(), 1);
        assert_eq!(result["entries"][0]["operation"], "PutObject");
        assert_eq!(result["dropped_entries"], 0);
    }
}
