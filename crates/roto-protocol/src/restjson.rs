//! Model-generated REST-JSON routes, with JSON, URI, query, header and payload bindings.
use roto_core::{AwsError, RawRequest, RawResponse};
use serde_json::{Map, Value};

use crate::{QueryParams, base64, restxml::percent_decode_path};

pub struct Binding {
    pub member: &'static str,
    pub wire: &'static str,
    pub location: &'static str,
    pub kind: &'static str,
}

pub struct Route {
    pub operation: &'static str,
    pub method: &'static str,
    pub path: &'static str,
    pub status: u16,
    pub input: &'static [Binding],
    pub output: &'static [Binding],
}

pub fn decode<'a>(routes: &'a [Route], req: &RawRequest) -> Result<(&'a Route, Value), AwsError> {
    let segments: Vec<_> = req.path.trim_start_matches('/').split('/').collect();
    for route in routes {
        if route.method != req.method {
            continue;
        }
        let template: Vec<_> = route.path.trim_start_matches('/').split('/').collect();
        if template.len() != segments.len() {
            continue;
        }
        let mut uri = Map::new();
        let mut matches = true;
        for (want, got) in template.iter().zip(&segments) {
            if let Some(name) = want.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                uri.insert(name.into(), Value::String(percent_decode_path(got)));
            } else if want != got {
                matches = false;
                break;
            }
        }
        if !matches {
            continue;
        }
        let payload = route.input.iter().find(|b| b.location == "payload");
        let mut body = if payload.is_some() || req.body.is_empty() {
            Map::new()
        } else {
            serde_json::from_slice::<Value>(&req.body)
                .map_err(|e| {
                    AwsError::sender(400, "InvalidRequestContentException", e.to_string())
                })?
                .as_object()
                .cloned()
                .ok_or_else(|| {
                    AwsError::invalid_parameter_value("Request body must be an object")
                })?
        };
        let query = QueryParams::parse(&req.query);
        for b in route.input {
            if b.location == "querystring" && b.kind == "list" {
                let values: Vec<_> = query
                    .get_all(b.wire)
                    .map(|s| Value::String(s.into()))
                    .collect();
                if !values.is_empty() {
                    body.insert(b.member.into(), Value::Array(values));
                }
                continue;
            }
            let value = match b.location {
                "uri" => uri.get(b.wire).and_then(Value::as_str),
                "querystring" => query.get(b.wire),
                "header" => req.header(&b.wire.to_ascii_lowercase()),
                "payload" => {
                    let value = match b.kind {
                        "blob" => Value::String(base64::encode(&req.body)),
                        "string" => Value::String(String::from_utf8_lossy(&req.body).into_owned()),
                        _ => serde_json::from_slice(&req.body)
                            .map_err(|e| AwsError::invalid_parameter_value(e.to_string()))?,
                    };
                    body.insert(b.member.into(), value);
                    continue;
                }
                _ => None,
            };
            if let Some(value) = value {
                let value = match b.kind {
                    "integer" | "long" | "boolean" | "double" | "float" => {
                        serde_json::from_str(value).map_err(|_| {
                            AwsError::invalid_parameter_value(format!("Invalid {}", b.member))
                        })?
                    }
                    _ => Value::String(value.into()),
                };
                body.insert(b.member.into(), value);
            }
        }
        return Ok((route, Value::Object(body)));
    }
    Err(AwsError::sender(
        404,
        "ResourceNotFoundException",
        "Unknown operation",
    ))
}

pub fn encode(route: &Route, mut response: RawResponse) -> Result<RawResponse, AwsError> {
    let mut value: Value =
        serde_json::from_slice(&response.body).map_err(|e| AwsError::internal(e.to_string()))?;
    response.status = route.status;
    response.headers.retain(|(k, _)| k != "content-type");
    response
        .headers
        .push(("content-type".into(), "application/json".into()));
    let mut payload = None;
    for b in route.output {
        let v = value.as_object_mut().and_then(|o| o.remove(b.member));
        if let Some(v) = v {
            match b.location {
                "header" => response.headers.push((
                    b.wire.to_ascii_lowercase(),
                    v.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| v.to_string()),
                )),
                "statusCode" => response.status = v.as_u64().unwrap_or(route.status as u64) as u16,
                "payload" => {
                    payload = Some(match b.kind {
                        "blob" => base64::decode(v.as_str().unwrap_or(""))
                            .ok_or_else(|| AwsError::internal("Invalid payload encoding"))?,
                        "string" => v.as_str().unwrap_or("").as_bytes().to_vec(),
                        _ => serde_json::to_vec(&v).unwrap(),
                    })
                }
                _ => {}
            }
        }
    }
    response.body = if route.output.iter().any(|b| b.location == "payload") {
        payload.unwrap_or_default()
    } else if response.status == 204 {
        Vec::new()
    } else {
        serde_json::to_vec(&value).unwrap()
    };
    Ok(response)
}
