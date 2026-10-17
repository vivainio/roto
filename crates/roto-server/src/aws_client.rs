//! Model-driven client codec for the REST-XML and Query protocols, so setup Lua can call
//! S3, IAM, SNS, STS and CloudFormation operations with boto3-shaped tables (`roto.call`).
//!
//! The server-side generated code only parses requests and writes responses; this is the
//! opposite direction. It interprets the botocore `service-2.json` models in `models/` at
//! runtime: input members are routed to the URI, query string, headers or body, and the XML
//! response is converted back into an AWS-shaped (PascalCase) JSON value.
use std::collections::HashMap;
use std::sync::OnceLock;

use roto_core::{RawRequest, RequestContext, ServiceHandler};
use serde_json::{Map, Value};

/// Services `roto.call` speaks through the XML protocols, driven by their embedded models.
const XML_SERVICES: &[&str] = &["s3", "iam", "sns", "sts", "cloudformation"];

type Models = HashMap<&'static str, Value>;

fn models() -> &'static Models {
    static CELL: OnceLock<Models> = OnceLock::new();
    CELL.get_or_init(|| {
        crate::luau_stubs::MODELS
            .iter()
            .filter(|(name, _)| XML_SERVICES.contains(name))
            .map(|(name, text)| (*name, serde_json::from_str(text).expect("embedded model")))
            .collect()
    })
}

/// JSON-protocol services need no model: the table is the request body and the operation
/// goes in `x-amz-target`.
const JSON_SERVICES: &[&str] = &[
    "dynamodb",
    "ssm",
    "secretsmanager",
    "kinesis",
    "kms",
    "logs",
    "events",
];

pub(crate) fn supports(service: &str) -> bool {
    JSON_SERVICES.contains(&service) || XML_SERVICES.contains(&service)
}

#[derive(Clone, Copy, PartialEq)]
enum Protocol {
    RestXml,
    Query,
}

struct Model<'a> {
    root: &'a Value,
    protocol: Protocol,
}

/// A member reference (`{"shape": ..., "locationName": ...}`) plus the name it is known by.
#[derive(Clone, Copy)]
struct Member<'a> {
    name: &'a str,
    spec: &'a Value,
}

impl<'a> Model<'a> {
    fn shape(&self, spec: &Value) -> &'a Value {
        let name = spec["shape"].as_str().unwrap_or_default();
        &self.root["shapes"][name]
    }
    fn members(&self, shape: &'a Value) -> Vec<Member<'a>> {
        shape["members"]
            .as_object()
            .map(|m| m.iter().map(|(name, spec)| Member { name, spec }).collect())
            .unwrap_or_default()
    }
}

fn wire_name<'a>(member: Member<'a>) -> &'a str {
    member.spec["locationName"].as_str().unwrap_or(member.name)
}

fn is_flattened(member_spec: &Value, shape: &Value) -> bool {
    member_spec["flattened"].as_bool().unwrap_or(false)
        || shape["flattened"].as_bool().unwrap_or(false)
}

fn scalar_text(value: &Value, what: &str) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        other => Err(format!("{what}: expected a scalar, got {other}")),
    }
}

fn percent_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || (keep_slash && b == b'/')
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn as_list<'v>(value: &'v Value, what: &str) -> Result<Vec<&'v Value>, String> {
    match value {
        Value::Array(items) => Ok(items.iter().collect()),
        // An empty Lua table is indistinguishable from an empty object.
        Value::Object(map) if map.is_empty() => Ok(Vec::new()),
        other => Err(format!("{what}: expected a list, got {other}")),
    }
}

fn as_map<'v>(value: &'v Value, what: &str) -> Result<&'v Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{what}: expected a table, got {value}"))
}

fn reject_unknown(
    model: &Model,
    shape: &Value,
    input: &Map<String, Value>,
    what: &str,
) -> Result<(), String> {
    for key in input.keys() {
        if !shape["members"].get(key.as_str()).is_some() {
            let _ = model;
            return Err(format!("{what}: unknown member {key}"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------------------

pub(crate) fn call(
    handler: &dyn ServiceHandler,
    ctx: &RequestContext,
    service: &str,
    operation: &str,
    input: &Value,
) -> Result<Value, String> {
    if JSON_SERVICES.contains(&service) {
        return call_json(handler, ctx, operation, input);
    }
    let root = models()
        .get(service)
        .ok_or_else(|| format!("Unknown service: {service}"))?;
    let protocol = match root["metadata"]["protocol"].as_str() {
        Some("rest-xml") => Protocol::RestXml,
        Some("query") => Protocol::Query,
        other => return Err(format!("Unsupported protocol for {service}: {other:?}")),
    };
    let model = Model { root, protocol };
    let op = root["operations"]
        .get(operation)
        .ok_or_else(|| format!("Unknown {service} operation: {operation}"))?;
    let request = match protocol {
        Protocol::RestXml => build_rest_xml(&model, op, operation, input)?,
        Protocol::Query => build_query(&model, op, operation, input)?,
    };
    let response = handler.handle(ctx, &request).map_err(|e| e.to_string())?;
    if response.status >= 400 {
        return Err(format!(
            "AWS request failed ({}): {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    parse_response(&model, op, &response)
}

fn call_json(
    handler: &dyn ServiceHandler,
    ctx: &RequestContext,
    operation: &str,
    input: &Value,
) -> Result<Value, String> {
    let body = if input.is_null() {
        serde_json::to_vec(&Value::Object(Map::new()))
    } else {
        serde_json::to_vec(input)
    }
    .map_err(|e| e.to_string())?;
    let response = handler
        .handle(
            ctx,
            &RawRequest {
                method: "POST".into(),
                path: "/".into(),
                headers: vec![("x-amz-target".into(), operation.to_string())],
                body,
                ..Default::default()
            },
        )
        .map_err(|e| e.to_string())?;
    if response.status >= 400 {
        return Err(format!(
            "AWS request failed ({}): {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    if response.body.is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_slice(&response.body).map_err(|e| e.to_string())
    }
}

fn input_object(input: &Value, operation: &str) -> Result<Map<String, Value>, String> {
    match input {
        Value::Null => Ok(Map::new()),
        Value::Object(map) => Ok(map.clone()),
        other => Err(format!("{operation}: input must be a table, got {other}")),
    }
}

fn build_rest_xml(
    model: &Model,
    op: &Value,
    operation: &str,
    input: &Value,
) -> Result<RawRequest, String> {
    let input = input_object(input, operation)?;
    let http = &op["http"];
    let method = http["method"].as_str().unwrap_or("POST").to_string();
    let uri = http["requestUri"].as_str().unwrap_or("/");
    let (mut path, literal_query) = match uri.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (uri.to_string(), String::new()),
    };
    let mut query: Vec<String> = if literal_query.is_empty() {
        Vec::new()
    } else {
        vec![literal_query]
    };
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut body_members: Vec<(Member, &Value)> = Vec::new();

    let empty = Value::Null;
    let in_shape = if op["input"].is_null() {
        &empty
    } else {
        model.shape(&op["input"])
    };
    reject_unknown(model, in_shape, &input, operation)?;
    let payload = in_shape["payload"].as_str();

    for member in model.members(in_shape) {
        let Some(value) = input.get(member.name) else {
            continue;
        };
        let shape = model.shape(member.spec);
        let loc = member.spec["locationName"].as_str().unwrap_or(member.name);
        match member.spec["location"].as_str() {
            Some("uri") => {
                let text = scalar_text(value, member.name)?;
                let greedy = format!("{{{loc}+}}");
                let plain = format!("{{{loc}}}");
                if path.contains(&greedy) {
                    path = path.replace(&greedy, &percent_encode(&text, true));
                } else {
                    path = path.replace(&plain, &percent_encode(&text, false));
                }
            }
            Some("querystring") => match shape["type"].as_str() {
                Some("list") => {
                    for item in as_list(value, member.name)? {
                        query.push(format!(
                            "{}={}",
                            percent_encode(loc, false),
                            percent_encode(&scalar_text(item, member.name)?, false)
                        ));
                    }
                }
                Some("map") => {
                    for (k, v) in as_map(value, member.name)? {
                        query.push(format!(
                            "{}={}",
                            percent_encode(k, false),
                            percent_encode(&scalar_text(v, member.name)?, false)
                        ));
                    }
                }
                _ => query.push(format!(
                    "{}={}",
                    percent_encode(loc, false),
                    percent_encode(&scalar_text(value, member.name)?, false)
                )),
            },
            Some("header") => {
                headers.push((loc.to_ascii_lowercase(), scalar_text(value, member.name)?));
            }
            Some("headers") => {
                let prefix = member.spec["locationName"].as_str().unwrap_or("");
                for (k, v) in as_map(value, member.name)? {
                    headers.push((
                        format!("{prefix}{k}").to_ascii_lowercase(),
                        scalar_text(v, member.name)?,
                    ));
                }
            }
            _ => body_members.push((member, value)),
        }
    }

    if let Some(payload_name) = payload {
        if let Some((member, value)) = body_members.iter().find(|(m, _)| m.name == payload_name) {
            let shape = model.shape(member.spec);
            match shape["type"].as_str() {
                Some("blob") | Some("string") => {
                    body = scalar_text(value, member.name)?.into_bytes();
                }
                _ => {
                    let mut xml = String::new();
                    write_xml(model, &mut xml, wire_name(*member), member.spec, value)?;
                    body = xml.into_bytes();
                }
            }
        }
    } else if !body_members.is_empty() {
        let root_name = in_shape["locationName"]
            .as_str()
            .or_else(|| op["input"]["locationName"].as_str())
            .or_else(|| op["input"]["shape"].as_str())
            .unwrap_or("Request");
        let mut xml = String::new();
        let ns = in_shape["xmlNamespace"]["uri"]
            .as_str()
            .map(|u| format!(" xmlns=\"{}\"", escape(u)))
            .unwrap_or_default();
        xml.push_str(&format!("<{root_name}{ns}>"));
        for (member, value) in &body_members {
            write_xml(model, &mut xml, wire_name(*member), member.spec, value)?;
        }
        xml.push_str(&format!("</{root_name}>"));
        body = xml.into_bytes();
    }

    Ok(RawRequest {
        method,
        path,
        query: query.join("&"),
        headers,
        body,
    })
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Serialise `value` as XML element `name` according to the member spec.
fn write_xml(
    model: &Model,
    out: &mut String,
    name: &str,
    spec: &Value,
    value: &Value,
) -> Result<(), String> {
    let shape = model.shape(spec);
    match shape["type"].as_str().unwrap_or("string") {
        "structure" => {
            let map = as_map(value, name)?;
            reject_unknown(model, shape, map, name)?;
            let mut attrs = String::new();
            let ns = spec["xmlNamespace"]["uri"]
                .as_str()
                .or_else(|| shape["xmlNamespace"]["uri"].as_str());
            if let Some(uri) = ns {
                attrs.push_str(&format!(" xmlns=\"{}\"", escape(uri)));
            }
            let members = model.members(shape);
            for member in &members {
                if member.spec["xmlAttribute"].as_bool().unwrap_or(false)
                    && let Some(v) = map.get(member.name)
                {
                    attrs.push_str(&format!(
                        " {}=\"{}\"",
                        wire_name(*member),
                        escape(&scalar_text(v, member.name)?)
                    ));
                }
            }
            out.push_str(&format!("<{name}{attrs}>"));
            for member in &members {
                if member.spec["xmlAttribute"].as_bool().unwrap_or(false) {
                    continue;
                }
                if let Some(v) = map.get(member.name) {
                    write_xml(model, out, wire_name(*member), member.spec, v)?;
                }
            }
            out.push_str(&format!("</{name}>"));
        }
        "list" => {
            let items = as_list(value, name)?;
            let item_spec = &shape["member"];
            if is_flattened(spec, shape) {
                for item in items {
                    write_xml(model, out, name, item_spec, item)?;
                }
            } else {
                let item_name = item_spec["locationName"].as_str().unwrap_or("member");
                out.push_str(&format!("<{name}>"));
                for item in items {
                    write_xml(model, out, item_name, item_spec, item)?;
                }
                out.push_str(&format!("</{name}>"));
            }
        }
        "map" => {
            let map = as_map(value, name)?;
            let key_name = shape["key"]["locationName"].as_str().unwrap_or("key");
            let value_name = shape["value"]["locationName"].as_str().unwrap_or("value");
            let flattened = is_flattened(spec, shape);
            if !flattened {
                out.push_str(&format!("<{name}>"));
            }
            for (k, v) in map {
                let entry = if flattened { name } else { "entry" };
                out.push_str(&format!("<{entry}>"));
                out.push_str(&format!("<{key_name}>{}</{key_name}>", escape(k)));
                write_xml(model, out, value_name, &shape["value"], v)?;
                out.push_str(&format!("</{entry}>"));
            }
            if !flattened {
                out.push_str(&format!("</{name}>"));
            }
        }
        "blob" => {
            let text = scalar_text(value, name)?;
            out.push_str(&format!(
                "<{name}>{}</{name}>",
                escape(&roto_protocol::base64::encode(text.as_bytes()))
            ));
        }
        _ => out.push_str(&format!(
            "<{name}>{}</{name}>",
            escape(&scalar_text(value, name)?)
        )),
    }
    Ok(())
}

fn build_query(
    model: &Model,
    op: &Value,
    operation: &str,
    input: &Value,
) -> Result<RawRequest, String> {
    let input = input_object(input, operation)?;
    let mut params: Vec<(String, String)> = vec![
        ("Action".into(), operation.to_string()),
        (
            "Version".into(),
            model.root["metadata"]["apiVersion"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        ),
    ];
    if !op["input"].is_null() {
        let shape = model.shape(&op["input"]);
        reject_unknown(model, shape, &input, operation)?;
        for member in model.members(shape) {
            if let Some(value) = input.get(member.name) {
                write_query(model, &mut params, wire_name(member), member.spec, value)?;
            }
        }
    }
    let body = params
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k, false), percent_encode(v, false)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(RawRequest {
        method: "POST".into(),
        path: "/".into(),
        query: String::new(),
        headers: vec![(
            "content-type".into(),
            "application/x-www-form-urlencoded; charset=utf-8".into(),
        )],
        body: body.into_bytes(),
    })
}

fn write_query(
    model: &Model,
    params: &mut Vec<(String, String)>,
    prefix: &str,
    spec: &Value,
    value: &Value,
) -> Result<(), String> {
    let shape = model.shape(spec);
    match shape["type"].as_str().unwrap_or("string") {
        "structure" => {
            let map = as_map(value, prefix)?;
            reject_unknown(model, shape, map, prefix)?;
            for member in model.members(shape) {
                if let Some(v) = map.get(member.name) {
                    let key = format!("{prefix}.{}", wire_name(member));
                    write_query(model, params, &key, member.spec, v)?;
                }
            }
        }
        "list" => {
            let items = as_list(value, prefix)?;
            if items.is_empty() {
                params.push((prefix.to_string(), String::new()));
                return Ok(());
            }
            let item_spec = &shape["member"];
            let list_prefix = if is_flattened(spec, shape) {
                match item_spec["locationName"].as_str() {
                    Some(name) => {
                        let mut parts: Vec<&str> = prefix.split('.').collect();
                        parts.pop();
                        parts.push(name);
                        parts.join(".")
                    }
                    None => prefix.to_string(),
                }
            } else {
                format!(
                    "{prefix}.{}",
                    item_spec["locationName"].as_str().unwrap_or("member")
                )
            };
            for (i, item) in items.into_iter().enumerate() {
                write_query(
                    model,
                    params,
                    &format!("{list_prefix}.{}", i + 1),
                    item_spec,
                    item,
                )?;
            }
        }
        "map" => {
            let map = as_map(value, prefix)?;
            let key_name = shape["key"]["locationName"].as_str().unwrap_or("key");
            let value_name = shape["value"]["locationName"].as_str().unwrap_or("value");
            let base = if is_flattened(spec, shape) {
                prefix.to_string()
            } else {
                format!("{prefix}.entry")
            };
            for (i, (k, v)) in map.iter().enumerate() {
                params.push((format!("{base}.{}.{key_name}", i + 1), k.clone()));
                write_query(
                    model,
                    params,
                    &format!("{base}.{}.{value_name}", i + 1),
                    &shape["value"],
                    v,
                )?;
            }
        }
        "blob" => params.push((
            prefix.to_string(),
            roto_protocol::base64::encode(scalar_text(value, prefix)?.as_bytes()),
        )),
        _ => params.push((prefix.to_string(), scalar_text(value, prefix)?)),
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------------------

fn parse_response(
    model: &Model,
    op: &Value,
    response: &roto_core::RawResponse,
) -> Result<Value, String> {
    if op["output"].is_null() {
        return Ok(Value::Null);
    }
    let shape = model.shape(&op["output"]);
    let mut result = Map::new();
    let text = String::from_utf8_lossy(&response.body);
    let header = |name: &str| {
        response
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };

    let mut body_shape_members: Vec<Member> = Vec::new();
    for member in model.members(shape) {
        match member.spec["location"].as_str() {
            Some("header") => {
                if let Some(v) = header(member.spec["locationName"].as_str().unwrap_or(member.name))
                {
                    result.insert(
                        member.name.to_string(),
                        typed_scalar(model.shape(member.spec), v),
                    );
                }
            }
            Some("headers") => {
                let prefix = member.spec["locationName"]
                    .as_str()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let mut map = Map::new();
                for (k, v) in &response.headers {
                    if let Some(rest) = k.to_ascii_lowercase().strip_prefix(&prefix) {
                        map.insert(rest.to_string(), Value::String(v.clone()));
                    }
                }
                if !map.is_empty() {
                    result.insert(member.name.to_string(), Value::Object(map));
                }
            }
            Some("statusCode") => {
                result.insert(member.name.to_string(), Value::from(response.status));
            }
            _ => body_shape_members.push(member),
        }
    }

    if let Some(payload) = shape["payload"].as_str() {
        if let Some(member) = body_shape_members.iter().find(|m| m.name == payload) {
            let payload_shape = model.shape(member.spec);
            match payload_shape["type"].as_str() {
                Some("blob") | Some("string") => {
                    result.insert(member.name.to_string(), Value::String(text.into_owned()));
                }
                _ => {
                    if !text.trim().is_empty() {
                        let doc = roxmltree::Document::parse(&text).map_err(|e| e.to_string())?;
                        result.insert(
                            member.name.to_string(),
                            parse_xml(model, member.spec, doc.root_element()),
                        );
                    }
                }
            }
        }
    } else if !body_shape_members.is_empty() && !text.trim().is_empty() {
        let doc = roxmltree::Document::parse(&text).map_err(|e| e.to_string())?;
        let mut node = doc.root_element();
        if model.protocol == Protocol::Query
            && let Some(wrapper) = op["output"]["resultWrapper"].as_str()
        {
            match children_named(node, wrapper).into_iter().next() {
                Some(inner) => node = inner,
                None => return Ok(Value::Object(result)),
            }
        }
        if let Value::Object(map) = parse_structure(model, shape, &body_shape_members, node) {
            result.extend(map);
        }
    }
    Ok(Value::Object(result))
}

fn children_named<'a, 'i>(
    node: roxmltree::Node<'a, 'i>,
    name: &str,
) -> Vec<roxmltree::Node<'a, 'i>> {
    node.children()
        .filter(|c| c.is_element() && c.tag_name().name() == name)
        .collect()
}

fn typed_scalar(shape: &Value, text: &str) -> Value {
    match shape["type"].as_str() {
        Some("integer") | Some("long") => text
            .trim()
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(text.into())),
        Some("float") | Some("double") => text
            .trim()
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.into())),
        Some("boolean") => Value::Bool(text.trim() == "true"),
        _ => Value::String(text.into()),
    }
}

fn parse_structure(
    model: &Model,
    shape: &Value,
    members: &[Member],
    node: roxmltree::Node,
) -> Value {
    let _ = shape;
    let mut out = Map::new();
    for member in members {
        let name = wire_name(*member);
        let member_shape = model.shape(member.spec);
        if member.spec["xmlAttribute"].as_bool().unwrap_or(false) {
            if let Some(v) = node.attribute(name) {
                out.insert(member.name.to_string(), Value::String(v.into()));
            }
            continue;
        }
        let kind = member_shape["type"].as_str().unwrap_or("string");
        let flattened = is_flattened(member.spec, member_shape);
        match kind {
            "list" if flattened => {
                let found = children_named(node, name);
                if !found.is_empty() {
                    let item_spec = &member_shape["member"];
                    out.insert(
                        member.name.to_string(),
                        Value::Array(
                            found
                                .into_iter()
                                .map(|n| parse_xml(model, item_spec, n))
                                .collect(),
                        ),
                    );
                }
            }
            "map" if flattened => {
                let found = children_named(node, name);
                if !found.is_empty() {
                    out.insert(
                        member.name.to_string(),
                        Value::Object(parse_entries(model, member_shape, found)),
                    );
                }
            }
            _ => {
                if let Some(child) = children_named(node, name).into_iter().next() {
                    out.insert(
                        member.name.to_string(),
                        parse_xml(model, member.spec, child),
                    );
                }
            }
        }
    }
    Value::Object(out)
}

fn parse_entries(
    model: &Model,
    map_shape: &Value,
    entries: Vec<roxmltree::Node>,
) -> Map<String, Value> {
    let key_name = map_shape["key"]["locationName"].as_str().unwrap_or("key");
    let value_name = map_shape["value"]["locationName"]
        .as_str()
        .unwrap_or("value");
    let mut out = Map::new();
    for entry in entries {
        let key = children_named(entry, key_name)
            .into_iter()
            .next()
            .and_then(|n| n.text().map(str::to_string))
            .unwrap_or_default();
        if let Some(v) = children_named(entry, value_name).into_iter().next() {
            out.insert(key, parse_xml(model, &map_shape["value"], v));
        }
    }
    out
}

/// Convert an element into a value according to the member spec.
fn parse_xml(model: &Model, spec: &Value, node: roxmltree::Node) -> Value {
    let shape = model.shape(spec);
    match shape["type"].as_str().unwrap_or("string") {
        "structure" => {
            let members = model.members(shape);
            parse_structure(model, shape, &members, node)
        }
        "list" => {
            let item_spec = &shape["member"];
            Value::Array(
                node.children()
                    .filter(|c| c.is_element())
                    .map(|c| parse_xml(model, item_spec, c))
                    .collect(),
            )
        }
        "map" => {
            let entries: Vec<_> = node.children().filter(|c| c.is_element()).collect();
            Value::Object(parse_entries(model, shape, entries))
        }
        _ => typed_scalar(shape, node.text().unwrap_or("")),
    }
}
