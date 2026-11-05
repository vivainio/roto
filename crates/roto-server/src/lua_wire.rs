//! Model-backed codecs for optional Lua REST/JSON and Query services.
use roto_core::{AwsError, RawRequest, RawResponse};
use roto_protocol::{
    QueryParams, Timestamp, XmlWriter, json_error, json_response, query_error, query_response,
};
use serde_json::{Map, Value, json};

pub struct Wire {
    pub model: Value,
    pub protocol: String,
}
fn ec2_response(ns: &str, op: &str, id: &str, inner: &str) -> RawResponse {
    let body =
        format!("<{op}Response xmlns=\"{ns}\"><requestId>{id}</requestId>{inner}</{op}Response>");
    let mut response = query_response(ns, op, None, id, "");
    response.body = body.into_bytes();
    response
}
fn ec2_error(e: &AwsError, id: &str) -> RawResponse {
    let mut w = XmlWriter::new();
    w.element("Code", &e.code);
    w.element("Message", &e.message);
    let body = format!(
        "<Response><Errors><Error>{}</Error></Errors><RequestID>{id}</RequestID></Response>",
        w.finish()
    );
    let mut response = query_error("", e, id);
    response.body = body.into_bytes();
    response
}
fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "InvalidParameterValue", message)
}
impl Wire {
    /// Query and EC2 requests are form-encoded `Action` calls; only the naming rules differ.
    fn form(&self) -> bool {
        matches!(self.protocol.as_str(), "query" | "ec2")
    }
    fn ec2(&self) -> bool {
        self.protocol == "ec2"
    }
    /// Wire name of a request member. EC2 capitalizes `locationName` and prefers `queryName`.
    fn request_name(&self, member: &Value, key: &str) -> String {
        if self.ec2() {
            if let Some(name) = member["queryName"].as_str() {
                return name.into();
            }
            if let Some(name) = member["locationName"].as_str() {
                let mut chars = name.chars();
                return chars
                    .next()
                    .map(|c| c.to_uppercase().chain(chars).collect())
                    .unwrap_or_default();
            }
            return key.into();
        }
        member["locationName"].as_str().unwrap_or(key).into()
    }
    fn shape<'a>(&'a self, reference: &Value) -> &'a Value {
        &self.model["shapes"][reference["shape"].as_str().unwrap_or("")]
    }
    pub fn operation_name(&self, req: &RawRequest) -> Option<String> {
        if self.form() {
            self.params(req).get("Action").map(str::to_owned)
        } else {
            self.route(req).map(|(name, _)| name)
        }
    }
    pub fn claims(&self, req: &RawRequest) -> bool {
        if self.form() {
            // Version disambiguates RDS from native IAM, STS, SNS, etc.
            let p = self.params(req);
            p.get("Version") == self.model["metadata"]["apiVersion"].as_str()
                && p.get("Action")
                    .is_some_and(|op| !self.model["operations"][op].is_null())
        } else {
            self.route(req).is_some()
        }
    }
    fn params(&self, req: &RawRequest) -> QueryParams {
        let mut p = QueryParams::parse(&req.query);
        p.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
        p
    }
    fn route(&self, req: &RawRequest) -> Option<(String, Map<String, Value>)> {
        let segments: Vec<_> = req.path.split('/').collect();
        for (name, op) in self.model["operations"].as_object()? {
            if op["http"]["method"].as_str()? != req.method {
                continue;
            }
            let template: Vec<_> = op["http"]["requestUri"].as_str()?.split('/').collect();
            if template.len() != segments.len() {
                continue;
            }
            let mut labels = Map::new();
            let mut matched = true;
            for (want, got) in template.iter().zip(&segments) {
                if let Some(label) = want.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                    labels.insert(
                        label.into(),
                        Value::String(roto_protocol::restxml::percent_decode_path(got)),
                    );
                } else if want != got {
                    matched = false;
                    break;
                }
            }
            if matched {
                return Some((name.clone(), labels));
            }
        }
        None
    }
    pub fn decode(&self, req: &RawRequest) -> Result<(String, Value), AwsError> {
        self.decode_inner(req).map_err(|mut error| {
            if self.protocol == "rest-json"
                && matches!(
                    error.code.as_str(),
                    "MissingParameter" | "InvalidParameterValue"
                )
            {
                error.code = "ValidationException".into();
            }
            error
        })
    }
    fn decode_inner(&self, req: &RawRequest) -> Result<(String, Value), AwsError> {
        if self.form() {
            let p = self.params(req);
            let op = p
                .get("Action")
                .ok_or_else(|| invalid("Action is required"))?;
            if self.model["operations"][op].is_null() {
                return Err(AwsError::sender(400, "InvalidAction", "Unknown action"));
            }
            let reference = &self.model["operations"][op]["input"];
            let input = self.read_query(reference, &p, "")?.unwrap_or(json!({}));
            self.validate(reference, &input)?;
            return Ok((op.into(), input));
        }
        let (op, labels) = self
            .route(req)
            .ok_or_else(|| AwsError::sender(404, "UnknownOperationException", "Unknown route"))?;
        let mut input = if req.body.is_empty() {
            Map::new()
        } else {
            serde_json::from_slice::<Value>(&req.body)
                .map_err(|_| AwsError::sender(400, "ValidationException", "Invalid JSON body"))?
                .as_object()
                .cloned()
                .ok_or_else(|| invalid("Expected an object"))?
        };
        let reference = &self.model["operations"][&op]["input"];
        let p = QueryParams::parse(&req.query);
        if let Some(members) = self.shape(reference)["members"].as_object() {
            for (name, member) in members {
                let wire = member["locationName"].as_str().unwrap_or(name);
                let v = match member["location"].as_str() {
                    Some("uri") => labels.get(wire).cloned(),
                    Some("querystring") => {
                        p.get(wire).map(|v| self.scalar(member, v)).transpose()?
                    }
                    Some("header") => req
                        .header(&wire.to_ascii_lowercase())
                        .map(|v| self.scalar(member, v))
                        .transpose()?,
                    _ => None,
                };
                if let Some(v) = v {
                    input.insert(name.clone(), v);
                }
            }
        }
        let input = Value::Object(input);
        self.validate(reference, &input)?;
        Ok((op, input))
    }
    fn scalar(&self, reference: &Value, text: &str) -> Result<Value, AwsError> {
        Ok(match self.shape(reference)["type"].as_str() {
            Some("boolean") => match text {
                "true" => json!(true),
                "false" => json!(false),
                _ => return Err(invalid("Invalid boolean")),
            },
            Some("integer" | "long") => json!(
                text.parse::<i64>()
                    .map_err(|_| invalid("Invalid integer"))?
            ),
            Some("float" | "double") => {
                json!(text.parse::<f64>().map_err(|_| invalid("Invalid number"))?)
            }
            Some("timestamp") => json!(
                Timestamp::parse(text)
                    .ok_or_else(|| invalid("Invalid timestamp"))?
                    .0
            ),
            _ => json!(text),
        })
    }
    fn read_query(
        &self,
        reference: &Value,
        p: &QueryParams,
        key: &str,
    ) -> Result<Option<Value>, AwsError> {
        let shape = self.shape(reference);
        if !key.is_empty() && !p.has_prefix(key) {
            return Ok(None);
        }
        match shape["type"].as_str() {
            Some("structure") => {
                let mut out = Map::new();
                if let Some(members) = shape["members"].as_object() {
                    for (name, member) in members {
                        let wire = self.request_name(member, name);
                        let path = if key.is_empty() {
                            wire
                        } else {
                            format!("{key}.{wire}")
                        };
                        if let Some(v) = self.read_query(member, p, &path)? {
                            out.insert(name.clone(), v);
                        }
                    }
                }
                Ok(Some(Value::Object(out)))
            }
            Some("list") => {
                let item = shape["member"]["locationName"].as_str().unwrap_or("member");
                let base = if self.ec2() || reference["flattened"].as_bool().unwrap_or(false) {
                    key.into()
                } else {
                    format!("{key}.{item}")
                };
                let mut out = Vec::new();
                for i in 1..=10000 {
                    match self.read_query(&shape["member"], p, &format!("{base}.{i}"))? {
                        Some(v) => out.push(v),
                        None => break,
                    }
                }
                Ok(Some(Value::Array(out)))
            }
            Some("map") => Err(invalid("Query maps are unsupported")),
            _ => p.get(key).map(|s| self.scalar(reference, s)).transpose(),
        }
    }
    fn validate(&self, reference: &Value, value: &Value) -> Result<(), AwsError> {
        let shape = self.shape(reference);
        let valid = match shape["type"].as_str() {
            Some("structure" | "map") => value.is_object(),
            Some("list") => value.is_array(),
            Some("boolean") => value.is_boolean(),
            Some("integer" | "long") => value.is_i64(),
            Some("double" | "float" | "timestamp") => value.is_number(),
            Some("string" | "blob") => value.is_string(),
            _ => true,
        };
        if !valid {
            return Err(invalid("Incorrect parameter type"));
        }
        if let Some(required) = shape["required"].as_array() {
            for key in required {
                let key = key.as_str().unwrap();
                if value.get(key).is_none() {
                    return Err(AwsError::missing_parameter(key));
                }
            }
        }
        if let Some(members) = shape["members"].as_object() {
            for (key, member) in members {
                if let Some(v) = value.get(key) {
                    self.validate(member, v)?;
                }
            }
        }
        if let Some(values) = value.as_array() {
            for v in values {
                self.validate(&shape["member"], v)?;
            }
        }
        Ok(())
    }
    fn write(&self, w: &mut XmlWriter, reference: &Value, name: &str, value: &Value) {
        let shape = self.shape(reference);
        if shape["type"].as_str() == Some("list") && reference["flattened"].as_bool() == Some(true)
        {
            // A flattened list repeats the member element instead of wrapping its items.
            let item = shape["member"]["locationName"].as_str().unwrap_or(name);
            for v in value.as_array().into_iter().flatten() {
                self.write(w, &shape["member"], item, v);
            }
            return;
        }
        w.open(name);
        match shape["type"].as_str() {
            Some("structure") => {
                if let (Some(members), Some(values)) =
                    (shape["members"].as_object(), value.as_object())
                {
                    for (key, v) in values {
                        if let Some(member) = members.get(key) {
                            self.write(
                                w,
                                member,
                                member["locationName"].as_str().unwrap_or(key),
                                v,
                            );
                        }
                    }
                }
            }
            Some("list") => {
                if let Some(values) = value.as_array() {
                    for v in values {
                        self.write(
                            w,
                            &shape["member"],
                            shape["member"]["locationName"]
                                .as_str()
                                .unwrap_or(if self.ec2() { "item" } else { "member" }),
                            v,
                        );
                    }
                }
            }
            Some("timestamp") => w.text(&Timestamp(value.as_i64().unwrap_or(0)).to_iso8601()),
            _ => w.text(
                &value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            ),
        }
        w.close(name);
    }
    pub fn response(&self, op: &str, result: Result<Value, AwsError>, id: &str) -> RawResponse {
        let ns = self.model["metadata"]["xmlNamespace"]
            .as_str()
            .unwrap_or("");
        if self.form() {
            return match result {
                Err(e) if self.ec2() => ec2_error(&e, id),
                Err(e) => query_error(ns, &e, id),
                Ok(value) => {
                    let reference = &self.model["operations"][op]["output"];
                    let wrapper = self.shape(reference)["resultWrapper"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("{op}Result"));
                    let mut w = XmlWriter::new();
                    if let (Some(members), Some(values)) = (
                        self.shape(reference)["members"].as_object(),
                        value.as_object(),
                    ) {
                        for (key, v) in values {
                            if let Some(member) = members.get(key) {
                                self.write(
                                    &mut w,
                                    member,
                                    member["locationName"].as_str().unwrap_or(key),
                                    v,
                                );
                            }
                        }
                    }
                    if self.ec2() {
                        return ec2_response(ns, op, id, &w.finish());
                    }
                    query_response(
                        ns,
                        op,
                        (!reference.is_null()).then_some(wrapper.as_str()),
                        id,
                        &w.finish(),
                    )
                }
            };
        }
        let mut response = match result {
            Ok(v) => json_response("1.1", id, &v),
            Err(e) => {
                let mut r = json_error("1.1", &e, id, false);
                r.headers.push(("x-amzn-errortype".into(), e.code));
                r
            }
        };
        response.headers.retain(|(k, _)| k != "content-type");
        response
            .headers
            .push(("content-type".into(), "application/json".into()));
        response
    }
}
