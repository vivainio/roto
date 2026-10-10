//! Locally configured HTTP API v2 Lambda proxy routes.
use roto_core::{RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{QueryParams, base64};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

#[derive(Default)]
pub struct Gateway {
    routes: Vec<Route>,
}
struct Route {
    method: String,
    path: String,
    function: String,
    region: String,
}

impl Gateway {
    pub fn load(path: &Path) -> Result<Self, String> {
        let value: Value = serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let routes = value
            .get("routes")
            .and_then(Value::as_array)
            .ok_or("expected routes array")?;
        let mut result = Self::default();
        for route in routes {
            let text = |key: &str| {
                route
                    .get(key)
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| format!("missing route {key}"))
            };
            let method = text("method")?.to_owned();
            let path = text("path")?.to_owned();
            if !matches!(
                method.as_str(),
                "ANY" | "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
            ) || !path.starts_with('/')
            {
                return Err("route needs an HTTP method (or ANY) and an absolute path".into());
            }
            for (index, segment) in path.split('/').enumerate() {
                if segment.contains(['{', '}'])
                    && !(segment.starts_with('{')
                        && segment.ends_with('}')
                        && segment.len() > 2
                        && !segment[1..segment.len() - 1].contains(['{', '}'])
                        && (!segment.ends_with("+}") || index == path.split('/').count() - 1))
                {
                    return Err(format!("invalid route path: {path}"));
                }
            }
            if result
                .routes
                .iter()
                .any(|r| r.method == method && r.path == path)
            {
                return Err(format!("duplicate route: {method} {path}"));
            }
            result.routes.push(Route {
                method,
                path,
                function: text("function")?.to_owned(),
                region: route
                    .get("region")
                    .and_then(Value::as_str)
                    .unwrap_or("us-east-1")
                    .into(),
            });
        }
        Ok(result)
    }

    pub fn handle(
        &self,
        lambda: &dyn ServiceHandler,
        ctx: &RequestContext,
        req: &RawRequest,
        source_ip: &str,
        protocol: &str,
    ) -> RawResponse {
        let path = req.path.strip_prefix("/roto-http").unwrap_or(&req.path);
        let path = if path.is_empty() { "/" } else { path };
        let matched = self
            .routes
            .iter()
            .filter(|r| r.method == "ANY" || r.method == req.method)
            .filter_map(|r| match_path(&r.path, path).map(|params| (r, params)))
            .max_by_key(|(r, _)| {
                (
                    r.path.split('/').filter(|s| !s.starts_with('{')).count(),
                    !r.path.contains("+}"),
                    r.method != "ANY",
                )
            });
        let Some((route, params)) = matched else {
            return error(404, "Not Found");
        };
        let mut ctx = ctx.clone();
        ctx.region = route.region.clone();
        // A full ARN selects the function's account and region, like other Lambda sources.
        if route.function.starts_with("arn:") {
            let fields: Vec<_> = route.function.split(':').collect();
            if fields.len() >= 7 {
                ctx.region = fields[3].into();
                ctx.account_id = fields[4].into();
            }
        }
        let event = event(req, &ctx, route, path, params, source_ip, protocol);
        let invoke = RawRequest {
            method: "POST".into(),
            path: format!("/2015-03-31/functions/{}/invocations", route.function),
            query: String::new(),
            headers: vec![],
            body: serde_json::to_vec(&event).unwrap(),
        };
        match lambda.handle(&ctx, &invoke) {
            Ok(result)
                if result.status == 200
                    && !result
                        .headers
                        .iter()
                        .any(|(k, _)| k.eq_ignore_ascii_case("x-amz-function-error")) =>
            {
                serde_json::from_slice(&result.body)
                    .ok()
                    .and_then(response)
                    .unwrap_or_else(|| error(502, "Internal Server Error"))
            }
            _ => error(502, "Internal Server Error"),
        }
    }
}

fn match_path(pattern: &str, path: &str) -> Option<BTreeMap<String, String>> {
    let mut params = BTreeMap::new();
    let expected: Vec<_> = pattern.split('/').collect();
    let actual: Vec<_> = path.split('/').collect();
    for (i, segment) in expected.iter().enumerate() {
        if segment.starts_with('{') && segment.ends_with('}') {
            let name = &segment[1..segment.len() - 1];
            let value = if let Some(name) = name.strip_suffix('+') {
                let remaining = actual.get(i..)?.join("/");
                if remaining.is_empty() {
                    return None;
                }
                params.insert(
                    name.into(),
                    roto_protocol::restxml::percent_decode_path(&remaining),
                );
                return Some(params);
            } else {
                actual.get(i)?
            };
            if value.is_empty() {
                return None;
            }
            params.insert(
                name.into(),
                roto_protocol::restxml::percent_decode_path(value),
            );
        } else if actual.get(i) != Some(segment) {
            return None;
        }
    }
    (expected.len() == actual.len()).then_some(params)
}

fn join(map: &mut BTreeMap<String, String>, key: String, value: String) {
    map.entry(key)
        .and_modify(|v| {
            v.push(',');
            v.push_str(&value);
        })
        .or_insert(value);
}

fn event(
    req: &RawRequest,
    ctx: &RequestContext,
    route: &Route,
    path: &str,
    params: BTreeMap<String, String>,
    source_ip: &str,
    protocol: &str,
) -> Value {
    let mut headers = BTreeMap::new();
    let mut cookies = Vec::new();
    for (key, value) in &req.headers {
        if key.eq_ignore_ascii_case("cookie") {
            cookies.extend(value.split(';').map(|s| s.trim().to_owned()));
        } else {
            join(&mut headers, key.to_ascii_lowercase(), value.clone());
        }
    }
    let mut query = BTreeMap::new();
    for pair in req.query.split('&').filter(|p| !p.is_empty()) {
        let decoded = QueryParams::parse(pair);
        let key = pair.split('=').next().unwrap();
        // Decode keys using the same query parser as values.
        let key = QueryParams::parse(&format!("key={key}"))
            .get("key")
            .unwrap_or("")
            .to_owned();
        join(
            &mut query,
            key.clone(),
            decoded.get(&key).unwrap_or("").into(),
        );
    }
    let content_type = req
        .header("content-type")
        .unwrap_or("text/plain")
        .split(';')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let text = content_type.starts_with("text/")
        || content_type == "application/json"
        || content_type.ends_with("+json")
        || content_type == "application/xml"
        || content_type.ends_with("+xml")
        || content_type == "application/x-www-form-urlencoded";
    let (body, encoded) = match std::str::from_utf8(&req.body) {
        Ok(s) if text => (s.to_owned(), false),
        _ => (base64::encode(&req.body), true),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let date = roto_protocol::timestamp::Timestamp(now.as_secs() as i64).to_http_date();
    let fields: Vec<_> = date.split(' ').collect();
    let time = format!(
        "{}/{}/{}:{} +0000",
        fields[1], fields[2], fields[3], fields[4]
    );
    let host = req.header("host").unwrap_or("localhost");
    let mut event = json!({"version":"2.0", "routeKey":format!("{} {}", route.method, route.path), "rawPath":path, "rawQueryString":req.query, "headers":headers, "isBase64Encoded":encoded,
        "requestContext":{"accountId":ctx.account_id,"apiId":"local", "domainName":host,"domainPrefix":"local","requestId":ctx.request_id,"routeKey":format!("{} {}", route.method, route.path),"stage":"$default","time":time,"timeEpoch":now.as_millis() as u64,
        "http":{"method":req.method,"path":path,"protocol":protocol,"sourceIp":source_ip,"userAgent":req.header("user-agent").unwrap_or("")}}});
    if !req.body.is_empty() {
        event["body"] = body.into();
    }
    if !params.is_empty() {
        event["pathParameters"] = json!(params);
    }
    if !query.is_empty() {
        event["queryStringParameters"] = json!(query);
    }
    if !cookies.is_empty() {
        event["cookies"] = json!(cookies);
    }
    event
}

fn error(status: u16, message: &str) -> RawResponse {
    RawResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&json!({"message":message})).unwrap(),
    }
}

fn response(value: Value) -> Option<RawResponse> {
    if value.get("statusCode").is_none() {
        return Some(RawResponse {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body: match value.as_str() {
                Some(text) => text.as_bytes().to_vec(),
                None => serde_json::to_vec(&value).ok()?,
            },
        });
    }
    let status = u16::try_from(value["statusCode"].as_u64()?).ok()?;
    if !(100..=599).contains(&status) {
        return None;
    }
    let mut headers = Vec::new();
    if let Some(map) = value.get("headers") {
        for (key, value) in map.as_object()? {
            let value = value.as_str()?;
            axum::http::HeaderName::try_from(key.as_str()).ok()?;
            axum::http::HeaderValue::try_from(value).ok()?;
            headers.push((key.clone(), value.into()));
        }
    }
    if let Some(cookies) = value.get("cookies") {
        for cookie in cookies.as_array()? {
            let cookie = cookie.as_str()?;
            axum::http::HeaderValue::try_from(cookie).ok()?;
            headers.push(("set-cookie".into(), cookie.into()));
        }
    }
    let body = value.get("body").map(Value::as_str).unwrap_or(Some(""))?;
    let encoded = value
        .get("isBase64Encoded")
        .map(Value::as_bool)
        .unwrap_or(Some(false))?;
    let body = if encoded {
        base64::decode(body)?
    } else {
        body.as_bytes().to_vec()
    };
    Some(RawResponse {
        status,
        headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn payload_and_binary_round_trip() {
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "request".into(),
            base_url: "http://localhost".into(),
        };
        let route = Route {
            method: "POST".into(),
            path: "/orders/{id}".into(),
            function: "echo".into(),
            region: ctx.region.clone(),
        };
        let req = RawRequest {
            method: "POST".into(),
            path: "/roto-http/orders/a%20b".into(),
            query: "tag=one&tag=two&a%2Bb=x+y".into(),
            headers: vec![
                ("Content-Type".into(), "application/octet-stream".into()),
                ("X-Test".into(), "one".into()),
                ("x-test".into(), "two".into()),
                ("Cookie".into(), "a=1; b=2".into()),
            ],
            body: vec![0, 255, 128],
        };
        let event = event(
            &req,
            &ctx,
            &route,
            "/orders/a%20b",
            match_path(&route.path, "/orders/a%20b").unwrap(),
            "192.0.2.1",
            "HTTP/1.1",
        );
        assert_eq!(event["version"], "2.0");
        assert_eq!(event["pathParameters"]["id"], "a b");
        assert_eq!(event["queryStringParameters"]["tag"], "one,two");
        assert_eq!(event["queryStringParameters"]["a+b"], "x y");
        assert_eq!(event["headers"]["x-test"], "one,two");
        assert_eq!(event["cookies"], json!(["a=1", "b=2"]));
        assert_eq!(event["requestContext"]["http"]["sourceIp"], "192.0.2.1");
        assert_eq!(event["isBase64Encoded"], true);
        let response = response(json!({"statusCode":201,"body":event["body"],"isBase64Encoded":true,"cookies":["a=1","b=2"]})).unwrap();
        assert_eq!(response.body, req.body);
        assert_eq!(
            response
                .headers
                .iter()
                .filter(|(k, _)| k == "set-cookie")
                .count(),
            2
        );
        assert!(
            super::response(json!({"statusCode":200,"body":{},"isBase64Encoded":false})).is_none()
        );
        assert!(
            super::response(json!({"statusCode":200,"body":"!","isBase64Encoded":true})).is_none()
        );
        assert_eq!(super::response(json!("hello")).unwrap().body, b"hello");
    }
    #[test]
    fn path_matching() {
        assert_eq!(match_path("/{proxy+}", "/a/b").unwrap()["proxy"], "a/b");
        assert!(match_path("/{proxy+}", "/").is_none());
        assert!(match_path("/orders/{id}", "/orders/a/b").is_none());
        assert!(match_path("/orders", "/orders/").is_none());
        assert!(match_path("/", "/").is_some());
    }
    #[test]
    fn routes_invoke_real_lambda_and_hide_execution_errors() {
        let store = roto_core::store::Store::ephemeral();
        let lambda = roto_svc_lambda::LambdaHandler::new(&store, Default::default()).unwrap();
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "request".into(),
            base_url: "http://localhost".into(),
        };
        for (name, command) in [
            ("echo", "cat"),
            ("fail", "exit 1"),
            (
                "proxy",
                "cat >/dev/null; printf '%s' '{\"statusCode\":201,\"body\":\"created\",\"cookies\":[\"a=1\",\"b=2\"]}'",
            ),
        ] {
            lambda.handle(&ctx, &RawRequest { method:"POST".into(), path:"/2015-03-31/functions".into(), body:serde_json::to_vec(&json!({"FunctionName":name,"Role":"arn:aws:iam::123456789012:role/test","Runtime":"provided.al2023","Handler":"external","Code":{"ZipFile":""}})).unwrap(), ..Default::default() }).unwrap();
            lambda
                .0
                .bind_executor(
                    &ctx,
                    name,
                    roto_svc_lambda::Executor::Command {
                        command: vec!["sh".into(), "-c".into(), command.into()],
                        cwd: None,
                        env: Default::default(),
                    },
                )
                .unwrap();
        }
        let gateway = Gateway {
            routes: vec![
                Route {
                    method: "ANY".into(),
                    path: "/{proxy+}".into(),
                    function: "echo".into(),
                    region: ctx.region.clone(),
                },
                Route {
                    method: "POST".into(),
                    path: "/created".into(),
                    function: "proxy".into(),
                    region: ctx.region.clone(),
                },
                Route {
                    method: "GET".into(),
                    path: "/fail".into(),
                    function: "fail".into(),
                    region: ctx.region.clone(),
                },
            ],
        };
        let mut request = RawRequest {
            method: "GET".into(),
            path: "/roto-http/orders/42".into(),
            ..Default::default()
        };
        let result = gateway.handle(&lambda, &ctx, &request, "127.0.0.1", "HTTP/1.1");
        assert_eq!(result.status, 200);
        let event: Value = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(event["pathParameters"]["proxy"], "orders/42");
        request.path = "/roto-http/fail".into();
        assert_eq!(
            gateway
                .handle(&lambda, &ctx, &request, "127.0.0.1", "HTTP/1.1")
                .status,
            502
        );
        request.method = "POST".into();
        request.path = "/roto-http/created".into();
        let result = gateway.handle(&lambda, &ctx, &request, "127.0.0.1", "HTTP/1.1");
        assert_eq!(result.status, 201);
        assert_eq!(result.body, b"created");
        assert_eq!(
            crate::into_response(result)
                .headers()
                .get_all("set-cookie")
                .iter()
                .count(),
            2
        );
        request.path = "/roto-http/".into();
        assert_eq!(
            gateway
                .handle(&lambda, &ctx, &request, "127.0.0.1", "HTTP/1.1")
                .status,
            404
        );
    }
}
