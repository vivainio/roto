//! Shared local WebSocket connections for IoT topic subscriptions and API Gateway callbacks.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use roto_core::{AwsError, RawRequest, RequestContext};
use roto_protocol::{QueryParams, restxml::percent_decode_path};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::App;

pub mod mqtt;

const OUTBOUND_CAPACITY: usize = 128;
const API_GATEWAY_MAX_MESSAGE_SIZE: usize = 128 * 1024;

#[derive(Clone, Debug)]
enum OutboundFrame {
    Text(String),
    Binary(Vec<u8>),
}

struct Client {
    outbound: mpsc::Sender<OutboundFrame>,
    subscriptions: HashSet<String>,
    api_gateway: Option<(String, String)>,
    mqtt: bool,
}

#[derive(Default)]
struct StateData {
    clients: HashMap<String, Client>,
}

/// Process-wide connection registry shared by IoT publish and API Gateway callbacks.
#[derive(Clone, Default)]
pub struct Hub(Arc<Mutex<StateData>>);

#[derive(Debug)]
pub enum PostError {
    Gone,
    TooLarge,
    LimitExceeded,
}

impl Hub {
    pub fn publish_iot(&self, request: &RawRequest) -> Result<(), AwsError> {
        let encoded_topic = request
            .path
            .strip_prefix("/topics/")
            .ok_or_else(|| invalid("Publish requires a topic"))?;
        let topic = percent_decode_path(encoded_topic);
        validate_topic(&topic)?;

        let query = QueryParams::parse(&request.query);
        let qos = match query.get("qos").unwrap_or("0") {
            "0" => 0,
            "1" => 1,
            _ => return Err(invalid("qos must be 0 or 1")),
        };
        match query.get("retain").unwrap_or("false") {
            "false" => {}
            "true" => return Err(invalid("Retained messages are not supported")),
            _ => return Err(invalid("retain must be true or false")),
        }

        let payload = match std::str::from_utf8(&request.body) {
            Ok(text) => json!({"encoding":"utf-8", "data":text}),
            Err(_) => json!({
                "encoding":"base64",
                "data":roto_protocol::base64::encode(&request.body),
            }),
        };
        let event = json!({
            "type":"message",
            "topic":topic,
            "qos":qos,
            "payload":payload,
        })
        .to_string();

        let mqtt_packet = roto_mqtt::publish(&topic, &request.body)
            .map_err(|_| invalid("Publish exceeds the local MQTT packet size limit"))?;
        let mut state = self.0.lock().unwrap();
        state.clients.retain(|_, client| {
            let matches = client
                .subscriptions
                .iter()
                .any(|filter| topic_matches(filter, &topic));
            if matches {
                !matches!(
                    client.outbound.try_send(if client.mqtt {
                        OutboundFrame::Binary(mqtt_packet.clone())
                    } else {
                        OutboundFrame::Text(event.clone())
                    }),
                    Err(mpsc::error::TrySendError::Closed(_))
                )
            } else {
                true
            }
        });
        Ok(())
    }

    pub fn post_to_connection(
        &self,
        id: &str,
        data: &[u8],
        api_stage: Option<(&str, &str)>,
    ) -> Result<(), PostError> {
        if data.len() > API_GATEWAY_MAX_MESSAGE_SIZE {
            return Err(PostError::TooLarge);
        }
        let frame = match std::str::from_utf8(data) {
            Ok(text) => OutboundFrame::Text(text.to_owned()),
            Err(_) => OutboundFrame::Binary(data.to_vec()),
        };
        let mut state = self.0.lock().unwrap();
        let Some(client) = state.clients.get(id) else {
            return Err(PostError::Gone);
        };
        if let Some((api_id, stage_name)) = api_stage
            && client
                .api_gateway
                .as_ref()
                .is_none_or(|(client_api, client_stage)| {
                    client_api != api_id || client_stage != stage_name
                })
        {
            return Err(PostError::Gone);
        }
        match client.outbound.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(PostError::LimitExceeded),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                state.clients.remove(id);
                Err(PostError::Gone)
            }
        }
    }
}

pub async fn websocket(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| serve(socket, app.websockets.clone()))
        .into_response()
}

pub async fn apigateway_default_websocket(
    State(app): State<Arc<App>>,
    Path(api_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
    req: axum::http::Request<axum::body::Body>,
) -> Response {
    let ws = match ws {
        Ok(ws) => ws,
        Err(_) => return crate::handle(State(app), req).await,
    };
    apigateway_upgrade(app, api_id, "$default".into(), query, headers, ws).await
}

pub async fn apigateway_websocket(
    State(app): State<Arc<App>>,
    Path((api_id, stage_name)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
    req: axum::http::Request<axum::body::Body>,
) -> Response {
    let ws = match ws {
        Ok(ws) => ws,
        Err(_) => return crate::handle(State(app), req).await,
    };
    apigateway_upgrade(app, api_id, stage_name, query, headers, ws).await
}

async fn apigateway_upgrade(
    app: Arc<App>,
    api_id: String,
    stage_name: String,
    query: HashMap<String, String>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(api) = app.api_gateway_v2.runtime_api(&api_id, &stage_name) else {
        return (StatusCode::NOT_FOUND, "Unknown WebSocket API or stage").into_response();
    };
    let connection_id = roto_core::ids::request_id();
    let connected_at = epoch_millis();
    let event_headers = headers_json(&headers);
    let query_string = query_string(&query);
    let host = headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost")
        .to_owned();
    let source_ip = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .unwrap_or("127.0.0.1")
        .trim()
        .to_owned();

    if let Some(integration_uri) = integration_uri(&api, "$connect") {
        let event = api_event(
            &api,
            "$connect",
            "CONNECT",
            &connection_id,
            &host,
            &source_ip,
            &event_headers,
            &query,
            &query_string,
            connected_at,
            None,
        );
        if let Err(reason) = invoke_lambda(&app, &integration_uri, event).await {
            tracing::warn!(%reason, "API Gateway $connect integration failed");
            return (
                StatusCode::BAD_GATEWAY,
                "WebSocket connect integration failed",
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| {
        serve_api_gateway(
            socket,
            app,
            api,
            connection_id,
            host,
            source_ip,
            event_headers,
            query,
            query_string,
            connected_at,
        )
    })
    .into_response()
}

#[allow(clippy::too_many_arguments)]
async fn serve_api_gateway(
    mut socket: WebSocket,
    app: Arc<App>,
    api: crate::apigateway_v2::RuntimeApi,
    connection_id: String,
    host: String,
    source_ip: String,
    headers: Value,
    query: HashMap<String, String>,
    query_string: String,
    connected_at: u64,
) {
    let (outbound, mut receiver) = mpsc::channel(OUTBOUND_CAPACITY);
    app.websockets.0.lock().unwrap().clients.insert(
        connection_id.clone(),
        Client {
            outbound,
            subscriptions: HashSet::new(),
            api_gateway: Some((api.api_id.clone(), api.stage_name.clone())),
            mqtt: false,
        },
    );
    // The local endpoint exposes the connection ID so a local client can call the
    // Management API without a separate Lambda or inspection endpoint.
    if send_json(
        &mut socket,
        json!({"type":"ready", "connectionId":connection_id}),
    )
    .await
    .is_err()
    {
        app.websockets
            .0
            .lock()
            .unwrap()
            .clients
            .remove(&connection_id);
        return;
    }

    loop {
        tokio::select! {
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { break; };
                match frame {
                    Message::Text(text) => {
                        let raw = text.as_str().as_bytes();
                        if let Some(route_key) = selected_route(&api, raw)
                            && let Some(integration_uri) = integration_uri(&api, &route_key) {
                                let event = api_event(
                                    &api, &route_key, "MESSAGE", &connection_id, &host,
                                    &source_ip, &headers, &query, &query_string, connected_at, Some(raw),
                                );
                                if api.route_responses.contains(&route_key) {
                                    if let Ok(payload) = invoke_lambda(&app, &integration_uri, event).await {
                                        let response = match String::from_utf8(payload) {
                                            Ok(text) => Message::Text(text.into()),
                                            Err(error) => Message::Binary(error.into_bytes().into()),
                                        };
                                        if socket.send(response).await.is_err() { break; }
                                    }
                                } else {
                                    let _ = invoke_lambda(&app, &integration_uri, event).await;
                                }
                            }
                    }
                    Message::Binary(bytes) => {
                        if let Some(integration_id) = api.routes.get("$default")
                            && let Some(integration_uri) = api.integrations.get(integration_id) {
                                let event = api_event(
                                    &api, "$default", "MESSAGE", &connection_id, &host,
                                    &source_ip, &headers, &query, &query_string, connected_at, Some(&bytes),
                                );
                                if api.route_responses.contains("$default") {
                                    if let Ok(payload) = invoke_lambda(&app, integration_uri, event).await {
                                        let response = match String::from_utf8(payload) {
                                            Ok(text) => Message::Text(text.into()),
                                            Err(error) => Message::Binary(error.into_bytes().into()),
                                        };
                                        if socket.send(response).await.is_err() { break; }
                                    }
                                } else {
                                    let _ = invoke_lambda(&app, integration_uri, event).await;
                                }
                            }
                    }
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() { break; }
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                }
            }
            outbound = receiver.recv() => {
                let Some(outbound) = outbound else { break; };
                let frame = match outbound {
                    OutboundFrame::Text(text) => Message::Text(text.into()),
                    OutboundFrame::Binary(data) => Message::Binary(data.into()),
                };
                if socket.send(frame).await.is_err() { break; }
            }
        }
    }
    app.websockets
        .0
        .lock()
        .unwrap()
        .clients
        .remove(&connection_id);
    if let Some(integration_uri) = integration_uri(&api, "$disconnect") {
        let event = api_event(
            &api,
            "$disconnect",
            "DISCONNECT",
            &connection_id,
            &host,
            &source_ip,
            &headers,
            &query,
            &query_string,
            connected_at,
            None,
        );
        let _ = invoke_lambda(&app, &integration_uri, event).await;
    }
}

fn selected_route(api: &crate::apigateway_v2::RuntimeApi, body: &[u8]) -> Option<String> {
    let expression = api
        .route_selection_expression
        .strip_prefix("$request.body.");
    let selected = expression
        .and_then(|expression| {
            serde_json::from_slice::<Value>(body)
                .ok()
                .map(|value| (expression, value))
        })
        .and_then(|(expression, value)| {
            expression
                .split('.')
                .try_fold(&value, |value, key| value.get(key))
                .map(|selected| {
                    selected
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| selected.to_string())
                })
        });
    selected
        .filter(|key| api.routes.contains_key(key))
        .or_else(|| {
            api.routes
                .contains_key("$default")
                .then(|| "$default".into())
        })
}

fn integration_uri(api: &crate::apigateway_v2::RuntimeApi, route_key: &str) -> Option<String> {
    api.routes
        .get(route_key)
        .and_then(|id| api.integrations.get(id))
        .cloned()
}

#[allow(clippy::too_many_arguments)]
fn api_event(
    api: &crate::apigateway_v2::RuntimeApi,
    route_key: &str,
    event_type: &str,
    connection_id: &str,
    host: &str,
    source_ip: &str,
    headers: &Value,
    query: &HashMap<String, String>,
    raw_query: &str,
    connected_at: u64,
    body: Option<&[u8]>,
) -> Value {
    let now = epoch_millis();
    let date = roto_protocol::timestamp::Timestamp((now / 1000) as i64).to_http_date();
    let date_parts: Vec<_> = date.split(' ').collect();
    let request_time = format!(
        "{}/{}/{}:{} +0000",
        date_parts[1], date_parts[2], date_parts[3], date_parts[4]
    );
    let request_id = roto_core::ids::request_id();
    let mut context = json!({
        "routeKey":route_key,
        "eventType":event_type,
        "extendedRequestId":request_id,
        "requestTimeEpoch":now,
        "messageDirection":"IN",
        "stage":api.stage_name,
        "connectedAt":connected_at,
        "requestTime":request_time,
        "apiId":api.api_id,
        "domainName":host,
        "connectionId":connection_id,
        "requestId":request_id,
        "identity":{"sourceIp":source_ip,"userAgent":headers.get("user-agent").and_then(Value::as_str).unwrap_or("")}
    });
    if event_type == "MESSAGE" {
        context["messageId"] = json!(roto_core::ids::request_id());
    }
    let mut event = json!({
        "headers":headers,
        "queryStringParameters":query,
        "requestContext":context,
        "isBase64Encoded":false
    });
    if !raw_query.is_empty() {
        event["multiValueQueryStringParameters"] = json!(
            query
                .iter()
                .map(|(key, value)| (key.clone(), vec![value.clone()]))
                .collect::<HashMap<_, _>>()
        );
    }
    if let Some(body) = body {
        match std::str::from_utf8(body) {
            Ok(text) => event["body"] = json!(text),
            Err(_) => {
                event["body"] = json!(roto_protocol::base64::encode(body));
                event["isBase64Encoded"] = json!(true);
            }
        }
    }
    event
}

async fn invoke_lambda(
    app: &Arc<App>,
    integration_uri: &str,
    event: Value,
) -> Result<Vec<u8>, String> {
    let function = integration_uri
        .split("functions/")
        .nth(1)
        .and_then(|suffix| suffix.split("/invocations").next())
        .or_else(|| {
            integration_uri
                .starts_with("arn:aws:lambda:")
                .then_some(integration_uri)
        })
        .ok_or_else(|| "unsupported API Gateway integration URI".to_owned())?;
    let fields: Vec<_> = function.split(':').collect();
    let mut context = RequestContext {
        account_id: app.account_id.clone(),
        region: "us-east-1".into(),
        access_key: None,
        request_id: roto_core::ids::request_id(),
        base_url: "http://localhost".into(),
    };
    if fields.len() >= 7 && fields[0] == "arn" && fields[2] == "lambda" {
        context.region = fields[3].into();
        context.account_id = fields[4].into();
    }
    let request = RawRequest {
        method: "POST".into(),
        path: format!("/2015-03-31/functions/{function}/invocations"),
        query: "LogType=Tail".into(),
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&event).map_err(|error| error.to_string())?,
    };
    let lambda = app
        .services
        .get("lambda")
        .cloned()
        .ok_or_else(|| "Lambda service is unavailable".to_owned())?;
    let response = tokio::task::spawn_blocking(move || lambda.handle(&context, &request))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    if response.status >= 400
        || response
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-amz-function-error"))
    {
        return Err("Lambda integration failed".into());
    }
    Ok(response.body)
}

fn headers_json(headers: &HeaderMap) -> Value {
    let mut result = serde_json::Map::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            result
                .entry(name.as_str().to_ascii_lowercase())
                .and_modify(|existing| {
                    if let Some(previous) = existing.as_str() {
                        *existing = json!(format!("{previous},{value}"));
                    }
                })
                .or_insert_with(|| json!(value));
        }
    }
    Value::Object(result)
}

fn query_string(query: &HashMap<String, String>) -> String {
    query
        .iter()
        .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn url_encode(value: &str) -> String {
    value.bytes().fold(String::new(), |mut result, byte| {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            result.push(char::from(byte));
        } else {
            result.push_str(&format!("%{byte:02X}"));
        }
        result
    })
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn serve(mut socket: WebSocket, hub: Hub) {
    let id = roto_core::ids::request_id();
    let (outbound, mut receiver) = mpsc::channel(OUTBOUND_CAPACITY);
    hub.0.lock().unwrap().clients.insert(
        id.clone(),
        Client {
            outbound,
            subscriptions: HashSet::new(),
            api_gateway: None,
            mqtt: false,
        },
    );
    if send_json(&mut socket, json!({"type":"ready", "connectionId":id}))
        .await
        .is_err()
    {
        hub.0.lock().unwrap().clients.remove(&id);
        return;
    }

    loop {
        tokio::select! {
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { break; };
                match frame {
                    Message::Text(text) => {
                        let response = command(text.as_str(), &hub, &id);
                        if send_json(&mut socket, response).await.is_err() {
                            break;
                        }
                    }
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Binary(_) => {
                        if send_error(&mut socket, "Send subscription commands as JSON text").await.is_err() {
                            break;
                        }
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                }
            }
            outbound = receiver.recv() => {
                let Some(outbound) = outbound else { break; };
                let frame = match outbound {
                    OutboundFrame::Text(text) => Message::Text(text.into()),
                    OutboundFrame::Binary(data) => Message::Binary(data.into()),
                };
                if socket.send(frame).await.is_err() {
                    break;
                }
            }
        }
    }
    hub.0.lock().unwrap().clients.remove(&id);
}

fn command(text: &str, hub: &Hub, id: &str) -> Value {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return json!({"type":"error", "message":"Expected a JSON object"});
    };
    let Some(action) = value.get("action").and_then(Value::as_str) else {
        return json!({"type":"error", "message":"Missing string field: action"});
    };
    let Some(filter) = value.get("topic").and_then(Value::as_str) else {
        return json!({"type":"error", "message":"Missing string field: topic"});
    };
    if !valid_topic_filter(filter) {
        return json!({"type":"error", "message":"Invalid topic filter"});
    }

    let mut state = hub.0.lock().unwrap();
    let Some(client) = state.clients.get_mut(id) else {
        return json!({"type":"error", "message":"WebSocket connection expired"});
    };
    match action {
        "subscribe" => {
            client.subscriptions.insert(filter.to_owned());
            json!({"type":"subscribed", "topic":filter})
        }
        "unsubscribe" => {
            client.subscriptions.remove(filter);
            json!({"type":"unsubscribed", "topic":filter})
        }
        _ => json!({"type":"error", "message":"action must be subscribe or unsubscribe"}),
    }
}

fn validate_topic(topic: &str) -> Result<(), AwsError> {
    if topic.is_empty() || topic.len() > 256 || topic.contains(['\0', '+', '#']) {
        return Err(invalid("Invalid MQTT topic name"));
    }
    Ok(())
}

fn valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.len() > 256 || filter.contains('\0') {
        return false;
    }
    let levels: Vec<_> = filter.split('/').collect();
    levels.iter().enumerate().all(|(index, level)| {
        (!level.contains('+') || *level == "+")
            && (!level.contains('#') || (*level == "#" && index + 1 == levels.len()))
    })
}

fn topic_matches(filter: &str, topic: &str) -> bool {
    // MQTT reserves $ topics from filters that begin with a wildcard.
    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }
    let mut topic_levels = topic.split('/');
    for (index, filter_level) in filter.split('/').enumerate() {
        match filter_level {
            "#" => return true,
            "+" if topic_levels.next().is_some() => {}
            "+" => return false,
            level if topic_levels.next() == Some(level) => {}
            _ => return false,
        }
        if index > 256 {
            return false;
        }
    }
    topic_levels.next().is_none()
}

async fn send_error(socket: &mut WebSocket, message: &str) -> Result<(), axum::Error> {
    send_json(socket, json!({"type":"error", "message":message})).await
}

async fn send_json(socket: &mut WebSocket, value: Value) -> Result<(), axum::Error> {
    socket.send(Message::Text(value.to_string().into())).await
}

pub fn handle_management_post(hub: &Hub, request: &RawRequest) -> Result<(), AwsError> {
    let marker = "/@connections/";
    let Some((prefix, encoded_id)) = request.path.rsplit_once(marker) else {
        return Err(AwsError::invalid_action("PostToConnection"));
    };
    if encoded_id.is_empty() || encoded_id.contains('/') {
        return Err(AwsError::sender(
            400,
            "BadRequestException",
            "Invalid connection ID",
        ));
    }
    let id = percent_decode_path(encoded_id);
    let prefix_parts: Vec<_> = prefix.split('/').filter(|part| !part.is_empty()).collect();
    let api_stage = (prefix_parts.len() >= 2).then(|| (prefix_parts[0], prefix_parts[1]));
    hub.post_to_connection(&id, &request.body, api_stage)
        .map_err(|error| match error {
            PostError::Gone => {
                AwsError::sender(410, "GoneException", "Connection is no longer available")
            }
            PostError::TooLarge => {
                AwsError::sender(413, "PayloadTooLargeException", "Message exceeds 128 KiB")
            }
            PostError::LimitExceeded => AwsError::sender(
                429,
                "LimitExceededException",
                "Connection send queue is full",
            ),
        })
}

fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "InvalidRequestException", message)
}
