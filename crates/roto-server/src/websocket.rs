//! Shared local WebSocket connections for IoT topic subscriptions and API Gateway callbacks.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use roto_core::{AwsError, RawRequest};
use roto_protocol::{QueryParams, restxml::percent_decode_path};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::App;

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

        let mut state = self.0.lock().unwrap();
        state.clients.retain(|_, client| {
            let matches = client
                .subscriptions
                .iter()
                .any(|filter| topic_matches(filter, &topic));
            if matches {
                !matches!(
                    client.outbound.try_send(OutboundFrame::Text(event.clone())),
                    Err(mpsc::error::TrySendError::Closed(_))
                )
            } else {
                true
            }
        });
        Ok(())
    }

    pub fn post_to_connection(&self, id: &str, data: &[u8]) -> Result<(), PostError> {
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

async fn serve(mut socket: WebSocket, hub: Hub) {
    let id = roto_core::ids::request_id();
    let (outbound, mut receiver) = mpsc::channel(OUTBOUND_CAPACITY);
    hub.0.lock().unwrap().clients.insert(
        id.clone(),
        Client {
            outbound,
            subscriptions: HashSet::new(),
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
    let Some((_, encoded_id)) = request.path.rsplit_once(marker) else {
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
    hub.post_to_connection(&id, &request.body)
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
