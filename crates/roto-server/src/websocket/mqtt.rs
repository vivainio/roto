//! MQTT transport and subscription handling; wire encoding lives in roto-mqtt.
use super::*;
use axum::extract::{ConnectInfo, OriginalUri};
use roto_mqtt::{Connect, Packet, decode};
use std::net::SocketAddr;

pub async fn websocket(
    State(app): State<Arc<App>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let headers = headers_json(&headers);
    let mut headers = headers.as_object().unwrap().clone();
    headers.remove("authorization");
    let request = json!({"path":uri.path(), "query":uri.query().unwrap_or(""),
        "headers":headers, "source_ip":peer.ip().to_string(), "source_port":peer.port()});
    ws.protocols(["mqtt"])
        .max_message_size(roto_mqtt::MAX_PACKET_SIZE)
        .on_upgrade(move |socket| serve(socket, app, request))
        .into_response()
}

async fn serve(mut socket: WebSocket, app: Arc<App>, mut request: Value) {
    let hub = &app.websockets;
    let id = roto_core::ids::request_id();
    request["connection_id"] = json!(id);
    let (outbound, mut receiver) = mpsc::channel(OUTBOUND_CAPACITY);
    hub.0.lock().unwrap().clients.insert(
        id.clone(),
        Client {
            outbound,
            subscriptions: HashSet::new(),
            api_gateway: None,
            mqtt: true,
        },
    );
    let mut connection = None;
    let mut buffer = Vec::new();
    'connection: loop {
        tokio::select! {
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { break };
                match frame {
                    Message::Binary(bytes) => {
                        buffer.extend_from_slice(&bytes);
                        let mut consumed = 0;
                        loop {
                            let packet = match decode(&buffer[consumed..]) {
                                Ok(Some((packet, length))) => { consumed += length; packet }
                                Ok(None) => break,
                                Err(_) => break 'connection,
                            };
                            let (response, close) = match command(packet, &app, &id, &mut connection, &request).await {
                                Ok(response) => response,
                                Err(()) => break 'connection,
                            };
                            if let Some(response) = response
                                && socket.send(Message::Binary(response.into())).await.is_err() {
                                    break 'connection;
                                }
                            if close { break 'connection; }
                        }
                        buffer.drain(..consumed);
                        if buffer.len() > roto_mqtt::MAX_PACKET_SIZE { break; }
                    }
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() { break; }
                    }
                    Message::Pong(_) => {}
                    Message::Text(_) | Message::Close(_) => break,
                }
            }
            frame = receiver.recv() => {
                let Some(OutboundFrame::Binary(bytes)) = frame else { break };
                if socket.send(Message::Binary(bytes.into())).await.is_err() { break; }
            }
        }
    }
    hub.0.lock().unwrap().clients.remove(&id);
    let _ = socket.send(Message::Close(None)).await;
}

fn connect_data(connect: &Connect) -> Value {
    json!({"protocol_version":4, "client_id":connect.client_id,
        "clean_session":connect.clean_session, "keep_alive":connect.keep_alive,
        "username":connect.username,
        "password":connect.password.as_ref().and_then(|bytes| std::str::from_utf8(bytes).ok()),
        "password_base64":connect.password.as_ref().map(|bytes| roto_protocol::base64::encode(bytes)),
        "will":connect.will.as_ref().map(|will| json!({"topic":will.topic,"qos":will.qos,
            "retain":will.retain,"payload_base64":roto_protocol::base64::encode(&will.payload)}))})
}

async fn hook(app: &Arc<App>, operation: &str, mut request: Value) -> Result<Value, String> {
    let hooks = app.hooks.lock().unwrap().clone();
    if !hooks.matches_mqtt(operation) {
        return Ok(Value::Null);
    }
    request["transport"] = json!("mqtt");
    request["operation"] = json!(operation);
    let context = json!({"account_id":app.account_id(),"region":"us-east-1",
        "request_id":roto_core::ids::request_id()});
    let operation = operation.to_owned();
    tokio::task::spawn_blocking(move || hooks.mqtt(&operation, request, context))
        .await
        .map_err(|error| error.to_string())?
}

/// Reserved `$` topics (e.g. `$aws/...`) are not writable by clients.
fn hub_publish_allowed(topic: &str) -> bool {
    !topic.starts_with('$')
}

async fn command(
    packet: Packet,
    app: &Arc<App>,
    id: &str,
    connection: &mut Option<Connect>,
    metadata: &Value,
) -> Result<(Option<Vec<u8>>, bool), ()> {
    let hub = &app.websockets;
    if connection.is_none() {
        let Packet::Connect(connect) = packet else {
            return Err(());
        };
        let mut request = metadata.clone();
        request["connect"] = connect_data(&connect);
        let code = match hook(app, "Connect", request).await.and_then(|value| {
            if value.is_null() {
                Ok(0)
            } else {
                value["return_code"]
                    .as_u64()
                    .filter(|code| *code <= 5)
                    .map(|code| code as u8)
                    .ok_or_else(|| "MQTT Connect hook requires return_code between 0 and 5".into())
            }
        }) {
            Ok(code) => code,
            Err(reason) => {
                tracing::warn!(%reason, "MQTT Connect hook failed");
                3
            }
        };
        if code == 0 {
            *connection = Some(connect);
        }
        return Ok((Some(roto_mqtt::connack(code)), code != 0));
    }
    let response = match packet {
        Packet::Connect(_) | Packet::Disconnect => return Err(()),
        Packet::Ping => Some(roto_mqtt::PINGRESP.to_vec()),
        Packet::Publish {
            id,
            topic,
            payload,
        } => {
            if !hub_publish_allowed(&topic) || hub.publish(&topic, 0, &payload).is_err() {
                return Err(());
            }
            id.map(roto_mqtt::puback)
        }
        Packet::Subscribe {
            id: packet_id,
            filters,
        } => {
            let mut request = metadata.clone();
            request["connect"] = connect_data(connection.as_ref().unwrap());
            request["packet_id"] = json!(packet_id);
            request["filters"] = json!(
                filters
                    .iter()
                    .map(|(topic, qos)| json!({"topic":topic,"qos":qos}))
                    .collect::<Vec<_>>()
            );
            let defaults = filters
                .iter()
                .map(|(filter, qos)| {
                    if *qos <= 1 && valid_topic_filter(filter) {
                        *qos
                    } else {
                        0x80
                    }
                })
                .collect::<Vec<_>>();
            let codes = match hook(app, "Subscribe", request).await.and_then(|value| {
                if value.is_null() {
                    return Ok(defaults.clone());
                }
                let codes = value["return_codes"]
                    .as_array()
                    .filter(|codes| codes.len() == filters.len())
                    .ok_or("MQTT Subscribe hook requires one return code per filter")?;
                codes
                    .iter()
                    .zip(&defaults)
                    .map(|(code, default)| {
                        code.as_u64()
                            .filter(|code| {
                                *code == 128 || (*default != 128 && *code <= u64::from(*default))
                            })
                            .map(|code| code as u8)
                            .ok_or_else(|| "Invalid MQTT subscription return code".into())
                    })
                    .collect::<Result<Vec<_>, String>>()
            }) {
                Ok(codes) => codes,
                Err(reason) => {
                    tracing::warn!(%reason, "MQTT Subscribe hook failed");
                    vec![0x80; filters.len()]
                }
            };
            let mut state = hub.0.lock().unwrap();
            let client = state.clients.get_mut(id).ok_or(())?;
            for ((filter, _), code) in filters.into_iter().zip(&codes) {
                if *code != 0x80 {
                    client.subscriptions.insert(filter);
                }
            }
            Some(roto_mqtt::suback(packet_id, &codes))
        }
        Packet::Unsubscribe {
            id: packet_id,
            filters,
        } => {
            let mut state = hub.0.lock().unwrap();
            let client = state.clients.get_mut(id).ok_or(())?;
            for filter in filters {
                client.subscriptions.remove(&filter);
            }
            Some(roto_mqtt::unsuback(packet_id))
        }
    };
    Ok((response, false))
}
