//! MQTT transport and subscription handling; wire encoding lives in roto-mqtt.
use super::*;
use roto_mqtt::{Packet, decode};

pub async fn websocket(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> Response {
    ws.protocols(["mqtt"])
        .max_message_size(roto_mqtt::MAX_PACKET_SIZE)
        .on_upgrade(move |socket| serve(socket, app.websockets.clone()))
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
            api_gateway: None,
            mqtt: true,
        },
    );
    let mut connected = false;
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
                            let response = match command(packet, &hub, &id, &mut connected) {
                                Ok(response) => response,
                                Err(()) => break 'connection,
                            };
                            if let Some(response) = response {
                                if socket.send(Message::Binary(response.into())).await.is_err() {
                                    break 'connection;
                                }
                            }
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

fn command(
    packet: Packet,
    hub: &Hub,
    id: &str,
    connected: &mut bool,
) -> Result<Option<Vec<u8>>, ()> {
    if !*connected {
        if packet != Packet::Connect {
            return Err(());
        }
        *connected = true;
        return Ok(Some(roto_mqtt::CONNACK.to_vec()));
    }
    let response = match packet {
        Packet::Connect | Packet::Disconnect => return Err(()),
        Packet::Ping => Some(roto_mqtt::PINGRESP.to_vec()),
        Packet::Publish { id } => id.map(roto_mqtt::puback),
        Packet::Subscribe {
            id: packet_id,
            filters,
        } => {
            let mut state = hub.0.lock().unwrap();
            let client = state.clients.get_mut(id).ok_or(())?;
            let codes = filters
                .into_iter()
                .map(|(filter, qos)| {
                    if qos <= 1 && valid_topic_filter(&filter) {
                        client.subscriptions.insert(filter);
                        qos
                    } else {
                        0x80
                    }
                })
                .collect::<Vec<_>>();
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
    Ok(response)
}
