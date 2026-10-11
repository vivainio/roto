//! Minimal MQTT 3.1.1 wire codec for Roto's local WebSocket listener.
//! No transport, sessions, or broker state. Only QoS 0/1 are supported.

pub const MAX_PACKET_SIZE: usize = 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum Packet {
    Connect,
    Subscribe {
        id: u16,
        filters: Vec<(String, u8)>,
    },
    Unsubscribe {
        id: u16,
        filters: Vec<String>,
    },
    /// Client payloads are deliberately discarded by the listener.
    Publish {
        id: Option<u16>,
    },
    Ping,
    Disconnect,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Malformed,
    Unsupported,
    TooLarge,
}

/// Decode one packet, returning its consumed length. Incomplete input returns
/// `None` without consuming anything; callers retain it for the next frame.
pub fn decode(input: &[u8]) -> Result<Option<(Packet, usize)>, Error> {
    let Some(&header) = input.first() else {
        return Ok(None);
    };
    let mut length = 0usize;
    let mut start = 0;
    for index in 0..4 {
        let Some(&byte) = input.get(index + 1) else {
            return Ok(None);
        };
        length |= usize::from(byte & 127) << (index * 7);
        if byte & 128 == 0 {
            start = index + 2;
            break;
        }
    }
    if start == 0 {
        return Err(Error::Malformed);
    }
    let end = start + length;
    if end > MAX_PACKET_SIZE {
        return Err(Error::TooLarge);
    }
    let Some(body) = input.get(start..end) else {
        return Ok(None);
    };
    let mut reader = Reader(body);
    let packet = match header {
        0x10 => {
            if reader.bytes()? != b"MQTT" || reader.byte()? != 4 {
                return Err(Error::Unsupported);
            }
            let flags = reader.byte()?;
            if flags & 1 != 0 || (flags & 4 == 0 && flags & 0x38 != 0) || (flags >> 3) & 3 == 3 {
                return Err(Error::Malformed);
            }
            reader.u16()?; // Keepalive is handled by responding to PINGREQ.
            reader.text()?; // Client ID and clean session are ignored.
            if flags & 4 != 0 {
                reader.text()?;
                reader.bytes()?;
            }
            if flags & 128 != 0 {
                reader.text()?;
            }
            if flags & 64 != 0 {
                reader.bytes()?;
            }
            reader.finished()?;
            Packet::Connect
        }
        0x82 => {
            let id = reader.id()?;
            let mut filters = Vec::new();
            while !reader.0.is_empty() {
                let topic = reader.text()?.to_owned();
                let qos = reader.byte()?;
                if qos > 2 {
                    return Err(Error::Malformed);
                }
                filters.push((topic, qos));
            }
            if filters.is_empty() {
                return Err(Error::Malformed);
            }
            Packet::Subscribe { id, filters }
        }
        0xa2 => {
            let id = reader.id()?;
            let mut filters = Vec::new();
            while !reader.0.is_empty() {
                filters.push(reader.text()?.to_owned());
            }
            if filters.is_empty() {
                return Err(Error::Malformed);
            }
            Packet::Unsubscribe { id, filters }
        }
        0xc0 | 0xe0 => {
            reader.finished()?;
            if header == 0xc0 {
                Packet::Ping
            } else {
                Packet::Disconnect
            }
        }
        header if header >> 4 == 3 => {
            let qos = (header >> 1) & 3;
            if qos > 1 {
                return Err(Error::Unsupported);
            }
            let topic = reader.text()?;
            if topic.is_empty() || topic.contains(['+', '#']) {
                return Err(Error::Malformed);
            }
            let id = if qos == 1 { Some(reader.id()?) } else { None };
            Packet::Publish { id }
        }
        _ => return Err(Error::Unsupported),
    };
    Ok(Some((packet, end)))
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn byte(&mut self) -> Result<u8, Error> {
        let (&value, rest) = self.0.split_first().ok_or(Error::Malformed)?;
        self.0 = rest;
        Ok(value)
    }
    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }
    fn id(&mut self) -> Result<u16, Error> {
        let id = self.u16()?;
        if id == 0 {
            Err(Error::Malformed)
        } else {
            Ok(id)
        }
    }
    fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let length = usize::from(self.u16()?);
        let bytes = self.0.get(..length).ok_or(Error::Malformed)?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }
    fn text(&mut self) -> Result<&'a str, Error> {
        let text = std::str::from_utf8(self.bytes()?).map_err(|_| Error::Malformed)?;
        if text.contains('\0') {
            return Err(Error::Malformed);
        }
        Ok(text)
    }
    fn finished(&self) -> Result<(), Error> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Malformed)
        }
    }
}

pub const CONNACK: &[u8] = &[0x20, 2, 0, 0];
pub const PINGRESP: &[u8] = &[0xd0, 0];

pub fn puback(id: u16) -> Vec<u8> {
    acknowledgement(0x40, id, &[])
}
pub fn unsuback(id: u16) -> Vec<u8> {
    acknowledgement(0xb0, id, &[])
}
/// Codes are granted QoS 0/1 or 0x80 for unsupported/invalid filters.
pub fn suback(id: u16, codes: &[u8]) -> Vec<u8> {
    acknowledgement(0x90, id, codes)
}
fn acknowledgement(header: u8, id: u16, codes: &[u8]) -> Vec<u8> {
    let mut body = id.to_be_bytes().to_vec();
    body.extend_from_slice(codes);
    frame(header, &body)
}

/// Outbound deliveries always use QoS 0 and preserve raw payload bytes.
pub fn publish(topic: &str, payload: &[u8]) -> Result<Vec<u8>, Error> {
    let length = u16::try_from(topic.len()).map_err(|_| Error::TooLarge)?;
    if payload.len() + topic.len() + 7 > MAX_PACKET_SIZE {
        return Err(Error::TooLarge);
    }
    let mut body = length.to_be_bytes().to_vec();
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    Ok(frame(0x30, &body))
}
fn frame(header: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![header];
    let mut length = body.len();
    loop {
        let byte = (length % 128) as u8;
        length /= 128;
        packet.push(byte | if length > 0 { 128 } else { 0 });
        if length == 0 {
            break;
        }
    }
    packet.extend_from_slice(body);
    packet
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and_combined_packets() {
        let packet = [0x82, 6, 0, 7, 0, 1, b'a', 1];
        for end in 0..packet.len() {
            assert_eq!(decode(&packet[..end]), Ok(None));
        }
        let mut combined = packet.to_vec();
        combined.extend_from_slice(&[0xc0, 0]);
        assert_eq!(
            decode(&combined),
            Ok(Some((
                Packet::Subscribe {
                    id: 7,
                    filters: vec![("a".into(), 1)],
                },
                8
            )))
        );
        assert_eq!(decode(&combined[8..]), Ok(Some((Packet::Ping, 2))));
    }

    #[test]
    fn rejects_bad_lengths_and_fields() {
        for input in [
            &[0x30, 128, 128, 128, 128][..],
            &[0x82, 2, 0, 0],
            &[0x82, 4, 0, 1, 255, 255],
            &[0xc0, 1, 0],
        ] {
            assert_eq!(decode(input), Err(Error::Malformed));
        }
        assert_eq!(decode(&[0x30, 255, 255, 127]), Err(Error::TooLarge));
        assert_eq!(
            decode(&[0x10, 7, 0, 4, b'M', b'Q', b'T', b'T', 5]),
            Err(Error::Unsupported)
        );
    }

    #[test]
    fn binary_publish_and_multibyte_lengths() {
        let payload = vec![255; 20000];
        let encoded = publish("demo/state", &payload).unwrap();
        assert_eq!(
            decode(&encoded),
            Ok(Some((Packet::Publish { id: None }, encoded.len())))
        );
        assert!(encoded.ends_with(&payload));
        assert_eq!(puback(258), [0x40, 2, 1, 2]);
        assert_eq!(suback(9, &[0, 1, 128]), [0x90, 5, 0, 9, 0, 1, 128]);
    }
}
