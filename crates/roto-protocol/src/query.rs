use roto_core::{AwsError, RawResponse};

use crate::timestamp::Timestamp;
use crate::xml::XmlWriter;

/// Decoded `application/x-www-form-urlencoded` parameters (request body and/or URL query).
#[derive(Debug, Default, Clone)]
pub struct QueryParams(Vec<(String, String)>);

impl QueryParams {
    pub fn parse(encoded: &str) -> Self {
        Self(
            encoded
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let (k, v) = p.split_once('=').unwrap_or((p, ""));
                    (percent_decode(k), percent_decode(v))
                })
                .collect(),
        )
    }

    pub fn extend(&mut self, other: QueryParams) {
        self.0.extend(other.0);
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Whether any parameter is `key` or starts with `key.`.
    pub fn has_prefix(&self, key: &str) -> bool {
        self.0
            .iter()
            .any(|(k, _)| k == key || k.strip_prefix(key).is_some_and(|r| r.starts_with('.')))
    }

    pub fn value<T: QueryValue>(&self, prefix: &str, name: &str) -> Result<Option<T>, AwsError> {
        T::read(self, &join_key(prefix, name))
    }

    /// Reads `prefix.name.<item>.N` (or `prefix.name.N` when flattened), N = 1, 2, …
    pub fn list<T: QueryValue>(
        &self,
        prefix: &str,
        name: &str,
        item: &str,
        flattened: bool,
    ) -> Result<Vec<T>, AwsError> {
        let base = join_key(prefix, name);
        let base = if flattened {
            base
        } else {
            join_key(&base, item)
        };
        let mut out = Vec::new();
        for n in 1.. {
            match T::read(self, &format!("{base}.{n}"))? {
                Some(v) => out.push(v),
                None => break,
            }
        }
        Ok(out)
    }
}

impl QueryParams {
    /// Reads a map: `prefix.name.entry.N.key` / `.value` (or `prefix.name.N.key` when flattened).
    pub fn map<V: QueryValue>(
        &self,
        prefix: &str,
        name: &str,
        key_name: &str,
        value_name: &str,
        flattened: bool,
    ) -> Result<std::collections::BTreeMap<String, V>, AwsError> {
        let base = join_key(prefix, name);
        let base = if flattened {
            base
        } else {
            join_key(&base, "entry")
        };
        let mut out = std::collections::BTreeMap::new();
        for n in 1.. {
            let Some(key) = self.get(&format!("{base}.{n}.{key_name}")) else {
                break;
            };
            let value_key = format!("{base}.{n}.{value_name}");
            let value = V::read(self, &value_key)?
                .ok_or_else(|| AwsError::missing_parameter(&value_key))?;
            out.insert(key.to_string(), value);
        }
        Ok(out)
    }
}

pub fn join_key(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

/// A value readable from query parameters at `key`.
pub trait QueryValue: Sized {
    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError>;
}

impl QueryValue for String {
    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {
        Ok(p.get(key).map(str::to_string))
    }
}

macro_rules! parsed_query {
    ($($t:ty),*) => {$(
        impl QueryValue for $t {
            fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {
                p.get(key).map(|v| v.parse::<$t>().map_err(|_| {
                    AwsError::sender(400, "InvalidParameterValue",
                        format!("Value '{v}' at '{key}' is not a valid {}", stringify!($t)))
                })).transpose()
            }
        }
    )*};
}
parsed_query!(i32, i64, f64);

impl QueryValue for bool {
    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {
        match p.get(key) {
            None => Ok(None),
            Some("true") => Ok(Some(true)),
            Some("false") => Ok(Some(false)),
            Some(v) => Err(AwsError::invalid_parameter_value(format!(
                "Value '{v}' at '{key}' must be true or false"
            ))),
        }
    }
}

impl QueryValue for crate::json::Blob {
    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {
        match p.get(key) {
            None => Ok(None),
            Some(v) => crate::base64::decode(v)
                .map(|b| Some(Self(b)))
                .ok_or_else(|| {
                    AwsError::invalid_parameter_value(format!(
                        "Value at '{key}' is not valid base64"
                    ))
                }),
        }
    }
}

impl QueryValue for Timestamp {
    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {
        Ok(i64::read(p, key)?.map(Timestamp))
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() && hex(b[i + 1]).is_some() && hex(b[i + 2]).is_some() => {
                out.push(hex(b[i + 1]).unwrap() << 4 | hex(b[i + 2]).unwrap());
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

fn xml_response(status: u16, request_id: &str, body: String) -> RawResponse {
    RawResponse {
        status,
        headers: vec![
            ("content-type".into(), "text/xml".into()),
            ("x-amzn-requestid".into(), request_id.into()),
        ],
        body: body.into_bytes(),
    }
}

/// `<{op}Response xmlns><{op}Result>{inner}</{op}Result><ResponseMetadata>…</ResponseMetadata></{op}Response>`.
/// `result_wrapper` is `None` for operations without an output shape.
pub fn query_response(
    ns: &str,
    op: &str,
    result_wrapper: Option<&str>,
    request_id: &str,
    inner: &str,
) -> RawResponse {
    let mut s = String::new();
    s.push_str(&format!("<{op}Response xmlns=\"{ns}\">"));
    if let Some(w) = result_wrapper {
        s.push_str(&format!("<{w}>{inner}</{w}>"));
    }
    s.push_str(&format!(
        "<ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata></{op}Response>"
    ));
    xml_response(200, request_id, s)
}

pub fn query_error(ns: &str, err: &AwsError, request_id: &str) -> RawResponse {
    let mut w = XmlWriter::new();
    w.element("Type", if err.sender { "Sender" } else { "Receiver" });
    w.element("Code", &err.code);
    w.element("Message", &err.message);
    let inner = w.finish();
    let body = format!(
        "<ErrorResponse xmlns=\"{ns}\"><Error>{inner}</Error><RequestId>{request_id}</RequestId></ErrorResponse>"
    );
    xml_response(err.status, request_id, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_form_data() {
        let p = QueryParams::parse("Action=GetCallerIdentity&Name=a+b%20c%2F&Empty=");
        assert_eq!(p.get("Action"), Some("GetCallerIdentity"));
        assert_eq!(p.get("Name"), Some("a b c/"));
        assert_eq!(p.get("Empty"), Some(""));
    }

    #[test]
    fn reads_lists() {
        let p = QueryParams::parse("Tags.member.1.Key=a&Tags.member.2.Key=b&Flat.1=x&Flat.2=y");
        let keys: Vec<String> = p.list("Tags.member", "Key", "", true).unwrap_or_default();
        assert!(keys.is_empty()); // wrong shape on purpose: name path is Tags.member.Key
        let flat: Vec<String> = p.list("", "Flat", "member", true).unwrap();
        assert_eq!(flat, ["x", "y"]);
        assert!(p.has_prefix("Tags"));
        assert!(!p.has_prefix("Tag"));
    }

    #[test]
    fn error_envelope() {
        let r = query_error("urn:x", &AwsError::sender(400, "Bad", "no <way>"), "rid");
        let body = String::from_utf8(r.body).unwrap();
        assert_eq!(r.status, 400);
        assert!(body.contains("<Code>Bad</Code>") && body.contains("no &lt;way&gt;"));
    }
}
