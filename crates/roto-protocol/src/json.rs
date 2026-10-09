use std::collections::BTreeMap;

use roto_core::{AwsError, RawResponse};
use serde_json::{Map, Value};

use crate::base64;
use crate::timestamp::Timestamp;

/// Binary member value (base64 on the wire).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Blob(pub Vec<u8>);

/// A member kept as raw JSON (for shapes whose own model cannot express every value, e.g.
/// DynamoDB's `AttributeValue`, where `{"L": []}` and `{}` must stay distinct).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JsonValue(pub Value);

impl FromJson for JsonValue {
    fn from_json(v: &Value, _path: &str) -> Result<Self, AwsError> {
        Ok(Self(v.clone()))
    }
}

impl ToJson for JsonValue {
    fn to_json(&self) -> Value {
        self.0.clone()
    }
}

pub trait FromJson: Sized {
    /// `path` is the dotted location, used in validation messages.
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError>;
}

pub trait ToJson {
    fn to_json(&self) -> Value;
}

fn bad(path: &str, want: &str) -> AwsError {
    AwsError::sender(
        400,
        "SerializationException",
        format!("Start of structure or map found where not expected: {path} must be {want}"),
    )
}

impl FromJson for String {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        v.as_str()
            .map(str::to_string)
            .ok_or_else(|| bad(path, "a string"))
    }
}

macro_rules! json_int {
    ($($t:ty),*) => {$(
        impl FromJson for $t {
            fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
                v.as_i64().and_then(|n| <$t>::try_from(n).ok()).ok_or_else(|| bad(path, "an integer"))
            }
        }
    )*};
}
json_int!(i32, i64);

impl FromJson for f64 {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        v.as_f64().ok_or_else(|| bad(path, "a number"))
    }
}

impl FromJson for bool {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        v.as_bool().ok_or_else(|| bad(path, "a boolean"))
    }
}

impl FromJson for Timestamp {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        v.as_f64()
            .map(|f| Timestamp(f as i64))
            .ok_or_else(|| bad(path, "a number"))
    }
}

impl FromJson for Blob {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        v.as_str()
            .and_then(base64::decode)
            .map(Blob)
            .ok_or_else(|| bad(path, "a base64 string"))
    }
}

impl<T: FromJson> FromJson for Vec<T> {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        let arr = v.as_array().ok_or_else(|| bad(path, "a list"))?;
        arr.iter()
            .enumerate()
            .map(|(i, e)| T::from_json(e, &format!("{path}.{}", i + 1)))
            .collect()
    }
}

impl<T: FromJson> FromJson for BTreeMap<String, T> {
    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {
        let obj = v.as_object().ok_or_else(|| bad(path, "a map"))?;
        obj.iter()
            .map(|(k, e)| Ok((k.clone(), T::from_json(e, &format!("{path}.{k}"))?)))
            .collect()
    }
}

impl ToJson for String {
    fn to_json(&self) -> Value {
        Value::String(self.clone())
    }
}

macro_rules! json_plain {
    ($($t:ty),*) => {$(
        impl ToJson for $t { fn to_json(&self) -> Value { Value::from(*self) } }
    )*};
}
json_plain!(i32, i64, f64, bool);

impl ToJson for Timestamp {
    fn to_json(&self) -> Value {
        Value::from(self.0)
    }
}

impl ToJson for Blob {
    fn to_json(&self) -> Value {
        Value::String(base64::encode(&self.0))
    }
}

impl<T: ToJson> ToJson for Vec<T> {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(ToJson::to_json).collect())
    }
}

impl<T: ToJson> ToJson for BTreeMap<String, T> {
    fn to_json(&self) -> Value {
        Value::Object(self.iter().map(|(k, v)| (k.clone(), v.to_json())).collect())
    }
}

/// Reads optional member `name` from `obj`; JSON `null` counts as absent.
pub fn member<T: FromJson>(
    obj: &Map<String, Value>,
    name: &str,
    path: &str,
) -> Result<Option<T>, AwsError> {
    match obj.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => T::from_json(
            v,
            &if path.is_empty() {
                name.to_string()
            } else {
                format!("{path}.{name}")
            },
        )
        .map(Some),
    }
}

pub fn as_object<'a>(v: &'a Value, path: &str) -> Result<&'a Map<String, Value>, AwsError> {
    v.as_object().ok_or_else(|| {
        bad(
            if path.is_empty() {
                "request body"
            } else {
                path
            },
            "an object",
        )
    })
}

/// Shared by `awsJson1.0` / `1.1` services.
pub fn json_response(version: &str, request_id: &str, body: &Value) -> RawResponse {
    RawResponse {
        status: 200,
        headers: vec![
            (
                "content-type".into(),
                format!("application/x-amz-json-{version}"),
            ),
            ("x-amzn-requestid".into(), request_id.into()),
        ],
        body: serde_json::to_vec(body).unwrap_or_default(),
    }
}

/// `query_compat` adds `x-amzn-query-error` (services with `awsQueryCompatible`, e.g. SQS), which
/// SDKs use to keep the legacy query-protocol error codes.
pub fn json_error(
    version: &str,
    err: &AwsError,
    request_id: &str,
    query_compat: bool,
) -> RawResponse {
    let mut headers = vec![
        (
            "content-type".into(),
            format!("application/x-amz-json-{version}"),
        ),
        ("x-amzn-requestid".into(), request_id.into()),
    ];
    if query_compat {
        let kind = if err.sender { "Sender" } else { "Receiver" };
        headers.push(("x-amzn-query-error".into(), format!("{};{kind}", err.code)));
    }
    let mut body = serde_json::json!({ "__type": err.code, "message": err.message });
    for (k, v) in &err.extra {
        // Structured extras (`CancellationReasons`, `Item`) are carried as JSON text.
        let parsed = if v.starts_with('[') || v.starts_with('{') {
            serde_json::from_str(v).ok()
        } else {
            None
        };
        body[k] = parsed.unwrap_or_else(|| serde_json::Value::String(v.clone()));
    }
    RawResponse {
        status: err.status,
        headers,
        body: body.to_string().into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_members_and_collections() {
        let v = json!({"A": "x", "N": 3, "L": ["a", "b"], "M": {"k": "v"}, "Z": null});
        let o = as_object(&v, "").unwrap();
        assert_eq!(member::<String>(o, "A", "").unwrap().as_deref(), Some("x"));
        assert_eq!(member::<i32>(o, "N", "").unwrap(), Some(3));
        assert_eq!(
            member::<Vec<String>>(o, "L", "").unwrap().unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            member::<BTreeMap<String, String>>(o, "M", "")
                .unwrap()
                .unwrap()["k"],
            "v"
        );
        assert_eq!(member::<String>(o, "Z", "").unwrap(), None);
        assert!(member::<i32>(o, "A", "").is_err());
    }

    #[test]
    fn query_compat_error_header() {
        let r = json_error(
            "1.0",
            &AwsError::sender(400, "AWS.SimpleQueueService.NonExistentQueue", "gone"),
            "r",
            true,
        );
        assert!(r.headers.contains(&(
            "x-amzn-query-error".into(),
            "AWS.SimpleQueueService.NonExistentQueue;Sender".into()
        )));
        assert_eq!(r.status, 400);
    }
}
