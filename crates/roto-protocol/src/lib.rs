//! Wire-protocol codecs: AWS `query` (form request, XML response) and `json` 1.0/1.1.

pub mod base64;
pub mod json;
pub mod query;
pub mod restxml;
pub mod timestamp;
pub mod xml;

pub use json::{Blob, FromJson, JsonValue, ToJson, json_error, json_response};
pub use query::{QueryParams, QueryValue, query_error, query_response};
pub use timestamp::Timestamp;
pub use xml::{XmlValue, XmlWriter};
