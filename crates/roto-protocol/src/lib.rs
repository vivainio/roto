//! Wire-protocol codecs. Phase 0 implements the AWS `query` protocol (form-encoded request,
//! XML response), which STS, IAM, SQS (query), SNS and others use.

pub mod query;
pub mod timestamp;
pub mod xml;

pub use query::{QueryParams, QueryValue, query_error, query_response};
pub use timestamp::Timestamp;
pub use xml::{XmlValue, XmlWriter};
