//! Shared foundation for roto: errors, request context, SigV4 scope parsing, and the
//! SQLite-backed [`store::Store`].

pub mod error;
pub mod http;
pub mod ids;
pub mod sigv4;
pub mod store;

pub use diesel;
pub use error::AwsError;
pub use http::{RawRequest, RawResponse, RequestContext, ServiceHandler};
pub use rusqlite;
