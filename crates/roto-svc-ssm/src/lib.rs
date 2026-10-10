//! SSM Parameter Store (JSON 1.1). Types and dispatch are generated from the botocore model;
//! parameters (with their version history, labels and tags) live in `ssm.db`.

#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

#[allow(clippy::all)]
mod generated;
mod models;
mod parameters;
mod schema;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::json_error;
use serde_json::Value;

pub use generated::{OPERATIONS, Service, dispatch};
pub use parameters::Ssm;

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE parameters (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    description TEXT,
    allowed_pattern TEXT,
    key_id TEXT,
    data_type TEXT NOT NULL,
    tier TEXT NOT NULL,
    policies TEXT,
    labels TEXT NOT NULL DEFAULT '[]',
    last_modified INTEGER NOT NULL,
    PRIMARY KEY (account_id, region, name, version)
);
CREATE TABLE resource_tags (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    UNIQUE (account_id, region, resource_type, resource_id, key)
);
",
}];

/// Operations with real behaviour; the rest answer `NotImplemented`.
pub const IMPLEMENTED: &[&str] = &[
    "PutParameter",
    "GetParameter",
    "GetParameters",
    "GetParametersByPath",
    "DeleteParameter",
    "DeleteParameters",
    "DescribeParameters",
    "GetParameterHistory",
    "LabelParameterVersion",
    "UnlabelParameterVersion",
    "AddTagsToResource",
    "RemoveTagsFromResource",
    "ListTagsForResource",
];

pub struct SsmHandler(pub Arc<Ssm>);

impl SsmHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Ssm::new(store)?)))
    }
}

impl ServiceHandler for SsmHandler {
    fn service(&self) -> &'static str {
        "ssm"
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let fail = |e: AwsError| json_error(generated::JSON_VERSION, &e, &ctx.request_id, false);
        let Some(target) = req.header("x-amz-target") else {
            return Ok(fail(AwsError::sender(
                400,
                "MissingAuthenticationToken",
                "Missing X-Amz-Target header",
            )));
        };
        let operation = target.rsplit('.').next().unwrap_or(target);
        let body: Value = if req.body.is_empty() {
            Value::Object(Default::default())
        } else {
            match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(fail(AwsError::sender(
                        400,
                        "SerializationException",
                        format!("invalid JSON: {e}"),
                    )));
                }
            }
        };
        Ok(dispatch(&*self.0, ctx, operation, &body).unwrap_or_else(fail))
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
