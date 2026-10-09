//! DynamoDB (JSON 1.0). Wire types come from the botocore model (`AttributeValue` is carried as raw
//! JSON); tables and items live in `dynamodb.db`, expressions are parsed and evaluated in Rust.

#![allow(
    clippy::collapsible_if,
    clippy::type_complexity,
    clippy::too_many_arguments
)]

#[allow(clippy::all)]
mod generated;

mod batch;
mod eval;
mod expr;
mod keys;
mod number;
mod query;
mod service;
mod table;
mod tables;
mod value;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::json_error;
use serde_json::Value;

pub use generated::{OPERATIONS, Service, dispatch};
pub use service::DynamoDb;

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: "
CREATE TABLE tables (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    table_id TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    key_schema TEXT NOT NULL,
    attr_defs TEXT NOT NULL,
    gsis TEXT NOT NULL DEFAULT '[]',
    lsis TEXT NOT NULL DEFAULT '[]',
    billing_mode TEXT NOT NULL,
    throughput TEXT,
    stream_spec TEXT,
    tags TEXT NOT NULL DEFAULT '[]',
    ttl_attr TEXT,
    ttl_enabled INTEGER NOT NULL DEFAULT 0,
    deletion_protection INTEGER NOT NULL DEFAULT 0,
    sse TEXT,
    table_class TEXT,
    PRIMARY KEY (account_id, region, name)
);
-- hk/rk are order-preserving encodings of the key attributes (see keys.rs).
CREATE TABLE items (
    table_id TEXT NOT NULL,
    hk BLOB NOT NULL,
    rk BLOB NOT NULL,
    item TEXT NOT NULL,
    PRIMARY KEY (table_id, hk, rk)
) WITHOUT ROWID;
",
    },
    Migration {
        version: 2,
        sql: "ALTER TABLE tables ADD COLUMN pitr INTEGER NOT NULL DEFAULT 0;",
    },
];

/// Operations with real behaviour; the rest answer `NotImplemented`.
pub const IMPLEMENTED: &[&str] = &[
    "CreateTable",
    "DeleteTable",
    "DescribeTable",
    "ListTables",
    "UpdateTable",
    "PutItem",
    "GetItem",
    "DeleteItem",
    "UpdateItem",
    "Query",
    "Scan",
    "BatchGetItem",
    "BatchWriteItem",
    "TransactWriteItems",
    "TransactGetItems",
    "TagResource",
    "UntagResource",
    "ListTagsOfResource",
    "UpdateTimeToLive",
    "DescribeTimeToLive",
    "DescribeEndpoints",
    "DescribeLimits",
    "DescribeContinuousBackups",
    "UpdateContinuousBackups",
];

pub struct DynamoDbHandler(pub Arc<DynamoDb>);

impl DynamoDbHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(DynamoDb::new(store)?)))
    }
}

impl ServiceHandler for DynamoDbHandler {
    fn service(&self) -> &'static str {
        "dynamodb"
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
