//! A small CloudWatch Logs slice: log-group CRUD, simple event filtering, and
//! Lambda executor output in `/aws/lambda/<function>` groups.

#[allow(clippy::all)]
mod generated;
mod schema;
mod service;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::json_error;
use serde_json::Value;

pub use generated::{OPERATIONS, Service, dispatch};
pub use service::CloudWatchLogs;

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE log_groups (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    log_group_name TEXT NOT NULL,
    creation_time INTEGER NOT NULL,
    retention_in_days INTEGER,
    tags TEXT NOT NULL DEFAULT '{}',
    stored_bytes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, region, log_group_name)
);
CREATE TABLE log_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    log_group_name TEXT NOT NULL,
    log_stream_name TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    ingestion_time INTEGER NOT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (account_id, region, log_group_name)
        REFERENCES log_groups(account_id, region, log_group_name) ON DELETE CASCADE
);
CREATE INDEX log_events_ordered
    ON log_events(account_id, region, log_group_name, timestamp, id);
",
}];

/// The first service slice intentionally implements resource CRUD and ordinary log access only.
pub const IMPLEMENTED: &[&str] = &[
    "CreateLogGroup",
    "DeleteLogGroup",
    "DescribeLogGroups",
    "FilterLogEvents",
    "PutRetentionPolicy",
    "DeleteRetentionPolicy",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
];

pub struct CloudWatchLogsHandler(pub Arc<CloudWatchLogs>);

impl CloudWatchLogsHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(CloudWatchLogs::new(store)?)))
    }
}

impl ServiceHandler for CloudWatchLogsHandler {
    fn service(&self) -> &'static str {
        "logs"
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
                Ok(value) => value,
                Err(error) => {
                    return Ok(fail(AwsError::sender(
                        400,
                        "SerializationException",
                        format!("invalid JSON: {error}"),
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
