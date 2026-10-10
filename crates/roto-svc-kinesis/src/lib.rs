//! Kinesis Data Streams (JSON 1.1), with persisted streams, shards and records.

#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

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
pub use service::Kinesis;

pub const MIGRATIONS: &[Migration] = &[Migration { version: 1, sql: "
CREATE TABLE streams (arn TEXT PRIMARY KEY, account_id TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL, metadata TEXT NOT NULL, UNIQUE(account_id, region, name));
CREATE TABLE records (stream_arn TEXT NOT NULL REFERENCES streams(arn) ON DELETE CASCADE, shard_id TEXT NOT NULL, sequence INTEGER NOT NULL, data BLOB NOT NULL, partition_key TEXT NOT NULL, arrived REAL NOT NULL, PRIMARY KEY(stream_arn, shard_id, sequence));
CREATE TABLE shard_sequences (stream_arn TEXT NOT NULL REFERENCES streams(arn) ON DELETE CASCADE, shard_id TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY(stream_arn, shard_id));
CREATE TABLE tokens (token TEXT PRIMARY KEY, account_id TEXT NOT NULL, region TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, expires REAL NOT NULL);
" }];

pub const IMPLEMENTED: &[&str] = &[
    "CreateStream",
    "DeleteStream",
    "ListStreams",
    "DescribeStream",
    "DescribeStreamSummary",
    "ListShards",
    "PutRecord",
    "PutRecords",
    "GetShardIterator",
    "GetRecords",
    "IncreaseStreamRetentionPeriod",
    "DecreaseStreamRetentionPeriod",
    "AddTagsToStream",
    "RemoveTagsFromStream",
    "ListTagsForStream",
    "UpdateStreamMode",
    "SplitShard",
    "MergeShards",
    "StartStreamEncryption",
    "StopStreamEncryption",
    "EnableEnhancedMonitoring",
    "DisableEnhancedMonitoring",
    "RegisterStreamConsumer",
    "DeregisterStreamConsumer",
    "DescribeStreamConsumer",
    "ListStreamConsumers",
    "DescribeLimits",
    "UpdateShardCount",
];

pub struct KinesisHandler(pub Arc<Kinesis>);

impl KinesisHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Kinesis::new(store)?)))
    }
}

impl ServiceHandler for KinesisHandler {
    fn service(&self) -> &'static str {
        "kinesis"
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
