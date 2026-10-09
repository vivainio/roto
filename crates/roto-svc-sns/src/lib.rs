//! SNS (query protocol). Types and dispatch are generated from the botocore model; topics and
//! subscriptions live in `sns.db`. Publishing fans out to SQS subscriptions in-process.

#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

mod filter;
#[allow(clippy::all)]
mod generated;
mod service;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{QueryParams, query_error};
use roto_svc_sqs::Sqs;

pub use generated::{NAMESPACE, OPERATIONS, Service, dispatch};
pub use service::Sns;

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE topics (
    arn TEXT NOT NULL PRIMARY KEY,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT NOT NULL DEFAULT '{}',
    tags TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    seq INTEGER NOT NULL
);
CREATE TABLE subscriptions (
    arn TEXT NOT NULL UNIQUE,
    topic_arn TEXT NOT NULL REFERENCES topics(arn) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    protocol TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    attributes TEXT NOT NULL DEFAULT '{}',
    confirmed INTEGER NOT NULL,
    token TEXT,
    seq INTEGER PRIMARY KEY AUTOINCREMENT
);
",
}];

/// Operations with real behaviour; the rest answer `NotImplemented`.
pub const IMPLEMENTED: &[&str] = &[
    "CreateTopic",
    "DeleteTopic",
    "ListTopics",
    "GetTopicAttributes",
    "SetTopicAttributes",
    "Subscribe",
    "ConfirmSubscription",
    "Unsubscribe",
    "ListSubscriptions",
    "ListSubscriptionsByTopic",
    "GetSubscriptionAttributes",
    "SetSubscriptionAttributes",
    "Publish",
    "PublishBatch",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "AddPermission",
    "RemovePermission",
];

pub struct SnsHandler(pub Arc<Sns>);

impl SnsHandler {
    pub fn new(store: &Store, sqs: Arc<Sqs>) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Sns::new(store, sqs)?)))
    }
}

impl ServiceHandler for SnsHandler {
    fn service(&self) -> &'static str {
        "sns"
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let mut params = QueryParams::parse(&req.query);
        params.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
        let Some(action) = params.get("Action").map(str::to_string) else {
            return Ok(query_error(
                NAMESPACE,
                &AwsError::missing_parameter("Action"),
                &ctx.request_id,
            ));
        };
        Ok(dispatch(&*self.0, ctx, &action, &params)
            .unwrap_or_else(|e| query_error(NAMESPACE, &e, &ctx.request_id)))
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
