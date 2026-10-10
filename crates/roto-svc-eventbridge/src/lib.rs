//! EventBridge buses, rules and persistent delivery to local Lambda/SQS targets.
#[allow(clippy::all)]
mod generated;
mod pattern;
mod service;

pub use generated::{OPERATIONS, Service, dispatch};
use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
pub use service::EventBridge;
use std::sync::Arc;

pub const IMPLEMENTED: &[&str] = &[
    "CreateEventBus",
    "DescribeEventBus",
    "DeleteEventBus",
    "ListEventBuses",
    "PutRule",
    "DescribeRule",
    "DeleteRule",
    "ListRules",
    "EnableRule",
    "DisableRule",
    "PutTargets",
    "RemoveTargets",
    "ListTargetsByRule",
    "PutEvents",
    "TestEventPattern",
];
pub(crate) const MIGRATIONS: &[Migration] = &[Migration { version: 1, sql: "
CREATE TABLE buses (account TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL, PRIMARY KEY(account,region,name));
CREATE TABLE rules (account TEXT NOT NULL, region TEXT NOT NULL, bus TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL, PRIMARY KEY(account,region,bus,name));
CREATE TABLE targets (account TEXT NOT NULL, region TEXT NOT NULL, bus TEXT NOT NULL, rule TEXT NOT NULL, id TEXT NOT NULL, config TEXT NOT NULL, PRIMARY KEY(account,region,bus,rule,id), FOREIGN KEY(account,region,bus,rule) REFERENCES rules(account,region,bus,name) ON DELETE CASCADE);
CREATE TABLE deliveries (id TEXT PRIMARY KEY, account TEXT NOT NULL, region TEXT NOT NULL, endpoint TEXT NOT NULL, target TEXT NOT NULL, config TEXT NOT NULL, event TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'queued', error TEXT);
CREATE INDEX pending_deliveries ON deliveries(state,due);
" }];

pub struct EventBridgeHandler(pub Arc<EventBridge>);
impl EventBridgeHandler {
    pub fn new(
        store: &Store,
        lambda: Arc<roto_svc_lambda::Lambda>,
        sqs: Arc<roto_svc_sqs::Sqs>,
    ) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(EventBridge::new(store, lambda, sqs)?)))
    }
    pub fn start(&self) {
        EventBridge::start(&self.0);
    }
}
impl ServiceHandler for EventBridgeHandler {
    fn service(&self) -> &'static str {
        "events"
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        req.header("x-amz-target")
            .is_some_and(|t| t.starts_with("AWSEvents."))
            || req.path == "/roto-api/events/deliveries"
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let result = if req.method == "GET" && req.path == "/roto-api/events/deliveries" {
            self.0
                .history(ctx)
                .map(|v| roto_protocol::json_response("1.1", &ctx.request_id, &v))
        } else {
            serde_json::from_slice(&req.body)
                .map_err(|e| AwsError::sender(400, "SerializationException", e.to_string()))
                .and_then(|body| {
                    let target = req
                        .header("x-amz-target")
                        .ok_or_else(|| AwsError::missing_parameter("X-Amz-Target"))?;
                    dispatch(
                        &*self.0,
                        ctx,
                        target.rsplit('.').next().unwrap_or(target),
                        &body,
                    )
                })
        };
        Ok(result.unwrap_or_else(|e| roto_protocol::json_error("1.1", &e, &ctx.request_id, false)))
    }
    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
