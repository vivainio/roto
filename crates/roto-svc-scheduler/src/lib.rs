//! EventBridge Scheduler: schedule groups, schedules, and persistent firing to local
//! Lambda/SQS targets.
#[allow(clippy::all)]
mod generated;
mod schedule;
mod schema;
mod service;

pub use generated::{OPERATIONS, ROUTES, Service, dispatch};
use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
pub use service::Scheduler;
use std::sync::Arc;

pub const IMPLEMENTED: &[&str] = &[
    "CreateScheduleGroup",
    "GetScheduleGroup",
    "DeleteScheduleGroup",
    "ListScheduleGroups",
    "CreateSchedule",
    "GetSchedule",
    "UpdateSchedule",
    "DeleteSchedule",
    "ListSchedules",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
];
pub(crate) const MIGRATIONS: &[Migration] = &[Migration { version: 1, sql: "
CREATE TABLE schedule_groups (account TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL, PRIMARY KEY(account,region,name));
CREATE TABLE schedules (account TEXT NOT NULL, region TEXT NOT NULL, grp TEXT NOT NULL, name TEXT NOT NULL, config TEXT NOT NULL, endpoint TEXT NOT NULL, next_fire INTEGER, anchor INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account,region,grp,name));
CREATE INDEX due_schedules ON schedules(next_fire);
CREATE TABLE deliveries (id TEXT PRIMARY KEY, account TEXT NOT NULL, region TEXT NOT NULL, endpoint TEXT NOT NULL, target TEXT NOT NULL, config TEXT NOT NULL, schedule_arn TEXT NOT NULL, scheduled INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'queued', error TEXT);
CREATE INDEX pending_schedule_deliveries ON deliveries(state,due);
" }];

pub struct SchedulerHandler(pub Arc<Scheduler>);
impl SchedulerHandler {
    pub fn new(
        store: &Store,
        lambda: Arc<roto_svc_lambda::Lambda>,
        sqs: Arc<roto_svc_sqs::Sqs>,
    ) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Scheduler::new(store, lambda, sqs)?)))
    }
    pub fn start(&self) {
        Scheduler::start(&self.0);
    }
}
impl ServiceHandler for SchedulerHandler {
    fn service(&self) -> &'static str {
        "scheduler"
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        req.path.starts_with("/roto-api/scheduler/")
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let result = if req.method == "GET" && req.path == "/roto-api/scheduler/deliveries" {
            self.0.history(ctx).map(|value| RawResponse {
                status: 200,
                headers: vec![("content-type".into(), "application/json".into())],
                body: serde_json::to_vec(&value).unwrap(),
            })
        } else {
            roto_protocol::restjson::decode(ROUTES, req).and_then(|(route, body)| {
                let response = dispatch(&*self.0, ctx, route.operation, &body)?;
                roto_protocol::restjson::encode(route, response)
            })
        };
        Ok(result.unwrap_or_else(|e| RawResponse {
            status: e.status,
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("x-amzn-errortype".into(), e.code.clone()),
                ("x-amzn-requestid".into(), ctx.request_id.clone()),
            ],
            body: serde_json::to_vec(&serde_json::json!({"Message": e.message})).unwrap(),
        }))
    }
    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
