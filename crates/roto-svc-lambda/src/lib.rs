//! Lambda metadata and invocation through locally configured command/HTTP executors.
mod executor;
#[allow(clippy::all)]
mod generated;
mod service;
#[cfg(test)]
mod tests;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use std::sync::Arc;

pub use executor::{Executor, Executors};
pub use generated::{OPERATIONS, Service};
pub use service::Lambda;

pub const IMPLEMENTED: &[&str] = &[
    "CreateFunction",
    "GetFunction",
    "GetFunctionConfiguration",
    "ListFunctions",
    "UpdateFunctionConfiguration",
    "UpdateFunctionCode",
    "DeleteFunction",
    "Invoke",
    "AddPermission",
    "RemovePermission",
    "GetPolicy",
    "TagResource",
    "UntagResource",
    "ListTags",
];

const MIGRATIONS: &[Migration] = &[Migration { version: 1, sql: "
CREATE TABLE functions (arn TEXT PRIMARY KEY, config TEXT NOT NULL, code TEXT NOT NULL, tags TEXT NOT NULL, policy TEXT NOT NULL DEFAULT '[]');
CREATE TABLE invocations (id TEXT PRIMARY KEY, arn TEXT NOT NULL, job TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, result TEXT, logs TEXT, created INTEGER NOT NULL);
CREATE INDEX invocation_queue ON invocations(state, due, created);
" }];

pub struct LambdaHandler(pub Arc<Lambda>);
impl LambdaHandler {
    pub fn new(store: &Store, executors: Executors) -> Result<Self, AwsError> {
        let lambda = Arc::new(Lambda::new(store, executors)?);
        Lambda::start_worker(&lambda);
        Ok(Self(lambda))
    }
}
impl ServiceHandler for LambdaHandler {
    fn service(&self) -> &'static str {
        "lambda"
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        req.path.starts_with("/2015-03-31/")
            || req.path.starts_with("/2017-03-31/tags/")
            || req.path.starts_with("/roto-api/lambda/")
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let result = if req.method == "GET" && req.path == "/roto-api/lambda/invocations" {
            self.0.history(ctx).map(|value| RawResponse {
                status: 200,
                headers: vec![],
                body: serde_json::to_vec(&value).unwrap(),
            })
        } else {
            generated::dispatch_http(&*self.0, ctx, req)
        };
        Ok(result.unwrap_or_else(|e| RawResponse {
            status: e.status,
            headers: vec![("content-type".into(), "application/json".into()), ("x-amzn-errortype".into(), e.code.clone()), ("x-amzn-requestid".into(), ctx.request_id.clone())],
            body: serde_json::to_vec(&serde_json::json!({"Type": if e.sender { "User" } else { "Service" }, "message": e.message})).unwrap(),
        }))
    }
    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
