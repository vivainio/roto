//! Lambda metadata and invocation through locally configured command/HTTP executors.
mod event_sources;
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
use roto_protocol::{FromJson, ToJson};

pub fn dispatch(
    svc: &Lambda,
    ctx: &RequestContext,
    operation: &str,
    body: &serde_json::Value,
) -> Result<RawResponse, AwsError> {
    if operation == "UpdateEventSourceMapping" {
        let input = generated::UpdateEventSourceMappingRequest::from_json(body, "")?;
        let responses = body
            .get("FunctionResponseTypes")
            .map(|_| input.function_response_types.clone());
        let output = svc.update_mapping_input(ctx, input, responses)?;
        Ok(RawResponse {
            status: 200,
            headers: vec![],
            body: serde_json::to_vec(&output.to_json()).unwrap(),
        })
    } else {
        generated::dispatch(svc, ctx, operation, body)
    }
}
pub use service::Lambda;

pub const IMPLEMENTED: &[&str] = &[
    "CreateEventSourceMapping",
    "GetEventSourceMapping",
    "ListEventSourceMappings",
    "UpdateEventSourceMapping",
    "DeleteEventSourceMapping",
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
" }, Migration { version: 2, sql: "
ALTER TABLE invocations ADD COLUMN origin TEXT NOT NULL DEFAULT 'async';
CREATE TABLE event_source_mappings (uuid TEXT PRIMARY KEY, account TEXT NOT NULL, region TEXT NOT NULL, source TEXT NOT NULL, function TEXT NOT NULL, config TEXT NOT NULL, UNIQUE(account,region,source,function));
" }];

pub struct LambdaHandler(pub Arc<Lambda>);
impl LambdaHandler {
    pub fn unstarted(
        store: &Store,
        executors: Executors,
        sqs: Arc<roto_svc_sqs::Sqs>,
    ) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Lambda::new(store, executors)?.with_sqs(sqs))))
    }
    pub fn start(&self, endpoint: String) {
        Lambda::start_worker(&self.0);
        Lambda::start_sqs_worker(&self.0, endpoint);
    }
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
            roto_protocol::restjson::decode(generated::ROUTES, req).and_then(|(route, body)| {
                let response = dispatch(&self.0, ctx, route.operation, &body)?;
                roto_protocol::restjson::encode(route, response)
            })
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
