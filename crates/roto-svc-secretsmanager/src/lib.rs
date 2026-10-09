//! Secrets Manager (JSON 1.1). Types and dispatch are generated from the botocore model;
//! secrets and their versions live in `secretsmanager.db`.

#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

#[allow(clippy::all)]
mod generated;
mod secrets;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::json_error;
use serde_json::Value;

pub use generated::{OPERATIONS, Service, dispatch};
pub use secrets::SecretsManager;

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE secrets (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    arn TEXT NOT NULL,
    description TEXT,
    kms_key_id TEXT,
    created_at INTEGER NOT NULL,
    changed_at INTEGER NOT NULL,
    accessed_at INTEGER,
    deleted_at INTEGER,
    tags TEXT NOT NULL DEFAULT '[]',
    policy TEXT,
    rotation_enabled INTEGER NOT NULL DEFAULT 0,
    rotation_lambda_arn TEXT,
    rotation_rules TEXT,
    last_rotated_at INTEGER,
    PRIMARY KEY (account_id, region, name)
);
CREATE TABLE secret_versions (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    secret_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    secret_string TEXT,
    secret_binary BLOB,
    stages TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL,
    UNIQUE (account_id, region, secret_name, version_id)
);
",
}];

/// Operations with real behaviour; the rest answer `NotImplemented`.
pub const IMPLEMENTED: &[&str] = &[
    "CreateSecret",
    "GetSecretValue",
    "PutSecretValue",
    "UpdateSecret",
    "DescribeSecret",
    "ListSecrets",
    "DeleteSecret",
    "RestoreSecret",
    "ListSecretVersionIds",
    "UpdateSecretVersionStage",
    "TagResource",
    "UntagResource",
    "GetRandomPassword",
    "PutResourcePolicy",
    "GetResourcePolicy",
    "DeleteResourcePolicy",
    "ValidateResourcePolicy",
    "RotateSecret",
    "CancelRotateSecret",
    "BatchGetSecretValue",
];

pub struct SecretsManagerHandler(pub Arc<SecretsManager>);

impl SecretsManagerHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(SecretsManager::new(store)?)))
    }
}

impl ServiceHandler for SecretsManagerHandler {
    fn service(&self) -> &'static str {
        "secretsmanager"
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
