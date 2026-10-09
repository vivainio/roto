//! IAM (query protocol). Types and dispatch are generated from the botocore model into
//! `generated.rs`; entities live in `iam.db`. IAM is global per account (region only selects the
//! partition used in ARNs).

#[allow(clippy::all)]
mod generated;
mod groups;
mod instance_profiles;
mod misc;
mod policies;
mod roles;
mod service;
mod users;
mod util;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{QueryParams, query_error};

pub use generated::{NAMESPACE, OPERATIONS, Service, dispatch};
pub use service::{Iam, KeyOwner};

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
-- Names are unique per account and case-insensitive, as in AWS.
CREATE TABLE users (
    account_id TEXT NOT NULL, name TEXT NOT NULL COLLATE NOCASE, path TEXT NOT NULL,
    user_id TEXT NOT NULL, created_at INTEGER NOT NULL, permissions_boundary TEXT,
    password_last_used INTEGER,
    PRIMARY KEY (account_id, name)
);
CREATE TABLE groups (
    account_id TEXT NOT NULL, name TEXT NOT NULL COLLATE NOCASE, path TEXT NOT NULL,
    group_id TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, name)
);
CREATE TABLE group_members (
    account_id TEXT NOT NULL, group_name TEXT NOT NULL COLLATE NOCASE, user_name TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (account_id, group_name, user_name)
);
CREATE TABLE roles (
    account_id TEXT NOT NULL, name TEXT NOT NULL COLLATE NOCASE, path TEXT NOT NULL,
    role_id TEXT NOT NULL, created_at INTEGER NOT NULL, assume_role_policy TEXT NOT NULL,
    description TEXT, max_session_duration INTEGER NOT NULL DEFAULT 3600, permissions_boundary TEXT,
    last_used_at INTEGER, last_used_region TEXT,
    PRIMARY KEY (account_id, name)
);
CREATE TABLE policies (
    account_id TEXT NOT NULL, arn TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL COLLATE NOCASE,
    path TEXT NOT NULL, policy_id TEXT NOT NULL, description TEXT, created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL, default_version TEXT NOT NULL, next_version INTEGER NOT NULL DEFAULT 2
);
CREATE TABLE policy_versions (
    policy_arn TEXT NOT NULL REFERENCES policies(arn) ON DELETE CASCADE, version_id TEXT NOT NULL,
    document TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY (policy_arn, version_id)
);
-- kind is user | group | role
CREATE TABLE inline_policies (
    account_id TEXT NOT NULL, kind TEXT NOT NULL, entity TEXT NOT NULL COLLATE NOCASE,
    name TEXT NOT NULL, document TEXT NOT NULL,
    PRIMARY KEY (account_id, kind, entity, name)
);
CREATE TABLE attachments (
    account_id TEXT NOT NULL, kind TEXT NOT NULL, entity TEXT NOT NULL COLLATE NOCASE,
    policy_arn TEXT NOT NULL,
    PRIMARY KEY (account_id, kind, entity, policy_arn)
);
CREATE TABLE access_keys (
    access_key_id TEXT NOT NULL PRIMARY KEY, account_id TEXT NOT NULL,
    user_name TEXT NOT NULL COLLATE NOCASE, secret TEXT NOT NULL, status TEXT NOT NULL,
    created_at INTEGER NOT NULL, last_used_at INTEGER, last_used_service TEXT, last_used_region TEXT
);
CREATE INDEX access_keys_user ON access_keys (account_id, user_name);
-- kind is user | role | policy | instance-profile; entity is the name (policy: ARN)
CREATE TABLE tags (
    account_id TEXT NOT NULL, kind TEXT NOT NULL, entity TEXT NOT NULL COLLATE NOCASE,
    key TEXT NOT NULL, value TEXT NOT NULL, seq INTEGER PRIMARY KEY AUTOINCREMENT,
    UNIQUE (account_id, kind, entity, key)
);
CREATE TABLE instance_profiles (
    account_id TEXT NOT NULL, name TEXT NOT NULL COLLATE NOCASE, path TEXT NOT NULL,
    profile_id TEXT NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, name)
);
CREATE TABLE profile_roles (
    account_id TEXT NOT NULL, profile TEXT NOT NULL COLLATE NOCASE, role TEXT NOT NULL COLLATE NOCASE,
    PRIMARY KEY (account_id, profile, role)
);
CREATE TABLE account_aliases (account_id TEXT NOT NULL PRIMARY KEY, alias TEXT NOT NULL);
",
}];

pub struct IamHandler(pub Arc<Iam>);

impl IamHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Iam::new(store)?)))
    }
}

impl ServiceHandler for IamHandler {
    fn service(&self) -> &'static str {
        "iam"
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

    fn resolve_account(&self, access_key: &str) -> Option<String> {
        self.0.key_owner(access_key).map(|o| o.account_id)
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}
