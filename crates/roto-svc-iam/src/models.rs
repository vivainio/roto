//! Stored IAM records. API models stay separate from the SQLite schema.
use crate::schema::{groups, instance_profiles, policies, policy_versions};
use diesel::prelude::*;

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct GroupRow {
    pub account_id: String,
    pub name: String,
    pub path: String,
    pub group_id: String,
    pub created_at: i64,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = instance_profiles)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ProfileRow {
    pub account_id: String,
    pub name: String,
    pub path: String,
    pub profile_id: String,
    pub created_at: i64,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = policies)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PolicyRow {
    pub account_id: String,
    pub arn: String,
    pub name: String,
    pub path: String,
    pub policy_id: String,
    pub description: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub default_version: String,
    pub next_version: i64,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = policy_versions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PolicyVersionRow {
    pub policy_arn: String,
    pub version_id: String,
    pub document: String,
    pub created_at: i64,
}
