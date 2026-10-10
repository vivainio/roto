use crate::schema::*;
use diesel::prelude::*;
#[derive(Queryable, Selectable)]
#[diesel(table_name = secrets)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SecretRow {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub kms_key_id: Option<String>,
    pub created_at: i64,
    pub changed_at: i64,
    pub accessed_at: Option<i64>,
    pub deleted_at: Option<i64>,
    pub tags: String,
    pub policy: Option<String>,
    pub rotation_enabled: i64,
    pub rotation_lambda_arn: Option<String>,
    pub rotation_rules: Option<String>,
    pub last_rotated_at: Option<i64>,
}
#[derive(Queryable, Selectable)]
#[diesel(table_name = secret_versions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct VersionRow {
    pub version_id: String,
    pub secret_string: Option<String>,
    pub secret_binary: Option<Vec<u8>>,
    pub stages: String,
    pub created_at: i64,
}
