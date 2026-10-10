use crate::schema::tables;
use diesel::prelude::*;
#[derive(Queryable, Selectable)]
#[diesel(table_name=tables)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct TableRow {
    pub account_id: String,
    pub region: String,
    pub name: String,
    pub table_id: String,
    pub created_at: i64,
    pub key_schema: String,
    pub attr_defs: String,
    pub gsis: String,
    pub lsis: String,
    pub billing_mode: String,
    pub throughput: Option<String>,
    pub stream_spec: Option<String>,
    pub tags: String,
    pub ttl_attr: Option<String>,
    pub ttl_enabled: i64,
    pub deletion_protection: i64,
    pub sse: Option<String>,
    pub table_class: Option<String>,
    pub pitr: i64,
}
