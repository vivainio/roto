use crate::schema::objects;
use diesel::prelude::*;
#[derive(Queryable, Selectable)]
#[diesel(table_name=objects)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ObjectRow {
    pub seq: i64,
    pub key: String,
    pub version_id: String,
    pub is_latest: i64,
    pub delete_marker: i64,
    pub size: i64,
    pub etag: String,
    pub content_type: String,
    pub last_modified: i64,
    pub path: String,
    pub metadata: String,
    pub headers: String,
    pub tags: String,
    pub storage_class: String,
    pub acl: Option<String>,
}
