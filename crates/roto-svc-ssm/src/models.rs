use crate::schema::{parameters, resource_tags};
use diesel::prelude::*;

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name=parameters)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ParameterRow {
    pub account_id: String,
    pub region: String,
    pub name: String,
    pub version: i64,
    #[diesel(column_name=type_)]
    pub ty: String,
    pub value: String,
    pub description: Option<String>,
    pub allowed_pattern: Option<String>,
    pub key_id: Option<String>,
    pub data_type: String,
    pub tier: String,
    pub policies: Option<String>,
    pub labels: String,
    pub last_modified: i64,
}

#[derive(Insertable)]
#[diesel(table_name=resource_tags)]
pub struct TagInsert<'a> {
    pub account_id: &'a str,
    pub region: &'a str,
    pub resource_type: &'a str,
    pub resource_id: &'a str,
    pub key: &'a str,
    pub value: &'a str,
}
