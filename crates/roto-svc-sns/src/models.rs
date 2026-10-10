use crate::schema::*;
use diesel::prelude::*;
#[derive(Queryable, Selectable)]
#[diesel(table_name=topics)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct TopicRow {
    pub arn: String,
    pub account_id: String,
    pub region: String,
    pub name: String,
    pub attributes: String,
    pub tags: String,
}
#[derive(Queryable, Selectable)]
#[diesel(table_name=subscriptions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SubRow {
    pub arn: String,
    pub topic_arn: String,
    pub account_id: String,
    pub protocol: String,
    pub endpoint: String,
    pub attributes: String,
    pub confirmed: i64,
    pub token: Option<String>,
}
