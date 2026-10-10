diesel::table! { tables (account_id, region, name) {
account_id -> Text,
region -> Text,
name -> Text,
table_id -> Text,
created_at -> BigInt,
key_schema -> Text,
attr_defs -> Text,
gsis -> Text,
lsis -> Text,
billing_mode -> Text,
throughput -> Nullable<Text>,
stream_spec -> Nullable<Text>,
tags -> Text,
ttl_attr -> Nullable<Text>,
ttl_enabled -> BigInt,
deletion_protection -> BigInt,
sse -> Nullable<Text>,
table_class -> Nullable<Text>,
pitr -> BigInt,
}}
diesel::table! { items (table_id, hk, rk) {
table_id -> Text,
hk -> Binary,
rk -> Binary,
item -> Text,
}}
diesel::table! { backups (arn) {
arn -> Text,
name -> Text,
table_name -> Text,
table_id -> Text,
created_at -> BigInt,
meta -> Text,
}}
diesel::table! { backup_items (arn, hk, rk) {
arn -> Text,
hk -> Binary,
rk -> Binary,
item -> Text,
}}

diesel::allow_tables_to_appear_in_same_query!(tables, items, backups, backup_items);
#[diesel::declare_sql_function]
extern "SQL" {
    fn length(value: diesel::sql_types::Text) -> diesel::sql_types::Integer;
}
