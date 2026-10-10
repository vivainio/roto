diesel::table! { log_groups (account_id, region, log_group_name) {
account_id -> Text,
region -> Text,
log_group_name -> Text,
creation_time -> BigInt,
retention_in_days -> Nullable<Integer>,
tags -> Text,
stored_bytes -> BigInt,
}}
diesel::table! { log_events (id) {
id -> BigInt,
account_id -> Text,
region -> Text,
log_group_name -> Text,
log_stream_name -> Text,
timestamp -> BigInt,
ingestion_time -> BigInt,
message -> Text,
}}

diesel::allow_tables_to_appear_in_same_query!(log_groups, log_events,);

#[diesel::declare_sql_function]
extern "SQL" {
    fn substr(
        value: diesel::sql_types::Text,
        start: diesel::sql_types::Integer,
        len: diesel::sql_types::Integer,
    ) -> diesel::sql_types::Text;
    fn length(value: diesel::sql_types::Text) -> diesel::sql_types::Integer;
}
