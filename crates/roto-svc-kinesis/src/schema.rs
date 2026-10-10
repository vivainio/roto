diesel::table! { streams (arn) {
arn -> Text,
account_id -> Text,
region -> Text,
name -> Text,
metadata -> Text,
}}
diesel::table! { records (stream_arn, shard_id, sequence) {
stream_arn -> Text,
shard_id -> Text,
sequence -> BigInt,
data -> Binary,
partition_key -> Text,
arrived -> Double,
}}
diesel::table! { shard_sequences (stream_arn, shard_id) {
stream_arn -> Text,
shard_id -> Text,
sequence -> BigInt,
}}
diesel::table! { tokens (token) {
token -> Text,
account_id -> Text,
region -> Text,
kind -> Text,
payload -> Text,
expires -> Double,
}}

#[diesel::declare_sql_function]
extern "SQL" {
    fn json_extract(
        value: diesel::sql_types::Text,
        path: diesel::sql_types::Text,
    ) -> diesel::sql_types::Nullable<diesel::sql_types::Text>;
}
