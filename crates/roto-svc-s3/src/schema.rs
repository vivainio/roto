diesel::table! { buckets (name) {
name -> Text,
account_id -> Text,
region -> Text,
created_at -> BigInt,
versioning -> Text,
}}
diesel::table! { bucket_configs (bucket, kind) {
bucket -> Text,
kind -> Text,
body -> Text,
}}
diesel::table! { objects (seq) {
seq -> BigInt,
bucket -> Text,
key -> Text,
version_id -> Text,
is_latest -> BigInt,
delete_marker -> BigInt,
size -> BigInt,
etag -> Text,
content_type -> Text,
last_modified -> BigInt,
path -> Text,
metadata -> Text,
headers -> Text,
tags -> Text,
storage_class -> Text,
acl -> Nullable<Text>,
}}
diesel::table! { uploads (upload_id) {
upload_id -> Text,
bucket -> Text,
key -> Text,
created_at -> BigInt,
attrs -> Text,
}}
diesel::table! { parts (upload_id, part_number) {
upload_id -> Text,
part_number -> Integer,
size -> BigInt,
etag -> Text,
path -> Text,
last_modified -> BigInt,
}}
diesel::table! { notification_outbox (seq) {
seq -> BigInt,
id -> Text,
target -> Text,
context -> Text,
event -> Text,
attempts -> Integer,
due -> BigInt,
error -> Nullable<Text>,
}}

#[diesel::declare_sql_function]
extern "SQL" {
    fn substr(
        value: diesel::sql_types::Text,
        start: diesel::sql_types::Integer,
        len: diesel::sql_types::Integer,
    ) -> diesel::sql_types::Text;
    fn length(value: diesel::sql_types::Text) -> diesel::sql_types::Integer;
}
