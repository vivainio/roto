diesel::table! { queues (id) {
id -> BigInt,
account_id -> Text,
region -> Text,
name -> Text,
attributes -> Text,
tags -> Text,
created_at -> BigInt,
modified_at -> BigInt,
}}
diesel::table! { messages (seq) {
seq -> BigInt,
queue_id -> BigInt,
message_id -> Text,
body -> Text,
md5 -> Text,
attrs -> Text,
md5_attrs -> Nullable<Text>,
sent_at -> BigInt,
visible_at -> BigInt,
receive_count -> BigInt,
first_received_at -> Nullable<BigInt>,
receipt_handle -> Nullable<Text>,
group_id -> Nullable<Text>,
dedup_id -> Nullable<Text>,
sender_id -> Text,
trace_header -> Nullable<Text>,
}}
diesel::table! { receipts (handle) {
handle -> Text,
queue_id -> BigInt,
seq -> Nullable<BigInt>,
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
