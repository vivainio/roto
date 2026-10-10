diesel::table! { topics (arn) {
arn -> Text,
account_id -> Text,
region -> Text,
name -> Text,
attributes -> Text,
tags -> Text,
created_at -> BigInt,
seq -> BigInt,
}}
diesel::table! { subscriptions (seq) {
arn -> Text,
topic_arn -> Text,
account_id -> Text,
region -> Text,
protocol -> Text,
endpoint -> Text,
attributes -> Text,
confirmed -> BigInt,
token -> Nullable<Text>,
seq -> BigInt,
}}
