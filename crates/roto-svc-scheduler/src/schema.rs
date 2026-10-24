diesel::table! { schedule_groups (account,region,name) {
account -> Text,
region -> Text,
name -> Text,
config -> Text,
}}
diesel::table! { schedules (account,region,grp,name) {
account -> Text,
region -> Text,
grp -> Text,
name -> Text,
config -> Text,
endpoint -> Text,
next_fire -> Nullable<BigInt>,
anchor -> BigInt,
}}
diesel::table! { deliveries (id) {
id -> Text,
account -> Text,
region -> Text,
endpoint -> Text,
target -> Text,
config -> Text,
schedule_arn -> Text,
scheduled -> BigInt,
attempts -> Integer,
due -> BigInt,
state -> Text,
error -> Nullable<Text>,
}}
