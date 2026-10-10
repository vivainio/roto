diesel::table! { buses (account,region,name) {
account -> Text,
region -> Text,
name -> Text,
config -> Text,
}}
diesel::table! { rules (account,region,bus,name) {
account -> Text,
region -> Text,
bus -> Text,
name -> Text,
config -> Text,
}}
diesel::table! { targets (account,region,bus,rule,id) {
account -> Text,
region -> Text,
bus -> Text,
rule -> Text,
id -> Text,
config -> Text,
}}
diesel::table! { deliveries (id) {
id -> Text,
account -> Text,
region -> Text,
endpoint -> Text,
target -> Text,
config -> Text,
event -> Text,
attempts -> Integer,
due -> BigInt,
state -> Text,
error -> Nullable<Text>,
}}
