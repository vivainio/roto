diesel::table! { functions (arn) {
arn -> Text,
config -> Text,
code -> Text,
tags -> Text,
policy -> Text,
}}
diesel::table! { invocations (id) {
id -> Text,
arn -> Text,
job -> Text,
state -> Text,
attempts -> Integer,
due -> BigInt,
result -> Nullable<Text>,
logs -> Nullable<Text>,
created -> BigInt,
origin -> Text,
}}
diesel::table! { event_source_mappings (uuid) {
uuid -> Text,
account -> Text,
region -> Text,
source -> Text,
function -> Text,
config -> Text,
}}

#[diesel::declare_sql_function]
extern "SQL" {
    fn json_set(
        value: diesel::sql_types::Text,
        path: diesel::sql_types::Text,
        replacement: diesel::sql_types::Text,
    ) -> diesel::sql_types::Text;
}
