//! Diesel table definitions for Parameter Store.
diesel::table! {
    parameters (account_id, region, name, version) {
        account_id -> Text,
        region -> Text,
        name -> Text,
        version -> BigInt,
        #[sql_name="type"]
        type_ -> Text,
        value -> Text,
        description -> Nullable<Text>,
        allowed_pattern -> Nullable<Text>,
        key_id -> Nullable<Text>,
        data_type -> Text,
        tier -> Text,
        policies -> Nullable<Text>,
        labels -> Text,
        last_modified -> BigInt,
    }
}

diesel::table! {
    resource_tags (seq) {
        account_id -> Text,
        region -> Text,
        resource_type -> Text,
        resource_id -> Text,
        key -> Text,
        value -> Text,
        seq -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(parameters, resource_tags);
