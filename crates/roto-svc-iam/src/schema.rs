//! Diesel table definitions for the existing IAM SQLite schema.
use roto_core::diesel;

diesel::table! {
    users (account_id, name) {
        account_id -> Text,
        name -> Text,
        path -> Text,
        user_id -> Text,
        created_at -> BigInt,
        permissions_boundary -> Nullable<Text>,
        password_last_used -> Nullable<BigInt>,
    }
}

diesel::table! {
    groups (account_id, name) {
        account_id -> Text,
        name -> Text,
        path -> Text,
        group_id -> Text,
        created_at -> BigInt,
    }
}

diesel::table! {
    group_members (account_id, group_name, user_name) {
        account_id -> Text,
        group_name -> Text,
        user_name -> Text,
    }
}

diesel::table! {
    roles (account_id, name) {
        account_id -> Text,
        name -> Text,
        path -> Text,
        role_id -> Text,
        created_at -> BigInt,
        assume_role_policy -> Text,
        description -> Nullable<Text>,
        max_session_duration -> Integer,
        permissions_boundary -> Nullable<Text>,
        last_used_at -> Nullable<BigInt>,
        last_used_region -> Nullable<Text>,
    }
}

diesel::table! {
    policies (arn) {
        account_id -> Text,
        arn -> Text,
        name -> Text,
        path -> Text,
        policy_id -> Text,
        description -> Nullable<Text>,
        created_at -> BigInt,
        updated_at -> BigInt,
        default_version -> Text,
        next_version -> BigInt,
    }
}

diesel::table! {
    policy_versions (policy_arn, version_id) {
        policy_arn -> Text,
        version_id -> Text,
        document -> Text,
        created_at -> BigInt,
    }
}

diesel::table! {
    inline_policies (account_id, kind, entity, name) {
        account_id -> Text,
        kind -> Text,
        entity -> Text,
        name -> Text,
        document -> Text,
    }
}

diesel::table! {
    attachments (account_id, kind, entity, policy_arn) {
        account_id -> Text,
        kind -> Text,
        entity -> Text,
        policy_arn -> Text,
    }
}

diesel::table! {
    access_keys (access_key_id) {
        access_key_id -> Text,
        account_id -> Text,
        user_name -> Text,
        secret -> Text,
        status -> Text,
        created_at -> BigInt,
        last_used_at -> Nullable<BigInt>,
        last_used_service -> Nullable<Text>,
        last_used_region -> Nullable<Text>,
    }
}

diesel::table! {
    tags (seq) {
        account_id -> Text,
        kind -> Text,
        entity -> Text,
        key -> Text,
        value -> Text,
        seq -> BigInt,
    }
}

diesel::table! {
    instance_profiles (account_id, name) {
        account_id -> Text,
        name -> Text,
        path -> Text,
        profile_id -> Text,
        created_at -> BigInt,
    }
}

diesel::table! {
    profile_roles (account_id, profile, role) {
        account_id -> Text,
        profile -> Text,
        role -> Text,
    }
}

diesel::table! {
    account_aliases (account_id) {
        account_id -> Text,
        alias -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    users,
    groups,
    group_members,
    roles,
    policies,
    policy_versions,
    inline_policies,
    attachments,
    access_keys,
    tags,
    instance_profiles,
    profile_roles,
    account_aliases
);

// Keep prefix filters literal and case-sensitive, matching the existing SQL.
use diesel::sql_types::{Integer, Text};
#[diesel::declare_sql_function]
extern "SQL" {
    fn substr(value: Text, start: Integer, len: Integer) -> Text;
    fn length(value: Text) -> Integer;
}
