diesel::table! {
    sessions (access_key_id) {
        access_key_id -> Text,
        account_id -> Text,
        role_arn -> Text,
        session_name -> Text,
        role_id -> Text,
        expires_at -> BigInt,
    }
}
