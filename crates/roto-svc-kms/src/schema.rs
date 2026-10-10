diesel::table! {
 keys (account, region, id) {
  account -> Text,
  region -> Text,
  id -> Text,
  metadata -> Text,
 }
}
diesel::table! {
 aliases (account, region, name) {
  account -> Text,
  region -> Text,
  name -> Text,
  key_id -> Text,
  created_at -> Double,
 }
}
diesel::allow_tables_to_appear_in_same_query!(keys, aliases);
