diesel::table! {
 stacks (account_id, region, name) {
 account_id -> Text,
 region -> Text,
 name -> Text,
 stack_id -> Text,
 body -> Text,
 }
}
