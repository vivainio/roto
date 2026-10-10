//! Tags (shared by users, roles, policies, instance profiles) and account aliases.

use crate::schema::*;
use diesel::SqliteConnection;
use roto_core::diesel::{self, prelude::*};
use roto_core::store::DieselDb as Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::util::*;

pub fn load_tags(
    tx: &mut SqliteConnection,
    account: &str,
    kind: &str,
    entity: &str,
) -> Result<Vec<Tag>, AwsError> {
    Ok(tags::table
        .filter(tags::account_id.eq(account))
        .filter(tags::kind.eq(kind))
        .filter(tags::entity.eq(entity))
        .order(tags::seq)
        .select((tags::key, tags::value))
        .load::<(String, String)>(tx)?
        .into_iter()
        .map(|(key, value)| Tag { key, value })
        .collect())
}

pub fn check_tags(existing: usize, new: &[Tag]) -> Result<(), AwsError> {
    let mut keys = std::collections::BTreeSet::new();
    for t in new {
        if !keys.insert(t.key.to_lowercase()) {
            return Err(validation(
                "Duplicate tag keys found. Please note that Tag keys are case insensitive.",
            ));
        }
        if t.key.is_empty() || t.key.chars().count() > 128 {
            return Err(validation(format!(
                "1 validation error detected: Value '{}' at 'tags.1.member.key' failed to satisfy constraint: Member must have length less than or equal to 128",
                t.key
            )));
        }
        if t.value.chars().count() > 256 {
            return Err(validation(format!(
                "1 validation error detected: Value '{}' at 'tags.1.member.value' failed to satisfy constraint: Member must have length less than or equal to 256",
                t.value
            )));
        }
    }
    if existing + new.len() > 50 {
        return Err(AwsError::sender(
            409,
            "LimitExceeded",
            "Maximum number of tags exceeded",
        ));
    }
    Ok(())
}

pub fn set_tags(
    tx: &mut SqliteConnection,
    account: &str,
    kind: &str,
    entity: &str,
    tags: &[Tag],
) -> Result<(), AwsError> {
    let existing = load_tags(tx, account, kind, entity)?;
    let new_keys = tags
        .iter()
        .filter(|t| !existing.iter().any(|e| e.key == t.key))
        .count();
    check_tags(0, tags)?;
    check_tags(existing.len() + new_keys, &[])?;
    for t in tags {
        diesel::insert_into(tags::table)
            .values((
                tags::account_id.eq(&account),
                tags::kind.eq(&kind),
                tags::entity.eq(&entity),
                tags::key.eq(&t.key),
                tags::value.eq(&t.value),
            ))
            .on_conflict((tags::account_id, tags::kind, tags::entity, tags::key))
            .do_update()
            .set(tags::value.eq(diesel::upsert::excluded(tags::value)))
            .execute(tx)?;
    }
    Ok(())
}

pub fn remove_tags(
    tx: &mut SqliteConnection,
    account: &str,
    kind: &str,
    entity: &str,
    keys: &[String],
) -> Result<(), AwsError> {
    for k in keys {
        diesel::delete(
            tags::table
                .filter(tags::account_id.eq(&account))
                .filter(tags::kind.eq(&kind))
                .filter(tags::entity.eq(&entity))
                .filter(tags::key.eq(&k)),
        )
        .execute(tx)?;
    }
    Ok(())
}

pub fn create_account_alias(
    db: &Db,
    ctx: &RequestContext,
    input: CreateAccountAliasRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let taken = diesel::select(diesel::dsl::exists(
            account_aliases::table.filter(account_aliases::alias.eq(&input.account_alias)),
        ))
        .get_result::<bool>(tx)?;
        if taken {
            return Err(exists("Account Alias", &input.account_alias));
        }
        diesel::insert_into(account_aliases::table)
            .values((
                account_aliases::account_id.eq(&ctx.account_id),
                account_aliases::alias.eq(&input.account_alias),
            ))
            .on_conflict(account_aliases::account_id)
            .do_update()
            .set(account_aliases::alias.eq(diesel::upsert::excluded(account_aliases::alias)))
            .execute(tx)?;
        Ok(())
    })
}

pub fn list_account_aliases(
    db: &Db,
    ctx: &RequestContext,
    _input: ListAccountAliasesRequest,
) -> Result<ListAccountAliasesResponse, AwsError> {
    db.transaction(|tx| {
        let alias = account_aliases::table
            .filter(account_aliases::account_id.eq(&ctx.account_id))
            .select(account_aliases::alias)
            .first::<String>(tx)
            .optional()?;
        Ok(ListAccountAliasesResponse {
            account_aliases: alias.into_iter().collect(),
            is_truncated: Some(false),
            marker: None,
        })
    })
}

pub fn delete_account_alias(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteAccountAliasRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let n = diesel::delete(
            account_aliases::table
                .filter(account_aliases::account_id.eq(&ctx.account_id))
                .filter(account_aliases::alias.eq(&input.account_alias)),
        )
        .execute(tx)?;
        if n == 0 {
            return Err(no_such("Account Alias", &input.account_alias));
        }
        Ok(())
    })
}
