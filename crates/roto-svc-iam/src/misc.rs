//! Tags (shared by users, roles, policies, instance profiles) and account aliases.

use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::util::*;

pub fn load_tags(
    tx: &Transaction,
    account: &str,
    kind: &str,
    entity: &str,
) -> Result<Vec<Tag>, AwsError> {
    let mut stmt = tx.prepare(
        "SELECT key, value FROM tags WHERE account_id = ?1 AND kind = ?2 AND entity = ?3 ORDER BY seq",
    )?;
    Ok(stmt
        .query_map(params![account, kind, entity], |r| {
            Ok(Tag {
                key: r.get(0)?,
                value: r.get(1)?,
            })
        })?
        .collect::<Result<_, _>>()?)
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
    tx: &Transaction,
    account: &str,
    kind: &str,
    entity: &str,
    tags: &[Tag],
) -> Result<(), AwsError> {
    let existing = load_tags(tx, account, kind, entity)?;
    let new_keys = tags
        .iter()
        .filter(|t| !existing.iter().any(|e| e.key.eq_ignore_ascii_case(&t.key)))
        .count();
    check_tags(existing.len(), tags)?;
    let _ = new_keys;
    for t in tags {
        tx.execute(
            "INSERT INTO tags (account_id, kind, entity, key, value) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (account_id, kind, entity, key) DO UPDATE SET value = excluded.value",
            params![account, kind, entity, t.key, t.value],
        )?;
    }
    Ok(())
}

pub fn remove_tags(
    tx: &Transaction,
    account: &str,
    kind: &str,
    entity: &str,
    keys: &[String],
) -> Result<(), AwsError> {
    for k in keys {
        tx.execute(
            "DELETE FROM tags WHERE account_id = ?1 AND kind = ?2 AND entity = ?3 AND key = ?4",
            params![account, kind, entity, k],
        )?;
    }
    Ok(())
}

pub fn create_account_alias(
    db: &Db,
    ctx: &RequestContext,
    input: CreateAccountAliasRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let taken: bool = tx
            .query_row(
                "SELECT 1 FROM account_aliases WHERE alias = ?1",
                params![input.account_alias],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if taken {
            return Err(exists("Account Alias", &input.account_alias));
        }
        tx.execute(
            "INSERT INTO account_aliases (account_id, alias) VALUES (?1, ?2)
             ON CONFLICT (account_id) DO UPDATE SET alias = excluded.alias",
            params![ctx.account_id, input.account_alias],
        )?;
        Ok(())
    })
}

pub fn list_account_aliases(
    db: &Db,
    ctx: &RequestContext,
    _input: ListAccountAliasesRequest,
) -> Result<ListAccountAliasesResponse, AwsError> {
    db.transaction(|tx| {
        let alias: Option<String> = tx
            .query_row(
                "SELECT alias FROM account_aliases WHERE account_id = ?1",
                params![ctx.account_id],
                |r| r.get(0),
            )
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
        let n = tx.execute(
            "DELETE FROM account_aliases WHERE account_id = ?1 AND alias = ?2",
            params![ctx.account_id, input.account_alias],
        )?;
        if n == 0 {
            return Err(no_such("Account Alias", &input.account_alias));
        }
        Ok(())
    })
}
