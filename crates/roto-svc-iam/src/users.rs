use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::misc::*;
use crate::util::*;

pub struct UserRow {
    pub name: String,
    pub path: String,
    pub user_id: String,
    pub created_at: i64,
    pub permissions_boundary: Option<String>,
    pub password_last_used: Option<i64>,
}

pub fn load_user(tx: &Transaction, account: &str, name: &str) -> Result<UserRow, AwsError> {
    tx.query_row(
        "SELECT name, path, user_id, created_at, permissions_boundary, password_last_used
         FROM users WHERE account_id = ?1 AND name = ?2",
        params![account, name],
        |r| {
            Ok(UserRow {
                name: r.get(0)?,
                path: r.get(1)?,
                user_id: r.get(2)?,
                created_at: r.get(3)?,
                permissions_boundary: r.get(4)?,
                password_last_used: r.get(5)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| no_such("user", name))
}

pub fn user_out(tx: &Transaction, ctx: &RequestContext, u: &UserRow) -> Result<User, AwsError> {
    Ok(User {
        arn: arn(ctx, &format!("user{}{}", u.path, u.name)),
        create_date: ts(u.created_at),
        password_last_used: u.password_last_used.map(ts),
        path: u.path.clone(),
        permissions_boundary: u.permissions_boundary.as_ref().map(|a| {
            AttachedPermissionsBoundary {
                permissions_boundary_arn: Some(a.clone()),
                permissions_boundary_type: Some("Policy".into()),
            }
        }),
        tags: load_tags(tx, &ctx.account_id, "user", &u.name)?,
        user_id: u.user_id.clone(),
        user_name: u.name.clone(),
    })
}

pub fn create_user(
    db: &Db,
    ctx: &RequestContext,
    input: CreateUserRequest,
) -> Result<CreateUserResponse, AwsError> {
    let path = normalize_path(input.path.as_deref())?;
    db.transaction(|tx| {
        let taken = tx
            .query_row(
                "SELECT 1 FROM users WHERE account_id = ?1 AND name = ?2",
                params![ctx.account_id, input.user_name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if taken {
            return Err(exists("User", &input.user_name));
        }
        check_tags(0, &input.tags)?;
        tx.execute(
            "INSERT INTO users (account_id, name, path, user_id, created_at, permissions_boundary)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ctx.account_id,
                input.user_name,
                path,
                gen_id("AIDA", 17),
                now(),
                input.permissions_boundary
            ],
        )?;
        set_tags(tx, &ctx.account_id, "user", &input.user_name, &input.tags)?;
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        Ok(CreateUserResponse {
            user: Some(user_out(tx, ctx, &u)?),
        })
    })
}

pub fn get_user(
    db: &Db,
    ctx: &RequestContext,
    input: GetUserRequest,
) -> Result<GetUserResponse, AwsError> {
    db.transaction(|tx| {
        let name = match &input.user_name {
            Some(n) => n.clone(),
            // No name: the caller's own user, found through the signing access key.
            None => ctx
                .access_key
                .as_deref()
                .and_then(|k| {
                    tx.query_row(
                        "SELECT user_name FROM access_keys WHERE access_key_id = ?1",
                        params![k],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                    .ok()
                    .flatten()
                })
                .ok_or_else(|| no_such("user", "default"))?,
        };
        let u = load_user(tx, &ctx.account_id, &name)?;
        Ok(GetUserResponse {
            user: user_out(tx, ctx, &u)?,
        })
    })
}

pub fn list_users(
    db: &Db,
    ctx: &RequestContext,
    input: ListUsersRequest,
) -> Result<ListUsersResponse, AwsError> {
    db.transaction(|tx| {
        let prefix = input.path_prefix.clone().unwrap_or_else(|| "/".into());
        let names: Vec<String> = {
            let mut stmt = tx.prepare(
                "SELECT name FROM users WHERE account_id = ?1 AND substr(path, 1, length(?2)) = ?2 ORDER BY name COLLATE NOCASE",
            )?;
            stmt.query_map(params![ctx.account_id, prefix], |r| r.get(0))?.collect::<Result<_, _>>()?
        };
        let (page, truncated, marker) = paginate(names, input.marker.as_deref(), input.max_items)?;
        let users = page
            .iter()
            .map(|n| user_out(tx, ctx, &load_user(tx, &ctx.account_id, n)?))
            .collect::<Result<_, _>>()?;
        Ok(ListUsersResponse { users, is_truncated: Some(truncated), marker })
    })
}

pub fn delete_user(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteUserRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        let count = |sql: &str| -> Result<i64, AwsError> {
            Ok(tx.query_row(sql, params![ctx.account_id, u.name], |r| r.get(0))?)
        };
        if count("SELECT COUNT(*) FROM access_keys WHERE account_id = ?1 AND user_name = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must delete access keys first."));
        }
        if count("SELECT COUNT(*) FROM inline_policies WHERE account_id = ?1 AND kind = 'user' AND entity = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must delete policies first."));
        }
        if count("SELECT COUNT(*) FROM attachments WHERE account_id = ?1 AND kind = 'user' AND entity = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must detach all policies first."));
        }
        tx.execute("DELETE FROM group_members WHERE account_id = ?1 AND user_name = ?2", params![ctx.account_id, u.name])?;
        tx.execute("DELETE FROM tags WHERE account_id = ?1 AND kind = 'user' AND entity = ?2", params![ctx.account_id, u.name])?;
        tx.execute("DELETE FROM users WHERE account_id = ?1 AND name = ?2", params![ctx.account_id, u.name])?;
        Ok(())
    })
}

pub fn update_user(
    db: &Db,
    ctx: &RequestContext,
    input: UpdateUserRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        let path = match &input.new_path {
            Some(p) => normalize_path(Some(p))?,
            None => u.path.clone(),
        };
        let new_name = input.new_user_name.clone().unwrap_or_else(|| u.name.clone());
        if !new_name.eq_ignore_ascii_case(&u.name) && load_user(tx, &ctx.account_id, &new_name).is_ok() {
            return Err(exists("User", &new_name));
        }
        tx.execute(
            "UPDATE users SET name = ?1, path = ?2 WHERE account_id = ?3 AND name = ?4",
            params![new_name, path, ctx.account_id, u.name],
        )?;
        // Rename everything that refers to the user by name.
        for sql in [
            "UPDATE access_keys SET user_name = ?1 WHERE account_id = ?2 AND user_name = ?3",
            "UPDATE group_members SET user_name = ?1 WHERE account_id = ?2 AND user_name = ?3",
            "UPDATE inline_policies SET entity = ?1 WHERE account_id = ?2 AND kind = 'user' AND entity = ?3",
            "UPDATE attachments SET entity = ?1 WHERE account_id = ?2 AND kind = 'user' AND entity = ?3",
            "UPDATE tags SET entity = ?1 WHERE account_id = ?2 AND kind = 'user' AND entity = ?3",
        ] {
            tx.execute(sql, params![new_name, ctx.account_id, u.name])?;
        }
        Ok(())
    })
}

pub fn tag_user(db: &Db, ctx: &RequestContext, input: TagUserRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        set_tags(tx, &ctx.account_id, "user", &u.name, &input.tags)
    })
}

pub fn untag_user(db: &Db, ctx: &RequestContext, input: UntagUserRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        remove_tags(tx, &ctx.account_id, "user", &u.name, &input.tag_keys)
    })
}

pub fn list_user_tags(
    db: &Db,
    ctx: &RequestContext,
    input: ListUserTagsRequest,
) -> Result<ListUserTagsResponse, AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        let tags = load_tags(tx, &ctx.account_id, "user", &u.name)?;
        let (tags, truncated, marker) = paginate(tags, input.marker.as_deref(), input.max_items)?;
        Ok(ListUserTagsResponse {
            tags,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

// ---- access keys -------------------------------------------------------------------------

/// The user an access-key request applies to: the named user, else the caller's own.
fn key_user(
    tx: &Transaction,
    ctx: &RequestContext,
    name: &Option<String>,
) -> Result<String, AwsError> {
    match name {
        Some(n) => Ok(load_user(tx, &ctx.account_id, n)?.name),
        None => ctx
            .access_key
            .as_deref()
            .and_then(|k| {
                tx.query_row(
                    "SELECT user_name FROM access_keys WHERE access_key_id = ?1",
                    params![k],
                    |r| r.get::<_, String>(0),
                )
                .optional()
                .ok()
                .flatten()
            })
            .ok_or_else(|| no_such("user", "default")),
    }
}

pub fn create_access_key(
    db: &Db,
    ctx: &RequestContext,
    input: CreateAccessKeyRequest,
) -> Result<CreateAccessKeyResponse, AwsError> {
    db.transaction(|tx| {
        let user = key_user(tx, ctx, &input.user_name)?;
        let n: i64 = tx.query_row(
            "SELECT COUNT(*) FROM access_keys WHERE account_id = ?1 AND user_name = ?2",
            params![ctx.account_id, user],
            |r| r.get(0),
        )?;
        if n >= 2 {
            return Err(AwsError::sender(
                409,
                "LimitExceeded",
                "Cannot exceed quota for AccessKeysPerUser: 2",
            ));
        }
        let (id, secret, created) = (gen_id("AKIA", 16), gen_secret(), now());
        tx.execute(
            "INSERT INTO access_keys (access_key_id, account_id, user_name, secret, status, created_at)
             VALUES (?1, ?2, ?3, ?4, 'Active', ?5)",
            params![id, ctx.account_id, user, secret, created],
        )?;
        Ok(CreateAccessKeyResponse {
            access_key: AccessKey {
                access_key_id: id,
                create_date: Some(ts(created)),
                secret_access_key: secret,
                status: "Active".into(),
                user_name: user,
            },
        })
    })
}

pub fn list_access_keys(
    db: &Db,
    ctx: &RequestContext,
    input: ListAccessKeysRequest,
) -> Result<ListAccessKeysResponse, AwsError> {
    db.transaction(|tx| {
        let user = key_user(tx, ctx, &input.user_name)?;
        let keys: Vec<AccessKeyMetadata> = {
            let mut stmt = tx.prepare(
                "SELECT access_key_id, status, created_at FROM access_keys
                 WHERE account_id = ?1 AND user_name = ?2 ORDER BY created_at, access_key_id",
            )?;
            stmt.query_map(params![ctx.account_id, user], |r| {
                Ok(AccessKeyMetadata {
                    access_key_id: Some(r.get(0)?),
                    status: Some(r.get(1)?),
                    create_date: Some(ts(r.get(2)?)),
                    user_name: Some(user.clone()),
                })
            })?
            .collect::<Result<_, _>>()?
        };
        let (keys, truncated, marker) = paginate(keys, input.marker.as_deref(), input.max_items)?;
        Ok(ListAccessKeysResponse {
            access_key_metadata: keys,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

pub fn update_access_key(
    db: &Db,
    ctx: &RequestContext,
    input: UpdateAccessKeyRequest,
) -> Result<(), AwsError> {
    if !matches!(input.status.as_str(), "Active" | "Inactive") {
        return Err(validation(format!(
            "1 validation error detected: Value '{}' at 'status' failed to satisfy constraint: Member must satisfy enum value set: [Active, Inactive]",
            input.status
        )));
    }
    db.transaction(|tx| {
        let user = key_user(tx, ctx, &input.user_name)?;
        let n = tx.execute(
            "UPDATE access_keys SET status = ?1 WHERE account_id = ?2 AND user_name = ?3 AND access_key_id = ?4",
            params![input.status, ctx.account_id, user, input.access_key_id],
        )?;
        if n == 0 {
            return Err(AwsError::sender(
                404,
                "NoSuchEntity",
                format!("The Access Key with id {} cannot be found.", input.access_key_id),
            ));
        }
        Ok(())
    })
}

pub fn delete_access_key(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteAccessKeyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let user = key_user(tx, ctx, &input.user_name)?;
        let n = tx.execute(
            "DELETE FROM access_keys WHERE account_id = ?1 AND user_name = ?2 AND access_key_id = ?3",
            params![ctx.account_id, user, input.access_key_id],
        )?;
        if n == 0 {
            return Err(AwsError::sender(
                404,
                "NoSuchEntity",
                format!("The Access Key with id {} cannot be found.", input.access_key_id),
            ));
        }
        Ok(())
    })
}

pub fn get_access_key_last_used(
    db: &Db,
    ctx: &RequestContext,
    input: GetAccessKeyLastUsedRequest,
) -> Result<GetAccessKeyLastUsedResponse, AwsError> {
    db.transaction(|tx| {
        let row = tx
            .query_row(
                "SELECT user_name, last_used_at, last_used_service, last_used_region FROM access_keys
                 WHERE account_id = ?1 AND access_key_id = ?2",
                params![ctx.account_id, input.access_key_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?, r.get::<_, Option<String>>(2)?, r.get::<_, Option<String>>(3)?)),
            )
            .optional()?
            .ok_or_else(|| validation(format!("Invalid Access Key ID or Access Key ID not found: {}", input.access_key_id)))?;
        Ok(GetAccessKeyLastUsedResponse {
            user_name: Some(row.0),
            access_key_last_used: Some(AccessKeyLastUsed {
                last_used_date: row.1.map(ts),
                region: row.3.unwrap_or_else(|| "N/A".into()),
                service_name: row.2.unwrap_or_else(|| "N/A".into()),
            }),
        })
    })
}
