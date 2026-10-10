use crate::schema::*;
use diesel::SqliteConnection;
use roto_core::diesel::{self, prelude::*};
use roto_core::store::DieselDb as Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::misc::*;
use crate::util::*;

#[derive(Queryable, Selectable)]
#[diesel(table_name=users)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct UserRow {
    pub name: String,
    pub path: String,
    pub user_id: String,
    pub created_at: i64,
    pub permissions_boundary: Option<String>,
    pub password_last_used: Option<i64>,
}

pub fn load_user(
    tx: &mut SqliteConnection,
    account: &str,
    name: &str,
) -> Result<UserRow, AwsError> {
    users::table
        .filter(users::account_id.eq(account))
        .filter(users::name.eq(name))
        .select(UserRow::as_select())
        .first::<UserRow>(tx)
        .optional()?
        .ok_or_else(|| no_such("user", name))
}

pub fn user_out(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    u: &UserRow,
) -> Result<User, AwsError> {
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
        let taken = diesel::select(diesel::dsl::exists(
            users::table
                .filter(users::account_id.eq(&ctx.account_id))
                .filter(users::name.eq(&input.user_name)),
        ))
        .get_result::<bool>(tx)?;
        if taken {
            return Err(exists("User", &input.user_name));
        }
        check_tags(0, &input.tags)?;
        diesel::insert_into(users::table)
            .values((
                users::account_id.eq(&ctx.account_id),
                users::name.eq(&input.user_name),
                users::path.eq(&path),
                users::user_id.eq(&gen_id("AIDA", 17)),
                users::created_at.eq(&now()),
                users::permissions_boundary.eq(&input.permissions_boundary),
            ))
            .execute(tx)?;
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
                    access_keys::table
                        .filter(access_keys::access_key_id.eq(&k))
                        .select(access_keys::user_name)
                        .first::<String>(tx)
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
            users::table
                .filter(
                    substr(users::path, 1, length(literal_prefix(Some(&prefix))))
                        .eq(literal_prefix(Some(&prefix))),
                )
                .filter(users::account_id.eq(&ctx.account_id))
                .order(users::name)
                .select(users::name)
                .load::<String>(tx)?
        };
        let (page, truncated, marker) = paginate(names, input.marker.as_deref(), input.max_items)?;
        let users = page
            .iter()
            .map(|n| {
                let row = load_user(tx, &ctx.account_id, n)?;
                user_out(tx, ctx, &row)
            })
            .collect::<Result<_, _>>()?;
        Ok(ListUsersResponse {
            users,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

pub fn delete_user(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteUserRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let u = load_user(tx, &ctx.account_id, &input.user_name)?;
        if access_keys::table
            .filter(access_keys::account_id.eq(&ctx.account_id))
            .filter(access_keys::user_name.eq(&u.name))
            .count()
            .get_result::<i64>(tx)?
            > 0
        {
            return Err(conflict(
                "Cannot delete entity, must delete access keys first.",
            ));
        }
        if inline_policies::table
            .filter(inline_policies::account_id.eq(&ctx.account_id))
            .filter(inline_policies::kind.eq("user"))
            .filter(inline_policies::entity.eq(&u.name))
            .count()
            .get_result::<i64>(tx)?
            > 0
        {
            return Err(conflict(
                "Cannot delete entity, must delete policies first.",
            ));
        }
        if attachments::table
            .filter(attachments::account_id.eq(&ctx.account_id))
            .filter(attachments::kind.eq("user"))
            .filter(attachments::entity.eq(&u.name))
            .count()
            .get_result::<i64>(tx)?
            > 0
        {
            return Err(conflict(
                "Cannot delete entity, must detach all policies first.",
            ));
        }
        diesel::delete(
            group_members::table
                .filter(group_members::account_id.eq(&ctx.account_id))
                .filter(group_members::user_name.eq(&u.name)),
        )
        .execute(tx)?;
        diesel::delete(
            tags::table
                .filter(tags::account_id.eq(&ctx.account_id))
                .filter(tags::kind.eq("user"))
                .filter(tags::entity.eq(&u.name)),
        )
        .execute(tx)?;
        diesel::delete(
            users::table
                .filter(users::account_id.eq(&ctx.account_id))
                .filter(users::name.eq(&u.name)),
        )
        .execute(tx)?;
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
        let new_name = input
            .new_user_name
            .clone()
            .unwrap_or_else(|| u.name.clone());
        if !new_name.eq_ignore_ascii_case(&u.name)
            && load_user(tx, &ctx.account_id, &new_name).is_ok()
        {
            return Err(exists("User", &new_name));
        }
        diesel::update(
            users::table
                .filter(users::account_id.eq(&ctx.account_id))
                .filter(users::name.eq(&u.name)),
        )
        .set((users::name.eq(&new_name), users::path.eq(&path)))
        .execute(tx)?;
        diesel::update(
            access_keys::table
                .filter(access_keys::account_id.eq(&ctx.account_id))
                .filter(access_keys::user_name.eq(&u.name)),
        )
        .set(access_keys::user_name.eq(&new_name))
        .execute(tx)?;
        diesel::update(
            group_members::table
                .filter(group_members::account_id.eq(&ctx.account_id))
                .filter(group_members::user_name.eq(&u.name)),
        )
        .set(group_members::user_name.eq(&new_name))
        .execute(tx)?;
        diesel::update(
            inline_policies::table
                .filter(inline_policies::account_id.eq(&ctx.account_id))
                .filter(inline_policies::kind.eq("user"))
                .filter(inline_policies::entity.eq(&u.name)),
        )
        .set(inline_policies::entity.eq(&new_name))
        .execute(tx)?;
        diesel::update(
            attachments::table
                .filter(attachments::account_id.eq(&ctx.account_id))
                .filter(attachments::kind.eq("user"))
                .filter(attachments::entity.eq(&u.name)),
        )
        .set(attachments::entity.eq(&new_name))
        .execute(tx)?;
        diesel::update(
            tags::table
                .filter(tags::account_id.eq(&ctx.account_id))
                .filter(tags::kind.eq("user"))
                .filter(tags::entity.eq(&u.name)),
        )
        .set(tags::entity.eq(&new_name))
        .execute(tx)?;
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
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &Option<String>,
) -> Result<String, AwsError> {
    match name {
        Some(n) => Ok(load_user(tx, &ctx.account_id, n)?.name),
        None => ctx
            .access_key
            .as_deref()
            .and_then(|k| {
                access_keys::table
                    .filter(access_keys::access_key_id.eq(&k))
                    .select(access_keys::user_name)
                    .first::<String>(tx)
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
        let n: i64 = access_keys::table
            .filter(access_keys::account_id.eq(&ctx.account_id))
            .filter(access_keys::user_name.eq(&user))
            .count()
            .first::<i64>(tx)?;
        if n >= 2 {
            return Err(AwsError::sender(
                409,
                "LimitExceeded",
                "Cannot exceed quota for AccessKeysPerUser: 2",
            ));
        }
        let (id, secret, created) = (gen_id("AKIA", 16), gen_secret(), now());
        diesel::insert_into(access_keys::table)
            .values((
                access_keys::access_key_id.eq(&id),
                access_keys::account_id.eq(&ctx.account_id),
                access_keys::user_name.eq(&user),
                access_keys::secret.eq(&secret),
                access_keys::status.eq("Active"),
                access_keys::created_at.eq(&created),
            ))
            .execute(tx)?;
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
            access_keys::table
                .filter(access_keys::account_id.eq(&ctx.account_id))
                .filter(access_keys::user_name.eq(&user))
                .order((access_keys::created_at, access_keys::access_key_id))
                .select((
                    access_keys::access_key_id,
                    access_keys::status,
                    access_keys::created_at,
                ))
                .load::<(String, String, i64)>(tx)?
                .into_iter()
                .map(|r| AccessKeyMetadata {
                    access_key_id: Some(r.0.clone()),
                    status: Some(r.1.clone()),
                    create_date: Some(ts(r.2)),
                    user_name: Some(user.clone()),
                })
                .collect::<Vec<_>>()
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
        let n = diesel::update(
            access_keys::table
                .filter(access_keys::account_id.eq(&ctx.account_id))
                .filter(access_keys::user_name.eq(&user))
                .filter(access_keys::access_key_id.eq(&input.access_key_id)),
        )
        .set(access_keys::status.eq(&input.status))
        .execute(tx)?;
        if n == 0 {
            return Err(AwsError::sender(
                404,
                "NoSuchEntity",
                format!(
                    "The Access Key with id {} cannot be found.",
                    input.access_key_id
                ),
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
        let n = diesel::delete(
            access_keys::table
                .filter(access_keys::account_id.eq(&ctx.account_id))
                .filter(access_keys::user_name.eq(&user))
                .filter(access_keys::access_key_id.eq(&input.access_key_id)),
        )
        .execute(tx)?;
        if n == 0 {
            return Err(AwsError::sender(
                404,
                "NoSuchEntity",
                format!(
                    "The Access Key with id {} cannot be found.",
                    input.access_key_id
                ),
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
        let row = access_keys::table
            .filter(access_keys::account_id.eq(&ctx.account_id))
            .filter(access_keys::access_key_id.eq(&input.access_key_id))
            .select((
                access_keys::user_name,
                access_keys::last_used_at,
                access_keys::last_used_service,
                access_keys::last_used_region,
            ))
            .first::<(String, Option<i64>, Option<String>, Option<String>)>(tx)
            .optional()?
            .ok_or_else(|| {
                validation(format!(
                    "Invalid Access Key ID or Access Key ID not found: {}",
                    input.access_key_id
                ))
            })?;
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
