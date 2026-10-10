use crate::schema::*;
use diesel::SqliteConnection;
use roto_core::diesel::{self, prelude::*};
use roto_core::store::DieselDb as Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::misc::*;
use crate::util::*;

#[derive(Queryable, Selectable)]
#[diesel(table_name=roles)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RoleRow {
    pub name: String,
    pub path: String,
    pub role_id: String,
    pub created_at: i64,
    pub assume_role_policy: String,
    pub description: Option<String>,
    pub max_session_duration: i32,
    pub permissions_boundary: Option<String>,
}

pub fn load_role(
    tx: &mut SqliteConnection,
    account: &str,
    name: &str,
) -> Result<RoleRow, AwsError> {
    roles::table
        .filter(roles::account_id.eq(account))
        .filter(roles::name.eq(name))
        .select(RoleRow::as_select())
        .first::<RoleRow>(tx)
        .optional()?
        .ok_or_else(|| no_such("role", name))
}

pub fn role_out(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    r: &RoleRow,
) -> Result<Role, AwsError> {
    Ok(Role {
        arn: arn(ctx, &format!("role{}{}", r.path, r.name)),
        assume_role_policy_document: Some(r.assume_role_policy.clone()),
        create_date: ts(r.created_at),
        description: r.description.clone(),
        max_session_duration: Some(r.max_session_duration),
        path: r.path.clone(),
        permissions_boundary: r.permissions_boundary.as_ref().map(|a| {
            AttachedPermissionsBoundary {
                permissions_boundary_arn: Some(a.clone()),
                permissions_boundary_type: Some("Policy".into()),
            }
        }),
        role_id: r.role_id.clone(),
        role_last_used: Some(RoleLastUsed {
            last_used_date: None,
            region: None,
        }),
        role_name: r.name.clone(),
        source_role_template: None,
        tags: load_tags(tx, &ctx.account_id, "role", &r.name)?,
    })
}

/// Trust and permission policies must be JSON documents.
pub fn check_policy_document(doc: &str) -> Result<(), AwsError> {
    match serde_json::from_str::<serde_json::Value>(doc) {
        Ok(serde_json::Value::Object(_)) => Ok(()),
        _ => Err(AwsError::sender(
            400,
            "MalformedPolicyDocument",
            "Syntax errors in policy.",
        )),
    }
}

fn check_session_duration(d: i32) -> Result<(), AwsError> {
    if (3600..=43_200).contains(&d) {
        Ok(())
    } else {
        Err(validation(format!(
            "1 validation error detected: Value '{d}' at 'maxSessionDuration' failed to satisfy constraint: Member must have value greater than or equal to 3600"
        )))
    }
}

pub fn create_role(
    db: &Db,
    ctx: &RequestContext,
    input: CreateRoleRequest,
) -> Result<CreateRoleResponse, AwsError> {
    let path = normalize_path(input.path.as_deref())?;
    check_policy_document(&input.assume_role_policy_document)?;
    let duration = input.max_session_duration.unwrap_or(3600);
    check_session_duration(duration)?;
    db.transaction(|tx| {
        if load_role(tx, &ctx.account_id, &input.role_name).is_ok() {
            return Err(exists("Role", &input.role_name));
        }
        check_tags(0, &input.tags)?;
        diesel::insert_into(roles::table)
            .values((
                roles::account_id.eq(&ctx.account_id),
                roles::name.eq(&input.role_name),
                roles::path.eq(&path),
                roles::role_id.eq(&gen_id("AROA", 17)),
                roles::created_at.eq(&now()),
                roles::assume_role_policy.eq(&input.assume_role_policy_document),
                roles::description.eq(&input.description),
                roles::max_session_duration.eq(&duration),
                roles::permissions_boundary.eq(&input.permissions_boundary),
            ))
            .execute(tx)?;
        set_tags(tx, &ctx.account_id, "role", &input.role_name, &input.tags)?;
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        Ok(CreateRoleResponse {
            role: role_out(tx, ctx, &r)?,
        })
    })
}

pub fn get_role(
    db: &Db,
    ctx: &RequestContext,
    input: GetRoleRequest,
) -> Result<GetRoleResponse, AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        Ok(GetRoleResponse {
            role: role_out(tx, ctx, &r)?,
        })
    })
}

pub fn list_roles(
    db: &Db,
    ctx: &RequestContext,
    input: ListRolesRequest,
) -> Result<ListRolesResponse, AwsError> {
    db.transaction(|tx| {
        let prefix = input.path_prefix.clone().unwrap_or_else(|| "/".into());
        let names: Vec<String> = {
            roles::table
                .filter(
                    substr(roles::path, 1, length(literal_prefix(Some(&prefix))))
                        .eq(literal_prefix(Some(&prefix))),
                )
                .filter(roles::account_id.eq(&ctx.account_id))
                .order(roles::name)
                .select(roles::name)
                .load::<String>(tx)?
        };
        let (page, truncated, marker) = paginate(names, input.marker.as_deref(), input.max_items)?;
        let roles = page
            .iter()
            .map(|n| {
                let row = load_role(tx, &ctx.account_id, n)?;
                role_out(tx, ctx, &row)
            })
            .collect::<Result<_, _>>()?;
        Ok(ListRolesResponse {
            roles,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

pub fn delete_role(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteRoleRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        if inline_policies::table
            .filter(inline_policies::account_id.eq(&ctx.account_id))
            .filter(inline_policies::kind.eq("role"))
            .filter(inline_policies::entity.eq(&r.name))
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
            .filter(attachments::kind.eq("role"))
            .filter(attachments::entity.eq(&r.name))
            .count()
            .get_result::<i64>(tx)?
            > 0
        {
            return Err(conflict(
                "Cannot delete entity, must detach all policies first.",
            ));
        }
        if profile_roles::table
            .filter(profile_roles::account_id.eq(&ctx.account_id))
            .filter(profile_roles::role.eq(&r.name))
            .count()
            .get_result::<i64>(tx)?
            > 0
        {
            return Err(conflict(
                "Cannot delete entity, must remove roles from instance profile first.",
            ));
        }
        diesel::delete(
            tags::table
                .filter(tags::account_id.eq(&ctx.account_id))
                .filter(tags::kind.eq("role"))
                .filter(tags::entity.eq(&r.name)),
        )
        .execute(tx)?;
        diesel::delete(
            roles::table
                .filter(roles::account_id.eq(&ctx.account_id))
                .filter(roles::name.eq(&r.name)),
        )
        .execute(tx)?;
        Ok(())
    })
}

pub fn update_role(
    db: &Db,
    ctx: &RequestContext,
    input: UpdateRoleRequest,
) -> Result<UpdateRoleResponse, AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        let duration = input.max_session_duration.unwrap_or(r.max_session_duration);
        check_session_duration(duration)?;
        diesel::update(
            roles::table
                .filter(roles::account_id.eq(&ctx.account_id))
                .filter(roles::name.eq(&r.name)),
        )
        .set((
            roles::description.eq(&input.description.or(r.description)),
            roles::max_session_duration.eq(&duration),
        ))
        .execute(tx)?;
        Ok(UpdateRoleResponse {})
    })
}

pub fn update_role_description(
    db: &Db,
    ctx: &RequestContext,
    input: UpdateRoleDescriptionRequest,
) -> Result<UpdateRoleDescriptionResponse, AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        diesel::update(
            roles::table
                .filter(roles::account_id.eq(&ctx.account_id))
                .filter(roles::name.eq(&r.name)),
        )
        .set(roles::description.eq(&input.description))
        .execute(tx)?;
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        Ok(UpdateRoleDescriptionResponse {
            role: Some(role_out(tx, ctx, &r)?),
        })
    })
}

pub fn update_assume_role_policy(
    db: &Db,
    ctx: &RequestContext,
    input: UpdateAssumeRolePolicyRequest,
) -> Result<(), AwsError> {
    check_policy_document(&input.policy_document)?;
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        diesel::update(
            roles::table
                .filter(roles::account_id.eq(&ctx.account_id))
                .filter(roles::name.eq(&r.name)),
        )
        .set(roles::assume_role_policy.eq(&input.policy_document))
        .execute(tx)?;
        Ok(())
    })
}

pub fn tag_role(db: &Db, ctx: &RequestContext, input: TagRoleRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        set_tags(tx, &ctx.account_id, "role", &r.name, &input.tags)
    })
}

pub fn untag_role(db: &Db, ctx: &RequestContext, input: UntagRoleRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        remove_tags(tx, &ctx.account_id, "role", &r.name, &input.tag_keys)
    })
}

pub fn list_role_tags(
    db: &Db,
    ctx: &RequestContext,
    input: ListRoleTagsRequest,
) -> Result<ListRoleTagsResponse, AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        let tags = load_tags(tx, &ctx.account_id, "role", &r.name)?;
        let (tags, truncated, marker) = paginate(tags, input.marker.as_deref(), input.max_items)?;
        Ok(ListRoleTagsResponse {
            tags,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
