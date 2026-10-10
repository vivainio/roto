use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::misc::*;
use crate::util::*;

pub fn attach_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: AttachRolePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_role(tx, &ctx.account_id, &i.role_name)?;
        tx.execute("INSERT OR IGNORE INTO attachments(account_id,kind,entity,policy_arn) VALUES(?1,'role',?2,?3)", params![ctx.account_id, i.role_name, i.policy_arn])?;
        Ok(())
    })
}

pub fn detach_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DetachRolePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_role(tx, &ctx.account_id, &i.role_name)?;
        tx.execute("DELETE FROM attachments WHERE account_id=?1 AND kind='role' AND entity=?2 AND policy_arn=?3", params![ctx.account_id, i.role_name, i.policy_arn])?;
        Ok(())
    })
}

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

pub fn load_role(tx: &Transaction, account: &str, name: &str) -> Result<RoleRow, AwsError> {
    tx.query_row(
        "SELECT name, path, role_id, created_at, assume_role_policy, description, max_session_duration,
                permissions_boundary FROM roles WHERE account_id = ?1 AND name = ?2",
        params![account, name],
        |r| {
            Ok(RoleRow {
                name: r.get(0)?,
                path: r.get(1)?,
                role_id: r.get(2)?,
                created_at: r.get(3)?,
                assume_role_policy: r.get(4)?,
                description: r.get(5)?,
                max_session_duration: r.get(6)?,
                permissions_boundary: r.get(7)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| no_such("role", name))
}

pub fn role_out(tx: &Transaction, ctx: &RequestContext, r: &RoleRow) -> Result<Role, AwsError> {
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
        tx.execute(
            "INSERT INTO roles (account_id, name, path, role_id, created_at, assume_role_policy, description,
                                max_session_duration, permissions_boundary)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                ctx.account_id,
                input.role_name,
                path,
                gen_id("AROA", 17),
                now(),
                input.assume_role_policy_document,
                input.description,
                duration,
                input.permissions_boundary
            ],
        )?;
        set_tags(tx, &ctx.account_id, "role", &input.role_name, &input.tags)?;
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        Ok(CreateRoleResponse { role: role_out(tx, ctx, &r)? })
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
            let mut stmt = tx.prepare(
                "SELECT name FROM roles WHERE account_id = ?1 AND substr(path, 1, length(?2)) = ?2 ORDER BY name COLLATE NOCASE",
            )?;
            stmt.query_map(params![ctx.account_id, prefix], |r| r.get(0))?.collect::<Result<_, _>>()?
        };
        let (page, truncated, marker) = paginate(names, input.marker.as_deref(), input.max_items)?;
        let roles = page
            .iter()
            .map(|n| role_out(tx, ctx, &load_role(tx, &ctx.account_id, n)?))
            .collect::<Result<_, _>>()?;
        Ok(ListRolesResponse { roles, is_truncated: Some(truncated), marker })
    })
}

pub fn delete_role(
    db: &Db,
    ctx: &RequestContext,
    input: DeleteRoleRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let r = load_role(tx, &ctx.account_id, &input.role_name)?;
        let count = |sql: &str| -> Result<i64, AwsError> {
            Ok(tx.query_row(sql, params![ctx.account_id, r.name], |row| row.get(0))?)
        };
        if count("SELECT COUNT(*) FROM inline_policies WHERE account_id = ?1 AND kind = 'role' AND entity = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must delete policies first."));
        }
        if count("SELECT COUNT(*) FROM attachments WHERE account_id = ?1 AND kind = 'role' AND entity = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must detach all policies first."));
        }
        if count("SELECT COUNT(*) FROM profile_roles WHERE account_id = ?1 AND role = ?2")? > 0 {
            return Err(conflict("Cannot delete entity, must remove roles from instance profile first."));
        }
        tx.execute("DELETE FROM tags WHERE account_id = ?1 AND kind = 'role' AND entity = ?2", params![ctx.account_id, r.name])?;
        tx.execute("DELETE FROM roles WHERE account_id = ?1 AND name = ?2", params![ctx.account_id, r.name])?;
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
        tx.execute(
            "UPDATE roles SET description = ?1, max_session_duration = ?2 WHERE account_id = ?3 AND name = ?4",
            params![input.description.or(r.description), duration, ctx.account_id, r.name],
        )?;
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
        tx.execute(
            "UPDATE roles SET description = ?1 WHERE account_id = ?2 AND name = ?3",
            params![input.description, ctx.account_id, r.name],
        )?;
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
        tx.execute(
            "UPDATE roles SET assume_role_policy = ?1 WHERE account_id = ?2 AND name = ?3",
            params![input.policy_document, ctx.account_id, r.name],
        )?;
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
