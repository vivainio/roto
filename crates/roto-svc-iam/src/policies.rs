use crate::generated::*;
use crate::misc::*;
use crate::models::{PolicyRow, PolicyVersionRow};
use crate::roles::{check_policy_document, load_role};
use crate::schema::*;
use crate::util::*;
use diesel::SqliteConnection;
use roto_core::diesel::{self, prelude::*};
use roto_core::store::DieselDb as Db;
use roto_core::{AwsError, RequestContext};

pub fn put_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: PutRolePolicyRequest,
) -> Result<(), AwsError> {
    check_policy_document(&i.policy_document)?;
    db.transaction(|tx| {
        load_role(tx, &ctx.account_id, &i.role_name)?;
        diesel::insert_into(inline_policies::table)
            .values((
                inline_policies::account_id.eq(&ctx.account_id),
                inline_policies::kind.eq("role"),
                inline_policies::entity.eq(&i.role_name),
                inline_policies::name.eq(&i.policy_name),
                inline_policies::document.eq(&i.policy_document),
            ))
            .on_conflict((
                inline_policies::account_id,
                inline_policies::kind,
                inline_policies::entity,
                inline_policies::name,
            ))
            .do_update()
            .set(inline_policies::document.eq(diesel::upsert::excluded(inline_policies::document)))
            .execute(tx)?;
        Ok(())
    })
}

pub fn delete_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DeleteRolePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_role(tx, &ctx.account_id, &i.role_name)?;
        diesel::delete(
            inline_policies::table
                .filter(inline_policies::account_id.eq(&ctx.account_id))
                .filter(inline_policies::kind.eq("role"))
                .filter(inline_policies::entity.eq(&i.role_name))
                .filter(inline_policies::name.eq(&i.policy_name)),
        )
        .execute(tx)?;
        Ok(())
    })
}

pub(crate) fn load_policy(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    policy_arn: &str,
) -> Result<Policy, AwsError> {
    let mut policy = policies::table
        .filter(policies::account_id.eq(&ctx.account_id))
        .filter(policies::arn.eq(&policy_arn))
        .select(PolicyRow::as_select())
        .first::<PolicyRow>(tx)
        .map(|r| Policy {
            arn: Some(r.arn),
            policy_name: Some(r.name),
            path: Some(r.path),
            policy_id: Some(r.policy_id),
            description: r.description,
            create_date: Some(ts(r.created_at)),
            update_date: Some(ts(r.updated_at)),
            default_version_id: Some(r.default_version),
            is_attachable: Some(true),
            ..Default::default()
        })
        .optional()?
        .ok_or_else(|| {
            AwsError::sender(
                404,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found"),
            )
        })?;
    policy.attachment_count = Some(
        attachments::table
            .filter(attachments::account_id.eq(&ctx.account_id))
            .filter(attachments::policy_arn.eq(&policy_arn))
            .count()
            .first::<i64>(tx)? as i32,
    );
    let boundary_count = users::table
        .filter(users::account_id.eq(&ctx.account_id))
        .filter(users::permissions_boundary.eq(policy_arn))
        .count()
        .get_result::<i64>(tx)?
        + roles::table
            .filter(roles::account_id.eq(&ctx.account_id))
            .filter(roles::permissions_boundary.eq(policy_arn))
            .count()
            .get_result::<i64>(tx)?;
    policy.permissions_boundary_usage_count = Some(boundary_count as i32);
    policy.tags = load_tags(tx, &ctx.account_id, "policy", policy_arn)?;
    Ok(policy)
}

pub fn create_policy(
    db: &Db,
    ctx: &RequestContext,
    i: CreatePolicyRequest,
) -> Result<CreatePolicyResponse, AwsError> {
    check_policy_document(&i.policy_document)?;
    let path = normalize_path(i.path.as_deref())?;
    let policy_arn = arn(ctx, &format!("policy{}{}", path, i.policy_name));
    db.transaction(|tx| {
        let duplicate: bool = diesel::select(diesel::dsl::exists(
            policies::table
                .filter(policies::account_id.eq(&ctx.account_id))
                .filter(policies::name.eq(&i.policy_name)),
        ))
        .first::<bool>(tx)?;
        if duplicate {
            return Err(AwsError::sender(
                409,
                "EntityAlreadyExists",
                format!(
                    "A policy called {} already exists. Duplicate names are not allowed.",
                    i.policy_name
                ),
            ));
        }
        let time = now();
        let row = PolicyRow {
            account_id: ctx.account_id.clone(),
            arn: policy_arn.clone(),
            name: i.policy_name.clone(),
            path: path.clone(),
            policy_id: gen_id("ANPA", 17),
            description: i.description.clone(),
            created_at: time,
            updated_at: time,
            default_version: "v1".into(),
            next_version: 2,
        };
        diesel::insert_into(policies::table)
            .values(&row)
            .execute(tx)?;
        let version = PolicyVersionRow {
            policy_arn: policy_arn.clone(),
            version_id: "v1".into(),
            document: i.policy_document.clone(),
            created_at: time,
        };
        diesel::insert_into(policy_versions::table)
            .values(&version)
            .execute(tx)?;
        set_policy_tags(tx, ctx, &policy_arn, &i.tags)?;
        Ok(CreatePolicyResponse {
            policy: Some(load_policy(tx, ctx, &policy_arn)?),
        })
    })
}
pub fn get_policy(
    db: &Db,
    ctx: &RequestContext,
    i: GetPolicyRequest,
) -> Result<GetPolicyResponse, AwsError> {
    db.transaction(|tx| {
        Ok(GetPolicyResponse {
            policy: Some(load_policy(tx, ctx, &i.policy_arn)?),
        })
    })
}
pub fn delete_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DeletePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let p = load_policy(tx, ctx, &i.policy_arn)?;
        if p.attachment_count.unwrap_or(0) > 0
            || p.permissions_boundary_usage_count.unwrap_or(0) > 0
        {
            return Err(conflict("Cannot delete a policy attached to entities."));
        }
        let count: i64 = policy_versions::table
            .filter(policy_versions::policy_arn.eq(&i.policy_arn))
            .count()
            .first::<i64>(tx)?;
        if count > 1 {
            return Err(conflict(
                "Cannot delete a policy with non-default versions.",
            ));
        }
        diesel::delete(
            policy_versions::table.filter(policy_versions::policy_arn.eq(&i.policy_arn)),
        )
        .execute(tx)?;
        diesel::delete(
            tags::table
                .filter(tags::account_id.eq(&ctx.account_id))
                .filter(tags::kind.eq("policy"))
                .filter(tags::entity.eq(&i.policy_arn)),
        )
        .execute(tx)?;
        diesel::delete(
            policies::table
                .filter(policies::account_id.eq(&ctx.account_id))
                .filter(policies::arn.eq(&i.policy_arn)),
        )
        .execute(tx)?;
        Ok(())
    })
}
pub fn list_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListPoliciesRequest,
) -> Result<ListPoliciesResponse, AwsError> {
    db.transaction(|tx| {
        if !matches!(i.scope.as_deref(), None | Some("All" | "Local" | "AWS")) {
            return Err(validation("Invalid policy scope."));
        }
        if !matches!(
            i.policy_usage_filter.as_deref(),
            None | Some("PermissionsPolicy" | "PermissionsBoundary")
        ) {
            return Err(validation("Invalid policy usage filter."));
        }
        let arns = policies::table
            .filter(policies::account_id.eq(&ctx.account_id))
            .order((policies::name, policies::arn))
            .select(policies::arn)
            .load::<String>(tx)?;
        let mut policies = Vec::new();
        for a in arns {
            let p = load_policy(tx, ctx, &a)?;
            let count = if i.policy_usage_filter.as_deref() == Some("PermissionsBoundary") {
                p.permissions_boundary_usage_count
            } else {
                p.attachment_count
            };
            if i.scope.as_deref() != Some("AWS")
                && p.path
                    .as_deref()
                    .unwrap_or("/")
                    .starts_with(i.path_prefix.as_deref().unwrap_or("/"))
                && (!i.only_attached.unwrap_or(false) || count.unwrap_or(0) > 0)
            {
                policies.push(p);
            }
        }
        let (policies, truncated, marker) = paginate(policies, i.marker.as_deref(), i.max_items)?;
        Ok(ListPoliciesResponse {
            policies,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
fn load_version(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    a: &str,
    v: &str,
) -> Result<PolicyVersion, AwsError> {
    let p = load_policy(tx, ctx, a)?;
    policy_versions::table
        .filter(policy_versions::policy_arn.eq(&a))
        .filter(policy_versions::version_id.eq(&v))
        .select(PolicyVersionRow::as_select())
        .first::<PolicyVersionRow>(tx)
        .map(|r| PolicyVersion {
            document: Some(r.document),
            create_date: Some(ts(r.created_at)),
            version_id: Some(r.version_id),
            is_default_version: Some(p.default_version_id.as_deref() == Some(v)),
        })
        .optional()?
        .ok_or_else(|| {
            AwsError::sender(
                404,
                "NoSuchEntity",
                format!("Policy {a} version {v} does not exist or is not attachable."),
            )
        })
}
pub fn get_policy_version(
    db: &Db,
    ctx: &RequestContext,
    i: GetPolicyVersionRequest,
) -> Result<GetPolicyVersionResponse, AwsError> {
    db.transaction(|tx| {
        Ok(GetPolicyVersionResponse {
            policy_version: Some(load_version(tx, ctx, &i.policy_arn, &i.version_id)?),
        })
    })
}
pub fn create_policy_version(
    db: &Db,
    ctx: &RequestContext,
    i: CreatePolicyVersionRequest,
) -> Result<CreatePolicyVersionResponse, AwsError> {
    check_policy_document(&i.policy_document)?;
    db.transaction(|tx| {
        load_policy(tx,ctx,&i.policy_arn)?;
        let count:i64=policy_versions::table.filter(policy_versions::policy_arn.eq(&i.policy_arn)).count().first::<i64>(tx)?;
        if count>=5 {return Err(AwsError::sender(409,"LimitExceeded","A managed policy can have up to 5 versions. Before you create a new version, you must delete an existing version."));}
        let next:i64=policies::table.filter(policies::arn.eq(&i.policy_arn)).select(policies::next_version).first::<i64>(tx)?;
        let version=format!("v{next}");
        let row=PolicyVersionRow {policy_arn:i.policy_arn.clone(),version_id:version.clone(),document:i.policy_document.clone(),created_at:now()};
        diesel::insert_into(policy_versions::table).values(&row).execute(tx)?;
        let default_version=if i.set_as_default.unwrap_or(false) {version.clone()} else {policies::table.filter(policies::arn.eq(&i.policy_arn)).select(policies::default_version).first::<String>(tx)?};
        diesel::update(policies::table.filter(policies::arn.eq(&i.policy_arn))).set((policies::next_version.eq(next+1),policies::updated_at.eq(now()),policies::default_version.eq(default_version))).execute(tx)?;
        Ok(CreatePolicyVersionResponse {policy_version:Some(load_version(tx,ctx,&i.policy_arn,&version)?)})
    })
}
pub fn list_policy_versions(
    db: &Db,
    ctx: &RequestContext,
    i: ListPolicyVersionsRequest,
) -> Result<ListPolicyVersionsResponse, AwsError> {
    db.transaction(|tx| {
        load_policy(tx, ctx, &i.policy_arn)?;
        let mut ids = policy_versions::table
            .filter(policy_versions::policy_arn.eq(&i.policy_arn))
            .select(policy_versions::version_id)
            .load::<String>(tx)?;
        ids.sort_by_key(|v| v[1..].parse::<i64>().unwrap_or_default());
        let (ids, truncated, marker) = paginate(ids, i.marker.as_deref(), i.max_items)?;
        let versions = ids
            .iter()
            .map(|v| load_version(tx, ctx, &i.policy_arn, v))
            .collect::<Result<_, _>>()?;
        Ok(ListPolicyVersionsResponse {
            versions,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
pub fn delete_policy_version(
    db: &Db,
    ctx: &RequestContext,
    i: DeletePolicyVersionRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let v = load_version(tx, ctx, &i.policy_arn, &i.version_id)?;
        if v.is_default_version == Some(true) {
            return Err(conflict("Cannot delete the default version of a policy."));
        }
        diesel::delete(
            policy_versions::table
                .filter(policy_versions::policy_arn.eq(&i.policy_arn))
                .filter(policy_versions::version_id.eq(&i.version_id)),
        )
        .execute(tx)?;
        Ok(())
    })
}
pub fn set_default_policy_version(
    db: &Db,
    ctx: &RequestContext,
    i: SetDefaultPolicyVersionRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_policy(tx,ctx,&i.policy_arn)?;
        if !i.version_id.strip_prefix('v').is_some_and(|s| !s.is_empty() && !s.starts_with('0') && s.bytes().all(|b|b.is_ascii_digit())) {
            return Err(validation(format!(r"Value '{}' at 'versionId' failed to satisfy constraint: Member must satisfy regular expression pattern: v[1-9][0-9]*(\.[A-Za-z0-9-]*)?",i.version_id)));
        }
        load_version(tx,ctx,&i.policy_arn,&i.version_id)?;
        diesel::update(policies::table.filter(policies::arn.eq(&i.policy_arn))).set((policies::default_version.eq(&i.version_id),policies::updated_at.eq(&now()))).execute(tx)?;
        Ok(())
    })
}
pub fn tag_policy(db: &Db, ctx: &RequestContext, i: TagPolicyRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_policy(tx, ctx, &i.policy_arn)?;
        set_policy_tags(tx, ctx, &i.policy_arn, &i.tags)
    })
}
pub fn untag_policy(db: &Db, ctx: &RequestContext, i: UntagPolicyRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_policy(tx, ctx, &i.policy_arn)?;
        check_policy_tag_keys(&i.tag_keys).map_err(|mut e| {
            e.message = format!("tagKeys: {}", e.message);
            e
        })?;
        remove_tags(tx, &ctx.account_id, "policy", &i.policy_arn, &i.tag_keys)
    })
}
pub fn list_policy_tags(
    db: &Db,
    ctx: &RequestContext,
    i: ListPolicyTagsRequest,
) -> Result<ListPolicyTagsResponse, AwsError> {
    db.transaction(|tx| {
        let p = load_policy(tx, ctx, &i.policy_arn)?;
        let (tags, truncated, marker) = paginate(p.tags, i.marker.as_deref(), i.max_items)?;
        Ok(ListPolicyTagsResponse {
            tags,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
fn check_entity(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
) -> Result<(), AwsError> {
    match kind {
        "role" => {
            load_role(tx, &ctx.account_id, name)?;
        }
        "user" => {
            crate::users::load_user(tx, &ctx.account_id, name)?;
        }
        "group" => {
            crate::groups::load_group(tx, ctx, name)?;
        }
        _ => return Err(validation("Invalid entity type.")),
    }
    Ok(())
}
pub(crate) fn attach(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
    a: &str,
) -> Result<(), AwsError> {
    check_entity(tx, ctx, kind, name)?;
    load_policy(tx, ctx, a).map_err(|e| {
        if e.code != "NoSuchEntity" {
            return e;
        }
        AwsError::sender(
            404,
            "NoSuchEntity",
            format!("Policy {a} does not exist or is not attachable."),
        )
    })?;
    diesel::insert_into(attachments::table)
        .values((
            attachments::account_id.eq(&ctx.account_id),
            attachments::kind.eq(&kind),
            attachments::entity.eq(&name),
            attachments::policy_arn.eq(&a),
        ))
        .on_conflict_do_nothing()
        .execute(tx)?;
    Ok(())
}
pub(crate) fn detach(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
    a: &str,
) -> Result<(), AwsError> {
    check_entity(tx, ctx, kind, name)?;
    let n = diesel::delete(
        attachments::table
            .filter(attachments::account_id.eq(&ctx.account_id))
            .filter(attachments::kind.eq(&kind))
            .filter(attachments::entity.eq(&name))
            .filter(attachments::policy_arn.eq(&a)),
    )
    .execute(tx)?;
    if n == 0 {
        return Err(AwsError::sender(
            404,
            "NoSuchEntity",
            format!("Policy {a} was not found."),
        ));
    }
    Ok(())
}
fn attached(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
    prefix: Option<&str>,
    marker: Option<&str>,
    max: Option<i32>,
) -> Result<(Vec<AttachedPolicy>, bool, Option<String>), AwsError> {
    check_entity(tx, ctx, kind, name)?;
    let items = attachments::table
        .inner_join(policies::table.on(policies::arn.eq(attachments::policy_arn)))
        .filter(attachments::account_id.eq(&ctx.account_id))
        .filter(attachments::kind.eq(kind))
        .filter(attachments::entity.eq(name))
        .filter(
            substr(policies::path, 1, length(literal_prefix(prefix))).eq(literal_prefix(prefix)),
        )
        .order((policies::name, policies::arn))
        .select((policies::arn, policies::name))
        .load::<(String, String)>(tx)?
        .into_iter()
        .map(|(policy_arn, policy_name)| AttachedPolicy {
            policy_arn: Some(policy_arn),
            policy_name: Some(policy_name),
        })
        .collect();
    paginate(items, marker, max)
}

pub fn attach_user_policy(
    db: &Db,
    ctx: &RequestContext,
    i: AttachUserPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| attach(tx, ctx, "user", &i.user_name, &i.policy_arn))
}

pub fn detach_user_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DetachUserPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| detach(tx, ctx, "user", &i.user_name, &i.policy_arn))
}

pub fn list_attached_user_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListAttachedUserPoliciesRequest,
) -> Result<ListAttachedUserPoliciesResponse, AwsError> {
    db.transaction(|tx| {
        let (attached_policies, truncated, marker) = attached(
            tx,
            ctx,
            "user",
            &i.user_name,
            i.path_prefix.as_deref(),
            i.marker.as_deref(),
            i.max_items,
        )?;
        Ok(ListAttachedUserPoliciesResponse {
            attached_policies,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

pub fn attach_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: AttachRolePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| attach(tx, ctx, "role", &i.role_name, &i.policy_arn))
}

pub fn detach_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DetachRolePolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| detach(tx, ctx, "role", &i.role_name, &i.policy_arn))
}

pub fn list_attached_role_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListAttachedRolePoliciesRequest,
) -> Result<ListAttachedRolePoliciesResponse, AwsError> {
    db.transaction(|tx| {
        let (attached_policies, truncated, marker) = attached(
            tx,
            ctx,
            "role",
            &i.role_name,
            i.path_prefix.as_deref(),
            i.marker.as_deref(),
            i.max_items,
        )?;
        Ok(ListAttachedRolePoliciesResponse {
            attached_policies,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

pub fn list_entities_for_policy(
    db: &Db,
    ctx: &RequestContext,
    i: ListEntitiesForPolicyRequest,
) -> Result<ListEntitiesForPolicyResponse, AwsError> {
    db.transaction(|tx| {
        load_policy(tx, ctx, &i.policy_arn)?;
        if !matches!(
            i.entity_filter.as_deref(),
            None | Some("User" | "Role" | "Group")
        ) {
            return Err(validation("Invalid entity filter."));
        }
        if !matches!(
            i.policy_usage_filter.as_deref(),
            None | Some("PermissionsPolicy" | "PermissionsBoundary")
        ) {
            return Err(validation("Invalid policy usage filter."));
        }
        let mut entities: Vec<(String, String, String)> = Vec::new();
        if i.entity_filter
            .as_ref()
            .is_none_or(|f| f.eq_ignore_ascii_case("user"))
        {
            let mut query = users::table
                .filter(users::account_id.eq(&ctx.account_id))
                .filter(
                    substr(
                        users::path,
                        1,
                        length(literal_prefix(i.path_prefix.as_deref())),
                    )
                    .eq(literal_prefix(i.path_prefix.as_deref())),
                )
                .into_boxed();
            if i.policy_usage_filter.as_deref() == Some("PermissionsBoundary") {
                query = query.filter(users::permissions_boundary.eq(&i.policy_arn));
            } else {
                query = query.filter(
                    users::name.eq_any(
                        attachments::table
                            .filter(attachments::account_id.eq(&ctx.account_id))
                            .filter(attachments::kind.eq("user"))
                            .filter(attachments::policy_arn.eq(&i.policy_arn))
                            .select(attachments::entity),
                    ),
                );
            }
            let rows = query
                .order(users::name)
                .select((users::name, users::user_id))
                .load::<(String, String)>(tx)?;
            entities.extend(
                rows.into_iter()
                    .map(|(name, id)| ("user".to_string(), name, id)),
            );
        }
        if i.entity_filter
            .as_ref()
            .is_none_or(|f| f.eq_ignore_ascii_case("role"))
        {
            let mut query = roles::table
                .filter(roles::account_id.eq(&ctx.account_id))
                .filter(
                    substr(
                        roles::path,
                        1,
                        length(literal_prefix(i.path_prefix.as_deref())),
                    )
                    .eq(literal_prefix(i.path_prefix.as_deref())),
                )
                .into_boxed();
            if i.policy_usage_filter.as_deref() == Some("PermissionsBoundary") {
                query = query.filter(roles::permissions_boundary.eq(&i.policy_arn));
            } else {
                query = query.filter(
                    roles::name.eq_any(
                        attachments::table
                            .filter(attachments::account_id.eq(&ctx.account_id))
                            .filter(attachments::kind.eq("role"))
                            .filter(attachments::policy_arn.eq(&i.policy_arn))
                            .select(attachments::entity),
                    ),
                );
            }
            let rows = query
                .order(roles::name)
                .select((roles::name, roles::role_id))
                .load::<(String, String)>(tx)?;
            entities.extend(
                rows.into_iter()
                    .map(|(name, id)| ("role".to_string(), name, id)),
            );
        }
        if i.entity_filter
            .as_ref()
            .is_none_or(|f| f.eq_ignore_ascii_case("group"))
        {
            let mut query = groups::table
                .filter(groups::account_id.eq(&ctx.account_id))
                .filter(
                    substr(
                        groups::path,
                        1,
                        length(literal_prefix(i.path_prefix.as_deref())),
                    )
                    .eq(literal_prefix(i.path_prefix.as_deref())),
                )
                .into_boxed();
            if i.policy_usage_filter.as_deref() != Some("PermissionsBoundary") {
                query = query.filter(
                    groups::name.eq_any(
                        attachments::table
                            .filter(attachments::account_id.eq(&ctx.account_id))
                            .filter(attachments::kind.eq("group"))
                            .filter(attachments::policy_arn.eq(&i.policy_arn))
                            .select(attachments::entity),
                    ),
                );
                let rows = query
                    .order(groups::name)
                    .select((groups::name, groups::group_id))
                    .load::<(String, String)>(tx)?;
                entities.extend(
                    rows.into_iter()
                        .map(|(name, id)| ("group".to_string(), name, id)),
                );
            }
        }
        let (entities, truncated, marker) = paginate(entities, i.marker.as_deref(), i.max_items)?;
        let mut out = ListEntitiesForPolicyResponse {
            is_truncated: Some(truncated),
            marker,
            ..Default::default()
        };
        for (kind, name, id) in entities {
            match kind.as_str() {
                "user" => out.policy_users.push(PolicyUser {
                    user_name: Some(name),
                    user_id: Some(id),
                }),
                "role" => out.policy_roles.push(PolicyRole {
                    role_name: Some(name),
                    role_id: Some(id),
                }),
                _ => out.policy_groups.push(PolicyGroup {
                    group_name: Some(name),
                    group_id: Some(id),
                }),
            }
        }
        Ok(out)
    })
}

fn check_policy_tag_keys(keys: &[String]) -> Result<(), AwsError> {
    if keys.len() > 50 {
        return Err(validation(
            "failed to satisfy constraint: Member must have length less than or equal to 50.",
        ));
    }
    for key in keys {
        if key.is_empty() || key.chars().count() > 128 {
            return Err(validation(
                "Member must have length less than or equal to 128.",
            ));
        }
        if !key
            .chars()
            .all(|c| c.is_alphanumeric() || c.is_whitespace() || "_.:/=+-@".contains(c))
        {
            return Err(validation(
                r"Member must satisfy regular expression pattern: [\p{L}\p{Z}\p{N}_.:/=+\-@]+",
            ));
        }
    }
    Ok(())
}
fn set_policy_tags(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    a: &str,
    tags: &[Tag],
) -> Result<(), AwsError> {
    check_policy_tag_keys(&tags.iter().map(|t| t.key.clone()).collect::<Vec<_>>())?;
    if tags.iter().any(|t| t.value.chars().count() > 256) {
        return Err(validation(
            "Member must have length less than or equal to 256.",
        ));
    }
    check_tags(0, tags)?;
    let existing = load_tags(tx, &ctx.account_id, "policy", a)?;
    let new_count = tags
        .iter()
        .filter(|t| !existing.iter().any(|e| e.key.eq_ignore_ascii_case(&t.key)))
        .count();
    if existing.len() + new_count > 50 {
        return Err(validation(
            "failed to satisfy constraint: Member must have length less than or equal to 50.",
        ));
    }
    for t in tags {
        diesel::delete(
            tags::table
                .filter(tags::account_id.eq(&ctx.account_id))
                .filter(tags::kind.eq("policy"))
                .filter(tags::entity.eq(&a))
                .filter(tags::key.eq(&t.key))
                .filter(tags::key.ne(&t.key)),
        )
        .execute(tx)?;
        diesel::insert_into(tags::table)
            .values((
                tags::account_id.eq(&ctx.account_id),
                tags::kind.eq("policy"),
                tags::entity.eq(&a),
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

pub fn attach_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: AttachGroupPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| attach(tx, ctx, "group", &i.group_name, &i.policy_arn))
}
pub fn detach_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DetachGroupPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| detach(tx, ctx, "group", &i.group_name, &i.policy_arn))
}
pub fn list_attached_group_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListAttachedGroupPoliciesRequest,
) -> Result<ListAttachedGroupPoliciesResponse, AwsError> {
    db.transaction(|tx| {
        let (attached_policies, t, marker) = attached(
            tx,
            ctx,
            "group",
            &i.group_name,
            i.path_prefix.as_deref(),
            i.marker.as_deref(),
            i.max_items,
        )?;
        Ok(ListAttachedGroupPoliciesResponse {
            attached_policies,
            is_truncated: Some(t),
            marker,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Iam, Service};
    use roto_core::store::Store;

    fn context() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn create(iam: &Iam, c: &RequestContext, name: &str) -> String {
        iam.create_policy(c,CreatePolicyRequest {policy_name:name.into(),policy_document:r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#.into(),..Default::default()}).unwrap().policy.unwrap().arn.unwrap()
    }
    #[test]
    fn versions_attachments_and_account_isolation() {
        let store = Store::ephemeral();
        let iam = Iam::new(&store).unwrap();
        let c = context();
        let a = create(&iam, &c, "test");
        iam.create_user(
            &c,
            CreateUserRequest {
                user_name: "alice".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut other = c.clone();
        other.account_id = "999999999999".into();
        assert!(
            iam.get_policy(
                &other,
                GetPolicyRequest {
                    policy_arn: a.clone()
                }
            )
            .is_err()
        );
        assert!(
            iam.attach_user_policy(
                &other,
                AttachUserPolicyRequest {
                    user_name: "alice".into(),
                    policy_arn: a.clone()
                }
            )
            .is_err()
        );
        iam.attach_user_policy(
            &c,
            AttachUserPolicyRequest {
                user_name: "alice".into(),
                policy_arn: a.clone(),
            },
        )
        .unwrap();
        iam.attach_user_policy(
            &c,
            AttachUserPolicyRequest {
                user_name: "ALICE".into(),
                policy_arn: a.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            iam.get_policy(
                &c,
                GetPolicyRequest {
                    policy_arn: a.clone()
                }
            )
            .unwrap()
            .policy
            .unwrap()
            .attachment_count,
            Some(1)
        );
        assert!(
            iam.delete_policy(
                &c,
                DeletePolicyRequest {
                    policy_arn: a.clone()
                }
            )
            .is_err()
        );
        let entities = iam
            .list_entities_for_policy(
                &c,
                ListEntitiesForPolicyRequest {
                    policy_arn: a.clone(),
                    entity_filter: Some("User".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(entities.policy_users[0].user_name.as_deref(), Some("alice"));
        iam.detach_user_policy(
            &c,
            DetachUserPolicyRequest {
                user_name: "alice".into(),
                policy_arn: a.clone(),
            },
        )
        .unwrap();
        for _ in 0..4 {
            iam.create_policy_version(
                &c,
                CreatePolicyVersionRequest {
                    policy_arn: a.clone(),
                    policy_document: "{}".into(),
                    set_as_default: Some(true),
                },
            )
            .unwrap();
        }
        assert!(
            iam.create_policy_version(
                &c,
                CreatePolicyVersionRequest {
                    policy_arn: a.clone(),
                    policy_document: "{}".into(),
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            iam.delete_policy_version(
                &c,
                DeletePolicyVersionRequest {
                    policy_arn: a.clone(),
                    version_id: "v5".into()
                }
            )
            .is_err()
        );
        iam.delete_policy_version(
            &c,
            DeletePolicyVersionRequest {
                policy_arn: a.clone(),
                version_id: "v2".into(),
            },
        )
        .unwrap();
        let v = iam
            .create_policy_version(
                &c,
                CreatePolicyVersionRequest {
                    policy_arn: a.clone(),
                    policy_document: "{}".into(),
                    ..Default::default()
                },
            )
            .unwrap()
            .policy_version
            .unwrap();
        assert_eq!(v.version_id.as_deref(), Some("v6"));
        assert!(
            iam.delete_policy(&c, DeletePolicyRequest { policy_arn: a })
                .is_err()
        );
        iam.reset().unwrap();
        create(&iam, &c, "test");
        let orphan_count: i64 = iam
            .db
            .read(|db| Ok(policy_versions::table.count().get_result::<i64>(db)?))
            .unwrap();
        assert_eq!(orphan_count, 1);
    }
    #[test]
    fn policy_pagination_and_restart() {
        let path = std::env::temp_dir().join(format!("roto-iam-{}", uuid::Uuid::new_v4()));
        let c = context();
        let a;
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            a = create(&iam, &c, "a");
            create(&iam, &c, "b");
            iam.tag_policy(
                &c,
                TagPolicyRequest {
                    policy_arn: a.clone(),
                    tags: vec![Tag {
                        key: "key".into(),
                        value: "value".into(),
                    }],
                },
            )
            .unwrap();
            iam.create_policy_version(
                &c,
                CreatePolicyVersionRequest {
                    policy_arn: a.clone(),
                    policy_document: "{}".into(),
                    set_as_default: Some(true),
                },
            )
            .unwrap();
        }
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            let p = iam
                .get_policy(
                    &c,
                    GetPolicyRequest {
                        policy_arn: a.clone(),
                    },
                )
                .unwrap()
                .policy
                .unwrap();
            assert_eq!(p.default_version_id.as_deref(), Some("v2"));
            assert_eq!(p.tags[0].value, "value");
            let first = iam
                .list_policies(
                    &c,
                    ListPoliciesRequest {
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(first.is_truncated, Some(true));
            let second = iam
                .list_policies(
                    &c,
                    ListPoliciesRequest {
                        marker: first.marker,
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(second.policies[0].policy_name.as_deref(), Some("b"));
            assert_eq!(second.is_truncated, Some(false));
            let v = iam
                .get_policy_version(
                    &c,
                    GetPolicyVersionRequest {
                        policy_arn: a,
                        version_id: "v2".into(),
                    },
                )
                .unwrap()
                .policy_version
                .unwrap();
            assert_eq!(v.document.as_deref(), Some("{}"));
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
