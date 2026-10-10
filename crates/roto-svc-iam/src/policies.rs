use crate::generated::*;
use crate::misc::*;
use crate::roles::{check_policy_document, load_role};
use crate::util::*;
use roto_core::rusqlite::params;
use roto_core::rusqlite::{OptionalExtension, Transaction};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

pub fn put_role_policy(
    db: &Db,
    ctx: &RequestContext,
    i: PutRolePolicyRequest,
) -> Result<(), AwsError> {
    check_policy_document(&i.policy_document)?;
    db.transaction(|tx| {
        load_role(tx, &ctx.account_id, &i.role_name)?;
        tx.execute("INSERT INTO inline_policies(account_id,kind,entity,name,document) VALUES(?1,'role',?2,?3,?4) ON CONFLICT(account_id,kind,entity,name) DO UPDATE SET document=excluded.document", params![ctx.account_id, i.role_name, i.policy_name, i.policy_document])?;
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
        tx.execute("DELETE FROM inline_policies WHERE account_id=?1 AND kind='role' AND entity=?2 AND name=?3", params![ctx.account_id, i.role_name, i.policy_name])?;
        Ok(())
    })
}

pub(crate) fn load_policy(
    tx: &Transaction,
    ctx: &RequestContext,
    policy_arn: &str,
) -> Result<Policy, AwsError> {
    let mut policy = tx.query_row(
        "SELECT arn,name,path,policy_id,description,created_at,updated_at,default_version FROM policies WHERE account_id=?1 AND arn=?2",
        params![ctx.account_id, policy_arn], |r| Ok(Policy {
            arn: Some(r.get(0)?), policy_name: Some(r.get(1)?), path: Some(r.get(2)?),
            policy_id: Some(r.get(3)?), description: r.get(4)?, create_date: Some(ts(r.get(5)?)),
            update_date: Some(ts(r.get(6)?)), default_version_id: Some(r.get(7)?),
            is_attachable: Some(true), ..Default::default()
        })
    ).optional()?.ok_or_else(|| AwsError::sender(404,"NoSuchEntity",format!("Policy {policy_arn} not found")))?;
    policy.attachment_count = Some(tx.query_row(
        "SELECT COUNT(*) FROM attachments WHERE account_id=?1 AND policy_arn=?2",
        params![ctx.account_id, policy_arn],
        |r| r.get(0),
    )?);
    policy.permissions_boundary_usage_count = Some(tx.query_row("SELECT (SELECT COUNT(*) FROM users WHERE account_id=?1 AND permissions_boundary=?2) + (SELECT COUNT(*) FROM roles WHERE account_id=?1 AND permissions_boundary=?2)",params![ctx.account_id,policy_arn],|r|r.get(0))?);
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
        let duplicate: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM policies WHERE account_id=?1 AND name=?2)",params![ctx.account_id,i.policy_name],|r|r.get(0))?;
        if duplicate {return Err(AwsError::sender(409,"EntityAlreadyExists",format!("A policy called {} already exists. Duplicate names are not allowed.",i.policy_name)));}
        let time=now();
        tx.execute("INSERT INTO policies(account_id,arn,name,path,policy_id,description,created_at,updated_at,default_version) VALUES(?1,?2,?3,?4,?5,?6,?7,?7,'v1')",params![ctx.account_id,policy_arn,i.policy_name,path,gen_id("ANPA",17),i.description,time])?;
        tx.execute("INSERT INTO policy_versions(policy_arn,version_id,document,created_at) VALUES(?1,'v1',?2,?3)",params![policy_arn,i.policy_document,time])?;
        set_policy_tags(tx,ctx,&policy_arn,&i.tags)?;
        Ok(CreatePolicyResponse {policy:Some(load_policy(tx,ctx,&policy_arn)?)})
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
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM policy_versions WHERE policy_arn=?1",
            params![i.policy_arn],
            |r| r.get(0),
        )?;
        if count > 1 {
            return Err(conflict(
                "Cannot delete a policy with non-default versions.",
            ));
        }
        tx.execute(
            "DELETE FROM policy_versions WHERE policy_arn=?1",
            params![i.policy_arn],
        )?;
        tx.execute(
            "DELETE FROM tags WHERE account_id=?1 AND kind='policy' AND entity=?2",
            params![ctx.account_id, i.policy_arn],
        )?;
        tx.execute(
            "DELETE FROM policies WHERE account_id=?1 AND arn=?2",
            params![ctx.account_id, i.policy_arn],
        )?;
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
        let mut stmt = tx.prepare(
            "SELECT arn FROM policies WHERE account_id=?1 ORDER BY name COLLATE NOCASE,arn",
        )?;
        let arns: Vec<String> = stmt
            .query_map(params![ctx.account_id], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
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
    tx: &Transaction,
    ctx: &RequestContext,
    a: &str,
    v: &str,
) -> Result<PolicyVersion, AwsError> {
    let p = load_policy(tx, ctx, a)?;
    tx.query_row(
        "SELECT document,created_at FROM policy_versions WHERE policy_arn=?1 AND version_id=?2",
        params![a, v],
        |r| {
            Ok(PolicyVersion {
                document: Some(r.get(0)?),
                create_date: Some(ts(r.get(1)?)),
                version_id: Some(v.into()),
                is_default_version: Some(p.default_version_id.as_deref() == Some(v)),
            })
        },
    )
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
        let count:i64=tx.query_row("SELECT COUNT(*) FROM policy_versions WHERE policy_arn=?1",params![i.policy_arn],|r|r.get(0))?;
        if count>=5 {return Err(AwsError::sender(409,"LimitExceeded","A managed policy can have up to 5 versions. Before you create a new version, you must delete an existing version."));}
        let next:i64=tx.query_row("SELECT next_version FROM policies WHERE arn=?1",params![i.policy_arn],|r|r.get(0))?;
        let version=format!("v{next}");
        tx.execute("INSERT INTO policy_versions(policy_arn,version_id,document,created_at) VALUES(?1,?2,?3,?4)",params![i.policy_arn,version,i.policy_document,now()])?;
        tx.execute("UPDATE policies SET next_version=next_version+1,updated_at=?2,default_version=CASE WHEN ?3 THEN ?4 ELSE default_version END WHERE arn=?1",params![i.policy_arn,now(),i.set_as_default.unwrap_or(false),version])?;
        Ok(CreatePolicyVersionResponse {policy_version:Some(load_version(tx,ctx,&i.policy_arn,&version)?)})
    })
}
pub fn list_policy_versions(
    db: &Db,
    ctx: &RequestContext,
    i: ListPolicyVersionsRequest,
) -> Result<ListPolicyVersionsResponse, AwsError> {
    db.transaction(|tx| {
        load_policy(tx,ctx,&i.policy_arn)?;
        let mut stmt=tx.prepare("SELECT version_id FROM policy_versions WHERE policy_arn=?1 ORDER BY CAST(substr(version_id,2) AS INTEGER)")?;
        let ids:Vec<String>=stmt.query_map(params![i.policy_arn],|r|r.get(0))?.collect::<Result<_,_>>()?;
        let (ids,truncated,marker)=paginate(ids,i.marker.as_deref(),i.max_items)?;
        let versions=ids.iter().map(|v|load_version(tx,ctx,&i.policy_arn,v)).collect::<Result<_,_>>()?;
        Ok(ListPolicyVersionsResponse {versions,is_truncated:Some(truncated),marker})
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
        tx.execute(
            "DELETE FROM policy_versions WHERE policy_arn=?1 AND version_id=?2",
            params![i.policy_arn, i.version_id],
        )?;
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
        tx.execute("UPDATE policies SET default_version=?2,updated_at=?3 WHERE arn=?1",params![i.policy_arn,i.version_id,now()])?;
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
    tx: &Transaction,
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
        _ => return Err(validation("Invalid entity type.")),
    }
    Ok(())
}
pub(crate) fn attach(
    tx: &Transaction,
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
    tx.execute(
        "INSERT OR IGNORE INTO attachments(account_id,kind,entity,policy_arn) VALUES(?1,?2,?3,?4)",
        params![ctx.account_id, kind, name, a],
    )?;
    Ok(())
}
pub(crate) fn detach(
    tx: &Transaction,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
    a: &str,
) -> Result<(), AwsError> {
    check_entity(tx, ctx, kind, name)?;
    let n = tx.execute(
        "DELETE FROM attachments WHERE account_id=?1 AND kind=?2 AND entity=?3 AND policy_arn=?4",
        params![ctx.account_id, kind, name, a],
    )?;
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
    tx: &Transaction,
    ctx: &RequestContext,
    kind: &str,
    name: &str,
    prefix: Option<&str>,
    marker: Option<&str>,
    max: Option<i32>,
) -> Result<(Vec<AttachedPolicy>, bool, Option<String>), AwsError> {
    check_entity(tx, ctx, kind, name)?;
    let mut stmt=tx.prepare("SELECT a.policy_arn,p.name FROM attachments a JOIN policies p ON p.arn=a.policy_arn WHERE a.account_id=?1 AND a.kind=?2 AND a.entity=?3 AND substr(p.path,1,length(?4))=?4 ORDER BY p.name COLLATE NOCASE,p.arn")?;
    let items = stmt
        .query_map(
            params![ctx.account_id, kind, name, prefix.unwrap_or("/")],
            |r| {
                Ok(AttachedPolicy {
                    policy_arn: Some(r.get(0)?),
                    policy_name: Some(r.get(1)?),
                })
            },
        )?
        .collect::<Result<_, _>>()?;
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
        if !matches!(i.entity_filter.as_deref(), None | Some("User" | "Role" | "Group")) {
            return Err(validation("Invalid entity filter."));
        }
        if !matches!(i.policy_usage_filter.as_deref(), None | Some("PermissionsPolicy" | "PermissionsBoundary")) {
            return Err(validation("Invalid policy usage filter."));
        }
        let mut entities: Vec<(String, String, String)> = Vec::new();
        for (kind, table, id) in [("user", "users", "user_id"), ("role", "roles", "role_id"), ("group", "groups", "group_id")] {
            if i.entity_filter.as_ref().is_some_and(|f| !f.eq_ignore_ascii_case(kind)) { continue; }
            let sql = if i.policy_usage_filter.as_deref() == Some("PermissionsBoundary") {
                if kind == "group" { continue; }
                format!("SELECT e.name,e.{id} FROM {table} e WHERE e.account_id=?1 AND e.permissions_boundary=?2 AND substr(e.path,1,length(?3))=?3 ORDER BY e.name COLLATE NOCASE")
            } else {
                format!("SELECT e.name,e.{id} FROM {table} e JOIN attachments a ON a.account_id=e.account_id AND a.entity=e.name WHERE a.account_id=?1 AND a.policy_arn=?2 AND a.kind='{kind}' AND substr(e.path,1,length(?3))=?3 ORDER BY e.name COLLATE NOCASE")
            };
            let mut stmt=tx.prepare(&sql)?;
            let rows=stmt.query_map(params![ctx.account_id,i.policy_arn,i.path_prefix.as_deref().unwrap_or("/")],|r| Ok((kind.to_string(),r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
            entities.extend(rows.collect::<Result<Vec<_>,_>>()?);
        }
        let (entities,truncated,marker)=paginate(entities,i.marker.as_deref(),i.max_items)?;
        let mut out=ListEntitiesForPolicyResponse {is_truncated:Some(truncated),marker,..Default::default()};
        for (kind,name,id) in entities {
            match kind.as_str() {
                "user"=>out.policy_users.push(PolicyUser {user_name:Some(name),user_id:Some(id)}),
                "role"=>out.policy_roles.push(PolicyRole {role_name:Some(name),role_id:Some(id)}),
                _=>out.policy_groups.push(PolicyGroup {group_name:Some(name),group_id:Some(id)}),
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
    tx: &Transaction,
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
        tx.execute("DELETE FROM tags WHERE account_id=?1 AND kind='policy' AND entity=?2 AND key=?3 COLLATE NOCASE AND key<>?3",params![ctx.account_id,a,t.key])?;
        tx.execute("INSERT INTO tags(account_id,kind,entity,key,value) VALUES(?1,'policy',?2,?3,?4) ON CONFLICT(account_id,kind,entity,key) DO UPDATE SET value=excluded.value",params![ctx.account_id,a,t.key,t.value])?;
    }
    Ok(())
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
            .read(|db| Ok(db.query_row("SELECT COUNT(*) FROM policy_versions", [], |r| r.get(0))?))
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
