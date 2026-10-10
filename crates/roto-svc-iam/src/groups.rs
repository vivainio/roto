use crate::generated::*;
use crate::models::GroupRow;
use crate::roles::check_policy_document;
use crate::schema::*;
use crate::users::{load_user, user_out};
use crate::util::*;
use diesel::SqliteConnection;
use roto_core::diesel::{self, prelude::*};
use roto_core::store::DieselDb as Db;
use roto_core::{AwsError, RequestContext};

pub(crate) fn load_group(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
) -> Result<Group, AwsError> {
    groups::table
        .filter(groups::account_id.eq(&ctx.account_id))
        .filter(groups::name.eq(&name))
        .select(GroupRow::as_select())
        .first::<GroupRow>(tx)
        .map(|r| Group {
            arn: arn(ctx, &format!("group{}{}", r.path, r.name)),
            group_name: r.name,
            path: r.path,
            group_id: r.group_id,
            create_date: ts(r.created_at),
        })
        .optional()?
        .ok_or_else(|| AwsError::sender(404, "NoSuchEntity", format!("Group {name} not found")))
}
pub fn create_group(
    db: &Db,
    ctx: &RequestContext,
    i: CreateGroupRequest,
) -> Result<CreateGroupResponse, AwsError> {
    let path = normalize_path(i.path.as_deref())?;
    db.transaction(|tx| {
        let exists: bool = diesel::select(diesel::dsl::exists(
            groups::table
                .filter(groups::account_id.eq(&ctx.account_id))
                .filter(groups::name.eq(&i.group_name)),
        ))
        .first::<bool>(tx)?;
        if exists {
            return Err(crate::util::exists("Group", &i.group_name));
        }
        let row = GroupRow {
            account_id: ctx.account_id.clone(),
            name: i.group_name.clone(),
            path,
            group_id: gen_id("AGPA", 17),
            created_at: now(),
        };
        diesel::insert_into(groups::table)
            .values(&row)
            .execute(tx)?;
        Ok(CreateGroupResponse {
            group: load_group(tx, ctx, &i.group_name)?,
        })
    })
}
pub fn get_group(
    db: &Db,
    ctx: &RequestContext,
    i: GetGroupRequest,
) -> Result<GetGroupResponse, AwsError> {
    db.transaction(|tx| {
        let group = load_group(tx, ctx, &i.group_name)?;
        let names = group_members::table
            .filter(group_members::account_id.eq(&ctx.account_id))
            .filter(group_members::group_name.eq(&i.group_name))
            .order(group_members::user_name)
            .select(group_members::user_name)
            .load::<String>(tx)?;
        let (names, truncated, marker) = paginate(names, i.marker.as_deref(), i.max_items)?;
        let users = names
            .iter()
            .map(|n| {
                let row = load_user(tx, &ctx.account_id, n)?;
                user_out(tx, ctx, &row)
            })
            .collect::<Result<_, _>>()?;
        Ok(GetGroupResponse {
            group,
            users,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
pub fn list_groups(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupsRequest,
) -> Result<ListGroupsResponse, AwsError> {
    db.transaction(|tx| {
        let names = groups::table
            .filter(
                substr(
                    groups::path,
                    1,
                    length(literal_prefix(Some(
                        i.path_prefix.as_deref().unwrap_or("/"),
                    ))),
                )
                .eq(literal_prefix(Some(
                    i.path_prefix.as_deref().unwrap_or("/"),
                ))),
            )
            .filter(groups::account_id.eq(&ctx.account_id))
            .order(groups::name)
            .select(groups::name)
            .load::<String>(tx)?;
        let (names, truncated, marker) = paginate(names, i.marker.as_deref(), i.max_items)?;
        let groups = names
            .iter()
            .map(|n| load_group(tx, ctx, n))
            .collect::<Result<_, _>>()?;
        Ok(ListGroupsResponse {
            groups,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
pub fn list_groups_for_user(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupsForUserRequest,
) -> Result<ListGroupsForUserResponse, AwsError> {
    db.transaction(|tx| {
        load_user(tx, &ctx.account_id, &i.user_name)?;
        let names = group_members::table
            .filter(group_members::account_id.eq(&ctx.account_id))
            .filter(group_members::user_name.eq(&i.user_name))
            .order(group_members::group_name)
            .select(group_members::group_name)
            .load::<String>(tx)?;
        let (names, truncated, marker) = paginate(names, i.marker.as_deref(), i.max_items)?;
        let groups = names
            .iter()
            .map(|n| load_group(tx, ctx, n))
            .collect::<Result<_, _>>()?;
        Ok(ListGroupsForUserResponse {
            groups,
            is_truncated: Some(truncated),
            marker,
        })
    })
}
pub fn add_user_to_group(
    db: &Db,
    ctx: &RequestContext,
    i: AddUserToGroupRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_user(tx, &ctx.account_id, &i.user_name)?;
        load_group(tx, ctx, &i.group_name)?;
        diesel::insert_into(group_members::table)
            .values((
                group_members::account_id.eq(&ctx.account_id),
                group_members::group_name.eq(&i.group_name),
                group_members::user_name.eq(&i.user_name),
            ))
            .on_conflict_do_nothing()
            .execute(tx)?;
        Ok(())
    })
}
pub fn remove_user_from_group(
    db: &Db,
    ctx: &RequestContext,
    i: RemoveUserFromGroupRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_group(tx, ctx, &i.group_name)?;
        let n = diesel::delete(
            group_members::table
                .filter(group_members::account_id.eq(&ctx.account_id))
                .filter(group_members::group_name.eq(&i.group_name))
                .filter(group_members::user_name.eq(&i.user_name)),
        )
        .execute(tx)?;
        if n == 0 {
            return Err(AwsError::sender(
                404,
                "NoSuchEntity",
                format!("User {} not in group {}", i.user_name, i.group_name),
            ));
        }
        Ok(())
    })
}
pub fn delete_group(db: &Db, ctx: &RequestContext, i: DeleteGroupRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_group(tx, ctx, &i.group_name).map_err(|e| {
            if e.code == "NoSuchEntity" {
                no_such("group", &i.group_name)
            } else {
                e
            }
        })?;
        let members = group_members::table
            .filter(group_members::account_id.eq(&ctx.account_id))
            .filter(group_members::group_name.eq(&i.group_name))
            .count()
            .get_result::<i64>(tx)?
            + inline_policies::table
                .filter(inline_policies::account_id.eq(&ctx.account_id))
                .filter(inline_policies::kind.eq("group"))
                .filter(inline_policies::entity.eq(&i.group_name))
                .count()
                .get_result::<i64>(tx)?
            + attachments::table
                .filter(attachments::account_id.eq(&ctx.account_id))
                .filter(attachments::kind.eq("group"))
                .filter(attachments::entity.eq(&i.group_name))
                .count()
                .get_result::<i64>(tx)?;
        if members > 0 {
            return Err(conflict(
                "Cannot delete group, must remove users and policies first.",
            ));
        }
        diesel::delete(
            groups::table
                .filter(groups::account_id.eq(&ctx.account_id))
                .filter(groups::name.eq(&i.group_name)),
        )
        .execute(tx)?;
        Ok(())
    })
}
pub fn update_group(db: &Db, ctx: &RequestContext, i: UpdateGroupRequest) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let g = load_group(tx, ctx, &i.group_name).map_err(|e| {
            if e.code == "NoSuchEntity" {
                no_such("group", &i.group_name)
            } else {
                e
            }
        })?;
        let name = i.new_group_name.unwrap_or(g.group_name.clone());
        let path = match i.new_path {
            Some(p) => normalize_path(Some(&p))?,
            None => g.path,
        };
        let exists: bool = diesel::select(diesel::dsl::exists(
            groups::table
                .filter(groups::account_id.eq(&ctx.account_id))
                .filter(groups::name.eq(&name))
                .filter(groups::name.ne(&g.group_name)),
        ))
        .first::<bool>(tx)?;
        if exists {
            return Err(AwsError::sender(
                409,
                "Conflict",
                format!("Group {name} already exists"),
            ));
        }
        diesel::update(
            groups::table
                .filter(groups::account_id.eq(&ctx.account_id))
                .filter(groups::name.eq(&g.group_name)),
        )
        .set((groups::name.eq(&name), groups::path.eq(&path)))
        .execute(tx)?;
        diesel::update(
            group_members::table
                .filter(group_members::account_id.eq(&ctx.account_id))
                .filter(group_members::group_name.eq(&g.group_name)),
        )
        .set(group_members::group_name.eq(&name))
        .execute(tx)?;
        diesel::update(
            inline_policies::table
                .filter(inline_policies::account_id.eq(&ctx.account_id))
                .filter(inline_policies::kind.eq("group"))
                .filter(inline_policies::entity.eq(&g.group_name)),
        )
        .set(inline_policies::entity.eq(&name))
        .execute(tx)?;
        diesel::update(
            attachments::table
                .filter(attachments::account_id.eq(&ctx.account_id))
                .filter(attachments::kind.eq("group"))
                .filter(attachments::entity.eq(&g.group_name)),
        )
        .set(attachments::entity.eq(&name))
        .execute(tx)?;
        Ok(())
    })
}
pub fn put_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: PutGroupPolicyRequest,
) -> Result<(), AwsError> {
    check_policy_document(&i.policy_document)?;
    db.transaction(|tx| {
        load_group(tx, ctx, &i.group_name)?;
        diesel::insert_into(inline_policies::table)
            .values((
                inline_policies::account_id.eq(&ctx.account_id),
                inline_policies::kind.eq("group"),
                inline_policies::entity.eq(&i.group_name),
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
pub fn get_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: GetGroupPolicyRequest,
) -> Result<GetGroupPolicyResponse, AwsError> {
    db.transaction(|tx| {
        let g = load_group(tx, ctx, &i.group_name)?;
        let document = inline_policies::table
            .filter(inline_policies::account_id.eq(&ctx.account_id))
            .filter(inline_policies::kind.eq("group"))
            .filter(inline_policies::entity.eq(&i.group_name))
            .filter(inline_policies::name.eq(&i.policy_name))
            .select(inline_policies::document)
            .first::<String>(tx)
            .optional()?
            .ok_or_else(|| {
                AwsError::sender(
                    404,
                    "NoSuchEntity",
                    format!("Policy {} not found", i.policy_name),
                )
            })?;
        Ok(GetGroupPolicyResponse {
            group_name: g.group_name,
            policy_name: i.policy_name,
            policy_document: document,
        })
    })
}
pub fn delete_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DeleteGroupPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_group(tx, ctx, &i.group_name)?;
        let n = diesel::delete(
            inline_policies::table
                .filter(inline_policies::account_id.eq(&ctx.account_id))
                .filter(inline_policies::kind.eq("group"))
                .filter(inline_policies::entity.eq(&i.group_name))
                .filter(inline_policies::name.eq(&i.policy_name)),
        )
        .execute(tx)?;
        if n == 0 {
            return Err(no_such("policy", &i.policy_name));
        }
        Ok(())
    })
}
pub fn list_group_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupPoliciesRequest,
) -> Result<ListGroupPoliciesResponse, AwsError> {
    db.transaction(|tx| {
        load_group(tx, ctx, &i.group_name)?;
        let names = inline_policies::table
            .filter(inline_policies::account_id.eq(&ctx.account_id))
            .filter(inline_policies::kind.eq("group"))
            .filter(inline_policies::entity.eq(&i.group_name))
            .order(inline_policies::name)
            .select(inline_policies::name)
            .load::<String>(tx)?;
        let (policy_names, truncated, marker) = paginate(names, i.marker.as_deref(), i.max_items)?;
        Ok(ListGroupPoliciesResponse {
            policy_names,
            is_truncated: Some(truncated),
            marker,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Iam, Service};
    use roto_core::store::Store;
    #[test]
    fn rename_preserves_members_and_policies_across_restart() {
        let path = std::env::temp_dir().join(format!("roto-groups-{}", uuid::Uuid::new_v4()));
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        };
        let policy;
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            iam.create_group(
                &ctx,
                CreateGroupRequest {
                    group_name: "g".into(),
                    ..Default::default()
                },
            )
            .unwrap();
            for name in ["alice", "bob"] {
                iam.create_user(
                    &ctx,
                    CreateUserRequest {
                        user_name: name.into(),
                        ..Default::default()
                    },
                )
                .unwrap();
                for _ in 0..2 {
                    iam.add_user_to_group(
                        &ctx,
                        AddUserToGroupRequest {
                            group_name: "G".into(),
                            user_name: name.into(),
                        },
                    )
                    .unwrap();
                }
            }
            policy = iam
                .create_policy(
                    &ctx,
                    CreatePolicyRequest {
                        policy_name: "p".into(),
                        policy_document: "{}".into(),
                        ..Default::default()
                    },
                )
                .unwrap()
                .policy
                .unwrap()
                .arn
                .unwrap();
            iam.attach_group_policy(
                &ctx,
                AttachGroupPolicyRequest {
                    group_name: "g".into(),
                    policy_arn: policy.clone(),
                },
            )
            .unwrap();
            iam.put_group_policy(
                &ctx,
                PutGroupPolicyRequest {
                    group_name: "g".into(),
                    policy_name: "inline".into(),
                    policy_document: "{}".into(),
                },
            )
            .unwrap();
            iam.update_group(
                &ctx,
                UpdateGroupRequest {
                    group_name: "g".into(),
                    new_group_name: Some("renamed".into()),
                    new_path: Some("/team/".into()),
                },
            )
            .unwrap();
        }
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            let mut other = ctx.clone();
            other.account_id = "999999999999".into();
            assert!(
                iam.get_group(
                    &other,
                    GetGroupRequest {
                        group_name: "renamed".into(),
                        ..Default::default()
                    }
                )
                .is_err()
            );
            let page = iam
                .get_group(
                    &ctx,
                    GetGroupRequest {
                        group_name: "renamed".into(),
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert!(page.group.arn.ends_with("group/team/renamed"));
            assert_eq!(page.users.len(), 1);
            assert_eq!(page.is_truncated, Some(true));
            let next = iam
                .get_group(
                    &ctx,
                    GetGroupRequest {
                        group_name: "renamed".into(),
                        marker: page.marker,
                        max_items: Some(1),
                    },
                )
                .unwrap();
            assert_eq!(next.users[0].user_name, "bob");
            assert_eq!(
                iam.list_groups_for_user(
                    &ctx,
                    ListGroupsForUserRequest {
                        user_name: "alice".into(),
                        ..Default::default()
                    }
                )
                .unwrap()
                .groups[0]
                    .group_name,
                "renamed"
            );
            assert!(
                iam.delete_group(
                    &ctx,
                    DeleteGroupRequest {
                        group_name: "renamed".into()
                    }
                )
                .is_err()
            );
            for name in ["alice", "bob"] {
                iam.remove_user_from_group(
                    &ctx,
                    RemoveUserFromGroupRequest {
                        group_name: "renamed".into(),
                        user_name: name.into(),
                    },
                )
                .unwrap();
            }
            iam.delete_group_policy(
                &ctx,
                DeleteGroupPolicyRequest {
                    group_name: "renamed".into(),
                    policy_name: "inline".into(),
                },
            )
            .unwrap();
            iam.detach_group_policy(
                &ctx,
                DetachGroupPolicyRequest {
                    group_name: "renamed".into(),
                    policy_arn: policy,
                },
            )
            .unwrap();
            iam.delete_group(
                &ctx,
                DeleteGroupRequest {
                    group_name: "renamed".into(),
                },
            )
            .unwrap();
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
