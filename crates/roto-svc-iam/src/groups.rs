use crate::generated::*;
use crate::roles::check_policy_document;
use crate::users::{load_user, user_out};
use crate::util::*;
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

pub(crate) fn load_group(
    tx: &Transaction,
    ctx: &RequestContext,
    name: &str,
) -> Result<Group, AwsError> {
    tx.query_row(
        "SELECT name,path,group_id,created_at FROM groups WHERE account_id=?1 AND name=?2",
        params![ctx.account_id, name],
        |r| {
            let name: String = r.get(0)?;
            let path: String = r.get(1)?;
            Ok(Group {
                arn: arn(ctx, &format!("group{path}{name}")),
                group_name: name,
                path,
                group_id: r.get(2)?,
                create_date: ts(r.get(3)?),
            })
        },
    )
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
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM groups WHERE account_id=?1 AND name=?2)",
            params![ctx.account_id, i.group_name],
            |r| r.get(0),
        )?;
        if exists {
            return Err(crate::util::exists("Group", &i.group_name));
        }
        tx.execute(
            "INSERT INTO groups(account_id,name,path,group_id,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![
                ctx.account_id,
                i.group_name,
                path,
                gen_id("AGPA", 17),
                now()
            ],
        )?;
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
    db.transaction(|tx|{
        let group=load_group(tx,ctx,&i.group_name)?;
        let mut stmt=tx.prepare("SELECT user_name FROM group_members WHERE account_id=?1 AND group_name=?2 ORDER BY user_name COLLATE NOCASE")?;
        let names:Vec<String>=stmt.query_map(params![ctx.account_id,i.group_name],|r|r.get(0))?.collect::<Result<_,_>>()?;
        let (names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        let users=names.iter().map(|n|user_out(tx,ctx,&load_user(tx,&ctx.account_id,n)?)).collect::<Result<_,_>>()?;
        Ok(GetGroupResponse {group,users,is_truncated:Some(truncated),marker})
    })
}
pub fn list_groups(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupsRequest,
) -> Result<ListGroupsResponse, AwsError> {
    db.transaction(|tx|{
        let mut stmt=tx.prepare("SELECT name FROM groups WHERE account_id=?1 AND substr(path,1,length(?2))=?2 ORDER BY name COLLATE NOCASE")?;
        let names:Vec<String>=stmt.query_map(params![ctx.account_id,i.path_prefix.as_deref().unwrap_or("/")],|r|r.get(0))?.collect::<Result<_,_>>()?;
        let (names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        let groups=names.iter().map(|n|load_group(tx,ctx,n)).collect::<Result<_,_>>()?;
        Ok(ListGroupsResponse {groups,is_truncated:Some(truncated),marker})
    })
}
pub fn list_groups_for_user(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupsForUserRequest,
) -> Result<ListGroupsForUserResponse, AwsError> {
    db.transaction(|tx|{
        load_user(tx,&ctx.account_id,&i.user_name)?;
        let mut stmt=tx.prepare("SELECT group_name FROM group_members WHERE account_id=?1 AND user_name=?2 ORDER BY group_name COLLATE NOCASE")?;
        let names:Vec<String>=stmt.query_map(params![ctx.account_id,i.user_name],|r|r.get(0))?.collect::<Result<_,_>>()?;
        let (names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        let groups=names.iter().map(|n|load_group(tx,ctx,n)).collect::<Result<_,_>>()?;
        Ok(ListGroupsForUserResponse {groups,is_truncated:Some(truncated),marker})
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
        tx.execute(
            "INSERT OR IGNORE INTO group_members(account_id,group_name,user_name) VALUES(?1,?2,?3)",
            params![ctx.account_id, i.group_name, i.user_name],
        )?;
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
        let n = tx.execute(
            "DELETE FROM group_members WHERE account_id=?1 AND group_name=?2 AND user_name=?3",
            params![ctx.account_id, i.group_name, i.user_name],
        )?;
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
    db.transaction(|tx|{
        load_group(tx,ctx,&i.group_name).map_err(|e|if e.code=="NoSuchEntity" {no_such("group",&i.group_name)}else{e})?;
        let members:i64=tx.query_row("SELECT (SELECT COUNT(*) FROM group_members WHERE account_id=?1 AND group_name=?2)+(SELECT COUNT(*) FROM inline_policies WHERE account_id=?1 AND kind='group' AND entity=?2)+(SELECT COUNT(*) FROM attachments WHERE account_id=?1 AND kind='group' AND entity=?2)",params![ctx.account_id,i.group_name],|r|r.get(0))?;
        if members>0{return Err(conflict("Cannot delete group, must remove users and policies first."));}
        tx.execute("DELETE FROM groups WHERE account_id=?1 AND name=?2",params![ctx.account_id,i.group_name])?;Ok(())
    })
}
pub fn update_group(db: &Db, ctx: &RequestContext, i: UpdateGroupRequest) -> Result<(), AwsError> {
    db.transaction(|tx|{
        let g=load_group(tx,ctx,&i.group_name).map_err(|e|if e.code=="NoSuchEntity" {no_such("group",&i.group_name)}else{e})?;
        let name=i.new_group_name.unwrap_or(g.group_name.clone());
        let path=match i.new_path {Some(p)=>normalize_path(Some(&p))?,None=>g.path};
        let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM groups WHERE account_id=?1 AND name=?2 AND name<>?3)",params![ctx.account_id,name,g.group_name],|r|r.get(0))?;
        if exists{return Err(AwsError::sender(409,"Conflict",format!("Group {name} already exists")));}
        tx.execute("UPDATE groups SET name=?1,path=?2 WHERE account_id=?3 AND name=?4",params![name,path,ctx.account_id,g.group_name])?;
        for sql in ["UPDATE group_members SET group_name=?1 WHERE account_id=?2 AND group_name=?3","UPDATE inline_policies SET entity=?1 WHERE account_id=?2 AND kind='group' AND entity=?3","UPDATE attachments SET entity=?1 WHERE account_id=?2 AND kind='group' AND entity=?3"]{tx.execute(sql,params![name,ctx.account_id,g.group_name])?;}
        Ok(())
    })
}
pub fn put_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: PutGroupPolicyRequest,
) -> Result<(), AwsError> {
    check_policy_document(&i.policy_document)?;
    db.transaction(|tx|{
        load_group(tx,ctx,&i.group_name)?;
        tx.execute("INSERT INTO inline_policies(account_id,kind,entity,name,document) VALUES(?1,'group',?2,?3,?4) ON CONFLICT(account_id,kind,entity,name) DO UPDATE SET document=excluded.document",params![ctx.account_id,i.group_name,i.policy_name,i.policy_document])?;Ok(())
    })
}
pub fn get_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: GetGroupPolicyRequest,
) -> Result<GetGroupPolicyResponse, AwsError> {
    db.transaction(|tx|{
        let g=load_group(tx,ctx,&i.group_name)?;
        let document=tx.query_row("SELECT document FROM inline_policies WHERE account_id=?1 AND kind='group' AND entity=?2 AND name=?3",params![ctx.account_id,i.group_name,i.policy_name],|r|r.get(0)).optional()?.ok_or_else(||AwsError::sender(404,"NoSuchEntity",format!("Policy {} not found",i.policy_name)))?;
        Ok(GetGroupPolicyResponse {group_name:g.group_name,policy_name:i.policy_name,policy_document:document})
    })
}
pub fn delete_group_policy(
    db: &Db,
    ctx: &RequestContext,
    i: DeleteGroupPolicyRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx|{
        load_group(tx,ctx,&i.group_name)?;
        let n=tx.execute("DELETE FROM inline_policies WHERE account_id=?1 AND kind='group' AND entity=?2 AND name=?3",params![ctx.account_id,i.group_name,i.policy_name])?;
        if n==0{return Err(no_such("policy",&i.policy_name));}Ok(())
    })
}
pub fn list_group_policies(
    db: &Db,
    ctx: &RequestContext,
    i: ListGroupPoliciesRequest,
) -> Result<ListGroupPoliciesResponse, AwsError> {
    db.transaction(|tx|{
        load_group(tx,ctx,&i.group_name)?;
        let mut stmt=tx.prepare("SELECT name FROM inline_policies WHERE account_id=?1 AND kind='group' AND entity=?2 ORDER BY name")?;
        let names=stmt.query_map(params![ctx.account_id,i.group_name],|r|r.get(0))?.collect::<Result<Vec<String>,_>>()?;
        let (policy_names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        Ok(ListGroupPoliciesResponse {policy_names,is_truncated:Some(truncated),marker})
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
