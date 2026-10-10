use crate::generated::*;
use crate::misc::{load_tags, remove_tags, set_tags};
use crate::roles::{load_role, role_out};
use crate::util::*;
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::Db;
use roto_core::{AwsError, RequestContext};

fn load_profile(
    tx: &Transaction,
    ctx: &RequestContext,
    name: &str,
) -> Result<InstanceProfile, AwsError> {
    let mut profile = tx.query_row(
        "SELECT name,path,profile_id,created_at FROM instance_profiles WHERE account_id=?1 AND name=?2",
        params![ctx.account_id,name], |r| {
            let name: String = r.get(0)?;
            let path: String = r.get(1)?;
            Ok(InstanceProfile {
                arn: arn(ctx, &format!("instance-profile{path}{name}")),
                instance_profile_name: name, path, instance_profile_id: r.get(2)?,
                create_date: ts(r.get(3)?), roles: Vec::new(), tags: Vec::new(),
            })
        },
    ).optional()?.ok_or_else(|| AwsError::sender(404, "NoSuchEntity", format!("Instance profile {name} not found")))?;
    let mut stmt = tx.prepare("SELECT role FROM profile_roles WHERE account_id=?1 AND profile=?2 ORDER BY role COLLATE NOCASE")?;
    let names = stmt
        .query_map(params![ctx.account_id, name], |r| r.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    profile.roles = names
        .iter()
        .map(|n| role_out(tx, ctx, &load_role(tx, &ctx.account_id, n)?))
        .collect::<Result<_, _>>()?;
    profile.tags = load_tags(tx, &ctx.account_id, "instance-profile", name)?;
    Ok(profile)
}

pub fn create_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: CreateInstanceProfileRequest,
) -> Result<CreateInstanceProfileResponse, AwsError> {
    let path = normalize_path(i.path.as_deref())?;
    db.transaction(|tx| {
        let taken: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM instance_profiles WHERE account_id=?1 AND name=?2)",params![ctx.account_id,i.instance_profile_name],|r|r.get(0))?;
        if taken { return Err(exists("Instance profile",&i.instance_profile_name)); }
        tx.execute("INSERT INTO instance_profiles(account_id,name,path,profile_id,created_at) VALUES(?1,?2,?3,?4,?5)",params![ctx.account_id,i.instance_profile_name,path,gen_id("AIPA",17),now()])?;
        set_tags(tx,&ctx.account_id,"instance-profile",&i.instance_profile_name,&i.tags)?;
        Ok(CreateInstanceProfileResponse {instance_profile:load_profile(tx,ctx,&i.instance_profile_name)?})
    })
}

pub fn get_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: GetInstanceProfileRequest,
) -> Result<GetInstanceProfileResponse, AwsError> {
    db.transaction(|tx| {
        Ok(GetInstanceProfileResponse {
            instance_profile: load_profile(tx, ctx, &i.instance_profile_name)?,
        })
    })
}

pub fn delete_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: DeleteInstanceProfileRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let profile = load_profile(tx, ctx, &i.instance_profile_name)?;
        if !profile.roles.is_empty() {
            return Err(conflict(
                "Cannot delete entity, must remove roles from instance profile first.",
            ));
        }
        tx.execute(
            "DELETE FROM tags WHERE account_id=?1 AND kind='instance-profile' AND entity=?2",
            params![ctx.account_id, i.instance_profile_name],
        )?;
        tx.execute(
            "DELETE FROM instance_profiles WHERE account_id=?1 AND name=?2",
            params![ctx.account_id, i.instance_profile_name],
        )?;
        Ok(())
    })
}

pub fn add_role_to_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: AddRoleToInstanceProfileRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        let profile = load_profile(tx, ctx, &i.instance_profile_name)?;
        let role = load_role(tx, &ctx.account_id, &i.role_name)?;
        if !profile.roles.is_empty() {
            return Err(AwsError::sender(
                409,
                "LimitExceeded",
                "Cannot exceed quota for InstanceSessionsPerInstanceProfile: 1",
            ));
        }
        tx.execute(
            "INSERT INTO profile_roles(account_id,profile,role) VALUES(?1,?2,?3)",
            params![ctx.account_id, profile.instance_profile_name, role.name],
        )?;
        Ok(())
    })
}

pub fn remove_role_from_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: RemoveRoleFromInstanceProfileRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_profile(tx, ctx, &i.instance_profile_name)?;
        load_role(tx, &ctx.account_id, &i.role_name)?;
        let removed = tx.execute(
            "DELETE FROM profile_roles WHERE account_id=?1 AND profile=?2 AND role=?3",
            params![ctx.account_id, i.instance_profile_name, i.role_name],
        )?;
        if removed == 0 {
            return Err(no_such("role in instance profile", &i.role_name));
        }
        Ok(())
    })
}

pub fn list_instance_profiles(
    db: &Db,
    ctx: &RequestContext,
    i: ListInstanceProfilesRequest,
) -> Result<ListInstanceProfilesResponse, AwsError> {
    db.transaction(|tx| {
        let mut stmt=tx.prepare("SELECT name FROM instance_profiles WHERE account_id=?1 AND substr(path,1,length(?2))=?2 ORDER BY name COLLATE NOCASE")?;
        let names=stmt.query_map(params![ctx.account_id,i.path_prefix.as_deref().unwrap_or("/")],|r|r.get(0))?.collect::<Result<Vec<String>,_>>()?;
        let (names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        let instance_profiles=names.iter().map(|n|load_profile(tx,ctx,n)).collect::<Result<_,_>>()?;
        Ok(ListInstanceProfilesResponse {instance_profiles,is_truncated:Some(truncated),marker})
    })
}

pub fn list_instance_profiles_for_role(
    db: &Db,
    ctx: &RequestContext,
    i: ListInstanceProfilesForRoleRequest,
) -> Result<ListInstanceProfilesForRoleResponse, AwsError> {
    db.transaction(|tx| {
        load_role(tx,&ctx.account_id,&i.role_name)?;
        let mut stmt=tx.prepare("SELECT profile FROM profile_roles WHERE account_id=?1 AND role=?2 ORDER BY profile COLLATE NOCASE")?;
        let names=stmt.query_map(params![ctx.account_id,i.role_name],|r|r.get(0))?.collect::<Result<Vec<String>,_>>()?;
        let (names,truncated,marker)=paginate(names,i.marker.as_deref(),i.max_items)?;
        let instance_profiles=names.iter().map(|n|load_profile(tx,ctx,n)).collect::<Result<_,_>>()?;
        Ok(ListInstanceProfilesForRoleResponse {instance_profiles,is_truncated:Some(truncated),marker})
    })
}

pub fn tag_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: TagInstanceProfileRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_profile(tx, ctx, &i.instance_profile_name)?;
        set_tags(
            tx,
            &ctx.account_id,
            "instance-profile",
            &i.instance_profile_name,
            &i.tags,
        )
    })
}

pub fn untag_instance_profile(
    db: &Db,
    ctx: &RequestContext,
    i: UntagInstanceProfileRequest,
) -> Result<(), AwsError> {
    db.transaction(|tx| {
        load_profile(tx, ctx, &i.instance_profile_name)?;
        remove_tags(
            tx,
            &ctx.account_id,
            "instance-profile",
            &i.instance_profile_name,
            &i.tag_keys,
        )
    })
}

pub fn list_instance_profile_tags(
    db: &Db,
    ctx: &RequestContext,
    i: ListInstanceProfileTagsRequest,
) -> Result<ListInstanceProfileTagsResponse, AwsError> {
    db.transaction(|tx| {
        let profile = load_profile(tx, ctx, &i.instance_profile_name)?;
        let (tags, truncated, marker) = paginate(profile.tags, i.marker.as_deref(), i.max_items)?;
        Ok(ListInstanceProfileTagsResponse {
            tags,
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

    fn context() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn create_role(iam: &Iam, ctx: &RequestContext, name: &str) {
        iam.create_role(
            ctx,
            CreateRoleRequest {
                role_name: name.into(),
                assume_role_policy_document: "{}".into(),
                ..Default::default()
            },
        )
        .unwrap();
    }
    fn create_profile(iam: &Iam, ctx: &RequestContext, name: &str) -> InstanceProfile {
        iam.create_instance_profile(
            ctx,
            CreateInstanceProfileRequest {
                instance_profile_name: name.into(),
                path: Some("/app/".into()),
                tags: vec![Tag {
                    key: "team".into(),
                    value: "dev".into(),
                }],
            },
        )
        .unwrap()
        .instance_profile
    }

    #[test]
    fn role_limit_conflicts_isolation_and_cleanup() {
        let store = Store::ephemeral();
        let iam = Iam::new(&store).unwrap();
        let ctx = context();
        let profile = create_profile(&iam, &ctx, "worker");
        assert!(profile.arn.ends_with("instance-profile/app/worker"));
        create_role(&iam, &ctx, "first");
        create_role(&iam, &ctx, "second");
        iam.add_role_to_instance_profile(
            &ctx,
            AddRoleToInstanceProfileRequest {
                instance_profile_name: "WORKER".into(),
                role_name: "FIRST".into(),
            },
        )
        .unwrap();
        let err = iam
            .add_role_to_instance_profile(
                &ctx,
                AddRoleToInstanceProfileRequest {
                    instance_profile_name: "worker".into(),
                    role_name: "second".into(),
                },
            )
            .unwrap_err();
        assert_eq!(err.code, "LimitExceeded");
        assert_eq!(
            iam.delete_role(
                &ctx,
                DeleteRoleRequest {
                    role_name: "first".into()
                }
            )
            .unwrap_err()
            .code,
            "DeleteConflict"
        );
        assert_eq!(
            iam.delete_instance_profile(
                &ctx,
                DeleteInstanceProfileRequest {
                    instance_profile_name: "worker".into()
                }
            )
            .unwrap_err()
            .code,
            "DeleteConflict"
        );
        let mut other = ctx.clone();
        other.account_id = "999999999999".into();
        create_role(&iam, &other, "first");
        create_profile(&iam, &other, "worker");
        assert!(
            iam.get_instance_profile(
                &other,
                GetInstanceProfileRequest {
                    instance_profile_name: "worker".into()
                }
            )
            .unwrap()
            .instance_profile
            .roles
            .is_empty()
        );
        assert!(
            iam.remove_role_from_instance_profile(
                &other,
                RemoveRoleFromInstanceProfileRequest {
                    instance_profile_name: "worker".into(),
                    role_name: "first".into()
                }
            )
            .is_err()
        );
        assert_eq!(
            iam.list_instance_profiles_for_role(
                &ctx,
                ListInstanceProfilesForRoleRequest {
                    role_name: "first".into(),
                    ..Default::default()
                }
            )
            .unwrap()
            .instance_profiles
            .len(),
            1
        );
        iam.remove_role_from_instance_profile(
            &ctx,
            RemoveRoleFromInstanceProfileRequest {
                instance_profile_name: "worker".into(),
                role_name: "first".into(),
            },
        )
        .unwrap();
        iam.delete_role(
            &ctx,
            DeleteRoleRequest {
                role_name: "first".into(),
            },
        )
        .unwrap();
        iam.delete_instance_profile(
            &ctx,
            DeleteInstanceProfileRequest {
                instance_profile_name: "worker".into(),
            },
        )
        .unwrap();
        assert_eq!(create_profile(&iam, &ctx, "worker").tags.len(), 1);
        iam.reset().unwrap();
        create_profile(&iam, &ctx, "worker");
        assert!(
            iam.list_instance_profiles_for_role(
                &ctx,
                ListInstanceProfilesForRoleRequest {
                    role_name: "first".into(),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn tags_can_be_replaced_at_capacity_and_invalid_create_rolls_back() {
        let store = Store::ephemeral();
        let iam = Iam::new(&store).unwrap();
        let ctx = context();
        let tags: Vec<Tag> = (0..50)
            .map(|n| Tag {
                key: format!("key{n}"),
                value: "old".into(),
            })
            .collect();
        iam.create_instance_profile(
            &ctx,
            CreateInstanceProfileRequest {
                instance_profile_name: "full".into(),
                tags,
                ..Default::default()
            },
        )
        .unwrap();
        iam.tag_instance_profile(
            &ctx,
            TagInstanceProfileRequest {
                instance_profile_name: "full".into(),
                tags: vec![Tag {
                    key: "key0".into(),
                    value: "new".into(),
                }],
            },
        )
        .unwrap();
        assert!(
            iam.tag_instance_profile(
                &ctx,
                TagInstanceProfileRequest {
                    instance_profile_name: "full".into(),
                    tags: vec![Tag {
                        key: "extra".into(),
                        value: "new".into()
                    }]
                }
            )
            .is_err()
        );
        let tags = iam
            .get_instance_profile(
                &ctx,
                GetInstanceProfileRequest {
                    instance_profile_name: "full".into(),
                },
            )
            .unwrap()
            .instance_profile
            .tags;
        assert_eq!(tags.len(), 50);
        assert_eq!(tags[0].value, "new");
        assert!(
            iam.create_instance_profile(
                &ctx,
                CreateInstanceProfileRequest {
                    instance_profile_name: "invalid".into(),
                    tags: vec![Tag {
                        key: "".into(),
                        value: "bad".into()
                    }],
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            iam.get_instance_profile(
                &ctx,
                GetInstanceProfileRequest {
                    instance_profile_name: "invalid".into()
                }
            )
            .is_err()
        );
        iam.create_instance_profile(
            &ctx,
            CreateInstanceProfileRequest {
                instance_profile_name: "invalid".into(),
                ..Default::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn profiles_roles_and_tags_survive_restart_with_pagination() {
        let path = std::env::temp_dir().join(format!("roto-profiles-{}", uuid::Uuid::new_v4()));
        let ctx = context();
        let id;
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            create_role(&iam, &ctx, "role");
            id = create_profile(&iam, &ctx, "a").instance_profile_id;
            create_profile(&iam, &ctx, "b");
            for name in ["a", "b"] {
                iam.add_role_to_instance_profile(
                    &ctx,
                    AddRoleToInstanceProfileRequest {
                        instance_profile_name: name.into(),
                        role_name: "role".into(),
                    },
                )
                .unwrap();
            }
            iam.tag_instance_profile(
                &ctx,
                TagInstanceProfileRequest {
                    instance_profile_name: "a".into(),
                    tags: vec![
                        Tag {
                            key: "team".into(),
                            value: "updated".into(),
                        },
                        Tag {
                            key: "env".into(),
                            value: "test".into(),
                        },
                    ],
                },
            )
            .unwrap();
        }
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let iam = Iam::new(&store).unwrap();
            let p = iam
                .get_instance_profile(
                    &ctx,
                    GetInstanceProfileRequest {
                        instance_profile_name: "a".into(),
                    },
                )
                .unwrap()
                .instance_profile;
            assert_eq!(p.instance_profile_id, id);
            assert_eq!(p.roles[0].role_name, "role");
            assert_eq!(p.tags[0].value, "updated");
            let first = iam
                .list_instance_profiles(
                    &ctx,
                    ListInstanceProfilesRequest {
                        path_prefix: Some("/app/".into()),
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(first.is_truncated, Some(true));
            let next = iam
                .list_instance_profiles(
                    &ctx,
                    ListInstanceProfilesRequest {
                        marker: first.marker,
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(next.instance_profiles[0].instance_profile_name, "b");
            assert_eq!(next.is_truncated, Some(false));
            assert!(
                iam.list_instance_profiles(
                    &ctx,
                    ListInstanceProfilesRequest {
                        path_prefix: Some("/missing/".into()),
                        ..Default::default()
                    }
                )
                .unwrap()
                .instance_profiles
                .is_empty()
            );
            let roles = iam
                .list_instance_profiles_for_role(
                    &ctx,
                    ListInstanceProfilesForRoleRequest {
                        role_name: "role".into(),
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            let next = iam
                .list_instance_profiles_for_role(
                    &ctx,
                    ListInstanceProfilesForRoleRequest {
                        role_name: "role".into(),
                        max_items: Some(1),
                        marker: roles.marker,
                    },
                )
                .unwrap();
            assert_eq!(next.instance_profiles[0].instance_profile_name, "b");
            let tags = iam
                .list_instance_profile_tags(
                    &ctx,
                    ListInstanceProfileTagsRequest {
                        instance_profile_name: "a".into(),
                        max_items: Some(1),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(tags.is_truncated, Some(true));
            let next = iam
                .list_instance_profile_tags(
                    &ctx,
                    ListInstanceProfileTagsRequest {
                        instance_profile_name: "a".into(),
                        max_items: Some(1),
                        marker: tags.marker,
                    },
                )
                .unwrap();
            assert_eq!(next.tags[0].key, "env");
            iam.untag_instance_profile(
                &ctx,
                UntagInstanceProfileRequest {
                    instance_profile_name: "a".into(),
                    tag_keys: vec!["env".into()],
                },
            )
            .unwrap();
            assert_eq!(
                iam.get_instance_profile(
                    &ctx,
                    GetInstanceProfileRequest {
                        instance_profile_name: "a".into()
                    }
                )
                .unwrap()
                .instance_profile
                .tags
                .len(),
                1
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
