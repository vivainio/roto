use std::sync::Arc;

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::{MIGRATIONS, misc, policies, roles, users};

pub struct Iam {
    pub(crate) db: Arc<Db>,
}

/// Who an IAM access key belongs to (used by STS and the server for multi-account routing).
#[derive(Debug, Clone)]
pub struct KeyOwner {
    pub account_id: String,
    pub user_name: String,
    pub user_id: String,
    pub path: String,
}

impl Iam {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("iam", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            for t in [
                "users",
                "groups",
                "group_members",
                "roles",
                "policies",
                "inline_policies",
                "attachments",
                "access_keys",
                "tags",
                "instance_profiles",
                "profile_roles",
                "account_aliases",
            ] {
                tx.execute(&format!("DELETE FROM {t}"), [])?;
            }
            Ok(())
        })
    }

    /// The id of the role with this ARN, if it exists.
    pub fn role_id_by_arn(&self, arn: &str) -> Option<String> {
        let account = arn.split(':').nth(4)?;
        let name = arn.rsplit('/').next()?;
        self.db
            .read(|c| {
                Ok(c.query_row(
                    "SELECT role_id FROM roles WHERE account_id = ?1 AND name = ?2",
                    params![account, name],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .ok()
            .flatten()
    }

    pub fn key_owner(&self, access_key: &str) -> Option<KeyOwner> {
        self.db
            .read(|c| {
                Ok(c.query_row(
                    "SELECT k.account_id, u.name, u.user_id, u.path FROM access_keys k
                     JOIN users u ON u.account_id = k.account_id AND u.name = k.user_name
                     WHERE k.access_key_id = ?1",
                    params![access_key],
                    |r| {
                        Ok(KeyOwner {
                            account_id: r.get(0)?,
                            user_name: r.get(1)?,
                            user_id: r.get(2)?,
                            path: r.get(3)?,
                        })
                    },
                )
                .optional()?)
            })
            .ok()
            .flatten()
    }
}

impl Service for Iam {
    fn put_role_policy(
        &self,
        ctx: &RequestContext,
        i: PutRolePolicyRequest,
    ) -> Result<(), AwsError> {
        policies::put_role_policy(&self.db, ctx, i)
    }
    fn delete_role_policy(
        &self,
        ctx: &RequestContext,
        i: DeleteRolePolicyRequest,
    ) -> Result<(), AwsError> {
        policies::delete_role_policy(&self.db, ctx, i)
    }
    fn attach_role_policy(
        &self,
        ctx: &RequestContext,
        i: AttachRolePolicyRequest,
    ) -> Result<(), AwsError> {
        roles::attach_role_policy(&self.db, ctx, i)
    }
    fn detach_role_policy(
        &self,
        ctx: &RequestContext,
        i: DetachRolePolicyRequest,
    ) -> Result<(), AwsError> {
        roles::detach_role_policy(&self.db, ctx, i)
    }
    fn create_user(
        &self,
        ctx: &RequestContext,
        i: CreateUserRequest,
    ) -> Result<CreateUserResponse, AwsError> {
        users::create_user(&self.db, ctx, i)
    }
    fn get_user(
        &self,
        ctx: &RequestContext,
        i: GetUserRequest,
    ) -> Result<GetUserResponse, AwsError> {
        users::get_user(&self.db, ctx, i)
    }
    fn list_users(
        &self,
        ctx: &RequestContext,
        i: ListUsersRequest,
    ) -> Result<ListUsersResponse, AwsError> {
        users::list_users(&self.db, ctx, i)
    }
    fn delete_user(&self, ctx: &RequestContext, i: DeleteUserRequest) -> Result<(), AwsError> {
        users::delete_user(&self.db, ctx, i)
    }
    fn update_user(&self, ctx: &RequestContext, i: UpdateUserRequest) -> Result<(), AwsError> {
        users::update_user(&self.db, ctx, i)
    }
    fn tag_user(&self, ctx: &RequestContext, i: TagUserRequest) -> Result<(), AwsError> {
        users::tag_user(&self.db, ctx, i)
    }
    fn untag_user(&self, ctx: &RequestContext, i: UntagUserRequest) -> Result<(), AwsError> {
        users::untag_user(&self.db, ctx, i)
    }
    fn list_user_tags(
        &self,
        ctx: &RequestContext,
        i: ListUserTagsRequest,
    ) -> Result<ListUserTagsResponse, AwsError> {
        users::list_user_tags(&self.db, ctx, i)
    }
    fn create_access_key(
        &self,
        ctx: &RequestContext,
        i: CreateAccessKeyRequest,
    ) -> Result<CreateAccessKeyResponse, AwsError> {
        users::create_access_key(&self.db, ctx, i)
    }
    fn list_access_keys(
        &self,
        ctx: &RequestContext,
        i: ListAccessKeysRequest,
    ) -> Result<ListAccessKeysResponse, AwsError> {
        users::list_access_keys(&self.db, ctx, i)
    }
    fn update_access_key(
        &self,
        ctx: &RequestContext,
        i: UpdateAccessKeyRequest,
    ) -> Result<(), AwsError> {
        users::update_access_key(&self.db, ctx, i)
    }
    fn delete_access_key(
        &self,
        ctx: &RequestContext,
        i: DeleteAccessKeyRequest,
    ) -> Result<(), AwsError> {
        users::delete_access_key(&self.db, ctx, i)
    }
    fn get_access_key_last_used(
        &self,
        ctx: &RequestContext,
        i: GetAccessKeyLastUsedRequest,
    ) -> Result<GetAccessKeyLastUsedResponse, AwsError> {
        users::get_access_key_last_used(&self.db, ctx, i)
    }
    fn create_account_alias(
        &self,
        ctx: &RequestContext,
        i: CreateAccountAliasRequest,
    ) -> Result<(), AwsError> {
        misc::create_account_alias(&self.db, ctx, i)
    }
    fn list_account_aliases(
        &self,
        ctx: &RequestContext,
        i: ListAccountAliasesRequest,
    ) -> Result<ListAccountAliasesResponse, AwsError> {
        misc::list_account_aliases(&self.db, ctx, i)
    }
    fn delete_account_alias(
        &self,
        ctx: &RequestContext,
        i: DeleteAccountAliasRequest,
    ) -> Result<(), AwsError> {
        misc::delete_account_alias(&self.db, ctx, i)
    }
    fn create_role(
        &self,
        ctx: &RequestContext,
        i: CreateRoleRequest,
    ) -> Result<CreateRoleResponse, AwsError> {
        roles::create_role(&self.db, ctx, i)
    }
    fn get_role(
        &self,
        ctx: &RequestContext,
        i: GetRoleRequest,
    ) -> Result<GetRoleResponse, AwsError> {
        roles::get_role(&self.db, ctx, i)
    }
    fn list_roles(
        &self,
        ctx: &RequestContext,
        i: ListRolesRequest,
    ) -> Result<ListRolesResponse, AwsError> {
        roles::list_roles(&self.db, ctx, i)
    }
    fn delete_role(&self, ctx: &RequestContext, i: DeleteRoleRequest) -> Result<(), AwsError> {
        roles::delete_role(&self.db, ctx, i)
    }
    fn update_role(
        &self,
        ctx: &RequestContext,
        i: UpdateRoleRequest,
    ) -> Result<UpdateRoleResponse, AwsError> {
        roles::update_role(&self.db, ctx, i)
    }
    fn update_role_description(
        &self,
        ctx: &RequestContext,
        i: UpdateRoleDescriptionRequest,
    ) -> Result<UpdateRoleDescriptionResponse, AwsError> {
        roles::update_role_description(&self.db, ctx, i)
    }
    fn update_assume_role_policy(
        &self,
        ctx: &RequestContext,
        i: UpdateAssumeRolePolicyRequest,
    ) -> Result<(), AwsError> {
        roles::update_assume_role_policy(&self.db, ctx, i)
    }
    fn tag_role(&self, ctx: &RequestContext, i: TagRoleRequest) -> Result<(), AwsError> {
        roles::tag_role(&self.db, ctx, i)
    }
    fn untag_role(&self, ctx: &RequestContext, i: UntagRoleRequest) -> Result<(), AwsError> {
        roles::untag_role(&self.db, ctx, i)
    }
    fn list_role_tags(
        &self,
        ctx: &RequestContext,
        i: ListRoleTagsRequest,
    ) -> Result<ListRoleTagsResponse, AwsError> {
        roles::list_role_tags(&self.db, ctx, i)
    }
}
