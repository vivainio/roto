use std::sync::Arc;

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::{MIGRATIONS, groups, misc, policies, roles, users};

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
                "policy_versions",
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
    fn create_group(
        &self,
        ctx: &RequestContext,
        i: CreateGroupRequest,
    ) -> Result<CreateGroupResponse, AwsError> {
        groups::create_group(&self.db, ctx, i)
    }
    fn get_group(
        &self,
        ctx: &RequestContext,
        i: GetGroupRequest,
    ) -> Result<GetGroupResponse, AwsError> {
        groups::get_group(&self.db, ctx, i)
    }
    fn list_groups(
        &self,
        ctx: &RequestContext,
        i: ListGroupsRequest,
    ) -> Result<ListGroupsResponse, AwsError> {
        groups::list_groups(&self.db, ctx, i)
    }
    fn list_groups_for_user(
        &self,
        ctx: &RequestContext,
        i: ListGroupsForUserRequest,
    ) -> Result<ListGroupsForUserResponse, AwsError> {
        groups::list_groups_for_user(&self.db, ctx, i)
    }
    fn get_group_policy(
        &self,
        ctx: &RequestContext,
        i: GetGroupPolicyRequest,
    ) -> Result<GetGroupPolicyResponse, AwsError> {
        groups::get_group_policy(&self.db, ctx, i)
    }
    fn list_group_policies(
        &self,
        ctx: &RequestContext,
        i: ListGroupPoliciesRequest,
    ) -> Result<ListGroupPoliciesResponse, AwsError> {
        groups::list_group_policies(&self.db, ctx, i)
    }
    fn list_attached_group_policies(
        &self,
        ctx: &RequestContext,
        i: ListAttachedGroupPoliciesRequest,
    ) -> Result<ListAttachedGroupPoliciesResponse, AwsError> {
        policies::list_attached_group_policies(&self.db, ctx, i)
    }
    fn delete_group(&self, ctx: &RequestContext, i: DeleteGroupRequest) -> Result<(), AwsError> {
        groups::delete_group(&self.db, ctx, i)
    }
    fn update_group(&self, ctx: &RequestContext, i: UpdateGroupRequest) -> Result<(), AwsError> {
        groups::update_group(&self.db, ctx, i)
    }
    fn add_user_to_group(
        &self,
        ctx: &RequestContext,
        i: AddUserToGroupRequest,
    ) -> Result<(), AwsError> {
        groups::add_user_to_group(&self.db, ctx, i)
    }
    fn remove_user_from_group(
        &self,
        ctx: &RequestContext,
        i: RemoveUserFromGroupRequest,
    ) -> Result<(), AwsError> {
        groups::remove_user_from_group(&self.db, ctx, i)
    }
    fn put_group_policy(
        &self,
        ctx: &RequestContext,
        i: PutGroupPolicyRequest,
    ) -> Result<(), AwsError> {
        groups::put_group_policy(&self.db, ctx, i)
    }
    fn delete_group_policy(
        &self,
        ctx: &RequestContext,
        i: DeleteGroupPolicyRequest,
    ) -> Result<(), AwsError> {
        groups::delete_group_policy(&self.db, ctx, i)
    }
    fn attach_group_policy(
        &self,
        ctx: &RequestContext,
        i: AttachGroupPolicyRequest,
    ) -> Result<(), AwsError> {
        policies::attach_group_policy(&self.db, ctx, i)
    }
    fn detach_group_policy(
        &self,
        ctx: &RequestContext,
        i: DetachGroupPolicyRequest,
    ) -> Result<(), AwsError> {
        policies::detach_group_policy(&self.db, ctx, i)
    }

    fn list_entities_for_policy(
        &self,
        ctx: &RequestContext,
        i: ListEntitiesForPolicyRequest,
    ) -> Result<ListEntitiesForPolicyResponse, AwsError> {
        policies::list_entities_for_policy(&self.db, ctx, i)
    }
    fn create_policy(
        &self,
        ctx: &RequestContext,
        i: CreatePolicyRequest,
    ) -> Result<CreatePolicyResponse, AwsError> {
        policies::create_policy(&self.db, ctx, i)
    }
    fn get_policy(
        &self,
        ctx: &RequestContext,
        i: GetPolicyRequest,
    ) -> Result<GetPolicyResponse, AwsError> {
        policies::get_policy(&self.db, ctx, i)
    }
    fn delete_policy(&self, ctx: &RequestContext, i: DeletePolicyRequest) -> Result<(), AwsError> {
        policies::delete_policy(&self.db, ctx, i)
    }
    fn list_policies(
        &self,
        ctx: &RequestContext,
        i: ListPoliciesRequest,
    ) -> Result<ListPoliciesResponse, AwsError> {
        policies::list_policies(&self.db, ctx, i)
    }
    fn create_policy_version(
        &self,
        ctx: &RequestContext,
        i: CreatePolicyVersionRequest,
    ) -> Result<CreatePolicyVersionResponse, AwsError> {
        policies::create_policy_version(&self.db, ctx, i)
    }
    fn get_policy_version(
        &self,
        ctx: &RequestContext,
        i: GetPolicyVersionRequest,
    ) -> Result<GetPolicyVersionResponse, AwsError> {
        policies::get_policy_version(&self.db, ctx, i)
    }
    fn delete_policy_version(
        &self,
        ctx: &RequestContext,
        i: DeletePolicyVersionRequest,
    ) -> Result<(), AwsError> {
        policies::delete_policy_version(&self.db, ctx, i)
    }
    fn list_policy_versions(
        &self,
        ctx: &RequestContext,
        i: ListPolicyVersionsRequest,
    ) -> Result<ListPolicyVersionsResponse, AwsError> {
        policies::list_policy_versions(&self.db, ctx, i)
    }
    fn set_default_policy_version(
        &self,
        ctx: &RequestContext,
        i: SetDefaultPolicyVersionRequest,
    ) -> Result<(), AwsError> {
        policies::set_default_policy_version(&self.db, ctx, i)
    }
    fn tag_policy(&self, ctx: &RequestContext, i: TagPolicyRequest) -> Result<(), AwsError> {
        policies::tag_policy(&self.db, ctx, i)
    }
    fn untag_policy(&self, ctx: &RequestContext, i: UntagPolicyRequest) -> Result<(), AwsError> {
        policies::untag_policy(&self.db, ctx, i)
    }
    fn list_policy_tags(
        &self,
        ctx: &RequestContext,
        i: ListPolicyTagsRequest,
    ) -> Result<ListPolicyTagsResponse, AwsError> {
        policies::list_policy_tags(&self.db, ctx, i)
    }
    fn attach_user_policy(
        &self,
        ctx: &RequestContext,
        i: AttachUserPolicyRequest,
    ) -> Result<(), AwsError> {
        policies::attach_user_policy(&self.db, ctx, i)
    }
    fn detach_user_policy(
        &self,
        ctx: &RequestContext,
        i: DetachUserPolicyRequest,
    ) -> Result<(), AwsError> {
        policies::detach_user_policy(&self.db, ctx, i)
    }
    fn list_attached_user_policies(
        &self,
        ctx: &RequestContext,
        i: ListAttachedUserPoliciesRequest,
    ) -> Result<ListAttachedUserPoliciesResponse, AwsError> {
        policies::list_attached_user_policies(&self.db, ctx, i)
    }
    fn list_attached_role_policies(
        &self,
        ctx: &RequestContext,
        i: ListAttachedRolePoliciesRequest,
    ) -> Result<ListAttachedRolePoliciesResponse, AwsError> {
        policies::list_attached_role_policies(&self.db, ctx, i)
    }

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
        policies::attach_role_policy(&self.db, ctx, i)
    }
    fn detach_role_policy(
        &self,
        ctx: &RequestContext,
        i: DetachRolePolicyRequest,
    ) -> Result<(), AwsError> {
        policies::detach_role_policy(&self.db, ctx, i)
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
