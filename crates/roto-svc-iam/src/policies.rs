use crate::generated::{DeleteRolePolicyRequest, PutRolePolicyRequest};
use crate::roles::{check_policy_document, load_role};
use roto_core::rusqlite::params;
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
