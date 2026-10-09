//! STS (query protocol). Types and dispatch are generated from the botocore model into
//! `generated.rs` (see `scripts/gen.sh`); this file holds only the behaviour.
//!
//! Credentials are issued but never enforced: no trust-policy or permission checks. What matters
//! is that a session's access key maps back to an identity and account, so `GetCallerIdentity`
//! and multi-account routing work.

#[allow(clippy::all)]
mod generated;

use std::sync::Arc;

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{QueryParams, Timestamp, base64, query_error};
use roto_svc_iam::Iam;

use generated::*;
pub use generated::{NAMESPACE, OPERATIONS, Service, dispatch};

/// Operations with real behaviour (the rest answer `NotImplemented`). Feeds the coverage matrix.
pub const IMPLEMENTED: &[&str] = &[
    "GetCallerIdentity",
    "GetAccessKeyInfo",
    "GetSessionToken",
    "GetFederationToken",
    "AssumeRole",
    "AssumeRoleWithWebIdentity",
    "AssumeRoleWithSAML",
];

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE sessions (
    access_key_id TEXT NOT NULL PRIMARY KEY,
    account_id TEXT NOT NULL,
    role_arn TEXT NOT NULL,
    session_name TEXT NOT NULL,
    role_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
",
}];

/// moto's fixed fixture credentials for `GetSessionToken` / `GetFederationToken`.
const FIXTURE_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const FIXTURE_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY";
const SESSION_TOKEN: &str = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3zrkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtpZ3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE";
const FEDERATION_TOKEN: &str = "AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQWLWsKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGdQrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==";

const MAX_POLICY_LEN: usize = 2048;

pub struct Sts {
    db: Arc<Db>,
    iam: Arc<Iam>,
}

impl Sts {
    pub fn new(store: &Store, iam: Arc<Iam>) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("sts", MIGRATIONS)?,
            iam,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM sessions", [])?;
            Ok(())
        })
    }

    fn session_account(&self, access_key: &str) -> Option<String> {
        self.db
            .read(|c| {
                Ok(c.query_row(
                    "SELECT account_id FROM sessions WHERE access_key_id = ?1",
                    params![access_key],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .ok()
            .flatten()
    }

    /// Issues credentials for a (possibly non-existent) role; the account comes from the role ARN.
    fn assume(
        &self,
        ctx: &RequestContext,
        role_arn: &str,
        session_name: &str,
        duration: Option<i32>,
        policy: &Option<String>,
    ) -> Result<(Credentials, AssumedRoleUser), AwsError> {
        check_policy(policy)?;
        let duration = i64::from(duration.unwrap_or(3600));
        let account = role_arn
            .split(':')
            .nth(4)
            .filter(|a| !a.is_empty())
            .unwrap_or(&ctx.account_id)
            .to_string();
        let role_name = role_arn.rsplit('/').next().unwrap_or(role_arn);
        let role_id = self
            .iam
            .role_id_by_arn(role_arn)
            .unwrap_or_else(|| random_id("AROA", "3X42LBCD", 9));
        let (access_key, secret, token) = (
            random_id("ASIA", "", 16),
            random_secret(),
            random_session_token(),
        );
        let expires = now() + duration;
        self.db.transaction(|tx| {
            tx.execute(
                "INSERT INTO sessions (access_key_id, account_id, role_arn, session_name, role_id, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![access_key, account, role_arn, session_name, role_id, expires],
            )?;
            Ok(())
        })?;
        let partition = partition(&ctx.region);
        Ok((
            Credentials {
                access_key_id: access_key,
                secret_access_key: secret,
                session_token: token,
                expiration: Timestamp(expires),
            },
            AssumedRoleUser {
                arn: format!(
                    "arn:{partition}:sts::{account}:assumed-role/{role_name}/{session_name}"
                ),
                assumed_role_id: format!("{role_id}:{session_name}"),
            },
        ))
    }
}

fn check_policy(policy: &Option<String>) -> Result<(), AwsError> {
    match policy {
        Some(p) if p.len() > MAX_POLICY_LEN => Err(AwsError::sender(
            400,
            "ValidationError",
            format!(
                "1 validation error detected: Value '{p}' at 'policy' failed to satisfy constraint: Member must have length less than or equal to {MAX_POLICY_LEN}"
            ),
        )),
        _ => Ok(()),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else {
        "aws"
    }
}

fn random_bytes(n: usize) -> Vec<u8> {
    (0..n.div_ceil(16))
        .flat_map(|_| *uuid::Uuid::new_v4().as_bytes())
        .take(n)
        .collect()
}

/// `prefix` + `fixed` + random uppercase alphanumerics, `len` random characters in total.
fn random_id(prefix: &str, fixed: &str, len: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let tail: String = random_bytes(len)
        .iter()
        .map(|b| CHARS[*b as usize % CHARS.len()] as char)
        .collect();
    format!("{prefix}{fixed}{tail}")
}

fn random_secret() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    random_bytes(40)
        .iter()
        .map(|b| CHARS[*b as usize % CHARS.len()] as char)
        .collect()
}

/// 356 characters starting `FQoGZXIvYXdzEBYaD`, like moto's.
fn random_session_token() -> String {
    const PREFIX: &str = "FQoGZXIvYXdzEBYaD";
    let b64 = base64::encode(&random_bytes(266));
    format!("{PREFIX}{}", &b64[PREFIX.len()..])
}

impl Service for Sts {
    fn get_caller_identity(
        &self,
        ctx: &RequestContext,
        _input: GetCallerIdentityRequest,
    ) -> Result<GetCallerIdentityResponse, AwsError> {
        let partition = partition(&ctx.region);
        if let Some(key) = ctx.access_key.as_deref() {
            let session = self.db.read(|c| {
                Ok(c.query_row(
                    "SELECT account_id, role_arn, session_name, role_id FROM sessions WHERE access_key_id = ?1",
                    params![key],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?)),
                )
                .optional()?)
            })?;
            if let Some((account, role_arn, session, role_id)) = session {
                let role_name = role_arn.rsplit('/').next().unwrap_or(&role_arn);
                return Ok(GetCallerIdentityResponse {
                    user_id: Some(format!("{role_id}:{session}")),
                    account: Some(account.clone()),
                    arn: Some(format!(
                        "arn:{partition}:sts::{account}:assumed-role/{role_name}/{session}"
                    )),
                });
            }
            if let Some(owner) = self.iam.key_owner(key) {
                return Ok(GetCallerIdentityResponse {
                    user_id: Some(owner.user_id),
                    account: Some(owner.account_id.clone()),
                    arn: Some(format!(
                        "arn:{partition}:iam::{}:user{}{}",
                        owner.account_id, owner.path, owner.user_name
                    )),
                });
            }
        }
        // Matches moto's defaults.
        Ok(GetCallerIdentityResponse {
            user_id: Some(FIXTURE_ACCESS_KEY.into()),
            account: Some(ctx.account_id.clone()),
            arn: Some(format!("arn:{partition}:sts::{}:user/moto", ctx.account_id)),
        })
    }

    fn get_access_key_info(
        &self,
        ctx: &RequestContext,
        input: GetAccessKeyInfoRequest,
    ) -> Result<GetAccessKeyInfoResponse, AwsError> {
        let account = self
            .session_account(&input.access_key_id)
            .or_else(|| {
                self.iam
                    .key_owner(&input.access_key_id)
                    .map(|o| o.account_id)
            })
            .unwrap_or_else(|| ctx.account_id.clone());
        Ok(GetAccessKeyInfoResponse {
            account: Some(account),
        })
    }

    fn get_session_token(
        &self,
        _ctx: &RequestContext,
        input: GetSessionTokenRequest,
    ) -> Result<GetSessionTokenResponse, AwsError> {
        Ok(GetSessionTokenResponse {
            credentials: Some(Credentials {
                access_key_id: FIXTURE_ACCESS_KEY.into(),
                secret_access_key: FIXTURE_SECRET.into(),
                session_token: SESSION_TOKEN.into(),
                expiration: Timestamp(now() + i64::from(input.duration_seconds.unwrap_or(43_200))),
            }),
        })
    }

    fn get_federation_token(
        &self,
        ctx: &RequestContext,
        input: GetFederationTokenRequest,
    ) -> Result<GetFederationTokenResponse, AwsError> {
        check_policy(&input.policy)?;
        Ok(GetFederationTokenResponse {
            credentials: Some(Credentials {
                access_key_id: FIXTURE_ACCESS_KEY.into(),
                secret_access_key: FIXTURE_SECRET.into(),
                session_token: FEDERATION_TOKEN.into(),
                expiration: Timestamp(now() + i64::from(input.duration_seconds.unwrap_or(43_200))),
            }),
            federated_user: Some(FederatedUser {
                arn: format!(
                    "arn:{}:sts::{}:federated-user/{}",
                    partition(&ctx.region),
                    ctx.account_id,
                    input.name
                ),
                federated_user_id: format!("{}:{}", ctx.account_id, input.name),
            }),
            packed_policy_size: Some(6),
        })
    }

    fn assume_role(
        &self,
        ctx: &RequestContext,
        input: AssumeRoleRequest,
    ) -> Result<AssumeRoleResponse, AwsError> {
        let (credentials, user) = self.assume(
            ctx,
            &input.role_arn,
            &input.role_session_name,
            input.duration_seconds,
            &input.policy,
        )?;
        Ok(AssumeRoleResponse {
            credentials: Some(credentials),
            assumed_role_user: Some(user),
            packed_policy_size: None,
            source_identity: input.source_identity,
        })
    }

    fn assume_role_with_web_identity(
        &self,
        ctx: &RequestContext,
        input: AssumeRoleWithWebIdentityRequest,
    ) -> Result<AssumeRoleWithWebIdentityResponse, AwsError> {
        let (credentials, user) = self.assume(
            ctx,
            &input.role_arn,
            &input.role_session_name,
            input.duration_seconds,
            &input.policy,
        )?;
        Ok(AssumeRoleWithWebIdentityResponse {
            credentials: Some(credentials),
            assumed_role_user: Some(user),
            packed_policy_size: None,
            provider: input.provider_id,
            audience: None,
            subject_from_web_identity_token: None,
            source_identity: None,
        })
    }

    fn assume_role_with_saml(
        &self,
        ctx: &RequestContext,
        input: AssumeRoleWithSAMLRequest,
    ) -> Result<AssumeRoleWithSAMLResponse, AwsError> {
        // Signatures are not verified; only the attributes we need are read.
        let bad = |m: &str| AwsError::sender(400, "InvalidIdentityToken", m.to_string());
        let xml = base64::decode(&input.saml_assertion)
            .ok_or_else(|| bad("SAML assertion is not valid base64"))?;
        let xml = String::from_utf8(xml).map_err(|_| bad("SAML assertion is not valid UTF-8"))?;
        let doc = roxmltree::Document::parse(xml.trim())
            .map_err(|e| bad(&format!("Invalid SAML assertion: {e}")))?;
        const ATTR: &str = "https://aws.amazon.com/SAML/Attributes/";
        let (mut session, mut duration, mut role) = (None, None, None);
        for attr in doc
            .descendants()
            .filter(|n| n.is_element() && n.tag_name().name() == "Attribute")
        {
            let value = attr
                .children()
                .find(|c| c.is_element() && c.tag_name().name() == "AttributeValue")
                .and_then(|v| v.text())
                .map(|t| t.trim().to_string());
            match attr.attribute("Name").and_then(|n| n.strip_prefix(ATTR)) {
                Some("RoleSessionName") => session = value,
                Some("SessionDuration") => duration = value.and_then(|v| v.parse().ok()),
                // "provider-arn,role-arn" in either order; the role is the `:role/` one.
                Some("Role") => {
                    role = value.and_then(|v| {
                        let parts: Vec<&str> = v.split(',').map(str::trim).collect();
                        parts
                            .iter()
                            .find(|p| p.contains(":role/"))
                            .or(parts.first())
                            .map(|p| p.to_string())
                    })
                }
                _ => {}
            }
        }
        let role = role.unwrap_or(input.role_arn.clone());
        let session = session.unwrap_or_else(|| "session".into());
        let (credentials, user) = self.assume(
            ctx,
            &role,
            &session,
            duration.or(input.duration_seconds),
            &input.policy,
        )?;
        Ok(AssumeRoleWithSAMLResponse {
            credentials: Some(credentials),
            assumed_role_user: Some(user),
            ..Default::default()
        })
    }
}

pub struct StsHandler(pub Arc<Sts>);

impl StsHandler {
    pub fn new(store: &Store, iam: Arc<Iam>) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Sts::new(store, iam)?)))
    }
}

impl ServiceHandler for StsHandler {
    fn service(&self) -> &'static str {
        "sts"
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let mut params = QueryParams::parse(&req.query);
        params.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
        let Some(action) = params.get("Action").map(str::to_string) else {
            return Ok(query_error(
                NAMESPACE,
                &AwsError::missing_parameter("Action"),
                &ctx.request_id,
            ));
        };
        Ok(dispatch(&*self.0, ctx, &action, &params)
            .unwrap_or_else(|e| query_error(NAMESPACE, &e, &ctx.request_id)))
    }

    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        let mut params = QueryParams::parse(&req.query);
        params.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
        params
            .get("Action")
            .is_some_and(|a| OPERATIONS.iter().any(|(op, _)| op == &a))
    }

    fn resolve_account(&self, access_key: &str) -> Option<String> {
        self.0.session_account(access_key)
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "rid".into(),
            base_url: "http://localhost:5000".into(),
        }
    }

    fn handler() -> StsHandler {
        let store = Store::ephemeral();
        let iam = Arc::new(Iam::new(&store).unwrap());
        StsHandler::new(&store, iam).unwrap()
    }

    fn call(body: &str) -> (u16, String) {
        let req = RawRequest {
            method: "POST".into(),
            body: body.as_bytes().to_vec(),
            ..Default::default()
        };
        let r = handler().handle(&ctx(), &req).unwrap();
        (r.status, String::from_utf8(r.body).unwrap())
    }

    #[test]
    fn get_caller_identity() {
        let (status, body) = call("Action=GetCallerIdentity&Version=2011-06-15");
        assert_eq!(status, 200);
        assert!(body.starts_with(
            "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
        ));
        assert!(body.contains("<Account>123456789012</Account>"));
        assert!(body.contains("<Arn>arn:aws:sts::123456789012:user/moto</Arn>"));
        assert!(body.contains("<RequestId>rid</RequestId>"));
        assert!(!body.contains('\n'));
    }

    #[test]
    fn unknown_action_is_invalid_action() {
        let (status, body) = call("Action=Nope");
        assert_eq!(status, 400);
        assert!(body.contains("<Code>InvalidAction</Code>"));
    }

    #[test]
    fn unimplemented_operation_is_501() {
        let (status, body) = call("Action=DecodeAuthorizationMessage&EncodedMessage=x");
        assert_eq!(status, 501);
        assert!(body.contains("<Code>NotImplemented</Code>"));
    }

    #[test]
    fn required_parameter_is_enforced() {
        let (status, body) = call("Action=GetAccessKeyInfo");
        assert_eq!(status, 400);
        assert!(body.contains("The request must contain the parameter AccessKeyId."));
    }
}
