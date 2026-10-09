//! STS (query protocol). Types and dispatch are generated from the botocore model into
//! `generated.rs` (see `scripts/gen.sh`); this file holds only the behaviour.

#[allow(clippy::all)]
mod generated;

use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{QueryParams, query_error};

use generated::*;
pub use generated::{NAMESPACE, OPERATIONS, Service, dispatch};

/// Operations with real behaviour (the rest answer `NotImplemented`). Feeds the coverage matrix.
pub const IMPLEMENTED: &[&str] = &["GetCallerIdentity", "GetAccessKeyInfo"];

#[derive(Default)]
pub struct Sts;

impl Service for Sts {
    fn get_caller_identity(
        &self,
        ctx: &RequestContext,
        _input: GetCallerIdentityRequest,
    ) -> Result<GetCallerIdentityResponse, AwsError> {
        // Matches moto's defaults until IAM-backed identities exist.
        Ok(GetCallerIdentityResponse {
            user_id: Some("AKIAIOSFODNN7KEXAMPLE".into()),
            account: Some(ctx.account_id.clone()),
            arn: Some(format!("arn:aws:sts::{}:user/moto", ctx.account_id)),
        })
    }

    fn get_access_key_info(
        &self,
        ctx: &RequestContext,
        _input: GetAccessKeyInfoRequest,
    ) -> Result<GetAccessKeyInfoResponse, AwsError> {
        Ok(GetAccessKeyInfoResponse {
            account: Some(ctx.account_id.clone()),
        })
    }
}

pub struct StsHandler<S: Service = Sts>(pub S);

impl Default for StsHandler {
    fn default() -> Self {
        Self(Sts)
    }
}

impl<S: Service> ServiceHandler for StsHandler<S> {
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
        Ok(dispatch(&self.0, ctx, &action, &params)
            .unwrap_or_else(|e| query_error(NAMESPACE, &e, &ctx.request_id)))
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
        }
    }

    fn call(body: &str) -> (u16, String) {
        let req = RawRequest {
            method: "POST".into(),
            body: body.as_bytes().to_vec(),
            ..Default::default()
        };
        let r = StsHandler::default().handle(&ctx(), &req).unwrap();
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
        let (status, body) = call("Action=GetSessionToken");
        assert_eq!(status, 501);
        assert!(body.contains("<Code>NotImplemented</Code>"));
    }

    #[test]
    fn required_parameter_is_enforced() {
        let (status, body) = call("Action=GetAccessKeyInfo");
        assert_eq!(status, 400);
        assert!(body.contains("The request must contain the parameter AccessKeyId"));
    }
}
