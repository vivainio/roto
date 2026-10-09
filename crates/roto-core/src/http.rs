use crate::error::AwsError;

/// A protocol-agnostic view of an incoming HTTP request.
#[derive(Debug, Clone, Default)]
pub struct RawRequest {
    pub method: String,
    pub path: String,
    /// Raw (still percent-encoded) query string, without the leading `?`.
    pub query: String,
    /// Header names are lower-cased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Who is calling, where, and which request this is.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub account_id: String,
    pub region: String,
    pub access_key: Option<String>,
    pub request_id: String,
    /// Scheme and authority the caller used, e.g. `http://localhost:5000`; used to build URLs.
    pub base_url: String,
}

/// One AWS service endpoint. Implementations decode the wire protocol, run the operation and
/// encode the response. They are synchronous because storage is blocking SQLite; the server
/// runs them on the blocking pool.
pub trait ServiceHandler: Send + Sync {
    /// SigV4 service name (credential scope), e.g. `sts`.
    fn service(&self) -> &'static str;
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError>;

    /// Drops all state (`POST /roto-api/reset`). Stateless services keep the default.
    fn reset(&self) -> Result<(), AwsError> {
        Ok(())
    }
}
