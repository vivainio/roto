//! Extraction of the SigV4 credential scope. Signatures are **not** verified.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialScope {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub service: String,
}

impl CredentialScope {
    /// Parses `AKID/20240101/us-east-1/sts/aws4_request`.
    pub fn parse(credential: &str) -> Option<Self> {
        let mut parts = credential.split('/');
        let access_key = parts.next()?.to_string();
        let date = parts.next()?.to_string();
        let region = parts.next()?.to_string();
        let service = parts.next()?.to_string();
        (parts.next()? == "aws4_request" && parts.next().is_none() && !access_key.is_empty())
            .then_some(Self {
                access_key,
                date,
                region,
                service,
            })
    }

    /// From an `Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...` header.
    pub fn from_authorization(header: &str) -> Option<Self> {
        let rest = header.strip_prefix("AWS4-HMAC-SHA256")?;
        let credential = rest
            .split(',')
            .map(str::trim)
            .find_map(|p| p.strip_prefix("Credential="))?;
        Self::parse(credential)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_authorization_header() {
        let h = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20240101/eu-west-1/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc";
        let s = CredentialScope::from_authorization(h).unwrap();
        assert_eq!(s.access_key, "AKIDEXAMPLE");
        assert_eq!(s.region, "eu-west-1");
        assert_eq!(s.service, "sts");
    }

    #[test]
    fn rejects_garbage() {
        assert!(CredentialScope::from_authorization("Bearer x").is_none());
        assert!(CredentialScope::parse("a/b/c").is_none());
    }
}
