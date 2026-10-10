use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn ts(secs: i64) -> Timestamp {
    Timestamp(secs)
}

/// `aws`, `aws-cn`, `aws-us-gov`, … from the region the caller signed for.
pub fn partition(region: &str) -> &'static str {
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

pub fn arn(ctx: &RequestContext, resource: &str) -> String {
    format!(
        "arn:{}:iam::{}:{resource}",
        partition(&ctx.region),
        ctx.account_id
    )
}

/// Random identifier such as `AIDA…` (IAM ids are a 4 letter prefix + uppercase alphanumerics).
pub fn gen_id(prefix: &str, len: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let bytes: Vec<u8> = (0..len.div_ceil(16))
        .flat_map(|_| *uuid::Uuid::new_v4().as_bytes())
        .collect();
    let tail: String = bytes
        .iter()
        .take(len)
        .map(|b| CHARS[(*b as usize) % CHARS.len()] as char)
        .collect();
    format!("{prefix}{tail}")
}

pub fn gen_secret() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes: Vec<u8> = (0..3)
        .flat_map(|_| *uuid::Uuid::new_v4().as_bytes())
        .collect();
    bytes
        .iter()
        .take(40)
        .map(|b| CHARS[(*b as usize) % CHARS.len()] as char)
        .collect()
}

pub fn no_such(kind: &str, name: &str) -> AwsError {
    AwsError::sender(
        404,
        "NoSuchEntity",
        format!("The {kind} with name {name} cannot be found."),
    )
}

pub fn exists(kind: &str, name: &str) -> AwsError {
    AwsError::sender(
        409,
        "EntityAlreadyExists",
        format!("{kind} with name {name} already exists."),
    )
}

pub fn conflict(message: impl Into<String>) -> AwsError {
    AwsError::sender(409, "DeleteConflict", message)
}

pub fn validation(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationError", message)
}

/// `/` or `/a/b/`; anything else is a validation error.
pub fn normalize_path(path: Option<&str>) -> Result<String, AwsError> {
    let p = path.unwrap_or("/");
    if p.is_empty() {
        return Ok("/".into());
    }
    if !p.starts_with('/') || !p.ends_with('/') {
        return Err(validation(
            "The specified value for path is invalid. It must begin and end with / and contain only alphanumeric characters and/or / characters.",
        ));
    }
    Ok(p.to_string())
}

/// Offset-based pagination: the marker is the index of the next item.
pub fn paginate<T>(
    items: Vec<T>,
    marker: Option<&str>,
    max_items: Option<i32>,
) -> Result<(Vec<T>, bool, Option<String>), AwsError> {
    let start = match marker {
        None | Some("") => 0,
        Some(m) => m
            .parse::<usize>()
            .map_err(|_| validation("Invalid Marker."))?,
    };
    let max = max_items.unwrap_or(100).clamp(1, 1000) as usize;
    let total = items.len();
    let page: Vec<T> = items.into_iter().skip(start).take(max).collect();
    let end = start + page.len();
    if end < total {
        Ok((page, true, Some(end.to_string())))
    } else {
        Ok((page, false, None))
    }
}

/// Default path prefix for literal SQLite substring filters.
pub fn literal_prefix(prefix: Option<&str>) -> &str {
    prefix.unwrap_or("/")
}
