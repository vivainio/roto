//! S3 (rest-xml). Types, routing and (de)serialisation are generated from the botocore model;
//! buckets and object metadata live in `s3.db`, object bodies as files (see [`blobs`]).

#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::unnecessary_lazy_evaluations,
    clippy::type_complexity
)]

#[allow(clippy::all)]
mod generated;
mod models;
mod schema;

mod acl;
mod blobs;
mod chunked;
mod config;
mod keypath;
mod multipart;
mod notifications;
mod service;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::restxml::rest_xml_error;

pub use generated::{OPERATIONS, Service, dispatch, route};
pub use service::S3;

/// Resolve a modelled S3 REST-XML operation without decoding its payload.
pub fn operation_name(request: &RawRequest) -> Option<&'static str> {
    let mut request = request.clone();
    if let Some(bucket) = request.header("host").and_then(virtual_host_bucket) {
        request.path = format!("/{bucket}{}", request.path);
    }
    let query = roto_protocol::QueryParams::parse(&request.query);
    route(&request, &query).map(|(name, _)| name)
}

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: "
CREATE TABLE buckets (
    name TEXT NOT NULL PRIMARY KEY,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    versioning TEXT NOT NULL DEFAULT ''
);
-- Bucket sub-resources (cors, lifecycle, policy, …) kept as the documents clients sent.
CREATE TABLE bucket_configs (
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (bucket, kind)
);
CREATE TABLE objects (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    is_latest INTEGER NOT NULL,
    delete_marker INTEGER NOT NULL DEFAULT 0,
    size INTEGER NOT NULL DEFAULT 0,
    etag TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT '',
    last_modified INTEGER NOT NULL,
    path TEXT NOT NULL DEFAULT '',
    metadata TEXT NOT NULL DEFAULT '{}',
    headers TEXT NOT NULL DEFAULT '{}',
    tags TEXT NOT NULL DEFAULT '{}',
    storage_class TEXT NOT NULL DEFAULT 'STANDARD',
    UNIQUE (bucket, key, version_id)
);
CREATE INDEX objects_listing ON objects (bucket, is_latest, key);
",
    },
    Migration {
        version: 2,
        sql: "
CREATE TABLE uploads (
    upload_id TEXT NOT NULL PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    attrs TEXT NOT NULL
);
CREATE TABLE parts (
    upload_id TEXT NOT NULL REFERENCES uploads(upload_id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    path TEXT NOT NULL,
    last_modified INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_number)
);
",
    },
    Migration {
        version: 3,
        sql: "ALTER TABLE objects ADD COLUMN acl TEXT;",
    },
    Migration {
        version: 4,
        sql: "CREATE TABLE notification_outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, target TEXT NOT NULL, context TEXT NOT NULL, event TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0, error TEXT);",
    },
];

/// Operations with real behaviour; the rest answer `NotImplemented`.
pub const IMPLEMENTED: &[&str] = &[
    "ListBuckets",
    "CreateBucket",
    "HeadBucket",
    "DeleteBucket",
    "GetBucketLocation",
    "PutObject",
    "GetObject",
    "HeadObject",
    "DeleteObject",
    "DeleteObjects",
    "CopyObject",
    "ListObjects",
    "ListObjectsV2",
    "GetObjectTagging",
    "PutObjectTagging",
    "DeleteObjectTagging",
    "GetBucketVersioning",
    "PutBucketVersioning",
    "CreateMultipartUpload",
    "UploadPart",
    "UploadPartCopy",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "ListParts",
    "ListMultipartUploads",
    "PutBucketCors",
    "GetBucketCors",
    "DeleteBucketCors",
    "PutBucketLifecycleConfiguration",
    "GetBucketLifecycleConfiguration",
    "DeleteBucketLifecycle",
    "PutBucketWebsite",
    "GetBucketWebsite",
    "DeleteBucketWebsite",
    "PutBucketEncryption",
    "GetBucketEncryption",
    "DeleteBucketEncryption",
    "PutBucketReplication",
    "GetBucketReplication",
    "DeleteBucketReplication",
    "PutBucketOwnershipControls",
    "GetBucketOwnershipControls",
    "DeleteBucketOwnershipControls",
    "PutPublicAccessBlock",
    "GetPublicAccessBlock",
    "DeletePublicAccessBlock",
    "PutBucketLogging",
    "GetBucketLogging",
    "PutBucketNotificationConfiguration",
    "GetBucketNotificationConfiguration",
    "PutBucketAccelerateConfiguration",
    "GetBucketAccelerateConfiguration",
    "PutBucketRequestPayment",
    "GetBucketRequestPayment",
    "PutBucketTagging",
    "GetBucketTagging",
    "DeleteBucketTagging",
    "PutBucketPolicy",
    "GetBucketPolicy",
    "DeleteBucketPolicy",
    "PutBucketAcl",
    "GetBucketAcl",
    "PutObjectAcl",
    "GetObjectAcl",
    "ListObjectVersions",
    "GetObjectAttributes",
];

pub struct S3Handler(pub Arc<S3>);

impl S3Handler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(S3::new(store)?)))
    }
    pub fn start_notifications(&self, lambda: Arc<roto_svc_lambda::Lambda>) {
        notifications::start_worker(&self.0, lambda);
    }
    pub fn start_notifications_with_events(
        &self,
        lambda: Arc<roto_svc_lambda::Lambda>,
        events: Arc<roto_svc_eventbridge::EventBridge>,
    ) {
        notifications::start_worker_with_events(&self.0, lambda, Some(events));
    }
    pub fn with_lambda(
        store: &Store,
        lambda: Arc<roto_svc_lambda::Lambda>,
    ) -> Result<Self, AwsError> {
        let handler = Self::new(store)?;
        notifications::start_worker(&handler.0, lambda);
        Ok(handler)
    }
}

/// `bucket.s3.amazonaws.com`, `bucket.s3.localhost:5070`, `bucket.localhost` → `bucket`.
fn virtual_host_bucket(host: &str) -> Option<String> {
    let host = host.split(':').next()?;
    if host.parse::<std::net::IpAddr>().is_ok() || !host.contains('.') {
        return None;
    }
    let labels: Vec<&str> = host.split('.').collect();
    if let Some(i) = labels
        .iter()
        .position(|l| *l == "s3" || l.starts_with("s3-"))
    {
        return (i > 0).then(|| labels[..i].join("."));
    }
    host.strip_suffix(".localhost").map(str::to_string)
}

impl ServiceHandler for S3Handler {
    fn service(&self) -> &'static str {
        "s3"
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        req.path == "/roto-api/s3/notifications"
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        if req.method == "GET" && req.path == "/roto-api/s3/notifications" {
            return notifications::history(&self.0, ctx);
        }
        let mut req = req.clone();
        if let Some(bucket) = req.header("host").and_then(virtual_host_bucket) {
            req.path = format!("/{bucket}{}", req.path);
        }
        if chunked::is_aws_chunked(
            req.header("x-amz-content-sha256"),
            req.header("content-encoding"),
        ) {
            match chunked::decode(&req.body) {
                Some(body) => req.body = body,
                None => {
                    let e = AwsError::sender(
                        400,
                        "IncompleteBody",
                        "The request body terminated unexpectedly",
                    );
                    return Ok(rest_xml_error(&e, &ctx.request_id));
                }
            }
        }
        let mut resp = match dispatch(&*self.0, ctx, &req) {
            Ok(mut r) => {
                // Range reads answer 206; the generated dispatcher only knows the default status.
                if r.status == 200 && r.headers.iter().any(|(k, _)| k == "content-range") {
                    r.status = 206;
                }
                r
            }
            Err(e) if e.status == 304 => RawResponse {
                status: 304,
                headers: Vec::new(),
                body: Vec::new(),
            },
            Err(e) => rest_xml_error(&e, &ctx.request_id),
        };
        resp.headers
            .push(("x-amz-request-id".into(), ctx.request_id.clone()));
        resp.headers.push(("x-amz-id-2".into(), "roto".into()));
        Ok(resp)
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_bucket_in_virtual_host() {
        assert_eq!(
            virtual_host_bucket("photos.s3.amazonaws.com").as_deref(),
            Some("photos")
        );
        assert_eq!(
            virtual_host_bucket("my.bucket.s3.us-west-2.amazonaws.com").as_deref(),
            Some("my.bucket")
        );
        assert_eq!(
            virtual_host_bucket("photos.localhost:5070").as_deref(),
            Some("photos")
        );
        assert_eq!(virtual_host_bucket("localhost:5070"), None);
        assert_eq!(virtual_host_bucket("127.0.0.1:5070"), None);
        assert_eq!(virtual_host_bucket("s3.amazonaws.com"), None);
    }
}
