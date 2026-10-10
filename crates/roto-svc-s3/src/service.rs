use std::collections::BTreeMap;
use std::sync::Arc;

use md5::{Digest, Md5};
use roto_core::rusqlite::{OptionalExtension, Row, Transaction, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{Blob, Timestamp};

use crate::blobs::{Blobs, Placement};
use crate::config;
use crate::generated::*;

pub const OWNER_ID: &str = "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a";
pub const OWNER_NAME: &str = "webfile";
const MAX_KEYS: i32 = 1000;

pub struct S3 {
    pub(crate) db: Arc<Db>,
    pub(crate) blobs: Blobs,
}

impl S3 {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        let db = store.db("s3", crate::MIGRATIONS)?;
        let root = store.blob_dir("s3")?;
        let blobs = Blobs::new(root).map_err(io)?;
        Ok(Self { db, blobs })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        let buckets: Vec<String> = self.db.read(|c| {
            let mut stmt = c.prepare("SELECT name FROM buckets")?;
            Ok(stmt
                .query_map([], |r| r.get(0))?
                .collect::<Result<_, _>>()?)
        })?;
        for b in buckets {
            self.blobs.remove_bucket(&b).map_err(io)?;
        }
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM buckets", [])?;
            tx.execute("DELETE FROM notification_outbox", [])?;
            Ok(())
        })
    }
}

pub(crate) fn io(e: std::io::Error) -> AwsError {
    AwsError::internal(format!("storage error: {e}"))
}

pub(crate) fn now_secs() -> i64 {
    now()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

fn new_version_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

// ---- errors ------------------------------------------------------------------------------

pub(crate) fn no_such_bucket(name: &str) -> AwsError {
    AwsError::sender(404, "NoSuchBucket", "The specified bucket does not exist")
        .with("BucketName", name)
}

pub(crate) fn no_such_key(key: &str) -> AwsError {
    AwsError::sender(404, "NoSuchKey", "The specified key does not exist.").with("Key", key)
}

pub(crate) fn invalid_argument(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "InvalidArgument", message)
}

// ---- rows --------------------------------------------------------------------------------

pub(crate) struct BucketRow {
    pub name: String,
    pub account_id: String,
    pub region: String,
    /// `""` (never configured), `Enabled` or `Suspended`.
    pub versioning: String,
}

impl BucketRow {
    pub fn enabled(&self) -> bool {
        self.versioning == "Enabled"
    }
    pub fn versioned(&self) -> bool {
        !self.versioning.is_empty()
    }
}

pub(crate) fn load_bucket(tx: &Transaction, name: &str) -> Result<BucketRow, AwsError> {
    tx.query_row(
        "SELECT name, account_id, region, versioning FROM buckets WHERE name = ?1",
        params![name],
        |r| {
            Ok(BucketRow {
                name: r.get(0)?,
                account_id: r.get(1)?,
                region: r.get(2)?,
                versioning: r.get(3)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| no_such_bucket(name))
}

#[derive(Clone)]
pub(crate) struct Obj {
    pub seq: i64,
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub delete_marker: bool,
    pub size: i64,
    pub etag: String,
    pub content_type: String,
    pub last_modified: i64,
    pub path: String,
    pub metadata: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub storage_class: String,
    pub acl: Option<String>,
}

const OBJ_COLS: &str = "seq, key, version_id, is_latest, delete_marker, size, etag, content_type, last_modified, path, metadata, headers, tags, storage_class, acl";

fn json_map(s: String) -> BTreeMap<String, String> {
    serde_json::from_str(&s).unwrap_or_default()
}

fn obj_from_row(r: &Row) -> roto_core::rusqlite::Result<Obj> {
    Ok(Obj {
        seq: r.get(0)?,
        key: r.get(1)?,
        version_id: r.get(2)?,
        is_latest: r.get::<_, i64>(3)? != 0,
        delete_marker: r.get::<_, i64>(4)? != 0,
        size: r.get(5)?,
        etag: r.get(6)?,
        content_type: r.get(7)?,
        last_modified: r.get(8)?,
        path: r.get(9)?,
        metadata: json_map(r.get(10)?),
        headers: json_map(r.get(11)?),
        tags: json_map(r.get(12)?),
        storage_class: r.get(13)?,
        acl: r.get(14)?,
    })
}

pub(crate) fn latest_obj(
    tx: &Transaction,
    bucket: &str,
    key: &str,
) -> Result<Option<Obj>, AwsError> {
    Ok(tx
        .query_row(
            &format!(
                "SELECT {OBJ_COLS} FROM objects WHERE bucket = ?1 AND key = ?2 AND is_latest = 1"
            ),
            params![bucket, key],
            obj_from_row,
        )
        .optional()?)
}

pub(crate) fn find_obj(
    tx: &Transaction,
    bucket: &str,
    key: &str,
    version: Option<&str>,
) -> Result<Option<Obj>, AwsError> {
    match version {
        None => latest_obj(tx, bucket, key),
        Some(v) => Ok(tx
            .query_row(
                &format!("SELECT {OBJ_COLS} FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3"),
                params![bucket, key, v],
                obj_from_row,
            )
            .optional()?),
    }
}

fn json_string(m: &BTreeMap<String, String>) -> String {
    serde_json::to_string(m).unwrap_or_else(|_| "{}".into())
}

/// Everything a new object version carries besides its key and body.
#[derive(Default, Clone)]
pub(crate) struct NewObj {
    pub content_type: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub storage_class: Option<String>,
    /// `AccessControlPolicy` XML, if one was given with the request.
    pub acl: Option<String>,
}

impl S3 {
    fn fix_moved(&self, tx: &Transaction, bucket: &str, p: &Placement) -> Result<(), AwsError> {
        for (old, new) in &p.moved {
            tx.execute(
                "UPDATE objects SET path = ?1 WHERE bucket = ?2 AND path = ?3",
                params![new, bucket, old],
            )?;
        }
        Ok(())
    }

    /// Makes the current latest version non-current, moving its file out of the live tree.
    fn retire_latest(&self, tx: &Transaction, bucket: &str, cur: &Obj) -> Result<(), AwsError> {
        let mut path = cur.path.clone();
        if !path.is_empty() {
            path = Blobs::version_rel(bucket, &cur.key, &cur.version_id);
            self.blobs.move_rel(&cur.path, &path).map_err(io)?;
        }
        tx.execute(
            "UPDATE objects SET is_latest = 0, path = ?1 WHERE seq = ?2",
            params![path, cur.seq],
        )?;
        Ok(())
    }

    fn drop_row(&self, tx: &Transaction, o: &Obj) -> Result<(), AwsError> {
        tx.execute("DELETE FROM objects WHERE seq = ?1", params![o.seq])?;
        if !o.path.is_empty() {
            self.blobs.remove(&o.path).map_err(io)?;
        }
        Ok(())
    }

    /// After removing the latest version, the newest remaining one becomes current again.
    fn promote_newest(&self, tx: &Transaction, bucket: &str, key: &str) -> Result<(), AwsError> {
        let next = tx
            .query_row(
                &format!(
                    "SELECT {OBJ_COLS} FROM objects WHERE bucket = ?1 AND key = ?2 ORDER BY last_modified DESC, seq DESC LIMIT 1"
                ),
                params![bucket, key],
                obj_from_row,
            )
            .optional()?;
        let Some(o) = next else { return Ok(()) };
        let mut path = o.path.clone();
        if !path.is_empty() {
            let p = self.blobs.relocate_live(&o.path, bucket, key).map_err(io)?;
            self.fix_moved(tx, bucket, &p)?;
            path = p.rel;
        }
        tx.execute(
            "UPDATE objects SET is_latest = 1, path = ?1 WHERE seq = ?2",
            params![path, o.seq],
        )?;
        Ok(())
    }

    /// Stores a new version of `key`; returns the stored row.
    pub(crate) fn store_object(
        &self,
        tx: &Transaction,
        b: &BucketRow,
        key: &str,
        data: &[u8],
        etag: String,
        attrs: NewObj,
    ) -> Result<Obj, AwsError> {
        let version_id = if b.enabled() {
            new_version_id()
        } else {
            "null".to_string()
        };
        if let Some(same) = find_obj(tx, &b.name, key, Some(&version_id))? {
            self.drop_row(tx, &same)?;
        }
        if let Some(cur) = latest_obj(tx, &b.name, key)? {
            self.retire_latest(tx, &b.name, &cur)?;
        }
        let placement = self.blobs.write_live(&b.name, key, data).map_err(io)?;
        self.fix_moved(tx, &b.name, &placement)?;
        let content_type = attrs
            .content_type
            .clone()
            .unwrap_or_else(|| "binary/octet-stream".into());
        let storage_class = attrs
            .storage_class
            .clone()
            .unwrap_or_else(|| "STANDARD".into());
        let last_modified = now();
        tx.execute(
            "INSERT INTO objects (bucket, key, version_id, is_latest, delete_marker, size, etag, content_type,
                                  last_modified, path, metadata, headers, tags, storage_class, acl)
             VALUES (?1, ?2, ?3, 1, 0, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                b.name,
                key,
                version_id,
                data.len() as i64,
                etag,
                content_type,
                last_modified,
                placement.rel,
                json_string(&attrs.metadata),
                json_string(&attrs.headers),
                json_string(&attrs.tags),
                storage_class,
                attrs.acl
            ],
        )?;
        Ok(find_obj(tx, &b.name, key, Some(&version_id))?.expect("just inserted"))
    }

    pub(crate) fn read_body(&self, o: &Obj) -> Result<Vec<u8>, AwsError> {
        if o.path.is_empty() {
            return Ok(Vec::new());
        }
        self.blobs.read(&o.path).map_err(io)
    }

    fn put_delete_marker(
        &self,
        tx: &Transaction,
        b: &BucketRow,
        key: &str,
    ) -> Result<String, AwsError> {
        let version_id = if b.enabled() {
            new_version_id()
        } else {
            "null".to_string()
        };
        if let Some(same) = find_obj(tx, &b.name, key, Some(&version_id))? {
            self.drop_row(tx, &same)?;
        }
        if let Some(cur) = latest_obj(tx, &b.name, key)? {
            self.retire_latest(tx, &b.name, &cur)?;
        }
        tx.execute(
            "INSERT INTO objects (bucket, key, version_id, is_latest, delete_marker, size, etag, content_type,
                                  last_modified, path, storage_class)
             VALUES (?1, ?2, ?3, 1, 1, 0, '', '', ?4, '', 'STANDARD')",
            params![b.name, key, version_id, now()],
        )?;
        Ok(version_id)
    }

    /// Returns `(delete_marker, version_id)` for the response.
    pub(crate) fn delete_one(
        &self,
        tx: &Transaction,
        b: &BucketRow,
        key: &str,
        version: Option<&str>,
    ) -> Result<(bool, Option<String>), AwsError> {
        match version {
            Some(v) => {
                let Some(o) = find_obj(tx, &b.name, key, Some(v))? else {
                    return Ok((false, Some(v.to_string())));
                };
                self.drop_row(tx, &o)?;
                if o.is_latest {
                    self.promote_newest(tx, &b.name, key)?;
                }
                Ok((o.delete_marker, Some(v.to_string())))
            }
            None if b.versioned() => {
                let vid = self.put_delete_marker(tx, b, key)?;
                Ok((true, Some(vid)))
            }
            None => {
                if let Some(o) = latest_obj(tx, &b.name, key)? {
                    self.drop_row(tx, &o)?;
                }
                Ok((false, None))
            }
        }
    }
}

/// `bucket/key`, `/bucket/key`, URL-encoded, optionally `?versionId=…`.
pub(crate) fn parse_copy_source(raw: &str) -> Result<(String, String, Option<String>), AwsError> {
    let src = roto_protocol::restxml::percent_decode_path(raw.trim_start_matches('/'));
    let (path, version) = match src.split_once("?versionId=") {
        Some((p, v)) => (p.to_string(), Some(v.to_string())),
        None => (src.clone(), None),
    };
    match path.split_once('/') {
        Some((b, k)) if !b.is_empty() && !k.is_empty() => {
            Ok((b.to_string(), k.to_string(), version))
        }
        _ => Err(invalid_argument(
            "Copy Source must mention the source bucket and key: sourcebucket/sourcekey",
        )),
    }
}

// ---- request helpers ---------------------------------------------------------------------

fn etag_quoted(e: &str) -> String {
    format!("\"{e}\"")
}

fn strip_quotes(e: &str) -> &str {
    e.trim().trim_matches('"')
}

/// `W/"x"`, `"x"`, `x` and lists of them, compared against a bare etag.
fn etag_matches(header: &str, etag: &str) -> bool {
    header
        .split(',')
        .map(|p| strip_quotes(p.trim().trim_start_matches("W/")))
        .any(|p| p == "*" || p == etag)
}

/// `key=value&…` as sent in `x-amz-tagging`.
fn parse_tagging_header(s: &str) -> Result<BTreeMap<String, String>, AwsError> {
    use roto_protocol::restxml::percent_decode_form;
    let mut out = BTreeMap::new();
    for pair in s.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode_form(k), percent_decode_form(v));
    }
    if out.len() > 10 {
        return Err(invalid_argument("Object tags cannot be greater than 10"));
    }
    Ok(out)
}

pub(crate) fn attrs_from_headers(
    content_type: &Option<String>,
    metadata: &BTreeMap<String, String>,
    cache_control: &Option<String>,
    content_disposition: &Option<String>,
    content_encoding: &Option<String>,
    content_language: &Option<String>,
    expires: &Option<String>,
    website_redirect: &Option<String>,
    sse: &Option<String>,
    storage_class: &Option<String>,
) -> NewObj {
    let mut headers = BTreeMap::new();
    for (k, v) in [
        ("cache_control", cache_control),
        ("content_disposition", content_disposition),
        ("content_encoding", content_encoding),
        ("content_language", content_language),
        ("expires", expires),
        ("website_redirect_location", website_redirect),
        ("server_side_encryption", sse),
    ] {
        if let Some(v) = v {
            headers.insert(k.to_string(), v.clone());
        }
    }
    NewObj {
        content_type: content_type.clone(),
        metadata: metadata.clone(),
        headers,
        tags: BTreeMap::new(),
        storage_class: storage_class.clone(),
        acl: None,
    }
}

/// Like moto, only the length is checked strictly; names must also be usable as a directory
/// (no path separators or control characters, and not hidden, since `.roto` is internal).
fn valid_bucket_name(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && !name.starts_with('.')
        && !name
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control())
}

/// RFC 3986 percent-encoding that keeps `/` (S3's `encoding-type=url`).
pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub(crate) fn owner() -> Owner {
    Owner {
        display_name: Some(OWNER_NAME.into()),
        id: Some(OWNER_ID.into()),
    }
}

/// Parses `bytes=a-b`, `bytes=a-`, `bytes=-n` against an object of `size` bytes.
/// `Ok(None)` means "ignore the header" (malformed or multi-range).
fn parse_range(h: &str, size: i64) -> Result<Option<(i64, i64)>, AwsError> {
    let Some(spec) = h.trim().strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        return Ok(None);
    }
    let Some((a, b)) = spec.split_once('-') else {
        return Ok(None);
    };
    let unsatisfiable = || {
        AwsError::sender(
            416,
            "InvalidRange",
            "The requested range is not satisfiable",
        )
        .with("ActualObjectSize", size.to_string())
        .with("RangeRequested", h.to_string())
    };
    let (start, end) = match (a.trim(), b.trim()) {
        ("", "") => return Ok(None),
        ("", n) => {
            let n: i64 = n.parse().map_err(|_| unsatisfiable())?;
            if n == 0 {
                return Err(unsatisfiable());
            }
            ((size - n).max(0), size - 1)
        }
        (s, "") => (s.parse().map_err(|_| unsatisfiable())?, size - 1),
        (s, e) => (
            s.parse().map_err(|_| unsatisfiable())?,
            e.parse::<i64>().map_err(|_| unsatisfiable())?.min(size - 1),
        ),
    };
    if start >= size || start > end {
        return Err(unsatisfiable());
    }
    Ok(Some((start, end)))
}

fn precondition_failed(cond: &str) -> AwsError {
    AwsError::sender(
        412,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
    )
    .with("Condition", cond)
}

fn not_modified() -> AwsError {
    AwsError::sender(304, "NotModified", "Not Modified")
}

/// If-Match / If-None-Match / If-(Un)Modified-Since, shared by GET, HEAD and copy sources.
fn check_conditions(
    o: &Obj,
    if_match: &Option<String>,
    if_none_match: &Option<String>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
) -> Result<(), AwsError> {
    if let Some(m) = if_match {
        if !etag_matches(m, &o.etag) {
            return Err(precondition_failed("If-Match"));
        }
    }
    if let Some(m) = if_none_match {
        if etag_matches(m, &o.etag) {
            return Err(not_modified());
        }
    }
    if let Some(t) = if_unmodified_since {
        if o.last_modified > t.0 {
            return Err(precondition_failed("If-Unmodified-Since"));
        }
    }
    if let Some(t) = if_modified_since {
        if o.last_modified <= t.0 {
            return Err(not_modified());
        }
    }
    Ok(())
}

macro_rules! object_output {
    ($T:ident, $o:expr, $b:expr) => {{
        let o: &Obj = $o;
        let mut out = $T::default();
        out.e_tag = Some(etag_quoted(&o.etag));
        out.last_modified = Some(Timestamp(o.last_modified));
        out.content_type = Some(o.content_type.clone());
        out.metadata = o.metadata.clone();
        out.accept_ranges = Some("bytes".into());
        out.version_id = (o.version_id != "null").then(|| o.version_id.clone());
        out.storage_class = (o.storage_class != "STANDARD").then(|| o.storage_class.clone());
        out.cache_control = o.headers.get("cache_control").cloned();
        out.content_disposition = o.headers.get("content_disposition").cloned();
        out.content_encoding = o.headers.get("content_encoding").cloned();
        out.content_language = o.headers.get("content_language").cloned();
        out.expires = o.headers.get("expires").cloned();
        out.website_redirect_location = o.headers.get("website_redirect_location").cloned();
        out.server_side_encryption = o.headers.get("server_side_encryption").cloned();
        out.ssekms_key_id = o.headers.get("ssekms_key_id").cloned();
        out.tag_count = (!o.tags.is_empty()).then_some(o.tags.len() as i32);
        out
    }};
}

impl S3 {
    fn read_response(
        &self,
        ctx: &RequestContext,
        bucket: &str,
        key: &str,
        version_id: &Option<String>,
        range: &Option<String>,
        conditions: (
            &Option<String>,
            &Option<String>,
            Option<Timestamp>,
            Option<Timestamp>,
        ),
        with_body: bool,
    ) -> Result<(Obj, Option<(i64, i64)>, Vec<u8>), AwsError> {
        let _ = ctx;
        self.db.transaction(|tx| {
            let b = load_bucket(tx, bucket)?;
            let o = find_obj(tx, bucket, key, version_id.as_deref())?;
            let Some(o) = o else {
                return Err(match version_id {
                    Some(v) if v != "null" => AwsError::sender(
                        404,
                        "NoSuchVersion",
                        "The specified version does not exist.",
                    )
                    .with("Key", key)
                    .with("VersionId", v.clone()),
                    _ => no_such_key(key),
                });
            };
            if o.delete_marker {
                return Err(if version_id.is_some() {
                    AwsError::sender(
                        405,
                        "MethodNotAllowed",
                        "The specified method is not allowed against this resource.",
                    )
                    .with("Method", "GET")
                    .with("ResourceType", "DeleteMarker")
                } else {
                    no_such_key(key).with("DeleteMarker", "true")
                });
            }
            let _ = b;
            check_conditions(&o, conditions.0, conditions.1, conditions.2, conditions.3)?;
            let range = match range {
                Some(r) => parse_range(r, o.size)?,
                None => None,
            };
            let body = if !with_body || o.path.is_empty() {
                Vec::new()
            } else if let Some((s, e)) = range {
                self.blobs
                    .read_range(&o.path, s as u64, (e - s + 1) as u64)
                    .map_err(io)?
            } else {
                self.blobs.read(&o.path).map_err(io)?
            };
            Ok((o, range, body))
        })
    }
}

impl Service for S3 {
    // ---- buckets ----
    fn list_buckets(
        &self,
        ctx: &RequestContext,
        _i: ListBucketsRequest,
    ) -> Result<ListBucketsOutput, AwsError> {
        self.db.transaction(|tx| {
            let mut stmt = tx.prepare(
                "SELECT name, created_at, region FROM buckets WHERE account_id = ?1 ORDER BY name",
            )?;
            let buckets = stmt
                .query_map(params![ctx.account_id], |r| {
                    Ok(Bucket {
                        name: Some(r.get(0)?),
                        creation_date: Some(Timestamp(r.get(1)?)),
                        bucket_region: Some(r.get(2)?),
                        bucket_arn: None,
                    })
                })?
                .collect::<Result<_, _>>()?;
            Ok(ListBucketsOutput {
                buckets,
                owner: Some(owner()),
                ..Default::default()
            })
        })
    }

    fn create_bucket(
        &self,
        ctx: &RequestContext,
        i: CreateBucketRequest,
    ) -> Result<CreateBucketOutput, AwsError> {
        if !valid_bucket_name(&i.bucket) {
            return Err(AwsError::sender(
                400,
                "InvalidBucketName",
                "The specified bucket is not valid.",
            )
            .with("BucketName", i.bucket.clone()));
        }
        let constraint = i
            .create_bucket_configuration
            .as_ref()
            .and_then(|c| c.location_constraint.clone());
        let region = match constraint.as_deref() {
            None | Some("") => "us-east-1".to_string(),
            Some("us-east-1") => {
                return Err(AwsError::sender(
                    400,
                    "InvalidLocationConstraint",
                    "The specified location-constraint is not valid",
                )
                .with("LocationConstraint", "us-east-1"));
            }
            Some(r) => r.to_string(),
        };
        if constraint.is_none() && ctx.region != "us-east-1" {
            return Err(AwsError::sender(
                400,
                "IllegalLocationConstraintException",
                "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to.",
            ));
        }
        self.db.transaction(|tx| {
            if let Ok(existing) = load_bucket(tx, &i.bucket) {
                if existing.account_id == ctx.account_id && existing.region == "us-east-1" && region == "us-east-1" {
                    return Ok(CreateBucketOutput { location: Some(format!("/{}", i.bucket)), ..Default::default() });
                }
                return Err(if existing.account_id == ctx.account_id {
                    AwsError::sender(
                        409,
                        "BucketAlreadyOwnedByYou",
                        "Your previous request to create the named bucket succeeded and you already own it.",
                    )
                    .with("BucketName", i.bucket.clone())
                } else {
                    AwsError::sender(
                        409,
                        "BucketAlreadyExists",
                        "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
                    )
                    .with("BucketName", i.bucket.clone())
                });
            }
            let lock = i.object_lock_enabled_for_bucket.unwrap_or(false);
            let versioning = if lock { "Enabled" } else { "" };
            tx.execute(
                "INSERT INTO buckets (name, account_id, region, created_at, versioning) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![i.bucket, ctx.account_id, region, now(), versioning],
            )?;
            self.blobs.create_bucket(&i.bucket).map_err(io)?;
            let acl = crate::acl::build(
                &i.acl,
                None,
                [
                    ("FULL_CONTROL", &i.grant_full_control),
                    ("READ", &i.grant_read),
                    ("READ_ACP", &i.grant_read_acp),
                    ("WRITE", &i.grant_write),
                    ("WRITE_ACP", &i.grant_write_acp),
                ],
            )?;
            if let Some(p) = acl {
                tx.execute(
                    "INSERT INTO bucket_configs (bucket, kind, body) VALUES (?1, 'acl', ?2)",
                    params![i.bucket, crate::acl::to_xml(&p)],
                )?;
            }
            Ok(CreateBucketOutput { location: Some(format!("/{}", i.bucket)), ..Default::default() })
        })
    }

    fn head_bucket(
        &self,
        _ctx: &RequestContext,
        i: HeadBucketRequest,
    ) -> Result<HeadBucketOutput, AwsError> {
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            Ok(HeadBucketOutput {
                bucket_region: Some(b.region),
                ..Default::default()
            })
        })
    }

    fn delete_bucket(&self, _ctx: &RequestContext, i: DeleteBucketRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let n: i64 = tx.query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket = ?1",
                params![i.bucket],
                |r| r.get(0),
            )?;
            if n > 0 {
                return Err(AwsError::sender(
                    409,
                    "BucketNotEmpty",
                    "The bucket you tried to delete is not empty",
                )
                .with("BucketName", i.bucket.clone()));
            }
            tx.execute("DELETE FROM buckets WHERE name = ?1", params![i.bucket])?;
            self.blobs.remove_bucket(&i.bucket).map_err(io)?;
            Ok(())
        })
    }

    fn get_bucket_location(
        &self,
        _ctx: &RequestContext,
        i: GetBucketLocationRequest,
    ) -> Result<GetBucketLocationOutput, AwsError> {
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            let location_constraint = (b.region != "us-east-1").then_some(b.region);
            Ok(GetBucketLocationOutput {
                location_constraint,
            })
        })
    }

    // ---- objects ----
    fn put_object(
        &self,
        ctx: &RequestContext,
        i: PutObjectRequest,
    ) -> Result<PutObjectOutput, AwsError> {
        let data = i.body.map(|b| b.0).unwrap_or_default();
        let etag = md5_hex(&data);
        if let Some(md5) = &i.content_md5 {
            let want = roto_protocol::base64::decode(md5.trim()).map(hex::encode);
            if want.as_deref() != Some(etag.as_str()) {
                return Err(AwsError::sender(
                    400,
                    "BadDigest",
                    "The Content-MD5 you specified did not match what we received.",
                )
                .with("ExpectedDigest", md5.clone())
                .with(
                    "CalculatedDigest",
                    roto_protocol::base64::encode(&hex::decode(&etag).unwrap_or_default()),
                ));
            }
        }
        let mut attrs = attrs_from_headers(
            &i.content_type,
            &i.metadata,
            &i.cache_control,
            &i.content_disposition,
            &i.content_encoding,
            &i.content_language,
            &i.expires,
            &i.website_redirect_location,
            &i.server_side_encryption,
            &i.storage_class,
        );
        if let Some(key_id) = &i.ssekms_key_id {
            attrs.headers.insert("ssekms_key_id".into(), key_id.clone());
        }
        if let Some(t) = &i.tagging {
            attrs.tags = parse_tagging_header(t)?;
        }
        attrs.acl = crate::acl::build(
            &i.acl,
            None,
            [
                ("FULL_CONTROL", &i.grant_full_control),
                ("READ", &i.grant_read),
                ("READ_ACP", &i.grant_read_acp),
                ("WRITE_ACP", &i.grant_write_acp),
                ("", &None),
            ],
        )?
        .map(|p| crate::acl::to_xml(&p));
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            let current = latest_obj(tx, &i.bucket, &i.key)?.filter(|o| !o.delete_marker);
            if let Some(m) = &i.if_none_match {
                if m.trim() == "*" && current.is_some() {
                    return Err(precondition_failed("If-None-Match"));
                }
            }
            if let Some(m) = &i.if_match {
                match &current {
                    Some(c) if etag_matches(m, &c.etag) => {}
                    Some(_) => return Err(precondition_failed("If-Match")),
                    None => return Err(no_such_key(&i.key)),
                }
            }
            let o = self.store_object(tx, &b, &i.key, &data, etag.clone(), attrs.clone())?;
            crate::notifications::record(tx, ctx, &b, &i.key, "ObjectCreated:Put", Some(&o))?;
            Ok(PutObjectOutput {
                e_tag: Some(etag_quoted(&etag)),
                version_id: (o.version_id != "null").then(|| o.version_id),
                server_side_encryption: attrs.headers.get("server_side_encryption").cloned(),
                ssekms_key_id: attrs.headers.get("ssekms_key_id").cloned(),
                size: Some(data.len() as i64),
                ..Default::default()
            })
        })
    }

    fn get_object(
        &self,
        ctx: &RequestContext,
        i: GetObjectRequest,
    ) -> Result<GetObjectOutput, AwsError> {
        let (o, range, body) = self.read_response(
            ctx,
            &i.bucket,
            &i.key,
            &i.version_id,
            &i.range,
            (
                &i.if_match,
                &i.if_none_match,
                i.if_modified_since,
                i.if_unmodified_since,
            ),
            true,
        )?;
        let mut out = object_output!(GetObjectOutput, &o, ());
        if let Some((s, e)) = range {
            out.content_range = Some(format!("bytes {s}-{e}/{}", o.size));
        }
        out.content_length = Some(body.len() as i64);
        out.body = Some(Blob(body));
        if let Some(v) = &i.response_content_type {
            out.content_type = Some(v.clone());
        }
        if let Some(v) = &i.response_cache_control {
            out.cache_control = Some(v.clone());
        }
        if let Some(v) = &i.response_content_disposition {
            out.content_disposition = Some(v.clone());
        }
        if let Some(v) = &i.response_content_encoding {
            out.content_encoding = Some(v.clone());
        }
        if let Some(v) = &i.response_content_language {
            out.content_language = Some(v.clone());
        }
        Ok(out)
    }

    fn head_object(
        &self,
        ctx: &RequestContext,
        i: HeadObjectRequest,
    ) -> Result<HeadObjectOutput, AwsError> {
        let (o, range, body) = self.read_response(
            ctx,
            &i.bucket,
            &i.key,
            &i.version_id,
            &i.range,
            (
                &i.if_match,
                &i.if_none_match,
                i.if_modified_since,
                i.if_unmodified_since,
            ),
            false,
        )?;
        let mut out = object_output!(HeadObjectOutput, &o, ());
        out.content_length = Some(match range {
            Some((s, e)) => e - s + 1,
            None => o.size,
        });
        if let Some((s, e)) = range {
            out.content_range = Some(format!("bytes {s}-{e}/{}", o.size));
        }
        let _ = body;
        Ok(out)
    }

    fn delete_object(
        &self,
        ctx: &RequestContext,
        i: DeleteObjectRequest,
    ) -> Result<DeleteObjectOutput, AwsError> {
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            let before = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?;
            let (marker, version_id) = self.delete_one(tx, &b, &i.key, i.version_id.as_deref())?;
            let removed = if marker {
                latest_obj(tx, &i.bucket, &i.key)?
            } else {
                before
            };
            if removed.is_some() {
                crate::notifications::record(
                    tx,
                    ctx,
                    &b,
                    &i.key,
                    if marker {
                        "ObjectRemoved:DeleteMarkerCreated"
                    } else {
                        "ObjectRemoved:Delete"
                    },
                    removed.as_ref(),
                )?;
            }
            Ok(DeleteObjectOutput {
                delete_marker: marker.then_some(true),
                version_id,
                ..Default::default()
            })
        })
    }

    fn delete_objects(
        &self,
        ctx: &RequestContext,
        i: DeleteObjectsRequest,
    ) -> Result<DeleteObjectsOutput, AwsError> {
        if i.delete.objects.is_empty() {
            return Err(AwsError::sender(
                400,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
            ));
        }
        if i.delete.objects.len() > 1000 {
            return Err(AwsError::sender(
                400,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
            ));
        }
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            let quiet = i.delete.quiet.unwrap_or(false);
            let mut out = DeleteObjectsOutput::default();
            for o in &i.delete.objects {
                let before = find_obj(tx, &i.bucket, &o.key, o.version_id.as_deref())?;
                let (marker, version_id) =
                    self.delete_one(tx, &b, &o.key, o.version_id.as_deref())?;
                let removed = if marker {
                    latest_obj(tx, &i.bucket, &o.key)?
                } else {
                    before
                };
                if removed.is_some() {
                    crate::notifications::record(
                        tx,
                        ctx,
                        &b,
                        &o.key,
                        if marker {
                            "ObjectRemoved:DeleteMarkerCreated"
                        } else {
                            "ObjectRemoved:Delete"
                        },
                        removed.as_ref(),
                    )?;
                }
                if !quiet {
                    out.deleted.push(DeletedObject {
                        key: Some(o.key.clone()),
                        version_id: o.version_id.clone(),
                        delete_marker: marker.then_some(true),
                        delete_marker_version_id: if marker { version_id } else { None },
                    });
                }
            }
            Ok(out)
        })
    }

    fn copy_object(
        &self,
        ctx: &RequestContext,
        i: CopyObjectRequest,
    ) -> Result<CopyObjectOutput, AwsError> {
        let (src_bucket, src_key, version) = parse_copy_source(&i.copy_source)?;
        self.db.transaction(|tx| {
            let dst = load_bucket(tx, &i.bucket)?;
            load_bucket(tx, &src_bucket)?;
            let Some(o) = find_obj(tx, &src_bucket, &src_key, version.as_deref())?.filter(|o| !o.delete_marker) else {
                return Err(no_such_key(&src_key));
            };
            check_conditions(
                &o,
                &i.copy_source_if_match,
                &i.copy_source_if_none_match,
                i.copy_source_if_modified_since,
                i.copy_source_if_unmodified_since,
            )
            .map_err(|e| if e.status == 304 { precondition_failed("x-amz-copy-source-If-None-Match") } else { e })?;
            let replace_meta = i.metadata_directive.as_deref() == Some("REPLACE");
            let replace_tags = i.tagging_directive.as_deref() == Some("REPLACE");
            let same = src_bucket == i.bucket && src_key == i.key;
            if same
                && !replace_meta
                && !replace_tags
                && i.storage_class.is_none()
                && i.website_redirect_location.is_none()
                && i.server_side_encryption.is_none()
                && !dst.versioned()
            {
                return Err(AwsError::sender(
                    400,
                    "InvalidRequest",
                    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
                ));
            }
            let data = self.read_body(&o)?;
            let mut attrs = NewObj {
                content_type: Some(o.content_type.clone()),
                metadata: o.metadata.clone(),
                headers: o.headers.clone(),
                tags: o.tags.clone(),
                storage_class: Some(i.storage_class.clone().unwrap_or_else(|| o.storage_class.clone())),
                acl: crate::acl::build(
                    &i.acl,
                    None,
                    [
                        ("FULL_CONTROL", &i.grant_full_control),
                        ("READ", &i.grant_read),
                        ("READ_ACP", &i.grant_read_acp),
                        ("WRITE_ACP", &i.grant_write_acp),
                        ("", &None),
                    ],
                )?
                .map(|p| crate::acl::to_xml(&p)),
            };
            if let Some(sse) = &i.server_side_encryption {
                attrs
                    .headers
                    .insert("server_side_encryption".into(), sse.clone());
            }
            if let Some(key_id) = &i.ssekms_key_id {
                attrs.headers.insert("ssekms_key_id".into(), key_id.clone());
            }
            if replace_meta {
                let fresh = attrs_from_headers(
                    &i.content_type,
                    &i.metadata,
                    &i.cache_control,
                    &i.content_disposition,
                    &i.content_encoding,
                    &i.content_language,
                    &i.expires,
                    &i.website_redirect_location,
                    &i.server_side_encryption,
                    &i.storage_class,
                );
                attrs.content_type = fresh.content_type.or(Some(o.content_type.clone()));
                attrs.metadata = fresh.metadata;
                attrs.headers = fresh.headers;
                if let Some(key_id) = &i.ssekms_key_id {
                    attrs.headers.insert("ssekms_key_id".into(), key_id.clone());
                }
            } else if let Some(key_id) = &i.ssekms_key_id {
                attrs.headers.insert("ssekms_key_id".into(), key_id.clone());
            }
            if replace_tags {
                attrs.tags = match &i.tagging {
                    Some(t) => parse_tagging_header(t)?,
                    None => BTreeMap::new(),
                };
            }
            let response_sse = attrs.headers.get("server_side_encryption").cloned();
            let response_ssekms_key_id = attrs.headers.get("ssekms_key_id").cloned();
            let n = self.store_object(tx, &dst, &i.key, &data, o.etag.clone(), attrs)?;
            crate::notifications::record(tx, ctx, &dst, &i.key, "ObjectCreated:Copy", Some(&n))?;
            Ok(CopyObjectOutput {
                copy_object_result: Some(CopyObjectResult {
                    e_tag: Some(etag_quoted(&n.etag)),
                    last_modified: Some(Timestamp(n.last_modified)),
                    ..Default::default()
                }),
                copy_source_version_id: (o.version_id != "null").then(|| o.version_id.clone()),
                server_side_encryption: response_sse,
                ssekms_key_id: response_ssekms_key_id,
                version_id: (n.version_id != "null").then(|| n.version_id),
                ..Default::default()
            })
        })
    }

    // ---- listing ----
    fn list_objects(
        &self,
        _ctx: &RequestContext,
        i: ListObjectsRequest,
    ) -> Result<ListObjectsOutput, AwsError> {
        let max = i.max_keys.unwrap_or(MAX_KEYS);
        validate_max_keys(max)?;
        let marker = i.marker.clone().unwrap_or_default();
        let page = self.list_page(
            &i.bucket,
            i.prefix.as_deref().unwrap_or(""),
            i.delimiter.as_deref(),
            &marker,
            max,
        )?;
        let url = i.encoding_type.as_deref() == Some("url");
        let enc = |s: &str| if url { url_encode(s) } else { s.to_string() };
        let truncated = page.truncated;
        Ok(ListObjectsOutput {
            name: Some(i.bucket.clone()),
            prefix: Some(enc(i.prefix.as_deref().unwrap_or(""))),
            marker: Some(enc(&marker)),
            max_keys: Some(max),
            delimiter: i.delimiter.as_deref().map(enc),
            encoding_type: url.then(|| "url".to_string()),
            is_truncated: Some(truncated),
            next_marker: (truncated && i.delimiter.is_some()).then(|| enc(&page.last)),
            contents: page
                .objects
                .iter()
                .map(|o| to_object(o, &enc, false))
                .collect(),
            common_prefixes: page
                .prefixes
                .iter()
                .map(|p| CommonPrefix {
                    prefix: Some(enc(p)),
                })
                .collect(),
            request_charged: None,
        })
    }

    fn list_objects_v2(
        &self,
        _ctx: &RequestContext,
        i: ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, AwsError> {
        let max = i.max_keys.unwrap_or(MAX_KEYS);
        validate_max_keys(max)?;
        let token = match &i.continuation_token {
            Some(t) => decode_token(t)?,
            None => i.start_after.clone().unwrap_or_default(),
        };
        let page = self.list_page(
            &i.bucket,
            i.prefix.as_deref().unwrap_or(""),
            i.delimiter.as_deref(),
            &token,
            max,
        )?;
        let url = i.encoding_type.as_deref() == Some("url");
        let enc = |s: &str| if url { url_encode(s) } else { s.to_string() };
        let fetch_owner = i.fetch_owner.unwrap_or(false);
        let contents: Vec<Object> = page
            .objects
            .iter()
            .map(|o| to_object(o, &enc, fetch_owner))
            .collect();
        let prefixes: Vec<CommonPrefix> = page
            .prefixes
            .iter()
            .map(|p| CommonPrefix {
                prefix: Some(enc(p)),
            })
            .collect();
        Ok(ListObjectsV2Output {
            name: Some(i.bucket.clone()),
            prefix: Some(enc(i.prefix.as_deref().unwrap_or(""))),
            delimiter: i.delimiter.as_deref().map(enc),
            max_keys: Some(max),
            encoding_type: url.then(|| "url".to_string()),
            key_count: Some((contents.len() + prefixes.len()) as i32),
            is_truncated: Some(page.truncated),
            continuation_token: i.continuation_token.clone(),
            next_continuation_token: page.truncated.then(|| encode_token(&page.last)),
            start_after: i.start_after.as_deref().map(enc),
            contents,
            common_prefixes: prefixes,
            request_charged: None,
        })
    }

    // ---- tagging ----
    fn get_object_tagging(
        &self,
        _ctx: &RequestContext,
        i: GetObjectTaggingRequest,
    ) -> Result<GetObjectTaggingOutput, AwsError> {
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .filter(|o| !o.delete_marker)
                .ok_or_else(|| no_such_key(&i.key))?;
            Ok(GetObjectTaggingOutput {
                tag_set: o
                    .tags
                    .iter()
                    .map(|(k, v)| Tag {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect(),
                version_id: (o.version_id != "null").then(|| o.version_id),
            })
        })
    }

    fn put_object_tagging(
        &self,
        _ctx: &RequestContext,
        i: PutObjectTaggingRequest,
    ) -> Result<PutObjectTaggingOutput, AwsError> {
        validate_tags(&i.tagging.tag_set)?;
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .filter(|o| !o.delete_marker)
                .ok_or_else(|| no_such_key(&i.key))?;
            let tags: BTreeMap<String, String> = i
                .tagging
                .tag_set
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect();
            tx.execute(
                "UPDATE objects SET tags = ?1 WHERE seq = ?2",
                params![json_string(&tags), o.seq],
            )?;
            Ok(PutObjectTaggingOutput {
                version_id: (o.version_id != "null").then(|| o.version_id),
            })
        })
    }

    fn delete_object_tagging(
        &self,
        _ctx: &RequestContext,
        i: DeleteObjectTaggingRequest,
    ) -> Result<DeleteObjectTaggingOutput, AwsError> {
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .filter(|o| !o.delete_marker)
                .ok_or_else(|| no_such_key(&i.key))?;
            tx.execute(
                "UPDATE objects SET tags = '{}' WHERE seq = ?1",
                params![o.seq],
            )?;
            Ok(DeleteObjectTaggingOutput {
                version_id: (o.version_id != "null").then(|| o.version_id),
            })
        })
    }

    // ---- versioning ----
    fn get_bucket_versioning(
        &self,
        _ctx: &RequestContext,
        i: GetBucketVersioningRequest,
    ) -> Result<GetBucketVersioningOutput, AwsError> {
        self.db.transaction(|tx| {
            let b = load_bucket(tx, &i.bucket)?;
            Ok(GetBucketVersioningOutput {
                status: b.versioned().then_some(b.versioning),
                ..Default::default()
            })
        })
    }

    fn put_bucket_versioning(
        &self,
        _ctx: &RequestContext,
        i: PutBucketVersioningRequest,
    ) -> Result<(), AwsError> {
        let status = i
            .versioning_configuration
            .status
            .clone()
            .unwrap_or_default();
        if !matches!(status.as_str(), "Enabled" | "Suspended") {
            return Err(AwsError::sender(
                400,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
            ));
        }
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            tx.execute(
                "UPDATE buckets SET versioning = ?1 WHERE name = ?2",
                params![status, i.bucket],
            )?;
            Ok(())
        })
    }

    // ---- multipart ----
    fn create_multipart_upload(
        &self,
        ctx: &RequestContext,
        i: CreateMultipartUploadRequest,
    ) -> Result<CreateMultipartUploadOutput, AwsError> {
        let mut attrs = attrs_from_headers(
            &i.content_type,
            &i.metadata,
            &i.cache_control,
            &i.content_disposition,
            &i.content_encoding,
            &i.content_language,
            &i.expires,
            &i.website_redirect_location,
            &i.server_side_encryption,
            &i.storage_class,
        );
        if let Some(t) = &i.tagging {
            attrs.tags = parse_tagging_header(t)?;
        }
        attrs.acl = crate::acl::build(
            &i.acl,
            None,
            [
                ("FULL_CONTROL", &i.grant_full_control),
                ("READ", &i.grant_read),
                ("READ_ACP", &i.grant_read_acp),
                ("WRITE_ACP", &i.grant_write_acp),
                ("", &None),
            ],
        )?
        .map(|p| crate::acl::to_xml(&p));
        crate::multipart::create(self, ctx, i, attrs)
    }
    fn upload_part(
        &self,
        ctx: &RequestContext,
        i: UploadPartRequest,
    ) -> Result<UploadPartOutput, AwsError> {
        crate::multipart::upload_part(self, ctx, i)
    }
    fn upload_part_copy(
        &self,
        ctx: &RequestContext,
        i: UploadPartCopyRequest,
    ) -> Result<UploadPartCopyOutput, AwsError> {
        crate::multipart::upload_part_copy(self, ctx, i)
    }
    fn complete_multipart_upload(
        &self,
        ctx: &RequestContext,
        i: CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadOutput, AwsError> {
        crate::multipart::complete(self, ctx, i)
    }
    fn abort_multipart_upload(
        &self,
        ctx: &RequestContext,
        i: AbortMultipartUploadRequest,
    ) -> Result<AbortMultipartUploadOutput, AwsError> {
        crate::multipart::abort(self, ctx, i)
    }
    fn list_parts(
        &self,
        ctx: &RequestContext,
        i: ListPartsRequest,
    ) -> Result<ListPartsOutput, AwsError> {
        crate::multipart::list_parts(self, ctx, i)
    }
    fn list_multipart_uploads(
        &self,
        ctx: &RequestContext,
        i: ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsOutput, AwsError> {
        crate::multipart::list_uploads(self, ctx, i)
    }

    // ---- bucket sub-resources ----
    fn put_bucket_cors(
        &self,
        _ctx: &RequestContext,
        i: PutBucketCorsRequest,
    ) -> Result<(), AwsError> {
        if i.cors_configuration.cors_rules.is_empty() {
            return Err(AwsError::sender(
                400,
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
            ));
        }
        config::save(
            self,
            &i.bucket,
            "cors",
            "CORSConfiguration",
            &i.cors_configuration,
        )
    }
    fn get_bucket_cors(
        &self,
        _ctx: &RequestContext,
        i: GetBucketCorsRequest,
    ) -> Result<GetBucketCorsOutput, AwsError> {
        config::load::<GetBucketCorsOutput>(self, &i.bucket, "cors")?.ok_or_else(|| {
            config::missing(
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
                &i.bucket,
            )
        })
    }
    fn delete_bucket_cors(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketCorsRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "cors")
    }

    fn put_bucket_lifecycle_configuration(
        &self,
        _ctx: &RequestContext,
        i: PutBucketLifecycleConfigurationRequest,
    ) -> Result<PutBucketLifecycleConfigurationOutput, AwsError> {
        let cfg = i
            .lifecycle_configuration
            .ok_or_else(|| AwsError::missing_parameter("LifecycleConfiguration"))?;
        config::save(self, &i.bucket, "lifecycle", "LifecycleConfiguration", &cfg)?;
        Ok(PutBucketLifecycleConfigurationOutput::default())
    }
    fn get_bucket_lifecycle_configuration(
        &self,
        _ctx: &RequestContext,
        i: GetBucketLifecycleConfigurationRequest,
    ) -> Result<GetBucketLifecycleConfigurationOutput, AwsError> {
        config::load::<GetBucketLifecycleConfigurationOutput>(self, &i.bucket, "lifecycle")?
            .ok_or_else(|| {
                config::missing(
                    "NoSuchLifecycleConfiguration",
                    "The lifecycle configuration does not exist",
                    &i.bucket,
                )
            })
    }
    fn delete_bucket_lifecycle(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketLifecycleRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "lifecycle")
    }

    fn put_bucket_website(
        &self,
        _ctx: &RequestContext,
        i: PutBucketWebsiteRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "website",
            "WebsiteConfiguration",
            &i.website_configuration,
        )
    }
    fn get_bucket_website(
        &self,
        _ctx: &RequestContext,
        i: GetBucketWebsiteRequest,
    ) -> Result<GetBucketWebsiteOutput, AwsError> {
        config::load::<GetBucketWebsiteOutput>(self, &i.bucket, "website")?.ok_or_else(|| {
            config::missing(
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
                &i.bucket,
            )
        })
    }
    fn delete_bucket_website(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketWebsiteRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "website")
    }

    fn put_bucket_encryption(
        &self,
        _ctx: &RequestContext,
        i: PutBucketEncryptionRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "encryption",
            "ServerSideEncryptionConfiguration",
            &i.server_side_encryption_configuration,
        )
    }
    fn get_bucket_encryption(
        &self,
        _ctx: &RequestContext,
        i: GetBucketEncryptionRequest,
    ) -> Result<GetBucketEncryptionOutput, AwsError> {
        let cfg = config::load::<ServerSideEncryptionConfiguration>(self, &i.bucket, "encryption")?
            .ok_or_else(|| {
                config::missing(
                    "ServerSideEncryptionConfigurationNotFoundError",
                    "The server side encryption configuration was not found",
                    &i.bucket,
                )
            })?;
        Ok(GetBucketEncryptionOutput {
            server_side_encryption_configuration: Some(cfg),
        })
    }
    fn delete_bucket_encryption(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketEncryptionRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "encryption")
    }

    fn put_bucket_replication(
        &self,
        _ctx: &RequestContext,
        i: PutBucketReplicationRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "replication",
            "ReplicationConfiguration",
            &i.replication_configuration,
        )
    }
    fn get_bucket_replication(
        &self,
        _ctx: &RequestContext,
        i: GetBucketReplicationRequest,
    ) -> Result<GetBucketReplicationOutput, AwsError> {
        let cfg = config::load::<ReplicationConfiguration>(self, &i.bucket, "replication")?
            .ok_or_else(|| {
                config::missing(
                    "ReplicationConfigurationNotFoundError",
                    "The replication configuration was not found",
                    &i.bucket,
                )
            })?;
        Ok(GetBucketReplicationOutput {
            replication_configuration: Some(cfg),
        })
    }
    fn delete_bucket_replication(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketReplicationRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "replication")
    }

    fn put_bucket_ownership_controls(
        &self,
        _ctx: &RequestContext,
        i: PutBucketOwnershipControlsRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "ownership",
            "OwnershipControls",
            &i.ownership_controls,
        )
    }
    fn get_bucket_ownership_controls(
        &self,
        _ctx: &RequestContext,
        i: GetBucketOwnershipControlsRequest,
    ) -> Result<GetBucketOwnershipControlsOutput, AwsError> {
        let cfg =
            config::load::<OwnershipControls>(self, &i.bucket, "ownership")?.ok_or_else(|| {
                config::missing(
                    "OwnershipControlsNotFoundError",
                    "The bucket ownership controls were not found",
                    &i.bucket,
                )
            })?;
        Ok(GetBucketOwnershipControlsOutput {
            ownership_controls: Some(cfg),
        })
    }
    fn delete_bucket_ownership_controls(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketOwnershipControlsRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "ownership")
    }

    fn put_public_access_block(
        &self,
        _ctx: &RequestContext,
        i: PutPublicAccessBlockRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "public_access_block",
            "PublicAccessBlockConfiguration",
            &i.public_access_block_configuration,
        )
    }
    fn get_public_access_block(
        &self,
        _ctx: &RequestContext,
        i: GetPublicAccessBlockRequest,
    ) -> Result<GetPublicAccessBlockOutput, AwsError> {
        let cfg =
            config::load::<PublicAccessBlockConfiguration>(self, &i.bucket, "public_access_block")?
                .ok_or_else(|| {
                    config::missing(
                        "NoSuchPublicAccessBlockConfiguration",
                        "The public access block configuration was not found",
                        &i.bucket,
                    )
                })?;
        Ok(GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(cfg),
        })
    }
    fn delete_public_access_block(
        &self,
        _ctx: &RequestContext,
        i: DeletePublicAccessBlockRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "public_access_block")
    }

    fn put_bucket_logging(
        &self,
        _ctx: &RequestContext,
        i: PutBucketLoggingRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "logging",
            "BucketLoggingStatus",
            &i.bucket_logging_status,
        )
    }
    fn get_bucket_logging(
        &self,
        _ctx: &RequestContext,
        i: GetBucketLoggingRequest,
    ) -> Result<GetBucketLoggingOutput, AwsError> {
        Ok(config::load::<GetBucketLoggingOutput>(self, &i.bucket, "logging")?.unwrap_or_default())
    }

    fn put_bucket_notification_configuration(
        &self,
        _ctx: &RequestContext,
        i: PutBucketNotificationConfigurationRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "notification",
            "NotificationConfiguration",
            &i.notification_configuration,
        )
    }
    fn get_bucket_notification_configuration(
        &self,
        _ctx: &RequestContext,
        i: GetBucketNotificationConfigurationRequest,
    ) -> Result<NotificationConfiguration, AwsError> {
        Ok(
            config::load::<NotificationConfiguration>(self, &i.bucket, "notification")?
                .unwrap_or_default(),
        )
    }

    fn put_bucket_accelerate_configuration(
        &self,
        _ctx: &RequestContext,
        i: PutBucketAccelerateConfigurationRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "accelerate",
            "AccelerateConfiguration",
            &i.accelerate_configuration,
        )
    }
    fn get_bucket_accelerate_configuration(
        &self,
        _ctx: &RequestContext,
        i: GetBucketAccelerateConfigurationRequest,
    ) -> Result<GetBucketAccelerateConfigurationOutput, AwsError> {
        Ok(
            config::load::<GetBucketAccelerateConfigurationOutput>(self, &i.bucket, "accelerate")?
                .unwrap_or_default(),
        )
    }

    fn put_bucket_request_payment(
        &self,
        _ctx: &RequestContext,
        i: PutBucketRequestPaymentRequest,
    ) -> Result<(), AwsError> {
        config::save(
            self,
            &i.bucket,
            "request_payment",
            "RequestPaymentConfiguration",
            &i.request_payment_configuration,
        )
    }
    fn get_bucket_request_payment(
        &self,
        _ctx: &RequestContext,
        i: GetBucketRequestPaymentRequest,
    ) -> Result<GetBucketRequestPaymentOutput, AwsError> {
        Ok(
            config::load::<GetBucketRequestPaymentOutput>(self, &i.bucket, "request_payment")?
                .unwrap_or(GetBucketRequestPaymentOutput {
                    payer: Some("BucketOwner".into()),
                }),
        )
    }

    fn put_bucket_tagging(
        &self,
        _ctx: &RequestContext,
        i: PutBucketTaggingRequest,
    ) -> Result<(), AwsError> {
        validate_tags(&i.tagging.tag_set)?;
        config::save(self, &i.bucket, "tagging", "Tagging", &i.tagging)
    }
    fn get_bucket_tagging(
        &self,
        _ctx: &RequestContext,
        i: GetBucketTaggingRequest,
    ) -> Result<GetBucketTaggingOutput, AwsError> {
        config::load::<GetBucketTaggingOutput>(self, &i.bucket, "tagging")?
            .ok_or_else(|| config::missing("NoSuchTagSet", "The TagSet does not exist", &i.bucket))
    }
    fn delete_bucket_tagging(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketTaggingRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "tagging")
    }

    fn put_bucket_policy(
        &self,
        _ctx: &RequestContext,
        i: PutBucketPolicyRequest,
    ) -> Result<(), AwsError> {
        if serde_json::from_str::<serde_json::Value>(&i.policy).is_err() {
            return Err(AwsError::sender(
                400,
                "MalformedPolicy",
                "Policies must be valid JSON and the first byte must be '{'",
            ));
        }
        config::save_raw(self, &i.bucket, "policy", &i.policy)
    }
    fn get_bucket_policy(
        &self,
        _ctx: &RequestContext,
        i: GetBucketPolicyRequest,
    ) -> Result<GetBucketPolicyOutput, AwsError> {
        let policy = config::load_raw(self, &i.bucket, "policy")?.ok_or_else(|| {
            config::missing(
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                &i.bucket,
            )
        })?;
        Ok(GetBucketPolicyOutput {
            policy: Some(policy),
        })
    }
    fn delete_bucket_policy(
        &self,
        _ctx: &RequestContext,
        i: DeleteBucketPolicyRequest,
    ) -> Result<(), AwsError> {
        config::remove(self, &i.bucket, "policy")
    }

    // ---- ACLs ----
    fn put_bucket_acl(
        &self,
        _ctx: &RequestContext,
        i: PutBucketAclRequest,
    ) -> Result<(), AwsError> {
        let policy = crate::acl::build(
            &i.acl,
            i.access_control_policy.as_ref(),
            [
                ("FULL_CONTROL", &i.grant_full_control),
                ("READ", &i.grant_read),
                ("READ_ACP", &i.grant_read_acp),
                ("WRITE", &i.grant_write),
                ("WRITE_ACP", &i.grant_write_acp),
            ],
        )?
        .ok_or_else(|| {
            AwsError::sender(
                400,
                "MissingSecurityHeader",
                "Your request was missing a required header",
            )
        })?;
        config::save_raw(self, &i.bucket, "acl", &crate::acl::to_xml(&policy))
    }
    fn get_bucket_acl(
        &self,
        _ctx: &RequestContext,
        i: GetBucketAclRequest,
    ) -> Result<GetBucketAclOutput, AwsError> {
        let doc = config::load_raw(self, &i.bucket, "acl")?;
        let p = crate::acl::stored_or_default(doc.as_deref());
        Ok(GetBucketAclOutput {
            grants: p.grants,
            owner: p.owner,
        })
    }
    fn put_object_acl(
        &self,
        _ctx: &RequestContext,
        i: PutObjectAclRequest,
    ) -> Result<PutObjectAclOutput, AwsError> {
        let policy = crate::acl::build(
            &i.acl,
            i.access_control_policy.as_ref(),
            [
                ("FULL_CONTROL", &i.grant_full_control),
                ("READ", &i.grant_read),
                ("READ_ACP", &i.grant_read_acp),
                ("WRITE", &i.grant_write),
                ("WRITE_ACP", &i.grant_write_acp),
            ],
        )?
        .ok_or_else(|| {
            AwsError::sender(
                400,
                "MissingSecurityHeader",
                "Your request was missing a required header",
            )
        })?;
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .filter(|o| !o.delete_marker)
                .ok_or_else(|| no_such_key(&i.key))?;
            tx.execute(
                "UPDATE objects SET acl = ?1 WHERE seq = ?2",
                params![crate::acl::to_xml(&policy), o.seq],
            )?;
            Ok(PutObjectAclOutput::default())
        })
    }
    fn get_object_acl(
        &self,
        _ctx: &RequestContext,
        i: GetObjectAclRequest,
    ) -> Result<GetObjectAclOutput, AwsError> {
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .filter(|o| !o.delete_marker)
                .ok_or_else(|| no_such_key(&i.key))?;
            let p = crate::acl::stored_or_default(o.acl.as_deref());
            Ok(GetObjectAclOutput {
                grants: p.grants,
                owner: p.owner,
                request_charged: None,
            })
        })
    }

    // ---- versions & attributes ----
    fn list_object_versions(
        &self,
        _ctx: &RequestContext,
        i: ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsOutput, AwsError> {
        self.list_versions(i)
    }

    fn get_object_attributes(
        &self,
        _ctx: &RequestContext,
        i: GetObjectAttributesRequest,
    ) -> Result<GetObjectAttributesOutput, AwsError> {
        if i.object_attributes.is_empty() {
            return Err(AwsError::sender(
                400,
                "InvalidArgument",
                "Argument x-amz-object-attributes must be provided",
            ));
        }
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let o = find_obj(tx, &i.bucket, &i.key, i.version_id.as_deref())?
                .ok_or_else(|| no_such_key(&i.key))?;
            if o.delete_marker {
                return Err(if i.version_id.is_some() {
                    AwsError::sender(
                        405,
                        "MethodNotAllowed",
                        "The specified method is not allowed against this resource.",
                    )
                } else {
                    no_such_key(&i.key).with("DeleteMarker", "true")
                });
            }
            let want = |a: &str| i.object_attributes.iter().any(|x| x == a);
            let mut out = GetObjectAttributesOutput {
                last_modified: Some(Timestamp(o.last_modified)),
                version_id: (o.version_id != "null").then(|| o.version_id.clone()),
                ..Default::default()
            };
            if want("ETag") {
                out.e_tag = Some(o.etag.clone());
            }
            if want("StorageClass") {
                out.storage_class = Some(o.storage_class.clone());
            }
            if want("ObjectSize") {
                out.object_size = Some(o.size);
            }
            if want("ObjectParts") {
                if let Some(layout) = o.headers.get("mp_parts") {
                    let all: Vec<(i32, i64)> = layout
                        .split(',')
                        .filter_map(|p| p.split_once(':'))
                        .filter_map(|(n, s)| Some((n.parse().ok()?, s.parse().ok()?)))
                        .collect();
                    let marker = i.part_number_marker.unwrap_or(0);
                    let max = i.max_parts.unwrap_or(1000).max(0) as usize;
                    let rest: Vec<_> = all.iter().filter(|(n, _)| *n > marker).collect();
                    let page: Vec<_> = rest.iter().take(max).collect();
                    out.object_parts = Some(GetObjectAttributesParts {
                        total_parts_count: Some(all.len() as i32),
                        part_number_marker: Some(marker),
                        next_part_number_marker: page.last().map(|p| p.0),
                        max_parts: Some(max as i32),
                        is_truncated: Some(rest.len() > max),
                        parts: page
                            .iter()
                            .map(|(n, s)| ObjectPart {
                                part_number: Some(*n),
                                size: Some(*s),
                                ..Default::default()
                            })
                            .collect(),
                    });
                }
            }
            Ok(out)
        })
    }
}

fn validate_max_keys(max: i32) -> Result<(), AwsError> {
    if max < 0 {
        return Err(invalid_argument(
            "Argument maxKeys must be an integer between 0 and 2147483647",
        )
        .with("ArgumentName", "maxKeys")
        .with("ArgumentValue", max.to_string()));
    }
    Ok(())
}

fn validate_tags(tags: &[Tag]) -> Result<(), AwsError> {
    let mut seen = std::collections::BTreeSet::new();
    for t in tags {
        if !seen.insert(&t.key) {
            return Err(AwsError::sender(
                400,
                "InvalidTag",
                "Cannot provide multiple Tags with the same key",
            )
            .with("TagKey", t.key.clone()));
        }
    }
    if tags.len() > 10 {
        return Err(AwsError::sender(
            400,
            "BadRequest",
            "Object tags cannot be greater than 10",
        ));
    }
    Ok(())
}

fn encode_token(key: &str) -> String {
    roto_protocol::base64::encode(key.as_bytes())
}

fn decode_token(t: &str) -> Result<String, AwsError> {
    roto_protocol::base64::decode(t)
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| invalid_argument("The continuation token provided is incorrect"))
}

fn to_object(o: &Obj, enc: &dyn Fn(&str) -> String, fetch_owner: bool) -> Object {
    Object {
        key: Some(enc(&o.key)),
        last_modified: Some(Timestamp(o.last_modified)),
        e_tag: Some(etag_quoted(&o.etag)),
        size: Some(o.size),
        storage_class: Some(o.storage_class.clone()),
        owner: fetch_owner.then(owner),
        ..Default::default()
    }
}

struct Page {
    objects: Vec<Obj>,
    prefixes: Vec<String>,
    truncated: bool,
    /// Last key or prefix returned; the next page starts after it.
    last: String,
}

impl S3 {
    /// Walks current, non-deleted objects in key order, grouping by delimiter.
    fn list_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: &str,
        max: i32,
    ) -> Result<Page, AwsError> {
        self.db.transaction(|tx| {
            load_bucket(tx, bucket)?;
            let mut page = Page { objects: Vec::new(), prefixes: Vec::new(), truncated: false, last: String::new() };
            if max == 0 {
                return Ok(page);
            }
            let delimiter = delimiter.filter(|d| !d.is_empty());
            let mut cursor = after.to_string();
            let mut exclusive = true; // key > cursor, or >= after a prefix skip
            let mut count = 0;
            loop {
                let sql = format!(
                    "SELECT {OBJ_COLS} FROM objects WHERE bucket = ?1 AND is_latest = 1 AND delete_marker = 0
                       AND key {} ?2 AND substr(key, 1, length(?3)) = ?3 ORDER BY key LIMIT 200",
                    if exclusive { ">" } else { ">=" }
                );
                let start = if cursor.as_str() < prefix { prefix.to_string() } else { cursor.clone() };
                let ex = exclusive && cursor.as_str() >= prefix;
                let sql = if ex { sql } else { sql.replace("key > ?2", "key >= ?2") };
                let rows: Vec<Obj> = {
                    let mut stmt = tx.prepare(&sql)?;
                    stmt.query_map(params![bucket, start, prefix], obj_from_row)?.collect::<Result<_, _>>()?
                };
                if rows.is_empty() {
                    return Ok(page);
                }
                let n = rows.len();
                let mut skipped_to: Option<String> = None;
                for o in rows {
                    let rest = &o.key[prefix.len()..];
                    let group = delimiter.and_then(|d| rest.find(d).map(|i| format!("{prefix}{}{d}", &rest[..i])));
                    if let Some(p) = &group {
                        if page.prefixes.last() == Some(p) {
                            continue;
                        }
                    }
                    if count >= max {
                        page.truncated = true;
                        return Ok(page);
                    }
                    count += 1;
                    match group {
                        Some(p) => {
                            page.last = p.clone();
                            page.prefixes.push(p.clone());
                            // Skip everything under this prefix.
                            skipped_to = Some(format!("{p}\u{10FFFF}"));
                            break;
                        }
                        None => {
                            page.last = o.key.clone();
                            page.objects.push(o);
                        }
                    }
                }
                match skipped_to {
                    Some(s) => {
                        cursor = s;
                        exclusive = true;
                    }
                    None => {
                        if n < 200 {
                            return Ok(page);
                        }
                        cursor = page.last.clone();
                        exclusive = true;
                    }
                }
            }
        })
    }
}

impl S3 {
    /// All versions and delete markers in key order (newest version first within a key).
    fn list_versions(
        &self,
        i: ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsOutput, AwsError> {
        let max = i.max_keys.unwrap_or(MAX_KEYS);
        validate_max_keys(max)?;
        self.db.transaction(|tx| {
            load_bucket(tx, &i.bucket)?;
            let prefix = i.prefix.clone().unwrap_or_default();
            let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
            let key_marker = i.key_marker.clone().unwrap_or_default();
            let vid_marker = i.version_id_marker.clone();
            let rows: Vec<Obj> = {
                let mut stmt = tx.prepare(&format!(
                    "SELECT {OBJ_COLS} FROM objects WHERE bucket = ?1 AND substr(key, 1, length(?2)) = ?2
                       AND key >= ?3 ORDER BY key, last_modified DESC, seq DESC"
                ))?;
                stmt.query_map(params![i.bucket, prefix, key_marker], obj_from_row)?.collect::<Result<_, _>>()?
            };
            let url = i.encoding_type.as_deref() == Some("url");
            let enc = |s: &str| if url { url_encode(s) } else { s.to_string() };
            let mut out = ListObjectVersionsOutput {
                name: Some(i.bucket.clone()),
                prefix: Some(enc(&prefix)),
                key_marker: Some(enc(&key_marker)),
                version_id_marker: vid_marker.clone(),
                max_keys: Some(max),
                delimiter: delimiter.as_deref().map(enc),
                encoding_type: url.then(|| "url".to_string()),
                is_truncated: Some(false),
                ..Default::default()
            };
            let mut skipping = vid_marker.is_some();
            let mut count = 0;
            let (mut last_key, mut last_vid) = (String::new(), String::new());
            for o in rows {
                if o.key == key_marker && !key_marker.is_empty() {
                    if let Some(v) = &vid_marker {
                        if skipping {
                            if &o.version_id == v {
                                skipping = false;
                            }
                            continue;
                        }
                    } else {
                        continue; // key marker without version marker: strictly after the key
                    }
                }
                let rest = &o.key[prefix.len()..];
                if let Some(d) = &delimiter {
                    if let Some(p) = rest.find(d.as_str()) {
                        let cp = format!("{prefix}{}{d}", &rest[..p]);
                        if out.common_prefixes.iter().any(|c| c.prefix.as_deref() == Some(&enc(&cp))) {
                            continue;
                        }
                        if count >= max {
                            out.is_truncated = Some(true);
                            break;
                        }
                        count += 1;
                        out.common_prefixes.push(CommonPrefix { prefix: Some(enc(&cp)) });
                        last_key = cp;
                        last_vid = String::new();
                        continue;
                    }
                }
                if count >= max {
                    out.is_truncated = Some(true);
                    break;
                }
                count += 1;
                last_key = o.key.clone();
                last_vid = o.version_id.clone();
                if o.delete_marker {
                    out.delete_markers.push(DeleteMarkerEntry {
                        key: Some(enc(&o.key)),
                        version_id: Some(o.version_id.clone()),
                        is_latest: Some(o.is_latest),
                        last_modified: Some(Timestamp(o.last_modified)),
                        owner: Some(owner()),
                    });
                } else {
                    out.versions.push(ObjectVersion {
                        key: Some(enc(&o.key)),
                        version_id: Some(o.version_id.clone()),
                        is_latest: Some(o.is_latest),
                        last_modified: Some(Timestamp(o.last_modified)),
                        e_tag: Some(etag_quoted(&o.etag)),
                        size: Some(o.size),
                        storage_class: Some(o.storage_class.clone()),
                        owner: Some(owner()),
                        ..Default::default()
                    });
                }
            }
            if out.is_truncated == Some(true) {
                out.next_key_marker = Some(enc(&last_key));
                if !last_vid.is_empty() {
                    out.next_version_id_marker = Some(last_vid);
                }
            }
            Ok(out)
        })
    }
}
