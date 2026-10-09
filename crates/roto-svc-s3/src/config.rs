//! Bucket sub-resources (CORS, lifecycle, website, …): stored as the XML document the client
//! sent and read back through the generated `XmlRead` of the matching output type.

use roto_core::AwsError;
use roto_core::rusqlite::{OptionalExtension, params};
use roto_protocol::restxml::{XmlRead, parse_xml};
use roto_protocol::{XmlValue, XmlWriter};

use crate::service::{S3, load_bucket};

pub(crate) fn save<T: XmlValue>(
    s3: &S3,
    bucket: &str,
    kind: &str,
    root: &str,
    value: &T,
) -> Result<(), AwsError> {
    let mut w = XmlWriter::new();
    value.write(&mut w, root);
    save_raw(s3, bucket, kind, &w.finish())
}

pub(crate) fn save_raw(s3: &S3, bucket: &str, kind: &str, body: &str) -> Result<(), AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, bucket)?;
        tx.execute(
            "INSERT INTO bucket_configs (bucket, kind, body) VALUES (?1, ?2, ?3)
             ON CONFLICT (bucket, kind) DO UPDATE SET body = excluded.body",
            params![bucket, kind, body],
        )?;
        Ok(())
    })
}

pub(crate) fn load_raw(s3: &S3, bucket: &str, kind: &str) -> Result<Option<String>, AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, bucket)?;
        Ok(tx
            .query_row(
                "SELECT body FROM bucket_configs WHERE bucket = ?1 AND kind = ?2",
                params![bucket, kind],
                |r| r.get(0),
            )
            .optional()?)
    })
}

/// Reads the stored document's root element as `T`.
pub(crate) fn load<T: XmlRead>(s3: &S3, bucket: &str, kind: &str) -> Result<Option<T>, AwsError> {
    match load_raw(s3, bucket, kind)? {
        None => Ok(None),
        Some(xml) => {
            let doc = parse_xml(xml.as_bytes())?;
            Ok(Some(T::read_xml(doc.root_element())?))
        }
    }
}

pub(crate) fn remove(s3: &S3, bucket: &str, kind: &str) -> Result<(), AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, bucket)?;
        tx.execute(
            "DELETE FROM bucket_configs WHERE bucket = ?1 AND kind = ?2",
            params![bucket, kind],
        )?;
        Ok(())
    })
}

/// 404 error for a sub-resource that was never configured.
pub(crate) fn missing(code: &str, message: &str, bucket: &str) -> AwsError {
    AwsError::sender(404, code, message).with("BucketName", bucket)
}
