//! Bucket sub-resources (CORS, lifecycle, website, …): stored as the XML document the client
//! sent and read back through the generated `XmlRead` of the matching output type.

use crate::schema::*;
use diesel::prelude::*;
use roto_core::AwsError;
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
        diesel::insert_into(bucket_configs::table)
            .values((
                bucket_configs::bucket.eq(&(bucket)),
                bucket_configs::kind.eq(&(kind)),
                bucket_configs::body.eq(&(body)),
            ))
            .on_conflict((bucket_configs::bucket, bucket_configs::kind))
            .do_update()
            .set(bucket_configs::body.eq(diesel::upsert::excluded(bucket_configs::body)))
            .execute(tx)?;
        Ok(())
    })
}

pub(crate) fn load_raw(s3: &S3, bucket: &str, kind: &str) -> Result<Option<String>, AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, bucket)?;
        Ok(bucket_configs::table
            .filter(bucket_configs::bucket.eq(&(bucket)))
            .filter(bucket_configs::kind.eq(&(kind)))
            .select(bucket_configs::body)
            .first::<String>(tx)
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
        diesel::delete(
            bucket_configs::table
                .filter(bucket_configs::bucket.eq(&(bucket)))
                .filter(bucket_configs::kind.eq(&(kind))),
        )
        .execute(tx)?;
        Ok(())
    })
}

/// 404 error for a sub-resource that was never configured.
pub(crate) fn missing(code: &str, message: &str, bucket: &str) -> AwsError {
    AwsError::sender(404, code, message).with("BucketName", bucket)
}
