//! Multipart uploads: parts are staged as files under `.roto/<bucket>/uploads/<id>/` and
//! concatenated into a normal object on completion.

use std::collections::BTreeMap;

use md5::{Digest, Md5};
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;

use crate::blobs::Blobs;
use crate::generated::*;
use crate::service::*;

const MIN_PART_SIZE: i64 = 5 * 1024 * 1024;

fn no_such_upload(upload_id: &str) -> AwsError {
    AwsError::sender(
        404,
        "NoSuchUpload",
        "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
    )
    .with("UploadId", upload_id)
}

fn attrs_to_json(a: &NewObj) -> String {
    serde_json::json!({
        "content_type": a.content_type,
        "metadata": a.metadata,
        "headers": a.headers,
        "tags": a.tags,
        "storage_class": a.storage_class,
        "acl": a.acl,
    })
    .to_string()
}

fn attrs_from_json(s: &str) -> NewObj {
    let v: serde_json::Value = serde_json::from_str(s).unwrap_or_default();
    let map = |k: &str| -> BTreeMap<String, String> {
        v[k].as_object()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    };
    NewObj {
        content_type: v["content_type"].as_str().map(String::from),
        metadata: map("metadata"),
        headers: map("headers"),
        tags: map("tags"),
        storage_class: v["storage_class"].as_str().map(String::from),
        acl: v["acl"].as_str().map(String::from),
    }
}

struct Upload {
    key: String,
    attrs: NewObj,
}

fn load_upload(
    tx: &Transaction,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Upload, AwsError> {
    tx.query_row(
        "SELECT key, attrs FROM uploads WHERE upload_id = ?1 AND bucket = ?2 AND key = ?3",
        params![upload_id, bucket, key],
        |r| {
            Ok(Upload {
                key: r.get(0)?,
                attrs: attrs_from_json(&r.get::<_, String>(1)?),
            })
        },
    )
    .optional()?
    .ok_or_else(|| no_such_upload(upload_id))
}

fn check_part_number(n: i32) -> Result<(), AwsError> {
    if (1..=10_000).contains(&n) {
        Ok(())
    } else {
        Err(
            invalid_argument("Part number must be an integer between 1 and 10000, inclusive")
                .with("ArgumentName", "partNumber")
                .with("ArgumentValue", n.to_string()),
        )
    }
}

pub(crate) fn create(
    s3: &S3,
    _ctx: &RequestContext,
    i: CreateMultipartUploadRequest,
    attrs: NewObj,
) -> Result<CreateMultipartUploadOutput, AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        let upload_id = uuid::Uuid::new_v4().simple().to_string();
        tx.execute(
            "INSERT INTO uploads (upload_id, bucket, key, created_at, attrs) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![upload_id, i.bucket, i.key, now_secs(), attrs_to_json(&attrs)],
        )?;
        Ok(CreateMultipartUploadOutput {
            bucket: Some(i.bucket.clone()),
            key: Some(i.key.clone()),
            upload_id: Some(upload_id),
            server_side_encryption: attrs.headers.get("server_side_encryption").cloned(),
            ..Default::default()
        })
    })
}

fn store_part(
    s3: &S3,
    tx: &Transaction,
    bucket: &str,
    upload_id: &str,
    part: i32,
    data: &[u8],
) -> Result<String, AwsError> {
    let etag = hex::encode(Md5::digest(data));
    let rel = Blobs::part_rel(bucket, upload_id, part);
    s3.blobs.write_rel(&rel, data).map_err(io)?;
    tx.execute(
        "INSERT INTO parts (upload_id, part_number, size, etag, path, last_modified) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (upload_id, part_number) DO UPDATE SET size = excluded.size, etag = excluded.etag,
                                                              path = excluded.path, last_modified = excluded.last_modified",
        params![upload_id, part, data.len() as i64, etag, rel, now_secs()],
    )?;
    Ok(etag)
}

pub(crate) fn upload_part(
    s3: &S3,
    _ctx: &RequestContext,
    i: UploadPartRequest,
) -> Result<UploadPartOutput, AwsError> {
    check_part_number(i.part_number)?;
    let data = i.body.map(|b| b.0).unwrap_or_default();
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        load_upload(tx, &i.bucket, &i.key, &i.upload_id)?;
        let etag = store_part(s3, tx, &i.bucket, &i.upload_id, i.part_number, &data)?;
        Ok(UploadPartOutput {
            e_tag: Some(format!("\"{etag}\"")),
            ..Default::default()
        })
    })
}

pub(crate) fn upload_part_copy(
    s3: &S3,
    _ctx: &RequestContext,
    i: UploadPartCopyRequest,
) -> Result<UploadPartCopyOutput, AwsError> {
    check_part_number(i.part_number)?;
    let (src_bucket, src_key, version) = parse_copy_source(&i.copy_source)?;
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        load_upload(tx, &i.bucket, &i.key, &i.upload_id)?;
        load_bucket(tx, &src_bucket)?;
        let o = find_obj(tx, &src_bucket, &src_key, version.as_deref())?
            .filter(|o| !o.delete_marker)
            .ok_or_else(|| no_such_key(&src_key))?;
        let mut data = s3.read_body(&o)?;
        if let Some(range) = &i.copy_source_range {
            let spec = range.trim().strip_prefix("bytes=").unwrap_or(range.trim());
            let (a, b) = spec.split_once('-').unwrap_or(("", ""));
            let (a, b): (usize, usize) = (
                a.parse().map_err(|_| invalid_argument("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"))?,
                b.parse().map_err(|_| invalid_argument("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"))?,
            );
            if a >= data.len() || b < a {
                return Err(invalid_argument("Range specified is not valid for source object of size: ".to_string() + &data.len().to_string()));
            }
            data = data[a..=b.min(data.len() - 1)].to_vec();
        }
        let etag = store_part(s3, tx, &i.bucket, &i.upload_id, i.part_number, &data)?;
        Ok(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult { e_tag: Some(format!("\"{etag}\"")), last_modified: Some(Timestamp(now_secs())), ..Default::default() }),
            copy_source_version_id: (o.version_id != "null").then(|| o.version_id.clone()),
            ..Default::default()
        })
    })
}

pub(crate) fn complete(
    s3: &S3,
    ctx: &RequestContext,
    i: CompleteMultipartUploadRequest,
) -> Result<CompleteMultipartUploadOutput, AwsError> {
    let requested = i
        .multipart_upload
        .as_ref()
        .map(|m| m.parts.clone())
        .unwrap_or_default();
    if requested.is_empty() {
        return Err(AwsError::sender(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        ));
    }
    s3.db.transaction(|tx| {
        let b = load_bucket(tx, &i.bucket)?;
        let up = load_upload(tx, &i.bucket, &i.key, &i.upload_id)?;
        let mut last = 0;
        let mut data = Vec::new();
        let mut digests = Vec::new();
        let mut layout: Vec<String> = Vec::new();
        for (idx, p) in requested.iter().enumerate() {
            let n = p.part_number.unwrap_or(0);
            if n <= last {
                return Err(AwsError::sender(
                    400,
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
                )
                .with("UploadId", i.upload_id.clone()));
            }
            last = n;
            let row: Option<(i64, String, String)> = tx
                .query_row(
                    "SELECT size, etag, path FROM parts WHERE upload_id = ?1 AND part_number = ?2",
                    params![i.upload_id, n],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let invalid = || {
                AwsError::sender(
                    400,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
                )
                .with("UploadId", i.upload_id.clone())
                .with("PartNumber", n.to_string())
            };
            let (size, etag, path) = row.ok_or_else(invalid)?;
            if let Some(given) = &p.e_tag {
                if given.trim().trim_matches('"') != etag {
                    return Err(invalid().with("ETag", given.clone()));
                }
            }
            if idx + 1 < requested.len() && size < MIN_PART_SIZE {
                return Err(AwsError::sender(
                    400,
                    "EntityTooSmall",
                    "Your proposed upload is smaller than the minimum allowed object size.",
                )
                .with("ETag", etag)
                .with("MinSizeAllowed", MIN_PART_SIZE.to_string())
                .with("PartNumber", n.to_string())
                .with("ProposedSize", size.to_string()));
            }
            layout.push(format!("{n}:{size}"));
            data.extend(s3.blobs.read(&path).map_err(io)?);
            digests.extend(hex::decode(&etag).unwrap_or_default());
        }
        let etag = format!("{}-{}", hex::encode(Md5::digest(&digests)), requested.len());
        let mut attrs = up.attrs.clone();
        attrs.headers.insert("mp_parts".into(), layout.join(","));
        let o = s3.store_object(tx, &b, &up.key, &data, etag.clone(), attrs)?;
        crate::notifications::record(tx, ctx, &b, &up.key, "ObjectCreated:CompleteMultipartUpload", Some(&o))?;
        tx.execute("DELETE FROM uploads WHERE upload_id = ?1", params![i.upload_id])?;
        s3.blobs.remove_dir(&format!(".roto/{}/uploads/{}", i.bucket, i.upload_id)).map_err(io)?;
        Ok(CompleteMultipartUploadOutput {
            bucket: Some(i.bucket.clone()),
            key: Some(i.key.clone()),
            e_tag: Some(format!("\"{etag}\"")),
            location: Some(format!("{}/{}/{}", ctx.base_url, i.bucket, i.key)),
            version_id: (o.version_id != "null").then(|| o.version_id),
            server_side_encryption: up.attrs.headers.get("server_side_encryption").cloned(),
            ..Default::default()
        })
    })
}

pub(crate) fn abort(
    s3: &S3,
    _ctx: &RequestContext,
    i: AbortMultipartUploadRequest,
) -> Result<AbortMultipartUploadOutput, AwsError> {
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        load_upload(tx, &i.bucket, &i.key, &i.upload_id)?;
        tx.execute(
            "DELETE FROM uploads WHERE upload_id = ?1",
            params![i.upload_id],
        )?;
        s3.blobs
            .remove_dir(&format!(".roto/{}/uploads/{}", i.bucket, i.upload_id))
            .map_err(io)?;
        Ok(AbortMultipartUploadOutput::default())
    })
}

pub(crate) fn list_parts(
    s3: &S3,
    _ctx: &RequestContext,
    i: ListPartsRequest,
) -> Result<ListPartsOutput, AwsError> {
    let max = i.max_parts.unwrap_or(1000);
    if max < 0 {
        return Err(invalid_argument(
            "Argument max-parts must be an integer between 0 and 2147483647",
        )
        .with("ArgumentName", "max-parts")
        .with("ArgumentValue", max.to_string()));
    }
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        let up = load_upload(tx, &i.bucket, &i.key, &i.upload_id)?;
        let marker = i.part_number_marker.unwrap_or(0);
        let mut stmt = tx.prepare(
            "SELECT part_number, size, etag, last_modified FROM parts
             WHERE upload_id = ?1 AND part_number > ?2 ORDER BY part_number LIMIT ?3",
        )?;
        let mut parts: Vec<Part> = stmt
            .query_map(params![i.upload_id, marker, max + 1], |r| {
                Ok(Part {
                    part_number: Some(r.get(0)?),
                    size: Some(r.get(1)?),
                    e_tag: Some(format!("\"{}\"", r.get::<_, String>(2)?)),
                    last_modified: Some(Timestamp(r.get(3)?)),
                    ..Default::default()
                })
            })?
            .collect::<Result<_, _>>()?;
        let truncated = parts.len() as i32 > max;
        parts.truncate(max as usize);
        let next = parts.last().and_then(|p| p.part_number);
        let who = Initiator {
            display_name: Some(OWNER_NAME.into()),
            id: Some(OWNER_ID.into()),
        };
        Ok(ListPartsOutput {
            bucket: Some(i.bucket.clone()),
            key: Some(up.key),
            upload_id: Some(i.upload_id.clone()),
            part_number_marker: Some(marker),
            next_part_number_marker: truncated.then_some(next).flatten(),
            max_parts: Some(max),
            is_truncated: Some(truncated),
            storage_class: Some(up.attrs.storage_class.unwrap_or_else(|| "STANDARD".into())),
            owner: Some(Owner {
                display_name: Some(OWNER_NAME.into()),
                id: Some(OWNER_ID.into()),
            }),
            initiator: Some(who),
            parts,
            ..Default::default()
        })
    })
}

pub(crate) fn list_uploads(
    s3: &S3,
    _ctx: &RequestContext,
    i: ListMultipartUploadsRequest,
) -> Result<ListMultipartUploadsOutput, AwsError> {
    let max = i.max_uploads.unwrap_or(1000);
    s3.db.transaction(|tx| {
        load_bucket(tx, &i.bucket)?;
        let prefix = i.prefix.clone().unwrap_or_default();
        let key_marker = i.key_marker.clone().unwrap_or_default();
        let id_marker = i.upload_id_marker.clone().unwrap_or_default();
        let mut stmt = tx.prepare(
            "SELECT upload_id, key, created_at, attrs FROM uploads
             WHERE bucket = ?1 AND substr(key, 1, length(?2)) = ?2
               AND (key > ?3 OR (key = ?3 AND upload_id > ?4))
             ORDER BY key, upload_id",
        )?;
        let rows: Vec<(String, String, i64, String)> = stmt
            .query_map(params![i.bucket, prefix, key_marker, id_marker], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<Result<_, _>>()?;
        let delimiter = i.delimiter.clone().filter(|d| !d.is_empty());
        let mut out = ListMultipartUploadsOutput {
            bucket: Some(i.bucket.clone()),
            prefix: i.prefix.clone(),
            delimiter: i.delimiter.clone(),
            key_marker: i.key_marker.clone(),
            upload_id_marker: i.upload_id_marker.clone(),
            max_uploads: Some(max),
            is_truncated: Some(false),
            ..Default::default()
        };
        let who = Initiator {
            display_name: Some(OWNER_NAME.into()),
            id: Some(OWNER_ID.into()),
        };
        let mut count = 0;
        for (id, key, created, attrs) in rows {
            let rest = &key[prefix.len()..];
            if let Some(d) = &delimiter {
                if let Some(p) = rest.find(d.as_str()) {
                    let cp = format!("{prefix}{}{d}", &rest[..p]);
                    if out
                        .common_prefixes
                        .iter()
                        .any(|c| c.prefix.as_deref() == Some(&cp))
                    {
                        continue;
                    }
                    if count >= max {
                        out.is_truncated = Some(true);
                        break;
                    }
                    count += 1;
                    out.common_prefixes.push(CommonPrefix { prefix: Some(cp) });
                    continue;
                }
            }
            if count >= max {
                out.is_truncated = Some(true);
                break;
            }
            count += 1;
            out.next_key_marker = Some(key.clone());
            out.next_upload_id_marker = Some(id.clone());
            out.uploads.push(MultipartUpload {
                upload_id: Some(id),
                key: Some(key),
                initiated: Some(Timestamp(created)),
                storage_class: Some(
                    attrs_from_json(&attrs)
                        .storage_class
                        .unwrap_or_else(|| "STANDARD".into()),
                ),
                owner: Some(Owner {
                    display_name: Some(OWNER_NAME.into()),
                    id: Some(OWNER_ID.into()),
                }),
                initiator: Some(who.clone()),
                ..Default::default()
            });
        }
        if out.is_truncated != Some(true) {
            out.next_key_marker = None;
            out.next_upload_id_marker = None;
        }
        Ok(out)
    })
}
