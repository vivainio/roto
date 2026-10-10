//! Read-only inspection of initialized resource stores. No arbitrary SQL or file paths.
use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use roto_core::rusqlite::{Connection, types::ValueRef};
use roto_core::{AwsError, RawRequest, RequestContext, ids};
use serde_json::{Map, Value, json};

use crate::App;

// Explicit public resource collections; internal migrations and credentials are omitted.
const COLLECTIONS: &[(&str, &[&str])] = &[
    (
        "s3",
        &["buckets", "objects", "bucket_configs", "uploads", "parts"],
    ),
    ("dynamodb", &["tables", "items", "backups", "backup_items"]),
    ("sqs", &["queues", "messages"]),
    ("lambda", &["functions", "event_source_mappings"]),
    ("events", &["buses", "rules", "targets"]),
    ("sns", &["topics", "subscriptions"]),
    ("ssm", &["parameters", "resource_tags"]),
    ("secretsmanager", &["secrets", "secret_versions"]),
    (
        "iam",
        &[
            "users",
            "groups",
            "roles",
            "policies",
            "policy_versions",
            "inline_policies",
            "attachments",
            "group_members",
            "instance_profiles",
            "profile_roles",
            "tags",
            "account_aliases",
        ],
    ),
];

pub async fn catalog() -> Json<Value> {
    Json(
        json!({"services": COLLECTIONS.iter().map(|(service, tables)| json!({"service":service,"collections":tables})).collect::<Vec<_>>() }),
    )
}

fn error(status: StatusCode, message: impl ToString) -> Response {
    (status, Json(json!({"message":message.to_string()}))).into_response()
}

pub async fn records(
    State(app): State<Arc<App>>,
    Path((service, table)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !COLLECTIONS
        .iter()
        .any(|(s, tables)| *s == service && tables.contains(&table.as_str()))
    {
        return error(StatusCode::NOT_FOUND, "Unknown resource collection");
    }
    let Some(db) = app.store.existing_db(&service) else {
        return error(StatusCode::NOT_FOUND, "Service is not initialized");
    };
    let offset = match query.get("offset").map(|s| s.parse::<u32>()).transpose() {
        Ok(offset) => offset.unwrap_or(0),
        Err(_) => return error(StatusCode::BAD_REQUEST, "Invalid offset"),
    };
    let filter = query.get("field").cloned().zip(query.get("value").cloned());
    if query.contains_key("field") != query.contains_key("value") {
        return error(StatusCode::BAD_REQUEST, "Both field and value are required");
    }
    match tokio::task::spawn_blocking(move || db.read(|c| page(c, &table, offset, filter))).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => error(
            StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.message,
        ),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "Inspection task failed"),
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn page(
    c: &Connection,
    table: &str,
    offset: u32,
    filter: Option<(String, String)>,
) -> Result<Value, AwsError> {
    let mut schema = c.prepare(&format!("PRAGMA table_info({})", quote(table)))?;
    let columns = schema
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, u32>(5)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    let (condition, values) = match filter {
        Some((field, value)) => {
            if !columns.iter().any(|(name, _)| name == &field) {
                return Err(AwsError::sender(
                    400,
                    "InvalidArgument",
                    "Unknown filter field",
                ));
            }
            (format!(" WHERE {} = ?1", quote(&field)), vec![value])
        }
        None => (String::new(), vec![]),
    };
    let mut keys: Vec<_> = columns.iter().filter(|(_, pk)| *pk > 0).collect();
    keys.sort_by_key(|(_, pk)| *pk);
    let order = keys
        .iter()
        .map(|(name, _)| quote(name))
        .collect::<Vec<_>>()
        .join(", ");
    let order = if order.is_empty() {
        "rowid".into()
    } else {
        order
    };
    let total: i64 = c.query_row(
        &format!("SELECT COUNT(*) FROM {}{condition}", quote(table)),
        roto_core::rusqlite::params_from_iter(&values),
        |r| r.get(0),
    )?;
    let mut stmt = c.prepare(&format!(
        "SELECT * FROM {}{condition} ORDER BY {order} LIMIT 50 OFFSET {offset}",
        quote(table)
    ))?;
    let names = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map(roto_core::rusqlite::params_from_iter(&values), |r| {
            let mut row = Map::new();
            for (i, name) in names.iter().enumerate() {
                let value = match r.get_ref(i)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(n) => json!(n),
                    ValueRef::Real(n) => json!(n),
                    ValueRef::Text(bytes) => {
                        let text = String::from_utf8_lossy(bytes);
                        // Unpack JSON documents for readable resource details.
                        matches!(
                            name.as_str(),
                            "config"
                                | "attributes"
                                | "tags"
                                | "item"
                                | "metadata"
                                | "headers"
                                | "acl"
                                | "code"
                                | "policy"
                                | "document"
                                | "assume_role_policy"
                                | "key_schema"
                                | "attr_defs"
                                | "gsis"
                                | "lsis"
                                | "throughput"
                                | "stream_spec"
                                | "sse"
                                | "meta"
                                | "attrs"
                                | "labels"
                                | "policies"
                                | "stages"
                                | "rotation_rules"
                        )
                        .then(|| serde_json::from_str::<Value>(&text).ok())
                        .flatten()
                        .filter(|v| v.is_object() || v.is_array())
                        .unwrap_or_else(|| json!(text))
                    }
                    ValueRef::Blob(bytes) => {
                        json!({"base64":roto_protocol::base64::encode(bytes),"bytes":bytes.len()})
                    }
                };
                // Object storage paths are implementation details, not resource data.
                if !(name == "path" && matches!(table, "objects" | "parts")) {
                    row.insert(name.clone(), value);
                }
            }
            Ok(Value::Object(row))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"records":rows,"total":total,"offset":offset,"limit":50}))
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

pub async fn object(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let (Some(bucket), Some(key)) = (query.get("bucket"), query.get("key")) else {
        return error(StatusCode::BAD_REQUEST, "Bucket and key are required");
    };
    let Some(handler) = app.services.get("s3").cloned() else {
        return error(StatusCode::NOT_FOUND, "S3 is not initialized");
    };
    let ctx = RequestContext {
        account_id: app.account_id.clone(),
        region: "us-east-1".into(),
        access_key: None,
        request_id: ids::request_id(),
        base_url: "http://localhost:5070".into(),
    };
    let mut req = RawRequest {
        method: "GET".into(),
        path: format!("/{}/{}", encode(bucket), encode(key)),
        query: query
            .get("version")
            .map(|v| format!("versionId={}", encode(v)))
            .unwrap_or_default(),
        ..Default::default()
    };
    let preview = query.get("preview").is_some_and(|v| v == "true");
    if preview {
        req.headers.push(("range".into(), "bytes=0-65535".into()));
    }
    match tokio::task::spawn_blocking(move || {
        let raw = handler.handle(&ctx, &req)?;
        if preview && raw.status == 416 {
            // The fixed range starts at zero, so an unsatisfiable range means an empty object.
            req.headers.clear();
            handler.handle(&ctx, &req)
        } else {
            Ok(raw)
        }
    })
    .await
    {
        Ok(Ok(mut raw)) => {
            if (200..300).contains(&raw.status) {
                raw.headers.retain(|(name, _)| {
                    !matches!(
                        name.as_str(),
                        "content-type" | "content-disposition" | "content-encoding"
                    )
                });
                // Never execute uploaded HTML/SVG on the inspection origin.
                raw.headers
                    .push(("content-type".into(), "application/octet-stream".into()));
                raw.headers
                    .push(("content-disposition".into(), "attachment".into()));
                raw.headers
                    .push(("x-content-type-options".into(), "nosniff".into()));
            }
            crate::into_response(raw)
        }
        Ok(Err(e)) => error(
            StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.message,
        ),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "Object read failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browse_pages_filters_and_json_without_mutating_resources() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE items (name TEXT PRIMARY KEY, item TEXT, data BLOB) WITHOUT ROWID;",
        )
        .unwrap();
        for i in 0..55 {
            c.execute(
                "INSERT INTO items VALUES (?1, ?2, ?3)",
                roto_core::rusqlite::params![
                    format!("item-{i:02}"),
                    r#"{"hello":"world"}"#,
                    &[0u8, 255][..]
                ],
            )
            .unwrap();
        }
        let first = page(&c, "items", 0, None).unwrap();
        assert_eq!(first["total"], 55);
        assert_eq!(first["records"].as_array().unwrap().len(), 50);
        assert_eq!(first["records"][0]["item"]["hello"], "world");
        assert_eq!(first["records"][0]["data"]["base64"], "AP8=");
        assert_eq!(
            page(&c, "items", 50, None).unwrap()["records"]
                .as_array()
                .unwrap()
                .len(),
            5
        );
        assert_eq!(
            page(&c, "items", 0, Some(("name".into(), "item-01".into()))).unwrap()["total"],
            1
        );
        assert_eq!(
            page(&c, "items", 0, Some(("name".into(), "' OR 1=1 --".into()))).unwrap()["total"],
            0
        );
        assert!(
            page(
                &c,
                "items",
                0,
                Some(("name; DROP TABLE items".into(), "x".into()))
            )
            .is_err()
        );
        assert_eq!(page(&c, "items", 0, None).unwrap()["total"], 55);
    }

    #[test]
    fn object_names_are_encoded_as_data() {
        assert_eq!(
            encode("folder/a +%?#ü.txt"),
            "folder%2Fa%20%2B%25%3F%23%C3%BC.txt"
        );
    }

    #[test]
    fn json_looking_names_remain_strings() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE objects (key TEXT PRIMARY KEY); INSERT INTO objects VALUES ('{}');",
        )
        .unwrap();
        assert_eq!(
            page(&c, "objects", 0, None).unwrap()["records"][0]["key"],
            "{}"
        );
    }
}
