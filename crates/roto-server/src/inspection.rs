//! Read-only inspection of initialized resource stores. No arbitrary SQL or file paths.
use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use roto_core::rusqlite::{Connection, OptionalExtension, types::ValueRef};
use roto_core::{AwsError, RawRequest, RequestContext, ids};
use serde_json::{Map, Value, json};

use crate::App;

// Explicit public resource collections; internal migrations and credentials are omitted.
const COLLECTIONS: &[(&str, &[&str])] = &[
    ("cloudformation", &["stacks"]),
    (
        "s3",
        &["buckets", "objects", "bucket_configs", "uploads", "parts"],
    ),
    ("dynamodb", &["tables", "items", "backups", "backup_items"]),
    ("sqs", &["queues", "messages"]),
    ("kinesis", &["streams", "records"]),
    ("lambda", &["functions", "event_source_mappings"]),
    ("events", &["buses", "rules", "targets"]),
    ("sns", &["topics", "subscriptions", "sms_messages"]),
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

/// Peek at all queued messages without receiving them or changing queue state.
pub async fn sqs_messages(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = invalid_account(&query) {
        return response;
    }
    let Some(db) = app.store.existing_db("sqs") else {
        return Json(json!({"messages": {}})).into_response();
    };
    let base_url = format!(
        "http://{}",
        headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("localhost:5070")
    );
    match tokio::task::spawn_blocking(move || db.read(|c| peek_sqs(c, &query, &base_url))).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, e.message),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "SQS inspection failed"),
    }
}

fn invalid_account(query: &HashMap<String, String>) -> Option<Response> {
    if let Some(account) = query.get("account")
        && (account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()))
    {
        return Some(
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "__type": "InvalidAccountID",
                    "message": format!("Account ID must be 12 digits, got: {account}"),
                })),
            )
                .into_response(),
        );
    }
    None
}

pub async fn sns_sms_messages(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Some(response) = invalid_account(&query) {
        return response;
    }
    let Some(db) = app.store.existing_db("sns") else {
        return Json(empty_sms(&query)).into_response();
    };
    match tokio::task::spawn_blocking(move || db.read(|c| peek_sms(c, &query))).await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => error(StatusCode::INTERNAL_SERVER_ERROR, e.message),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "SNS inspection failed"),
    }
}

fn empty_sms(query: &HashMap<String, String>) -> Value {
    let mut value = json!({"sms_messages": {}, "region": query.get("region").map(String::as_str).unwrap_or("us-east-1")});
    if let Some(phone) = query.get("phoneNumber") {
        value["sms_messages"][phone] = json!([]);
    }
    value
}

fn peek_sms(c: &Connection, query: &HashMap<String, String>) -> Result<Value, AwsError> {
    let mut result = empty_sms(query);
    let mut statement = c.prepare("SELECT phone_number, record FROM sms_messages WHERE (?1 IS NULL OR account_id = ?1) AND (?2 IS NULL OR region = ?2) AND (?3 IS NULL OR phone_number = ?3) ORDER BY seq")?;
    let mut rows = statement.query(roto_core::rusqlite::params![
        query.get("account"),
        query.get("region"),
        query.get("phoneNumber")
    ])?;
    while let Some(row) = rows.next()? {
        let phone: String = row.get(0)?;
        let record: Value = serde_json::from_str(&row.get::<_, String>(1)?)
            .map_err(|_| AwsError::internal("Invalid stored SMS record"))?;
        result["sms_messages"]
            .as_object_mut()
            .unwrap()
            .entry(phone)
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap()
            .push(record);
    }
    Ok(result)
}

pub async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    let services: Map<String, Value> = app
        .services
        .keys()
        .map(|service| ((*service).to_owned(), json!("available")))
        .collect();
    Json(json!({"services": services, "version": env!("CARGO_PKG_VERSION")}))
}

fn peek_sqs(
    c: &Connection,
    query: &HashMap<String, String>,
    base_url: &str,
) -> Result<Value, AwsError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let queue_filter = query.get("QueueUrl");
    let queue_ref = queue_filter.map(|url| {
        let path = url
            .split_once("://")
            .map(|(_, rest)| rest.split_once('/').map(|(_, path)| path).unwrap_or(""))
            .unwrap_or(url);
        let path = if url.contains("://") {
            path.split(['?', '#']).next().unwrap_or("")
        } else {
            path
        };
        let mut parts = path.trim_matches('/').rsplit('/');
        let name = parts.next().unwrap_or("");
        (parts.next(), name)
    });
    let mut queues =
        c.prepare("SELECT id, account_id, region, name, attributes FROM queues ORDER BY id")?;
    let mut messages = c.prepare("SELECT seq, message_id, body, md5, attrs, md5_attrs, sent_at, visible_at, receive_count, first_received_at, group_id, dedup_id, trace_header, sender_id FROM messages WHERE queue_id = ?1 ORDER BY seq")?;
    let mut result = json!({"messages": {}});
    let mut rows = queues.query([])?;
    while let Some(q) = rows.next()? {
        let id: i64 = q.get(0)?;
        let account: String = q.get(1)?;
        let region: String = q.get(2)?;
        let name: String = q.get(3)?;
        if query.get("account").is_some_and(|a| a != &account)
            || query.get("region").is_some_and(|r| r != &region)
            || queue_ref.is_some_and(|(a, n)| n != name || a.is_some_and(|a| a != account))
        {
            continue;
        }
        let attrs: Value = serde_json::from_str(&q.get::<_, String>(4)?)
            .map_err(|_| AwsError::internal("Invalid stored queue attributes"))?;
        let fifo = attrs.get("FifoQueue").and_then(Value::as_str) == Some("true");
        let mut rendered = Vec::new();
        let mut rows = messages.query([id])?;
        while let Some(m) = rows.next()? {
            let sent: i64 = m.get(6)?;
            let visible: i64 = m.get(7)?;
            let first: Option<i64> = m.get(9)?;
            let trace: Option<String> = m.get(12)?;
            let mut system =
                json!({"SenderId": m.get::<_, String>(13)?, "SentTimestamp": sent.to_string()});
            if let Some(trace) = trace {
                system["AWSTraceHeader"] = json!(trace);
            }
            let attributes: Value = serde_json::from_str(&m.get::<_, String>(4)?)
                .map_err(|_| AwsError::internal("Invalid stored message attributes"))?;
            rendered.push(json!({
                "MessageId": m.get::<_, String>(1)?, "Body": m.get::<_, String>(2)?,
                "MD5OfBody": m.get::<_, String>(3)?, "MD5OfMessageAttributes": m.get::<_, Option<String>>(5)?,
                "SentTimestamp": sent / 1000, "VisibleAt": visible / 1000, "IsVisible": visible <= now,
                "ReceiveCount": m.get::<_, i64>(8)?, "FirstReceiveTimestamp": first.map(|t| t / 1000),
                "MessageAttributes": attributes, "Attributes": system,
                "MessageGroupId": m.get::<_, Option<String>>(10)?, "MessageDeduplicationId": m.get::<_, Option<String>>(11)?,
                "SequenceNumber": if fifo { Some(format!("{:018}", m.get::<_, i64>(0)?)) } else { None },
            }));
        }
        let url = queue_filter
            .cloned()
            .unwrap_or_else(|| format!("{base_url}/{account}/{name}"));
        let accounts = result["messages"].as_object_mut().unwrap();
        let regions = accounts
            .entry(account)
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .unwrap();
        let queues = regions
            .entry(region)
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .unwrap();
        queues.insert(url, json!(rendered));
    }
    Ok(result)
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

pub async fn dynamodb_query(State(app): State<Arc<App>>, Json(input): Json<Value>) -> Response {
    let Some(table_id) = input
        .get("table_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return error(StatusCode::BAD_REQUEST, "table_id is required");
    };
    let partition_value = input
        .get("partition_value")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let Some(db) = app.store.existing_db("dynamodb") else {
        return error(StatusCode::NOT_FOUND, "DynamoDB is not initialized");
    };
    match tokio::task::spawn_blocking(move || {
        db.read(|c| query_dynamodb_items(c, &table_id, &partition_value))
    })
    .await
    {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => error(
            StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.message,
        ),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "DynamoDB query failed"),
    }
}

fn query_dynamodb_items(
    c: &Connection,
    table_id: &str,
    partition_value: &str,
) -> Result<Value, AwsError> {
    let table = c
        .query_row(
            "SELECT name, key_schema, attr_defs FROM tables WHERE table_id = ?1",
            [table_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| AwsError::sender(404, "ResourceNotFoundException", "Table not found"))?;
    let (partition_key, key_type) = if partition_value.is_empty() {
        (None, None)
    } else {
        let key_schema: Value = serde_json::from_str(&table.1)
            .map_err(|_| AwsError::internal("Invalid DynamoDB table key schema"))?;
        let key = key_schema
            .as_array()
            .and_then(|keys| {
                keys.iter()
                    .find(|key| key.get("KeyType").and_then(Value::as_str) == Some("HASH"))
            })
            .and_then(|key| key.get("AttributeName").and_then(Value::as_str))
            .ok_or_else(|| AwsError::internal("DynamoDB table has no partition key"))?;
        let attr_defs: Value = serde_json::from_str(&table.2)
            .map_err(|_| AwsError::internal("Invalid DynamoDB table attribute definitions"))?;
        let key_type = attr_defs
            .as_array()
            .and_then(|defs| {
                defs.iter()
                    .find(|def| def.get("AttributeName").and_then(Value::as_str) == Some(key))
            })
            .and_then(|def| def.get("AttributeType").and_then(Value::as_str))
            .ok_or_else(|| AwsError::internal("DynamoDB partition key type is missing"))?;
        if !matches!(key_type, "S" | "N" | "B") {
            return Err(AwsError::internal(
                "Unsupported DynamoDB partition key type",
            ));
        }
        match key_type {
            "S" => {}
            "N" if !partition_value.parse::<f64>().is_ok_and(f64::is_finite) => {
                return Err(AwsError::sender(
                    400,
                    "ValidationException",
                    "Partition key value must be a number",
                ));
            }
            "B" if roto_protocol::base64::decode(partition_value)
                .is_none_or(|value| value.is_empty()) =>
            {
                return Err(AwsError::sender(
                    400,
                    "ValidationException",
                    "Partition key value must be non-empty base64",
                ));
            }
            _ => {}
        }
        (Some(key.to_owned()), Some(key_type.to_owned()))
    };
    let found = if let (Some(key), Some(key_type)) = (partition_key.as_deref(), key_type.as_deref())
    {
        let key_json = serde_json::to_string(key)
            .map_err(|_| AwsError::internal("Unable to encode DynamoDB partition key"))?;
        let path = format!("$.{key_json}.{key_type}");
        let value_clause = if key_type == "N" {
            "CAST(json_extract(item, ?2) AS NUMERIC) = CAST(?3 AS NUMERIC)"
        } else {
            "json_extract(item, ?2) = ?3"
        };
        let mut statement = c.prepare(&format!(
            "SELECT item FROM items WHERE table_id = ?1 AND {value_clause} ORDER BY hk, rk LIMIT 101"
        ))?;
        statement
            .query_map([table_id, &path, partition_value], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let mut statement =
            c.prepare("SELECT item FROM items WHERE table_id = ?1 ORDER BY hk, rk LIMIT 101")?;
        statement
            .query_map([table_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    let has_more = found.len() > 100;
    let items = found
        .iter()
        .take(100)
        .map(|item| {
            serde_json::from_str::<Value>(item)
                .map_err(|_| AwsError::internal("Invalid stored DynamoDB item"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "tableName": table.0,
        "partitionKey": partition_key,
        "count": items.len(),
        "hasMore": has_more,
        "items": items,
    }))
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
                        (table == "stacks" && name == "body"
                            || table == "sms_messages" && name == "record"
                            || matches!(
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
                            ))
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
                    !["content-type", "content-disposition", "content-encoding"]
                        .iter()
                        .any(|h| name.eq_ignore_ascii_case(h))
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
