use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{FromJson, ToJson};

use crate::MIGRATIONS;
use crate::generated::*;

const NO_QUEUE: &str = "AWS.SimpleQueueService.NonExistentQueue";
const DEFAULTS: &[(&str, &str)] = &[
    ("VisibilityTimeout", "30"),
    ("MaximumMessageSize", "1048576"),
    ("MessageRetentionPeriod", "345600"),
    ("DelaySeconds", "0"),
    ("ReceiveMessageWaitTimeSeconds", "0"),
];
/// name -> inclusive range for integer attributes.
const RANGES: &[(&str, i64, i64)] = &[
    ("VisibilityTimeout", 0, 43_200),
    ("MaximumMessageSize", 1024, 1_048_576),
    ("MessageRetentionPeriod", 60, 1_209_600),
    ("DelaySeconds", 0, 900),
    ("ReceiveMessageWaitTimeSeconds", 0, 20),
    ("KmsDataKeyReusePeriodSeconds", 60, 86_400),
];
const OTHER_ATTRS: &[&str] = &[
    "Policy",
    "RedrivePolicy",
    "RedriveAllowPolicy",
    "KmsMasterKeyId",
    "SqsManagedSseEnabled",
    "FifoQueue",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
];

pub struct Sqs {
    db: Arc<Db>,
}

impl Sqs {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("sqs", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM queues", [])?;
            Ok(())
        })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn err(code: &str, message: impl Into<String>) -> AwsError {
    AwsError::sender(400, code, message)
}

fn invalid_value(message: impl Into<String>) -> AwsError {
    err("InvalidParameterValue", message)
}

fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn json_string<T: ToJson>(v: &T) -> String {
    v.to_json().to_string()
}

fn from_json_string<T: FromJson + Default>(s: &str) -> T {
    serde_json::from_str(s)
        .ok()
        .and_then(|v| T::from_json(&v, "").ok())
        .unwrap_or_default()
}

struct Queue {
    id: i64,
    account: String,
    region: String,
    name: String,
    attrs: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    created_at: i64,
    modified_at: i64,
}

impl Queue {
    fn arn(&self) -> String {
        format!("arn:aws:sqs:{}:{}:{}", self.region, self.account, self.name)
    }

    fn is_fifo(&self) -> bool {
        self.name.ends_with(".fifo")
    }

    fn url(&self, ctx: &RequestContext) -> String {
        format!("{}/{}/{}", ctx.base_url, self.account, self.name)
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.attrs
            .get(key)
            .map(String::as_str)
            .or_else(|| DEFAULTS.iter().find(|(k, _)| *k == key).map(|(_, v)| *v))
    }

    fn int(&self, key: &str) -> i64 {
        self.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// `(dead-letter queue name, maxReceiveCount)` from the redrive policy, if any.
    fn redrive(&self) -> Option<(String, i64)> {
        let policy: serde_json::Value =
            serde_json::from_str(self.attrs.get("RedrivePolicy")?).ok()?;
        let arn = policy.get("deadLetterTargetArn")?.as_str()?;
        let max = match policy.get("maxReceiveCount")? {
            serde_json::Value::String(s) => s.parse().ok()?,
            v => v.as_i64()?,
        };
        Some((arn.rsplit(':').next()?.to_string(), max))
    }
}

fn load_queue(
    tx: &Transaction,
    account: &str,
    region: &str,
    name: &str,
) -> Result<Option<Queue>, AwsError> {
    Ok(tx
        .query_row(
            "SELECT id, attributes, tags, created_at, modified_at FROM queues
             WHERE account_id = ?1 AND region = ?2 AND name = ?3",
            params![account, region, name],
            |r| {
                Ok(Queue {
                    id: r.get(0)?,
                    account: account.to_string(),
                    region: region.to_string(),
                    name: name.to_string(),
                    attrs: from_json_string(&r.get::<_, String>(1)?),
                    tags: from_json_string(&r.get::<_, String>(2)?),
                    created_at: r.get(3)?,
                    modified_at: r.get(4)?,
                })
            },
        )
        .optional()?)
}

fn queue_from_url(tx: &Transaction, ctx: &RequestContext, url: &str) -> Result<Queue, AwsError> {
    let mut parts = url.trim_end_matches('/').rsplit('/');
    let name = parts.next().unwrap_or_default();
    let account = parts
        .next()
        .filter(|a| a.len() == 12 && a.bytes().all(|b| b.is_ascii_digit()))
        .unwrap_or(&ctx.account_id);
    load_queue(tx, account, &ctx.region, name)?
        .ok_or_else(|| err(NO_QUEUE, "The specified queue does not exist."))
}

fn validate_attributes(attrs: &BTreeMap<String, String>) -> Result<(), AwsError> {
    for (k, v) in attrs {
        if let Some((_, lo, hi)) = RANGES.iter().find(|(n, ..)| n == k) {
            match v.parse::<i64>() {
                Ok(n) if (*lo..=*hi).contains(&n) => {}
                _ => {
                    return Err(invalid_value(format!(
                        "Invalid value for the parameter {k}. Reason: {k} must be an integer between {lo} and {hi}."
                    )));
                }
            }
        } else if !OTHER_ATTRS.contains(&k.as_str()) {
            return Err(err(
                "InvalidAttributeName",
                format!("Unknown Attribute {k}."),
            ));
        }
    }
    Ok(())
}

fn expire(tx: &Transaction, q: &Queue, now: i64) -> Result<(), AwsError> {
    tx.execute(
        "DELETE FROM messages WHERE queue_id = ?1 AND sent_at < ?2",
        params![q.id, now - q.int("MessageRetentionPeriod") * 1000],
    )?;
    Ok(())
}

fn queue_attributes(tx: &Transaction, q: &Queue) -> Result<BTreeMap<String, String>, AwsError> {
    let now = now_ms();
    expire(tx, q, now)?;
    let count = |sql: &str| -> Result<String, AwsError> {
        Ok(tx
            .query_row(sql, params![q.id, now], |r| r.get::<_, i64>(0))?
            .to_string())
    };
    let mut out: BTreeMap<String, String> = DEFAULTS
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    out.extend(q.attrs.clone());
    if q.is_fifo() {
        out.entry("FifoQueue".into()).or_insert("true".into());
        out.entry("ContentBasedDeduplication".into())
            .or_insert("false".into());
        out.entry("DeduplicationScope".into())
            .or_insert("queue".into());
        out.entry("FifoThroughputLimit".into())
            .or_insert("perQueue".into());
    }
    out.insert("QueueArn".into(), q.arn());
    out.insert("CreatedTimestamp".into(), q.created_at.to_string());
    out.insert("LastModifiedTimestamp".into(), q.modified_at.to_string());
    out.insert(
        "ApproximateNumberOfMessages".into(),
        count("SELECT COUNT(*) FROM messages WHERE queue_id = ?1 AND visible_at <= ?2")?,
    );
    out.insert(
        "ApproximateNumberOfMessagesNotVisible".into(),
        count("SELECT COUNT(*) FROM messages WHERE queue_id = ?1 AND visible_at > ?2 AND receive_count > 0")?,
    );
    out.insert(
        "ApproximateNumberOfMessagesDelayed".into(),
        count("SELECT COUNT(*) FROM messages WHERE queue_id = ?1 AND visible_at > ?2 AND receive_count = 0")?,
    );
    Ok(out)
}

/// AWS's canonical digest of message attributes (name-sorted, length-prefixed fields).
pub(crate) fn md5_of_attributes(attrs: &BTreeMap<String, MessageAttributeValue>) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    let mut buf = Vec::new();
    fn put(buf: &mut Vec<u8>, bytes: &[u8]) {
        buf.extend((bytes.len() as u32).to_be_bytes());
        buf.extend(bytes);
    }
    for (name, v) in attrs {
        put(&mut buf, name.as_bytes());
        put(&mut buf, v.data_type.as_bytes());
        if v.data_type.starts_with("Binary") {
            let data = v
                .binary_value
                .as_ref()
                .map(|b| b.0.clone())
                .unwrap_or_default();
            buf.push(2);
            put(&mut buf, &data);
        } else {
            let data = v.string_value.clone().unwrap_or_default();
            buf.push(1);
            put(&mut buf, data.as_bytes());
        }
    }
    Some(md5_hex(&buf))
}

struct NewMessage {
    body: String,
    delay: Option<i32>,
    attrs: BTreeMap<String, MessageAttributeValue>,
    group: Option<String>,
    dedup: Option<String>,
}

struct Sent {
    message_id: String,
    md5: String,
    md5_attrs: Option<String>,
    sequence: Option<String>,
}

fn put_message(
    tx: &Transaction,
    ctx: &RequestContext,
    q: &Queue,
    m: NewMessage,
) -> Result<Sent, AwsError> {
    let max = q.int("MaximumMessageSize");
    if m.body.len() as i64 > max {
        return Err(invalid_value(format!(
            "One or more parameters are invalid. Reason: Message must be shorter than {max} bytes."
        )));
    }
    let md5 = md5_hex(m.body.as_bytes());
    let md5_attrs = md5_of_attributes(&m.attrs);
    let now = now_ms();

    let mut dedup = None;
    if q.is_fifo() {
        if m.group.is_none() {
            return Err(AwsError::missing_parameter("MessageGroupId"));
        }
        dedup = m
            .dedup
            .clone()
            .or_else(|| (q.get("ContentBasedDeduplication") == Some("true")).then(|| md5.clone()));
        let Some(d) = &dedup else {
            return Err(invalid_value(
                "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
            ));
        };
        // 5 minute deduplication window.
        let existing: Option<(String, i64)> = tx
            .query_row(
                "SELECT message_id, seq FROM messages WHERE queue_id = ?1 AND dedup_id = ?2 AND sent_at > ?3",
                params![q.id, d, now - 300_000],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((message_id, seq)) = existing {
            return Ok(Sent {
                message_id,
                md5,
                md5_attrs,
                sequence: Some(format!("{seq:018}")),
            });
        }
    }

    let delay = i64::from(m.delay.unwrap_or(q.int("DelaySeconds") as i32));
    if !(0..=900).contains(&delay) {
        return Err(invalid_value(format!(
            "Value {delay} for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and <= 900."
        )));
    }
    let message_id = new_id();
    tx.execute(
        "INSERT INTO messages (queue_id, message_id, body, md5, attrs, md5_attrs, sent_at, visible_at,
                               group_id, dedup_id, sender_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            q.id,
            message_id,
            m.body,
            md5,
            json_string(&m.attrs),
            md5_attrs,
            now,
            now + delay * 1000,
            m.group,
            dedup,
            ctx.account_id
        ],
    )?;
    let sequence = q
        .is_fifo()
        .then(|| format!("{:018}", tx.last_insert_rowid()));
    Ok(Sent {
        message_id,
        md5,
        md5_attrs,
        sequence,
    })
}

struct Row {
    seq: i64,
    message_id: String,
    body: String,
    md5: String,
    attrs: String,
    md5_attrs: Option<String>,
    sent_at: i64,
    receive_count: i64,
    first_received_at: Option<i64>,
    group_id: Option<String>,
    dedup_id: Option<String>,
    sender_id: String,
}

fn receive_once(
    tx: &Transaction,
    q: &Queue,
    max: usize,
    visibility: i64,
    want_attrs: &BTreeSet<String>,
    want_message_attrs: &[String],
) -> Result<Vec<Message>, AwsError> {
    let now = now_ms();
    expire(tx, q, now)?;
    let rows: Vec<Row> = {
        let mut stmt = tx.prepare(
            "SELECT seq, message_id, body, md5, attrs, md5_attrs, sent_at, receive_count, first_received_at,
                    group_id, dedup_id, sender_id
             FROM messages m
             WHERE queue_id = ?1 AND visible_at <= ?2
               AND (group_id IS NULL OR NOT EXISTS (
                     SELECT 1 FROM messages o WHERE o.queue_id = m.queue_id AND o.group_id = m.group_id
                       AND o.visible_at > ?2 AND o.receive_count > 0))
             ORDER BY seq LIMIT 200",
        )?;
        stmt.query_map(params![q.id, now], |r| {
            Ok(Row {
                seq: r.get(0)?,
                message_id: r.get(1)?,
                body: r.get(2)?,
                md5: r.get(3)?,
                attrs: r.get(4)?,
                md5_attrs: r.get(5)?,
                sent_at: r.get(6)?,
                receive_count: r.get(7)?,
                first_received_at: r.get(8)?,
                group_id: r.get(9)?,
                dedup_id: r.get(10)?,
                sender_id: r.get(11)?,
            })
        })?
        .collect::<Result<_, _>>()?
    };

    let redrive = q.redrive();
    let mut out = Vec::new();
    for row in rows {
        if out.len() >= max {
            break;
        }
        if let Some((dlq_name, max_receives)) = &redrive {
            if row.receive_count >= *max_receives {
                if let Some(dlq) = load_queue(tx, &q.account, &q.region, dlq_name)? {
                    tx.execute(
                        "INSERT INTO messages (queue_id, message_id, body, md5, attrs, md5_attrs, sent_at, visible_at,
                                               group_id, dedup_id, sender_id)
                         SELECT ?1, message_id, body, md5, attrs, md5_attrs, ?2, ?2, group_id, NULL, sender_id
                         FROM messages WHERE seq = ?3",
                        params![dlq.id, now, row.seq],
                    )?;
                }
                tx.execute("DELETE FROM messages WHERE seq = ?1", params![row.seq])?;
                continue;
            }
        }
        let handle = format!("{}{}", new_id().replace('-', ""), new_id().replace('-', ""));
        let first = row.first_received_at.unwrap_or(now);
        tx.execute(
            "UPDATE messages SET receipt_handle = ?1, visible_at = ?2, receive_count = receive_count + 1,
                                 first_received_at = ?3 WHERE seq = ?4",
            params![handle, now + visibility * 1000, first, row.seq],
        )?;

        let all = want_attrs.contains("All");
        let mut attributes = BTreeMap::new();
        let mut add = |k: &str, v: String| {
            if all || want_attrs.contains(k) {
                attributes.insert(k.to_string(), v);
            }
        };
        add("SenderId", row.sender_id.clone());
        add("SentTimestamp", row.sent_at.to_string());
        add(
            "ApproximateReceiveCount",
            (row.receive_count + 1).to_string(),
        );
        add("ApproximateFirstReceiveTimestamp", first.to_string());
        if q.is_fifo() {
            if let Some(g) = &row.group_id {
                add("MessageGroupId", g.clone());
            }
            if let Some(d) = &row.dedup_id {
                add("MessageDeduplicationId", d.clone());
            }
            add("SequenceNumber", format!("{:018}", row.seq));
        }

        let stored: BTreeMap<String, MessageAttributeValue> = from_json_string(&row.attrs);
        let message_attributes: BTreeMap<_, _> = stored
            .into_iter()
            .filter(|(name, _)| {
                want_message_attrs.iter().any(|w| {
                    w == "All"
                        || w == ".*"
                        || w == name
                        || w.strip_suffix(".*").is_some_and(|p| name.starts_with(p))
                })
            })
            .collect();
        out.push(Message {
            attributes,
            body: Some(row.body),
            md5_of_body: Some(row.md5),
            md5_of_message_attributes: if message_attributes.is_empty() {
                None
            } else {
                row.md5_attrs
            },
            message_attributes,
            message_id: Some(row.message_id),
            receipt_handle: Some(handle),
        });
    }
    Ok(out)
}

fn check_batch(op: &str, ids: &[&str]) -> Result<(), AwsError> {
    const P: &str = "AWS.SimpleQueueService.";
    if ids.is_empty() {
        return Err(err(
            &format!("{P}EmptyBatchRequest"),
            format!("There should be at least one {op}RequestEntry in the request."),
        ));
    }
    if ids.len() > 10 {
        return Err(err(
            &format!("{P}TooManyEntriesInBatchRequest"),
            format!(
                "Maximum number of entries per request are 10. You have sent {}.",
                ids.len()
            ),
        ));
    }
    let mut seen = BTreeSet::new();
    for id in ids {
        if id.is_empty()
            || id.len() > 80
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(err(
                &format!("{P}InvalidBatchEntryId"),
                "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
            ));
        }
        if !seen.insert(*id) {
            return Err(err(
                &format!("{P}BatchEntryIdsNotDistinct"),
                format!("Id {id} repeated."),
            ));
        }
    }
    Ok(())
}

fn validate_visibility(v: i32) -> Result<(), AwsError> {
    if (0..=43_200).contains(&v) {
        Ok(())
    } else {
        Err(invalid_value(format!(
            "Value {v} for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200, if provided."
        )))
    }
}

impl Service for Sqs {
    fn create_queue(
        &self,
        ctx: &RequestContext,
        input: CreateQueueRequest,
    ) -> Result<CreateQueueResult, AwsError> {
        let name = &input.queue_name;
        let base = name.strip_suffix(".fifo").unwrap_or(name);
        if base.is_empty()
            || base.len() > 80
            || !base
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(invalid_value(
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
            ));
        }
        validate_attributes(&input.attributes)?;
        let fifo_attr = input.attributes.get("FifoQueue").map(String::as_str) == Some("true");
        if fifo_attr != name.ends_with(".fifo") {
            return Err(invalid_value(
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.",
            ));
        }
        self.db.transaction(|tx| {
            if let Some(q) = load_queue(tx, &ctx.account_id, &ctx.region, name)? {
                for (k, v) in &input.attributes {
                    if q.get(k) != Some(v.as_str()) {
                        return Err(err(
                            "QueueAlreadyExists",
                            format!("A queue already exists with the same name and a different value for attribute {k}"),
                        ));
                    }
                }
                return Ok(CreateQueueResult { queue_url: Some(q.url(ctx)) });
            }
            let now = now_ms() / 1000;
            tx.execute(
                "INSERT INTO queues (account_id, region, name, attributes, tags, created_at, modified_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![ctx.account_id, ctx.region, name, json_string(&input.attributes), json_string(&input.tags), now],
            )?;
            Ok(CreateQueueResult { queue_url: Some(format!("{}/{}/{}", ctx.base_url, ctx.account_id, name)) })
        })
    }

    fn delete_queue(
        &self,
        ctx: &RequestContext,
        input: DeleteQueueRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            tx.execute("DELETE FROM queues WHERE id = ?1", params![q.id])?;
            Ok(())
        })
    }

    fn get_queue_url(
        &self,
        ctx: &RequestContext,
        input: GetQueueUrlRequest,
    ) -> Result<GetQueueUrlResult, AwsError> {
        self.db.transaction(|tx| {
            let account = input
                .queue_owner_aws_account_id
                .as_deref()
                .unwrap_or(&ctx.account_id);
            let q = load_queue(tx, account, &ctx.region, &input.queue_name)?
                .ok_or_else(|| err(NO_QUEUE, "The specified queue does not exist."))?;
            Ok(GetQueueUrlResult {
                queue_url: Some(q.url(ctx)),
            })
        })
    }

    fn list_queues(
        &self,
        ctx: &RequestContext,
        input: ListQueuesRequest,
    ) -> Result<ListQueuesResult, AwsError> {
        let limit = input.max_results.unwrap_or(1000);
        if !(1..=1000).contains(&limit) {
            return Err(invalid_value(
                "Value for parameter MaxResults is invalid. Reason: MaxResults must be an integer between 1 and 1000.",
            ));
        }
        self.db.transaction(|tx| {
            let prefix = input.queue_name_prefix.clone().unwrap_or_default();
            let after = input.next_token.clone().unwrap_or_default();
            let mut stmt = tx.prepare(
                "SELECT name FROM queues WHERE account_id = ?1 AND region = ?2 AND name > ?3
                   AND substr(name, 1, length(?4)) = ?4 ORDER BY name LIMIT ?5",
            )?;
            let mut names: Vec<String> = stmt
                .query_map(
                    params![ctx.account_id, ctx.region, after, prefix, limit + 1],
                    |r| r.get(0),
                )?
                .collect::<Result<_, _>>()?;
            let next_token = (names.len() as i32 > limit).then(|| {
                names.truncate(limit as usize);
                names.last().cloned().unwrap_or_default()
            });
            Ok(ListQueuesResult {
                queue_urls: names
                    .iter()
                    .map(|n| format!("{}/{}/{}", ctx.base_url, ctx.account_id, n))
                    .collect(),
                next_token,
            })
        })
    }

    fn get_queue_attributes(
        &self,
        ctx: &RequestContext,
        input: GetQueueAttributesRequest,
    ) -> Result<GetQueueAttributesResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            let all = queue_attributes(tx, &q)?;
            let attributes = if input.attribute_names.is_empty() {
                BTreeMap::new()
            } else if input.attribute_names.iter().any(|n| n == "All") {
                all
            } else {
                let mut out = BTreeMap::new();
                for n in &input.attribute_names {
                    let known = RANGES.iter().any(|(k, ..)| k == n)
                        || OTHER_ATTRS.contains(&n.as_str())
                        || n.starts_with("Approximate")
                        || matches!(
                            n.as_str(),
                            "QueueArn" | "CreatedTimestamp" | "LastModifiedTimestamp"
                        );
                    if !known {
                        return Err(err(
                            "InvalidAttributeName",
                            format!("Unknown Attribute {n}."),
                        ));
                    }
                    if let Some(v) = all.get(n) {
                        out.insert(n.clone(), v.clone());
                    }
                }
                out
            };
            Ok(GetQueueAttributesResult { attributes })
        })
    }

    fn set_queue_attributes(
        &self,
        ctx: &RequestContext,
        input: SetQueueAttributesRequest,
    ) -> Result<(), AwsError> {
        validate_attributes(&input.attributes)?;
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            q.attrs.extend(input.attributes.clone());
            tx.execute(
                "UPDATE queues SET attributes = ?1, modified_at = ?2 WHERE id = ?3",
                params![json_string(&q.attrs), now_ms() / 1000, q.id],
            )?;
            Ok(())
        })
    }

    fn send_message(
        &self,
        ctx: &RequestContext,
        input: SendMessageRequest,
    ) -> Result<SendMessageResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            let sent = put_message(
                tx,
                ctx,
                &q,
                NewMessage {
                    body: input.message_body.clone(),
                    delay: input.delay_seconds,
                    attrs: input.message_attributes.clone(),
                    group: input.message_group_id.clone(),
                    dedup: input.message_deduplication_id.clone(),
                },
            )?;
            Ok(SendMessageResult {
                md5_of_message_body: Some(sent.md5),
                md5_of_message_attributes: sent.md5_attrs,
                md5_of_message_system_attributes: None,
                message_id: Some(sent.message_id),
                sequence_number: sent.sequence,
            })
        })
    }

    fn send_message_batch(
        &self,
        ctx: &RequestContext,
        input: SendMessageBatchRequest,
    ) -> Result<SendMessageBatchResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            let ids: Vec<&str> = input.entries.iter().map(|e| e.id.as_str()).collect();
            check_batch("SendMessageBatch", &ids)?;
            let mut result = SendMessageBatchResult::default();
            for e in &input.entries {
                let sent = put_message(
                    tx,
                    ctx,
                    &q,
                    NewMessage {
                        body: e.message_body.clone(),
                        delay: e.delay_seconds,
                        attrs: e.message_attributes.clone(),
                        group: e.message_group_id.clone(),
                        dedup: e.message_deduplication_id.clone(),
                    },
                );
                match sent {
                    Ok(s) => result.successful.push(SendMessageBatchResultEntry {
                        id: e.id.clone(),
                        md5_of_message_body: s.md5,
                        md5_of_message_attributes: s.md5_attrs,
                        md5_of_message_system_attributes: None,
                        message_id: s.message_id,
                        sequence_number: s.sequence,
                    }),
                    Err(error) => result.failed.push(BatchResultErrorEntry {
                        id: e.id.clone(),
                        code: error.code,
                        message: Some(error.message),
                        sender_fault: error.sender,
                    }),
                }
            }
            Ok(result)
        })
    }

    fn receive_message(
        &self,
        ctx: &RequestContext,
        input: ReceiveMessageRequest,
    ) -> Result<ReceiveMessageResult, AwsError> {
        let max = input.max_number_of_messages.unwrap_or(1);
        if !(1..=10).contains(&max) {
            return Err(invalid_value(format!(
                "Value {max} for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."
            )));
        }
        if let Some(v) = input.visibility_timeout {
            validate_visibility(v)?;
        }
        if let Some(w) = input.wait_time_seconds {
            if !(0..=20).contains(&w) {
                return Err(invalid_value(format!(
                    "Value {w} for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided."
                )));
            }
        }
        let want_attrs: BTreeSet<String> = input
            .attribute_names
            .iter()
            .chain(&input.message_system_attribute_names)
            .cloned()
            .collect();
        let started = std::time::Instant::now();
        loop {
            let (messages, wait) = self.db.transaction(|tx| {
                let q = queue_from_url(tx, ctx, &input.queue_url)?;
                let visibility = input
                    .visibility_timeout
                    .map_or_else(|| q.int("VisibilityTimeout"), i64::from);
                let wait = input
                    .wait_time_seconds
                    .map_or_else(|| q.int("ReceiveMessageWaitTimeSeconds"), i64::from);
                let m = receive_once(
                    tx,
                    &q,
                    max as usize,
                    visibility,
                    &want_attrs,
                    &input.message_attribute_names,
                )?;
                Ok((m, wait))
            })?;
            if !messages.is_empty() || started.elapsed() >= Duration::from_secs(wait as u64) {
                return Ok(ReceiveMessageResult { messages });
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn delete_message(
        &self,
        ctx: &RequestContext,
        input: DeleteMessageRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            tx.execute(
                "DELETE FROM messages WHERE queue_id = ?1 AND receipt_handle = ?2",
                params![q.id, input.receipt_handle],
            )?;
            Ok(())
        })
    }

    fn delete_message_batch(
        &self,
        ctx: &RequestContext,
        input: DeleteMessageBatchRequest,
    ) -> Result<DeleteMessageBatchResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            let ids: Vec<&str> = input.entries.iter().map(|e| e.id.as_str()).collect();
            check_batch("DeleteMessageBatch", &ids)?;
            let mut result = DeleteMessageBatchResult::default();
            for e in &input.entries {
                tx.execute(
                    "DELETE FROM messages WHERE queue_id = ?1 AND receipt_handle = ?2",
                    params![q.id, e.receipt_handle],
                )?;
                result
                    .successful
                    .push(DeleteMessageBatchResultEntry { id: e.id.clone() });
            }
            Ok(result)
        })
    }

    fn change_message_visibility(
        &self,
        ctx: &RequestContext,
        input: ChangeMessageVisibilityRequest,
    ) -> Result<(), AwsError> {
        validate_visibility(input.visibility_timeout)?;
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            set_visibility(tx, &q, &input.receipt_handle, input.visibility_timeout)
        })
    }

    fn change_message_visibility_batch(
        &self,
        ctx: &RequestContext,
        input: ChangeMessageVisibilityBatchRequest,
    ) -> Result<ChangeMessageVisibilityBatchResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            let ids: Vec<&str> = input.entries.iter().map(|e| e.id.as_str()).collect();
            check_batch("ChangeMessageVisibilityBatch", &ids)?;
            let mut result = ChangeMessageVisibilityBatchResult::default();
            for e in &input.entries {
                let outcome =
                    validate_visibility(e.visibility_timeout.unwrap_or(0)).and_then(|()| {
                        set_visibility(tx, &q, &e.receipt_handle, e.visibility_timeout.unwrap_or(0))
                    });
                match outcome {
                    Ok(()) => result
                        .successful
                        .push(ChangeMessageVisibilityBatchResultEntry { id: e.id.clone() }),
                    Err(error) => result.failed.push(BatchResultErrorEntry {
                        id: e.id.clone(),
                        code: error.code,
                        message: Some(error.message),
                        sender_fault: error.sender,
                    }),
                }
            }
            Ok(result)
        })
    }

    fn purge_queue(&self, ctx: &RequestContext, input: PurgeQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            tx.execute("DELETE FROM messages WHERE queue_id = ?1", params![q.id])?;
            Ok(())
        })
    }

    fn tag_queue(&self, ctx: &RequestContext, input: TagQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            q.tags.extend(input.tags.clone());
            tx.execute(
                "UPDATE queues SET tags = ?1 WHERE id = ?2",
                params![json_string(&q.tags), q.id],
            )?;
            Ok(())
        })
    }

    fn untag_queue(&self, ctx: &RequestContext, input: UntagQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            for k in &input.tag_keys {
                q.tags.remove(k);
            }
            tx.execute(
                "UPDATE queues SET tags = ?1 WHERE id = ?2",
                params![json_string(&q.tags), q.id],
            )?;
            Ok(())
        })
    }

    fn list_queue_tags(
        &self,
        ctx: &RequestContext,
        input: ListQueueTagsRequest,
    ) -> Result<ListQueueTagsResult, AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            Ok(ListQueueTagsResult { tags: q.tags })
        })
    }
}

fn set_visibility(tx: &Transaction, q: &Queue, handle: &str, seconds: i32) -> Result<(), AwsError> {
    let now = now_ms();
    let visible_at: Option<i64> = tx
        .query_row(
            "SELECT visible_at FROM messages WHERE queue_id = ?1 AND receipt_handle = ?2",
            params![q.id, handle],
            |r| r.get(0),
        )
        .optional()?;
    match visible_at {
        None => Err(err(
            "ReceiptHandleIsInvalid",
            format!("The input receipt handle \"{handle}\" is not a valid receipt handle."),
        )),
        Some(v) if v <= now => Err(err(
            "MessageNotInflight",
            "The message referred to is not in flight.",
        )),
        Some(_) => {
            tx.execute(
                "UPDATE messages SET visible_at = ?1 WHERE queue_id = ?2 AND receipt_handle = ?3",
                params![now + i64::from(seconds) * 1000, q.id, handle],
            )?;
            Ok(())
        }
    }
}
