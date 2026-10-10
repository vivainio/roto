use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::schema::*;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use md5::{Digest, Md5};
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{FromJson, ToJson};
use sha2::Sha256;

use crate::MIGRATIONS;
use crate::generated::*;

const NO_QUEUE: &str = "AWS.SimpleQueueService.NonExistentQueue";
const NO_QUEUE_MSG: &str = "The specified queue does not exist for this wsdl version.";
const ALLOWED_PERMISSIONS: &[&str] = &[
    "*",
    "ChangeMessageVisibility",
    "DeleteMessage",
    "GetQueueAttributes",
    "GetQueueUrl",
    "ListDeadLetterSourceQueues",
    "PurgeQueue",
    "ReceiveMessage",
    "SendMessage",
];
const DEFAULTS: &[(&str, &str)] = &[
    ("VisibilityTimeout", "30"),
    ("MaximumMessageSize", "262144"),
    ("MessageRetentionPeriod", "345600"),
    ("DelaySeconds", "0"),
    ("ReceiveMessageWaitTimeSeconds", "0"),
];
/// name -> inclusive range for integer attributes.
const RANGES: &[(&str, i64, i64)] = &[
    ("VisibilityTimeout", 0, 43_200),
    ("MaximumMessageSize", 1024, 262_144),
    ("MessageRetentionPeriod", 1, 1_209_600),
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
            db: store.diesel_db("sqs", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(queues::table).execute(tx)?;
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
    tx: &mut SqliteConnection,
    account: &str,
    region: &str,
    name: &str,
) -> Result<Option<Queue>, AwsError> {
    Ok(queues::table
        .filter(queues::account_id.eq(account))
        .filter(queues::region.eq(region))
        .filter(queues::name.eq(name))
        .select((
            queues::id,
            queues::attributes,
            queues::tags,
            queues::created_at,
            queues::modified_at,
        ))
        .first::<(i64, String, String, i64, i64)>(tx)
        .optional()?
        .map(|(id, attrs, tags, created_at, modified_at)| Queue {
            id,
            account: account.into(),
            region: region.into(),
            name: name.into(),
            attrs: from_json_string(&attrs),
            tags: from_json_string(&tags),
            created_at,
            modified_at,
        }))
}

fn queue_from_url(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    url: &str,
) -> Result<Queue, AwsError> {
    let mut parts = url.trim_end_matches('/').rsplit('/');
    let name = parts.next().unwrap_or_default();
    let account = parts
        .next()
        .filter(|a| a.len() == 12 && a.bytes().all(|b| b.is_ascii_digit()))
        .unwrap_or(&ctx.account_id);
    load_queue(tx, account, &ctx.region, name)?.ok_or_else(|| err(NO_QUEUE, NO_QUEUE_MSG))
}

/// Serialises like Python's `json.dumps` (`", "` and `": "` separators); clients compare
/// attribute strings that moto produced that way.
fn py_json(v: &serde_json::Value) -> String {
    use serde::Serialize;
    use serde_json::ser::Formatter;
    use std::io;
    struct PyFormat;
    impl Formatter for PyFormat {
        fn begin_array_value<W: ?Sized + io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> io::Result<()> {
            if first { Ok(()) } else { w.write_all(b", ") }
        }
        fn begin_object_key<W: ?Sized + io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> io::Result<()> {
            if first { Ok(()) } else { w.write_all(b", ") }
        }
        fn begin_object_value<W: ?Sized + io::Write>(&mut self, w: &mut W) -> io::Result<()> {
            w.write_all(b": ")
        }
    }
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, PyFormat);
    let _ = v.serialize(&mut ser);
    String::from_utf8(buf).unwrap_or_default()
}

fn invalid_attr_value(name: &str) -> AwsError {
    err(
        "InvalidAttributeValue",
        format!("Invalid value for the parameter {name}."),
    )
}

/// Validates attributes given to CreateQueue/SetQueueAttributes. Like moto, unknown names are
/// ignored. Returns the attributes to store; an empty `Policy`/`RedrivePolicy` removes the
/// attribute, which is signalled by an empty value.
fn normalize_attributes(
    tx: &mut SqliteConnection,
    fifo: bool,
    attrs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, AwsError> {
    let mut out = BTreeMap::new();
    for (k, v) in attrs {
        let k = &k.trim().to_string(); // moto normalises names, so "VisibilityTimeout " works
        if let Some((_, lo, hi)) = RANGES.iter().find(|(n, ..)| n == k) {
            match v.parse::<i64>() {
                Ok(n) if (*lo..=*hi).contains(&n) => {}
                _ => return Err(invalid_attr_value(k)),
            }
            out.insert(k.clone(), v.clone());
        } else if k == "RedrivePolicy" {
            out.insert(
                k.clone(),
                normalize_redrive(tx, fifo, v)?.unwrap_or_default(),
            );
        } else if OTHER_ATTRS.contains(&k.as_str()) {
            out.insert(k.clone(), v.clone());
        }
    }
    Ok(out)
}

/// `Ok(None)` for an empty policy (meaning "remove").
fn normalize_redrive(
    tx: &mut SqliteConnection,
    fifo: bool,
    raw: &str,
) -> Result<Option<String>, AwsError> {
    let bad = || invalid_value("Redrive policy is not a dict or valid json");
    if raw.is_empty() {
        return Ok(None);
    }
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|_| bad())?;
    let serde_json::Value::Object(mut obj) = value else {
        return Err(bad());
    };
    if obj.is_empty() {
        return Ok(None);
    }
    let arn = obj
        .get("deadLetterTargetArn")
        .and_then(|v| v.as_str())
        .ok_or_else(|| invalid_value("Redrive policy does not contain deadLetterTargetArn"))?
        .to_string();
    let max = match obj.get("maxReceiveCount") {
        Some(serde_json::Value::String(s)) => s.parse::<i64>().map_err(|_| bad())?,
        Some(v) => v.as_i64().ok_or_else(bad)?,
        None => {
            return Err(invalid_value(
                "Redrive policy does not contain maxReceiveCount",
            ));
        }
    };
    obj.insert("maxReceiveCount".into(), max.into());
    let parts: Vec<&str> = arn.split(':').collect();
    let dlq = match parts.as_slice() {
        [_, _, "sqs", region, account, name] => load_queue(tx, account, region, name)?,
        _ => None,
    };
    let dlq = dlq.ok_or_else(|| err(NO_QUEUE, format!("Could not find DLQ for {arn}")))?;
    if fifo && !dlq.is_fifo() {
        return Err(err(
            "InvalidParameterCombination",
            "Fifo queues cannot use non fifo dead letter queues",
        ));
    }
    Ok(Some(py_json(&serde_json::Value::Object(obj))))
}

/// Stored attributes with empty values dropped (the "remove" signal).
fn merge_attributes(stored: &mut BTreeMap<String, String>, new: BTreeMap<String, String>) {
    for (k, v) in new {
        if v.is_empty() && matches!(k.as_str(), "Policy" | "RedrivePolicy") {
            stored.remove(&k);
        } else {
            stored.insert(k, v);
        }
    }
}

fn validate_message_attributes(
    attrs: &BTreeMap<String, MessageAttributeValue>,
) -> Result<(), AwsError> {
    let invalid = |m: String| err("MessageAttributesInvalid", m);
    for (name, v) in attrs {
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        {
            return Err(invalid(format!(
                "The message attribute name '{name}' is invalid. Attribute name can contain A-Z, a-z, 0-9, underscore (_), hyphen (-), and period (.) characters."
            )));
        }
        if v.data_type.is_empty() {
            return Err(invalid(format!(
                "The message attribute '{name}' must contain non-empty message attribute value."
            )));
        }
        let prefix = v.data_type.split('.').next().unwrap_or_default();
        if !matches!(prefix, "String" | "Binary" | "Number") {
            return Err(invalid(format!(
                "The message attribute '{name}' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String."
            )));
        }
        let missing = if prefix == "Binary" {
            v.binary_value.is_none()
        } else {
            v.string_value.is_none()
        };
        if missing {
            return Err(invalid(format!(
                "The message attribute '{name}' must contain non-empty message attribute value for message attribute type '{}'.",
                v.data_type
            )));
        }
    }
    Ok(())
}

fn expire(tx: &mut SqliteConnection, q: &Queue, now: i64) -> Result<(), AwsError> {
    diesel::delete(
        messages::table
            .filter(messages::queue_id.eq(&(q.id)))
            .filter(messages::sent_at.lt(&(now - q.int("MessageRetentionPeriod") * 1000))),
    )
    .execute(tx)?;
    Ok(())
}

fn queue_attributes(
    tx: &mut SqliteConnection,
    q: &Queue,
) -> Result<BTreeMap<String, String>, AwsError> {
    let now = now_ms();
    expire(tx, q, now)?;
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
        messages::table
            .filter(messages::queue_id.eq(q.id))
            .filter(messages::visible_at.le(now))
            .count()
            .get_result::<i64>(tx)?
            .to_string(),
    );
    out.insert(
        "ApproximateNumberOfMessagesNotVisible".into(),
        messages::table
            .filter(messages::queue_id.eq(q.id))
            .filter(messages::visible_at.gt(now))
            .filter(messages::receive_count.gt(0_i64))
            .count()
            .get_result::<i64>(tx)?
            .to_string(),
    );
    out.insert(
        "ApproximateNumberOfMessagesDelayed".into(),
        messages::table
            .filter(messages::queue_id.eq(q.id))
            .filter(messages::visible_at.gt(now))
            .filter(messages::receive_count.eq(0_i64))
            .count()
            .get_result::<i64>(tx)?
            .to_string(),
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
    trace_header: Option<String>,
}

struct Sent {
    message_id: String,
    md5: String,
    md5_attrs: Option<String>,
    sequence: Option<String>,
}

/// Checks that do not depend on the position in a batch (moto's `_validate_message`).
fn validate_new_message(q: &Queue, m: &NewMessage) -> Result<(), AwsError> {
    validate_message_attributes(&m.attrs)?;
    if q.is_fifo() {
        let content_based = q.get("ContentBasedDeduplication") == Some("true");
        if !content_based && m.group.is_none() {
            return Err(AwsError::missing_parameter("MessageGroupId"));
        }
        if !content_based && m.dedup.is_none() {
            return Err(invalid_value(
                "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
            ));
        }
        if m.delay.unwrap_or(0) > 0 {
            return Err(invalid_value(format!(
                "Value {} for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.",
                m.delay.unwrap_or(0)
            )));
        }
    }
    let max = q.int("MaximumMessageSize");
    if m.body.len() as i64 > max {
        return Err(invalid_value(format!(
            "One or more parameters are invalid. Reason: Message must be shorter than {max} bytes."
        )));
    }
    match (&m.group, q.is_fifo()) {
        (None, true) => Err(AwsError::missing_parameter("MessageGroupId")),
        (Some(g), false) => Err(invalid_value(format!(
            "Value {g} for parameter MessageGroupId is invalid. Reason: The request include parameter that is not valid for this queue type."
        ))),
        _ => Ok(()),
    }
}

fn put_message(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    q: &Queue,
    m: NewMessage,
) -> Result<Sent, AwsError> {
    validate_new_message(q, &m)?;
    let md5 = md5_hex(m.body.as_bytes());
    let md5_attrs = md5_of_attributes(&m.attrs);
    let now = now_ms();

    let mut dedup = m.dedup.clone(); // reported back on receive, even for standard queues
    if q.is_fifo() {
        let id = m.dedup.clone().unwrap_or_else(|| {
            let mut h = Sha256::new();
            h.update(m.body.as_bytes());
            hex::encode(h.finalize())
        });
        // 5 minute deduplication window.
        let existing: Option<(String, i64)> = messages::table
            .filter(messages::queue_id.eq(&(q.id)))
            .filter(messages::dedup_id.eq(&(id)))
            .filter(messages::sent_at.gt(&(now - 300_000)))
            .select((messages::message_id, messages::seq))
            .first::<(String, i64)>(tx)
            .optional()?;
        if let Some((message_id, seq)) = existing {
            return Ok(Sent {
                message_id,
                md5,
                md5_attrs,
                sequence: Some(format!("{seq:018}")),
            });
        }
        dedup = Some(id);
    }

    let delay = i64::from(m.delay.unwrap_or(q.int("DelaySeconds") as i32));
    if !(0..=900).contains(&delay) {
        return Err(invalid_value(format!(
            "Value {delay} for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and <= 900."
        )));
    }
    let message_id = new_id();
    let inserted_sequence: i64 = diesel::insert_into(messages::table)
        .values((
            messages::queue_id.eq(&(q.id)),
            messages::message_id.eq(&(message_id)),
            messages::body.eq(&(m.body)),
            messages::md5.eq(&(md5)),
            messages::attrs.eq(&(json_string(&m.attrs))),
            messages::md5_attrs.eq(&(md5_attrs)),
            messages::sent_at.eq(&(now)),
            messages::visible_at.eq(&(now + delay * 1000)),
            messages::group_id.eq(&(m.group)),
            messages::dedup_id.eq(&(dedup)),
            messages::sender_id.eq(&(ctx.account_id)),
            messages::trace_header.eq(&(m.trace_header)),
        ))
        .returning(messages::seq)
        .get_result(tx)?;
    let sequence = q.is_fifo().then(|| format!("{:018}", inserted_sequence));
    Ok(Sent {
        message_id,
        md5,
        md5_attrs,
        sequence,
    })
}

#[derive(Queryable, Selectable)]
#[diesel(table_name=messages)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
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
    trace_header: Option<String>,
}

fn receive_once(
    tx: &mut SqliteConnection,
    q: &Queue,
    max: usize,
    visibility: i64,
    want_attrs: &BTreeSet<String>,
    want_message_attrs: &[String],
) -> Result<Vec<Message>, AwsError> {
    let now = now_ms();
    expire(tx, q, now)?;
    let other = diesel::alias!(messages as other);
    let blocked = other
        .filter(other.field(messages::queue_id).eq(messages::queue_id))
        .filter(other.field(messages::group_id).eq(messages::group_id))
        .filter(other.field(messages::visible_at).gt(now))
        .filter(other.field(messages::receive_count).gt(0_i64));
    let rows = messages::table
        .filter(messages::queue_id.eq(q.id))
        .filter(messages::visible_at.le(now))
        .filter(
            messages::group_id
                .is_null()
                .or(diesel::dsl::not(diesel::dsl::exists(blocked))),
        )
        .order(messages::seq)
        .limit(200)
        .select(Row::as_select())
        .load::<Row>(tx)?;

    let redrive = q.redrive();
    let mut out = Vec::new();
    for row in rows {
        if out.len() >= max {
            break;
        }
        if let Some((dlq_name, max_receives)) = &redrive
            && row.receive_count >= *max_receives
        {
            if let Some(dlq) = load_queue(tx, &q.account, &q.region, dlq_name)? {
                diesel::insert_into(messages::table)
                    .values((
                        messages::queue_id.eq(dlq.id),
                        messages::message_id.eq(&row.message_id),
                        messages::body.eq(&row.body),
                        messages::md5.eq(&row.md5),
                        messages::attrs.eq(&row.attrs),
                        messages::md5_attrs.eq(&row.md5_attrs),
                        messages::sent_at.eq(now),
                        messages::visible_at.eq(now),
                        messages::group_id.eq(&row.group_id),
                        messages::sender_id.eq(&row.sender_id),
                        messages::trace_header.eq(&row.trace_header),
                    ))
                    .execute(tx)?;
            }
            diesel::delete(messages::table.filter(messages::seq.eq(&(row.seq)))).execute(tx)?;
            continue;
        }
        let handle = format!("{}{}", new_id().replace('-', ""), new_id().replace('-', ""));
        let first = row.first_received_at.unwrap_or(now);
        diesel::update(messages::table.filter(messages::seq.eq(&(row.seq))))
            .set((
                messages::receipt_handle.eq(&(handle)),
                messages::visible_at.eq(&(now + visibility * 1000)),
                messages::receive_count.eq(&(row.receive_count + 1)),
                messages::first_received_at.eq(&(first)),
            ))
            .execute(tx)?;
        diesel::insert_into(receipts::table)
            .values((
                receipts::handle.eq(&(handle)),
                receipts::queue_id.eq(&(q.id)),
                receipts::seq.eq(&(row.seq)),
            ))
            .execute(tx)?;

        let all = want_attrs.contains("All");
        let mut attributes = BTreeMap::new();
        if let Some(t) = &row.trace_header {
            attributes.insert("AWSTraceHeader".to_string(), t.clone());
        }
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
        if let Some(g) = &row.group_id {
            add("MessageGroupId", g.clone());
        }
        if let Some(d) = &row.dedup_id {
            add("MessageDeduplicationId", d.clone());
        }
        if q.is_fifo() {
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
    const P: &str = "";
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
        let fifo_attr = input.attributes.get("FifoQueue").map(String::as_str) == Some("true");
        if fifo_attr != name.ends_with(".fifo") {
            return Err(invalid_value(
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.",
            ));
        }
        self.db.transaction(|tx| {
            let attrs = normalize_attributes(tx, fifo_attr, &input.attributes)?;
            if let Some(q) = load_queue(tx, &ctx.account_id, &ctx.region, name)? {
                for (k, v) in &attrs {
                    if q.get(k) != Some(v.as_str()) {
                        return Err(err(
                            "QueueAlreadyExists",
                            format!("A queue already exists with the same name and a different value for attribute {k}"),
                        ));
                    }
                }
                return Ok(CreateQueueResult { queue_url: Some(q.url(ctx)) });
            }
            let mut stored = BTreeMap::new();
            merge_attributes(&mut stored, attrs);
            let now = now_ms() / 1000;
            diesel::insert_into(queues::table).values((queues::account_id.eq(&(ctx.account_id)),queues::region.eq(&(ctx.region)),queues::name.eq(&(name)),queues::attributes.eq(&(json_string(&stored))),queues::tags.eq(&(json_string(&input.tags))),queues::created_at.eq(&(now)),queues::modified_at.eq(&(now)))).execute(tx)?;
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
            diesel::delete(queues::table.filter(queues::id.eq(&(q.id)))).execute(tx)?;
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
                .ok_or_else(|| err(NO_QUEUE, NO_QUEUE_MSG))?;
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
            let mut names = queues::table
                .filter(queues::account_id.eq(&ctx.account_id))
                .filter(queues::region.eq(&ctx.region))
                .filter(queues::name.gt(&after))
                .filter(substr(queues::name, 1, length(&prefix)).eq(&prefix))
                .order(queues::name)
                .limit(i64::from(limit) + 1)
                .select(queues::name)
                .load::<String>(tx)?;
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
            let names: Vec<&str> = input.attribute_names.iter().map(String::as_str).collect();
            let mut out = BTreeMap::new();
            for n in names {
                if n == "All" {
                    return Ok(GetQueueAttributesResult { attributes: all });
                }
                let known = RANGES.iter().any(|(k, ..)| *k == n)
                    || OTHER_ATTRS.contains(&n)
                    || n.starts_with("Approximate")
                    || matches!(n, "QueueArn" | "CreatedTimestamp" | "LastModifiedTimestamp");
                if !known {
                    return Err(err(
                        "InvalidAttributeName",
                        format!("Unknown Attribute {n}."),
                    ));
                }
                if let Some(v) = all.get(n) {
                    out.insert(n.to_string(), v.clone());
                }
            }
            Ok(GetQueueAttributesResult { attributes: out })
        })
    }

    fn set_queue_attributes(
        &self,
        ctx: &RequestContext,
        input: SetQueueAttributesRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            let attrs = normalize_attributes(tx, q.is_fifo(), &input.attributes)?;
            merge_attributes(&mut q.attrs, attrs);
            diesel::update(queues::table.filter(queues::id.eq(&(q.id))))
                .set((
                    queues::attributes.eq(&(json_string(&q.attrs))),
                    queues::modified_at.eq(&(now_ms() / 1000)),
                ))
                .execute(tx)?;
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
                    trace_header: input
                        .message_system_attributes
                        .get("AWSTraceHeader")
                        .and_then(|v| v.string_value.clone()),
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
            let total: usize = input.entries.iter().map(|e| e.message_body.len()).sum();
            if total > 262_144 {
                return Err(err(
                    "BatchRequestTooLong",
                    format!("Batch requests cannot be longer than 262144 bytes. You have sent {total} bytes."),
                ));
            }
            let new = |e: &SendMessageBatchRequestEntry| NewMessage {
                body: e.message_body.clone(),
                delay: e.delay_seconds,
                attrs: e.message_attributes.clone(),
                group: e.message_group_id.clone(),
                dedup: e.message_deduplication_id.clone(),
                trace_header: e
                    .message_system_attributes
                    .get("AWSTraceHeader")
                    .and_then(|v| v.string_value.clone()),
            };
            // Validate every message before sending any.
            for e in &input.entries {
                validate_new_message(&q, &new(e))?;
            }
            let mut result = SendMessageBatchResult::default();
            for e in &input.entries {
                match put_message(tx, ctx, &q, new(e)) {
                    Ok(s) => result.successful.push(SendMessageBatchResultEntry {
                        id: e.id.clone(),
                        md5_of_message_body: s.md5,
                        md5_of_message_attributes: s.md5_attrs,
                        md5_of_message_system_attributes: None,
                        message_id: s.message_id,
                        sequence_number: s.sequence,
                    }),
                    Err(error) if error.message.contains("DelaySeconds is invalid") => {
                        result.failed.push(BatchResultErrorEntry {
                            id: e.id.clone(),
                            code: error.code,
                            message: Some(error.message),
                            sender_fault: true,
                        })
                    }
                    Err(error) => return Err(error),
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
        if let Some(w) = input.wait_time_seconds
            && !(0..=20).contains(&w)
        {
            return Err(invalid_value(format!(
                "Value {w} for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided."
            )));
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
            delete_by_receipt(tx, &q, &input.receipt_handle)
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
                match delete_by_receipt(tx, &q, &e.receipt_handle) {
                    Ok(()) => result
                        .successful
                        .push(DeleteMessageBatchResultEntry { id: e.id.clone() }),
                    Err(_) => result
                        .failed
                        .push(invalid_receipt_entry(&e.id, &e.receipt_handle)),
                }
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
                let seconds = e.visibility_timeout.unwrap_or(0);
                if validate_visibility(seconds).is_err() {
                    result.failed.push(BatchResultErrorEntry {
                        id: e.id.clone(),
                        code: "InvalidParameterValue".into(),
                        message: Some("Visibility timeout invalid".into()),
                        sender_fault: true,
                    });
                    continue;
                }
                match set_visibility(tx, &q, &e.receipt_handle, seconds) {
                    Ok(()) => result
                        .successful
                        .push(ChangeMessageVisibilityBatchResultEntry { id: e.id.clone() }),
                    Err(error) if error.code == "ReceiptHandleIsInvalid" => result
                        .failed
                        .push(invalid_receipt_entry(&e.id, &e.receipt_handle)),
                    Err(error) => return Err(error),
                }
            }
            Ok(result)
        })
    }

    fn purge_queue(&self, ctx: &RequestContext, input: PurgeQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let q = queue_from_url(tx, ctx, &input.queue_url)?;
            diesel::delete(messages::table.filter(messages::queue_id.eq(&(q.id)))).execute(tx)?;
            Ok(())
        })
    }

    fn tag_queue(&self, ctx: &RequestContext, input: TagQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            if input.tags.is_empty() {
                return Err(AwsError::missing_parameter("Tags"));
            }
            if input.tags.len() > 50 {
                return Err(invalid_value(format!(
                    "Too many tags added for queue {}.",
                    q.name
                )));
            }
            q.tags.extend(input.tags.clone());
            diesel::update(queues::table.filter(queues::id.eq(&(q.id))))
                .set(queues::tags.eq(&(json_string(&q.tags))))
                .execute(tx)?;
            Ok(())
        })
    }

    fn untag_queue(&self, ctx: &RequestContext, input: UntagQueueRequest) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            if input.tag_keys.is_empty() {
                return Err(invalid_value(
                    "Tag keys must be between 1 and 128 characters in length.",
                ));
            }
            for k in &input.tag_keys {
                q.tags.remove(k);
            }
            diesel::update(queues::table.filter(queues::id.eq(&(q.id))))
                .set(queues::tags.eq(&(json_string(&q.tags))))
                .execute(tx)?;
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

    fn add_permission(
        &self,
        ctx: &RequestContext,
        input: AddPermissionRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            if input.actions.is_empty() {
                return Err(AwsError::missing_parameter("Actions"));
            }
            if input.aws_account_ids.is_empty() {
                return Err(invalid_value("Value [] for parameter PrincipalId is invalid. Reason: Unable to verify."));
            }
            if input.actions.len() > 7 {
                return Err(AwsError::sender(
                    403,
                    "OverLimit",
                    format!("{} Actions were found, maximum allowed is 7.", input.actions.len()),
                ));
            }
            if let Some(bad) = input.actions.iter().find(|a| !ALLOWED_PERMISSIONS.contains(&a.as_str())) {
                return Err(invalid_value(format!(
                    "Value SQS:{bad} for parameter ActionName is invalid. Reason: Only the queue owner is allowed to invoke this action."
                )));
            }
            let mut policy = policy_json(&q);
            let statements = policy["Statement"].as_array_mut().ok_or_else(|| AwsError::internal("bad policy"))?;
            if statements.iter().any(|s| s["Sid"] == input.label) {
                return Err(invalid_value(format!(
                    "Value {} for parameter Label is invalid. Reason: Already exists.",
                    input.label
                )));
            }
            let one_or_many = |items: Vec<String>| match items.len() {
                1 => serde_json::Value::String(items.into_iter().next().unwrap_or_default()),
                _ => serde_json::json!(items),
            };
            statements.push(serde_json::json!({
                "Sid": input.label,
                "Effect": "Allow",
                "Principal": {"AWS": one_or_many(
                    input.aws_account_ids.iter().map(|a| format!("arn:aws:iam::{a}:root")).collect())},
                "Action": one_or_many(input.actions.iter().map(|a| format!("SQS:{a}")).collect()),
                "Resource": q.arn(),
            }));
            q.attrs.insert("Policy".into(), policy.to_string());
            diesel::update(queues::table.filter(queues::id.eq(&(q.id)))).set(queues::attributes.eq(&(json_string(&q.attrs)))).execute(tx)?;
            Ok(())
        })
    }

    fn remove_permission(
        &self,
        ctx: &RequestContext,
        input: RemovePermissionRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut q = queue_from_url(tx, ctx, &input.queue_url)?;
            let mut policy = policy_json(&q);
            let statements = policy["Statement"].as_array_mut().ok_or_else(|| AwsError::internal("bad policy"))?;
            let before = statements.len();
            statements.retain(|s| s["Sid"] != input.label);
            if statements.len() == before {
                return Err(invalid_value(format!(
                    "Value {} for parameter Label is invalid. Reason: can't find label on existing policy.",
                    input.label
                )));
            }
            q.attrs.insert("Policy".into(), policy.to_string());
            diesel::update(queues::table.filter(queues::id.eq(&(q.id)))).set(queues::attributes.eq(&(json_string(&q.attrs)))).execute(tx)?;
            Ok(())
        })
    }
}

fn policy_json(q: &Queue) -> serde_json::Value {
    q.attrs
        .get("Policy")
        .and_then(|p| serde_json::from_str(p).ok())
        .filter(|p: &serde_json::Value| p["Statement"].is_array())
        .unwrap_or_else(|| {
            serde_json::json!({
                "Version": "2012-10-17",
                "Id": format!("{}/SQSDefaultPolicy", q.arn()),
                "Statement": [],
            })
        })
}

fn invalid_receipt_entry(id: &str, handle: &str) -> BatchResultErrorEntry {
    BatchResultErrorEntry {
        id: id.to_string(),
        code: "ReceiptHandleIsInvalid".into(),
        message: Some(format!(
            "The input receipt handle \"{handle}\" is not a valid receipt handle."
        )),
        sender_fault: true,
    }
}

fn invalid_receipt() -> AwsError {
    err(
        "ReceiptHandleIsInvalid",
        "The input receipt handle is invalid.",
    )
}

/// `Some(Some(seq))` live message, `Some(None)` message since deleted, `None` unknown handle.
fn lookup_receipt(
    tx: &mut SqliteConnection,
    q: &Queue,
    handle: &str,
) -> Result<Option<Option<i64>>, AwsError> {
    Ok(receipts::table
        .filter(receipts::queue_id.eq(&(q.id)))
        .filter(receipts::handle.eq(&(handle)))
        .select(receipts::seq)
        .first::<Option<i64>>(tx)
        .optional()?)
}

/// Any receipt handle ever issued for a message deletes it; deleting again is a no-op.
fn delete_by_receipt(tx: &mut SqliteConnection, q: &Queue, handle: &str) -> Result<(), AwsError> {
    match lookup_receipt(tx, q, handle)? {
        None => Err(invalid_receipt()),
        Some(None) => Ok(()),
        Some(Some(seq)) => {
            diesel::delete(messages::table.filter(messages::seq.eq(&(seq)))).execute(tx)?;
            Ok(())
        }
    }
}

fn set_visibility(
    tx: &mut SqliteConnection,
    q: &Queue,
    handle: &str,
    seconds: i32,
) -> Result<(), AwsError> {
    let Some(Some(seq)) = lookup_receipt(tx, q, handle)? else {
        return Err(invalid_receipt());
    };
    let now = now_ms();
    let sent_at: i64 = messages::table
        .filter(messages::seq.eq(&(seq)))
        .select(messages::sent_at)
        .first::<i64>(tx)?;
    let visible_at = now + i64::from(seconds) * 1000;
    if visible_at - sent_at > 43_200_000 {
        return Err(invalid_value(format!(
            "Value {seconds} for parameter VisibilityTimeout is invalid. Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds]"
        )));
    }
    diesel::update(messages::table.filter(messages::seq.eq(&(seq))))
        .set(messages::visible_at.eq(&(visible_at)))
        .execute(tx)?;
    Ok(())
}

/// A message attribute handed over by another service (SNS fan-out).
pub struct ExternalAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

impl Sqs {
    /// Delivers a message from another service to the queue with ARN `queue_arn`.
    /// Returns `Ok(false)` when the queue does not exist (publishers skip such subscriptions).
    pub fn deliver(
        &self,
        queue_arn: &str,
        body: &str,
        attributes: &BTreeMap<String, ExternalAttribute>,
        group_id: Option<String>,
        dedup_id: Option<String>,
    ) -> Result<bool, AwsError> {
        let parts: Vec<&str> = queue_arn.split(':').collect();
        let [_, _, "sqs", region, account, name] = parts.as_slice() else {
            return Ok(false);
        };
        let ctx = RequestContext {
            account_id: account.to_string(),
            region: region.to_string(),
            access_key: None,
            request_id: new_id(),
            base_url: String::new(),
        };
        self.db.transaction(|tx| {
            let Some(q) = load_queue(tx, account, region, name)? else {
                return Ok(false);
            };
            let attrs = attributes
                .iter()
                .map(|(k, a)| {
                    (
                        k.clone(),
                        MessageAttributeValue {
                            data_type: a.data_type.clone(),
                            string_value: a.string_value.clone(),
                            binary_value: a.binary_value.clone().map(roto_protocol::Blob),
                            ..Default::default()
                        },
                    )
                })
                .collect();
            let group = if q.is_fifo() {
                group_id.or_else(|| Some("default".into()))
            } else {
                None
            };
            put_message(
                tx,
                &ctx,
                &q,
                NewMessage {
                    body: body.to_string(),
                    delay: None,
                    attrs,
                    group,
                    dedup: dedup_id,
                    trace_header: None,
                },
            )?;
            Ok(true)
        })
    }
}

impl Sqs {
    /// Validate the supported SQS source and return its visibility timeout.
    pub fn event_source_visibility(
        &self,
        ctx: &RequestContext,
        source: &str,
    ) -> Result<i64, AwsError> {
        let name = self.event_source_name(ctx, source)?;
        self.db.transaction(|tx| {
            let q = load_queue(tx, &ctx.account_id, &ctx.region, name)?
                .ok_or_else(|| err(NO_QUEUE, NO_QUEUE_MSG))?;
            if q.is_fifo() {
                return Err(AwsError::not_implemented("lambda", "FIFO event sources"));
            }
            Ok(q.int("VisibilityTimeout"))
        })
    }
    fn event_source_name<'a>(
        &self,
        ctx: &RequestContext,
        source: &'a str,
    ) -> Result<&'a str, AwsError> {
        let prefix = format!("arn:aws:sqs:{}:{}:", ctx.region, ctx.account_id);
        source
            .strip_prefix(&prefix)
            .filter(|n| !n.is_empty() && !n.contains(':'))
            .ok_or_else(|| invalid_value("SQS event source must be in the same account and region"))
    }
    /// Claim a batch using the normal receive transaction, including expiry and DLQ redrive.
    pub fn receive_event_batch(
        &self,
        ctx: &RequestContext,
        source: &str,
        batch_size: usize,
    ) -> Result<serde_json::Value, AwsError> {
        if !(1..=10).contains(&batch_size) {
            return Err(invalid_value("Event batch size must be between 1 and 10"));
        }
        let name = self.event_source_name(ctx, source)?;
        let mut max = batch_size;
        loop {
            let result = self.db.transaction(|tx| {
                let q = load_queue(tx, &ctx.account_id, &ctx.region, name)?
                    .ok_or_else(|| err(NO_QUEUE, NO_QUEUE_MSG))?;
                if q.is_fifo() {
                    return Err(AwsError::not_implemented("lambda", "FIFO event sources"));
                }
                let messages = receive_once(
                    tx,
                    &q,
                    max,
                    q.int("VisibilityTimeout"),
                    &BTreeSet::from(["All".into()]),
                    &["All".into()],
                )?;
                let records: Vec<_> = messages
                    .into_iter()
                    .map(|m| event_record(m, source, &ctx.region))
                    .collect();
                let event = serde_json::json!({"Records":records});
                if serde_json::to_vec(&event).unwrap().len() > 6 * 1024 * 1024 {
                    return Err(invalid_value("EventBatchTooLarge"));
                }
                Ok(event)
            });
            match result {
                Err(e) if e.message == "EventBatchTooLarge" && max > 1 => max -= 1,
                other => return other,
            }
        }
    }
    pub fn acknowledge_event(
        &self,
        ctx: &RequestContext,
        source: &str,
        receipt: &str,
    ) -> Result<(), AwsError> {
        let name = self.event_source_name(ctx, source)?;
        self.db.transaction(|tx| {
            let q = load_queue(tx, &ctx.account_id, &ctx.region, name)?
                .ok_or_else(|| err(NO_QUEUE, NO_QUEUE_MSG))?;
            // A stale execution must not delete a message received again by another consumer.
            diesel::delete(
                messages::table
                    .filter(messages::queue_id.eq(&(q.id)))
                    .filter(messages::receipt_handle.eq(&(receipt))),
            )
            .execute(tx)?;
            Ok(())
        })
    }
}

fn event_record(message: Message, source: &str, region: &str) -> serde_json::Value {
    use serde_json::{Map, Value, json};
    let attributes: Map<String, Value> = message.message_attributes.into_iter().map(|(name, attr)| {
        let mut value = json!({
            "dataType": attr.data_type,
            "stringListValues": attr.string_list_values,
            "binaryListValues": attr.binary_list_values.into_iter().map(|b| roto_protocol::base64::encode(&b.0)).collect::<Vec<_>>(),
        });
        if let Some(text) = attr.string_value { value["stringValue"] = json!(text); }
        if let Some(bytes) = attr.binary_value { value["binaryValue"] = json!(roto_protocol::base64::encode(&bytes.0)); }
        (name, value)
    }).collect();
    json!({
        "messageId": message.message_id,
        "receiptHandle": message.receipt_handle,
        "body": message.body,
        "attributes": message.attributes,
        "messageAttributes": attributes,
        "md5OfBody": message.md5_of_body,
        "eventSource": "aws:sqs",
        "eventSourceARN": source,
        "awsRegion": region,
    })
}
