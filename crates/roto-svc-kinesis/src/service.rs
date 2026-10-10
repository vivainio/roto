use crate::{MIGRATIONS, generated::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{FromJson, ToJson};
use serde_json::{Value, json};
use std::sync::Arc;

pub struct Kinesis {
    db: Arc<Db>,
}
impl Kinesis {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("kinesis", MIGRATIONS)?,
        })
    }
    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM streams", [])?;
            tx.execute("DELETE FROM tokens", [])?;
            Ok(())
        })
    }
    fn call(&self, ctx: &RequestContext, op: &str, input: &Value) -> Result<Value, AwsError> {
        self.db.transaction(|tx| call(tx, ctx, op, input))
    }
}
fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn err(code: &str, message: impl Into<String>) -> AwsError {
    AwsError::sender(400, code, message)
}
fn invalid(message: impl Into<String>) -> AwsError {
    err("InvalidArgumentException", message)
}
fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or("")
}
fn number(v: &Value, key: &str, default: usize) -> Result<usize, AwsError> {
    match v.get(key) {
        None => Ok(default),
        Some(n) => n
            .as_u64()
            .filter(|n| *n > 0 && *n <= usize::MAX as u64)
            .map(|n| n as usize)
            .ok_or_else(|| invalid(format!("{key} must be positive"))),
    }
}
fn arn(ctx: &RequestContext, name: &str) -> String {
    format!(
        "arn:aws:kinesis:{}:{}:stream/{name}",
        ctx.region, ctx.account_id
    )
}
fn missing(ctx: &RequestContext, name: &str) -> AwsError {
    err(
        "ResourceNotFoundException",
        format!("Stream {name} under account {} not found.", ctx.account_id),
    )
}
fn load(tx: &Transaction, ctx: &RequestContext, input: &Value) -> Result<Value, AwsError> {
    let name = text(input, "StreamName");
    let id = if name.is_empty() {
        text(input, "StreamARN").to_string()
    } else {
        arn(ctx, name)
    };
    if id.is_empty() {
        return Err(invalid("StreamName or StreamARN is required"));
    }
    let data: Option<String> = tx
        .query_row(
            "SELECT metadata FROM streams WHERE arn=?1 AND account_id=?2 AND region=?3",
            params![id, ctx.account_id, ctx.region],
            |r| r.get(0),
        )
        .optional()?;
    let s: Value = data
        .map(|s| serde_json::from_str(&s).map_err(|e| AwsError::internal(e.to_string())))
        .unwrap_or_else(|| {
            Err(missing(
                ctx,
                if name.is_empty() {
                    id.rsplit('/').next().unwrap_or(&id)
                } else {
                    name
                },
            ))
        })?;
    if !text(input, "StreamARN").is_empty() && text(input, "StreamARN") != text(&s, "StreamARN") {
        return Err(invalid(
            "StreamName and StreamARN must identify the same stream",
        ));
    }
    // Expire against the current retention before a later increase can expose old records.
    tx.execute(
        "DELETE FROM records WHERE stream_arn=?1 AND arrived<?2",
        params![
            id,
            now() - s["RetentionPeriodHours"].as_f64().unwrap_or(24.) * 3600.
        ],
    )?;
    Ok(s)
}
fn save(tx: &Transaction, s: &Value) -> Result<(), AwsError> {
    tx.execute(
        "UPDATE streams SET metadata=?1 WHERE arn=?2",
        params![s.to_string(), text(s, "StreamARN")],
    )?;
    Ok(())
}
fn shard(id: usize, start: u128, end: u128) -> Value {
    json!({"ShardId":format!("shardId-{id:012}"),"HashKeyRange":{"StartingHashKey":start.to_string(),"EndingHashKey":end.to_string()},"SequenceNumberRange":{"StartingSequenceNumber":"0"}})
}
fn ranges(count: usize) -> Vec<(u128, u128)> {
    let n = count as u128;
    let width = u128::MAX / n;
    let remainder = u128::MAX % n;
    (0..count)
        .map(|i| {
            let boundary = |j: u128| width * j + ((remainder + 1) * j) / n;
            let start = boundary(i as u128);
            let end = if i + 1 == count {
                u128::MAX
            } else {
                boundary(i as u128 + 1) - 1
            };
            (start, end)
        })
        .collect()
}
fn open(s: &Value) -> bool {
    s["SequenceNumberRange"]
        .get("EndingSequenceNumber")
        .is_none()
}
fn hash(s: &Value, key: &str) -> Result<u128, AwsError> {
    text(&s["HashKeyRange"], key)
        .parse()
        .map_err(|_| AwsError::internal("invalid stored shard hash range"))
}
fn token(
    tx: &Transaction,
    ctx: &RequestContext,
    kind: &str,
    payload: Value,
    ttl: f64,
) -> Result<String, AwsError> {
    let id = uuid::Uuid::new_v4().to_string();
    let current = now();
    tx.execute("DELETE FROM tokens WHERE expires<?1", [current])?;
    tx.execute(
        "INSERT INTO tokens VALUES (?1,?2,?3,?4,?5,?6)",
        params![
            id,
            ctx.account_id,
            ctx.region,
            kind,
            payload.to_string(),
            current + ttl
        ],
    )?;
    Ok(id)
}
fn read_token(
    tx: &Transaction,
    ctx: &RequestContext,
    kind: &str,
    id: &str,
) -> Result<Value, AwsError> {
    let found: Option<(String,f64)>=tx.query_row("SELECT payload,expires FROM tokens WHERE token=?1 AND account_id=?2 AND region=?3 AND kind=?4",params![id,ctx.account_id,ctx.region,kind],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (payload, expires) = found.ok_or_else(|| invalid("Invalid token"))?;
    if expires < now() {
        return Err(err(
            if kind == "iterator" {
                "ExpiredIteratorException"
            } else {
                "ExpiredNextTokenException"
            },
            "Token expired",
        ));
    }
    serde_json::from_str(&payload).map_err(|e| AwsError::internal(e.to_string()))
}
fn next_sequence(tx: &Transaction, stream: &str, shard: &str) -> Result<i64, AwsError> {
    Ok(tx
        .query_row(
            "SELECT sequence + 1 FROM shard_sequences WHERE stream_arn=?1 AND shard_id=?2",
            params![stream, shard],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(1))
}
fn validate_record(v: &Value, index: Option<usize>) -> Result<Vec<u8>, AwsError> {
    let data = STANDARD
        .decode(text(v, "Data"))
        .map_err(|_| err("SerializationException", "Invalid base64 data"))?;
    let key = text(v, "PartitionKey");
    if key.is_empty() || key.chars().count() > 256 {
        return Err(err(
            "ValidationException",
            "PartitionKey must contain 1 to 256 characters",
        ));
    }
    if data.len() + key.len() > 1048576 {
        let path = index
            .map(|i| format!("records.{i}.member.data"))
            .unwrap_or_else(|| "data".into());
        return Err(err(
            "ValidationException",
            format!(
                "1 validation error detected: Value at '{path}' failed to satisfy constraint: Member must have length less than or equal to 1048576"
            ),
        ));
    }
    if let Some(explicit) = v.get("ExplicitHashKey") {
        explicit
            .as_str()
            .unwrap_or("")
            .parse::<u128>()
            .map_err(|_| {
                invalid("ExplicitHashKey must be a decimal integer between 0 and 2^128 - 1")
            })?;
    }
    Ok(data)
}
fn put(tx: &Transaction, s: &Value, v: &Value, data: &[u8]) -> Result<Value, AwsError> {
    let key = text(v, "PartitionKey");
    let h = if let Some(explicit) = v.get("ExplicitHashKey") {
        explicit
            .as_str()
            .unwrap_or("")
            .parse::<u128>()
            .map_err(|_| invalid("Invalid ExplicitHashKey"))?
    } else {
        u128::from_be_bytes(md5::compute(key.as_bytes()).0)
    };
    let shards = s["Shards"].as_array().unwrap();
    let target = shards
        .iter()
        .find(|shard| {
            open(shard)
                && hash(shard, "StartingHashKey").is_ok_and(|start| h >= start)
                && hash(shard, "EndingHashKey").is_ok_and(|end| h <= end)
        })
        .ok_or_else(|| AwsError::internal("no open shard for hash key"))?;
    let id = text(target, "ShardId");
    let stream = text(s, "StreamARN");
    let seq = next_sequence(tx, stream, id)?;
    tx.execute("INSERT INTO shard_sequences VALUES (?1,?2,?3) ON CONFLICT(stream_arn,shard_id) DO UPDATE SET sequence=excluded.sequence",params![stream,id,seq])?;
    tx.execute(
        "INSERT INTO records VALUES (?1,?2,?3,?4,?5,?6)",
        params![stream, id, seq, data, key, now()],
    )?;
    Ok(json!({"ShardId":id,"SequenceNumber":seq.to_string(),"EncryptionType":s["EncryptionType"]}))
}
fn close(tx: &Transaction, stream: &str, shard: &mut Value) -> Result<(), AwsError> {
    let seq = next_sequence(tx, stream, text(shard, "ShardId"))? - 1;
    shard["SequenceNumberRange"]["EndingSequenceNumber"] = json!(seq.to_string());
    Ok(())
}
fn call(
    tx: &Transaction,
    ctx: &RequestContext,
    op: &str,
    input: &Value,
) -> Result<Value, AwsError> {
    match op {
        "CreateStream" => {
            let name = text(input, "StreamName");
            if name.is_empty()
                || name.len() > 128
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            {
                return Err(err("ValidationException", "Invalid stream name"));
            }
            let mode = input["StreamModeDetails"]["StreamMode"]
                .as_str()
                .unwrap_or("PROVISIONED");
            if !["PROVISIONED", "ON_DEMAND"].contains(&mode) {
                return Err(invalid("Invalid stream mode"));
            }
            let count = number(input, "ShardCount", if mode == "ON_DEMAND" { 4 } else { 0 })?;
            if count == 0 || count > 10000 {
                return Err(err(
                    "ValidationException",
                    "ShardCount must be between 1 and 10000",
                ));
            }
            let id = arn(ctx, name);
            if tx.query_row("SELECT COUNT(*) FROM streams WHERE arn=?1", [&id], |r| {
                r.get::<_, i64>(0)
            })? > 0
            {
                return Err(err(
                    "ResourceInUseException",
                    format!(
                        "Stream {name} under account {} already exists.",
                        ctx.account_id
                    ),
                ));
            }
            let shards: Vec<_> = ranges(count)
                .into_iter()
                .enumerate()
                .map(|(i, (start, end))| shard(i, start, end))
                .collect();
            let s = json!({"StreamName":name,"StreamARN":id,"StreamStatus":"ACTIVE","StreamCreationTimestamp":now(),"RetentionPeriodHours":24,"StreamModeDetails":{"StreamMode":mode},"EncryptionType":"NONE","EnhancedMonitoring":[{"ShardLevelMetrics":[]}],"Shards":shards,"Tags":{},"Consumers":[]});
            tx.execute(
                "INSERT INTO streams VALUES (?1,?2,?3,?4,?5)",
                params![id, ctx.account_id, ctx.region, name, s.to_string()],
            )?;
            return Ok(json!({}));
        }
        "ListStreams" => {
            let limit = number(input, "Limit", 10)?;
            if limit > 10000 {
                return Err(invalid("Limit must not exceed 10000"));
            }
            let start = if input.get("NextToken").is_some() {
                read_token(tx, ctx, "streams", text(input, "NextToken"))?["start"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            } else {
                text(input, "ExclusiveStartStreamName").to_string()
            };
            let mut stmt=tx.prepare("SELECT metadata FROM streams WHERE account_id=?1 AND region=?2 AND name>?3 ORDER BY name")?;
            let raw = stmt
                .query_map(params![ctx.account_id, ctx.region, start], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let more = raw.len() > limit;
            let mut names = Vec::new();
            let mut summaries = Vec::new();
            for raw in raw.into_iter().take(limit) {
                let s: Value =
                    serde_json::from_str(&raw).map_err(|e| AwsError::internal(e.to_string()))?;
                names.push(s["StreamName"].clone());
                summaries.push(json!({"StreamName":s["StreamName"],"StreamARN":s["StreamARN"],"StreamStatus":s["StreamStatus"],"StreamModeDetails":s["StreamModeDetails"],"StreamCreationTimestamp":s["StreamCreationTimestamp"]}));
            }
            let mut result =
                json!({"StreamNames":names,"StreamSummaries":summaries,"HasMoreStreams":more});
            if more {
                result["NextToken"] = json!(token(
                    tx,
                    ctx,
                    "streams",
                    json!({"start":names.last()}),
                    300.
                )?);
            }
            return Ok(result);
        }
        "DescribeLimits" => {
            let mut stmt =
                tx.prepare("SELECT metadata FROM streams WHERE account_id=?1 AND region=?2")?;
            let streams = stmt
                .query_map(params![ctx.account_id, ctx.region], |r| {
                    r.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut shards = 0;
            let mut demand = 0;
            for raw in streams {
                let stream: Value =
                    serde_json::from_str(&raw).map_err(|e| AwsError::internal(e.to_string()))?;
                shards += stream["Shards"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|s| open(s))
                    .count();
                if stream["StreamModeDetails"]["StreamMode"] == "ON_DEMAND" {
                    demand += 1;
                }
            }
            return Ok(
                json!({"ShardLimit":10000,"OpenShardCount":shards,"OnDemandStreamCount":demand,"OnDemandStreamCountLimit":50}),
            );
        }
        "GetRecords" => {
            let cursor = read_token(tx, ctx, "iterator", text(input, "ShardIterator"))?;
            let s = load(tx, ctx, &json!({"StreamARN":cursor["stream"]}))?;
            if input.get("StreamARN").is_some() && input["StreamARN"] != cursor["stream"] {
                return Err(invalid("Iterator does not belong to StreamARN"));
            }
            let limit = number(input, "Limit", 10000)?;
            if limit > 10000 {
                return Err(invalid("Limit must not exceed 10000"));
            }
            let stream = text(&cursor, "stream");
            let id = text(&cursor, "shard");
            let position = cursor["sequence"].as_i64().unwrap_or(1);
            let cutoff = now() - s["RetentionPeriodHours"].as_f64().unwrap_or(24.) * 3600.;
            let mut stmt=tx.prepare("SELECT sequence,data,partition_key,arrived FROM records WHERE stream_arn=?1 AND shard_id=?2 AND sequence>=?3 AND arrived>=?4 AND arrived>=?5 ORDER BY sequence LIMIT ?6")?;
            let rows = stmt
                .query_map(
                    params![
                        stream,
                        id,
                        position,
                        cutoff,
                        cursor["timestamp"].as_f64().unwrap_or(0.),
                        limit as i64
                    ],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Vec<u8>>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, f64>(3)?,
                        ))
                    },
                )?
                .collect::<Result<Vec<_>, _>>()?;
            let mut records = Vec::new();
            let mut size = 0;
            let mut next = position;
            let mut last_time = None;
            for (seq, data, key, time) in rows {
                if size + data.len() > 10 * 1024 * 1024 {
                    break;
                }
                size += data.len();
                next = seq + 1;
                last_time = Some(time);
                records.push(json!({"SequenceNumber":seq.to_string(),"Data":STANDARD.encode(data),"PartitionKey":key,"ApproximateArrivalTimestamp":time,"EncryptionType":s["EncryptionType"]}));
            }
            let latest:Option<f64>=tx.query_row("SELECT MAX(arrived) FROM records WHERE stream_arn=?1 AND shard_id=?2 AND arrived>=?3",params![stream,id,cutoff],|r|r.get(0))?;
            let lag = latest
                .zip(last_time)
                .map(|(latest, last)| ((latest - last).max(0.) * 1000.) as i64)
                .unwrap_or(0);
            let target = s["Shards"]
                .as_array()
                .unwrap()
                .iter()
                .find(|shard| text(shard, "ShardId") == id)
                .ok_or_else(|| invalid("Invalid iterator shard"))?;
            let mut result = json!({"Records":records,"MillisBehindLatest":lag});
            if open(target) || next < next_sequence(tx, stream, id)? {
                result["NextShardIterator"] = json!(token(
                    tx,
                    ctx,
                    "iterator",
                    json!({"stream":stream,"shard":id,"sequence":next,"timestamp":cursor["timestamp"]}),
                    300.
                )?);
            }
            return Ok(result);
        }
        _ => {}
    }
    // ARN-only consumer calls carry the stream identity inside the consumer ARN.
    let mut identity = input.clone();
    if let Some((stream, _)) = text(input, "ConsumerARN").split_once("/consumer/") {
        identity["StreamARN"] = json!(stream);
    }
    if op == "ListShards" && input.get("NextToken").is_some() {
        identity = read_token(tx, ctx, "shards", text(input, "NextToken"))?;
    }
    if op == "ListStreamConsumers" && input.get("NextToken").is_some() {
        identity = read_token(tx, ctx, "consumers", text(input, "NextToken"))?;
    }
    if ["DescribeStreamConsumer", "DeregisterStreamConsumer"].contains(&op)
        && !text(input, "ConsumerARN").is_empty()
        && !text(input, "ConsumerARN").contains("/consumer/")
    {
        return Err(err(
            "ResourceNotFoundException",
            format!(
                "Consumer {}, account {} not found.",
                text(input, "ConsumerARN"),
                ctx.account_id
            ),
        ));
    }
    let mut s = load(tx, ctx, &identity)?;
    let stream = text(&s, "StreamARN").to_string();
    let name = text(&s, "StreamName").to_string();
    let mut result = json!({});
    match op {
        "DeleteStream" => {
            tx.execute("DELETE FROM tokens WHERE json_extract(payload,'$.stream')=?1 OR json_extract(payload,'$.StreamARN')=?1", [&stream])?;
            tx.execute("DELETE FROM streams WHERE arn=?1", [&stream])?;
            return Ok(result);
        }
        "DescribeStream" | "DescribeStreamSummary" => {
            let mut desc = s.clone();
            desc.as_object_mut().unwrap().remove("Tags");
            desc.as_object_mut().unwrap().remove("Consumers");
            if op == "DescribeStreamSummary" {
                desc.as_object_mut().unwrap().remove("Shards");
                desc["OpenShardCount"] = json!(
                    s["Shards"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|s| open(s))
                        .count()
                );
                desc["ConsumerCount"] = json!(s["Consumers"].as_array().unwrap().len());
                return Ok(json!({"StreamDescriptionSummary":desc}));
            }
            let shards = s["Shards"].as_array().unwrap();
            let start = text(input, "ExclusiveStartShardId");
            let filtered: Vec<_> = shards
                .iter()
                .filter(|s| text(s, "ShardId") > start)
                .cloned()
                .collect();
            let limit = number(input, "Limit", 100)?;
            desc["HasMoreShards"] = json!(filtered.len() > limit);
            desc["Shards"] = json!(filtered.into_iter().take(limit).collect::<Vec<_>>());
            return Ok(json!({"StreamDescription":desc}));
        }
        "ListShards" => {
            let start = text(&identity, "ExclusiveStartShardId");
            let limit = number(input, "MaxResults", 1000)?;
            let filter = text(&input["ShardFilter"], "Type");
            if ![
                "",
                "AFTER_SHARD_ID",
                "AT_LATEST",
                "AT_TRIM_HORIZON",
                "FROM_TRIM_HORIZON",
            ]
            .contains(&filter)
            {
                return Err(AwsError::not_implemented(
                    "kinesis",
                    "ListShards timestamp filters",
                ));
            }
            let start = if filter == "AFTER_SHARD_ID" {
                text(&input["ShardFilter"], "ShardId")
            } else {
                start
            };
            let selected: Vec<_> = s["Shards"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|shard| {
                    text(shard, "ShardId") > start && (filter != "AT_LATEST" || open(shard))
                })
                .cloned()
                .collect();
            let more = selected.len() > limit;
            let page: Vec<_> = selected.into_iter().take(limit).collect();
            result = json!({"Shards":page});
            if more {
                result["NextToken"] = json!(token(
                    tx,
                    ctx,
                    "shards",
                    json!({"StreamARN":stream,"ExclusiveStartShardId":page.last().unwrap()["ShardId"]}),
                    300.
                )?);
            }
            return Ok(result);
        }
        "PutRecord" | "PutRecords" => {
            if op == "PutRecord" {
                return put(tx, &s, input, &validate_record(input, None)?);
            }
            let records = input["Records"]
                .as_array()
                .ok_or_else(|| invalid("Records required"))?;
            if records.is_empty() || records.len() > 500 {
                return Err(err(
                    "ValidationException",
                    "1 validation error detected: Value at 'records' failed to satisfy constraint: Member must have length less than or equal to 500",
                ));
            }
            let total: usize = records
                .iter()
                .map(|v| {
                    STANDARD
                        .decode(text(v, "Data"))
                        .map(|d| d.len() + text(v, "PartitionKey").len())
                        .unwrap_or(0)
                })
                .sum();
            if total > 5 * 1024 * 1024 {
                return Err(invalid("Records size exceeds 5 MB limit"));
            }
            let data = records
                .iter()
                .enumerate()
                .map(|(i, v)| validate_record(v, Some(i + 1)))
                .collect::<Result<Vec<_>, _>>()?;
            let records = records
                .iter()
                .zip(data)
                .map(|(v, d)| put(tx, &s, v, &d))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(
                json!({"FailedRecordCount":0,"Records":records,"EncryptionType":s["EncryptionType"]}),
            );
        }
        "GetShardIterator" => {
            let id = text(input, "ShardId");
            if !s["Shards"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| text(s, "ShardId") == id)
            {
                return Err(err(
                    "ResourceNotFoundException",
                    format!(
                        "Shard {id} in stream {name} under account {} does not exist",
                        ctx.account_id
                    ),
                ));
            }
            let kind = text(input, "ShardIteratorType");
            let mut timestamp = 0.;
            let seq = match kind {
                "TRIM_HORIZON" => 1,
                "LATEST" => next_sequence(tx, &stream, id)?,
                "AT_TIMESTAMP" => {
                    timestamp = input["Timestamp"]
                        .as_f64()
                        .ok_or_else(|| invalid("Timestamp required"))?;
                    1
                }
                "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER" => {
                    let n = text(input, "StartingSequenceNumber")
                        .parse::<i64>()
                        .map_err(|_| invalid("StartingSequenceNumber must be an integer"))?;
                    if n < 0 || n == i64::MAX {
                        return Err(invalid("Invalid StartingSequenceNumber"));
                    }
                    n + if kind == "AFTER_SEQUENCE_NUMBER" {
                        1
                    } else {
                        0
                    }
                }
                _ => return Err(invalid(format!("Invalid ShardIteratorType: {kind}"))),
            };
            return Ok(
                json!({"ShardIterator":token(tx,ctx,"iterator",json!({"stream":stream,"shard":id,"sequence":seq,"timestamp":timestamp}),300.)?}),
            );
        }
        "IncreaseStreamRetentionPeriod" | "DecreaseStreamRetentionPeriod" => {
            let n = number(input, "RetentionPeriodHours", 0)?;
            let old = s["RetentionPeriodHours"].as_u64().unwrap_or(24) as usize;
            if n < 24 {
                return Err(invalid(format!(
                    "Minimum allowed retention period is 24 hours. Requested retention period ({n} hours) is too short."
                )));
            }
            if n > 8760 {
                return Err(invalid(format!(
                    "Maximum allowed retention period is 8760 hours. Requested retention period ({n} hours) is too long."
                )));
            }
            if op.starts_with("Increase") && n < old {
                return Err(invalid(format!(
                    "Requested retention period ({n} hours) for stream {name} can not be shorter than existing retention period ({old} hours). Use DecreaseRetentionPeriod API."
                )));
            }
            if op.starts_with("Decrease") && n > old {
                return Err(invalid(format!(
                    "Requested retention period ({n} hours) for stream {name} can not be longer than existing retention period ({old} hours). Use IncreaseRetentionPeriod API."
                )));
            }
            s["RetentionPeriodHours"] = json!(n);
            tx.execute(
                "DELETE FROM records WHERE stream_arn=?1 AND arrived<?2",
                params![stream, now() - n as f64 * 3600.],
            )?;
        }
        "AddTagsToStream" => {
            let tags = input["Tags"]
                .as_object()
                .ok_or_else(|| invalid("Tags required"))?;
            for (k, v) in tags {
                s["Tags"][k] = v.clone();
            }
            if s["Tags"].as_object().unwrap().len() > 50 {
                return Err(err("LimitExceededException", "Maximum 50 tags per stream"));
            }
        }
        "RemoveTagsFromStream" => {
            if let Some(keys) = input["TagKeys"].as_array() {
                for key in keys {
                    s["Tags"]
                        .as_object_mut()
                        .unwrap()
                        .remove(key.as_str().unwrap_or(""));
                }
            }
        }
        "ListTagsForStream" => {
            let start = text(input, "ExclusiveStartTagKey");
            let limit = number(input, "Limit", 10)?;
            let tags: Vec<_> = s["Tags"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.as_str() > start)
                .map(|(k, v)| json!({"Key":k,"Value":v}))
                .collect();
            return Ok(
                json!({"HasMoreTags":tags.len()>limit,"Tags":tags.into_iter().take(limit).collect::<Vec<_>>()}),
            );
        }
        "UpdateStreamMode" => {
            let mode = text(&input["StreamModeDetails"], "StreamMode");
            if !["ON_DEMAND", "PROVISIONED"].contains(&mode) {
                return Err(invalid("Invalid stream mode"));
            }
            s["StreamModeDetails"] = input["StreamModeDetails"].clone();
        }
        "StartStreamEncryption" => {
            if text(input, "EncryptionType") != "KMS" || text(input, "KeyId").is_empty() {
                return Err(invalid("KMS EncryptionType and KeyId required"));
            }
            s["EncryptionType"] = json!("KMS");
            s["KeyId"] = input["KeyId"].clone();
        }
        "StopStreamEncryption" => {
            s["EncryptionType"] = json!("NONE");
            s.as_object_mut().unwrap().remove("KeyId");
        }
        "EnableEnhancedMonitoring" | "DisableEnhancedMonitoring" => {
            let old = s["EnhancedMonitoring"][0]["ShardLevelMetrics"]
                .as_array()
                .unwrap()
                .clone();
            let requested = input["ShardLevelMetrics"]
                .as_array()
                .ok_or_else(|| invalid("ShardLevelMetrics required"))?;
            let mut metrics = old.clone();
            if op.starts_with("Enable") {
                for metric in requested {
                    if !metrics.contains(metric) {
                        metrics.push(metric.clone());
                    }
                }
            } else if requested.contains(&json!("ALL")) {
                metrics.clear();
            } else {
                metrics.retain(|m| !requested.contains(m));
            }
            s["EnhancedMonitoring"] = json!([{"ShardLevelMetrics":metrics}]);
            result = json!({"StreamName":name,"StreamARN":stream,"CurrentShardLevelMetrics":old,"DesiredShardLevelMetrics":metrics});
        }
        "RegisterStreamConsumer" => {
            let name = text(input, "ConsumerName");
            let consumers = s["Consumers"].as_array_mut().unwrap();
            if consumers.iter().any(|c| text(c, "ConsumerName") == name) {
                return Err(err("ResourceInUseException", "Consumer already exists"));
            }
            if consumers.len() >= 20 {
                return Err(err(
                    "LimitExceededException",
                    "Maximum 20 consumers per stream",
                ));
            }
            let consumer = json!({"ConsumerName":name,"ConsumerARN":format!("{stream}/consumer/{name}"),"ConsumerStatus":"ACTIVE","ConsumerCreationTimestamp":now()});
            consumers.push(consumer.clone());
            result = json!({"Consumer":consumer});
        }
        "ListStreamConsumers" => {
            let limit = number(input, "MaxResults", 100)?;
            let start = identity["offset"].as_u64().unwrap_or(0) as usize;
            let consumers = s["Consumers"].as_array().unwrap();
            result =
                json!({"Consumers":consumers.iter().skip(start).take(limit).collect::<Vec<_>>()});
            if start + limit < consumers.len() {
                result["NextToken"] = json!(token(
                    tx,
                    ctx,
                    "consumers",
                    json!({"StreamARN":stream,"offset":start+limit}),
                    300.
                )?);
            }
            return Ok(result);
        }
        "DescribeStreamConsumer" | "DeregisterStreamConsumer" => {
            let consumers = s["Consumers"].as_array_mut().unwrap();
            let name = text(input, "ConsumerName");
            let id = text(input, "ConsumerARN");
            let index = consumers
                .iter()
                .position(|c| {
                    if id.is_empty() {
                        text(c, "ConsumerName") == name
                    } else {
                        text(c, "ConsumerARN") == id
                    }
                })
                .ok_or_else(|| {
                    err(
                        "ResourceNotFoundException",
                        format!(
                            "Consumer {}, account {} not found.",
                            if id.is_empty() { name } else { id },
                            ctx.account_id
                        ),
                    )
                })?;
            if op.starts_with("Describe") {
                let mut c = consumers[index].clone();
                c["StreamARN"] = json!(stream);
                return Ok(json!({"ConsumerDescription":c}));
            }
            consumers.remove(index);
        }
        "UpdateShardCount" => {
            if s["StreamModeDetails"]["StreamMode"] == "ON_DEMAND" {
                return Err(err(
                    "ValidationException",
                    format!(
                        "Request is invalid. Stream {name} under account {} is in On-Demand mode.",
                        ctx.account_id
                    ),
                ));
            }
            if text(input, "ScalingType") != "UNIFORM_SCALING" {
                return Err(invalid("Invalid ScalingType"));
            }
            let target = number(input, "TargetShardCount", 0)?;
            if target == 0 || target > 10000 {
                return Err(invalid("TargetShardCount must be between 1 and 10000"));
            }
            let shards = s["Shards"].as_array_mut().unwrap();
            let current = shards.iter().filter(|s| open(s)).count();
            // Match the observable split-all, then merge-adjacent scaling strategy.
            while shards.iter().filter(|s| open(s)).count() < target {
                let indices: Vec<_> = shards
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| open(s))
                    .map(|(i, _)| i)
                    .collect();
                for index in indices {
                    let parent = text(&shards[index], "ShardId").to_string();
                    let start = hash(&shards[index], "StartingHashKey")?;
                    let end = hash(&shards[index], "EndingHashKey")?;
                    let boundary = start + (end - start) / 2 + 1;
                    if boundary <= start {
                        return Err(invalid("Shard hash range cannot be split further"));
                    }
                    close(tx, &stream, &mut shards[index])?;
                    for (a, b) in [(start, boundary - 1), (boundary, end)] {
                        let mut child = shard(shards.len(), a, b);
                        child["ParentShardId"] = json!(parent);
                        shards.push(child);
                    }
                }
            }
            while shards.iter().filter(|s| open(s)).count() > target {
                let mut pieces: Vec<_> = shards
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| open(s))
                    .map(|(i, s)| (hash(s, "StartingHashKey").unwrap(), i))
                    .collect();
                pieces.sort_unstable();
                let merges = pieces.len() - target;
                for pair in pieces.as_chunks::<2>().0.iter().take(merges) {
                    let a = pair[0].1;
                    let b = pair[1].1;
                    let parent = text(&shards[a], "ShardId").to_string();
                    let adjacent = text(&shards[b], "ShardId").to_string();
                    let low = hash(&shards[a], "StartingHashKey")?;
                    let high = hash(&shards[b], "EndingHashKey")?;
                    close(tx, &stream, &mut shards[a])?;
                    close(tx, &stream, &mut shards[b])?;
                    let mut child = shard(shards.len(), low, high);
                    child["ParentShardId"] = json!(parent);
                    child["AdjacentParentShardId"] = json!(adjacent);
                    shards.push(child);
                }
            }
            result = json!({"StreamName":name,"StreamARN":stream,"CurrentShardCount":current,"TargetShardCount":target});
        }
        "SplitShard" | "MergeShards" => {
            let key = if op == "SplitShard" {
                "ShardToSplit"
            } else {
                "ShardToMerge"
            };
            let id = text(input, key);
            let shards = s["Shards"].as_array_mut().unwrap();
            if id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            {
                return Err(err(
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{id}' at 'shardToSplit' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
                    ),
                ));
            }
            let index = shards
                .iter()
                .position(|s| text(s, "ShardId") == id)
                .ok_or_else(|| {
                    err(
                        "ResourceNotFoundException",
                        format!(
                            "Could not find shard {id} in stream {name} under account {}.",
                            ctx.account_id
                        ),
                    )
                })?;
            if !open(&shards[index]) {
                return Err(invalid(format!(
                    "Shard {id} in stream {name} under account {} has already been merged or split, and thus is not eligible for merging or splitting.",
                    ctx.account_id
                )));
            }
            let start = hash(&shards[index], "StartingHashKey")?;
            let end = hash(&shards[index], "EndingHashKey")?;
            if op == "SplitShard" {
                let value = text(input, "NewStartingHashKey");
                let new=value.parse::<u128>().map_err(|_|err("ValidationException",format!("1 validation error detected: Value '{value}' at 'newStartingHashKey' failed to satisfy constraint: Member must satisfy regular expression pattern: 0|([1-9]\\d{{0,38}})")))?;
                if new <= start.saturating_add(1) || new >= end {
                    return Err(invalid(format!(
                        "NewStartingHashKey {new} used in SplitShard() on shard {id} in stream {name} under account {} is not both greater than one plus the shard's StartingHashKey {start} and less than the shard's EndingHashKey {end}.",
                        ctx.account_id
                    )));
                }
                close(tx, &stream, &mut shards[index])?;
                for (a, b) in [(start, new - 1), (new, end)] {
                    let mut child = shard(shards.len(), a, b);
                    child["ParentShardId"] = json!(id);
                    shards.push(child);
                }
            } else {
                let other = text(input, "AdjacentShardToMerge");
                let adjacent = shards
                    .iter()
                    .position(|s| text(s, "ShardId") == other && open(s))
                    .ok_or_else(|| invalid(other))?;
                let a = hash(&shards[adjacent], "StartingHashKey")?;
                let b = hash(&shards[adjacent], "EndingHashKey")?;
                if end.checked_add(1) != Some(a) && b.checked_add(1) != Some(start) {
                    return Err(invalid(other));
                }
                close(tx, &stream, &mut shards[index])?;
                close(tx, &stream, &mut shards[adjacent])?;
                let mut child = shard(shards.len(), start.min(a), end.max(b));
                child["ParentShardId"] = json!(id);
                child["AdjacentParentShardId"] = json!(other);
                shards.push(child);
            }
        }
        _ => return Err(AwsError::not_implemented("kinesis", op)),
    }
    save(tx, &s)?;
    Ok(result)
}

impl Service for Kinesis {
    fn update_shard_count(
        &self,
        ctx: &RequestContext,
        input: UpdateShardCountInput,
    ) -> Result<UpdateShardCountOutput, AwsError> {
        UpdateShardCountOutput::from_json(
            &self.call(ctx, "UpdateShardCount", &input.to_json())?,
            "",
        )
    }

    fn add_tags_to_stream(
        &self,
        ctx: &RequestContext,
        input: AddTagsToStreamInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "AddTagsToStream", &input.to_json())?;
        Ok(())
    }
    fn create_stream(
        &self,
        ctx: &RequestContext,
        input: CreateStreamInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "CreateStream", &input.to_json())?;
        Ok(())
    }
    fn decrease_stream_retention_period(
        &self,
        ctx: &RequestContext,
        input: DecreaseStreamRetentionPeriodInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "DecreaseStreamRetentionPeriod", &input.to_json())?;
        Ok(())
    }
    fn delete_stream(
        &self,
        ctx: &RequestContext,
        input: DeleteStreamInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "DeleteStream", &input.to_json())?;
        Ok(())
    }
    fn deregister_stream_consumer(
        &self,
        ctx: &RequestContext,
        input: DeregisterStreamConsumerInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "DeregisterStreamConsumer", &input.to_json())?;
        Ok(())
    }
    fn describe_limits(
        &self,
        ctx: &RequestContext,
        input: DescribeLimitsInput,
    ) -> Result<DescribeLimitsOutput, AwsError> {
        let result = self.call(ctx, "DescribeLimits", &input.to_json())?;
        DescribeLimitsOutput::from_json(&result, "")
    }
    fn describe_stream(
        &self,
        ctx: &RequestContext,
        input: DescribeStreamInput,
    ) -> Result<DescribeStreamOutput, AwsError> {
        let result = self.call(ctx, "DescribeStream", &input.to_json())?;
        DescribeStreamOutput::from_json(&result, "")
    }
    fn describe_stream_consumer(
        &self,
        ctx: &RequestContext,
        input: DescribeStreamConsumerInput,
    ) -> Result<DescribeStreamConsumerOutput, AwsError> {
        let result = self.call(ctx, "DescribeStreamConsumer", &input.to_json())?;
        DescribeStreamConsumerOutput::from_json(&result, "")
    }
    fn describe_stream_summary(
        &self,
        ctx: &RequestContext,
        input: DescribeStreamSummaryInput,
    ) -> Result<DescribeStreamSummaryOutput, AwsError> {
        let result = self.call(ctx, "DescribeStreamSummary", &input.to_json())?;
        DescribeStreamSummaryOutput::from_json(&result, "")
    }
    fn disable_enhanced_monitoring(
        &self,
        ctx: &RequestContext,
        input: DisableEnhancedMonitoringInput,
    ) -> Result<EnhancedMonitoringOutput, AwsError> {
        let result = self.call(ctx, "DisableEnhancedMonitoring", &input.to_json())?;
        EnhancedMonitoringOutput::from_json(&result, "")
    }
    fn enable_enhanced_monitoring(
        &self,
        ctx: &RequestContext,
        input: EnableEnhancedMonitoringInput,
    ) -> Result<EnhancedMonitoringOutput, AwsError> {
        let result = self.call(ctx, "EnableEnhancedMonitoring", &input.to_json())?;
        EnhancedMonitoringOutput::from_json(&result, "")
    }
    fn get_records(
        &self,
        ctx: &RequestContext,
        input: GetRecordsInput,
    ) -> Result<GetRecordsOutput, AwsError> {
        let result = self.call(ctx, "GetRecords", &input.to_json())?;
        GetRecordsOutput::from_json(&result, "")
    }
    fn get_shard_iterator(
        &self,
        ctx: &RequestContext,
        input: GetShardIteratorInput,
    ) -> Result<GetShardIteratorOutput, AwsError> {
        let result = self.call(ctx, "GetShardIterator", &input.to_json())?;
        GetShardIteratorOutput::from_json(&result, "")
    }
    fn increase_stream_retention_period(
        &self,
        ctx: &RequestContext,
        input: IncreaseStreamRetentionPeriodInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "IncreaseStreamRetentionPeriod", &input.to_json())?;
        Ok(())
    }
    fn list_shards(
        &self,
        ctx: &RequestContext,
        input: ListShardsInput,
    ) -> Result<ListShardsOutput, AwsError> {
        let result = self.call(ctx, "ListShards", &input.to_json())?;
        ListShardsOutput::from_json(&result, "")
    }
    fn list_stream_consumers(
        &self,
        ctx: &RequestContext,
        input: ListStreamConsumersInput,
    ) -> Result<ListStreamConsumersOutput, AwsError> {
        let result = self.call(ctx, "ListStreamConsumers", &input.to_json())?;
        ListStreamConsumersOutput::from_json(&result, "")
    }
    fn list_streams(
        &self,
        ctx: &RequestContext,
        input: ListStreamsInput,
    ) -> Result<ListStreamsOutput, AwsError> {
        let result = self.call(ctx, "ListStreams", &input.to_json())?;
        ListStreamsOutput::from_json(&result, "")
    }
    fn list_tags_for_stream(
        &self,
        ctx: &RequestContext,
        input: ListTagsForStreamInput,
    ) -> Result<ListTagsForStreamOutput, AwsError> {
        let result = self.call(ctx, "ListTagsForStream", &input.to_json())?;
        ListTagsForStreamOutput::from_json(&result, "")
    }
    fn merge_shards(&self, ctx: &RequestContext, input: MergeShardsInput) -> Result<(), AwsError> {
        self.call(ctx, "MergeShards", &input.to_json())?;
        Ok(())
    }
    fn put_record(
        &self,
        ctx: &RequestContext,
        input: PutRecordInput,
    ) -> Result<PutRecordOutput, AwsError> {
        let result = self.call(ctx, "PutRecord", &input.to_json())?;
        PutRecordOutput::from_json(&result, "")
    }
    fn put_records(
        &self,
        ctx: &RequestContext,
        input: PutRecordsInput,
    ) -> Result<PutRecordsOutput, AwsError> {
        let result = self.call(ctx, "PutRecords", &input.to_json())?;
        PutRecordsOutput::from_json(&result, "")
    }
    fn register_stream_consumer(
        &self,
        ctx: &RequestContext,
        input: RegisterStreamConsumerInput,
    ) -> Result<RegisterStreamConsumerOutput, AwsError> {
        let result = self.call(ctx, "RegisterStreamConsumer", &input.to_json())?;
        RegisterStreamConsumerOutput::from_json(&result, "")
    }
    fn remove_tags_from_stream(
        &self,
        ctx: &RequestContext,
        input: RemoveTagsFromStreamInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "RemoveTagsFromStream", &input.to_json())?;
        Ok(())
    }
    fn split_shard(&self, ctx: &RequestContext, input: SplitShardInput) -> Result<(), AwsError> {
        self.call(ctx, "SplitShard", &input.to_json())?;
        Ok(())
    }
    fn start_stream_encryption(
        &self,
        ctx: &RequestContext,
        input: StartStreamEncryptionInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "StartStreamEncryption", &input.to_json())?;
        Ok(())
    }
    fn stop_stream_encryption(
        &self,
        ctx: &RequestContext,
        input: StopStreamEncryptionInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "StopStreamEncryption", &input.to_json())?;
        Ok(())
    }
    fn update_stream_mode(
        &self,
        ctx: &RequestContext,
        input: UpdateStreamModeInput,
    ) -> Result<(), AwsError> {
        self.call(ctx, "UpdateStreamMode", &input.to_json())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(account: &str, region: &str) -> RequestContext {
        RequestContext {
            account_id: account.into(),
            region: region.into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn create(k: &Kinesis, ctx: &RequestContext) {
        k.call(
            ctx,
            "CreateStream",
            &json!({"StreamName":"test","ShardCount":2}),
        )
        .unwrap();
    }
    fn iterator(k: &Kinesis, ctx: &RequestContext, shard: &str) -> String {
        k.call(
            ctx,
            "GetShardIterator",
            &json!({"StreamName":"test","ShardId":shard,"ShardIteratorType":"TRIM_HORIZON"}),
        )
        .unwrap()["ShardIterator"]
            .as_str()
            .unwrap()
            .into()
    }
    #[test]
    fn hash_ranges_cover_entire_keyspace_without_gaps() {
        for count in [1, 2, 3, 10, 13, 10000] {
            let ranges = ranges(count);
            assert_eq!(ranges[0].0, 0);
            assert_eq!(ranges.last().unwrap().1, u128::MAX);
            for pair in ranges.windows(2) {
                assert_eq!(pair[0].1 + 1, pair[1].0);
            }
            let widths: Vec<_> = ranges.iter().map(|(a, b)| b - a).collect();
            assert!(widths.iter().max().unwrap() - widths.iter().min().unwrap() <= 1);
        }
        assert_eq!(ranges(2)[1].0, 1u128 << 127);
    }
    #[test]
    fn batch_validation_rolls_back_and_expired_iterators_are_rejected() {
        let store = Store::ephemeral();
        let k = Kinesis::new(&store).unwrap();
        let ctx = context("123", "us-east-1");
        create(&k, &ctx);
        let batch = json!({"StreamName":"test","Records":[{"Data":"YQ==","PartitionKey":"first"},{"Data":"Yg==","PartitionKey":"second","ExplicitHashKey":"bad"}]});
        assert!(k.call(&ctx, "PutRecords", &batch).is_err());
        let it = iterator(&k, &ctx, "shardId-000000000000");
        assert_eq!(
            k.call(&ctx, "GetRecords", &json!({"ShardIterator":it}))
                .unwrap()["Records"],
            json!([])
        );
        k.db.transaction(|tx| {
            tx.execute("UPDATE tokens SET expires=0", [])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            k.call(&ctx, "GetRecords", &json!({"ShardIterator":it}))
                .unwrap_err()
                .code,
            "ExpiredIteratorException"
        );
    }
    #[test]
    fn expiry_keeps_sequence_numbers_and_does_not_revive_records_on_retention_increase() {
        let store = Store::ephemeral();
        let k = Kinesis::new(&store).unwrap();
        let ctx = context("123", "us-east-1");
        create(&k, &ctx);
        let write =
            json!({"StreamName":"test","Data":"YQ==","PartitionKey":"key","ExplicitHashKey":"0"});
        assert_eq!(
            k.call(&ctx, "PutRecord", &write).unwrap()["SequenceNumber"],
            "1"
        );
        k.db.transaction(|tx| {
            tx.execute("UPDATE records SET arrived=?1", [now() - 25. * 3600.])?;
            Ok(())
        })
        .unwrap();
        k.call(
            &ctx,
            "IncreaseStreamRetentionPeriod",
            &json!({"StreamName":"test","RetentionPeriodHours":48}),
        )
        .unwrap();
        assert_eq!(
            k.call(&ctx, "PutRecord", &write).unwrap()["SequenceNumber"],
            "2"
        );
        let it = iterator(&k, &ctx, "shardId-000000000000");
        let records = k
            .call(&ctx, "GetRecords", &json!({"ShardIterator":it}))
            .unwrap();
        assert_eq!(records["Records"].as_array().unwrap().len(), 1);
        assert_eq!(records["Records"][0]["SequenceNumber"], "2");
    }
    #[test]
    fn iterators_are_scoped_and_deleted_stream_cursors_cannot_read_a_recreated_stream() {
        let store = Store::ephemeral();
        let k = Kinesis::new(&store).unwrap();
        let ctx = context("123", "us-east-1");
        create(&k, &ctx);
        let it = iterator(&k, &ctx, "shardId-000000000000");
        for other in [context("456", "us-east-1"), context("123", "us-west-2")] {
            create(&k, &other);
            assert!(
                k.call(&other, "GetRecords", &json!({"ShardIterator":it}))
                    .is_err()
            );
        }
        k.call(&ctx, "DeleteStream", &json!({"StreamName":"test"}))
            .unwrap();
        create(&k, &ctx);
        assert!(
            k.call(&ctx, "GetRecords", &json!({"ShardIterator":it}))
                .is_err()
        );
    }
    #[test]
    fn closed_shards_drain_and_new_writes_route_to_children() {
        let store = Store::ephemeral();
        let k = Kinesis::new(&store).unwrap();
        let ctx = context("123", "us-east-1");
        create(&k, &ctx);
        let write =
            json!({"StreamName":"test","Data":"YQ==","PartitionKey":"key","ExplicitHashKey":"0"});
        k.call(&ctx, "PutRecord", &write).unwrap();
        let it = iterator(&k, &ctx, "shardId-000000000000");
        k.call(&ctx,"SplitShard",&json!({"StreamName":"test","ShardToSplit":"shardId-000000000000","NewStartingHashKey":"100"})).unwrap();
        assert_eq!(
            k.call(&ctx, "PutRecord", &write).unwrap()["ShardId"],
            "shardId-000000000002"
        );
        let page = k
            .call(&ctx, "GetRecords", &json!({"ShardIterator":it}))
            .unwrap();
        assert_eq!(page["Records"].as_array().unwrap().len(), 1);
        assert!(page.get("NextShardIterator").is_none());
    }
}
