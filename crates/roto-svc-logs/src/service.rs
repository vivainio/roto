use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::generated::*;
use crate::schema::*;
use diesel::prelude::*;
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;

use crate::MIGRATIONS;

#[derive(Queryable, Selectable)]
#[diesel(table_name = log_groups)]
struct GroupRow {
    log_group_name: String,
    creation_time: i64,
    retention_in_days: Option<i32>,
    tags: String,
    stored_bytes: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = log_events)]
struct EventRow {
    id: i64,
    log_stream_name: String,
    timestamp: i64,
    ingestion_time: i64,
    message: String,
}

pub struct CloudWatchLogs {
    db: Arc<Db>,
}

impl CloudWatchLogs {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("logs", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(log_events::table).execute(tx)?;
            diesel::delete(log_groups::table).execute(tx)?;
            Ok(())
        })
    }

    /// Store local Lambda executor output in `/aws/lambda/<function>`.
    pub fn record_lambda_invocation(
        &self,
        function_arn: &str,
        invocation_id: &str,
        message: &str,
    ) -> Result<(), AwsError> {
        let parts: Vec<_> = function_arn.splitn(6, ':').collect();
        if parts.len() != 6 || parts[0] != "arn" || parts[2] != "lambda" {
            return Err(AwsError::internal(
                "invalid Lambda function ARN for log delivery",
            ));
        }
        let region = parts[3];
        let account = parts[4];
        let Some(function_name) = parts[5].strip_prefix("function:") else {
            return Err(AwsError::internal(
                "invalid Lambda function ARN for log delivery",
            ));
        };
        let group_name = format!("/aws/lambda/{function_name}");
        let now = now_ms();
        let date = Timestamp(now / 1000).to_iso8601();
        let date = date.get(..10).unwrap_or("1970-01-01").replace('-', "/");
        let stream_name = format!("{date}/[$LATEST]{invocation_id}");

        self.db.transaction(|tx| {
            ensure_group(tx, account, region, &group_name, now)?;
            for (index, line) in message.lines().enumerate() {
                insert_event(
                    tx,
                    account,
                    region,
                    &group_name,
                    &stream_name,
                    now + index as i64,
                    now,
                    line,
                )?;
            }
            Ok(())
        })
    }

    fn group(&self, ctx: &RequestContext, name: &str) -> Result<GroupRow, AwsError> {
        self.db
            .read(|c| {
                Ok(log_groups::table
                    .filter(log_groups::account_id.eq(&ctx.account_id))
                    .filter(log_groups::region.eq(&ctx.region))
                    .filter(log_groups::log_group_name.eq(name))
                    .select(GroupRow::as_select())
                    .first::<GroupRow>(c)
                    .optional()?)
            })?
            .ok_or_else(|| not_found(&format!("Log group {name}")))
    }

    fn update_tags(
        &self,
        ctx: &RequestContext,
        arn: &str,
        update: impl FnOnce(&mut BTreeMap<String, String>),
    ) -> Result<(), AwsError> {
        let name = group_name(ctx, None, Some(arn))?;
        self.db.transaction(|tx| {
            let row = require_group(tx, &ctx.account_id, &ctx.region, &name)?;
            let mut tags: BTreeMap<String, String> =
                serde_json::from_str(&row.tags).unwrap_or_default();
            update(&mut tags);
            diesel::update(
                log_groups::table
                    .filter(log_groups::account_id.eq(&ctx.account_id))
                    .filter(log_groups::region.eq(&ctx.region))
                    .filter(log_groups::log_group_name.eq(&name)),
            )
            .set(log_groups::tags.eq(serde_json::to_string(&tags).unwrap_or_else(|_| "{}".into())))
            .execute(tx)?;
            Ok(())
        })
    }
}

impl Service for CloudWatchLogs {
    fn create_log_group(
        &self,
        ctx: &RequestContext,
        input: CreateLogGroupRequest,
    ) -> Result<(), AwsError> {
        validate_group_name(&input.log_group_name)?;
        if input.kms_key_id.is_some()
            || input.deletion_protection_enabled == Some(true)
            || input
                .log_group_class
                .as_deref()
                .is_some_and(|class| class != "STANDARD")
        {
            return Err(AwsError::not_implemented(
                "logs",
                "KMS, deletion protection, or non-standard log group classes",
            ));
        }
        let tags = serde_json::to_string(&input.tags).unwrap_or_else(|_| "{}".into());
        self.db.transaction(|tx| {
            if group_exists(tx, &ctx.account_id, &ctx.region, &input.log_group_name)? {
                return Err(err(
                    "ResourceAlreadyExistsException",
                    format!("Log group {} already exists", input.log_group_name),
                ));
            }
            diesel::insert_into(log_groups::table)
                .values((
                    log_groups::account_id.eq(&ctx.account_id),
                    log_groups::region.eq(&ctx.region),
                    log_groups::log_group_name.eq(&input.log_group_name),
                    log_groups::creation_time.eq(now_ms()),
                    log_groups::tags.eq(tags),
                    log_groups::stored_bytes.eq(0_i64),
                ))
                .execute(tx)?;
            Ok(())
        })
    }

    fn delete_log_group(
        &self,
        ctx: &RequestContext,
        input: DeleteLogGroupRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let deleted = diesel::delete(
                log_groups::table
                    .filter(log_groups::account_id.eq(&ctx.account_id))
                    .filter(log_groups::region.eq(&ctx.region))
                    .filter(log_groups::log_group_name.eq(&input.log_group_name)),
            )
            .execute(tx)?;
            if deleted == 0 {
                return Err(not_found(&format!("Log group {}", input.log_group_name)));
            }
            Ok(())
        })
    }

    fn describe_log_groups(
        &self,
        ctx: &RequestContext,
        input: DescribeLogGroupsRequest,
    ) -> Result<DescribeLogGroupsResponse, AwsError> {
        if input.include_linked_accounts == Some(true)
            || !input.account_identifiers.is_empty()
            || !input.log_group_identifiers.is_empty()
            || input.log_group_name_pattern.is_some()
        {
            return Err(AwsError::not_implemented(
                "logs",
                "linked-account and pattern log group searches",
            ));
        }
        if input
            .log_group_class
            .as_deref()
            .is_some_and(|class| class != "STANDARD")
        {
            return Err(invalid("Only STANDARD log groups are supported"));
        }
        let rows = self.db.read(|c| {
            let mut query = log_groups::table
                .filter(log_groups::account_id.eq(&ctx.account_id))
                .filter(log_groups::region.eq(&ctx.region))
                .into_boxed();
            if let Some(prefix) = &input.log_group_name_prefix {
                query =
                    query.filter(substr(log_groups::log_group_name, 1, length(prefix)).eq(prefix));
            }
            Ok(query
                .order(log_groups::log_group_name.asc())
                .select(GroupRow::as_select())
                .load::<GroupRow>(c)?)
        })?;
        let (rows, next_token) = page(rows, input.next_token.as_deref(), input.limit, 50)?;
        Ok(DescribeLogGroupsResponse {
            log_groups: rows.iter().map(|row| group_shape(ctx, row)).collect(),
            next_token,
        })
    }

    fn put_retention_policy(
        &self,
        ctx: &RequestContext,
        input: PutRetentionPolicyRequest,
    ) -> Result<(), AwsError> {
        if ![
            1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557,
            2922, 3288, 3653,
        ]
        .contains(&input.retention_in_days)
        {
            return Err(invalid("Unsupported retentionInDays value"));
        }
        self.db.transaction(|tx| {
            let updated = diesel::update(
                log_groups::table
                    .filter(log_groups::account_id.eq(&ctx.account_id))
                    .filter(log_groups::region.eq(&ctx.region))
                    .filter(log_groups::log_group_name.eq(&input.log_group_name)),
            )
            .set(log_groups::retention_in_days.eq(Some(input.retention_in_days)))
            .execute(tx)?;
            if updated == 0 {
                return Err(not_found(&format!("Log group {}", input.log_group_name)));
            }
            Ok(())
        })
    }

    fn delete_retention_policy(
        &self,
        ctx: &RequestContext,
        input: DeleteRetentionPolicyRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let updated = diesel::update(
                log_groups::table
                    .filter(log_groups::account_id.eq(&ctx.account_id))
                    .filter(log_groups::region.eq(&ctx.region))
                    .filter(log_groups::log_group_name.eq(&input.log_group_name)),
            )
            .set(log_groups::retention_in_days.eq(None::<i32>))
            .execute(tx)?;
            if updated == 0 {
                return Err(not_found(&format!("Log group {}", input.log_group_name)));
            }
            Ok(())
        })
    }

    fn filter_log_events(
        &self,
        ctx: &RequestContext,
        input: FilterLogEventsRequest,
    ) -> Result<FilterLogEventsResponse, AwsError> {
        let name = group_name(
            ctx,
            input.log_group_name.as_deref(),
            input.log_group_identifier.as_deref(),
        )?;
        self.group(ctx, &name)?;
        let mut query = log_events::table
            .filter(log_events::account_id.eq(&ctx.account_id))
            .filter(log_events::region.eq(&ctx.region))
            .filter(log_events::log_group_name.eq(&name))
            .into_boxed();
        if let Some(start) = input.start_time {
            query = query.filter(log_events::timestamp.ge(start));
        }
        if let Some(end) = input.end_time {
            query = query.filter(log_events::timestamp.lt(end));
        }
        if !input.log_stream_names.is_empty() {
            query = query.filter(log_events::log_stream_name.eq_any(&input.log_stream_names));
        } else if let Some(prefix) = &input.log_stream_name_prefix {
            query = query.filter(substr(log_events::log_stream_name, 1, length(prefix)).eq(prefix));
        }
        let mut events = self.db.read(|c| {
            Ok(query
                .order((log_events::timestamp.asc(), log_events::id.asc()))
                .select(EventRow::as_select())
                .load::<EventRow>(c)?)
        })?;
        let pattern = input
            .filter_pattern
            .as_deref()
            .unwrap_or("")
            .trim_matches('"');
        if !pattern.is_empty() {
            events.retain(|event| event.message.contains(pattern));
        }
        let limit = event_limit(input.limit)?;
        let start = input
            .next_token
            .as_deref()
            .map(parse_token)
            .transpose()?
            .unwrap_or(0)
            .min(events.len());
        let end = start.saturating_add(limit).min(events.len());
        let page = &events[start..end];
        let next_token = (end < events.len()).then(|| end.to_string());
        let stream_names: BTreeSet<_> = page
            .iter()
            .map(|event| event.log_stream_name.clone())
            .collect();
        Ok(FilterLogEventsResponse {
            events: page
                .iter()
                .map(|event| FilteredLogEvent {
                    event_id: Some(event.id.to_string()),
                    ingestion_time: Some(event.ingestion_time),
                    log_stream_name: Some(event.log_stream_name.clone()),
                    message: Some(event.message.clone()),
                    timestamp: Some(event.timestamp),
                })
                .collect(),
            next_token,
            searched_log_streams: stream_names
                .into_iter()
                .map(|name| SearchedLogStream {
                    log_stream_name: Some(name),
                    searched_completely: Some(true),
                })
                .collect(),
        })
    }

    fn tag_resource(
        &self,
        ctx: &RequestContext,
        input: TagResourceRequest,
    ) -> Result<(), AwsError> {
        self.update_tags(ctx, &input.resource_arn, |tags| {
            tags.extend(input.tags.clone());
        })
    }

    fn untag_resource(
        &self,
        ctx: &RequestContext,
        input: UntagResourceRequest,
    ) -> Result<(), AwsError> {
        self.update_tags(ctx, &input.resource_arn, |tags| {
            for key in &input.tag_keys {
                tags.remove(key);
            }
        })
    }

    fn list_tags_for_resource(
        &self,
        ctx: &RequestContext,
        input: ListTagsForResourceRequest,
    ) -> Result<ListTagsForResourceResponse, AwsError> {
        let row = self.group(ctx, &group_name(ctx, None, Some(&input.resource_arn))?)?;
        Ok(ListTagsForResourceResponse {
            tags: serde_json::from_str(&row.tags).unwrap_or_default(),
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

fn invalid(message: impl Into<String>) -> AwsError {
    err("InvalidParameterException", message)
}

fn not_found(resource: &str) -> AwsError {
    err(
        "ResourceNotFoundException",
        format!("{resource} does not exist"),
    )
}

fn validate_group_name(name: &str) -> Result<(), AwsError> {
    if name.is_empty() || name.len() > 512 || name.contains('\0') {
        return Err(invalid("logGroupName must be between 1 and 512 characters"));
    }
    Ok(())
}

fn group_name(
    ctx: &RequestContext,
    name: Option<&str>,
    identifier: Option<&str>,
) -> Result<String, AwsError> {
    if let Some(name) = name {
        return Ok(name.to_owned());
    }
    let Some(identifier) = identifier else {
        return Err(AwsError::missing_parameter("logGroupName"));
    };
    if let Some((_, name)) = identifier.rsplit_once(":log-group:") {
        let expected = format!(":logs:{}:{}:log-group:", ctx.region, ctx.account_id);
        if !identifier.starts_with("arn:") || !identifier.contains(&expected) {
            return Err(not_found("Log group"));
        }
        return Ok(name.strip_suffix(":*").unwrap_or(name).to_owned());
    }
    if identifier.starts_with("arn:") {
        return Err(not_found("Log group"));
    }
    Ok(identifier.to_owned())
}

fn group_exists(
    tx: &mut diesel::sqlite::SqliteConnection,
    account: &str,
    region: &str,
    name: &str,
) -> Result<bool, AwsError> {
    Ok(log_groups::table
        .filter(log_groups::account_id.eq(account))
        .filter(log_groups::region.eq(region))
        .filter(log_groups::log_group_name.eq(name))
        .count()
        .get_result::<i64>(tx)?
        > 0)
}

fn require_group(
    tx: &mut diesel::sqlite::SqliteConnection,
    account: &str,
    region: &str,
    name: &str,
) -> Result<GroupRow, AwsError> {
    log_groups::table
        .filter(log_groups::account_id.eq(account))
        .filter(log_groups::region.eq(region))
        .filter(log_groups::log_group_name.eq(name))
        .select(GroupRow::as_select())
        .first::<GroupRow>(tx)
        .optional()?
        .ok_or_else(|| not_found(&format!("Log group {name}")))
}

fn ensure_group(
    tx: &mut diesel::sqlite::SqliteConnection,
    account: &str,
    region: &str,
    group: &str,
    created: i64,
) -> Result<(), AwsError> {
    diesel::insert_into(log_groups::table)
        .values((
            log_groups::account_id.eq(account),
            log_groups::region.eq(region),
            log_groups::log_group_name.eq(group),
            log_groups::creation_time.eq(created),
            log_groups::tags.eq("{}"),
            log_groups::stored_bytes.eq(0_i64),
        ))
        .on_conflict_do_nothing()
        .execute(tx)?;
    Ok(())
}

fn insert_event(
    tx: &mut diesel::sqlite::SqliteConnection,
    account: &str,
    region: &str,
    group: &str,
    stream: &str,
    timestamp: i64,
    ingestion: i64,
    message: &str,
) -> Result<(), AwsError> {
    diesel::insert_into(log_events::table)
        .values((
            log_events::account_id.eq(account),
            log_events::region.eq(region),
            log_events::log_group_name.eq(group),
            log_events::log_stream_name.eq(stream),
            log_events::timestamp.eq(timestamp),
            log_events::ingestion_time.eq(ingestion),
            log_events::message.eq(message),
        ))
        .execute(tx)?;
    let stored: i64 = log_groups::table
        .filter(log_groups::account_id.eq(account))
        .filter(log_groups::region.eq(region))
        .filter(log_groups::log_group_name.eq(group))
        .select(log_groups::stored_bytes)
        .first(tx)?;
    diesel::update(
        log_groups::table
            .filter(log_groups::account_id.eq(account))
            .filter(log_groups::region.eq(region))
            .filter(log_groups::log_group_name.eq(group)),
    )
    .set(log_groups::stored_bytes.eq(stored + message.len() as i64))
    .execute(tx)?;
    Ok(())
}

fn group_shape(ctx: &RequestContext, row: &GroupRow) -> LogGroup {
    let arn = format!(
        "arn:aws:logs:{}:{}:log-group:{}:*",
        ctx.region, ctx.account_id, row.log_group_name
    );
    LogGroup {
        arn: Some(arn.clone()),
        creation_time: Some(row.creation_time),
        log_group_arn: Some(arn),
        log_group_class: Some("STANDARD".into()),
        log_group_name: Some(row.log_group_name.clone()),
        retention_in_days: row.retention_in_days,
        stored_bytes: Some(row.stored_bytes),
        ..Default::default()
    }
}

fn page<T>(
    rows: Vec<T>,
    token: Option<&str>,
    requested: Option<i32>,
    maximum: i32,
) -> Result<(Vec<T>, Option<String>), AwsError> {
    let limit = requested.unwrap_or(maximum);
    if !(1..=maximum).contains(&limit) {
        return Err(invalid(format!("limit must be between 1 and {maximum}")));
    }
    let offset = token.map(parse_token).transpose()?.unwrap_or(0);
    let total = rows.len();
    let page = rows
        .into_iter()
        .skip(offset)
        .take(limit as usize)
        .collect::<Vec<_>>();
    let next = (offset + page.len() < total).then(|| (offset + page.len()).to_string());
    Ok((page, next))
}

fn event_limit(requested: Option<i32>) -> Result<usize, AwsError> {
    let limit = requested.unwrap_or(10_000);
    if !(1..=10_000).contains(&limit) {
        return Err(invalid("limit must be between 1 and 10000"));
    }
    Ok(limit as usize)
}

fn parse_token(token: &str) -> Result<usize, AwsError> {
    token
        .parse::<usize>()
        .map_err(|_| invalid("Invalid nextToken"))
}
