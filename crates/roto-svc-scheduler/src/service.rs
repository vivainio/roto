use crate::generated::*;
use crate::schedule::{self, Expr, Window};
use crate::schema::*;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use jiff::tz::TimeZone;
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::{FromJson, ToJson};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_GROUP: &str = "default";
const MAX_ATTEMPTS: i32 = 3;
/// Finished deliveries kept for inspection.
const KEPT_DELIVERIES: i64 = 1000;

pub struct Scheduler {
    db: Arc<Db>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    sqs: Arc<roto_svc_sqs::Sqs>,
    started: i64,
}
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationException", message)
}
fn missing(what: &str, name: &str) -> AwsError {
    AwsError::sender(
        404,
        "ResourceNotFoundException",
        format!("{what} {name} does not exist."),
    )
}
fn conflict(what: &str, name: &str) -> AwsError {
    AwsError::sender(
        409,
        "ConflictException",
        format!("{what} {name} already exists."),
    )
}
fn corrupt(e: serde_json::Error) -> AwsError {
    AwsError::internal(e.to_string())
}
fn name(kind: &str, value: &str) -> Result<(), AwsError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(invalid(format!(
            "{kind} name must be 1-64 characters of [0-9a-zA-Z-_.]"
        )));
    }
    Ok(())
}
fn group_arn(ctx: &RequestContext, group: &str) -> String {
    format!(
        "arn:aws:scheduler:{}:{}:schedule-group/{group}",
        ctx.region, ctx.account_id
    )
}
fn schedule_arn(ctx: &RequestContext, group: &str, name: &str) -> String {
    format!(
        "arn:aws:scheduler:{}:{}:schedule/{group}/{name}",
        ctx.region, ctx.account_id
    )
}
fn group_of(group: Option<&str>) -> Result<&str, AwsError> {
    let group = group.unwrap_or(DEFAULT_GROUP);
    name("Schedule group", group)?;
    Ok(group)
}
fn group_exists(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    group: &str,
) -> Result<bool, AwsError> {
    Ok(group == DEFAULT_GROUP
        || diesel::select(diesel::dsl::exists(
            schedule_groups::table
                .filter(schedule_groups::account.eq(&ctx.account_id))
                .filter(schedule_groups::region.eq(&ctx.region))
                .filter(schedule_groups::name.eq(group)),
        ))
        .first::<bool>(tx)?)
}
fn load_schedule(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    group: &str,
    name: &str,
) -> Result<Value, AwsError> {
    let text: Option<String> = schedules::table
        .filter(schedules::account.eq(&ctx.account_id))
        .filter(schedules::region.eq(&ctx.region))
        .filter(schedules::grp.eq(group))
        .filter(schedules::name.eq(name))
        .select(schedules::config)
        .first::<String>(tx)
        .optional()?;
    serde_json::from_str(&text.ok_or_else(|| missing("Schedule", name))?).map_err(corrupt)
}
/// Sorted `(key, item)` rows -> one page of at most `limit`, resuming after `token`.
fn page(
    mut rows: Vec<(String, Value)>,
    limit: Option<i32>,
    token: Option<String>,
) -> Result<(Vec<Value>, Option<String>), AwsError> {
    let limit = limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(invalid("MaxResults must be between 1 and 100"));
    }
    if let Some(token) = token {
        rows.retain(|(key, _)| key.as_str() > token.as_str());
    }
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = more.then(|| rows.last().unwrap().0.clone());
    Ok((rows.into_iter().map(|(_, v)| v).collect(), next))
}
fn instant(value: &Value, field: &str) -> Option<i64> {
    value[field].as_i64()
}
fn zone(config: &Value) -> Result<TimeZone, AwsError> {
    let zone = config["ScheduleExpressionTimezone"]
        .as_str()
        .unwrap_or("UTC");
    TimeZone::get(zone).map_err(|_| invalid(format!("Invalid time zone: {zone}")))
}
/// A taggable resource named by ARN.
enum Resource {
    Group(String),
    Schedule(String, String),
}
/// A validated schedule ready to store.
struct Prepared {
    config: Value,
    /// `None` for an expression roto cannot evaluate: stored, but never fires.
    expr: Option<Expr>,
    tz: TimeZone,
    start: Option<i64>,
    end: Option<i64>,
    anchor: i64,
}
impl Prepared {
    fn window(&self) -> Window<'_> {
        Window {
            tz: &self.tz,
            start: self.start,
            end: self.end,
            anchor: self.anchor,
        }
    }
}
fn prepare(
    ctx: &RequestContext,
    group: &str,
    name: &str,
    mut input: Value,
    created: Option<i64>,
    now: i64,
) -> Result<Prepared, AwsError> {
    input.as_object_mut().unwrap().remove("ClientToken");
    let expression = input["ScheduleExpression"].as_str().unwrap_or_default();
    let expr = schedule::parse(expression).ok();
    let tz = zone(&input)?;
    let state = input["State"].as_str().unwrap_or("ENABLED").to_owned();
    if !["ENABLED", "DISABLED"].contains(&state.as_str()) {
        return Err(invalid("State must be ENABLED or DISABLED"));
    }
    let action = input["ActionAfterCompletion"]
        .as_str()
        .unwrap_or("NONE")
        .to_owned();
    if !["NONE", "DELETE"].contains(&action.as_str()) {
        return Err(invalid("ActionAfterCompletion must be NONE or DELETE"));
    }
    let window = &input["FlexibleTimeWindow"];
    match (
        window["Mode"].as_str(),
        window["MaximumWindowInMinutes"].as_i64(),
    ) {
        (Some("OFF"), _) => {}
        (Some("FLEXIBLE"), Some(1..=1440)) => {}
        (Some("FLEXIBLE"), _) => {
            return Err(invalid(
                "MaximumWindowInMinutes must be between 1 and 1440 when Mode is FLEXIBLE",
            ));
        }
        _ => return Err(invalid("FlexibleTimeWindow Mode must be OFF or FLEXIBLE")),
    }
    let start = instant(&input, "StartDate");
    let end = instant(&input, "EndDate");
    if let (Some(start), Some(end)) = (start, end)
        && end <= start
    {
        return Err(invalid("EndDate must be after StartDate"));
    }
    let interval = match expr {
        Some(Expr::Rate(interval)) => Some(interval),
        _ => None,
    };
    let anchor = interval.map_or(0, |i| start.unwrap_or(now + i));
    let zone_name = input["ScheduleExpressionTimezone"]
        .as_str()
        .unwrap_or("UTC")
        .to_owned();
    if input["Target"]["RetryPolicy"].is_null() {
        input["Target"]["RetryPolicy"] =
            json!({"MaximumEventAgeInSeconds": 86400, "MaximumRetryAttempts": 185});
    }
    let object = input.as_object_mut().unwrap();
    object.retain(|_, v| !v.is_null());
    object.insert("Name".into(), json!(name));
    object.insert("GroupName".into(), json!(group));
    object.insert("Arn".into(), json!(schedule_arn(ctx, group, name)));
    object.insert("State".into(), json!(state));
    object.insert("ActionAfterCompletion".into(), json!(action));
    object.insert("ScheduleExpressionTimezone".into(), json!(zone_name));
    object.insert("CreationDate".into(), json!(created.unwrap_or(now)));
    object.insert("LastModificationDate".into(), json!(now));
    Ok(Prepared {
        config: input,
        expr,
        tz,
        start,
        end,
        anchor,
    })
}
/// When a freshly stored schedule first fires. A one-time `at()` already in the past fires on
/// the next tick rather than being silently dropped.
fn first_fire(prepared: &Prepared, now: i64) -> Option<i64> {
    let expr = prepared.expr.as_ref()?;
    schedule::next_after(expr, &prepared.window(), now).or_else(|| {
        (matches!(expr, Expr::At(_))
            && prepared.end.is_none_or(|end| now <= end)
            && prepared.start.is_none_or(|start| start <= now))
        .then_some(now)
    })
}
fn substitute(input: &str, arn: &str, scheduled: i64, id: &str, attempt: i32) -> String {
    let when = jiff::Timestamp::from_second(scheduled)
        .map(|t| t.strftime("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default();
    input
        .replace("<aws.scheduler.scheduled-time>", &when)
        .replace("<aws.scheduler.schedule-arn>", arn)
        .replace("<aws.scheduler.execution-id>", id)
        .replace("<aws.scheduler.attempt-number>", &attempt.to_string())
}

impl Scheduler {
    pub fn new(
        store: &Store,
        lambda: Arc<roto_svc_lambda::Lambda>,
        sqs: Arc<roto_svc_sqs::Sqs>,
    ) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("scheduler", crate::MIGRATIONS)?,
            lambda,
            sqs,
            started: now_secs(),
        })
    }
    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(schedules::table).execute(tx)?;
            diesel::delete(schedule_groups::table).execute(tx)?;
            diesel::delete(deliveries::table).execute(tx)?;
            Ok(())
        })
    }
    pub fn history(&self, ctx: &RequestContext) -> Result<Value, AwsError> {
        self.db.read(|c| {
            let rows = deliveries::table
                .filter(deliveries::account.eq(&ctx.account_id))
                .filter(deliveries::region.eq(&ctx.region))
                .order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid").desc())
                .limit(100)
                .select((
                    deliveries::id,
                    deliveries::schedule_arn,
                    deliveries::target,
                    deliveries::scheduled,
                    deliveries::state,
                    deliveries::attempts,
                    deliveries::error,
                ))
                .load::<(String, String, String, i64, String, i32, Option<String>)>(c)?;
            Ok(json!({"deliveries": rows.into_iter().map(|(id, schedule, target, scheduled, state, attempts, error)| {
                json!({"id": id, "schedule": schedule, "target": target, "scheduled": scheduled,
                    "state": state, "attempts": attempts, "error": error})
            }).collect::<Vec<_>>()}))
        })
    }
    pub fn start(this: &Arc<Self>) {
        let weak = Arc::downgrade(this);
        std::thread::Builder::new()
            .name("roto-scheduler".into())
            .spawn(move || {
                while let Some(scheduler) = weak.upgrade() {
                    if let Err(e) = scheduler.fire_due(now_secs()) {
                        eprintln!("Scheduler tick: {e}");
                    }
                    loop {
                        match scheduler.deliver_next() {
                            Ok(true) => {}
                            Ok(false) => break,
                            Err(e) => {
                                eprintln!("Scheduler delivery: {e}");
                                break;
                            }
                        }
                    }
                    drop(scheduler);
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
            .expect("Scheduler worker");
    }
    /// Queues a delivery for every enabled schedule whose `next_fire` has arrived and moves
    /// `next_fire` to the next future occurrence, so downtime never backfills missed ticks.
    pub(crate) fn fire_due(&self, now: i64) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let due = schedules::table
                .filter(schedules::next_fire.le(now))
                .order(schedules::next_fire)
                .select((
                    schedules::account,
                    schedules::region,
                    schedules::grp,
                    schedules::name,
                    schedules::config,
                    schedules::endpoint,
                    schedules::next_fire,
                    schedules::anchor,
                ))
                .load::<(String, String, String, String, String, String, Option<i64>, i64)>(tx)?;
            for (account, region, group, name, text, endpoint, scheduled, anchor) in due {
                let config: Value = serde_json::from_str(&text).map_err(corrupt)?;
                let scheduled = scheduled.unwrap_or(now);
                let fires = config["State"] == "ENABLED";
                if fires {
                    diesel::insert_into(deliveries::table)
                        .values((
                            deliveries::id.eq(&ids::request_id()),
                            deliveries::account.eq(&account),
                            deliveries::region.eq(&region),
                            deliveries::endpoint.eq(&endpoint),
                            deliveries::target.eq(config["Target"]["Arn"].as_str().unwrap_or("")),
                            deliveries::config.eq(&config["Target"].to_string()),
                            deliveries::schedule_arn.eq(config["Arn"].as_str().unwrap_or("")),
                            deliveries::scheduled.eq(scheduled),
                        ))
                        .execute(tx)?;
                }
                let expr = schedule::parse(config["ScheduleExpression"].as_str().unwrap_or(""));
                let tz = zone(&config)?;
                let next = expr.ok().and_then(|expr| {
                    schedule::next_after(
                        &expr,
                        &Window {
                            tz: &tz,
                            start: instant(&config, "StartDate"),
                            end: instant(&config, "EndDate"),
                            anchor,
                        },
                        now,
                    )
                });
                let key = schedules::table
                    .filter(schedules::account.eq(&account))
                    .filter(schedules::region.eq(&region))
                    .filter(schedules::grp.eq(&group))
                    .filter(schedules::name.eq(&name));
                if next.is_none() && fires && config["ActionAfterCompletion"] == "DELETE" {
                    diesel::delete(key).execute(tx)?;
                } else {
                    diesel::update(key)
                        .set(schedules::next_fire.eq(next))
                        .execute(tx)?;
                }
            }
            diesel::sql_query(format!(
                "DELETE FROM deliveries WHERE state != 'queued' AND rowid <= (SELECT max(rowid) FROM deliveries) - {KEPT_DELIVERIES}"
            ))
            .execute(tx)?;
            Ok(())
        })
    }
    /// Delivers one queued message; `false` when nothing was due.
    fn deliver_next(&self) -> Result<bool, AwsError> {
        let row = self.db.read(|c| {
            Ok(deliveries::table
                .filter(deliveries::state.eq("queued"))
                .filter(deliveries::due.le(now_millis()))
                .order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid"))
                .select((
                    deliveries::id,
                    deliveries::account,
                    deliveries::region,
                    deliveries::endpoint,
                    deliveries::target,
                    deliveries::config,
                    deliveries::schedule_arn,
                    deliveries::scheduled,
                    deliveries::attempts,
                ))
                .first::<(
                    String,
                    String,
                    String,
                    String,
                    String,
                    String,
                    String,
                    i64,
                    i32,
                )>(c)
                .optional()?)
        })?;
        let Some((id, account_id, region, base_url, arn, config, schedule, scheduled, attempts)) =
            row
        else {
            return Ok(false);
        };
        let ctx = RequestContext {
            account_id,
            region,
            base_url,
            request_id: id.clone(),
            access_key: None,
        };
        let attempts = attempts + 1;
        let result = (|| {
            let target: Value = serde_json::from_str(&config).map_err(corrupt)?;
            let input = substitute(
                target["Input"].as_str().unwrap_or("{}"),
                &schedule,
                scheduled,
                &id,
                attempts,
            );
            if arn.starts_with("arn:aws:lambda:") {
                let event = serde_json::from_str(&input).unwrap_or(Value::String(input));
                self.lambda.enqueue(&ctx, &arn, event)?;
            } else if arn.starts_with("arn:aws:sqs:") {
                let group = target["SqsParameters"]["MessageGroupId"]
                    .as_str()
                    .map(str::to_owned);
                if !self
                    .sqs
                    .deliver(&arn, &input, &BTreeMap::new(), group, None)?
                {
                    return Err(missing("Queue", &arn));
                }
            } else {
                return Err(AwsError::not_implemented(
                    "scheduler",
                    &format!("target {arn}: only SQS queues and Lambda functions can be fired"),
                ));
            }
            Ok(())
        })();
        let state = if result.is_ok() {
            "succeeded"
        } else if attempts >= MAX_ATTEMPTS {
            "failed"
        } else {
            "queued"
        };
        let error = result.err().map(|e| e.to_string());
        self.db.transaction(|tx| {
            diesel::update(deliveries::table.filter(deliveries::id.eq(&id)))
                .set((
                    deliveries::state.eq(state),
                    deliveries::attempts.eq(attempts),
                    deliveries::due.eq(now_millis() + 1000 * i64::from(attempts)),
                    deliveries::error.eq(&error),
                ))
                .execute(tx)?;
            Ok(())
        })?;
        Ok(true)
    }
    fn group_config(
        &self,
        ctx: &RequestContext,
        group: &str,
        tx: &mut SqliteConnection,
    ) -> Result<Value, AwsError> {
        if group == DEFAULT_GROUP {
            return Ok(
                json!({"Name": group, "Arn": group_arn(ctx, group), "State": "ACTIVE",
                "CreationDate": self.started, "LastModificationDate": self.started}),
            );
        }
        let text: Option<String> = schedule_groups::table
            .filter(schedule_groups::account.eq(&ctx.account_id))
            .filter(schedule_groups::region.eq(&ctx.region))
            .filter(schedule_groups::name.eq(group))
            .select(schedule_groups::config)
            .first::<String>(tx)
            .optional()?;
        serde_json::from_str(&text.ok_or_else(|| missing("Schedule group", group))?)
            .map_err(corrupt)
    }
    /// The group or schedule a taggable resource ARN names.
    fn resource_from_arn(&self, ctx: &RequestContext, arn: &str) -> Result<Resource, AwsError> {
        let rest = arn
            .strip_prefix(&format!(
                "arn:aws:scheduler:{}:{}:",
                ctx.region, ctx.account_id
            ))
            .ok_or_else(|| missing("Resource", arn))?;
        if let Some(group) = rest.strip_prefix("schedule-group/") {
            Ok(Resource::Group(group.to_owned()))
        } else if let Some((group, name)) = rest
            .strip_prefix("schedule/")
            .and_then(|r| r.split_once('/'))
        {
            Ok(Resource::Schedule(group.to_owned(), name.to_owned()))
        } else {
            Err(missing("Resource", arn))
        }
    }
    fn resource_config(
        &self,
        tx: &mut SqliteConnection,
        ctx: &RequestContext,
        resource: &Resource,
    ) -> Result<Value, AwsError> {
        match resource {
            Resource::Group(group) => self.group_config(ctx, group, tx),
            Resource::Schedule(group, name) => load_schedule(tx, ctx, group, name),
        }
    }
    fn store(
        &self,
        ctx: &RequestContext,
        group: &str,
        name: &str,
        prepared: &Prepared,
        now: i64,
        replace: bool,
    ) -> Result<String, AwsError> {
        let arn = schedule_arn(ctx, group, name);
        self.db.transaction(|tx| {
            if !group_exists(tx, ctx, group)? {
                return Err(missing("Schedule group", group));
            }
            let existing = load_schedule(tx, ctx, group, name);
            let created = match (&existing, replace) {
                (Ok(_), false) => return Err(conflict("Schedule", name)),
                (Err(_), true) => return Err(missing("Schedule", name)),
                (Ok(old), true) => instant(old, "CreationDate"),
                (Err(_), false) => None,
            };
            let mut config = prepared.config.clone();
            if let Some(created) = created {
                config["CreationDate"] = json!(created);
                // Timestamps are whole seconds; an update still lands after creation.
                config["LastModificationDate"] = json!(now.max(created + 1));
            }
            if let Ok(old) = &existing
                && !old["Tags"].is_null()
            {
                config["Tags"] = old["Tags"].clone();
            }
            let next = first_fire(prepared, now);
            let row = (
                schedules::account.eq(&ctx.account_id),
                schedules::region.eq(&ctx.region),
                schedules::grp.eq(group),
                schedules::name.eq(name),
                schedules::config.eq(config.to_string()),
                schedules::endpoint.eq(&ctx.base_url),
                schedules::next_fire.eq(next),
                schedules::anchor.eq(prepared.anchor),
            );
            diesel::replace_into(schedules::table)
                .values(row)
                .execute(tx)?;
            Ok(())
        })?;
        Ok(arn)
    }
}

#[roto_core::implements_operations]
impl Service for Scheduler {
    fn create_schedule_group(
        &self,
        ctx: &RequestContext,
        i: CreateScheduleGroupInput,
    ) -> Result<CreateScheduleGroupOutput, AwsError> {
        name("Schedule group", &i.name)?;
        let arn = group_arn(ctx, &i.name);
        let now = now_secs();
        let tags: Vec<Value> = i.tags.iter().map(ToJson::to_json).collect();
        let config = json!({"Name": i.name, "Arn": arn, "State": "ACTIVE", "Tags": tags,
            "CreationDate": now, "LastModificationDate": now});
        self.db.transaction(|tx| {
            if group_exists(tx, ctx, &i.name)? {
                return Err(conflict("Schedule group", &i.name));
            }
            diesel::insert_into(schedule_groups::table)
                .values((
                    schedule_groups::account.eq(&ctx.account_id),
                    schedule_groups::region.eq(&ctx.region),
                    schedule_groups::name.eq(&i.name),
                    schedule_groups::config.eq(config.to_string()),
                ))
                .execute(tx)?;
            Ok(())
        })?;
        CreateScheduleGroupOutput::from_json(&json!({"ScheduleGroupArn": arn}), "")
    }
    fn get_schedule_group(
        &self,
        ctx: &RequestContext,
        i: GetScheduleGroupInput,
    ) -> Result<GetScheduleGroupOutput, AwsError> {
        name("Schedule group", &i.name)?;
        let mut config = self
            .db
            .transaction(|tx| self.group_config(ctx, &i.name, tx))?;
        config.as_object_mut().unwrap().remove("Tags");
        GetScheduleGroupOutput::from_json(&config, "")
    }
    fn delete_schedule_group(
        &self,
        ctx: &RequestContext,
        i: DeleteScheduleGroupInput,
    ) -> Result<DeleteScheduleGroupOutput, AwsError> {
        name("Schedule group", &i.name)?;
        if i.name == DEFAULT_GROUP {
            return Err(invalid("The default schedule group cannot be deleted"));
        }
        self.db.transaction(|tx| {
            let deleted = diesel::delete(
                schedule_groups::table
                    .filter(schedule_groups::account.eq(&ctx.account_id))
                    .filter(schedule_groups::region.eq(&ctx.region))
                    .filter(schedule_groups::name.eq(&i.name)),
            )
            .execute(tx)?;
            if deleted == 0 {
                return Err(missing("Schedule group", &i.name));
            }
            diesel::delete(
                schedules::table
                    .filter(schedules::account.eq(&ctx.account_id))
                    .filter(schedules::region.eq(&ctx.region))
                    .filter(schedules::grp.eq(&i.name)),
            )
            .execute(tx)?;
            Ok(())
        })?;
        DeleteScheduleGroupOutput::from_json(&json!({}), "")
    }
    fn list_schedule_groups(
        &self,
        ctx: &RequestContext,
        i: ListScheduleGroupsInput,
    ) -> Result<ListScheduleGroupsOutput, AwsError> {
        let prefix = i.name_prefix.unwrap_or_default();
        let mut rows = self.db.transaction(|tx| {
            let stored = schedule_groups::table
                .filter(schedule_groups::account.eq(&ctx.account_id))
                .filter(schedule_groups::region.eq(&ctx.region))
                .select(schedule_groups::config)
                .load::<String>(tx)?;
            let mut rows = vec![(
                DEFAULT_GROUP.to_owned(),
                self.group_config(ctx, DEFAULT_GROUP, tx)?,
            )];
            for text in stored {
                let config: Value = serde_json::from_str(&text).map_err(corrupt)?;
                rows.push((config["Name"].as_str().unwrap_or("").to_owned(), config));
            }
            Ok(rows)
        })?;
        rows.retain(|(name, _)| name.starts_with(&prefix));
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let (mut groups, next) = page(rows, i.max_results, i.next_token)?;
        for group in &mut groups {
            group.as_object_mut().unwrap().remove("Tags");
        }
        ListScheduleGroupsOutput::from_json(
            &json!({"ScheduleGroups": groups, "NextToken": next}),
            "",
        )
    }
    fn create_schedule(
        &self,
        ctx: &RequestContext,
        i: CreateScheduleInput,
    ) -> Result<CreateScheduleOutput, AwsError> {
        name("Schedule", &i.name)?;
        let group = group_of(i.group_name.as_deref())?.to_owned();
        let now = now_secs();
        let prepared = prepare(ctx, &group, &i.name, i.to_json(), None, now)?;
        let arn = self.store(ctx, &group, &i.name, &prepared, now, false)?;
        CreateScheduleOutput::from_json(&json!({"ScheduleArn": arn}), "")
    }
    fn update_schedule(
        &self,
        ctx: &RequestContext,
        i: UpdateScheduleInput,
    ) -> Result<UpdateScheduleOutput, AwsError> {
        name("Schedule", &i.name)?;
        let group = group_of(i.group_name.as_deref())?.to_owned();
        let now = now_secs();
        let prepared = prepare(ctx, &group, &i.name, i.to_json(), None, now)?;
        let arn = self.store(ctx, &group, &i.name, &prepared, now, true)?;
        UpdateScheduleOutput::from_json(&json!({"ScheduleArn": arn}), "")
    }
    fn get_schedule(
        &self,
        ctx: &RequestContext,
        i: GetScheduleInput,
    ) -> Result<GetScheduleOutput, AwsError> {
        name("Schedule", &i.name)?;
        let group = group_of(i.group_name.as_deref())?;
        let config = self
            .db
            .transaction(|tx| load_schedule(tx, ctx, group, &i.name))?;
        GetScheduleOutput::from_json(&config, "")
    }
    fn delete_schedule(
        &self,
        ctx: &RequestContext,
        i: DeleteScheduleInput,
    ) -> Result<DeleteScheduleOutput, AwsError> {
        name("Schedule", &i.name)?;
        let group = group_of(i.group_name.as_deref())?;
        self.db.transaction(|tx| {
            let deleted = diesel::delete(
                schedules::table
                    .filter(schedules::account.eq(&ctx.account_id))
                    .filter(schedules::region.eq(&ctx.region))
                    .filter(schedules::grp.eq(group))
                    .filter(schedules::name.eq(&i.name)),
            )
            .execute(tx)?;
            if deleted == 0 {
                return Err(missing("Schedule", &i.name));
            }
            Ok(())
        })?;
        DeleteScheduleOutput::from_json(&json!({}), "")
    }
    fn list_schedules(
        &self,
        ctx: &RequestContext,
        i: ListSchedulesInput,
    ) -> Result<ListSchedulesOutput, AwsError> {
        if let Some(state) = &i.state
            && !["ENABLED", "DISABLED"].contains(&state.as_str())
        {
            return Err(invalid("State must be ENABLED or DISABLED"));
        }
        let prefix = i.name_prefix.unwrap_or_default();
        let texts = self.db.transaction(|tx| {
            let mut query = schedules::table
                .filter(schedules::account.eq(&ctx.account_id))
                .filter(schedules::region.eq(&ctx.region))
                .into_boxed();
            if let Some(group) = &i.group_name {
                query = query.filter(schedules::grp.eq(group.clone()));
            }
            Ok(query
                .order((schedules::grp, schedules::name))
                .select(schedules::config)
                .load::<String>(tx)?)
        })?;
        let mut rows = Vec::new();
        for text in texts {
            let config: Value = serde_json::from_str(&text).map_err(corrupt)?;
            let name = config["Name"].as_str().unwrap_or("");
            if !name.starts_with(&prefix)
                || i.state.as_deref().is_some_and(|s| config["State"] != s)
            {
                continue;
            }
            rows.push((
                format!("{}/{name}", config["GroupName"].as_str().unwrap_or("")),
                json!({"Arn": config["Arn"], "Name": name, "GroupName": config["GroupName"],
                    "State": config["State"], "CreationDate": config["CreationDate"],
                    "LastModificationDate": config["LastModificationDate"],
                    "Target": {"Arn": config["Target"]["Arn"]}}),
            ));
        }
        let (schedules, next) = page(rows, i.max_results, i.next_token)?;
        ListSchedulesOutput::from_json(&json!({"Schedules": schedules, "NextToken": next}), "")
    }
    fn tag_resource(
        &self,
        ctx: &RequestContext,
        i: TagResourceInput,
    ) -> Result<TagResourceOutput, AwsError> {
        self.update_tags(ctx, &i.resource_arn, |tags| {
            for tag in &i.tags {
                tags.insert(tag.key.clone(), tag.value.clone());
            }
        })?;
        TagResourceOutput::from_json(&json!({}), "")
    }
    fn untag_resource(
        &self,
        ctx: &RequestContext,
        i: UntagResourceInput,
    ) -> Result<UntagResourceOutput, AwsError> {
        self.update_tags(ctx, &i.resource_arn, |tags| {
            for key in &i.tag_keys {
                tags.remove(key);
            }
        })?;
        UntagResourceOutput::from_json(&json!({}), "")
    }
    fn list_tags_for_resource(
        &self,
        ctx: &RequestContext,
        i: ListTagsForResourceInput,
    ) -> Result<ListTagsForResourceOutput, AwsError> {
        let resource = self.resource_from_arn(ctx, &i.resource_arn)?;
        let config = self
            .db
            .transaction(|tx| self.resource_config(tx, ctx, &resource))?;
        ListTagsForResourceOutput::from_json(&json!({"Tags": config["Tags"]}), "")
    }
}

impl Scheduler {
    fn update_tags(
        &self,
        ctx: &RequestContext,
        arn: &str,
        change: impl FnOnce(&mut BTreeMap<String, String>),
    ) -> Result<(), AwsError> {
        let resource = self.resource_from_arn(ctx, arn)?;
        if matches!(&resource, Resource::Group(group) if group == DEFAULT_GROUP) {
            return Err(invalid("The default schedule group cannot be tagged"));
        }
        self.db.transaction(|tx| {
            let mut config = self.resource_config(tx, ctx, &resource)?;
            let mut tags: BTreeMap<String, String> = config["Tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| {
                    Some((
                        t["Key"].as_str()?.to_owned(),
                        t["Value"].as_str()?.to_owned(),
                    ))
                })
                .collect();
            change(&mut tags);
            config["Tags"] = tags
                .iter()
                .map(|(k, v)| json!({"Key": k, "Value": v}))
                .collect();
            let text = config.to_string();
            match &resource {
                Resource::Group(group) => diesel::update(
                    schedule_groups::table
                        .filter(schedule_groups::account.eq(&ctx.account_id))
                        .filter(schedule_groups::region.eq(&ctx.region))
                        .filter(schedule_groups::name.eq(group)),
                )
                .set(schedule_groups::config.eq(text))
                .execute(tx)?,
                Resource::Schedule(group, name) => diesel::update(
                    schedules::table
                        .filter(schedules::account.eq(&ctx.account_id))
                        .filter(schedules::region.eq(&ctx.region))
                        .filter(schedules::grp.eq(group))
                        .filter(schedules::name.eq(name)),
                )
                .set(schedules::config.eq(text))
                .execute(tx)?,
            };
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roto_core::store::StoreOptions;

    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
            access_key: None,
        }
    }
    fn open(dir: &std::path::Path) -> Scheduler {
        let store = Store::open(dir, StoreOptions::default()).unwrap();
        let lambda = Arc::new(roto_svc_lambda::Lambda::new(&store, Default::default()).unwrap());
        let sqs = roto_svc_sqs::SqsHandler::new(&store).unwrap().0;
        Scheduler::new(&store, lambda, sqs).unwrap()
    }
    fn create(
        scheduler: &Scheduler,
        name: &str,
        expression: &str,
        extra: Value,
    ) -> Result<(), AwsError> {
        let mut body = json!({
            "Name": name,
            "ScheduleExpression": expression,
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:missing", "RoleArn": "arn:aws:iam::123456789012:role/r",
                "Input": "{\"at\":\"<aws.scheduler.scheduled-time>\"}"},
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let input = CreateScheduleInput::from_json(&body, "")?;
        scheduler.create_schedule(
            &ctx(),
            CreateScheduleInput {
                name: name.into(),
                ..input
            },
        )?;
        Ok(())
    }
    fn deliveries(scheduler: &Scheduler) -> Vec<Value> {
        scheduler.history(&ctx()).unwrap()["deliveries"]
            .as_array()
            .unwrap()
            .clone()
    }
    fn next_fire(scheduler: &Scheduler, name: &str) -> Option<i64> {
        scheduler
            .db
            .read(|c| {
                Ok(schedules::table
                    .filter(schedules::name.eq(name))
                    .select(schedules::next_fire)
                    .first::<Option<i64>>(c)?)
            })
            .unwrap()
    }

    #[test]
    fn schedules_survive_restart_without_backfill_or_duplicates() {
        let dir = std::env::temp_dir().join(format!("roto-scheduler-{}", ids::request_id()));
        let now = now_secs();
        {
            let scheduler = open(&dir);
            create(&scheduler, "poll", "rate(1 minutes)", json!({})).unwrap();
            create(
                &scheduler,
                "off",
                "rate(1 minutes)",
                json!({"State": "DISABLED"}),
            )
            .unwrap();
            assert!(next_fire(&scheduler, "poll").unwrap() > now);
        }
        let scheduler = open(&dir);
        scheduler.fire_due(now).unwrap();
        assert!(deliveries(&scheduler).is_empty());
        // The server was down for an hour: one delivery per schedule, not sixty.
        let later = now + 3600;
        scheduler.fire_due(later).unwrap();
        scheduler.fire_due(later).unwrap();
        let sent = deliveries(&scheduler);
        assert_eq!(sent.len(), 1);
        assert!(
            sent[0]["schedule"]
                .as_str()
                .unwrap()
                .ends_with("schedule/default/poll")
        );
        let next = next_fire(&scheduler, "poll").unwrap();
        assert!(next > later && next <= later + 60);
        // A disabled schedule advances but never fires.
        assert!(next_fire(&scheduler, "off").unwrap() > later);
        drop(scheduler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn one_time_schedule_fires_once_and_deletes_itself() {
        let dir = std::env::temp_dir().join(format!("roto-scheduler-{}", ids::request_id()));
        let scheduler = open(&dir);
        create(
            &scheduler,
            "once",
            "at(2020-01-01T00:00:00)",
            json!({"ActionAfterCompletion": "DELETE"}),
        )
        .unwrap();
        create(&scheduler, "kept", "at(2020-01-01T00:00:00)", json!({})).unwrap();
        scheduler.fire_due(now_secs()).unwrap();
        scheduler.fire_due(now_secs()).unwrap();
        assert_eq!(deliveries(&scheduler).len(), 2);
        let get = |name: &str| {
            scheduler.get_schedule(
                &ctx(),
                GetScheduleInput {
                    name: name.into(),
                    group_name: None,
                },
            )
        };
        assert_eq!(get("once").unwrap_err().code, "ResourceNotFoundException");
        assert_eq!(
            get("kept").unwrap().action_after_completion.as_deref(),
            Some("NONE")
        );
        assert_eq!(next_fire(&scheduler, "kept"), None);
        drop(scheduler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_bad_requests_like_aws() {
        let dir = std::env::temp_dir().join(format!("roto-scheduler-{}", ids::request_id()));
        let scheduler = open(&dir);
        create(
            &scheduler,
            "a",
            "cron(0 2 * * ? *)",
            json!({"ScheduleExpressionTimezone": "Europe/Helsinki"}),
        )
        .unwrap();
        for (expression, extra) in [
            (
                "cron(0 2 * * ? *)",
                json!({"ScheduleExpressionTimezone": "Mars/Base"}),
            ),
            ("rate(1 minutes)", json!({"State": "PAUSED"})),
            (
                "rate(1 minutes)",
                json!({"FlexibleTimeWindow": {"Mode": "FLEXIBLE"}}),
            ),
        ] {
            let err = create(&scheduler, "b", expression, extra).unwrap_err();
            assert_eq!(err.code, "ValidationException", "{expression}");
        }
        assert_eq!(
            create(&scheduler, "a", "rate(5 minutes)", json!({}))
                .unwrap_err()
                .code,
            "ConflictException"
        );
        assert_eq!(
            create(
                &scheduler,
                "c",
                "rate(5 minutes)",
                json!({"GroupName": "nope"})
            )
            .unwrap_err()
            .code,
            "ResourceNotFoundException"
        );
        drop(scheduler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unevaluable_expressions_and_targets_are_stored_but_never_fire() {
        let dir = std::env::temp_dir().join(format!("roto-scheduler-{}", ids::request_id()));
        let scheduler = open(&dir);
        create(&scheduler, "typo", "some cron", json!({})).unwrap();
        create(
            &scheduler,
            "other",
            "rate(1 minutes)",
            json!({"Target": {"Arn": "arn1", "RoleArn": "n/a"}}),
        )
        .unwrap();
        assert_eq!(next_fire(&scheduler, "typo"), None);
        scheduler.fire_due(now_secs() + 3600).unwrap();
        assert_eq!(deliveries(&scheduler).len(), 1);
        assert_eq!(
            scheduler
                .get_schedule(
                    &ctx(),
                    GetScheduleInput {
                        name: "typo".into(),
                        group_name: None
                    }
                )
                .unwrap()
                .schedule_expression
                .as_deref(),
            Some("some cron")
        );
        drop(scheduler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn substitutes_context_attributes() {
        assert_eq!(
            substitute(
                "{\"t\":\"<aws.scheduler.scheduled-time>\",\"n\":<aws.scheduler.attempt-number>}",
                "arn",
                1_800_000_000,
                "id",
                2
            ),
            "{\"t\":\"2027-01-15T08:00:00Z\",\"n\":2}"
        );
    }
}
