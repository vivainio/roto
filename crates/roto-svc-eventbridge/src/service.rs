use crate::schema::*;
use crate::{generated::*, pattern};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::{FromJson, Timestamp, ToJson};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct EventBridge {
    db: Arc<Db>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    sqs: Arc<roto_svc_sqs::Sqs>,
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationException", message)
}
fn missing(name: &str) -> AwsError {
    AwsError::sender(
        400,
        "ResourceNotFoundException",
        format!("Resource does not exist: {name}"),
    )
}
fn name(value: &str) -> Result<(), AwsError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(invalid("Invalid resource name"));
    }
    Ok(())
}
fn bus_name(ctx: &RequestContext, value: Option<&str>) -> Result<String, AwsError> {
    let value = value.unwrap_or("default");
    let value = if value.starts_with("arn:") {
        value
            .strip_prefix(&format!(
                "arn:aws:events:{}:{}:event-bus/",
                ctx.region, ctx.account_id
            ))
            .ok_or_else(|| invalid("Bus must be in the same account and region"))?
    } else {
        value
    };
    name(value)?;
    Ok(value.into())
}
fn bus_arn(ctx: &RequestContext, bus: &str) -> String {
    format!(
        "arn:aws:events:{}:{}:event-bus/{bus}",
        ctx.region, ctx.account_id
    )
}
fn rule_arn(ctx: &RequestContext, bus: &str, rule: &str) -> String {
    format!(
        "arn:aws:events:{}:{}:rule/{}{}",
        ctx.region,
        ctx.account_id,
        if bus == "default" {
            String::new()
        } else {
            format!("{bus}/")
        },
        rule
    )
}
fn ensure_bus(tx: &mut SqliteConnection, ctx: &RequestContext, bus: &str) -> Result<(), AwsError> {
    if bus != "default"
        && !diesel::select(diesel::dsl::exists(
            buses::table
                .filter(buses::account.eq(&ctx.account_id))
                .filter(buses::region.eq(&ctx.region))
                .filter(buses::name.eq(&bus)),
        ))
        .first::<bool>(tx)?
    {
        return Err(missing(bus));
    }
    Ok(())
}
fn load_rule(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    bus: &str,
    rule: &str,
) -> Result<Value, AwsError> {
    let text: Option<String> = rules::table
        .filter(rules::account.eq(&ctx.account_id))
        .filter(rules::region.eq(&ctx.region))
        .filter(rules::bus.eq(&bus))
        .filter(rules::name.eq(&rule))
        .select(rules::config)
        .first::<String>(tx)
        .optional()?;
    serde_json::from_str(&text.ok_or_else(|| missing(rule))?)
        .map_err(|e| AwsError::internal(e.to_string()))
}
fn page(
    mut values: Vec<Value>,
    limit: Option<i32>,
    token: Option<String>,
    field: &str,
) -> Result<(Vec<Value>, Option<String>), AwsError> {
    let limit = limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(invalid("Limit must be between 1 and 100"));
    }
    if let Some(token) = token {
        values.retain(|v| v[field].as_str().is_some_and(|s| s > token.as_str()));
    }
    let more = values.len() > limit as usize;
    values.truncate(limit as usize);
    let next = more.then(|| values.last().unwrap()[field].as_str().unwrap().to_owned());
    Ok((values, next))
}
fn supported(value: &Value, allowed: &[&str]) -> Result<(), AwsError> {
    for (key, value) in value.as_object().unwrap() {
        if !allowed.contains(&key.as_str()) && !value.is_null() {
            return Err(AwsError::not_implemented(
                "events",
                &format!("setting {key}"),
            ));
        }
    }
    Ok(())
}
fn input_path<'a>(event: &'a Value, path: &str) -> Result<&'a Value, AwsError> {
    if path == "$" {
        return Ok(event);
    }
    let path = path
        .strip_prefix("$.")
        .ok_or_else(|| invalid("InputPath supports $ and dot-separated object keys"))?;
    let mut value = event;
    for key in path.split('.') {
        value = value
            .get(key)
            .ok_or_else(|| invalid(format!("InputPath is absent: {path}")))?;
    }
    Ok(value)
}
impl EventBridge {
    pub fn new(
        store: &Store,
        lambda: Arc<roto_svc_lambda::Lambda>,
        sqs: Arc<roto_svc_sqs::Sqs>,
    ) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("events", crate::MIGRATIONS)?,
            lambda,
            sqs,
        })
    }
    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(targets::table).execute(tx)?;
            diesel::delete(rules::table).execute(tx)?;
            diesel::delete(buses::table).execute(tx)?;
            diesel::delete(deliveries::table).execute(tx)?;
            Ok(())
        })
    }
    pub fn enqueue_event(
        &self,
        ctx: &RequestContext,
        bus: &str,
        event: Value,
    ) -> Result<(), AwsError> {
        let bus = bus_name(ctx, Some(bus))?;
        self.db.transaction(|tx| {
            ensure_bus(tx, ctx, &bus)?;
            let rules = rules::table
                .filter(rules::account.eq(&ctx.account_id))
                .filter(rules::region.eq(&ctx.region))
                .filter(rules::bus.eq(&bus))
                .order(rules::name)
                .select((rules::name, rules::config))
                .load::<(String, String)>(tx)?;
            for (rule, text) in rules {
                let config: Value =
                    serde_json::from_str(&text).map_err(|e| AwsError::internal(e.to_string()))?;
                if config["State"] != "ENABLED" {
                    continue;
                }
                let pattern = pattern::parse(config["EventPattern"].as_str().unwrap())?;
                if !pattern::matches(&pattern, &event) {
                    continue;
                }
                let targets = targets::table
                    .filter(targets::account.eq(&ctx.account_id))
                    .filter(targets::region.eq(&ctx.region))
                    .filter(targets::bus.eq(&bus))
                    .filter(targets::rule.eq(&rule))
                    .order(targets::id)
                    .select(targets::config)
                    .load::<String>(tx)?;
                for source in targets {
                    let target: Value = serde_json::from_str(&source)
                        .map_err(|e| AwsError::internal(e.to_string()))?;
                    // Keep the source event in the queue; input selection happens at delivery so failures are inspectable.
                    diesel::insert_into(deliveries::table)
                        .values((
                            deliveries::id.eq(&ids::request_id()),
                            deliveries::account.eq(&ctx.account_id),
                            deliveries::region.eq(&ctx.region),
                            deliveries::endpoint.eq(&ctx.base_url),
                            deliveries::target.eq(target["Arn"].as_str().unwrap_or("")),
                            deliveries::config.eq(&source),
                            deliveries::event.eq(&event.to_string()),
                        ))
                        .execute(tx)?;
                }
            }
            Ok(())
        })
    }
    pub fn history(&self, ctx: &RequestContext) -> Result<Value, AwsError> {
        self.db.read(|c|{
        let rows=deliveries::table.filter(deliveries::account.eq(&ctx.account_id)).filter(deliveries::region.eq(&ctx.region)).order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid").desc()).limit(100).select((deliveries::id,deliveries::target,deliveries::state,deliveries::attempts,deliveries::error)).load::<(String,String,String,i32,Option<String>)>(c)?;
        Ok(json!({"deliveries":rows.into_iter().map(|(id,target,state,attempts,error)|json!({"id":id,"target":target,"state":state,"attempts":attempts,"error":error})).collect::<Vec<_>>() }))
    })
    }
    pub fn start(this: &Arc<Self>) {
        let weak = Arc::downgrade(this);
        std::thread::Builder::new()
            .name("roto-eventbridge".into())
            .spawn(move || {
                while let Some(events) = weak.upgrade() {
                    if let Err(e) = events.deliver_next() {
                        eprintln!("EventBridge delivery: {e}");
                    }
                    drop(events);
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
            .expect("EventBridge worker");
    }
    fn deliver_next(&self) -> Result<(), AwsError> {
        let row = self.db.read(|c| {
            Ok(deliveries::table
                .filter(deliveries::state.eq("queued"))
                .filter(deliveries::due.le(now()))
                .order(diesel::dsl::sql::<diesel::sql_types::BigInt>("rowid"))
                .select((
                    deliveries::id,
                    deliveries::account,
                    deliveries::region,
                    deliveries::endpoint,
                    deliveries::target,
                    deliveries::config,
                    deliveries::event,
                    deliveries::attempts,
                ))
                .first::<(String, String, String, String, String, String, String, i32)>(c)
                .optional()?)
        })?;
        let Some((id, account_id, region, base_url, arn, config, event, attempts)) = row else {
            return Ok(());
        };
        let ctx = RequestContext {
            account_id,
            region,
            base_url,
            request_id: id.clone(),
            access_key: None,
        };
        let result = (|| {
            let target: Value =
                serde_json::from_str(&config).map_err(|e| AwsError::internal(e.to_string()))?;
            let event: Value =
                serde_json::from_str(&event).map_err(|e| AwsError::internal(e.to_string()))?;
            let event = if let Some(input) = target["Input"].as_str() {
                serde_json::from_str(input).map_err(|e| invalid(e.to_string()))?
            } else if let Some(path) = target["InputPath"].as_str() {
                input_path(&event, path)?.clone()
            } else {
                event
            };
            if arn.starts_with("arn:aws:lambda:") {
                self.lambda.enqueue(&ctx, &arn, event)?;
            } else {
                let group = target["SqsParameters"]["MessageGroupId"]
                    .as_str()
                    .map(str::to_owned);
                if !self
                    .sqs
                    .deliver(&arn, &event.to_string(), &BTreeMap::new(), group, None)?
                {
                    return Err(missing(&arn));
                }
            }
            Ok(())
        })();
        let attempts = attempts + 1;
        let state = if result.is_ok() {
            "succeeded"
        } else if attempts >= 3 {
            "failed"
        } else {
            "queued"
        };
        let error = result.err().map(|e| e.to_string());
        self.db.transaction(|tx| {
            diesel::update(deliveries::table.filter(deliveries::id.eq(&id)))
                .set((
                    deliveries::state.eq(&state),
                    deliveries::attempts.eq(&attempts),
                    deliveries::due.eq(now() + 1000 * i64::from(attempts)),
                    deliveries::error.eq(&error),
                ))
                .execute(tx)?;
            Ok(())
        })
    }
    fn set_state(
        &self,
        ctx: &RequestContext,
        bus: Option<&str>,
        rule: &str,
        state: &str,
    ) -> Result<(), AwsError> {
        let bus = bus_name(ctx, bus)?;
        self.db.transaction(|tx| {
            let mut value = load_rule(tx, ctx, &bus, rule)?;
            value["State"] = json!(state);
            diesel::update(
                rules::table
                    .filter(rules::account.eq(&ctx.account_id))
                    .filter(rules::region.eq(&ctx.region))
                    .filter(rules::bus.eq(&bus))
                    .filter(rules::name.eq(&rule)),
            )
            .set(rules::config.eq(&value.to_string()))
            .execute(tx)?;
            Ok(())
        })
    }
}

impl Service for EventBridge {
    fn create_event_bus(
        &self,
        ctx: &RequestContext,
        i: CreateEventBusRequest,
    ) -> Result<CreateEventBusResponse, AwsError> {
        name(&i.name)?;
        supported(&i.to_json(), &["Name", "Description"])?;
        if i.name == "default" {
            return Err(invalid("The default event bus already exists"));
        }
        let arn = bus_arn(ctx, &i.name);
        let config = json!({"Name":i.name,"Arn":arn,"Description":i.description});
        self.db.transaction(|tx| {
            let exists: bool = diesel::select(diesel::dsl::exists(
                buses::table
                    .filter(buses::account.eq(&ctx.account_id))
                    .filter(buses::region.eq(&ctx.region))
                    .filter(buses::name.eq(&i.name)),
            ))
            .first::<bool>(tx)?;
            if exists {
                return Err(AwsError::sender(
                    400,
                    "ResourceAlreadyExistsException",
                    "Event bus already exists",
                ));
            }
            diesel::insert_into(buses::table)
                .values((
                    buses::account.eq(&ctx.account_id),
                    buses::region.eq(&ctx.region),
                    buses::name.eq(&i.name),
                    buses::config.eq(&config.to_string()),
                ))
                .execute(tx)?;
            Ok(())
        })?;
        CreateEventBusResponse::from_json(
            &json!({"EventBusArn":arn,"Description":i.description}),
            "",
        )
    }
    fn describe_event_bus(
        &self,
        ctx: &RequestContext,
        i: DescribeEventBusRequest,
    ) -> Result<DescribeEventBusResponse, AwsError> {
        let bus = bus_name(ctx, i.name.as_deref())?;
        let value = self.db.transaction(|tx| {
            ensure_bus(tx, ctx, &bus)?;
            if bus == "default" {
                return Ok(json!({"Name":bus,"Arn":bus_arn(ctx,&bus)}));
            }
            let text: String = buses::table
                .filter(buses::account.eq(&ctx.account_id))
                .filter(buses::region.eq(&ctx.region))
                .filter(buses::name.eq(&bus))
                .select(buses::config)
                .first::<String>(tx)?;
            serde_json::from_str(&text).map_err(|e| AwsError::internal(e.to_string()))
        })?;
        DescribeEventBusResponse::from_json(&value, "")
    }
    fn delete_event_bus(
        &self,
        ctx: &RequestContext,
        i: DeleteEventBusRequest,
    ) -> Result<(), AwsError> {
        let bus = bus_name(ctx, Some(&i.name))?;
        if bus == "default" {
            return Err(invalid("Cannot delete default event bus"));
        }
        self.db.transaction(|tx| {
            let count: i64 = rules::table
                .filter(rules::account.eq(&ctx.account_id))
                .filter(rules::region.eq(&ctx.region))
                .filter(rules::bus.eq(&bus))
                .count()
                .first::<i64>(tx)?;
            if count > 0 {
                return Err(invalid("Event bus still has rules"));
            }
            diesel::delete(
                buses::table
                    .filter(buses::account.eq(&ctx.account_id))
                    .filter(buses::region.eq(&ctx.region))
                    .filter(buses::name.eq(&bus)),
            )
            .execute(tx)?;
            Ok(())
        })
    }
    fn list_event_buses(
        &self,
        ctx: &RequestContext,
        i: ListEventBusesRequest,
    ) -> Result<ListEventBusesResponse, AwsError> {
        let mut values = self.db.read(|c| {
            let texts = buses::table
                .filter(buses::account.eq(&ctx.account_id))
                .filter(buses::region.eq(&ctx.region))
                .order(buses::name)
                .select(buses::config)
                .load::<String>(c)?;
            Ok(texts
                .iter()
                .map(|s| serde_json::from_str::<Value>(s).unwrap())
                .collect::<Vec<_>>())
        })?;
        values.push(json!({"Name":"default","Arn":bus_arn(ctx,"default")}));
        values.sort_by(|a, b| a["Name"].as_str().cmp(&b["Name"].as_str()));
        if let Some(prefix) = i.name_prefix {
            values.retain(|v| v["Name"].as_str().unwrap().starts_with(&prefix));
        }
        let (values, next) = page(values, i.limit, i.next_token, "Name")?;
        ListEventBusesResponse::from_json(&json!({"EventBuses":values,"NextToken":next}), "")
    }
    fn put_rule(
        &self,
        ctx: &RequestContext,
        i: PutRuleRequest,
    ) -> Result<PutRuleResponse, AwsError> {
        name(&i.name)?;
        if i.name.len() > 64 {
            return Err(invalid("Rule name exceeds 64 characters"));
        }
        supported(
            &i.to_json(),
            &[
                "Name",
                "EventBusName",
                "EventPattern",
                "Description",
                "State",
            ],
        )?;
        let pattern = i
            .event_pattern
            .as_deref()
            .ok_or_else(|| invalid("EventPattern is required"))?;
        pattern::parse(pattern)?;
        let state = i.state.as_deref().unwrap_or("ENABLED");
        if !matches!(state, "ENABLED" | "DISABLED") {
            return Err(invalid("Unsupported rule state"));
        }
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        let arn = rule_arn(ctx, &bus, &i.name);
        let value = json!({"Name":i.name,"Arn":arn,"EventBusName":bus,"EventPattern":pattern,"State":state,"Description":i.description});
        self.db.transaction(|tx| {
            ensure_bus(tx, ctx, &bus)?;
            diesel::insert_into(rules::table)
                .values((
                    rules::account.eq(&ctx.account_id),
                    rules::region.eq(&ctx.region),
                    rules::bus.eq(&bus),
                    rules::name.eq(&i.name),
                    rules::config.eq(&value.to_string()),
                ))
                .on_conflict((rules::account, rules::region, rules::bus, rules::name))
                .do_update()
                .set(rules::config.eq(diesel::upsert::excluded(rules::config)))
                .execute(tx)?;
            Ok(())
        })?;
        Ok(PutRuleResponse {
            rule_arn: Some(arn),
        })
    }
    fn describe_rule(
        &self,
        ctx: &RequestContext,
        i: DescribeRuleRequest,
    ) -> Result<DescribeRuleResponse, AwsError> {
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        let value = self
            .db
            .transaction(|tx| load_rule(tx, ctx, &bus, &i.name))?;
        DescribeRuleResponse::from_json(&value, "")
    }
    fn delete_rule(&self, ctx: &RequestContext, i: DeleteRuleRequest) -> Result<(), AwsError> {
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        self.db.transaction(|tx| {
            let count: i64 = targets::table
                .filter(targets::account.eq(&ctx.account_id))
                .filter(targets::region.eq(&ctx.region))
                .filter(targets::bus.eq(&bus))
                .filter(targets::rule.eq(&i.name))
                .count()
                .first::<i64>(tx)?;
            if count > 0 {
                return Err(invalid("Remove targets before deleting the rule"));
            }
            diesel::delete(
                rules::table
                    .filter(rules::account.eq(&ctx.account_id))
                    .filter(rules::region.eq(&ctx.region))
                    .filter(rules::bus.eq(&bus))
                    .filter(rules::name.eq(&i.name)),
            )
            .execute(tx)?;
            Ok(())
        })
    }
    fn enable_rule(&self, ctx: &RequestContext, i: EnableRuleRequest) -> Result<(), AwsError> {
        self.set_state(ctx, i.event_bus_name.as_deref(), &i.name, "ENABLED")
    }
    fn disable_rule(&self, ctx: &RequestContext, i: DisableRuleRequest) -> Result<(), AwsError> {
        self.set_state(ctx, i.event_bus_name.as_deref(), &i.name, "DISABLED")
    }
    fn list_rules(
        &self,
        ctx: &RequestContext,
        i: ListRulesRequest,
    ) -> Result<ListRulesResponse, AwsError> {
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        let mut values = self.db.transaction(|tx| {
            ensure_bus(tx, ctx, &bus)?;
            let texts = rules::table
                .filter(rules::account.eq(&ctx.account_id))
                .filter(rules::region.eq(&ctx.region))
                .filter(rules::bus.eq(&bus))
                .order(rules::name)
                .select(rules::config)
                .load::<String>(tx)?;
            Ok(texts
                .iter()
                .map(|s| serde_json::from_str::<Value>(s).unwrap())
                .collect::<Vec<_>>())
        })?;
        if let Some(prefix) = i.name_prefix {
            values.retain(|v| v["Name"].as_str().unwrap().starts_with(&prefix));
        }
        let (values, next) = page(values, i.limit, i.next_token, "Name")?;
        ListRulesResponse::from_json(&json!({"Rules":values,"NextToken":next}), "")
    }
    fn put_targets(
        &self,
        ctx: &RequestContext,
        i: PutTargetsRequest,
    ) -> Result<PutTargetsResponse, AwsError> {
        if i.targets.is_empty() || i.targets.len() > 10 {
            return Err(invalid("Targets must contain 1–10 entries"));
        }
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        self.db.transaction(|tx| {
            load_rule(tx, ctx, &bus, &i.rule)?;
            let mut failed = Vec::new();
            for target in i.targets {
                let result = (|| {
                    name(&target.id)?;
                    if target.id.len() > 64 {
                        return Err(invalid("Target ID exceeds 64 characters"));
                    }
                    supported(
                        &target.to_json(),
                        &["Id", "Arn", "Input", "InputPath", "SqsParameters"],
                    )?;
                    let lambda_prefix =
                        format!("arn:aws:lambda:{}:{}:function:", ctx.region, ctx.account_id);
                    let sqs_prefix = format!("arn:aws:sqs:{}:{}:", ctx.region, ctx.account_id);
                    if !target.arn.starts_with(&lambda_prefix)
                        && !target.arn.starts_with(&sqs_prefix)
                    {
                        return Err(invalid(
                            "Only Lambda and SQS targets in the same account/region are supported",
                        ));
                    }
                    if target.input.is_some() && target.input_path.is_some() {
                        return Err(invalid("Input and InputPath are mutually exclusive"));
                    }
                    if let Some(input) = &target.input {
                        serde_json::from_str::<Value>(input)
                            .map_err(|_| invalid("Input must be valid JSON"))?;
                    }
                    if let Some(path) = &target.input_path
                        && path != "$"
                        && (!path.starts_with("$.") || path.contains(['[', ']', '*']))
                    {
                        return Err(invalid("InputPath supports dot-separated object keys"));
                    }
                    diesel::insert_into(targets::table)
                        .values((
                            targets::account.eq(&ctx.account_id),
                            targets::region.eq(&ctx.region),
                            targets::bus.eq(&bus),
                            targets::rule.eq(&i.rule),
                            targets::id.eq(&target.id),
                            targets::config.eq(&target.to_json().to_string()),
                        ))
                        .on_conflict((
                            targets::account,
                            targets::region,
                            targets::bus,
                            targets::rule,
                            targets::id,
                        ))
                        .do_update()
                        .set(targets::config.eq(diesel::upsert::excluded(targets::config)))
                        .execute(tx)?;
                    Ok(())
                })();
                if let Err(e) = result {
                    failed.push(
                        json!({"TargetId":target.id,"ErrorCode":e.code,"ErrorMessage":e.message}),
                    );
                }
            }
            PutTargetsResponse::from_json(
                &json!({"FailedEntryCount":failed.len(),"FailedEntries":failed}),
                "",
            )
        })
    }
    fn remove_targets(
        &self,
        ctx: &RequestContext,
        i: RemoveTargetsRequest,
    ) -> Result<RemoveTargetsResponse, AwsError> {
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        self.db.transaction(|tx| {
            load_rule(tx, ctx, &bus, &i.rule)?;
            for id in i.ids {
                diesel::delete(
                    targets::table
                        .filter(targets::account.eq(&ctx.account_id))
                        .filter(targets::region.eq(&ctx.region))
                        .filter(targets::bus.eq(&bus))
                        .filter(targets::rule.eq(&i.rule))
                        .filter(targets::id.eq(&id)),
                )
                .execute(tx)?;
            }
            Ok(RemoveTargetsResponse {
                failed_entry_count: Some(0),
                ..Default::default()
            })
        })
    }
    fn list_targets_by_rule(
        &self,
        ctx: &RequestContext,
        i: ListTargetsByRuleRequest,
    ) -> Result<ListTargetsByRuleResponse, AwsError> {
        let bus = bus_name(ctx, i.event_bus_name.as_deref())?;
        let values = self.db.transaction(|tx| {
            load_rule(tx, ctx, &bus, &i.rule)?;
            let texts = targets::table
                .filter(targets::account.eq(&ctx.account_id))
                .filter(targets::region.eq(&ctx.region))
                .filter(targets::bus.eq(&bus))
                .filter(targets::rule.eq(&i.rule))
                .order(targets::id)
                .select(targets::config)
                .load::<String>(tx)?;
            Ok(texts
                .iter()
                .map(|s| serde_json::from_str::<Value>(s).unwrap())
                .collect::<Vec<_>>())
        })?;
        let (values, next) = page(values, i.limit, i.next_token, "Id")?;
        ListTargetsByRuleResponse::from_json(&json!({"Targets":values,"NextToken":next}), "")
    }
    fn put_events(
        &self,
        ctx: &RequestContext,
        i: PutEventsRequest,
    ) -> Result<PutEventsResponse, AwsError> {
        if i.endpoint_id.is_some() {
            return Err(AwsError::not_implemented("events", "global endpoints"));
        }
        if i.entries.is_empty() || i.entries.len() > 10 {
            return Err(invalid("Entries must contain 1–10 events"));
        }
        let mut results = Vec::new();
        let mut failures = 0;
        for entry in i.entries {
            let result = (|| {
                let source = entry
                    .source
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid("Source is required"))?;
                let detail_type = entry
                    .detail_type
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| invalid("DetailType is required"))?;
                let detail: Value = serde_json::from_str(
                    entry
                        .detail
                        .as_deref()
                        .ok_or_else(|| invalid("Detail is required"))?,
                )
                .map_err(|_| invalid("Detail must be valid JSON"))?;
                if !detail.is_object() {
                    return Err(invalid("Detail must be a JSON object"));
                }
                let id = ids::request_id();
                let event = json!({"version":"0","id":id,"source":source,"detail-type":detail_type,"account":ctx.account_id,"region":ctx.region,"time":entry.time.unwrap_or_else(Timestamp::now).to_iso8601(),"resources":entry.resources,"detail":detail});
                self.enqueue_event(
                    ctx,
                    entry.event_bus_name.as_deref().unwrap_or("default"),
                    event,
                )?;
                Ok(id)
            })();
            match result {
                Ok(id) => results.push(PutEventsResultEntry {
                    event_id: Some(id),
                    ..Default::default()
                }),
                Err(e) => {
                    failures += 1;
                    results.push(PutEventsResultEntry {
                        error_code: Some(e.code),
                        error_message: Some(e.message),
                        ..Default::default()
                    });
                }
            }
        }
        Ok(PutEventsResponse {
            entries: results,
            failed_entry_count: Some(failures),
        })
    }
    fn test_event_pattern(
        &self,
        _ctx: &RequestContext,
        i: TestEventPatternRequest,
    ) -> Result<TestEventPatternResponse, AwsError> {
        let pattern = pattern::parse(&i.event_pattern)?;
        let event: Value =
            serde_json::from_str(&i.event).map_err(|_| invalid("Event must be valid JSON"))?;
        Ok(TestEventPatternResponse {
            result: Some(pattern::matches(&pattern, &event)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roto_core::store::StoreOptions;
    #[test]
    fn deliveries_survive_restart_and_fail_after_three_attempts() {
        let dir = std::env::temp_dir().join(format!("roto-events-{}", ids::request_id()));
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
            access_key: None,
        };
        let open = || {
            let store = Store::open(&dir, StoreOptions::default()).unwrap();
            let lambda =
                Arc::new(roto_svc_lambda::Lambda::new(&store, Default::default()).unwrap());
            let sqs = roto_svc_sqs::SqsHandler::new(&store).unwrap().0;
            EventBridge::new(&store, lambda, sqs).unwrap()
        };
        {
            let events = open();
            dispatch(
                &events,
                &ctx,
                "PutRule",
                &json!({"Name":"test","EventPattern":"{\"source\":[\"app\"]}"}),
            )
            .unwrap();
            dispatch(&events,&ctx,"PutTargets",&json!({"Rule":"test","Targets":[{"Id":"missing","Arn":"arn:aws:sqs:us-east-1:123456789012:missing"}]})).unwrap();
            events
                .enqueue_event(&ctx, "default", json!({"source":"app"}))
                .unwrap();
            let mut other = ctx.clone();
            other.region = "eu-west-1".into();
            assert!(
                events.history(&other).unwrap()["deliveries"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
        }
        let events = open();
        assert_eq!(
            events.history(&ctx).unwrap()["deliveries"][0]["state"],
            "queued"
        );
        for _ in 0..3 {
            events
                .db
                .transaction(|tx| {
                    diesel::update(deliveries::table)
                        .set(deliveries::due.eq(0_i64))
                        .execute(tx)?;
                    Ok(())
                })
                .unwrap();
            events.deliver_next().unwrap();
        }
        let history = events.history(&ctx).unwrap();
        assert_eq!(history["deliveries"][0]["state"], "failed");
        assert_eq!(history["deliveries"][0]["attempts"], 3);
        drop(events);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
