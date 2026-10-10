//! Persistent SQS event-source mappings. SQS owns leases and retries; Lambda executes once.
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::schema::*;
use diesel::prelude::*;
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::{FromJson, ToJson};
use serde_json::{Value, json};

use crate::executor::execute;
use crate::generated::*;
use crate::service::{Lambda, arn, conflict, invalid, missing, now};

fn supported(input: &Value, allowed: &[&str]) -> Result<(), AwsError> {
    for (key, value) in input.as_object().unwrap() {
        if !allowed.contains(&key.as_str())
            && !value.is_null()
            && value.as_array().is_none_or(|a| !a.is_empty())
            && value.as_object().is_none_or(|a| !a.is_empty())
        {
            return Err(AwsError::not_implemented(
                "lambda",
                &format!("event source setting {key}"),
            ));
        }
    }
    if input["MaximumBatchingWindowInSeconds"]
        .as_i64()
        .is_some_and(|n| n != 0)
    {
        return Err(AwsError::not_implemented(
            "lambda",
            "event source batching windows",
        ));
    }
    Ok(())
}
fn mapping(value: &Value) -> Result<EventSourceMappingConfiguration, AwsError> {
    EventSourceMappingConfiguration::from_json(value, "")
}
fn scope(ctx: &RequestContext, source: &str, function: &str) -> Result<(), AwsError> {
    if !source.starts_with(&format!("arn:aws:sqs:{}:{}:", ctx.region, ctx.account_id))
        || !function.starts_with(&format!(
            "arn:aws:lambda:{}:{}:function:",
            ctx.region, ctx.account_id
        ))
    {
        return Err(invalid(
            "Event source and function must be in the same account and region",
        ));
    }
    Ok(())
}

impl Lambda {
    fn validate_mapping(&self, ctx: &RequestContext, value: &Value) -> Result<(), AwsError> {
        let batch = value["BatchSize"].as_i64().unwrap_or(10);
        if !(1..=10).contains(&batch) {
            return Err(invalid("BatchSize must be between 1 and 10"));
        }
        if value["FunctionResponseTypes"]
            .as_array()
            .is_some_and(|a| a.iter().any(|s| s != "ReportBatchItemFailures") || a.len() > 1)
        {
            return Err(invalid("Only ReportBatchItemFailures is supported"));
        }
        let source = value["EventSourceArn"]
            .as_str()
            .ok_or_else(|| invalid("EventSourceArn is required"))?;
        let function = value["FunctionArn"].as_str().unwrap();
        scope(ctx, source, function)?;
        let config = self.load(function)?;
        let sqs = self.sqs.as_ref().ok_or_else(|| {
            AwsError::not_implemented(
                "lambda",
                "SQS event sources without a connected SQS service",
            )
        })?;
        let visibility = sqs.event_source_visibility(ctx, source)?;
        if visibility < config["Timeout"].as_i64().unwrap_or(3) {
            return Err(invalid(
                "Queue visibility timeout must be at least the function timeout",
            ));
        }
        Ok(())
    }
    pub(crate) fn create_mapping(
        &self,
        ctx: &RequestContext,
        i: CreateEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        supported(
            &i.to_json(),
            &[
                "EventSourceArn",
                "FunctionName",
                "Enabled",
                "BatchSize",
                "MaximumBatchingWindowInSeconds",
                "FunctionResponseTypes",
            ],
        )?;
        let function = arn(ctx, &i.function_name)?;
        let id = ids::request_id();
        let value = json!({"UUID":id,"EventSourceArn":i.event_source_arn,"FunctionArn":function,"BatchSize":i.batch_size.unwrap_or(10),"MaximumBatchingWindowInSeconds":0,"FunctionResponseTypes":i.function_response_types,"State":if i.enabled.unwrap_or(true) {"Enabled"} else {"Disabled"},"StateTransitionReason":"USER_INITIATED","LastModified":now()/1000,"LastProcessingResult":"No records processed", "EventSourceMappingArn":format!("arn:aws:lambda:{}:{}:event-source-mapping:{id}",ctx.region,ctx.account_id)});
        self.validate_mapping(ctx, &value)?;
        self.db.transaction(|tx| {
            let exists: bool = diesel::select(diesel::dsl::exists(
                event_source_mappings::table
                    .filter(event_source_mappings::account.eq(&(ctx.account_id)))
                    .filter(event_source_mappings::region.eq(&(ctx.region)))
                    .filter(
                        event_source_mappings::source
                            .eq(value["EventSourceArn"].as_str().unwrap_or_default()),
                    )
                    .filter(event_source_mappings::function.eq(&(function))),
            ))
            .first::<bool>(tx)?;
            if exists {
                return Err(conflict(
                    "An event source mapping already exists for this queue and function",
                ));
            }
            diesel::insert_into(event_source_mappings::table)
                .values((
                    event_source_mappings::uuid.eq(&(id)),
                    event_source_mappings::account.eq(&(ctx.account_id)),
                    event_source_mappings::region.eq(&(ctx.region)),
                    event_source_mappings::source
                        .eq(value["EventSourceArn"].as_str().unwrap_or_default()),
                    event_source_mappings::function.eq(&(function)),
                    event_source_mappings::config.eq(&(value.to_string())),
                ))
                .execute(tx)?;
            Ok(())
        })?;
        mapping(&value)
    }
    pub(crate) fn get_mapping(
        &self,
        ctx: &RequestContext,
        id: &str,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        self.db.read(|c| {
            let text: Option<String> = event_source_mappings::table
                .filter(event_source_mappings::uuid.eq(&(id)))
                .filter(event_source_mappings::account.eq(&(ctx.account_id)))
                .filter(event_source_mappings::region.eq(&(ctx.region)))
                .select(event_source_mappings::config)
                .first::<String>(c)
                .optional()?;
            mapping(
                &serde_json::from_str::<Value>(&text.ok_or_else(|| missing(id))?)
                    .map_err(|e| AwsError::internal(e.to_string()))?,
            )
        })
    }
    pub(crate) fn delete_mapping(
        &self,
        ctx: &RequestContext,
        id: &str,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        let mut m = self.get_mapping(ctx, id)?;
        self.db.transaction(|tx| {
            diesel::delete(
                event_source_mappings::table
                    .filter(event_source_mappings::uuid.eq(&(id)))
                    .filter(event_source_mappings::account.eq(&(ctx.account_id)))
                    .filter(event_source_mappings::region.eq(&(ctx.region))),
            )
            .execute(tx)?;
            Ok(())
        })?;
        m.state = Some("Deleting".into());
        Ok(m)
    }
    pub(crate) fn update_mapping(
        &self,
        ctx: &RequestContext,
        i: UpdateEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        let responses =
            (!i.function_response_types.is_empty()).then(|| i.function_response_types.clone());
        self.update_mapping_input(ctx, i, responses)
    }
    pub(crate) fn update_mapping_input(
        &self,
        ctx: &RequestContext,
        i: UpdateEventSourceMappingRequest,
        responses: Option<Vec<String>>,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        supported(
            &i.to_json(),
            &[
                "UUID",
                "FunctionName",
                "Enabled",
                "BatchSize",
                "MaximumBatchingWindowInSeconds",
                "FunctionResponseTypes",
            ],
        )?;
        let mut value = self.get_mapping(ctx, &i.uuid)?.to_json();
        if let Some(name) = i.function_name {
            value["FunctionArn"] = json!(arn(ctx, &name)?);
        }
        if let Some(batch) = i.batch_size {
            value["BatchSize"] = json!(batch);
        }
        if let Some(enabled) = i.enabled {
            value["State"] = json!(if enabled { "Enabled" } else { "Disabled" });
        }
        if let Some(responses) = responses {
            value["FunctionResponseTypes"] = json!(responses);
        }
        value["LastModified"] = json!(now() / 1000);
        self.validate_mapping(ctx, &value)?;
        self.db.transaction(|tx| {
            let exists: bool = diesel::select(diesel::dsl::exists(
                event_source_mappings::table
                    .filter(event_source_mappings::account.eq(&(ctx.account_id)))
                    .filter(event_source_mappings::region.eq(&(ctx.region)))
                    .filter(
                        event_source_mappings::source
                            .eq(value["EventSourceArn"].as_str().unwrap_or_default()),
                    )
                    .filter(
                        event_source_mappings::function
                            .eq(value["FunctionArn"].as_str().unwrap_or_default()),
                    )
                    .filter(event_source_mappings::uuid.ne(&(i.uuid))),
            ))
            .first::<bool>(tx)?;
            if exists {
                return Err(conflict(
                    "An event source mapping already exists for this queue and function",
                ));
            }
            if diesel::update(
                event_source_mappings::table
                    .filter(event_source_mappings::uuid.eq(&(i.uuid)))
                    .filter(event_source_mappings::account.eq(&(ctx.account_id)))
                    .filter(event_source_mappings::region.eq(&(ctx.region))),
            )
            .set((
                event_source_mappings::function
                    .eq(value["FunctionArn"].as_str().unwrap_or_default()),
                event_source_mappings::config.eq(&(value.to_string())),
            ))
            .execute(tx)?
                == 0
            {
                return Err(missing(&i.uuid));
            }
            Ok(())
        })?;
        mapping(&value)
    }
    pub(crate) fn list_mappings(
        &self,
        ctx: &RequestContext,
        i: ListEventSourceMappingsRequest,
    ) -> Result<ListEventSourceMappingsResponse, AwsError> {
        let function = i.function_name.map(|n| arn(ctx, &n)).transpose()?;
        let limit = i.max_items.unwrap_or(100);
        if !(1..=10000).contains(&limit) {
            return Err(invalid("MaxItems must be between 1 and 10000"));
        }
        self.db.read(|c| {
            let mut q = event_source_mappings::table
                .filter(event_source_mappings::account.eq(&ctx.account_id))
                .filter(event_source_mappings::region.eq(&ctx.region))
                .into_boxed();
            if let Some(source) = &i.event_source_arn {
                q = q.filter(event_source_mappings::source.eq(source));
            }
            if let Some(function) = &function {
                q = q.filter(event_source_mappings::function.eq(function));
            }
            let rows = q
                .filter(event_source_mappings::uuid.gt(i.marker.unwrap_or_default()))
                .order(event_source_mappings::uuid)
                .limit(i64::from(limit) + 1)
                .select(event_source_mappings::config)
                .load::<String>(c)?;
            let mut values = Vec::new();
            for text in rows {
                values.push(mapping(
                    &serde_json::from_str::<Value>(&text)
                        .map_err(|e| AwsError::internal(e.to_string()))?,
                )?);
            }
            let next_marker = if values.len() > limit as usize {
                values.pop();
                values.last().and_then(|v| v.uuid.clone())
            } else {
                None
            };
            Ok(ListEventSourceMappingsResponse {
                event_source_mappings: values,
                next_marker,
            })
        })
    }
    pub(crate) fn start_sqs_worker(this: &Arc<Self>, endpoint: String) {
        if this.sqs.is_none() {
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(this);
        std::thread::Builder::new()
            .name("roto-sqs-lambda".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("SQS Lambda runtime");
                while let Some(lambda) = weak.upgrade() {
                    let ids = lambda.db.read(|c| {
                        Ok(event_source_mappings::table
                            .order(event_source_mappings::uuid)
                            .select((
                                event_source_mappings::uuid,
                                event_source_mappings::account,
                                event_source_mappings::region,
                            ))
                            .load::<(String, String, String)>(c)?)
                    });
                    match ids {
                        Ok(ids) => {
                            for (id, account_id, region) in ids {
                                let ctx = RequestContext {
                                    account_id,
                                    region,
                                    access_key: None,
                                    request_id: ids::request_id(),
                                    base_url: endpoint.clone(),
                                };
                                if let Err(e) = lambda.process_sqs_batch(&ctx, &id, &runtime) {
                                    lambda.processing_result(&ctx, &id, &format!("Error: {e}"));
                                }
                            }
                        }
                        Err(e) => eprintln!("SQS Lambda mappings: {e}"),
                    }
                    drop(lambda);
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
            .expect("SQS Lambda worker");
    }
    fn processing_result(&self, ctx: &RequestContext, id: &str, result: &str) {
        if let Err(e) = self.db.transaction(|tx| {
            diesel::update(
                event_source_mappings::table
                    .filter(event_source_mappings::uuid.eq(id))
                    .filter(event_source_mappings::account.eq(&ctx.account_id))
                    .filter(event_source_mappings::region.eq(&ctx.region)),
            )
            .set(event_source_mappings::config.eq(json_set(
                event_source_mappings::config,
                "$.LastProcessingResult",
                result,
            )))
            .execute(tx)?;
            Ok(())
        }) {
            eprintln!("SQS Lambda status persistence: {e}");
        }
    }
    pub(crate) fn process_sqs_batch(
        &self,
        ctx: &RequestContext,
        id: &str,
        runtime: &tokio::runtime::Runtime,
    ) -> Result<(), AwsError> {
        let m = self.get_mapping(ctx, id)?;
        if m.state.as_deref() != Some("Enabled") {
            return Ok(());
        }
        let source = m.event_source_arn.as_deref().unwrap();
        let function = m.function_arn.as_deref().unwrap();
        // Check binding/configuration before claiming messages.
        let mut job = self.job(ctx, function, Value::Null, None)?;
        self.validate_mapping(ctx, &m.to_json())?;
        let sqs = self.sqs.as_ref().unwrap();
        let event = sqs.receive_event_batch(ctx, source, m.batch_size.unwrap_or(10) as usize)?;
        let records = event["Records"].as_array().unwrap();
        if records.is_empty() {
            return Ok(());
        }
        job.event = event.clone();
        self.record(&job, "running")?;
        let outcome = runtime.block_on(execute(&job));
        self.finish(&job, &outcome, 1, false)?;
        if outcome.failed {
            self.processing_result(ctx, id, "Function error");
            return Ok(());
        }
        let failures = if m
            .function_response_types
            .iter()
            .any(|s| s == "ReportBatchItemFailures")
        {
            match partial_failures(&outcome.payload, records) {
                Ok(ids) => ids,
                Err(e) => {
                    self.processing_result(
                        ctx,
                        id,
                        &format!("Invalid partial batch response: {e}"),
                    );
                    return Ok(());
                }
            }
        } else {
            BTreeSet::new()
        };
        for record in records {
            if !failures.contains(record["messageId"].as_str().unwrap()) {
                sqs.acknowledge_event(ctx, source, record["receiptHandle"].as_str().unwrap())?;
            }
        }
        self.processing_result(
            ctx,
            id,
            if failures.is_empty() {
                "OK"
            } else {
                "Partial batch failure"
            },
        );
        Ok(())
    }
}
fn partial_failures(payload: &Value, records: &[Value]) -> Result<BTreeSet<String>, &'static str> {
    if payload.is_null() || payload.get("batchItemFailures").is_none() {
        return Ok(BTreeSet::new());
    }
    let Some(items) = payload["batchItemFailures"].as_array() else {
        return Err("batchItemFailures must be an array");
    };
    let mut failures = BTreeSet::new();
    for item in items {
        let id = item["itemIdentifier"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("itemIdentifier must be a nonempty string")?;
        if !records.iter().any(|r| r["messageId"] == id) {
            return Err("unknown message ID");
        }
        failures.insert(id.to_owned());
    }
    Ok(failures)
}
