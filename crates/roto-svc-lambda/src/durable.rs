//! Durable execution backing store and checkpoint protocol for SDK steps and waits.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use diesel::prelude::*;
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::{Blob, FromJson, ToJson, base64};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::executor::{Job, execute};
use crate::generated::*;
use crate::schema::durable_executions as table;
use crate::service::{Lambda, conflict, invalid, missing, now};

#[path = "durable_extra.rs"]
mod extra;
use extra::{
    CallbackTimer, InvokeLink, expire_callbacks, history_event, next_due, replay_operations,
};

#[derive(Deserialize, Serialize)]
struct Execution {
    job: Job,
    name: String,
    input: String,
    token: String,
    operations: Vec<Value>, // Start-sequence order; timestamps stored in milliseconds.
    status: String,
    start: i64,
    deadline: i64,
    end: Option<i64>,
    result: Option<String>,
    error: Option<Value>,
    clients: BTreeMap<String, (Value, Value)>,
    #[serde(default)]
    history: Vec<Value>,
    #[serde(default)]
    callbacks: BTreeMap<String, CallbackTimer>,
    #[serde(default)]
    links: BTreeMap<String, InvokeLink>,
    #[serde(default)]
    wake: bool,
    #[serde(default = "default_retention")]
    retention: i64,
}

fn default_retention() -> i64 {
    86400000
}

fn token() -> String {
    base64::encode(ids::request_id().as_bytes())
}

fn decode(data: &str) -> Result<Execution, AwsError> {
    serde_json::from_str(data).map_err(|e| AwsError::internal(e.to_string()))
}

fn load(tx: &mut SqliteConnection, arn: &str) -> Result<Execution, AwsError> {
    let data = table::table
        .find(arn)
        .select(table::data)
        .first::<String>(tx)
        .optional()?
        .ok_or_else(|| missing(arn))?;
    decode(&data)
}

fn save(tx: &mut SqliteConnection, arn: &str, e: &Execution) -> Result<(), AwsError> {
    diesel::update(table::table.find(arn))
        .set(table::data.eq(serde_json::to_string(e).unwrap()))
        .execute(tx)?;
    Ok(())
}

fn scope(ctx: &RequestContext, arn: &str) -> Result<(), AwsError> {
    if !arn.starts_with(&format!(
        "arn:aws:lambda:{}:{}:function:",
        ctx.region, ctx.account_id
    )) {
        return Err(missing(arn));
    }
    Ok(())
}

fn check_token(e: &Execution, supplied: &str) -> Result<(), AwsError> {
    if e.status != "RUNNING" || e.token != supplied || now() >= e.deadline {
        return Err(conflict(
            "Checkpoint token is stale or execution is no longer running",
        ));
    }
    Ok(())
}

/// Runtime envelope timestamps are milliseconds; AWS REST APIs use epoch seconds.
fn api_operations(ops: &[Value]) -> Vec<Value> {
    fn convert(v: &mut Value) {
        if let Some(object) = v.as_object_mut() {
            for (key, value) in object {
                if key.ends_with("Timestamp") {
                    if let Some(ms) = value.as_i64() {
                        *value = json!(ms / 1000);
                    }
                } else {
                    convert(value);
                }
            }
        }
    }
    let mut ops = ops.to_vec();
    for op in &mut ops {
        convert(op);
    }
    ops
}

pub(crate) fn validate_config(config: &Value) -> Result<(), AwsError> {
    if let Some(durable) = config.get("DurableConfig") {
        if durable
            .get("ExecutionTimeout")
            .is_some_and(|n| !n.as_i64().is_some_and(|n| (1..=31622400).contains(&n)))
        {
            return Err(invalid(
                "DurableConfig.ExecutionTimeout must be 1–31622400 seconds",
            ));
        }
        if durable
            .get("RetentionPeriodInDays")
            .is_some_and(|n| !n.as_i64().is_some_and(|n| (1..=90).contains(&n)))
        {
            return Err(invalid("DurableConfig.RetentionPeriodInDays must be 1–90"));
        }
    }
    Ok(())
}

impl Lambda {
    pub(crate) fn durable_invoke(
        &self,
        ctx: &RequestContext,
        i: InvocationRequest,
        config: &Value,
    ) -> Result<InvocationResponse, AwsError> {
        if i.qualifier.is_none()
            && !i.function_name.rsplit(':').next().is_some_and(|suffix| {
                suffix == "$LATEST"
                    || i.function_name.split(':').count()
                        > if i.function_name.starts_with("arn:") {
                            7
                        } else {
                            1
                        }
            })
        {
            return Err(invalid(
                "Durable functions require a qualified function name",
            ));
        }
        let kind = i.invocation_type.as_deref().unwrap_or("RequestResponse");
        if kind == "DryRun" {
            return Ok(InvocationResponse {
                status_code: Some(204),
                ..Default::default()
            });
        }
        let input = i
            .payload
            .as_ref()
            .map_or(b"null".as_slice(), |b| b.0.as_slice());
        if input.len()
            > if kind == "Event" {
                1024 * 1024
            } else {
                6 * 1024 * 1024
            }
        {
            return Err(AwsError::sender(
                413,
                "RequestTooLargeException",
                "Payload too large",
            ));
        }
        let event: Value = serde_json::from_slice(if input.is_empty() { b"null" } else { input })
            .map_err(|e| {
            AwsError::sender(400, "InvalidRequestContentException", e.to_string())
        })?;
        let input = event.to_string();
        let job = self.job(
            ctx,
            config["FunctionArn"].as_str().unwrap(),
            event,
            i.client_context,
        )?;
        let name = i.durable_execution_name.unwrap_or_else(ids::request_id);
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(invalid("Invalid DurableExecutionName"));
        }
        let prefix = format!(
            "{}/durable-execution/{name}/",
            crate::versions::qualified(&job.arn)
        );
        let execution_arn = self.db.transaction(|tx| {
            let existing = table::table.filter(table::function_arn.eq(&job.arn)).filter(table::name.eq(&name))
                .select((table::arn, table::data)).first::<(String, String)>(tx).optional()?;
            if let Some((arn, data)) = existing {
                if decode(&data)?.input != input { return Err(conflict("Execution name already used with another input")); }
                return Ok(arn);
            }
            let id = ids::request_id();
            let arn = format!("{prefix}{id}");
            let start = now();
            let mut e = Execution {
                job: job.clone(), name: name.clone(), input: input.clone(), token: token(),
                operations: vec![json!({"Id":id,"Type":"EXECUTION","Status":"STARTED","StartTimestamp":start,"ExecutionDetails":{"InputPayload":input}})],
                status: "RUNNING".into(), start,
                deadline: start + config["DurableConfig"]["ExecutionTimeout"].as_i64().unwrap_or(86400) * 1000,
                end: None, result: None, error: None, clients: BTreeMap::new(),
                history: Vec::new(), callbacks: BTreeMap::new(), links: BTreeMap::new(), wake: false,
                retention: config["DurableConfig"]["RetentionPeriodInDays"].as_i64().unwrap_or(1) * 86400000,
            };
            let execution_timeout = (e.deadline-e.start)/1000;
            history_event(&mut e, "ExecutionStarted", Some(&id), json!({"ExecutionTimeout":execution_timeout,"Input":{"Payload":input,"Truncated":false}}));
            diesel::insert_into(table::table).values((table::arn.eq(&arn), table::function_arn.eq(&job.arn), table::name.eq(&name), table::data.eq(serde_json::to_string(&e).unwrap()), table::state.eq("queued"), table::due.eq(start))).execute(tx)?;
            Ok(arn)
        })?;
        if kind == "Event" {
            return Ok(InvocationResponse {
                status_code: Some(202),
                durable_execution_arn: Some(execution_arn),
                ..Default::default()
            });
        }
        let until = Instant::now() + Duration::from_secs(job.timeout);
        loop {
            let e = self.db.read(|tx| load(tx, &execution_arn))?;
            if e.status != "RUNNING" {
                return Ok(InvocationResponse {
                    status_code: Some(200),
                    durable_execution_arn: Some(execution_arn),
                    executed_version: Some(config["Version"].as_str().unwrap_or("$LATEST").into()),
                    function_error: (e.status != "SUCCEEDED").then(|| "Unhandled".into()),
                    payload: Some(Blob(if e.status == "SUCCEEDED" {
                        e.result.unwrap_or_else(|| "null".into()).into_bytes()
                    } else {
                        serde_json::to_vec(&e.error.unwrap_or(json!({"errorType":e.status})))
                            .unwrap()
                    })),
                    ..Default::default()
                });
            }
            if Instant::now() >= until {
                return Err(AwsError::receiver(
                    504,
                    "ServiceException",
                    "Synchronous durable invocation timed out; execution continues",
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub(crate) fn durable_checkpoint(
        &self,
        ctx: &RequestContext,
        i: CheckpointDurableExecutionRequest,
    ) -> Result<CheckpointDurableExecutionResponse, AwsError> {
        scope(ctx, &i.durable_execution_arn)?;
        let request = i.to_json();
        let response = self.db.transaction(|tx| {
            let mut e = load(tx, &i.durable_execution_arn)?;
            expire_callbacks(&mut e);
            if let Some(client) = &i.client_token
                && let Some((previous, response)) = e.clients.get(client) {
                if previous != &request { return Err(conflict("ClientToken was reused with different checkpoint updates")); }
                return Ok(response.clone());
            }
            check_token(&e, &i.checkpoint_token)?;
            for update in &i.updates { apply_update(&mut e, &i.durable_execution_arn, &update.to_json())?; }
            e.token = token();
            let response = json!({"CheckpointToken":e.token,"NewExecutionState":{"Operations":api_operations(&e.operations)}});
            if let Some(client) = &i.client_token { e.clients.insert(client.clone(), (request.clone(), response.clone())); }
            save(tx, &i.durable_execution_arn, &e)?;
            if e.status != "RUNNING" {
                diesel::update(table::table.find(&i.durable_execution_arn)).set((table::state.eq("done"),table::due.eq(now()+e.retention))).execute(tx)?;
            }
            Ok(response)
        })?;
        CheckpointDurableExecutionResponse::from_json(&response, "")
    }

    pub(crate) fn durable_state(
        &self,
        ctx: &RequestContext,
        i: GetDurableExecutionStateRequest,
    ) -> Result<GetDurableExecutionStateResponse, AwsError> {
        scope(ctx, &i.durable_execution_arn)?;
        self.db.read(|tx| {
            let e = load(tx, &i.durable_execution_arn)?;
            check_token(&e, &i.checkpoint_token)?;
            let operations = replay_operations(&e);
            let start = i.marker.as_deref().unwrap_or("0").parse::<usize>().map_err(|_| invalid("Invalid Marker"))?;
            let max = i.max_items.unwrap_or(100);
            if !(0..=1000).contains(&max) || start > operations.len() { return Err(invalid("Invalid pagination parameters")); }
            let end = (start + if max == 0 {100} else {max as usize}).min(operations.len());
            GetDurableExecutionStateResponse::from_json(&json!({"Operations":api_operations(&operations[start..end]),"NextMarker":(end < operations.len()).then(|| end.to_string())}), "")
        })
    }

    pub(crate) fn durable_get(
        &self,
        ctx: &RequestContext,
        i: GetDurableExecutionRequest,
    ) -> Result<GetDurableExecutionResponse, AwsError> {
        scope(ctx, &i.durable_execution_arn)?;
        self.db.read(|tx| {
            let e = load(tx, &i.durable_execution_arn)?;
            GetDurableExecutionResponse::from_json(&json!({
                "DurableExecutionArn":i.durable_execution_arn,"DurableExecutionName":e.name,
                "FunctionArn":crate::versions::qualified(&e.job.arn),"Version":crate::versions::qualified(&e.job.arn).rsplit(':').next(),
                "StartTimestamp":e.start / 1000,"EndTimestamp":e.end.map(|n| n / 1000),"Status":e.status,
                "InputPayload":Some(e.input),
                "Result":e.result,
                "Error":e.error
            }), "")
        })
    }

    pub(crate) fn durable_stop(
        &self,
        ctx: &RequestContext,
        i: StopDurableExecutionRequest,
    ) -> Result<StopDurableExecutionResponse, AwsError> {
        scope(ctx, &i.durable_execution_arn)?;
        self.db.transaction(|tx| {
            let mut e = load(tx, &i.durable_execution_arn)?;
            if e.status != "RUNNING" {
                return Err(conflict("Execution is already terminal"));
            }
            e.status = "STOPPED".into();
            e.end = Some(now());
            e.token = token();
            e.error = i.error.map(|e| e.to_json());
            let details = json!({"Error":{"Payload":e.error,"Truncated":false}});
            history_event(&mut e, "ExecutionStopped", None, details);
            save(tx, &i.durable_execution_arn, &e)?;
            diesel::update(table::table.find(&i.durable_execution_arn))
                .set((table::state.eq("done"), table::due.eq(now() + e.retention)))
                .execute(tx)?;
            Ok(StopDurableExecutionResponse {
                stop_timestamp: roto_protocol::Timestamp(now() / 1000),
            })
        })
    }

    pub(crate) fn run_durable(&self, runtime: &tokio::runtime::Runtime) -> Result<(), AwsError> {
        self.maintain_durable()?;
        let next = self.db.transaction(|tx| {
            let row = table::table.filter(table::state.eq_any(["queued", "suspended"]))
                .filter(table::due.le(now())).order(table::due).select((table::arn, table::data))
                .first::<(String, String)>(tx).optional()?;
            let Some((arn, data)) = row else {return Ok(None)};
            let mut e = decode(&data)?;
            if now() >= e.deadline {
                e.status = "TIMED_OUT".into(); e.end = Some(now()); e.token = token();
                history_event(&mut e, "ExecutionTimedOut", None, json!({}));
                save(tx, &arn, &e)?;
                diesel::update(table::table.find(&arn)).set((table::state.eq("done"), table::due.eq(now()+e.retention))).execute(tx)?;
                return Ok(None);
            }
            expire_callbacks(&mut e);
            e.wake = false;
            let mut expired_waits = Vec::new();
            for op in &mut e.operations {
                if op["Status"] == "STARTED" && op["Type"] == "WAIT"
                    && op["WaitDetails"]["ScheduledEndTimestamp"].as_i64().is_some_and(|n| n <= now()) {
                    op["Status"] = json!("SUCCEEDED"); op["EndTimestamp"] = json!(now());
                    expired_waits.push((op["Id"].as_str().unwrap().to_owned(), (op["WaitDetails"]["ScheduledEndTimestamp"].as_i64().unwrap()-op["StartTimestamp"].as_i64().unwrap_or(now()))/1000));
                }
                if op["Status"] == "PENDING" && op["Type"] == "STEP"
                    && op["StepDetails"]["NextAttemptTimestamp"].as_i64().is_some_and(|n| n <= now()) {
                    op["Status"] = json!("READY");
                }
            }
            for (id,duration) in expired_waits {history_event(&mut e,"WaitSucceeded",Some(&id),json!({"Duration":duration}));}
            e.token = token();
            let mut job = e.job.clone();
            job.request_id = ids::request_id();
            job.timeout = job.timeout.min(((e.deadline - now()).max(1000) as u64).div_ceil(1000));
            job.event = json!({"DurableExecutionArn":arn,"CheckpointToken":e.token,"InitialExecutionState":{"Operations":replay_operations(&e)}});
            save(tx, &arn, &e)?;
            diesel::update(table::table.find(&arn)).set(table::state.eq("running")).execute(tx)?;
            Ok(Some((arn, job)))
        })?;
        let Some((arn, job)) = next else {
            return Ok(());
        };
        self.record(&job, "running")?;
        let outcome = runtime.block_on(execute(&job, &self.rie_pool));
        self.finish(&job, &outcome, 1, false)?;
        self.db.transaction(|tx| {
            let mut e = load(tx, &arn)?;
            // Stop/reset may have won while the function was running.
            if e.status != "RUNNING" {return Ok(())}
            let status = outcome.payload["Status"].as_str().unwrap_or("INVALID");
            e.token = token();
            let (state, due) = if now() >= e.deadline {
                e.status = "TIMED_OUT".into(); e.end = Some(now());
                ("done", now())
            } else if !outcome.failed && status == "PENDING" {
                let due = next_due(&e);
                ("suspended", due)
            } else if outcome.failed || status == "RETRY" {
                // A crashed invocation replays from its last durable checkpoint.
                ("queued", (now() + 1000).min(e.deadline))
            } else {
                e.end = Some(now());
                if status == "SUCCEEDED" {
                    e.status = "SUCCEEDED".into();
                    if e.result.is_none() { e.result = outcome.payload["Result"].as_str().map(str::to_owned); }
                } else {
                    e.status = "FAILED".into();
                    e.error = Some(outcome.payload.get("Error").cloned().unwrap_or(json!({"ErrorType":"InvalidDurableResponse","ErrorMessage":"Expected SDK Status response"})));
                }
                ("done", now())
            };
            if state == "done" {
                let kind = match e.status.as_str() {"SUCCEEDED" => "ExecutionSucceeded", "TIMED_OUT" => "ExecutionTimedOut", _ => "ExecutionFailed"};
                let details = if e.status == "SUCCEEDED" {json!({"Result":{"Payload":e.result,"Truncated":false}})} else {json!({"Error":{"Payload":e.error,"Truncated":false}})};
                history_event(&mut e, kind, None, details);
            }
            let due = if state == "done" {now()+e.retention} else {due};
            save(tx, &arn, &e)?;
            diesel::update(table::table.find(&arn)).set((table::state.eq(state), table::due.eq(due))).execute(tx)?;
            Ok(())
        })
    }
}

fn apply_update(e: &mut Execution, arn: &str, update: &Value) -> Result<(), AwsError> {
    if e.status != "RUNNING" {
        return Err(conflict("Execution is already terminal"));
    }
    let kind = update["Type"].as_str().unwrap_or("");
    let action = update["Action"].as_str().unwrap_or("");
    if !matches!(
        kind,
        "STEP" | "WAIT" | "EXECUTION" | "CONTEXT" | "CALLBACK" | "CHAINED_INVOKE"
    ) {
        return Err(AwsError::not_implemented(
            "lambda",
            "durable operation type",
        ));
    }
    if kind == "EXECUTION" {
        match action {
            "SUCCEED" => {
                e.result = update["Payload"].as_str().map(str::to_owned);
                e.status = "SUCCEEDED".into();
            }
            "FAIL" => {
                e.error = update.get("Error").cloned();
                e.status = "FAILED".into();
            }
            _ => return Err(invalid("Unsupported execution action")),
        }
        e.end = Some(now());
        let details = if action == "SUCCEED" {
            json!({"Result":{"Payload":e.result,"Truncated":false}})
        } else {
            json!({"Error":{"Payload":e.error,"Truncated":false}})
        };
        history_event(
            e,
            if action == "SUCCEED" {
                "ExecutionSucceeded"
            } else {
                "ExecutionFailed"
            },
            None,
            details,
        );
        return Ok(());
    }
    let id = update["Id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid("Operation Id is required"))?;
    let position = e.operations.iter().position(|op| op["Id"] == id);
    if position.is_none() && action != "START" {
        return Err(invalid("Operation must be started before completing it"));
    }
    let index = position.unwrap_or(e.operations.len());
    if index == e.operations.len() {
        let mut op = json!({"Id":id,"Type":kind,"Status":"STARTED","StartTimestamp":now()});
        for key in ["ParentId", "Name", "SubType"] {
            if let Some(v) = update.get(key) {
                op[key] = v.clone();
            }
        }
        e.operations.push(op);
    }
    let op = &mut e.operations[index];
    if op["Type"] != kind
        || matches!(
            op["Status"].as_str(),
            Some("SUCCEEDED" | "FAILED" | "CANCELLED")
        )
    {
        return Err(conflict("Invalid operation transition"));
    }
    match (kind, action) {
        ("WAIT", "START") => {
            let seconds = update["WaitOptions"]["WaitSeconds"]
                .as_i64()
                .filter(|n| (0..=31622400).contains(n))
                .ok_or_else(|| invalid("Invalid WaitSeconds"))?;
            // Repeated START must not move an existing timer.
            if op.get("WaitDetails").is_none() {
                op["WaitDetails"] = json!({"ScheduledEndTimestamp":now() + seconds * 1000});
            }
        }
        ("CALLBACK", "START") => {
            if !e.callbacks.contains_key(id) {
                let options = &update["CallbackOptions"];
                let timeout = options["TimeoutSeconds"].as_i64().unwrap_or(86400);
                let heartbeat = options["HeartbeatTimeoutSeconds"].as_i64();
                if !(1..=31622400).contains(&timeout)
                    || heartbeat.is_some_and(|n| !(1..=31622400).contains(&n))
                {
                    return Err(invalid("Invalid callback timeout"));
                }
                let callback = base64::encode(
                    json!({"arn":arn,"id":id,"nonce":ids::request_id()})
                        .to_string()
                        .as_bytes(),
                );
                op["CallbackDetails"] = json!({"CallbackId":callback});
                e.callbacks.insert(
                    id.into(),
                    CallbackTimer {
                        token: callback,
                        deadline: now() + timeout * 1000,
                        heartbeat: heartbeat.map(|n| n * 1000),
                        heartbeat_due: heartbeat.map(|n| now() + n * 1000),
                    },
                );
            }
        }
        ("CONTEXT", "START") => {
            op["ContextDetails"] = json!({"ReplayChildren":false});
        }
        ("CONTEXT", "SUCCEED" | "FAIL") => {
            op["Status"] = json!(if action == "SUCCEED" {
                "SUCCEEDED"
            } else {
                "FAILED"
            });
            op["EndTimestamp"] = json!(now());
            op["ContextDetails"] = json!({"ReplayChildren":update["ContextOptions"]["ReplayChildren"].as_bool().unwrap_or(false)});
            for (key, source) in [("Result", "Payload"), ("Error", "Error")] {
                if let Some(value) = update.get(source) {
                    op["ContextDetails"][key] = value.clone();
                }
            }
        }
        ("CHAINED_INVOKE", "START") => {
            if !e.links.contains_key(id) {
                let function = update["ChainedInvokeOptions"]["FunctionName"]
                    .as_str()
                    .ok_or_else(|| invalid("ChainedInvokeOptions.FunctionName is required"))?;
                e.links.insert(
                    id.into(),
                    InvokeLink {
                        function: function.into(),
                        payload: update["Payload"].as_str().unwrap_or("null").into(),
                        child: None,
                    },
                );
                op["ChainedInvokeDetails"] = json!({});
            }
        }
        ("STEP", "START") => {
            if op.get("StepDetails").is_none() {
                op["StepDetails"] = json!({"Attempt":0});
            }
            op["Status"] = json!("STARTED");
        }
        ("STEP", "SUCCEED" | "FAIL") => {
            op["Status"] = json!(if action == "SUCCEED" {
                "SUCCEEDED"
            } else {
                "FAILED"
            });
            op["EndTimestamp"] = json!(now());
            if let Some(v) = update.get("Payload") {
                op["StepDetails"]["Result"] = v.clone();
            }
            if let Some(v) = update.get("Error") {
                op["StepDetails"]["Error"] = v.clone();
            }
        }
        ("STEP", "RETRY") => {
            let delay = update["StepOptions"]["NextAttemptDelaySeconds"]
                .as_i64()
                .unwrap_or(0);
            if !(0..=31622400).contains(&delay) {
                return Err(invalid("Invalid retry delay"));
            }
            op["Status"] = json!("PENDING");
            op["StepDetails"]["Attempt"] =
                json!(op["StepDetails"]["Attempt"].as_i64().unwrap_or(0) + 1);
            op["StepDetails"]["NextAttemptTimestamp"] = json!(now() + delay * 1000);
            if let Some(payload) = update.get("Payload") {
                op["StepDetails"]["Result"] = payload.clone();
            }
            if let Some(error) = update.get("Error") {
                op["StepDetails"]["Error"] = error.clone();
            }
        }
        (_, "CANCEL") => {
            op["Status"] = json!("CANCELLED");
            op["EndTimestamp"] = json!(now());
        }
        _ => return Err(invalid("Unsupported operation action")),
    }
    let event_kind = format!(
        "{}{}",
        match kind {
            "STEP" => "Step",
            "WAIT" => "Wait",
            "CONTEXT" => "Context",
            "CALLBACK" => "Callback",
            "CHAINED_INVOKE" => "ChainedInvoke",
            _ => "Execution",
        },
        match action {
            "START" => "Started",
            "SUCCEED" => "Succeeded",
            "CANCEL" => "Cancelled",
            _ => "Failed",
        }
    );
    let details = extra::operation_event_details(op, update);
    history_event(e, &event_kind, Some(id), details);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Executors, Service};
    use roto_core::store::Store;

    fn fixture() -> (Lambda, RequestContext, String) {
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
            access_key: None,
        };
        let lambda = Lambda::new(&Store::ephemeral(), Executors::default()).unwrap();
        lambda
            .bind_executor(
                &ctx,
                "durable",
                crate::inline_executor(|_, _| Ok(json!(null))),
            )
            .unwrap();
        lambda
            .create_function(
                &ctx,
                CreateFunctionRequest::from_json(
                    &json!({
                        "FunctionName":"durable","Role":"local","Code":{"ZipFile":""},
                        "DurableConfig":{"ExecutionTimeout":60}
                    }),
                    "",
                )
                .unwrap(),
            )
            .unwrap();
        let response = lambda.invoke(&ctx, InvocationRequest::from_json(&json!({
            "FunctionName":"durable:$LATEST","InvocationType":"Event","Payload":base64::encode(b"{}")
        }), "").unwrap()).unwrap();
        (lambda, ctx, response.durable_execution_arn.unwrap())
    }

    fn checkpoint(
        lambda: &Lambda,
        ctx: &RequestContext,
        arn: &str,
        token: &str,
        client: &str,
        updates: Value,
    ) -> Result<CheckpointDurableExecutionResponse, AwsError> {
        lambda.durable_checkpoint(ctx, CheckpointDurableExecutionRequest::from_json(&json!({
            "DurableExecutionArn":arn,"CheckpointToken":token,"ClientToken":client,"Updates":updates
        }), "")?)
    }

    #[test]
    fn checkpoints_are_atomic_idempotent_and_reject_stale_tokens() {
        let (lambda, ctx, arn) = fixture();
        let original = lambda.db.read(|tx| load(tx, &arn)).unwrap().token;
        let updates = json!([{"Id":"step","Type":"STEP","Action":"START","Name":"first"}]);
        let response =
            checkpoint(&lambda, &ctx, &arn, &original, "request-1", updates.clone()).unwrap();
        assert_ne!(
            response.checkpoint_token.as_deref(),
            Some(original.as_str())
        );
        assert_eq!(
            checkpoint(&lambda, &ctx, &arn, &original, "request-1", updates.clone()).unwrap(),
            response
        );
        assert_eq!(
            checkpoint(&lambda, &ctx, &arn, &original, "request-2", updates)
                .unwrap_err()
                .status,
            409
        );
        assert_eq!(
            checkpoint(&lambda, &ctx, &arn, &original, "request-1", json!([]))
                .unwrap_err()
                .status,
            409
        );
        let next = response.checkpoint_token.unwrap();
        let invalid_batch = json!([
            {"Id":"step","Type":"STEP","Action":"SUCCEED","Payload":"42"},
            {"Id":"unknown","Type":"UNKNOWN","Action":"START"}
        ]);
        assert_eq!(
            checkpoint(&lambda, &ctx, &arn, &next, "bad", invalid_batch)
                .unwrap_err()
                .status,
            501
        );
        let saved = lambda.db.read(|tx| load(tx, &arn)).unwrap();
        assert_eq!(saved.token, next);
        assert_eq!(saved.operations[1]["Status"], "STARTED");
        assert_eq!(saved.operations.len(), 2);
        checkpoint(
            &lambda,
            &ctx,
            &arn,
            &next,
            "request-3",
            json!([
                {"Id":"step","Type":"STEP","Action":"SUCCEED","Payload":"42"}
            ]),
        )
        .unwrap();
        let saved = lambda.db.read(|tx| load(tx, &arn)).unwrap();
        assert_eq!(saved.operations[1]["StepDetails"]["Result"], "42");
    }

    #[test]
    fn replay_state_is_paginated_and_scoped_to_account_and_region() {
        let (lambda, ctx, arn) = fixture();
        let original = lambda.db.read(|tx| load(tx, &arn)).unwrap().token;
        let response = checkpoint(
            &lambda,
            &ctx,
            &arn,
            &original,
            "start",
            json!([
                {"Id":"step","Type":"STEP","Action":"START"},
                {"Id":"wait","Type":"WAIT","Action":"START","WaitOptions":{"WaitSeconds":5}}
            ]),
        )
        .unwrap();
        let token = response.checkpoint_token.unwrap();
        let request = GetDurableExecutionStateRequest {
            durable_execution_arn: arn.clone(),
            checkpoint_token: token,
            max_items: Some(1),
            marker: None,
        };
        let first = lambda.durable_state(&ctx, request.clone()).unwrap();
        assert_eq!(first.operations.len(), 1);
        let second = lambda
            .durable_state(
                &ctx,
                GetDurableExecutionStateRequest {
                    marker: first.next_marker,
                    ..request.clone()
                },
            )
            .unwrap();
        assert_eq!(second.operations[0].id, "step");
        let last = lambda
            .durable_state(
                &ctx,
                GetDurableExecutionStateRequest {
                    marker: second.next_marker,
                    ..request.clone()
                },
            )
            .unwrap();
        assert_eq!(last.operations[0].id, "wait");
        assert!(last.next_marker.is_none());
        let other = RequestContext {
            account_id: "999999999999".into(),
            ..ctx.clone()
        };
        assert_eq!(
            lambda
                .durable_state(&other, request.clone())
                .unwrap_err()
                .status,
            404
        );
        let other = RequestContext {
            region: "eu-west-1".into(),
            ..ctx.clone()
        };
        assert_eq!(
            lambda.durable_state(&other, request).unwrap_err().status,
            404
        );
        lambda
            .durable_stop(
                &ctx,
                StopDurableExecutionRequest {
                    durable_execution_arn: arn.clone(),
                    error: None,
                },
            )
            .unwrap();
        assert_eq!(
            checkpoint(&lambda, &ctx, &arn, &original, "late", json!([]))
                .unwrap_err()
                .status,
            409
        );
    }

    #[test]
    fn terminal_execution_checkpoint_does_not_reinvoke_the_handler() {
        let (lambda, ctx, arn) = fixture();
        let token = lambda.db.read(|tx| load(tx, &arn)).unwrap().token;
        checkpoint(
            &lambda,
            &ctx,
            &arn,
            &token,
            "terminal",
            json!([
                {"Id":"result","Type":"EXECUTION","Action":"SUCCEED","Payload":"{\"done\":true}"}
            ]),
        )
        .unwrap();
        let saved = lambda.db.read(|tx| load(tx, &arn)).unwrap();
        assert_eq!(saved.status, "SUCCEEDED");
        assert_eq!(saved.result.as_deref(), Some("{\"done\":true}"));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        lambda.run_durable(&runtime).unwrap();
        assert!(
            lambda.history(&ctx).unwrap()["invocations"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}
