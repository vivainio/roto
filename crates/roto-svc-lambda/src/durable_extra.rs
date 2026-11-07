use super::*;
use crate::schema::invocations;

#[derive(Serialize, Deserialize)]
pub(super) struct CallbackTimer {
    pub token: String,
    pub deadline: i64,
    pub heartbeat: Option<i64>,
    pub heartbeat_due: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(super) struct InvokeLink {
    pub function: String,
    pub payload: String,
    pub child: Option<String>,
}

pub(super) fn history_event(e: &mut Execution, kind: &str, id: Option<&str>, details: Value) {
    let mut event = json!({"EventId":e.history.len()+1,"EventTimestamp":now(),"EventType":kind});
    if let Some(id) = id {
        event["Id"] = json!(id);
        if let Some(op) = e.operations.iter().find(|op| op["Id"] == id) {
            for key in ["Name", "ParentId", "SubType"] {
                if let Some(v) = op.get(key) {
                    event[key] = v.clone();
                }
            }
        }
    }
    event[format!("{kind}Details")] = details;
    e.history.push(event);
}

pub(super) fn operation_event_details(op: &Value, update: &Value) -> Value {
    if update["Type"] == "CALLBACK" {
        return json!({"CallbackId":op["CallbackDetails"]["CallbackId"],"Timeout":update["CallbackOptions"]["TimeoutSeconds"],"HeartbeatTimeout":update["CallbackOptions"]["HeartbeatTimeoutSeconds"]});
    }
    if update["Type"] == "CHAINED_INVOKE" {
        return json!({"FunctionName":update["ChainedInvokeOptions"]["FunctionName"],"Input":{"Payload":update["Payload"],"Truncated":false}});
    }
    if update["Type"] == "WAIT" && update["Action"] == "START" {
        return json!({"Duration":update["WaitOptions"]["WaitSeconds"],"ScheduledEndTimestamp":op["WaitDetails"]["ScheduledEndTimestamp"]});
    }
    match update["Action"].as_str() {
        Some("SUCCEED") => {
            json!({"Result":{"Payload":update["Payload"],"Truncated":false},"RetryDetails":{}})
        }
        Some("FAIL" | "RETRY") => {
            json!({"Error":{"Payload":update["Error"],"Truncated":false},"RetryDetails":{"NextAttemptDelaySeconds":update["StepOptions"]["NextAttemptDelaySeconds"]}})
        }
        _ => json!({}),
    }
}

pub(super) fn replay_operations(e: &Execution) -> Vec<Value> {
    // Terminal contexts carry their own result. Their children are only replayed
    // when the SDK explicitly requested ReplayChildren (large result rebuilding).
    let hidden: std::collections::BTreeSet<&str> = e
        .operations
        .iter()
        .filter(|op| {
            op["Type"] == "CONTEXT"
                && matches!(op["Status"].as_str(), Some("SUCCEEDED" | "FAILED"))
                && op["ContextDetails"]["ReplayChildren"] != true
        })
        .filter_map(|op| op["Id"].as_str())
        .collect();
    e.operations
        .iter()
        .filter(|op| {
            let mut parent = op["ParentId"].as_str();
            let mut remaining = e.operations.len();
            while let Some(id) = parent {
                if hidden.contains(id) {
                    return false;
                }
                if remaining == 0 {
                    return false;
                }
                remaining -= 1;
                parent = e
                    .operations
                    .iter()
                    .find(|op| op["Id"] == id)
                    .and_then(|op| op["ParentId"].as_str());
            }
            true
        })
        .cloned()
        .collect()
}

pub(super) fn expire_callbacks(e: &mut Execution) {
    let mut expired = Vec::new();
    for (id, timer) in &e.callbacks {
        let heartbeat = timer.heartbeat_due.is_some_and(|due| due <= now());
        if (timer.deadline <= now() || heartbeat)
            && let Some(op) = e
                .operations
                .iter_mut()
                .find(|op| op["Id"] == id.as_str() && op["Status"] == "STARTED")
        {
            op["Status"] = json!("TIMED_OUT");
            op["EndTimestamp"] = json!(now());
            op["CallbackDetails"]["Error"] = json!({"ErrorType":if heartbeat {"Callback.Heartbeat"} else {"Callback.Timeout"},"ErrorMessage":"Callback timed out"});
            expired.push((id.clone(), heartbeat));
        }
    }
    for (id, heartbeat) in expired {
        e.wake = true;
        history_event(
            e,
            "CallbackTimedOut",
            Some(&id),
            json!({"TimeoutType":if heartbeat {"HEARTBEAT"} else {"TIMEOUT"},"Error":{}}),
        );
    }
}

pub(super) fn next_due(e: &Execution) -> i64 {
    if e.wake {
        return now();
    }
    let mut due = e.deadline;
    for op in &e.operations {
        let when = match (op["Type"].as_str(), op["Status"].as_str()) {
            (Some("WAIT"), Some("STARTED")) => op["WaitDetails"]["ScheduledEndTimestamp"].as_i64(),
            (Some("STEP"), Some("PENDING")) => op["StepDetails"]["NextAttemptTimestamp"].as_i64(),
            (Some("CALLBACK"), Some("STARTED")) => e
                .callbacks
                .get(op["Id"].as_str().unwrap_or(""))
                .map(|c| c.deadline.min(c.heartbeat_due.unwrap_or(i64::MAX))),
            (Some("CHAINED_INVOKE"), Some("STARTED")) => None,
            _ => None,
        };
        if let Some(when) = when {
            due = due.min(when);
        }
    }
    due
}

fn page(marker: Option<&str>, max: Option<i32>, length: usize) -> Result<(usize, usize), AwsError> {
    let start = marker
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| invalid("Invalid Marker"))?;
    let max = max.unwrap_or(100);
    if !(0..=1000).contains(&max) || start > length {
        return Err(invalid("Invalid pagination parameters"));
    }
    Ok((
        start,
        (start + if max == 0 { 100 } else { max as usize }).min(length),
    ))
}

impl Lambda {
    pub(crate) fn durable_callback(
        &self,
        ctx: &RequestContext,
        callback: &str,
        action: &str,
        payload: Option<Value>,
    ) -> Result<(), AwsError> {
        let decoded = base64::decode(callback)
            .and_then(|data| serde_json::from_slice::<Value>(&data).ok())
            .ok_or_else(|| invalid("Invalid CallbackId"))?;
        let arn = decoded["arn"]
            .as_str()
            .ok_or_else(|| invalid("Invalid CallbackId"))?;
        let id = decoded["id"]
            .as_str()
            .ok_or_else(|| invalid("Invalid CallbackId"))?;
        scope(ctx, arn)?;
        self.db.transaction(|tx| {
            let mut e = load(tx, arn)?;
            expire_callbacks(&mut e);
            let timer = e
                .callbacks
                .get_mut(id)
                .filter(|timer| timer.token == callback)
                .ok_or_else(|| missing("callback"))?;
            let op = e
                .operations
                .iter_mut()
                .find(|op| op["Id"] == id)
                .ok_or_else(|| missing("callback"))?;
            if e.status != "RUNNING" || now() >= e.deadline || op["Status"] != "STARTED" {
                return Err(conflict("Callback or execution is already terminal"));
            }
            if action == "heartbeat" {
                timer.heartbeat_due = timer.heartbeat.map(|interval| now() + interval);
            } else {
                op["Status"] = json!(if action == "success" {
                    "SUCCEEDED"
                } else {
                    "FAILED"
                });
                op["EndTimestamp"] = json!(now());
                let key = if action == "success" {
                    "Result"
                } else {
                    "Error"
                };
                if let Some(payload) = payload {
                    op["CallbackDetails"][key] = payload;
                }
                e.wake = true;
                let details = if action == "success" {
                    json!({"Result":{"Payload":op["CallbackDetails"]["Result"],"Truncated":false}})
                } else {
                    json!({"Error":{"Payload":op["CallbackDetails"]["Error"],"Truncated":false}})
                };
                history_event(
                    &mut e,
                    if action == "success" {
                        "CallbackSucceeded"
                    } else {
                        "CallbackFailed"
                    },
                    Some(id),
                    details,
                );
            }
            save(tx, arn, &e)?;
            // A callback can race the invocation returning PENDING. Only wake an
            // already suspended execution; an active one observes e.wake on exit.
            diesel::update(table::table.find(arn).filter(table::state.eq("suspended")))
                .set(table::due.eq(next_due(&e)))
                .execute(tx)?;
            Ok(())
        })
    }

    pub(crate) fn durable_history(
        &self,
        ctx: &RequestContext,
        i: GetDurableExecutionHistoryRequest,
    ) -> Result<GetDurableExecutionHistoryResponse, AwsError> {
        scope(ctx, &i.durable_execution_arn)?;
        self.db.read(|tx| {
            let e = load(tx, &i.durable_execution_arn)?;
            let mut history = api_operations(&e.history);
            if i.reverse_order == Some(true) {history.reverse();}
            if i.include_execution_data != Some(true) {
                fn redact(v: &mut Value) {
                    if let Some(o) = v.as_object_mut() {o.remove("Payload"); for child in o.values_mut() {redact(child);}}
                }
                for event in &mut history {redact(event);}
            }
            let (start, end) = page(i.marker.as_deref(), i.max_items, history.len())?;
            GetDurableExecutionHistoryResponse::from_json(&json!({"Events":&history[start..end],"NextMarker":(end<history.len()).then(|| end.to_string())}), "")
        })
    }

    pub(crate) fn durable_list(
        &self,
        ctx: &RequestContext,
        i: ListDurableExecutionsByFunctionRequest,
    ) -> Result<ListDurableExecutionsByFunctionResponse, AwsError> {
        let function = self.resolve_arn(ctx, &i.function_name, i.qualifier.as_deref())?;
        self.db.read(|tx| {
            let rows = table::table.select((table::arn, table::data)).load::<(String,String)>(tx)?;
            let mut values = Vec::new();
            for (arn, data) in rows {
                let e = decode(&data)?;
                if e.job.arn != function && !(i.qualifier.is_none() && function.split(':').count()==7 && e.job.arn.starts_with(&format!("{function}:"))) {continue;}
                if i.durable_execution_name.as_deref().is_some_and(|name| name != e.name)
                    || (!i.statuses.is_empty() && !i.statuses.contains(&e.status))
                    || i.started_after.is_some_and(|t| e.start <= t.0*1000)
                    || i.started_before.is_some_and(|t| e.start >= t.0*1000) {continue;}
                values.push(json!({"DurableExecutionArn":arn,"DurableExecutionName":e.name,"FunctionArn":crate::versions::qualified(&e.job.arn),"StartTimestamp":e.start/1000,"EndTimestamp":e.end.map(|t|t/1000),"Status":e.status}));
            }
            values.sort_by(|a,b| (a["StartTimestamp"].as_i64(),a["DurableExecutionArn"].as_str()).cmp(&(b["StartTimestamp"].as_i64(),b["DurableExecutionArn"].as_str())));
            if i.reverse_order != Some(false) {values.reverse();}
            let (start,end) = page(i.marker.as_deref(),i.max_items,values.len())?;
            ListDurableExecutionsByFunctionResponse::from_json(&json!({"DurableExecutions":&values[start..end],"NextMarker":(end<values.len()).then(||end.to_string())}), "")
        })
    }

    pub(super) fn maintain_durable(&self) -> Result<(), AwsError> {
        // Retention applies after reaching a terminal state. The queue's due
        // column holds the expiry time for terminal executions.
        self.db.transaction(|tx| {
            diesel::delete(
                table::table
                    .filter(table::state.eq("done"))
                    .filter(table::due.le(now())),
            )
            .execute(tx)?;
            Ok(())
        })?;
        let pending = self.db.read(|tx| {
            let rows = table::table
                .filter(table::state.eq_any(["queued", "suspended"]))
                .select((table::arn, table::data))
                .load::<(String, String)>(tx)?;
            let mut links = Vec::new();
            for (arn, data) in rows {
                let e = decode(&data)?;
                if e.status != "RUNNING" {
                    continue;
                }
                for (id, link) in &e.links {
                    if e.operations
                        .iter()
                        .any(|op| op["Id"] == id.as_str() && op["Status"] == "STARTED")
                    {
                        links.push((arn.clone(), id.clone(), link.clone(), e.job.clone()));
                    }
                }
            }
            Ok(links)
        })?;
        for (arn, id, link, parent) in pending {
            self.advance_link(&arn, &id, link, &parent)?;
        }
        Ok(())
    }

    fn advance_link(
        &self,
        arn: &str,
        id: &str,
        link: InvokeLink,
        parent: &Job,
    ) -> Result<(), AwsError> {
        let parts: Vec<&str> = parent.arn.split(':').collect();
        let ctx = RequestContext {
            region: parent.region.clone(),
            account_id: parts[4].into(),
            base_url: parent.endpoint.clone(),
            request_id: ids::request_id(),
            access_key: None,
        };
        let state = if let Some(child) = &link.child {
            if child.contains("/durable-execution/") {
                let e = self.db.read(|tx| load(tx, child))?;
                if e.status == "RUNNING" {
                    None
                } else {
                    Some((
                        e.status == "SUCCEEDED",
                        e.result.map(Value::String),
                        e.error,
                    ))
                }
            } else {
                self.db.read(|tx| {
                    let row = invocations::table
                        .find(child)
                        .select((invocations::state, invocations::result))
                        .first::<(String, Option<String>)>(tx)
                        .optional()?;
                    Ok(row.and_then(|(state, result)| {
                        if !matches!(state.as_str(), "succeeded" | "failed") {
                            return None;
                        }
                        let result = result.unwrap_or_else(|| "null".into());
                        Some(if state == "succeeded" {
                            (true, Some(Value::String(result)), None)
                        } else {
                            (
                                false,
                                None,
                                Some(json!({"ErrorType":"FunctionError","ErrorMessage":result})),
                            )
                        })
                    }))
                })?
            }
        } else {
            let start: Result<String, AwsError> = (|| {
                let name = format!("chain-{}", crate::service::chain_id(arn, id));
                let job = self.job(
                    &ctx,
                    &link.function,
                    serde_json::from_str(&link.payload)
                        .map_err(|_| invalid("Invalid chained payload"))?,
                    None,
                )?;
                let config = self.load(&job.arn)?;
                if config.get("DurableConfig").is_some() {
                    let response = self.invoke(
                        &ctx,
                        InvocationRequest {
                            function_name: link.function.clone(),
                            invocation_type: Some("Event".into()),
                            durable_execution_name: Some(name),
                            payload: Some(Blob(link.payload.as_bytes().to_vec())),
                            ..Default::default()
                        },
                    )?;
                    Ok(response.durable_execution_arn.unwrap())
                } else {
                    let mut job = job;
                    job.request_id = name;
                    let exists = self.db.read(|tx| {
                        Ok(invocations::table
                            .find(&job.request_id)
                            .count()
                            .get_result::<i64>(tx)?
                            > 0)
                    })?;
                    if !exists {
                        self.record(&job, "queued")?;
                    }
                    Ok(job.request_id)
                }
            })();
            match start {
                Ok(child) => {
                    self.db.transaction(|tx| {
                        let mut e = load(tx, arn)?;
                        if let Some(link) = e.links.get_mut(id) {
                            link.child = Some(child);
                        }
                        save(tx, arn, &e)
                    })?;
                    None
                }
                Err(error) => Some((
                    false,
                    None,
                    Some(json!({"ErrorType":error.code,"ErrorMessage":error.message})),
                )),
            }
        };
        if let Some((success, result, error)) = state {
            self.db.transaction(|tx| {
                let mut e=load(tx,arn)?;
                if e.status!="RUNNING" {return Ok(());}
                if let Some(op)=e.operations.iter_mut().find(|op|op["Id"]==id && op["Status"]=="STARTED") {
                    op["Status"]=json!(if success {"SUCCEEDED"} else {"FAILED"});
                    op["EndTimestamp"]=json!(now());
                    op["ChainedInvokeDetails"]=json!({"Result":result,"Error":error});
                    let details=if success {json!({"Result":{"Payload":op["ChainedInvokeDetails"]["Result"],"Truncated":false}})} else {json!({"Error":{"Payload":op["ChainedInvokeDetails"]["Error"],"Truncated":false}})};
                    history_event(&mut e,if success {"ChainedInvokeSucceeded"} else {"ChainedInvokeFailed"},Some(id),details);
                    e.wake=true;
                    save(tx,arn,&e)?;
                    diesel::update(table::table.find(arn).filter(table::state.eq("suspended"))).set(table::due.eq(now())).execute(tx)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }
}
