use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::{Blob, FromJson, ToJson, base64};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::executor::{Executors, Job, Outcome, execute};
use crate::generated::*;

pub struct Lambda {
    pub(crate) db: Arc<Db>,
    executors: RwLock<Executors>,
    pub(crate) sqs: Option<Arc<roto_svc_sqs::Sqs>>,
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
pub(crate) fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "InvalidParameterValueException", message)
}
pub(crate) fn missing(arn: &str) -> AwsError {
    AwsError::sender(
        404,
        "ResourceNotFoundException",
        format!("Function not found: {arn}"),
    )
}
pub(crate) fn conflict(message: impl Into<String>) -> AwsError {
    AwsError::sender(409, "ResourceConflictException", message)
}

pub(crate) fn arn(ctx: &RequestContext, name: &str) -> Result<String, AwsError> {
    if name.starts_with("arn:") {
        let prefix = format!("arn:aws:lambda:{}:{}:function:", ctx.region, ctx.account_id);
        let name = name.strip_prefix(&prefix).ok_or_else(|| missing(name))?;
        return arn(ctx, name);
    }
    let name = name.strip_suffix(":$LATEST").unwrap_or(name);
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid(
            "FunctionName must contain 1–64 letters, digits, hyphens or underscores; only $LATEST is supported",
        ));
    }
    Ok(format!(
        "arn:aws:lambda:{}:{}:function:{name}",
        ctx.region, ctx.account_id
    ))
}
fn qualifier(q: &Option<String>) -> Result<(), AwsError> {
    if q.as_deref().is_some_and(|q| q != "$LATEST") {
        return Err(AwsError::not_implemented("lambda", "versions/aliases"));
    }
    Ok(())
}
fn config(value: &Value) -> Result<FunctionConfiguration, AwsError> {
    FunctionConfiguration::from_json(value, "")
}
fn validate_config(value: &Value) -> Result<(), AwsError> {
    if !value["Timeout"]
        .as_u64()
        .is_some_and(|n| (1..=900).contains(&n))
    {
        return Err(invalid("Timeout must be between 1 and 900 seconds"));
    }
    if !value["MemorySize"]
        .as_u64()
        .is_some_and(|n| (128..=10240).contains(&n))
    {
        return Err(invalid("MemorySize must be between 128 and 10240"));
    }
    if let Some(env) = value["Environment"]["Variables"].as_object() {
        for (k, v) in env {
            if k.is_empty()
                || k.contains(['=', '\0'])
                || v.as_str().is_none_or(|v| v.contains('\0'))
            {
                return Err(invalid("Invalid environment variable"));
            }
        }
    }
    Ok(())
}
fn stamp(value: &mut Value) {
    value["RevisionId"] = json!(ids::request_id());
    value["LastModified"] = json!(roto_protocol::Timestamp(now() / 1000).to_iso8601());
}

impl Lambda {
    pub fn new(store: &Store, executors: Executors) -> Result<Self, AwsError> {
        for executor in executors.functions.values() {
            executor.validate().map_err(invalid)?;
        }
        let db = store.db("lambda", crate::MIGRATIONS)?;
        db.transaction(|tx| {
            tx.execute(
                "UPDATE invocations SET state='queued' WHERE state='running' AND origin='async'",
                [],
            )?;
            Ok(())
        })?;
        db.transaction(|tx| {
            tx.execute("UPDATE invocations SET state='failed', logs='Execution interrupted by server restart' WHERE state='running' AND origin='direct'", [])?;
            Ok(())
        })?;
        Ok(Self {
            db,
            executors: RwLock::new(executors),
            sqs: None,
        })
    }
    pub fn bind_executor(
        &self,
        ctx: &RequestContext,
        name: &str,
        executor: crate::Executor,
    ) -> Result<(), AwsError> {
        executor.validate().map_err(invalid)?;
        let arn = arn(ctx, name)?;
        self.executors
            .write()
            .unwrap()
            .functions
            .insert(arn, executor);
        Ok(())
    }
    pub fn with_sqs(mut self, sqs: Arc<roto_svc_sqs::Sqs>) -> Self {
        self.sqs = Some(sqs);
        self
    }
    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM event_source_mappings", [])?;
            tx.execute("DELETE FROM functions", [])?;
            tx.execute("DELETE FROM invocations", [])?;
            Ok(())
        })
    }
    pub(crate) fn load(&self, arn: &str) -> Result<Value, AwsError> {
        self.db.read(|c| {
            let value: Option<String> = c
                .query_row("SELECT config FROM functions WHERE arn=?1", [arn], |r| {
                    r.get(0)
                })
                .optional()?;
            serde_json::from_str(&value.ok_or_else(|| missing(arn))?)
                .map_err(|e| AwsError::internal(e.to_string()))
        })
    }
    fn save_config(&self, arn: &str, value: &Value) -> Result<(), AwsError> {
        validate_config(value)?;
        self.db.transaction(|tx| {
            if tx.execute(
                "UPDATE functions SET config=?2 WHERE arn=?1",
                params![arn, value.to_string()],
            )? == 0
            {
                return Err(missing(arn));
            }
            Ok(())
        })
    }
    pub(crate) fn job(
        &self,
        ctx: &RequestContext,
        name: &str,
        event: Value,
        client_context: Option<String>,
    ) -> Result<Job, AwsError> {
        let arn = arn(ctx, name)?;
        let value = self.load(&arn)?;
        let bindings = self.executors.read().unwrap();
        let executor = bindings
            .functions
            .get(&arn)
            .or_else(|| {
                bindings
                    .functions
                    .get(value["FunctionName"].as_str().unwrap())
            })
            .cloned()
            .ok_or_else(|| {
                invalid(format!(
                    "No local executor bound to {arn}; configure --lambda-executors"
                ))
            })?;
        let environment = value["Environment"]["Variables"]
            .as_object()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Job {
            executor,
            arn,
            request_id: ids::request_id(),
            region: ctx.region.clone(),
            endpoint: ctx.base_url.clone(),
            environment,
            timeout: value["Timeout"].as_u64().unwrap(),
            event,
            client_context,
        })
    }
    pub(crate) fn record(&self, job: &Job, state: &str) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute(
                "INSERT INTO invocations (id,arn,job,state,created,origin) VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    job.request_id,
                    job.arn,
                    serde_json::to_string(job).unwrap(),
                    state,
                    now(),
                    if state == "queued" { "async" } else { "direct" }
                ],
            )?;
            Ok(())
        })
    }
    pub(crate) fn finish(
        &self,
        job: &Job,
        outcome: &Outcome,
        attempts: i32,
        retry: bool,
    ) -> Result<(), AwsError> {
        let state = if retry && outcome.failed && attempts < 3 {
            "queued"
        } else if outcome.failed {
            "failed"
        } else {
            "succeeded"
        };
        self.db.transaction(|tx| { tx.execute("UPDATE invocations SET state=?2, attempts=?3, due=?4, result=?5, logs=?6 WHERE id=?1", params![job.request_id,state,attempts,now()+i64::from(attempts)*1000,outcome.payload.to_string(),outcome.logs])?; Ok(()) })
    }
    /// Used by service event sources after their transaction commits.
    pub fn enqueue(
        &self,
        ctx: &RequestContext,
        name: &str,
        event: Value,
    ) -> Result<String, AwsError> {
        let job = self.job(ctx, name, event, None)?;
        self.record(&job, "queued")?;
        Ok(job.request_id)
    }
    pub fn history(&self, ctx: &RequestContext) -> Result<Value, AwsError> {
        let prefix = format!(
            "arn:aws:lambda:{}:{}:function:%",
            ctx.region, ctx.account_id
        );
        self.db.read(|c| {
            let mut stmt = c.prepare("SELECT id,arn,state,attempts,result,logs,created FROM invocations WHERE arn LIKE ?1 ORDER BY created DESC, rowid DESC LIMIT 100")?;
            let rows = stmt.query_map([prefix], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,i32>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,i64>(6)?)))?;
            let mut values = Vec::new();
            for row in rows { let (id,arn,state,attempts,result,logs,created) = row?; values.push(json!({"id":id,"functionArn":arn,"state":state,"attempts":attempts,"result":result.and_then(|s| serde_json::from_str::<Value>(&s).ok()),"logs":logs,"created":created})); }
            Ok(json!({"invocations":values}))
        })
    }
    pub(crate) fn start_worker(this: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(this);
        std::thread::Builder::new().name("roto-lambda".into()).spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("Lambda runtime");
            while let Some(lambda) = weak.upgrade() {
                let next = lambda.db.transaction(|tx| {
                    let row: Option<(String,String,i32)> = tx.query_row("SELECT id,job,attempts FROM invocations WHERE state='queued' AND due<=?1 ORDER BY created LIMIT 1", [now()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                    if let Some((id,_,_)) = &row { tx.execute("UPDATE invocations SET state='running' WHERE id=?1", [id])?; }
                    Ok(row)
                });
                match next {
                    Ok(Some((_,source,attempts))) => match serde_json::from_str::<Job>(&source) {
                        Ok(job) => { let outcome = runtime.block_on(execute(&job)); if let Err(e) = lambda.finish(&job, &outcome, attempts+1, true) { eprintln!("Lambda result persistence: {e}"); } }
                        Err(e) => eprintln!("Invalid persisted Lambda job: {e}"),
                    },
                    Ok(None) => { drop(lambda); std::thread::sleep(Duration::from_millis(50)); }
                    Err(e) => { eprintln!("Lambda queue: {e}"); drop(lambda); std::thread::sleep(Duration::from_millis(100)); }
                }
            }
        }).expect("Lambda worker");
    }
}

impl Service for Lambda {
    fn create_event_source_mapping(
        &self,
        ctx: &RequestContext,
        i: CreateEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        self.create_mapping(ctx, i)
    }
    fn get_event_source_mapping(
        &self,
        ctx: &RequestContext,
        i: GetEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        self.get_mapping(ctx, &i.uuid)
    }
    fn delete_event_source_mapping(
        &self,
        ctx: &RequestContext,
        i: DeleteEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        self.delete_mapping(ctx, &i.uuid)
    }
    fn update_event_source_mapping(
        &self,
        ctx: &RequestContext,
        i: UpdateEventSourceMappingRequest,
    ) -> Result<EventSourceMappingConfiguration, AwsError> {
        self.update_mapping(ctx, i)
    }
    fn list_event_source_mappings(
        &self,
        ctx: &RequestContext,
        i: ListEventSourceMappingsRequest,
    ) -> Result<ListEventSourceMappingsResponse, AwsError> {
        self.list_mappings(ctx, i)
    }

    fn create_function(
        &self,
        ctx: &RequestContext,
        i: CreateFunctionRequest,
    ) -> Result<FunctionConfiguration, AwsError> {
        let arn = arn(ctx, &i.function_name)?;
        if i.publish == Some(true) {
            return Err(AwsError::not_implemented("lambda", "PublishVersion"));
        }
        let mut value = i.to_json();
        let code = value
            .as_object_mut()
            .unwrap()
            .remove("Code")
            .unwrap_or(json!({}));
        let tags = value
            .as_object_mut()
            .unwrap()
            .remove("Tags")
            .unwrap_or(json!({}));
        let bytes = code["ZipFile"]
            .as_str()
            .and_then(base64::decode)
            .unwrap_or_default();
        value["FunctionArn"] = json!(arn);
        value["Timeout"] = json!(i.timeout.unwrap_or(3));
        value["MemorySize"] = json!(i.memory_size.unwrap_or(128));
        value["Description"] = json!(i.description.unwrap_or_default());
        value["Version"] = json!("$LATEST");
        value["State"] = json!("Active");
        value["LastUpdateStatus"] = json!("Successful");
        value["CodeSize"] = json!(bytes.len());
        value["CodeSha256"] = json!(base64::encode(&Sha256::digest(&bytes)));
        value["PackageType"] = value.get("PackageType").cloned().unwrap_or(json!("Zip"));
        stamp(&mut value);
        validate_config(&value)?;
        self.db.transaction(|tx| {
            if tx.query_row("SELECT COUNT(*) FROM functions WHERE arn=?1", [&arn], |r| {
                r.get::<_, i64>(0)
            })? > 0
            {
                return Err(conflict(format!("Function already exists: {arn}")));
            }
            tx.execute(
                "INSERT INTO functions (arn,config,code,tags) VALUES (?1,?2,?3,?4)",
                params![arn, value.to_string(), code.to_string(), tags.to_string()],
            )?;
            Ok(())
        })?;
        config(&value)
    }
    fn get_function_configuration(
        &self,
        ctx: &RequestContext,
        i: GetFunctionConfigurationRequest,
    ) -> Result<FunctionConfiguration, AwsError> {
        qualifier(&i.qualifier)?;
        config(&self.load(&arn(ctx, &i.function_name)?)?)
    }
    fn get_function(
        &self,
        ctx: &RequestContext,
        i: GetFunctionRequest,
    ) -> Result<GetFunctionResponse, AwsError> {
        qualifier(&i.qualifier)?;
        let arn = arn(ctx, &i.function_name)?;
        let value = self.load(&arn)?;
        let tags: String = self.db.read(|c| {
            Ok(
                c.query_row("SELECT tags FROM functions WHERE arn=?1", [&arn], |r| {
                    r.get(0)
                })?,
            )
        })?;
        GetFunctionResponse::from_json(
            &json!({"Configuration":value,"Code":{"RepositoryType":"Local"},"Tags":serde_json::from_str::<Value>(&tags).unwrap()}),
            "",
        )
    }
    fn list_functions(
        &self,
        ctx: &RequestContext,
        i: ListFunctionsRequest,
    ) -> Result<ListFunctionsResponse, AwsError> {
        let limit = i.max_items.unwrap_or(50);
        if !(1..=10000).contains(&limit) {
            return Err(invalid("MaxItems out of range"));
        }
        if i.function_version.as_deref().is_some_and(|s| s != "ALL") {
            return Err(invalid("Invalid FunctionVersion"));
        }
        let prefix = format!(
            "arn:aws:lambda:{}:{}:function:%",
            ctx.region, ctx.account_id
        );
        let mut values = self.db.read(|c| {
            let mut stmt = c.prepare(
                "SELECT config FROM functions WHERE arn LIKE ?1 AND arn>?2 ORDER BY arn LIMIT ?3",
            )?;
            Ok(stmt
                .query_map(
                    params![prefix, i.marker.unwrap_or_default(), limit + 1],
                    |r| r.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?)
        })?;
        let more = values.len() > limit as usize;
        values.truncate(limit as usize);
        let values: Vec<Value> = values
            .iter()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let marker = more.then(|| {
            values.last().unwrap()["FunctionArn"]
                .as_str()
                .unwrap()
                .to_owned()
        });
        ListFunctionsResponse::from_json(&json!({"Functions":values,"NextMarker":marker}), "")
    }
    fn update_function_configuration(
        &self,
        ctx: &RequestContext,
        i: UpdateFunctionConfigurationRequest,
    ) -> Result<FunctionConfiguration, AwsError> {
        let arn = arn(ctx, &i.function_name)?;
        let mut value = self.load(&arn)?;
        if i.revision_id
            .as_deref()
            .is_some_and(|r| Some(r) != value["RevisionId"].as_str())
        {
            return Err(AwsError::sender(
                412,
                "PreconditionFailedException",
                "RevisionId mismatch",
            ));
        }
        for (k, v) in i.to_json().as_object().unwrap() {
            if k != "FunctionName" && k != "RevisionId" {
                value[k] = v.clone();
            }
        }
        stamp(&mut value);
        self.save_config(&arn, &value)?;
        config(&value)
    }
    fn update_function_code(
        &self,
        ctx: &RequestContext,
        i: UpdateFunctionCodeRequest,
    ) -> Result<FunctionConfiguration, AwsError> {
        let arn = arn(ctx, &i.function_name)?;
        let mut value = self.load(&arn)?;
        if i.publish == Some(true) {
            return Err(AwsError::not_implemented("lambda", "PublishVersion"));
        }
        if i.revision_id
            .as_deref()
            .is_some_and(|r| Some(r) != value["RevisionId"].as_str())
        {
            return Err(AwsError::sender(
                412,
                "PreconditionFailedException",
                "RevisionId mismatch",
            ));
        }
        if i.dry_run == Some(true) {
            return config(&value);
        }
        let bytes = i
            .zip_file
            .as_ref()
            .map(|b| b.0.as_slice())
            .unwrap_or_default();
        value["CodeSize"] = json!(bytes.len());
        value["CodeSha256"] = json!(base64::encode(&Sha256::digest(bytes)));
        stamp(&mut value);
        self.db.transaction(|tx| {
            tx.execute(
                "UPDATE functions SET config=?2,code=?3 WHERE arn=?1",
                params![arn, value.to_string(), i.to_json().to_string()],
            )?;
            Ok(())
        })?;
        config(&value)
    }
    fn delete_function(
        &self,
        ctx: &RequestContext,
        i: DeleteFunctionRequest,
    ) -> Result<DeleteFunctionResponse, AwsError> {
        qualifier(&i.qualifier)?;
        let arn = arn(ctx, &i.function_name)?;
        self.db.transaction(|tx| {
            if tx.execute("DELETE FROM functions WHERE arn=?1", [arn])? == 0 {
                return Err(missing(&i.function_name));
            }
            Ok(())
        })?;
        Ok(DeleteFunctionResponse {
            status_code: Some(204),
        })
    }
    fn invoke(
        &self,
        ctx: &RequestContext,
        i: InvocationRequest,
    ) -> Result<InvocationResponse, AwsError> {
        qualifier(&i.qualifier)?;
        let kind = i.invocation_type.as_deref().unwrap_or("RequestResponse");
        if !matches!(kind, "RequestResponse" | "Event" | "DryRun") {
            return Err(invalid("Invalid InvocationType"));
        }
        if i.log_type
            .as_deref()
            .is_some_and(|l| !matches!(l, "Tail" | "None"))
        {
            return Err(invalid("Invalid LogType"));
        }
        self.load(&arn(ctx, &i.function_name)?)?;
        if kind == "DryRun" {
            return Ok(InvocationResponse {
                status_code: Some(204),
                ..Default::default()
            });
        }
        let payload = i.payload.unwrap_or(Blob(b"null".to_vec())).0;
        let max = if kind == "Event" {
            1024 * 1024
        } else {
            6 * 1024 * 1024
        };
        if payload.len() > max {
            return Err(AwsError::sender(
                413,
                "RequestTooLargeException",
                "Payload too large",
            ));
        }
        let event = serde_json::from_slice(if payload.is_empty() {
            b"null"
        } else {
            &payload
        })
        .map_err(|e| AwsError::sender(400, "InvalidRequestContentException", e.to_string()))?;
        let job = self.job(
            ctx,
            &i.function_name,
            event,
            if kind == "RequestResponse" {
                i.client_context
            } else {
                None
            },
        )?;
        if kind == "Event" {
            self.record(&job, "queued")?;
            return Ok(InvocationResponse {
                status_code: Some(202),
                ..Default::default()
            });
        }
        self.record(&job, "running")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| AwsError::internal(e.to_string()))?;
        let outcome = runtime.block_on(execute(&job));
        self.finish(&job, &outcome, 1, false)?;
        let bytes = outcome.logs.as_bytes();
        Ok(InvocationResponse {
            status_code: Some(200),
            function_error: outcome.failed.then(|| "Unhandled".into()),
            payload: Some(Blob(serde_json::to_vec(&outcome.payload).unwrap())),
            executed_version: Some("$LATEST".into()),
            log_result: (i.log_type.as_deref() == Some("Tail"))
                .then(|| base64::encode(&bytes[bytes.len().saturating_sub(4096)..])),
            ..Default::default()
        })
    }
    fn add_permission(
        &self,
        ctx: &RequestContext,
        i: AddPermissionRequest,
    ) -> Result<AddPermissionResponse, AwsError> {
        qualifier(&i.qualifier)?;
        let arn = arn(ctx, &i.function_name)?;
        self.load(&arn)?;
        let mut statement = json!({"Sid":i.statement_id,"Effect":"Allow","Action":i.action,"Principal":{"Service":i.principal},"Resource":arn});
        if let Some(source) = i.source_arn {
            statement["Condition"]["ArnLike"]["AWS:SourceArn"] = json!(source);
        }
        if let Some(account) = i.source_account {
            statement["Condition"]["StringEquals"]["AWS:SourceAccount"] = json!(account);
        }
        self.db.transaction(|tx| {
            let source: String =
                tx.query_row("SELECT policy FROM functions WHERE arn=?1", [&arn], |r| {
                    r.get(0)
                })?;
            let mut statements: Vec<Value> = serde_json::from_str(&source).unwrap();
            if statements.iter().any(|s| s["Sid"] == statement["Sid"]) {
                return Err(conflict("StatementId already exists"));
            }
            statements.push(statement.clone());
            tx.execute(
                "UPDATE functions SET policy=?2 WHERE arn=?1",
                params![arn, serde_json::to_string(&statements).unwrap()],
            )?;
            Ok(())
        })?;
        Ok(AddPermissionResponse {
            statement: Some(statement.to_string()),
        })
    }
    fn get_policy(
        &self,
        ctx: &RequestContext,
        i: GetPolicyRequest,
    ) -> Result<GetPolicyResponse, AwsError> {
        qualifier(&i.qualifier)?;
        let arn = arn(ctx, &i.function_name)?;
        let value = self.load(&arn)?;
        let source: String = self.db.read(|c| {
            Ok(
                c.query_row("SELECT policy FROM functions WHERE arn=?1", [arn], |r| {
                    r.get(0)
                })?,
            )
        })?;
        let statements: Value = serde_json::from_str(&source).unwrap();
        if statements.as_array().unwrap().is_empty() {
            return Err(missing("function policy"));
        }
        Ok(GetPolicyResponse {
            policy: Some(
                json!({"Version":"2012-10-17","Id":"default","Statement":statements}).to_string(),
            ),
            revision_id: value["RevisionId"].as_str().map(str::to_owned),
        })
    }
    fn remove_permission(
        &self,
        ctx: &RequestContext,
        i: RemovePermissionRequest,
    ) -> Result<(), AwsError> {
        qualifier(&i.qualifier)?;
        let arn = arn(ctx, &i.function_name)?;
        self.load(&arn)?;
        self.db.transaction(|tx| {
            let source: String =
                tx.query_row("SELECT policy FROM functions WHERE arn=?1", [&arn], |r| {
                    r.get(0)
                })?;
            let mut statements: Vec<Value> = serde_json::from_str(&source).unwrap();
            let old = statements.len();
            statements.retain(|s| s["Sid"].as_str() != Some(&i.statement_id));
            if old == statements.len() {
                return Err(missing("policy statement"));
            }
            tx.execute(
                "UPDATE functions SET policy=?2 WHERE arn=?1",
                params![arn, serde_json::to_string(&statements).unwrap()],
            )?;
            Ok(())
        })
    }
    fn list_tags(
        &self,
        ctx: &RequestContext,
        i: ListTagsRequest,
    ) -> Result<ListTagsResponse, AwsError> {
        let arn = arn(ctx, &i.resource)?;
        self.load(&arn)?;
        let tags: String = self.db.read(|c| {
            Ok(
                c.query_row("SELECT tags FROM functions WHERE arn=?1", [arn], |r| {
                    r.get(0)
                })?,
            )
        })?;
        ListTagsResponse::from_json(
            &json!({"Tags":serde_json::from_str::<Value>(&tags).unwrap()}),
            "",
        )
    }
    fn tag_resource(&self, ctx: &RequestContext, i: TagResourceRequest) -> Result<(), AwsError> {
        self.tags(ctx, &i.resource, i.tags, Vec::new())
    }
    fn untag_resource(
        &self,
        ctx: &RequestContext,
        i: UntagResourceRequest,
    ) -> Result<(), AwsError> {
        self.tags(ctx, &i.resource, BTreeMap::new(), i.tag_keys)
    }
}
impl Lambda {
    fn tags(
        &self,
        ctx: &RequestContext,
        resource: &str,
        add: BTreeMap<String, String>,
        remove: Vec<String>,
    ) -> Result<(), AwsError> {
        let arn = arn(ctx, resource)?;
        self.load(&arn)?;
        self.db.transaction(|tx| {
            let source: String =
                tx.query_row("SELECT tags FROM functions WHERE arn=?1", [&arn], |r| {
                    r.get(0)
                })?;
            let mut tags: BTreeMap<String, String> = serde_json::from_str(&source).unwrap();
            tags.extend(add);
            for k in remove {
                tags.remove(&k);
            }
            tx.execute(
                "UPDATE functions SET tags=?2 WHERE arn=?1",
                params![arn, serde_json::to_string(&tags).unwrap()],
            )?;
            Ok(())
        })
    }
}
