//! Synchronous CloudFormation stacks for SQS, SNS, S3 and DynamoDB resources.
//! State is scoped by account/region and persisted after each resource mutation.

#[allow(clippy::all)]
mod generated;
mod resources;
mod template;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler, ids};
use roto_protocol::{QueryParams, Timestamp, query_error};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use generated::*;
pub use generated::{NAMESPACE, OPERATIONS, Service, dispatch};
use resources::{Resource, Resources, replacement, tag_list, tag_map};
use template::{Resolver, partition, text};

pub const IMPLEMENTED: &[&str] = &[
    "CreateStack",
    "UpdateStack",
    "DeleteStack",
    "DescribeStacks",
    "ListStackResources",
    "DescribeStackResources",
];

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "CREATE TABLE stacks (
        account_id TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL,
        stack_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL,
        PRIMARY KEY (account_id, region, name)
    );",
}];

pub(crate) fn validation(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationError", message)
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[derive(Clone, Serialize, Deserialize)]
struct StackState {
    name: String,
    id: String,
    status: String,
    reason: Option<String>,
    created: i64,
    updated: Option<i64>,
    template: Value,
    parameters: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
    resources: Vec<Resource>,
    outputs: Vec<StoredOutput>,
}
#[derive(Clone, Serialize, Deserialize)]
struct StoredOutput {
    key: String,
    value: String,
    description: Option<String>,
    export: Option<String>,
}

impl StackState {
    fn resolver<'a>(&'a self, ctx: &'a RequestContext) -> Resolver<'a> {
        Resolver {
            ctx,
            stack_name: &self.name,
            stack_id: &self.id,
            parameters: &self.parameters,
            resources: &self.resources,
        }
    }
    fn response(&self) -> Stack {
        Stack {
            stack_name: self.name.clone(),
            stack_id: Some(self.id.clone()),
            stack_status: self.status.clone(),
            stack_status_reason: self.reason.clone(),
            creation_time: Timestamp(self.created),
            last_updated_time: self.updated.map(Timestamp),
            description: self.template["Description"].as_str().map(str::to_string),
            parameters: self
                .parameters
                .iter()
                .map(|(k, v)| Parameter {
                    parameter_key: Some(k.clone()),
                    parameter_value: Some(v.clone()),
                    ..Default::default()
                })
                .collect(),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| Tag {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            outputs: self
                .outputs
                .iter()
                .map(|o| Output {
                    output_key: Some(o.key.clone()),
                    output_value: Some(o.value.clone()),
                    description: o.description.clone(),
                    export_name: o.export.clone(),
                })
                .collect(),
            ..Default::default()
        }
    }
    fn resource(&self, r: &Resource) -> StackResource {
        StackResource {
            logical_resource_id: r.logical_id.clone(),
            physical_resource_id: Some(r.physical_id.clone()),
            resource_type: r.resource_type.clone(),
            resource_status: r.status.clone(),
            resource_status_reason: r.reason.clone(),
            stack_id: Some(self.id.clone()),
            stack_name: Some(self.name.clone()),
            timestamp: Timestamp(self.updated.unwrap_or(self.created)),
            ..Default::default()
        }
    }
}

pub struct CloudFormation {
    db: Arc<Db>,
    resources: Resources,
    mutation: Mutex<()>,
}

impl CloudFormation {
    pub fn new(
        store: &Store,
        handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    ) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("cloudformation", MIGRATIONS)?,
            resources: Resources::new(handlers),
            mutation: Mutex::new(()),
        })
    }
    fn save(&self, ctx: &RequestContext, stack: &StackState) -> Result<(), AwsError> {
        let body = serde_json::to_string(stack).map_err(|e| AwsError::internal(e.to_string()))?;
        self.db.transaction(|tx| {
            tx.execute("INSERT INTO stacks(account_id,region,name,stack_id,body) VALUES(?1,?2,?3,?4,?5)
                ON CONFLICT(account_id,region,name) DO UPDATE SET stack_id=excluded.stack_id,body=excluded.body",
                params![ctx.account_id,ctx.region,stack.name,stack.id,body])?;
            Ok(())
        })
    }
    fn find(&self, ctx: &RequestContext, name: &str) -> Result<Option<StackState>, AwsError> {
        let body: Option<String> = self.db.read(|c| Ok(c.query_row(
            "SELECT body FROM stacks WHERE account_id=?1 AND region=?2 AND (name=?3 OR stack_id=?3)",
            params![ctx.account_id,ctx.region,name], |r|r.get(0)).optional()?))?;
        body.map(|body| serde_json::from_str(&body).map_err(|e| AwsError::internal(e.to_string())))
            .transpose()
    }
    fn load(&self, ctx: &RequestContext, name: &str) -> Result<StackState, AwsError> {
        self.find(ctx, name)?
            .filter(|s| s.status != "DELETE_COMPLETE" || s.id == name)
            .ok_or_else(|| validation(format!("Stack with id {name} does not exist")))
    }
    fn all(&self, ctx: &RequestContext) -> Result<Vec<StackState>, AwsError> {
        self.db.read(|c| {
            let mut statement = c.prepare(
                "SELECT body FROM stacks WHERE account_id=?1 AND region=?2 ORDER BY name",
            )?;
            let rows = statement.query_map(params![ctx.account_id, ctx.region], |r| {
                r.get::<_, String>(0)
            })?;
            rows.map(|row| {
                serde_json::from_str(&row?).map_err(|e| AwsError::internal(e.to_string()))
            })
            .collect()
        })
    }
    fn reconcile(&self, ctx: &RequestContext, stack: &mut StackState) -> Result<(), AwsError> {
        let ordered = template::order(&stack.template)?;
        for id in &ordered {
            let definition = &stack.template["Resources"][id];
            let ty = definition["Type"].as_str().unwrap().to_string();
            let mut props = stack
                .resolver(ctx)
                .resolve(definition.get("Properties").unwrap_or(&json!({})))?;
            let mut tags = stack.tags.clone();
            tags.extend(tag_map(&props)?);
            if !tags.is_empty() {
                props["Tags"] = tag_list(tags);
            }
            let existing = stack.resources.iter().position(|r| r.logical_id == *id);
            if let Some(index) =
                existing.filter(|index| !replacement(&stack.resources[*index], &ty, &props))
            {
                if stack.resources[index].properties != props {
                    self.configure(ctx, stack, index, &props, true)?;
                }
            } else {
                let resource = self
                    .resources
                    .create(ctx, &stack.name, id, &ty, props.clone())?;
                stack.resources.push(resource);
                self.save(ctx, stack)?;
                let index = stack.resources.len() - 1;
                self.configure(ctx, stack, index, &props, false)?;
                if let Some(index) = existing {
                    self.remove(ctx, stack, index)?;
                }
            }
        }
        for index in (0..stack.resources.len()).rev() {
            if !ordered.contains(&stack.resources[index].logical_id) {
                self.remove(ctx, stack, index)?;
            }
        }
        stack.resources.sort_by_key(|r| {
            ordered
                .iter()
                .position(|id| id == &r.logical_id)
                .unwrap_or(usize::MAX)
        });
        let mut outputs = Vec::new();
        if let Some(value) = stack.template.get("Outputs") {
            for (key, definition) in value
                .as_object()
                .ok_or_else(|| validation("Outputs must be an object"))?
            {
                let value = text(
                    &stack.resolver(ctx).resolve(
                        definition
                            .get("Value")
                            .ok_or_else(|| validation("Output requires Value"))?,
                    )?,
                )?;
                let export = definition
                    .get("Export")
                    .map(|v| {
                        text(
                            &stack.resolver(ctx).resolve(
                                v.get("Name")
                                    .ok_or_else(|| validation("Export requires Name"))?,
                            )?,
                        )
                    })
                    .transpose()?;
                outputs.push(StoredOutput {
                    key: key.clone(),
                    value,
                    export,
                    description: definition["Description"].as_str().map(str::to_string),
                });
            }
        }
        stack.outputs = outputs;
        Ok(())
    }
    fn configure(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        index: usize,
        props: &Value,
        updating: bool,
    ) -> Result<(), AwsError> {
        let action = if updating { "UPDATE" } else { "CREATE" };
        stack.resources[index].status = format!("{action}_IN_PROGRESS");
        stack.resources[index].reason = None;
        self.save(ctx, stack)?;
        let result = self
            .resources
            .configure(ctx, &mut stack.resources[index], props, updating);
        stack.resources[index].status = format!(
            "{action}_{}",
            if result.is_ok() { "COMPLETE" } else { "FAILED" }
        );
        stack.resources[index].reason = result.as_ref().err().map(ToString::to_string);
        self.save(ctx, stack)?;
        result
    }
    fn remove(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        index: usize,
    ) -> Result<(), AwsError> {
        stack.resources[index].status = "DELETE_IN_PROGRESS".into();
        self.save(ctx, stack)?;
        match self.resources.delete(ctx, &stack.resources[index]) {
            Ok(()) => {
                stack.resources.remove(index);
                self.save(ctx, stack)
            }
            Err(e) => {
                stack.resources[index].status = "DELETE_FAILED".into();
                stack.resources[index].reason = Some(e.to_string());
                self.save(ctx, stack)?;
                Err(e)
            }
        }
    }
    fn complete(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        action: &str,
        result: Result<(), AwsError>,
    ) -> Result<(), AwsError> {
        match result {
            Ok(()) => {
                stack.status = format!("{action}_COMPLETE");
                stack.reason = None;
                self.save(ctx, stack)
            }
            Err(error) => {
                stack.status = format!("{action}_FAILED");
                stack.reason = Some(error.to_string());
                self.save(ctx, stack)?;
                Err(error)
            }
        }
    }
}

fn parameters(
    template: &Value,
    inputs: Vec<Parameter>,
    previous: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, AwsError> {
    let mut result = BTreeMap::new();
    if let Some(definitions) = template.get("Parameters") {
        for (key, definition) in definitions
            .as_object()
            .ok_or_else(|| validation("Parameters must be an object"))?
        {
            if let Some(v) = previous.and_then(|p| p.get(key)) {
                result.insert(key.clone(), v.clone());
            } else if let Some(value) = definition.get("Default") {
                result.insert(key.clone(), text(value)?);
            }
        }
    }
    for input in inputs {
        let key = input
            .parameter_key
            .ok_or_else(|| validation("Parameter requires ParameterKey"))?;
        if template["Parameters"].get(&key).is_none() {
            return Err(validation(format!("Unknown parameter: {key}")));
        }
        if input.use_previous_value == Some(true) {
            let v = previous
                .and_then(|p| p.get(&key))
                .ok_or_else(|| validation(format!("No previous value for {key}")))?;
            result.insert(key, v.clone());
        } else {
            result.insert(
                key,
                input
                    .parameter_value
                    .ok_or_else(|| validation("Parameter requires ParameterValue"))?,
            );
        }
    }
    if let Some(definitions) = template.get("Parameters").and_then(Value::as_object) {
        for key in definitions.keys() {
            if !result.contains_key(key) {
                return Err(validation(format!("Parameter {key} requires a value")));
            }
        }
    }
    Ok(result)
}

impl Service for CloudFormation {
    fn create_stack(
        &self,
        ctx: &RequestContext,
        input: CreateStackInput,
    ) -> Result<CreateStackOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if input.template_url.is_some() {
            return Err(validation("TemplateURL is not supported; use TemplateBody"));
        }
        if input.enable_termination_protection == Some(true)
            || input.stack_policy_body.is_some()
            || input.stack_policy_url.is_some()
            || input.rollback_configuration.is_some()
            || input.on_failure.is_some()
        {
            return Err(validation(
                "Stack protection, policies and rollback options are not supported",
            ));
        }
        if input.stack_name.is_empty()
            || !input
                .stack_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err(validation("Invalid StackName"));
        }
        if self
            .find(ctx, &input.stack_name)?
            .is_some_and(|s| s.status != "DELETE_COMPLETE")
        {
            return Err(AwsError::sender(
                400,
                "AlreadyExistsException",
                format!("Stack {} already exists", input.stack_name),
            ));
        }
        let template = template::parse(
            input
                .template_body
                .as_deref()
                .ok_or_else(|| validation("TemplateBody is required"))?,
        )?;
        let parameters = parameters(&template, input.parameters, None)?;
        let mut stack = StackState {
            id: format!(
                "arn:{}:cloudformation:{}:{}:stack/{}/{}",
                partition(&ctx.region),
                ctx.region,
                ctx.account_id,
                input.stack_name,
                ids::request_id()
            ),
            name: input.stack_name,
            status: "CREATE_IN_PROGRESS".into(),
            reason: None,
            created: now(),
            updated: None,
            template,
            parameters,
            tags: input.tags.into_iter().map(|t| (t.key, t.value)).collect(),
            resources: Vec::new(),
            outputs: Vec::new(),
        };
        self.save(ctx, &stack)?;
        let result = self.reconcile(ctx, &mut stack);
        self.complete(ctx, &mut stack, "CREATE", result)?;
        Ok(CreateStackOutput {
            stack_id: Some(stack.id),
            ..Default::default()
        })
    }
    fn update_stack(
        &self,
        ctx: &RequestContext,
        input: UpdateStackInput,
    ) -> Result<UpdateStackOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if input.template_url.is_some() {
            return Err(validation("TemplateURL is not supported; use TemplateBody"));
        }
        if input.stack_policy_body.is_some()
            || input.stack_policy_url.is_some()
            || input.stack_policy_during_update_body.is_some()
            || input.stack_policy_during_update_url.is_some()
            || input.rollback_configuration.is_some()
        {
            return Err(validation(
                "Stack policies and rollback options are not supported",
            ));
        }
        let mut stack = self.load(ctx, &input.stack_name)?;
        if stack.status != "CREATE_COMPLETE" && stack.status != "UPDATE_COMPLETE" {
            return Err(validation(format!(
                "Stack is in {} state; delete it before recreating",
                stack.status
            )));
        }
        let template = match input.template_body {
            Some(body) => template::parse(&body)?,
            None if input.use_previous_template == Some(true) => stack.template.clone(),
            _ => {
                return Err(validation(
                    "TemplateBody or UsePreviousTemplate is required",
                ));
            }
        };
        let parameters = parameters(&template, input.parameters, Some(&stack.parameters))?;
        let tags = if input.tags.is_empty() {
            stack.tags.clone()
        } else {
            input.tags.into_iter().map(|t| (t.key, t.value)).collect()
        };
        if stack.template == template && stack.parameters == parameters && stack.tags == tags {
            return Err(validation("No updates are to be performed."));
        }
        stack.template = template;
        stack.parameters = parameters;
        stack.tags = tags;
        stack.status = "UPDATE_IN_PROGRESS".into();
        stack.updated = Some(now());
        self.save(ctx, &stack)?;
        let result = self.reconcile(ctx, &mut stack);
        self.complete(ctx, &mut stack, "UPDATE", result)?;
        Ok(UpdateStackOutput {
            stack_id: Some(stack.id),
            ..Default::default()
        })
    }
    fn delete_stack(&self, ctx: &RequestContext, input: DeleteStackInput) -> Result<(), AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if !input.retain_resources.is_empty() || input.deletion_mode.is_some() {
            return Err(validation(
                "RetainResources and DeletionMode are not supported",
            ));
        }
        let Some(mut stack) = self.find(ctx, &input.stack_name)? else {
            return Ok(());
        };
        if stack.status == "DELETE_COMPLETE" {
            return Ok(());
        }
        stack.status = "DELETE_IN_PROGRESS".into();
        self.save(ctx, &stack)?;
        let result = (|| {
            while !stack.resources.is_empty() {
                let index = stack.resources.len() - 1;
                self.remove(ctx, &mut stack, index)?;
            }
            stack.outputs.clear();
            Ok(())
        })();
        self.complete(ctx, &mut stack, "DELETE", result)
    }
    fn describe_stacks(
        &self,
        ctx: &RequestContext,
        input: DescribeStacksInput,
    ) -> Result<DescribeStacksOutput, AwsError> {
        if input.next_token.is_some() {
            return Err(validation("Pagination tokens are not supported"));
        }
        let stacks = if let Some(name) = input.stack_name {
            vec![self.load(ctx, &name)?]
        } else {
            self.all(ctx)?
                .into_iter()
                .filter(|s| s.status != "DELETE_COMPLETE")
                .collect()
        };
        Ok(DescribeStacksOutput {
            stacks: stacks.iter().map(StackState::response).collect(),
            ..Default::default()
        })
    }
    fn list_stack_resources(
        &self,
        ctx: &RequestContext,
        input: ListStackResourcesInput,
    ) -> Result<ListStackResourcesOutput, AwsError> {
        if input.next_token.is_some() {
            return Err(validation("Pagination tokens are not supported"));
        }
        let stack = self.load(ctx, &input.stack_name)?;
        Ok(ListStackResourcesOutput {
            stack_resource_summaries: stack
                .resources
                .iter()
                .map(|r| StackResourceSummary {
                    logical_resource_id: r.logical_id.clone(),
                    physical_resource_id: Some(r.physical_id.clone()),
                    resource_type: r.resource_type.clone(),
                    resource_status: r.status.clone(),
                    resource_status_reason: r.reason.clone(),
                    last_updated_timestamp: Timestamp(stack.updated.unwrap_or(stack.created)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }
    fn describe_stack_resources(
        &self,
        ctx: &RequestContext,
        input: DescribeStackResourcesInput,
    ) -> Result<DescribeStackResourcesOutput, AwsError> {
        if input.stack_name.is_some() && input.physical_resource_id.is_some() {
            return Err(validation(
                "Specify StackName or PhysicalResourceId, not both",
            ));
        }
        let stacks = if let Some(name) = input.stack_name {
            vec![self.load(ctx, &name)?]
        } else if let Some(ref id) = input.physical_resource_id {
            self.all(ctx)?
                .into_iter()
                .filter(|s| s.resources.iter().any(|r| &r.physical_id == id))
                .collect()
        } else {
            return Err(validation("StackName or PhysicalResourceId is required"));
        };
        Ok(DescribeStackResourcesOutput {
            stack_resources: stacks
                .iter()
                .flat_map(|s| {
                    s.resources
                        .iter()
                        .filter(|r| {
                            input
                                .logical_resource_id
                                .as_ref()
                                .is_none_or(|id| id == &r.logical_id)
                        })
                        .map(|r| s.resource(r))
                })
                .collect(),
        })
    }
}

pub struct CloudFormationHandler(pub Arc<CloudFormation>);
impl CloudFormationHandler {
    pub fn new(
        store: &Store,
        handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    ) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(CloudFormation::new(store, handlers)?)))
    }
}
impl ServiceHandler for CloudFormationHandler {
    fn service(&self) -> &'static str {
        "cloudformation"
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let mut params = QueryParams::parse(&req.query);
        params.extend(QueryParams::parse(&String::from_utf8_lossy(&req.body)));
        let result = params
            .get("Action")
            .ok_or_else(|| AwsError::missing_parameter("Action"))
            .and_then(|action| dispatch(&*self.0, ctx, action, &params));
        Ok(result.unwrap_or_else(|e| query_error(NAMESPACE, &e, &ctx.request_id)))
    }
    fn reset(&self) -> Result<(), AwsError> {
        let _guard = self.0.mutation.lock().unwrap();
        self.0.db.transaction(|tx| {
            tx.execute("DELETE FROM stacks", [])?;
            Ok(())
        })
    }
}
