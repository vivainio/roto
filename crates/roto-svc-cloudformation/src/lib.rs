//! Synchronous CloudFormation stacks for SQS, SNS, S3 and DynamoDB resources.
//! State is scoped by account/region and persisted after each resource mutation.

mod change_sets;
#[allow(clippy::all)]
mod generated;
mod resources;
mod schema;
mod spec;
mod template;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use diesel::prelude::*;
use roto_core::store::{DieselDb as Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler, ids};
use roto_protocol::{QueryParams, Timestamp, query_error};
use schema::stacks;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use change_sets::{
    ChangeSetState, change_output, check_policy, parameter_output, plan, tag_output,
    validate_policy,
};
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
    "ListStacks",
    "DescribeStackEvents",
    "GetTemplate",
    "CreateChangeSet",
    "DescribeChangeSet",
    "ExecuteChangeSet",
    "DeleteChangeSet",
    "ListChangeSets",
    "ListExports",
    "SetStackPolicy",
    "GetStackPolicy",
    "UpdateTerminationProtection",
];

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "CREATE TABLE stacks (
        account_id TEXT NOT NULL, region TEXT NOT NULL, name TEXT NOT NULL,
        stack_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL,
        PRIMARY KEY (account_id, region, name)
    );",
}];
const EXPORT_PAGE_SIZE: usize = 100;

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
    #[serde(default)]
    events: Vec<StoredEvent>,
    #[serde(default = "default_on_failure")]
    on_failure: String,
    #[serde(default)]
    termination_protection: bool,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    root_id: Option<String>,
    #[serde(default)]
    nesting_depth: u8,
    #[serde(default)]
    stack_policy: Option<String>,
    #[serde(default)]
    change_sets: Vec<ChangeSetState>,
}
#[derive(Clone, Serialize, Deserialize)]
struct StoredEvent {
    id: String,
    timestamp: i64,
    logical_id: String,
    resource_type: String,
    physical_id: String,
    status: String,
    reason: Option<String>,
}
/// Events for every status transition between two persisted snapshots. A
/// snapshot from a previous stack with the same name is not a baseline.
fn event_changes(old: Option<&StackState>, new: &StackState) -> Vec<StoredEvent> {
    let at = now();
    let mut events = Vec::new();
    let mut push =
        |logical: &str, ty: &str, physical: &str, status: &str, reason: Option<&String>| {
            events.push(StoredEvent {
                id: ids::request_id(),
                timestamp: at,
                logical_id: logical.into(),
                resource_type: ty.into(),
                physical_id: physical.into(),
                status: status.into(),
                reason: reason.cloned(),
            })
        };
    if old.is_none_or(|o| o.status != new.status || o.reason != new.reason) {
        push(
            &new.name,
            "AWS::CloudFormation::Stack",
            &new.id,
            &new.status,
            new.reason.as_ref(),
        );
    }
    for r in &new.resources {
        let before = old.and_then(|o| o.resources.iter().find(|x| x.logical_id == r.logical_id));
        if before.is_none_or(|b| {
            b.status != r.status || b.reason != r.reason || b.physical_id != r.physical_id
        }) {
            push(
                &r.logical_id,
                &r.resource_type,
                &r.physical_id,
                &r.status,
                r.reason.as_ref(),
            );
        }
    }
    for r in old.iter().flat_map(|o| &o.resources) {
        if !new.resources.iter().any(|x| x.logical_id == r.logical_id) {
            let status = if r.deletion_policy == "Retain" {
                "DELETE_SKIPPED"
            } else {
                "DELETE_COMPLETE"
            };
            push(
                &r.logical_id,
                &r.resource_type,
                &r.physical_id,
                status,
                None,
            );
        }
    }
    events
}
#[derive(Clone, Serialize, Deserialize)]
struct StoredOutput {
    key: String,
    value: String,
    description: Option<String>,
    export: Option<String>,
}

fn stack_can_import(stack: &StackState) -> bool {
    !matches!(
        stack.status.as_str(),
        "REVIEW_IN_PROGRESS" | "DELETE_COMPLETE" | "ROLLBACK_COMPLETE"
    )
}

fn stack_can_export(stack: &StackState) -> bool {
    !stack.outputs.is_empty()
        && !matches!(
            stack.status.as_str(),
            "DELETE_COMPLETE" | "ROLLBACK_COMPLETE"
        )
}

impl StackState {
    fn resolver<'a>(
        &'a self,
        ctx: &'a RequestContext,
        exports: BTreeMap<String, String>,
    ) -> Resolver<'a> {
        Resolver {
            ctx,
            stack_name: &self.name,
            stack_id: &self.id,
            parameters: &self.parameters,
            resources: &self.resources,
            conditions: &self.template["Conditions"],
            exports,
        }
    }
    fn response(&self) -> Stack {
        Stack {
            stack_name: self.name.clone(),
            stack_id: Some(self.id.clone()),
            stack_status: self.status.clone(),
            stack_status_reason: self.reason.clone(),
            parent_id: self.parent_id.clone(),
            root_id: Some(self.root_id.clone().unwrap_or_else(|| self.id.clone())),
            enable_termination_protection: Some(self.termination_protection),
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
    /// Loaded on the first request that checks a template. `None` when unavailable.
    spec: OnceLock<Option<spec::Spec>>,
}

impl CloudFormation {
    pub fn new(
        store: &Store,
        handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>,
    ) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("cloudformation", MIGRATIONS)?,
            resources: Resources::new(handlers),
            mutation: Mutex::new(()),
            spec: OnceLock::new(),
        })
    }
    /// Check each resource's properties against the published spec, when it is available.
    fn spec(&self) -> Option<&spec::Spec> {
        self.spec.get_or_init(spec::load).as_ref()
    }
    fn check_spec(&self, template: &Value) -> Result<(), AwsError> {
        let Some(spec) = self.spec() else {
            return Ok(());
        };
        let Some(resources) = template["Resources"].as_object() else {
            return Ok(());
        };
        let no_properties = json!({});
        for (id, definition) in resources {
            let ty = definition["Type"].as_str().unwrap_or_default();
            let properties = definition.get("Properties").unwrap_or(&no_properties);
            spec.check(ty, properties)
                .map_err(|e| validation(format!("Resource {id}: {e}")))?;
        }
        Ok(())
    }
    fn save(&self, ctx: &RequestContext, stack: &StackState) -> Result<(), AwsError> {
        let old = self
            .find(ctx, &stack.name)?
            .filter(|old| old.id == stack.id);
        let mut persisted = stack.clone();
        persisted.events = old
            .as_ref()
            .map(|old| old.events.clone())
            .unwrap_or_default();
        persisted.events.extend(event_changes(old.as_ref(), stack));
        let body =
            serde_json::to_string(&persisted).map_err(|e| AwsError::internal(e.to_string()))?;
        self.db.transaction(|tx| {
            diesel::insert_into(stacks::table)
                .values((
                    stacks::account_id.eq(&ctx.account_id),
                    stacks::region.eq(&ctx.region),
                    stacks::name.eq(&stack.name),
                    stacks::stack_id.eq(&stack.id),
                    stacks::body.eq(&body),
                ))
                .on_conflict((stacks::account_id, stacks::region, stacks::name))
                .do_update()
                .set((
                    stacks::stack_id.eq(diesel::upsert::excluded(stacks::stack_id)),
                    stacks::body.eq(diesel::upsert::excluded(stacks::body)),
                ))
                .execute(tx)?;
            Ok(())
        })
    }
    fn find(&self, ctx: &RequestContext, name: &str) -> Result<Option<StackState>, AwsError> {
        let body: Option<String> = self.db.read(|c| {
            Ok(stacks::table
                .filter(stacks::account_id.eq(&ctx.account_id))
                .filter(stacks::region.eq(&ctx.region))
                .filter(stacks::name.eq(name).or(stacks::stack_id.eq(name)))
                .select(stacks::body)
                .first(c)
                .optional()?)
        })?;
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
            stacks::table
                .filter(stacks::account_id.eq(&ctx.account_id))
                .filter(stacks::region.eq(&ctx.region))
                .order(stacks::name)
                .select(stacks::body)
                .load::<String>(c)?
                .into_iter()
                .map(|body| {
                    serde_json::from_str(&body).map_err(|e| AwsError::internal(e.to_string()))
                })
                .collect()
        })
    }
    /// Export values visible to a stack in this account and region.
    fn export_values(
        &self,
        ctx: &RequestContext,
        excluded_stack_id: Option<&str>,
    ) -> Result<BTreeMap<String, String>, AwsError> {
        let mut exports = BTreeMap::new();
        for stack in self.all(ctx)? {
            if Some(stack.id.as_str()) == excluded_stack_id || !stack_can_export(&stack) {
                continue;
            }
            for output in stack.outputs {
                let Some(name) = output.export else {
                    continue;
                };
                if exports.insert(name.clone(), output.value).is_some() {
                    return Err(validation(format!(
                        "Export name {name} is already in use in this account and region"
                    )));
                }
            }
        }
        Ok(exports)
    }
    fn imported_names(
        &self,
        ctx: &RequestContext,
        stack: &StackState,
    ) -> Result<BTreeSet<String>, AwsError> {
        fn collect(
            value: &Value,
            resolver: &Resolver<'_>,
            imports: &mut BTreeSet<String>,
        ) -> Result<(), AwsError> {
            match value {
                Value::Object(object) => {
                    if let Some(expression) = object.get("Fn::ImportValue") {
                        let name = text(&resolver.resolve(expression)?)?;
                        if !name.is_empty() {
                            imports.insert(name);
                        }
                    }
                    for child in object.values() {
                        collect(child, resolver, imports)?;
                    }
                }
                Value::Array(values) => {
                    for child in values {
                        collect(child, resolver, imports)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }

        let resolver = stack.resolver(ctx, BTreeMap::new());
        let mut imports = BTreeSet::new();
        collect(&stack.template, &resolver, &mut imports)?;
        Ok(imports)
    }
    /// Enforce account/region export uniqueness and prevent breaking imports.
    fn validate_export_changes(
        &self,
        ctx: &RequestContext,
        stack: &StackState,
        outputs: &[StoredOutput],
    ) -> Result<(), AwsError> {
        let stacks = self.all(ctx)?;
        let mut other_exports = BTreeMap::new();
        for other in &stacks {
            if other.id == stack.id || !stack_can_export(other) {
                continue;
            }
            for output in &other.outputs {
                if let Some(name) = &output.export
                    && other_exports
                        .insert(name.clone(), other.name.clone())
                        .is_some()
                {
                    return Err(validation(format!(
                        "Export name {name} is already in use in this account and region"
                    )));
                }
            }
        }

        let mut next = BTreeMap::new();
        for output in outputs {
            let Some(name) = &output.export else {
                continue;
            };
            if name.is_empty() {
                return Err(validation("Export Name must not be empty"));
            }
            if next.insert(name.as_str(), output.value.as_str()).is_some() {
                return Err(validation(format!(
                    "Export name {name} is used more than once by stack {}",
                    stack.name
                )));
            }
            if let Some(owner) = other_exports.get(name) {
                return Err(validation(format!(
                    "Export name {name} is already exported by stack {owner}"
                )));
            }
        }

        for old in &stack.outputs {
            let Some(name) = &old.export else {
                continue;
            };
            if next
                .get(name.as_str())
                .is_some_and(|value| *value == old.value.as_str())
            {
                continue;
            }
            for consumer in &stacks {
                if consumer.id == stack.id || !stack_can_import(consumer) {
                    continue;
                }
                if self.imported_names(ctx, consumer)?.contains(name) {
                    return Err(validation(format!(
                        "Export {name} cannot be modified or removed because stack {} imports it",
                        consumer.name
                    )));
                }
            }
        }
        Ok(())
    }
    fn reconcile(&self, ctx: &RequestContext, stack: &mut StackState) -> Result<(), AwsError> {
        let exports = self.export_values(ctx, Some(&stack.id))?;
        let ordered = template::order(&stack.template)?;
        for id in &ordered {
            let definition = &stack.template["Resources"][id];
            let ty = definition["Type"].as_str().unwrap().to_string();
            let props = desired_properties(stack, ctx, definition, &exports)?;
            let existing = stack.resources.iter().position(|r| r.logical_id == *id);
            if let Some(index) =
                existing.filter(|index| !replacement(&stack.resources[*index], &ty, &props))
            {
                stack.resources[index].deletion_policy = policy(definition, "DeletionPolicy");
                stack.resources[index].update_replace_policy =
                    policy(definition, "UpdateReplacePolicy");
                if stack.resources[index].properties != props {
                    self.configure(ctx, stack, index, &props, true)?;
                }
            } else {
                let mut resource =
                    self.resources
                        .create(ctx, &stack.name, id, &ty, props.clone())?;
                resource.deletion_policy = policy(definition, "DeletionPolicy");
                resource.update_replace_policy = policy(definition, "UpdateReplacePolicy");
                stack.resources.push(resource);
                self.save(ctx, stack)?;
                let index = stack.resources.len() - 1;
                self.configure(ctx, stack, index, &props, false)?;
                if let Some(index) = existing {
                    let replace_policy = stack.resources[index].update_replace_policy.clone();
                    self.remove_with_policy(ctx, stack, index, &replace_policy)?;
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
                    &stack.resolver(ctx, exports.clone()).resolve(
                        definition
                            .get("Value")
                            .ok_or_else(|| validation("Output requires Value"))?,
                    )?,
                )?;
                let export = definition
                    .get("Export")
                    .map(|v| {
                        text(
                            &stack.resolver(ctx, exports.clone()).resolve(
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
        self.validate_export_changes(ctx, stack, &outputs)?;
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
        let result = if stack.resources[index].resource_type == "AWS::CloudFormation::Stack" {
            self.configure_nested_stack(ctx, stack, index, props, updating)
        } else {
            self.resources
                .configure(ctx, &mut stack.resources[index], props, updating)
        };
        if result.is_ok() {
            stack.resources[index].properties = props.clone();
        }
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
        let policy = stack.resources[index].deletion_policy.clone();
        self.remove_with_policy(ctx, stack, index, &policy)
    }
    fn remove_with_policy(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        index: usize,
        policy: &str,
    ) -> Result<(), AwsError> {
        if policy == "Retain" {
            stack.resources.remove(index);
            return self.save(ctx, stack);
        }
        stack.resources[index].status = "DELETE_IN_PROGRESS".into();
        self.save(ctx, stack)?;
        let result = if stack.resources[index].resource_type == "AWS::CloudFormation::Stack" {
            self.delete_nested_stack(ctx, &stack.resources[index])
        } else {
            self.resources.delete(ctx, &stack.resources[index])
        };
        match result {
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
    fn settle(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        status: &str,
        reason: Option<String>,
    ) -> Result<(), AwsError> {
        stack.status = status.into();
        stack.reason = reason;
        self.save(ctx, stack)
    }
    /// Apply the create outcome. Failures are recorded in the stack status, as in
    /// CloudFormation, so the API call itself still succeeds.
    fn finish_create(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        result: Result<(), AwsError>,
        on_failure: &str,
    ) -> Result<(), AwsError> {
        let error = match result {
            Ok(()) => return self.settle(ctx, stack, "CREATE_COMPLETE", None),
            Err(error) => error.to_string(),
        };
        match on_failure {
            "DO_NOTHING" => self.settle(ctx, stack, "CREATE_FAILED", Some(error)),
            "DELETE" => {
                self.settle(ctx, stack, "DELETE_IN_PROGRESS", Some(error))?;
                match self.remove_all(ctx, stack, &[], false) {
                    Ok(()) => self.settle(ctx, stack, "DELETE_COMPLETE", None),
                    Err(e) => self.settle(ctx, stack, "DELETE_FAILED", Some(e.to_string())),
                }
            }
            _ => {
                self.settle(ctx, stack, "ROLLBACK_IN_PROGRESS", Some(error.clone()))?;
                match self.remove_all(ctx, stack, &[], false) {
                    Ok(()) => self.settle(ctx, stack, "ROLLBACK_COMPLETE", Some(error)),
                    Err(e) => self.settle(ctx, stack, "ROLLBACK_FAILED", Some(e.to_string())),
                }
            }
        }
    }
    /// Reconcile to a new template, rolling back to the previous one on failure
    /// unless rollback is disabled.
    fn apply_update(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        template: Value,
        parameters: BTreeMap<String, String>,
        tags: BTreeMap<String, String>,
        disable_rollback: bool,
    ) -> Result<(), AwsError> {
        let previous = (
            std::mem::replace(&mut stack.template, template),
            std::mem::replace(&mut stack.parameters, parameters),
            std::mem::replace(&mut stack.tags, tags),
        );
        stack.status = "UPDATE_IN_PROGRESS".into();
        stack.reason = None;
        stack.updated = Some(now());
        self.save(ctx, stack)?;
        let error = match self.reconcile(ctx, stack) {
            Ok(()) => return self.settle(ctx, stack, "UPDATE_COMPLETE", None),
            Err(error) => error,
        };
        if disable_rollback {
            return self.settle(ctx, stack, "UPDATE_FAILED", Some(error.to_string()));
        }
        self.settle(
            ctx,
            stack,
            "UPDATE_ROLLBACK_IN_PROGRESS",
            Some(error.to_string()),
        )?;
        stack.template = previous.0;
        stack.parameters = previous.1;
        stack.tags = previous.2;
        match self.reconcile(ctx, stack) {
            Ok(()) => self.settle(
                ctx,
                stack,
                "UPDATE_ROLLBACK_COMPLETE",
                Some(error.to_string()),
            ),
            Err(rollback) => self.settle(
                ctx,
                stack,
                "UPDATE_ROLLBACK_FAILED",
                Some(rollback.to_string()),
            ),
        }
    }
    /// Remove every resource, newest first. Resources in `retain` are dropped
    /// from the stack without deleting them. With `force`, failed deletions are
    /// dropped too.
    fn remove_all(
        &self,
        ctx: &RequestContext,
        stack: &mut StackState,
        retain: &[String],
        force: bool,
    ) -> Result<(), AwsError> {
        while !stack.resources.is_empty() {
            let index = stack.resources.len() - 1;
            let policy = if retain.contains(&stack.resources[index].logical_id) {
                "Retain".to_string()
            } else {
                stack.resources[index].deletion_policy.clone()
            };
            if let Err(error) = self.remove_with_policy(ctx, stack, index, &policy) {
                if !force {
                    return Err(error);
                }
                stack.resources.remove(index);
                self.save(ctx, stack)?;
            }
        }
        Ok(())
    }

    fn configure_nested_stack(
        &self,
        ctx: &RequestContext,
        parent: &mut StackState,
        index: usize,
        props: &Value,
        updating: bool,
    ) -> Result<(), AwsError> {
        let name = parent.resources[index].name.clone();
        let result = self.configure_nested_stack_state(ctx, parent, &name, props, updating);
        if let Some(child) = self.find(ctx, &name)? {
            parent.resources[index].physical_id = child.id.clone();
            parent.resources[index].attributes = nested_stack_attributes(&child);
        }
        result
    }

    fn configure_nested_stack_state(
        &self,
        ctx: &RequestContext,
        parent: &StackState,
        name: &str,
        props: &Value,
        updating: bool,
    ) -> Result<(), AwsError> {
        let template_url = props["TemplateURL"]
            .as_str()
            .ok_or_else(|| validation("AWS::CloudFormation::Stack requires a TemplateURL"))?;
        let body = self.resources.template_from_url(ctx, template_url)?;
        let template = template::parse(&body)?;
        self.check_spec(&template)?;
        let inputs = nested_parameters(props)?;
        let tags = tag_map(props)?;
        let parent_id = parent.id.clone();
        let root_id = parent.root_id.clone().unwrap_or_else(|| parent.id.clone());
        let depth = parent.nesting_depth.saturating_add(1);
        if depth > MAX_NESTED_STACK_DEPTH {
            return Err(validation(format!(
                "Nested stack depth exceeds {MAX_NESTED_STACK_DEPTH}"
            )));
        }

        let current = self
            .find(ctx, name)?
            .filter(|s| s.status != "DELETE_COMPLETE");
        if updating {
            if let Some(mut child) = current {
                if child.parent_id.as_deref() != Some(parent_id.as_str()) {
                    return Err(validation(format!(
                        "Nested stack name {name} is already in use"
                    )));
                }
                let child_parameters = parameters(&template, inputs, Some(&child.parameters))?;
                if child.template != template
                    || child.parameters != child_parameters
                    || child.tags != tags
                {
                    self.apply_update(ctx, &mut child, template, child_parameters, tags, false)?;
                    if child.status != "UPDATE_COMPLETE" {
                        return Err(validation(format!(
                            "Nested stack update failed: {}",
                            child.reason.as_deref().unwrap_or(&child.status)
                        )));
                    }
                }
                return Ok(());
            }
        } else if current.is_some() {
            return Err(validation(format!(
                "Nested stack name {name} is already in use"
            )));
        }

        let child_parameters = parameters(&template, inputs, None)?;
        let mut child = StackState {
            id: new_stack_id(ctx, name),
            name: name.into(),
            status: "CREATE_IN_PROGRESS".into(),
            reason: None,
            created: now(),
            updated: None,
            template,
            parameters: child_parameters,
            tags,
            resources: Vec::new(),
            outputs: Vec::new(),
            events: Vec::new(),
            on_failure: default_on_failure(),
            termination_protection: false,
            parent_id: Some(parent_id),
            root_id: Some(root_id),
            nesting_depth: depth,
            stack_policy: None,
            change_sets: Vec::new(),
        };
        self.save(ctx, &child)?;
        let result = self.reconcile(ctx, &mut child);
        self.finish_create(ctx, &mut child, result, "ROLLBACK")?;
        if child.status != "CREATE_COMPLETE" {
            return Err(validation(format!(
                "Nested stack creation failed: {}",
                child.reason.as_deref().unwrap_or(&child.status)
            )));
        }
        Ok(())
    }

    fn delete_nested_stack(
        &self,
        ctx: &RequestContext,
        resource: &Resource,
    ) -> Result<(), AwsError> {
        let Some(mut child) = self.find(ctx, &resource.name)? else {
            return Ok(());
        };
        if child.status == "DELETE_COMPLETE" {
            return Ok(());
        }
        self.validate_export_changes(ctx, &child, &[])?;
        child.status = "DELETE_IN_PROGRESS".into();
        child.reason = None;
        self.save(ctx, &child)?;
        let result = self.remove_all(ctx, &mut child, &[], false);
        match result {
            Ok(()) => {
                child.outputs.clear();
                self.settle(ctx, &mut child, "DELETE_COMPLETE", None)
            }
            Err(error) => {
                self.settle(ctx, &mut child, "DELETE_FAILED", Some(error.to_string()))?;
                Err(error)
            }
        }
    }
}

const EVENT_PAGE_SIZE: usize = 100;
const MAX_NESTED_STACK_DEPTH: u8 = 16;

fn nested_parameters(props: &Value) -> Result<Vec<Parameter>, AwsError> {
    let Some(value) = props.get("Parameters") else {
        return Ok(Vec::new());
    };
    let parameters = value
        .as_object()
        .ok_or_else(|| validation("Nested stack Parameters must be an object"))?;
    parameters
        .iter()
        .map(|(key, value)| {
            Ok(Parameter {
                parameter_key: Some(key.clone()),
                parameter_value: Some(text(value)?),
                ..Default::default()
            })
        })
        .collect()
}

fn nested_stack_attributes(stack: &StackState) -> BTreeMap<String, Value> {
    stack
        .outputs
        .iter()
        .map(|output| (format!("Outputs.{}", output.key), json!(output.value)))
        .collect()
}

fn default_on_failure() -> String {
    "ROLLBACK".into()
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

fn new_stack_id(ctx: &RequestContext, name: &str) -> String {
    format!(
        "arn:{}:cloudformation:{}:{}:stack/{}/{}",
        partition(&ctx.region),
        ctx.region,
        ctx.account_id,
        name,
        ids::request_id()
    )
}

fn on_failure(value: Option<&str>, disable_rollback: Option<bool>) -> Result<String, AwsError> {
    match (value, disable_rollback) {
        (Some(_), Some(_)) => Err(validation(
            "OnFailure and DisableRollback are mutually exclusive",
        )),
        (Some(value @ ("ROLLBACK" | "DELETE" | "DO_NOTHING")), None) => Ok(value.into()),
        (Some(_), None) => Err(validation(
            "OnFailure must be ROLLBACK, DELETE or DO_NOTHING",
        )),
        (None, Some(true)) => Ok("DO_NOTHING".into()),
        (None, _) => Ok(default_on_failure()),
    }
}

fn rollback_configuration(config: Option<&RollbackConfiguration>) -> Result<(), AwsError> {
    if config.is_some_and(|c| !c.rollback_triggers.is_empty()) {
        return Err(validation("Rollback triggers are not supported"));
    }
    Ok(())
}

/// Resolve a resource's properties as the stack would create them, including
/// the tags it inherits from the stack.
fn desired_properties(
    stack: &StackState,
    ctx: &RequestContext,
    definition: &Value,
    exports: &BTreeMap<String, String>,
) -> Result<Value, AwsError> {
    let mut props = stack
        .resolver(ctx, exports.clone())
        .resolve(definition.get("Properties").unwrap_or(&json!({})))?;
    let mut tags = stack.tags.clone();
    tags.extend(tag_map(&props)?);
    if !tags.is_empty() {
        props["Tags"] = tag_list(tags);
    }
    Ok(props)
}

fn policy(definition: &Value, key: &str) -> String {
    definition
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("Delete")
        .to_string()
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
    fn create_change_set(
        &self,
        ctx: &RequestContext,
        input: CreateChangeSetInput,
    ) -> Result<CreateChangeSetOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if input.template_url.is_some() {
            return Err(validation("TemplateURL is not supported; use TemplateBody"));
        }
        if !input.resources_to_import.is_empty()
            || input.include_nested_stacks == Some(true)
            || input.import_existing_resources == Some(true)
        {
            return Err(validation(
                "Import and nested change sets are not supported",
            ));
        }
        if !valid_name(&input.change_set_name) {
            return Err(validation("Invalid ChangeSetName"));
        }
        let existing = self
            .find(ctx, &input.stack_name)?
            .filter(|s| s.status != "DELETE_COMPLETE");
        let kind = match (input.change_set_type.as_deref(), existing.is_some()) {
            (Some("CREATE") | None, false) => "CREATE",
            (Some("UPDATE") | None, true) => "UPDATE",
            (Some("CREATE"), true) => {
                return Err(AwsError::sender(
                    400,
                    "AlreadyExistsException",
                    format!("Stack {} already exists", input.stack_name),
                ));
            }
            (Some("UPDATE"), false) => {
                return Err(validation(format!(
                    "Stack [{}] does not exist",
                    input.stack_name
                )));
            }
            (Some(other), _) => {
                return Err(validation(format!("Unsupported ChangeSetType: {other}")));
            }
        };
        if kind == "CREATE" && !valid_name(&input.stack_name) {
            return Err(validation("Invalid StackName"));
        }
        let mut stack = match existing {
            Some(stack) => stack,
            None => StackState {
                id: new_stack_id(ctx, &input.stack_name),
                name: input.stack_name.clone(),
                status: "REVIEW_IN_PROGRESS".into(),
                reason: None,
                created: now(),
                updated: None,
                template: json!({}),
                parameters: BTreeMap::new(),
                tags: BTreeMap::new(),
                resources: Vec::new(),
                outputs: Vec::new(),
                events: Vec::new(),
                on_failure: default_on_failure(),
                termination_protection: false,
                stack_policy: None,
                change_sets: Vec::new(),
                parent_id: None,
                root_id: None,
                nesting_depth: 0,
            },
        };
        let template = match input.template_body.as_deref() {
            Some(body) => template::parse(body)?,
            None if kind == "UPDATE" && input.use_previous_template == Some(true) => {
                stack.template.clone()
            }
            None => {
                return Err(validation(
                    "TemplateBody or UsePreviousTemplate is required",
                ));
            }
        };
        let previous = (kind == "UPDATE").then_some(&stack.parameters);
        self.check_spec(&template)?;
        let parameters = parameters(&template, input.parameters, previous)?;
        let tags = if input.tags.is_empty() {
            stack.tags.clone()
        } else {
            input.tags.into_iter().map(|t| (t.key, t.value)).collect()
        };
        if stack
            .change_sets
            .iter()
            .any(|c| c.name == input.change_set_name)
        {
            return Err(AwsError::sender(
                400,
                "AlreadyExistsException",
                format!("ChangeSet [{}] already exists", input.change_set_name),
            ));
        }
        let target = StackState {
            template: template.clone(),
            parameters: parameters.clone(),
            tags: tags.clone(),
            ..stack.clone()
        };
        let exports = self.export_values(ctx, Some(&stack.id))?;
        let changes = plan(ctx, &stack, &target, &exports)?;
        let mut change_set = ChangeSetState {
            id: format!(
                "arn:{}:cloudformation:{}:{}:changeSet/{}/{}",
                partition(&ctx.region),
                ctx.region,
                ctx.account_id,
                input.change_set_name,
                ids::request_id()
            ),
            name: input.change_set_name,
            description: input.description,
            kind: kind.into(),
            created: now(),
            status: "CREATE_COMPLETE".into(),
            reason: None,
            execution: "AVAILABLE".into(),
            template,
            parameters,
            tags,
            changes,
        };
        if change_set.changes.is_empty() {
            change_set.status = "FAILED".into();
            change_set.execution = "UNAVAILABLE".into();
            change_set.reason = Some(
                "The submitted information didn't contain changes. Submit different information to create a change set."
                    .into(),
            );
        }
        let output = CreateChangeSetOutput {
            id: Some(change_set.id.clone()),
            stack_id: Some(stack.id.clone()),
        };
        stack.change_sets.push(change_set);
        self.save(ctx, &stack)?;
        Ok(output)
    }

    fn describe_change_set(
        &self,
        ctx: &RequestContext,
        input: DescribeChangeSetInput,
    ) -> Result<DescribeChangeSetOutput, AwsError> {
        if input.next_token.is_some() {
            return Err(validation("Pagination tokens are not supported"));
        }
        let (stack, index) =
            self.locate_change_set(ctx, input.stack_name.as_deref(), &input.change_set_name)?;
        let change_set = &stack.change_sets[index];
        Ok(DescribeChangeSetOutput {
            change_set_id: Some(change_set.id.clone()),
            change_set_name: Some(change_set.name.clone()),
            changes: change_set.changes.iter().map(change_output).collect(),
            creation_time: Some(Timestamp(change_set.created)),
            description: change_set.description.clone(),
            execution_status: Some(change_set.execution.clone()),
            parameters: parameter_output(&change_set.parameters),
            stack_id: Some(stack.id.clone()),
            stack_name: Some(stack.name.clone()),
            status: Some(change_set.status.clone()),
            status_reason: change_set.reason.clone(),
            tags: tag_output(&change_set.tags),
            ..Default::default()
        })
    }

    fn execute_change_set(
        &self,
        ctx: &RequestContext,
        input: ExecuteChangeSetInput,
    ) -> Result<ExecuteChangeSetOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        let (mut stack, index) =
            self.locate_change_set(ctx, input.stack_name.as_deref(), &input.change_set_name)?;
        let change_set = stack.change_sets[index].clone();
        if change_set.execution != "AVAILABLE" {
            return Err(validation(format!(
                "ChangeSet [{}] cannot be executed in its current status of [{}]",
                change_set.id, change_set.execution
            )));
        }
        check_policy(stack.stack_policy.as_deref(), &change_set.changes)?;
        let disable_rollback = input.disable_rollback == Some(true);
        for (i, other) in stack.change_sets.iter_mut().enumerate() {
            if i != index && other.execution == "AVAILABLE" {
                other.execution = "OBSOLETE".into();
            }
        }
        stack.change_sets[index].execution = "EXECUTE_IN_PROGRESS".into();
        if stack.status == "REVIEW_IN_PROGRESS" {
            stack.template = change_set.template;
            stack.parameters = change_set.parameters;
            stack.tags = change_set.tags;
            stack.status = "CREATE_IN_PROGRESS".into();
            stack.reason = None;
            self.save(ctx, &stack)?;
            let on_failure = if disable_rollback {
                "DO_NOTHING".to_string()
            } else {
                stack.on_failure.clone()
            };
            let result = self.reconcile(ctx, &mut stack);
            self.finish_create(ctx, &mut stack, result, &on_failure)?;
        } else {
            self.apply_update(
                ctx,
                &mut stack,
                change_set.template,
                change_set.parameters,
                change_set.tags,
                disable_rollback,
            )?;
        }
        let succeeded = matches!(stack.status.as_str(), "CREATE_COMPLETE" | "UPDATE_COMPLETE");
        stack.change_sets[index].execution = if succeeded {
            "EXECUTE_COMPLETE"
        } else {
            "EXECUTE_FAILED"
        }
        .into();
        self.save(ctx, &stack)?;
        Ok(ExecuteChangeSetOutput {})
    }

    fn delete_change_set(
        &self,
        ctx: &RequestContext,
        input: DeleteChangeSetInput,
    ) -> Result<DeleteChangeSetOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        // Deleting a change set that does not exist succeeds, as it does in AWS.
        let Ok((mut stack, index)) =
            self.locate_change_set(ctx, input.stack_name.as_deref(), &input.change_set_name)
        else {
            return Ok(DeleteChangeSetOutput {});
        };
        if stack.change_sets[index].execution == "EXECUTE_IN_PROGRESS" {
            return Err(validation(
                "ChangeSet cannot be deleted while it is executing",
            ));
        }
        stack.change_sets.remove(index);
        if stack.status == "REVIEW_IN_PROGRESS" && stack.change_sets.is_empty() {
            stack.status = "DELETE_COMPLETE".into();
        }
        self.save(ctx, &stack)?;
        Ok(DeleteChangeSetOutput {})
    }

    fn list_change_sets(
        &self,
        ctx: &RequestContext,
        input: ListChangeSetsInput,
    ) -> Result<ListChangeSetsOutput, AwsError> {
        if input.next_token.is_some() {
            return Err(validation("Pagination tokens are not supported"));
        }
        let stack = self.load(ctx, &input.stack_name)?;
        Ok(ListChangeSetsOutput {
            summaries: stack
                .change_sets
                .iter()
                .map(|c| ChangeSetSummary {
                    change_set_id: Some(c.id.clone()),
                    change_set_name: Some(c.name.clone()),
                    creation_time: Some(Timestamp(c.created)),
                    description: c.description.clone(),
                    execution_status: Some(c.execution.clone()),
                    stack_id: Some(stack.id.clone()),
                    stack_name: Some(stack.name.clone()),
                    status: Some(c.status.clone()),
                    status_reason: c.reason.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    fn set_stack_policy(
        &self,
        ctx: &RequestContext,
        input: SetStackPolicyInput,
    ) -> Result<(), AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if input.stack_policy_url.is_some() {
            return Err(validation(
                "StackPolicyURL is not supported; use StackPolicyBody",
            ));
        }
        let body = input
            .stack_policy_body
            .ok_or_else(|| validation("StackPolicyBody is required"))?;
        validate_policy(&body)?;
        let mut stack = self.load(ctx, &input.stack_name)?;
        stack.stack_policy = Some(body);
        self.save(ctx, &stack)
    }

    fn get_stack_policy(
        &self,
        ctx: &RequestContext,
        input: GetStackPolicyInput,
    ) -> Result<GetStackPolicyOutput, AwsError> {
        Ok(GetStackPolicyOutput {
            stack_policy_body: self.load(ctx, &input.stack_name)?.stack_policy,
        })
    }

    fn update_termination_protection(
        &self,
        ctx: &RequestContext,
        input: UpdateTerminationProtectionInput,
    ) -> Result<UpdateTerminationProtectionOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        let mut stack = self.load(ctx, &input.stack_name)?;
        stack.termination_protection = input.enable_termination_protection;
        self.save(ctx, &stack)?;
        Ok(UpdateTerminationProtectionOutput {
            stack_id: Some(stack.id),
        })
    }
    fn create_stack(
        &self,
        ctx: &RequestContext,
        input: CreateStackInput,
    ) -> Result<CreateStackOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        if input.template_url.is_some() {
            return Err(validation("TemplateURL is not supported; use TemplateBody"));
        }
        if input.stack_policy_url.is_some() {
            return Err(validation(
                "StackPolicyURL is not supported; use StackPolicyBody",
            ));
        }
        let on_failure = on_failure(input.on_failure.as_deref(), input.disable_rollback)?;
        rollback_configuration(input.rollback_configuration.as_ref())?;
        if let Some(body) = &input.stack_policy_body {
            validate_policy(body)?;
        }
        if !valid_name(&input.stack_name) {
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
        self.check_spec(&template)?;
        let parameters = parameters(&template, input.parameters, None)?;
        let id = new_stack_id(ctx, &input.stack_name);
        let mut stack = StackState {
            id: id.clone(),
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
            events: Vec::new(),
            on_failure,
            termination_protection: input.enable_termination_protection.unwrap_or(false),
            stack_policy: input.stack_policy_body,
            change_sets: Vec::new(),
            parent_id: None,
            root_id: Some(id),
            nesting_depth: 0,
        };
        self.save(ctx, &stack)?;
        let result = self.reconcile(ctx, &mut stack);
        let on_failure = stack.on_failure.clone();
        self.finish_create(ctx, &mut stack, result, &on_failure)?;
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
        if input.stack_policy_url.is_some() || input.stack_policy_during_update_url.is_some() {
            return Err(validation(
                "Stack policy URLs are not supported; use StackPolicyBody",
            ));
        }
        rollback_configuration(input.rollback_configuration.as_ref())?;
        if let Some(body) = &input.stack_policy_body {
            validate_policy(body)?;
        }
        if let Some(body) = &input.stack_policy_during_update_body {
            validate_policy(body)?;
        }
        let mut stack = self.load(ctx, &input.stack_name)?;
        if !matches!(
            stack.status.as_str(),
            "CREATE_COMPLETE" | "UPDATE_COMPLETE" | "UPDATE_ROLLBACK_COMPLETE"
        ) {
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
        self.check_spec(&template)?;
        let parameters = parameters(&template, input.parameters, Some(&stack.parameters))?;
        let tags = if input.tags.is_empty() {
            stack.tags.clone()
        } else {
            input.tags.into_iter().map(|t| (t.key, t.value)).collect()
        };
        if stack.template == template && stack.parameters == parameters && stack.tags == tags {
            return Err(validation("No updates are to be performed."));
        }
        let target = StackState {
            template: template.clone(),
            parameters: parameters.clone(),
            tags: tags.clone(),
            ..stack.clone()
        };
        let exports = self.export_values(ctx, Some(&stack.id))?;
        let changes = plan(ctx, &stack, &target, &exports)?;
        let policy = input
            .stack_policy_during_update_body
            .or_else(|| stack.stack_policy.clone());
        check_policy(policy.as_deref(), &changes)?;
        self.apply_update(ctx, &mut stack, template, parameters, tags, false)?;
        if let Some(body) = input.stack_policy_body {
            stack.stack_policy = Some(body);
            self.save(ctx, &stack)?;
        }
        Ok(UpdateStackOutput {
            stack_id: Some(stack.id),
            ..Default::default()
        })
    }
    fn delete_stack(&self, ctx: &RequestContext, input: DeleteStackInput) -> Result<(), AwsError> {
        let _guard = self.mutation.lock().unwrap();
        let force = match input.deletion_mode.as_deref() {
            None | Some("STANDARD") => false,
            Some("FORCE_DELETE_STACK") => true,
            Some(_) => {
                return Err(validation(
                    "DeletionMode must be STANDARD or FORCE_DELETE_STACK",
                ));
            }
        };
        let Some(mut stack) = self.find(ctx, &input.stack_name)? else {
            return Ok(());
        };
        if stack.status == "DELETE_COMPLETE" {
            return Ok(());
        }
        if stack.termination_protection {
            return Err(validation(format!(
                "Stack [{}] cannot be deleted while TerminationProtection is enabled.",
                stack.name
            )));
        }
        self.validate_export_changes(ctx, &stack, &[])?;
        if let Some(missing) = input
            .retain_resources
            .iter()
            .find(|id| !stack.resources.iter().any(|r| &r.logical_id == *id))
        {
            return Err(validation(format!(
                "Resource {missing} does not exist in stack {}",
                stack.name
            )));
        }
        stack.status = "DELETE_IN_PROGRESS".into();
        stack.reason = None;
        self.save(ctx, &stack)?;
        let result = self.remove_all(ctx, &mut stack, &input.retain_resources, force);
        match result {
            Ok(()) => {
                stack.outputs.clear();
                self.settle(ctx, &mut stack, "DELETE_COMPLETE", None)
            }
            Err(error) => self.settle(ctx, &mut stack, "DELETE_FAILED", Some(error.to_string())),
        }
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
    fn list_exports(
        &self,
        ctx: &RequestContext,
        input: ListExportsInput,
    ) -> Result<ListExportsOutput, AwsError> {
        let _guard = self.mutation.lock().unwrap();
        let offset = input
            .next_token
            .as_deref()
            .map(str::parse::<usize>)
            .transpose()
            .map_err(|_| validation("Invalid NextToken"))?
            .unwrap_or(0);
        let mut exports = Vec::new();
        for stack in self.all(ctx)? {
            if !stack_can_export(&stack) {
                continue;
            }
            for output in stack.outputs {
                let Some(name) = output.export else {
                    continue;
                };
                exports.push((
                    name.clone(),
                    Export {
                        exporting_stack_id: Some(stack.id.clone()),
                        name: Some(name),
                        value: Some(output.value),
                    },
                ));
            }
        }
        exports.sort_by(|a, b| a.0.cmp(&b.0));
        let next_token = (offset + EXPORT_PAGE_SIZE < exports.len())
            .then(|| (offset + EXPORT_PAGE_SIZE).to_string());
        Ok(ListExportsOutput {
            exports: exports
                .into_iter()
                .skip(offset)
                .take(EXPORT_PAGE_SIZE)
                .map(|(_, export)| export)
                .collect(),
            next_token,
        })
    }
    fn list_stacks(
        &self,
        ctx: &RequestContext,
        input: ListStacksInput,
    ) -> Result<ListStacksOutput, AwsError> {
        if input.next_token.is_some() {
            return Err(validation("Pagination tokens are not supported"));
        }
        let stacks = self
            .all(ctx)?
            .into_iter()
            .filter(|stack| {
                input.stack_status_filter.is_empty()
                    || input
                        .stack_status_filter
                        .iter()
                        .any(|status| status == &stack.status)
            })
            .map(|stack| StackSummary {
                creation_time: Timestamp(stack.created),
                deletion_time: (stack.status == "DELETE_COMPLETE")
                    .then(|| Timestamp(stack.updated.unwrap_or(stack.created))),
                last_updated_time: stack.updated.map(Timestamp),
                parent_id: stack.parent_id.clone(),
                root_id: Some(stack.root_id.clone().unwrap_or_else(|| stack.id.clone())),
                stack_id: Some(stack.id.clone()),
                stack_name: stack.name.clone(),
                stack_status: stack.status.clone(),
                stack_status_reason: stack.reason.clone(),
                template_description: stack.template["Description"].as_str().map(str::to_string),
                ..Default::default()
            })
            .collect();
        Ok(ListStacksOutput {
            stack_summaries: stacks,
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
    fn get_template(
        &self,
        ctx: &RequestContext,
        input: GetTemplateInput,
    ) -> Result<GetTemplateOutput, AwsError> {
        if input.change_set_name.is_some() {
            return Err(validation("Change sets are not supported"));
        }
        let stack = self.load(
            ctx,
            input
                .stack_name
                .as_deref()
                .ok_or_else(|| validation("StackName is required"))?,
        )?;
        Ok(GetTemplateOutput {
            stages_available: vec!["Original".into()],
            template_body: Some(
                serde_json::to_string(&stack.template)
                    .map_err(|e| AwsError::internal(e.to_string()))?,
            ),
        })
    }
    fn describe_stack_events(
        &self,
        ctx: &RequestContext,
        input: DescribeStackEventsInput,
    ) -> Result<DescribeStackEventsOutput, AwsError> {
        let offset = match input.next_token.as_deref() {
            None => 0,
            Some(token) => token
                .parse::<usize>()
                .map_err(|_| validation("Invalid NextToken"))?,
        };
        let stack = self.load(ctx, &input.stack_name)?;
        // Newest first, as CloudFormation returns them.
        let events: Vec<_> = stack.events.iter().rev().collect();
        let next_token = (offset + EVENT_PAGE_SIZE < events.len())
            .then(|| (offset + EVENT_PAGE_SIZE).to_string());
        Ok(DescribeStackEventsOutput {
            stack_events: events
                .into_iter()
                .skip(offset)
                .take(EVENT_PAGE_SIZE)
                .map(|e| StackEvent {
                    event_id: e.id.clone(),
                    stack_id: stack.id.clone(),
                    stack_name: stack.name.clone(),
                    logical_resource_id: Some(e.logical_id.clone()),
                    physical_resource_id: Some(e.physical_id.clone()),
                    resource_type: Some(e.resource_type.clone()),
                    resource_status: Some(e.status.clone()),
                    resource_status_reason: e.reason.clone(),
                    timestamp: Timestamp(e.timestamp),
                    ..Default::default()
                })
                .collect(),
            next_token,
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
            diesel::delete(stacks::table).execute(tx)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost".into(),
        }
    }

    fn service() -> CloudFormation {
        let store = Store::ephemeral();
        let sqs: Arc<dyn ServiceHandler> = Arc::new(roto_svc_sqs::SqsHandler::new(&store).unwrap());
        let svc = CloudFormation::new(&store, HashMap::from([("sqs", sqs)])).unwrap();
        // Keep unit tests offline: no spec download.
        assert!(svc.spec.set(None).is_ok());
        svc
    }

    fn queue(name: &str) -> Value {
        json!({"Type": "AWS::SQS::Queue", "Properties": {"QueueName": name}})
    }

    fn template(resources: Value) -> String {
        json!({ "Resources": resources }).to_string()
    }

    fn create(svc: &CloudFormation, stack: &str, body: String) -> Result<(), AwsError> {
        svc.create_stack(
            &ctx(),
            CreateStackInput {
                stack_name: stack.into(),
                template_body: Some(body),
                ..Default::default()
            },
        )
        .map(|_| ())
    }

    fn update(svc: &CloudFormation, stack: &str, body: String) -> Result<(), AwsError> {
        svc.update_stack(
            &ctx(),
            UpdateStackInput {
                stack_name: stack.into(),
                template_body: Some(body),
                ..Default::default()
            },
        )
        .map(|_| ())
    }

    fn status(svc: &CloudFormation, stack: &str) -> String {
        svc.describe_stacks(
            &ctx(),
            DescribeStacksInput {
                stack_name: Some(stack.into()),
                ..Default::default()
            },
        )
        .unwrap()
        .stacks[0]
            .stack_status
            .clone()
    }

    fn resource_ids(svc: &CloudFormation, stack: &str) -> Vec<String> {
        svc.list_stack_resources(
            &ctx(),
            ListStackResourcesInput {
                stack_name: stack.into(),
                ..Default::default()
            },
        )
        .unwrap()
        .stack_resource_summaries
        .into_iter()
        .map(|r| r.logical_resource_id)
        .collect()
    }

    #[test]
    fn change_set_reports_replacement_when_only_resource_type_changes() {
        let svc = service();
        create(
            &svc,
            "type-change",
            template(json!({"Resource": {"Type": "AWS::SQS::Queue"}})),
        )
        .unwrap();
        svc.create_change_set(
            &ctx(),
            CreateChangeSetInput {
                stack_name: "type-change".into(),
                change_set_name: "replace".into(),
                template_body: Some(template(json!({"Resource": {"Type": "AWS::SNS::Topic"}}))),
                ..Default::default()
            },
        )
        .unwrap();
        let stack = svc.load(&ctx(), "type-change").unwrap();
        let changes = &stack.change_sets[0].changes;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].action, "Modify");
        assert_eq!(changes[0].replacement, Some(true));
    }

    #[test]
    fn events_are_newest_first_and_paginate() {
        let svc = service();
        let resources: serde_json::Map<String, Value> = (0..60)
            .map(|i| (format!("Queue{i}"), queue(&format!("events-{i}"))))
            .collect();
        create(&svc, "events", template(resources.into())).unwrap();
        let input = |next_token| DescribeStackEventsInput {
            stack_name: "events".into(),
            next_token,
        };
        let first = svc.describe_stack_events(&ctx(), input(None)).unwrap();
        assert_eq!(first.stack_events.len(), EVENT_PAGE_SIZE);
        assert_eq!(
            first.stack_events[0].logical_resource_id.as_deref(),
            Some("events")
        );
        assert_eq!(
            first.stack_events[0].resource_status.as_deref(),
            Some("CREATE_COMPLETE")
        );
        let mut total = first.stack_events.len();
        let mut next = first.next_token;
        while let Some(token) = next {
            let page = svc
                .describe_stack_events(&ctx(), input(Some(token)))
                .unwrap();
            assert!(page.stack_events.len() <= EVENT_PAGE_SIZE);
            total += page.stack_events.len();
            next = page.next_token;
        }
        assert!(total > EVENT_PAGE_SIZE);
    }

    #[test]
    fn get_template_returns_the_stored_template() {
        let svc = service();
        let body = template(json!({"Queue": queue("template-queue")}));
        create(&svc, "template", body.clone()).unwrap();
        let output = svc
            .get_template(
                &ctx(),
                GetTemplateInput {
                    stack_name: Some("template".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let stored: Value = serde_json::from_str(&output.template_body.unwrap()).unwrap();
        assert_eq!(stored, serde_json::from_str::<Value>(&body).unwrap());
    }

    #[test]
    fn change_set_previews_then_executes_an_update() {
        let svc = service();
        create(&svc, "changes", template(json!({"A": queue("changes-a")}))).unwrap();
        let target = template(json!({
            "A": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "changes-a", "DelaySeconds": 5}},
            "B": queue("changes-b"),
        }));
        svc.create_change_set(
            &ctx(),
            CreateChangeSetInput {
                stack_name: "changes".into(),
                change_set_name: "preview".into(),
                template_body: Some(target),
                ..Default::default()
            },
        )
        .unwrap();
        let described = svc
            .describe_change_set(
                &ctx(),
                DescribeChangeSetInput {
                    stack_name: Some("changes".into()),
                    change_set_name: "preview".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        let change = |id: &str| {
            described
                .changes
                .iter()
                .map(|c| c.resource_change.as_ref().unwrap())
                .find(|c| c.logical_resource_id.as_deref() == Some(id))
                .unwrap()
                .clone()
        };
        assert_eq!(change("B").action.as_deref(), Some("Add"));
        assert_eq!(change("A").action.as_deref(), Some("Modify"));
        assert_eq!(change("A").replacement.as_deref(), Some("False"));
        // Previewing does not touch the stack.
        assert_eq!(resource_ids(&svc, "changes"), ["A"]);

        svc.execute_change_set(
            &ctx(),
            ExecuteChangeSetInput {
                stack_name: Some("changes".into()),
                change_set_name: "preview".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(status(&svc, "changes"), "UPDATE_COMPLETE");
        assert_eq!(resource_ids(&svc, "changes"), ["A", "B"]);
    }

    #[test]
    fn stack_policy_denies_replacement_but_allows_other_updates() {
        let svc = service();
        create(&svc, "policy", template(json!({"A": queue("policy-a")}))).unwrap();
        svc.set_stack_policy(
            &ctx(),
            SetStackPolicyInput {
                stack_name: "policy".into(),
                stack_policy_body: Some(
                    json!({"Statement": [{"Effect": "Deny", "Action": "Update:Replace",
                        "Resource": "LogicalResourceId/A"}]})
                    .to_string(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let renamed = template(json!({"A": queue("policy-a-renamed")}));
        let error = update(&svc, "policy", renamed).unwrap_err();
        assert!(
            error.to_string().contains("denied by stack policy"),
            "{error}"
        );
        assert_eq!(status(&svc, "policy"), "CREATE_COMPLETE");

        let delayed = template(json!({
            "A": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "policy-a", "DelaySeconds": 5}}
        }));
        update(&svc, "policy", delayed).unwrap();
        assert_eq!(status(&svc, "policy"), "UPDATE_COMPLETE");
    }

    #[test]
    fn failed_create_rolls_back_created_resources() {
        let svc = service();
        let body = template(json!({
            "A": queue("rollback-a"),
            "B": {"Type": "AWS::SQS::Queue", "DependsOn": "A", "Properties": {"QueueName": "bad name!"}},
        }));
        create(&svc, "rollback", body.clone()).unwrap();
        assert_eq!(status(&svc, "rollback"), "ROLLBACK_COMPLETE");
        assert!(resource_ids(&svc, "rollback").is_empty());

        svc.create_stack(
            &ctx(),
            CreateStackInput {
                stack_name: "kept".into(),
                template_body: Some(body),
                disable_rollback: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(status(&svc, "kept"), "CREATE_FAILED");
        // B never got a physical resource, so it is not listed.
        assert_eq!(resource_ids(&svc, "kept"), ["A"]);
    }

    #[test]
    fn failed_update_restores_the_previous_template() {
        let svc = service();
        let original = template(json!({"A": queue("restore-a")}));
        create(&svc, "restore", original.clone()).unwrap();
        let failing = template(json!({
            "A": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "restore-a", "DelaySeconds": 5}},
            "B": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "bad name!"}},
        }));
        update(&svc, "restore", failing).unwrap();
        assert_eq!(status(&svc, "restore"), "UPDATE_ROLLBACK_COMPLETE");
        assert_eq!(resource_ids(&svc, "restore"), ["A"]);
        let stored = svc
            .get_template(
                &ctx(),
                GetTemplateInput {
                    stack_name: Some("restore".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&stored.template_body.unwrap()).unwrap(),
            serde_json::from_str::<Value>(&original).unwrap()
        );
    }

    #[test]
    fn termination_protection_blocks_delete_until_disabled() {
        let svc = service();
        svc.create_stack(
            &ctx(),
            CreateStackInput {
                stack_name: "protected".into(),
                template_body: Some(template(json!({"A": queue("protected-a")}))),
                enable_termination_protection: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        let delete = |svc: &CloudFormation| {
            svc.delete_stack(
                &ctx(),
                DeleteStackInput {
                    stack_name: "protected".into(),
                    ..Default::default()
                },
            )
        };
        let error = delete(&svc).unwrap_err();
        assert!(
            error.to_string().contains("TerminationProtection"),
            "{error}"
        );
        svc.update_termination_protection(
            &ctx(),
            UpdateTerminationProtectionInput {
                stack_name: "protected".into(),
                enable_termination_protection: false,
            },
        )
        .unwrap();
        delete(&svc).unwrap();
        // Deleted stacks are no longer described.
        assert!(
            svc.describe_stacks(
                &ctx(),
                DescribeStacksInput {
                    stack_name: Some("protected".into()),
                    ..Default::default()
                },
            )
            .is_err()
        );
    }
}
