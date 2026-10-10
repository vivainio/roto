//! Change sets, stack policies and termination protection.
//!
//! A change set stores the target template and the changes planned against the
//! stack when it was created. Executing it runs the same reconcile path as
//! `UpdateStack` (or the create path for a stack still in `REVIEW_IN_PROGRESS`).
use std::collections::BTreeMap;

use super::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct ChangeSetState {
    pub name: String,
    pub id: String,
    pub description: Option<String>,
    pub kind: String,
    pub created: i64,
    pub status: String,
    pub reason: Option<String>,
    pub execution: String,
    pub template: Value,
    pub parameters: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub changes: Vec<PlannedChange>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PlannedChange {
    pub action: String,
    pub logical_id: String,
    pub resource_type: String,
    pub physical_id: Option<String>,
    /// Only meaningful for `Modify`; `None` means the planner could not tell.
    pub replacement: Option<bool>,
}

/// Compare the resources a target template would produce with the current stack.
pub fn plan(
    ctx: &RequestContext,
    current: &StackState,
    target: &StackState,
    exports: &BTreeMap<String, String>,
) -> Result<Vec<PlannedChange>, AwsError> {
    let order = template::order(&target.template)?;
    let mut changes = Vec::new();
    for id in &order {
        let definition = &target.template["Resources"][id];
        let ty = definition["Type"].as_str().unwrap_or_default().to_string();
        match current.resources.iter().find(|r| &r.logical_id == id) {
            None => changes.push(PlannedChange {
                action: "Add".into(),
                logical_id: id.clone(),
                resource_type: ty,
                physical_id: None,
                replacement: None,
            }),
            Some(old) => {
                // Properties that reference resources created by this update cannot
                // be resolved during planning; those are reported as unknown changes.
                let replaced = match desired_properties(target, ctx, definition, exports) {
                    Ok(props) if ty == old.resource_type && props == old.properties => continue,
                    Ok(props) => Some(replacement(old, &ty, &props)),
                    Err(_) => None,
                };
                changes.push(PlannedChange {
                    action: "Modify".into(),
                    logical_id: id.clone(),
                    resource_type: ty,
                    physical_id: Some(old.physical_id.clone()),
                    replacement: replaced,
                });
            }
        }
    }
    for old in &current.resources {
        if !order.contains(&old.logical_id) {
            changes.push(PlannedChange {
                action: "Remove".into(),
                logical_id: old.logical_id.clone(),
                resource_type: old.resource_type.clone(),
                physical_id: Some(old.physical_id.clone()),
                replacement: None,
            });
        }
    }
    Ok(changes)
}

/// Parse a stack policy and return its statements.
pub fn validate_policy(body: &str) -> Result<Vec<Value>, AwsError> {
    let value: Value = serde_json::from_str(body)
        .map_err(|e| validation(format!("Invalid StackPolicyBody: {e}")))?;
    let statements = value["Statement"]
        .as_array()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation("StackPolicyBody requires a non-empty Statement list"))?;
    for statement in statements {
        if !matches!(statement["Effect"].as_str(), Some("Allow" | "Deny"))
            || statement.get("Action").is_none()
            || statement.get("Resource").is_none()
        {
            return Err(validation(
                "Each policy statement needs Effect (Allow or Deny), Action and Resource",
            ));
        }
    }
    Ok(statements.clone())
}

/// Reject a plan if a `Deny` statement covers any change it contains. Updates
/// are otherwise allowed, as in CloudFormation.
pub fn check_policy(policy: Option<&str>, changes: &[PlannedChange]) -> Result<(), AwsError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let statements = validate_policy(policy)?;
    for change in changes {
        let action = match (change.action.as_str(), change.replacement) {
            ("Add", _) => continue,
            ("Remove", _) => "Update:Delete",
            (_, Some(true)) => "Update:Replace",
            _ => "Update:Modify",
        };
        let resource = format!("LogicalResourceId/{}", change.logical_id);
        for statement in &statements {
            if statement["Effect"] == "Deny"
                && pattern_matches(&statement["Action"], action)
                && pattern_matches(&statement["Resource"], &resource)
            {
                return Err(validation(format!(
                    "Action denied by stack policy: {action} on {resource}"
                )));
            }
        }
    }
    Ok(())
}

/// Match a string or list of strings, where a trailing `*` is a prefix wildcard.
fn pattern_matches(patterns: &Value, text: &str) -> bool {
    let matches = |pattern: &str| match pattern.strip_suffix('*') {
        Some(prefix) => text.starts_with(prefix),
        None => pattern == text,
    };
    match patterns {
        Value::String(pattern) => matches(pattern),
        Value::Array(list) => list.iter().filter_map(Value::as_str).any(matches),
        _ => false,
    }
}

pub(crate) fn change_output(change: &PlannedChange) -> Change {
    let replacement = match (change.action.as_str(), change.replacement) {
        ("Modify", None) => Some("Conditional"),
        (_, Some(true)) => Some("True"),
        (_, Some(false)) => Some("False"),
        _ => None,
    };
    Change {
        r#type: Some("Resource".into()),
        resource_change: Some(ResourceChange {
            action: Some(change.action.clone()),
            logical_resource_id: Some(change.logical_id.clone()),
            physical_resource_id: change.physical_id.clone(),
            resource_type: Some(change.resource_type.clone()),
            replacement: replacement.map(str::to_string),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(crate) fn parameter_output(parameters: &BTreeMap<String, String>) -> Vec<Parameter> {
    parameters
        .iter()
        .map(|(k, v)| Parameter {
            parameter_key: Some(k.clone()),
            parameter_value: Some(v.clone()),
            ..Default::default()
        })
        .collect()
}

pub(crate) fn tag_output(tags: &BTreeMap<String, String>) -> Vec<Tag> {
    tags.iter()
        .map(|(k, v)| Tag {
            key: k.clone(),
            value: v.clone(),
        })
        .collect()
}

impl CloudFormation {
    /// Find a change set by name or ARN, in one stack or across all stacks.
    pub(crate) fn locate_change_set(
        &self,
        ctx: &RequestContext,
        stack_name: Option<&str>,
        name_or_id: &str,
    ) -> Result<(StackState, usize), AwsError> {
        let stacks = match stack_name {
            Some(name) => vec![self.load(ctx, name)?],
            None => self.all(ctx)?,
        };
        stacks
            .into_iter()
            .find_map(|stack| {
                let index = stack
                    .change_sets
                    .iter()
                    .position(|c| c.name == name_or_id || c.id == name_or_id)?;
                Some((stack, index))
            })
            .ok_or_else(|| validation(format!("ChangeSet [{name_or_id}] does not exist")))
    }
}
