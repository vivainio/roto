//! Validate the supported template language and resolve resource dependencies.
use std::collections::{BTreeMap, BTreeSet};

use roto_core::{AwsError, RequestContext};
use serde_json::{Map, Value, json};

use crate::resources::{Resource, validate_properties};
use crate::validation;

pub fn parse(body: &str) -> Result<Value, AwsError> {
    let template: Value = serde_json::from_str(body)
        .or_else(|_| serde_yaml::from_str(body))
        .map_err(|e| validation(format!("Template format error: {e}")))?;
    let object = template
        .as_object()
        .ok_or_else(|| validation("Template must be an object"))?;
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "AWSTemplateFormatVersion"
                | "Description"
                | "Metadata"
                | "Parameters"
                | "Resources"
                | "Outputs"
        ) {
            return Err(validation(format!("Unsupported template section: {key}")));
        }
    }
    let resources = template["Resources"]
        .as_object()
        .ok_or_else(|| validation("Template requires a Resources object"))?;
    if let Some(parameters) = template.get("Parameters") {
        for (name, definition) in parameters
            .as_object()
            .ok_or_else(|| validation("Parameters must be an object"))?
        {
            let object = definition
                .as_object()
                .ok_or_else(|| validation("Parameter definition must be an object"))?;
            if definition["Type"] != "String" {
                return Err(validation(format!(
                    "Only String parameters are supported: {name}"
                )));
            }
            for key in object.keys() {
                if !matches!(key.as_str(), "Type" | "Default" | "Description") {
                    return Err(validation(format!(
                        "Unsupported parameter field {name}.{key}"
                    )));
                }
            }
        }
    }
    if let Some(outputs) = template.get("Outputs") {
        for (name, definition) in outputs
            .as_object()
            .ok_or_else(|| validation("Outputs must be an object"))?
        {
            let object = definition
                .as_object()
                .ok_or_else(|| validation("Output definition must be an object"))?;
            if !object.contains_key("Value") {
                return Err(validation(format!("Output {name} requires Value")));
            }
            for key in object.keys() {
                if !matches!(key.as_str(), "Value" | "Description" | "Export") {
                    return Err(validation(format!("Unsupported output field {name}.{key}")));
                }
            }
        }
    }
    for (id, resource) in resources {
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(validation(format!("Invalid logical resource ID: {id}")));
        }
        let ty = resource["Type"]
            .as_str()
            .ok_or_else(|| validation(format!("Resource {id} requires Type")))?;
        validate_properties(ty, resource.get("Properties").unwrap_or(&json!({})))?;
        let definition = resource
            .as_object()
            .ok_or_else(|| validation("Resource must be an object"))?;
        for key in definition.keys() {
            if !matches!(
                key.as_str(),
                "Type" | "Properties" | "DependsOn" | "Metadata"
            ) {
                return Err(validation(format!("Unsupported resource field {id}.{key}")));
            }
        }
    }
    validate_functions(&template)?;
    order(&template)?;
    Ok(template)
}

fn validate_functions(value: &Value) -> Result<(), AwsError> {
    match value {
        Value::Object(o) => {
            for (key, v) in o {
                if key.starts_with("Fn::") && !matches!(key.as_str(), "Fn::GetAtt" | "Fn::Sub") {
                    return Err(validation(format!("Unsupported intrinsic function: {key}")));
                }
                validate_functions(v)?;
            }
        }
        Value::Array(a) => {
            for v in a {
                validate_functions(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn dependencies(value: &Value, result: &mut BTreeSet<String>) {
    match value {
        Value::Object(o) => {
            if let Some(Value::String(id)) = o.get("Ref") {
                result.insert(id.clone());
            }
            if let Some(v) = o.get("Fn::GetAtt")
                && let Some(id) = v
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(Value::as_str)
                    .or_else(|| v.as_str().and_then(|s| s.split('.').next()))
            {
                result.insert(id.into());
            }
            if let Some(v) = o.get("Fn::Sub") {
                let (text, overrides) = match v {
                    Value::String(s) => (s.as_str(), None),
                    Value::Array(a) => (
                        a.first().and_then(Value::as_str).unwrap_or(""),
                        a.get(1).and_then(Value::as_object),
                    ),
                    _ => ("", None),
                };
                let mut rest = text;
                while let Some((_, tail)) = rest.split_once("${") {
                    let Some((key, tail)) = tail.split_once('}') else {
                        break;
                    };
                    if !key.starts_with('!') && !overrides.is_some_and(|m| m.contains_key(key)) {
                        result.insert(key.split('.').next().unwrap_or(key).into());
                    }
                    rest = tail;
                }
            }
            for v in o.values() {
                dependencies(v, result);
            }
        }
        Value::Array(a) => {
            for v in a {
                dependencies(v, result);
            }
        }
        _ => {}
    }
}

/// Topological ordering includes both explicit DependsOn and implicit references.
pub fn order(template: &Value) -> Result<Vec<String>, AwsError> {
    let resources = template["Resources"]
        .as_object()
        .ok_or_else(|| validation("Resources must be an object"))?;
    let parameters = template.get("Parameters").and_then(Value::as_object);
    let mut pending = BTreeMap::new();
    for (id, definition) in resources {
        let mut deps = BTreeSet::new();
        dependencies(&definition["Properties"], &mut deps);
        if let Some(explicit) = definition.get("DependsOn") {
            let mut explicit_deps = BTreeSet::new();
            match explicit {
                Value::String(s) => {
                    explicit_deps.insert(s.clone());
                }
                Value::Array(a) => {
                    for dep in a {
                        explicit_deps.insert(
                            dep.as_str()
                                .ok_or_else(|| validation("DependsOn must contain logical IDs"))?
                                .into(),
                        );
                    }
                }
                _ => return Err(validation("DependsOn must be a string or list")),
            }
            for dep in explicit_deps {
                if !resources.contains_key(&dep) {
                    return Err(validation(format!(
                        "DependsOn requires a resource ID: {dep}"
                    )));
                }
                deps.insert(dep);
            }
        }
        for dep in &deps {
            if !resources.contains_key(dep)
                && !parameters.is_some_and(|p| p.contains_key(dep))
                && !pseudo(dep)
            {
                return Err(validation(format!(
                    "Unresolved dependency {dep} in resource {id}"
                )));
            }
        }
        deps.retain(|d| resources.contains_key(d));
        pending.insert(id.clone(), deps);
    }
    let mut ordered = Vec::new();
    while !pending.is_empty() {
        let ready: Vec<_> = pending
            .iter()
            .filter(|(_, deps)| deps.is_empty())
            .map(|(id, _)| id.clone())
            .collect();
        if ready.is_empty() {
            return Err(validation("Circular resource dependency"));
        }
        for id in ready {
            pending.remove(&id);
            for deps in pending.values_mut() {
                deps.remove(&id);
            }
            ordered.push(id);
        }
    }
    Ok(ordered)
}

fn pseudo(id: &str) -> bool {
    matches!(
        id,
        "AWS::AccountId"
            | "AWS::Region"
            | "AWS::StackName"
            | "AWS::StackId"
            | "AWS::Partition"
            | "AWS::URLSuffix"
    )
}

pub fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

pub struct Resolver<'a> {
    pub ctx: &'a RequestContext,
    pub stack_name: &'a str,
    pub stack_id: &'a str,
    pub parameters: &'a BTreeMap<String, String>,
    pub resources: &'a [Resource],
}

impl Resolver<'_> {
    fn reference(&self, id: &str) -> Result<Value, AwsError> {
        let value = match id {
            "AWS::AccountId" => &self.ctx.account_id,
            "AWS::Region" => &self.ctx.region,
            "AWS::StackName" => self.stack_name,
            "AWS::StackId" => self.stack_id,
            "AWS::Partition" => partition(&self.ctx.region),
            "AWS::URLSuffix" => {
                if self.ctx.region.starts_with("cn-") {
                    "amazonaws.com.cn"
                } else {
                    "amazonaws.com"
                }
            }
            _ => self
                .parameters
                .get(id)
                .map(String::as_str)
                .or_else(|| {
                    self.resources
                        .iter()
                        .find(|r| r.logical_id == id)
                        .map(|r| r.physical_id.as_str())
                })
                .ok_or_else(|| validation(format!("Unresolved Ref: {id}")))?,
        };
        Ok(json!(value))
    }

    fn attribute(&self, id: &str, attr: &str) -> Result<Value, AwsError> {
        self.resources
            .iter()
            .find(|r| r.logical_id == id)
            .and_then(|r| r.attributes.get(attr))
            .cloned()
            .ok_or_else(|| validation(format!("Unresolved attribute: {id}.{attr}")))
    }

    pub fn resolve(&self, value: &Value) -> Result<Value, AwsError> {
        match value {
            Value::Object(o) if o.contains_key("Ref") => self.reference(
                o["Ref"]
                    .as_str()
                    .ok_or_else(|| validation("Ref requires a string"))?,
            ),
            Value::Object(o) if o.contains_key("Fn::GetAtt") => {
                let (id, attr) = match &o["Fn::GetAtt"] {
                    Value::String(s) => s
                        .split_once('.')
                        .ok_or_else(|| validation("GetAtt requires resource.attribute"))?,
                    Value::Array(a) if a.len() == 2 => (
                        a[0].as_str()
                            .ok_or_else(|| validation("GetAtt requires resource ID"))?,
                        a[1].as_str()
                            .ok_or_else(|| validation("GetAtt requires attribute name"))?,
                    ),
                    _ => return Err(validation("GetAtt requires resource and attribute")),
                };
                self.attribute(id, attr)
            }
            Value::Object(o) if o.contains_key("Fn::Sub") => {
                let (text, overrides) = match &o["Fn::Sub"] {
                    Value::String(s) => (s.as_str(), Map::new()),
                    Value::Array(a) if a.len() == 2 => (
                        a[0].as_str()
                            .ok_or_else(|| validation("Sub requires a string"))?,
                        self.resolve(&a[1])?
                            .as_object()
                            .cloned()
                            .ok_or_else(|| validation("Sub variables must be an object"))?,
                    ),
                    _ => return Err(validation("Sub requires a string or [string, variables]")),
                };
                let mut rest = text;
                let mut output = String::new();
                while let Some((head, tail)) = rest.split_once("${") {
                    output.push_str(head);
                    let (key, tail) = tail
                        .split_once('}')
                        .ok_or_else(|| validation("Unterminated Sub variable"))?;
                    if let Some(literal) = key.strip_prefix('!') {
                        output.push_str(&format!("${{{literal}}}"));
                    } else {
                        let v = if let Some(v) = overrides.get(key) {
                            v.clone()
                        } else if let Some((id, attr)) = key.split_once('.') {
                            self.attribute(id, attr)?
                        } else {
                            self.reference(key)?
                        };
                        output.push_str(&crate::template::text(&v)?);
                    }
                    rest = tail;
                }
                output.push_str(rest);
                Ok(json!(output))
            }
            Value::Object(o) => Ok(Value::Object(
                o.iter()
                    .map(|(k, v)| Ok((k.clone(), self.resolve(v)?)))
                    .collect::<Result<_, AwsError>>()?,
            )),
            Value::Array(a) => Ok(Value::Array(
                a.iter()
                    .map(|v| self.resolve(v))
                    .collect::<Result<_, _>>()?,
            )),
            _ => Ok(value.clone()),
        }
    }
}

pub fn text(v: &Value) -> Result<String, AwsError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(_) | Value::Bool(_) => Ok(v.to_string()),
        _ => Err(validation("Expected a scalar value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_overrides_do_not_create_false_dependencies() {
        let template = parse(r#"{"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Fn::Sub":["${Queue}",{"Queue":"literal-name"}]}}}}}"#).unwrap();
        assert_eq!(order(&template).unwrap(), ["Queue"]);
    }

    #[test]
    fn explicit_and_implicit_dependencies_are_ordered() {
        let template = parse(r#"{"Resources":{
            "A":{"Type":"AWS::SNS::Topic","Properties":{"Subscription":[{"Protocol":"sqs","Endpoint":{"Fn::Sub":"${C.Arn}"}}]}},
            "B":{"Type":"AWS::SQS::Queue"},
            "C":{"Type":"AWS::SQS::Queue","DependsOn":"B"}
        }}"#).unwrap();
        assert_eq!(order(&template).unwrap(), ["B", "C", "A"]);
    }

    #[test]
    fn circular_unknown_and_non_resource_dependencies_are_rejected() {
        for source in [
            r#"{"Resources":{"A":{"Type":"AWS::SQS::Queue","DependsOn":"B"},"B":{"Type":"AWS::SQS::Queue","DependsOn":"A"}}}"#,
            r#"{"Resources":{"A":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Ref":"Missing"}}}}}"#,
            r#"{"Parameters":{"Label":{"Type":"String"}},"Resources":{"A":{"Type":"AWS::SQS::Queue","DependsOn":"Label"}}}"#,
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn unsupported_features_fail_validation() {
        for source in [
            r#"{"Resources":{"A":{"Type":"AWS::SQS::Queue","DeletionPolicy":"Retain"}}}"#,
            r#"{"Resources":{"A":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Fn::Join":["",[]]}}}}}"#,
            r#"{"Parameters":{"Secret":{"Type":"String","NoEcho":true}},"Resources":{}}"#,
            r#"{"Resources":{"A":{"Type":"AWS::S3::Bucket","Properties":{"WebsiteConfiguration":{}}}}}"#,
            r#"{"Resources":{},"Outputs":{"A":{"Value":"example","Condition":"unsupported"}}}"#,
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn substitution_escape_variables_and_missing_attributes() {
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost".into(),
        };
        let params = BTreeMap::from([("Label".into(), "hello<&".into())]);
        let resolver = Resolver {
            ctx: &ctx,
            stack_name: "stack",
            stack_id: "id",
            parameters: &params,
            resources: &[],
        };
        assert_eq!(resolver.resolve(&json!({"Fn::Sub":["${AWS::Region}:${Label}:${custom}:${!literal}",{"custom":12}]})).unwrap(), "us-east-1:hello<&:12:${literal}");
        assert!(
            resolver
                .resolve(&json!({"Fn::GetAtt":["Missing","Arn"]}))
                .is_err()
        );
        assert!(
            resolver
                .resolve(&json!({"Fn::Sub":"${unterminated"}))
                .is_err()
        );
    }
}
