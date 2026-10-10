//! Validate the supported template language and resolve resource dependencies.
use std::collections::{BTreeMap, BTreeSet};

use roto_core::{AwsError, RequestContext};
use serde_json::{Map, Value, json};

use crate::resources::{Resource, validate_properties};
use crate::validation;

fn yaml_functions(value: serde_yaml::Value) -> Result<serde_yaml::Value, serde_yaml::Error> {
    use serde_yaml::Value as Y;
    Ok(match value {
        Y::Tagged(tagged) => {
            let tag = tagged.tag.to_string();
            let key = match tag.as_str() {
                "!Ref" => "Ref",
                "!GetAtt" => "Fn::GetAtt",
                "!Sub" => "Fn::Sub",
                _ => {
                    return Err(serde::de::Error::custom(format!(
                        "Unsupported YAML tag: {tag}"
                    )));
                }
            };
            Y::Mapping(
                [(Y::String(key.into()), yaml_functions(tagged.value)?)]
                    .into_iter()
                    .collect(),
            )
        }
        Y::Sequence(values) => Y::Sequence(
            values
                .into_iter()
                .map(yaml_functions)
                .collect::<Result<_, _>>()?,
        ),
        Y::Mapping(values) => Y::Mapping(
            values
                .into_iter()
                .map(|(k, v)| Ok((k, yaml_functions(v)?)))
                .collect::<Result<_, serde_yaml::Error>>()?,
        ),
        other => other,
    })
}

pub fn parse(body: &str) -> Result<Value, AwsError> {
    let template: Value = serde_json::from_str(body)
        .or_else(|_| {
            serde_yaml::from_str::<serde_yaml::Value>(body).and_then(|v| {
                serde_json::to_value(yaml_functions(v)?).map_err(serde::de::Error::custom)
            })
        })
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
                | "Conditions"
                | "Resources"
                | "Outputs"
        ) {
            return Err(validation(format!("Unsupported template section: {key}")));
        }
    }
    let resources = template["Resources"]
        .as_object()
        .ok_or_else(|| validation("Template requires a Resources object"))?;
    if let Some(conditions) = template.get("Conditions") {
        conditions
            .as_object()
            .ok_or_else(|| validation("Conditions must be an object"))?;
    }
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
                "Type"
                    | "Properties"
                    | "DependsOn"
                    | "Metadata"
                    | "DeletionPolicy"
                    | "UpdateReplacePolicy"
            ) {
                return Err(validation(format!("Unsupported resource field {id}.{key}")));
            }
        }
        for key in ["DeletionPolicy", "UpdateReplacePolicy"] {
            if let Some(policy) = definition.get(key) {
                match policy.as_str() {
                    Some("Delete" | "Retain") => {}
                    Some(value) => {
                        return Err(validation(format!(
                            "Unsupported {key} for resource {id}: {value}"
                        )));
                    }
                    None => {
                        return Err(validation(format!(
                            "{key} for resource {id} must be a string"
                        )));
                    }
                }
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
                if key.starts_with("Fn::")
                    && !matches!(
                        key.as_str(),
                        "Fn::GetAtt"
                            | "Fn::Sub"
                            | "Fn::Join"
                            | "Fn::If"
                            | "Fn::Equals"
                            | "Fn::And"
                            | "Fn::Or"
                            | "Fn::Not"
                    )
                {
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
            | "AWS::NoValue"
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
    /// The template's `Conditions` object, or `Value::Null` when absent.
    pub conditions: &'a Value,
}

impl Resolver<'_> {
    fn reference(&self, id: &str) -> Result<Value, AwsError> {
        let value = match id {
            "AWS::NoValue" => {
                return Err(validation(
                    "AWS::NoValue is only supported as a property or list item",
                ));
            }
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

    /// Follow `Fn::If` chains to the branch selected by the conditions.
    fn select<'v>(&self, mut value: &'v Value) -> Result<&'v Value, AwsError> {
        while let Some(args) = value.get("Fn::If") {
            let args = args
                .as_array()
                .filter(|args| args.len() == 3)
                .ok_or_else(|| validation("Fn::If requires [condition, true, false]"))?;
            let name = args[0]
                .as_str()
                .ok_or_else(|| validation("Fn::If requires a condition name"))?;
            value = if self.condition(name)? {
                &args[1]
            } else {
                &args[2]
            };
        }
        Ok(value)
    }

    /// Like `select`, but `None` when the value is `AWS::NoValue`, meaning the
    /// enclosing property or list item is omitted.
    fn present<'v>(&self, value: &'v Value) -> Result<Option<&'v Value>, AwsError> {
        let value = self.select(value)?;
        let no_value = value.get("Ref").and_then(Value::as_str) == Some("AWS::NoValue");
        Ok((!no_value).then_some(value))
    }

    fn condition(&self, name: &str) -> Result<bool, AwsError> {
        let definition = self
            .conditions
            .get(name)
            .ok_or_else(|| validation(format!("Unknown condition: {name}")))?;
        self.evaluate(definition)
    }

    fn evaluate(&self, expression: &Value) -> Result<bool, AwsError> {
        let (function, args) = expression
            .as_object()
            .filter(|o| o.len() == 1)
            .and_then(|o| o.iter().next())
            .ok_or_else(|| validation("A condition must contain exactly one function"))?;
        match function.as_str() {
            "Condition" => self.condition(
                args.as_str()
                    .ok_or_else(|| validation("Condition reference must be a name"))?,
            ),
            "Fn::Equals" => {
                let args = args
                    .as_array()
                    .filter(|args| args.len() == 2)
                    .ok_or_else(|| validation("Fn::Equals requires two values"))?;
                Ok(self.resolve(&args[0])? == self.resolve(&args[1])?)
            }
            "Fn::Not" => {
                let args = args
                    .as_array()
                    .filter(|args| args.len() == 1)
                    .ok_or_else(|| validation("Fn::Not requires one condition"))?;
                Ok(!self.evaluate(&args[0])?)
            }
            "Fn::And" | "Fn::Or" => {
                let args = args
                    .as_array()
                    .filter(|args| !args.is_empty())
                    .ok_or_else(|| validation(format!("{function} requires conditions")))?;
                let and = function == "Fn::And";
                for arg in args {
                    if self.evaluate(arg)? != and {
                        return Ok(!and);
                    }
                }
                Ok(and)
            }
            _ => Err(validation(format!(
                "Unsupported condition function: {function}"
            ))),
        }
    }

    pub fn resolve(&self, value: &Value) -> Result<Value, AwsError> {
        let value = self.select(value)?;
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
            Value::Object(o) if o.contains_key("Fn::Join") => {
                let args = o["Fn::Join"]
                    .as_array()
                    .filter(|args| args.len() == 2)
                    .ok_or_else(|| validation("Join requires [delimiter, values]"))?;
                let delimiter = text(&self.resolve(&args[0])?)?;
                let values = args[1]
                    .as_array()
                    .ok_or_else(|| validation("Join values must be a list"))?;
                let values = values
                    .iter()
                    .map(|value| self.resolve(value).and_then(|v| text(&v)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!(values.join(&delimiter)))
            }
            Value::Object(o) => {
                let mut object = Map::new();
                for (k, v) in o {
                    if let Some(v) = self.present(v)? {
                        object.insert(k.clone(), self.resolve(v)?);
                    }
                }
                Ok(Value::Object(object))
            }
            Value::Array(a) => {
                let mut array = Vec::new();
                for v in a {
                    if let Some(v) = self.present(v)? {
                        array.push(self.resolve(v)?);
                    }
                }
                Ok(Value::Array(array))
            }
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
    fn yaml_intrinsic_short_tags_match_json_and_unknown_tags_fail() {
        let body = "Resources:\n  Stream:\n    Type: AWS::Kinesis::Stream\nOutputs:\n  Name:\n    Value: !Ref Stream\n  Arn:\n    Value: !GetAtt Stream.Arn\n  Label:\n    Value: !Sub '${AWS::Region}'\n";
        let parsed = parse(body).unwrap();
        assert_eq!(parsed["Outputs"]["Name"]["Value"], json!({"Ref":"Stream"}));
        assert_eq!(
            parsed["Outputs"]["Arn"]["Value"],
            json!({"Fn::GetAtt":"Stream.Arn"})
        );
        assert_eq!(
            parsed["Outputs"]["Label"]["Value"],
            json!({"Fn::Sub":"${AWS::Region}"})
        );
        assert!(parse(&body.replace("!Ref", "!Unsupported")).is_err());
    }

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
            r#"{"Resources":{"A":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Fn::Select":[0,["a"]]}}}}}"#,
            r#"{"Parameters":{"Secret":{"Type":"String","NoEcho":true}},"Resources":{}}"#,
            r#"{"Resources":{"A":{"Type":"AWS::S3::Bucket","Properties":{"WebsiteConfiguration":{}}}}}"#,
            r#"{"Resources":{},"Outputs":{"A":{"Value":"example","Condition":"unsupported"}}}"#,
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn deletion_and_update_replace_policies_accept_delete_and_retain() {
        for key in ["DeletionPolicy", "UpdateReplacePolicy"] {
            let source = format!(
                r#"{{"Resources":{{"Queue":{{"Type":"AWS::SQS::Queue","{key}":"Retain"}}}}}}"#
            );
            assert!(parse(&source).is_ok());
        }
        let unsupported =
            r#"{"Resources":{"Queue":{"Type":"AWS::SQS::Queue","DeletionPolicy":"Snapshot"}}}"#;
        assert!(parse(unsupported).is_err());
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
            conditions: &Value::Null,
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

    #[test]
    fn conditions_select_branches_and_omit_no_value() {
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost".into(),
        };
        let conditions = json!({
            "IsUsEast": {"Fn::Equals": [{"Ref": "AWS::Region"}, "us-east-1"]},
            "IsCn": {"Fn::Equals": [{"Ref": "AWS::Region"}, "cn-north-1"]},
            "Either": {"Fn::Or": [{"Condition": "IsCn"}, {"Condition": "IsUsEast"}]},
            "Both": {"Fn::And": [{"Condition": "IsCn"}, {"Fn::Not": [{"Condition": "IsUsEast"}]}]}
        });
        let resolver = Resolver {
            ctx: &ctx,
            stack_name: "stack",
            stack_id: "id",
            parameters: &BTreeMap::new(),
            resources: &[],
            conditions: &conditions,
        };
        assert_eq!(
            resolver
                .resolve(&json!({"Fn::If": ["IsUsEast", "yes", "no"]}))
                .unwrap(),
            "yes"
        );
        assert_eq!(
            resolver
                .resolve(&json!({"Fn::If": ["Both", "yes", "no"]}))
                .unwrap(),
            "no"
        );
        assert_eq!(
            resolver
                .resolve(&json!({"Fn::If": ["Either", {"Ref": "AWS::Region"}, "no"]}))
                .unwrap(),
            "us-east-1"
        );
        assert_eq!(
            resolver
                .resolve(&json!({
                    "Name": {"Fn::If": ["Both", "x", {"Ref": "AWS::NoValue"}]},
                    "Other": ["a", {"Fn::If": ["Both", "b", {"Ref": "AWS::NoValue"}]}]
                }))
                .unwrap(),
            json!({"Other": ["a"]})
        );
        assert!(resolver.resolve(&json!({"Ref": "AWS::NoValue"})).is_err());
        assert!(
            resolver
                .resolve(&json!({"Fn::If": ["Missing", "a", "b"]}))
                .is_err()
        );
    }

    #[test]
    fn join_resolves_scalars_and_reports_invalid_shapes() {
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost".into(),
        };
        let resolver = Resolver {
            ctx: &ctx,
            stack_name: "stack",
            stack_id: "id",
            parameters: &BTreeMap::new(),
            resources: &[],
            conditions: &Value::Null,
        };
        assert_eq!(
            resolver
                .resolve(&json!({"Fn::Join": ["", ["arn:", {"Ref": "AWS::Partition"}, ":iam::aws:policy/Example"]]}))
                .unwrap(),
            "arn:aws:iam::aws:policy/Example"
        );
        assert!(resolver.resolve(&json!({"Fn::Join": ["-"]})).is_err());
    }
}
