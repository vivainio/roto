//! Response-model validation for Step Functions TestState mock results.
use serde_json::Value;

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Bound the cache because models supplied by service bundles can contain arbitrary patterns.
fn matches_pattern(pattern: &str, value: &str) -> Result<bool, String> {
    static CACHE: OnceLock<Mutex<HashMap<String, Result<regex::Regex, String>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(compiled) = cache.lock().unwrap().get(pattern).cloned() {
        return compiled.map(|re| re.is_match(value));
    }
    let compiled = regex::Regex::new(pattern).map_err(|_| "unsupported model pattern".to_owned());
    let mut cache = cache.lock().unwrap();
    if cache.len() < 256 {
        cache.insert(pattern.to_owned(), compiled.clone());
    }
    drop(cache);
    compiled.map(|re| re.is_match(value))
}

/// Validate an operation result using the complete vendored model, not the pruned editor model.
pub(crate) fn validate_operation(
    service: &str,
    operation: &str,
    value: &Value,
    strict: bool,
) -> Result<(), String> {
    static MODELS: OnceLock<HashMap<&'static str, Value>> = OnceLock::new();
    let models = MODELS.get_or_init(|| {
        [
            (
                "cloudformation",
                include_str!(concat!(env!("OUT_DIR"), "/cloudformation-mock.json")),
            ),
            (
                "dynamodb",
                include_str!(concat!(env!("OUT_DIR"), "/dynamodb-mock.json")),
            ),
            (
                "ecs",
                include_str!("../../../models/ecs/response-model.json"),
            ),
            (
                "events",
                include_str!(concat!(env!("OUT_DIR"), "/eventbridge-mock.json")),
            ),
            (
                "iam",
                include_str!(concat!(env!("OUT_DIR"), "/iam-mock.json")),
            ),
            (
                "kinesis",
                include_str!(concat!(env!("OUT_DIR"), "/kinesis-mock.json")),
            ),
            (
                "kms",
                include_str!(concat!(env!("OUT_DIR"), "/kms-mock.json")),
            ),
            (
                "lambda",
                include_str!(concat!(env!("OUT_DIR"), "/lambda-mock.json")),
            ),
            (
                "s3",
                include_str!(concat!(env!("OUT_DIR"), "/s3-mock.json")),
            ),
            (
                "secretsmanager",
                include_str!(concat!(env!("OUT_DIR"), "/secretsmanager-mock.json")),
            ),
            (
                "sns",
                include_str!(concat!(env!("OUT_DIR"), "/sns-mock.json")),
            ),
            (
                "sqs",
                include_str!(concat!(env!("OUT_DIR"), "/sqs-mock.json")),
            ),
            (
                "ssm",
                include_str!(concat!(env!("OUT_DIR"), "/ssm-mock.json")),
            ),
            (
                "sts",
                include_str!(concat!(env!("OUT_DIR"), "/sts-mock.json")),
            ),
            (
                "states",
                include_str!(concat!(env!("OUT_DIR"), "/states-mock.json")),
            ),
            (
                "scheduler",
                include_str!(concat!(env!("OUT_DIR"), "/scheduler-mock.json")),
            ),
            (
                "logs",
                include_str!(concat!(env!("OUT_DIR"), "/logs-mock.json")),
            ),
        ]
        .into_iter()
        .map(|(name, text)| (name, serde_json::from_str(text).expect("vendored model")))
        .collect()
    });
    let model = models
        .get(service)
        .ok_or_else(|| format!("No response model available for {service}"))?;
    validate_model_operation(model, service, operation, value, strict)
}

/// Validate an operation against a model shipped by a Luau service bundle.
pub(crate) fn validate_model_operation(
    model: &Value,
    service: &str,
    operation: &str,
    value: &Value,
    strict: bool,
) -> Result<(), String> {
    let sync2 = service == "states" && operation == "DescribeExecutionSync2";
    let operation = if sync2 {
        "DescribeExecution"
    } else {
        operation
    };
    let operation_model = model["operations"]
        .get(operation)
        .ok_or_else(|| format!("No response model for {service}.{operation}"))?;
    let output = &operation_model["output"];
    if output.is_null() {
        return if value.as_object().is_some_and(|o| o.is_empty()) {
            Ok(())
        } else {
            Err("$: expected empty response object".into())
        };
    }
    // Optimized Lambda Payload and nested execution .sync:2 Output are decoded JSON,
    // although the underlying API models describe them as a blob and string respectively.
    if ((service == "lambda" && operation == "Invoke") || sync2) && value.is_object() {
        let mut shaped = value.clone();
        shaped
            .as_object_mut()
            .unwrap()
            .remove(if sync2 { "Output" } else { "Payload" });
        validate(model, output, &shaped, strict)
    } else {
        validate(model, output, value, strict)
    }
}

/// Validate modeled fields, leaving unknown fields alone as TestState does. `strict` additionally
/// requires fields the model marks required. Paths in errors identify nested invalid values.
pub(crate) fn validate(
    model: &Value,
    reference: &Value,
    value: &Value,
    strict: bool,
) -> Result<(), String> {
    fn visit(
        model: &Value,
        reference: &Value,
        value: &Value,
        strict: bool,
        path: &str,
        depth: usize,
    ) -> Result<(), String> {
        if depth > 128 {
            return Err(format!("{path}: shape nesting exceeds 128 levels"));
        }
        let shape = &model["shapes"][reference["shape"].as_str().unwrap_or("")];
        let kind = shape["type"]
            .as_str()
            .ok_or_else(|| format!("{path}: unknown model shape"))?;
        let valid = match kind {
            "structure" | "map" => value.is_object(),
            "list" => value.is_array(),
            "string" | "blob" => value.is_string(),
            "boolean" => value.is_boolean(),
            "integer" => value.as_i64().is_some_and(|n| i32::try_from(n).is_ok()),
            "long" => value.as_i64().is_some(),
            "float" | "double" => value.is_number(),
            "timestamp" => value.is_number() || value.is_string(),
            "document" => true,
            _ => return Err(format!("{path}: unsupported model type {kind}")),
        };
        if !valid {
            return Err(format!("{path}: expected {kind}"));
        }
        if let Some(values) = shape["enum"].as_array()
            && !values.contains(value)
        {
            return Err(format!("{path}: value is not in the modeled enum"));
        }
        if let (Some(text), Some(pattern)) = (value.as_str(), shape["pattern"].as_str()) {
            match matches_pattern(pattern, text) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(format!("{path}: value does not match the modeled pattern"));
                }
                Err(error) => return Err(format!("{path}: {error}; use fieldValidationMode NONE")),
            }
        }
        if let Some(object) = value.as_object() {
            if kind == "structure" {
                if strict && let Some(required) = shape["required"].as_array() {
                    for name in required.iter().filter_map(Value::as_str) {
                        if !object.contains_key(name) {
                            return Err(format!("{path}.{name}: required field is missing"));
                        }
                    }
                }
                if let Some(members) = shape["members"].as_object() {
                    for (name, item) in object {
                        if let Some(member) = members.get(name) {
                            visit(
                                model,
                                member,
                                item,
                                strict,
                                &format!("{path}.{name}"),
                                depth + 1,
                            )?;
                        }
                    }
                }
            } else if kind == "map" {
                for (name, item) in object {
                    visit(
                        model,
                        &shape["key"],
                        &Value::String(name.clone()),
                        strict,
                        &format!("{path} key"),
                        depth + 1,
                    )?;
                    visit(
                        model,
                        &shape["value"],
                        item,
                        strict,
                        &format!("{path}[{name:?}]"),
                        depth + 1,
                    )?;
                }
            }
        }
        if let Some(items) = value.as_array()
            && kind == "list"
        {
            for (index, item) in items.iter().enumerate() {
                visit(
                    model,
                    &shape["member"],
                    item,
                    strict,
                    &format!("{path}[{index}]"),
                    depth + 1,
                )?;
            }
        }
        let length = match value {
            Value::String(s) => Some(s.chars().count()),
            Value::Array(v) => Some(v.len()),
            Value::Object(v) if kind == "map" => Some(v.len()),
            _ => None,
        };
        if let Some(length) = length {
            if shape["min"]
                .as_u64()
                .is_some_and(|min| (length as u64) < min)
            {
                return Err(format!("{path}: length is below model minimum"));
            }
            if shape["max"]
                .as_u64()
                .is_some_and(|max| (length as u64) > max)
            {
                return Err(format!("{path}: length exceeds model maximum"));
            }
        }
        if let Some(number) = value.as_f64() {
            if shape["min"].as_f64().is_some_and(|min| number < min) {
                return Err(format!("{path}: value is below model minimum"));
            }
            if shape["max"].as_f64().is_some_and(|max| number > max) {
                return Err(format!("{path}: value exceeds model maximum"));
            }
        }
        Ok(())
    }
    visit(model, reference, value, strict, "$", 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn validates_nested_shapes_and_required_fields_by_mode() {
        let model = json!({"shapes": {
            "Output": {"type":"structure", "required":["Items"], "members":{"Items":{"shape":"Items"}}},
            "Items": {"type":"list", "min":1, "member":{"shape":"Item"}},
            "Item": {"type":"structure", "required":["Name"], "members":{"Name":{"shape":"Name"}}},
            "Name": {"type":"string", "min":2}
        }});
        let root = json!({"shape":"Output"});
        assert!(validate(&model, &root, &json!({}), true).is_err());
        assert!(validate(&model, &root, &json!({}), false).is_ok());
        assert!(validate(&model, &root, &json!({"Items":[{}]}), true).is_err());
        assert!(validate(&model, &root, &json!({"Items":[{}]}), false).is_ok());
        assert!(validate(&model, &root, &json!({"Items":[{"Name":3}]}), false).is_err());
        assert!(validate(&model, &root, &json!({"Items":[{"Name":"x"}]}), false).is_err());
        assert!(
            validate(
                &model,
                &root,
                &json!({"Items":[{"Name":"ok", "extra":3}], "unknown":true}),
                true
            )
            .is_ok()
        );
    }

    #[test]
    fn validates_patterns_without_implicitly_anchoring() {
        let model = json!({"shapes": {
            "Value": {"type":"string", "pattern":"[a-z]+"},
            "Anchored": {"type":"string", "pattern":"^[a-z]+$"},
            "Unsupported": {"type":"string", "pattern":"^(?!x).+"}
        }});
        assert!(validate(&model, &json!({"shape":"Value"}), &json!("!abc!"), true).is_ok());
        assert!(validate(&model, &json!({"shape":"Value"}), &json!("!!!"), false).is_err());
        assert!(validate(&model, &json!({"shape":"Anchored"}), &json!("!abc!"), true).is_err());
        assert!(
            validate(&model, &json!({"shape":"Unsupported"}), &json!("abc"), true)
                .unwrap_err()
                .contains("unsupported model pattern")
        );
    }
}
