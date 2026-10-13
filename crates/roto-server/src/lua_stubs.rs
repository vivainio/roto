//! Generates the `roto.d.lua` type stubs for lua-language-server (`roto-server --lua-stubs`).
//!
//! The hand-written helper declarations live in `lua_stubs.lua`. The typed `roto.call`
//! overloads and AWS shape classes are generated here from the embedded botocore models.
use std::fmt::Write as _;

use serde_json::Value;

const TEMPLATE: &str = include_str!("lua_stubs.lua");

pub(crate) fn render() -> String {
    let mut overloads = String::new();
    let mut shapes = String::new();
    for (service, model) in crate::aws_client::embedded_models() {
        write_service(service, model, &mut overloads, &mut shapes);
    }
    TEMPLATE
        .replace("--@@CALL_OVERLOADS@@\n", &overloads)
        .replace("--@@AWS_SHAPES@@\n", &shapes)
}

fn lua_type(service: &str, model: &Value, spec: &Value) -> String {
    let name = spec["shape"].as_str().unwrap_or_default();
    let shape = &model["shapes"][name];
    match shape["type"].as_str().unwrap_or("string") {
        "structure" => format!("aws.{service}.{name}"),
        "list" => {
            let inner = lua_type(service, model, &shape["member"]);
            if inner.contains(' ') || inner.contains('|') {
                format!("({inner})[]")
            } else {
                format!("{inner}[]")
            }
        }
        "map" => format!(
            "table<string, {}>",
            lua_type(service, model, &shape["value"])
        ),
        "integer" | "long" | "float" | "double" => "number".into(),
        "boolean" => "boolean".into(),
        _ => "string".into(),
    }
}

fn write_service(service: &str, model: &Value, overloads: &mut String, shapes: &mut String) {
    let mut operations: Vec<&String> = model["operations"]
        .as_object()
        .map(|ops| ops.keys().collect())
        .unwrap_or_default();
    operations.sort();
    for operation in operations {
        let op = &model["operations"][operation.as_str()];
        let input = if op["input"].is_null() {
            "input?: table".to_string()
        } else {
            format!("input: {}", lua_type(service, model, &op["input"]))
        };
        let output = if op["output"].is_null() {
            "nil".to_string()
        } else {
            lua_type(service, model, &op["output"])
        };
        let _ = writeln!(
            overloads,
            "---@overload fun(service: \"{service}\", operation: \"{operation}\", {input}): {output}"
        );
    }
    let mut names: Vec<&String> = model["shapes"]
        .as_object()
        .map(|all| {
            all.iter()
                .filter(|(_, shape)| shape["type"] == "structure")
                .map(|(name, _)| name)
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    let _ = writeln!(shapes, "-- {service} shapes");
    for name in names {
        let shape = &model["shapes"][name.as_str()];
        let _ = writeln!(shapes, "---@class aws.{service}.{name}");
        let required: Vec<&str> = shape["required"]
            .as_array()
            .map(|r| r.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let mut members: Vec<(&String, &Value)> = shape["members"]
            .as_object()
            .map(|m| m.iter().collect())
            .unwrap_or_default();
        members.sort_by_key(|(name, _)| name.as_str());
        for (member, spec) in members {
            let optional = if required.contains(&member.as_str()) {
                ""
            } else {
                "?"
            };
            let _ = writeln!(
                shapes,
                "---@field {member}{optional} {}",
                lua_type(service, model, spec)
            );
        }
        shapes.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::render;
    use std::collections::BTreeSet;

    #[test]
    fn stubs_declare_every_accepted_option_and_typed_operation() {
        let stubs = render();
        assert!(!stubs.contains("--@@"), "unreplaced marker");
        let declared = |name: &str| {
            stubs.contains(&format!("---@field {name} "))
                || stubs.contains(&format!("---@field {name}? "))
        };
        let groups: &[(&str, &[&str])] = &[
            ("bucket", &["cors", "notify", "skip_validation"]),
            (
                "cors",
                &["methods", "origins", "headers", "expose", "max_age", "id"],
            ),
            (
                "notify",
                &[
                    "queue", "topic", "lambda", "events", "prefix", "suffix", "id",
                ],
            ),
            ("table", &["hash", "range", "types", "gsis"]),
            ("gsi", &["name", "hash", "range", "projection"]),
            ("object", &["content_type", "metadata", "headers"]),
            ("link_dir", &["include", "exclude"]),
            ("object headers", crate::setup::LINK_HEADERS),
            ("queue", &["visibility_timeout", "attributes", "tags"]),
            (
                "function",
                &[
                    "timeout",
                    "memory_size",
                    "description",
                    "environment",
                    "executor",
                    "handler",
                ],
            ),
            (
                "event source",
                &["batch_size", "enabled", "report_batch_item_failures"],
            ),
        ];
        for (group, names) in groups {
            for name in *names {
                assert!(declared(name), "stubs lack {group} option {name}");
            }
        }
        for symbol in [
            "function roto.sqs.queue",
            "function roto.dynamodb.table",
            "function roto.dynamodb.put",
            "function roto.s3.bucket",
            "function roto.s3.put",
            "function roto.s3.link",
            "function roto.s3.link_dir",
            "function roto.lambda.fn",
            "function roto.lambda.bind",
            "function roto.lambda.event_source",
            "function roto.call",
            "function roto.wire",
            "function roto.json_decode",
            "function roto.array",
            "function roto.hooks.request",
            "function roto.hooks.mqtt_connect",
            "function roto.hooks.mqtt_subscribe",
            "operation: \"PutBucketCors\", input: aws.s3.PutBucketCorsRequest",
            "operation: \"CreateRole\", input: aws.iam.CreateRoleRequest",
            "---@class aws.s3.CORSRule",
        ] {
            assert!(stubs.contains(symbol), "stubs lack {symbol}");
        }
        let classes: Vec<&str> = stubs
            .lines()
            .filter_map(|l| l.strip_prefix("---@class "))
            .collect();
        let unique: BTreeSet<&str> = classes.iter().copied().collect();
        assert_eq!(classes.len(), unique.len(), "duplicate class names");
    }
}
