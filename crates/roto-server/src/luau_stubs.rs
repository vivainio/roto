//! Generates the `roto.d.luau` type definitions for `luau-analyze` and `luau-lsp`
//! (`roto-server --lua-stubs`).
//!
//! The hand-written setup API lives in `luau_stubs.d.luau`. The typed `roto.aws.<service>`
//! clients and the AWS shape aliases are generated here from the botocore models in `models/`.
use std::fmt::Write as _;

use serde_json::Value;

/// The operations given typed clients, per `roto.aws` service.
const TYPED: &str = include_str!("../typed-operations.json");

const TEMPLATE: &str = include_str!("luau_stubs.d.luau");

/// `(roto.aws name, model)` for every service with a model; `eventbridge` shares `events`.
pub(crate) const MODELS: &[(&str, &str)] = &[
    (
        "cloudformation",
        include_str!(concat!(env!("OUT_DIR"), "/cloudformation.json")),
    ),
    (
        "dynamodb",
        include_str!(concat!(env!("OUT_DIR"), "/dynamodb.json")),
    ),
    (
        "events",
        include_str!(concat!(env!("OUT_DIR"), "/eventbridge.json")),
    ),
    ("iam", include_str!(concat!(env!("OUT_DIR"), "/iam.json"))),
    (
        "kinesis",
        include_str!(concat!(env!("OUT_DIR"), "/kinesis.json")),
    ),
    ("kms", include_str!(concat!(env!("OUT_DIR"), "/kms.json"))),
    (
        "lambda",
        include_str!(concat!(env!("OUT_DIR"), "/lambda.json")),
    ),
    ("s3", include_str!(concat!(env!("OUT_DIR"), "/s3.json"))),
    (
        "secretsmanager",
        include_str!(concat!(env!("OUT_DIR"), "/secretsmanager.json")),
    ),
    ("sns", include_str!(concat!(env!("OUT_DIR"), "/sns.json"))),
    ("sqs", include_str!(concat!(env!("OUT_DIR"), "/sqs.json"))),
    ("ssm", include_str!(concat!(env!("OUT_DIR"), "/ssm.json"))),
    ("sts", include_str!(concat!(env!("OUT_DIR"), "/sts.json"))),
];

pub(crate) fn render() -> String {
    let mut clients = String::new();
    let mut shapes = String::new();
    for (service, text) in MODELS {
        let mut model: Value = serde_json::from_str(text).expect("embedded model");
        crate::prune::prune(&mut model, &typed_operations(service));
        write_service(service, &model, &mut clients, &mut shapes);
        if *service == "events" {
            let _ = writeln!(clients, "\t\teventbridge: aws_events_Client,");
        }
    }
    TEMPLATE
        .replace("--@@AWS_CLIENTS@@\n", &clients)
        .replace("--@@AWS_SHAPES@@\n", &shapes)
}

/// The operations `service` gets typed clients for: all it implements, or a chosen list.
pub(crate) fn typed_operations(service: &str) -> Vec<String> {
    let all: serde_json::Map<String, Value> =
        serde_json::from_str(TYPED).expect("typed operations");
    match &all[service] {
        Value::String(mode) if mode == "implemented" => crate::supported::operations(service)
            .into_iter()
            .flatten()
            .map(str::to_owned)
            .collect(),
        list => list
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
    }
}

const KEYWORDS: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "if", "in", "local",
    "nil", "not", "or", "repeat", "return", "then", "true", "until", "while", "continue", "export",
    "type", "typeof",
];

fn identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !KEYWORDS.contains(&name)
}

/// A table-type key: bare when it is an identifier, `["quoted"]` otherwise.
fn key(name: &str) -> String {
    if identifier(name) {
        name.to_string()
    } else {
        format!("[{name:?}]")
    }
}

fn alias(service: &str, shape: &str) -> String {
    let clean: String = shape
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("aws_{service}_{clean}")
}

fn luau_type(service: &str, model: &Value, spec: &Value) -> String {
    let name = spec["shape"].as_str().unwrap_or_default();
    let shape = &model["shapes"][name];
    match shape["type"].as_str().unwrap_or("string") {
        "structure" => alias(service, name),
        "list" => format!("{{ {} }}", luau_type(service, model, &shape["member"])),
        "map" => format!(
            "{{ [string]: {} }}",
            luau_type(service, model, &shape["value"])
        ),
        "integer" | "long" | "float" | "double" => "number".into(),
        "boolean" => "boolean".into(),
        "timestamp" => "(number | string)".into(),
        "document" => "any".into(),
        // Enum members stay plain strings: literal unions are rejected inside nested tables by
        // the old Luau solver, and AWS enums are open-ended in practice.
        _ => "string".into(),
    }
}

fn write_service(service: &str, model: &Value, clients: &mut String, shapes: &mut String) {
    let mut operations: Vec<&String> = model["operations"]
        .as_object()
        .map(|ops| ops.keys().collect())
        .unwrap_or_default();
    operations.sort();
    let _ = writeln!(shapes, "-- {service} client");
    let _ = writeln!(shapes, "type {} = {{", alias(service, "Client"));
    for operation in operations {
        let op = &model["operations"][operation.as_str()];
        let input = match op["input"]["shape"].as_str() {
            None => "input: { [string]: any }?".to_string(),
            Some(name) => {
                let all_optional = model["shapes"][name]["required"]
                    .as_array()
                    .is_none_or(Vec::is_empty);
                format!(
                    "input: {}{}",
                    luau_type(service, model, &op["input"]),
                    if all_optional { "?" } else { "" }
                )
            }
        };
        let output = if op["output"].is_null() {
            "()".to_string()
        } else {
            luau_type(service, model, &op["output"])
        };
        let _ = writeln!(shapes, "\t{}: ({input}) -> {output},", key(operation));
    }
    let _ = writeln!(shapes, "}}\n");
    let _ = writeln!(clients, "\t\t{service}: {},", alias(service, "Client"));

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
        let required: Vec<&str> = shape["required"]
            .as_array()
            .map(|r| r.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let mut members: Vec<(&String, &Value)> = shape["members"]
            .as_object()
            .map(|m| m.iter().collect())
            .unwrap_or_default();
        members.sort_by_key(|(name, _)| name.as_str());
        let _ = writeln!(shapes, "type {} = {{", alias(service, name));
        for (member, spec) in members {
            let optional = if required.contains(&member.as_str()) {
                ""
            } else {
                "?"
            };
            let _ = writeln!(
                shapes,
                "\t{}: {}{optional},",
                key(member),
                luau_type(service, model, spec)
            );
        }
        let _ = writeln!(shapes, "}}\n");
    }
}

#[cfg(test)]
mod tests {
    use super::render;

    #[test]
    fn definitions_declare_every_accepted_option_and_typed_operation() {
        let defs = render();
        assert!(!defs.contains("--@@"), "unreplaced marker");
        let declared = |name: &str| defs.contains(&format!("\t{name}: "));
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
                "executor",
                &[
                    "command",
                    "cwd",
                    "env",
                    "url",
                    "headers",
                    "container",
                    "warm",
                ],
            ),
            (
                "event source",
                &["batch_size", "enabled", "report_batch_item_failures"],
            ),
        ];
        for (group, names) in groups {
            for name in *names {
                assert!(declared(name), "definitions lack {group} option {name}");
            }
        }
        for symbol in [
            "\t\tqueue: (",
            "\t\ttable: (",
            "\t\tput: (",
            "\t\tbucket: (",
            "\t\tlink: (",
            "\t\tlink_dir: (",
            "\t\tfn: (",
            "\t\tbind: (",
            "\t\tevent_source: (",
            "\tcall: (",
            "\twire: (",
            "\tjson_decode: (",
            "\tarray: <T>(",
            "\tencode_base64: (",
            "\tdecode_base64: (",
            "\t\trequest: (",
            "\t\tmqtt_connect: (",
            "\t\tmqtt_subscribe: (",
            "\t\trds_data_connection: (",
        ] {
            assert!(defs.contains(symbol), "definitions lack {symbol:?}");
        }
        for service in [
            "s3", "iam", "sqs", "lambda", "dynamodb", "ssm", "events", "kms", "sns",
        ] {
            assert!(
                defs.contains(&format!("type aws_{service}_Client = {{")),
                "{service}"
            );
        }
        assert!(defs.contains("eventbridge: aws_events_Client,"));
        assert!(defs.contains("PutBucketCors: (input: aws_s3_PutBucketCorsRequest) -> ()"));
        assert!(defs.contains("type aws_s3_CORSRule = {"));
    }

    #[test]
    fn aliases_are_unique() {
        let defs = render();
        let names: Vec<&str> = defs
            .lines()
            .filter_map(|l| l.strip_prefix("type "))
            .filter_map(|l| l.split_once(' ').map(|(n, _)| n))
            .collect();
        let unique: std::collections::BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(names.len(), unique.len(), "duplicate type aliases");
    }
}
