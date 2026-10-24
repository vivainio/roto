//! Generates the `roto.d.luau` type definitions for `luau-analyze` and `luau-lsp`
//! (`roto-server --lua-stubs`).
//!
//! The hand-written setup API lives in `luau_stubs.d.luau`. The typed `roto.aws.<service>`
//! clients and the AWS shape aliases are generated here from the botocore models in `models/`.
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

/// The operations given typed clients, per `roto.aws` service.
const TYPED: &str = include_str!("../typed-operations.json");

const TEMPLATE: &str = include_str!("luau_stubs.d.luau");

/// Definitions for service-module bundles (`--lua-services`), which see a different `roto`.
const SERVICE_TEMPLATE: &str = include_str!("luau_service_stubs.d.luau");

pub(crate) fn render_service() -> String {
    SERVICE_TEMPLATE.to_owned()
}

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

/// Whether `shape` is, or contains, an enum. Such structures get a second, input-side alias
/// whose enum members are literal unions.
fn has_enum(model: &Value, name: &str, seen: &mut std::collections::HashMap<String, bool>) -> bool {
    if let Some(known) = seen.get(name) {
        return *known;
    }
    // A cycle is resolved to false on the way in; any enum elsewhere in the cycle still counts.
    seen.insert(name.to_string(), false);
    let shape = &model["shapes"][name];
    let found = match shape["type"].as_str().unwrap_or("string") {
        "structure" => shape["members"].as_object().is_some_and(|members| {
            members
                .values()
                .any(|m| has_enum(model, m["shape"].as_str().unwrap_or_default(), seen))
        }),
        "list" => has_enum(
            model,
            shape["member"]["shape"].as_str().unwrap_or_default(),
            seen,
        ),
        "map" => has_enum(
            model,
            shape["value"]["shape"].as_str().unwrap_or_default(),
            seen,
        ),
        "string" => shape["enum"].as_array().is_some_and(|e| !e.is_empty()),
        _ => false,
    };
    seen.insert(name.to_string(), found);
    found
}

/// Every shape reachable from `name`, inserted into `out`.
fn reach(model: &Value, name: &str, out: &mut std::collections::HashSet<String>) {
    if !out.insert(name.to_string()) {
        return;
    }
    let shape = &model["shapes"][name];
    let children: Vec<&Value> = match shape["type"].as_str().unwrap_or("string") {
        "structure" => shape["members"]
            .as_object()
            .map(|m| m.values().collect())
            .unwrap_or_default(),
        "list" => vec![&shape["member"]],
        "map" => vec![&shape["value"]],
        _ => Vec::new(),
    };
    for child in children {
        reach(model, child["shape"].as_str().unwrap_or_default(), out);
    }
}

/// Which side of a request a type is written for. Inputs restrict enum members to their known
/// values so a misspelled one is an error; outputs stay `string` because AWS adds enum values.
#[derive(Clone, Copy, PartialEq)]
enum Side {
    Input,
    Output,
}

fn luau_type(
    service: &str,
    model: &Value,
    spec: &Value,
    side: Side,
    enums: &mut std::collections::HashMap<String, bool>,
) -> String {
    let name = spec["shape"].as_str().unwrap_or_default();
    let shape = &model["shapes"][name];
    match shape["type"].as_str().unwrap_or("string") {
        "structure" if side == Side::Input && has_enum(model, name, enums) => {
            format!("{}In", alias(service, name))
        }
        "structure" => alias(service, name),
        "list" => format!(
            "{{ {} }}",
            luau_type(service, model, &shape["member"], side, enums)
        ),
        "map" => format!(
            "{{ [string]: {} }}",
            luau_type(service, model, &shape["value"], side, enums)
        ),
        "integer" | "long" | "float" | "double" => "number".into(),
        "boolean" => "boolean".into(),
        "timestamp" => "(number | string)".into(),
        "document" => "any".into(),
        // Literal unions in nested tables need the new Luau solver, which `--check` uses.
        _ => match shape["enum"].as_array() {
            Some(values) if side == Side::Input && !values.is_empty() => {
                let members: Vec<String> = values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|v| format!("{v:?}"))
                    .collect();
                format!("({})", members.join(" | "))
            }
            _ => "string".into(),
        },
    }
}

fn write_service(service: &str, model: &Value, clients: &mut String, shapes: &mut String) {
    let mut enums = std::collections::HashMap::new();
    let mut input_shapes = std::collections::HashSet::new();
    for op in model["operations"]
        .as_object()
        .into_iter()
        .flat_map(|o| o.values())
    {
        if let Some(name) = op["input"]["shape"].as_str() {
            reach(model, name, &mut input_shapes);
        }
    }
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
                    luau_type(service, model, &op["input"], Side::Input, &mut enums),
                    if all_optional { "?" } else { "" }
                )
            }
        };
        let output = if op["output"].is_null() {
            "()".to_string()
        } else {
            luau_type(service, model, &op["output"], Side::Output, &mut enums)
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
        let sides: &[Side] =
            if input_shapes.contains(name.as_str()) && has_enum(model, name, &mut enums) {
                &[Side::Output, Side::Input]
            } else {
                &[Side::Output]
            };
        for &side in sides {
            let suffix = if side == Side::Input { "In" } else { "" };
            let _ = writeln!(shapes, "type {}{suffix} = {{", alias(service, name));
            for (member, spec) in &members {
                let optional = if required.contains(&member.as_str()) {
                    ""
                } else {
                    "?"
                };
                let _ = writeln!(
                    shapes,
                    "\t{}: {}{optional},",
                    key(member),
                    luau_type(service, model, spec, side, &mut enums)
                );
            }
            let _ = writeln!(shapes, "}}\n");
        }
    }
}

const INSTALL_HINT: &str = "\
error: `luau-lsp` was not found, and `roto-server --check` needs it to type-check scripts.
Install it from https://github.com/JohnnyMorganz/luau-lsp/releases and put the binary on your PATH,
or set ROTO_LUAU_LSP to its full path. Steps per platform are in docs/lua-types.md (Install luau-lsp).";

/// `roto-server --check`: type-checks setup scripts against the definitions of this build by
/// running `luau-lsp analyze`. Returns the process exit code.
pub(crate) fn check(files: &[PathBuf]) -> i32 {
    analyze(files, &render(), "setup scripts")
}

/// `roto-server --check-services`: the same for service modules and their `common.luau`.
pub(crate) fn check_services(files: &[PathBuf]) -> i32 {
    analyze(files, &render_service(), "service modules")
}

fn analyze(files: &[PathBuf], definitions: &str, what: &str) -> i32 {
    let program = std::env::var("ROTO_LUAU_LSP").unwrap_or_else(|_| "luau-lsp".into());
    let dir = std::env::temp_dir().join(format!("roto-luau-{}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&dir)
        .and_then(|()| std::fs::write(dir.join("roto.d.luau"), definitions))
    {
        eprintln!("error: cannot write Lua stubs to {}: {e}", dir.display());
        return 1;
    }
    for file in files {
        match std::fs::read_to_string(file) {
            Ok(text) if !text.lines().any(|l| l.trim() == "--!strict") => eprintln!(
                "warning: {} has no `--!strict` line, so most mistakes will pass unnoticed",
                file.display()
            ),
            _ => {}
        }
    }
    let status = Command::new(&program)
        .args(["analyze", "--platform=standard", "--flag:LuauSolverV2=true"])
        .arg(format!(
            "--definitions={}",
            dir.join("roto.d.luau").display()
        ))
        .args(files)
        .status();
    let _ = std::fs::remove_dir_all(&dir);
    match status {
        Ok(status) => status.code().unwrap_or(1),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("{INSTALL_HINT}");
            127
        }
        Err(e) => {
            eprintln!("error: cannot run {program} to check {what}: {e}");
            1
        }
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
            ("bucket", &["cors", "notify", "eventbridge", "skip_validation"]),
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
                    "runtime",
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
        assert!(!defs.contains("eventbridge: aws_events_Client,"));
        assert!(defs.contains("PutBucketCors: (input: aws_s3_PutBucketCorsRequestIn) -> ()"));
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

    #[test]
    fn input_enums_are_literal_unions_and_outputs_stay_strings() {
        let defs = render();
        assert!(defs.contains("Type: (\"String\" | \"StringList\" | \"SecureString\")?,"));
        // `Parameter` is an output-only shape: its `Type` must stay a plain string.
        let parameter = defs.split("type aws_ssm_Parameter = {").nth(1).unwrap();
        let parameter = parameter.split("\n}").next().unwrap();
        assert!(parameter.contains("Type: string?,"), "{parameter}");
    }
}
