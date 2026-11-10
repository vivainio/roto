//! Embeds the botocore models without their prose: `documentation`, `documentationUrl` and
//! `examples` are most of each model's bytes and nothing at runtime or in the Luau type
//! generator reads them.
use std::path::PathBuf;

use serde_json::Value;

include!("src/prune.rs");

/// Models the `roto.call` XML codec reads at runtime in full; the rest only feed the types.
const RUNTIME_MODELS: &[&str] = &["s3", "iam", "sns", "sts", "cloudformation"];

const MODELS: &[&str] = &[
    "cloudformation",
    "dynamodb",
    "eventbridge",
    "iam",
    "kinesis",
    "kms",
    "lambda",
    "s3",
    "secretsmanager",
    "sns",
    "sqs",
    "ssm",
    "sts",
    "states",
    "scheduler",
    "logs",
];

fn strip(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|k, _| {
                !matches!(
                    k.as_str(),
                    "documentation" | "documentationUrl" | "examples"
                )
            });
            map.values_mut().for_each(strip);
        }
        Value::Array(items) => items.iter_mut().for_each(strip),
        _ => {}
    }
}

fn main() {
    let models =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../../models");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let typed: serde_json::Map<String, Value> =
        serde_json::from_str(&std::fs::read_to_string("typed-operations.json").unwrap()).unwrap();
    println!("cargo:rerun-if-changed=typed-operations.json");
    for name in MODELS {
        let source = models.join(name).join("service-2.json");
        println!("cargo:rerun-if-changed={}", source.display());
        let mut model: Value = serde_json::from_str(&std::fs::read_to_string(&source).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", source.display()));
        strip(&mut model);
        // TestState validation needs all response shapes, before editor-operation pruning.
        std::fs::write(
            out.join(format!("{name}-mock.json")),
            serde_json::to_string(&model).unwrap(),
        )
        .unwrap();
        // eventbridge's `roto.aws` name is `events`. A list prunes types-only models here; a
        // service typed in full ("implemented") is pruned by the generator, which can see
        // what the service implements.
        let service = if *name == "eventbridge" {
            "events"
        } else {
            name
        };
        if *name == "states" {
            let path = models.join("../services/states/service.luau");
            println!("cargo:rerun-if-changed={}", path.display());
            let code = std::fs::read_to_string(path).expect("Step Functions service source");
            let operations: Vec<String> = code
                .lines()
                .filter_map(|line| {
                    let name = line.strip_prefix("function M.")?.split_once('(')?.0;
                    (!name.starts_with("__")).then(|| name.to_owned())
                })
                .collect();
            prune(&mut model, &operations);
        }
        if !RUNTIME_MODELS.contains(name)
            && let Some(list) = typed.get(service).and_then(Value::as_array)
        {
            let operations: Vec<String> = list
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            prune(&mut model, &operations);
        }
        std::fs::write(
            out.join(format!("{name}.json")),
            serde_json::to_string(&model).unwrap(),
        )
        .unwrap();
    }
    println!("cargo:rerun-if-changed=build.rs");
}
