//! Embeds the botocore models without their prose: `documentation`, `documentationUrl` and
//! `examples` are most of each model's bytes and nothing at runtime or in the Luau type
//! generator reads them.
use std::path::PathBuf;

use serde_json::Value;

const MODELS: &[&str] = &[
    "cloudformation",
    "dynamodb",
    "eventbridge",
    "iam",
    "kinesis",
    "kms",
    "lambda",
    "logs",
    "s3",
    "secretsmanager",
    "sns",
    "sqs",
    "ssm",
    "sts",
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
    for name in MODELS {
        let source = models.join(name).join("service-2.json");
        println!("cargo:rerun-if-changed={}", source.display());
        let mut model: Value = serde_json::from_str(&std::fs::read_to_string(&source).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", source.display()));
        strip(&mut model);
        std::fs::write(
            out.join(format!("{name}.json")),
            serde_json::to_string(&model).unwrap(),
        )
        .unwrap();
    }
    println!("cargo:rerun-if-changed=build.rs");
}
