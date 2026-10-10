//! The published CloudFormation resource specification, used to check templates.
//!
//! The pinned version is downloaded on the first CloudFormation request that
//! needs it and kept in a cache directory. The file's SHA-256 is verified both
//! for cached copies and for downloads. If the file cannot be fetched or
//! verified, template checks fall back to Roto's built-in rules.
//!
//! Set `ROTO_CFN_SPEC=offline` to skip the download. Set `ROTO_CACHE_DIR` to
//! choose the cache directory; the default is `~/.cache/roto`.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::PathBuf;

use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const VERSION: &str = "267.0.0";
const URL: &str =
    "https://d1uauaxba7bl26.cloudfront.net/267.0.0/gzip/CloudFormationResourceSpecification.json";
const SHA256: &str = "e330c292a00c3ae391cb736bcab567e6cf0607e893d75086cd4a574b678af829";

#[derive(Deserialize)]
struct Document {
    #[serde(rename = "ResourceTypes")]
    resource_types: HashMap<String, ResourceType>,
}

#[derive(Deserialize)]
struct ResourceType {
    #[serde(rename = "Properties", default)]
    properties: HashMap<String, Property>,
}

#[derive(Deserialize)]
struct Property {
    #[serde(rename = "Required", default)]
    required: bool,
    #[serde(rename = "PrimitiveType", default)]
    primitive_type: Option<String>,
}

#[derive(Clone)]
struct PropertySpec {
    required: bool,
    primitive: Option<String>,
}

/// Top-level resource properties from the spec.
pub struct Spec {
    types: HashMap<String, BTreeMap<String, PropertySpec>>,
}

impl Spec {
    /// Parse the uncompressed JSON document.
    pub fn from_json(json: &[u8]) -> Result<Self, String> {
        let document: Document = serde_json::from_slice(json).map_err(|e| e.to_string())?;
        let types = document
            .resource_types
            .into_iter()
            .map(|(name, resource)| {
                let properties = resource
                    .properties
                    .into_iter()
                    .map(|(property, p)| {
                        let spec = PropertySpec {
                            required: p.required,
                            primitive: p.primitive_type,
                        };
                        (property, spec)
                    })
                    .collect();
                (name, properties)
            })
            .collect();
        Ok(Self { types })
    }

    /// Check a resource's properties against the spec: names, required
    /// properties, and scalar types. Types the spec does not list are not checked.
    pub fn check(&self, ty: &str, properties: &Value) -> Result<(), String> {
        let Some(known) = self.types.get(ty) else {
            return Ok(());
        };
        let given = properties
            .as_object()
            .ok_or_else(|| format!("Properties of {ty} must be an object"))?;
        if let Some(unknown) = given.keys().find(|key| !known.contains_key(*key)) {
            return Err(format!("Unknown property {unknown} for {ty}"));
        }
        if let Some((name, _)) = known
            .iter()
            .find(|(name, p)| p.required && !given.contains_key(*name))
        {
            return Err(format!("Missing required property {name} for {ty}"));
        }
        for (name, value) in given {
            let Some(primitive) = known[name].primitive.as_deref() else {
                continue;
            };
            if !scalar_matches(primitive, value) {
                return Err(format!("Property {name} of {ty} must be a {primitive}"));
            }
        }
        Ok(())
    }
}

/// Intrinsic functions such as `Ref` or `Fn::Sub` are resolved later, so they
/// are accepted for any primitive type.
fn scalar_matches(primitive: &str, value: &Value) -> bool {
    let intrinsic = value
        .as_object()
        .is_some_and(|o| o.len() == 1 && o.keys().all(|k| k == "Ref" || k.starts_with("Fn::")));
    if intrinsic {
        return true;
    }
    match primitive {
        "String" => value.is_string() || value.is_number() || value.is_boolean(),
        "Integer" | "Long" => {
            value.is_i64()
                || value.is_u64()
                || value.as_str().is_some_and(|s| s.parse::<i64>().is_ok())
        }
        "Double" => value.is_number() || value.as_str().is_some_and(|s| s.parse::<f64>().is_ok()),
        "Boolean" => value.is_boolean() || matches!(value.as_str(), Some("true" | "false")),
        // Json and other primitives are not checked further here.
        _ => true,
    }
}

/// A small stand-in for the published spec, used by offline tests.
#[cfg(test)]
pub(crate) const FIXTURE: &str = r#"{
    "ResourceTypes": {
        "AWS::SQS::Queue": {"Properties": {
            "QueueName": {"Required": false, "PrimitiveType": "String"},
            "FifoQueue": {"Required": true, "PrimitiveType": "Boolean"},
            "DelaySeconds": {"Required": false, "PrimitiveType": "Integer"}
        }},
        "AWS::DynamoDB::Table": {"Properties": {
            "TableName": {"Required": false, "PrimitiveType": "String"},
            "AttributeDefinitions": {"Required": false, "Type": "List"}
        }}
    },
    "PropertyTypes": {}
}"#;

/// Load the pinned spec from the cache, downloading it when needed. Returns
/// `None` when the spec is disabled or unavailable.
pub fn load() -> Option<Spec> {
    if std::env::var("ROTO_CFN_SPEC").is_ok_and(|value| value == "offline") {
        return None;
    }
    let path = cache_dir().join(format!(
        "CloudFormationResourceSpecification-{VERSION}.json.gz"
    ));
    let gzip = match std::fs::read(&path) {
        Ok(bytes) if verify(&bytes) => bytes,
        _ => match fetch() {
            Ok(bytes) => {
                if let Err(error) = write_cache(&path, &bytes) {
                    eprintln!("warning: could not cache the CloudFormation spec: {error}");
                }
                bytes
            }
            Err(error) => {
                eprintln!(
                    "warning: CloudFormation resource spec {VERSION} unavailable ({error}); using built-in template checks"
                );
                return None;
            }
        },
    };
    let mut json = Vec::new();
    let decoded = GzDecoder::new(gzip.as_slice())
        .read_to_end(&mut json)
        .map_err(|e| e.to_string())
        .and_then(|_| Spec::from_json(&json));
    match decoded {
        Ok(spec) => Some(spec),
        Err(error) => {
            eprintln!(
                "warning: could not read the CloudFormation spec ({error}); using built-in template checks"
            );
            None
        }
    }
}

fn verify(bytes: &[u8]) -> bool {
    let digest: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    digest == SHA256
}

fn fetch() -> Result<Vec<u8>, String> {
    // A dedicated thread keeps the blocking client away from the async runtime.
    let bytes = std::thread::spawn(|| -> Result<Vec<u8>, String> {
        let response = reqwest::blocking::get(URL)
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|e| e.to_string())?;
        response
            .bytes()
            .map(|b| b.to_vec())
            .map_err(|e| e.to_string())
    })
    .join()
    .map_err(|_| "download thread panicked".to_string())??;
    if !verify(&bytes) {
        return Err("checksum does not match the pinned version".into());
    }
    Ok(bytes)
}

fn write_cache(path: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let temporary = path.with_extension("partial");
    std::fs::File::create(&temporary)?.write_all(bytes)?;
    std::fs::rename(temporary, path)
}

fn cache_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("ROTO_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".cache").join("roto"),
        None => std::env::temp_dir().join("roto"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use super::FIXTURE;

    #[test]
    fn unknown_and_missing_properties_are_rejected() {
        let spec = Spec::from_json(FIXTURE.as_bytes()).unwrap();
        spec.check("AWS::SQS::Queue", &json!({"FifoQueue": true}))
            .unwrap();
        let unknown = spec
            .check(
                "AWS::SQS::Queue",
                &json!({"FifoQueue": true, "QueueNmae": "typo"}),
            )
            .unwrap_err();
        assert!(unknown.contains("Unknown property QueueNmae"), "{unknown}");
        let missing = spec
            .check("AWS::SQS::Queue", &json!({"QueueName": "queue"}))
            .unwrap_err();
        assert!(
            missing.contains("Missing required property FifoQueue"),
            "{missing}"
        );
    }

    #[test]
    fn types_outside_the_spec_are_not_checked() {
        let spec = Spec::from_json(FIXTURE.as_bytes()).unwrap();
        spec.check("AWS::Made::Up", &json!({"Anything": 1}))
            .unwrap();
    }

    #[test]
    fn checksum_detects_changed_bytes() {
        assert!(!verify(b"not the pinned file"));
    }

    #[test]
    fn scalar_values_must_match_their_primitive_type() {
        let spec = Spec::from_json(FIXTURE.as_bytes()).unwrap();
        // Numeric strings are accepted for Integer, as CloudFormation does.
        spec.check(
            "AWS::SQS::Queue",
            &json!({"FifoQueue": true, "DelaySeconds": "5"}),
        )
        .unwrap();
        // Intrinsic functions are resolved later, so they pass any primitive check.
        spec.check(
            "AWS::SQS::Queue",
            &json!({"FifoQueue": {"Ref": "Flag"}, "DelaySeconds": {"Fn::If": ["C", 1, 2]}}),
        )
        .unwrap();
        let wrong_number = spec
            .check(
                "AWS::SQS::Queue",
                &json!({"FifoQueue": true, "DelaySeconds": "soon"}),
            )
            .unwrap_err();
        assert!(
            wrong_number.contains("DelaySeconds of AWS::SQS::Queue must be a Integer"),
            "{wrong_number}"
        );
        let wrong_boolean = spec
            .check("AWS::SQS::Queue", &json!({"FifoQueue": "maybe"}))
            .unwrap_err();
        assert!(wrong_boolean.contains("FifoQueue"), "{wrong_boolean}");
        let object_string = spec
            .check(
                "AWS::SQS::Queue",
                &json!({"FifoQueue": true, "QueueName": ["a"]}),
            )
            .unwrap_err();
        assert!(object_string.contains("QueueName"), "{object_string}");
    }

    /// Downloads the pinned spec. Run with `cargo test -- --ignored`.
    #[test]
    #[ignore = "downloads the CloudFormation resource specification"]
    fn handled_types_match_the_published_spec() {
        let spec = load().expect("pinned spec available");
        let examples = [
            (
                "AWS::SQS::Queue",
                json!({"QueueName": "queue", "FifoQueue": false}),
            ),
            ("AWS::SNS::Topic", json!({"TopicName": "topic"})),
            ("AWS::S3::Bucket", json!({})),
            ("AWS::Kinesis::Stream", json!({"ShardCount": 1})),
            (
                "AWS::DynamoDB::Table",
                json!({"KeySchema": [], "AttributeDefinitions": []}),
            ),
            ("AWS::IAM::Role", json!({"AssumeRolePolicyDocument": {}})),
            (
                "AWS::IAM::Policy",
                json!({"PolicyName": "p", "PolicyDocument": {}, "Roles": []}),
            ),
            (
                "AWS::Lambda::Function",
                json!({"Code": {}, "Role": "arn", "Runtime": "python3.12", "Handler": "h"}),
            ),
        ];
        for (ty, properties) in examples {
            spec.check(ty, &properties)
                .unwrap_or_else(|e| panic!("{e}"));
        }
    }
}
