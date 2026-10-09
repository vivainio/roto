//! Generates typed request/response structs, query (de)serialisation and an operation
//! dispatcher from a botocore `service-2.json` model (`query` and `json` protocols).
//!
//! Usage: roto-codegen <service-2.json> <out.rs>
//!
//! Operations whose shapes use features the codegen does not support yet (maps, blobs,
//! nested lists) are still listed in `OPERATIONS` but dispatch to `NotImplemented`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::Value;

mod restxml;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: roto-codegen <service-2.json> <out.rs>");
        std::process::exit(2);
    }
    let model: Value =
        serde_json::from_str(&std::fs::read_to_string(&args[1]).expect("read model"))
            .expect("parse model");
    let code = Generator::new(&model).generate();
    std::fs::write(&args[2], code).expect("write output");
}

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Str,
    Int,
    Long,
    Bool,
    Double,
    Time,
    Blob,
    Struct(String),
    Map(Box<Kind>),
    /// Kept as raw JSON (see `RAW_SHAPES`).
    Raw,
    List {
        item: Box<Kind>,
        item_name: String,
        flattened: bool,
    },
    Unsupported(String),
}

/// `(serviceId, shape)` pairs carried as raw JSON instead of a generated struct.
const RAW_SHAPES: &[(&str, &str)] = &[("DynamoDB", "AttributeValue")];

/// `(serviceId, shape, member)`: collections that are always present in JSON responses, even when
/// empty (DynamoDB lists `Items: []`; most services omit empty lists).
const ALWAYS_EMIT: &[(&str, &str, &str)] = &[
    ("DynamoDB", "ListTablesOutput", "TableNames"),
    ("DynamoDB", "ListTagsOfResourceOutput", "Tags"),
    ("DynamoDB", "QueryOutput", "Items"),
    ("DynamoDB", "ScanOutput", "Items"),
    ("DynamoDB", "BatchGetItemOutput", "Responses"),
    ("DynamoDB", "BatchGetItemOutput", "UnprocessedKeys"),
    ("DynamoDB", "BatchWriteItemOutput", "UnprocessedItems"),
    ("DynamoDB", "TransactGetItemsOutput", "Responses"),
    ("DynamoDB", "ListBackupsOutput", "BackupSummaries"),
    ("SSM", "GetParametersResult", "Parameters"),
    ("SSM", "GetParametersResult", "InvalidParameters"),
    ("SSM", "GetParametersByPathResult", "Parameters"),
    ("SSM", "DeleteParametersResult", "DeletedParameters"),
    ("SSM", "DeleteParametersResult", "InvalidParameters"),
    ("SSM", "DescribeParametersResult", "Parameters"),
    ("SSM", "GetParameterHistoryResult", "Parameters"),
    ("SSM", "ParameterHistory", "Labels"),
    ("SSM", "ParameterMetadata", "Policies"),
    ("SSM", "ParameterHistory", "Policies"),
    ("SSM", "LabelParameterVersionResult", "InvalidLabels"),
    ("SSM", "UnlabelParameterVersionResult", "InvalidLabels"),
    ("SSM", "UnlabelParameterVersionResult", "RemovedLabels"),
    ("SSM", "ListTagsForResourceResult", "TagList"),
    ("Secrets Manager", "ListSecretsResponse", "SecretList"),
    (
        "Secrets Manager",
        "BatchGetSecretValueResponse",
        "SecretValues",
    ),
    ("Secrets Manager", "DescribeSecretResponse", "Tags"),
    ("Secrets Manager", "SecretListEntry", "Tags"),
    (
        "Secrets Manager",
        "ListSecretVersionIdsResponse",
        "Versions",
    ),
];

/// operation -> (input shape, output shape, result wrapper)
type Ops = BTreeMap<String, (Option<String>, Option<String>, Option<String>)>;

struct Generator<'a> {
    model: &'a Value,
    json: bool,
    rest_xml: bool,
}

impl<'a> Generator<'a> {
    fn new(model: &'a Value) -> Self {
        let proto = model["metadata"]["protocol"].as_str().unwrap_or("");
        assert!(
            matches!(proto, "query" | "json" | "rest-xml"),
            "unsupported protocol {proto}"
        );
        Self {
            model,
            json: proto == "json",
            rest_xml: proto == "rest-xml",
        }
    }

    fn shape(&self, name: &str) -> &'a Value {
        &self.model["shapes"][name]
    }

    fn kind(&self, shape_name: &str) -> Kind {
        let service_id = self.model["metadata"]["serviceId"].as_str().unwrap_or("");
        if RAW_SHAPES.contains(&(service_id, shape_name)) {
            return Kind::Raw;
        }
        let s = self.shape(shape_name);
        if s["eventstream"].as_bool().unwrap_or(false) {
            return Kind::Unsupported(format!("event stream {shape_name}"));
        }
        match s["type"].as_str().unwrap_or("") {
            "string" => Kind::Str,
            "integer" => Kind::Int,
            "long" => Kind::Long,
            "boolean" => Kind::Bool,
            "double" | "float" => Kind::Double,
            "timestamp" => Kind::Time,
            "structure" => Kind::Struct(shape_name.to_string()),
            "blob" if self.json || self.rest_xml => Kind::Blob,
            "map" if self.json || self.rest_xml => {
                Kind::Map(Box::new(self.kind(s["value"]["shape"].as_str().unwrap())))
            }
            "list" => {
                let member = &s["member"];
                let item = self.kind(member["shape"].as_str().unwrap());
                if !self.json && matches!(item, Kind::List { .. }) {
                    return Kind::Unsupported(format!("nested list {shape_name}"));
                }
                Kind::List {
                    item: Box::new(item),
                    item_name: member["locationName"]
                        .as_str()
                        .unwrap_or("member")
                        .to_string(),
                    flattened: s["flattened"].as_bool().unwrap_or(false),
                }
            }
            other => Kind::Unsupported(format!("{other} {shape_name}")),
        }
    }

    /// Collects every structure reachable from `root`; returns the first unsupported feature, if any.
    fn collect(&self, root: &str, seen: &mut BTreeSet<String>) -> Option<String> {
        if !seen.insert(root.to_string()) {
            return None;
        }
        for (_, m) in self.shape(root)["members"]
            .as_object()
            .into_iter()
            .flatten()
        {
            let k = self.kind(m["shape"].as_str().unwrap());
            if let Some(why) = self.check_kind(&k, seen) {
                return Some(why);
            }
        }
        None
    }

    fn check_kind(&self, k: &Kind, seen: &mut BTreeSet<String>) -> Option<String> {
        match k {
            Kind::Unsupported(why) => Some(why.clone()),
            Kind::Struct(n) => self.collect(n, seen),
            Kind::List { item, .. } | Kind::Map(item) => self.check_kind(item, seen),
            _ => None,
        }
    }

    fn rust_type(&self, k: &Kind) -> String {
        match k {
            Kind::Str => "String".into(),
            Kind::Int => "i32".into(),
            Kind::Long => "i64".into(),
            Kind::Bool => "bool".into(),
            Kind::Double => "f64".into(),
            Kind::Time => "Timestamp".into(),
            Kind::Struct(n) => pascal(n),
            Kind::Blob => "Blob".into(),
            Kind::Raw => "JsonValue".into(),
            Kind::List { item, .. } => format!("Vec<{}>", self.rust_type(item)),
            Kind::Map(v) => format!("BTreeMap<String, {}>", self.rust_type(v)),
            Kind::Unsupported(_) => unreachable!(),
        }
    }

    fn generate(&self) -> String {
        if self.rest_xml {
            return self.generate_rest_xml();
        }
        let meta = &self.model["metadata"];
        let mut out = String::new();
        out.push_str("// @generated by roto-codegen from the botocore model. DO NOT EDIT.\n");
        out.push_str(
            "#![allow(clippy::all, dead_code, unused_variables, unused_mut, unused_imports)]\n\n",
        );
        out.push_str("use roto_core::{AwsError, RequestContext, RawResponse};\n");
        if self.json {
            out.push_str("use roto_protocol::json::{as_object, member};\n");
            out.push_str(
                "use roto_protocol::{Blob, FromJson, JsonValue, Timestamp, ToJson, json_response};\n",
            );
            out.push_str("use serde_json::{Map, Value};\nuse std::collections::BTreeMap;\n\n");
            let _ = writeln!(
                out,
                "pub const JSON_VERSION: &str = {:?};",
                meta["jsonVersion"].as_str().unwrap_or("1.0")
            );
            let _ = writeln!(
                out,
                "pub const TARGET_PREFIX: &str = {:?};",
                meta["targetPrefix"].as_str().unwrap_or("")
            );
            let _ = writeln!(
                out,
                "pub const QUERY_COMPATIBLE: bool = {};",
                meta.get("awsQueryCompatible").is_some()
            );
        } else {
            let ns = meta["xmlNamespace"].as_str().expect("xmlNamespace");
            out.push_str("use roto_protocol::query::join_key;\n");
            out.push_str("use roto_protocol::{QueryParams, QueryValue, Timestamp, XmlValue, XmlWriter, query_response};\n\n");
            let _ = writeln!(out, "pub const NAMESPACE: &str = {ns:?};");
        }
        let _ = writeln!(
            out,
            "pub const API_VERSION: &str = {:?};\n",
            meta["apiVersion"].as_str().unwrap()
        );

        let mut ops = Ops::new();
        let mut structs = BTreeSet::new();
        let mut unsupported: BTreeMap<String, String> = BTreeMap::new();
        for (name, op) in self.model["operations"].as_object().unwrap() {
            let input = op["input"]["shape"].as_str().map(String::from);
            let output = op["output"]["shape"].as_str().map(String::from);
            let wrapper = op["output"]["resultWrapper"].as_str().map(String::from);
            let mut seen = BTreeSet::new();
            let mut why = None;
            for root in input.iter().chain(output.iter()) {
                why = why.or(self.collect(root, &mut seen));
            }
            match why {
                None => structs.extend(seen),
                Some(w) => {
                    unsupported.insert(name.clone(), w);
                }
            }
            ops.insert(name.clone(), (input, output, wrapper));
        }

        out.push_str("/// (operation, supported by the generated codec)\n");
        out.push_str("pub const OPERATIONS: &[(&str, bool)] = &[\n");
        for name in ops.keys() {
            let _ = writeln!(out, "    ({name:?}, {}),", !unsupported.contains_key(name));
        }
        out.push_str("];\n\n");

        for s in &structs {
            self.gen_struct(&mut out, s);
        }
        self.gen_trait(&mut out, &ops, &unsupported);
        self.gen_dispatch(&mut out, &ops, &unsupported);
        out
    }

    fn members(&self, shape: &str) -> Vec<(String, String, Kind, bool)> {
        let s = self.shape(shape);
        let required: BTreeSet<&str> = s["required"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
            .collect();
        let mut v: Vec<_> = s["members"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(name, m)| {
                let wire = if self.json {
                    name.clone()
                } else {
                    m["locationName"].as_str().unwrap_or(name).to_string()
                };
                (
                    name.clone(),
                    wire,
                    self.kind(m["shape"].as_str().unwrap()),
                    required.contains(name.as_str()),
                )
            })
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    fn gen_struct(&self, out: &mut String, shape: &str) {
        let name = pascal(shape);
        let members = self.members(shape);
        let _ = writeln!(
            out,
            "#[derive(Debug, Clone, Default, PartialEq)]\npub struct {name} {{"
        );
        for (m, _, k, req) in &members {
            let ty = self.rust_type(k);
            if matches!(k, Kind::List { .. } | Kind::Map(_)) || *req {
                let _ = writeln!(out, "    pub {}: {ty},", field(m));
            } else {
                let _ = writeln!(out, "    pub {}: Option<{ty}>,", field(m));
            }
        }
        out.push_str("}\n\n");

        if self.json {
            self.gen_struct_json(out, &name, &members);
            return;
        }
        let _ = writeln!(out, "impl QueryValue for {name} {{");
        out.push_str(
            "    fn read(p: &QueryParams, key: &str) -> Result<Option<Self>, AwsError> {\n",
        );
        out.push_str("        if !p.has_prefix(key) { return Ok(None); }\n");
        out.push_str("        Ok(Some(Self::read_members(p, key)?))\n    }\n}\n\n");

        let _ = writeln!(out, "impl XmlValue for {name} {{");
        out.push_str("    fn write(&self, w: &mut XmlWriter, name: &str) {\n");
        out.push_str("        w.open(name);\n        self.write_members(w);\n        w.close(name);\n    }\n}\n\n");

        let _ = writeln!(out, "impl {name} {{");
        out.push_str(
            "    pub fn read_members(p: &QueryParams, prefix: &str) -> Result<Self, AwsError> {\n",
        );
        out.push_str("        let s = Self {\n");
        for (m, wire, k, req) in &members {
            match k {
                Kind::List {
                    item_name,
                    flattened,
                    ..
                } => {
                    let _ = writeln!(
                        out,
                        "            {}: p.list(prefix, {wire:?}, {item_name:?}, {flattened})?,",
                        field(m)
                    );
                }
                _ if *req => {
                    let _ = writeln!(
                        out,
                        "            {}: p.value(prefix, {wire:?})?.ok_or_else(|| AwsError::missing_parameter(&join_key(prefix, {wire:?})))?,",
                        field(m)
                    );
                }
                _ => {
                    let _ = writeln!(out, "            {}: p.value(prefix, {wire:?})?,", field(m));
                }
            }
        }
        out.push_str("        };\n");
        for (m, wire, k, req) in &members {
            if *req && matches!(k, Kind::List { .. }) {
                let _ = writeln!(
                    out,
                    "        if s.{}.is_empty() {{ return Err(AwsError::missing_parameter(&join_key(prefix, {wire:?}))); }}",
                    field(m)
                );
            }
        }
        out.push_str("        Ok(s)\n    }\n\n");
        out.push_str("    pub fn write_members(&self, w: &mut XmlWriter) {\n");
        for (m, wire, k, req) in &members {
            match k {
                Kind::List {
                    item_name,
                    flattened,
                    ..
                } => {
                    let _ = writeln!(
                        out,
                        "        w.list({wire:?}, {item_name:?}, {flattened}, &self.{});",
                        field(m)
                    );
                }
                _ if *req => {
                    let _ = writeln!(out, "        self.{}.write(w, {wire:?});", field(m));
                }
                _ => {
                    let _ = writeln!(
                        out,
                        "        if let Some(v) = &self.{} {{ v.write(w, {wire:?}); }}",
                        field(m)
                    );
                }
            }
        }
        out.push_str("    }\n}\n\n");
    }

    fn gen_trait(&self, out: &mut String, ops: &Ops, unsupported: &BTreeMap<String, String>) {
        let svc = self.model["metadata"]["endpointPrefix"].as_str().unwrap();
        out.push_str(
            "/// Implement the operations you support; the rest answer `NotImplemented`.\n",
        );
        out.push_str("pub trait Service: Send + Sync {\n");
        for (name, (input, output, _)) in ops {
            if unsupported.contains_key(name) {
                continue;
            }
            let in_ty = input.as_deref().map(pascal).unwrap_or_else(|| "()".into());
            let out_ty = output.as_deref().map(pascal).unwrap_or_else(|| "()".into());
            let default = format!("Err(AwsError::not_implemented({svc:?}, {name:?}))");
            let _ = writeln!(
                out,
                "    fn {}(&self, ctx: &RequestContext, input: {in_ty}) -> Result<{out_ty}, AwsError> {{\n        let _ = (ctx, input);\n        {default}\n    }}",
                snake(name)
            );
        }
        out.push_str("}\n\n");
    }

    fn gen_dispatch(&self, out: &mut String, ops: &Ops, unsupported: &BTreeMap<String, String>) {
        let svc = self.model["metadata"]["endpointPrefix"].as_str().unwrap();
        if self.json {
            return self.gen_dispatch_json(out, ops, unsupported);
        }
        out.push_str("pub fn dispatch<S: Service + ?Sized>(\n    svc: &S,\n    ctx: &RequestContext,\n    action: &str,\n    params: &QueryParams,\n) -> Result<RawResponse, AwsError> {\n    match action {\n");
        for (name, (input, output, wrapper)) in ops {
            let _ = writeln!(out, "        {name:?} => {{");
            if unsupported.contains_key(name) {
                let _ = writeln!(
                    out,
                    "            Err(AwsError::not_implemented({svc:?}, {name:?}))"
                );
            } else {
                let call = match input {
                    Some(i) => format!(
                        "svc.{}(ctx, {}::read_members(params, \"\")?)?",
                        snake(name),
                        pascal(i)
                    ),
                    None => format!("svc.{}(ctx, ())?", snake(name)),
                };
                let _ = writeln!(out, "            let output = {call};");
                out.push_str("            let mut w = XmlWriter::new();\n");
                if output.is_some() {
                    out.push_str("            output.write_members(&mut w);\n");
                }
                let wrapper = match wrapper {
                    Some(w) => format!("Some({w:?})"),
                    None => "None".into(),
                };
                let _ = writeln!(
                    out,
                    "            Ok(query_response(NAMESPACE, {name:?}, {wrapper}, &ctx.request_id, &w.finish()))"
                );
            }
            out.push_str("        }\n");
        }
        out.push_str("        other => Err(AwsError::invalid_action(other)),\n    }\n}\n");
    }
}

impl Generator<'_> {
    fn gen_struct_json(
        &self,
        out: &mut String,
        name: &str,
        members: &[(String, String, Kind, bool)],
    ) {
        let coll = |k: &Kind| matches!(k, Kind::List { .. } | Kind::Map(_));
        let _ = writeln!(out, "impl FromJson for {name} {{");
        out.push_str("    fn from_json(v: &Value, path: &str) -> Result<Self, AwsError> {\n");
        out.push_str("        let o = as_object(v, path)?;\n        let s = Self {\n");
        for (m, wire, k, req) in members {
            let tail = if coll(k) && !*req {
                ".unwrap_or_default()".to_string()
            } else if *req {
                format!(".ok_or_else(|| AwsError::missing_parameter({wire:?}))?")
            } else {
                String::new()
            };
            let _ = writeln!(
                out,
                "            {}: member(o, {wire:?}, path)?{tail},",
                field(m)
            );
        }
        out.push_str("        };\n");
        out.push_str("        Ok(s)\n    }\n}\n\n");
        let _ = writeln!(out, "impl ToJson for {name} {{");
        out.push_str("    fn to_json(&self) -> Value {\n        let mut o = Map::new();\n");
        for (m, wire, k, req) in members {
            let service_id = self.model["metadata"]["serviceId"].as_str().unwrap_or("");
            let always = ALWAYS_EMIT.contains(&(service_id, name, wire.as_str()));
            if (!coll(k) && *req) || (coll(k) && always) {
                let _ = writeln!(
                    out,
                    "        o.insert({wire:?}.into(), self.{}.to_json());",
                    field(m)
                );
            } else if coll(k) {
                let _ = writeln!(
                    out,
                    "        if !self.{0}.is_empty() {{ o.insert({wire:?}.into(), self.{0}.to_json()); }}",
                    field(m)
                );
            } else {
                let _ = writeln!(
                    out,
                    "        if let Some(v) = &self.{} {{ o.insert({wire:?}.into(), v.to_json()); }}",
                    field(m)
                );
            }
        }
        out.push_str("        Value::Object(o)\n    }\n}\n\n");
    }

    fn gen_dispatch_json(
        &self,
        out: &mut String,
        ops: &Ops,
        unsupported: &BTreeMap<String, String>,
    ) {
        let svc = self.model["metadata"]["endpointPrefix"].as_str().unwrap();
        out.push_str("pub fn dispatch<S: Service + ?Sized>(\n    svc: &S,\n    ctx: &RequestContext,\n    operation: &str,\n    body: &Value,\n) -> Result<RawResponse, AwsError> {\n    match operation {\n");
        for (name, (input, output, _)) in ops {
            let _ = writeln!(out, "        {name:?} => {{");
            if unsupported.contains_key(name) {
                let _ = writeln!(
                    out,
                    "            Err(AwsError::not_implemented({svc:?}, {name:?}))"
                );
            } else {
                let call = match input {
                    Some(i) => format!(
                        "svc.{}(ctx, {}::from_json(body, \"\")?)?",
                        snake(name),
                        pascal(i)
                    ),
                    None => format!("svc.{}(ctx, ())?", snake(name)),
                };
                let _ = writeln!(out, "            let output = {call};");
                let json = if output.is_some() {
                    "output.to_json()"
                } else {
                    "Value::Object(Map::new())"
                };
                let _ = writeln!(
                    out,
                    "            Ok(json_response(JSON_VERSION, &ctx.request_id, &{json}))"
                );
            }
            out.push_str("        }\n");
        }
        out.push_str("        other => Err(AwsError::invalid_action(other)),\n    }\n}\n");
    }
}

fn pascal(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
        .unwrap_or_default()
}

fn snake(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_lowercase());
            if prev.is_lowercase() || prev.is_ascii_digit() || (prev.is_uppercase() && next_lower) {
                out.push('_');
            }
        }
        out.extend(c.to_lowercase());
    }
    out
}

fn field(member: &str) -> String {
    let s = snake(member);
    const KEYWORDS: &[&str] = &[
        "type", "ref", "match", "use", "mod", "move", "self", "in", "as", "fn", "loop", "where",
    ];
    if KEYWORDS.contains(&s.as_str()) {
        format!("r#{s}")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_cases_acronyms() {
        assert_eq!(snake("GetCallerIdentity"), "get_caller_identity");
        assert_eq!(snake("AssumeRoleWithSAML"), "assume_role_with_saml");
        assert_eq!(snake("SAMLAssertion"), "saml_assertion");
        assert_eq!(snake("DurationSeconds"), "duration_seconds");
        assert_eq!(snake("UserId"), "user_id");
    }

    #[test]
    fn pascal_cases_shape_names() {
        assert_eq!(pascal("tagType"), "TagType");
        assert_eq!(pascal("Credentials"), "Credentials");
    }
}
