//! `rest-xml` generation: route table, HTTP bindings, XML bodies, dispatcher.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::Value;

use super::{Generator, Kind, field, pascal, snake};

#[derive(Debug, Clone, PartialEq)]
enum Loc {
    Body,
    Uri(String),
    Query(String),
    Header(String),
    Prefix(String),
}

#[derive(Debug, Clone)]
struct Rm {
    name: String,
    kind: Kind,
    required: bool,
    loc: Loc,
    /// XML element (or attribute) name.
    wire: String,
    /// Item element name for wrapped lists.
    item: String,
    flattened: bool,
    attr: bool,
}

/// Requests that share a URI are told apart by a required header (S3 copy variants).
/// Operations that lose a tie against a sibling with the same method and URI.
const DEMOTED: &[&str] = &["ListDirectoryBuckets"];

const HEADER_ROUTES: &[(&str, &str)] = &[
    ("CopyObject", "x-amz-copy-source"),
    ("UploadPartCopy", "x-amz-copy-source"),
];

/// XML root element for outputs whose body is not a single payload member.
fn root_name(shape: &str) -> String {
    match shape {
        "ListObjectsV2Output" | "ListObjectsOutput" => "ListBucketResult".into(),
        "ListBucketsOutput" => "ListAllMyBucketsResult".into(),
        "GetBucketAclOutput" | "GetObjectAclOutput" => "AccessControlPolicy".into(),
        "ListObjectVersionsOutput" => "ListVersionsResult".into(),
        "ListMultipartUploadsOutput" => "ListMultipartUploadsResult".into(),
        "ListPartsOutput" => "ListPartsResult".into(),
        "CompleteMultipartUploadOutput" => "CompleteMultipartUploadResult".into(),
        "CreateMultipartUploadOutput" => "InitiateMultipartUploadResult".into(),
        "DeleteObjectsOutput" => "DeleteResult".into(),
        "GetObjectAttributesOutput" => "GetObjectAttributesResponse".into(),
        other => other
            .trim_end_matches("Output")
            .trim_end_matches("Response")
            .to_string(),
    }
}

struct Route {
    op: String,
    method: String,
    segs: Vec<(char, String)>, // 'L'iteral, 'V'ar, 'G'reedy
    query: Vec<(String, Option<String>)>,
    header: Option<&'static str>,
    /// Loses ties against a non-deprecated operation on the same URI.
    demoted: bool,
}

fn parse_route(op: &str, method: &str, uri: &str) -> Route {
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let segs = path
        .trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(
            |s| match s.strip_prefix('{').and_then(|r| r.strip_suffix('}')) {
                Some(v) => match v.strip_suffix('+') {
                    Some(g) => ('G', g.to_string()),
                    None => ('V', v.to_string()),
                },
                None => ('L', s.to_string()),
            },
        )
        .collect();
    let query = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) if !v.is_empty() => (k.to_string(), Some(v.to_string())),
            Some((k, _)) => (k.to_string(), None),
            None => (p.to_string(), None),
        })
        .collect();
    Route {
        op: op.into(),
        method: method.into(),
        segs,
        query,
        header: HEADER_ROUTES
            .iter()
            .find(|(o, _)| *o == op)
            .map(|(_, h)| *h),
        demoted: DEMOTED.contains(&op),
    }
}

impl Generator<'_> {
    fn rx_members(&self, shape: &str) -> Vec<Rm> {
        let s = self.shape(shape);
        let required: BTreeSet<&str> = s["required"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
            .collect();
        let mut out: Vec<Rm> = Vec::new();
        for (name, m) in s["members"].as_object().into_iter().flatten() {
            let member_shape = m["shape"].as_str().unwrap();
            let kind = self.kind(member_shape);
            let loc_name = m["locationName"].as_str();
            let list_shape = self.shape(member_shape);
            let flattened = m["flattened"].as_bool().unwrap_or(false)
                || list_shape["flattened"].as_bool().unwrap_or(false);
            let item = list_shape["member"]["locationName"]
                .as_str()
                .unwrap_or("member")
                .to_string();
            let loc = match m["location"].as_str() {
                Some("uri") => Loc::Uri(loc_name.unwrap_or(name).to_string()),
                Some("querystring") => Loc::Query(loc_name.unwrap_or(name).to_string()),
                Some("header") => Loc::Header(loc_name.unwrap_or(name).to_string()),
                Some("headers") => Loc::Prefix(loc_name.unwrap_or("").to_string()),
                _ => Loc::Body,
            };
            // Flattened lists repeat the member's own element name; fall back to the item name.
            let wire = match (&kind, flattened, loc_name) {
                (Kind::List { .. }, true, None)
                    if list_shape["member"]["locationName"].is_string() =>
                {
                    item.clone()
                }
                (_, _, Some(l)) => l.to_string(),
                _ => name.clone(),
            };
            out.push(Rm {
                name: name.clone(),
                kind,
                required: required.contains(name.as_str()),
                loc,
                wire,
                item,
                flattened,
                attr: m["xmlAttribute"].as_bool().unwrap_or(false),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    fn rx_type(&self, m: &Rm) -> String {
        let ty = self.rust_type(&m.kind);
        if matches!(m.kind, Kind::List { .. } | Kind::Map(_)) || m.required {
            ty
        } else {
            format!("Option<{ty}>")
        }
    }

    pub(crate) fn generate_rest_xml(&self) -> String {
        let meta = &self.model["metadata"];
        let default_ns = if meta["serviceId"].as_str() == Some("S3") {
            Some("http://s3.amazonaws.com/doc/2006-03-01/")
        } else {
            None
        };
        let mut out = String::new();
        out.push_str("// @generated by roto-codegen from the botocore model. DO NOT EDIT.\n");
        out.push_str("#![allow(clippy::all, dead_code, unused_variables, unused_mut, unused_imports, unused_assignments)]\n\n");
        out.push_str("use std::collections::BTreeMap;\n\n");
        out.push_str("use roto_core::{AwsError, RawRequest, RawResponse, RequestContext};\n");
        out.push_str("use roto_protocol::restxml::{HttpValue, XmlRead, attribute, list as header_list, member, member_list, parse_xml, prefixed, scalar};\n");
        out.push_str("use roto_protocol::{Blob, QueryParams, Timestamp, XmlValue, XmlWriter};\n\n");
        let _ = writeln!(
            out,
            "pub const API_VERSION: &str = {:?};\n",
            meta["apiVersion"].as_str().unwrap()
        );

        // Operations, supported-ness, shapes.
        let mut ops: Vec<(String, Value)> = self.model["operations"]
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect();
        ops.sort_by(|a, b| a.0.cmp(&b.0));
        let mut structs = BTreeSet::new();
        let mut inputs = BTreeSet::new();
        let mut outputs = BTreeSet::new();
        let mut unsupported: BTreeMap<String, String> = BTreeMap::new();
        for (name, op) in &ops {
            let input = op["input"]["shape"].as_str();
            let output = op["output"]["shape"].as_str();
            let mut seen = BTreeSet::new();
            let mut why = None;
            for root in input.iter().chain(output.iter()) {
                why = why.or(self.collect(root, &mut seen));
            }
            match why {
                None => {
                    structs.extend(seen);
                    inputs.extend(input.map(String::from));
                    outputs.extend(output.map(String::from));
                }
                Some(w) => {
                    unsupported.insert(name.clone(), w);
                }
            }
        }

        out.push_str("/// (operation, supported by the generated codec)\npub const OPERATIONS: &[(&str, bool)] = &[\n");
        for (name, _) in &ops {
            let _ = writeln!(out, "    ({name:?}, {}),", !unsupported.contains_key(name));
        }
        out.push_str("];\n\n");

        for s in &structs {
            self.rx_struct(
                &mut out,
                s,
                inputs.contains(s),
                outputs.contains(s),
                default_ns,
            );
        }
        self.rx_routes(&mut out, &ops, &unsupported);
        self.rx_trait_and_dispatch(&mut out, &ops, &unsupported);
        out
    }

    fn rx_struct(
        &self,
        out: &mut String,
        shape: &str,
        is_input: bool,
        is_output: bool,
        default_ns: Option<&str>,
    ) {
        let name = pascal(shape);
        let members = self.rx_members(shape);
        let _ = writeln!(
            out,
            "#[derive(Debug, Clone, Default, PartialEq)]\npub struct {name} {{"
        );
        for m in &members {
            let _ = writeln!(out, "    pub {}: {},", field(&m.name), self.rx_type(m));
        }
        out.push_str("}\n\n");

        let body: Vec<&Rm> = members.iter().filter(|m| m.loc == Loc::Body).collect();
        let payload = self.shape(shape)["payload"].as_str();
        let ns = self.shape(shape)["xmlNamespace"].clone();

        // ---- XmlRead
        let _ = writeln!(
            out,
            "impl XmlRead for {name} {{\n    fn read_xml(n: roxmltree::Node) -> Result<Self, AwsError> {{\n        Ok(Self {{"
        );
        for m in &body {
            let _ = writeln!(
                out,
                "            {}: {},",
                field(&m.name),
                self.xml_read_expr(m, "n")
            );
        }
        out.push_str("            ..Default::default()\n        })\n    }\n}\n\n");

        // ---- XmlValue
        let _ = writeln!(
            out,
            "impl XmlValue for {name} {{\n    fn write(&self, w: &mut XmlWriter, name: &str) {{"
        );
        out.push_str("        let mut attrs: Vec<(String, String)> = Vec::new();\n");
        if let Some(prefix) = ns["prefix"].as_str() {
            let _ = writeln!(
                out,
                "        attrs.push((\"xmlns:{prefix}\".to_string(), {:?}.to_string()));",
                ns["uri"].as_str().unwrap_or("")
            );
        } else if let Some(uri) = ns["uri"].as_str() {
            let _ = writeln!(
                out,
                "        attrs.push((\"xmlns\".to_string(), {uri:?}.to_string()));"
            );
        }
        for m in body.iter().filter(|m| m.attr) {
            if m.required {
                let _ = writeln!(
                    out,
                    "        attrs.push(({:?}.to_string(), self.{}.format_value()));",
                    m.wire,
                    field(&m.name)
                );
            } else {
                let _ = writeln!(
                    out,
                    "        if let Some(v) = &self.{} {{ attrs.push(({:?}.to_string(), v.format_value())); }}",
                    field(&m.name),
                    m.wire
                );
            }
        }
        out.push_str("        let refs: Vec<(&str, &str)> = attrs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();\n");
        out.push_str("        w.open_with(name, &refs);\n        self.write_body(w);\n        w.close(name);\n    }\n}\n\n");

        let _ = writeln!(
            out,
            "impl {name} {{\n    pub fn write_body(&self, w: &mut XmlWriter) {{"
        );
        for m in body.iter().filter(|m| !m.attr) {
            let f = field(&m.name);
            match &m.kind {
                Kind::List { .. } => {
                    let _ = writeln!(
                        out,
                        "        if !self.{f}.is_empty() {{ w.list({:?}, {:?}, {}, &self.{f}); }}",
                        m.wire, m.item, m.flattened
                    );
                }
                Kind::Blob | Kind::Map(_) => {}
                _ if m.required => {
                    let _ = writeln!(out, "        self.{f}.write(w, {:?});", m.wire);
                }
                _ => {
                    let _ = writeln!(
                        out,
                        "        if let Some(v) = &self.{f} {{ v.write(w, {:?}); }}",
                        m.wire
                    );
                }
            }
        }
        out.push_str("    }\n");

        // ---- request binding
        if is_input {
            out.push_str("\n    pub fn from_http(req: &RawRequest, uri: &BTreeMap<String, String>, q: &QueryParams) -> Result<Self, AwsError> {\n");
            let has_body_members = payload.is_none() && !body.is_empty();
            if has_body_members {
                out.push_str("        let doc = if req.body.is_empty() { None } else { Some(parse_xml(&req.body)?) };\n");
                out.push_str("        let root = doc.as_ref().map(|d| d.root_element());\n");
            }
            out.push_str("        Ok(Self {\n");
            for m in &members {
                let f = field(&m.name);
                let expr = match &m.loc {
                    Loc::Uri(n) => {
                        self.bind_scalar(m, &format!("uri.get({n:?}).map(String::as_str)"), n)
                    }
                    Loc::Query(n) => self.bind_scalar(m, &format!("q.get({n:?})"), n),
                    Loc::Header(n) => {
                        let lower = n.to_lowercase();
                        match &m.kind {
                            Kind::List { item, .. } => {
                                format!(
                                    "header_list::<{}>(req.header({lower:?}), {n:?})?",
                                    self.rust_type(item)
                                )
                            }
                            _ => self.bind_scalar(m, &format!("req.header({lower:?})"), n),
                        }
                    }
                    Loc::Prefix(p) => format!("prefixed(req, {:?})", p.to_lowercase()),
                    Loc::Body if payload == Some(m.name.as_str()) => self.payload_read(m),
                    Loc::Body if has_body_members => match &m.kind {
                        Kind::List { .. } => format!(
                            "match root {{ Some(r) => {}, None => Vec::new() }}",
                            self.xml_read_expr(m, "r")
                        ),
                        _ if m.required => format!(
                            "match root {{ Some(r) => {}, None => return Err(AwsError::missing_parameter({:?})) }}",
                            self.xml_read_expr(m, "r"),
                            m.name
                        ),
                        _ => format!(
                            "match root {{ Some(r) => {}, None => None }}",
                            self.xml_read_expr(m, "r")
                        ),
                    },
                    Loc::Body => "Default::default()".to_string(),
                };
                let _ = writeln!(out, "            {f}: {expr},");
            }
            out.push_str("        })\n    }\n");
        }

        // ---- response binding
        if is_output {
            out.push_str("\n    pub fn to_http(&self) -> RawResponse {\n");
            out.push_str("        let mut headers: Vec<(String, String)> = Vec::new();\n        let mut body: Vec<u8> = Vec::new();\n");
            for m in &members {
                let f = field(&m.name);
                match &m.loc {
                    Loc::Header(n) => match &m.kind {
                        Kind::List { .. } => {
                            let _ = writeln!(
                                out,
                                "        if !self.{f}.is_empty() {{ headers.push(({n:?}.to_string(), self.{f}.iter().map(|v| v.format_value()).collect::<Vec<_>>().join(\", \"))); }}"
                            );
                        }
                        _ if m.required => {
                            let _ = writeln!(
                                out,
                                "        headers.push(({n:?}.to_string(), self.{f}.format_value()));"
                            );
                        }
                        _ => {
                            let _ = writeln!(
                                out,
                                "        if let Some(v) = &self.{f} {{ headers.push(({n:?}.to_string(), v.format_value())); }}"
                            );
                        }
                    },
                    Loc::Prefix(p) => {
                        let _ = writeln!(
                            out,
                            "        for (k, v) in &self.{f} {{ headers.push((format!(\"{{}}{{}}\", {p:?}, k), v.clone())); }}"
                        );
                    }
                    _ => {}
                }
            }
            if let Some(p) = payload {
                let m = members.iter().find(|m| m.name == p).unwrap();
                let f = field(&m.name);
                match &m.kind {
                    Kind::Blob => {
                        if m.required {
                            let _ = writeln!(out, "        body = self.{f}.0.clone();");
                        } else {
                            let _ = writeln!(
                                out,
                                "        if let Some(b) = &self.{f} {{ body = b.0.clone(); }}"
                            );
                        }
                    }
                    Kind::Str => {
                        let _ = writeln!(
                            out,
                            "        if let Some(s) = &self.{f} {{ body = s.clone().into_bytes(); }}"
                        );
                    }
                    _ => {
                        let _ = writeln!(out, "        let mut w = XmlWriter::new();");
                        if m.required {
                            let _ = writeln!(out, "        self.{f}.write(&mut w, {:?});", m.wire);
                        } else {
                            let _ = writeln!(
                                out,
                                "        if let Some(v) = &self.{f} {{ v.write(&mut w, {:?}); }}",
                                m.wire
                            );
                        }
                        out.push_str("        let xml = w.finish();\n        if !xml.is_empty() {\n            body = format!(\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\\n{xml}\").into_bytes();\n            headers.push((\"content-type\".to_string(), \"application/xml\".to_string()));\n        }\n");
                    }
                }
            } else if !body.is_empty() {
                let root = root_name(shape);
                out.push_str("        let mut w = XmlWriter::new();\n");
                match default_ns {
                    Some(ns) => {
                        let _ = writeln!(
                            out,
                            "        w.open_with({root:?}, &[(\"xmlns\", {ns:?})]);"
                        );
                    }
                    None => {
                        let _ = writeln!(out, "        w.open({root:?});");
                    }
                }
                out.push_str("        self.write_body(&mut w);\n");
                let _ = writeln!(out, "        w.close({root:?});");
                out.push_str("        body = format!(\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\\n{}\", w.finish()).into_bytes();\n");
                out.push_str("        headers.push((\"content-type\".to_string(), \"application/xml\".to_string()));\n");
            }
            out.push_str("        RawResponse { status: 200, headers, body }\n    }\n");
        }
        out.push_str("}\n\n");
    }

    fn bind_scalar(&self, m: &Rm, src: &str, what: &str) -> String {
        match &m.kind {
            Kind::List { item, .. } => {
                format!("header_list::<{}>({src}, {what:?})?", self.rust_type(item))
            }
            _ => {
                let ty = self.rust_type(&m.kind);
                let base = format!("scalar::<{ty}>({src}, {what:?})?");
                if m.required {
                    format!("{base}.ok_or_else(|| AwsError::missing_parameter({what:?}))?")
                } else {
                    base
                }
            }
        }
    }

    fn payload_read(&self, m: &Rm) -> String {
        match &m.kind {
            Kind::Blob => {
                if m.required {
                    "Blob(req.body.clone())".into()
                } else {
                    "if req.body.is_empty() { None } else { Some(Blob(req.body.clone())) }".into()
                }
            }
            Kind::Str if m.required => "String::from_utf8_lossy(&req.body).into_owned()".into(),
            Kind::Str => "if req.body.is_empty() { None } else { Some(String::from_utf8_lossy(&req.body).into_owned()) }".into(),
            k => {
                let ty = self.rust_type(k);
                let some = format!("{{ let doc = parse_xml(&req.body)?; Some({ty}::read_xml(doc.root_element())?) }}");
                if m.required {
                    format!("if req.body.is_empty() {{ return Err(AwsError::missing_parameter({:?})) }} else {{ let doc = parse_xml(&req.body)?; {ty}::read_xml(doc.root_element())? }}", m.name)
                } else {
                    format!("if req.body.is_empty() {{ None }} else {some}")
                }
            }
        }
    }

    /// Expression reading body member `m` from XML node variable `node`.
    fn xml_read_expr(&self, m: &Rm, node: &str) -> String {
        let ty = self.rust_type(&m.kind);
        match &m.kind {
            Kind::List { item, .. } => format!(
                "member_list::<{}>({node}, {:?}, {:?}, {})?",
                self.rust_type(item),
                m.wire,
                m.item,
                m.flattened
            ),
            Kind::Map(_) => "Default::default()".into(),
            _ if m.attr => {
                let base = format!("attribute::<{ty}>({node}, {:?})?", m.wire);
                if m.required {
                    format!("{base}.unwrap_or_default()")
                } else {
                    base
                }
            }
            _ => {
                let base = format!("member::<{ty}>({node}, {:?})?", m.wire);
                if m.required {
                    format!(
                        "{base}.ok_or_else(|| AwsError::missing_parameter({:?}))?",
                        m.wire
                    )
                } else {
                    base
                }
            }
        }
    }

    fn rx_routes(
        &self,
        out: &mut String,
        ops: &[(String, Value)],
        unsupported: &BTreeMap<String, String>,
    ) {
        let mut routes: Vec<Route> = ops
            .iter()
            .filter(|(n, _)| !unsupported.contains_key(n))
            .map(|(n, o)| {
                let mut r = parse_route(
                    n,
                    o["http"]["method"].as_str().unwrap_or("GET"),
                    o["http"]["requestUri"].as_str().unwrap_or("/"),
                );
                // Required query-string members tell same-URI operations apart (uploadId, partNumber, id, …).
                if let Some(input) = o["input"]["shape"].as_str() {
                    for m in self.rx_members(input) {
                        if let (Loc::Query(q), true) = (&m.loc, m.required)
                            && !r.query.iter().any(|(k, _)| k == q)
                        {
                            r.query.push((q.clone(), None));
                        }
                    }
                }
                r.demoted |= o["deprecated"].as_bool().unwrap_or(false);
                r
            })
            .collect();
        // Most specific first: required headers/query, then more literal segments, then fewer greedy vars.
        routes.sort_by_key(|r| {
            let demoted = r.demoted;
            let lits = r.segs.iter().filter(|s| s.0 == 'L').count();
            let greedy = r.segs.iter().filter(|s| s.0 == 'G').count();
            (
                std::cmp::Reverse(r.query.len() + usize::from(r.header.is_some())),
                std::cmp::Reverse(r.segs.len()),
                std::cmp::Reverse(lits),
                greedy,
                demoted,
            )
        });
        out.push_str("enum Seg { Lit(&'static str), Var(&'static str), Greedy(&'static str) }\n\n");
        out.push_str("struct Route {\n    op: &'static str,\n    method: &'static str,\n    segs: &'static [Seg],\n    query: &'static [(&'static str, Option<&'static str>)],\n    header: Option<&'static str>,\n}\n\n");
        out.push_str("static ROUTES: &[Route] = &[\n");
        for r in &routes {
            let segs: Vec<String> = r
                .segs
                .iter()
                .map(|(k, v)| match k {
                    'L' => format!("Seg::Lit({v:?})"),
                    'V' => format!("Seg::Var({v:?})"),
                    _ => format!("Seg::Greedy({v:?})"),
                })
                .collect();
            let query: Vec<String> = r
                .query
                .iter()
                .map(|(k, v)| match v {
                    Some(v) => format!("({k:?}, Some({v:?}))"),
                    None => format!("({k:?}, None)"),
                })
                .collect();
            let header = match r.header {
                Some(h) => format!("Some({h:?})"),
                None => "None".into(),
            };
            let _ = writeln!(
                out,
                "    Route {{ op: {:?}, method: {:?}, segs: &[{}], query: &[{}], header: {header} }},",
                r.op,
                r.method,
                segs.join(", "),
                query.join(", ")
            );
        }
        out.push_str("];\n\n");
        out.push_str(
            r#"fn match_path(segs: &[Seg], path: &str) -> Option<BTreeMap<String, String>> {
    let mut rest = path;
    let mut vars = BTreeMap::new();
    for seg in segs {
        rest = rest.strip_prefix('/')?;
        match seg {
            Seg::Lit(l) => {
                rest = rest.strip_prefix(l)?;
                if !(rest.is_empty() || rest.starts_with('/')) {
                    return None;
                }
            }
            Seg::Var(v) => {
                let end = rest.find('/').unwrap_or(rest.len());
                if end == 0 {
                    return None;
                }
                vars.insert(v.to_string(), roto_protocol::restxml::percent_decode_path(&rest[..end]));
                rest = &rest[end..];
            }
            Seg::Greedy(v) => {
                if rest.is_empty() {
                    return None;
                }
                vars.insert(v.to_string(), roto_protocol::restxml::percent_decode_path(rest));
                rest = "";
            }
        }
    }
    (rest.is_empty() || rest == "/").then_some(vars)
}

/// Finds the operation for a request and extracts its URI variables.
pub fn route(req: &RawRequest, q: &QueryParams) -> Option<(&'static str, BTreeMap<String, String>)> {
    ROUTES.iter().find_map(|r| {
        if r.method != req.method {
            return None;
        }
        if let Some(h) = r.header {
            req.header(h)?;
        }
        for (k, v) in r.query {
            match (q.get(k), v) {
                (None, _) => return None,
                (Some(have), Some(want)) if have != *want => return None,
                _ => {}
            }
        }
        match_path(r.segs, &req.path).map(|vars| (r.op, vars))
    })
}

"#,
        );
    }

    fn rx_trait_and_dispatch(
        &self,
        out: &mut String,
        ops: &[(String, Value)],
        unsupported: &BTreeMap<String, String>,
    ) {
        let svc = self.model["metadata"]["endpointPrefix"].as_str().unwrap();
        out.push_str("/// Implement the operations you support; the rest answer `NotImplemented`.\npub trait Service: Send + Sync {\n");
        for (name, op) in ops {
            if unsupported.contains_key(name) {
                continue;
            }
            let in_ty = op["input"]["shape"]
                .as_str()
                .map(pascal)
                .unwrap_or_else(|| "()".into());
            let out_ty = op["output"]["shape"]
                .as_str()
                .map(pascal)
                .unwrap_or_else(|| "()".into());
            let _ = writeln!(
                out,
                "    fn {}(&self, ctx: &RequestContext, input: {in_ty}) -> Result<{out_ty}, AwsError> {{\n        let _ = (ctx, input);\n        Err(AwsError::not_implemented({svc:?}, {name:?}))\n    }}",
                snake(name)
            );
        }
        out.push_str("}\n\n");
        out.push_str("pub fn dispatch<S: Service + ?Sized>(svc: &S, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {\n    let q = QueryParams::parse(&req.query);\n    let Some((op, uri)) = route(req, &q) else {\n        return Err(AwsError::sender(405, \"MethodNotAllowed\", \"The specified method is not allowed against this resource.\"));\n    };\n    match op {\n");
        for (name, op) in ops {
            if unsupported.contains_key(name) {
                continue;
            }
            let status = op["http"]["responseCode"].as_u64().unwrap_or(200);
            let _ = writeln!(out, "        {name:?} => {{");
            let call = match op["input"]["shape"].as_str() {
                Some(i) => format!(
                    "svc.{}(ctx, {}::from_http(req, &uri, &q)?)?",
                    snake(name),
                    pascal(i)
                ),
                None => format!("svc.{}(ctx, ())?", snake(name)),
            };
            let _ = writeln!(out, "            let output = {call};");
            if op["output"]["shape"].is_string() {
                let _ = writeln!(
                    out,
                    "            let mut r = output.to_http();\n            r.status = {status};\n            Ok(r)"
                );
            } else {
                let _ = writeln!(
                    out,
                    "            Ok(RawResponse {{ status: {status}, headers: Vec::new(), body: Vec::new() }})"
                );
            }
            out.push_str("        }\n");
        }
        out.push_str("        other => Err(AwsError::not_implemented(\"s3\", other)),\n    }\n}\n");
    }
}
