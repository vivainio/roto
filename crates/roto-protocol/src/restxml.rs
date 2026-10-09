//! Runtime support for the `rest-xml` protocol (S3, Route 53, CloudFront): HTTP bindings
//! (URI, query, header, prefixed headers, payload) and XML bodies.

use std::collections::BTreeMap;

use roto_core::{AwsError, RawRequest, RawResponse};
use roxmltree::{Document, Node};

use crate::json::Blob;
use crate::query::QueryParams;
use crate::timestamp::Timestamp;
use crate::xml::XmlWriter;

/// Scalars that travel in URIs, query strings and headers.
pub trait HttpValue: Sized {
    fn parse_value(s: &str) -> Option<Self>;
    fn format_value(&self) -> String;
}

impl HttpValue for String {
    fn parse_value(s: &str) -> Option<Self> {
        Some(s.to_string())
    }
    fn format_value(&self) -> String {
        self.clone()
    }
}

macro_rules! http_parsed {
    ($($t:ty),*) => {$(
        impl HttpValue for $t {
            fn parse_value(s: &str) -> Option<Self> { s.trim().parse().ok() }
            fn format_value(&self) -> String { self.to_string() }
        }
    )*};
}
http_parsed!(i32, i64, f64, bool);

impl HttpValue for Timestamp {
    fn parse_value(s: &str) -> Option<Self> {
        Timestamp::parse(s)
    }
    /// Header form; query/body use ISO-8601 via [`crate::xml::XmlValue`].
    fn format_value(&self) -> String {
        self.to_http_date()
    }
}

fn invalid(what: &str, v: &str) -> AwsError {
    AwsError::sender(
        400,
        "InvalidArgument",
        format!("Invalid value '{v}' for {what}"),
    )
}

/// Binds an optional string (header, query, URI) to a typed scalar.
pub fn scalar<T: HttpValue>(v: Option<&str>, what: &str) -> Result<Option<T>, AwsError> {
    v.map(|s| T::parse_value(s).ok_or_else(|| invalid(what, s)))
        .transpose()
}

/// Comma-separated header list.
pub fn list<T: HttpValue>(v: Option<&str>, what: &str) -> Result<Vec<T>, AwsError> {
    match v {
        None => Ok(Vec::new()),
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| T::parse_value(p).ok_or_else(|| invalid(what, p)))
            .collect(),
    }
}

/// Percent-decodes a URI path segment (unlike form data, `+` stays a plus).
pub fn percent_decode_path(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (
                (b[i + 1] as char).to_digit(16),
                (b[i + 2] as char).to_digit(16),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-decodes form-encoded text (`+` is a space), as used in `x-amz-tagging`.
pub fn percent_decode_form(s: &str) -> String {
    percent_decode_path(&s.replace('+', " "))
}

/// All headers named `prefix*`, with the prefix stripped (e.g. `x-amz-meta-`).
pub fn prefixed(req: &RawRequest, prefix: &str) -> BTreeMap<String, String> {
    req.headers
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(prefix)
                .map(|rest| (rest.to_string(), v.clone()))
        })
        .collect()
}

pub fn query_value<'a>(q: &'a QueryParams, key: &str) -> Option<&'a str> {
    q.get(key)
}

pub fn parse_xml(body: &[u8]) -> Result<Document<'_>, AwsError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| AwsError::sender(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema."))?;
    Document::parse(text).map_err(|_| {
        AwsError::sender(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    })
}

/// A value readable from an XML element's text or children.
pub trait XmlRead: Sized {
    fn read_xml(node: Node) -> Result<Self, AwsError>;
}

fn malformed() -> AwsError {
    AwsError::sender(
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema.",
    )
}

fn text_of(node: Node) -> String {
    node.text().unwrap_or("").to_string()
}

impl XmlRead for String {
    fn read_xml(node: Node) -> Result<Self, AwsError> {
        Ok(text_of(node))
    }
}

macro_rules! xml_parsed {
    ($($t:ty),*) => {$(
        impl XmlRead for $t {
            fn read_xml(node: Node) -> Result<Self, AwsError> {
                text_of(node).trim().parse().map_err(|_| malformed())
            }
        }
    )*};
}
xml_parsed!(i32, i64, f64, bool);

impl XmlRead for Timestamp {
    fn read_xml(node: Node) -> Result<Self, AwsError> {
        Timestamp::parse(&text_of(node)).ok_or_else(malformed)
    }
}

impl XmlRead for Blob {
    fn read_xml(node: Node) -> Result<Self, AwsError> {
        crate::base64::decode(text_of(node).trim())
            .map(Blob)
            .ok_or_else(malformed)
    }
}

fn element_children<'a, 'i>(
    node: Node<'a, 'i>,
    name: &'a str,
) -> impl Iterator<Item = Node<'a, 'i>> {
    node.children()
        .filter(move |c| c.is_element() && c.tag_name().name() == name)
}

/// First child element called `name`.
pub fn member<T: XmlRead>(node: Node, name: &str) -> Result<Option<T>, AwsError> {
    element_children(node, name)
        .next()
        .map(T::read_xml)
        .transpose()
}

/// A list: flattened lists repeat `name`; wrapped lists nest `item` elements inside `name`.
pub fn member_list<T: XmlRead>(
    node: Node,
    name: &str,
    item: &str,
    flattened: bool,
) -> Result<Vec<T>, AwsError> {
    if flattened {
        element_children(node, name).map(T::read_xml).collect()
    } else {
        match element_children(node, name).next() {
            Some(wrapper) => element_children(wrapper, item).map(T::read_xml).collect(),
            None => Ok(Vec::new()),
        }
    }
}

/// XML attribute by local name (`xsi:type` is read as `type`).
pub fn attribute<T: HttpValue>(node: Node, name: &str) -> Result<Option<T>, AwsError> {
    let local = name.rsplit(':').next().unwrap_or(name);
    let found = node
        .attributes()
        .find(|a| a.name() == local)
        .map(|a| a.value());
    scalar(found, name)
}

impl XmlWriter {
    pub fn open_with(&mut self, name: &str, attrs: &[(&str, &str)]) {
        self.open_raw(name, attrs);
    }
}

/// `<Error><Code/><Message/>…extra…<RequestId/><HostId/></Error>`, the S3-style error body.
pub fn rest_xml_error(err: &AwsError, request_id: &str) -> RawResponse {
    let mut w = XmlWriter::new();
    w.open("Error");
    w.element("Code", &err.code);
    w.element("Message", &err.message);
    for (k, v) in &err.extra {
        w.element(k, v);
    }
    w.element("RequestId", request_id);
    w.element("HostId", "roto");
    w.close("Error");
    RawResponse {
        status: err.status,
        headers: vec![
            ("content-type".into(), "application/xml".into()),
            ("x-amz-request-id".into(), request_id.into()),
            ("x-amz-id-2".into(), "roto".into()),
        ],
        body: format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}", w.finish()).into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_paths() {
        assert_eq!(percent_decode_path("a%20b+c%2Fd"), "a b+c/d");
        assert_eq!(percent_decode_path("100%"), "100%");
        assert_eq!(percent_decode_path("%e2%82%ac"), "€");
    }

    #[test]
    fn binds_scalars_lists_and_prefixed_headers() {
        assert_eq!(scalar::<i32>(Some("7"), "n").unwrap(), Some(7));
        assert!(scalar::<i32>(Some("x"), "n").is_err());
        assert_eq!(
            list::<String>(Some("a, b,c"), "l").unwrap(),
            ["a", "b", "c"]
        );
        let req = RawRequest {
            headers: vec![
                ("x-amz-meta-a".into(), "1".into()),
                ("host".into(), "h".into()),
            ],
            ..Default::default()
        };
        assert_eq!(prefixed(&req, "x-amz-meta-")["a"], "1");
    }

    #[test]
    fn reads_wrapped_and_flattened_lists() {
        let doc = parse_xml(
            b"<R><Tags><Tag><K>a</K></Tag><Tag><K>b</K></Tag></Tags><C>x</C><C>y</C></R>",
        )
        .unwrap();
        let root = doc.root_element();
        struct Tag(String);
        impl XmlRead for Tag {
            fn read_xml(n: Node) -> Result<Self, AwsError> {
                Ok(Tag(member::<String>(n, "K")?.unwrap_or_default()))
            }
        }
        let tags: Vec<Tag> = member_list(root, "Tags", "Tag", false).unwrap();
        assert_eq!(
            tags.iter().map(|t| t.0.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        let c: Vec<String> = member_list(root, "C", "", true).unwrap();
        assert_eq!(c, ["x", "y"]);
    }
}
