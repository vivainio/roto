//! Browser-facing S3 objects and directory listings, separate from the AWS API.
use crate::{config, generated::*, service::S3};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext};
use roto_protocol::restxml::percent_decode_path;

pub fn bucket_from_host(host: &str) -> Option<String> {
    let host = host.split(':').next()?;
    let (bucket, domain) = host.split_once(".s3-website.")?;
    (!bucket.is_empty() && !domain.is_empty()).then(|| bucket.to_owned())
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
fn html(status: u16, body: String) -> RawResponse {
    RawResponse {
        status,
        headers: vec![("content-type".into(), "text/html; charset=utf-8".into())],
        body: body.into_bytes(),
    }
}
fn redirect(location: String) -> RawResponse {
    RawResponse {
        status: 302,
        headers: vec![("location".into(), location)],
        body: vec![],
    }
}
fn mime(key: &str) -> Option<&'static str> {
    Some(
        match key.rsplit('.').next()?.to_ascii_lowercase().as_str() {
            "html" | "htm" => "text/html; charset=utf-8",
            "css" => "text/css; charset=utf-8",
            "js" | "mjs" => "text/javascript; charset=utf-8",
            "json" => "application/json",
            "svg" => "image/svg+xml",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "ico" => "image/x-icon",
            "txt" => "text/plain; charset=utf-8",
            "pdf" => "application/pdf",
            "wasm" => "application/wasm",
            "woff" => "font/woff",
            "woff2" => "font/woff2",
            _ => return None,
        },
    )
}
fn object(
    s3: &S3,
    ctx: &RequestContext,
    req: &RawRequest,
    bucket: &str,
    key: &str,
) -> Result<RawResponse, AwsError> {
    let out = s3.get_object(
        ctx,
        GetObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            range: req.header("range").map(str::to_owned),
            if_match: req.header("if-match").map(str::to_owned),
            if_none_match: req.header("if-none-match").map(str::to_owned),
            ..Default::default()
        },
    )?;
    if let Some(location) = &out.website_redirect_location {
        let location = if location.starts_with('/') && !location.starts_with("//") {
            format!(
                "{}{}",
                req.header("x-roto-website-prefix").unwrap_or(""),
                location
            )
        } else {
            location.clone()
        };
        return Ok(redirect(location));
    }
    let mut raw = out.to_http();
    raw.headers
        .retain(|(n, _)| !n.eq_ignore_ascii_case("content-disposition"));
    if out
        .content_type
        .as_deref()
        .is_none_or(|t| t == "application/octet-stream" || t == "binary/octet-stream")
    {
        if let Some(t) = mime(key) {
            raw.headers
                .retain(|(n, _)| !n.eq_ignore_ascii_case("content-type"));
            raw.headers.push(("content-type".into(), t.into()));
        }
    }
    Ok(raw)
}

pub(crate) fn serve(
    s3: &S3,
    ctx: &RequestContext,
    req: &RawRequest,
    bucket: &str,
) -> Result<RawResponse, AwsError> {
    if req.method != "GET" && req.method != "HEAD" {
        return Ok(RawResponse {
            status: 405,
            headers: vec![("allow".into(), "GET, HEAD".into())],
            body: vec![],
        });
    }
    let config = config::load::<GetBucketWebsiteOutput>(s3, bucket, "website")?.unwrap_or_default();
    let key = percent_decode_path(req.path.trim_start_matches('/'));
    let base = req.header("x-roto-website-prefix").unwrap_or("");
    let mut response = if let Some(r) = &config.redirect_all_requests_to {
        redirect(format!(
            "{}://{}/{}",
            r.protocol.as_deref().unwrap_or("http"),
            r.host_name,
            encode(&key)
        ))
    } else {
        let result = if !key.is_empty() && !key.ends_with('/') {
            object(s3, ctx, req, bucket, &key)
        } else {
            Err(AwsError::sender(404, "NoSuchKey", "Not found"))
        };
        match result {
            Ok(raw) => raw,
            Err(e) if e.code == "NoSuchKey" => {
                let prefix = if key.is_empty() || key.ends_with('/') {
                    key.clone()
                } else {
                    format!("{key}/")
                };
                let index = format!(
                    "{prefix}{}",
                    config
                        .index_document
                        .as_ref()
                        .map(|i| i.suffix.as_str())
                        .unwrap_or("index.html")
                );
                let index_result = object(s3, ctx, req, bucket, &index);
                if let Err(e) = &index_result {
                    if e.code != "NoSuchKey" {
                        return Err(e.clone());
                    }
                }
                let page = s3.list_objects_v2(
                    ctx,
                    ListObjectsV2Request {
                        bucket: bucket.into(),
                        prefix: Some(prefix.clone()),
                        delimiter: Some("/".into()),
                        max_keys: Some(100),
                        continuation_token: roto_protocol::QueryParams::parse(&req.query)
                            .get("continuation")
                            .map(str::to_owned),
                        ..Default::default()
                    },
                )?;
                let exists = key.is_empty()
                    || index_result.is_ok()
                    || !page.contents.is_empty()
                    || !page.common_prefixes.is_empty();
                if exists && !key.is_empty() && !key.ends_with('/') {
                    redirect(format!(
                        "{base}/{}{}",
                        encode(&prefix),
                        if req.query.is_empty() {
                            String::new()
                        } else {
                            format!("?{}", req.query)
                        }
                    ))
                } else if let Ok(raw) = index_result {
                    raw
                } else if exists {
                    let title = escape(&format!("{bucket}/{prefix}"));
                    let mut body = format!(
                        "<!doctype html><meta charset=utf-8><title>{title}</title><h1>{title}</h1><ul>"
                    );
                    if !prefix.is_empty() {
                        body.push_str("<li><a href=\"../\">../</a></li>");
                    }
                    for name in page
                        .common_prefixes
                        .iter()
                        .filter_map(|p| p.prefix.as_deref())
                        .chain(page.contents.iter().filter_map(|o| o.key.as_deref()))
                    {
                        if name == prefix {
                            continue;
                        }
                        body.push_str(&format!(
                            "<li><a href=\"{}\">{}</a></li>",
                            escape(&encode(name.strip_prefix(&prefix).unwrap_or(name))),
                            escape(name.strip_prefix(&prefix).unwrap_or(name))
                        ));
                    }
                    body.push_str("</ul>");
                    if let Some(token) = page.next_continuation_token {
                        body.push_str(&format!(
                            "<a href=\"?continuation={}\">Next page</a>",
                            escape(&encode(&token).replace('/', "%2F"))
                        ));
                    }
                    html(200, body)
                } else if let Some(error) = &config.error_document {
                    let mut raw = object(s3, ctx, req, bucket, &error.key)
                        .unwrap_or_else(|_| html(404, "<h1>Object not found</h1>".into()));
                    raw.status = 404;
                    raw
                } else {
                    html(404, "<h1>Object not found</h1>".into())
                }
            }
            Err(e) => return Err(e),
        }
    };
    for rule in &config.routing_rules {
        let condition = rule.condition.as_ref();
        let prefix = condition
            .and_then(|c| c.key_prefix_equals.as_deref())
            .unwrap_or("");
        if !key.starts_with(prefix)
            || condition
                .and_then(|c| c.http_error_code_returned_equals.as_deref())
                .is_some_and(|code| code != response.status.to_string())
        {
            continue;
        }
        let r = &rule.redirect;
        let replacement =
            r.replace_key_with
                .clone()
                .unwrap_or_else(|| match &r.replace_key_prefix_with {
                    Some(replace) => format!("{replace}{}", &key[prefix.len()..]),
                    None => key.clone(),
                });
        let location = if r.host_name.is_some() || r.protocol.is_some() {
            format!(
                "{}://{}{}/{}",
                r.protocol.as_deref().unwrap_or("http"),
                r.host_name.as_deref().unwrap_or(
                    req.header("x-roto-website-authority")
                        .or_else(|| req.header("host"))
                        .unwrap_or("localhost")
                ),
                if r.host_name.is_none() { base } else { "" },
                encode(&replacement)
            )
        } else {
            format!("{base}/{}", encode(&replacement))
        };
        response = redirect(location);
        response.status = r
            .http_redirect_code
            .as_deref()
            .and_then(|v| v.parse::<u16>().ok())
            .filter(|v| (300..400).contains(v))
            .unwrap_or(301);
        break;
    }
    if req.method == "HEAD" {
        if !response
            .headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("content-length"))
        {
            response
                .headers
                .push(("content-length".into(), response.body.len().to_string()));
        }
        response.body.clear();
    }
    Ok(response)
}
