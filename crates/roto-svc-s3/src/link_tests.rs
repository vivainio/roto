//! `S3::link_object`: bodies served from host files that roto must never modify.

use std::path::PathBuf;

use roto_core::store::Store;
use roto_core::{RawRequest, RawResponse, RequestContext, ServiceHandler};

use crate::{LinkAttrs, S3Handler};

struct Fixture {
    _store: Store,
    s3: S3Handler,
    ctx: RequestContext,
    dir: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("roto-links-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::ephemeral();
        let s3 = S3Handler::new(&store).unwrap();
        let ctx = RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "link-test".into(),
            base_url: "http://localhost:5070".into(),
        };
        Self {
            _store: store,
            s3,
            ctx,
            dir,
        }
    }

    fn host(&self, name: &str, body: &str) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    fn link(&self, key: &str, host: &std::path::Path) {
        self.s3
            .0
            .link_object(&self.ctx, "bkt", key, host, LinkAttrs::default())
            .unwrap();
    }

    fn call(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> RawResponse {
        self.s3
            .handle(
                &self.ctx,
                &RawRequest {
                    method: method.into(),
                    path: path.into(),
                    query: query.into(),
                    headers: headers
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    body: body.as_bytes().to_vec(),
                },
            )
            .unwrap()
    }

    fn get(&self, key: &str) -> RawResponse {
        self.call("GET", &format!("/bkt/{key}"), "", &[], "")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn header<'a>(r: &'a RawResponse, name: &str) -> Option<&'a str> {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

#[test]
fn linked_object_is_readable_listable_and_ranged() {
    let f = Fixture::new();
    let host = f.host("data.csv", "0123456789");
    f.link("demo/data.csv", &host);

    let got = f.get("demo/data.csv");
    assert_eq!(
        (got.status, got.body.as_slice()),
        (200, b"0123456789".as_slice())
    );
    assert_eq!(header(&got, "content-type"), Some("text/csv"));
    let etag = header(&got, "etag").unwrap().to_string();
    assert_eq!(etag.len(), 34, "{etag}");
    assert_ne!(
        etag, "\"781e5e245d69b566979b86e28d23f2c7\"",
        "not the body md5"
    );

    let head = f.call("HEAD", "/bkt/demo/data.csv", "", &[], "");
    assert_eq!(header(&head, "content-length"), Some("10"));

    let ranged = f.call(
        "GET",
        "/bkt/demo/data.csv",
        "",
        &[("range", "bytes=2-4")],
        "",
    );
    assert_eq!(ranged.body, b"234");
    assert_eq!(header(&ranged, "content-range"), Some("bytes 2-4/10"));

    let list = f.call("GET", "/bkt", "list-type=2&prefix=demo%2F", &[], "");
    assert!(String::from_utf8_lossy(&list.body).contains("<Key>demo/data.csv</Key>"));
}

#[test]
fn gz_stays_binary_and_attrs_override_the_guess() {
    let f = Fixture::new();
    let gz = f.host("rows.csv.gz", "x");
    f.link("rows.csv.gz", &gz);
    assert_eq!(
        header(&f.get("rows.csv.gz"), "content-type"),
        Some("binary/octet-stream")
    );
    let attrs = LinkAttrs {
        content_type: Some("text/x-custom".into()),
        metadata: [("owner".to_string(), "me".to_string())].into(),
        headers: [("cache_control".to_string(), "no-cache".to_string())].into(),
    };
    f.s3.0
        .link_object(&f.ctx, "bkt", "custom", &gz, attrs)
        .unwrap();
    let got = f.get("custom");
    assert_eq!(header(&got, "content-type"), Some("text/x-custom"));
    assert_eq!(header(&got, "x-amz-meta-owner"), Some("me"));
    assert_eq!(header(&got, "cache-control"), Some("no-cache"));
}

#[test]
fn deleting_or_overwriting_never_touches_the_host_file() {
    let f = Fixture::new();
    let host = f.host("a.txt", "host body");
    f.link("k", &host);
    assert_eq!(f.call("PUT", "/bkt/k", "", &[], "uploaded").status, 200);
    assert_eq!(f.get("k").body, b"uploaded");
    assert_eq!(std::fs::read_to_string(&host).unwrap(), "host body");

    f.link("k", &host);
    assert_eq!(f.call("DELETE", "/bkt/k", "", &[], "").status, 204);
    assert_eq!(f.get("k").status, 404);
    assert_eq!(std::fs::read_to_string(&host).unwrap(), "host body");
}

#[test]
fn relink_refreshes_and_a_missing_file_is_an_internal_error() {
    let f = Fixture::new();
    let host = f.host("a.txt", "one");
    f.link("k", &host);
    std::fs::write(&host, "three").unwrap();
    f.link("k", &host);
    let head = f.call("HEAD", "/bkt/k", "", &[], "");
    assert_eq!(header(&head, "content-length"), Some("5"));
    let list = f.call("GET", "/bkt", "list-type=2", &[], "");
    assert_eq!(
        String::from_utf8_lossy(&list.body).matches("<Key>").count(),
        1
    );

    std::fs::remove_file(&host).unwrap();
    let gone = f.get("k");
    assert_eq!(gone.status, 500);
    assert!(String::from_utf8_lossy(&gone.body).contains("a.txt"));

    let err =
        f.s3.0
            .link_object(
                &f.ctx,
                "b",
                "missing",
                &f.dir.join("nope"),
                LinkAttrs::default(),
            )
            .unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
}

#[test]
fn versioned_links_keep_serving_old_versions() {
    let f = Fixture::new();
    let host = f.host("a.txt", "host body");
    assert_eq!(f.call("PUT", "/bkt", "", &[], "").status, 200);
    let enable = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(f.call("PUT", "/bkt", "versioning", &[], enable).status, 200);
    f.link("k", &host);
    let old = header(&f.get("k"), "x-amz-version-id").unwrap().to_string();

    assert_eq!(f.call("PUT", "/bkt/k", "", &[], "newer").status, 200);
    assert_eq!(f.get("k").body, b"newer");
    let first = f.call("GET", "/bkt/k", &format!("versionId={old}"), &[], "");
    assert_eq!(first.body, b"host body");

    // Removing the newest version promotes the link again.
    let newest = header(&f.get("k"), "x-amz-version-id").unwrap().to_string();
    f.call("DELETE", "/bkt/k", &format!("versionId={newest}"), &[], "");
    assert_eq!(f.get("k").body, b"host body");
    assert_eq!(std::fs::read_to_string(&host).unwrap(), "host body");
}

#[test]
fn deleting_the_bucket_or_resetting_keeps_host_files() {
    let f = Fixture::new();
    let host = f.host("a.txt", "host body");
    f.link("k", &host);
    f.call("DELETE", "/bkt/k", "", &[], "");
    assert_eq!(f.call("DELETE", "/bkt", "", &[], "").status, 204);
    assert!(host.exists());

    f.link("k", &host);
    f.s3.reset().unwrap();
    assert_eq!(std::fs::read_to_string(&host).unwrap(), "host body");
}
