//! Maps S3 object keys to file system paths so the data directory looks like the buckets:
//! `s3://photos/2024/cat.jpg` is the file `<root>/photos/2024/cat.jpg`.
//!
//! Rules (the database, not the file system, is the source of truth for the real key):
//! * keys split on `/`; each segment is one path component;
//! * segments that are empty, `.`/`..`, the reserved `.roto-self`, or that contain `%` or control
//!   characters are percent-escaped (`%` always becomes `%25`, so the mapping stays injective);
//! * segments longer than 200 bytes become `<prefix>~<hash>`;
//! * a trailing `/` makes the key a *folder marker*, which has no file of its own.

use sha2::{Digest, Sha256};

/// File name that holds an object whose key is also a directory prefix (`a` and `a/b`).
pub const SELF_FILE: &str = ".roto-self";
const MAX_SEGMENT: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPath {
    pub segments: Vec<String>,
    /// Key ends with `/`: a console-style folder placeholder, stored as metadata only.
    pub folder: bool,
}

pub fn key_path(key: &str) -> KeyPath {
    let folder = key.ends_with('/');
    let trimmed = if folder { &key[..key.len() - 1] } else { key };
    let segments = trimmed.split('/').map(escape_segment).collect();
    KeyPath { segments, folder }
}

fn escape_segment(seg: &str) -> String {
    match seg {
        "" => return "%_".into(),
        "." => return "%2E".into(),
        ".." => return "%2E%2E".into(),
        SELF_FILE => return "%2Eroto-self".into(),
        _ => {}
    }
    let mut out = String::with_capacity(seg.len());
    for c in seg.chars() {
        match c {
            '%' => out.push_str("%25"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => out.push_str(&format!("%{:02X}", c as u32)),
            c => out.push(c),
        }
    }
    if out.len() > MAX_SEGMENT {
        let digest = hex::encode(Sha256::digest(seg.as_bytes()));
        let mut cut = 150;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out = format!("{}~{}", &out[..cut], &digest[..16]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn p(key: &str) -> String {
        key_path(key).segments.join("/")
    }

    #[test]
    fn plain_keys_keep_their_shape() {
        assert_eq!(p("2024/cat.jpg"), "2024/cat.jpg");
        assert_eq!(p("a b/ü/日本語.txt"), "a b/ü/日本語.txt");
        assert!(key_path("folder/").folder);
        assert_eq!(p("folder/"), "folder");
    }

    #[test]
    fn dangerous_segments_are_escaped() {
        assert_eq!(p("../etc/passwd"), "%2E%2E/etc/passwd");
        assert_eq!(p("a//b"), "a/%_/b");
        assert_eq!(p("/lead"), "%_/lead");
        assert_eq!(p("./x/./y"), "%2E/x/%2E/y");
        assert_eq!(p("a/.roto-self"), "a/%2Eroto-self");
        assert_eq!(p("100%/x"), "100%25/x");
        assert_eq!(p("nul\0byte"), "nul%00byte");
    }

    #[test]
    fn long_segments_are_shortened_deterministically() {
        let long = "x".repeat(300);
        let a = p(&long);
        assert!(a.len() < 200);
        assert_eq!(a, p(&long));
        assert_ne!(a, p(&format!("{}y", long)));
    }

    #[test]
    fn mapping_is_injective_for_tricky_keys() {
        let keys = [
            "a",
            "a/b",
            "a//b",
            "a/./b",
            "a/%_/b",
            "%_",
            "",
            "%",
            "%25",
            "%2E",
            ".",
            "..",
            "%2E%2E",
            "a/b/c",
            "a%2Fb",
            ".roto-self",
            "%2Eroto-self",
            "a/.roto-self",
            "a/%2Eroto-self",
            "é",
            "e\u{301}",
        ];
        let mut seen: HashMap<String, &str> = HashMap::new();
        for k in keys {
            let path = p(k);
            if let Some(prev) = seen.insert(path.clone(), k) {
                panic!("keys {prev:?} and {k:?} both map to {path:?}");
            }
        }
    }
}
