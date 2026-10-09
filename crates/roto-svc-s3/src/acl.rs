//! Access control lists. Stored as the `AccessControlPolicy` XML document; never enforced.

use roto_core::AwsError;
use roto_protocol::restxml::{XmlRead, parse_xml};
use roto_protocol::{XmlValue, XmlWriter};

use crate::generated::*;
use crate::service::{OWNER_ID, OWNER_NAME};

const ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
const AUTHENTICATED_USERS: &str = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
const LOG_DELIVERY: &str = "http://acs.amazonaws.com/groups/s3/LogDelivery";

fn owner_grantee() -> Grantee {
    Grantee {
        id: Some(OWNER_ID.into()),
        display_name: Some(OWNER_NAME.into()),
        r#type: "CanonicalUser".into(),
        ..Default::default()
    }
}

fn group(uri: &str) -> Grantee {
    Grantee {
        uri: Some(uri.into()),
        r#type: "Group".into(),
        ..Default::default()
    }
}

fn grant(grantee: Grantee, permission: &str) -> Grant {
    Grant {
        grantee: Some(grantee),
        permission: Some(permission.into()),
    }
}

pub(crate) fn owner() -> Owner {
    Owner {
        display_name: Some(OWNER_NAME.into()),
        id: Some(OWNER_ID.into()),
    }
}

pub(crate) fn default_policy() -> AccessControlPolicy {
    AccessControlPolicy {
        owner: Some(owner()),
        grants: vec![grant(owner_grantee(), "FULL_CONTROL")],
    }
}

fn canned(name: &str) -> Result<AccessControlPolicy, AwsError> {
    let mut p = default_policy();
    match name {
        "private" => {}
        "public-read" => p.grants.push(grant(group(ALL_USERS), "READ")),
        "public-read-write" => {
            p.grants.push(grant(group(ALL_USERS), "READ"));
            p.grants.push(grant(group(ALL_USERS), "WRITE"));
        }
        "authenticated-read" => p.grants.push(grant(group(AUTHENTICATED_USERS), "READ")),
        "log-delivery-write" => {
            p.grants.push(grant(group(LOG_DELIVERY), "WRITE"));
            p.grants.push(grant(group(LOG_DELIVERY), "READ_ACP"));
        }
        "aws-exec-read" | "bucket-owner-read" | "bucket-owner-full-control" => {}
        other => {
            return Err(
                AwsError::sender(400, "InvalidArgument", "Invalid canned ACL")
                    .with("ArgumentName", "x-amz-acl")
                    .with("ArgumentValue", other),
            );
        }
    }
    Ok(p)
}

/// `id="…", uri="…", emailAddress="…"` as sent in `x-amz-grant-*` headers.
fn header_grantees(h: &str) -> Result<Vec<Grantee>, AwsError> {
    h.split(',')
        .map(|part| {
            let (k, v) = part.split_once('=').ok_or_else(|| {
                AwsError::sender(400, "InvalidArgument", "Argument format not recognized")
            })?;
            let v = v.trim().trim_matches('"').to_string();
            Ok(match k.trim().to_ascii_lowercase().as_str() {
                "id" => Grantee {
                    id: Some(v),
                    r#type: "CanonicalUser".into(),
                    ..Default::default()
                },
                // moto's tests (and some clients) spell it `url`.
                "uri" | "url" => Grantee {
                    uri: Some(v),
                    r#type: "Group".into(),
                    ..Default::default()
                },
                "emailaddress" => Grantee {
                    email_address: Some(v),
                    r#type: "AmazonCustomerByEmail".into(),
                    ..Default::default()
                },
                _ => {
                    return Err(AwsError::sender(
                        400,
                        "InvalidArgument",
                        "Argument format not recognized",
                    ));
                }
            })
        })
        .collect()
}

/// Body policy wins over `x-amz-grant-*` headers, which win over a canned ACL.
pub(crate) fn build(
    canned_acl: &Option<String>,
    body: Option<&AccessControlPolicy>,
    headers: [(&str, &Option<String>); 5],
) -> Result<Option<AccessControlPolicy>, AwsError> {
    if let Some(b) = body {
        let mut p = b.clone();
        p.owner.get_or_insert_with(owner);
        return Ok(Some(p));
    }
    let given: Vec<_> = headers.iter().filter(|(_, v)| v.is_some()).collect();
    if !given.is_empty() && canned_acl.is_some() {
        return Err(AwsError::sender(
            400,
            "InvalidRequest",
            "Specifying both Canned ACLs and Header Grants is not allowed",
        ));
    }
    if !given.is_empty() {
        let mut p = AccessControlPolicy {
            owner: Some(owner()),
            grants: Vec::new(),
        };
        for (perm, value) in given {
            for g in header_grantees(value.as_deref().unwrap_or_default())? {
                p.grants.push(grant(g, perm));
            }
        }
        return Ok(Some(p));
    }
    canned_acl.as_deref().map(canned).transpose()
}

pub(crate) fn to_xml(p: &AccessControlPolicy) -> String {
    let mut w = XmlWriter::new();
    p.write(&mut w, "AccessControlPolicy");
    w.finish()
}

pub(crate) fn from_xml(s: &str) -> Option<AccessControlPolicy> {
    let doc = parse_xml(s.as_bytes()).ok()?;
    AccessControlPolicy::read_xml(doc.root_element()).ok()
}

/// Stored document, or the owner-only default.
pub(crate) fn stored_or_default(doc: Option<&str>) -> AccessControlPolicy {
    doc.and_then(from_xml).unwrap_or_else(default_policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canned_public_read_adds_all_users() {
        let p = build(
            &Some("public-read".into()),
            None,
            [
                ("", &None),
                ("", &None),
                ("", &None),
                ("", &None),
                ("", &None),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.grants.len(), 2);
        assert_eq!(
            p.grants[1].grantee.as_ref().unwrap().uri.as_deref(),
            Some(ALL_USERS)
        );
        assert!(canned("nonsense").is_err());
    }

    #[test]
    fn header_grants_parse_ids_and_uris() {
        let read = Some(format!("uri=\"{ALL_USERS}\", id=\"abc\""));
        let none = None;
        let p = build(
            &None,
            None,
            [
                ("READ", &read),
                ("", &none),
                ("", &none),
                ("", &none),
                ("", &none),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.grants.len(), 2);
        assert_eq!(
            p.grants[1].grantee.as_ref().unwrap().id.as_deref(),
            Some("abc")
        );
    }

    #[test]
    fn round_trips_through_xml() {
        let p = canned("public-read-write").unwrap();
        let xml = to_xml(&p);
        assert!(xml.contains("xsi:type=\"Group\""), "{xml}");
        assert_eq!(from_xml(&xml).unwrap(), p);
    }
}
