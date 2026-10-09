//! Key schemas and order-preserving key encoding.
//!
//! Items are stored under `(table, hk, rk)` where `hk`/`rk` are the encoded hash and range key
//! values. Encodings compare bytewise in the same order DynamoDB sorts the key type: strings by
//! UTF-8 bytes, numbers numerically, binary by bytes. A table without a range key uses an empty `rk`.

use serde_json::Value;

use crate::number;
use crate::value::{self, Item, type_of};

#[derive(Debug, Clone, PartialEq)]
pub struct KeyAttr {
    pub name: String,
    /// `S`, `N` or `B`.
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeySchema {
    pub hash: KeyAttr,
    pub range: Option<KeyAttr>,
}

/// Encodes a key attribute value, checking it has the declared type.
pub fn encode(attr: &KeyAttr, v: &Value) -> Result<Vec<u8>, String> {
    let actual = type_of(v).unwrap_or("?");
    if actual != attr.ty {
        return Err(format!(
            "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
            attr.name, attr.ty, actual
        ));
    }
    match actual {
        "S" => {
            let s = v["S"].as_str().unwrap_or("");
            if s.is_empty() {
                return Err(format!(
                    "One or more parameter values were invalid: An AttributeValue may not contain an empty string. Key: {}",
                    attr.name
                ));
            }
            Ok(s.as_bytes().to_vec())
        }
        "N" => number::parse(v["N"].as_str().unwrap_or(""))
            .map(|n| number::sort_key(&n))
            .map_err(|_| {
                format!(
                    "The parameter cannot be converted to a numeric value: {}",
                    v["N"].as_str().unwrap_or("")
                )
            }),
        "B" => {
            let b = value::decode_b(v).unwrap_or_default();
            if b.is_empty() {
                return Err(format!(
                    "One or more parameter values were invalid: An AttributeValue may not contain an empty binary type. Key: {}",
                    attr.name
                ));
            }
            Ok(b)
        }
        _ => unreachable!(),
    }
}

impl KeySchema {
    pub fn attrs(&self) -> Vec<&KeyAttr> {
        std::iter::once(&self.hash)
            .chain(self.range.as_ref())
            .collect()
    }

    /// Encoded `(hash, range)` of an item; errors if a key attribute is missing or mistyped.
    pub fn item_keys(&self, item: &Item) -> Result<(Vec<u8>, Vec<u8>), String> {
        let get = |a: &KeyAttr| -> Result<Vec<u8>, String> {
            let v = item.get(&a.name).ok_or_else(|| {
                format!(
                    "One or more parameter values were invalid: Missing the key {} in the item",
                    a.name
                )
            })?;
            encode(a, v)
        };
        let hk = get(&self.hash)?;
        let rk = match &self.range {
            Some(r) => get(r)?,
            None => Vec::new(),
        };
        Ok((hk, rk))
    }

    /// Validates a `Key` parameter (exactly the key attributes) and returns its encodings.
    pub fn request_key(&self, key: &Item) -> Result<(Vec<u8>, Vec<u8>), String> {
        let expected = self.attrs().len();
        if key.len() != expected || self.attrs().iter().any(|a| !key.contains_key(&a.name)) {
            return Err("The provided key element does not match the schema".into());
        }
        self.item_keys(key)
    }

    /// Only the key attributes of an item.
    pub fn key_of(&self, item: &Item) -> Item {
        self.attrs()
            .iter()
            .filter_map(|a| item.get(&a.name).map(|v| (a.name.clone(), v.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> KeySchema {
        KeySchema {
            hash: KeyAttr {
                name: "pk".into(),
                ty: "S".into(),
            },
            range: Some(KeyAttr {
                name: "sk".into(),
                ty: "N".into(),
            }),
        }
    }

    fn item(v: Value) -> Item {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn encodes_and_orders() {
        let s = schema();
        let (h1, r1) = s
            .item_keys(&item(json!({"pk": {"S": "a"}, "sk": {"N": "9"}})))
            .unwrap();
        let (h2, r2) = s
            .item_keys(&item(json!({"pk": {"S": "a"}, "sk": {"N": "10"}})))
            .unwrap();
        assert_eq!(h1, h2);
        assert!(r1 < r2, "numeric order, not text order");
    }

    #[test]
    fn rejects_bad_keys() {
        let s = schema();
        assert!(
            s.item_keys(&item(json!({"pk": {"S": "a"}})))
                .unwrap_err()
                .contains("Missing the key sk")
        );
        assert!(
            s.item_keys(&item(json!({"pk": {"N": "1"}, "sk": {"N": "1"}})))
                .unwrap_err()
                .contains("Type mismatch for key pk expected: S actual: N")
        );
        assert!(
            s.item_keys(&item(json!({"pk": {"S": ""}, "sk": {"N": "1"}})))
                .unwrap_err()
                .contains("empty string")
        );
        assert!(
            s.request_key(&item(json!({"pk": {"S": "a"}})))
                .unwrap_err()
                .contains("does not match the schema")
        );
        assert!(
            s.request_key(&item(
                json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "x": {"S": "y"}})
            ))
            .is_err()
        );
    }
}
