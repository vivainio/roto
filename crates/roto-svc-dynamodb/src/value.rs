//! Attribute values in DynamoDB JSON form (`{"S":"x"}`, `{"N":"1"}`, `{"M":{…}}`), kept as
//! `serde_json::Value` so items round-trip through the wire format and SQLite unchanged.

use std::cmp::Ordering;

use serde_json::{Map, Value, json};

use crate::number;

pub type Item = Map<String, Value>;

pub const TYPES: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// The single type tag of an attribute value, if it is well-formed.
pub fn type_of(v: &Value) -> Option<&'static str> {
    let obj = v.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let k = obj.keys().next()?;
    TYPES.iter().copied().find(|t| t == k)
}

pub fn n(text: &str) -> Value {
    json!({ "N": text })
}

fn as_number(v: &Value) -> Option<number::Number> {
    number::parse(v.get("N")?.as_str()?).ok()
}

fn set_numbers(v: &Value) -> Option<Vec<number::Number>> {
    v.get("NS")?
        .as_array()?
        .iter()
        .map(|x| number::parse(x.as_str()?).ok())
        .collect()
}

/// Ordering for the comparable scalar types (S, N, B); `None` when types differ or aren't ordered.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (type_of(a)?, type_of(b)?) {
        ("S", "S") => Some(a["S"].as_str()?.as_bytes().cmp(b["S"].as_str()?.as_bytes())),
        ("N", "N") => Some(number::cmp(&as_number(a)?, &as_number(b)?)),
        ("B", "B") => Some(decode_b(a)?.cmp(&decode_b(b)?)),
        _ => None,
    }
}

pub fn decode_b(v: &Value) -> Option<Vec<u8>> {
    roto_protocol::base64::decode(v.get("B")?.as_str()?)
}

/// DynamoDB equality: numbers compare numerically, sets ignore order, containers recurse.
pub fn equals(a: &Value, b: &Value) -> bool {
    let (Some(ta), Some(tb)) = (type_of(a), type_of(b)) else {
        return false;
    };
    if ta != tb {
        return false;
    }
    match ta {
        "N" => matches!((as_number(a), as_number(b)), (Some(x), Some(y)) if x == y),
        "NS" => match (set_numbers(a), set_numbers(b)) {
            (Some(x), Some(y)) => x.len() == y.len() && x.iter().all(|i| y.contains(i)),
            _ => false,
        },
        "SS" | "BS" => {
            let (Some(x), Some(y)) = (a[ta].as_array(), b[ta].as_array()) else {
                return false;
            };
            x.len() == y.len() && x.iter().all(|i| y.contains(i))
        }
        "M" => {
            let (Some(x), Some(y)) = (a["M"].as_object(), b["M"].as_object()) else {
                return false;
            };
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| equals(v, w)))
        }
        "L" => {
            let (Some(x), Some(y)) = (a["L"].as_array(), b["L"].as_array()) else {
                return false;
            };
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| equals(v, w))
        }
        _ => a == b,
    }
}

/// Size in bytes as DynamoDB counts it (item limit 400 KB, `size()` function).
pub fn size(v: &Value) -> usize {
    match type_of(v) {
        Some("S") => v["S"].as_str().map_or(0, str::len),
        Some("N") => number::parse(v["N"].as_str().unwrap_or("0")).map_or(0, |n| {
            let digits = number::to_string(&n)
                .chars()
                .filter(char::is_ascii_digit)
                .count();
            digits / 2 + 1 + usize::from(n.sign() == bigdecimal::num_bigint::Sign::Minus)
        }),
        Some("B") => decode_b(v).map_or(0, |b| b.len()),
        Some("SS") => v["SS"].as_array().map_or(0, |a| {
            a.iter().map(|x| x.as_str().map_or(0, str::len)).sum()
        }),
        Some("NS") => v["NS"].as_array().map_or(0, |a| {
            a.iter().map(|x| size(&n(x.as_str().unwrap_or("0")))).sum()
        }),
        Some("BS") => v["BS"].as_array().map_or(0, |a| {
            a.iter()
                .map(|x| {
                    roto_protocol::base64::decode(x.as_str().unwrap_or("")).map_or(0, |b| b.len())
                })
                .sum()
        }),
        Some("M") => v["M"].as_object().map_or(0, |m| {
            m.iter().map(|(k, x)| k.len() + size(x) + 3).sum::<usize>() + 3
        }),
        Some("L") => v["L"]
            .as_array()
            .map_or(0, |l| l.iter().map(|x| size(x) + 1).sum::<usize>() + 3),
        Some("NULL") | Some("BOOL") => 1,
        _ => 0,
    }
}

pub fn item_size(item: &Item) -> usize {
    item.iter().map(|(k, v)| k.len() + size(v)).sum()
}

/// Structural validation of a value coming from a client.
pub fn validate(v: &Value) -> Result<(), String> {
    let ty = type_of(v).ok_or("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")?;
    match ty {
        "S" if v["S"].is_string() => Ok(()),
        "N" => {
            let text = v["N"].as_str().ok_or("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")?;
            number::parse(text).map(|_| ()).map_err(|e| match e {
                number::NumberError::Invalid => format!("The parameter cannot be converted to a numeric value: {text}"),
                number::NumberError::TooPrecise => "Attempting to store more than 38 significant digits in a Number".into(),
                number::NumberError::OutOfRange => "Number overflow. Attempting to store a number with magnitude larger than supported range".to_string(),
            })
        }
        "B" if decode_b(v).is_some() => Ok(()),
        "SS" | "NS" | "BS" => {
            let items = v[ty].as_array().ok_or("An string set may not be empty")?;
            if items.is_empty() {
                return Err(
                    "One or more parameter values were invalid: An string set  may not be empty"
                        .into(),
                );
            }
            let mut seen = Vec::new();
            for x in items {
                if seen.contains(&x) {
                    return Err("One or more parameter values were invalid: Input collection contains duplicates".into());
                }
                seen.push(x);
                if ty == "NS" {
                    number::parse(x.as_str().unwrap_or("")).map_err(|_| {
                        format!(
                            "The parameter cannot be converted to a numeric value: {}",
                            x.as_str().unwrap_or("")
                        )
                    })?;
                }
            }
            Ok(())
        }
        "M" => v["M"]
            .as_object()
            .ok_or("invalid map")?
            .values()
            .try_for_each(validate),
        "L" => v["L"]
            .as_array()
            .ok_or("invalid list")?
            .iter()
            .try_for_each(validate),
        "NULL" | "BOOL" => Ok(()),
        _ => Err(
            "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
                .into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> Value {
        json!({ "S": text })
    }

    #[test]
    fn type_detection_and_validation() {
        assert_eq!(type_of(&s("x")), Some("S"));
        assert_eq!(type_of(&json!({"S": "x", "N": "1"})), None);
        assert!(validate(&n("1.5")).is_ok());
        assert!(validate(&n("abc")).is_err());
        assert!(validate(&json!({"SS": []})).is_err());
        assert!(validate(&json!({"SS": ["a", "a"]})).is_err());
        assert!(validate(&json!({"M": {"a": {"L": [{"N": "1"}]}}})).is_ok());
        assert!(validate(&json!({"M": {"a": {"N": "x"}}})).is_err());
    }

    #[test]
    fn comparison_and_equality() {
        assert_eq!(compare(&n("10"), &n("9.5")), Some(Ordering::Greater));
        assert_eq!(compare(&s("a"), &s("b")), Some(Ordering::Less));
        assert_eq!(compare(&s("a"), &n("1")), None);
        assert!(equals(&n("1.0"), &n("1")));
        assert!(equals(
            &json!({"SS": ["a", "b"]}),
            &json!({"SS": ["b", "a"]})
        ));
        assert!(!equals(
            &json!({"L": [{"N": "1"}, {"N": "2"}]}),
            &json!({"L": [{"N": "2"}, {"N": "1"}]})
        ));
    }
}
