//! SNS subscription filter policies (`FilterPolicy`), on message attributes or the JSON body.

use std::collections::BTreeMap;

use serde_json::Value;

/// A message attribute as filters see it: its SNS data type and string value.
pub struct Attr {
    pub data_type: String,
    pub value: String,
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// One condition from a policy value list (`"x"`, `5`, `{"prefix": "a"}`, …) against a candidate value.
fn condition_matches(cond: &Value, candidates: &[Value]) -> bool {
    match cond {
        Value::Object(o) => {
            let (op, arg) = match o.iter().next() {
                Some(kv) => kv,
                None => return false,
            };
            match op.as_str() {
                "prefix" => candidates.iter().any(|c| {
                    c.as_str()
                        .zip(arg.as_str())
                        .is_some_and(|(c, p)| c.starts_with(p))
                }),
                "suffix" => candidates.iter().any(|c| {
                    c.as_str()
                        .zip(arg.as_str())
                        .is_some_and(|(c, p)| c.ends_with(p))
                }),
                "equals-ignore-case" => candidates.iter().any(|c| {
                    c.as_str()
                        .zip(arg.as_str())
                        .is_some_and(|(c, p)| c.eq_ignore_ascii_case(p))
                }),
                "anything-but" => {
                    let banned: Vec<Value> = match arg {
                        Value::Array(a) => a.clone(),
                        other => vec![other.clone()],
                    };
                    !candidates.is_empty()
                        && candidates.iter().all(|c| {
                            !banned.iter().any(|b| match b {
                                Value::Object(_) => condition_matches(b, std::slice::from_ref(c)),
                                _ => scalar_eq(b, c),
                            })
                        })
                }
                "numeric" => {
                    let Some(parts) = arg.as_array() else {
                        return false;
                    };
                    candidates.iter().filter_map(num).any(|x| {
                        parts.chunks(2).all(|pair| {
                            let (Some(op), Some(v)) = (pair[0].as_str(), pair.get(1).and_then(num))
                            else {
                                return false;
                            };
                            match op {
                                "=" => x == v,
                                ">" => x > v,
                                ">=" => x >= v,
                                "<" => x < v,
                                "<=" => x <= v,
                                _ => false,
                            }
                        })
                    })
                }
                _ => false,
            }
        }
        scalar => candidates.iter().any(|c| scalar_eq(scalar, c)),
    }
}

fn scalar_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => num(a).zip(num(b)).is_some_and(|(x, y)| x == y),
    }
}

/// `exists` conditions are evaluated against presence, everything else against values.
fn key_matches(conds: &[Value], present: bool, candidates: &[Value]) -> bool {
    conds
        .iter()
        .any(|c| match c.get("exists").and_then(Value::as_bool) {
            Some(want) => want == present,
            None => present && condition_matches(c, candidates),
        })
}

pub fn matches_attributes(policy: &Value, attrs: &BTreeMap<String, Attr>) -> bool {
    let Some(obj) = policy.as_object() else {
        return true;
    };
    obj.iter().all(|(name, conds)| {
        let Some(conds) = conds.as_array() else {
            return false;
        };
        match attrs.get(name) {
            None => key_matches(conds, false, &[]),
            Some(a) => {
                let candidates: Vec<Value> = if a.data_type == "String.Array" {
                    serde_json::from_str::<Vec<Value>>(&a.value).unwrap_or_default()
                } else if a.data_type.starts_with("Number") {
                    a.value
                        .parse::<f64>()
                        .ok()
                        .map(|n| vec![Value::from(n)])
                        .unwrap_or_default()
                } else {
                    vec![Value::String(a.value.clone())]
                };
                key_matches(conds, true, &candidates)
            }
        }
    })
}

/// Body filtering: policy keys address JSON fields (nested policies address nested objects).
pub fn matches_body(policy: &Value, body: &Value) -> bool {
    let Some(obj) = policy.as_object() else {
        return true;
    };
    obj.iter().all(|(name, conds)| {
        let field = body.get(name);
        match conds {
            Value::Array(conds) => {
                let candidates: Vec<Value> = match field {
                    Some(Value::Array(a)) => a.clone(),
                    Some(v) => vec![v.clone()],
                    None => vec![],
                };
                key_matches(conds, field.is_some(), &candidates)
            }
            nested @ Value::Object(_) => field.is_some_and(|f| matches_body(nested, f)),
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn attrs(pairs: &[(&str, &str, &str)]) -> BTreeMap<String, Attr> {
        pairs
            .iter()
            .map(|(k, t, v)| {
                (
                    k.to_string(),
                    Attr {
                        data_type: t.to_string(),
                        value: v.to_string(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn exact_prefix_and_or() {
        let a = attrs(&[("store", "String", "example_corp"), ("n", "Number", "7")]);
        assert!(matches_attributes(
            &json!({"store": ["example_corp", "x"]}),
            &a
        ));
        assert!(matches_attributes(
            &json!({"store": [{"prefix": "example"}]}),
            &a
        ));
        assert!(!matches_attributes(&json!({"store": ["other"]}), &a));
        assert!(!matches_attributes(&json!({"missing": ["x"]}), &a));
    }

    #[test]
    fn numeric_exists_and_anything_but() {
        let a = attrs(&[("n", "Number", "7"), ("s", "String", "a")]);
        assert!(matches_attributes(
            &json!({"n": [{"numeric": [">", 5, "<=", 7]}]}),
            &a
        ));
        assert!(!matches_attributes(
            &json!({"n": [{"numeric": ["<", 5]}]}),
            &a
        ));
        assert!(matches_attributes(&json!({"zz": [{"exists": false}]}), &a));
        assert!(matches_attributes(&json!({"s": [{"exists": true}]}), &a));
        assert!(matches_attributes(
            &json!({"s": [{"anything-but": ["b", "c"]}]}),
            &a
        ));
        assert!(!matches_attributes(
            &json!({"s": [{"anything-but": "a"}]}),
            &a
        ));
    }

    #[test]
    fn string_array_attribute_and_body() {
        let a = attrs(&[("tags", "String.Array", "[\"x\", \"y\"]")]);
        assert!(matches_attributes(&json!({"tags": ["y"]}), &a));
        let body = json!({"detail": {"kind": "a", "n": 3}, "ok": true});
        assert!(matches_body(
            &json!({"detail": {"kind": ["a"]}, "ok": [true]}),
            &body
        ));
        assert!(!matches_body(&json!({"detail": {"kind": ["b"]}}), &body));
    }
}
