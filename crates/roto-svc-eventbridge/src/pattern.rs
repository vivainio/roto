use roto_core::AwsError;
use serde_json::Value;

fn invalid(message: &str) -> AwsError {
    AwsError::sender(400, "InvalidEventPatternException", message)
}

pub fn parse(source: &str) -> Result<Value, AwsError> {
    let value: Value =
        serde_json::from_str(source).map_err(|_| invalid("EventPattern must be valid JSON"))?;
    validate(&value)?;
    Ok(value)
}
fn validate(value: &Value) -> Result<(), AwsError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("Event pattern must be an object"))?;
    for (key, value) in object {
        if key == "$or" {
            let values = value
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or_else(|| invalid("$or must contain patterns"))?;
            for value in values {
                validate(value)?;
            }
        } else if let Some(values) = value.as_array() {
            if values.is_empty() {
                return Err(invalid("Pattern alternatives cannot be empty"));
            }
            for alternative in values {
                if let Some(op) = alternative.as_object() {
                    if op.len() != 1 {
                        return Err(invalid("A pattern operator must contain one key"));
                    }
                    let (name, operand) = op.iter().next().unwrap();
                    let valid = match name.as_str() {
                        "prefix" | "suffix" | "equals-ignore-case" => operand.is_string(),
                        "exists" => operand.is_boolean(),
                        "anything-but" => {
                            operand.is_string()
                                || operand.is_number()
                                || operand.as_array().is_some_and(|v| {
                                    v.iter().all(|v| v.is_string() || v.is_number())
                                })
                        }
                        "numeric" => operand.as_array().is_some_and(|a| {
                            !a.is_empty()
                                && a.len() % 2 == 0
                                && a.chunks(2).all(|p| {
                                    p[0].as_str().is_some_and(|op| {
                                        matches!(op, "=" | ">" | ">=" | "<" | "<=")
                                    }) && p[1].is_number()
                                })
                        }),
                        _ => false,
                    };
                    if !valid {
                        return Err(invalid("Unsupported or malformed pattern operator"));
                    }
                } else if alternative.is_array() {
                    return Err(invalid("Nested alternative arrays are unsupported"));
                }
            }
        } else {
            validate(value)?;
        }
    }
    Ok(())
}

pub fn matches(pattern: &Value, event: &Value) -> bool {
    pattern.as_object().is_some_and(|p| {
        p.iter().all(|(key, want)| {
            if key == "$or" {
                return want
                    .as_array()
                    .is_some_and(|a| a.iter().any(|p| matches(p, event)));
            }
            let got = event.get(key);
            if let Some(alternatives) = want.as_array() {
                alternatives.iter().any(|a| alternative(a, got))
            } else {
                got.is_some_and(|got| matches(want, got))
            }
        })
    })
}
fn alternative(want: &Value, got: Option<&Value>) -> bool {
    if let Some(exists) = want.get("exists").and_then(Value::as_bool) {
        return exists == got.is_some();
    }
    let Some(got) = got else {
        return false;
    };
    if let Some(values) = got.as_array() {
        return values.iter().any(|got| alternative(want, Some(got)));
    }
    let Some(op) = want.as_object() else {
        return want == got;
    };
    let (name, value) = op.iter().next().unwrap();
    match name.as_str() {
        "prefix" => got
            .as_str()
            .is_some_and(|s| s.starts_with(value.as_str().unwrap())),
        "suffix" => got
            .as_str()
            .is_some_and(|s| s.ends_with(value.as_str().unwrap())),
        "equals-ignore-case" => got
            .as_str()
            .is_some_and(|s| s.eq_ignore_ascii_case(value.as_str().unwrap())),
        "anything-but" => value.as_array().map_or(got != value, |a| !a.contains(got)),
        "numeric" => got.as_f64().is_some_and(|n| {
            value.as_array().unwrap().chunks(2).all(|p| {
                let operand = p[1].as_f64().unwrap();
                match p[0].as_str().unwrap() {
                    "=" => n == operand,
                    ">" => n > operand,
                    ">=" => n >= operand,
                    "<" => n < operand,
                    "<=" => n <= operand,
                    _ => false,
                }
            })
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn nested_operators_arrays_and_missing_fields() {
        let p = parse(r#"{"source":["app"],"detail":{"size":[{"numeric":[">=",10,"<",20]}],"key":[{"prefix":"uploads/"}],"deleted":[{"exists":false}],"tags":["ready"]},"$or":[{"region":["us-east-1"]},{"region":["eu-west-1"]}]}"#).unwrap();
        let mut e = json!({"source":"app","region":"us-east-1","detail":{"size":12,"key":"uploads/a","tags":["other","ready"]}});
        assert!(matches(&p, &e));
        e["detail"]["size"] = json!(20);
        assert!(!matches(&p, &e));
        e["detail"]["size"] = json!(12);
        e["detail"]["deleted"] = Value::Null;
        assert!(!matches(&p, &e));
        assert!(parse(r#"{"detail":{"key":[{"wildcard":"*"}]}}"#).is_err());
    }
}
