//! `:name` parameter substitution for the RDS Data API.
//!
//! The Data API sends typed parameter values, but a plain PostgreSQL server infers parameter
//! types from the statement and expects matching binary encodings. Inlining escaped literals
//! and using the text protocol keeps the proxy independent of the server's inferred types.
use base64::{Engine, engine::general_purpose::STANDARD};
use roto_core::AwsError;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;

fn bad(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "BadRequestException", message)
}

fn string_literal(s: &str) -> Result<String, AwsError> {
    if s.contains('\0') {
        return Err(bad("String parameters cannot contain NUL characters"));
    }
    // E'' strings treat backslashes the same regardless of standard_conforming_strings.
    Ok(format!(
        "E'{}'",
        s.replace('\\', "\\\\").replace('\'', "''")
    ))
}

fn cast(hint: &str) -> Option<&'static str> {
    Some(match hint {
        "DATE" => "date",
        "DECIMAL" => "numeric",
        "JSON" => "json",
        "TIME" => "time",
        "TIMESTAMP" => "timestamp",
        "UUID" => "uuid",
        _ => return None,
    })
}

/// SQL literal for one `Field` union value.
fn literal(value: &Value, hint: Option<&str>) -> Result<String, AwsError> {
    let field = value
        .as_object()
        .filter(|f| f.len() == 1)
        .ok_or_else(|| bad("Each parameter value must set exactly one Field member"))?;
    let (kind, v) = field.iter().next().unwrap();
    let lit = match (kind.as_str(), v) {
        ("isNull", Value::Bool(true)) => return Ok("NULL".into()),
        ("booleanValue", Value::Bool(b)) => if *b { "TRUE" } else { "FALSE" }.into(),
        ("longValue", Value::Number(n)) if n.is_i64() || n.is_u64() => n.to_string(),
        ("doubleValue", Value::Number(n)) => match n.as_f64() {
            Some(f) if f.is_finite() => format!("{f:?}"),
            _ => return Err(bad("doubleValue must be finite")),
        },
        ("stringValue", Value::String(s)) => string_literal(s)?,
        ("blobValue", Value::String(s)) => {
            let bytes = STANDARD
                .decode(s)
                .map_err(|_| bad("blobValue must be base64"))?;
            let mut hex = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                write!(hex, "{b:02x}").unwrap();
            }
            format!("decode('{hex}','hex')")
        }
        ("arrayValue", _) => return Err(bad("arrayValue parameters are not supported by roto")),
        _ => return Err(bad(format!("Invalid parameter value member: {kind}"))),
    };
    Ok(match hint.and_then(cast) {
        Some(ty) => format!("({lit})::{ty}"),
        None => lit,
    })
}

/// Literals by parameter name from a `parameters` array of `SqlParameter`.
pub fn literals(parameters: &Value) -> Result<HashMap<String, String>, AwsError> {
    let mut out = HashMap::new();
    for p in parameters.as_array().map(Vec::as_slice).unwrap_or_default() {
        let name = p["name"]
            .as_str()
            .ok_or_else(|| bad("Parameters require a name"))?;
        let value = p
            .get("value")
            .ok_or_else(|| bad(format!("Parameter {name} requires a value")))?;
        out.insert(name.to_owned(), literal(value, p["typeHint"].as_str())?);
    }
    Ok(out)
}

fn ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Replaces `:name` placeholders outside strings, quoted identifiers, comments, dollar quotes
/// and `::` casts.
pub fn bind(sql: &str, params: &HashMap<String, String>) -> Result<String, AwsError> {
    let c: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        let prev = if i > 0 { c[i - 1] } else { ' ' };
        match ch {
            '\'' => {
                // E'...' strings also escape with backslashes.
                let escapes = matches!(prev, 'e' | 'E') && (i < 2 || !ident(c[i - 2]));
                out.push(ch);
                i += 1;
                while i < c.len() {
                    out.push(c[i]);
                    if escapes && c[i] == '\\' && i + 1 < c.len() {
                        out.push(c[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    if c[i - 1] == '\'' {
                        if c.get(i) == Some(&'\'') {
                            out.push('\'');
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '"' => {
                out.push(ch);
                i += 1;
                while i < c.len() {
                    out.push(c[i]);
                    i += 1;
                    if c[i - 1] == '"' {
                        if c.get(i) == Some(&'"') {
                            out.push('"');
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if c.get(i + 1) == Some(&'-') => {
                while i < c.len() && c[i] != '\n' {
                    out.push(c[i]);
                    i += 1;
                }
            }
            '/' if c.get(i + 1) == Some(&'*') => {
                let mut depth = 0;
                while i < c.len() {
                    if c[i] == '/' && c.get(i + 1) == Some(&'*') {
                        depth += 1;
                        out.push_str("/*");
                        i += 2;
                    } else if c[i] == '*' && c.get(i + 1) == Some(&'/') {
                        depth -= 1;
                        out.push_str("*/");
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        out.push(c[i]);
                        i += 1;
                    }
                }
            }
            '$' if !ident(prev) => {
                // $tag$ ... $tag$ (the tag cannot start with a digit; $1 is a positional ref).
                let mut j = i + 1;
                while j < c.len() && ident(c[j]) {
                    j += 1;
                }
                let tag_ok =
                    c.get(j) == Some(&'$') && !c.get(i + 1).is_some_and(|d| d.is_ascii_digit());
                if tag_ok {
                    let tag: Vec<char> = c[i..=j].to_vec();
                    let end = (j + 1..c.len())
                        .find(|&k| c[k..].starts_with(&tag))
                        .map(|k| k + tag.len())
                        .unwrap_or(c.len());
                    out.extend(&c[i..end]);
                    i = end;
                } else {
                    out.push(ch);
                    i += 1;
                }
            }
            ':' if c.get(i + 1) == Some(&':') => {
                out.push_str("::");
                i += 2;
            }
            ':' if prev != ':' && c.get(i + 1).is_some_and(|d| d.is_alphabetic() || *d == '_') => {
                let mut j = i + 1;
                while j < c.len() && ident(c[j]) {
                    j += 1;
                }
                let name: String = c[i + 1..j].iter().collect();
                let lit = params
                    .get(&name)
                    .ok_or_else(|| bad(format!("No value was provided for parameter :{name}")))?;
                out.push_str(lit);
                i = j;
            }
            _ => {
                out.push(ch);
                i += 1;
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(v: Value) -> HashMap<String, String> {
        literals(&v).unwrap()
    }

    #[test]
    fn substitutes_typed_values() {
        let p = params(json!([
            {"name": "id", "value": {"longValue": 7}},
            {"name": "name", "value": {"stringValue": "O'Hara \\ x"}},
            {"name": "n", "value": {"isNull": true}},
            {"name": "d", "value": {"stringValue": "2024-01-02"}, "typeHint": "DATE"},
            {"name": "b", "value": {"blobValue": "AQI="}},
        ]));
        assert_eq!(
            bind("select :id, :name, :n, :d, :b", &p).unwrap(),
            "select 7, E'O''Hara \\\\ x', NULL, (E'2024-01-02')::date, decode('0102','hex')"
        );
    }

    #[test]
    fn skips_strings_comments_casts_and_dollar_quotes() {
        let p = params(json!([{"name": "x", "value": {"longValue": 1}}]));
        let sql =
            "select ':x', \":x\", $$:x$$, $t$:x$t$, a::int, $1 -- :x\n, /* :x */ :x, E'\\':x'";
        assert_eq!(
            bind(sql, &p).unwrap(),
            "select ':x', \":x\", $$:x$$, $t$:x$t$, a::int, $1 -- :x\n, /* :x */ 1, E'\\':x'"
        );
    }

    #[test]
    fn rejects_missing_and_array_parameters() {
        assert!(bind("select :nope", &HashMap::new()).is_err());
        assert!(literals(&json!([{"name": "a", "value": {"arrayValue": {}}}])).is_err());
    }
}
