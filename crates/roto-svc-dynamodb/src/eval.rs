//! Evaluation of parsed expressions against items.

use std::cmp::Ordering;

use serde_json::{Map, Value, json};

use crate::expr::*;
use crate::number;
use crate::value::{self, Item, type_of};

pub struct Scope<'a> {
    pub values: &'a Map<String, Value>,
    pub what: &'static str,
}

const BAD_PATH: &str = "The document path provided in the update expression is invalid for update";
const BAD_OPERAND: &str = "An operand in the update expression has an incorrect data type";

/// The value at `path`, if every step exists.
pub fn lookup<'i>(item: &'i Item, path: &Path) -> Option<&'i Value> {
    let (first, rest) = path.split_first()?;
    let PathElem::Attr(name) = first else {
        return None;
    };
    let mut cur = item.get(name)?;
    for step in rest {
        cur = match step {
            PathElem::Attr(a) => cur.get("M")?.get(a)?,
            PathElem::Index(i) => cur.get("L")?.get(*i)?,
        };
    }
    Some(cur)
}

fn placeholder(scope: &Scope, name: &str) -> Result<Value, String> {
    scope
        .values
        .get(name)
        .cloned()
        .ok_or_else(|| format!("Invalid {}: An expression attribute value used in expression is not defined; attribute value: {name}", scope.what))
}

fn incorrect_type(scope: &Scope, func: &str, ty: &str) -> String {
    format!(
        "Invalid {}: Incorrect operand type for operator or function; operator or function: {func}, operand type: {ty}",
        scope.what
    )
}

/// `None` is a missing attribute.
pub fn eval_operand(scope: &Scope, item: &Item, op: &Operand) -> Result<Option<Value>, String> {
    match op {
        Operand::Path(p) => Ok(lookup(item, p).cloned()),
        Operand::Placeholder(name) => placeholder(scope, name).map(Some),
        Operand::Func(name, args) if name == "size" && args.len() == 1 => {
            let Some(v) = eval_operand(scope, item, &args[0])? else {
                return Ok(None);
            };
            let ty = type_of(&v).unwrap_or("?");
            let n = match ty {
                "S" => v["S"].as_str().map_or(0, |s| s.len()),
                "B" => value::decode_b(&v).map_or(0, |b| b.len()),
                "SS" | "NS" | "BS" => v[ty].as_array().map_or(0, Vec::len),
                "L" => v["L"].as_array().map_or(0, Vec::len),
                "M" => v["M"].as_object().map_or(0, Map::len),
                other => return Err(incorrect_type(scope, "size", other)),
            };
            Ok(Some(value::n(&n.to_string())))
        }
        Operand::Func(name, _) => Err(format!(
            "Invalid {}: Invalid function name; function: {name}",
            scope.what
        )),
    }
}

fn compare(op: CmpOp, a: &Option<Value>, b: &Option<Value>) -> bool {
    let (Some(a), Some(b)) = (a, b) else {
        // A missing attribute is "not equal" to anything and fails every other comparison.
        return op == CmpOp::Ne;
    };
    match op {
        CmpOp::Eq => value::equals(a, b),
        CmpOp::Ne => !value::equals(a, b),
        _ => match value::compare(a, b) {
            Some(o) => match op {
                CmpOp::Lt => o == Ordering::Less,
                CmpOp::Le => o != Ordering::Greater,
                CmpOp::Gt => o == Ordering::Greater,
                CmpOp::Ge => o != Ordering::Less,
                _ => unreachable!(),
            },
            None => false,
        },
    }
}

pub fn eval_cond(scope: &Scope, item: &Item, cond: &Cond) -> Result<bool, String> {
    Ok(match cond {
        Cond::And(a, b) => eval_cond(scope, item, a)? && eval_cond(scope, item, b)?,
        Cond::Or(a, b) => eval_cond(scope, item, a)? || eval_cond(scope, item, b)?,
        Cond::Not(a) => !eval_cond(scope, item, a)?,
        Cond::Compare(op, a, b) => compare(
            *op,
            &eval_operand(scope, item, a)?,
            &eval_operand(scope, item, b)?,
        ),
        Cond::Between(x, lo, hi) => {
            let (x, lo, hi) = (
                eval_operand(scope, item, x)?,
                eval_operand(scope, item, lo)?,
                eval_operand(scope, item, hi)?,
            );
            if let (Some(l), Some(h)) = (&lo, &hi) {
                if value::compare(l, h) == Some(Ordering::Greater) {
                    return Err(format!(
                        "Invalid {}: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {{{}}}, upper bound operand: AttributeValue: {{{}}}",
                        scope.what,
                        short(l),
                        short(h)
                    ));
                }
            }
            compare(CmpOp::Ge, &x, &lo) && compare(CmpOp::Le, &x, &hi)
        }
        Cond::In(x, list) => {
            let x = eval_operand(scope, item, x)?;
            for o in list {
                if compare(CmpOp::Eq, &x, &eval_operand(scope, item, o)?) {
                    return Ok(true);
                }
            }
            false
        }
        Cond::Function(name, args) => eval_function(scope, item, name, args)?,
    })
}

fn short(v: &Value) -> String {
    let ty = type_of(v).unwrap_or("?");
    format!(
        "{ty}:{}",
        v[ty]
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| v[ty].to_string())
    )
}

fn eval_function(scope: &Scope, item: &Item, name: &str, args: &[Operand]) -> Result<bool, String> {
    let arity = |n: usize| -> Result<(), String> {
        if args.len() == n {
            Ok(())
        } else {
            Err(format!(
                "Invalid {}: Incorrect number of operands for operator or function; operator or function: {name}, number of operands: {}",
                scope.what,
                args.len()
            ))
        }
    };
    match name {
        "attribute_exists" | "attribute_not_exists" => {
            arity(1)?;
            let Operand::Path(p) = &args[0] else {
                return Err(format!(
                    "Invalid {}: Operator or function requires a document path; operator or function: {name}",
                    scope.what
                ));
            };
            Ok(lookup(item, p).is_some() == (name == "attribute_exists"))
        }
        "attribute_type" => {
            arity(2)?;
            let v = eval_operand(scope, item, &args[0])?;
            let t = eval_operand(scope, item, &args[1])?;
            let Some(t) = t.as_ref().and_then(|t| t.get("S")).and_then(|s| s.as_str()) else {
                return Err(incorrect_type(scope, "attribute_type", "N"));
            };
            if !value::TYPES.contains(&t) {
                return Err(format!(
                    "One or more parameter values were invalid: Invalid attribute type name found: {t}"
                ));
            }
            Ok(v.as_ref().and_then(type_of) == Some(t))
        }
        "begins_with" => {
            arity(2)?;
            let (a, b) = (
                eval_operand(scope, item, &args[0])?,
                eval_operand(scope, item, &args[1])?,
            );
            let Some(b) = b else { return Ok(false) };
            if !matches!(type_of(&b), Some("S" | "B")) {
                return Err(incorrect_type(
                    scope,
                    "begins_with",
                    type_of(&b).unwrap_or("?"),
                ));
            }
            Ok(match (a.as_ref().and_then(type_of), type_of(&b)) {
                (Some("S"), Some("S")) => a.unwrap()["S"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with(b["S"].as_str().unwrap_or("")),
                (Some("B"), Some("B")) => value::decode_b(&a.unwrap())
                    .unwrap_or_default()
                    .starts_with(&value::decode_b(&b).unwrap_or_default()),
                _ => false,
            })
        }
        "contains" => {
            arity(2)?;
            let (a, b) = (
                eval_operand(scope, item, &args[0])?,
                eval_operand(scope, item, &args[1])?,
            );
            let (Some(a), Some(b)) = (a, b) else {
                return Ok(false);
            };
            let bt = type_of(&b).unwrap_or("?");
            Ok(match type_of(&a) {
                Some("S") if bt == "S" => a["S"]
                    .as_str()
                    .unwrap_or("")
                    .contains(b["S"].as_str().unwrap_or("")),
                Some("SS") if bt == "S" => a["SS"].as_array().is_some_and(|l| l.contains(&b["S"])),
                Some("NS") if bt == "N" => a["NS"].as_array().is_some_and(|l| {
                    l.iter()
                        .any(|x| value::equals(&value::n(x.as_str().unwrap_or("")), &b))
                }),
                Some("BS") if bt == "B" => a["BS"].as_array().is_some_and(|l| l.contains(&b["B"])),
                Some("L") => a["L"]
                    .as_array()
                    .is_some_and(|l| l.iter().any(|x| value::equals(x, &b))),
                Some("B") if bt == "B" => {
                    let (hay, needle) = (
                        value::decode_b(&a).unwrap_or_default(),
                        value::decode_b(&b).unwrap_or_default(),
                    );
                    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle.as_slice())
                }
                _ => false,
            })
        }
        other => Err(format!(
            "Invalid {}: Invalid function name; function: {other}",
            scope.what
        )),
    }
}

// ---- updates ----------------------------------------------------------------------------

fn path_text(path: &Path) -> String {
    format!(
        "[{}]",
        path.iter()
            .map(|e| match e {
                PathElem::Attr(a) => a.clone(),
                PathElem::Index(i) => format!("[{i}]"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// All touched paths must be pairwise disjoint (neither equal to nor a prefix of another).
pub fn check_overlaps(upd: &UpdateExpr) -> Result<(), String> {
    let paths: Vec<&Path> = upd
        .set
        .iter()
        .map(|(p, _)| p)
        .chain(upd.remove.iter())
        .chain(upd.add.iter().map(|(p, _)| p))
        .chain(upd.delete.iter().map(|(p, _)| p))
        .collect();
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            let n = a.len().min(b.len());
            if a[..n] == b[..n] {
                return Err(format!(
                    "Invalid UpdateExpression: Two document paths overlap with each other; must remove or rewrite one of these paths; path one: {}, path two: {}",
                    path_text(a),
                    path_text(b)
                ));
            }
        }
    }
    Ok(())
}

fn set_in_map(map: &mut Map<String, Value>, path: &[PathElem], v: Value) -> Result<(), String> {
    match path {
        [PathElem::Attr(a)] => {
            map.insert(a.clone(), v);
            Ok(())
        }
        [PathElem::Attr(a), rest @ ..] => set_in_value(map.get_mut(a).ok_or(BAD_PATH)?, rest, v),
        _ => Err(BAD_PATH.into()),
    }
}

fn set_in_value(cur: &mut Value, path: &[PathElem], v: Value) -> Result<(), String> {
    match path {
        [PathElem::Attr(_), ..] => set_in_map(
            cur.get_mut("M")
                .and_then(Value::as_object_mut)
                .ok_or(BAD_PATH)?,
            path,
            v,
        ),
        [PathElem::Index(i)] => {
            let list = cur
                .get_mut("L")
                .and_then(Value::as_array_mut)
                .ok_or(BAD_PATH)?;
            if *i < list.len() {
                list[*i] = v;
            } else {
                list.push(v);
            }
            Ok(())
        }
        [PathElem::Index(i), rest @ ..] => {
            let list = cur
                .get_mut("L")
                .and_then(Value::as_array_mut)
                .ok_or(BAD_PATH)?;
            set_in_value(list.get_mut(*i).ok_or(BAD_PATH)?, rest, v)
        }
        [] => Err(BAD_PATH.into()),
    }
}

fn remove_in_map(map: &mut Map<String, Value>, path: &[PathElem]) {
    match path {
        [PathElem::Attr(a)] => {
            map.remove(a);
        }
        [PathElem::Attr(a), rest @ ..] => {
            if let Some(c) = map.get_mut(a) {
                remove_in_value(c, rest);
            }
        }
        _ => {}
    }
}

fn remove_in_value(cur: &mut Value, path: &[PathElem]) {
    match path {
        [PathElem::Attr(_), ..] => {
            if let Some(m) = cur.get_mut("M").and_then(Value::as_object_mut) {
                remove_in_map(m, path);
            }
        }
        [PathElem::Index(i)] => {
            if let Some(l) = cur.get_mut("L").and_then(Value::as_array_mut) {
                if *i < l.len() {
                    l.remove(*i);
                }
            }
        }
        [PathElem::Index(i), rest @ ..] => {
            if let Some(c) = cur
                .get_mut("L")
                .and_then(Value::as_array_mut)
                .and_then(|l| l.get_mut(*i))
            {
                remove_in_value(c, rest);
            }
        }
        [] => {}
    }
}

fn operand_value(scope: &Scope, original: &Item, op: &Operand) -> Result<Value, String> {
    match op {
        Operand::Func(name, args) if name == "if_not_exists" && args.len() == 2 => {
            if let Operand::Path(p) = &args[0] {
                if let Some(v) = lookup(original, p) {
                    return Ok(v.clone());
                }
            }
            operand_value(scope, original, &args[1])
        }
        Operand::Func(name, args) if name == "list_append" && args.len() == 2 => {
            let (a, b) = (
                operand_value(scope, original, &args[0])?,
                operand_value(scope, original, &args[1])?,
            );
            match (
                a.get("L").and_then(Value::as_array),
                b.get("L").and_then(Value::as_array),
            ) {
                (Some(x), Some(y)) => {
                    Ok(json!({"L": x.iter().chain(y).cloned().collect::<Vec<_>>()}))
                }
                _ => Err(BAD_OPERAND.into()),
            }
        }
        Operand::Func(name, _) => Err(format!(
            "Invalid UpdateExpression: Invalid function name; function: {name}"
        )),
        other => eval_operand(scope, original, other)?.ok_or_else(|| {
            "The provided expression refers to an attribute that does not exist in the item"
                .to_string()
        }),
    }
}

fn arithmetic(a: &Value, b: &Value, subtract: bool) -> Result<Value, String> {
    let (Some(x), Some(y)) = (
        a.get("N").and_then(Value::as_str),
        b.get("N").and_then(Value::as_str),
    ) else {
        return Err(BAD_OPERAND.into());
    };
    let (x, y) = (
        number::parse(x).map_err(|_| BAD_OPERAND)?,
        number::parse(y).map_err(|_| BAD_OPERAND)?,
    );
    let r = if subtract { x - y } else { x + y };
    // Results are bounded like any stored number.
    let text = number::to_string(&r);
    number::parse(&text).map_err(|_| "Number overflow. Attempting to store a number with magnitude larger than supported range".to_string())?;
    Ok(value::n(&text))
}

/// Applies `upd` to `item`. SET expressions read from the item as it was before the update.
pub fn apply_update(scope: &Scope, item: &mut Item, upd: &UpdateExpr) -> Result<(), String> {
    check_overlaps(upd)?;
    let original = item.clone();

    let mut computed = Vec::new();
    for (path, sv) in &upd.set {
        let v = match sv {
            SetValue::Operand(o) => operand_value(scope, &original, o)?,
            SetValue::Plus(a, b) => arithmetic(
                &operand_value(scope, &original, a)?,
                &operand_value(scope, &original, b)?,
                false,
            )?,
            SetValue::Minus(a, b) => arithmetic(
                &operand_value(scope, &original, a)?,
                &operand_value(scope, &original, b)?,
                true,
            )?,
        };
        computed.push((path, v));
    }
    for (path, v) in computed {
        set_in_map(item, path, v)?;
    }
    for path in &upd.remove {
        remove_in_map(item, path);
    }
    for (path, op) in &upd.add {
        let add = operand_value(scope, &original, op)?;
        let ty = type_of(&add).unwrap_or("?");
        let existing = lookup(item, path).cloned();
        let new = match (ty, existing) {
            ("N", None) => add,
            ("N", Some(cur)) => arithmetic(&cur, &add, false)?,
            ("SS" | "NS" | "BS", None) => add,
            ("SS" | "NS" | "BS", Some(cur)) if type_of(&cur) == Some(ty) => {
                let mut all = cur[ty].as_array().cloned().unwrap_or_default();
                for x in add[ty].as_array().into_iter().flatten() {
                    if !all
                        .iter()
                        .any(|y| value::equals(&json!({ty: [y]}), &json!({ty: [x]})))
                    {
                        all.push(x.clone());
                    }
                }
                json!({ty: all})
            }
            _ => return Err(BAD_OPERAND.into()),
        };
        set_in_map(item, path, new)?;
    }
    for (path, op) in &upd.delete {
        let del = operand_value(scope, &original, op)?;
        let ty = type_of(&del).unwrap_or("?");
        if !matches!(ty, "SS" | "NS" | "BS") {
            return Err(BAD_OPERAND.into());
        }
        let Some(cur) = lookup(item, path).cloned() else {
            continue;
        };
        if type_of(&cur) != Some(ty) {
            return Err(BAD_OPERAND.into());
        }
        let remove = del[ty].as_array().cloned().unwrap_or_default();
        let kept: Vec<Value> = cur[ty]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|x| {
                !remove
                    .iter()
                    .any(|r| value::equals(&json!({ty: [r]}), &json!({ty: [x]})))
            })
            .cloned()
            .collect();
        if kept.is_empty() {
            remove_in_map(item, path);
        } else {
            set_in_map(item, path, json!({ty: kept}))?;
        }
    }
    Ok(())
}

// ---- projection -------------------------------------------------------------------------

fn insert_projected(out: &mut Map<String, Value>, path: &[PathElem], v: &Value) {
    match path {
        [PathElem::Attr(a)] => {
            out.insert(a.clone(), v.clone());
        }
        [PathElem::Attr(a), rest @ ..] => {
            let entry = out.entry(a.clone()).or_insert_with(|| match rest[0] {
                PathElem::Attr(_) => json!({"M": {}}),
                PathElem::Index(_) => json!({"L": []}),
            });
            insert_into_value(entry, rest, v);
        }
        _ => {}
    }
}

fn insert_into_value(cur: &mut Value, path: &[PathElem], v: &Value) {
    match path {
        [PathElem::Attr(_), ..] => {
            if let Some(m) = cur.get_mut("M").and_then(Value::as_object_mut) {
                insert_projected(m, path, v);
            }
        }
        [PathElem::Index(_)] => {
            if let Some(l) = cur.get_mut("L").and_then(Value::as_array_mut) {
                l.push(v.clone());
            }
        }
        [PathElem::Index(_), rest @ ..] => {
            if let Some(l) = cur.get_mut("L").and_then(Value::as_array_mut) {
                let mut child = match rest[0] {
                    PathElem::Attr(_) => json!({"M": {}}),
                    PathElem::Index(_) => json!({"L": []}),
                };
                insert_into_value(&mut child, rest, v);
                l.push(child);
            }
        }
        [] => {}
    }
}

pub fn project(item: &Item, paths: &[Path]) -> Item {
    let mut out = Map::new();
    for p in paths {
        if let Some(v) = lookup(item, p) {
            insert_projected(&mut out, p, v);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(v: Value) -> Item {
        v.as_object().unwrap().clone()
    }

    fn cond(src: &str, vals: Value, it: &Item) -> Result<bool, String> {
        let vals = vals.as_object().unwrap().clone();
        let mut env = Env::new(None, Some(&vals));
        let c = parse_condition(src, "ConditionExpression", &mut env)?;
        eval_cond(
            &Scope {
                values: &vals,
                what: "ConditionExpression",
            },
            it,
            &c,
        )
    }

    fn update(src: &str, vals: Value, it: &mut Item) -> Result<(), String> {
        let vals = vals.as_object().unwrap().clone();
        let mut env = Env::new(None, Some(&vals));
        let u = parse_update(src, &mut env)?;
        apply_update(
            &Scope {
                values: &vals,
                what: "UpdateExpression",
            },
            it,
            &u,
        )
    }

    fn sample() -> Item {
        item(json!({
            "id": {"S": "1"}, "n": {"N": "5"}, "tags": {"SS": ["a", "b"]},
            "nested": {"M": {"x": {"N": "1"}, "l": {"L": [{"S": "p"}, {"S": "q"}]}}}, "txt": {"S": "hello world"}
        }))
    }

    #[test]
    fn comparisons_and_logic() {
        let it = sample();
        assert!(cond("n = :v", json!({":v": {"N": "5.0"}}), &it).unwrap());
        assert!(
            cond(
                "n > :v AND n < :w",
                json!({":v": {"N": "4"}, ":w": {"N": "6"}}),
                &it
            )
            .unwrap()
        );
        assert!(
            cond(
                "n BETWEEN :a AND :b",
                json!({":a": {"N": "5"}, ":b": {"N": "9"}}),
                &it
            )
            .unwrap()
        );
        assert!(
            cond(
                "n IN (:a, :b)",
                json!({":a": {"N": "1"}, ":b": {"N": "5"}}),
                &it
            )
            .unwrap()
        );
        assert!(cond("ghostx <> :a", json!({":a": {"N": "1"}}), &it).unwrap());
        assert!(!cond("ghostx = :a OR ghostx > :a", json!({":a": {"N": "1"}}), &it).unwrap());
        assert!(!cond("n = :s", json!({":s": {"S": "5"}}), &it).unwrap());
        assert!(cond("NOT (n = :a)", json!({":a": {"N": "1"}}), &it).unwrap());
    }

    #[test]
    fn functions() {
        let it = sample();
        assert!(
            cond(
                "attribute_exists(nested.x) AND attribute_not_exists(nope)",
                json!({}),
                &it
            )
            .unwrap()
        );
        assert!(cond("attribute_exists(nested.l[1])", json!({}), &it).unwrap());
        assert!(!cond("attribute_exists(nested.l[2])", json!({}), &it).unwrap());
        assert!(cond("begins_with(txt, :p)", json!({":p": {"S": "hello"}}), &it).unwrap());
        assert!(
            cond(
                "contains(txt, :p) AND contains(tags, :t) AND contains(nested.l, :q)",
                json!({":p": {"S": "o w"}, ":t": {"S": "b"}, ":q": {"S": "q"}}),
                &it
            )
            .unwrap()
        );
        assert!(
            cond(
                "size(txt) = :n AND size(tags) = :two",
                json!({":n": {"N": "11"}, ":two": {"N": "2"}}),
                &it
            )
            .unwrap()
        );
        assert!(cond("attribute_type(n, :t)", json!({":t": {"S": "N"}}), &it).unwrap());
        assert!(cond("begins_with(n, :p)", json!({":p": {"N": "5"}}), &it).is_err());
    }

    #[test]
    fn set_remove_add_delete() {
        let mut it = sample();
        update("SET n = n + :one, nested.x = :two, nested.l[1] = :z, fresh = if_not_exists(fresh, :two) REMOVE txt ADD tags :more",
            json!({":one": {"N": "1"}, ":two": {"N": "2"}, ":z": {"S": "Z"}, ":more": {"SS": ["c"]}}), &mut it).unwrap();
        update(
            "DELETE tags :gone",
            json!({":gone": {"SS": ["a"]}}),
            &mut it,
        )
        .unwrap();
        assert_eq!(it["n"], json!({"N": "6"}));
        assert_eq!(it["nested"]["M"]["x"], json!({"N": "2"}));
        assert_eq!(it["nested"]["M"]["l"]["L"][1], json!({"S": "Z"}));
        assert_eq!(it["fresh"], json!({"N": "2"}));
        assert!(!it.contains_key("txt"));
        let mut tags: Vec<_> = it["tags"]["SS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        tags.sort();
        assert_eq!(tags, ["b", "c"]);
    }

    #[test]
    fn list_append_add_number_and_set_delete_to_empty() {
        let mut it = sample();
        update(
            "SET nested.l = list_append(nested.l, :x) ADD cnt :five",
            json!({":x": {"L": [{"S": "r"}]}, ":five": {"N": "5"}}),
            &mut it,
        )
        .unwrap();
        assert_eq!(it["nested"]["M"]["l"]["L"].as_array().unwrap().len(), 3);
        assert_eq!(it["cnt"], json!({"N": "5"}));
        update(
            "DELETE tags :all",
            json!({":all": {"SS": ["a", "b"]}}),
            &mut it,
        )
        .unwrap();
        assert!(!it.contains_key("tags"));
    }

    #[test]
    fn remove_list_element_shifts() {
        let mut it = sample();
        update("REMOVE nested.l[0]", json!({}), &mut it).unwrap();
        assert_eq!(it["nested"]["M"]["l"]["L"], json!([{"S": "q"}]));
    }

    #[test]
    fn update_errors() {
        let mut it = sample();
        assert!(
            update("SET a.b.c = :v", json!({":v": {"N": "1"}}), &mut it)
                .unwrap_err()
                .contains("document path provided")
        );
        assert!(
            update("SET txt = txt + :v", json!({":v": {"N": "1"}}), &mut it)
                .unwrap_err()
                .contains("incorrect data type")
        );
        assert!(
            update("SET ghost = ghost + :v", json!({":v": {"N": "1"}}), &mut it)
                .unwrap_err()
                .contains("does not exist in the item")
        );
        assert!(
            update(
                "SET nested.x = :v REMOVE nested",
                json!({":v": {"N": "1"}}),
                &mut it
            )
            .unwrap_err()
            .contains("overlap")
        );
        assert!(
            update("ADD txt :v", json!({":v": {"N": "1"}}), &mut it)
                .unwrap_err()
                .contains("incorrect data type")
        );
    }

    #[test]
    fn projection_keeps_structure() {
        let it = sample();
        let mut env = Env::new(None, None);
        let paths = parse_projection("id, nested.x, nested.l[1], gone", &mut env).unwrap();
        let out = project(&it, &paths);
        assert_eq!(out["id"], json!({"S": "1"}));
        assert_eq!(
            out["nested"],
            json!({"M": {"x": {"N": "1"}, "l": {"L": [{"S": "q"}]}}})
        );
        assert!(!out.contains_key("gone"));
    }
}
