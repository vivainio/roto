//! Query and Scan.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use roto_core::rusqlite::params;
use roto_core::{AwsError, RequestContext};
use serde_json::Value;

use crate::eval::{Scope, eval_cond, project};
use crate::expr::*;
use crate::generated::*;
use crate::keys::{self, KeyAttr, KeySchema};
use crate::service::*;
use crate::table::*;
use crate::value::{self, Item};

/// One candidate item with its position in the table and in the index being read.
struct Cand {
    item: Item,
    hk: Vec<u8>,
    rk: Vec<u8>,
    ihk: Vec<u8>,
    irk: Vec<u8>,
}

#[derive(Debug)]
enum RangeCond {
    Cmp(CmpOp, Value),
    Between(Value, Value),
    BeginsWith(Value),
}

#[derive(Debug)]
struct KeyCond {
    hash: Value,
    range: Option<RangeCond>,
}

fn placeholder_value(
    op: &Operand,
    values: &serde_json::Map<String, Value>,
) -> Result<Value, AwsError> {
    match op {
        Operand::Placeholder(p) => values.get(p).cloned().ok_or_else(|| ve(format!("Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: {p}"))),
        _ => Err(ve("Invalid KeyConditionExpression: Syntax error; key conditions must compare a key attribute to a value")),
    }
}

fn leaf_attr(op: &Operand) -> Option<&str> {
    match op {
        Operand::Path(p) if p.len() == 1 => match &p[0] {
            PathElem::Attr(a) => Some(a),
            _ => None,
        },
        _ => None,
    }
}

fn flatten<'c>(c: &'c Cond, out: &mut Vec<&'c Cond>) -> Result<(), AwsError> {
    match c {
        Cond::And(a, b) => {
            flatten(a, out)?;
            flatten(b, out)
        }
        Cond::Or(_, _) => Err(ve(
            "Invalid KeyConditionExpression: Operator not supported: OR",
        )),
        Cond::Not(_) => Err(ve(
            "Invalid KeyConditionExpression: Operator not supported: NOT",
        )),
        other => {
            out.push(other);
            Ok(())
        }
    }
}

fn type_check(attr: &KeyAttr, v: &Value) -> Result<(), AwsError> {
    if value::type_of(v) != Some(attr.ty.as_str()) {
        return Err(invalid_param(
            "Condition parameter type does not match schema type",
        ));
    }
    Ok(())
}

fn key_cond_from_expression(
    c: &Cond,
    schema: &KeySchema,
    values: &serde_json::Map<String, Value>,
) -> Result<KeyCond, AwsError> {
    let mut leaves = Vec::new();
    flatten(c, &mut leaves)?;
    let (mut hash, mut range): (Option<Value>, Option<RangeCond>) = (None, None);
    for leaf in leaves {
        let (attr, cond) = match leaf {
            Cond::Compare(op, a, b) => match (leaf_attr(a), leaf_attr(b)) {
                (Some(n), _) => (n, RangeCond::Cmp(*op, placeholder_value(b, values)?)),
                (None, Some(n)) => {
                    // `:v < attr` reads as `attr > :v`
                    let flipped = match op {
                        CmpOp::Lt => CmpOp::Gt,
                        CmpOp::Le => CmpOp::Ge,
                        CmpOp::Gt => CmpOp::Lt,
                        CmpOp::Ge => CmpOp::Le,
                        o => *o,
                    };
                    (n, RangeCond::Cmp(flipped, placeholder_value(a, values)?))
                }
                _ => {
                    return Err(ve(
                        "Invalid KeyConditionExpression: Syntax error; key conditions must compare a key attribute to a value",
                    ));
                }
            },
            Cond::Between(a, lo, hi) => {
                let n = leaf_attr(a)
                    .ok_or_else(|| ve("Invalid KeyConditionExpression: Syntax error"))?;
                (
                    n,
                    RangeCond::Between(
                        placeholder_value(lo, values)?,
                        placeholder_value(hi, values)?,
                    ),
                )
            }
            Cond::Function(name, args) if name == "begins_with" && args.len() == 2 => {
                let n = leaf_attr(&args[0])
                    .ok_or_else(|| ve("Invalid KeyConditionExpression: Syntax error"))?;
                (
                    n,
                    RangeCond::BeginsWith(placeholder_value(&args[1], values)?),
                )
            }
            Cond::Function(name, _) => {
                return Err(ve(format!(
                    "Invalid KeyConditionExpression: Invalid function name; function: {name}"
                )));
            }
            _ => return Err(ve("Invalid KeyConditionExpression: Syntax error")),
        };
        if attr == schema.hash.name {
            match cond {
                RangeCond::Cmp(CmpOp::Eq, v) if hash.is_none() => {
                    type_check(&schema.hash, &v)?;
                    hash = Some(v);
                }
                RangeCond::Cmp(CmpOp::Eq, _) => {
                    return Err(ve(
                        "Invalid KeyConditionExpression: Multiple conditions on the same key",
                    ));
                }
                _ => return Err(ve("Query key condition not supported")),
            }
        } else if schema.range.as_ref().is_some_and(|r| r.name == attr) {
            if range.is_some() {
                return Err(ve(
                    "Invalid KeyConditionExpression: The KeyConditionExpression contains multiple conditions on the same key attribute",
                ));
            }
            let r = schema.range.as_ref().unwrap();
            match &cond {
                RangeCond::Cmp(_, v) | RangeCond::BeginsWith(v) => type_check(r, v)?,
                RangeCond::Between(a, b) => {
                    type_check(r, a)?;
                    type_check(r, b)?;
                }
            }
            if let RangeCond::BeginsWith(v) = &cond {
                if value::type_of(v) == Some("N") {
                    return Err(ve(
                        "Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: N",
                    ));
                }
            }
            range = Some(cond);
        } else {
            return Err(ve(format!(
                "Query condition missed key schema element: {}",
                schema.hash.name
            )));
        }
    }
    let hash = hash.ok_or_else(|| {
        ve(format!(
            "Query condition missed key schema element: {}",
            schema.hash.name
        ))
    })?;
    Ok(KeyCond { hash, range })
}

fn key_cond_from_legacy(
    conds: &BTreeMap<String, Condition>,
    schema: &KeySchema,
) -> Result<KeyCond, AwsError> {
    let mut hash = None;
    let mut range = None;
    for (name, c) in conds {
        let list: Vec<Value> = c.attribute_value_list.iter().map(|v| v.0.clone()).collect();
        if *name == schema.hash.name {
            if c.comparison_operator != "EQ" {
                return Err(ve("Query key condition not supported"));
            }
            let v = list.first().cloned().ok_or_else(|| {
                invalid_param("Invalid number of argument(s) for the EQ ComparisonOperator")
            })?;
            type_check(&schema.hash, &v)?;
            hash = Some(v);
        } else if let Some(r) = schema.range.as_ref().filter(|r| r.name == *name) {
            let rc = match (c.comparison_operator.as_str(), list.as_slice()) {
                ("EQ", [v]) => RangeCond::Cmp(CmpOp::Eq, v.clone()),
                ("LE", [v]) => RangeCond::Cmp(CmpOp::Le, v.clone()),
                ("LT", [v]) => RangeCond::Cmp(CmpOp::Lt, v.clone()),
                ("GE", [v]) => RangeCond::Cmp(CmpOp::Ge, v.clone()),
                ("GT", [v]) => RangeCond::Cmp(CmpOp::Gt, v.clone()),
                ("BEGINS_WITH", [v]) => RangeCond::BeginsWith(v.clone()),
                ("BETWEEN", [a, b]) => RangeCond::Between(a.clone(), b.clone()),
                (op, _) => return Err(ve(format!("Query key condition not supported: {op}"))),
            };
            match &rc {
                RangeCond::Cmp(_, v) | RangeCond::BeginsWith(v) => type_check(r, v)?,
                RangeCond::Between(a, b) => {
                    type_check(r, a)?;
                    type_check(r, b)?;
                }
            }
            range = Some(rc);
        } else {
            return Err(ve("Query condition missed key schema element"));
        }
    }
    let hash = hash.ok_or_else(|| {
        ve(format!(
            "Query condition missed key schema element: {}",
            schema.hash.name
        ))
    })?;
    Ok(KeyCond { hash, range })
}

fn range_matches(rc: &RangeCond, v: &Value) -> bool {
    use Ordering::*;
    match rc {
        RangeCond::Cmp(op, x) => match value::compare(v, x) {
            Some(o) => match op {
                CmpOp::Eq => o == Equal,
                CmpOp::Ne => o != Equal,
                CmpOp::Lt => o == Less,
                CmpOp::Le => o != Greater,
                CmpOp::Gt => o == Greater,
                CmpOp::Ge => o != Less,
            },
            None => false,
        },
        RangeCond::Between(a, b) => {
            value::compare(v, a).is_some_and(|o| o != Less)
                && value::compare(v, b).is_some_and(|o| o != Greater)
        }
        RangeCond::BeginsWith(p) => match (value::type_of(v), value::type_of(p)) {
            (Some("S"), Some("S")) => v["S"]
                .as_str()
                .unwrap_or("")
                .starts_with(p["S"].as_str().unwrap_or("")),
            (Some("B"), Some("B")) => value::decode_b(v)
                .unwrap_or_default()
                .starts_with(&value::decode_b(p).unwrap_or_default()),
            _ => false,
        },
    }
}

/// Smallest byte string greater than every string starting with `prefix` (None if unbounded).
fn prefix_upper(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut up = prefix.to_vec();
    while let Some(last) = up.pop() {
        if last < 0xFF {
            up.push(last + 1);
            return Some(up);
        }
    }
    None
}

fn parse_item(text: &str) -> Item {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

fn index_attrs_visible(
    item: &Item,
    proj: &Projection,
    table: &KeySchema,
    index: &KeySchema,
) -> Item {
    match proj.projection_type.as_deref() {
        Some("KEYS_ONLY") | Some("INCLUDE") => {
            let mut keep: Vec<&str> = table
                .attrs()
                .iter()
                .chain(index.attrs().iter())
                .map(|a| a.name.as_str())
                .collect();
            if proj.projection_type.as_deref() == Some("INCLUDE") {
                keep.extend(proj.non_key_attributes.iter().map(String::as_str));
            }
            item.iter()
                .filter(|(k, _)| keep.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        _ => item.clone(),
    }
}

/// Everything shared by Query and Scan after the candidates are known.
struct Run<'a> {
    filter: Option<Cond>,
    legacy_filter: &'a BTreeMap<String, Condition>,
    conditional_operator: &'a Option<String>,
    values: &'a serde_json::Map<String, Value>,
    projection: Option<Vec<Path>>,
    attributes_to_get: &'a [String],
    count_only: bool,
    limit: Option<usize>,
}

struct Page {
    items: Vec<Item>,
    count: i32,
    scanned: i32,
    last_key: Option<Item>,
}

fn run_candidates(
    cands: Vec<Cand>,
    run: &Run,
    table: &KeySchema,
    index: Option<&KeySchema>,
) -> Result<Page, AwsError> {
    let mut page = Page {
        items: Vec::new(),
        count: 0,
        scanned: 0,
        last_key: None,
    };
    let total = cands.len();
    for (n, c) in cands.into_iter().enumerate() {
        if run.limit.is_some_and(|l| n >= l) {
            // More candidates exist beyond the limit: report where evaluation stopped.
            break;
        }
        page.scanned += 1;
        let mut keep = true;
        if let Some(f) = &run.filter {
            keep = eval_cond(
                &Scope {
                    values: run.values,
                    what: "FilterExpression",
                },
                &c.item,
                f,
            )
            .map_err(ve)?;
        } else if !run.legacy_filter.is_empty() {
            let mut results = Vec::new();
            for (name, cond) in run.legacy_filter {
                let list: Vec<Value> = cond
                    .attribute_value_list
                    .iter()
                    .map(|v| v.0.clone())
                    .collect();
                results.push(legacy_op(
                    &cond.comparison_operator,
                    c.item.get(name),
                    &list,
                )?);
            }
            keep = if run.conditional_operator.as_deref() == Some("OR") {
                results.iter().any(|r| *r)
            } else {
                results.iter().all(|r| *r)
            };
        }
        if keep {
            page.count += 1;
            if !run.count_only {
                let out = match &run.projection {
                    Some(paths) => project(&c.item, paths),
                    None if !run.attributes_to_get.is_empty() => c
                        .item
                        .iter()
                        .filter(|(k, _)| run.attributes_to_get.contains(k))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    None => c.item.clone(),
                };
                page.items.push(out);
            }
        }
        let is_last_allowed = run.limit.is_some_and(|l| n + 1 == l);
        if is_last_allowed && n + 1 < total {
            let mut key = table.key_of(&c.item);
            if let Some(ix) = index {
                key.extend(ix.key_of(&c.item));
            }
            page.last_key = Some(key);
        }
    }
    Ok(page)
}

fn limit_of(limit: Option<i32>) -> Result<Option<usize>, AwsError> {
    match limit {
        Some(l) if l < 1 => Err(ve(format!(
            "1 validation error detected: Value '{l}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        ))),
        Some(l) => Ok(Some(l as usize)),
        None => Ok(None),
    }
}

fn select_mode(
    select: &Option<String>,
    has_projection: bool,
    index: bool,
) -> Result<bool, AwsError> {
    match select.as_deref() {
        None => Ok(false),
        Some("COUNT") => Ok(true),
        Some("ALL_ATTRIBUTES") if has_projection => Err(ve(
            "Cannot specify the AttributesToGet when choosing to get ALL_ATTRIBUTES",
        )),
        Some("ALL_ATTRIBUTES") if index => Err(ve(
            "One or more parameter values were invalid: ALL_ATTRIBUTES can be used for index only if all attributes are projected",
        )),
        Some("ALL_ATTRIBUTES") | Some("ALL_PROJECTED_ATTRIBUTES") => Ok(false),
        Some("SPECIFIC_ATTRIBUTES") if has_projection => Ok(false),
        Some("SPECIFIC_ATTRIBUTES") => Err(ve(
            "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES",
        )),
        Some(other) => Err(ve(format!(
            "1 validation error detected: Value '{other}' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
        ))),
    }
}

/// Reads all items of the table in key order (base table) or sorted by index keys.
fn gather(
    tx: &roto_core::rusqlite::Transaction,
    t: &Table,
    table_schema: &KeySchema,
    index: Option<(&KeySchema, &Projection)>,
    key_cond: Option<&KeyCond>,
    forward: bool,
    start: Option<&Item>,
) -> Result<Vec<Cand>, AwsError> {
    let mut cands: Vec<Cand> = Vec::new();
    match index {
        None => {
            let mut sql = String::from("SELECT hk, rk, item FROM items WHERE table_id = ?1");
            let mut binds: Vec<Vec<u8>> = Vec::new();
            if let Some(kc) = key_cond {
                sql.push_str(" AND hk = ?2");
                binds.push(keys::encode(&table_schema.hash, &kc.hash).map_err(ve)?);
                if let (Some(rc), Some(rattr)) = (&kc.range, &table_schema.range) {
                    let enc = |v: &Value| keys::encode(rattr, v).map_err(ve);
                    match rc {
                        RangeCond::Cmp(op, v) => {
                            let sym = match op {
                                CmpOp::Eq => "=",
                                CmpOp::Ne => "<>",
                                CmpOp::Lt => "<",
                                CmpOp::Le => "<=",
                                CmpOp::Gt => ">",
                                CmpOp::Ge => ">=",
                            };
                            sql.push_str(&format!(" AND rk {sym} ?{}", binds.len() + 2));
                            binds.push(enc(v)?);
                        }
                        RangeCond::Between(a, b) => {
                            sql.push_str(&format!(
                                " AND rk >= ?{} AND rk <= ?{}",
                                binds.len() + 2,
                                binds.len() + 3
                            ));
                            binds.push(enc(a)?);
                            binds.push(enc(b)?);
                        }
                        RangeCond::BeginsWith(p) => {
                            let lo = enc(p)?;
                            sql.push_str(&format!(" AND rk >= ?{}", binds.len() + 2));
                            if let Some(up) = prefix_upper(&lo) {
                                sql.push_str(&format!(" AND rk < ?{}", binds.len() + 3));
                                binds.push(lo);
                                binds.push(up);
                            } else {
                                binds.push(lo);
                            }
                        }
                    }
                }
            }
            if let Some(s) = start {
                let (shk, srk) = table_schema
                    .item_keys(s)
                    .map_err(|_| ve("The provided starting key is invalid"))?;
                let n = binds.len() + 2;
                if key_cond.is_some() {
                    sql.push_str(&format!(" AND rk {} ?{n}", if forward { ">" } else { "<" }));
                    binds.push(srk);
                    let _ = shk;
                } else {
                    sql.push_str(&format!(
                        " AND (hk, rk) {} (?{n}, ?{})",
                        if forward { ">" } else { "<" },
                        n + 1
                    ));
                    binds.push(shk);
                    binds.push(srk);
                }
            }
            sql.push_str(if forward {
                " ORDER BY hk, rk"
            } else {
                " ORDER BY hk DESC, rk DESC"
            });
            let mut stmt = tx.prepare(&sql)?;
            let mut all: Vec<&dyn roto_core::rusqlite::ToSql> = vec![&t.id];
            all.extend(binds.iter().map(|b| b as &dyn roto_core::rusqlite::ToSql));
            let rows = stmt.query_map(all.as_slice(), |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (hk, rk, text) = row?;
                cands.push(Cand {
                    item: parse_item(&text),
                    ihk: hk.clone(),
                    irk: rk.clone(),
                    hk,
                    rk,
                });
            }
        }
        Some((ischema, proj)) => {
            let mut stmt = tx.prepare("SELECT hk, rk, item FROM items WHERE table_id = ?1")?;
            let rows = stmt.query_map(params![t.id], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (hk, rk, text) = row?;
                let item = parse_item(&text);
                // Items missing an index key attribute are not in the index.
                let Ok((ihk, irk)) = ischema.item_keys(&item) else {
                    continue;
                };
                if let Some(kc) = key_cond {
                    let hv = &item[&ischema.hash.name];
                    if !value::equals(hv, &kc.hash) {
                        continue;
                    }
                    if let (Some(rc), Some(r)) = (&kc.range, &ischema.range) {
                        if !range_matches(rc, &item[&r.name]) {
                            continue;
                        }
                    }
                }
                let visible = index_attrs_visible(&item, proj, table_schema, ischema);
                cands.push(Cand {
                    item: visible,
                    hk,
                    rk,
                    ihk,
                    irk,
                });
            }
            cands.sort_by(|a, b| {
                (&a.ihk, &a.irk, &a.hk, &a.rk).cmp(&(&b.ihk, &b.irk, &b.hk, &b.rk))
            });
            if !forward {
                cands.reverse();
            }
            if let Some(s) = start {
                let (ihk, irk) = ischema
                    .item_keys(s)
                    .map_err(|_| ve("The provided starting key is invalid"))?;
                let (hk, rk) = table_schema
                    .item_keys(s)
                    .map_err(|_| ve("The provided starting key is invalid"))?;
                let pos = (ihk, irk, hk, rk);
                cands.retain(|c| {
                    let here = (&c.ihk, &c.irk, &c.hk, &c.rk);
                    let there = (&pos.0, &pos.1, &pos.2, &pos.3);
                    if forward { here > there } else { here < there }
                });
            }
        }
    }
    Ok(cands)
}

fn last_key_out(k: Option<Item>) -> BTreeMap<String, roto_protocol::JsonValue> {
    k.map(|k| from_item(&k)).unwrap_or_default()
}

pub(crate) fn query(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: QueryInput,
) -> Result<QueryOutput, AwsError> {
    if i.key_condition_expression.is_none() && i.key_conditions.is_empty() {
        return Err(ve(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.",
        ));
    }
    d.db.transaction(|tx| {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let table_schema = t.schema()?;
        let (schema, proj) = schema_for_index(&t, &i.index_name)?;
        let values = values_map(&i.expression_attribute_values)?;
        let mut env = Env::new(some(&i.expression_attribute_names), some_map(&values));

        if i.key_condition_expression.is_some() && !i.key_conditions.is_empty() {
            return Err(ve("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"));
        }
        if i.key_condition_expression.is_none() && i.key_conditions.is_empty() {
            return Err(ve("Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."));
        }
        if i.filter_expression.is_some() && !i.query_filter.is_empty() {
            return Err(ve("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter} Expression parameters: {FilterExpression}"));
        }
        let key_expr = i.key_condition_expression.as_deref().map(|s| parse_condition(s, "KeyConditionExpression", &mut env)).transpose().map_err(ve)?;
        let filter = i.filter_expression.as_deref().map(|s| parse_condition(s, "FilterExpression", &mut env)).transpose().map_err(ve)?;
        let projection = i.projection_expression.as_deref().map(|s| parse_projection(s, &mut env)).transpose().map_err(ve)?;
        if projection.is_some() && !i.attributes_to_get.is_empty() {
            return Err(ve("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"));
        }
        finish_env(
            &env,
            &[
                ("KeyConditionExpression", i.key_condition_expression.is_some()),
                ("FilterExpression", i.filter_expression.is_some()),
                ("ProjectionExpression", i.projection_expression.is_some()),
            ],
        )?;
        let key_cond = match &key_expr {
            Some(c) => key_cond_from_expression(c, &schema, &values)?,
            None => key_cond_from_legacy(&i.key_conditions, &schema)?,
        };
        let count_only = select_mode(&i.select, projection.is_some() || !i.attributes_to_get.is_empty(), i.index_name.is_some())?;
        let forward = i.scan_index_forward.unwrap_or(true);
        let start = (!i.exclusive_start_key.is_empty()).then(|| to_item(&i.exclusive_start_key));
        if i.index_name.is_some() && i.consistent_read == Some(true) && t.gsis.iter().any(|g| Some(&g.index_name) == i.index_name.as_ref()) {
            return Err(ve("Consistent reads are not supported on global secondary indexes"));
        }
        let index = proj.as_ref().map(|p| (&schema, p));
        let cands = gather(tx, &t, &table_schema, index, Some(&key_cond), forward, start.as_ref())?;
        let run = Run {
            filter,
            legacy_filter: &i.query_filter,
            conditional_operator: &i.conditional_operator,
            values: &values,
            projection,
            attributes_to_get: &i.attributes_to_get,
            count_only,
            limit: limit_of(i.limit)?,
        };
        let page = run_candidates(cands, &run, &table_schema, index.map(|(s, _)| s))?;
        Ok(QueryOutput {
            count: Some(page.count),
            scanned_count: Some(page.scanned),
            items: page.items.iter().map(from_item).collect(),
            last_evaluated_key: last_key_out(page.last_key),
            consumed_capacity: None,
        })
    })
}

fn segment_of(hk: &[u8], total: i32) -> i32 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in hk {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % total as u64) as i32
}

pub(crate) fn scan(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: ScanInput,
) -> Result<ScanOutput, AwsError> {
    d.db.transaction(|tx| {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let table_schema = t.schema()?;
        let (schema, proj) = schema_for_index(&t, &i.index_name)?;
        let values = values_map(&i.expression_attribute_values)?;
        let mut env = Env::new(some(&i.expression_attribute_names), some_map(&values));
        if i.filter_expression.is_some() && !i.scan_filter.is_empty() {
            return Err(ve("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"));
        }
        let filter = i.filter_expression.as_deref().map(|s| parse_condition(s, "FilterExpression", &mut env)).transpose().map_err(ve)?;
        let projection = i.projection_expression.as_deref().map(|s| parse_projection(s, &mut env)).transpose().map_err(ve)?;
        if projection.is_some() && !i.attributes_to_get.is_empty() {
            return Err(ve("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"));
        }
        finish_env(&env, &[("FilterExpression", i.filter_expression.is_some()), ("ProjectionExpression", i.projection_expression.is_some())])?;
        if i.segment.is_some() != i.total_segments.is_some() {
            return Err(ve("The TotalSegments and Segment parameters are both required for parallel scan"));
        }
        if let (Some(s), Some(n)) = (i.segment, i.total_segments) {
            if n < 1 || s < 0 || s >= n {
                return Err(ve(format!("The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: {s} Total Segments: {n}")));
            }
        }
        let count_only = select_mode(&i.select, projection.is_some() || !i.attributes_to_get.is_empty(), i.index_name.is_some())?;
        let start = (!i.exclusive_start_key.is_empty()).then(|| to_item(&i.exclusive_start_key));
        let index = proj.as_ref().map(|p| (&schema, p));
        let mut cands = gather(tx, &t, &table_schema, index, None, true, start.as_ref())?;
        if let Some(n) = i.total_segments {
            let s = i.segment.unwrap_or(0);
            cands.retain(|c| segment_of(&c.hk, n) == s);
        }
        let run = Run {
            filter,
            legacy_filter: &i.scan_filter,
            conditional_operator: &i.conditional_operator,
            values: &values,
            projection,
            attributes_to_get: &i.attributes_to_get,
            count_only,
            limit: limit_of(i.limit)?,
        };
        let page = run_candidates(cands, &run, &table_schema, index.map(|(s, _)| s))?;
        Ok(ScanOutput {
            count: Some(page.count),
            scanned_count: Some(page.scanned),
            items: page.items.iter().map(from_item).collect(),
            last_evaluated_key: last_key_out(page.last_key),
            consumed_capacity: None,
        })
    })
}
