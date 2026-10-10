use std::collections::BTreeMap;
use std::sync::Arc;

use crate::schema::*;
use crate::table::Table;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::JsonValue;
use serde_json::{Map, Value};

use crate::eval::{Scope, apply_update, eval_cond, project};
use crate::expr::*;
use crate::generated::*;
use crate::keys::KeySchema;
use crate::table::*;
use crate::value::{self, Item};

const MAX_ITEM_SIZE: usize = 400 * 1024;

pub struct DynamoDb {
    pub(crate) db: Arc<Db>,
}

impl DynamoDb {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("dynamodb", crate::MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(items::table).execute(tx)?;
            diesel::delete(tables::table).execute(tx)?;
            diesel::delete(backup_items::table).execute(tx)?;
            diesel::delete(backups::table).execute(tx)?;
            Ok(())
        })
    }
}

pub(crate) fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ---- conversions ------------------------------------------------------------------------

pub(crate) fn to_item(m: &BTreeMap<String, JsonValue>) -> Item {
    m.iter().map(|(k, v)| (k.clone(), v.0.clone())).collect()
}

pub(crate) fn from_item(i: &Item) -> BTreeMap<String, JsonValue> {
    i.iter()
        .map(|(k, v)| (k.clone(), JsonValue(v.clone())))
        .collect()
}

pub(crate) fn values_map(m: &BTreeMap<String, JsonValue>) -> Result<Map<String, Value>, AwsError> {
    let mut out = Map::new();
    for (k, v) in m {
        value::validate(&v.0).map_err(|e| invalid_param(&e))?;
        out.insert(k.clone(), v.0.clone());
    }
    Ok(out)
}

pub(crate) fn some<T>(m: &BTreeMap<String, T>) -> Option<&BTreeMap<String, T>> {
    (!m.is_empty()).then_some(m)
}

pub(crate) fn some_map(m: &Map<String, Value>) -> Option<&Map<String, Value>> {
    (!m.is_empty()).then_some(m)
}

/// Rejects names/values given without expressions, and unused ones.
pub(crate) fn finish_env(env: &Env, exprs: &[(&str, bool)]) -> Result<(), AwsError> {
    if !exprs.iter().any(|(_, present)| *present) {
        let null: Vec<&str> = exprs.iter().map(|(n, _)| *n).collect();
        let null = null.join(" and ");
        if env.names.is_some() {
            return Err(ve(format!(
                "ExpressionAttributeNames can only be specified when using expressions: {null} is null"
            )));
        }
        if env.values.is_some() {
            return Err(ve(format!(
                "ExpressionAttributeValues can only be specified when using expressions: {null} is null"
            )));
        }
    }
    match env.unused_error() {
        Some(e) => Err(ve(e)),
        None => Ok(()),
    }
}

fn conditional_failed() -> AwsError {
    AwsError::sender(
        400,
        "ConditionalCheckFailedException",
        "The conditional request failed",
    )
}

// ---- legacy conditions (Expected / QueryFilter / ScanFilter) ----------------------------

pub(crate) fn legacy_op(
    op: &str,
    actual: Option<&Value>,
    list: &[Value],
) -> Result<bool, AwsError> {
    let arg = |n: usize| -> Result<&Value, AwsError> {
        list.get(n).ok_or_else(|| {
            invalid_param("Invalid number of argument(s) for the comparison operator")
        })
    };
    let cmp = |want: fn(std::cmp::Ordering) -> bool| -> Result<bool, AwsError> {
        Ok(match actual {
            Some(a) => value::compare(a, arg(0)?).is_some_and(want),
            None => false,
        })
    };
    Ok(match op {
        "EQ" => actual.is_some_and(|a| value::equals(a, arg(0).unwrap_or(&Value::Null))),
        "NE" => !actual.is_some_and(|a| value::equals(a, arg(0).unwrap_or(&Value::Null))),
        "LE" => cmp(|o| o.is_le())?,
        "LT" => cmp(|o| o.is_lt())?,
        "GE" => cmp(|o| o.is_ge())?,
        "GT" => cmp(|o| o.is_gt())?,
        "NOT_NULL" => actual.is_some(),
        "NULL" => actual.is_none(),
        "CONTAINS" | "NOT_CONTAINS" => {
            let found = match (actual, list.first()) {
                (Some(a), Some(b)) => match (value::type_of(a), value::type_of(b)) {
                    (Some("S"), Some("S")) => a["S"]
                        .as_str()
                        .unwrap_or("")
                        .contains(b["S"].as_str().unwrap_or("")),
                    (Some("SS"), Some("S")) => {
                        a["SS"].as_array().is_some_and(|l| l.contains(&b["S"]))
                    }
                    (Some("NS"), Some("N")) => a["NS"].as_array().is_some_and(|l| {
                        l.iter()
                            .any(|x| value::equals(&value::n(x.as_str().unwrap_or("")), b))
                    }),
                    (Some("BS"), Some("B")) => {
                        a["BS"].as_array().is_some_and(|l| l.contains(&b["B"]))
                    }
                    (Some("L"), _) => a["L"]
                        .as_array()
                        .is_some_and(|l| l.iter().any(|x| value::equals(x, b))),
                    _ => false,
                },
                _ => false,
            };
            found == (op == "CONTAINS")
        }
        "BEGINS_WITH" => match (actual, list.first()) {
            (Some(a), Some(b)) => match (value::type_of(a), value::type_of(b)) {
                (Some("S"), Some("S")) => a["S"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with(b["S"].as_str().unwrap_or("")),
                (Some("B"), Some("B")) => value::decode_b(a)
                    .unwrap_or_default()
                    .starts_with(&value::decode_b(b).unwrap_or_default()),
                _ => false,
            },
            _ => false,
        },
        "IN" => actual.is_some_and(|a| list.iter().any(|x| value::equals(a, x))),
        "BETWEEN" => match actual {
            Some(a) => {
                value::compare(a, arg(0)?).is_some_and(|o| o.is_ge())
                    && value::compare(a, arg(1)?).is_some_and(|o| o.is_le())
            }
            None => false,
        },
        other => {
            return Err(invalid_param(&format!(
                "Unsupported comparison operator {other}"
            )));
        }
    })
}

fn all_or_any(results: Vec<bool>, conditional_operator: &Option<String>) -> bool {
    if conditional_operator.as_deref() == Some("OR") {
        results.iter().any(|r| *r)
    } else {
        results.iter().all(|r| *r)
    }
}

fn eval_expected(
    item: Option<&Item>,
    expected: &BTreeMap<String, ExpectedAttributeValue>,
    op: &Option<String>,
) -> Result<bool, AwsError> {
    let mut results = Vec::new();
    for (name, e) in expected {
        let actual = item.and_then(|i| i.get(name));
        let list: Vec<Value> = e.attribute_value_list.iter().map(|v| v.0.clone()).collect();
        let r = match (&e.comparison_operator, e.exists, &e.value) {
            (Some(op), _, _) => {
                if e.value.is_some() && !list.is_empty() {
                    return Err(invalid_param(
                        "Exists and ComparisonOperator cannot be used together",
                    ));
                }
                let list = if list.is_empty() {
                    e.value.iter().map(|v| v.0.clone()).collect()
                } else {
                    list
                };
                legacy_op(op, actual, &list)?
            }
            (None, Some(false), None) => actual.is_none(),
            (None, Some(false), Some(_)) => {
                return Err(invalid_param(
                    "Value or AttributeValueList may not be used when Exists is false",
                ));
            }
            (None, Some(true), None) => {
                return Err(invalid_param("Value must be provided when Exists is true"));
            }
            (None, _, Some(v)) => actual.is_some_and(|a| value::equals(a, &v.0)),
            (None, None, None) => return Err(invalid_param("Value or Exists must be provided")),
        };
        results.push(r);
    }
    Ok(all_or_any(results, op))
}

// ---- storage helpers ---------------------------------------------------------------------

pub(crate) fn load_item_pub(
    tx: &mut SqliteConnection,
    t: &Table,
    hk: &[u8],
    rk: &[u8],
) -> Result<Option<Item>, AwsError> {
    let text: Option<String> = items::table
        .filter(items::table_id.eq(&(t.id)))
        .filter(items::hk.eq(&(hk)))
        .filter(items::rk.eq(&(rk)))
        .select(items::item)
        .first::<String>(tx)
        .optional()?;
    Ok(text
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.as_object().cloned()))
}

fn store_item(
    tx: &mut SqliteConnection,
    t: &Table,
    hk: &[u8],
    rk: &[u8],
    item: &Item,
) -> Result<(), AwsError> {
    diesel::insert_into(items::table)
        .values((
            items::table_id.eq(&(t.id)),
            items::hk.eq(&(hk)),
            items::rk.eq(&(rk)),
            items::item.eq(&(Value::Object(item.clone()).to_string())),
        ))
        .on_conflict((items::table_id, items::hk, items::rk))
        .do_update()
        .set(items::item.eq(diesel::upsert::excluded(items::item)))
        .execute(tx)?;
    Ok(())
}

fn remove_item(tx: &mut SqliteConnection, t: &Table, hk: &[u8], rk: &[u8]) -> Result<(), AwsError> {
    diesel::delete(
        items::table
            .filter(items::table_id.eq(&(t.id)))
            .filter(items::hk.eq(&(hk)))
            .filter(items::rk.eq(&(rk))),
    )
    .execute(tx)?;
    Ok(())
}

/// Checks every value of a client-supplied item and the item size.
fn validate_item(item: &Item) -> Result<(), AwsError> {
    validate_item_for(item, false)
}

fn validate_item_for(item: &Item, update: bool) -> Result<(), AwsError> {
    for (k, v) in item {
        if k.is_empty() {
            return Err(invalid_param("Empty attribute name"));
        }
        value::validate(v).map_err(|e| invalid_param(&e))?;
    }
    if value::item_size(item) > MAX_ITEM_SIZE {
        return Err(ve(if update {
            "Item size to update has exceeded the maximum allowed size"
        } else {
            "Item size has exceeded the maximum allowed size"
        }));
    }
    Ok(())
}

/// Index key attributes must have the declared type when present in an item.
fn check_index_key_types(t: &Table, item: &Item) -> Result<(), AwsError> {
    let defs: BTreeMap<&str, &str> = t
        .attr_defs
        .iter()
        .map(|a| (a.attribute_name.as_str(), a.attribute_type.as_str()))
        .collect();
    let keys = t
        .gsis
        .iter()
        .flat_map(|g| g.key_schema.iter())
        .chain(t.lsis.iter().flat_map(|l| l.key_schema.iter()));
    for k in keys {
        if let (Some(v), Some(want)) = (
            item.get(&k.attribute_name),
            defs.get(k.attribute_name.as_str()),
        ) {
            let actual = value::type_of(v).unwrap_or("?");
            if actual != *want {
                return Err(invalid_param(&format!(
                    "Type mismatch for Index Key {} Expected: {} Actual: {} IndexName: {}",
                    k.attribute_name,
                    want,
                    actual,
                    t.gsis
                        .iter()
                        .find(|g| g
                            .key_schema
                            .iter()
                            .any(|x| x.attribute_name == k.attribute_name))
                        .map(|g| g.index_name.clone())
                        .or_else(|| t
                            .lsis
                            .iter()
                            .find(|l| l
                                .key_schema
                                .iter()
                                .any(|x| x.attribute_name == k.attribute_name))
                            .map(|l| l.index_name.clone()))
                        .unwrap_or_default()
                )));
            }
        }
    }
    Ok(())
}

fn return_values(rv: &Option<String>, allowed: &[&str]) -> Result<String, AwsError> {
    const ENUM: [&str; 5] = ["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"];
    let v = rv.clone().unwrap_or_else(|| "NONE".into());
    if allowed.contains(&v.as_str()) {
        Ok(v)
    } else if ENUM.contains(&v.as_str()) {
        Err(ve("Return values set to invalid value"))
    } else {
        Err(ve(format!(
            "1 validation error detected: Value '{v}' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"
        )))
    }
}

// ---- expression plumbing -----------------------------------------------------------------

fn parse_cond_opt(
    src: &Option<String>,
    what: &'static str,
    env: &mut Env,
) -> Result<Option<Cond>, AwsError> {
    src.as_deref()
        .map(|s| parse_condition(s, what, env))
        .transpose()
        .map_err(ve)
}

fn eval_cond_expr(
    c: &Option<Cond>,
    values: &Map<String, Value>,
    item: Option<&Item>,
) -> Result<bool, AwsError> {
    match c {
        None => Ok(true),
        Some(c) => {
            let empty = Item::new();
            eval_cond(
                &Scope {
                    values,
                    what: "ConditionExpression",
                },
                item.unwrap_or(&empty),
                c,
            )
            .map_err(ve)
        }
    }
}

/// Chooses between `ConditionExpression` and legacy `Expected`, which cannot be mixed.
fn check_condition(
    existing: Option<&Item>,
    parsed: &Option<Cond>,
    values: &Map<String, Value>,
    expected: &BTreeMap<String, ExpectedAttributeValue>,
    conditional_operator: &Option<String>,
    expression_present: bool,
) -> Result<(), AwsError> {
    if expression_present && !expected.is_empty() {
        return Err(ve(
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}",
        ));
    }
    if conditional_operator.is_some() && expected.is_empty() {
        return Err(ve("ConditionalOperator can only be used with Expected"));
    }
    let ok = if !expected.is_empty() {
        eval_expected(existing, expected, conditional_operator)?
    } else {
        eval_cond_expr(parsed, values, existing)?
    };
    if ok {
        Ok(())
    } else {
        Err(conditional_failed())
    }
}

pub(crate) fn project_pub(
    item: &Item,
    projection: &Option<String>,
    attributes_to_get: &[String],
    names: Option<&BTreeMap<String, String>>,
) -> Result<Item, AwsError> {
    if let Some(p) = projection {
        if !attributes_to_get.is_empty() {
            return Err(ve(
                "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}",
            ));
        }
        let mut env = Env::new(names, None);
        let paths = parse_projection(p, &mut env).map_err(ve)?;
        return Ok(project(item, &paths));
    }
    if !attributes_to_get.is_empty() {
        return Ok(item
            .iter()
            .filter(|(k, _)| attributes_to_get.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect());
    }
    Ok(item.clone())
}

fn check_projection_names(
    projection: &Option<String>,
    names: Option<&BTreeMap<String, String>>,
) -> Result<(), AwsError> {
    if let Some(p) = projection {
        let mut env = Env::new(names, None);
        parse_projection(p, &mut env).map_err(ve)?;
        if let Some(n) = names {
            if let Some(unused) = n.keys().find(|k| !env.used_names.contains(*k)) {
                let _ = unused;
            }
        }
    }
    Ok(())
}

// ---- legacy AttributeUpdates -> UpdateExpr ----------------------------------------------

fn legacy_update(
    updates: &BTreeMap<String, AttributeValueUpdate>,
    key_names: &[String],
    existing: &Item,
) -> Result<(UpdateExpr, Map<String, Value>), AwsError> {
    let mut u = UpdateExpr::default();
    let mut vals = Map::new();
    for (i, (name, upd)) in updates.iter().enumerate() {
        if key_names.contains(name) {
            return Err(invalid_param(&format!(
                "Cannot update attribute {name}. This attribute is part of the key"
            )));
        }
        let path = vec![PathElem::Attr(name.clone())];
        let ph = format!(":legacy{i}");
        let action = upd.action.clone().unwrap_or_else(|| "PUT".into());
        match (action.as_str(), &upd.value) {
            ("PUT", Some(v)) => {
                value::validate(&v.0).map_err(|e| invalid_param(&e))?;
                vals.insert(ph.clone(), v.0.clone());
                u.set
                    .push((path, SetValue::Operand(Operand::Placeholder(ph))));
            }
            ("ADD", Some(v)) => {
                vals.insert(ph.clone(), v.0.clone());
                // Legacy ADD on a list appends the given elements.
                let is_list = value::type_of(&v.0) == Some("L");
                if is_list && existing.contains_key(name) {
                    u.set.push((
                        path.clone(),
                        SetValue::Operand(Operand::Func(
                            "list_append".into(),
                            vec![Operand::Path(path), Operand::Placeholder(ph)],
                        )),
                    ));
                } else if is_list {
                    u.set
                        .push((path, SetValue::Operand(Operand::Placeholder(ph))));
                } else {
                    u.add.push((path, Operand::Placeholder(ph)));
                }
            }
            ("DELETE", None) => u.remove.push(path),
            ("DELETE", Some(v)) => {
                vals.insert(ph.clone(), v.0.clone());
                u.delete.push((path, Operand::Placeholder(ph)));
            }
            ("PUT" | "ADD", None) => {
                return Err(invalid_param(
                    "Only DELETE action is allowed when no attribute value is specified",
                ));
            }
            (other, _) => {
                return Err(ve(format!(
                    "1 validation error detected: Value '{other}' at 'attributeUpdates.{name}.member.action' failed to satisfy constraint: Member must satisfy enum value set: [ADD, PUT, DELETE]"
                )));
            }
        }
    }
    Ok((u, vals))
}

fn touched_paths(u: &UpdateExpr) -> Vec<Path> {
    u.set
        .iter()
        .map(|(p, _)| p.clone())
        .chain(u.remove.iter().cloned())
        .chain(u.add.iter().map(|(p, _)| p.clone()))
        .chain(u.delete.iter().map(|(p, _)| p.clone()))
        .collect()
}

fn touched_attrs(u: &UpdateExpr) -> Vec<String> {
    touched_paths(u)
        .iter()
        .filter_map(|p| match p.first() {
            Some(PathElem::Attr(a)) => Some(a.clone()),
            _ => None,
        })
        .collect()
}

fn consumed(table: &str, requested: &Option<String>, units: f64) -> Option<ConsumedCapacity> {
    match requested.as_deref() {
        Some("TOTAL") | Some("INDEXES") => Some(ConsumedCapacity {
            table_name: Some(table.to_string()),
            capacity_units: Some(units),
            table: (requested.as_deref() == Some("INDEXES")).then(|| Capacity {
                capacity_units: Some(units),
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => None,
    }
}

impl DynamoDb {
    pub(crate) fn put_item_tx(
        &self,
        tx: &mut SqliteConnection,
        ctx: &RequestContext,
        i: &PutItemInput,
    ) -> Result<(PutItemOutput, Option<Item>), AwsError> {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let schema = t.schema()?;
        let item = to_item(&i.item);
        let rv = return_values(&i.return_values, &["NONE", "ALL_OLD"])?;
        let values = values_map(&i.expression_attribute_values)?;
        let mut env = Env::new(some(&i.expression_attribute_names), some_map(&values));
        let cond = parse_cond_opt(&i.condition_expression, "ConditionExpression", &mut env)?;
        finish_env(
            &env,
            &[("ConditionExpression", i.condition_expression.is_some())],
        )?;
        validate_item(&item)?;
        let (hk, rk) = schema.item_keys(&item).map_err(ve)?;
        check_index_key_types(&t, &item)?;
        let old = load_item_pub(tx, &t, &hk, &rk)?;
        check_condition(
            old.as_ref(),
            &cond,
            &values,
            &i.expected,
            &i.conditional_operator,
            i.condition_expression.is_some(),
        )?;
        store_item(tx, &t, &hk, &rk, &item)?;
        let out = PutItemOutput {
            attributes: if rv == "ALL_OLD" {
                old.as_ref().map(from_item).unwrap_or_default()
            } else {
                BTreeMap::new()
            },
            consumed_capacity: consumed(&t.name, &i.return_consumed_capacity, 1.0),
            ..Default::default()
        };
        Ok((out, old))
    }

    pub(crate) fn delete_item_tx(
        &self,
        tx: &mut SqliteConnection,
        ctx: &RequestContext,
        i: &DeleteItemInput,
    ) -> Result<DeleteItemOutput, AwsError> {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let schema = t.schema()?;
        let rv = return_values(&i.return_values, &["NONE", "ALL_OLD"])?;
        let values = values_map(&i.expression_attribute_values)?;
        let mut env = Env::new(some(&i.expression_attribute_names), some_map(&values));
        let cond = parse_cond_opt(&i.condition_expression, "ConditionExpression", &mut env)?;
        finish_env(
            &env,
            &[("ConditionExpression", i.condition_expression.is_some())],
        )?;
        let key = to_item(&i.key);
        let (hk, rk) = schema.request_key(&key).map_err(ve)?;
        let old = load_item_pub(tx, &t, &hk, &rk)?;
        check_condition(
            old.as_ref(),
            &cond,
            &values,
            &i.expected,
            &i.conditional_operator,
            i.condition_expression.is_some(),
        )?;
        remove_item(tx, &t, &hk, &rk)?;
        Ok(DeleteItemOutput {
            attributes: if rv == "ALL_OLD" {
                old.as_ref().map(from_item).unwrap_or_default()
            } else {
                BTreeMap::new()
            },
            consumed_capacity: consumed(&t.name, &i.return_consumed_capacity, 1.0),
            ..Default::default()
        })
    }

    pub(crate) fn update_item_tx(
        &self,
        tx: &mut SqliteConnection,
        ctx: &RequestContext,
        i: &UpdateItemInput,
    ) -> Result<UpdateItemOutput, AwsError> {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let schema = t.schema()?;
        let rv = return_values(
            &i.return_values,
            &["NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"],
        )?;
        let mut values = values_map(&i.expression_attribute_values)?;
        let mut env = Env::new(some(&i.expression_attribute_names), some_map(&values));
        let cond = parse_cond_opt(&i.condition_expression, "ConditionExpression", &mut env)?;
        let update = i
            .update_expression
            .as_deref()
            .map(|s| parse_update(s, &mut env))
            .transpose()
            .map_err(ve)?;
        finish_env(
            &env,
            &[
                ("UpdateExpression", i.update_expression.is_some()),
                ("ConditionExpression", i.condition_expression.is_some()),
            ],
        )?;
        if update.is_some() && !i.attribute_updates.is_empty() {
            return Err(ve(
                "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributeUpdates} Expression parameters: {UpdateExpression}",
            ));
        }
        let key = to_item(&i.key);
        let (hk, rk) = schema.request_key(&key).map_err(ve)?;
        let old = load_item_pub(tx, &t, &hk, &rk)?;
        check_condition(
            old.as_ref(),
            &cond,
            &values,
            &i.expected,
            &i.conditional_operator,
            i.condition_expression.is_some(),
        )?;

        let key_names: Vec<String> = schema.attrs().iter().map(|a| a.name.clone()).collect();
        let (upd, extra) = match update {
            Some(u) => (u, Map::new()),
            None => legacy_update(
                &i.attribute_updates,
                &key_names,
                old.as_ref().unwrap_or(&key),
            )?,
        };
        values.extend(extra);
        // Key attributes cannot be changed.
        for name in touched_attrs(&upd) {
            if key_names.contains(&name) {
                return Err(invalid_param(&format!(
                    "Cannot update attribute {name}. This attribute is part of the key"
                )));
            }
        }
        let mut item = old.clone().unwrap_or_else(|| key.clone());
        apply_update(
            &Scope {
                values: &values,
                what: "UpdateExpression",
            },
            &mut item,
            &upd,
        )
        .map_err(ve)?;
        validate_item_for(&item, true)?;
        check_index_key_types(&t, &item)?;
        store_item(tx, &t, &hk, &rk, &item)?;

        let attributes = match rv.as_str() {
            "ALL_OLD" => old.as_ref().map(from_item).unwrap_or_default(),
            "ALL_NEW" => from_item(&item),
            "UPDATED_OLD" => old
                .as_ref()
                .map(|o| from_item(&project(o, &touched_paths(&upd))))
                .unwrap_or_default(),
            "UPDATED_NEW" => from_item(&project(&item, &touched_paths(&upd))),
            _ => BTreeMap::new(),
        };
        Ok(UpdateItemOutput {
            attributes,
            consumed_capacity: consumed(&t.name, &i.return_consumed_capacity, 1.0),
            ..Default::default()
        })
    }
}

impl Service for DynamoDb {
    fn put_item(&self, ctx: &RequestContext, i: PutItemInput) -> Result<PutItemOutput, AwsError> {
        self.db
            .transaction(|tx| self.put_item_tx(tx, ctx, &i).map(|(o, _)| o))
    }

    fn delete_item(
        &self,
        ctx: &RequestContext,
        i: DeleteItemInput,
    ) -> Result<DeleteItemOutput, AwsError> {
        self.db.transaction(|tx| self.delete_item_tx(tx, ctx, &i))
    }

    fn update_item(
        &self,
        ctx: &RequestContext,
        i: UpdateItemInput,
    ) -> Result<UpdateItemOutput, AwsError> {
        self.db.transaction(|tx| self.update_item_tx(tx, ctx, &i))
    }

    fn get_item(&self, ctx: &RequestContext, i: GetItemInput) -> Result<GetItemOutput, AwsError> {
        self.db.transaction(|tx| {
            let t = Table::load(tx, ctx, &i.table_name)?;
            let schema = t.schema()?;
            let key = to_item(&i.key);
            let (hk, rk) = schema.request_key(&key).map_err(ve)?;
            let names = some(&i.expression_attribute_names);
            check_projection_names(&i.projection_expression, names)?;
            if i.projection_expression.is_none() && names.is_some() {
                return Err(ve("ExpressionAttributeNames can only be specified when using expressions: ProjectionExpression is null"));
            }
            let item = match load_item_pub(tx, &t, &hk, &rk)? {
                Some(item) => project_pub(&item, &i.projection_expression, &i.attributes_to_get, names)?,
                None => Item::new(),
            };
            Ok(GetItemOutput { item: from_item(&item), consumed_capacity: consumed(&t.name, &i.return_consumed_capacity, 0.5) })
        })
    }

    // ---- tables ----
    fn create_table(
        &self,
        ctx: &RequestContext,
        i: CreateTableInput,
    ) -> Result<CreateTableOutput, AwsError> {
        crate::tables::create_table(self, ctx, i)
    }
    fn delete_table(
        &self,
        ctx: &RequestContext,
        i: DeleteTableInput,
    ) -> Result<DeleteTableOutput, AwsError> {
        crate::tables::delete_table(self, ctx, i)
    }
    fn describe_table(
        &self,
        ctx: &RequestContext,
        i: DescribeTableInput,
    ) -> Result<DescribeTableOutput, AwsError> {
        self.db.transaction(|tx| {
            let t = Table::load(tx, ctx, &i.table_name)?;
            Ok(DescribeTableOutput {
                table: Some(t.describe(tx)?),
            })
        })
    }
    fn list_tables(
        &self,
        ctx: &RequestContext,
        i: ListTablesInput,
    ) -> Result<ListTablesOutput, AwsError> {
        crate::tables::list_tables(self, ctx, i)
    }
    fn update_table(
        &self,
        ctx: &RequestContext,
        i: UpdateTableInput,
    ) -> Result<UpdateTableOutput, AwsError> {
        crate::tables::update_table(self, ctx, i)
    }
    fn create_backup(
        &self,
        ctx: &RequestContext,
        i: CreateBackupInput,
    ) -> Result<CreateBackupOutput, AwsError> {
        crate::backups::create(self, ctx, i)
    }
    fn describe_backup(
        &self,
        ctx: &RequestContext,
        i: DescribeBackupInput,
    ) -> Result<DescribeBackupOutput, AwsError> {
        crate::backups::describe(self, ctx, i)
    }
    fn list_backups(
        &self,
        ctx: &RequestContext,
        i: ListBackupsInput,
    ) -> Result<ListBackupsOutput, AwsError> {
        crate::backups::list(self, ctx, i)
    }
    fn delete_backup(
        &self,
        ctx: &RequestContext,
        i: DeleteBackupInput,
    ) -> Result<DeleteBackupOutput, AwsError> {
        crate::backups::delete(self, ctx, i)
    }
    fn restore_table_from_backup(
        &self,
        ctx: &RequestContext,
        i: RestoreTableFromBackupInput,
    ) -> Result<RestoreTableFromBackupOutput, AwsError> {
        crate::backups::restore_from_backup(self, ctx, i)
    }
    fn restore_table_to_point_in_time(
        &self,
        ctx: &RequestContext,
        i: RestoreTableToPointInTimeInput,
    ) -> Result<RestoreTableToPointInTimeOutput, AwsError> {
        crate::backups::restore_to_point_in_time(self, ctx, i)
    }
    fn batch_get_item(
        &self,
        ctx: &RequestContext,
        i: BatchGetItemInput,
    ) -> Result<BatchGetItemOutput, AwsError> {
        crate::batch::batch_get(self, ctx, i)
    }
    fn batch_write_item(
        &self,
        ctx: &RequestContext,
        i: BatchWriteItemInput,
    ) -> Result<BatchWriteItemOutput, AwsError> {
        crate::batch::batch_write(self, ctx, i)
    }
    fn transact_write_items(
        &self,
        ctx: &RequestContext,
        i: TransactWriteItemsInput,
    ) -> Result<TransactWriteItemsOutput, AwsError> {
        crate::batch::transact_write(self, ctx, i)
    }
    fn transact_get_items(
        &self,
        ctx: &RequestContext,
        i: TransactGetItemsInput,
    ) -> Result<TransactGetItemsOutput, AwsError> {
        crate::batch::transact_get(self, ctx, i)
    }
    fn tag_resource(&self, ctx: &RequestContext, i: TagResourceInput) -> Result<(), AwsError> {
        crate::batch::tag_resource(self, ctx, i)
    }
    fn untag_resource(&self, ctx: &RequestContext, i: UntagResourceInput) -> Result<(), AwsError> {
        crate::batch::untag_resource(self, ctx, i)
    }
    fn list_tags_of_resource(
        &self,
        ctx: &RequestContext,
        i: ListTagsOfResourceInput,
    ) -> Result<ListTagsOfResourceOutput, AwsError> {
        crate::batch::list_tags(self, ctx, i)
    }
    fn update_time_to_live(
        &self,
        ctx: &RequestContext,
        i: UpdateTimeToLiveInput,
    ) -> Result<UpdateTimeToLiveOutput, AwsError> {
        crate::batch::update_ttl(self, ctx, i)
    }
    fn describe_time_to_live(
        &self,
        ctx: &RequestContext,
        i: DescribeTimeToLiveInput,
    ) -> Result<DescribeTimeToLiveOutput, AwsError> {
        crate::batch::describe_ttl(self, ctx, i)
    }
    fn describe_endpoints(
        &self,
        ctx: &RequestContext,
        _i: DescribeEndpointsRequest,
    ) -> Result<DescribeEndpointsResponse, AwsError> {
        Ok(crate::batch::describe_endpoints(ctx))
    }
    fn describe_limits(
        &self,
        _ctx: &RequestContext,
        _i: DescribeLimitsInput,
    ) -> Result<DescribeLimitsOutput, AwsError> {
        Ok(crate::batch::describe_limits())
    }
    fn describe_continuous_backups(
        &self,
        ctx: &RequestContext,
        i: DescribeContinuousBackupsInput,
    ) -> Result<DescribeContinuousBackupsOutput, AwsError> {
        crate::batch::describe_continuous_backups(self, ctx, i)
    }
    fn update_continuous_backups(
        &self,
        ctx: &RequestContext,
        i: UpdateContinuousBackupsInput,
    ) -> Result<UpdateContinuousBackupsOutput, AwsError> {
        crate::batch::update_continuous_backups(self, ctx, i)
    }
    fn query(&self, ctx: &RequestContext, i: QueryInput) -> Result<QueryOutput, AwsError> {
        crate::query::query(self, ctx, i)
    }
    fn scan(&self, ctx: &RequestContext, i: ScanInput) -> Result<ScanOutput, AwsError> {
        crate::query::scan(self, ctx, i)
    }
}

pub(crate) fn schema_for_index(
    t: &Table,
    index: &Option<String>,
) -> Result<(KeySchema, Option<Projection>), AwsError> {
    match index {
        None => Ok((t.schema()?, None)),
        Some(name) => {
            if let Some(g) = t.gsis.iter().find(|g| &g.index_name == name) {
                return Ok((t.schema_of(&g.key_schema)?, Some(g.projection.clone())));
            }
            if let Some(l) = t.lsis.iter().find(|l| &l.index_name == name) {
                return Ok((t.schema_of(&l.key_schema)?, Some(l.projection.clone())));
            }
            let mut available: Vec<&str> = t.gsis.iter().map(|g| g.index_name.as_str()).collect();
            available.extend(t.lsis.iter().map(|l| l.index_name.as_str()));
            Err(AwsError::sender(
                400,
                "ResourceNotFoundException",
                format!(
                    "Invalid index: {name} for table: {}. Available indexes are: {}",
                    t.name,
                    available.join(", ")
                ),
            ))
        }
    }
}
