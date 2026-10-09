//! Batch and transactional operations, tagging, TTL and small metadata calls.

use std::collections::{BTreeMap, BTreeSet};

use roto_core::rusqlite::Transaction;
use roto_core::{AwsError, RequestContext};
use roto_protocol::{JsonValue, Timestamp};
use serde_json::{Value, json};

use crate::eval::{Scope, eval_cond};
use crate::expr::*;
use crate::generated::*;
use crate::service::*;
use crate::table::*;
use crate::value::Item;

fn too_many(op: &str) -> AwsError {
    ve(format!("Too many items requested for the {op} call"))
}

pub(crate) fn batch_get(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: BatchGetItemInput,
) -> Result<BatchGetItemOutput, AwsError> {
    if i.request_items.is_empty() {
        return Err(ve(
            "1 validation error detected: Value null at 'requestItems' failed to satisfy constraint: Member must not be null",
        ));
    }
    let total: usize = i.request_items.values().map(|k| k.keys.len()).sum();
    if total > 100 {
        return Err(ve("Too many items requested for the BatchGetItem call"));
    }
    d.db.transaction(|tx| {
        let mut out = BatchGetItemOutput::default();
        for (name, ka) in &i.request_items {
            let t = Table::load(tx, ctx, name)?;
            let schema = t.schema()?;
            if ka.keys.is_empty() {
                return Err(ve("1 validation error detected: Value '[]' at 'requestItems.keys' failed to satisfy constraint: Member must have length greater than or equal to 1"));
            }
            let names = some(&ka.expression_attribute_names);
            let mut seen = BTreeSet::new();
            let mut items = Vec::new();
            for key in &ka.keys {
                let key = to_item(key);
                let enc = schema.request_key(&key).map_err(ve)?;
                if !seen.insert(enc.clone()) {
                    return Err(ve("Provided list of item keys contains duplicates"));
                }
                if let Some(item) = load_item_pub(tx, &t, &enc.0, &enc.1)? {
                    items.push(from_item(&project_pub(&item, &ka.projection_expression, &ka.attributes_to_get, names)?));
                }
            }
            out.responses.insert(name.clone(), items);
        }
        if let Some("TOTAL" | "INDEXES") = i.return_consumed_capacity.as_deref() {
            out.consumed_capacity = i
                .request_items
                .iter()
                .map(|(n, k)| ConsumedCapacity { table_name: Some(n.clone()), capacity_units: Some(k.keys.len() as f64 * 0.5), ..Default::default() })
                .collect();
        }
        Ok(out)
    })
}

pub(crate) fn batch_write(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: BatchWriteItemInput,
) -> Result<BatchWriteItemOutput, AwsError> {
    if i.request_items.is_empty() {
        return Err(ve(
            "1 validation error detected: Value null at 'requestItems' failed to satisfy constraint: Member must not be null",
        ));
    }
    let total: usize = i.request_items.values().map(Vec::len).sum();
    if total > 25 {
        return Err(too_many("BatchWriteItem"));
    }
    d.db.transaction(|tx| {
        // Validate the whole batch (duplicates, shape) before writing anything.
        let mut seen = BTreeSet::new();
        for (name, reqs) in &i.request_items {
            let t = Table::load(tx, ctx, name)?;
            let schema = t.schema()?;
            if reqs.is_empty() {
                return Err(ve("1 validation error detected: Value '[]' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to 25, Member must have length greater than or equal to 1]"));
            }
            for r in reqs {
                let key = match (&r.put_request, &r.delete_request) {
                    (Some(p), None) => schema.item_keys(&to_item(&p.item)).map_err(ve)?,
                    (None, Some(del)) => schema.request_key(&to_item(&del.key)).map_err(ve)?,
                    _ => return Err(ve("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")),
                };
                if !seen.insert((name.clone(), key)) {
                    return Err(ve("Provided list of item keys contains duplicates"));
                }
            }
        }
        for (name, reqs) in &i.request_items {
            for r in reqs {
                if let Some(p) = &r.put_request {
                    d.put_item_tx(tx, ctx, &PutItemInput { table_name: name.clone(), item: p.item.clone(), ..Default::default() })?;
                } else if let Some(del) = &r.delete_request {
                    d.delete_item_tx(tx, ctx, &DeleteItemInput { table_name: name.clone(), key: del.key.clone(), ..Default::default() })?;
                }
            }
        }
        let mut out = BatchWriteItemOutput::default();
        if let Some("TOTAL" | "INDEXES") = i.return_consumed_capacity.as_deref() {
            out.consumed_capacity = i
                .request_items
                .iter()
                .map(|(n, r)| ConsumedCapacity { table_name: Some(n.clone()), capacity_units: Some(r.len() as f64), ..Default::default() })
                .collect();
        }
        Ok(out)
    })
}

fn cancellation(codes: Vec<(String, String, Option<Item>)>) -> AwsError {
    let reasons: Vec<Value> = codes
        .iter()
        .map(|(code, msg, item)| {
            let mut r = json!({"Code": code});
            if code != "None" {
                r["Message"] = json!(msg);
            }
            if let Some(i) = item {
                r["Item"] = Value::Object(i.clone());
            }
            r
        })
        .collect();
    let summary: Vec<&str> = codes.iter().map(|(c, _, _)| c.as_str()).collect();
    AwsError::sender(
        400,
        "TransactionCanceledException",
        format!(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
            summary.join(", ")
        ),
    )
    .with("CancellationReasons", Value::Array(reasons).to_string())
}

fn condition_check_tx(
    d: &DynamoDb,
    tx: &Transaction,
    ctx: &RequestContext,
    c: &ConditionCheck,
) -> Result<(), AwsError> {
    let t = Table::load(tx, ctx, &c.table_name)?;
    let schema = t.schema()?;
    let values = values_map(&c.expression_attribute_values)?;
    let mut env = Env::new(
        some(&c.expression_attribute_names),
        if values.is_empty() {
            None
        } else {
            Some(&values)
        },
    );
    let cond =
        parse_condition(&c.condition_expression, "ConditionExpression", &mut env).map_err(ve)?;
    finish_env(&env, &[("ConditionExpression", true)])?;
    let (hk, rk) = schema.request_key(&to_item(&c.key)).map_err(ve)?;
    let existing = load_item_pub(tx, &t, &hk, &rk)?;
    let empty = Item::new();
    let ok = eval_cond(
        &Scope {
            values: &values,
            what: "ConditionExpression",
        },
        existing.as_ref().unwrap_or(&empty),
        &cond,
    )
    .map_err(ve)?;
    let _ = d;
    if ok {
        Ok(())
    } else {
        Err(AwsError::sender(
            400,
            "ConditionalCheckFailedException",
            "The conditional request failed",
        ))
    }
}

pub(crate) fn transact_write(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: TransactWriteItemsInput,
) -> Result<TransactWriteItemsOutput, AwsError> {
    if i.transact_items.is_empty() {
        return Err(ve(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
        ));
    }
    if i.transact_items.len() > 100 {
        return Err(ve(
            "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
        ));
    }
    d.db.transaction(|tx| {
        // One operation per item.
        let mut seen = BTreeSet::new();
        for t in &i.transact_items {
            let (table, key) = if let Some(p) = &t.put {
                (&p.table_name, Some(&p.item))
            } else if let Some(x) = &t.delete {
                (&x.table_name, Some(&x.key))
            } else if let Some(u) = &t.update {
                (&u.table_name, Some(&u.key))
            } else if let Some(c) = &t.condition_check {
                (&c.table_name, Some(&c.key))
            } else {
                return Err(ve(
                    "TransactItems can only contain one of Check, Put, Update or Delete",
                ));
            };
            let tbl = Table::load(tx, ctx, table)?;
            let schema = tbl.schema()?;
            let k = to_item(key.unwrap());
            let enc = schema
                .item_keys(&k)
                .map_err(ve)
                .or_else(|_| schema.request_key(&k).map_err(ve))?;
            if !seen.insert((table.clone(), enc)) {
                return Err(ve(
                    "Transaction request cannot include multiple operations on one item",
                ));
            }
        }
        let mut reasons: Vec<(String, String, Option<Item>)> = Vec::new();
        let mut failed = false;
        for t in &i.transact_items {
            let result = if let Some(p) = &t.put {
                d.put_item_tx(
                    tx,
                    ctx,
                    &PutItemInput {
                        table_name: p.table_name.clone(),
                        item: p.item.clone(),
                        condition_expression: p.condition_expression.clone(),
                        expression_attribute_names: p.expression_attribute_names.clone(),
                        expression_attribute_values: p.expression_attribute_values.clone(),
                        ..Default::default()
                    },
                )
                .map(|_| ())
            } else if let Some(x) = &t.delete {
                d.delete_item_tx(
                    tx,
                    ctx,
                    &DeleteItemInput {
                        table_name: x.table_name.clone(),
                        key: x.key.clone(),
                        condition_expression: x.condition_expression.clone(),
                        expression_attribute_names: x.expression_attribute_names.clone(),
                        expression_attribute_values: x.expression_attribute_values.clone(),
                        ..Default::default()
                    },
                )
                .map(|_| ())
            } else if let Some(u) = &t.update {
                d.update_item_tx(
                    tx,
                    ctx,
                    &UpdateItemInput {
                        table_name: u.table_name.clone(),
                        key: u.key.clone(),
                        update_expression: Some(u.update_expression.clone()),
                        condition_expression: u.condition_expression.clone(),
                        expression_attribute_names: u.expression_attribute_names.clone(),
                        expression_attribute_values: u.expression_attribute_values.clone(),
                        ..Default::default()
                    },
                )
                .map(|_| ())
            } else if let Some(c) = &t.condition_check {
                condition_check_tx(d, tx, ctx, c)
            } else {
                Ok(())
            };
            match result {
                Ok(()) => reasons.push(("None".into(), String::new(), None)),
                Err(e) if e.code == "ConditionalCheckFailedException" => {
                    failed = true;
                    let rv = t
                        .put
                        .as_ref()
                        .and_then(|p| p.return_values_on_condition_check_failure.clone())
                        .or_else(|| {
                            t.delete
                                .as_ref()
                                .and_then(|x| x.return_values_on_condition_check_failure.clone())
                        })
                        .or_else(|| {
                            t.update
                                .as_ref()
                                .and_then(|u| u.return_values_on_condition_check_failure.clone())
                        })
                        .or_else(|| {
                            t.condition_check
                                .as_ref()
                                .and_then(|c| c.return_values_on_condition_check_failure.clone())
                        });
                    let item = if rv.as_deref() == Some("ALL_OLD") {
                        existing_item_for(d, tx, ctx, t)?
                    } else {
                        None
                    };
                    reasons.push((
                        "ConditionalCheckFailed".into(),
                        "The conditional request failed".into(),
                        item,
                    ));
                }
                Err(e) => return Err(e),
            }
        }
        if failed {
            return Err(cancellation(reasons));
        }
        Ok(TransactWriteItemsOutput::default())
    })
}

fn existing_item_for(
    d: &DynamoDb,
    tx: &Transaction,
    ctx: &RequestContext,
    t: &TransactWriteItem,
) -> Result<Option<Item>, AwsError> {
    let (table, key, is_item) = if let Some(p) = &t.put {
        (&p.table_name, &p.item, true)
    } else if let Some(x) = &t.delete {
        (&x.table_name, &x.key, false)
    } else if let Some(u) = &t.update {
        (&u.table_name, &u.key, false)
    } else if let Some(c) = &t.condition_check {
        (&c.table_name, &c.key, false)
    } else {
        return Ok(None);
    };
    let _ = d;
    let tbl = Table::load(tx, ctx, table)?;
    let schema = tbl.schema()?;
    let k = to_item(key);
    let enc = if is_item {
        schema.item_keys(&k)
    } else {
        schema.request_key(&k)
    }
    .map_err(ve)?;
    load_item_pub(tx, &tbl, &enc.0, &enc.1)
}

pub(crate) fn transact_get(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: TransactGetItemsInput,
) -> Result<TransactGetItemsOutput, AwsError> {
    if i.transact_items.is_empty() {
        return Err(ve(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
        ));
    }
    if i.transact_items.len() > 100 {
        return Err(ve(
            "1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
        ));
    }
    d.db.transaction(|tx| {
        let mut out = TransactGetItemsOutput::default();
        for t in &i.transact_items {
            let g = &t.get;
            let table = Table::load(tx, ctx, &g.table_name)?;
            let schema = table.schema()?;
            let (hk, rk) = schema.request_key(&to_item(&g.key)).map_err(ve)?;
            let item = match load_item_pub(tx, &table, &hk, &rk)? {
                Some(item) => from_item(&project_pub(
                    &item,
                    &g.projection_expression,
                    &[],
                    some(&g.expression_attribute_names),
                )?),
                None => BTreeMap::new(),
            };
            out.responses.push(ItemResponse { item });
        }
        Ok(out)
    })
}

// ---- tags --------------------------------------------------------------------------------

fn resource_table(tx: &Transaction, ctx: &RequestContext, arn: &str) -> Result<Table, AwsError> {
    Table::find(tx, ctx, arn)?.ok_or_else(|| {
        AwsError::sender(
            400,
            "ResourceNotFoundException",
            format!("Requested resource not found: ResourceArn: {arn} not found"),
        )
    })
}

pub(crate) fn tag_resource(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: TagResourceInput,
) -> Result<(), AwsError> {
    if i.tags.is_empty() {
        return Err(ve("Invalid TagsToAdd: TagsToAdd must not be empty"));
    }
    d.db.transaction(|tx| {
        let mut t = resource_table(tx, ctx, &i.resource_arn)?;
        for tag in &i.tags {
            match t.tags.iter_mut().find(|x| x.key == tag.key) {
                Some(x) => x.value = tag.value.clone(),
                None => t.tags.push(tag.clone()),
            }
        }
        t.save(tx)
    })
}

pub(crate) fn untag_resource(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: UntagResourceInput,
) -> Result<(), AwsError> {
    d.db.transaction(|tx| {
        let mut t = resource_table(tx, ctx, &i.resource_arn)?;
        t.tags.retain(|x| !i.tag_keys.contains(&x.key));
        t.save(tx)
    })
}

pub(crate) fn list_tags(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: ListTagsOfResourceInput,
) -> Result<ListTagsOfResourceOutput, AwsError> {
    d.db.transaction(|tx| {
        let t = resource_table(tx, ctx, &i.resource_arn)?;
        Ok(ListTagsOfResourceOutput {
            tags: t.tags,
            next_token: None,
        })
    })
}

// ---- TTL, endpoints, limits, backups -----------------------------------------------------

pub(crate) fn update_ttl(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: UpdateTimeToLiveInput,
) -> Result<UpdateTimeToLiveOutput, AwsError> {
    d.db.transaction(|tx| {
        let mut t = Table::load(tx, ctx, &i.table_name)?;
        let spec = &i.time_to_live_specification;
        if spec.attribute_name.is_empty() {
            return Err(ve(
                "TimeToLiveSpecification.AttributeName must be non-empty",
            ));
        }
        if spec.enabled && t.ttl_enabled {
            return Err(ve("TimeToLive is already enabled"));
        }
        if !spec.enabled && !t.ttl_enabled {
            return Err(ve("TimeToLive is already disabled"));
        }
        t.ttl_enabled = spec.enabled;
        t.ttl_attr = Some(spec.attribute_name.clone());
        t.save(tx)?;
        Ok(UpdateTimeToLiveOutput {
            time_to_live_specification: Some(spec.clone()),
        })
    })
}

pub(crate) fn describe_ttl(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: DescribeTimeToLiveInput,
) -> Result<DescribeTimeToLiveOutput, AwsError> {
    d.db.transaction(|tx| {
        let t = Table::load(tx, ctx, &i.table_name)?;
        let status = if t.ttl_enabled { "ENABLED" } else { "DISABLED" };
        Ok(DescribeTimeToLiveOutput {
            time_to_live_description: Some(TimeToLiveDescription {
                time_to_live_status: Some(status.into()),
                attribute_name: t.ttl_enabled.then(|| t.ttl_attr.clone()).flatten(),
            }),
        })
    })
}

pub(crate) fn describe_endpoints(ctx: &RequestContext) -> DescribeEndpointsResponse {
    DescribeEndpointsResponse {
        endpoints: vec![Endpoint {
            address: format!("dynamodb.{}.amazonaws.com", ctx.region),
            cache_period_in_minutes: 1440,
        }],
    }
}

pub(crate) fn describe_limits() -> DescribeLimitsOutput {
    DescribeLimitsOutput {
        account_max_read_capacity_units: Some(80_000),
        account_max_write_capacity_units: Some(80_000),
        table_max_read_capacity_units: Some(40_000),
        table_max_write_capacity_units: Some(40_000),
    }
}

fn backups_description(t: &Table) -> ContinuousBackupsDescription {
    let now = Timestamp(crate::service::now());
    ContinuousBackupsDescription {
        continuous_backups_status: "ENABLED".into(),
        point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
            point_in_time_recovery_status: Some(if t.pitr { "ENABLED" } else { "DISABLED" }.into()),
            earliest_restorable_date_time: t.pitr.then_some(Timestamp(t.created_at)),
            latest_restorable_date_time: t.pitr.then_some(now),
            recovery_period_in_days: t.pitr.then_some(35),
        }),
    }
}

pub(crate) fn describe_continuous_backups(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: DescribeContinuousBackupsInput,
) -> Result<DescribeContinuousBackupsOutput, AwsError> {
    d.db.transaction(|tx| {
        let t = Table::load(tx, ctx, &i.table_name).map_err(|e| {
            if e.code == "ResourceNotFoundException" {
                AwsError::sender(
                    400,
                    "TableNotFoundException",
                    format!("Table not found: {}", i.table_name),
                )
            } else {
                e
            }
        })?;
        Ok(DescribeContinuousBackupsOutput {
            continuous_backups_description: Some(backups_description(&t)),
        })
    })
}

pub(crate) fn update_continuous_backups(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: UpdateContinuousBackupsInput,
) -> Result<UpdateContinuousBackupsOutput, AwsError> {
    d.db.transaction(|tx| {
        let mut t = Table::load(tx, ctx, &i.table_name).map_err(|e| {
            if e.code == "ResourceNotFoundException" {
                AwsError::sender(
                    400,
                    "TableNotFoundException",
                    format!("Table not found: {}", i.table_name),
                )
            } else {
                e
            }
        })?;
        t.pitr = i
            .point_in_time_recovery_specification
            .point_in_time_recovery_enabled;
        t.save(tx)?;
        Ok(UpdateContinuousBackupsOutput {
            continuous_backups_description: Some(backups_description(&t)),
        })
    })
}

#[allow(dead_code)]
fn unused(_: JsonValue) {}
