//! CreateTable / DeleteTable / ListTables / UpdateTable.

use std::collections::BTreeSet;

use roto_core::rusqlite::params;
use roto_core::{AwsError, RequestContext};

use crate::generated::*;
use crate::service::{DynamoDb, now};
use crate::table::*;

fn validate_name(name: &str) -> Result<(), AwsError> {
    let mut errors = Vec::new();
    if name.len() < 3 {
        errors.push(format!("Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        || name.is_empty()
    {
        errors.push(format!("Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"));
    }
    match errors.len() {
        0 => Ok(()),
        1 => Err(ve(format!("1 validation error detected: {}", errors[0]))),
        n => Err(ve(format!(
            "{n} validation errors detected: {}",
            errors.join("; ")
        ))),
    }
}

fn validate_key_schema(ks: &[KeySchemaElement], what: &str) -> Result<(), AwsError> {
    if ks.is_empty() || ks.len() > 2 {
        return Err(ve(format!(
            "1 validation error detected: Value '{what}' at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2"
        )));
    }
    if ks[0].key_type != "HASH" {
        return Err(invalid_param(
            "Invalid key order. Hash Key must be specified first in key schema",
        ));
    }
    if ks.len() == 2 && ks[1].key_type != "RANGE" {
        return Err(invalid_param("Too many hash keys specified in key schema"));
    }
    if ks.len() == 2 && ks[0].attribute_name == ks[1].attribute_name {
        return Err(invalid_param(
            "Two Key Schema elements have the same AttributeName",
        ));
    }
    Ok(())
}

fn validate_definitions(
    defs: &[AttributeDefinition],
    used: &BTreeSet<String>,
) -> Result<(), AwsError> {
    for d in defs {
        if !matches!(d.attribute_type.as_str(), "S" | "N" | "B") {
            return Err(ve(format!(
                "1 validation error detected: Value '{}' at 'attributeDefinitions.1.member.attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]",
                d.attribute_type
            )));
        }
    }
    let defined: BTreeSet<&str> = defs.iter().map(|d| d.attribute_name.as_str()).collect();
    if used.iter().any(|u| !defined.contains(u.as_str())) {
        return Err(invalid_param(
            "Invalid Key Schema: a key attribute is not defined in AttributeDefinitions",
        ));
    }
    if defined.len() != used.len() {
        return Err(invalid_param(
            "Number of attributes in KeySchema does not exactly match number of attributes defined in AttributeDefinitions",
        ));
    }
    Ok(())
}

pub(crate) fn create_table(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: CreateTableInput,
) -> Result<CreateTableOutput, AwsError> {
    validate_name(&i.table_name)?;
    validate_key_schema(&i.key_schema, &i.table_name)?;
    let billing = i
        .billing_mode
        .clone()
        .unwrap_or_else(|| "PROVISIONED".into());
    if !matches!(billing.as_str(), "PROVISIONED" | "PAY_PER_REQUEST") {
        return Err(ve(format!(
            "1 validation error detected: Value '{billing}' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]"
        )));
    }
    if billing == "PAY_PER_REQUEST" {
        if i.provisioned_throughput.is_some() {
            return Err(invalid_param(
                "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST",
            ));
        }
    } else if i.provisioned_throughput.is_none() {
        return Err(ve("No provisioned throughput specified for the table"));
    }
    let mut used: BTreeSet<String> = i
        .key_schema
        .iter()
        .map(|k| k.attribute_name.clone())
        .collect();
    for g in &i.global_secondary_indexes {
        validate_key_schema(&g.key_schema, &g.index_name)?;
        used.extend(g.key_schema.iter().map(|k| k.attribute_name.clone()));
    }
    if !i.local_secondary_indexes.is_empty() {
        if i.key_schema.len() != 2 {
            return Err(invalid_param(
                "Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex",
            ));
        }
        for l in &i.local_secondary_indexes {
            validate_key_schema(&l.key_schema, &l.index_name)?;
            if l.key_schema[0].attribute_name != i.key_schema[0].attribute_name {
                return Err(invalid_param(&format!(
                    "Index KeySchema does not have the same leading hash key as table KeySchema for index: {}",
                    l.index_name
                )));
            }
            used.extend(l.key_schema.iter().map(|k| k.attribute_name.clone()));
        }
    }
    validate_definitions(&i.attribute_definitions, &used)?;
    let mut names = BTreeSet::new();
    for n in i
        .global_secondary_indexes
        .iter()
        .map(|g| &g.index_name)
        .chain(i.local_secondary_indexes.iter().map(|l| &l.index_name))
    {
        if !names.insert(n) {
            return Err(invalid_param(&format!("Duplicate index name: {n}")));
        }
    }
    d.db.transaction(|tx| {
        if Table::find(tx, ctx, &i.table_name)?.is_some() {
            return Err(AwsError::sender(
                400,
                "ResourceInUseException",
                format!("Table already exists: {}", i.table_name),
            ));
        }
        let t = Table {
            id: uuid::Uuid::new_v4().to_string(),
            account: ctx.account_id.clone(),
            region: ctx.region.clone(),
            name: i.table_name.clone(),
            created_at: now(),
            key_schema: i.key_schema.clone(),
            attr_defs: i.attribute_definitions.clone(),
            gsis: i.global_secondary_indexes.clone(),
            lsis: i.local_secondary_indexes.clone(),
            billing_mode: billing.clone(),
            throughput: i.provisioned_throughput.clone(),
            stream: i.stream_specification.clone(),
            tags: i.tags.clone(),
            ttl_attr: None,
            ttl_enabled: false,
            deletion_protection: i.deletion_protection_enabled.unwrap_or(false),
            sse: i.sse_specification.clone(),
            table_class: i.table_class.clone(),
            pitr: false,
        };
        t.insert(tx)?;
        Ok(CreateTableOutput {
            table_description: Some(t.describe(tx)?),
        })
    })
}

pub(crate) fn delete_table(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: DeleteTableInput,
) -> Result<DeleteTableOutput, AwsError> {
    d.db.transaction(|tx| {
        let t = Table::load(tx, ctx, &i.table_name)?;
        if t.deletion_protection {
            return Err(ve("Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first."));
        }
        let mut desc = t.describe(tx)?;
        desc.table_status = Some("DELETING".into());
        tx.execute("DELETE FROM items WHERE table_id = ?1", params![t.id])?;
        tx.execute("DELETE FROM tables WHERE table_id = ?1", params![t.id])?;
        Ok(DeleteTableOutput { table_description: Some(desc) })
    })
}

pub(crate) fn list_tables(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: ListTablesInput,
) -> Result<ListTablesOutput, AwsError> {
    let limit = i.limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(ve(format!(
            "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value {}",
            if limit < 1 {
                "greater than or equal to 1"
            } else {
                "less than or equal to 100"
            }
        )));
    }
    d.db.transaction(|tx| {
        let after = i.exclusive_start_table_name.clone().unwrap_or_default();
        let mut stmt = tx.prepare("SELECT name FROM tables WHERE account_id = ?1 AND region = ?2 AND name > ?3 ORDER BY name LIMIT ?4")?;
        let mut names: Vec<String> =
            stmt.query_map(params![ctx.account_id, ctx.region, after, limit + 1], |r| r.get(0))?.collect::<Result<_, _>>()?;
        let more = names.len() as i32 > limit;
        names.truncate(limit as usize);
        Ok(ListTablesOutput { last_evaluated_table_name: more.then(|| names.last().cloned()).flatten(), table_names: names })
    })
}

pub(crate) fn update_table(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: UpdateTableInput,
) -> Result<UpdateTableOutput, AwsError> {
    d.db.transaction(|tx| {
        let mut t = Table::load(tx, ctx, &i.table_name)?;
        if let Some(b) = &i.billing_mode {
            t.billing_mode = b.clone();
            if b == "PAY_PER_REQUEST" {
                t.throughput = None;
            }
        }
        if let Some(p) = &i.provisioned_throughput {
            if t.billing_mode == "PAY_PER_REQUEST" {
                return Err(invalid_param("Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"));
            }
            t.throughput = Some(p.clone());
        }
        if let Some(dp) = i.deletion_protection_enabled {
            t.deletion_protection = dp;
        }
        if let Some(s) = &i.stream_specification {
            t.stream = Some(s.clone());
        }
        if let Some(s) = &i.sse_specification {
            t.sse = Some(s.clone());
        }
        if let Some(c) = &i.table_class {
            t.table_class = Some(c.clone());
        }
        for def in &i.attribute_definitions {
            if !t.attr_defs.iter().any(|a| a.attribute_name == def.attribute_name) {
                t.attr_defs.push(def.clone());
            }
        }
        for u in &i.global_secondary_index_updates {
            if let Some(c) = &u.create {
                if t.gsis.iter().any(|g| g.index_name == c.index_name) {
                    return Err(invalid_param(&format!("Index already exists: {}", c.index_name)));
                }
                t.gsis.push(GlobalSecondaryIndex {
                    index_name: c.index_name.clone(),
                    key_schema: c.key_schema.clone(),
                    projection: c.projection.clone(),
                    provisioned_throughput: c.provisioned_throughput.clone(),
                    on_demand_throughput: c.on_demand_throughput.clone(),
                    warm_throughput: None,
                });
            }
            if let Some(del) = &u.delete {
                let before = t.gsis.len();
                t.gsis.retain(|g| g.index_name != del.index_name);
                if t.gsis.len() == before {
                    return Err(AwsError::sender(400, "ResourceNotFoundException", format!("Requested resource not found: Index: {} not found", del.index_name)));
                }
            }
            if let Some(up) = &u.update {
                match t.gsis.iter_mut().find(|g| g.index_name == up.index_name) {
                    Some(g) => g.provisioned_throughput = up.provisioned_throughput.clone(),
                    None => {
                        return Err(AwsError::sender(400, "ResourceNotFoundException", format!("Requested resource not found: Index: {} not found", up.index_name)));
                    }
                }
            }
        }
        t.save(tx)?;
        Ok(UpdateTableOutput { table_description: Some(t.describe(tx)?) })
    })
}
