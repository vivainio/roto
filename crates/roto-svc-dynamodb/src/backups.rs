//! On-demand backups and restores. A backup is a full copy of the table definition and items;
//! point-in-time restore copies the current state (there is no change history).

use crate::schema::*;
use crate::table::Table;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;

use crate::generated::*;
use crate::service::*;

fn backup_not_found(arn: &str) -> AwsError {
    AwsError::sender(
        400,
        "BackupNotFoundException",
        format!("Backup not found: {arn}"),
    )
}

fn table_not_found(name: &str) -> AwsError {
    AwsError::sender(
        400,
        "TableNotFoundException",
        format!("Table not found: {name}"),
    )
}

#[derive(Queryable, Selectable)]
#[diesel(table_name=backups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct BackupRow {
    arn: String,
    name: String,
    table_name: String,
    table_id: String,
    created_at: i64,
    meta: String,
}

fn load_backup(tx: &mut SqliteConnection, arn: &str) -> Result<BackupRow, AwsError> {
    backups::table
        .filter(backups::arn.eq(arn))
        .select(BackupRow::as_select())
        .first(tx)
        .optional()?
        .ok_or_else(|| backup_not_found(arn))
}

fn details(b: &BackupRow) -> BackupDetails {
    BackupDetails {
        backup_arn: b.arn.clone(),
        backup_name: b.name.clone(),
        backup_status: "AVAILABLE".into(),
        backup_type: "USER".into(),
        backup_creation_date_time: Timestamp(b.created_at),
        backup_size_bytes: Some(0),
        backup_expiry_date_time: None,
    }
}

fn description(
    b: &BackupRow,
    ctx: &RequestContext,
    count: i64,
) -> Result<BackupDescription, AwsError> {
    let t = Table::from_meta(&b.meta, &b.table_id, &b.table_name, ctx, b.created_at);
    Ok(BackupDescription {
        backup_details: Some(details(b)),
        source_table_details: Some(SourceTableDetails {
            table_name: t.name.clone(),
            table_id: t.id.clone(),
            table_arn: Some(t.arn()),
            table_creation_date_time: Timestamp(t.created_at),
            key_schema: t.key_schema.clone(),
            provisioned_throughput: t.throughput.clone().unwrap_or(ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }),
            billing_mode: Some(t.billing_mode.clone()),
            item_count: Some(count),
            table_size_bytes: Some(0),
            ..Default::default()
        }),
        source_table_feature_details: None,
    })
}

fn item_count(tx: &mut SqliteConnection, arn: &str) -> Result<i64, AwsError> {
    Ok(backup_items::table
        .filter(backup_items::arn.eq(&(arn)))
        .count()
        .first::<i64>(tx)?)
}

pub(crate) fn create(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: CreateBackupInput,
) -> Result<CreateBackupOutput, AwsError> {
    d.db.transaction(|tx| {
        let t =
            Table::find(tx, ctx, &i.table_name)?.ok_or_else(|| table_not_found(&i.table_name))?;
        let created = now();
        let arn = format!(
            "{}/backup/{:010}-{}",
            t.arn(),
            created,
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        diesel::insert_into(backups::table)
            .values((
                backups::arn.eq(&(arn)),
                backups::name.eq(&(i.backup_name)),
                backups::table_name.eq(&(t.name)),
                backups::table_id.eq(&(t.id)),
                backups::created_at.eq(&(created)),
                backups::meta.eq(&(t.to_meta())),
            ))
            .execute(tx)?;
        diesel::insert_into(backup_items::table)
            .values(items::table.filter(items::table_id.eq(&t.id)).select((
                (&arn).into_sql::<diesel::sql_types::Text>(),
                items::hk,
                items::rk,
                items::item,
            )))
            .into_columns((
                backup_items::arn,
                backup_items::hk,
                backup_items::rk,
                backup_items::item,
            ))
            .execute(tx)?;
        let b = load_backup(tx, &arn)?;
        Ok(CreateBackupOutput {
            backup_details: Some(details(&b)),
        })
    })
}

pub(crate) fn describe(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: DescribeBackupInput,
) -> Result<DescribeBackupOutput, AwsError> {
    d.db.transaction(|tx| {
        let b = load_backup(tx, &i.backup_arn)?;
        let n = item_count(tx, &b.arn)?;
        Ok(DescribeBackupOutput {
            backup_description: Some(description(&b, ctx, n)?),
        })
    })
}

pub(crate) fn list(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: ListBackupsInput,
) -> Result<ListBackupsOutput, AwsError> {
    d.db.transaction(|tx| {
        if let Some(name) = &i.table_name {
            if Table::find(tx, ctx, name)?.is_none() {
                return Err(table_not_found(name));
            }
        }
        let arns = backups::table
            .order((backups::created_at, backups::arn))
            .select(backups::arn)
            .load::<String>(tx)?;
        let mut out = ListBackupsOutput::default();
        for arn in arns {
            let b = load_backup(tx, &arn)?;
            if i.table_name.as_ref().is_some_and(|n| n != &b.table_name) {
                continue;
            }
            if i.time_range_lower_bound.is_some_and(|t| b.created_at < t.0)
                || i.time_range_upper_bound
                    .is_some_and(|t| b.created_at >= t.0)
            {
                continue;
            }
            if i.backup_type
                .as_deref()
                .is_some_and(|t| t != "USER" && t != "ALL")
            {
                continue;
            }
            let t = Table::from_meta(&b.meta, &b.table_id, &b.table_name, ctx, b.created_at);
            let det = details(&b);
            out.backup_summaries.push(BackupSummary {
                table_name: Some(b.table_name.clone()),
                table_id: Some(b.table_id.clone()),
                table_arn: Some(t.arn()),
                backup_arn: Some(det.backup_arn),
                backup_name: Some(det.backup_name),
                backup_creation_date_time: Some(det.backup_creation_date_time),
                backup_status: Some(det.backup_status),
                backup_type: Some(det.backup_type),
                backup_size_bytes: det.backup_size_bytes,
                backup_expiry_date_time: None,
            });
        }
        Ok(out)
    })
}

pub(crate) fn delete(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: DeleteBackupInput,
) -> Result<DeleteBackupOutput, AwsError> {
    d.db.transaction(|tx| {
        let b = load_backup(tx, &i.backup_arn)?;
        let n = item_count(tx, &b.arn)?;
        let mut desc = description(&b, ctx, n)?;
        if let Some(det) = desc.backup_details.as_mut() {
            det.backup_status = "DELETED".into();
        }
        diesel::delete(backup_items::table.filter(backup_items::arn.eq(&(b.arn)))).execute(tx)?;
        diesel::delete(backups::table.filter(backups::arn.eq(&(b.arn)))).execute(tx)?;
        Ok(DeleteBackupOutput {
            backup_description: Some(desc),
        })
    })
}

fn restore_into(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    mut t: Table,
    target: &str,
    copy_items: impl FnOnce(&mut SqliteConnection, &Table) -> Result<(), AwsError>,
) -> Result<TableDescription, AwsError> {
    if Table::find(tx, ctx, target)?.is_some() {
        return Err(AwsError::sender(
            400,
            "TableAlreadyExistsException",
            format!("Table already exists: {target}"),
        ));
    }
    t.id = uuid::Uuid::new_v4().to_string();
    t.name = target.to_string();
    t.account = ctx.account_id.clone();
    t.region = ctx.region.clone();
    t.created_at = now();
    t.insert(tx)?;
    copy_items(tx, &t)?;
    t.describe(tx)
}

pub(crate) fn restore_from_backup(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: RestoreTableFromBackupInput,
) -> Result<RestoreTableFromBackupOutput, AwsError> {
    d.db.transaction(|tx| {
        let b = load_backup(tx, &i.backup_arn)?;
        let mut t = Table::from_meta(&b.meta, &b.table_id, &b.table_name, ctx, b.created_at);
        if let Some(m) = &i.billing_mode_override {
            t.billing_mode = m.clone();
        }
        if let Some(p) = &i.provisioned_throughput_override {
            t.throughput = Some(p.clone());
        }
        if !i.global_secondary_index_override.is_empty() {
            t.gsis = i.global_secondary_index_override.clone();
        }
        if !i.local_secondary_index_override.is_empty() {
            t.lsis = i.local_secondary_index_override.clone();
        }
        let desc = restore_into(tx, ctx, t, &i.target_table_name, |tx, nt| {
            diesel::insert_into(items::table)
                .values(
                    backup_items::table
                        .filter(backup_items::arn.eq(&b.arn))
                        .select((
                            (&nt.id).into_sql::<diesel::sql_types::Text>(),
                            backup_items::hk,
                            backup_items::rk,
                            backup_items::item,
                        )),
                )
                .into_columns((items::table_id, items::hk, items::rk, items::item))
                .execute(tx)?;
            Ok(())
        })?;
        Ok(RestoreTableFromBackupOutput {
            table_description: Some(desc),
        })
    })
}

pub(crate) fn restore_to_point_in_time(
    d: &DynamoDb,
    ctx: &RequestContext,
    i: RestoreTableToPointInTimeInput,
) -> Result<RestoreTableToPointInTimeOutput, AwsError> {
    d.db.transaction(|tx| {
        let source = i
            .source_table_name
            .clone()
            .or(i.source_table_arn.clone())
            .unwrap_or_default();
        let src = Table::find(tx, ctx, &source)?.ok_or_else(|| table_not_found(&source))?;
        if !src.pitr {
            return Err(AwsError::sender(
                400,
                "PointInTimeRecoveryUnavailableException",
                format!(
                    "Point in time recovery is not enabled for table '{}'",
                    src.name
                ),
            ));
        }
        let src_id = src.id.clone();
        let mut t = Table::from_meta(&src.to_meta(), &src.id, &src.name, ctx, src.created_at);
        if let Some(m) = &i.billing_mode_override {
            t.billing_mode = m.clone();
        }
        if let Some(p) = &i.provisioned_throughput_override {
            t.throughput = Some(p.clone());
        }
        t.pitr = false;
        let desc = restore_into(tx, ctx, t, &i.target_table_name, |tx, nt| {
            diesel::insert_into(items::table)
                .values(items::table.filter(items::table_id.eq(&src_id)).select((
                    (&nt.id).into_sql::<diesel::sql_types::Text>(),
                    items::hk,
                    items::rk,
                    items::item,
                )))
                .into_columns((items::table_id, items::hk, items::rk, items::item))
                .execute(tx)?;
            Ok(())
        })?;
        Ok(RestoreTableToPointInTimeOutput {
            table_description: Some(desc),
        })
    })
}
