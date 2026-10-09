//! Table metadata: persistence, key-schema derivation and `TableDescription` building.

use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{FromJson, Timestamp, ToJson};

use crate::generated::*;
use crate::keys::{KeyAttr, KeySchema};

pub fn ve(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationException", message)
}

pub fn not_found() -> AwsError {
    AwsError::sender(
        400,
        "ResourceNotFoundException",
        "Requested resource not found",
    )
}

pub fn invalid_param(message: &str) -> AwsError {
    ve(format!(
        "One or more parameter values were invalid: {message}"
    ))
}

pub struct Table {
    pub id: String,
    pub account: String,
    pub region: String,
    pub name: String,
    pub created_at: i64,
    pub key_schema: Vec<KeySchemaElement>,
    pub attr_defs: Vec<AttributeDefinition>,
    pub gsis: Vec<GlobalSecondaryIndex>,
    pub lsis: Vec<LocalSecondaryIndex>,
    pub billing_mode: String,
    pub throughput: Option<ProvisionedThroughput>,
    pub stream: Option<StreamSpecification>,
    pub tags: Vec<Tag>,
    pub ttl_attr: Option<String>,
    pub ttl_enabled: bool,
    pub deletion_protection: bool,
    pub sse: Option<SSESpecification>,
    pub table_class: Option<String>,
    pub pitr: bool,
}

fn list_json<T: ToJson>(v: &[T]) -> String {
    serde_json::Value::Array(v.iter().map(ToJson::to_json).collect()).to_string()
}

fn from_list<T: FromJson>(s: &str) -> Vec<T> {
    serde_json::from_str::<serde_json::Value>(s)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .map(|a| a.iter().filter_map(|x| T::from_json(x, "").ok()).collect())
        .unwrap_or_default()
}

fn opt_json<T: ToJson>(v: &Option<T>) -> Option<String> {
    v.as_ref().map(|x| x.to_json().to_string())
}

fn from_opt<T: FromJson>(s: Option<String>) -> Option<T> {
    s.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| T::from_json(&v, "").ok())
}

/// Accepts a table name or a table ARN.
pub fn table_name(name_or_arn: &str) -> &str {
    name_or_arn
        .rsplit_once("table/")
        .map_or(name_or_arn, |(_, n)| n.split('/').next().unwrap_or(n))
}

impl Table {
    pub fn arn(&self) -> String {
        format!(
            "arn:{}:dynamodb:{}:{}:table/{}",
            partition(&self.region),
            self.region,
            self.account,
            self.name
        )
    }

    pub fn load(
        tx: &Transaction,
        ctx: &RequestContext,
        name_or_arn: &str,
    ) -> Result<Table, AwsError> {
        Self::find(tx, ctx, name_or_arn)?.ok_or_else(not_found)
    }

    pub fn find(
        tx: &Transaction,
        ctx: &RequestContext,
        name_or_arn: &str,
    ) -> Result<Option<Table>, AwsError> {
        // ARNs carry their own account and region.
        let (account, region) = match name_or_arn.strip_prefix("arn:") {
            Some(rest) => {
                let parts: Vec<&str> = rest.split(':').collect();
                (
                    parts.get(3).unwrap_or(&ctx.account_id.as_str()).to_string(),
                    parts.get(2).unwrap_or(&ctx.region.as_str()).to_string(),
                )
            }
            None => (ctx.account_id.clone(), ctx.region.clone()),
        };
        let name = table_name(name_or_arn);
        Ok(tx
            .query_row(
                "SELECT table_id, created_at, key_schema, attr_defs, gsis, lsis, billing_mode, throughput, stream_spec,
                        tags, ttl_attr, ttl_enabled, deletion_protection, sse, table_class, pitr
                 FROM tables WHERE account_id = ?1 AND region = ?2 AND name = ?3",
                params![account, region, name],
                |r| {
                    Ok(Table {
                        id: r.get(0)?,
                        account: account.clone(),
                        region: region.clone(),
                        name: name.to_string(),
                        created_at: r.get(1)?,
                        key_schema: from_list(&r.get::<_, String>(2)?),
                        attr_defs: from_list(&r.get::<_, String>(3)?),
                        gsis: from_list(&r.get::<_, String>(4)?),
                        lsis: from_list(&r.get::<_, String>(5)?),
                        billing_mode: r.get(6)?,
                        throughput: from_opt(r.get(7)?),
                        stream: from_opt(r.get(8)?),
                        tags: from_list(&r.get::<_, String>(9)?),
                        ttl_attr: r.get(10)?,
                        ttl_enabled: r.get::<_, i64>(11)? != 0,
                        deletion_protection: r.get::<_, i64>(12)? != 0,
                        sse: from_opt(r.get(13)?),
                        table_class: r.get(14)?,
                        pitr: r.get::<_, i64>(15)? != 0,
                    })
                },
            )
            .optional()?)
    }

    /// The table definition as JSON (for backups).
    pub fn to_meta(&self) -> String {
        let arr = |v: Vec<serde_json::Value>| serde_json::Value::Array(v);
        serde_json::json!({
            "key_schema": arr(self.key_schema.iter().map(ToJson::to_json).collect()),
            "attr_defs": arr(self.attr_defs.iter().map(ToJson::to_json).collect()),
            "gsis": arr(self.gsis.iter().map(ToJson::to_json).collect()),
            "lsis": arr(self.lsis.iter().map(ToJson::to_json).collect()),
            "billing_mode": self.billing_mode,
            "throughput": self.throughput.as_ref().map(ToJson::to_json),
            "stream": self.stream.as_ref().map(ToJson::to_json),
            "tags": arr(self.tags.iter().map(ToJson::to_json).collect()),
            "sse": self.sse.as_ref().map(ToJson::to_json),
            "table_class": self.table_class,
            "deletion_protection": self.deletion_protection,
        })
        .to_string()
    }

    pub fn from_meta(
        meta: &str,
        id: &str,
        name: &str,
        ctx: &RequestContext,
        created_at: i64,
    ) -> Table {
        let m: serde_json::Value = serde_json::from_str(meta).unwrap_or_default();
        let list = |k: &str| m[k].to_string();
        let opt = |k: &str| {
            if m[k].is_null() {
                None
            } else {
                Some(m[k].to_string())
            }
        };
        Table {
            id: id.to_string(),
            account: ctx.account_id.clone(),
            region: ctx.region.clone(),
            name: name.to_string(),
            created_at,
            key_schema: from_list(&list("key_schema")),
            attr_defs: from_list(&list("attr_defs")),
            gsis: from_list(&list("gsis")),
            lsis: from_list(&list("lsis")),
            billing_mode: m["billing_mode"]
                .as_str()
                .unwrap_or("PROVISIONED")
                .to_string(),
            throughput: from_opt(opt("throughput")),
            stream: from_opt(opt("stream")),
            tags: from_list(&list("tags")),
            ttl_attr: None,
            ttl_enabled: false,
            deletion_protection: m["deletion_protection"].as_bool().unwrap_or(false),
            sse: from_opt(opt("sse")),
            table_class: m["table_class"].as_str().map(String::from),
            pitr: false,
        }
    }

    pub fn insert(&self, tx: &Transaction) -> Result<(), AwsError> {
        tx.execute(
            "INSERT INTO tables (account_id, region, name, table_id, created_at, key_schema, attr_defs, gsis, lsis,
                                 billing_mode, throughput, stream_spec, tags, ttl_attr, ttl_enabled, deletion_protection,
                                 sse, table_class, pitr)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                self.account,
                self.region,
                self.name,
                self.id,
                self.created_at,
                list_json(&self.key_schema),
                list_json(&self.attr_defs),
                list_json(&self.gsis),
                list_json(&self.lsis),
                self.billing_mode,
                opt_json(&self.throughput),
                opt_json(&self.stream),
                list_json(&self.tags),
                self.ttl_attr,
                i64::from(self.ttl_enabled),
                i64::from(self.deletion_protection),
                opt_json(&self.sse),
                self.table_class,
                i64::from(self.pitr)
            ],
        )?;
        Ok(())
    }

    /// Writes back everything that `UpdateTable`, tagging and TTL can change.
    pub fn save(&self, tx: &Transaction) -> Result<(), AwsError> {
        tx.execute(
            "UPDATE tables SET attr_defs = ?1, gsis = ?2, billing_mode = ?3, throughput = ?4, stream_spec = ?5,
                               tags = ?6, ttl_attr = ?7, ttl_enabled = ?8, deletion_protection = ?9, sse = ?10,
                               table_class = ?11, pitr = ?12
             WHERE table_id = ?13",
            params![
                list_json(&self.attr_defs),
                list_json(&self.gsis),
                self.billing_mode,
                opt_json(&self.throughput),
                opt_json(&self.stream),
                list_json(&self.tags),
                self.ttl_attr,
                i64::from(self.ttl_enabled),
                i64::from(self.deletion_protection),
                opt_json(&self.sse),
                self.table_class,
                i64::from(self.pitr),
                self.id
            ],
        )?;
        Ok(())
    }

    fn attr_type(&self, name: &str) -> Option<String> {
        self.attr_defs
            .iter()
            .find(|a| a.attribute_name == name)
            .map(|a| a.attribute_type.clone())
    }

    pub fn schema_of(&self, key_schema: &[KeySchemaElement]) -> Result<KeySchema, AwsError> {
        let make = |e: &KeySchemaElement| -> Result<KeyAttr, AwsError> {
            Ok(KeyAttr {
                name: e.attribute_name.clone(),
                ty: self
                    .attr_type(&e.attribute_name)
                    .ok_or_else(|| ve("Invalid key schema: attribute not defined"))?,
            })
        };
        let hash = key_schema
            .iter()
            .find(|e| e.key_type == "HASH")
            .ok_or_else(|| ve("Missing hash key"))?;
        let range = key_schema.iter().find(|e| e.key_type == "RANGE");
        Ok(KeySchema {
            hash: make(hash)?,
            range: range.map(make).transpose()?,
        })
    }

    pub fn schema(&self) -> Result<KeySchema, AwsError> {
        self.schema_of(&self.key_schema)
    }

    pub fn describe(&self, tx: &Transaction) -> Result<TableDescription, AwsError> {
        let (count, size): (i64, i64) = tx.query_row(
            "SELECT COUNT(*), COALESCE(SUM(length(item)), 0) FROM items WHERE table_id = ?1",
            params![self.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let pay_per_request = self.billing_mode == "PAY_PER_REQUEST";
        let throughput = |t: &Option<ProvisionedThroughput>| ProvisionedThroughputDescription {
            read_capacity_units: Some(if pay_per_request {
                0
            } else {
                t.as_ref().map_or(0, |t| t.read_capacity_units)
            }),
            write_capacity_units: Some(if pay_per_request {
                0
            } else {
                t.as_ref().map_or(0, |t| t.write_capacity_units)
            }),
            number_of_decreases_today: Some(0),
            ..Default::default()
        };
        let mut d = TableDescription {
            table_name: Some(self.name.clone()),
            table_arn: Some(self.arn()),
            table_id: Some(self.id.clone()),
            table_status: Some("ACTIVE".into()),
            creation_date_time: Some(Timestamp(self.created_at)),
            key_schema: self.key_schema.clone(),
            attribute_definitions: self.attr_defs.clone(),
            item_count: Some(count),
            table_size_bytes: Some(size),
            provisioned_throughput: Some(throughput(&self.throughput)),
            deletion_protection_enabled: Some(self.deletion_protection),
            billing_mode_summary: pay_per_request.then(|| BillingModeSummary {
                billing_mode: Some("PAY_PER_REQUEST".into()),
                last_update_to_pay_per_request_date_time: Some(Timestamp(self.created_at)),
            }),
            ..Default::default()
        };
        for g in &self.gsis {
            d.global_secondary_indexes
                .push(GlobalSecondaryIndexDescription {
                    index_name: Some(g.index_name.clone()),
                    key_schema: g.key_schema.clone(),
                    projection: Some(g.projection.clone()),
                    index_status: Some("ACTIVE".into()),
                    backfilling: Some(false),
                    provisioned_throughput: Some(throughput(&g.provisioned_throughput)),
                    index_size_bytes: Some(0),
                    item_count: Some(0),
                    index_arn: Some(format!("{}/index/{}", self.arn(), g.index_name)),
                    ..Default::default()
                });
        }
        for l in &self.lsis {
            d.local_secondary_indexes
                .push(LocalSecondaryIndexDescription {
                    index_name: Some(l.index_name.clone()),
                    key_schema: l.key_schema.clone(),
                    projection: Some(l.projection.clone()),
                    index_size_bytes: Some(0),
                    item_count: Some(0),
                    index_arn: Some(format!("{}/index/{}", self.arn(), l.index_name)),
                });
        }
        if let Some(s) = &self.stream {
            if s.stream_enabled {
                let label = Timestamp(self.created_at).to_iso8601();
                d.latest_stream_arn = Some(format!("{}/stream/{label}", self.arn()));
                d.latest_stream_label = Some(label);
                d.stream_specification = Some(s.clone());
            }
        }
        if let Some(sse) = &self.sse {
            if sse.enabled.unwrap_or(false) {
                d.sse_description = Some(SSEDescription {
                    status: Some("ENABLED".into()),
                    sse_type: sse.sse_type.clone().or(Some("KMS".into())),
                    kms_master_key_arn: sse.kms_master_key_id.clone(),
                    ..Default::default()
                });
            }
        }
        if let Some(c) = &self.table_class {
            d.table_class_summary = Some(TableClassSummary {
                table_class: Some(c.clone()),
                ..Default::default()
            });
        }
        Ok(d)
    }
}

pub fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}
