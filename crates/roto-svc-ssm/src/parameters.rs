use std::sync::Arc;

use crate::models::{ParameterRow, TagInsert};
use crate::schema::{parameters, resource_tags};
use diesel::{SqliteConnection, prelude::*};
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;

use crate::MIGRATIONS;
use crate::generated::*;

const VERSION_LIMIT: usize = 100;

pub struct Ssm {
    db: Arc<Db>,
}

impl Ssm {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("ssm", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(parameters::table).execute(tx)?;
            diesel::delete(resource_tags::table).execute(tx)?;
            Ok(())
        })
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn ve(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationException", message)
}

fn not_found(name: &str) -> AwsError {
    AwsError::sender(
        400,
        "ParameterNotFound",
        format!("Parameter {name} not found."),
    )
}

#[derive(Clone)]
struct Param {
    name: String,
    version: i64,
    ty: String,
    /// Stored as given; SecureString values carry the moto-style `kms:<key>:` prefix.
    value: String,
    description: Option<String>,
    allowed_pattern: Option<String>,
    key_id: Option<String>,
    data_type: String,
    tier: String,
    policies: Option<String>,
    labels: Vec<String>,
    last_modified: i64,
}

impl From<ParameterRow> for Param {
    fn from(r: ParameterRow) -> Self {
        Self {
            name: r.name,
            version: r.version,
            ty: r.ty,
            value: r.value,
            description: r.description,
            allowed_pattern: r.allowed_pattern,
            key_id: r.key_id,
            data_type: r.data_type,
            tier: r.tier,
            policies: r.policies,
            labels: serde_json::from_str(&r.labels).unwrap_or_default(),
            last_modified: r.last_modified,
        }
    }
}

fn versions(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
) -> Result<Vec<Param>, AwsError> {
    Ok(parameters::table
        .filter(parameters::account_id.eq(&ctx.account_id))
        .filter(parameters::region.eq(&ctx.region))
        .filter(parameters::name.eq(name))
        .order(parameters::version)
        .select(ParameterRow::as_select())
        .load::<ParameterRow>(tx)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// Latest version of every parameter, ordered by name.
fn latest_all(tx: &mut SqliteConnection, ctx: &RequestContext) -> Result<Vec<Param>, AwsError> {
    let previous = diesel::alias!(parameters as previous);
    let latest = previous
        .filter(
            previous
                .field(parameters::account_id)
                .eq(parameters::account_id),
        )
        .filter(previous.field(parameters::region).eq(parameters::region))
        .filter(previous.field(parameters::name).eq(parameters::name))
        .select(diesel::dsl::max(previous.field(parameters::version)))
        .single_value();
    Ok(parameters::table
        .filter(parameters::account_id.eq(&ctx.account_id))
        .filter(parameters::region.eq(&ctx.region))
        .filter(parameters::version.nullable().eq(latest))
        .order(parameters::name)
        .select(ParameterRow::as_select())
        .load::<ParameterRow>(tx)?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// `name`, `name:3` (version) or `name:label`.
fn resolve(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    selector: &str,
) -> Result<Option<Param>, AwsError> {
    // A parameter ARN names the parameter after `:parameter`; the leading slash is optional.
    if selector.starts_with("arn:") {
        let Some((_, rest)) = selector.split_once(":parameter") else {
            return Ok(None);
        };
        return match resolve(tx, ctx, rest)? {
            Some(p) => Ok(Some(p)),
            None => resolve(tx, ctx, rest.trim_start_matches('/')),
        };
    }
    let parts: Vec<&str> = selector.split(':').collect();
    if parts.len() > 2 {
        return Ok(None);
    }
    let all = versions(tx, ctx, parts[0])?;
    match (parts.get(1), all.is_empty()) {
        (_, true) => Ok(None),
        (None, false) => Ok(all.last().cloned()),
        (Some(v), false) if v.chars().all(|c| c.is_ascii_digit()) => {
            let n: i64 = v.parse().unwrap_or(-1);
            match all.iter().find(|p| p.version == n) {
                Some(p) => Ok(Some(p.clone())),
                None => Err(AwsError::sender(
                    400,
                    "ParameterVersionNotFound",
                    format!(
                        "Systems Manager could not find version {v} of {}. Verify the version and try again.",
                        parts[0]
                    ),
                )),
            }
        }
        (Some(label), false) => Ok(all
            .iter()
            .rev()
            .find(|p| p.labels.iter().any(|l| l == label))
            .cloned()),
    }
}

fn arn(ctx: &RequestContext, name: &str) -> String {
    let part = if ctx.region.starts_with("cn-") {
        "aws-cn"
    } else {
        "aws"
    };
    if name.starts_with('/') {
        format!(
            "arn:{part}:ssm:{}:{}:parameter{name}",
            ctx.region, ctx.account_id
        )
    } else {
        format!(
            "arn:{part}:ssm:{}:{}:parameter/{name}",
            ctx.region, ctx.account_id
        )
    }
}

fn decrypted(p: &Param) -> String {
    if p.ty != "SecureString" {
        return p.value.clone();
    }
    let prefix = format!("kms:{}:", p.key_id.as_deref().unwrap_or("default"));
    p.value
        .strip_prefix(&prefix)
        .unwrap_or(&p.value)
        .to_string()
}

fn to_parameter(ctx: &RequestContext, p: &Param, decrypt: bool) -> Parameter {
    Parameter {
        name: Some(p.name.clone()),
        r#type: Some(p.ty.clone()),
        value: Some(if decrypt {
            decrypted(p)
        } else {
            p.value.clone()
        }),
        version: Some(p.version),
        last_modified_date: Some(Timestamp(p.last_modified)),
        arn: Some(arn(ctx, &p.name)),
        data_type: Some(p.data_type.clone()),
        selector: None,
        source_result: None,
    }
}

fn to_metadata(ctx: &RequestContext, p: &Param) -> ParameterMetadata {
    ParameterMetadata {
        name: Some(p.name.clone()),
        r#type: Some(p.ty.clone()),
        arn: Some(arn(ctx, &p.name)),
        version: Some(p.version),
        last_modified_date: Some(Timestamp(p.last_modified)),
        last_modified_user: Some("N/A".into()),
        description: p.description.clone(),
        key_id: p.key_id.clone(),
        allowed_pattern: p.allowed_pattern.clone(),
        data_type: Some(p.data_type.clone()),
        tier: Some(p.tier.clone()),
        policies: Vec::new(),
    }
}

fn page<T>(
    items: Vec<T>,
    token: &Option<String>,
    max: usize,
) -> Result<(Vec<T>, Option<String>), AwsError> {
    let start = match token.as_deref() {
        None | Some("") => 0,
        Some(t) => t.trim().parse::<usize>().map_err(|_| {
            AwsError::sender(400, "InvalidNextToken", "The specified token is not valid")
        })?,
    };
    let total = items.len();
    let out: Vec<T> = items.into_iter().skip(start).take(max).collect();
    // A full page always carries a token, even when it was the last one (as moto does).
    let _ = total;
    let next = (out.len() == max).then(|| (start + out.len()).to_string());
    Ok((out, next))
}

fn matches_filter(p: &Param, tags: &[Tag], key: &str, option: &str, values: &[String]) -> bool {
    let name = format!("/{}", p.name.trim_start_matches('/'));
    let subject: Vec<String> = match key {
        "Name" => vec![if option == "Contains" {
            p.name.clone()
        } else {
            name.clone()
        }],
        "Path" => vec![name.clone()],
        "Type" => vec![p.ty.clone()],
        "KeyId" => p.key_id.iter().cloned().collect(),
        "Label" => p.labels.clone(),
        "Tier" => vec![p.tier.clone()],
        "DataType" => vec![p.data_type.clone()],
        k if k.starts_with("tag:") => tags
            .iter()
            .filter(|t| t.key == k[4..])
            .map(|t| t.value.clone())
            .collect(),
        "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
        _ => return true,
    };
    if subject.is_empty() {
        return false;
    }
    let norm = |v: &String| {
        if key == "Name" && option != "Contains" {
            format!("/{}", v.trim_start_matches('/'))
        } else {
            v.clone()
        }
    };
    if key == "Path" {
        return values.iter().any(|v| {
            let v = format!("/{}", v.trim_matches('/'));
            let depth = |s: &str| s.split('/').count();
            match option {
                "Recursive" => v == "/" || name.starts_with(&format!("{v}/")),
                _ => {
                    if v == "/" {
                        depth(&name) == 2
                    } else {
                        name.starts_with(&format!("{v}/")) && depth(&name) == depth(&v) + 1
                    }
                }
            }
        });
    }
    if values.is_empty() {
        return true; // tag-existence style filter
    }
    subject.iter().any(|s| {
        values.iter().any(|v| {
            let v = norm(v);
            match option {
                "BeginsWith" => s.starts_with(&v),
                "Contains" => s.contains(&v),
                _ => *s == v,
            }
        })
    })
}

fn load_tags(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    ty: &str,
    id: &str,
) -> Result<Vec<Tag>, AwsError> {
    Ok(resource_tags::table
        .filter(resource_tags::account_id.eq(&ctx.account_id))
        .filter(resource_tags::region.eq(&ctx.region))
        .filter(resource_tags::resource_type.eq(ty))
        .filter(resource_tags::resource_id.eq(id))
        .order(resource_tags::seq)
        .select((resource_tags::key, resource_tags::value))
        .load::<(String, String)>(tx)?
        .into_iter()
        .map(|(key, value)| Tag { key, value })
        .collect())
}

fn set_tags(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    ty: &str,
    id: &str,
    tags: &[Tag],
) -> Result<(), AwsError> {
    for t in tags {
        let row = TagInsert {
            account_id: &ctx.account_id,
            region: &ctx.region,
            resource_type: ty,
            resource_id: id,
            key: &t.key,
            value: &t.value,
        };
        diesel::insert_into(resource_tags::table)
            .values(&row)
            .on_conflict((
                resource_tags::account_id,
                resource_tags::region,
                resource_tags::resource_type,
                resource_tags::resource_id,
                resource_tags::key,
            ))
            .do_update()
            .set(resource_tags::value.eq(diesel::upsert::excluded(resource_tags::value)))
            .execute(tx)?;
    }
    Ok(())
}

fn check_resource(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    ty: &str,
    id: &str,
) -> Result<(), AwsError> {
    match ty {
        "Parameter" => {
            if versions(tx, ctx, id)?.is_empty() {
                return Err(AwsError::sender(
                    400,
                    "InvalidResourceId",
                    "Invalid Resource Id",
                ));
            }
            Ok(())
        }
        "Document" | "MaintenanceWindow" | "ManagedInstance" | "PatchBaseline" | "OpsItem"
        | "OpsMetadata" => Err(AwsError::sender(
            400,
            "InvalidResourceId",
            "Invalid Resource Id",
        )),
        _ => Err(AwsError::sender(
            400,
            "InvalidResourceType",
            "Invalid Resource Type",
        )),
    }
}

impl Service for Ssm {
    fn put_parameter(
        &self,
        ctx: &RequestContext,
        i: PutParameterRequest,
    ) -> Result<PutParameterResult, AwsError> {
        let overwrite = i.overwrite.unwrap_or(false);
        if i.value.is_empty() {
            return Err(ve(
                "1 validation error detected: Value '' at 'value' failed to satisfy constraint: Member must have length greater than or equal to 1.",
            ));
        }
        if overwrite && !i.tags.is_empty() {
            return Err(ve(
                "Invalid request: tags and overwrite can't be used together. To create a parameter with tags, please remove overwrite flag. To update tags for an existing parameter, please use AddTagsToResource or RemoveTagsFromResource.",
            ));
        }
        let lower = i.name.to_lowercase();
        let bare = lower.trim_start_matches('/');
        if bare.starts_with("aws") || bare.starts_with("ssm") {
            let is_path = i.name.matches('/').count() > 1;
            if lower.starts_with("/aws") && is_path {
                return Err(AwsError::sender(
                    400,
                    "AccessDeniedException",
                    format!("No access to reserved parameter name: {}.", i.name),
                ));
            }
            return Err(ve(if is_path {
                "Parameter name: can't be prefixed with \"ssm\" (case-insensitive). If formed as a path, it can consist of sub-paths divided by slash symbol; each sub-path can be formed as a mix of letters, numbers and the following 3 symbols .-_"
            } else {
                "Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive)."
            }));
        }
        if let Some(t) = &i.r#type {
            if !matches!(t.as_str(), "String" | "StringList" | "SecureString") {
                return Err(ve(format!(
                    "1 validation error detected: Value '{t}' at 'type' failed to satisfy constraint: Member must satisfy enum value set: [SecureString, StringList, String]"
                )));
            }
        }
        let data_type = i.data_type.clone().unwrap_or_else(|| "text".into());
        if !matches!(data_type.as_str(), "text" | "aws:ec2:image") {
            return Err(ve(format!(
                "The following data type is not supported: {data_type} (Data type names are all lowercase.)"
            )));
        }
        self.db.transaction(|tx| {
            let prior = versions(tx, ctx, &i.name)?;
            let previous = prior.last().cloned();
            if previous.is_none() && i.r#type.is_none() {
                return Err(ve("A parameter type is required when you create a parameter."));
            }
            if previous.is_some() && !overwrite {
                return Err(AwsError::sender(
                    400,
                    "ParameterAlreadyExists",
                    "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
                ));
            }
            if prior.len() >= VERSION_LIMIT {
                let oldest = &prior[0];
                if !oldest.labels.is_empty() {
                    return Err(AwsError::sender(
                        400,
                        "ParameterMaxVersionLimitExceeded",
                        format!("You attempted to create a new version of {} by calling the PutParameter API with the overwrite flag. Version {}, the oldest version, can't be deleted because it has a label associated with it. Move the label to another version of the parameter, and try again.", i.name, oldest.version),
                    ));
                }
                diesel::delete(parameters::table.filter(parameters::account_id.eq(&ctx.account_id)).filter(parameters::region.eq(&ctx.region)).filter(parameters::name.eq(&i.name)).filter(parameters::version.eq(&oldest.version))).execute(tx)?;
            }
            let ty = i.r#type.clone().or_else(|| previous.as_ref().map(|p| p.ty.clone())).unwrap_or_else(|| "String".into());
            let key_id = i.key_id.clone().or_else(|| previous.as_ref().and_then(|p| p.key_id.clone())).or_else(|| (ty == "SecureString").then(|| "alias/aws/ssm".to_string()));
            let value = if ty == "SecureString" { format!("kms:{}:{}", key_id.as_deref().unwrap_or("default"), i.value) } else { i.value.clone() };
            let tier = i.tier.clone().or_else(|| previous.as_ref().map(|p| p.tier.clone())).unwrap_or_else(|| "Standard".into());
            let version = previous.as_ref().map_or(1, |p| p.version + 1);
            let row=ParameterRow {
                account_id:ctx.account_id.clone(),region:ctx.region.clone(),name:i.name.clone(),version,ty,value,
                description:i.description.clone().or_else(|| previous.as_ref().and_then(|p|p.description.clone())),
                allowed_pattern:i.allowed_pattern.clone().or_else(|| previous.as_ref().and_then(|p|p.allowed_pattern.clone())),
                key_id,data_type,tier:tier.clone(),policies:i.policies.clone().or_else(|| previous.as_ref().and_then(|p|p.policies.clone())),labels:"[]".into(),
                last_modified:previous.as_ref().map_or_else(now,|p|now().max(p.last_modified+1)),
            };
            diesel::insert_into(parameters::table).values(&row).execute(tx)?;
            set_tags(tx, ctx, "Parameter", &i.name, &i.tags)?;
            Ok(PutParameterResult { version: Some(version), tier: Some(tier) })
        })
    }

    fn get_parameter(
        &self,
        ctx: &RequestContext,
        i: GetParameterRequest,
    ) -> Result<GetParameterResult, AwsError> {
        if i.name.starts_with("/aws/reference/secretsmanager/") && i.with_decryption != Some(true) {
            return Err(ve(
                "WithDecryption flag must be True for retrieving a Secret Manager secret.",
            ));
        }
        self.db.transaction(|tx| {
            let p = resolve(tx, ctx, &i.name)?.ok_or_else(|| not_found(&i.name))?;
            Ok(GetParameterResult {
                parameter: Some(to_parameter(ctx, &p, i.with_decryption.unwrap_or(false))),
            })
        })
    }

    fn get_parameters(
        &self,
        ctx: &RequestContext,
        i: GetParametersRequest,
    ) -> Result<GetParametersResult, AwsError> {
        if i.names.len() > 10 {
            return Err(ve(format!(
                "1 validation error detected: Value '[{}]' at 'names' failed to satisfy constraint: Member must have length less than or equal to 10.",
                i.names.join(", ")
            )));
        }
        self.db.transaction(|tx| {
            let mut out = GetParametersResult::default();
            let mut seen = std::collections::BTreeSet::new();
            for n in i.names.iter().filter(|n| seen.insert(n.as_str())) {
                match resolve(tx, ctx, n) {
                    Ok(Some(p)) => out.parameters.push(to_parameter(
                        ctx,
                        &p,
                        i.with_decryption.unwrap_or(false),
                    )),
                    _ => out.invalid_parameters.push(n.clone()),
                }
            }
            Ok(out)
        })
    }

    fn get_parameters_by_path(
        &self,
        ctx: &RequestContext,
        i: GetParametersByPathRequest,
    ) -> Result<GetParametersByPathResult, AwsError> {
        let max = i.max_results.unwrap_or(10);
        if !(1..=10).contains(&max) {
            return Err(ve(format!(
                "1 validation error detected: Value '{max}' at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to 10"
            )));
        }
        if !i.path.starts_with('/') {
            return Err(ve(
                "The parameter doesn't meet the parameter name requirements. The parameter name must begin with a forward slash \"/\". It can't be prefixed with \"aws\" or \"ssm\" (case-insensitive). It must use only letters, numbers, or the following symbols: . (period), - (hyphen), _ (underscore). Special characters are not allowed. All sub-paths, if specified, must use the forward slash symbol \"/\". Valid example: /get/parameters2-/by1./path0_.",
            ));
        }
        self.db.transaction(|tx| {
            let prefix = format!("{}/", i.path.trim_end_matches('/'));
            let recursive = i.recursive.unwrap_or(false);
            let mut matched = Vec::new();
            for p in latest_all(tx, ctx)? {
                let normalized = format!("/{}", p.name.trim_start_matches('/'));
                let Some(rest) = normalized.strip_prefix(&prefix) else {
                    continue;
                };
                if !recursive && rest.contains('/') {
                    continue;
                }
                let tags = load_tags(tx, ctx, "Parameter", &p.name)?;
                if i.parameter_filters.iter().all(|f| {
                    matches_filter(
                        &p,
                        &tags,
                        &f.key,
                        f.option.as_deref().unwrap_or("Equals"),
                        &f.values,
                    )
                }) {
                    matched.push(p);
                }
            }
            let (items, next_token) = page(matched, &i.next_token, max as usize)?;
            Ok(GetParametersByPathResult {
                parameters: items
                    .iter()
                    .map(|p| to_parameter(ctx, p, i.with_decryption.unwrap_or(false)))
                    .collect(),
                next_token,
            })
        })
    }

    fn delete_parameter(
        &self,
        ctx: &RequestContext,
        i: DeleteParameterRequest,
    ) -> Result<DeleteParameterResult, AwsError> {
        self.db.transaction(|tx| {
            let n = diesel::delete(
                parameters::table
                    .filter(parameters::account_id.eq(&ctx.account_id))
                    .filter(parameters::region.eq(&ctx.region))
                    .filter(parameters::name.eq(&i.name)),
            )
            .execute(tx)?;
            if n == 0 {
                return Err(not_found(&i.name));
            }
            diesel::delete(
                resource_tags::table
                    .filter(resource_tags::account_id.eq(&ctx.account_id))
                    .filter(resource_tags::region.eq(&ctx.region))
                    .filter(resource_tags::resource_type.eq("Parameter"))
                    .filter(resource_tags::resource_id.eq(&i.name)),
            )
            .execute(tx)?;
            Ok(DeleteParameterResult::default())
        })
    }

    fn delete_parameters(
        &self,
        ctx: &RequestContext,
        i: DeleteParametersRequest,
    ) -> Result<DeleteParametersResult, AwsError> {
        self.db.transaction(|tx| {
            let mut out = DeleteParametersResult::default();
            for n in &i.names {
                let removed = diesel::delete(
                    parameters::table
                        .filter(parameters::account_id.eq(&ctx.account_id))
                        .filter(parameters::region.eq(&ctx.region))
                        .filter(parameters::name.eq(&n)),
                )
                .execute(tx)?;
                if removed > 0 {
                    out.deleted_parameters.push(n.clone())
                } else {
                    out.invalid_parameters.push(n.clone())
                }
            }
            Ok(out)
        })
    }

    fn describe_parameters(
        &self,
        ctx: &RequestContext,
        i: DescribeParametersRequest,
    ) -> Result<DescribeParametersResult, AwsError> {
        let max = i.max_results.unwrap_or(10);
        if !(1..=50).contains(&max) {
            return Err(ve(format!(
                "1 validation error detected: Value '{max}' at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to 50"
            )));
        }
        self.db.transaction(|tx| {
            let mut matched = Vec::new();
            for p in latest_all(tx, ctx)? {
                let tags = load_tags(tx, ctx, "Parameter", &p.name)?;
                let by_filters = i
                    .filters
                    .iter()
                    .all(|f| matches_filter(&p, &tags, &f.key, "Equals", &f.values));
                let by_param_filters = i.parameter_filters.iter().all(|f| {
                    matches_filter(
                        &p,
                        &tags,
                        &f.key,
                        f.option.as_deref().unwrap_or(if f.key == "Path" {
                            "OneLevel"
                        } else {
                            "Equals"
                        }),
                        &f.values,
                    )
                });
                if by_filters && by_param_filters {
                    matched.push(p);
                }
            }
            let (items, next_token) = page(matched, &i.next_token, max as usize)?;
            Ok(DescribeParametersResult {
                parameters: items.iter().map(|p| to_metadata(ctx, p)).collect(),
                next_token,
            })
        })
    }

    fn get_parameter_history(
        &self,
        ctx: &RequestContext,
        i: GetParameterHistoryRequest,
    ) -> Result<GetParameterHistoryResult, AwsError> {
        let max = i.max_results.unwrap_or(50);
        if max > 50 {
            return Err(ve(format!(
                "1 validation error detected: Value '{max}' at 'maxResults' failed to satisfy constraint: Member must have value less than or equal to 50."
            )));
        }
        self.db.transaction(|tx| {
            let all = versions(tx, ctx, &i.name)?;
            if all.is_empty() {
                return Err(not_found(&i.name));
            }
            let (items, next_token) = page(all, &i.next_token, max.max(1) as usize)?;
            let decrypt = i.with_decryption.unwrap_or(false);
            Ok(GetParameterHistoryResult {
                parameters: items
                    .iter()
                    .map(|p| ParameterHistory {
                        name: Some(p.name.clone()),
                        r#type: Some(p.ty.clone()),
                        key_id: p.key_id.clone(),
                        last_modified_date: Some(Timestamp(p.last_modified)),
                        last_modified_user: Some("N/A".into()),
                        description: p.description.clone(),
                        value: Some(if decrypt {
                            decrypted(p)
                        } else {
                            p.value.clone()
                        }),
                        allowed_pattern: p.allowed_pattern.clone(),
                        version: Some(p.version),
                        labels: p.labels.clone(),
                        tier: Some(p.tier.clone()),
                        policies: Vec::new(),
                        data_type: Some(p.data_type.clone()),
                    })
                    .collect(),
                next_token,
            })
        })
    }

    fn label_parameter_version(
        &self,
        ctx: &RequestContext,
        i: LabelParameterVersionRequest,
    ) -> Result<LabelParameterVersionResult, AwsError> {
        self.db.transaction(|tx| {
            let all = versions(tx, ctx, &i.name)?;
            if all.is_empty() {
                return Err(not_found(&i.name));
            }
            let version = i.parameter_version.unwrap_or_else(|| all.last().map_or(1, |p| p.version));
            let Some(target) = all.iter().find(|p| p.version == version) else {
                return Err(AwsError::sender(
                    400,
                    "ParameterVersionNotFound",
                    format!("Systems Manager could not find version {version} of {}. Verify the version and try again.", i.name),
                ));
            };
            let mut invalid = Vec::new();
            let mut add = Vec::new();
            for l in &i.labels {
                if l.starts_with("aws") || l.starts_with("ssm") || l.chars().next().is_some_and(|c| c.is_ascii_digit()) || !l.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')) {
                    invalid.push(l.clone());
                    continue;
                }
                if l.len() > 100 {
                    return Err(ve(format!("1 validation error detected: Value '[{l}]' at 'labels' failed to satisfy constraint: Member must satisfy constraint: [Member must have length less than or equal to 100, Member must have length greater than or equal to 1]")));
                }
                if !target.labels.contains(l) && !add.contains(l) {
                    add.push(l.clone());
                }
            }
            if target.labels.len() + add.len() > 10 {
                return Err(AwsError::sender(
                    400,
                    "ParameterVersionLabelLimitExceeded",
                    "An error occurred (ParameterVersionLabelLimitExceeded) when calling the LabelParameterVersion operation: A parameter version can have maximum 10 labels.Move one or more labels to another version and try again.",
                ));
            }
            // A label lives on one version only: move it.
            for p in &all {
                let mut labels = p.labels.clone();
                if p.version == version {
                    labels.extend(add.iter().cloned());
                } else {
                    labels.retain(|l| !i.labels.contains(l) || invalid.contains(l));
                }
                diesel::update(parameters::table.filter(parameters::account_id.eq(&ctx.account_id)).filter(parameters::region.eq(&ctx.region)).filter(parameters::name.eq(&i.name)).filter(parameters::version.eq(&p.version))).set(parameters::labels.eq(&serde_json::to_string(&labels).unwrap_or_default())).execute(tx)?;
            }
            Ok(LabelParameterVersionResult { invalid_labels: invalid, parameter_version: Some(version) })
        })
    }

    fn unlabel_parameter_version(
        &self,
        ctx: &RequestContext,
        i: UnlabelParameterVersionRequest,
    ) -> Result<UnlabelParameterVersionResult, AwsError> {
        self.db.transaction(|tx| {
            let all = versions(tx, ctx, &i.name)?;
            if all.is_empty() {
                return Err(not_found(&i.name));
            }
            let Some(target) = all.iter().find(|p| p.version == i.parameter_version) else {
                return Err(AwsError::sender(
                    400,
                    "ParameterVersionNotFound",
                    format!("Systems Manager could not find version {} of {}. Verify the version and try again.", i.parameter_version, i.name),
                ));
            };
            let (removed, invalid): (Vec<String>, Vec<String>) = i.labels.iter().cloned().partition(|l| target.labels.contains(l));
            let kept: Vec<String> = target.labels.iter().filter(|l| !removed.contains(l)).cloned().collect();
            diesel::update(parameters::table.filter(parameters::account_id.eq(&ctx.account_id)).filter(parameters::region.eq(&ctx.region)).filter(parameters::name.eq(&i.name)).filter(parameters::version.eq(&i.parameter_version))).set(parameters::labels.eq(&serde_json::to_string(&kept).unwrap_or_default())).execute(tx)?;
            Ok(UnlabelParameterVersionResult { removed_labels: removed, invalid_labels: invalid })
        })
    }

    fn add_tags_to_resource(
        &self,
        ctx: &RequestContext,
        i: AddTagsToResourceRequest,
    ) -> Result<AddTagsToResourceResult, AwsError> {
        self.db.transaction(|tx| {
            check_resource(tx, ctx, &i.resource_type, &i.resource_id)?;
            set_tags(tx, ctx, &i.resource_type, &i.resource_id, &i.tags)?;
            Ok(AddTagsToResourceResult::default())
        })
    }

    fn remove_tags_from_resource(
        &self,
        ctx: &RequestContext,
        i: RemoveTagsFromResourceRequest,
    ) -> Result<RemoveTagsFromResourceResult, AwsError> {
        self.db.transaction(|tx| {
            check_resource(tx, ctx, &i.resource_type, &i.resource_id)?;
            for k in &i.tag_keys {
                diesel::delete(
                    resource_tags::table
                        .filter(resource_tags::account_id.eq(&ctx.account_id))
                        .filter(resource_tags::region.eq(&ctx.region))
                        .filter(resource_tags::resource_type.eq(&i.resource_type))
                        .filter(resource_tags::resource_id.eq(&i.resource_id))
                        .filter(resource_tags::key.eq(&k)),
                )
                .execute(tx)?;
            }
            Ok(RemoveTagsFromResourceResult::default())
        })
    }

    fn list_tags_for_resource(
        &self,
        ctx: &RequestContext,
        i: ListTagsForResourceRequest,
    ) -> Result<ListTagsForResourceResult, AwsError> {
        self.db.transaction(|tx| {
            check_resource(tx, ctx, &i.resource_type, &i.resource_id)?;
            Ok(ListTagsForResourceResult {
                tag_list: load_tags(tx, ctx, &i.resource_type, &i.resource_id)?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn put(
        ssm: &Ssm,
        c: &RequestContext,
        name: &str,
        value: &str,
        overwrite: bool,
    ) -> Result<PutParameterResult, AwsError> {
        ssm.put_parameter(
            c,
            PutParameterRequest {
                name: name.into(),
                value: value.into(),
                r#type: Some("SecureString".into()),
                overwrite: Some(overwrite),
                ..Default::default()
            },
        )
    }
    fn get(ssm: &Ssm, c: &RequestContext, name: &str) -> Parameter {
        ssm.get_parameter(
            c,
            GetParameterRequest {
                name: name.into(),
                with_decryption: Some(true),
            },
        )
        .unwrap()
        .parameter
        .unwrap()
    }
    #[test]
    fn labels_tags_latest_and_scope_survive_restart() {
        let path = std::env::temp_dir().join(format!("roto-ssm-{}", uuid::Uuid::new_v4()));
        let c = ctx();
        let mut region = c.clone();
        region.region = "eu-west-1".into();
        let mut account = c.clone();
        account.account_id = "999999999999".into();
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let ssm = Ssm::new(&store).unwrap();
            put(&ssm, &c, "/app/key", "one", false).unwrap();
            put(&ssm, &c, "/app/key", "two", true).unwrap();
            put(&ssm, &region, "/app/key", "region", false).unwrap();
            put(&ssm, &account, "/app/key", "account", false).unwrap();
            ssm.label_parameter_version(
                &c,
                LabelParameterVersionRequest {
                    name: "/app/key".into(),
                    parameter_version: Some(1),
                    labels: vec!["stable".into()],
                },
            )
            .unwrap();
            ssm.add_tags_to_resource(
                &c,
                AddTagsToResourceRequest {
                    resource_type: "Parameter".into(),
                    resource_id: "/app/key".into(),
                    tags: vec![Tag {
                        key: "team".into(),
                        value: "one".into(),
                    }],
                },
            )
            .unwrap();
            ssm.add_tags_to_resource(
                &c,
                AddTagsToResourceRequest {
                    resource_type: "Parameter".into(),
                    resource_id: "/app/key".into(),
                    tags: vec![
                        Tag {
                            key: "team".into(),
                            value: "two".into(),
                        },
                        Tag {
                            key: "env".into(),
                            value: "dev".into(),
                        },
                    ],
                },
            )
            .unwrap();
        }
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let ssm = Ssm::new(&store).unwrap();
            assert_eq!(get(&ssm, &c, "/app/key").value.as_deref(), Some("two"));
            assert_eq!(
                get(&ssm, &c, "/app/key:stable").value.as_deref(),
                Some("one")
            );
            assert_eq!(
                get(&ssm, &region, "/app/key").value.as_deref(),
                Some("region")
            );
            assert_eq!(
                get(&ssm, &account, "/app/key").value.as_deref(),
                Some("account")
            );
            let latest = ssm.db.read(|tx| latest_all(tx, &c)).unwrap();
            assert_eq!(latest.len(), 1);
            assert_eq!(latest[0].version, 2);
            let tags = ssm
                .list_tags_for_resource(
                    &c,
                    ListTagsForResourceRequest {
                        resource_type: "Parameter".into(),
                        resource_id: "/app/key".into(),
                    },
                )
                .unwrap()
                .tag_list;
            assert_eq!(tags.len(), 2);
            assert_eq!(tags[0].value, "two");
            ssm.label_parameter_version(
                &c,
                LabelParameterVersionRequest {
                    name: "/app/key".into(),
                    parameter_version: Some(2),
                    labels: vec!["stable".into()],
                },
            )
            .unwrap();
            assert_eq!(get(&ssm, &c, "/app/key:stable").version, Some(2));
            let history = ssm
                .get_parameter_history(
                    &c,
                    GetParameterHistoryRequest {
                        name: "/app/key".into(),
                        max_results: Some(1),
                        with_decryption: Some(true),
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(history.parameters[0].version, Some(1));
            assert!(history.parameters[0].labels.is_empty());
            assert!(history.next_token.is_some());
            ssm.delete_parameter(
                &c,
                DeleteParameterRequest {
                    name: "/app/key".into(),
                },
            )
            .unwrap();
            assert_eq!(
                get(&ssm, &region, "/app/key").value.as_deref(),
                Some("region")
            );
            assert!(
                ssm.db
                    .read(|tx| load_tags(tx, &c, "Parameter", "/app/key"))
                    .unwrap()
                    .is_empty()
            );
            ssm.reset().unwrap();
            assert!(
                ssm.db
                    .read(|tx| latest_all(tx, &region))
                    .unwrap()
                    .is_empty()
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn labeled_oldest_version_blocks_pruning_atomically() {
        let store = Store::ephemeral();
        let ssm = Ssm::new(&store).unwrap();
        let c = ctx();
        for n in 1..=VERSION_LIMIT {
            assert_eq!(
                put(&ssm, &c, "key", &n.to_string(), n > 1).unwrap().version,
                Some(n as i64)
            );
        }
        ssm.label_parameter_version(
            &c,
            LabelParameterVersionRequest {
                name: "key".into(),
                parameter_version: Some(1),
                labels: vec!["stable".into()],
            },
        )
        .unwrap();
        assert_eq!(
            put(&ssm, &c, "key", "blocked", true).unwrap_err().code,
            "ParameterMaxVersionLimitExceeded"
        );
        assert_eq!(get(&ssm, &c, "key").version, Some(100));
        assert_eq!(get(&ssm, &c, "key:stable").version, Some(1));
        ssm.label_parameter_version(
            &c,
            LabelParameterVersionRequest {
                name: "key".into(),
                parameter_version: Some(2),
                labels: vec!["stable".into()],
            },
        )
        .unwrap();
        assert_eq!(
            put(&ssm, &c, "key", "next", true).unwrap().version,
            Some(101)
        );
        let history = ssm.db.read(|tx| versions(tx, &c, "key")).unwrap();
        assert_eq!(history.len(), 100);
        assert_eq!(history[0].version, 2);
        assert_eq!(get(&ssm, &c, "key:stable").version, Some(2));
    }
}
