use std::sync::Arc;

use crate::models::{SecretRow, VersionRow};
use crate::schema::*;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use roto_core::store::{DieselDb as Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::{Blob, Timestamp};

use crate::MIGRATIONS;
use crate::generated::*;

pub struct SecretsManager {
    db: Arc<Db>,
}

impl SecretsManager {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.diesel_db("secretsmanager", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            diesel::delete(secret_versions::table).execute(tx)?;
            diesel::delete(secrets::table).execute(tx)?;
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

fn err(code: &str, message: impl Into<String>) -> AwsError {
    AwsError::sender(400, code, message)
}

fn not_found() -> AwsError {
    err(
        "ResourceNotFoundException",
        "Secrets Manager can't find the specified secret.",
    )
}

fn invalid_request(message: &str) -> AwsError {
    err("InvalidRequestException", message)
}

struct Secret {
    name: String,
    arn: String,
    description: Option<String>,
    kms_key_id: Option<String>,
    created_at: i64,
    changed_at: i64,
    accessed_at: Option<i64>,
    deleted_at: Option<i64>,
    tags: Vec<Tag>,
    policy: Option<String>,
    rotation_enabled: bool,
    rotation_lambda_arn: Option<String>,
    rotation_rules: Option<RotationRulesType>,
    last_rotated_at: Option<i64>,
}

impl From<SecretRow> for Secret {
    fn from(r: SecretRow) -> Self {
        use roto_protocol::FromJson;
        let tags = serde_json::from_str::<serde_json::Value>(&r.tags)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .map(|a| {
                a.iter()
                    .filter_map(|x| Tag::from_json(x, "").ok())
                    .collect()
            })
            .unwrap_or_default();
        Self {
            name: r.name,
            arn: r.arn,
            description: r.description,
            kms_key_id: r.kms_key_id,
            created_at: r.created_at,
            changed_at: r.changed_at,
            accessed_at: r.accessed_at,
            deleted_at: r.deleted_at,
            policy: r.policy,
            rotation_lambda_arn: r.rotation_lambda_arn,
            last_rotated_at: r.last_rotated_at,
            tags,
            rotation_enabled: r.rotation_enabled != 0,
            rotation_rules: r
                .rotation_rules
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| RotationRulesType::from_json(&v, "").ok()),
        }
    }
}

fn tags_json(tags: &[Tag]) -> String {
    use roto_protocol::ToJson;
    serde_json::Value::Array(tags.iter().map(ToJson::to_json).collect()).to_string()
}

/// Looks a secret up by name, full ARN, or ARN without the random suffix.
fn find(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    id: &str,
) -> Result<Option<Secret>, AwsError> {
    let (account, region, name_part) = match id.strip_prefix("arn:") {
        Some(rest) => {
            let parts: Vec<&str> = rest.splitn(6, ':').collect();
            if parts.len() == 6 && parts[4] == "secret" {
                (
                    parts[3].to_string(),
                    parts[2].to_string(),
                    Some(parts[5].to_string()),
                )
            } else {
                return Ok(None);
            }
        }
        None => (ctx.account_id.clone(), ctx.region.clone(), None),
    };
    let query = secrets::table
        .filter(secrets::account_id.eq(&account))
        .filter(secrets::region.eq(&region));
    match name_part {
        None => Ok(query
            .filter(secrets::name.eq(id))
            .select(SecretRow::as_select())
            .first(tx)
            .optional()?
            .map(Into::into)),
        Some(n) => {
            let exact = query
                .filter(secrets::arn.eq(id))
                .select(SecretRow::as_select())
                .first(tx)
                .optional()?;
            if exact.is_some() {
                return Ok(exact.map(Into::into));
            }
            Ok(query
                .filter(secrets::name.eq(n))
                .select(SecretRow::as_select())
                .first(tx)
                .optional()?
                .map(Into::into))
        }
    }
}

fn require(tx: &mut SqliteConnection, ctx: &RequestContext, id: &str) -> Result<Secret, AwsError> {
    find(tx, ctx, id)?.ok_or_else(not_found)
}

fn require_live(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    id: &str,
    _op: &str,
) -> Result<Secret, AwsError> {
    let s = require(tx, ctx, id)?;
    if s.deleted_at.is_some() {
        return Err(invalid_request(
            "You can't perform this operation on the secret because it was marked for deletion.",
        ));
    }
    Ok(s)
}

#[derive(Clone)]
struct Version {
    version_id: String,
    secret_string: Option<String>,
    secret_binary: Option<Vec<u8>>,
    stages: Vec<String>,
    created_at: i64,
}

fn versions(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
) -> Result<Vec<Version>, AwsError> {
    Ok(secret_versions::table
        .filter(secret_versions::account_id.eq(&ctx.account_id))
        .filter(secret_versions::region.eq(&ctx.region))
        .filter(secret_versions::secret_name.eq(name))
        .order(secret_versions::seq)
        .select(VersionRow::as_select())
        .load(tx)?
        .into_iter()
        .map(|r| Version {
            version_id: r.version_id,
            secret_string: r.secret_string,
            secret_binary: r.secret_binary,
            stages: serde_json::from_str(&r.stages).unwrap_or_default(),
            created_at: r.created_at,
        })
        .collect())
}

fn save_stages(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
    version_id: &str,
    stages: &[String],
) -> Result<(), AwsError> {
    diesel::update(
        secret_versions::table
            .filter(secret_versions::account_id.eq(&ctx.account_id))
            .filter(secret_versions::region.eq(&ctx.region))
            .filter(secret_versions::secret_name.eq(&name))
            .filter(secret_versions::version_id.eq(&version_id)),
    )
    .set(secret_versions::stages.eq(&serde_json::to_string(stages).unwrap_or_default()))
    .execute(tx)?;
    Ok(())
}

/// Moves `stage` to `target` (removing it from every other version).
fn move_stage(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
    stage: &str,
    target: &str,
) -> Result<(), AwsError> {
    for v in versions(tx, ctx, name)? {
        let mut stages = v.stages.clone();
        let has = stages.iter().any(|s| s == stage);
        if v.version_id == target && !has {
            stages.push(stage.to_string());
        } else if v.version_id != target && has {
            stages.retain(|s| s != stage);
        } else {
            continue;
        }
        save_stages(tx, ctx, name, &v.version_id, &stages)?;
    }
    Ok(())
}

fn new_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn insert_version(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    name: &str,
    version_id: &str,
    string: &Option<String>,
    binary: &Option<Blob>,
    stages: &[String],
) -> Result<(), AwsError> {
    diesel::insert_into(secret_versions::table)
        .values((
            secret_versions::account_id.eq(&ctx.account_id),
            secret_versions::region.eq(&ctx.region),
            secret_versions::secret_name.eq(&name),
            secret_versions::version_id.eq(&version_id),
            secret_versions::secret_string.eq(string),
            secret_versions::secret_binary.eq(&binary.as_ref().map(|b| b.0.clone())),
            secret_versions::stages.eq(&serde_json::to_string(stages).unwrap_or_default()),
            secret_versions::created_at.eq(&now()),
        ))
        .execute(tx)?;
    // The new current version demotes the old one; a label can sit on one version only.
    for stage in stages {
        if stage == "AWSCURRENT" {
            let prev: Option<String> = versions(tx, ctx, name)?
                .into_iter()
                .find(|v| v.version_id != version_id && v.stages.iter().any(|s| s == "AWSCURRENT"))
                .map(|v| v.version_id);
            move_stage(tx, ctx, name, "AWSCURRENT", version_id)?;
            if let Some(p) = prev {
                move_stage(tx, ctx, name, "AWSPREVIOUS", &p)?;
            }
        } else {
            move_stage(tx, ctx, name, stage, version_id)?;
        }
    }
    diesel::update(
        secrets::table
            .filter(secrets::account_id.eq(&ctx.account_id))
            .filter(secrets::region.eq(&ctx.region))
            .filter(secrets::name.eq(&name)),
    )
    .set(secrets::changed_at.eq(&now()))
    .execute(tx)?;
    Ok(())
}

fn random_suffix() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    uuid::Uuid::new_v4()
        .as_bytes()
        .iter()
        .take(6)
        .map(|b| CHARS[*b as usize % CHARS.len()] as char)
        .collect()
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

fn page<T>(
    items: Vec<T>,
    token: &Option<String>,
    max: usize,
) -> Result<(Vec<T>, Option<String>), AwsError> {
    let start = match token.as_deref() {
        None | Some("") => 0,
        Some(t) => t.parse::<usize>().map_err(|_| {
            err(
                "InvalidNextTokenException",
                "You provided an invalid value for parameter NextToken.",
            )
        })?,
    };
    let total = items.len();
    let out: Vec<T> = items.into_iter().skip(start).take(max).collect();
    let next = (start + out.len() < total).then(|| (start + out.len()).to_string());
    Ok((out, next))
}

fn stages_by_version(vs: &[Version]) -> std::collections::BTreeMap<String, Vec<String>> {
    vs.iter()
        .filter(|v| !v.stages.is_empty())
        .map(|v| (v.version_id.clone(), v.stages.clone()))
        .collect()
}

fn describe_fields(s: &Secret, vs: &[Version]) -> DescribeSecretResponse {
    DescribeSecretResponse {
        arn: Some(s.arn.clone()),
        name: Some(s.name.clone()),
        description: s.description.clone(),
        kms_key_id: s.kms_key_id.clone(),
        created_date: Some(Timestamp(s.created_at)),
        last_changed_date: Some(Timestamp(s.changed_at)),
        last_accessed_date: s.accessed_at.map(Timestamp),
        deleted_date: s.deleted_at.map(Timestamp),
        tags: s.tags.clone(),
        rotation_enabled: s.rotation_enabled.then_some(true),
        rotation_lambda_arn: s.rotation_lambda_arn.clone(),
        rotation_rules: s.rotation_rules.clone(),
        last_rotated_date: s.last_rotated_at.map(Timestamp),
        version_ids_to_stages: stages_by_version(vs),
        ..Default::default()
    }
}

fn value_response(s: &Secret, v: &Version) -> GetSecretValueResponse {
    GetSecretValueResponse {
        arn: Some(s.arn.clone()),
        name: Some(s.name.clone()),
        version_id: Some(v.version_id.clone()),
        version_stages: v.stages.clone(),
        created_date: Some(Timestamp(v.created_at)),
        secret_string: v.secret_string.clone(),
        secret_binary: v.secret_binary.clone().map(Blob),
    }
}

fn validate_tags(tags: &[Tag]) -> Result<(), AwsError> {
    let mut seen = std::collections::BTreeSet::new();
    for t in tags {
        if !seen.insert(t.key.as_deref().unwrap_or_default()) {
            return Err(err(
                "InvalidParameterException",
                "Duplicate tag keys found. Please note that Tag keys are case insensitive.",
            ));
        }
    }
    Ok(())
}

fn matches_filter(s: &Secret, key: &str, values: &[String]) -> bool {
    let (negate, vals): (bool, Vec<&str>) =
        if values.iter().all(|v| v.starts_with('!')) && !values.is_empty() {
            (true, values.iter().map(|v| &v[1..]).collect())
        } else {
            (false, values.iter().map(String::as_str).collect())
        };
    let words = |text: &str, v: &str| {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .any(|w| w.starts_with(&v.to_lowercase()))
            || text.to_lowercase().starts_with(&v.to_lowercase())
    };
    let hit = |candidates: Vec<String>| candidates.iter().any(|c| vals.iter().any(|v| words(c, v)));
    let m = match key {
        "name" => hit(vec![s.name.clone()]),
        "description" => hit(s.description.clone().into_iter().collect()),
        "tag-key" => hit(s.tags.iter().filter_map(|t| t.key.clone()).collect()),
        "tag-value" => hit(s.tags.iter().filter_map(|t| t.value.clone()).collect()),
        "all" => {
            hit(vec![s.name.clone()])
                || hit(s.description.clone().into_iter().collect())
                || hit(s.tags.iter().filter_map(|t| t.key.clone()).collect())
                || hit(s.tags.iter().filter_map(|t| t.value.clone()).collect())
        }
        _ => true,
    };
    m != negate
}

fn random_password(i: &GetRandomPasswordRequest) -> Result<String, AwsError> {
    let length = i.password_length.unwrap_or(32);
    if !(1..=4096).contains(&length) {
        return Err(err(
            "InvalidParameterException",
            "Password length must be between 1 and 4096.",
        ));
    }
    let exclude = i.exclude_characters.clone().unwrap_or_default();
    let ok = |c: &char| !exclude.contains(*c);
    let mut classes: Vec<Vec<char>> = Vec::new();
    if !i.exclude_lowercase.unwrap_or(false) {
        classes.push(('a'..='z').filter(ok).collect());
    }
    if !i.exclude_uppercase.unwrap_or(false) {
        classes.push(('A'..='Z').filter(ok).collect());
    }
    if !i.exclude_numbers.unwrap_or(false) {
        classes.push(('0'..='9').filter(ok).collect());
    }
    if !i.exclude_punctuation.unwrap_or(false) {
        classes.push(
            "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
                .chars()
                .filter(ok)
                .collect(),
        );
    }
    if i.include_space.unwrap_or(false) {
        classes.push(vec![' ']);
    }
    classes.retain(|c| !c.is_empty());
    let pool: Vec<char> = classes.iter().flatten().copied().collect();
    if pool.is_empty() {
        return Err(err(
            "InvalidParameterException",
            "No characters available to generate a password.",
        ));
    }
    let bytes: Vec<u8> = (0..(length as usize + classes.len()).div_ceil(16))
        .flat_map(|_| *uuid::Uuid::new_v4().as_bytes())
        .collect();
    let mut out: Vec<char> = bytes
        .iter()
        .take(length as usize)
        .map(|b| pool[*b as usize % pool.len()])
        .collect();
    if i.require_each_included_type.unwrap_or(true) && (length as usize) >= classes.len() {
        for (n, class) in classes.iter().enumerate() {
            out[n] = class[bytes[length as usize + n] as usize % class.len()];
        }
        // Spread the required characters through the password.
        let len = out.len();
        for n in 0..classes.len() {
            out.swap(n, (bytes[(n * 7) % bytes.len()] as usize) % len);
        }
    }
    Ok(out.into_iter().collect())
}

impl Service for SecretsManager {
    fn create_secret(
        &self,
        ctx: &RequestContext,
        i: CreateSecretRequest,
    ) -> Result<CreateSecretResponse, AwsError> {
        validate_tags(&i.tags)?;
        if i.name.is_empty() || i.name.len() > 512 {
            return Err(err(
                "ValidationException",
                "Invalid name. Must be a valid name containing alphanumeric characters, or any of the following: -/_+=.@!",
            ));
        }
        self.db.transaction(|tx| {
            if let Some(existing) = find(tx, ctx, &i.name)? {
                // Same request token and value: idempotent.
                let vs = versions(tx, ctx, &existing.name)?;
                if let Some(token) = &i.client_request_token {
                    if let Some(v) = vs.iter().find(|v| &v.version_id == token) {
                        if v.secret_string == i.secret_string
                            && v.secret_binary == i.secret_binary.as_ref().map(|b| b.0.clone())
                        {
                            return Ok(CreateSecretResponse {
                                arn: Some(existing.arn),
                                name: Some(existing.name),
                                version_id: Some(v.version_id.clone()),
                                ..Default::default()
                            });
                        }
                    }
                }
                return Err(err(
                    "ResourceExistsException",
                    "A resource with the ID you requested already exists.",
                ));
            }
            let arn = format!(
                "arn:{}:secretsmanager:{}:{}:secret:{}-{}",
                partition(&ctx.region),
                ctx.region,
                ctx.account_id,
                i.name,
                random_suffix()
            );
            diesel::insert_into(secrets::table)
                .values((
                    secrets::account_id.eq(&ctx.account_id),
                    secrets::region.eq(&ctx.region),
                    secrets::name.eq(&i.name),
                    secrets::arn.eq(&arn),
                    secrets::description.eq(&i.description),
                    secrets::kms_key_id.eq(&i.kms_key_id),
                    secrets::created_at.eq(&now()),
                    secrets::changed_at.eq(&now()),
                    secrets::tags.eq(&tags_json(&i.tags)),
                ))
                .execute(tx)?;
            let has_value = i.secret_string.is_some() || i.secret_binary.is_some();
            let version_id = i.client_request_token.clone().unwrap_or_else(new_token);
            if has_value {
                insert_version(
                    tx,
                    ctx,
                    &i.name,
                    &version_id,
                    &i.secret_string,
                    &i.secret_binary,
                    &["AWSCURRENT".to_string()],
                )?;
            }
            Ok(CreateSecretResponse {
                arn: Some(arn),
                name: Some(i.name.clone()),
                version_id: has_value.then_some(version_id),
                ..Default::default()
            })
        })
    }

    fn get_secret_value(
        &self,
        ctx: &RequestContext,
        i: GetSecretValueRequest,
    ) -> Result<GetSecretValueResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            let vs = versions(tx, ctx, &s.name)?;
            if let (Some(id), Some(stage)) = (&i.version_id, &i.version_stage) {
                if vs.iter().any(|v| &v.version_id == id && !v.stages.contains(stage)) {
                    return Err(invalid_request("You provided a VersionStage that is not associated to the provided VersionId."));
                }
            }
            if s.deleted_at.is_some() {
                return Err(invalid_request("You can't perform this operation on the secret because it was marked for deletion."));
            }
            let stage = i.version_stage.clone().unwrap_or_else(|| "AWSCURRENT".into());
            let found = match &i.version_id {
                Some(id) => vs.iter().find(|v| &v.version_id == id),
                None => vs.iter().find(|v| v.stages.contains(&stage)),
            };
            let Some(v) = found else {
                return Err(err(
                    "ResourceNotFoundException",
                    match &i.version_id {
                        Some(id) => format!("Secrets Manager can't find the specified secret value for VersionId: {id}"),
                        None => format!("Secrets Manager can't find the specified secret value for staging label: {stage}"),
                    },
                ));
            };
            diesel::update(secrets::table.filter(secrets::account_id.eq(&ctx.account_id)).filter(secrets::region.eq(&ctx.region)).filter(secrets::name.eq(&s.name))).set(secrets::accessed_at.eq(&now())).execute(tx)?;
            Ok(value_response(&s, v))
        })
    }

    fn put_secret_value(
        &self,
        ctx: &RequestContext,
        i: PutSecretValueRequest,
    ) -> Result<PutSecretValueResponse, AwsError> {
        if i.secret_string.is_none() && i.secret_binary.is_none() {
            return Err(err(
                "InvalidRequestException",
                "You must provide either SecretString or SecretBinary.",
            ));
        }
        if i.secret_string.is_some() && i.secret_binary.is_some() {
            return Err(err(
                "InvalidRequestException",
                "You can't specify both SecretString and SecretBinary.",
            ));
        }
        self.db.transaction(|tx| {
            let s = require_live(tx, ctx, &i.secret_id, "PutSecretValue")?;
            let version_id = i.client_request_token.clone().unwrap_or_else(new_token);
            let stages = if i.version_stages.is_empty() {
                vec!["AWSCURRENT".to_string()]
            } else {
                i.version_stages.clone()
            };
            let existing = versions(tx, ctx, &s.name)?;
            if let Some(v) = existing.iter().find(|v| v.version_id == version_id) {
                if v.secret_string == i.secret_string
                    && v.secret_binary == i.secret_binary.as_ref().map(|b| b.0.clone())
                {
                    return Ok(PutSecretValueResponse {
                        arn: Some(s.arn),
                        name: Some(s.name),
                        version_id: Some(version_id),
                        version_stages: v.stages.clone(),
                    });
                }
                return Err(err(
                    "ResourceExistsException",
                    "You can't modify an existing secret version with a different value.",
                ));
            }
            insert_version(
                tx,
                ctx,
                &s.name,
                &version_id,
                &i.secret_string,
                &i.secret_binary,
                &stages,
            )?;
            let stages = versions(tx, ctx, &s.name)?
                .into_iter()
                .find(|v| v.version_id == version_id)
                .map(|v| v.stages)
                .unwrap_or(stages);
            Ok(PutSecretValueResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                version_id: Some(version_id),
                version_stages: stages,
            })
        })
    }

    fn update_secret(
        &self,
        ctx: &RequestContext,
        i: UpdateSecretRequest,
    ) -> Result<UpdateSecretResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require_live(tx, ctx, &i.secret_id, "UpdateSecret")?;
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set((
                secrets::description.eq(&i.description.clone().or_else(|| s.description.clone())),
                secrets::kms_key_id.eq(&i.kms_key_id.clone().or_else(|| s.kms_key_id.clone())),
                secrets::changed_at.eq(&now()),
            ))
            .execute(tx)?;
            let has_value = i.secret_string.is_some() || i.secret_binary.is_some();
            let version_id = i.client_request_token.clone().unwrap_or_else(new_token);
            if has_value {
                insert_version(
                    tx,
                    ctx,
                    &s.name,
                    &version_id,
                    &i.secret_string,
                    &i.secret_binary,
                    &["AWSCURRENT".to_string()],
                )?;
            }
            Ok(UpdateSecretResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                version_id: has_value.then_some(version_id),
            })
        })
    }

    fn describe_secret(
        &self,
        ctx: &RequestContext,
        i: DescribeSecretRequest,
    ) -> Result<DescribeSecretResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            let vs = versions(tx, ctx, &s.name)?;
            Ok(describe_fields(&s, &vs))
        })
    }

    fn list_secrets(
        &self,
        ctx: &RequestContext,
        i: ListSecretsRequest,
    ) -> Result<ListSecretsResponse, AwsError> {
        let max = i.max_results.unwrap_or(100);
        if !(1..=100).contains(&max) {
            return Err(err(
                "InvalidParameterException",
                "MaxResults must be between 1 and 100.",
            ));
        }
        for f in &i.filters {
            match f.key.as_deref() {
                Some(
                    "name" | "description" | "tag-key" | "tag-value" | "primary-region"
                    | "owning-service" | "all",
                ) => {}
                other => {
                    return Err(err(
                        "ValidationException",
                        format!(
                            "1 validation error detected: Value '{}' at 'filters.1.member.key' failed to satisfy constraint: Member must satisfy enum value set: [description, name, tag-key, tag-value, primary-region, owning-service, all]",
                            other.unwrap_or("")
                        ),
                    ));
                }
            }
        }
        self.db.transaction(|tx| {
            let mut all: Vec<Secret> = secrets::table
                .filter(secrets::account_id.eq(&ctx.account_id))
                .filter(secrets::region.eq(&ctx.region))
                .order((secrets::created_at, secrets::name))
                .select(SecretRow::as_select())
                .load(tx)?
                .into_iter()
                .map(Into::into)
                .collect();
            all.retain(|s| i.include_planned_deletion.unwrap_or(false) || s.deleted_at.is_none());
            all.retain(|s| {
                i.filters
                    .iter()
                    .all(|f| matches_filter(s, f.key.as_deref().unwrap_or(""), &f.values))
            });
            match i.sort_by.as_deref() {
                Some("name") => all.sort_by(|a, b| a.name.cmp(&b.name)),
                Some("created-date") => all.sort_by_key(|s| s.created_at),
                Some("last-changed-date") => all.sort_by_key(|s| s.changed_at),
                Some("last-accessed-date") => all.sort_by_key(|s| s.accessed_at),
                _ => {}
            }
            if i.sort_order.as_deref() == Some("desc") {
                all.reverse();
            }
            let (items, next_token) = page(all, &i.next_token, max as usize)?;
            let mut out = Vec::new();
            for s in &items {
                let vs = versions(tx, ctx, &s.name)?;
                let d = describe_fields(s, &vs);
                out.push(SecretListEntry {
                    arn: d.arn,
                    name: d.name,
                    description: d.description,
                    kms_key_id: d.kms_key_id,
                    created_date: d.created_date,
                    last_changed_date: d.last_changed_date,
                    last_accessed_date: d.last_accessed_date,
                    deleted_date: d.deleted_date,
                    tags: d.tags,
                    rotation_enabled: d.rotation_enabled,
                    rotation_lambda_arn: d.rotation_lambda_arn,
                    rotation_rules: d.rotation_rules,
                    last_rotated_date: d.last_rotated_date,
                    secret_versions_to_stages: d.version_ids_to_stages,
                    ..Default::default()
                });
            }
            Ok(ListSecretsResponse {
                secret_list: out,
                next_token,
            })
        })
    }

    fn delete_secret(
        &self,
        ctx: &RequestContext,
        i: DeleteSecretRequest,
    ) -> Result<DeleteSecretResponse, AwsError> {
        if let Some(d) = i.recovery_window_in_days {
            if !(7..=30).contains(&d) {
                return Err(err(
                    "InvalidParameterException",
                    "The RecoveryWindowInDays value must be between 7 and 30 days (inclusive).",
                ));
            }
            if i.force_delete_without_recovery == Some(true) {
                return Err(err(
                    "InvalidParameterException",
                    "You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.",
                ));
            }
        }
        let force = i.force_delete_without_recovery.unwrap_or(false);
        self.db.transaction(|tx| {
            let Some(s) = find(tx, ctx, &i.secret_id)? else {
                if force {
                    let arn = format!("arn:{}:secretsmanager:{}:{}:secret:{}", partition(&ctx.region), ctx.region, ctx.account_id, i.secret_id);
                    return Ok(DeleteSecretResponse { arn: Some(arn), name: Some(i.secret_id.clone()), deletion_date: Some(Timestamp(now())) });
                }
                return Err(not_found());
            };
            if s.deleted_at.is_some() && !force {
                return Err(invalid_request("You tried to perform the operation on a secret that's currently marked deleted."));
            }
            let when = now() + i.recovery_window_in_days.unwrap_or(30) * 86_400;
            if force {
                diesel::delete(secret_versions::table.filter(secret_versions::account_id.eq(&ctx.account_id)).filter(secret_versions::region.eq(&ctx.region)).filter(secret_versions::secret_name.eq(&s.name))).execute(tx)?;
                diesel::delete(secrets::table.filter(secrets::account_id.eq(&ctx.account_id)).filter(secrets::region.eq(&ctx.region)).filter(secrets::name.eq(&s.name))).execute(tx)?;
                return Ok(DeleteSecretResponse { arn: Some(s.arn), name: Some(s.name), deletion_date: Some(Timestamp(now())) });
            }
            diesel::update(secrets::table.filter(secrets::account_id.eq(&ctx.account_id)).filter(secrets::region.eq(&ctx.region)).filter(secrets::name.eq(&s.name))).set(secrets::deleted_at.eq(&when)).execute(tx)?;
            Ok(DeleteSecretResponse { arn: Some(s.arn), name: Some(s.name), deletion_date: Some(Timestamp(when)) })
        })
    }

    fn restore_secret(
        &self,
        ctx: &RequestContext,
        i: RestoreSecretRequest,
    ) -> Result<RestoreSecretResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::deleted_at.eq(None::<i64>))
            .execute(tx)?;
            Ok(RestoreSecretResponse {
                arn: Some(s.arn),
                name: Some(s.name),
            })
        })
    }

    fn list_secret_version_ids(
        &self,
        ctx: &RequestContext,
        i: ListSecretVersionIdsRequest,
    ) -> Result<ListSecretVersionIdsResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            let mut vs = versions(tx, ctx, &s.name)?;
            if !i.include_deprecated.unwrap_or(false) {
                vs.retain(|v| !v.stages.is_empty());
            }
            let (items, next_token) = page(
                vs,
                &i.next_token,
                i.max_results.unwrap_or(100).clamp(1, 100) as usize,
            )?;
            Ok(ListSecretVersionIdsResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                next_token,
                versions: items
                    .iter()
                    .map(|v| SecretVersionsListEntry {
                        version_id: Some(v.version_id.clone()),
                        version_stages: v.stages.clone(),
                        created_date: Some(Timestamp(v.created_at)),
                        last_accessed_date: None,
                        kms_key_ids: Vec::new(),
                    })
                    .collect(),
            })
        })
    }

    fn update_secret_version_stage(
        &self,
        ctx: &RequestContext,
        i: UpdateSecretVersionStageRequest,
    ) -> Result<UpdateSecretVersionStageResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require_live(tx, ctx, &i.secret_id, "UpdateSecretVersionStage")?;
            let vs = versions(tx, ctx, &s.name)?;
            let has = |id: &str| vs.iter().any(|v| v.version_id == id);
            if let Some(from) = &i.remove_from_version_id {
                let Some(v) = vs.iter().find(|v| &v.version_id == from) else {
                    return Err(err(
                        "ResourceNotFoundException",
                        format!("Secrets Manager can't find the specified secret version: {from}"),
                    ));
                };
                if !v.stages.contains(&i.version_stage) {
                    return Err(invalid_request(&format!(
                        "The staging label {} is not attached to version {from}",
                        i.version_stage
                    )));
                }
            }
            if let Some(to) = &i.move_to_version_id {
                if !has(to) {
                    return Err(err(
                        "ResourceNotFoundException",
                        format!("Secrets Manager can't find the specified secret version: {to}"),
                    ));
                }
            }
            if i.version_stage == "AWSCURRENT" {
                if let Some(to) = &i.move_to_version_id {
                    let prev = vs
                        .iter()
                        .find(|v| v.stages.iter().any(|s| s == "AWSCURRENT"))
                        .map(|v| v.version_id.clone());
                    move_stage(tx, ctx, &s.name, "AWSCURRENT", to)?;
                    if let Some(p) = prev.filter(|p| p != to) {
                        move_stage(tx, ctx, &s.name, "AWSPREVIOUS", &p)?;
                    }
                }
            } else if let Some(to) = &i.move_to_version_id {
                move_stage(tx, ctx, &s.name, &i.version_stage, to)?;
            } else if let Some(from) = &i.remove_from_version_id {
                if let Some(v) = vs.iter().find(|v| &v.version_id == from) {
                    let mut stages = v.stages.clone();
                    stages.retain(|x| x != &i.version_stage);
                    save_stages(tx, ctx, &s.name, from, &stages)?;
                }
            }
            Ok(UpdateSecretVersionStageResponse {
                arn: Some(s.arn),
                name: Some(s.name),
            })
        })
    }

    fn tag_resource(&self, ctx: &RequestContext, i: TagResourceRequest) -> Result<(), AwsError> {
        validate_tags(&i.tags)?;
        self.db.transaction(|tx| {
            let mut s = require(tx, ctx, &i.secret_id)?;
            for t in &i.tags {
                match s.tags.iter_mut().find(|x| x.key == t.key) {
                    Some(x) => x.value = t.value.clone(),
                    None => s.tags.push(t.clone()),
                }
            }
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::tags.eq(&tags_json(&s.tags)))
            .execute(tx)?;
            Ok(())
        })
    }

    fn untag_resource(
        &self,
        ctx: &RequestContext,
        i: UntagResourceRequest,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut s = require(tx, ctx, &i.secret_id)?;
            s.tags
                .retain(|t| !t.key.as_ref().is_some_and(|k| i.tag_keys.contains(k)));
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::tags.eq(&tags_json(&s.tags)))
            .execute(tx)?;
            Ok(())
        })
    }

    fn get_random_password(
        &self,
        _ctx: &RequestContext,
        i: GetRandomPasswordRequest,
    ) -> Result<GetRandomPasswordResponse, AwsError> {
        Ok(GetRandomPasswordResponse {
            random_password: Some(random_password(&i)?),
        })
    }

    fn put_resource_policy(
        &self,
        ctx: &RequestContext,
        i: PutResourcePolicyRequest,
    ) -> Result<PutResourcePolicyResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            if serde_json::from_str::<serde_json::Value>(&i.resource_policy).is_err() {
                return Err(err(
                    "MalformedPolicyDocumentException",
                    "This resource policy contains invalid JSON text.",
                ));
            }
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::policy.eq(&i.resource_policy))
            .execute(tx)?;
            Ok(PutResourcePolicyResponse {
                arn: Some(s.arn),
                name: Some(s.name),
            })
        })
    }

    fn get_resource_policy(
        &self,
        ctx: &RequestContext,
        i: GetResourcePolicyRequest,
    ) -> Result<GetResourcePolicyResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            Ok(GetResourcePolicyResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                resource_policy: s.policy,
            })
        })
    }

    fn delete_resource_policy(
        &self,
        ctx: &RequestContext,
        i: DeleteResourcePolicyRequest,
    ) -> Result<DeleteResourcePolicyResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require(tx, ctx, &i.secret_id)?;
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::policy.eq(None::<String>))
            .execute(tx)?;
            Ok(DeleteResourcePolicyResponse {
                arn: Some(s.arn),
                name: Some(s.name),
            })
        })
    }

    fn validate_resource_policy(
        &self,
        _ctx: &RequestContext,
        i: ValidateResourcePolicyRequest,
    ) -> Result<ValidateResourcePolicyResponse, AwsError> {
        if serde_json::from_str::<serde_json::Value>(&i.resource_policy).is_err() {
            return Err(err(
                "MalformedPolicyDocumentException",
                "This resource policy contains invalid JSON text.",
            ));
        }
        Ok(ValidateResourcePolicyResponse {
            policy_validation_passed: Some(true),
            validation_errors: Vec::new(),
        })
    }

    fn rotate_secret(
        &self,
        ctx: &RequestContext,
        i: RotateSecretRequest,
    ) -> Result<RotateSecretResponse, AwsError> {
        use roto_protocol::ToJson;
        self.db.transaction(|tx| {
            let s = require_live(tx, ctx, &i.secret_id, "RotateSecret")?;
            let lambda = i
                .rotation_lambda_arn
                .clone()
                .or(s.rotation_lambda_arn.clone());
            let rules = i.rotation_rules.clone().or(s.rotation_rules.clone());
            if let Some(r) = &rules {
                if let Some(d) = r.automatically_after_days {
                    if !(1..=1000).contains(&d) {
                        return Err(err(
                            "InvalidParameterException",
                            "RotationRules.AutomaticallyAfterDays must be within 1-1000.",
                        ));
                    }
                }
            }
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set((
                secrets::rotation_enabled.eq(1_i64),
                secrets::rotation_lambda_arn.eq(&lambda),
                secrets::rotation_rules.eq(&rules.as_ref().map(|r| r.to_json().to_string())),
                secrets::last_rotated_at.eq(&now()),
            ))
            .execute(tx)?;
            // Without a rotation function to run, rotation promotes a fresh copy of the current value.
            let vs = versions(tx, ctx, &s.name)?;
            let version_id = i.client_request_token.clone().unwrap_or_else(new_token);
            if let Some(cur) = vs
                .iter()
                .find(|v| v.stages.iter().any(|x| x == "AWSCURRENT"))
            {
                let secret_binary = cur.secret_binary.clone().map(Blob);
                insert_version(
                    tx,
                    ctx,
                    &s.name,
                    &version_id,
                    &cur.secret_string,
                    &secret_binary,
                    &["AWSCURRENT".to_string()],
                )?;
            }
            Ok(RotateSecretResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                version_id: Some(version_id),
            })
        })
    }

    fn cancel_rotate_secret(
        &self,
        ctx: &RequestContext,
        i: CancelRotateSecretRequest,
    ) -> Result<CancelRotateSecretResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = require_live(tx, ctx, &i.secret_id, "CancelRotateSecret")?;
            if !s.rotation_enabled {
                return Err(invalid_request(
                    "You tried to cancel the rotation of a secret that is not rotating.",
                ));
            }
            diesel::update(
                secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::name.eq(&s.name)),
            )
            .set(secrets::rotation_enabled.eq(0_i64))
            .execute(tx)?;
            Ok(CancelRotateSecretResponse {
                arn: Some(s.arn),
                name: Some(s.name),
                version_id: None,
            })
        })
    }

    fn batch_get_secret_value(
        &self,
        ctx: &RequestContext,
        i: BatchGetSecretValueRequest,
    ) -> Result<BatchGetSecretValueResponse, AwsError> {
        if !i.secret_id_list.is_empty() && !i.filters.is_empty() {
            return Err(err(
                "InvalidParameterException",
                "Either 'SecretIdList' or 'Filters' must be provided, but not both.",
            ));
        }
        if i.max_results.is_some() && i.filters.is_empty() {
            return Err(err(
                "InvalidParameterException",
                "'Filters' not specified. 'Filters' must also be specified when 'MaxResults' is provided.",
            ));
        }
        self.db.transaction(|tx| {
            let mut entries = Vec::new();
            let mut errors = Vec::new();
            let mut collect = |tx: &mut SqliteConnection, s: &Secret| -> Result<(), AwsError> {
                let vs = versions(tx, ctx, &s.name)?;
                if let Some(v) = vs
                    .iter()
                    .find(|v| v.stages.iter().any(|x| x == "AWSCURRENT"))
                {
                    let r = value_response(s, v);
                    entries.push(SecretValueEntry {
                        arn: r.arn,
                        name: r.name,
                        version_id: r.version_id,
                        version_stages: r.version_stages,
                        created_date: r.created_date,
                        secret_string: r.secret_string,
                        secret_binary: r.secret_binary,
                    });
                }
                Ok(())
            };
            for id in &i.secret_id_list {
                match find(tx, ctx, id)? {
                    Some(s) if s.deleted_at.is_none() => collect(tx, &s)?,
                    _ => errors.push(APIErrorType {
                        secret_id: Some(id.clone()),
                        error_code: Some("ResourceNotFoundException".into()),
                        message: Some("Secrets Manager can't find the specified secret.".into()),
                    }),
                }
            }
            if !i.filters.is_empty() {
                let all: Vec<Secret> = secrets::table
                    .filter(secrets::account_id.eq(&ctx.account_id))
                    .filter(secrets::region.eq(&ctx.region))
                    .filter(secrets::deleted_at.is_null())
                    .order(secrets::name)
                    .select(SecretRow::as_select())
                    .load(tx)?
                    .into_iter()
                    .map(Into::into)
                    .collect();
                for s in all.iter().filter(|s| {
                    i.filters
                        .iter()
                        .all(|f| matches_filter(s, f.key.as_deref().unwrap_or(""), &f.values))
                }) {
                    collect(tx, s)?;
                }
            }
            let (page_items, next_token) = page(
                entries,
                &i.next_token,
                i.max_results.unwrap_or(20).clamp(1, 20) as usize,
            )?;
            Ok(BatchGetSecretValueResponse {
                secret_values: page_items,
                errors,
                next_token,
            })
        })
    }
}
