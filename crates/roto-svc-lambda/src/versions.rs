use crate::executor::Executor;
use crate::generated::*;
use crate::schema::{aliases, functions};
use crate::service::{Lambda, arn, conflict, invalid, missing};
use diesel::prelude::*;
use roto_core::{AwsError, RequestContext, ids};
use roto_protocol::FromJson;
use serde_json::{Value, json};

pub(crate) fn qualified(arn: &str) -> String {
    if arn.split(':').count() == 7 {
        format!("{arn}:$LATEST")
    } else {
        arn.into()
    }
}

fn base(ctx: &RequestContext, name: &str) -> Result<String, AwsError> {
    let arn = arn(ctx, name)?;
    if arn.split(':').count() != 7 {
        return Err(invalid("An unqualified function name is required"));
    }
    Ok(arn)
}

impl Lambda {
    pub(crate) fn resolve_arn(
        &self,
        ctx: &RequestContext,
        name: &str,
        qualifier: Option<&str>,
    ) -> Result<String, AwsError> {
        let mut resolved = arn(ctx, name)?;
        if let Some(q) = qualifier {
            let requested = arn(
                ctx,
                &format!(
                    "{}:{q}",
                    if resolved.split(':').count() > 7 {
                        resolved.rsplit_once(':').unwrap().0
                    } else {
                        &resolved
                    }
                ),
            )?;
            if resolved.split(':').count() > 7 && resolved != requested {
                return Err(invalid("Conflicting qualifiers"));
            }
            resolved = requested;
        }
        if resolved.split(':').count() > 7 {
            let suffix = resolved.rsplit_once(':').unwrap().1;
            if !suffix.bytes().all(|b| b.is_ascii_digit()) {
                let config = self
                    .db
                    .read(|tx| {
                        Ok(aliases::table
                            .find(&resolved)
                            .select(aliases::config)
                            .first::<String>(tx)
                            .optional()?)
                    })?
                    .ok_or_else(|| missing(&resolved))?;
                let value: Value =
                    serde_json::from_str(&config).map_err(|e| AwsError::internal(e.to_string()))?;
                resolved = format!(
                    "{}:{}",
                    resolved.rsplit_once(':').unwrap().0,
                    value["FunctionVersion"].as_str().unwrap()
                );
            }
        }
        Ok(resolved)
    }

    pub(crate) fn version_publish(
        &self,
        ctx: &RequestContext,
        i: PublishVersionRequest,
    ) -> Result<FunctionConfiguration, AwsError> {
        let base = base(ctx, &i.function_name)?;
        let executor: Option<Executor> = self.bound_executor(&base);
        self.db.transaction(|tx| {
            let (config, code, tags, policy) = functions::table
                .find(&base)
                .select((
                    functions::config,
                    functions::code,
                    functions::tags,
                    functions::policy,
                ))
                .first::<(String, String, String, String)>(tx)
                .optional()?
                .ok_or_else(|| missing(&base))?;
            let mut config: Value = serde_json::from_str(&config).unwrap();
            if i.revision_id
                .as_deref()
                .is_some_and(|r| Some(r) != config["RevisionId"].as_str())
                || i.code_sha256
                    .as_deref()
                    .is_some_and(|s| Some(s) != config["CodeSha256"].as_str())
            {
                return Err(AwsError::sender(
                    412,
                    "PreconditionFailedException",
                    "Function revision or code hash changed",
                ));
            }
            let rows = functions::table.select(functions::arn).load::<String>(tx)?;
            let version = rows
                .iter()
                .filter_map(|name| name.strip_prefix(&format!("{base}:"))?.parse::<u64>().ok())
                .max()
                .unwrap_or(0)
                + 1;
            let arn = format!("{base}:{version}");
            config["FunctionArn"] = json!(arn);
            config["Version"] = json!(version.to_string());
            if let Some(description) = i.description {
                config["Description"] = json!(description);
            }
            let mut code: Value = serde_json::from_str(&code).unwrap();
            if let Some(executor) = executor {
                code["__roto_executor"] = serde_json::to_value(executor).unwrap();
            }
            diesel::insert_into(functions::table)
                .values((
                    functions::arn.eq(&arn),
                    functions::config.eq(config.to_string()),
                    functions::code.eq(code.to_string()),
                    functions::tags.eq(tags),
                    functions::policy.eq(policy),
                ))
                .execute(tx)?;
            FunctionConfiguration::from_json(&config, "")
        })
    }

    pub(crate) fn alias_set(
        &self,
        ctx: &RequestContext,
        input: Value,
        update: bool,
    ) -> Result<AliasConfiguration, AwsError> {
        let base = base(ctx, input["FunctionName"].as_str().unwrap_or(""))?;
        self.load(&base)?;
        let name = input["Name"].as_str().unwrap_or("");
        if name.is_empty()
            || name.len() > 128
            || name.bytes().all(|b| b.is_ascii_digit())
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(invalid("Invalid alias name"));
        }
        if input["RoutingConfig"]["AdditionalVersionWeights"]
            .as_object()
            .is_some_and(|m| !m.is_empty())
        {
            return Err(invalid("Weighted alias routing is not supported"));
        }
        let arn = format!("{base}:{name}");
        self.db.transaction(|tx| {
            let previous = aliases::table
                .find(&arn)
                .select(aliases::config)
                .first::<String>(tx)
                .optional()?;
            if !update && previous.is_some() {
                return Err(conflict("Alias already exists"));
            }
            if update && previous.is_none() {
                return Err(missing(&arn));
            }
            let mut value = previous
                .map(|s| serde_json::from_str::<Value>(&s).unwrap())
                .unwrap_or(json!({"Name":name,"AliasArn":arn,"Description":""}));
            if input["RevisionId"]
                .as_str()
                .is_some_and(|r| Some(r) != value["RevisionId"].as_str())
            {
                return Err(AwsError::sender(
                    412,
                    "PreconditionFailedException",
                    "Alias revision changed",
                ));
            }
            for key in ["FunctionVersion", "Description", "RoutingConfig"] {
                if let Some(v) = input.get(key) {
                    value[key] = v.clone();
                }
            }
            let version = value["FunctionVersion"]
                .as_str()
                .ok_or_else(|| invalid("FunctionVersion is required"))?;
            if version == "$LATEST"
                || !version.bytes().all(|b| b.is_ascii_digit())
                || version == "0"
            {
                return Err(invalid("Alias must reference a published version"));
            }
            if functions::table
                .find(format!("{base}:{version}"))
                .count()
                .get_result::<i64>(tx)?
                == 0
            {
                return Err(missing(version));
            }
            value["RevisionId"] = json!(ids::request_id());
            diesel::insert_into(aliases::table)
                .values((aliases::arn.eq(&arn), aliases::config.eq(value.to_string())))
                .on_conflict(aliases::arn)
                .do_update()
                .set(aliases::config.eq(value.to_string()))
                .execute(tx)?;
            AliasConfiguration::from_json(&value, "")
        })
    }

    pub(crate) fn alias_get(
        &self,
        ctx: &RequestContext,
        name: &str,
        alias: &str,
    ) -> Result<AliasConfiguration, AwsError> {
        let arn = format!("{}:{alias}", base(ctx, name)?);
        self.db.read(|tx| {
            let data = aliases::table
                .find(&arn)
                .select(aliases::config)
                .first::<String>(tx)
                .optional()?
                .ok_or_else(|| missing(&arn))?;
            AliasConfiguration::from_json(&serde_json::from_str::<Value>(&data).unwrap(), "")
        })
    }

    pub(crate) fn alias_delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        alias: &str,
    ) -> Result<(), AwsError> {
        let arn = format!("{}:{alias}", base(ctx, name)?);
        self.db.transaction(|tx| {
            if diesel::delete(aliases::table.find(&arn)).execute(tx)? == 0 {
                return Err(missing(&arn));
            }
            Ok(())
        })
    }

    pub(crate) fn version_list(
        &self,
        ctx: &RequestContext,
        i: ListVersionsByFunctionRequest,
    ) -> Result<ListVersionsByFunctionResponse, AwsError> {
        let base = base(ctx, &i.function_name)?;
        self.load(&base)?;
        let values = self.db.read(|tx| {
            Ok(functions::table
                .select((functions::arn, functions::config))
                .load::<(String, String)>(tx)?)
        })?;
        let mut versions: Vec<Value> = values
            .into_iter()
            .filter(|(arn, _)| {
                arn == &base
                    || arn
                        .strip_prefix(&format!("{base}:"))
                        .is_some_and(|q| q.bytes().all(|b| b.is_ascii_digit()))
            })
            .map(|(_, s)| serde_json::from_str(&s).unwrap())
            .collect();
        versions.sort_by_key(|v| {
            v["Version"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        });
        let (start, end) = page(i.marker.as_deref(), i.max_items, versions.len())?;
        ListVersionsByFunctionResponse::from_json(
            &json!({"Versions":&versions[start..end],"NextMarker":(end<versions.len()).then(||end.to_string())}),
            "",
        )
    }

    pub(crate) fn alias_list(
        &self,
        ctx: &RequestContext,
        i: ListAliasesRequest,
    ) -> Result<ListAliasesResponse, AwsError> {
        let base = base(ctx, &i.function_name)?;
        self.load(&base)?;
        let rows = self.db.read(|tx| {
            Ok(aliases::table
                .order(aliases::arn)
                .select((aliases::arn, aliases::config))
                .load::<(String, String)>(tx)?)
        })?;
        let values: Vec<Value> = rows
            .into_iter()
            .filter(|(arn, _)| arn.starts_with(&format!("{base}:")))
            .map(|(_, s)| serde_json::from_str::<Value>(&s).unwrap())
            .filter(|v| {
                i.function_version
                    .as_deref()
                    .is_none_or(|version| Some(version) == v["FunctionVersion"].as_str())
            })
            .collect();
        let (start, end) = page(i.marker.as_deref(), i.max_items, values.len())?;
        ListAliasesResponse::from_json(
            &json!({"Aliases":&values[start..end],"NextMarker":(end<values.len()).then(||end.to_string())}),
            "",
        )
    }
}

fn page(marker: Option<&str>, max: Option<i32>, len: usize) -> Result<(usize, usize), AwsError> {
    let start = marker
        .unwrap_or("0")
        .parse::<usize>()
        .map_err(|_| invalid("Invalid Marker"))?;
    let max = max.unwrap_or(50);
    if !(1..=10000).contains(&max) || start > len {
        return Err(invalid("Invalid pagination parameters"));
    }
    Ok((start, (start + max as usize).min(len)))
}
