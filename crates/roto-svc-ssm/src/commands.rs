//! SSM Run Command, record only. `SendCommand` stores the command and one invocation per target
//! instance; nothing runs. Invocations stay `Pending` until a fixture settles them with the
//! Roto-only `RotoCompleteCommandInvocation` operation, which chooses the status, output and
//! exit code that `GetCommandInvocation` and `ListCommandInvocations` then report.
//!
//! Targets are instance IDs only (`InstanceIds`, or a `Targets` entry keyed `InstanceIds`):
//! resolving tag targets would need EC2, which services cannot call. State lives in
//! `ssm-commands.db`, separate from Parameter Store.

use std::sync::Arc;

use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;
use serde_json::{Value, json};

use crate::generated::*;

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
CREATE TABLE commands (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    command_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (account_id, region, command_id)
);
CREATE TABLE invocations (
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    command_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (account_id, region, command_id, instance_id)
);
",
}];

const FINAL: &[&str] = &["Success", "Failed", "TimedOut", "Cancelled"];
const STATUSES: &[&str] = &[
    "Pending",
    "InProgress",
    "Delayed",
    "Success",
    "Failed",
    "TimedOut",
    "Cancelled",
    "Cancelling",
];
const PAGE: usize = 50;

pub struct Commands {
    db: Arc<Db>,
}

fn ve(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "ValidationException", message)
}

fn now() -> i64 {
    // Virtual clock: `POST /roto-api/clock/advance` moves it.
    roto_core::clock::now_secs()
}

fn iso(seconds: i64) -> String {
    Timestamp(seconds).to_iso8601()
}

fn text(value: &Value, key: &str) -> Option<String> {
    value[key].as_str().map(str::to_owned)
}

/// The plugin name the real agent reports for the common shell documents.
fn plugin_name(document: &str) -> String {
    match document {
        "AWS-RunShellScript" => "aws:runShellScript".into(),
        "AWS-RunPowerShellScript" => "aws:runPowerShellScript".into(),
        other => other.into(),
    }
}

fn invalid_instance(id: &str) -> AwsError {
    AwsError::sender(
        400,
        "InvalidInstanceId",
        format!("Instances [[{id}]] not in a valid state for account"),
    )
}

fn offset(token: &Option<String>) -> Result<usize, AwsError> {
    match token {
        None => Ok(0),
        Some(t) => t.parse().map_err(|_| {
            AwsError::sender(400, "InvalidNextToken", "The specified token is not valid")
        }),
    }
}

fn page_size(max: Option<i32>) -> Result<usize, AwsError> {
    match max {
        None => Ok(PAGE),
        Some(n) if (1..=50).contains(&n) => Ok(n as usize),
        Some(_) => Err(ve("MaxResults must be between 1 and 50")),
    }
}

/// Command-level status derived from its invocations.
fn rollup(invocations: &[Value]) -> (&'static str, i32, i32) {
    let status = |v: &Value| v["status"].as_str().unwrap_or("Pending").to_owned();
    let all: Vec<String> = invocations.iter().map(status).collect();
    let completed = all.iter().filter(|s| FINAL.contains(&s.as_str())).count() as i32;
    let errors = all
        .iter()
        .filter(|s| matches!(s.as_str(), "Failed" | "TimedOut"))
        .count() as i32;
    let overall = if all.iter().all(|s| s == "Pending") {
        "Pending"
    } else if completed < all.len() as i32 {
        "InProgress"
    } else if all.iter().all(|s| s == "Success") {
        "Success"
    } else if all.iter().any(|s| s == "Failed") {
        "Failed"
    } else if all.iter().any(|s| s == "TimedOut") {
        "TimedOut"
    } else {
        "Cancelled"
    };
    (overall, completed, errors)
}

fn matches_filters(
    filters: &[CommandFilter],
    status: &str,
    document: &str,
    requested: i64,
) -> Result<bool, AwsError> {
    for f in filters {
        let hit = match f.key.as_str() {
            "Status" => status == f.value,
            "DocumentName" => document == f.value,
            "InvokedAfter" => requested >= Timestamp::parse(&f.value).map_or(0, |t| t.0),
            "InvokedBefore" => requested <= Timestamp::parse(&f.value).map_or(i64::MAX, |t| t.0),
            "ExecutionStage" => true,
            other => return Err(ve(format!("Unsupported filter key: {other}"))),
        };
        if !hit {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Commands {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("ssm-commands", MIGRATIONS)?,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM invocations", [])?;
            tx.execute("DELETE FROM commands", [])?;
            Ok(())
        })
    }

    fn command_rows(
        tx: &roto_core::rusqlite::Connection,
        ctx: &RequestContext,
    ) -> Result<Vec<Value>, AwsError> {
        let mut stmt = tx.prepare(
            "SELECT body FROM commands WHERE account_id=?1 AND region=?2 ORDER BY seq DESC",
        )?;
        let rows = stmt.query_map(params![ctx.account_id, ctx.region], |r| {
            r.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(serde_json::from_str(&row?).map_err(|e| AwsError::internal(e.to_string()))?);
        }
        Ok(out)
    }

    fn invocation_rows(
        tx: &roto_core::rusqlite::Connection,
        ctx: &RequestContext,
        command_id: Option<&str>,
    ) -> Result<Vec<Value>, AwsError> {
        let mut stmt = tx.prepare(
            "SELECT body FROM invocations WHERE account_id=?1 AND region=?2
               AND (?3 IS NULL OR command_id=?3) ORDER BY seq DESC, instance_id",
        )?;
        let rows = stmt.query_map(params![ctx.account_id, ctx.region, command_id], |r| {
            r.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(serde_json::from_str(&row?).map_err(|e| AwsError::internal(e.to_string()))?);
        }
        Ok(out)
    }

    fn to_command(command: &Value, invocations: &[Value]) -> Command {
        let (status, completed, errors) = rollup(invocations);
        let requested = command["requested"].as_i64().unwrap_or(0);
        Command {
            command_id: text(command, "command_id"),
            comment: text(command, "comment"),
            document_name: text(command, "document_name"),
            document_version: text(command, "document_version"),
            instance_ids: serde_json::from_value(command["instance_ids"].clone())
                .unwrap_or_default(),
            parameters: serde_json::from_value(command["parameters"].clone()).unwrap_or_default(),
            requested_date_time: Some(Timestamp(requested)),
            expires_after: Some(Timestamp(requested + 3600)),
            service_role: text(command, "service_role"),
            timeout_seconds: command["timeout_seconds"].as_i64().map(|n| n as i32),
            output_s3_bucket_name: text(command, "output_s3_bucket_name"),
            output_s3_key_prefix: text(command, "output_s3_key_prefix"),
            max_concurrency: text(command, "max_concurrency"),
            max_errors: text(command, "max_errors"),
            status: Some(status.into()),
            status_details: Some(status.into()),
            target_count: Some(invocations.len() as i32),
            completed_count: Some(completed),
            error_count: Some(errors),
            delivery_timed_out_count: Some(0),
            ..Default::default()
        }
    }

    pub fn send_command(
        &self,
        ctx: &RequestContext,
        i: SendCommandRequest,
    ) -> Result<SendCommandResult, AwsError> {
        let mut instances = i.instance_ids.clone();
        for target in &i.targets {
            if target.key.as_deref() != Some("InstanceIds") {
                return Err(ve(
                    "Only Targets with Key InstanceIds are supported; use InstanceIds or list the instance IDs",
                ));
            }
            instances.extend(target.values.iter().cloned());
        }
        if instances.is_empty() {
            return Err(AwsError::sender(
                400,
                "InvalidTarget",
                "At least one of InstanceIds or Targets is required",
            ));
        }
        if instances.len() > 50 {
            return Err(ve("At most 50 instances can be targeted"));
        }
        for id in &instances {
            if !(id.starts_with("i-") || id.starts_with("mi-")) {
                return Err(invalid_instance(id));
            }
        }
        instances.dedup();
        let requested = now();
        let command_id = roto_core::ids::request_id();
        let command = json!({
            "command_id": command_id,
            "document_name": i.document_name,
            "document_version": i.document_version,
            "comment": i.comment,
            "instance_ids": instances,
            "parameters": i.parameters,
            "requested": requested,
            "service_role": i.service_role_arn,
            "timeout_seconds": i.timeout_seconds,
            "output_s3_bucket_name": i.output_s3_bucket_name,
            "output_s3_key_prefix": i.output_s3_key_prefix,
            "max_concurrency": i.max_concurrency,
            "max_errors": i.max_errors,
        });
        let invocations: Vec<Value> = instances
            .iter()
            .map(|instance| {
                json!({
                    "command_id": command_id,
                    "instance_id": instance,
                    "document_name": i.document_name,
                    "document_version": i.document_version,
                    "comment": i.comment,
                    "requested": requested,
                    "status": "Pending",
                    "status_details": "Pending",
                    "stdout": "",
                    "stderr": "",
                    "response_code": -1,
                })
            })
            .collect();
        self.db.transaction(|tx| {
            let seq: i64 = tx.query_row("SELECT COALESCE(MAX(seq),0)+1 FROM commands", [], |r| {
                r.get(0)
            })?;
            tx.execute(
                "INSERT INTO commands (account_id,region,command_id,seq,body) VALUES (?1,?2,?3,?4,?5)",
                params![ctx.account_id, ctx.region, command_id, seq, command.to_string()],
            )?;
            for invocation in &invocations {
                tx.execute(
                    "INSERT INTO invocations (account_id,region,command_id,instance_id,seq,body) VALUES (?1,?2,?3,?4,?5,?6)",
                    params![
                        ctx.account_id,
                        ctx.region,
                        command_id,
                        invocation["instance_id"].as_str(),
                        seq,
                        invocation.to_string()
                    ],
                )?;
            }
            Ok(())
        })?;
        Ok(SendCommandResult {
            command: Some(Self::to_command(&command, &invocations)),
        })
    }

    pub fn list_commands(
        &self,
        ctx: &RequestContext,
        i: ListCommandsRequest,
    ) -> Result<ListCommandsResult, AwsError> {
        let size = page_size(i.max_results)?;
        let start = offset(&i.next_token)?;
        self.db.read(|conn| {
            let mut out = Vec::new();
            for command in Self::command_rows(conn, ctx)? {
                let id = text(&command, "command_id").unwrap_or_default();
                if i.command_id.as_ref().is_some_and(|c| *c != id) {
                    continue;
                }
                if i.instance_id.as_ref().is_some_and(|instance| {
                    !command["instance_ids"]
                        .as_array()
                        .is_some_and(|ids| ids.iter().any(|v| v.as_str() == Some(instance)))
                }) {
                    continue;
                }
                let invocations = Self::invocation_rows(conn, ctx, Some(&id))?;
                let summary = Self::to_command(&command, &invocations);
                if matches_filters(
                    &i.filters,
                    summary.status.as_deref().unwrap_or(""),
                    summary.document_name.as_deref().unwrap_or(""),
                    command["requested"].as_i64().unwrap_or(0),
                )? {
                    out.push(summary);
                }
            }
            let total = out.len();
            if start > total {
                return Err(AwsError::sender(
                    400,
                    "InvalidNextToken",
                    "The specified token is not valid",
                ));
            }
            let commands: Vec<Command> = out.into_iter().skip(start).take(size).collect();
            let next = (start + size < total).then(|| (start + size).to_string());
            Ok(ListCommandsResult {
                commands,
                next_token: next,
            })
        })
    }

    pub fn list_command_invocations(
        &self,
        ctx: &RequestContext,
        i: ListCommandInvocationsRequest,
    ) -> Result<ListCommandInvocationsResult, AwsError> {
        let size = page_size(i.max_results)?;
        let start = offset(&i.next_token)?;
        self.db.read(|conn| {
            let mut out = Vec::new();
            for inv in Self::invocation_rows(conn, ctx, i.command_id.as_deref())? {
                if i.instance_id
                    .as_ref()
                    .is_some_and(|id| Some(id.as_str()) != inv["instance_id"].as_str())
                {
                    continue;
                }
                let status = inv["status"].as_str().unwrap_or("Pending");
                let document = inv["document_name"].as_str().unwrap_or("");
                if !matches_filters(
                    &i.filters,
                    status,
                    document,
                    inv["requested"].as_i64().unwrap_or(0),
                )? {
                    continue;
                }
                let plugins = if i.details == Some(true) {
                    vec![CommandPlugin {
                        name: Some(plugin_name(document)),
                        status: Some(status.into()),
                        status_details: text(&inv, "status_details"),
                        response_code: inv["response_code"].as_i64().map(|n| n as i32),
                        output: text(&inv, "stdout"),
                        ..Default::default()
                    }]
                } else {
                    Vec::new()
                };
                out.push(CommandInvocation {
                    command_id: text(&inv, "command_id"),
                    instance_id: text(&inv, "instance_id"),
                    instance_name: Some(String::new()),
                    comment: text(&inv, "comment"),
                    document_name: Some(document.into()),
                    document_version: text(&inv, "document_version"),
                    requested_date_time: inv["requested"].as_i64().map(Timestamp),
                    status: Some(status.into()),
                    status_details: text(&inv, "status_details"),
                    command_plugins: plugins,
                    ..Default::default()
                });
            }
            let total = out.len();
            if start > total {
                return Err(AwsError::sender(
                    400,
                    "InvalidNextToken",
                    "The specified token is not valid",
                ));
            }
            let invocations: Vec<CommandInvocation> =
                out.into_iter().skip(start).take(size).collect();
            let next = (start + size < total).then(|| (start + size).to_string());
            Ok(ListCommandInvocationsResult {
                command_invocations: invocations,
                next_token: next,
            })
        })
    }

    pub fn get_command_invocation(
        &self,
        ctx: &RequestContext,
        i: GetCommandInvocationRequest,
    ) -> Result<GetCommandInvocationResult, AwsError> {
        let body: Option<String> = self.db.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT body FROM invocations WHERE account_id=?1 AND region=?2 AND command_id=?3 AND instance_id=?4",
                    params![ctx.account_id, ctx.region, i.command_id, i.instance_id],
                    |r| r.get(0),
                )
                .optional()?)
        })?;
        let inv: Value = serde_json::from_str(&body.ok_or_else(|| {
            AwsError::sender(
                400,
                "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
            )
        })?)
        .map_err(|e| AwsError::internal(e.to_string()))?;
        let document = inv["document_name"].as_str().unwrap_or("");
        let plugin = plugin_name(document);
        if i.plugin_name.as_ref().is_some_and(|p| *p != plugin) {
            return Err(AwsError::sender(
                400,
                "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
            ));
        }
        let started = inv["started"].as_i64();
        let finished = inv["finished"].as_i64();
        Ok(GetCommandInvocationResult {
            command_id: text(&inv, "command_id"),
            instance_id: text(&inv, "instance_id"),
            comment: text(&inv, "comment"),
            document_name: Some(document.into()),
            document_version: text(&inv, "document_version"),
            plugin_name: Some(plugin),
            response_code: inv["response_code"].as_i64().map(|n| n as i32),
            standard_output_content: text(&inv, "stdout"),
            standard_error_content: text(&inv, "stderr"),
            standard_output_url: Some(String::new()),
            standard_error_url: Some(String::new()),
            status: text(&inv, "status"),
            status_details: text(&inv, "status_details"),
            execution_start_date_time: started.map(iso),
            execution_end_date_time: finished.map(iso),
            execution_elapsed_time: started.zip(finished).map(|(s, f)| format!("PT{}S", f - s)),
            ..Default::default()
        })
    }

    pub fn cancel_command(
        &self,
        ctx: &RequestContext,
        i: CancelCommandRequest,
    ) -> Result<CancelCommandResult, AwsError> {
        self.db.transaction(|tx| {
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM commands WHERE account_id=?1 AND region=?2 AND command_id=?3",
                    params![ctx.account_id, ctx.region, i.command_id],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_none() {
                return Err(AwsError::sender(
                    400,
                    "InvalidCommandId",
                    "The specified command ID is not valid",
                ));
            }
            for mut inv in Self::invocation_rows(tx, ctx, Some(&i.command_id))? {
                let status = inv["status"].as_str().unwrap_or("Pending");
                let wanted = i.instance_ids.is_empty()
                    || i.instance_ids
                        .iter()
                        .any(|id| Some(id.as_str()) == inv["instance_id"].as_str());
                if wanted && !FINAL.contains(&status) {
                    inv["status"] = json!("Cancelled");
                    inv["status_details"] = json!("Cancelled");
                    inv["finished"] = json!(now());
                    Self::save(tx, ctx, &inv)?;
                }
            }
            Ok(CancelCommandResult {})
        })
    }

    fn save(
        tx: &roto_core::rusqlite::Connection,
        ctx: &RequestContext,
        inv: &Value,
    ) -> Result<(), AwsError> {
        tx.execute(
            "UPDATE invocations SET body=?1 WHERE account_id=?2 AND region=?3 AND command_id=?4 AND instance_id=?5",
            params![
                inv.to_string(),
                ctx.account_id,
                ctx.region,
                inv["command_id"].as_str(),
                inv["instance_id"].as_str()
            ],
        )?;
        Ok(())
    }

    /// Roto extension (not an SSM operation): settle invocations of one command. Without
    /// `InstanceId` it applies to every invocation of the command.
    pub fn complete_invocation(
        &self,
        ctx: &RequestContext,
        input: &Value,
    ) -> Result<Value, AwsError> {
        let command_id = input["CommandId"]
            .as_str()
            .ok_or_else(|| AwsError::missing_parameter("CommandId"))?;
        let status = input["Status"].as_str().unwrap_or("Success");
        if !STATUSES.contains(&status) {
            return Err(ve(format!(
                "Status must be one of: {}",
                STATUSES.join(", ")
            )));
        }
        let instance = input["InstanceId"].as_str();
        self.db.transaction(|tx| {
            let rows = Self::invocation_rows(tx, ctx, Some(command_id))?;
            if rows.is_empty() {
                return Err(AwsError::sender(
                    400,
                    "InvalidCommandId",
                    "The specified command ID is not valid",
                ));
            }
            let mut matched = 0;
            for mut inv in rows {
                if instance.is_some_and(|id| Some(id) != inv["instance_id"].as_str()) {
                    continue;
                }
                matched += 1;
                let moment = now();
                inv["status"] = json!(status);
                inv["status_details"] = json!(input["StatusDetails"].as_str().unwrap_or(status));
                if let Some(out) = input["StandardOutputContent"].as_str() {
                    inv["stdout"] = json!(out);
                }
                if let Some(err) = input["StandardErrorContent"].as_str() {
                    inv["stderr"] = json!(err);
                }
                let default_code = match status {
                    "Success" => 0,
                    "Failed" | "TimedOut" => 1,
                    _ => -1,
                };
                inv["response_code"] =
                    json!(input["ResponseCode"].as_i64().unwrap_or(default_code));
                if inv["started"].is_null() && status != "Pending" {
                    inv["started"] = json!(moment);
                }
                if FINAL.contains(&status) {
                    inv["finished"] = json!(moment);
                }
                Self::save(tx, ctx, &inv)?;
            }
            if matched == 0 {
                return Err(AwsError::sender(
                    400,
                    "InvocationDoesNotExist",
                    "No invocation for that instance",
                ));
            }
            Ok(json!({}))
        })
    }
}
