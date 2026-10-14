//! RDS Data API proxy to a PostgreSQL server.
//!
//! The server for each request is chosen by a Lua hook (`roto.hooks.rds_data_connection`)
//! through [`RdsData::set_resolver`]. Statements run with the PostgreSQL text protocol, so
//! `:name` parameters are inlined as escaped literals (see [`bind`]).
mod bind;

use base64::{Engine, engine::general_purpose::STANDARD};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, types::Type};

/// What a connection resolver is told about the request.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    pub resource_arn: String,
    pub secret_arn: Option<String>,
    pub database: Option<String>,
    pub account_id: String,
    pub region: String,
}

/// Returns a PostgreSQL connection string, or `None` when nothing is configured.
pub type Resolver = Arc<dyn Fn(&ConnectRequest) -> Result<Option<String>, String> + Send + Sync>;

const TRANSACTION_TTL: Duration = Duration::from_secs(180);

struct Transaction {
    client: Arc<Client>,
    started: Instant,
}

pub struct RdsData {
    /// Owned runtime; the synchronous handler blocks on it from the server's blocking pool.
    runtime: Option<tokio::runtime::Runtime>,
    resolver: RwLock<Option<Resolver>>,
    clients: Mutex<HashMap<String, Arc<Client>>>,
    transactions: Mutex<HashMap<String, Transaction>>,
}

impl Drop for RdsData {
    fn drop(&mut self) {
        // Dropping a runtime inside an async context would panic.
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

pub struct RdsDataHandler(pub Arc<RdsData>);

impl RdsDataHandler {
    pub fn new() -> Self {
        Self(Arc::new(RdsData {
            runtime: Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("rds-data")
                    .enable_all()
                    .build()
                    .expect("RDS Data runtime"),
            ),
            resolver: RwLock::new(None),
            clients: Mutex::default(),
            transactions: Mutex::default(),
        }))
    }
}

impl Default for RdsDataHandler {
    fn default() -> Self {
        Self::new()
    }
}

fn bad(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "BadRequestException", message)
}

fn db_error(e: tokio_postgres::Error) -> AwsError {
    if let Some(db) = e.as_db_error() {
        return bad(format!(
            "ERROR: {}; SQLState: {}",
            db.message(),
            db.code().code()
        ));
    }
    let mut message = e.to_string();
    let mut source = std::error::Error::source(&e);
    while let Some(s) = source {
        message.push_str(": ");
        message.push_str(&s.to_string());
        source = s.source();
    }
    AwsError::receiver(500, "InternalServerErrorException", message)
}

fn jdbc_type(ty: &Type) -> i64 {
    match *ty {
        Type::BOOL => -7,
        Type::INT2 => 5,
        Type::INT4 => 4,
        Type::INT8 => -5,
        Type::FLOAT4 => 7,
        Type::FLOAT8 => 8,
        Type::NUMERIC => 2,
        Type::TEXT | Type::VARCHAR | Type::NAME => 12,
        Type::BPCHAR => 1,
        Type::DATE => 91,
        Type::TIME => 92,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => 93,
        Type::BYTEA => -2,
        _ => 1111,
    }
}

/// A text-protocol column value as a Data API `Field`.
fn field(ty: &Type, text: Option<&str>) -> Value {
    let Some(text) = text else {
        return json!({"isNull": true});
    };
    let string = || json!({"stringValue": text});
    match *ty {
        Type::BOOL => json!({"booleanValue": text == "t"}),
        Type::INT2 | Type::INT4 | Type::INT8 | Type::OID => text
            .parse::<i64>()
            .map_or_else(|_| string(), |n| json!({"longValue": n})),
        Type::FLOAT4 | Type::FLOAT8 => text
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .map_or_else(string, |f| json!({"doubleValue": f})),
        Type::BYTEA => match text.strip_prefix("\\x") {
            Some(hex) if hex.len() % 2 == 0 && hex.is_ascii() => {
                let bytes: Option<Vec<u8>> = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
                    .collect();
                bytes.map_or_else(string, |b| json!({"blobValue": STANDARD.encode(b)}))
            }
            _ => string(),
        },
        _ => string(),
    }
}

/// A `Field` as a plain JSON value for `formatRecordsAs: JSON`.
fn plain(field: &Value) -> Value {
    let (kind, v) = field.as_object().and_then(|o| o.iter().next()).unwrap();
    if kind == "isNull" {
        Value::Null
    } else {
        v.clone()
    }
}

fn required<'a>(body: &'a Value, name: &str) -> Result<&'a str, AwsError> {
    body[name]
        .as_str()
        .ok_or_else(|| bad(format!("Missing required parameter: {name}")))
}

impl RdsData {
    /// Set by the server once Lua hooks are available.
    pub fn set_resolver(&self, resolver: Resolver) {
        *self.resolver.write().unwrap() = Some(resolver);
    }

    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.runtime.as_ref().expect("runtime").block_on(f)
    }

    fn url(&self, ctx: &RequestContext, body: &Value) -> Result<String, AwsError> {
        let arn = required(body, "resourceArn")?;
        let request = ConnectRequest {
            resource_arn: arn.to_owned(),
            secret_arn: body["secretArn"].as_str().map(str::to_owned),
            database: body["database"].as_str().map(str::to_owned),
            account_id: ctx.account_id.clone(),
            region: ctx.region.clone(),
        };
        let resolver = self.resolver.read().unwrap().clone();
        let url = resolver
            .ok_or_else(|| "no resolver is installed".to_owned())
            .and_then(|r| r(&request))
            .map_err(|e| AwsError::receiver(500, "InternalServerErrorException", e))?;
        url.ok_or_else(|| {
            bad(format!(
                "No database connection is configured for {arn}; register one with \
                 roto.hooks.rds_data_connection"
            ))
        })
    }

    async fn connect(url: &str) -> Result<Client, AwsError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(db_error)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// A shared connection for single statements outside a transaction.
    fn shared(&self, url: &str) -> Result<Arc<Client>, AwsError> {
        if let Some(c) = self
            .clients
            .lock()
            .unwrap()
            .get(url)
            .filter(|c| !c.is_closed())
        {
            return Ok(c.clone());
        }
        let client = Arc::new(self.block_on(Self::connect(url))?);
        self.clients
            .lock()
            .unwrap()
            .insert(url.to_owned(), client.clone());
        Ok(client)
    }

    fn transaction(&self, id: &str) -> Result<Arc<Client>, AwsError> {
        let mut txs = self.transactions.lock().unwrap();
        txs.retain(|_, t| t.started.elapsed() < TRANSACTION_TTL);
        txs.get(id).map(|t| t.client.clone()).ok_or_else(|| {
            AwsError::sender(
                404,
                "TransactionNotFoundException",
                format!("Transaction {id} Not Found"),
            )
        })
    }

    fn client(&self, ctx: &RequestContext, body: &Value) -> Result<Arc<Client>, AwsError> {
        match body["transactionId"].as_str() {
            Some(id) => self.transaction(id),
            None => self.shared(&self.url(ctx, body)?),
        }
    }

    /// Runs one statement and returns `(columns, rows, command_count)`.
    async fn run(
        client: &Client,
        sql: &str,
    ) -> Result<(Vec<(String, Type)>, Vec<Vec<Value>>, u64), AwsError> {
        // Preparing yields column types; it also rejects multi-statement strings like Aurora.
        let statement = client.prepare(sql).await.map_err(db_error)?;
        let columns: Vec<(String, Type)> = statement
            .columns()
            .iter()
            .map(|c| (c.name().to_owned(), c.type_().clone()))
            .collect();
        let mut rows = Vec::new();
        let mut count = 0;
        for message in client.simple_query(sql).await.map_err(db_error)? {
            match message {
                SimpleQueryMessage::Row(row) => rows.push(
                    columns
                        .iter()
                        .enumerate()
                        .map(|(i, (_, ty))| field(ty, row.get(i)))
                        .collect(),
                ),
                SimpleQueryMessage::CommandComplete(n) => count = n,
                _ => {}
            }
        }
        Ok((columns, rows, count))
    }

    fn execute(&self, ctx: &RequestContext, body: &Value) -> Result<Value, AwsError> {
        let sql = bind::bind(
            required(body, "sql")?,
            &bind::literals(&body["parameters"])?,
        )?;
        let client = self.client(ctx, body)?;
        let (columns, rows, count) = self.block_on(Self::run(&client, &sql))?;
        let mut out = json!({"numberOfRecordsUpdated": if columns.is_empty() { count } else { 0 }});
        if columns.is_empty() {
            return Ok(out);
        }
        if body["formatRecordsAs"] == "JSON" {
            let objects: Vec<Value> = rows
                .iter()
                .map(|row| {
                    columns
                        .iter()
                        .zip(row)
                        .map(|((name, _), f)| (name.clone(), plain(f)))
                        .collect::<serde_json::Map<_, _>>()
                        .into()
                })
                .collect();
            out["formattedRecords"] = Value::String(Value::Array(objects).to_string());
        } else {
            out["records"] = rows.into();
        }
        if body["includeResultMetadata"] == true {
            out["columnMetadata"] = columns
                .iter()
                .map(|(name, ty)| {
                    json!({"name": name, "label": name, "typeName": ty.name(),
                        "type": jdbc_type(ty), "nullable": 2, "precision": 0, "scale": 0,
                        "isAutoIncrement": false, "isCaseSensitive": false,
                        "isCurrency": false, "isSigned": false,
                        "schemaName": "", "tableName": "", "arrayBaseColumnName": ""})
                })
                .collect::<Vec<_>>()
                .into();
        }
        Ok(out)
    }

    fn batch(&self, ctx: &RequestContext, body: &Value) -> Result<Value, AwsError> {
        let sql = required(body, "sql")?;
        let sets: Vec<Value> = match body["parameterSets"].as_array() {
            Some(sets) if !sets.is_empty() => sets.clone(),
            _ => vec![Value::Null],
        };
        let statements = sets
            .iter()
            .map(|set| bind::bind(sql, &bind::literals(set)?))
            .collect::<Result<Vec<_>, _>>()?;
        // Outside a caller transaction the batch is atomic on its own connection.
        let own = match body["transactionId"].as_str() {
            Some(_) => None,
            None => Some(Arc::new(
                self.block_on(Self::connect(&self.url(ctx, body)?))?,
            )),
        };
        let client = match &own {
            Some(c) => c.clone(),
            None => self.client(ctx, body)?,
        };
        self.block_on(async {
            if own.is_some() {
                client.batch_execute("BEGIN").await.map_err(db_error)?;
            }
            let mut results = Vec::new();
            for statement in &statements {
                match client.simple_query(statement).await {
                    Ok(_) => results.push(json!({"generatedFields": []})),
                    Err(e) => {
                        if own.is_some() {
                            let _ = client.batch_execute("ROLLBACK").await;
                        }
                        return Err(db_error(e));
                    }
                }
            }
            if own.is_some() {
                client.batch_execute("COMMIT").await.map_err(db_error)?;
            }
            Ok(json!({"updateResults": results}))
        })
    }

    fn begin(&self, ctx: &RequestContext, body: &Value) -> Result<Value, AwsError> {
        let client = Arc::new(self.block_on(Self::connect(&self.url(ctx, body)?))?);
        self.block_on(client.batch_execute("BEGIN"))
            .map_err(db_error)?;
        let id = uuid::Uuid::new_v4().to_string();
        let mut txs = self.transactions.lock().unwrap();
        txs.retain(|_, t| t.started.elapsed() < TRANSACTION_TTL);
        txs.insert(
            id.clone(),
            Transaction {
                client,
                started: Instant::now(),
            },
        );
        Ok(json!({"transactionId": id}))
    }

    fn finish(&self, body: &Value, commit: bool) -> Result<Value, AwsError> {
        let id = required(body, "transactionId")?;
        self.transaction(id)?;
        let tx = self.transactions.lock().unwrap().remove(id);
        let Some(tx) = tx else {
            return Err(AwsError::sender(
                404,
                "TransactionNotFoundException",
                format!("Transaction {id} Not Found"),
            ));
        };
        self.block_on(
            tx.client
                .batch_execute(if commit { "COMMIT" } else { "ROLLBACK" }),
        )
        .map_err(db_error)?;
        Ok(
            json!({"transactionStatus": if commit { "Transaction Committed" } else { "Rollback Complete" }}),
        )
    }
}

fn operation(req: &RawRequest) -> Option<&'static str> {
    if req.method != "POST" {
        return None;
    }
    Some(match req.path.trim_end_matches('/') {
        "/Execute" => "ExecuteStatement",
        "/BatchExecute" => "BatchExecuteStatement",
        "/BeginTransaction" => "BeginTransaction",
        "/CommitTransaction" => "CommitTransaction",
        "/RollbackTransaction" => "RollbackTransaction",
        _ => return None,
    })
}

impl ServiceHandler for RdsDataHandler {
    fn service(&self) -> &'static str {
        "rds-data"
    }

    fn operation_name(&self, req: &RawRequest) -> Option<String> {
        operation(req).map(str::to_owned)
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let result = (|| {
            let op = operation(req).ok_or_else(|| {
                AwsError::sender(
                    404,
                    "NotFoundException",
                    format!("Unknown path {} {}", req.method, req.path),
                )
            })?;
            let body: Value = serde_json::from_slice(&req.body)
                .map_err(|_| bad("Request body must be valid JSON"))?;
            match op {
                "ExecuteStatement" => self.0.execute(ctx, &body),
                "BatchExecuteStatement" => self.0.batch(ctx, &body),
                "BeginTransaction" => self.0.begin(ctx, &body),
                "CommitTransaction" => self.0.finish(&body, true),
                _ => self.0.finish(&body, false),
            }
        })();
        let (status, mut headers, body) = match result {
            Ok(v) => (200, vec![], v),
            Err(e) => (
                e.status,
                vec![("x-amzn-errortype".to_owned(), e.code.clone())],
                json!({"message": e.message}),
            ),
        };
        headers.push(("content-type".into(), "application/json".into()));
        headers.push(("x-amzn-requestid".into(), ctx.request_id.clone()));
        Ok(RawResponse {
            status,
            headers,
            body: serde_json::to_vec(&body).unwrap(),
        })
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.clients.lock().unwrap().clear();
        self.0.transactions.lock().unwrap().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_text_values_to_fields() {
        assert_eq!(field(&Type::INT4, Some("42")), json!({"longValue": 42}));
        assert_eq!(field(&Type::BOOL, Some("t")), json!({"booleanValue": true}));
        assert_eq!(
            field(&Type::FLOAT8, Some("1.5")),
            json!({"doubleValue": 1.5})
        );
        assert_eq!(
            field(&Type::FLOAT8, Some("NaN")),
            json!({"stringValue": "NaN"})
        );
        assert_eq!(
            field(&Type::BYTEA, Some("\\x0102")),
            json!({"blobValue": "AQI="})
        );
        assert_eq!(
            field(&Type::NUMERIC, Some("1.50")),
            json!({"stringValue": "1.50"})
        );
        assert_eq!(field(&Type::TEXT, None), json!({"isNull": true}));
    }
}
