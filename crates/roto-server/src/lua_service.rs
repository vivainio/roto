//! Lazy, trusted Lua services. Storage callbacks live only for one SQLite transaction.
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use base64::Engine;
use sha2::{Digest, Sha256};

use mlua::{Lua, LuaSerdeExt, Table, Value as LuaValue};
use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{json_error, json_response};
use serde_json::{Value, json};

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "CREATE TABLE documents (namespace TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(namespace,id))",
}];

pub struct LuaService {
    definition: Definition,
    store: Arc<Store>,
    bundle: Arc<Bundle>,
    runtime: Mutex<Option<Runtime>>,
}
struct Definition {
    service: &'static str,
    target: String,
    json_version: String,
    script: String,
    common: Option<String>,
    wire: Option<crate::lua_wire::Wire>,
}

struct Runtime {
    lua: Lua,
    operations: Table,
    db: Arc<Db>,
}

// Archives are read in place; nothing is extracted. A shared open archive keeps
// its central-directory index stable across the lazy loading of different services.
enum Bundle {
    Directory(PathBuf),
    Zip(Mutex<zip::ZipArchive<std::fs::File>>),
}

const MAX_BUNDLE_FILE: u64 = 1024 * 1024;

fn relative_path(path: &str) -> bool {
    !path.is_empty()
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

impl Bundle {
    fn open(path: PathBuf) -> Result<Self, AwsError> {
        if path.is_dir() {
            return Ok(Self::Directory(path));
        }
        let file = std::fs::File::open(&path).map_err(|e| {
            AwsError::internal(format!(
                "cannot open Lua service bundle {}: {e}",
                path.display()
            ))
        })?;
        let archive = zip::ZipArchive::new(file).map_err(|e| {
            AwsError::internal(format!("invalid Lua service ZIP {}: {e}", path.display()))
        })?;
        for name in archive.file_names() {
            let name = name.map_err(|e| AwsError::internal(e.to_string()))?;
            if !relative_path(name.trim_end_matches('/')) {
                return Err(AwsError::internal(format!("unsafe ZIP entry: {name}")));
            }
        }
        Ok(Self::Zip(Mutex::new(archive)))
    }

    fn read(&self, path: &str) -> Result<String, AwsError> {
        let read = |reader: &mut dyn Read| -> Result<String, AwsError> {
            let mut bytes = Vec::new();
            reader
                .take(MAX_BUNDLE_FILE + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| AwsError::internal(format!("cannot read bundle file {path}: {e}")))?;
            if bytes.len() as u64 > MAX_BUNDLE_FILE {
                return Err(AwsError::internal(format!(
                    "bundle file {path} exceeds 1 MiB"
                )));
            }
            String::from_utf8(bytes)
                .map_err(|e| AwsError::internal(format!("invalid UTF-8 in {path}: {e}")))
        };
        match self {
            Self::Directory(directory) => {
                let mut file = std::fs::File::open(directory.join(path)).map_err(|e| {
                    AwsError::internal(format!("cannot open bundle file {path}: {e}"))
                })?;
                read(&mut file)
            }
            Self::Zip(archive) => {
                let mut archive = archive.lock().unwrap();
                let mut file = archive.by_name(path).map_err(|e| {
                    AwsError::internal(format!("cannot open ZIP entry {path}: {e}"))
                })?;
                read(&mut file)
            }
        }
    }
}

/// Read only the root manifest. No bundle means no Lua services; scripts and
/// databases remain unopened until the first request for each installed service.
pub fn discover(store: Arc<Store>, path: Option<PathBuf>) -> Result<Vec<LuaService>, AwsError> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let bundle = Arc::new(Bundle::open(path)?);
    let manifest: Value = serde_json::from_str(&bundle.read("manifest.json")?)
        .map_err(|e| AwsError::internal(format!("invalid Lua bundle manifest: {e}")))?;
    let invalid =
        |message: &str| AwsError::internal(format!("invalid Lua bundle manifest: {message}"));
    let object = manifest
        .as_object()
        .ok_or_else(|| invalid("expected an object"))?;
    if object
        .keys()
        .any(|k| !matches!(k.as_str(), "version" | "common" | "services"))
    {
        return Err(invalid("unknown root manifest field"));
    }
    if manifest["version"].as_u64() != Some(1) {
        return Err(invalid("version must be 1"));
    }
    let common = match manifest.get("common") {
        Some(value) => {
            let path = value
                .as_str()
                .filter(|p| relative_path(p) && p.ends_with(".luau"))
                .ok_or_else(|| invalid("common must be a relative Lua script path"))?;
            Some(path.to_owned())
        }
        None => None,
    };
    let entries = manifest["services"]
        .as_array()
        .ok_or_else(|| invalid("services must be an array"))?;
    let mut services: Vec<LuaService> = Vec::new();
    let mut names = std::collections::HashSet::new();
    // Validate all entries before interning names with process lifetime.
    let mut definitions = Vec::new();
    for entry in entries {
        let object = entry
            .as_object()
            .ok_or_else(|| invalid("service entry must be an object"))?;
        if object.keys().any(|k| {
            !matches!(
                k.as_str(),
                "service" | "script" | "target_prefix" | "json_version" | "protocol" | "model"
            )
        }) {
            return Err(invalid("unknown service entry field"));
        }
        let name = entry["service"]
            .as_str()
            .ok_or_else(|| invalid("service is required"))?;
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        {
            return Err(invalid(
                "service must contain only lowercase letters, digits, or hyphens",
            ));
        }
        if !names.insert(name.to_owned()) {
            return Err(invalid("duplicate service"));
        }
        let script = entry["script"]
            .as_str()
            .filter(|p| relative_path(p) && p.ends_with(".luau"))
            .ok_or_else(|| invalid("script must be a relative Lua script path"))?;
        let protocol = entry["protocol"].as_str().unwrap_or("json");
        let wire = match protocol {
            "json" => None,
            "rest-json" | "query" => {
                if entry.get("target_prefix").is_some() || entry.get("json_version").is_some() {
                    return Err(invalid("model protocols cannot specify JSON target fields"));
                }
                let path = entry["model"]
                    .as_str()
                    .filter(|p| relative_path(p) && p.ends_with(".json"))
                    .ok_or_else(|| invalid("model must be a relative JSON path"))?;
                let model: Value = serde_json::from_str(&bundle.read(path)?)
                    .map_err(|e| invalid(&format!("invalid service model: {e}")))?;
                if model["metadata"]["protocol"].as_str() != Some(protocol)
                    || !model["operations"].is_object()
                    || !model["shapes"].is_object()
                {
                    return Err(invalid("model protocol/operations/shapes mismatch"));
                }
                Some(crate::lua_wire::Wire {
                    model,
                    protocol: protocol.into(),
                })
            }
            _ => return Err(invalid("unsupported protocol")),
        };
        let target = if wire.is_some() {
            ""
        } else {
            let target = entry["target_prefix"]
                .as_str()
                .ok_or_else(|| invalid("target_prefix is required"))?;
            if target.len() < 2
                || !target.ends_with('.')
                || !target
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
            {
                return Err(invalid(
                    "target_prefix must be an ASCII target ending with a dot",
                ));
            }
            if definitions.iter().any(|d: &Definition| {
                !d.target.is_empty()
                    && (d.target.starts_with(target) || target.starts_with(&d.target))
            }) {
                return Err(invalid("target_prefix overlaps another service"));
            }
            target
        };
        let version = if wire.is_some() {
            "1.1"
        } else {
            let version = entry["json_version"]
                .as_str()
                .ok_or_else(|| invalid("json_version is required"))?;
            if !matches!(version, "1.0" | "1.1") {
                return Err(invalid("json_version must be 1.0 or 1.1"));
            }
            version
        };
        definitions.push(Definition {
            service: Box::leak(name.to_owned().into_boxed_str()),
            script: script.into(),
            target: target.into(),
            json_version: version.into(),
            common: common.clone(),
            wire,
        });
    }
    for definition in definitions {
        services.push(LuaService {
            definition,
            store: store.clone(),
            bundle: bundle.clone(),
            runtime: Mutex::new(None),
        });
    }
    Ok(services)
}

fn lua_error(error: mlua::Error) -> AwsError {
    match &error {
        mlua::Error::ExternalError(e) => {
            if let Some(e) = e.downcast_ref::<AwsError>() {
                return e.clone();
            }
        }
        mlua::Error::CallbackError { cause, .. } => return lua_error((**cause).clone()),
        _ => {}
    }
    AwsError::internal(format!("Lua service: {error}"))
}

impl LuaService {
    fn load(&self) -> Result<Runtime, AwsError> {
        let source = self.bundle.read(&self.definition.script)?;
        let lua = Lua::new();
        let api = lua.create_table().map_err(lua_error)?;
        api.set("version", env!("CARGO_PKG_VERSION"))
            .map_err(lua_error)?;
        api.set("api_version", crate::setup::LUA_API_VERSION)
            .map_err(lua_error)?;
        api.set(
            "array",
            lua.create_function(|lua, table: Option<mlua::Table>| match table {
                Some(table) => {
                    table.set_metatable(Some(lua.array_metatable()))?;
                    Ok(mlua::Value::Table(table))
                }
                None => lua.to_value(&Vec::<Value>::new()),
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set("null", lua.null()).map_err(lua_error)?;
        api.set(
            "id",
            lua.create_function(|_, prefix: String| {
                Ok(format!(
                    "{prefix}{}",
                    roto_core::ids::request_id().replace('-', "")[..12].to_owned()
                ))
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set(
            "error",
            lua.create_function(
                |_, (code, message, status): (String, String, Option<u16>)| -> mlua::Result<()> {
                    Err(mlua::Error::external(AwsError::sender(
                        status.unwrap_or(400),
                        code,
                        message,
                    )))
                },
            )
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set(
            "uuid",
            lua.create_function(|_, ()| Ok(roto_core::ids::request_id()))
                .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set(
            "sha256",
            lua.create_function(|_, value: String| {
                Ok(Sha256::digest(value.as_bytes())
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>())
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set(
            "decode_base64",
            lua.create_function(|_, value: String| {
                base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                    .map_err(mlua::Error::external)
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        api.set(
            "json_decode",
            lua.create_function(|lua, (text, code): (String, Option<String>)| {
                let value: Value = serde_json::from_str(&text).map_err(|_| {
                    mlua::Error::external(AwsError::sender(
                        400,
                        code.unwrap_or_else(|| "InvalidParameterException".into()),
                        "Invalid JSON document",
                    ))
                })?;
                lua.to_value(&value)
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        lua.globals().set("roto", api).map_err(lua_error)?;
        if let Some(path) = &self.definition.common {
            lua.load(self.bundle.read(path)?)
                .set_name(format!("@{path}"))
                .exec()
                .map_err(lua_error)?;
        }
        let operations = lua
            .load(&source)
            .set_name(format!("@{}", self.definition.script))
            .eval::<Table>()
            .map_err(lua_error)?;
        let db = self.store.db(self.definition.service, MIGRATIONS)?;
        Ok(Runtime {
            lua,
            operations,
            db,
        })
    }

    fn dispatch(&self, ctx: &RequestContext, req: &RawRequest) -> Result<Value, AwsError> {
        let (operation, input) = self.decode(req)?;
        self.invoke(ctx, &operation, input)
    }

    fn decode(&self, req: &RawRequest) -> Result<(String, Value), AwsError> {
        if let Some(wire) = &self.definition.wire {
            return wire.decode(req);
        }
        let operation = req
            .header("x-amz-target")
            .and_then(|s| s.strip_prefix(self.definition.target.as_str()))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsError::sender(
                    400,
                    "UnknownOperationException",
                    format!("Expected target prefix {}", self.definition.target),
                )
            })?;
        let input: Value = serde_json::from_slice(&req.body)
            .map_err(|e| AwsError::sender(400, "SerializationException", e.to_string()))?;
        if !input.is_object() {
            return Err(AwsError::sender(
                400,
                "SerializationException",
                "Expected an object",
            ));
        }
        Ok((operation.into(), input))
    }

    fn invoke(
        &self,
        ctx: &RequestContext,
        operation: &str,
        input: Value,
    ) -> Result<Value, AwsError> {
        let mut runtime = self.runtime.lock().unwrap();
        if runtime.is_none() {
            *runtime = Some(self.load()?);
        }
        let runtime = runtime.as_ref().unwrap();
        let function = runtime
            .operations
            .get::<Option<mlua::Function>>(operation)
            .map_err(lua_error)?
            .ok_or_else(|| AwsError::not_implemented(self.definition.service, operation))?;
        let lua = &runtime.lua;
        runtime.db.transaction(|tx| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            crate::setup::set_deadline(lua, deadline, "Lua service timed out");
            let result = lua.scope(|scope| {
                let db = lua.create_table()?;
                db.set("get", scope.create_function(|lua, (namespace, id): (String, String)| {
                    let body: Option<String> = tx.query_row("SELECT body FROM documents WHERE namespace=?1 AND id=?2", params![namespace,id], |r| r.get(0)).optional().map_err(mlua::Error::external)?;
                    match body {
                        Some(body) => lua.to_value(&serde_json::from_str::<Value>(&body).map_err(mlua::Error::external)?),
                        None => Ok(LuaValue::Nil),
                    }
                })?)?;
                db.set("put", scope.create_function(|lua, (namespace, id, value): (String, String, LuaValue)| {
                    let body = serde_json::to_string(&lua.from_value::<Value>(value)?).map_err(mlua::Error::external)?;
                    tx.execute("INSERT INTO documents(namespace,id,body) VALUES (?1,?2,?3) ON CONFLICT(namespace,id) DO UPDATE SET body=excluded.body", params![namespace,id,body]).map_err(mlua::Error::external)?;
                    Ok(())
                })?)?;
                db.set("delete", scope.create_function(|_, (namespace, id): (String, String)| {
                    tx.execute("DELETE FROM documents WHERE namespace=?1 AND id=?2", params![namespace,id]).map_err(mlua::Error::external)?;
                    Ok(())
                })?)?;
                db.set("list", scope.create_function(|lua, namespace: String| {
                    let mut stmt = tx.prepare("SELECT body FROM documents WHERE namespace=?1 ORDER BY id").map_err(mlua::Error::external)?;
                    let rows = stmt.query_map([namespace], |r| r.get::<_, String>(0)).map_err(mlua::Error::external)?;
                    let mut values = Vec::new();
                    for row in rows {
                        values.push(serde_json::from_str::<Value>(&row.map_err(mlua::Error::external)?).map_err(mlua::Error::external)?);
                    }
                    lua.to_value(&values)
                })?)?;
                let context = lua.to_value(&json!({"account_id":ctx.account_id,"region":ctx.region,"request_id":ctx.request_id}))?;
                let output: LuaValue = function.call((context, lua.to_value(&input)?, db))?;
                lua.from_value::<Value>(output)
            });
            lua.remove_interrupt();
            result.map_err(lua_error)
        })
    }
}

impl ServiceHandler for LuaService {
    fn operation_name(&self, req: &RawRequest) -> Option<String> {
        self.definition
            .wire
            .as_ref()
            .and_then(|wire| wire.operation_name(req))
    }
    fn service(&self) -> &'static str {
        self.definition.service
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        if let Some(wire) = &self.definition.wire {
            return wire.claims(req);
        }
        req.header("x-amz-target")
            .is_some_and(|s| s.starts_with(self.definition.target.as_str()))
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        if let Some(wire) = &self.definition.wire {
            let (operation, result) = match self.decode(req) {
                Ok((op, input)) => {
                    let result = self.invoke(ctx, &op, input);
                    (op, result)
                }
                Err(e) => (String::new(), Err(e)),
            };
            return Ok(wire.response(&operation, result, &ctx.request_id));
        }
        Ok(match self.dispatch(ctx, req) {
            Ok(body) => json_response(&self.definition.json_version, &ctx.request_id, &body),
            Err(error) => json_error(
                &self.definition.json_version,
                &error,
                &ctx.request_id,
                false,
            ),
        })
    }
    fn reset(&self) -> Result<(), AwsError> {
        let runtime = self.runtime.lock().unwrap();
        let db = match runtime.as_ref() {
            Some(runtime) => Some(runtime.db.clone()),
            None => match self.store.existing_db(self.service()) {
                Some(db) => Some(db),
                None if self
                    .store
                    .data_dir()
                    .is_some_and(|dir| dir.join(format!("{}.db", self.service())).is_file()) =>
                {
                    Some(self.store.db(self.service(), MIGRATIONS)?)
                }
                None => None,
            },
        };
        if let Some(db) = db {
            db.transaction(|tx| {
                tx.execute("DELETE FROM documents", [])?;
                Ok(())
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_service(store: Arc<Store>, name: &str, path: Option<PathBuf>) -> LuaService {
    let path =
        path.unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services"));
    discover(store, Some(path))
        .unwrap()
        .into_iter()
        .find(|s| s.service() == name)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn services_are_optional_and_directory_manifest_is_lazy() {
        let store = Arc::new(Store::ephemeral());
        assert!(discover(store.clone(), None).unwrap().is_empty());
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services");
        let services = discover(store.clone(), Some(bundle)).unwrap();
        assert_eq!(services.len(), 11);
        for service in &services {
            assert!(store.existing_db(service.service()).is_none());
            service.reset().unwrap();
            assert!(store.existing_db(service.service()).is_none());
        }
    }

    #[test]
    fn common_helpers_preserve_arrays_and_modeled_http_statuses() {
        let services = discover(
            Arc::new(Store::ephemeral()),
            Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services")),
        )
        .unwrap();
        let service = services
            .iter()
            .find(|s| s.service() == "servicediscovery")
            .unwrap();
        let ctx = context("123456789012", "us-east-1");
        let invoke = |operation: &str, input: Value| {
            let mut req = request(operation, input);
            req.headers[0].1 = format!("Route53AutoNaming_v20170314.{operation}");
            service.handle(&ctx, &req).unwrap()
        };
        let response = invoke("ListNamespaces", json!({}));
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            json!({"Namespaces":[]})
        );
        let response = invoke("GetNamespace", json!({"Id":"missing"}));
        assert_eq!(response.status, 404);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap()["__type"],
            "NamespaceNotFound"
        );
        assert_eq!(
            invoke("CreateHttpNamespace", json!({"Name":"example","Tags":{}})).status,
            400
        );
        assert_eq!(
            invoke("ListNamespaces", json!({})).body,
            b"{\"Namespaces\":[]}"
        );
        assert_eq!(
            invoke("CreateHttpNamespace", json!({"Name":"example","Tags":[]})).status,
            200
        );
        assert_eq!(
            invoke("CreateHttpNamespace", json!({"Name":"example"})).status,
            409
        );
    }

    #[test]
    fn custom_manifest_discovers_lazy_crud_service_without_rust_registration() {
        let store = Arc::new(Store::ephemeral());
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/lua-services");
        let services = discover(store.clone(), Some(bundle)).unwrap();
        let service = services.iter().find(|s| s.service() == "widgets").unwrap();
        assert!(store.existing_db("widgets").is_none());
        assert!(store.existing_db("organizations").is_none());
        let ctx = context("123456789012", "us-east-1");
        let invoke = |operation: &str, input: Value| {
            let mut req = request(operation, input);
            req.headers[0].1 = format!("Widgets_20261010.{operation}");
            assert!(service.claims_unsigned(&req));
            service.dispatch(&ctx, &req)
        };
        assert_eq!(
            invoke("ListWidgets", json!({})).unwrap(),
            json!({"Widgets":[]})
        );
        let value = json!({"Id":"hello","Value":{"nested":[null,[],{}]}});
        assert_eq!(
            invoke("CreateWidget", value.clone()).unwrap()["Widget"],
            value
        );
        assert_eq!(
            invoke("CreateWidget", value.clone()).unwrap_err().code,
            "AlreadyExistsException"
        );
        assert_eq!(
            invoke("GetWidget", json!({"Id":"hello"})).unwrap()["Widget"],
            value
        );
        assert_eq!(
            invoke("UpdateWidget", json!({"Id":"hello","Value":42})).unwrap()["Widget"]["Value"],
            42
        );
        let mut req = request("GetWidget", json!({"Id":"hello"}));
        req.headers[0].1 = "Widgets_20261010.GetWidget".into();
        assert_eq!(
            service
                .dispatch(&context("999999999999", "us-east-1"), &req)
                .unwrap_err()
                .code,
            "ResourceNotFoundException"
        );
        assert_eq!(
            service
                .dispatch(&context("123456789012", "eu-west-1"), &req)
                .unwrap_err()
                .code,
            "ResourceNotFoundException"
        );
        invoke("DeleteWidget", json!({"Id":"hello"})).unwrap();
        assert_eq!(
            invoke("ListWidgets", json!({})).unwrap(),
            json!({"Widgets":[]})
        );
        assert!(store.existing_db("organizations").is_none());
    }

    fn manifest(entry: Value) -> Value {
        json!({"version":1,"services":[entry]})
    }
    fn widget_entry() -> Value {
        json!({"service":"widgets","script":"widgets/service.luau","target_prefix":"Widgets.","json_version":"1.0"})
    }

    #[test]
    fn manifests_validate_routing_and_paths_without_loading_scripts() {
        let dir =
            std::env::temp_dir().join(format!("roto-manifest-{}", roto_core::ids::request_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("manifest.json");
        let valid = manifest(widget_entry());
        let mut invalid = vec![
            json!({"version":2,"services":[]}),
            json!({"version":1,"services":{}}),
            json!({"version":1,"services":[],"typo":true}),
            json!({"version":1,"common":"../common.luau","services":[]}),
            json!({"version":1,"services":[widget_entry(),widget_entry()]}),
        ];
        for (key, value) in [
            ("service", "../widgets"),
            ("target_prefix", "Widgets"),
            ("json_version", "2.0"),
            ("script", "../outside.luau"),
            ("script", "/outside.luau"),
            ("script", "C:/outside.luau"),
            ("typo", "true"),
        ] {
            let mut entry = widget_entry();
            entry[key] = json!(value);
            invalid.push(manifest(entry));
        }
        let mut other = widget_entry();
        other["service"] = json!("other");
        other["target_prefix"] = json!("Widgets.Nested.");
        invalid.push(json!({"version":1,"services":[widget_entry(),other]}));
        for invalid in invalid {
            std::fs::write(&path, invalid.to_string()).unwrap();
            assert!(
                discover(Arc::new(Store::ephemeral()), Some(dir.clone())).is_err(),
                "{invalid}"
            );
        }
        std::fs::write(&path, valid.to_string()).unwrap();
        let store = Arc::new(Store::ephemeral());
        let services = discover(store.clone(), Some(dir.clone())).unwrap();
        assert_eq!(services.len(), 1);
        let service = &services[0];
        assert!(store.existing_db("widgets").is_none());
        let mut req = request("GetWidget", json!({}));
        req.headers[0].1 = "Widgets.GetWidget".into();
        assert_eq!(
            service
                .handle(&context("123456789012", "us-east-1"), &req)
                .unwrap()
                .status,
            500
        );
        assert!(store.existing_db("widgets").is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn zip_manifest_is_lazy_and_scripts_are_read_without_extraction() {
        use std::io::Write;
        let path =
            std::env::temp_dir().join(format!("roto-bundle-{}.zip", roto_core::ids::request_id()));
        let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("manifest.json", options).unwrap();
        zip.write_all(manifest(widget_entry()).to_string().as_bytes())
            .unwrap();
        zip.start_file("widgets/service.luau", options).unwrap();
        zip.write_all(
            b"return { ListWidgets = function(ctx,input,db) return {Widgets=roto.array()} end }",
        )
        .unwrap();
        zip.finish().unwrap();
        let store = Arc::new(Store::ephemeral());
        let services = discover(store.clone(), Some(path.clone())).unwrap();
        assert!(store.existing_db("widgets").is_none());
        let mut req = request("ListWidgets", json!({}));
        req.headers[0].1 = "Widgets.ListWidgets".into();
        assert_eq!(
            services[0]
                .dispatch(&context("123456789012", "us-east-1"), &req)
                .unwrap(),
            json!({"Widgets":[]})
        );
        assert!(store.existing_db("widgets").is_some());
        drop(services);
        std::fs::remove_file(path).unwrap();
    }

    fn test_organizations(store: Arc<Store>, path: Option<PathBuf>) -> LuaService {
        test_service(store, "organizations", path)
    }

    fn context(account: &str, region: &str) -> RequestContext {
        RequestContext {
            account_id: account.into(),
            region: region.into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn request(operation: &str, input: Value) -> RawRequest {
        RawRequest {
            method: "POST".into(),
            path: "/".into(),
            headers: vec![(
                "x-amz-target".into(),
                format!("AWSOrganizationsV20161128.{operation}"),
            )],
            body: serde_json::to_vec(&input).unwrap(),
            ..Default::default()
        }
    }
    fn call(service: &LuaService, ctx: &RequestContext, operation: &str, input: Value) -> Value {
        let response = service.handle(ctx, &request(operation, input)).unwrap();
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response.status, 200, "{body}");
        body
    }

    #[test]
    fn lazy_global_state_hierarchy_pagination_and_reset() {
        let store = Arc::new(Store::ephemeral());
        let service = test_organizations(store.clone(), None);
        let ctx = context("123456789012", "us-east-1");
        service.reset().unwrap();
        assert!(store.existing_db("organizations").is_none());
        assert!(service.claims_unsigned(&request("CreateOrganization", json!({}))));
        let created = call(&service, &ctx, "CreateOrganization", json!({}));
        let other_region = context(&ctx.account_id, "eu-west-1");
        assert_eq!(
            call(&service, &other_region, "DescribeOrganization", json!({})),
            created
        );
        let roots = call(&service, &ctx, "ListRoots", json!({}));
        let root = roots["Roots"][0]["Id"].as_str().unwrap();
        assert_eq!(
            call(
                &service,
                &ctx,
                "ListOrganizationalUnitsForParent",
                json!({"ParentId":root})
            )["OrganizationalUnits"],
            json!([])
        );
        let first = call(
            &service,
            &ctx,
            "CreateOrganizationalUnit",
            json!({"ParentId":root,"Name":"Engineering","Tags":[{"Key":"team","Value":"eng"}]}),
        );
        let id = first["OrganizationalUnit"]["Id"].as_str().unwrap();
        call(
            &service,
            &ctx,
            "CreateOrganizationalUnit",
            json!({"ParentId":root,"Name":"Finance"}),
        );
        let page = call(
            &service,
            &ctx,
            "ListOrganizationalUnitsForParent",
            json!({"ParentId":root,"MaxResults":1}),
        );
        assert_eq!(page["OrganizationalUnits"].as_array().unwrap().len(), 1);
        let next = call(
            &service,
            &ctx,
            "ListOrganizationalUnitsForParent",
            json!({"ParentId":root,"MaxResults":1,"NextToken":page["NextToken"]}),
        );
        assert_ne!(
            page["OrganizationalUnits"][0]["Id"],
            next["OrganizationalUnits"][0]["Id"]
        );
        assert!(next.get("NextToken").is_none());
        call(
            &service,
            &ctx,
            "UpdateOrganizationalUnit",
            json!({"OrganizationalUnitId":id,"Name":"Platform"}),
        );
        assert_eq!(
            call(&service, &ctx, "ListParents", json!({"ChildId":id}))["Parents"][0]["Id"],
            root
        );
        assert_eq!(
            call(
                &service,
                &ctx,
                "ListTagsForResource",
                json!({"ResourceId":id})
            )["Tags"],
            json!([{"Key":"team","Value":"eng"}])
        );
        call(
            &service,
            &ctx,
            "UntagResource",
            json!({"ResourceId":id,"TagKeys":["team"]}),
        );
        assert_eq!(
            call(
                &service,
                &ctx,
                "ListTagsForResource",
                json!({"ResourceId":id})
            )["Tags"],
            json!([])
        );
        let child = call(
            &service,
            &ctx,
            "CreateOrganizationalUnit",
            json!({"ParentId":id,"Name":"Child"}),
        );
        let response = service
            .handle(
                &ctx,
                &request(
                    "DeleteOrganizationalUnit",
                    json!({"OrganizationalUnitId":id}),
                ),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap()["__type"],
            "OrganizationalUnitNotEmptyException"
        );
        call(
            &service,
            &ctx,
            "DeleteOrganizationalUnit",
            json!({"OrganizationalUnitId":child["OrganizationalUnit"]["Id"]}),
        );
        call(
            &service,
            &ctx,
            "DeleteOrganizationalUnit",
            json!({"OrganizationalUnitId":id}),
        );
        assert_eq!(
            call(&service, &ctx, "ListAccounts", json!({}))["Accounts"][0]["Id"],
            ctx.account_id
        );
        let outsider = context("999999999999", "us-east-1");
        assert_eq!(
            service
                .dispatch(&outsider, &request("DescribeOrganization", json!({})))
                .unwrap_err()
                .code,
            "AWSOrganizationsNotInUseException"
        );
        service.reset().unwrap();
        assert_eq!(
            service
                .dispatch(&ctx, &request("DescribeOrganization", json!({})))
                .unwrap_err()
                .code,
            "AWSOrganizationsNotInUseException"
        );
        call(&service, &ctx, "CreateOrganization", json!({}));
    }

    #[test]
    fn errors_roll_back_all_script_writes() {
        let service = test_organizations(Arc::new(Store::ephemeral()), None);
        let ctx = context("123456789012", "us-east-1");
        call(&service, &ctx, "CreateOrganization", json!({}));
        let roots = call(&service, &ctx, "ListRoots", json!({}));
        let root = &roots["Roots"][0]["Id"];
        let error = service.dispatch(&ctx,&request("CreateOrganizationalUnit",json!({"ParentId":root,"Name":"Rollback","Tags":[{"Key":"good","Value":"yes"},{"Key":"bad","Value":123}]}))).unwrap_err();
        assert_eq!(error.code, "InvalidInputException");
        assert_eq!(
            call(
                &service,
                &ctx,
                "ListOrganizationalUnitsForParent",
                json!({"ParentId":root})
            )["OrganizationalUnits"],
            json!([])
        );
        assert_eq!(
            service
                .dispatch(&ctx, &request("CreateAccount", json!({})))
                .unwrap_err()
                .status,
            501
        );
        assert_eq!(
            service
                .dispatch(&ctx, &request("ListRoots", json!({"NextToken":"bad"})))
                .unwrap_err()
                .code,
            "InvalidInputException"
        );
    }

    #[test]
    fn concurrent_first_requests_initialize_once() {
        let store = Arc::new(Store::ephemeral());
        let service = Arc::new(test_organizations(store.clone(), None));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let service = service.clone();
                std::thread::spawn(move || {
                    service.dispatch(
                        &context("123456789012", "us-east-1"),
                        &request("CreateOrganization", json!({})),
                    )
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| e.code == "AlreadyInOrganizationException")
        );
        let db = store.existing_db("organizations").unwrap();
        assert_eq!(
            db.read(|c| Ok(
                c.query_row("SELECT COUNT(*) FROM _roto_migrations", [], |r| r
                    .get::<_, u32>(0))?
            ))
            .unwrap(),
            1
        );
    }

    #[test]
    fn separate_bundle_loads_lazily_and_state_survives_restart() {
        let dir =
            std::env::temp_dir().join(format!("roto-lua-service-{}", roto_core::ids::request_id()));
        let bundle = dir.join("bundle");
        std::fs::create_dir_all(bundle.join("organizations")).unwrap();
        std::fs::write(bundle.join("manifest.json"), manifest(json!({"service":"organizations","script":"organizations/service.luau","target_prefix":"AWSOrganizationsV20161128.","json_version":"1.1"})).to_string()).unwrap();
        let ctx = context("123456789012", "us-east-1");
        let created;
        {
            let store = Arc::new(Store::open(dir.join("data"), Default::default()).unwrap());
            let service = test_organizations(store.clone(), Some(bundle.clone()));
            assert!(
                service
                    .dispatch(&ctx, &request("CreateOrganization", json!({})))
                    .is_err()
            );
            assert!(store.existing_db("organizations").is_none());
            std::fs::write(
                bundle.join("organizations/service.luau"),
                std::fs::read(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../services/organizations/service.luau"),
                )
                .unwrap(),
            )
            .unwrap();
            created = call(&service, &ctx, "CreateOrganization", json!({}));
        }
        let store = Arc::new(Store::open(dir.join("data"), Default::default()).unwrap());
        let service = test_organizations(store.clone(), Some(bundle));
        assert!(store.existing_db("organizations").is_none());
        assert_eq!(
            call(&service, &ctx, "DescribeOrganization", json!({})),
            created
        );
        drop(service);
        drop(store);
        let store = Arc::new(Store::open(dir.join("data"), Default::default()).unwrap());
        let service = test_organizations(store.clone(), None);
        service.reset().unwrap();
        assert!(service.runtime.lock().unwrap().is_none());
        assert_eq!(
            service
                .dispatch(&ctx, &request("DescribeOrganization", json!({})))
                .unwrap_err()
                .code,
            "AWSOrganizationsNotInUseException"
        );
        drop(service);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
