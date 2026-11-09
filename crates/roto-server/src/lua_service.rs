//! Lazy, trusted Lua services. Storage callbacks live only for one SQLite transaction.
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use base64::Engine;
use md5::Md5;
use sha1::Digest as Sha1Digest;
use sha2::{Digest, Sha256};

use mlua::{Lua, LuaSerdeExt, Table, Value as LuaValue};
use roto_core::clock;
use roto_core::rusqlite::{OptionalExtension, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{json_error, json_response};
use serde_json::{Value, json};

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: "CREATE TABLE documents (namespace TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(namespace,id))",
    },
    // Timers. `gen_no` changes whenever a key is rescheduled, so a running job can tell that its
    // handler replaced it. Times are virtual-clock milliseconds (see `roto_core::clock`).
    Migration {
        version: 2,
        sql: "CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL,
            region TEXT NOT NULL,
            key TEXT NOT NULL,
            run_at INTEGER NOT NULL,
            lease_until INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            gen_no INTEGER NOT NULL DEFAULT 0,
            payload TEXT NOT NULL DEFAULT 'null',
            UNIQUE(account, region, key));
            CREATE INDEX jobs_due ON jobs(run_at)",
    },
];

pub(crate) fn hash_text(text: &str, algorithm: &str) -> Option<String> {
    let digest = match algorithm {
        "MD5" => Md5::digest(text.as_bytes()).to_vec(),
        "SHA-1" => sha1::Sha1::digest(text.as_bytes()).to_vec(),
        "SHA-256" => Sha256::digest(text.as_bytes()).to_vec(),
        "SHA-384" => sha2::Sha384::digest(text.as_bytes()).to_vec(),
        "SHA-512" => sha2::Sha512::digest(text.as_bytes()).to_vec(),
        _ => return None,
    };
    Some(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// A claimed job is invisible to other claimers for this long; a crash mid-job retries after it.
const JOB_LEASE_MS: i64 = 30_000;
/// A job whose handler keeps failing is dropped after this many attempts.
const JOB_MAX_ATTEMPTS: i64 = 5;
/// Jobs one drain may run, so a handler that reschedules itself forever cannot hang a caller.
const DRAIN_BUDGET: usize = 10_000;

/// Every service handler, filled in once the server has built them all. Luau services reach
/// other services through it (`roto.call`, `roto.wire`).
pub type Peers = Arc<OnceLock<HashMap<&'static str, Weak<dyn ServiceHandler>>>>;

/// Deepest chain of service-to-service calls (a service calling a service calling ...).
const MAX_CALL_DEPTH: usize = 8;

thread_local! {
    /// Services whose Luau is running on this thread, outermost first. A service holds its VM
    /// lock while it runs, so calling one already on the stack would deadlock; it is refused.
    static CALL_STACK: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

struct StackGuard;
impl StackGuard {
    fn enter(service: &'static str) -> Result<Self, AwsError> {
        CALL_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.len() >= MAX_CALL_DEPTH {
                return Err(AwsError::internal(format!(
                    "service calls nested deeper than {MAX_CALL_DEPTH}: {} -> {service}",
                    stack.join(" -> ")
                )));
            }
            stack.push(service);
            Ok(StackGuard)
        })
    }
}
impl Drop for StackGuard {
    fn drop(&mut self) {
        CALL_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

type WireResponse = (u16, Vec<(String, String)>, Vec<u8>);
type WireArgs = (String, Vec<(String, String)>, Vec<u8>);

/// A failed call to another service, in the shape Luau sees it.
struct PeerError {
    status: u16,
    code: String,
    message: String,
}

impl PeerError {
    fn local(code: &str, message: String) -> Self {
        PeerError {
            status: 500,
            code: code.into(),
            message,
        }
    }
    /// Reads the status, error code and message out of an error response body (JSON or XML).
    fn from_response(status: u16, body: &[u8]) -> Self {
        let text = String::from_utf8_lossy(body);
        let (mut code, message) = if let Ok(json) = serde_json::from_str::<Value>(&text) {
            let field = |names: &[&str]| {
                names
                    .iter()
                    .find_map(|n| json.get(*n).and_then(Value::as_str))
                    .map(str::to_owned)
            };
            (
                field(&["__type", "code", "Code"]).unwrap_or_default(),
                field(&["message", "Message"]).unwrap_or_default(),
            )
        } else {
            let tag = |name: &str| {
                let open = format!("<{name}>");
                let start = text.find(&open)? + open.len();
                let end = text[start..].find(&format!("</{name}>"))? + start;
                Some(text[start..end].to_owned())
            };
            (
                tag("Code").unwrap_or_default(),
                tag("Message").unwrap_or_default(),
            )
        };
        if let Some((_, short)) = code.rsplit_once('#') {
            code = short.to_owned();
        }
        if code.is_empty() {
            code = format!("HttpStatus{status}");
        }
        PeerError {
            status,
            code,
            message,
        }
    }
    fn from_aws(error: &AwsError) -> Self {
        let text = error.to_string();
        PeerError {
            status: 500,
            code: "InternalFailure".into(),
            message: text,
        }
    }
    fn into_table(self, lua: &Lua) -> mlua::Result<Table> {
        let t = lua.create_table()?;
        t.set("ok", false)?;
        t.set("status", self.status)?;
        t.set("code", self.code)?;
        t.set("message", self.message)?;
        Ok(t)
    }
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.code, self.status, self.message)
    }
}

pub struct LuaService {
    definition: Definition,
    peers: Peers,
    store: Arc<Store>,
    bundle: Arc<Bundle>,
    runtime: Mutex<Option<Runtime>>,
}
struct Definition {
    service: &'static str,
    /// SigV4 service name when it differs from the service identity (see `Group`).
    signing: &'static str,
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
                "service"
                    | "signing_name"
                    | "script"
                    | "target_prefix"
                    | "json_version"
                    | "protocol"
                    | "model"
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
        let signing = match entry.get("signing_name") {
            None => name,
            Some(value) => value
                .as_str()
                .filter(|s| {
                    !s.is_empty()
                        && s.len() <= 64
                        && s.bytes()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                })
                .ok_or_else(|| invalid("signing_name must be a lowercase service name"))?,
        };
        if signing != name && !matches!(entry["protocol"].as_str(), Some("query" | "ec2")) {
            return Err(invalid("signing_name requires a query or ec2 protocol"));
        }
        let script = entry["script"]
            .as_str()
            .filter(|p| relative_path(p) && p.ends_with(".luau"))
            .ok_or_else(|| invalid("script must be a relative Lua script path"))?;
        let protocol = entry["protocol"].as_str().unwrap_or("json");
        let wire = match protocol {
            "json" => None,
            "rest-json" | "query" | "ec2" => {
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
            signing: Box::leak(signing.to_owned().into_boxed_str()),
            script: script.into(),
            target: target.into(),
            json_version: version.into(),
            common: common.clone(),
            wire,
        });
    }
    // Services may share a SigV4 signing name only when the query `Version` tells them apart.
    for (i, a) in definitions.iter().enumerate() {
        for b in &definitions[i + 1..] {
            if a.signing != b.signing {
                continue;
            }
            let version = |d: &Definition| {
                d.wire
                    .as_ref()
                    .and_then(|w| w.model["metadata"]["apiVersion"].as_str())
                    .map(str::to_owned)
            };
            let (va, vb) = (version(a), version(b));
            if va.is_none() || va == vb {
                return Err(invalid(
                    "services sharing a signing_name need distinct query API versions",
                ));
            }
        }
    }
    let peers = Peers::default();
    for definition in definitions {
        services.push(LuaService {
            definition,
            peers: peers.clone(),
            store: store.clone(),
            bundle: bundle.clone(),
            runtime: Mutex::new(None),
        });
    }
    Ok(services)
}

/// Lua services that share one SigV4 signing name (for example classic and v2 Elastic Load
/// Balancing). A request goes to the member whose query `Version` and action match; anything
/// else falls to the first member so errors keep their usual shape.
struct Group {
    signing: &'static str,
    members: Vec<Arc<LuaService>>,
}

impl Group {
    fn pick(&self, req: &RawRequest) -> &LuaService {
        self.members
            .iter()
            .find(|m| m.claims_unsigned(req))
            .unwrap_or(&self.members[0])
            .as_ref()
    }
}

impl ServiceHandler for Group {
    fn service(&self) -> &'static str {
        self.signing
    }
    fn operation_name(&self, req: &RawRequest) -> Option<String> {
        self.pick(req).operation_name(req)
    }
    fn claims_unsigned(&self, req: &RawRequest) -> bool {
        self.members.iter().any(|m| m.claims_unsigned(req))
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        self.pick(req).handle(ctx, req)
    }
    fn reset(&self) -> Result<(), AwsError> {
        self.members.iter().try_for_each(|m| m.reset())
    }
}

/// Registers discovered services by SigV4 signing name, grouping those that share one. The
/// returned [`Jobs`] runs the services' timers; call [`Jobs::start`] to begin.
pub fn into_handlers(services: Vec<LuaService>) -> (Vec<Arc<dyn ServiceHandler>>, Arc<Jobs>) {
    let services: Vec<Arc<LuaService>> = services.into_iter().map(Arc::new).collect();
    let mut groups: Vec<Group> = Vec::new();
    for service in &services {
        let signing = service.definition.signing;
        match groups.iter_mut().find(|g| g.signing == signing) {
            Some(group) => group.members.push(service.clone()),
            None => groups.push(Group {
                signing,
                members: vec![service.clone()],
            }),
        }
    }
    let handlers = groups
        .into_iter()
        .map(|g| {
            if g.members.len() == 1 && g.members[0].definition.service == g.signing {
                g.members[0].clone() as Arc<dyn ServiceHandler>
            } else {
                Arc::new(g)
            }
        })
        .collect();
    let jobs = Arc::new(Jobs {
        services,
        gate: Mutex::new(()),
    });
    (handlers, jobs)
}

/// Runs the timers of every Lua service: a worker thread polls for due jobs, and [`Jobs::drain`]
/// lets a test run everything that is due right now (after advancing the virtual clock).
///
/// A job is a row written by `db.schedule(key, ...)` in the same transaction as the handler's
/// document writes. When it is due the service's `__job(ctx, job, db)` runs in a new
/// transaction that also deletes the job, so a job that returns is done and a job that raises
/// is rolled back and retried with backoff. A crash mid-job retries once its lease expires:
/// delivery is at least once.
#[derive(Default)]
pub struct Jobs {
    services: Vec<Arc<LuaService>>,
    /// Held for a whole pass, so `drain` waits for a pass the worker already started.
    gate: Mutex<()>,
}

impl Jobs {
    /// Summarize persisted bundle documents without starting a service or creating a database.
    pub fn document_summary(&self) -> Result<Vec<(String, String, Vec<String>)>, AwsError> {
        fn rows(c: &roto_core::rusqlite::Connection) -> Result<Vec<(String, String)>, AwsError> {
            let mut statement =
                c.prepare("SELECT namespace, id FROM documents ORDER BY namespace, id")?;
            Ok(statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?)
        }
        let mut result = Vec::new();
        for service in &self.services {
            let name = service.definition.service;
            let records = if let Some(db) = service.store.existing_db(name) {
                db.read(rows)?
            } else if let Some(path) = service
                .store
                .data_dir()
                .map(|dir| dir.join(format!("{name}.db")))
                .filter(|path| path.is_file())
            {
                let connection = roto_core::rusqlite::Connection::open_with_flags(
                    path,
                    roto_core::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | roto_core::rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                rows(&connection)?
            } else {
                Vec::new()
            };
            let mut grouped = std::collections::BTreeMap::<String, Vec<String>>::new();
            for (namespace, id) in records {
                grouped.entry(namespace).or_default().push(id);
            }
            result.extend(
                grouped
                    .into_iter()
                    .map(|(namespace, ids)| (name.to_owned(), namespace, ids)),
            );
        }
        Ok(result)
    }
    pub fn has_service(&self, service: &str) -> bool {
        self.services
            .iter()
            .any(|s| s.definition.service == service)
    }
    /// Invoke a decoded bundle operation through the service's transactional dispatcher.
    pub fn call_decoded(
        &self,
        service: &str,
        ctx: &RequestContext,
        operation: &str,
        input: Value,
    ) -> Option<Result<Value, AwsError>> {
        self.services
            .iter()
            .find(|s| s.definition.service == service)
            .map(|s| {
                if let Some(wire) = &s.definition.wire {
                    wire.validate_decoded(operation, &input)?;
                    s.invoke(ctx, operation, input)
                } else {
                    let body = serde_json::to_vec(&input)
                        .map_err(|e| AwsError::internal(e.to_string()))?;
                    s.dispatch(
                        ctx,
                        &RawRequest {
                            method: "POST".into(),
                            path: "/".into(),
                            headers: vec![(
                                "x-amz-target".into(),
                                format!("{}{operation}", s.definition.target),
                            )],
                            body,
                            ..Default::default()
                        },
                    )
                }
            })
    }

    /// Gives the Lua services the handlers of every service, so they can call each other.
    pub fn connect(&self, handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>) {
        if let Some(service) = self.services.first() {
            let weak = handlers
                .into_iter()
                .map(|(name, handler)| (name, Arc::downgrade(&handler)))
                .collect();
            let _ = service.peers.set(weak);
        }
    }

    pub fn start(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("roto-lua-jobs".into())
            .spawn(move || {
                while let Some(jobs) = weak.upgrade() {
                    {
                        let _gate = jobs.gate.lock().unwrap();
                        jobs.pass(&mut 1000);
                    }
                    drop(jobs);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            })
            .expect("Lua job worker");
    }

    /// Runs due jobs until none are left, including jobs that earlier ones scheduled for a time
    /// that has already arrived. Returns how many ran and whether it finished (`false` means the
    /// budget ran out, for example a job that reschedules itself with no delay).
    pub fn drain(&self) -> (usize, bool) {
        let _gate = self.gate.lock().unwrap();
        self.drain_locked(&mut { DRAIN_BUDGET })
    }

    fn drain_locked(&self, budget: &mut usize) -> (usize, bool) {
        let mut total = 0;
        loop {
            let ran = self.pass(budget);
            total += ran;
            if ran == 0 {
                return (total, true);
            }
            if *budget == 0 {
                return (total, false);
            }
        }
    }

    /// Moves the virtual clock forward by `ms`, stopping at each job's due time on the way so
    /// the job runs at that time and anything it schedules is measured from it. A chain of
    /// one-hour timers therefore completes inside a single long advance, and every timestamp
    /// the jobs write is the time they would really have run.
    pub fn advance(&self, ms: i64) -> Result<(usize, bool), &'static str> {
        if ms < 0 {
            return Err("the clock can only move forward");
        }
        let _gate = self.gate.lock().unwrap();
        let target = clock::now_ms().saturating_add(ms);
        let mut budget = DRAIN_BUDGET;
        let mut total = 0;
        loop {
            let next = self
                .services
                .iter()
                .filter_map(|service| match service.next_job_ms() {
                    Ok(next) => next,
                    Err(e) => {
                        eprintln!("Lua service {} jobs: {e}", service.definition.service);
                        None
                    }
                })
                .min();
            let Some(next) = next.filter(|next| *next <= target) else {
                break;
            };
            clock::advance_to_ms(next);
            let (ran, finished) = self.drain_locked(&mut budget);
            total += ran;
            if !finished {
                return Ok((total, false));
            }
            if ran == 0 {
                break; // due but unclaimable: do not spin
            }
        }
        clock::advance_to_ms(target);
        let (ran, finished) = self.drain_locked(&mut budget);
        Ok((total + ran, finished))
    }

    fn pass(&self, budget: &mut usize) -> usize {
        let mut ran = 0;
        for service in &self.services {
            while *budget > 0 {
                match service.run_next_job() {
                    Ok(true) => {
                        ran += 1;
                        *budget -= 1;
                    }
                    Ok(false) => break,
                    Err(e) => {
                        eprintln!("Lua service {} jobs: {e}", service.definition.service);
                        break;
                    }
                }
            }
        }
        ran
    }
}

struct Claimed {
    id: i64,
    generation: i64,
    account: String,
    region: String,
    key: String,
    payload: String,
    attempts: i64,
    run_at: i64,
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
        // Services see the virtual clock, so advancing it moves every timestamp they write.
        api.set(
            "now",
            lua.create_function(|_, ()| Ok(clock::now_ms() as f64 / 1000.0))
                .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        let os: Table = lua.globals().get("os").map_err(lua_error)?;
        let real_time: mlua::Function = os.get("time").map_err(lua_error)?;
        os.set(
            "time",
            lua.create_function(move |_, date: Option<Table>| match date {
                Some(date) => real_time.call::<f64>(date),
                None => Ok(clock::now_secs() as f64),
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        let real_date: mlua::Function = os.get("date").map_err(lua_error)?;
        os.set(
            "date",
            lua.create_function(move |_, (format, at): (Option<String>, Option<f64>)| {
                real_date.call::<LuaValue>((format, at.unwrap_or(clock::now_secs() as f64)))
            })
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
            "hash",
            lua.create_function(|_, (text, algorithm): (String, String)| {
                crate::lua_service::hash_text(&text, &algorithm).ok_or_else(|| {
                    mlua::Error::external(format!("unsupported hash algorithm {algorithm}"))
                })
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
        api.set(
            "encode_base64",
            lua.create_function(|_, value: mlua::LuaString| {
                Ok(base64::engine::general_purpose::STANDARD.encode(value.as_bytes()))
            })
            .map_err(lua_error)?,
        )
        .map_err(lua_error)?;
        // Compact JSON text of a Luau value; whole numbers print as integers.
        api.set(
            "json_encode",
            lua.create_function(|lua, value: LuaValue| {
                let value: Value = lua.from_value(value)?;
                serde_json::to_string(&integral_numbers(value)).map_err(mlua::Error::external)
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
        // `__`-prefixed entries (such as `__job`) are internal and not reachable over HTTP.
        if operation.starts_with("__") {
            return Err(AwsError::not_implemented(
                self.definition.service,
                operation,
            ));
        }
        self.execute(ctx, operation, input, |_| Ok(()))
    }

    /// Runs one module function in a transaction. `finish` runs in the same transaction after
    /// the function returns, so its writes commit or roll back together with the function's.
    fn execute(
        &self,
        ctx: &RequestContext,
        operation: &str,
        input: Value,
        finish: impl FnOnce(&roto_core::rusqlite::Transaction<'_>) -> Result<(), AwsError>,
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
        let service_name = self.definition.service;
        let _stack = StackGuard::enter(service_name)?;
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
                db.set("schedule", scope.create_function(|lua, (key, options): (String, Option<Table>)| {
                    if key.is_empty() {
                        return Err(job_error("a job key must not be empty"));
                    }
                    let mut run_at = clock::now_ms();
                    let mut payload = Value::Null;
                    if let Some(options) = options {
                        let at: Option<f64> = options.get("at")?;
                        let delay: Option<f64> = options.get("delay")?;
                        if at.is_some() && delay.is_some() {
                            return Err(job_error("schedule takes `at` or `delay`, not both"));
                        }
                        if let Some(delay) = delay {
                            if !delay.is_finite() || delay < 0.0 {
                                return Err(job_error("`delay` must be a non-negative number of seconds"));
                            }
                            run_at += (delay * 1000.0).round() as i64;
                        }
                        if let Some(at) = at {
                            if !at.is_finite() {
                                return Err(job_error("`at` must be a Unix time in seconds"));
                            }
                            run_at = (at * 1000.0).round() as i64;
                        }
                        payload = lua.from_value(options.get::<LuaValue>("payload")?)?;
                    }
                    let payload = serde_json::to_string(&payload).map_err(mlua::Error::external)?;
                    tx.execute(
                        "INSERT INTO jobs(account,region,key,run_at,payload) VALUES (?1,?2,?3,?4,?5) \
                         ON CONFLICT(account,region,key) DO UPDATE SET run_at=excluded.run_at, \
                         payload=excluded.payload, lease_until=0, attempts=0, gen_no=gen_no+1",
                        params![ctx.account_id, ctx.region, key, run_at, payload],
                    ).map_err(mlua::Error::external)?;
                    Ok(())
                })?)?;
                db.set("cancel", scope.create_function(|_, key: String| {
                    tx.execute("DELETE FROM jobs WHERE account=?1 AND region=?2 AND key=?3", params![ctx.account_id, ctx.region, key]).map_err(mlua::Error::external)?;
                    Ok(())
                })?)?;
                let context = lua.to_value(&json!({"account_id":ctx.account_id,"region":ctx.region,"request_id":ctx.request_id}))?;
                let roto: Table = lua.globals().get("roto")?;
                self.install_peer_calls(&roto, scope, ctx)?;
                let output = function.call::<LuaValue>((context, lua.to_value(&input)?, db));
                for name in ["call", "try_call", "wire", "try_wire"] {
                    roto.set(name, LuaValue::Nil)?;
                }
                lua.from_value::<Value>(output?)
            });
            lua.remove_interrupt();
            let value = result.map_err(lua_error)?;
            finish(tx)?;
            Ok(value)
        })
    }

    /// Adds `roto.call`, `roto.try_call`, `roto.wire` and `roto.try_wire` for the length of one
    /// execution. They run the other service's handler in this thread, in its own transaction:
    /// there is no atomicity across services, and a service cannot call itself or any service
    /// already running on this thread's stack.
    fn install_peer_calls<'scope, 'env>(
        &'env self,
        roto: &Table,
        scope: &'scope mlua::Scope<'scope, 'env>,
        ctx: &'env RequestContext,
    ) -> mlua::Result<()> {
        let me = self.definition.service;
        let peers = &self.peers;
        let handler = move |service: &str| -> Result<Arc<dyn ServiceHandler>, PeerError> {
            let handler = peers
                .get()
                .and_then(|map| map.get(service))
                .and_then(Weak::upgrade)
                .ok_or_else(|| {
                    PeerError::local(
                        "ServiceUnavailable",
                        format!("Service unavailable: {service}"),
                    )
                })?;
            let busy = CALL_STACK.with(|stack| stack.borrow().contains(&handler.service()));
            if busy || service == me {
                return Err(PeerError::local(
                    "ReentrantCall",
                    format!("{me} cannot call {service}: it is already running on this call stack"),
                ));
            }
            Ok(handler)
        };
        let call =
            move |service: &str, operation: &str, input: Value| -> Result<Value, PeerError> {
                let handler = handler(service)?;
                if !crate::aws_client::supports(service) {
                    return Err(PeerError::local(
                        "UnsupportedService",
                        format!("Unknown service for roto.call: {service}; use roto.wire"),
                    ));
                }
                crate::aws_client::call(handler.as_ref(), ctx, service, operation, &input).map_err(
                    |message| match message.strip_prefix("AWS request failed (") {
                        Some(rest) => {
                            let (status, body) = rest.split_once("): ").unwrap_or(("500", rest));
                            PeerError::from_response(status.parse().unwrap_or(500), body.as_bytes())
                        }
                        None => PeerError::local("InvalidRequest", message),
                    },
                )
            };
        let wire = move |service: &str,
                         method: String,
                         path: String,
                         query: String,
                         headers: Vec<(String, String)>,
                         body: Vec<u8>|
              -> Result<WireResponse, PeerError> {
            let handler = handler(service)?;
            let response = handler
                .handle(
                    ctx,
                    &RawRequest {
                        method,
                        path,
                        query,
                        headers,
                        body,
                    },
                )
                .map_err(|e| PeerError::from_aws(&e))?;
            if response.status >= 400 {
                return Err(PeerError::from_response(response.status, &response.body));
            }
            Ok((response.status, response.headers, response.body))
        };
        let call = Arc::new(call);
        let wire = Arc::new(wire);

        let input_of = |lua: &Lua, input: LuaValue| -> mlua::Result<Value> {
            if input.is_nil() {
                Ok(json!({}))
            } else {
                lua.from_value(input)
            }
        };
        let wire_args = |lua: &Lua, opts: Option<Table>| -> mlua::Result<WireArgs> {
            let Some(opts) = opts else {
                return Ok((String::new(), Vec::new(), Vec::new()));
            };
            let headers: std::collections::BTreeMap<String, String> =
                match opts.get::<LuaValue>("headers")? {
                    LuaValue::Nil => Default::default(),
                    value => lua.from_value(value)?,
                };
            let body = match opts.get::<LuaValue>("body")? {
                LuaValue::Nil => Vec::new(),
                LuaValue::String(s) => s.as_bytes().to_vec(),
                value => serde_json::to_vec(&lua.from_value::<Value>(value)?)
                    .map_err(mlua::Error::external)?,
            };
            Ok((
                opts.get::<Option<String>>("query")?.unwrap_or_default(),
                headers
                    .into_iter()
                    .map(|(k, v)| (k.to_ascii_lowercase(), v))
                    .collect(),
                body,
            ))
        };
        let wire_result = |lua: &Lua, (status, headers, body): WireResponse| {
            let headers: serde_json::Map<String, Value> = headers
                .into_iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), Value::String(v)))
                .collect();
            lua.to_value(&json!({
                "ok": true,
                "status": status,
                "headers": headers,
                "body": String::from_utf8_lossy(&body),
            }))
        };

        let c = call.clone();
        roto.set(
            "call",
            scope.create_function(
                move |lua, (service, operation, input): (String, String, LuaValue)| {
                    let input = input_of(lua, input)?;
                    let value = c(&service, &operation, input)
                        .map_err(|e| mlua::Error::external(e.to_string()))?;
                    lua.to_value(&value)
                },
            )?,
        )?;
        let c = call;
        roto.set(
            "try_call",
            scope.create_function(
                move |lua, (service, operation, input): (String, String, LuaValue)| {
                    let input = input_of(lua, input)?;
                    match c(&service, &operation, input) {
                        Ok(value) => lua.to_value(&json!({"ok": true, "value": value})),
                        Err(e) => Ok(LuaValue::Table(e.into_table(lua)?)),
                    }
                },
            )?,
        )?;
        let w = wire.clone();
        roto.set("wire", scope.create_function(move |lua, (service, method, path, opts): (String, String, String, Option<Table>)| {
            let (query, headers, body) = wire_args(lua, opts)?;
            let out = w(&service, method, path, query, headers, body).map_err(|e| mlua::Error::external(e.to_string()))?;
            wire_result(lua, out)
        })?)?;
        let w = wire;
        roto.set("try_wire", scope.create_function(move |lua, (service, method, path, opts): (String, String, String, Option<Table>)| {
            let (query, headers, body) = wire_args(lua, opts)?;
            match w(&service, method, path, query, headers, body) {
                Ok(out) => wire_result(lua, out),
                Err(e) => Ok(LuaValue::Table(e.into_table(lua)?)),
            }
        })?)?;
        Ok(())
    }

    /// The service's database if it already exists; never creates one.
    fn existing_db(&self) -> Result<Option<Arc<Db>>, AwsError> {
        if let Some(runtime) = self.runtime.lock().unwrap().as_ref() {
            return Ok(Some(runtime.db.clone()));
        }
        let service = self.definition.service;
        if let Some(db) = self.store.existing_db(service) {
            return Ok(Some(db));
        }
        if self
            .store
            .data_dir()
            .is_some_and(|dir| dir.join(format!("{service}.db")).is_file())
        {
            return Ok(Some(self.store.db(service, MIGRATIONS)?));
        }
        Ok(None)
    }

    /// When the next job could run (due time or lease expiry), without loading the service.
    fn next_job_ms(&self) -> Result<Option<i64>, AwsError> {
        let Some(db) = self.existing_db()? else {
            return Ok(None);
        };
        db.read(|conn| {
            Ok(
                conn.query_row("SELECT MIN(MAX(run_at, lease_until)) FROM jobs", [], |r| {
                    r.get::<_, Option<i64>>(0)
                })?,
            )
        })
    }

    /// Claims and runs the earliest due job. Returns whether there was one.
    fn run_next_job(&self) -> Result<bool, AwsError> {
        let Some(db) = self.existing_db()? else {
            return Ok(false);
        };
        let now = clock::now_ms();
        let job = db.transaction(|tx| {
            let job = tx
                .query_row(
                    "SELECT id,gen_no,account,region,key,payload,attempts,run_at FROM jobs \
                     WHERE run_at<=?1 AND lease_until<=?1 ORDER BY run_at,id LIMIT 1",
                    [now],
                    |r| {
                        Ok(Claimed {
                            id: r.get(0)?,
                            generation: r.get(1)?,
                            account: r.get(2)?,
                            region: r.get(3)?,
                            key: r.get(4)?,
                            payload: r.get(5)?,
                            attempts: r.get::<_, i64>(6)? + 1,
                            run_at: r.get(7)?,
                        })
                    },
                )
                .optional()?;
            if let Some(job) = &job {
                tx.execute(
                    "UPDATE jobs SET lease_until=?1, attempts=?2 WHERE id=?3",
                    params![now + JOB_LEASE_MS, job.attempts, job.id],
                )?;
            }
            Ok(job)
        })?;
        let Some(job) = job else {
            return Ok(false);
        };
        let ctx = RequestContext {
            account_id: job.account.clone(),
            region: job.region.clone(),
            access_key: None,
            request_id: roto_core::ids::request_id(),
            base_url: String::new(),
        };
        let input = json!({
            "key": job.key,
            "payload": serde_json::from_str::<Value>(&job.payload).unwrap_or(Value::Null),
            "attempts": job.attempts,
            "run_at": job.run_at as f64 / 1000.0,
        });
        let (id, generation) = (job.id, job.generation);
        let outcome = self.execute(&ctx, "__job", input, |tx| {
            // Gone unless the handler rescheduled this key, which bumps `gen_no` and keeps the row.
            tx.execute(
                "DELETE FROM jobs WHERE id=?1 AND gen_no=?2",
                params![id, generation],
            )?;
            Ok(())
        });
        if let Err(error) = outcome {
            let service = self.definition.service;
            let give_up = job.attempts >= JOB_MAX_ATTEMPTS;
            if give_up {
                eprintln!(
                    "Lua service {service} dropped job {} after {} attempts: {error}",
                    job.key, job.attempts
                );
            } else {
                eprintln!(
                    "Lua service {service} job {} failed (attempt {}): {error}",
                    job.key, job.attempts
                );
            }
            let retry_at = clock::now_ms() + (1000_i64 << job.attempts.min(6)).min(60_000);
            db.transaction(|tx| {
                if give_up {
                    tx.execute(
                        "DELETE FROM jobs WHERE id=?1 AND gen_no=?2",
                        params![id, generation],
                    )?;
                } else {
                    tx.execute(
                        "UPDATE jobs SET lease_until=0, run_at=?1 WHERE id=?2 AND gen_no=?3",
                        params![retry_at, id, generation],
                    )?;
                }
                Ok(())
            })?;
        }
        Ok(true)
    }
}

pub(crate) fn integral_numbers(value: Value) -> Value {
    match value {
        Value::Number(n) => match n.as_f64() {
            Some(f) if n.is_f64() && f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 => {
                json!(f as i64)
            }
            _ => Value::Number(n),
        },
        Value::Array(items) => Value::Array(items.into_iter().map(integral_numbers).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, integral_numbers(v)))
                .collect(),
        ),
        other => other,
    }
}

fn job_error(message: &str) -> mlua::Error {
    mlua::Error::external(AwsError::sender(400, "InvalidParameterException", message))
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
        if let Some(db) = self.existing_db()? {
            db.transaction(|tx| {
                tx.execute("DELETE FROM documents", [])?;
                tx.execute("DELETE FROM jobs", [])?;
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
    fn document_summary_reads_existing_persistent_stores_without_initializing_services() {
        let dir =
            std::env::temp_dir().join(format!("roto-summary-{}", roto_core::ids::request_id()));
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/lua-services");
        {
            let store = Store::open(&dir, Default::default()).unwrap();
            let db = store.db("widgets", MIGRATIONS).unwrap();
            db.transaction(|tx| {
                tx.execute(
                    "INSERT INTO documents(namespace,id,body) VALUES (?1,?2,?3)",
                    params![
                        "123456789012/us-east-1/widgets",
                        "widget-1",
                        r#"{"secret":"not-in-summary"}"#
                    ],
                )?;
                Ok(())
            })
            .unwrap();
        }
        let store = Arc::new(Store::open(&dir, Default::default()).unwrap());
        let (_, jobs) = into_handlers(discover(store.clone(), Some(bundle)).unwrap());
        assert!(store.existing_db("widgets").is_none());
        let summary = jobs.document_summary().unwrap();
        assert_eq!(
            summary,
            vec![(
                "widgets".into(),
                "123456789012/us-east-1/widgets".into(),
                vec!["widget-1".into()]
            )]
        );
        assert!(store.existing_db("widgets").is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn services_are_optional_and_directory_manifest_is_lazy() {
        let store = Arc::new(Store::ephemeral());
        assert!(discover(store.clone(), None).unwrap().is_empty());
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services");
        let services = discover(store.clone(), Some(bundle)).unwrap();
        assert_eq!(services.len(), 17);
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

    fn form_request(body: &str) -> RawRequest {
        RawRequest {
            method: "POST".into(),
            path: "/".into(),
            headers: vec![(
                "content-type".into(),
                "application/x-www-form-urlencoded".into(),
            )],
            body: body.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn services_sharing_a_signing_name_dispatch_on_the_query_version() {
        let store = Arc::new(Store::ephemeral());
        let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../services");
        let (handlers, _jobs) = into_handlers(discover(store, Some(bundle)).unwrap());
        assert_eq!(
            handlers
                .iter()
                .filter(|h| h.service() == "elasticloadbalancing")
                .count(),
            1
        );
        let group = handlers
            .iter()
            .find(|h| h.service() == "elasticloadbalancing")
            .unwrap();
        let ctx = context("123456789012", "us-east-1");
        let classic = form_request("Action=DescribeLoadBalancers&Version=2012-06-01");
        let v2 = form_request("Action=DescribeLoadBalancers&Version=2015-12-01");
        let classic_body = String::from_utf8(group.handle(&ctx, &classic).unwrap().body).unwrap();
        let v2_body = String::from_utf8(group.handle(&ctx, &v2).unwrap().body).unwrap();
        assert!(
            classic_body.contains("<LoadBalancerDescriptions>"),
            "{classic_body}"
        );
        assert!(v2_body.contains("<LoadBalancers>"), "{v2_body}");
        // Both claim unsigned calls by version; anything else falls to the first member.
        assert!(group.claims_unsigned(&classic) && group.claims_unsigned(&v2));
        assert!(!group.claims_unsigned(&form_request("Action=DescribeLoadBalancers&Version=1")));
    }

    #[test]
    fn signing_names_must_be_distinguishable_by_version() {
        let dir = std::env::temp_dir().join(format!("roto-sign-{}", roto_core::ids::request_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let model = |version: &str| json!({"metadata":{"apiVersion":version,"protocol":"query"},"operations":{},"shapes":{}});
        std::fs::write(dir.join("a.json"), model("2012-06-01").to_string()).unwrap();
        std::fs::write(dir.join("b.json"), model("2012-06-01").to_string()).unwrap();
        std::fs::write(dir.join("c.json"), model("2015-12-01").to_string()).unwrap();
        let entry = |name: &str, model: &str| json!({"service":name,"signing_name":"shared","protocol":"query","script":"s.luau","model":model});
        let write = |entries: Vec<Value>| {
            std::fs::write(
                dir.join("manifest.json"),
                json!({"version":1,"services":entries}).to_string(),
            )
            .unwrap();
        };
        write(vec![entry("one", "a.json"), entry("two", "b.json")]);
        assert!(discover(Arc::new(Store::ephemeral()), Some(dir.clone())).is_err());
        write(vec![entry("one", "a.json"), entry("two", "c.json")]);
        assert_eq!(
            discover(Arc::new(Store::ephemeral()), Some(dir.clone()))
                .unwrap()
                .len(),
            2
        );
        // A JSON service cannot alias another signing name.
        let mut json_entry = widget_entry();
        json_entry["signing_name"] = json!("other");
        write(vec![json_entry]);
        assert!(discover(Arc::new(Store::ephemeral()), Some(dir.clone())).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ec2_protocol_capitalizes_request_names_and_uses_its_own_envelopes() {
        let store = Arc::new(Store::ephemeral());
        let service = test_service(store, "ec2", None);
        let ctx = context("123456789012", "us-east-1");
        let call = |body: &str| {
            let response = service.handle(&ctx, &form_request(body)).unwrap();
            (response.status, String::from_utf8(response.body).unwrap())
        };
        let (status, run) = call(
            "Action=RunInstances&Version=2016-11-15&ImageId=ami-1&MinCount=1&MaxCount=1\
             &TagSpecification.1.ResourceType=instance&TagSpecification.1.Tag.1.Key=Name&TagSpecification.1.Tag.1.Value=n",
        );
        assert_eq!(status, 200, "{run}");
        assert!(
            run.contains("<requestId>") && !run.contains("ResponseMetadata"),
            "{run}"
        );
        assert!(
            run.contains("<instancesSet><item>") && run.contains("<instanceId>i-"),
            "{run}"
        );
        let (_, described) = call(
            "Action=DescribeInstances&Version=2016-11-15&Filter.1.Name=tag%3AName&Filter.1.Value.1=n",
        );
        assert!(described.contains("<reservationSet><item>"), "{described}");
        assert!(
            described.contains("<tagSet><item><key>Name</key><value>n</value></item></tagSet>"),
            "{described}"
        );
        let (status, error) = call(
            "Action=DescribeInstances&Version=2016-11-15&Filter.1.Name=bogus&Filter.1.Value.1=x",
        );
        assert_eq!(status, 400);
        assert!(
            error.starts_with("<Response><Errors><Error><Code>InvalidParameterValue</Code>"),
            "{error}"
        );
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
            MIGRATIONS.len() as u32
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

    const TICKER: &str = r#"
local M = {}
function M.Start(ctx, input, db)
    db.schedule("tick", {delay = input.delay, payload = {left = input.count, fail = input.fail}})
    return {}
end
function M.Cancel(ctx, input, db) db.cancel("tick") return {} end
function M.Ticks(ctx, input, db) return {Ticks = db.list("ticks")} end
function M.__job(ctx, job, db)
    if job.payload.fail then error("boom") end
    db.put("ticks", string.format("%03d", #db.list("ticks")), {
        at = os.time(), left = job.payload.left, account = ctx.account_id,
        region = ctx.region, attempts = job.attempts,
    })
    if job.payload.left > 1 then
        db.schedule(job.key, {delay = 60, payload = {left = job.payload.left - 1}})
    end
    return {}
end
return M
"#;

    fn ticker() -> (Arc<dyn ServiceHandler>, Arc<Jobs>, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("roto-ticker-{}", roto_core::ids::request_id()));
        std::fs::create_dir_all(dir.join("ticker")).unwrap();
        std::fs::write(
            dir.join("manifest.json"),
            manifest(json!({"service":"ticker","script":"ticker/service.luau","target_prefix":"Ticker.","json_version":"1.0"}))
                .to_string(),
        )
        .unwrap();
        std::fs::write(dir.join("ticker/service.luau"), TICKER).unwrap();
        let services = discover(Arc::new(Store::ephemeral()), Some(dir.clone())).unwrap();
        let (mut handlers, jobs) = into_handlers(services);
        (handlers.remove(0), jobs, dir)
    }

    fn ticker_call(
        handler: &Arc<dyn ServiceHandler>,
        ctx: &RequestContext,
        operation: &str,
        input: Value,
    ) -> (u16, Value) {
        let mut req = request(operation, input);
        req.headers[0].1 = format!("Ticker.{operation}");
        let response = handler.handle(ctx, &req).unwrap();
        (
            response.status,
            serde_json::from_slice(&response.body).unwrap(),
        )
    }

    #[test]
    fn jobs_run_with_their_scope_cancel_reset_and_stay_internal() {
        let (handler, jobs, dir) = ticker();
        let ctx = context("111122223333", "eu-west-1");
        // `__`-prefixed functions are not reachable over HTTP.
        assert_ne!(ticker_call(&handler, &ctx, "__job", json!({})).0, 200);
        // The job runs as the account and region that scheduled it.
        ticker_call(&handler, &ctx, "Start", json!({"delay":0,"count":1}));
        assert_eq!(jobs.drain(), (1, true));
        let (_, body) = ticker_call(&handler, &ctx, "Ticks", json!({}));
        assert_eq!(body["Ticks"][0]["account"], "111122223333");
        assert_eq!(body["Ticks"][0]["region"], "eu-west-1");
        assert_eq!(body["Ticks"][0]["attempts"], 1);
        // Another account's key does not collide, and cancel removes only its own job.
        let other = context("444455556666", "eu-west-1");
        ticker_call(&handler, &ctx, "Start", json!({"delay":0,"count":1}));
        ticker_call(&handler, &other, "Start", json!({"delay":0,"count":1}));
        ticker_call(&handler, &ctx, "Cancel", json!({}));
        assert_eq!(jobs.drain(), (1, true));
        // Reset drops pending jobs along with documents.
        ticker_call(&handler, &ctx, "Start", json!({"delay":0,"count":1}));
        handler.reset().unwrap();
        assert_eq!(jobs.drain(), (0, true));
        // Bad scheduling arguments are client errors.
        let (status, _) = ticker_call(&handler, &ctx, "Start", json!({"delay":-1,"count":1}));
        assert_eq!(status, 400);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn services_encode_json_with_integral_numbers_and_nulls() {
        let dir = std::env::temp_dir().join(format!("roto-json-{}", roto_core::ids::request_id()));
        std::fs::create_dir_all(dir.join("enc")).unwrap();
        std::fs::write(
            dir.join("manifest.json"),
            json!({"version":1,"services":[{"service":"enc","script":"enc/service.luau","target_prefix":"Enc.","json_version":"1.0"}]}).to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.join("enc/service.luau"),
            r#"
local M = {}
function M.Encode(ctx, input, db)
    return {
        Text = roto.json_encode({a = roto.array({1, 2.5}), b = roto.null}),
        Empty = roto.json_encode(roto.array({})),
        Round = roto.json_decode(roto.json_encode({n = 3})).n,
    }
end
return M
"#,
        )
        .unwrap();
        let services = discover(Arc::new(Store::ephemeral()), Some(dir.clone())).unwrap();
        let (handlers, _jobs) = into_handlers(services);
        let req = RawRequest {
            method: "POST".into(),
            path: "/".into(),
            headers: vec![("x-amz-target".into(), "Enc.Encode".into())],
            body: b"{}".to_vec(),
            ..Default::default()
        };
        let response = handlers[0]
            .handle(&context("111122223333", "us-east-1"), &req)
            .unwrap();
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["Text"], r#"{"a":[1,2.5],"b":null}"#);
        assert_eq!(body["Empty"], "[]");
        assert_eq!(body["Round"], 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    // The only test that moves the shared virtual clock; keep clock steps in this one test.
    #[test]
    fn advancing_the_clock_fires_chained_timers_and_retries_failures() {
        let (handler, jobs, dir) = ticker();
        let ctx = context("123456789012", "us-east-1");
        ticker_call(&handler, &ctx, "Start", json!({"delay":0,"count":3}));
        assert_eq!(
            jobs.drain(),
            (1, true),
            "the first tick is due, the next is 60s away"
        );
        for _ in 0..2 {
            assert_eq!(jobs.drain(), (0, true));
            clock::advance_ms(61_000).unwrap();
            assert_eq!(jobs.drain(), (1, true));
        }
        assert_eq!(
            jobs.advance(3_600_000).unwrap(),
            (0, true),
            "the chain ended after three ticks"
        );
        let (_, body) = ticker_call(&handler, &ctx, "Ticks", json!({}));
        let at: Vec<i64> = body["Ticks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["at"].as_i64().unwrap())
            .collect();
        assert_eq!(at.len(), 3);
        assert!(at[1] - at[0] >= 60 && at[2] - at[1] >= 60, "{at:?}");

        // One long advance steps through a whole chain, running each tick at its own due time.
        ticker_call(&handler, &ctx, "Start", json!({"delay":0,"count":3}));
        assert_eq!(jobs.advance(3 * 3_600_000).unwrap(), (3, true));
        let (_, body) = ticker_call(&handler, &ctx, "Ticks", json!({}));
        let ticks = body["Ticks"].as_array().unwrap();
        assert_eq!(ticks.len(), 6);
        let chain: Vec<i64> = ticks[3..]
            .iter()
            .map(|t| t["at"].as_i64().unwrap())
            .collect();
        assert!(
            chain[1] - chain[0] == 60 && chain[2] - chain[1] == 60,
            "{chain:?}"
        );
        assert!(jobs.advance(-1).is_err());

        // A failing handler rolls back, retries with backoff, and is dropped after 5 attempts.
        ticker_call(
            &handler,
            &ctx,
            "Start",
            json!({"delay":0,"count":1,"fail":true}),
        );
        let mut ran = jobs.drain().0;
        assert_eq!(jobs.drain(), (0, true), "the retry waits for its backoff");
        for _ in 0..7 {
            clock::advance_ms(70_000).unwrap();
            ran += jobs.drain().0;
        }
        assert_eq!(ran, 5);
        clock::reset();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn services_call_each_other_but_not_back_into_a_running_service() {
        let dir = std::env::temp_dir().join(format!("roto-peers-{}", roto_core::ids::request_id()));
        let entry = |name: &str, prefix: &str| json!({"service":name,"script":format!("{name}/service.luau"),"target_prefix":prefix,"json_version":"1.0"});
        for name in ["alpha", "beta"] {
            std::fs::create_dir_all(dir.join(name)).unwrap();
        }
        std::fs::write(
            dir.join("manifest.json"),
            json!({"version":1,"services":[entry("alpha","Alpha."),entry("beta","Beta.")]})
                .to_string(),
        )
        .unwrap();
        // Hop forwards to the other service with one fewer hop; at zero it answers.
        for (name, other, other_prefix) in [("alpha", "beta", "Beta."), ("beta", "alpha", "Alpha.")]
        {
            std::fs::write(
                dir.join(format!("{name}/service.luau")),
                format!(r#"
local M = {{}}
function M.Echo(ctx, input, db) return {{From = "{name}", Account = ctx.account_id, Input = input}} end
function M.Hop(ctx, input, db)
    if input.hops == 0 then return {{Done = "{name}"}} end
    local r = roto.try_wire("{other}", "POST", "/", {{headers = {{["x-amz-target"] = "{other_prefix}Hop"}}, body = {{hops = input.hops - 1}}}})
    if not r.ok then return {{Failed = r.code, Message = r.message}} end
    return roto.json_decode(r.body)
end
function M.Self(ctx, input, db) return roto.try_wire("{name}", "POST", "/", {{headers = {{["x-amz-target"] = "{name}.Echo"}}}}) end
function M.Missing(ctx, input, db) return roto.try_wire("nope", "POST", "/") end
function M.Raise(ctx, input, db) return roto.wire("nope", "POST", "/") end
function M.Late(ctx, input, db) return roto.call and {{Has = true}} or {{Has = false}} end
return M
"#),
            )
            .unwrap();
        }
        let services = discover(Arc::new(Store::ephemeral()), Some(dir.clone())).unwrap();
        let (handlers, jobs) = into_handlers(services);
        jobs.connect(handlers.iter().map(|h| (h.service(), h.clone())).collect());
        let alpha = &handlers[0];
        let ctx = context("111122223333", "eu-west-1");
        let invoke = |op: &str, input: Value| -> Value {
            let req = RawRequest {
                method: "POST".into(),
                path: "/".into(),
                headers: vec![("x-amz-target".into(), format!("Alpha.{op}"))],
                body: serde_json::to_vec(&input).unwrap(),
                ..Default::default()
            };
            let response = alpha.handle(&ctx, &req).unwrap();
            serde_json::from_slice(&response.body).unwrap()
        };
        // A call to a different service and back out works until the same service is reached again.
        let reentered = invoke("Hop", json!({"hops": 2}));
        assert_eq!(reentered["Failed"], "ReentrantCall", "{reentered}");
        let direct = invoke("Self", json!({}));
        assert_eq!(direct["code"], "ReentrantCall", "{direct}");
        let missing = invoke("Missing", json!({}));
        assert_eq!(missing["code"], "ServiceUnavailable");
        let raised = invoke("Raise", json!({}));
        assert!(
            raised["message"]
                .as_str()
                .unwrap()
                .contains("Service unavailable"),
            "{raised}"
        );
        // The peer functions exist only while a service is executing.
        assert_eq!(invoke("Late", json!({}))["Has"], true);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
