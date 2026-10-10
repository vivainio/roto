//! SQLite-backed persistence. One database file per service under the data directory.
//!
//! Every database is opened in WAL mode and brought up to date with that service's ordered
//! [`Migration`]s. `Store::ephemeral` keeps everything in memory for fast tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use diesel::Connection as DieselConnection;
use diesel::SqliteConnection;
use diesel::connection::SimpleConnection;
use rusqlite::Connection;

use crate::error::AwsError;

/// A schema step. Versions must be unique and are applied in ascending order, once.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub version: u32,
    pub sql: &'static str,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreOptions {
    /// `synchronous=FULL` instead of `NORMAL`.
    pub durable: bool,
}

pub struct Store {
    dir: Option<PathBuf>,
    /// Lazily created scratch directory for blobs in ephemeral mode; removed on drop.
    scratch: Mutex<Option<PathBuf>>,
    options: StoreOptions,
    dbs: Mutex<HashMap<String, Arc<Db>>>,
    diesel_dbs: Mutex<HashMap<String, Arc<DieselDb>>>,
    memory_id: String,
}

impl Store {
    pub fn open(dir: impl Into<PathBuf>, options: StoreOptions) -> Result<Self, AwsError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| {
            AwsError::internal(format!("cannot create data dir {}: {e}", dir.display()))
        })?;
        Ok(Self {
            scratch: Mutex::default(),
            dir: Some(dir),
            options,
            dbs: Mutex::default(),
            diesel_dbs: Mutex::default(),
            memory_id: crate::ids::request_id(),
        })
    }

    pub fn ephemeral() -> Self {
        Self {
            scratch: Mutex::default(),
            dir: None,
            options: StoreOptions::default(),
            dbs: Mutex::default(),
            diesel_dbs: Mutex::default(),
            memory_id: crate::ids::request_id(),
        }
    }

    /// A directory for large blobs (object bodies, code packages). Persistent under the data
    /// directory, or a process-private temporary directory in ephemeral mode.
    pub fn blob_dir(&self, name: &str) -> Result<PathBuf, AwsError> {
        let base = match &self.dir {
            Some(d) => d.clone(),
            None => {
                let mut scratch = self.scratch.lock().unwrap();
                scratch
                    .get_or_insert_with(|| {
                        std::env::temp_dir().join(format!(
                            "roto-{}-{}",
                            std::process::id(),
                            crate::ids::request_id()
                        ))
                    })
                    .clone()
            }
        };
        let dir = base.join(name);
        std::fs::create_dir_all(&dir)
            .map_err(|e| AwsError::internal(format!("cannot create {}: {e}", dir.display())))?;
        Ok(dir)
    }

    pub fn data_dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    fn database_url(&self, service: &str) -> String {
        match &self.dir {
            Some(dir) => dir
                .join(format!("{service}.db"))
                .to_string_lossy()
                .into_owned(),
            None => format!(
                "file:roto-{}-{service}?mode=memory&cache=shared",
                self.memory_id
            ),
        }
    }

    /// Opens a typed Diesel connection to a service database. The legacy connection
    /// initializes migrations and remains available to the inspection UI. Both paths
    /// take the same service lock, so SQLite transactions and inspection are serialized.
    pub fn diesel_db(
        &self,
        service: &str,
        migrations: &[Migration],
    ) -> Result<Arc<DieselDb>, AwsError> {
        let mut dbs = self.diesel_dbs.lock().unwrap();
        if let Some(db) = dbs.get(service) {
            return Ok(db.clone());
        }
        let legacy = self.db(service, migrations)?;
        let mut conn = SqliteConnection::establish(&self.database_url(service))?;
        conn.batch_execute(&format!("PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL; PRAGMA synchronous={}; PRAGMA foreign_keys=ON;", if self.options.durable {"FULL"} else {"NORMAL"}))?;
        let db = Arc::new(DieselDb {
            conn: Mutex::new(conn),
            legacy,
        });
        dbs.insert(service.to_string(), db.clone());
        Ok(db)
    }

    /// Returns an already initialized service database without opening or migrating it.
    pub fn existing_db(&self, service: &str) -> Option<Arc<Db>> {
        self.dbs.lock().unwrap().get(service).cloned()
    }

    /// Opens (once) the database for `service`, applying any pending migrations.
    pub fn db(&self, service: &str, migrations: &[Migration]) -> Result<Arc<Db>, AwsError> {
        let mut dbs = self.dbs.lock().unwrap();
        if let Some(db) = dbs.get(service) {
            return Ok(db.clone());
        }
        let mut conn = match &self.dir {
            Some(dir) => Connection::open(dir.join(format!("{service}.db")))?,
            None => Connection::open(self.database_url(service))?,
        };
        configure(&conn, self.options)?;
        migrate(&mut conn, migrations)?;
        let db = Arc::new(Db {
            conn: Mutex::new(conn),
        });
        dbs.insert(service.to_string(), db.clone());
        Ok(db)
    }
}

fn configure(conn: &Connection, options: StoreOptions) -> rusqlite::Result<()> {
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(
        None,
        "synchronous",
        if options.durable { "FULL" } else { "NORMAL" },
    )?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn migrate(conn: &mut Connection, migrations: &[Migration]) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _roto_migrations (
             version INTEGER PRIMARY KEY,
             applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))",
    )?;
    let current: u32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM _roto_migrations",
        [],
        |r| r.get(0),
    )?;
    let mut pending: Vec<_> = migrations.iter().filter(|m| m.version > current).collect();
    pending.sort_by_key(|m| m.version);
    for m in pending {
        let tx = conn.transaction()?;
        tx.execute_batch(m.sql)?;
        tx.execute(
            "INSERT INTO _roto_migrations (version) VALUES (?1)",
            [m.version],
        )?;
        tx.commit()?;
    }
    Ok(())
}

/// A single SQLite connection behind a mutex. Operations run in one transaction each.
pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    /// Runs `f` inside a transaction: committed on `Ok`, rolled back on `Err`.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, AwsError>,
    ) -> Result<T, AwsError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    pub fn read<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, AwsError>,
    ) -> Result<T, AwsError> {
        f(&self.conn.lock().unwrap())
    }
}

/// Typed SQLite access for services converted to Diesel. The legacy connection
/// is only a migration/inspection bridge; handlers use this connection exclusively.
pub struct DieselDb {
    conn: Mutex<SqliteConnection>,
    legacy: Arc<Db>,
}
impl DieselDb {
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut SqliteConnection) -> Result<T, AwsError>,
    ) -> Result<T, AwsError> {
        self.legacy
            .read(|_| self.conn.lock().unwrap().transaction(f))
    }
    pub fn read<T>(
        &self,
        f: impl FnOnce(&mut SqliteConnection) -> Result<T, AwsError>,
    ) -> Result<T, AwsError> {
        self.legacy.read(|_| f(&mut self.conn.lock().unwrap()))
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Some(dir) = self.scratch.lock().unwrap().take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: &[Migration] = &[
        Migration {
            version: 1,
            sql: "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)",
        },
        Migration {
            version: 2,
            sql: "ALTER TABLE t ADD COLUMN extra TEXT",
        },
    ];

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("roto-test-{name}-{}", crate::ids::request_id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn persists_across_reopen_and_migrates_once() {
        let dir = tmp("persist");
        {
            let store = Store::open(&dir, StoreOptions::default()).unwrap();
            let db = store.db("svc", M).unwrap();
            db.transaction(|tx| Ok(tx.execute("INSERT INTO t (k, v) VALUES ('a', '1')", [])?))
                .unwrap();
        }
        let store = Store::open(&dir, StoreOptions::default()).unwrap();
        let db = store.db("svc", M).unwrap();
        let (v, versions): (String, u32) = db
            .read(|c| {
                let v = c.query_row("SELECT v FROM t WHERE k='a'", [], |r| r.get(0))?;
                let n = c.query_row("SELECT COUNT(*) FROM _roto_migrations", [], |r| r.get(0))?;
                Ok((v, n))
            })
            .unwrap();
        assert_eq!(v, "1");
        assert_eq!(versions, 2);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn diesel_shares_inspection_state_and_rolls_back() {
        use diesel::{
            RunQueryDsl, sql_query,
            sql_types::{BigInt, Text},
        };
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        let store = Store::ephemeral();
        let typed = store.diesel_db("svc", M).unwrap();
        typed
            .transaction(|c| {
                sql_query("INSERT INTO t(k,v) VALUES (?1,?2)")
                    .bind::<Text, _>("a")
                    .bind::<Text, _>("typed")
                    .execute(c)?;
                Ok(())
            })
            .unwrap();
        let legacy = store.existing_db("svc").unwrap();
        let value: String = legacy
            .read(|c| Ok(c.query_row("SELECT v FROM t WHERE k='a'", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(value, "typed");
        let result: Result<(), AwsError> = typed.transaction(|c| {
            sql_query("INSERT INTO t(k,v) VALUES ('b','rollback')").execute(c)?;
            Err(AwsError::internal("rollback"))
        });
        assert!(result.is_err());
        assert_eq!(
            typed
                .read(|c| Ok(sql_query("SELECT COUNT(*) AS n FROM t")
                    .get_result::<Count>(c)?
                    .n))
                .unwrap(),
            1
        );
        let other = Store::ephemeral();
        assert_eq!(
            other
                .diesel_db("svc", M)
                .unwrap()
                .read(|c| Ok(sql_query("SELECT COUNT(*) AS n FROM t")
                    .get_result::<Count>(c)?
                    .n))
                .unwrap(),
            0
        );
    }

    #[test]
    fn diesel_persists_and_uses_existing_migrations() {
        use diesel::{RunQueryDsl, sql_query, sql_types::Text};
        #[derive(diesel::QueryableByName)]
        struct Value {
            #[diesel(sql_type = Text)]
            v: String,
        }
        let dir = tmp("diesel-persist");
        {
            let store = Store::open(&dir, StoreOptions { durable: true }).unwrap();
            store
                .diesel_db("svc", M)
                .unwrap()
                .transaction(|c| {
                    sql_query("INSERT INTO t(k,v) VALUES ('a','persisted')").execute(c)?;
                    Ok(())
                })
                .unwrap();
        }
        {
            let store = Store::open(&dir, StoreOptions::default()).unwrap();
            let db = store.diesel_db("svc", M).unwrap();
            assert_eq!(
                db.read(|c| Ok(sql_query("SELECT v FROM t WHERE k='a'")
                    .get_result::<Value>(c)?
                    .v))
                    .unwrap(),
                "persisted"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_transaction_rolls_back() {
        let store = Store::ephemeral();
        let db = store.db("svc", M).unwrap();
        let r: Result<(), _> = db.transaction(|tx| {
            tx.execute("INSERT INTO t (k, v) VALUES ('a', '1')", [])?;
            Err(AwsError::internal("boom"))
        });
        assert!(r.is_err());
        let n: u32 = db
            .read(|c| Ok(c.query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(n, 0);
    }
}
