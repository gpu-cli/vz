//! Pre-migration backup and rollback for the durable state store.
//!
//! A state store whose on-disk schema version is below
//! [`StateStore::CURRENT_SCHEMA_VERSION`](super::StateStore::CURRENT_SCHEMA_VERSION)
//! is migrated in place the first time this build opens it. Those migrations
//! commit one step at a time, so a failure part way through leaves a store the
//! producing release can no longer read and the previous release never could.
//!
//! This module takes a byte-identical copy of the store before the first schema
//! write and restores it if the open fails, so a failed upgrade leaves exactly
//! the bytes the previous release wrote. The copy is retained on success too:
//! it is the artifact a rollback to the previous release reads.
//!
//! Migration barrier `mig.filesystem.state_store_backup` in
//! `config/vz-0.4-migration-barriers.json` is the normative statement of this
//! behaviour.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::StackError;

/// Directory, relative to the backup root, holding pre-migration store copies.
pub const BACKUP_DIRNAME: &str = "state-store-backups";

/// Fault-injection switch consumed by the release gate's migration scenario.
///
/// The gate must be able to fail a migration through the installed binaries,
/// not only through an in-process test hook, because the assertion it needs is
/// that the *installed* upgrade path restores the backup. The only accepted
/// value is [`FAILPOINT_AFTER_SCHEMA_MIGRATION`]; anything else is rejected so
/// a typo cannot silently disable the injection.
pub const FAILPOINT_ENV: &str = "VZ_STATE_STORE_MIGRATION_FAILPOINT";

/// Fail the open after every schema migration has committed.
///
/// This is the worst survivable moment: the file on disk is fully migrated and
/// the previous release cannot read it, so only a restored backup makes a
/// rollback possible.
pub const FAILPOINT_AFTER_SCHEMA_MIGRATION: &str = "after_schema_migration";

/// Sidecar record written beside a pre-migration backup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationBackupRecord {
    /// Schema version of the store when the backup was taken.
    pub from_schema_version: u32,
    /// Schema version this build migrates to.
    pub to_schema_version: u32,
    /// SHA-256 of the backed-up store file.
    pub sha256: String,
    /// Absolute path of the store the backup was taken from.
    pub state_store_path: String,
    /// Absolute path of the backup itself.
    pub backup_path: String,
    /// Wall-clock nanoseconds at which the backup was taken.
    pub created_unix_ns: u128,
    /// Whether the migration that followed this backup completed.
    pub migration_completed: bool,
    /// Whether this backup was restored over the store after a failed open.
    pub restored: bool,
}

/// A retained pre-migration copy of one state store.
#[derive(Debug)]
pub struct MigrationBackup {
    original: PathBuf,
    backup: PathBuf,
    record_path: PathBuf,
    record: MigrationBackupRecord,
}

impl MigrationBackup {
    /// Path of the retained backup file.
    pub fn path(&self) -> &Path {
        &self.backup
    }

    /// Path of the JSON sidecar describing the backup.
    pub fn record_path(&self) -> &Path {
        &self.record_path
    }

    /// The sidecar contents.
    pub fn record(&self) -> &MigrationBackupRecord {
        &self.record
    }

    /// Mark the migration this backup guarded as completed.
    pub fn record_success(mut self) -> Result<MigrationBackupRecord, StackError> {
        self.record.migration_completed = true;
        write_record(&self.record_path, &self.record)?;
        Ok(self.record)
    }

    /// Restore the backup over the store and report why.
    ///
    /// Every caller has already dropped its connection: SQLite keeps no lock on
    /// a closed database, and the sidecar journal files a failed migration may
    /// have left are removed here rather than merged with restored bytes.
    pub fn restore(mut self, source: StackError) -> Result<super::StateStore, StackError> {
        remove_sidecars(&self.original)?;
        let staged = self.original.with_extension("restore-staging");
        copy_file(&self.backup, &staged)?;
        fs::rename(&staged, &self.original)?;
        fsync_dir(parent_of(&self.original)?)?;
        let restored = digest_file(&self.original)?;
        if restored != self.record.sha256 {
            return Err(StackError::InvalidSpec(format!(
                "state store migration failed and the restored backup at {} does not reproduce \
                 its recorded pre-migration digest (recorded {}, restored {}); the store at {} \
                 must be recovered by hand. Original failure: {source}",
                self.backup.display(),
                self.record.sha256,
                restored,
                self.original.display(),
            )));
        }
        self.record.restored = true;
        write_record(&self.record_path, &self.record)?;
        Err(StackError::InvalidSpec(format!(
            "state store migration from schema version {} failed; the pre-migration backup at {} \
             was restored over {} byte-for-byte (sha256 {}), so the previous release can read it. \
             Underlying failure: {source}",
            self.record.from_schema_version,
            self.backup.display(),
            self.original.display(),
            self.record.sha256,
        )))
    }
}

/// Take a pre-migration backup if opening `path` will migrate it.
///
/// Returns `None` when there is nothing to protect: no store on disk yet, or a
/// store already at the current schema version. `backup_root` defaults to the
/// store's own directory; the daemon passes its runtime data dir.
pub fn prepare(
    path: &Path,
    backup_root: Option<&Path>,
) -> Result<Option<MigrationBackup>, StackError> {
    let Some(from) = existing_schema_version(path)? else {
        return Ok(None);
    };
    let to = super::StateStore::CURRENT_SCHEMA_VERSION;
    if from >= to {
        return Ok(None);
    }
    let root = match backup_root {
        Some(root) => root.to_path_buf(),
        None => parent_of(path)?.to_path_buf(),
    };
    let dir = root.join(BACKUP_DIRNAME);
    fs::create_dir_all(&dir)?;
    let created_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            StackError::InvalidSpec(format!("system clock before the epoch: {error}"))
        })?
        .as_nanos();
    let stem = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "state store path {} has no file name",
                path.display()
            ))
        })?;
    let backup = dir.join(format!("{stem}.v{from}.{created_unix_ns}.bak"));
    copy_file(path, &backup)?;
    fsync_dir(&dir)?;
    let sha256 = digest_file(&backup)?;
    let record = MigrationBackupRecord {
        from_schema_version: from,
        to_schema_version: to,
        sha256,
        state_store_path: path.display().to_string(),
        backup_path: backup.display().to_string(),
        created_unix_ns,
        migration_completed: false,
        restored: false,
    };
    let record_path = backup.with_extension("bak.json");
    write_record(&record_path, &record)?;
    fsync_dir(&dir)?;
    Ok(Some(MigrationBackup {
        original: path.to_path_buf(),
        backup,
        record_path,
        record,
    }))
}

/// Fail the open when the gate's fault-injection switch names this point.
pub fn injected_failure() -> Result<(), StackError> {
    injected_failure_from(std::env::var(FAILPOINT_ENV).ok().as_deref())
}

/// [`injected_failure`] over an explicit value, so it is testable without the
/// process-wide environment.
fn injected_failure_from(value: Option<&str>) -> Result<(), StackError> {
    match value {
        None | Some("") => Ok(()),
        Some(FAILPOINT_AFTER_SCHEMA_MIGRATION) => Err(StackError::InvalidSpec(format!(
            "{FAILPOINT_ENV}={FAILPOINT_AFTER_SCHEMA_MIGRATION}: migration failure injected after \
             the schema migrations committed"
        ))),
        Some(other) => Err(StackError::InvalidSpec(format!(
            "{FAILPOINT_ENV}={other} is not a known migration failpoint (expected \
             {FAILPOINT_AFTER_SCHEMA_MIGRATION})"
        ))),
    }
}

/// Schema version of an existing, non-empty store, or `None` when there is none.
fn existing_schema_version(path: &Path) -> Result<Option<u32>, StackError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() > 0 => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM control_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()
        // A store with no `control_metadata` table is rejected by `init_schema`
        // with its own diagnosis; nothing here needs to pre-empt it.
        .unwrap_or(None);
    Ok(value.and_then(|text| text.parse::<u32>().ok()))
}

fn parent_of(path: &Path) -> Result<&Path, StackError> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            StackError::InvalidSpec(format!(
                "state store path {} has no parent directory",
                path.display()
            ))
        })
}

/// Remove the `-wal`/`-shm` sidecars of a store about to be overwritten.
fn remove_sidecars(path: &Path) -> Result<(), StackError> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let sidecar = PathBuf::from(name);
        match fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn copy_file(from: &Path, to: &Path) -> Result<(), StackError> {
    let mut source = fs::File::open(from)?;
    let mut bytes = Vec::new();
    source.read_to_end(&mut bytes)?;
    let mut target = fs::File::create(to)?;
    target.write_all(&bytes)?;
    target.sync_all()?;
    Ok(())
}

fn fsync_dir(dir: &Path) -> Result<(), StackError> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn write_record(path: &Path, record: &MigrationBackupRecord) -> Result<(), StackError> {
    let mut file = fs::File::create(path)?;
    file.write_all(serde_json::to_string_pretty(record)?.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn digest_file(path: &Path) -> Result<String, StackError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::state_store::{StateStore, StateStorePragmas};

    /// Build a store at the given legacy schema version by rewinding a fresh one.
    fn legacy_store(path: &Path, version: u32) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE control_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL,
                 updated_at TEXT NOT NULL DEFAULT (datetime('now')));",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO control_metadata (key, value) VALUES ('schema_version', ?1)",
            [version.to_string()],
        )
        .unwrap();
    }

    #[test]
    fn a_current_store_is_not_backed_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        drop(StateStore::open(&path).unwrap());
        assert!(prepare(&path, None).unwrap().is_none());
        assert!(!dir.path().join(BACKUP_DIRNAME).exists());
    }

    #[test]
    fn an_absent_store_is_not_backed_up() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            prepare(&dir.path().join("absent.db"), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_legacy_store_is_copied_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        legacy_store(&path, 1);
        let before = std::fs::read(&path).unwrap();
        let backup = prepare(&path, None).unwrap().expect("backup taken");
        assert_eq!(std::fs::read(backup.path()).unwrap(), before);
        assert_eq!(backup.record().from_schema_version, 1);
        assert_eq!(
            backup.record().to_schema_version,
            StateStore::CURRENT_SCHEMA_VERSION
        );
        assert!(!backup.record().migration_completed);
        let recorded: MigrationBackupRecord =
            serde_json::from_slice(&std::fs::read(backup.record_path()).unwrap()).unwrap();
        assert_eq!(&recorded, backup.record());
    }

    #[test]
    fn the_backup_root_may_be_a_separate_directory() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        legacy_store(&path, 1);
        let backup = prepare(&path, Some(runtime.path()))
            .unwrap()
            .expect("backup taken");
        assert!(
            backup
                .path()
                .starts_with(runtime.path().join(BACKUP_DIRNAME))
        );
    }

    #[test]
    fn restore_puts_the_original_bytes_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        legacy_store(&path, 1);
        let before = std::fs::read(&path).unwrap();
        let backup = prepare(&path, None).unwrap().expect("backup taken");
        // Something else entirely, standing in for a half-migrated store.
        std::fs::write(&path, b"a store this build wrote and cannot roll back").unwrap();
        let record_path = backup.record_path().to_path_buf();
        let error = backup
            .restore(StackError::InvalidSpec("injected".to_string()))
            .err()
            .expect("restore reports the original failure");
        assert!(error.to_string().contains("was restored over"), "{error}");
        assert!(error.to_string().contains("injected"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let recorded: MigrationBackupRecord =
            serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
        assert!(recorded.restored);
    }

    #[test]
    fn the_failpoint_fires_only_for_its_exact_value() {
        assert!(injected_failure_from(None).is_ok());
        assert!(injected_failure_from(Some("")).is_ok());
        let injected = injected_failure_from(Some(FAILPOINT_AFTER_SCHEMA_MIGRATION))
            .expect_err("the declared failpoint fires");
        assert!(
            injected.to_string().contains("injected after"),
            "{injected}"
        );
        let typo = injected_failure_from(Some("after_schema_migratio"))
            .expect_err("a misspelled failpoint is rejected rather than ignored");
        assert!(
            typo.to_string()
                .contains("is not a known migration failpoint"),
            "{typo}"
        );
    }

    #[test]
    fn a_fresh_store_opens_at_the_current_version_without_a_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let store = StateStore::open_with_pragmas(&path, StateStorePragmas::default()).unwrap();
        assert_eq!(
            store.schema_version().unwrap(),
            StateStore::CURRENT_SCHEMA_VERSION
        );
        assert!(!dir.path().join(BACKUP_DIRNAME).exists());
    }
}
