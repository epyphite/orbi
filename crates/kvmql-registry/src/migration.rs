use rusqlite::Connection;
use tracing::info;

use crate::error::RegistryError;
use crate::schema::SCHEMA_V1;

/// Run all pending migrations against the given connection.
///
/// The migration system is forward-only (no rollback). It creates the
/// `schema_version` table if it does not exist, then applies any unapplied
/// versions in order.
pub fn run_migrations(conn: &Connection) -> Result<(), RegistryError> {
    // Ensure the schema_version table exists so we can query it.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL,
            description TEXT
        );",
    )?;

    let current_version: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if current_version < 1 {
        info!("applying registry schema v1");
        conn.execute_batch(SCHEMA_V1)
            .map_err(|e| RegistryError::Migration(format!("v1 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![1, "initial schema — 15 tables"],
        )?;

        info!("registry schema v1 applied successfully");
    }

    if current_version < 2 {
        info!("applying registry schema v2 — resources table");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS resources (
                id TEXT PRIMARY KEY,
                resource_type TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                name TEXT,
                status TEXT NOT NULL DEFAULT 'creating',
                config TEXT,
                outputs TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT,
                labels TEXT
            );"
        )
        .map_err(|e| RegistryError::Migration(format!("v2 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![2, "add resources table"],
        )?;

        info!("registry schema v2 applied successfully");
    }

    if current_version < 3 {
        info!("applying registry schema v3 — state_snapshots table");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS state_snapshots (
                id TEXT PRIMARY KEY,
                tag TEXT,
                statement TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                previous_state TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );"
        )
        .map_err(|e| RegistryError::Migration(format!("v3 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![3, "add state_snapshots table"],
        )?;

        info!("registry schema v3 applied successfully");
    }

    if current_version < 4 {
        info!("applying registry schema v4 — plans table");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS plans (
                id TEXT PRIMARY KEY,
                name TEXT,
                source TEXT NOT NULL,
                plan_output TEXT NOT NULL,
                checksum TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'applied', 'failed', 'rejected')),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                created_by TEXT,
                approved_at TEXT,
                approved_by TEXT,
                applied_at TEXT,
                applied_by TEXT,
                error TEXT,
                environment TEXT
            );"
        )
        .map_err(|e| RegistryError::Migration(format!("v4 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![4, "add plans table"],
        )?;

        info!("registry schema v4 applied successfully");
    }

    if current_version < 5 {
        info!("applying registry schema v5 — applied_files table");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS applied_files (
                id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                statements_count INTEGER NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now')),
                applied_by TEXT,
                environment TEXT,
                status TEXT NOT NULL DEFAULT 'applied'
                    CHECK (status IN ('applied', 'partial', 'failed'))
            );"
        )
        .map_err(|e| RegistryError::Migration(format!("v5 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![5, "add applied_files table"],
        )?;

        info!("registry schema v5 applied successfully");
    }

    if current_version < 6 {
        info!("applying registry schema v6 — expand provider types");
        // SQLite doesn't support ALTER TABLE DROP CONSTRAINT, so we recreate
        // the providers table with the expanded CHECK constraint.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS providers_v6 (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL CHECK (type IN ('kvm','aws','gcp','azure','cloudflare','github','kubernetes')),
                driver TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'unknown' CHECK (status IN ('healthy','degraded','offline','unknown')),
                enabled INTEGER NOT NULL DEFAULT 1,
                host TEXT,
                region TEXT,
                auth_ref TEXT NOT NULL,
                labels TEXT,
                latency_ms INTEGER,
                added_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen TEXT
            );
            INSERT INTO providers_v6 SELECT * FROM providers;
            DROP TABLE providers;
            ALTER TABLE providers_v6 RENAME TO providers;"
        )
        .map_err(|e| RegistryError::Migration(format!("v6 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![6, "expand provider types (cloudflare, github, kubernetes)"],
        )?;

        info!("registry schema v6 applied successfully");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        // Running again should be a no-op.
        run_migrations(&conn).unwrap();

        let version: i64 = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 6);
    }
}
