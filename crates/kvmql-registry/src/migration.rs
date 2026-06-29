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
            );",
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
            );",
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
            );",
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
            );",
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

    if current_version < 7 {
        info!("applying registry schema v7 — add ssh provider type");
        // Expand the providers type CHECK constraint to accept 'ssh'.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS providers_v7 (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL CHECK (type IN ('kvm','aws','gcp','azure','cloudflare','github','kubernetes','ssh')),
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
            INSERT INTO providers_v7 SELECT * FROM providers;
            DROP TABLE providers;
            ALTER TABLE providers_v7 RENAME TO providers;"
        )
        .map_err(|e| RegistryError::Migration(format!("v7 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![7, "add ssh provider type for file/directory/symlink resources"],
        )?;

        info!("registry schema v7 applied successfully");
    }

    if current_version < 8 {
        info!("applying registry schema v8 — add import_log table");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS import_log (
                id TEXT PRIMARY KEY,
                provider_id TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('new','existing','missing','error')),
                details TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_import_log_provider ON import_log(provider_id);
            CREATE INDEX IF NOT EXISTS idx_import_log_action ON import_log(action);",
        )
        .map_err(|e| RegistryError::Migration(format!("v8 DDL failed: {e}")))?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![8, "add import_log table for IMPORT RESOURCES discovery"],
        )?;

        info!("registry schema v8 applied successfully");
    }

    if current_version < 9 {
        info!("applying registry schema v9 — pricing + cost_estimate tables");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pricing (
                provider      TEXT NOT NULL,
                region        TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                param         TEXT NOT NULL DEFAULT '',
                hourly        REAL NOT NULL DEFAULT 0.0,
                monthly       REAL NOT NULL DEFAULT 0.0,
                unit          TEXT NOT NULL DEFAULT 'instance',
                updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (provider, region, resource_type, param)
            );

            CREATE TABLE IF NOT EXISTS cost_estimate (
                id              TEXT PRIMARY KEY,
                resource_id     TEXT NOT NULL,
                resource_type   TEXT NOT NULL,
                provider        TEXT NOT NULL DEFAULT 'aws',
                description     TEXT,
                quantity        INTEGER NOT NULL DEFAULT 1,
                hourly          REAL NOT NULL DEFAULT 0.0,
                monthly         REAL NOT NULL DEFAULT 0.0,
                estimated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .map_err(|e| RegistryError::Migration(format!("v9 DDL failed: {e}")))?;

        // Seed default AWS pricing data
        seed_aws_pricing(conn)?;
        seed_aws_regional_pricing(conn)?;
        seed_azure_pricing(conn)?;

        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?1, datetime('now'), ?2)",
            rusqlite::params![9, "add pricing + cost_estimate tables with AWS seed data"],
        )?;

        info!("registry schema v9 applied successfully");
    }

    Ok(())
}

/// Seed default AWS pricing data for us-east-1.
fn seed_aws_pricing(conn: &Connection) -> Result<(), RegistryError> {
    let rows: &[(&str, &str, &str, &str, f64, f64, &str)] = &[
        // EKS control plane
        (
            "aws",
            "us-east-1",
            "eks_cluster",
            "",
            0.10,
            73.00,
            "cluster",
        ),
        // EC2 instance types (for EKS nodegroups)
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "t3.micro",
            0.0104,
            7.59,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "t3.small",
            0.0208,
            15.18,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "t3.medium",
            0.0416,
            30.37,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "t3.large",
            0.0832,
            60.74,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "t3.xlarge",
            0.1664,
            121.47,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "m5.large",
            0.096,
            70.08,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "m5.xlarge",
            0.192,
            140.16,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "c6i.large",
            0.085,
            62.05,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "r6i.large",
            0.126,
            91.98,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "eks_nodegroup",
            "g5.xlarge",
            1.006,
            734.38,
            "instance",
        ),
        // RDS PostgreSQL
        (
            "aws",
            "us-east-1",
            "rds_postgres",
            "db.t3.micro",
            0.018,
            13.14,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "rds_postgres",
            "db.t3.small",
            0.036,
            26.28,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "rds_postgres",
            "db.t3.medium",
            0.072,
            52.56,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "rds_postgres",
            "db.r5.large",
            0.24,
            175.20,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "rds_postgres",
            "db.r6g.large",
            0.216,
            157.68,
            "instance",
        ),
        // ElastiCache Redis
        (
            "aws",
            "us-east-1",
            "elasticache_redis",
            "cache.t3.micro",
            0.017,
            12.41,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "elasticache_redis",
            "cache.t3.small",
            0.034,
            24.82,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "elasticache_redis",
            "cache.t3.medium",
            0.068,
            49.64,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "elasticache_redis",
            "cache.r6g.large",
            0.166,
            121.18,
            "instance",
        ),
        // Replication groups (same pricing as redis nodes)
        (
            "aws",
            "us-east-1",
            "elasticache_replication_group",
            "cache.t3.micro",
            0.017,
            12.41,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "elasticache_replication_group",
            "cache.t3.small",
            0.034,
            24.82,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "elasticache_replication_group",
            "cache.t3.medium",
            0.068,
            49.64,
            "instance",
        ),
        // MSK Kafka
        (
            "aws",
            "us-east-1",
            "msk_cluster",
            "kafka.t3.small",
            0.054,
            39.42,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "msk_cluster",
            "kafka.m5.large",
            0.228,
            166.44,
            "instance",
        ),
        (
            "aws",
            "us-east-1",
            "msk_cluster",
            "kafka.m5.xlarge",
            0.456,
            332.88,
            "instance",
        ),
        // Fixed-price services
        (
            "aws",
            "us-east-1",
            "nat_gateway",
            "",
            0.045,
            32.85,
            "gateway",
        ),
        (
            "aws",
            "us-east-1",
            "vpc_endpoint",
            "Interface",
            0.01,
            7.30,
            "endpoint-az",
        ),
        (
            "aws",
            "us-east-1",
            "vpc_endpoint",
            "Gateway",
            0.0,
            0.0,
            "endpoint",
        ),
        ("aws", "us-east-1", "kms_key", "", 0.00137, 1.00, "key"),
        ("aws", "us-east-1", "s3_bucket", "", 0.0, 0.023, "gb-month"),
        // Free resources
        ("aws", "us-east-1", "vpc", "", 0.0, 0.0, "vpc"),
        ("aws", "us-east-1", "aws_subnet", "", 0.0, 0.0, "subnet"),
        ("aws", "us-east-1", "security_group", "", 0.0, 0.0, "sg"),
        ("aws", "us-east-1", "iam_role", "", 0.0, 0.0, "role"),
        ("aws", "us-east-1", "iam_policy", "", 0.0, 0.0, "policy"),
        ("aws", "us-east-1", "ses_domain", "", 0.0, 0.0, "domain"),
        ("aws", "us-east-1", "acm_certificate", "", 0.0, 0.0, "cert"),
    ];

    for &(provider, region, resource_type, param, hourly, monthly, unit) in rows {
        conn.execute(
            "INSERT OR IGNORE INTO pricing (provider, region, resource_type, param, hourly, monthly, unit)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![provider, region, resource_type, param, hourly, monthly, unit],
        )?;
    }

    Ok(())
}

/// Replicate us-east-1 AWS prices into additional regions with multipliers.
fn seed_aws_regional_pricing(conn: &Connection) -> Result<(), RegistryError> {
    let regional_multipliers: &[(&str, f64)] = &[
        ("us-west-2", 1.0),
        ("eu-west-1", 1.05),
        ("ap-southeast-1", 1.10),
        ("ap-northeast-1", 1.15),
        ("af-south-1", 1.20),
    ];

    for &(region, multiplier) in regional_multipliers {
        conn.execute(
            "INSERT OR IGNORE INTO pricing (provider, region, resource_type, param, hourly, monthly, unit)
             SELECT provider, ?1, resource_type, param,
                    ROUND(hourly * ?2, 5), ROUND(monthly * ?2, 2), unit
             FROM pricing WHERE provider = 'aws' AND region = 'us-east-1'",
            rusqlite::params![region, multiplier],
        )?;
    }
    Ok(())
}

/// Seed Azure pricing for eastus, then derive southafricanorth and westeurope.
fn seed_azure_pricing(conn: &Connection) -> Result<(), RegistryError> {
    let rows: &[(&str, &str, &str, &str, f64, f64, &str)] = &[
        // PostgreSQL Flexible Server
        (
            "azure", "eastus", "postgres", "B1ms", 0.0210, 15.33, "instance",
        ),
        (
            "azure", "eastus", "postgres", "B2s", 0.0420, 30.66, "instance",
        ),
        (
            "azure",
            "eastus",
            "postgres",
            "GP_Standard_D2s_v3",
            0.1570,
            114.61,
            "instance",
        ),
        (
            "azure",
            "eastus",
            "postgres",
            "GP_Standard_D4s_v3",
            0.3140,
            229.22,
            "instance",
        ),
        (
            "azure",
            "eastus",
            "postgres",
            "GP_Standard_D8s_v3",
            0.6280,
            458.44,
            "instance",
        ),
        // Redis Cache
        ("azure", "eastus", "redis", "C0", 0.0220, 16.06, "instance"),
        ("azure", "eastus", "redis", "C1", 0.0550, 40.15, "instance"),
        ("azure", "eastus", "redis", "C2", 0.1100, 80.30, "instance"),
        ("azure", "eastus", "redis", "P1", 0.3080, 224.84, "instance"),
        // AKS (control plane is free, nodes are VM cost)
        ("azure", "eastus", "aks", "", 0.0, 0.0, "cluster"),
        (
            "azure",
            "eastus",
            "aks",
            "Standard_D2s_v3",
            0.096,
            70.08,
            "instance",
        ),
        (
            "azure",
            "eastus",
            "aks",
            "Standard_D4s_v3",
            0.192,
            140.16,
            "instance",
        ),
        (
            "azure",
            "eastus",
            "aks",
            "Standard_D8s_v3",
            0.384,
            280.32,
            "instance",
        ),
        // Storage Account
        (
            "azure",
            "eastus",
            "storage_account",
            "Standard_LRS",
            0.0,
            0.018,
            "gb-month",
        ),
        (
            "azure",
            "eastus",
            "storage_account",
            "Standard_GRS",
            0.0,
            0.036,
            "gb-month",
        ),
        // Container Registry
        (
            "azure",
            "eastus",
            "container_registry",
            "Basic",
            0.00685,
            5.00,
            "registry",
        ),
        (
            "azure",
            "eastus",
            "container_registry",
            "Standard",
            0.0274,
            20.00,
            "registry",
        ),
        (
            "azure",
            "eastus",
            "container_registry",
            "Premium",
            0.0685,
            50.00,
            "registry",
        ),
        // Container App (consumption-based)
        ("azure", "eastus", "container_app", "", 0.0, 0.0, "app"),
        // Load Balancer
        (
            "azure",
            "eastus",
            "load_balancer",
            "Standard",
            0.0250,
            18.25,
            "lb",
        ),
        // DNS Zone
        ("azure", "eastus", "dns_zone", "", 0.0, 0.50, "zone"),
        // Free resources
        ("azure", "eastus", "vnet", "", 0.0, 0.0, "vnet"),
        ("azure", "eastus", "subnet", "", 0.0, 0.0, "subnet"),
        ("azure", "eastus", "nsg", "", 0.0, 0.0, "nsg"),
        ("azure", "eastus", "nsg_rule", "", 0.0, 0.0, "rule"),
        ("azure", "eastus", "vnet_peering", "", 0.0, 0.0, "peering"),
        ("azure", "eastus", "dns_vnet_link", "", 0.0, 0.0, "link"),
    ];

    for &(provider, region, resource_type, param, hourly, monthly, unit) in rows {
        conn.execute(
            "INSERT OR IGNORE INTO pricing (provider, region, resource_type, param, hourly, monthly, unit)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![provider, region, resource_type, param, hourly, monthly, unit],
        )?;
    }

    // South Africa North (~1.25x of eastus for most services)
    conn.execute(
        "INSERT OR IGNORE INTO pricing (provider, region, resource_type, param, hourly, monthly, unit)
         SELECT provider, 'southafricanorth', resource_type, param,
                ROUND(hourly * 1.25, 5), ROUND(monthly * 1.25, 2), unit
         FROM pricing WHERE provider = 'azure' AND region = 'eastus'",
        [],
    )?;

    // West Europe (~1.10x of eastus)
    conn.execute(
        "INSERT OR IGNORE INTO pricing (provider, region, resource_type, param, hourly, monthly, unit)
         SELECT provider, 'westeurope', resource_type, param,
                ROUND(hourly * 1.10, 5), ROUND(monthly * 1.10, 2), unit
         FROM pricing WHERE provider = 'azure' AND region = 'eastus'",
        [],
    )?;

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
        assert_eq!(version, 9);
    }
}
