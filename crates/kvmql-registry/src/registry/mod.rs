mod read;
mod write;

#[cfg(test)]
mod tests;

use rusqlite::Connection;

use crate::error::RegistryError;
use crate::migration::run_migrations;

// ---------------------------------------------------------------------------
// Row structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProviderRow {
    pub id: String,
    pub provider_type: String,
    pub driver: String,
    pub status: String,
    pub enabled: bool,
    pub host: Option<String>,
    pub region: Option<String>,
    pub auth_ref: String,
    pub labels: Option<String>,
    pub latency_ms: Option<i64>,
    pub added_at: String,
    pub last_seen: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MicrovmRow {
    pub id: String,
    pub provider_id: String,
    pub tenant: String,
    pub status: String,
    pub image_id: Option<String>,
    pub vcpus: Option<i64>,
    pub memory_mb: Option<i64>,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i64>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
    pub hostname: Option<String>,
    pub metadata: Option<String>,
    pub labels: Option<String>,
    pub created_at: String,
    pub last_seen: Option<String>,
    pub is_stale: bool,
}

#[derive(Debug, Clone)]
pub struct VolumeRow {
    pub id: String,
    pub provider_id: String,
    pub microvm_id: Option<String>,
    pub volume_type: String,
    pub size_gb: i64,
    pub status: String,
    pub device_name: Option<String>,
    pub iops: Option<i64>,
    pub encrypted: bool,
    pub created_at: String,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ImageRow {
    pub id: String,
    pub name: String,
    pub os: String,
    pub distro: String,
    pub version: String,
    pub arch: String,
    pub image_type: String,
    pub provider_id: Option<String>,
    pub kernel_path: Option<String>,
    pub rootfs_path: Option<String>,
    pub disk_path: Option<String>,
    pub cloud_ref: Option<String>,
    pub source: String,
    pub checksum_sha256: Option<String>,
    pub size_mb: Option<i64>,
    pub status: String,
    pub imported_at: String,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuditLogRow {
    pub id: String,
    pub event_time: String,
    pub principal: Option<String>,
    pub action: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub outcome: String,
    pub reason: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryHistoryRow {
    pub id: String,
    pub executed_at: String,
    pub principal: Option<String>,
    pub statement: String,
    pub normalized_stmt: Option<String>,
    pub verb: String,
    pub targets: Option<String>,
    pub duration_ms: Option<i64>,
    pub status: String,
    pub notifications: Option<String>,
    pub rows_affected: Option<i64>,
    pub result_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct PrincipalRow {
    pub id: String,
    pub principal_type: String,
    pub auth_ref: String,
    pub created_at: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct GrantRow {
    pub id: String,
    pub principal_id: String,
    pub verbs: String,
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub conditions: Option<String>,
    pub granted_at: String,
    pub granted_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResourceRow {
    pub id: String,
    pub resource_type: String,
    pub provider_id: String,
    pub name: Option<String>,
    pub status: String,
    pub config: Option<String>,
    pub outputs: Option<String>,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterMemberRow {
    pub cluster_id: String,
    pub provider_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub id: String,
    pub microvm_id: String,
    pub provider_id: String,
    pub destination: String,
    pub tag: Option<String>,
    pub size_mb: Option<i64>,
    pub taken_at: String,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: String,
    pub event_time: String,
    pub event_type: String,
    pub microvm_id: Option<String>,
    pub volume_id: Option<String>,
    pub image_id: Option<String>,
    pub provider_id: Option<String>,
    pub principal: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetricRow {
    pub id: String,
    pub microvm_id: String,
    pub sampled_at: String,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i64>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StateSnapshotRow {
    pub id: String,
    pub tag: Option<String>,
    pub statement: String,
    pub target_type: String,
    pub target_id: String,
    pub previous_state: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct PlanRow {
    pub id: String,
    pub name: Option<String>,
    pub source: String,
    pub plan_output: String,
    pub checksum: String,
    pub status: String,
    pub created_at: String,
    pub created_by: Option<String>,
    pub approved_at: Option<String>,
    pub approved_by: Option<String>,
    pub applied_at: Option<String>,
    pub applied_by: Option<String>,
    pub error: Option<String>,
    pub environment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ImportLogRow {
    pub id: String,
    pub provider_id: String,
    pub resource_type: String,
    pub resource_id: String,
    pub action: String,
    pub details: Option<String>,
    pub imported_at: String,
}

#[derive(Debug, Clone)]
pub struct PricingRow {
    pub provider: String,
    pub region: String,
    pub resource_type: String,
    pub param: String,
    pub hourly: f64,
    pub monthly: f64,
    pub unit: String,
}

#[derive(Debug, Clone)]
pub struct CostEstimateRow {
    pub id: String,
    pub resource_id: String,
    pub resource_type: String,
    pub provider: String,
    pub description: Option<String>,
    pub quantity: i64,
    pub hourly: f64,
    pub monthly: f64,
    pub estimated_at: String,
}

pub struct AppliedFileRow {
    pub id: String,
    pub file_path: String,
    pub file_hash: String,
    pub statements_count: i64,
    pub applied_at: String,
    pub applied_by: Option<String>,
    pub environment: Option<String>,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct Registry {
    pub(super) conn: Connection,
}

impl Registry {
    /// Open (or create) a registry backed by a file on disk.
    pub fn open(path: &str) -> Result<Self, RegistryError> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// Open an in-memory registry (useful for tests).
    pub fn open_in_memory() -> Result<Self, RegistryError> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self, RegistryError> {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }
}
