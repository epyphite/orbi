use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroVm {
    pub id: String,
    pub provider_id: String,
    pub tenant: String,
    pub status: String,
    pub image_id: Option<String>,
    pub vcpus: Option<i32>,
    pub memory_mb: Option<i32>,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i32>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
    pub hostname: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub labels: Option<serde_json::Value>,
    pub created_at: String,
    pub last_seen: Option<String>,
    pub is_stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    pub id: String,
    pub provider_id: String,
    pub microvm_id: Option<String>,
    pub vol_type: String,
    pub size_gb: i64,
    pub status: String,
    pub device_name: Option<String>,
    pub iops: Option<i32>,
    pub encrypted: bool,
    pub created_at: String,
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Image {
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
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageRef {
    pub image_id: String,
    pub provider_id: String,
    pub resolved_type: String,
    pub kernel_path: Option<String>,
    pub rootfs_path: Option<String>,
    pub cloud_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub microvm_id: String,
    pub provider_id: String,
    pub destination: String,
    pub tag: Option<String>,
    pub size_mb: Option<i64>,
    pub taken_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub microvm_id: String,
    pub sampled_at: String,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i32>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateParams {
    pub id: Option<String>,
    pub tenant: String,
    pub vcpus: i32,
    pub memory_mb: i32,
    pub image_id: String,
    pub hostname: Option<String>,
    pub network: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub labels: Option<serde_json::Value>,
    /// Resolved SSH public key content (or "generate" sentinel).
    pub ssh_key: Option<String>,
    /// Original credential reference for the SSH key (for audit trail).
    pub ssh_key_ref: Option<String>,
    /// Admin username for VM access.
    pub admin_user: Option<String>,
    /// Resolved cloud-init / user-data content.
    pub cloud_init: Option<String>,
    /// Original credential reference for cloud-init (for audit trail).
    pub cloud_init_ref: Option<String>,
    /// Resolved password (discouraged; prefer ssh_key).
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeParams {
    pub id: Option<String>,
    pub size_gb: i64,
    pub vol_type: String,
    pub encrypted: bool,
    pub iops: Option<i32>,
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageParams {
    pub id: String,
    pub name: String,
    pub os: String,
    pub distro: String,
    pub version: String,
    pub arch: String,
    pub image_type: String,
    pub source: String,
    pub kernel: Option<String>,
    pub rootfs: Option<String>,
    pub checksum: Option<String>,
    pub labels: Option<serde_json::Value>,
}
