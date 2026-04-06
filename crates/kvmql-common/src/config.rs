use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlPlaneConfig {
    pub control_plane: ControlPlaneSection,
    pub registry: RegistrySection,
    #[serde(default)]
    pub images: ImagesSection,
    #[serde(default)]
    pub auth: AuthSection,
    #[serde(default)]
    pub vault: VaultSection,
    #[serde(default)]
    pub runtime: RuntimeSection,
    #[serde(default)]
    pub logging: LoggingSection,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlPlaneSection {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_unix_socket")]
    pub unix_socket: String,
    #[serde(default)]
    pub advertise_addr: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegistrySection {
    #[serde(default = "default_registry_path")]
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ImagesSection {
    #[serde(default = "default_image_store_path")]
    pub store_path: String,
    #[serde(default)]
    pub catalog_url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AuthSection {
    #[serde(default)]
    pub ca: String,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct VaultSection {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub auth: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeSection {
    #[serde(default = "default_execution_mode")]
    pub execution_mode: String,
    #[serde(default = "default_state_ttl")]
    pub state_ttl_seconds: u64,
    #[serde(default = "default_query_timeout")]
    pub query_timeout_ms: u64,
    #[serde(default = "default_fanout")]
    pub fanout_concurrency: u32,
    #[serde(default = "default_heartbeat")]
    pub agent_heartbeat_s: u64,
    #[serde(default = "default_stale_threshold")]
    pub agent_stale_threshold: u32,
    #[serde(default = "default_metrics_retention")]
    pub metrics_retention_h: u64,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            execution_mode: default_execution_mode(),
            state_ttl_seconds: default_state_ttl(),
            query_timeout_ms: default_query_timeout(),
            fanout_concurrency: default_fanout(),
            agent_heartbeat_s: default_heartbeat(),
            agent_stale_threshold: default_stale_threshold(),
            metrics_retention_h: default_metrics_retention(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingSection {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_log_path")]
    pub path: String,
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            path: default_log_path(),
        }
    }
}

// Agent config
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentConfig {
    pub agent: AgentSection,
    pub control_plane: AgentControlPlaneSection,
    pub driver: AgentDriverSection,
    #[serde(default)]
    pub images: AgentImagesSection,
    #[serde(default)]
    pub runtime: AgentRuntimeSection,
    #[serde(default)]
    pub logging: LoggingSection,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentSection {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentControlPlaneSection {
    pub address: String,
    #[serde(default)]
    pub ca: String,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentDriverSection {
    #[serde(rename = "type")]
    pub driver_type: String,
    #[serde(default)]
    pub api_socket: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AgentImagesSection {
    #[serde(default = "default_image_store_path")]
    pub store_path: String,
    #[serde(default = "default_max_store_gb")]
    pub max_store_gb: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentRuntimeSection {
    #[serde(default = "default_heartbeat")]
    pub heartbeat_s: u64,
    #[serde(default = "default_state_push")]
    pub state_push_s: u64,
    #[serde(default = "default_reconnect_max")]
    pub reconnect_max_s: u64,
}

impl Default for AgentRuntimeSection {
    fn default() -> Self {
        Self {
            heartbeat_s: default_heartbeat(),
            state_push_s: default_state_push(),
            reconnect_max_s: default_reconnect_max(),
        }
    }
}

fn default_bind() -> String { "0.0.0.0:9090".into() }
fn default_unix_socket() -> String { "/var/run/kvmql/control.sock".into() }
fn default_registry_path() -> String { "/var/kvmql/state.db".into() }
fn default_image_store_path() -> String { "/var/kvmql/images".into() }
fn default_execution_mode() -> String { "permissive".into() }
fn default_state_ttl() -> u64 { 30 }
fn default_query_timeout() -> u64 { 5000 }
fn default_fanout() -> u32 { 10 }
fn default_heartbeat() -> u64 { 5 }
fn default_stale_threshold() -> u32 { 5 }
fn default_metrics_retention() -> u64 { 1 }
fn default_log_level() -> String { "info".into() }
fn default_log_format() -> String { "json".into() }
fn default_log_path() -> String { "/var/log/kvmql/kvmql.log".into() }
fn default_max_store_gb() -> u64 { 500 }
fn default_state_push() -> u64 { 10 }
fn default_reconnect_max() -> u64 { 60 }
