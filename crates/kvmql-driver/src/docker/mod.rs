pub mod cli;
pub mod resources;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

use cli::DockerCli;

/// Docker driver for container lifecycle and Compose stack management.
///
/// Supports both individual containers (`docker run/stop/rm`) and
/// multi-container stacks (`docker compose up/down/scale`).
///
/// ```sql
/// -- Local Docker daemon
/// ADD PROVIDER id = 'docker-local'
///   type = 'docker' driver = 'docker'
///   auth = 'none';
///
/// -- Remote Docker host via SSH
/// ADD PROVIDER id = 'docker-prod'
///   type = 'docker' driver = 'docker'
///   host = 'ssh://deploy@prod.dc.local'
///   auth = 'file:~/.ssh/id_ed25519';
/// ```
pub struct DockerDriver {
    cli: DockerCli,
    manifest: CapabilityManifest,
}

impl DockerDriver {
    /// Create a driver for the local Docker daemon.
    pub fn new() -> Self {
        Self {
            cli: DockerCli::new(),
            manifest: build_docker_manifest(),
        }
    }

    /// Create a driver for a remote Docker host.
    pub fn with_host(host: &str) -> Self {
        Self {
            cli: DockerCli::with_host(host),
            manifest: build_docker_manifest(),
        }
    }

    /// Return a resource provisioner.
    pub fn provisioner(&self) -> resources::DockerResourceProvisioner {
        resources::DockerResourceProvisioner {
            cli: self.cli.clone(),
        }
    }
}

impl Default for DockerDriver {
    fn default() -> Self {
        Self::new()
    }
}

fn build_docker_manifest() -> CapabilityManifest {
    use Capability::*;

    let mut capabilities = HashMap::new();

    let supported = [Create, Destroy];
    for cap in supported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: true,
                notes: None,
            },
        );
    }

    let unsupported = [
        Pause,
        Resume,
        Snapshot,
        Restore,
        AlterCpuLive,
        AlterMemoryLive,
        WatchMetric,
        Placement,
        CustomKernel,
        Vsock,
        Balloon,
        HotplugVolume,
        HotplugNetwork,
        LiveMigration,
        NestedVirt,
        GpuPassthrough,
        VolumeResizeLive,
        VolumeEncrypt,
        ImageImport,
        ImagePublish,
    ];
    for cap in unsupported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: false,
                notes: Some("not applicable to Docker containers".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn cli_err(msg: String) -> DriverError {
    DriverError::Connection(msg)
}

#[async_trait]
impl Driver for DockerDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let name = params
            .id
            .clone()
            .unwrap_or_else(|| format!("orbi-{}", uuid::Uuid::new_v4()));
        let image = params.image_id.clone();

        let container_id = tokio::task::spawn_blocking(move || cli.run(&name, &image, &[]))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(MicroVm {
            id: container_id,
            provider_id: "docker".into(),
            tenant: params.tenant,
            status: "running".into(),
            image_id: Some(params.image_id),
            vcpus: None,
            memory_mb: None,
            cpu_pct: None,
            mem_used_mb: None,
            net_rx_kbps: None,
            net_tx_kbps: None,
            hostname: params.hostname,
            metadata: params.metadata,
            labels: params.labels,
            created_at: chrono::Utc::now().to_rfc3339(),
            last_seen: None,
            is_stale: false,
        })
    }

    async fn destroy(&self, id: &str, force: bool) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let name = id.to_string();
        tokio::task::spawn_blocking(move || cli.rm(&name, force))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn pause(&self, id: &str) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let name = id.to_string();
        tokio::task::spawn_blocking(move || cli.stop(&name))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn resume(&self, id: &str) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let name = id.to_string();
        tokio::task::spawn_blocking(move || cli.start(&name))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn alter(&self, _id: &str, _params: serde_json::Value) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "Docker containers are immutable; destroy and recreate with new params".into(),
        ))
    }

    async fn snapshot(
        &self,
        _id: &str,
        _destination: &str,
        _tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        Err(DriverError::Unsupported(
            "Docker does not support snapshots; use docker commit or image layers".into(),
        ))
    }

    async fn restore(&self, _id: &str, _source: &str) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "Docker does not support restore; recreate from image".into(),
        ))
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        let cli = self.cli.clone();
        let containers = tokio::task::spawn_blocking(move || cli.ps_json())
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(containers
            .into_iter()
            .map(|c| {
                let name = c["Names"]
                    .as_str()
                    .unwrap_or(c["ID"].as_str().unwrap_or("?"));
                let status_str = c["Status"].as_str().unwrap_or("unknown");
                let status = if status_str.contains("Up") {
                    "running"
                } else {
                    "stopped"
                };
                MicroVm {
                    id: name.to_string(),
                    provider_id: "docker".into(),
                    tenant: "local".into(),
                    status: status.into(),
                    image_id: c["Image"].as_str().map(|s| s.to_string()),
                    vcpus: None,
                    memory_mb: None,
                    cpu_pct: None,
                    mem_used_mb: None,
                    net_rx_kbps: None,
                    net_tx_kbps: None,
                    hostname: Some(name.to_string()),
                    metadata: None,
                    labels: None,
                    created_at: c["CreatedAt"].as_str().unwrap_or("").to_string(),
                    last_seen: Some(chrono::Utc::now().to_rfc3339()),
                    is_stale: false,
                }
            })
            .collect())
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let name = id.to_string();
        let info = tokio::task::spawn_blocking(move || cli.inspect(&name))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        let status = info
            .pointer("/State/Status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        Ok(MicroVm {
            id: id.to_string(),
            provider_id: "docker".into(),
            tenant: "local".into(),
            status: status.into(),
            image_id: info
                .pointer("/Config/Image")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            vcpus: None,
            memory_mb: None,
            cpu_pct: None,
            mem_used_mb: None,
            net_rx_kbps: None,
            net_tx_kbps: None,
            hostname: info
                .pointer("/Config/Hostname")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            metadata: Some(info),
            labels: None,
            created_at: String::new(),
            last_seen: Some(chrono::Utc::now().to_rfc3339()),
            is_stale: false,
        })
    }

    async fn metrics(&self, _id: &str) -> Result<MetricSample, DriverError> {
        Err(DriverError::Unsupported(
            "Docker metrics: use docker stats or Prometheus".into(),
        ))
    }

    // ── Volume / Image — not applicable at Driver trait level ───────

    async fn create_volume(&self, _p: VolumeParams) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "use CREATE RESOURCE 'docker_volume' instead".into(),
        ))
    }
    async fn destroy_volume(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported("use DESTROY RESOURCE".into()))
    }
    async fn attach_volume(
        &self,
        _v: &str,
        _vm: &str,
        _d: Option<&str>,
    ) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "Docker volumes are attached at container creation via 'volumes' param".into(),
        ))
    }
    async fn detach_volume(&self, _v: &str, _vm: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "Docker volumes: recreate container".into(),
        ))
    }
    async fn resize_volume(&self, _id: &str, _s: i64) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "Docker volumes cannot be resized".into(),
        ))
    }
    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        Err(DriverError::Unsupported(
            "use SELECT * FROM resources WHERE resource_type = 'docker_volume'".into(),
        ))
    }

    async fn import_image(&self, _p: ImageParams) -> Result<Image, DriverError> {
        Err(DriverError::Unsupported("use docker pull".into()))
    }
    async fn remove_image(&self, _id: &str, _f: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported("use docker rmi".into()))
    }
    async fn resolve_image(&self, _id: &str) -> Result<ImageRef, DriverError> {
        Err(DriverError::Unsupported(
            "Docker images are referenced by name:tag".into(),
        ))
    }
    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        Err(DriverError::Unsupported("use docker images".into()))
    }

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        let cli = self.cli.clone();
        let start = std::time::Instant::now();
        let result = tokio::task::spawn_blocking(move || cli.check_available())
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?;
        let latency = start.elapsed().as_millis() as u64;

        match result {
            Ok(()) => Ok(HealthStatus {
                healthy: true,
                latency_ms: Some(latency),
                message: Some("Docker daemon is responsive".into()),
            }),
            Err(e) => Ok(HealthStatus {
                healthy: false,
                latency_ms: Some(latency),
                message: Some(format!("Docker health check failed: {e}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::Capability;

    #[test]
    fn test_capabilities() {
        let d = DockerDriver::new();
        let caps = d.capabilities();

        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(!caps.supports(&Capability::Snapshot));
        assert!(!caps.supports(&Capability::LiveMigration));
        assert!(!caps.supports(&Capability::Pause)); // stop != pause in Docker semantics
    }

    #[test]
    fn test_with_host() {
        // Verify it constructs without panicking; host is private
        let _d = DockerDriver::with_host("ssh://deploy@prod");
    }

    #[tokio::test]
    async fn test_alter_unsupported() {
        let d = DockerDriver::new();
        let err = d.alter("test", serde_json::json!({})).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
        assert!(err.to_string().contains("immutable"));
    }

    #[tokio::test]
    async fn test_snapshot_unsupported() {
        let d = DockerDriver::new();
        let err = d.snapshot("c1", "/tmp", None).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_metrics_unsupported() {
        let d = DockerDriver::new();
        let err = d.metrics("c1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }
}
