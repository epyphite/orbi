pub mod cli;
pub mod mapper;
pub mod resources;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

use cli::AzureCli;

/// Azure VM driver that shells out to the `az` CLI.
///
/// Each driver instance is bound to a subscription and optionally a default
/// resource group. All blocking `az` invocations are wrapped in
/// `tokio::task::spawn_blocking` so they don't stall the async runtime.
pub struct AzureVmDriver {
    subscription: String,
    resource_group: Option<String>,
    cli: AzureCli,
    capabilities: CapabilityManifest,
}

impl AzureVmDriver {
    /// Create a driver bound to a subscription (uses whatever resource group
    /// is passed per-call or inferred from labels).
    pub fn new(subscription: &str) -> Self {
        let cli = AzureCli::new()
            .with_subscription(subscription);
        Self {
            subscription: subscription.to_string(),
            resource_group: None,
            cli,
            capabilities: build_azure_manifest(),
        }
    }

    /// Create a driver bound to both a subscription and a default resource group.
    pub fn with_resource_group(subscription: &str, rg: &str) -> Self {
        let cli = AzureCli::new()
            .with_subscription(subscription)
            .with_resource_group(rg);
        Self {
            subscription: subscription.to_string(),
            resource_group: Some(rg.to_string()),
            cli,
            capabilities: build_azure_manifest(),
        }
    }

    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// Return the effective resource group: either the one explicitly provided,
    /// the driver default, or an error.
    fn effective_rg(&self, explicit: Option<&str>) -> Result<String, DriverError> {
        explicit
            .map(|s| s.to_string())
            .or_else(|| self.resource_group.clone())
            .ok_or_else(|| {
                DriverError::Internal(
                    "no resource group specified and no default configured".into(),
                )
            })
    }

    /// Extract resource group from a VM's labels (set by our mapper), falling
    /// back to the driver default.
    fn rg_from_id_or_default(&self, _id: &str) -> Result<String, DriverError> {
        self.resource_group.clone().ok_or_else(|| {
            DriverError::Internal(
                "no resource group configured; pass one via labels or use with_resource_group"
                    .into(),
            )
        })
    }
}

fn build_azure_manifest() -> CapabilityManifest {
    use Capability::*;

    let supported = [
        Create, Destroy, Snapshot, Restore,
        WatchMetric, Placement, VolumeEncrypt,
        ImageImport, ImagePublish,
    ];

    let unsupported = [
        Pause, Resume, CustomKernel, Vsock, Balloon,
        HotplugVolume, HotplugNetwork, LiveMigration,
        NestedVirt, GpuPassthrough,
        AlterCpuLive, AlterMemoryLive, VolumeResizeLive,
    ];

    let mut capabilities = HashMap::new();

    for cap in supported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: true,
                notes: None,
            },
        );
    }

    for cap in unsupported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: false,
                notes: Some("not supported on Azure VM".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn cli_err(msg: String) -> DriverError {
    DriverError::Connection(msg)
}

#[async_trait]
impl Driver for AzureVmDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.capabilities
    }

    // ── MicroVM operations ──────────────────────────────────────────

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.subscription.clone();
        let rg = self.effective_rg(
            params
                .labels
                .as_ref()
                .and_then(|l| l.get("resource_group"))
                .and_then(|v| v.as_str()),
        )?;

        let vm_name = params
            .id
            .clone()
            .unwrap_or_else(|| format!("kvmql-{}", uuid::Uuid::new_v4()));

        let image_id = params.image_id.clone();
        let vm_size = mapper::specs_to_vm_size(params.vcpus, params.memory_mb).to_string();
        let hostname = params.hostname.clone();
        let admin_user = params.admin_user.clone();
        let ssh_key = params.ssh_key.clone();
        let cloud_init = params.cloud_init.clone();
        let password = params.password.clone();

        let name_clone = vm_name.clone();
        let rg_clone = rg.clone();

        let result = tokio::task::spawn_blocking(move || {
            // Collect owned strings that extra args will reference.
            let mut extra_owned: Vec<String> = Vec::new();

            // We need to store the hostname in a variable that lives long enough.
            if let Some(ref h) = hostname {
                extra_owned.push("--computer-name".into());
                extra_owned.push(h.clone());
            }

            // SSH access
            if let Some(ref user) = admin_user {
                extra_owned.push("--admin-username".into());
                extra_owned.push(user.clone());
            }
            if let Some(ref key) = ssh_key {
                if key == "generate" {
                    extra_owned.push("--generate-ssh-keys".into());
                } else {
                    extra_owned.push("--ssh-key-values".into());
                    extra_owned.push(key.clone());
                }
            }

            // Cloud-init / custom-data
            if let Some(ref init) = cloud_init {
                let init_path = if init.starts_with('/') || init.starts_with("./") {
                    init.clone()
                } else {
                    let tmp = std::env::temp_dir()
                        .join(format!("kvmql-init-{}", name_clone));
                    std::fs::write(&tmp, init)
                        .map_err(|e| format!("failed to write cloud-init temp file: {e}"))?;
                    tmp.to_string_lossy().to_string()
                };
                extra_owned.push("--custom-data".into());
                extra_owned.push(init_path);
            }

            // Password authentication (discouraged)
            if let Some(ref pass) = password {
                extra_owned.push("--admin-password".into());
                extra_owned.push(pass.clone());
                extra_owned.push("--authentication-type".into());
                extra_owned.push("password".into());
            }

            let extra_refs: Vec<&str> = extra_owned.iter().map(|s| s.as_str()).collect();
            cli.vm_create(&name_clone, &image_id, &vm_size, &rg_clone, &extra_refs)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        Ok(mapper::map_vm(&result, &provider_id))
    }

    async fn destroy(&self, id: &str, force: bool) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(id)?;
        let name = id.to_string();

        tokio::task::spawn_blocking(move || cli.vm_delete(&name, &rg, force))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn pause(&self, _id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "Azure does not support pause/resume; use stop/start".into(),
        ))
    }

    async fn resume(&self, _id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "Azure does not support pause/resume; use stop/start".into(),
        ))
    }

    async fn alter(&self, _id: &str, _params: serde_json::Value) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "Azure does not support live VM alteration".into(),
        ))
    }

    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(id)?;
        let provider_id = self.subscription.clone();
        let vm_name = id.to_string();
        let snap_name = destination.to_string();
        let rg_clone = rg.clone();

        // First, get the VM to find its OS disk name.
        let vm_json = tokio::task::spawn_blocking({
            let cli = cli.clone();
            let vm = vm_name.clone();
            let rg = rg_clone.clone();
            move || cli.vm_show(&vm, &rg)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        let os_disk_name = vm_json["storageProfile"]["osDisk"]["name"]
            .as_str()
            .ok_or_else(|| {
                DriverError::Internal(format!("could not find OS disk for VM {vm_name}"))
            })?
            .to_string();

        // Create snapshot from the OS disk.
        let result = tokio::task::spawn_blocking(move || {
            cli.snapshot_create(&snap_name, &os_disk_name, &rg_clone)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        let mut snapshot = mapper::map_snapshot(&result, &provider_id);
        // Override microvm_id and tag with the values we know.
        snapshot.microvm_id = vm_name;
        if let Some(t) = tag {
            snapshot.tag = Some(t.to_string());
        }
        Ok(snapshot)
    }

    async fn restore(&self, _id: &str, _source: &str) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "Azure snapshot restore is not yet implemented; recreate the VM from a snapshot-based image".into(),
        ))
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        let cli = self.cli.clone();
        let rg = self
            .resource_group
            .clone()
            .ok_or_else(|| {
                DriverError::Internal(
                    "resource group required for listing VMs".into(),
                )
            })?;
        let provider_id = self.subscription.clone();

        let vms = tokio::task::spawn_blocking(move || cli.vm_list(&rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(vms
            .iter()
            .map(|v| mapper::map_vm(v, &provider_id))
            .collect())
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(id)?;
        let provider_id = self.subscription.clone();
        let name = id.to_string();

        let json = tokio::task::spawn_blocking(move || cli.vm_show(&name, &rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(mapper::map_vm(&json, &provider_id))
    }

    async fn metrics(&self, _id: &str) -> Result<MetricSample, DriverError> {
        Err(DriverError::Unsupported(
            "Azure metrics are available via Azure Monitor, not the az CLI".into(),
        ))
    }

    // ── Volume operations ───────────────────────────────────────────

    async fn create_volume(&self, params: VolumeParams) -> Result<Volume, DriverError> {
        let cli = self.cli.clone();
        let rg = self.effective_rg(
            params
                .labels
                .as_ref()
                .and_then(|l| l.get("resource_group"))
                .and_then(|v| v.as_str()),
        )?;
        let provider_id = self.subscription.clone();

        let disk_name = params
            .id
            .clone()
            .unwrap_or_else(|| format!("kvmql-disk-{}", uuid::Uuid::new_v4()));
        let size_gb = params.size_gb;

        let result = tokio::task::spawn_blocking(move || {
            cli.disk_create(&disk_name, size_gb, &rg)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        Ok(mapper::map_disk(&result, &provider_id))
    }

    async fn destroy_volume(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(id)?;
        let name = id.to_string();

        tokio::task::spawn_blocking(move || cli.disk_delete(&name, &rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        _device: Option<&str>,
    ) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(vm_id)?;
        let vm = vm_id.to_string();
        let disk = vol_id.to_string();

        tokio::task::spawn_blocking(move || cli.vm_disk_attach(&vm, &disk, &rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn detach_volume(&self, vol_id: &str, vm_id: &str) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let rg = self.rg_from_id_or_default(vm_id)?;
        let vm = vm_id.to_string();
        let disk = vol_id.to_string();

        tokio::task::spawn_blocking(move || cli.vm_disk_detach(&vm, &disk, &rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn resize_volume(&self, _id: &str, _size_gb: i64) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "Azure does not support live volume resize via az CLI".into(),
        ))
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        let cli = self.cli.clone();
        let rg = self
            .resource_group
            .clone()
            .ok_or_else(|| {
                DriverError::Internal(
                    "resource group required for listing disks".into(),
                )
            })?;
        let provider_id = self.subscription.clone();

        let disks = tokio::task::spawn_blocking(move || cli.disk_list(&rg))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(disks
            .iter()
            .map(|d| mapper::map_disk(d, &provider_id))
            .collect())
    }

    // ── Image operations ────────────────────────────────────────────

    async fn import_image(&self, _params: ImageParams) -> Result<Image, DriverError> {
        Err(DriverError::Unsupported(
            "Azure image management is not yet supported via az CLI driver".into(),
        ))
    }

    async fn remove_image(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "Azure image management is not yet supported via az CLI driver".into(),
        ))
    }

    async fn resolve_image(&self, _image_id: &str) -> Result<ImageRef, DriverError> {
        Err(DriverError::Unsupported(
            "Azure image management is not yet supported via az CLI driver".into(),
        ))
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        Err(DriverError::Unsupported(
            "Azure image management is not yet supported via az CLI driver".into(),
        ))
    }

    // ── Health ──────────────────────────────────────────────────────

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        let cli = self.cli.clone();
        let start = std::time::Instant::now();

        let result = tokio::task::spawn_blocking(move || cli.account_show())
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?;

        let latency = start.elapsed().as_millis() as u64;

        match result {
            Ok(_) => Ok(HealthStatus {
                healthy: true,
                latency_ms: Some(latency),
                message: Some("Azure CLI is responsive".into()),
            }),
            Err(e) => Ok(HealthStatus {
                healthy: false,
                latency_ms: Some(latency),
                message: Some(format!("Azure health check failed: {e}")),
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
        let d = AzureVmDriver::new("sub-12345");
        let caps = d.capabilities();

        // Supported
        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(caps.supports(&Capability::Snapshot));
        assert!(caps.supports(&Capability::Restore));
        assert!(caps.supports(&Capability::WatchMetric));
        assert!(caps.supports(&Capability::Placement));
        assert!(caps.supports(&Capability::VolumeEncrypt));
        assert!(caps.supports(&Capability::ImageImport));
        assert!(caps.supports(&Capability::ImagePublish));

        // Not supported
        assert!(!caps.supports(&Capability::Pause));
        assert!(!caps.supports(&Capability::Resume));
        assert!(!caps.supports(&Capability::CustomKernel));
        assert!(!caps.supports(&Capability::Vsock));
        assert!(!caps.supports(&Capability::Balloon));
        assert!(!caps.supports(&Capability::HotplugVolume));
        assert!(!caps.supports(&Capability::HotplugNetwork));
        assert!(!caps.supports(&Capability::LiveMigration));
        assert!(!caps.supports(&Capability::NestedVirt));
        assert!(!caps.supports(&Capability::GpuPassthrough));
        assert!(!caps.supports(&Capability::AlterCpuLive));
        assert!(!caps.supports(&Capability::AlterMemoryLive));
        assert!(!caps.supports(&Capability::VolumeResizeLive));
    }

    #[test]
    fn test_constructor_new() {
        let d = AzureVmDriver::new("sub-abc");
        assert_eq!(d.subscription(), "sub-abc");
        assert!(d.resource_group.is_none());
    }

    #[test]
    fn test_constructor_with_resource_group() {
        let d = AzureVmDriver::with_resource_group("sub-abc", "my-rg");
        assert_eq!(d.subscription(), "sub-abc");
        assert_eq!(d.resource_group.as_deref(), Some("my-rg"));
    }

    #[tokio::test]
    async fn test_pause_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.pause("some-vm").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
        assert!(err.to_string().contains("pause/resume"));
    }

    #[tokio::test]
    async fn test_resume_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.resume("some-vm").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
        assert!(err.to_string().contains("pause/resume"));
    }

    #[tokio::test]
    async fn test_alter_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d
            .alter("vm-1", serde_json::json!({"vcpus": 4}))
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_restore_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.restore("vm-1", "snap-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_resize_volume_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.resize_volume("disk-1", 100).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_image_ops_unsupported() {
        let d = AzureVmDriver::new("sub-12345");

        let err = d.list_images().await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));

        let err = d.resolve_image("img-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));

        let err = d.remove_image("img-1", false).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_metrics_unsupported() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.metrics("vm-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[test]
    fn test_effective_rg_explicit() {
        let d = AzureVmDriver::new("sub-12345");
        let rg = d.effective_rg(Some("explicit-rg")).unwrap();
        assert_eq!(rg, "explicit-rg");
    }

    #[test]
    fn test_effective_rg_default() {
        let d = AzureVmDriver::with_resource_group("sub-12345", "default-rg");
        let rg = d.effective_rg(None).unwrap();
        assert_eq!(rg, "default-rg");
    }

    #[test]
    fn test_effective_rg_missing() {
        let d = AzureVmDriver::new("sub-12345");
        let err = d.effective_rg(None).unwrap_err();
        assert!(matches!(err, DriverError::Internal(_)));
    }
}
