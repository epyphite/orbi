pub mod govc;
pub mod resources;
pub mod vmrun;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

use govc::GovcClient;
use vmrun::VmrunClient;

/// The two CLI backends for VMware.
pub enum VmwareBackend {
    /// VMware Workstation / Fusion — local VMs via `vmrun` CLI.
    Vmrun(VmrunClient),
    /// VMware vSphere / vCenter — datacenter VMs via `govc` CLI.
    Govc(GovcClient),
}

/// VMware driver supporting both Workstation/Fusion (vmrun) and vSphere (govc).
///
/// Select the backend via the `driver` field in `ADD PROVIDER`:
/// - `driver = 'vmrun'` → local VMware Workstation / Fusion
/// - `driver = 'govc'`  → vSphere / vCenter
pub struct VmwareDriver {
    backend: VmwareBackend,
    manifest: CapabilityManifest,
}

impl VmwareDriver {
    /// Create a driver for VMware Workstation / Fusion (local VMs).
    pub fn new_vmrun() -> Self {
        Self {
            backend: VmwareBackend::Vmrun(VmrunClient::new()),
            manifest: build_vmware_manifest(false),
        }
    }

    /// Create a driver for vSphere / vCenter.
    pub fn new_govc(datacenter: Option<&str>) -> Self {
        let client = match datacenter {
            Some(dc) => GovcClient::with_datacenter(dc),
            None => GovcClient::new(),
        };
        Self {
            backend: VmwareBackend::Govc(client),
            manifest: build_vmware_manifest(true),
        }
    }

    /// Return a resource provisioner for managed resource operations.
    pub fn provisioner(&self) -> resources::VmwareResourceProvisioner {
        match &self.backend {
            VmwareBackend::Vmrun(c) => resources::VmwareResourceProvisioner {
                backend: VmwareBackend::Vmrun(c.clone()),
            },
            VmwareBackend::Govc(c) => resources::VmwareResourceProvisioner {
                backend: VmwareBackend::Govc(c.clone()),
            },
        }
    }
}

fn build_vmware_manifest(is_vsphere: bool) -> CapabilityManifest {
    use Capability::*;

    let mut capabilities = HashMap::new();

    // Both backends
    let always_supported = [Create, Destroy, Snapshot, Restore];
    for cap in always_supported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: true,
                notes: None,
            },
        );
    }

    // vmrun: pause/resume; govc: pause (suspend) supported
    for cap in [Pause, Resume] {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: true,
                notes: Some("suspend/resume".into()),
            },
        );
    }

    // vSphere-only capabilities
    let vsphere_only = [
        AlterCpuLive,
        AlterMemoryLive,
        LiveMigration,
        Placement,
        GpuPassthrough,
        NestedVirt,
    ];
    for cap in vsphere_only {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: is_vsphere,
                notes: if is_vsphere {
                    None
                } else {
                    Some("vSphere only — use govc backend".into())
                },
            },
        );
    }

    // Generally unsupported
    let unsupported = [
        CustomKernel,
        Vsock,
        Balloon,
        HotplugVolume,
        HotplugNetwork,
        VolumeResizeLive,
        VolumeEncrypt,
        ImageImport,
        ImagePublish,
        WatchMetric,
    ];
    for cap in unsupported {
        capabilities.insert(
            cap,
            CapabilityEntry {
                supported: false,
                notes: Some("not supported on VMware driver".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn cli_err(msg: String) -> DriverError {
    DriverError::Connection(msg)
}

#[async_trait]
impl Driver for VmwareDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    // ── MicroVM operations ──────────────────────────────────────────

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let id = params
            .id
            .clone()
            .unwrap_or_else(|| format!("vmware-{}", uuid::Uuid::new_v4()));

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let image = params.image_id.clone();
                let vm_id = id.clone();

                let dest = format!(
                    "{}/VMs/{vm_id}/{vm_id}.vmx",
                    std::env::var("HOME").unwrap_or_else(|_| "/tmp".into())
                );

                tokio::task::spawn_blocking(move || {
                    client.clone_vm(&image, &dest, "full")?;
                    client.start(&dest)?;
                    Ok::<_, String>(dest)
                })
                .await
                .map_err(|e| DriverError::Internal(e.to_string()))?
                .map_err(cli_err)?;
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let image = params.image_id.clone();
                let vm_id = id.clone();
                let vcpus = params.vcpus;
                let mem = params.memory_mb;

                tokio::task::spawn_blocking(move || {
                    client.vm_clone(&image, &vm_id, true)?;
                    client.vm_change(&vm_id, Some(vcpus), Some(mem))?;
                    Ok::<_, String>(())
                })
                .await
                .map_err(|e| DriverError::Internal(e.to_string()))?
                .map_err(cli_err)?;
            }
        }

        Ok(MicroVm {
            id,
            provider_id: "vmware".into(),
            tenant: params.tenant,
            status: "running".into(),
            image_id: Some(params.image_id),
            vcpus: Some(params.vcpus),
            memory_mb: Some(params.memory_mb),
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

    async fn destroy(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || {
                    let _ = client.stop(&vm_id);
                    client.delete_vm(&vm_id)
                })
                .await
                .map_err(|e| DriverError::Internal(e.to_string()))?
                .map_err(cli_err)
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || client.vm_destroy(&vm_id))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)
            }
        }
    }

    async fn pause(&self, id: &str) -> Result<(), DriverError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || client.suspend(&vm_id))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || client.vm_power(&vm_id, "suspend"))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)
            }
        }
    }

    async fn resume(&self, id: &str) -> Result<(), DriverError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || client.unpause(&vm_id))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                tokio::task::spawn_blocking(move || client.vm_power(&vm_id, "on"))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)
            }
        }
    }

    async fn alter(&self, id: &str, params: serde_json::Value) -> Result<MicroVm, DriverError> {
        match &self.backend {
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                let cpus = params
                    .get("vcpus")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32);
                let mem = params
                    .get("memory_mb")
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32);

                tokio::task::spawn_blocking(move || client.vm_change(&vm_id, cpus, mem))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;

                self.get(id).await
            }
            VmwareBackend::Vmrun(_) => Err(DriverError::Unsupported(
                "vmrun does not support live VM reconfiguration; use vSphere (govc)".into(),
            )),
        }
    }

    async fn snapshot(
        &self,
        id: &str,
        _destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        let snap_name = tag.unwrap_or("orbi-snapshot").to_string();
        let vm_id = id.to_string();

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let name = snap_name.clone();
                let vid = vm_id.clone();
                tokio::task::spawn_blocking(move || client.snapshot(&vid, &name))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let name = snap_name.clone();
                let vid = vm_id.clone();
                tokio::task::spawn_blocking(move || client.snapshot_create(&vid, &name))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;
            }
        }

        Ok(Snapshot {
            id: format!("{vm_id}/{snap_name}"),
            microvm_id: vm_id,
            provider_id: "vmware".into(),
            destination: snap_name.clone(),
            tag: Some(snap_name),
            size_mb: None,
            taken_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn restore(&self, id: &str, source: &str) -> Result<MicroVm, DriverError> {
        let vm_id = id.to_string();
        let snap_name = source.to_string();

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let vid = vm_id.clone();
                let name = snap_name.clone();
                tokio::task::spawn_blocking(move || client.revert_to_snapshot(&vid, &name))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vid = vm_id.clone();
                let name = snap_name.clone();
                tokio::task::spawn_blocking(move || client.snapshot_revert(&vid, &name))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;
            }
        }

        self.get(id).await
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                let vms = tokio::task::spawn_blocking(move || client.list())
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;

                Ok(vms
                    .into_iter()
                    .map(|vmx| {
                        let name = std::path::Path::new(&vmx)
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("unknown")
                            .to_string();
                        MicroVm {
                            id: vmx,
                            provider_id: "vmware".into(),
                            tenant: "local".into(),
                            status: "running".into(),
                            image_id: None,
                            vcpus: None,
                            memory_mb: None,
                            cpu_pct: None,
                            mem_used_mb: None,
                            net_rx_kbps: None,
                            net_tx_kbps: None,
                            hostname: Some(name),
                            metadata: None,
                            labels: None,
                            created_at: String::new(),
                            last_seen: Some(chrono::Utc::now().to_rfc3339()),
                            is_stale: false,
                        }
                    })
                    .collect())
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let paths = tokio::task::spawn_blocking(move || client.find("m"))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;

                Ok(paths
                    .into_iter()
                    .map(|path| {
                        let name = path.rsplit('/').next().unwrap_or(&path).to_string();
                        MicroVm {
                            id: name.clone(),
                            provider_id: "vmware".into(),
                            tenant: "vsphere".into(),
                            status: "discovered".into(),
                            image_id: None,
                            vcpus: None,
                            memory_mb: None,
                            cpu_pct: None,
                            mem_used_mb: None,
                            net_rx_kbps: None,
                            net_tx_kbps: None,
                            hostname: Some(name),
                            metadata: Some(serde_json::json!({"vsphere_path": path})),
                            labels: None,
                            created_at: String::new(),
                            last_seen: Some(chrono::Utc::now().to_rfc3339()),
                            is_stale: false,
                        }
                    })
                    .collect())
            }
        }
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        match &self.backend {
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                let vm_id = id.to_string();
                let info = tokio::task::spawn_blocking(move || client.vm_info(&vm_id))
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
                    .map_err(cli_err)?;

                Ok(MicroVm {
                    id: id.to_string(),
                    provider_id: "vmware".into(),
                    tenant: "vsphere".into(),
                    status: "running".into(),
                    image_id: None,
                    vcpus: info
                        .pointer("/virtualMachines/0/config/hardware/numCPU")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32),
                    memory_mb: info
                        .pointer("/virtualMachines/0/config/hardware/memoryMB")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32),
                    cpu_pct: None,
                    mem_used_mb: None,
                    net_rx_kbps: None,
                    net_tx_kbps: None,
                    hostname: Some(id.to_string()),
                    metadata: Some(info),
                    labels: None,
                    created_at: String::new(),
                    last_seen: Some(chrono::Utc::now().to_rfc3339()),
                    is_stale: false,
                })
            }
            VmwareBackend::Vmrun(_) => {
                // vmrun has no "info" command — return basic info
                Ok(MicroVm {
                    id: id.to_string(),
                    provider_id: "vmware".into(),
                    tenant: "local".into(),
                    status: "running".into(),
                    image_id: None,
                    vcpus: None,
                    memory_mb: None,
                    cpu_pct: None,
                    mem_used_mb: None,
                    net_rx_kbps: None,
                    net_tx_kbps: None,
                    hostname: None,
                    metadata: None,
                    labels: None,
                    created_at: String::new(),
                    last_seen: Some(chrono::Utc::now().to_rfc3339()),
                    is_stale: false,
                })
            }
        }
    }

    async fn metrics(&self, _id: &str) -> Result<MetricSample, DriverError> {
        Err(DriverError::Unsupported(
            "VMware metrics not yet implemented".into(),
        ))
    }

    // ── Volume operations (not supported via VMware driver) ─────────

    async fn create_volume(&self, _params: VolumeParams) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "standalone volumes not supported on VMware — use disk parameter in VM creation".into(),
        ))
    }

    async fn destroy_volume(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "VMware volume ops not supported".into(),
        ))
    }

    async fn attach_volume(
        &self,
        _vol: &str,
        _vm: &str,
        _dev: Option<&str>,
    ) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "VMware volume ops not supported".into(),
        ))
    }

    async fn detach_volume(&self, _vol: &str, _vm: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "VMware volume ops not supported".into(),
        ))
    }

    async fn resize_volume(&self, _id: &str, _size_gb: i64) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "VMware volume ops not supported".into(),
        ))
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        Err(DriverError::Unsupported(
            "VMware volume ops not supported".into(),
        ))
    }

    // ── Image operations (not supported via VMware driver) ──────────

    async fn import_image(&self, _params: ImageParams) -> Result<Image, DriverError> {
        Err(DriverError::Unsupported(
            "VMware image ops not supported".into(),
        ))
    }

    async fn remove_image(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "VMware image ops not supported".into(),
        ))
    }

    async fn resolve_image(&self, _image_id: &str) -> Result<ImageRef, DriverError> {
        Err(DriverError::Unsupported(
            "VMware image ops not supported".into(),
        ))
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        Err(DriverError::Unsupported(
            "VMware image ops not supported".into(),
        ))
    }

    // ── Health ──────────────────────────────────────────────────────

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        let start = std::time::Instant::now();

        let result = match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let client = client.clone();
                tokio::task::spawn_blocking(move || client.check_available())
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
            }
            VmwareBackend::Govc(client) => {
                let client = client.clone();
                tokio::task::spawn_blocking(move || client.check_available())
                    .await
                    .map_err(|e| DriverError::Internal(e.to_string()))?
            }
        };

        let latency = start.elapsed().as_millis() as u64;

        match result {
            Ok(()) => Ok(HealthStatus {
                healthy: true,
                latency_ms: Some(latency),
                message: Some("VMware CLI is responsive".into()),
            }),
            Err(e) => Ok(HealthStatus {
                healthy: false,
                latency_ms: Some(latency),
                message: Some(format!("VMware health check failed: {e}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::Capability;

    #[test]
    fn test_vmrun_capabilities() {
        let d = VmwareDriver::new_vmrun();
        let caps = d.capabilities();

        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(caps.supports(&Capability::Snapshot));
        assert!(caps.supports(&Capability::Restore));
        assert!(caps.supports(&Capability::Pause));
        assert!(caps.supports(&Capability::Resume));

        // vSphere-only
        assert!(!caps.supports(&Capability::AlterCpuLive));
        assert!(!caps.supports(&Capability::LiveMigration));
        assert!(!caps.supports(&Capability::GpuPassthrough));
    }

    #[test]
    fn test_govc_capabilities() {
        let d = VmwareDriver::new_govc(Some("DC-Kenya"));
        let caps = d.capabilities();

        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(caps.supports(&Capability::Snapshot));
        assert!(caps.supports(&Capability::Pause));
        assert!(caps.supports(&Capability::AlterCpuLive));
        assert!(caps.supports(&Capability::AlterMemoryLive));
        assert!(caps.supports(&Capability::LiveMigration));
        assert!(caps.supports(&Capability::GpuPassthrough));
    }

    #[tokio::test]
    async fn test_vmrun_alter_unsupported() {
        let d = VmwareDriver::new_vmrun();
        let err = d.alter("test", serde_json::json!({})).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_volume_ops_unsupported() {
        let d = VmwareDriver::new_vmrun();
        assert!(d
            .create_volume(VolumeParams {
                id: None,
                size_gb: 10,
                vol_type: "ssd".into(),
                encrypted: false,
                iops: None,
                labels: None,
            })
            .await
            .is_err());
        assert!(d.destroy_volume("v", false).await.is_err());
        assert!(d.list_volumes().await.is_err());
    }

    #[tokio::test]
    async fn test_metrics_unsupported() {
        let d = VmwareDriver::new_govc(None);
        let err = d.metrics("vm-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }
}
