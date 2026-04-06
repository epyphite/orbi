use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

struct MockState {
    vms: HashMap<String, MicroVm>,
    volumes: HashMap<String, Volume>,
    images: HashMap<String, Image>,
    snapshots: HashMap<String, Snapshot>,
}

pub struct MockDriver {
    state: RwLock<MockState>,
    manifest: CapabilityManifest,
}

impl MockDriver {
    pub fn new() -> Self {
        let manifest = build_mock_manifest();
        Self {
            state: RwLock::new(MockState {
                vms: HashMap::new(),
                volumes: HashMap::new(),
                images: HashMap::new(),
                snapshots: HashMap::new(),
            }),
            manifest,
        }
    }
}

impl Default for MockDriver {
    fn default() -> Self {
        Self::new()
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn build_mock_manifest() -> CapabilityManifest {
    use Capability::*;

    let supported = [
        Create, Destroy, Pause, Resume, Snapshot, Restore,
        AlterCpuLive, AlterMemoryLive, WatchMetric,
        CustomKernel, Vsock, Balloon, HotplugVolume, HotplugNetwork,
        VolumeResizeLive, ImageImport, ImagePublish, NestedVirt,
    ];

    let unsupported = [
        Placement, LiveMigration, GpuPassthrough, VolumeEncrypt,
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
                notes: Some("not supported by mock driver".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

#[async_trait]
impl Driver for MockDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    // ── MicroVM operations ──────────────────────────────────────────

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let id = params.id.unwrap_or_else(new_id);
        let mut state = self.state.write().await;

        if state.vms.contains_key(&id) {
            return Err(DriverError::AlreadyExists(format!("VM {id}")));
        }

        let vm = MicroVm {
            id: id.clone(),
            provider_id: format!("mock-{id}"),
            tenant: params.tenant,
            status: "running".into(),
            image_id: Some(params.image_id),
            vcpus: Some(params.vcpus),
            memory_mb: Some(params.memory_mb),
            cpu_pct: Some(0.0),
            mem_used_mb: Some(0),
            net_rx_kbps: Some(0.0),
            net_tx_kbps: Some(0.0),
            hostname: params.hostname,
            metadata: params.metadata,
            labels: params.labels,
            created_at: now_iso(),
            last_seen: Some(now_iso()),
            is_stale: false,
        };

        state.vms.insert(id, vm.clone());
        Ok(vm)
    }

    async fn destroy(&self, id: &str, force: bool) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vm = state
            .vms
            .get(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if !force && vm.status == "paused" {
            return Err(DriverError::InvalidState(
                "cannot destroy paused VM without force".into(),
            ));
        }

        // Check for attached volumes
        if !force {
            let attached: Vec<_> = state
                .volumes
                .values()
                .filter(|v| v.microvm_id.as_deref() == Some(id))
                .map(|v| v.id.clone())
                .collect();
            if !attached.is_empty() {
                return Err(DriverError::InvalidState(format!(
                    "VM {id} has attached volumes: {}; use force=true or detach first",
                    attached.join(", ")
                )));
            }
        }

        // Detach any volumes if forcing
        if force {
            for vol in state.volumes.values_mut() {
                if vol.microvm_id.as_deref() == Some(id) {
                    vol.microvm_id = None;
                    vol.status = "available".into();
                    vol.device_name = None;
                }
            }
        }

        state.vms.remove(id);
        Ok(())
    }

    async fn pause(&self, id: &str) -> Result<(), DriverError> {
        let mut state = self.state.write().await;
        let vm = state
            .vms
            .get_mut(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.status != "running" {
            return Err(DriverError::InvalidState(format!(
                "cannot pause VM in state '{}'",
                vm.status
            )));
        }

        vm.status = "paused".into();
        Ok(())
    }

    async fn resume(&self, id: &str) -> Result<(), DriverError> {
        let mut state = self.state.write().await;
        let vm = state
            .vms
            .get_mut(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.status != "paused" {
            return Err(DriverError::InvalidState(format!(
                "cannot resume VM in state '{}'",
                vm.status
            )));
        }

        vm.status = "running".into();
        Ok(())
    }

    async fn alter(&self, id: &str, params: serde_json::Value) -> Result<MicroVm, DriverError> {
        let mut state = self.state.write().await;
        let vm = state
            .vms
            .get_mut(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.status != "running" {
            return Err(DriverError::InvalidState(format!(
                "cannot alter VM in state '{}'",
                vm.status
            )));
        }

        if let Some(vcpus) = params.get("vcpus").and_then(|v| v.as_i64()) {
            vm.vcpus = Some(vcpus as i32);
        }
        if let Some(memory_mb) = params.get("memory_mb").and_then(|v| v.as_i64()) {
            vm.memory_mb = Some(memory_mb as i32);
        }

        Ok(vm.clone())
    }

    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        let mut state = self.state.write().await;
        let vm = state
            .vms
            .get(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.status != "running" && vm.status != "paused" {
            return Err(DriverError::InvalidState(format!(
                "cannot snapshot VM in state '{}'",
                vm.status
            )));
        }

        let snap_id = new_id();
        let snapshot = Snapshot {
            id: snap_id.clone(),
            microvm_id: id.to_string(),
            provider_id: format!("mock-snap-{snap_id}"),
            destination: destination.to_string(),
            tag: tag.map(|t| t.to_string()),
            size_mb: Some(256),
            taken_at: now_iso(),
        };

        state.snapshots.insert(snap_id, snapshot.clone());
        Ok(snapshot)
    }

    async fn restore(&self, id: &str, source: &str) -> Result<MicroVm, DriverError> {
        let mut state = self.state.write().await;

        // Find snapshot by source (match destination)
        let snapshot = state
            .snapshots
            .values()
            .find(|s| s.destination == source || s.id == source)
            .cloned()
            .ok_or_else(|| DriverError::NotFound(format!("snapshot from source '{source}'")))?;

        let vm_id = if state.vms.contains_key(id) {
            id.to_string()
        } else {
            id.to_string()
        };

        let vm = MicroVm {
            id: vm_id.clone(),
            provider_id: format!("mock-{vm_id}"),
            tenant: "restored".into(),
            status: "running".into(),
            image_id: None,
            vcpus: Some(1),
            memory_mb: Some(512),
            cpu_pct: Some(0.0),
            mem_used_mb: Some(0),
            net_rx_kbps: Some(0.0),
            net_tx_kbps: Some(0.0),
            hostname: None,
            metadata: None,
            labels: None,
            created_at: now_iso(),
            last_seen: Some(now_iso()),
            is_stale: false,
        };

        let _ = &snapshot; // acknowledge use
        state.vms.insert(vm_id, vm.clone());
        Ok(vm)
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        let state = self.state.read().await;
        Ok(state.vms.values().cloned().collect())
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        let state = self.state.read().await;
        state
            .vms
            .get(id)
            .cloned()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))
    }

    async fn metrics(&self, id: &str) -> Result<MetricSample, DriverError> {
        let state = self.state.read().await;
        let vm = state
            .vms
            .get(id)
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        Ok(MetricSample {
            microvm_id: vm.id.clone(),
            sampled_at: now_iso(),
            cpu_pct: vm.cpu_pct,
            mem_used_mb: vm.mem_used_mb,
            net_rx_kbps: vm.net_rx_kbps,
            net_tx_kbps: vm.net_tx_kbps,
        })
    }

    // ── Volume operations ───────────────────────────────────────────

    async fn create_volume(&self, params: VolumeParams) -> Result<Volume, DriverError> {
        let id = params.id.unwrap_or_else(new_id);
        let mut state = self.state.write().await;

        if state.volumes.contains_key(&id) {
            return Err(DriverError::AlreadyExists(format!("Volume {id}")));
        }

        let vol = Volume {
            id: id.clone(),
            provider_id: format!("mock-vol-{id}"),
            microvm_id: None,
            vol_type: params.vol_type,
            size_gb: params.size_gb,
            status: "available".into(),
            device_name: None,
            iops: params.iops,
            encrypted: params.encrypted,
            created_at: now_iso(),
            labels: params.labels,
        };

        state.volumes.insert(id, vol.clone());
        Ok(vol)
    }

    async fn destroy_volume(&self, id: &str, force: bool) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vol = state
            .volumes
            .get(id)
            .ok_or_else(|| DriverError::NotFound(format!("Volume {id}")))?;

        if vol.microvm_id.is_some() && !force {
            return Err(DriverError::InvalidState(
                "cannot destroy attached volume without force".into(),
            ));
        }

        state.volumes.remove(id);
        Ok(())
    }

    async fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        device: Option<&str>,
    ) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        // Verify VM exists
        if !state.vms.contains_key(vm_id) {
            return Err(DriverError::NotFound(format!("VM {vm_id}")));
        }

        let vol = state
            .volumes
            .get_mut(vol_id)
            .ok_or_else(|| DriverError::NotFound(format!("Volume {vol_id}")))?;

        if vol.microvm_id.is_some() {
            return Err(DriverError::InvalidState(format!(
                "volume {vol_id} is already attached"
            )));
        }

        vol.microvm_id = Some(vm_id.to_string());
        vol.status = "attached".into();
        vol.device_name = device.map(|d| d.to_string()).or(Some("/dev/vdb".into()));

        Ok(())
    }

    async fn detach_volume(&self, vol_id: &str, vm_id: &str) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vol = state
            .volumes
            .get_mut(vol_id)
            .ok_or_else(|| DriverError::NotFound(format!("Volume {vol_id}")))?;

        match &vol.microvm_id {
            Some(attached_vm) if attached_vm == vm_id => {
                vol.microvm_id = None;
                vol.status = "available".into();
                vol.device_name = None;
                Ok(())
            }
            Some(attached_vm) => Err(DriverError::InvalidState(format!(
                "volume {vol_id} is attached to {attached_vm}, not {vm_id}"
            ))),
            None => Err(DriverError::InvalidState(format!(
                "volume {vol_id} is not attached"
            ))),
        }
    }

    async fn resize_volume(&self, id: &str, size_gb: i64) -> Result<Volume, DriverError> {
        let mut state = self.state.write().await;
        let vol = state
            .volumes
            .get_mut(id)
            .ok_or_else(|| DriverError::NotFound(format!("Volume {id}")))?;

        if size_gb <= vol.size_gb {
            return Err(DriverError::InvalidState(
                "new size must be larger than current size".into(),
            ));
        }

        vol.size_gb = size_gb;
        Ok(vol.clone())
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        let state = self.state.read().await;
        Ok(state.volumes.values().cloned().collect())
    }

    // ── Image operations ────────────────────────────────────────────

    async fn import_image(&self, params: ImageParams) -> Result<Image, DriverError> {
        let mut state = self.state.write().await;

        if state.images.contains_key(&params.id) {
            return Err(DriverError::AlreadyExists(format!("Image {}", params.id)));
        }

        let image = Image {
            id: params.id.clone(),
            name: params.name,
            os: params.os,
            distro: params.distro,
            version: params.version,
            arch: params.arch,
            image_type: params.image_type,
            provider_id: Some(format!("mock-img-{}", params.id)),
            kernel_path: params.kernel,
            rootfs_path: params.rootfs,
            disk_path: None,
            cloud_ref: None,
            source: params.source,
            checksum_sha256: params.checksum,
            size_mb: Some(512),
            status: "available".into(),
            imported_at: now_iso(),
            labels: params.labels,
        };

        state.images.insert(image.id.clone(), image.clone());
        Ok(image)
    }

    async fn remove_image(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        if !state.images.contains_key(id) {
            return Err(DriverError::NotFound(format!("Image {id}")));
        }

        state.images.remove(id);
        Ok(())
    }

    async fn resolve_image(&self, image_id: &str) -> Result<ImageRef, DriverError> {
        let state = self.state.read().await;
        let img = state
            .images
            .get(image_id)
            .ok_or_else(|| DriverError::NotFound(format!("Image {image_id}")))?;

        Ok(ImageRef {
            image_id: img.id.clone(),
            provider_id: img.provider_id.clone().unwrap_or_default(),
            resolved_type: img.image_type.clone(),
            kernel_path: img.kernel_path.clone(),
            rootfs_path: img.rootfs_path.clone(),
            cloud_ref: img.cloud_ref.clone(),
        })
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        let state = self.state.read().await;
        Ok(state.images.values().cloned().collect())
    }

    // ── Health ──────────────────────────────────────────────────────

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        Ok(HealthStatus {
            healthy: true,
            latency_ms: Some(0),
            message: Some("mock driver is always healthy".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock() -> MockDriver {
        MockDriver::new()
    }

    fn vm_params(tenant: &str) -> CreateParams {
        CreateParams {
            id: None,
            tenant: tenant.into(),
            vcpus: 2,
            memory_mb: 512,
            image_id: "img-test".into(),
            hostname: Some("test-vm".into()),
            network: None,
            metadata: None,
            labels: None,
            ssh_key: None,
            ssh_key_ref: None,
            admin_user: None,
            cloud_init: None,
            cloud_init_ref: None,
            password: None,
        }
    }

    fn vol_params() -> VolumeParams {
        VolumeParams {
            id: None,
            size_gb: 10,
            vol_type: "virtio-blk".into(),
            encrypted: false,
            iops: None,
            labels: None,
        }
    }

    // ── VM lifecycle tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_create_and_get_vm() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        assert_eq!(vm.status, "running");
        assert_eq!(vm.tenant, "acme");
        assert_eq!(vm.vcpus, Some(2));
        assert_eq!(vm.memory_mb, Some(512));

        let fetched = d.get(&vm.id).await.unwrap();
        assert_eq!(fetched.id, vm.id);
    }

    #[tokio::test]
    async fn test_create_duplicate_vm() {
        let d = mock();
        let params = CreateParams {
            id: Some("vm-dup".into()),
            tenant: "acme".into(),
            vcpus: 1,
            memory_mb: 256,
            image_id: "img-test".into(),
            hostname: None,
            network: None,
            metadata: None,
            labels: None,
            ssh_key: None,
            ssh_key_ref: None,
            admin_user: None,
            cloud_init: None,
            cloud_init_ref: None,
            password: None,
        };
        d.create(params.clone()).await.unwrap();
        let err = d.create(params).await.unwrap_err();
        assert!(matches!(err, DriverError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_list_vms() {
        let d = mock();
        d.create(vm_params("a")).await.unwrap();
        d.create(vm_params("b")).await.unwrap();

        let vms = d.list().await.unwrap();
        assert_eq!(vms.len(), 2);
    }

    #[tokio::test]
    async fn test_destroy_vm() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        d.destroy(&vm.id, false).await.unwrap();

        let err = d.get(&vm.id).await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_get_nonexistent_vm() {
        let d = mock();
        let err = d.get("no-such-vm").await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    // ── Pause / Resume ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_pause_resume() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();

        d.pause(&vm.id).await.unwrap();
        let paused = d.get(&vm.id).await.unwrap();
        assert_eq!(paused.status, "paused");

        d.resume(&vm.id).await.unwrap();
        let resumed = d.get(&vm.id).await.unwrap();
        assert_eq!(resumed.status, "running");
    }

    #[tokio::test]
    async fn test_pause_stopped_vm_fails() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        d.pause(&vm.id).await.unwrap();

        // Can't pause an already paused VM
        let err = d.pause(&vm.id).await.unwrap_err();
        assert!(matches!(err, DriverError::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_resume_running_vm_fails() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();

        // Can't resume a running VM
        let err = d.resume(&vm.id).await.unwrap_err();
        assert!(matches!(err, DriverError::InvalidState(_)));
    }

    // ── Snapshot / Restore ──────────────────────────────────────────

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();

        let snap = d
            .snapshot(&vm.id, "/snapshots/snap1", Some("v1"))
            .await
            .unwrap();
        assert_eq!(snap.microvm_id, vm.id);
        assert_eq!(snap.destination, "/snapshots/snap1");
        assert_eq!(snap.tag.as_deref(), Some("v1"));

        // Destroy the original VM
        d.destroy(&vm.id, true).await.unwrap();

        // Restore from snapshot
        let restored = d.restore("restored-vm", &snap.destination).await.unwrap();
        assert_eq!(restored.id, "restored-vm");
        assert_eq!(restored.status, "running");
    }

    // ── Volume lifecycle tests ──────────────────────────────────────

    #[tokio::test]
    async fn test_create_and_list_volumes() {
        let d = mock();
        let vol = d.create_volume(vol_params()).await.unwrap();
        assert_eq!(vol.status, "available");
        assert_eq!(vol.size_gb, 10);

        let vols = d.list_volumes().await.unwrap();
        assert_eq!(vols.len(), 1);
    }

    #[tokio::test]
    async fn test_attach_detach_volume() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        let vol = d.create_volume(vol_params()).await.unwrap();

        // Attach
        d.attach_volume(&vol.id, &vm.id, Some("/dev/vdc"))
            .await
            .unwrap();

        let vols = d.list_volumes().await.unwrap();
        let attached_vol = vols.iter().find(|v| v.id == vol.id).unwrap();
        assert_eq!(attached_vol.status, "attached");
        assert_eq!(attached_vol.microvm_id.as_deref(), Some(vm.id.as_str()));
        assert_eq!(attached_vol.device_name.as_deref(), Some("/dev/vdc"));

        // Detach
        d.detach_volume(&vol.id, &vm.id).await.unwrap();

        let vols = d.list_volumes().await.unwrap();
        let detached_vol = vols.iter().find(|v| v.id == vol.id).unwrap();
        assert_eq!(detached_vol.status, "available");
        assert!(detached_vol.microvm_id.is_none());
    }

    #[tokio::test]
    async fn test_destroy_attached_volume_without_force_fails() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        let vol = d.create_volume(vol_params()).await.unwrap();
        d.attach_volume(&vol.id, &vm.id, None).await.unwrap();

        let err = d.destroy_volume(&vol.id, false).await.unwrap_err();
        assert!(matches!(err, DriverError::InvalidState(_)));

        // Force destroy works
        d.destroy_volume(&vol.id, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_destroy_vm_with_attached_volume_without_force_fails() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        let vol = d.create_volume(vol_params()).await.unwrap();
        d.attach_volume(&vol.id, &vm.id, None).await.unwrap();

        let err = d.destroy(&vm.id, false).await.unwrap_err();
        assert!(matches!(err, DriverError::InvalidState(_)));

        // Force destroy works and detaches volumes
        d.destroy(&vm.id, true).await.unwrap();

        let vols = d.list_volumes().await.unwrap();
        let vol = vols.iter().find(|v| v.id == vol.id).unwrap();
        assert_eq!(vol.status, "available");
        assert!(vol.microvm_id.is_none());
    }

    #[tokio::test]
    async fn test_attach_volume_to_nonexistent_vm_fails() {
        let d = mock();
        let vol = d.create_volume(vol_params()).await.unwrap();

        let err = d
            .attach_volume(&vol.id, "no-such-vm", None)
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_double_attach_volume_fails() {
        let d = mock();
        let vm = d.create(vm_params("acme")).await.unwrap();
        let vol = d.create_volume(vol_params()).await.unwrap();
        d.attach_volume(&vol.id, &vm.id, None).await.unwrap();

        let err = d
            .attach_volume(&vol.id, &vm.id, None)
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::InvalidState(_)));
    }

    // ── Image operations ────────────────────────────────────────────

    #[tokio::test]
    async fn test_import_and_list_images() {
        let d = mock();
        let params = ImageParams {
            id: "img-1".into(),
            name: "Ubuntu 22.04".into(),
            os: "linux".into(),
            distro: "ubuntu".into(),
            version: "22.04".into(),
            arch: "x86_64".into(),
            image_type: "rootfs".into(),
            source: "local".into(),
            kernel: Some("/boot/vmlinux".into()),
            rootfs: Some("/images/ubuntu.ext4".into()),
            checksum: None,
            labels: None,
        };
        let img = d.import_image(params).await.unwrap();
        assert_eq!(img.id, "img-1");
        assert_eq!(img.status, "available");

        let imgs = d.list_images().await.unwrap();
        assert_eq!(imgs.len(), 1);
    }

    #[tokio::test]
    async fn test_resolve_image() {
        let d = mock();
        let params = ImageParams {
            id: "img-2".into(),
            name: "Alpine".into(),
            os: "linux".into(),
            distro: "alpine".into(),
            version: "3.18".into(),
            arch: "x86_64".into(),
            image_type: "rootfs".into(),
            source: "local".into(),
            kernel: Some("/boot/vmlinux".into()),
            rootfs: Some("/images/alpine.ext4".into()),
            checksum: None,
            labels: None,
        };
        d.import_image(params).await.unwrap();

        let resolved = d.resolve_image("img-2").await.unwrap();
        assert_eq!(resolved.image_id, "img-2");
        assert_eq!(resolved.kernel_path.as_deref(), Some("/boot/vmlinux"));
    }

    // ── Health ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_health_check() {
        let d = mock();
        let health = d.health_check().await.unwrap();
        assert!(health.healthy);
    }

    // ── Capabilities ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_capabilities() {
        let d = mock();
        let caps = d.capabilities();
        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(caps.supports(&Capability::Pause));
        assert!(caps.supports(&Capability::Resume));
        assert!(caps.supports(&Capability::Snapshot));
        assert!(!caps.supports(&Capability::LiveMigration));
        assert!(!caps.supports(&Capability::GpuPassthrough));
    }
}
