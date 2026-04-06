pub mod client;
pub mod mapper;

use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

use client::FirecrackerClient;
use mapper::ResolvedImage;

/// Internal tracked state for volumes and images.
///
/// Firecracker manages exactly one VM per process, so the driver tracks a
/// single optional `MicroVm` plus associated volumes and images.
struct TrackedState {
    vm: Option<MicroVm>,
    volumes: HashMap<String, Volume>,
    images: HashMap<String, Image>,
}

/// Real Firecracker driver that communicates with a Firecracker VMM instance
/// via its REST API over a Unix socket.
///
/// Each `FirecrackerDriver` is bound to a single Firecracker process identified
/// by `socket_path`. Because Firecracker runs one microVM per process, the
/// driver tracks at most one VM internally.
pub struct FirecrackerDriver {
    client: FirecrackerClient,
    provider_id: String,
    manifest: CapabilityManifest,
    state: RwLock<TrackedState>,
}

impl FirecrackerDriver {
    pub fn new(socket_path: impl Into<String>) -> Self {
        let socket: String = socket_path.into();
        let provider_id = format!("fc-{}", uuid::Uuid::new_v4());
        Self {
            client: FirecrackerClient::new(&socket),
            provider_id,
            manifest: build_firecracker_manifest(),
            state: RwLock::new(TrackedState {
                vm: None,
                volumes: HashMap::new(),
                images: HashMap::new(),
            }),
        }
    }

    pub fn socket_path(&self) -> &str {
        self.client.socket_path()
    }
}

fn build_firecracker_manifest() -> CapabilityManifest {
    use Capability::*;

    let supported = [
        Create, Destroy, Pause, Resume, Snapshot, Restore,
        CustomKernel, Vsock, Balloon, HotplugVolume,
        WatchMetric, ImageImport,
    ];

    let unsupported = [
        AlterCpuLive, AlterMemoryLive, LiveMigration, GpuPassthrough,
        Placement, VolumeEncrypt, ImagePublish, HotplugNetwork,
        NestedVirt, VolumeResizeLive,
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
                notes: Some("not supported by Firecracker".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn client_err_to_driver(e: client::ClientError) -> DriverError {
    match e {
        client::ClientError::Connection(msg) => DriverError::Connection(msg),
        client::ClientError::Request(msg) => DriverError::Internal(msg),
        client::ClientError::ApiError { status, body } => {
            DriverError::Internal(format!("Firecracker API error {status}: {body}"))
        }
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[async_trait]
impl Driver for FirecrackerDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    // ── MicroVM operations ──────────────────────────────────────────

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let mut state = self.state.write().await;

        if state.vm.is_some() {
            return Err(DriverError::AlreadyExists(
                "Firecracker instance already has a VM".into(),
            ));
        }

        // Resolve image to get kernel + rootfs paths.
        let image = state
            .images
            .get(&params.image_id)
            .ok_or_else(|| {
                DriverError::NotFound(format!("Image {} not found", params.image_id))
            })?;

        let resolved = ResolvedImage::from_image_ref(&ImageRef {
            image_id: image.id.clone(),
            provider_id: image.provider_id.clone().unwrap_or_default(),
            resolved_type: image.image_type.clone(),
            kernel_path: image.kernel_path.clone(),
            rootfs_path: image.rootfs_path.clone(),
            cloud_ref: image.cloud_ref.clone(),
        })
        .ok_or_else(|| {
            DriverError::Internal(format!(
                "Image {} is missing kernel_path or rootfs_path",
                params.image_id
            ))
        })?;

        let vm_id = params.id.clone().unwrap_or_else(new_id);

        let vm = mapper::create_vm(
            &self.client,
            &params,
            &resolved,
            &vm_id,
            &self.provider_id,
        )
        .await
        .map_err(client_err_to_driver)?;

        state.vm = Some(vm.clone());
        Ok(vm)
    }

    async fn destroy(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vm = state
            .vm
            .as_ref()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.id != id {
            return Err(DriverError::NotFound(format!("VM {id}")));
        }

        // Send graceful shutdown (SendCtrlAltDel).
        self.client
            .stop_instance()
            .await
            .map_err(client_err_to_driver)?;

        // Detach any volumes that were attached to this VM.
        for vol in state.volumes.values_mut() {
            if vol.microvm_id.as_deref() == Some(id) {
                vol.microvm_id = None;
                vol.status = "available".into();
                vol.device_name = None;
            }
        }

        state.vm = None;
        Ok(())
    }

    async fn pause(&self, id: &str) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vm = state
            .vm
            .as_mut()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.id != id {
            return Err(DriverError::NotFound(format!("VM {id}")));
        }

        if vm.status != "running" {
            return Err(DriverError::InvalidState(format!(
                "cannot pause VM in state '{}'",
                vm.status
            )));
        }

        self.client
            .pause_instance()
            .await
            .map_err(client_err_to_driver)?;

        vm.status = "paused".into();
        Ok(())
    }

    async fn resume(&self, id: &str) -> Result<(), DriverError> {
        let mut state = self.state.write().await;

        let vm = state
            .vm
            .as_mut()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.id != id {
            return Err(DriverError::NotFound(format!("VM {id}")));
        }

        if vm.status != "paused" {
            return Err(DriverError::InvalidState(format!(
                "cannot resume VM in state '{}'",
                vm.status
            )));
        }

        self.client
            .resume_instance()
            .await
            .map_err(client_err_to_driver)?;

        vm.status = "running".into();
        Ok(())
    }

    async fn alter(&self, id: &str, _params: serde_json::Value) -> Result<MicroVm, DriverError> {
        let state = self.state.read().await;

        let vm = state
            .vm
            .as_ref()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.id != id {
            return Err(DriverError::NotFound(format!("VM {id}")));
        }

        // Firecracker does not support live CPU/memory alteration.
        Err(DriverError::Unsupported(
            "Firecracker does not support live VM alteration".into(),
        ))
    }

    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        let state = self.state.read().await;

        let vm = state
            .vm
            .as_ref()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

        if vm.id != id {
            return Err(DriverError::NotFound(format!("VM {id}")));
        }

        if vm.status != "running" && vm.status != "paused" {
            return Err(DriverError::InvalidState(format!(
                "cannot snapshot VM in state '{}'",
                vm.status
            )));
        }

        let mem_path = format!("{destination}.mem");

        self.client
            .create_snapshot(destination, &mem_path)
            .await
            .map_err(client_err_to_driver)?;

        Ok(mapper::build_snapshot(
            id,
            &self.provider_id,
            destination,
            tag,
        ))
    }

    async fn restore(&self, id: &str, source: &str) -> Result<MicroVm, DriverError> {
        let mut state = self.state.write().await;

        if state.vm.is_some() {
            return Err(DriverError::AlreadyExists(
                "Firecracker instance already has a VM; destroy it first".into(),
            ));
        }

        let mem_path = format!("{source}.mem");

        self.client
            .load_snapshot(source, &mem_path)
            .await
            .map_err(client_err_to_driver)?;

        let now = now_iso();
        let vm = MicroVm {
            id: id.to_string(),
            provider_id: self.provider_id.clone(),
            tenant: "restored".into(),
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
            created_at: now.clone(),
            last_seen: Some(now),
            is_stale: false,
        };

        state.vm = Some(vm.clone());
        Ok(vm)
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        let state = self.state.read().await;
        Ok(state.vm.iter().cloned().collect())
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        let state = self.state.read().await;

        state
            .vm
            .as_ref()
            .filter(|vm| vm.id == id)
            .cloned()
            .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))
    }

    async fn metrics(&self, id: &str) -> Result<MetricSample, DriverError> {
        {
            let state = self.state.read().await;
            let vm = state
                .vm
                .as_ref()
                .filter(|vm| vm.id == id)
                .ok_or_else(|| DriverError::NotFound(format!("VM {id}")))?;

            // If we have locally tracked values, use them.
            if vm.vcpus.is_some() || vm.memory_mb.is_some() {
                return Ok(mapper::metrics_from_machine_config(
                    id,
                    vm.vcpus.unwrap_or(0),
                    vm.memory_mb.unwrap_or(0),
                ));
            }
        }

        // Fallback: query machine-config from Firecracker.
        let mc = self
            .client
            .get_machine_config()
            .await
            .map_err(client_err_to_driver)?;

        Ok(mapper::metrics_from_machine_config(
            id,
            mc.vcpu_count,
            mc.mem_size_mib,
        ))
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
            provider_id: format!("fc-vol-{id}"),
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
        let vm = state
            .vm
            .as_ref()
            .ok_or_else(|| DriverError::NotFound(format!("VM {vm_id}")))?;

        if vm.id != vm_id {
            return Err(DriverError::NotFound(format!("VM {vm_id}")));
        }

        let vol = state
            .volumes
            .get(vol_id)
            .ok_or_else(|| DriverError::NotFound(format!("Volume {vol_id}")))?;

        if vol.microvm_id.is_some() {
            return Err(DriverError::InvalidState(format!(
                "volume {vol_id} is already attached"
            )));
        }

        let drive_id = device.unwrap_or("data0");
        let path = vol
            .labels
            .as_ref()
            .and_then(|l| l.get("path_on_host"))
            .and_then(|v| v.as_str())
            .unwrap_or("/dev/vdb");

        // Call Firecracker API to add the drive.
        self.client
            .add_drive(drive_id, path, false, false)
            .await
            .map_err(client_err_to_driver)?;

        let vol = state.volumes.get_mut(vol_id).unwrap();
        vol.microvm_id = Some(vm_id.to_string());
        vol.status = "attached".into();
        vol.device_name = Some(drive_id.to_string());

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

    async fn resize_volume(&self, _id: &str, _size_gb: i64) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "Firecracker does not support live volume resize".into(),
        ))
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
            provider_id: Some(format!("fc-img-{}", params.id)),
            kernel_path: params.kernel,
            rootfs_path: params.rootfs,
            disk_path: None,
            cloud_ref: None,
            source: params.source,
            checksum_sha256: params.checksum,
            size_mb: None,
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
        let start = std::time::Instant::now();

        match self.client.get_instance_info().await {
            Ok(_) => Ok(HealthStatus {
                healthy: true,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: Some("Firecracker instance is responsive".into()),
            }),
            Err(e) => Ok(HealthStatus {
                healthy: false,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: Some(format!("Firecracker health check failed: {e}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::Capability;

    // ── Capability manifest tests ──────────────────────────────────

    #[test]
    fn test_capabilities_supported() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let caps = fc.capabilities();
        assert!(caps.supports(&Capability::Create));
        assert!(caps.supports(&Capability::Destroy));
        assert!(caps.supports(&Capability::Pause));
        assert!(caps.supports(&Capability::Resume));
        assert!(caps.supports(&Capability::Snapshot));
        assert!(caps.supports(&Capability::Restore));
        assert!(caps.supports(&Capability::CustomKernel));
        assert!(caps.supports(&Capability::Vsock));
        assert!(caps.supports(&Capability::Balloon));
        assert!(caps.supports(&Capability::HotplugVolume));
        assert!(caps.supports(&Capability::WatchMetric));
        assert!(caps.supports(&Capability::ImageImport));
    }

    #[test]
    fn test_capabilities_unsupported() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let caps = fc.capabilities();
        assert!(!caps.supports(&Capability::AlterCpuLive));
        assert!(!caps.supports(&Capability::AlterMemoryLive));
        assert!(!caps.supports(&Capability::LiveMigration));
        assert!(!caps.supports(&Capability::GpuPassthrough));
        assert!(!caps.supports(&Capability::Placement));
        assert!(!caps.supports(&Capability::VolumeEncrypt));
        assert!(!caps.supports(&Capability::ImagePublish));
        assert!(!caps.supports(&Capability::HotplugNetwork));
        assert!(!caps.supports(&Capability::NestedVirt));
        assert!(!caps.supports(&Capability::VolumeResizeLive));
    }

    #[test]
    fn test_socket_path() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        assert_eq!(fc.socket_path(), "/tmp/fc.sock");
    }

    // ── Image operations (no socket needed) ────────────────────────

    #[tokio::test]
    async fn test_import_and_resolve_image() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = ImageParams {
            id: "img-1".into(),
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

        let img = fc.import_image(params).await.unwrap();
        assert_eq!(img.id, "img-1");
        assert_eq!(img.status, "available");

        let resolved = fc.resolve_image("img-1").await.unwrap();
        assert_eq!(resolved.kernel_path.as_deref(), Some("/boot/vmlinux"));
        assert_eq!(resolved.rootfs_path.as_deref(), Some("/images/alpine.ext4"));
    }

    #[tokio::test]
    async fn test_import_duplicate_image_fails() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = ImageParams {
            id: "img-dup".into(),
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

        fc.import_image(params.clone()).await.unwrap();
        let err = fc.import_image(params).await.unwrap_err();
        assert!(matches!(err, DriverError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_list_images() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = ImageParams {
            id: "img-list".into(),
            name: "Ubuntu".into(),
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

        fc.import_image(params).await.unwrap();
        let images = fc.list_images().await.unwrap();
        assert_eq!(images.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_image() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = ImageParams {
            id: "img-rm".into(),
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

        fc.import_image(params).await.unwrap();
        fc.remove_image("img-rm", false).await.unwrap();

        let err = fc.resolve_image("img-rm").await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_remove_nonexistent_image_fails() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let err = fc.remove_image("no-such-img", false).await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    // ── Volume operations (no socket needed) ───────────────────────

    #[tokio::test]
    async fn test_create_and_list_volumes() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = VolumeParams {
            id: Some("vol-1".into()),
            size_gb: 10,
            vol_type: "virtio-blk".into(),
            encrypted: false,
            iops: None,
            labels: None,
        };

        let vol = fc.create_volume(params).await.unwrap();
        assert_eq!(vol.id, "vol-1");
        assert_eq!(vol.status, "available");

        let vols = fc.list_volumes().await.unwrap();
        assert_eq!(vols.len(), 1);
    }

    #[tokio::test]
    async fn test_create_duplicate_volume_fails() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = VolumeParams {
            id: Some("vol-dup".into()),
            size_gb: 10,
            vol_type: "virtio-blk".into(),
            encrypted: false,
            iops: None,
            labels: None,
        };

        fc.create_volume(params.clone()).await.unwrap();
        let err = fc.create_volume(params).await.unwrap_err();
        assert!(matches!(err, DriverError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_destroy_volume() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = VolumeParams {
            id: Some("vol-rm".into()),
            size_gb: 10,
            vol_type: "virtio-blk".into(),
            encrypted: false,
            iops: None,
            labels: None,
        };

        fc.create_volume(params).await.unwrap();
        fc.destroy_volume("vol-rm", false).await.unwrap();

        let vols = fc.list_volumes().await.unwrap();
        assert!(vols.is_empty());
    }

    #[tokio::test]
    async fn test_resize_volume_unsupported() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let err = fc.resize_volume("vol-1", 20).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    // ── VM operations that don't need a socket ─────────────────────

    #[tokio::test]
    async fn test_list_empty() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let vms = fc.list().await.unwrap();
        assert!(vms.is_empty());
    }

    #[tokio::test]
    async fn test_get_nonexistent_vm() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let err = fc.get("no-such-vm").await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_alter_unsupported() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let err = fc
            .alter("vm-1", serde_json::json!({"vcpus": 4}))
            .await
            .unwrap_err();
        // alter returns NotFound since there's no VM, but even with one it's unsupported
        assert!(
            matches!(err, DriverError::NotFound(_)) || matches!(err, DriverError::Unsupported(_))
        );
    }

    // ── Health check without socket returns unhealthy ───────────────

    #[tokio::test]
    async fn test_health_check_no_socket() {
        let fc = FirecrackerDriver::new("/tmp/nonexistent.sock");
        let health = fc.health_check().await.unwrap();
        assert!(!health.healthy);
        assert!(health.message.unwrap().contains("failed"));
    }

    // ── Create fails without image ─────────────────────────────────

    #[tokio::test]
    async fn test_create_without_image_fails() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let params = CreateParams {
            id: Some("vm-1".into()),
            tenant: "acme".into(),
            vcpus: 2,
            memory_mb: 512,
            image_id: "no-such-image".into(),
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

        let err = fc.create(params).await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }

    // ── client error mapping ───────────────────────────────────────

    #[test]
    fn test_client_error_to_driver_error() {
        let e = client_err_to_driver(client::ClientError::Connection("refused".into()));
        assert!(matches!(e, DriverError::Connection(_)));

        let e = client_err_to_driver(client::ClientError::Request("timeout".into()));
        assert!(matches!(e, DriverError::Internal(_)));

        let e = client_err_to_driver(client::ClientError::ApiError {
            status: 400,
            body: "bad request".into(),
        });
        assert!(matches!(e, DriverError::Internal(_)));
        assert!(e.to_string().contains("400"));
    }
}
