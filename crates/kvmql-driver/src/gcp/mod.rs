use std::collections::HashMap;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

/// GCP Compute Engine driver skeleton.
///
/// Would communicate with GCP Compute Engine via the google-cloud SDK.
/// This is a placeholder implementation; all operations return a connection error
/// until the real SDK integration is wired up.
pub struct GcpComputeDriver {
    project: String,
    manifest: CapabilityManifest,
}

impl GcpComputeDriver {
    pub fn new(project: &str) -> Self {
        Self {
            project: project.to_string(),
            manifest: build_gcp_manifest(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }
}

fn build_gcp_manifest() -> CapabilityManifest {
    use Capability::*;

    let supported = [
        Create, Destroy, Snapshot, Restore,
        WatchMetric, Placement, VolumeEncrypt,
        ImageImport, ImagePublish, VolumeResizeLive,
    ];

    let unsupported = [
        Pause, Resume, CustomKernel, Vsock, Balloon,
        HotplugVolume, HotplugNetwork, LiveMigration,
        NestedVirt, GpuPassthrough,
        AlterCpuLive, AlterMemoryLive,
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
                notes: Some("not supported on GCP Compute Engine".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn not_connected() -> DriverError {
    DriverError::Connection("GCP Compute Engine driver not connected".into())
}

#[async_trait]
impl Driver for GcpComputeDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    // -- MicroVM operations --------------------------------------------------

    async fn create(&self, _params: CreateParams) -> Result<MicroVm, DriverError> {
        Err(not_connected())
    }

    async fn destroy(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn pause(&self, _id: &str) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn resume(&self, _id: &str) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn alter(&self, _id: &str, _params: serde_json::Value) -> Result<MicroVm, DriverError> {
        Err(not_connected())
    }

    async fn snapshot(
        &self,
        _id: &str,
        _destination: &str,
        _tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        Err(not_connected())
    }

    async fn restore(&self, _id: &str, _source: &str) -> Result<MicroVm, DriverError> {
        Err(not_connected())
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        Err(not_connected())
    }

    async fn get(&self, _id: &str) -> Result<MicroVm, DriverError> {
        Err(not_connected())
    }

    async fn metrics(&self, _id: &str) -> Result<MetricSample, DriverError> {
        Err(not_connected())
    }

    // -- Volume operations ---------------------------------------------------

    async fn create_volume(&self, _params: VolumeParams) -> Result<Volume, DriverError> {
        Err(not_connected())
    }

    async fn destroy_volume(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn attach_volume(
        &self,
        _vol_id: &str,
        _vm_id: &str,
        _device: Option<&str>,
    ) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn detach_volume(&self, _vol_id: &str, _vm_id: &str) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn resize_volume(&self, _id: &str, _size_gb: i64) -> Result<Volume, DriverError> {
        Err(not_connected())
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        Err(not_connected())
    }

    // -- Image operations ----------------------------------------------------

    async fn import_image(&self, _params: ImageParams) -> Result<Image, DriverError> {
        Err(not_connected())
    }

    async fn remove_image(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(not_connected())
    }

    async fn resolve_image(&self, _image_id: &str) -> Result<ImageRef, DriverError> {
        Err(not_connected())
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        Err(not_connected())
    }

    // -- Health --------------------------------------------------------------

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        Err(not_connected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities() {
        let d = GcpComputeDriver::new("my-project");
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
        assert!(caps.supports(&Capability::VolumeResizeLive));

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
    }

    #[tokio::test]
    async fn test_create_returns_not_connected() {
        let d = GcpComputeDriver::new("my-project");
        let params = CreateParams {
            id: None,
            tenant: "acme".into(),
            vcpus: 2,
            memory_mb: 512,
            image_id: "gcp-image-test".into(),
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
        let err = d.create(params).await.unwrap_err();
        assert!(matches!(err, DriverError::Connection(_)));
        assert!(err.to_string().contains("GCP Compute Engine driver not connected"));
    }
}
