use crate::capability::CapabilityManifest;
use crate::mock::MockDriver;
use crate::traits::{Driver, DriverError};
use crate::types::*;
use async_trait::async_trait;

/// A simulation driver that returns realistic fake data without calling any
/// cloud provider.  It delegates to [`MockDriver`] for state tracking but
/// enriches responses with plausible cloud-like metrics and identifiers.
pub struct SimulationDriver {
    inner: MockDriver,
    provider_type: String, // "azure", "aws", "gcp", "kvm"
}

impl SimulationDriver {
    pub fn new(provider_type: &str) -> Self {
        Self {
            inner: MockDriver::new(),
            provider_type: provider_type.to_string(),
        }
    }
}

#[async_trait]
impl Driver for SimulationDriver {
    fn capabilities(&self) -> &CapabilityManifest {
        self.inner.capabilities()
    }

    // -- MicroVM operations --------------------------------------------------

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let mut vm = self.inner.create(params).await?;
        // Enrich with realistic cloud-like data
        vm.cpu_pct = Some(12.5);
        vm.mem_used_mb = Some(vm.memory_mb.unwrap_or(512) / 2);
        vm.net_rx_kbps = Some(45.2);
        vm.net_tx_kbps = Some(22.8);
        Ok(vm)
    }

    async fn destroy(&self, id: &str, force: bool) -> Result<(), DriverError> {
        self.inner.destroy(id, force).await
    }

    async fn pause(&self, id: &str) -> Result<(), DriverError> {
        self.inner.pause(id).await
    }

    async fn resume(&self, id: &str) -> Result<(), DriverError> {
        self.inner.resume(id).await
    }

    async fn alter(&self, id: &str, params: serde_json::Value) -> Result<MicroVm, DriverError> {
        self.inner.alter(id, params).await
    }

    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        self.inner.snapshot(id, destination, tag).await
    }

    async fn restore(&self, id: &str, source: &str) -> Result<MicroVm, DriverError> {
        self.inner.restore(id, source).await
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        self.inner.list().await
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        self.inner.get(id).await
    }

    async fn metrics(&self, id: &str) -> Result<MetricSample, DriverError> {
        self.inner.metrics(id).await
    }

    // -- Volume operations ---------------------------------------------------

    async fn create_volume(&self, params: VolumeParams) -> Result<Volume, DriverError> {
        self.inner.create_volume(params).await
    }

    async fn destroy_volume(&self, id: &str, force: bool) -> Result<(), DriverError> {
        self.inner.destroy_volume(id, force).await
    }

    async fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        device: Option<&str>,
    ) -> Result<(), DriverError> {
        self.inner.attach_volume(vol_id, vm_id, device).await
    }

    async fn detach_volume(&self, vol_id: &str, vm_id: &str) -> Result<(), DriverError> {
        self.inner.detach_volume(vol_id, vm_id).await
    }

    async fn resize_volume(&self, id: &str, size_gb: i64) -> Result<Volume, DriverError> {
        self.inner.resize_volume(id, size_gb).await
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        self.inner.list_volumes().await
    }

    // -- Image operations ----------------------------------------------------

    async fn import_image(&self, params: ImageParams) -> Result<Image, DriverError> {
        self.inner.import_image(params).await
    }

    async fn remove_image(&self, id: &str, force: bool) -> Result<(), DriverError> {
        self.inner.remove_image(id, force).await
    }

    async fn resolve_image(&self, image_id: &str) -> Result<ImageRef, DriverError> {
        self.inner.resolve_image(image_id).await
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        self.inner.list_images().await
    }

    // -- Health --------------------------------------------------------------

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        Ok(HealthStatus {
            healthy: true,
            latency_ms: Some(3),
            message: Some("simulation mode -- always healthy".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simulation_driver_create_enriches_metrics() {
        let d = SimulationDriver::new("azure");
        let params = CreateParams {
            id: Some("sim-vm-1".into()),
            tenant: "test".into(),
            vcpus: 2,
            memory_mb: 1024,
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
        let vm = d.create(params).await.unwrap();
        assert_eq!(vm.cpu_pct, Some(12.5));
        assert_eq!(vm.mem_used_mb, Some(512)); // 1024 / 2
        assert_eq!(vm.net_rx_kbps, Some(45.2));
        assert_eq!(vm.net_tx_kbps, Some(22.8));
    }

    #[tokio::test]
    async fn test_simulation_driver_health_always_healthy() {
        let d = SimulationDriver::new("aws");
        let h = d.health_check().await.unwrap();
        assert!(h.healthy);
        assert_eq!(h.latency_ms, Some(3));
    }

    #[tokio::test]
    async fn test_simulation_driver_delegates_lifecycle() {
        let d = SimulationDriver::new("kvm");
        let params = CreateParams {
            id: Some("sim-vm-lc".into()),
            tenant: "test".into(),
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
        d.create(params).await.unwrap();

        // pause / resume
        d.pause("sim-vm-lc").await.unwrap();
        let vm = d.get("sim-vm-lc").await.unwrap();
        assert_eq!(vm.status, "paused");

        d.resume("sim-vm-lc").await.unwrap();
        let vm = d.get("sim-vm-lc").await.unwrap();
        assert_eq!(vm.status, "running");

        // destroy
        d.destroy("sim-vm-lc", false).await.unwrap();
        let err = d.get("sim-vm-lc").await.unwrap_err();
        assert!(matches!(err, DriverError::NotFound(_)));
    }
}
