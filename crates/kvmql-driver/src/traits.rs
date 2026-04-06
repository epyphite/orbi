use async_trait::async_trait;

use crate::capability::CapabilityManifest;
use crate::types::*;

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait Driver: Send + Sync {
    fn capabilities(&self) -> &CapabilityManifest;

    // MicroVM operations
    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError>;
    async fn destroy(&self, id: &str, force: bool) -> Result<(), DriverError>;
    async fn pause(&self, id: &str) -> Result<(), DriverError>;
    async fn resume(&self, id: &str) -> Result<(), DriverError>;
    async fn alter(&self, id: &str, params: serde_json::Value) -> Result<MicroVm, DriverError>;
    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError>;
    async fn restore(&self, id: &str, source: &str) -> Result<MicroVm, DriverError>;
    async fn list(&self) -> Result<Vec<MicroVm>, DriverError>;
    async fn get(&self, id: &str) -> Result<MicroVm, DriverError>;
    async fn metrics(&self, id: &str) -> Result<MetricSample, DriverError>;

    // Volume operations
    async fn create_volume(&self, params: VolumeParams) -> Result<Volume, DriverError>;
    async fn destroy_volume(&self, id: &str, force: bool) -> Result<(), DriverError>;
    async fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        device: Option<&str>,
    ) -> Result<(), DriverError>;
    async fn detach_volume(&self, vol_id: &str, vm_id: &str) -> Result<(), DriverError>;
    async fn resize_volume(&self, id: &str, size_gb: i64) -> Result<Volume, DriverError>;
    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError>;

    // Image operations
    async fn import_image(&self, params: ImageParams) -> Result<Image, DriverError>;
    async fn remove_image(&self, id: &str, force: bool) -> Result<(), DriverError>;
    async fn resolve_image(&self, image_id: &str) -> Result<ImageRef, DriverError>;
    async fn list_images(&self) -> Result<Vec<Image>, DriverError>;

    // Health
    async fn health_check(&self) -> Result<HealthStatus, DriverError>;
}
