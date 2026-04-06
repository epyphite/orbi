pub mod cli;
pub mod mapper;
pub mod resources;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::capability::{Capability, CapabilityEntry, CapabilityManifest};
use crate::traits::{Driver, DriverError};
use crate::types::*;

use cli::AwsCli;

/// AWS EC2 driver that shells out to the `aws` CLI.
///
/// Each driver instance is bound to a region and optionally a profile.
/// All blocking `aws` invocations are wrapped in
/// `tokio::task::spawn_blocking` so they don't stall the async runtime.
pub struct AwsEc2Driver {
    region: String,
    profile: Option<String>,
    cli: AwsCli,
    manifest: CapabilityManifest,
}

impl AwsEc2Driver {
    /// Create a driver bound to a region.
    pub fn new(region: &str) -> Self {
        let cli = AwsCli::with_region(region);
        Self {
            region: region.to_string(),
            profile: None,
            cli,
            manifest: build_aws_manifest(),
        }
    }

    /// Create a driver bound to a region and a named profile.
    pub fn with_profile(region: &str, profile: &str) -> Self {
        let cli = AwsCli::with_region(region).with_profile(profile);
        Self {
            region: region.to_string(),
            profile: Some(profile.to_string()),
            cli,
            manifest: build_aws_manifest(),
        }
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    /// Return the default availability zone (region + "a") for operations
    /// that require an AZ.
    fn default_az(&self) -> String {
        format!("{}a", self.region)
    }
}

fn build_aws_manifest() -> CapabilityManifest {
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
                notes: Some("not supported on AWS EC2".into()),
            },
        );
    }

    CapabilityManifest { capabilities }
}

fn cli_err(msg: String) -> DriverError {
    DriverError::Connection(msg)
}

#[async_trait]
impl Driver for AwsEc2Driver {
    fn capabilities(&self) -> &CapabilityManifest {
        &self.manifest
    }

    // ── MicroVM operations ──────────────────────────────────────────

    async fn create(&self, params: CreateParams) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();

        let instance_type =
            mapper::specs_to_instance_type(params.vcpus, params.memory_mb).to_string();
        let image_id = params.image_id.clone();

        let vm_name = params
            .id
            .clone()
            .unwrap_or_else(|| format!("kvmql-{}", uuid::Uuid::new_v4()));

        let ssh_key = params.ssh_key.clone();
        let cloud_init = params.cloud_init.clone();

        // Import SSH key pair if provided
        let cli_for_key = cli.clone();
        let key_name_for_import = format!("kvmql-{}", &vm_name);
        if let Some(ref key) = ssh_key {
            if key != "generate" {
                let key_clone = key.clone();
                let name_clone = key_name_for_import.clone();
                let cli_clone = cli_for_key.clone();
                tokio::task::spawn_blocking(move || {
                    cli_clone.ec2_import_key_pair(&name_clone, &key_clone)
                })
                .await
                .map_err(|e| DriverError::Internal(e.to_string()))?
                .map_err(cli_err)?;
            }
        }

        let result = tokio::task::spawn_blocking(move || {
            let mut run_params: Vec<(&str, &str)> = vec![
                ("image-id", &image_id),
                ("instance-type", &instance_type),
                ("min-count", "1"),
                ("max-count", "1"),
            ];

            // Tag the instance with its name
            let tag_spec =
                format!("ResourceType=instance,Tags=[{{Key=Name,Value={vm_name}}}]");
            run_params.push(("tag-specifications", &tag_spec));

            // SSH key pair reference
            if ssh_key.is_some() {
                run_params.push(("key-name", &key_name_for_import));
            }

            // Cloud-init / user-data
            let user_data_path;
            if let Some(ref init) = cloud_init {
                user_data_path = if init.starts_with('/') || init.starts_with("./") {
                    format!("file://{init}")
                } else {
                    let tmp = std::env::temp_dir()
                        .join(format!("kvmql-userdata-{}", &vm_name));
                    std::fs::write(&tmp, init)
                        .map_err(|e| format!("failed to write user-data temp file: {e}"))?;
                    format!("file://{}", tmp.to_string_lossy())
                };
                run_params.push(("user-data", &user_data_path));
            }

            cli.ec2_run_instances(&run_params)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        // run-instances returns {"Instances": [...]}
        let instance = result
            .get("Instances")
            .and_then(|i| i.as_array())
            .and_then(|arr| arr.first())
            .cloned()
            .unwrap_or(result.clone());

        Ok(mapper::map_ec2_instance(&instance, &provider_id))
    }

    async fn destroy(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let instance_id = id.to_string();

        tokio::task::spawn_blocking(move || cli.ec2_terminate_instances(&instance_id))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn pause(&self, _id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "AWS EC2 does not support pause/resume; use stop/start instead".into(),
        ))
    }

    async fn resume(&self, _id: &str) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "AWS EC2 does not support pause/resume; use stop/start instead".into(),
        ))
    }

    async fn alter(&self, _id: &str, _params: serde_json::Value) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "AWS EC2 does not support live instance alteration".into(),
        ))
    }

    async fn snapshot(
        &self,
        id: &str,
        destination: &str,
        tag: Option<&str>,
    ) -> Result<Snapshot, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();
        let volume_id = id.to_string();
        let description = destination.to_string();

        let result = tokio::task::spawn_blocking(move || {
            cli.ec2_create_snapshot(&volume_id, &description)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        let mut snapshot = mapper::map_ebs_snapshot(&result, &provider_id);
        if let Some(t) = tag {
            snapshot.tag = Some(t.to_string());
        }
        Ok(snapshot)
    }

    async fn restore(&self, _id: &str, _source: &str) -> Result<MicroVm, DriverError> {
        Err(DriverError::Unsupported(
            "AWS snapshot restore is not yet implemented; create an AMI from a snapshot and launch from it".into(),
        ))
    }

    async fn list(&self) -> Result<Vec<MicroVm>, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();

        let instances = tokio::task::spawn_blocking(move || {
            cli.ec2_describe_instances(None)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        Ok(instances
            .iter()
            .map(|i| mapper::map_ec2_instance(i, &provider_id))
            .collect())
    }

    async fn get(&self, id: &str) -> Result<MicroVm, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();
        let instance_id = id.to_string();

        let instances = tokio::task::spawn_blocking(move || {
            cli.ec2_describe_instances(Some(&[("instance-id", &instance_id)]))
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        let instance = instances
            .first()
            .ok_or_else(|| DriverError::NotFound(format!("instance {id} not found")))?;

        Ok(mapper::map_ec2_instance(instance, &provider_id))
    }

    async fn metrics(&self, _id: &str) -> Result<MetricSample, DriverError> {
        Err(DriverError::Unsupported(
            "AWS metrics are available via CloudWatch, not the aws CLI driver".into(),
        ))
    }

    // ── Volume operations ───────────────────────────────────────────

    async fn create_volume(&self, params: VolumeParams) -> Result<Volume, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();
        let az = self.default_az();
        let size_gb = params.size_gb;
        let vol_type = params.vol_type.clone();

        let result = tokio::task::spawn_blocking(move || {
            let extra: Vec<(&str, &str)> = vec![("volume-type", &vol_type)];
            cli.ec2_create_volume(size_gb, &az, &extra)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)?;

        Ok(mapper::map_ebs_volume(&result, &provider_id))
    }

    async fn destroy_volume(&self, id: &str, _force: bool) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let volume_id = id.to_string();

        tokio::task::spawn_blocking(move || cli.ec2_delete_volume(&volume_id))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        device: Option<&str>,
    ) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let volume_id = vol_id.to_string();
        let instance_id = vm_id.to_string();
        let dev = device.unwrap_or("/dev/xvdf").to_string();

        tokio::task::spawn_blocking(move || {
            cli.ec2_attach_volume(&volume_id, &instance_id, &dev)
        })
        .await
        .map_err(|e| DriverError::Internal(e.to_string()))?
        .map_err(cli_err)
    }

    async fn detach_volume(&self, vol_id: &str, _vm_id: &str) -> Result<(), DriverError> {
        let cli = self.cli.clone();
        let volume_id = vol_id.to_string();

        tokio::task::spawn_blocking(move || cli.ec2_detach_volume(&volume_id))
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)
    }

    async fn resize_volume(&self, _id: &str, _size_gb: i64) -> Result<Volume, DriverError> {
        Err(DriverError::Unsupported(
            "AWS EBS does not support live volume resize via aws CLI driver".into(),
        ))
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, DriverError> {
        let cli = self.cli.clone();
        let provider_id = self.region.clone();

        let volumes = tokio::task::spawn_blocking(move || cli.ec2_describe_volumes())
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?
            .map_err(cli_err)?;

        Ok(volumes
            .iter()
            .map(|v| mapper::map_ebs_volume(v, &provider_id))
            .collect())
    }

    // ── Image operations ────────────────────────────────────────────

    async fn import_image(&self, _params: ImageParams) -> Result<Image, DriverError> {
        Err(DriverError::Unsupported(
            "AWS image management is not yet supported via aws CLI driver".into(),
        ))
    }

    async fn remove_image(&self, _id: &str, _force: bool) -> Result<(), DriverError> {
        Err(DriverError::Unsupported(
            "AWS image management is not yet supported via aws CLI driver".into(),
        ))
    }

    async fn resolve_image(&self, _image_id: &str) -> Result<ImageRef, DriverError> {
        Err(DriverError::Unsupported(
            "AWS image management is not yet supported via aws CLI driver".into(),
        ))
    }

    async fn list_images(&self) -> Result<Vec<Image>, DriverError> {
        Err(DriverError::Unsupported(
            "AWS image management is not yet supported via aws CLI driver".into(),
        ))
    }

    // ── Health ──────────────────────────────────────────────────────

    async fn health_check(&self) -> Result<HealthStatus, DriverError> {
        let cli = self.cli.clone();
        let start = std::time::Instant::now();

        let result = tokio::task::spawn_blocking(move || cli.sts_get_caller_identity())
            .await
            .map_err(|e| DriverError::Internal(e.to_string()))?;

        let latency = start.elapsed().as_millis() as u64;

        match result {
            Ok(_) => Ok(HealthStatus {
                healthy: true,
                latency_ms: Some(latency),
                message: Some("AWS CLI is responsive".into()),
            }),
            Err(e) => Ok(HealthStatus {
                healthy: false,
                latency_ms: Some(latency),
                message: Some(format!("AWS health check failed: {e}")),
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
        let d = AwsEc2Driver::new("us-east-1");
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
        let d = AwsEc2Driver::new("us-west-2");
        assert_eq!(d.region(), "us-west-2");
        assert!(d.profile.is_none());
    }

    #[test]
    fn test_constructor_with_profile() {
        let d = AwsEc2Driver::with_profile("eu-west-1", "staging");
        assert_eq!(d.region(), "eu-west-1");
        assert_eq!(d.profile.as_deref(), Some("staging"));
    }

    #[test]
    fn test_default_az() {
        let d = AwsEc2Driver::new("us-east-1");
        assert_eq!(d.default_az(), "us-east-1a");
    }

    #[tokio::test]
    async fn test_pause_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d.pause("i-12345").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
        assert!(err.to_string().contains("pause/resume"));
    }

    #[tokio::test]
    async fn test_resume_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d.resume("i-12345").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
        assert!(err.to_string().contains("pause/resume"));
    }

    #[tokio::test]
    async fn test_alter_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d
            .alter("i-12345", serde_json::json!({"vcpus": 4}))
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_restore_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d.restore("i-12345", "snap-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_resize_volume_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d.resize_volume("vol-1", 100).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_image_ops_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");

        let err = d.list_images().await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));

        let err = d.resolve_image("ami-1").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));

        let err = d.remove_image("ami-1", false).await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }

    #[tokio::test]
    async fn test_metrics_unsupported() {
        let d = AwsEc2Driver::new("us-east-1");
        let err = d.metrics("i-12345").await.unwrap_err();
        assert!(matches!(err, DriverError::Unsupported(_)));
    }
}
