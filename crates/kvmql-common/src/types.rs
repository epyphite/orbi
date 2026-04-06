use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderType {
    Kvm,
    Aws,
    Gcp,
    Azure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DriverType {
    Firecracker,
    Libvirt,
    Ec2,
    Compute,
    AzureVm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProviderStatus {
    Healthy,
    Degraded,
    Offline,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MicrovmStatus {
    Creating,
    Running,
    Stopped,
    Paused,
    Stopping,
    Destroying,
    Snapshotting,
    Error,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VolumeStatus {
    Creating,
    Available,
    Attaching,
    Attached,
    Detaching,
    Resizing,
    Deleting,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImageStatus {
    Importing,
    Available,
    Publishing,
    Removing,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutionMode {
    Strict,
    Permissive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Capability {
    Create,
    Destroy,
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
}
