use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Debug, Clone)]
pub struct CapabilityEntry {
    pub supported: bool,
    pub notes: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CapabilityManifest {
    pub capabilities: HashMap<Capability, CapabilityEntry>,
}

impl CapabilityManifest {
    pub fn supports(&self, cap: &Capability) -> bool {
        self.capabilities
            .get(cap)
            .map(|e| e.supported)
            .unwrap_or(false)
    }

    pub fn get(&self, cap: &Capability) -> Option<&CapabilityEntry> {
        self.capabilities.get(cap)
    }
}
