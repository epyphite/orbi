use serde_json::Value;

use crate::types::{MicroVm, Snapshot, Volume};

/// Map an Azure VM size string to (vcpus, memory_mb).
/// Returns `(None, None)` for unrecognised sizes.
pub fn vm_size_to_specs(size: &str) -> (Option<i32>, Option<i32>) {
    match size {
        "Standard_B1s" => (Some(1), Some(1024)),
        "Standard_B1ms" => (Some(1), Some(2048)),
        "Standard_B2s" => (Some(2), Some(4096)),
        "Standard_B2ms" => (Some(2), Some(8192)),
        "Standard_D2s_v3" => (Some(2), Some(8192)),
        "Standard_D4s_v3" => (Some(4), Some(16384)),
        "Standard_D8s_v3" => (Some(8), Some(32768)),
        _ => (None, None),
    }
}

/// Pick the closest Azure VM size for the requested vcpus and memory_mb.
/// Falls back to `Standard_B2s` if nothing is a reasonable match.
pub fn specs_to_vm_size(vcpus: i32, memory_mb: i32) -> &'static str {
    // Table ordered by (vcpus, memory_mb) ascending.
    let table: &[(&str, i32, i32)] = &[
        ("Standard_B1s", 1, 1024),
        ("Standard_B1ms", 1, 2048),
        ("Standard_B2s", 2, 4096),
        ("Standard_B2ms", 2, 8192),
        ("Standard_D2s_v3", 2, 8192),
        ("Standard_D4s_v3", 4, 16384),
        ("Standard_D8s_v3", 8, 32768),
    ];

    let mut best: Option<(&str, i64)> = None;

    for &(name, v, m) in table {
        // Only consider sizes where vcpus >= requested and memory >= requested.
        if v >= vcpus && m >= memory_mb {
            let distance = ((v - vcpus) as i64).abs() + ((m - memory_mb) as i64).abs();
            match best {
                None => best = Some((name, distance)),
                Some((_, d)) if distance < d => best = Some((name, distance)),
                _ => {}
            }
        }
    }

    best.map(|(name, _)| name).unwrap_or("Standard_B2s")
}

/// Map Azure CLI `powerState` string to a KVMQL status string.
fn map_power_state(power_state: &str) -> &'static str {
    match power_state {
        "VM running" => "running",
        "VM deallocated" => "stopped",
        "VM stopped" => "stopped",
        "VM starting" => "starting",
        "VM deallocating" => "stopping",
        "VM stopping" => "stopping",
        _ => "unknown",
    }
}

/// Convert Azure VM JSON (from `az vm show --show-details`) to `MicroVm`.
pub fn map_vm(json: &Value, provider_id: &str) -> MicroVm {
    let name = json["name"].as_str().unwrap_or("unknown").to_string();
    let location = json["location"].as_str().unwrap_or("unknown").to_string();

    let power_state = json["powerState"]
        .as_str()
        .unwrap_or("unknown");
    let status = map_power_state(power_state).to_string();

    let vm_size = json["hardwareProfile"]["vmSize"]
        .as_str()
        .unwrap_or("");
    let (vcpus, memory_mb) = vm_size_to_specs(vm_size);

    let image_ref = json["storageProfile"]["imageReference"]["offer"]
        .as_str()
        .or_else(|| json["storageProfile"]["imageReference"]["id"].as_str())
        .map(|s| s.to_string());

    let created_at = json["timeCreated"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let hostname = json["osProfile"]["computerName"]
        .as_str()
        .map(|s| s.to_string());

    let labels = {
        let mut label_map = serde_json::Map::new();
        label_map.insert("location".to_string(), Value::String(location));
        label_map.insert("vmSize".to_string(), Value::String(vm_size.to_string()));
        if let Some(tags) = json.get("tags") {
            label_map.insert("tags".to_string(), tags.clone());
        }
        Some(Value::Object(label_map))
    };

    MicroVm {
        id: name,
        provider_id: provider_id.to_string(),
        tenant: json["resourceGroup"]
            .as_str()
            .unwrap_or("unknown")
            .to_string(),
        status,
        image_id: image_ref,
        vcpus,
        memory_mb,
        cpu_pct: None,
        mem_used_mb: None,
        net_rx_kbps: None,
        net_tx_kbps: None,
        hostname,
        metadata: None,
        labels,
        created_at,
        last_seen: Some(chrono::Utc::now().to_rfc3339()),
        is_stale: false,
    }
}

/// Convert Azure disk JSON (from `az disk list`) to `Volume`.
pub fn map_disk(json: &Value, provider_id: &str) -> Volume {
    let name = json["name"].as_str().unwrap_or("unknown").to_string();
    let size_gb = json["diskSizeGb"].as_i64().unwrap_or(0);

    let disk_state = json["diskState"]
        .as_str()
        .unwrap_or("Unknown");
    let status = match disk_state {
        "Attached" => "attached",
        "Unattached" => "available",
        "Reserved" => "reserved",
        _ => "unknown",
    }
    .to_string();

    let managed_by = json["managedBy"]
        .as_str()
        .map(|s| {
            // managedBy is a full ARM resource ID; extract the VM name.
            s.rsplit('/').next().unwrap_or(s).to_string()
        });

    let sku = json["sku"]["name"]
        .as_str()
        .unwrap_or("Standard_LRS")
        .to_string();

    let encrypted = json["encryption"]["type"]
        .as_str()
        .map(|t| t != "None")
        .unwrap_or(false);

    let created_at = json["timeCreated"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let labels = json.get("tags").cloned();

    Volume {
        id: name,
        provider_id: provider_id.to_string(),
        microvm_id: managed_by,
        vol_type: sku,
        size_gb,
        status,
        device_name: None,
        iops: json["diskIOPSReadWrite"].as_i64().map(|v| v as i32),
        encrypted,
        created_at,
        labels,
    }
}

/// Convert Azure snapshot JSON to `Snapshot`.
pub fn map_snapshot(json: &Value, provider_id: &str) -> Snapshot {
    let name = json["name"].as_str().unwrap_or("unknown").to_string();
    let source = json["creationData"]["sourceResourceId"]
        .as_str()
        .unwrap_or("")
        .to_string();

    // Extract the VM name from the source disk resource ID if possible.
    let microvm_id = source
        .split('/')
        .collect::<Vec<&str>>()
        .iter()
        .rev()
        .nth(0)
        .unwrap_or(&"unknown")
        .to_string();

    let size_mb = json["diskSizeBytes"]
        .as_i64()
        .map(|b| b / (1024 * 1024));

    let taken_at = json["timeCreated"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let tag = json.get("tags")
        .and_then(|t| t.get("kvmql_tag"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Snapshot {
        id: name,
        microvm_id,
        provider_id: provider_id.to_string(),
        destination: source,
        tag,
        size_mb,
        taken_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_vm_size_lookup() {
        assert_eq!(vm_size_to_specs("Standard_B1s"), (Some(1), Some(1024)));
        assert_eq!(vm_size_to_specs("Standard_B1ms"), (Some(1), Some(2048)));
        assert_eq!(vm_size_to_specs("Standard_B2s"), (Some(2), Some(4096)));
        assert_eq!(vm_size_to_specs("Standard_B2ms"), (Some(2), Some(8192)));
        assert_eq!(vm_size_to_specs("Standard_D2s_v3"), (Some(2), Some(8192)));
        assert_eq!(vm_size_to_specs("Standard_D4s_v3"), (Some(4), Some(16384)));
        assert_eq!(vm_size_to_specs("Standard_D8s_v3"), (Some(8), Some(32768)));
        assert_eq!(vm_size_to_specs("SomeCustomSize"), (None, None));
    }

    #[test]
    fn test_specs_to_vm_size() {
        assert_eq!(specs_to_vm_size(1, 1024), "Standard_B1s");
        assert_eq!(specs_to_vm_size(2, 4096), "Standard_B2s");
        assert_eq!(specs_to_vm_size(4, 16384), "Standard_D4s_v3");
        // Should pick smallest that fits
        assert_eq!(specs_to_vm_size(1, 512), "Standard_B1s");
        assert_eq!(specs_to_vm_size(2, 2048), "Standard_B2s");
    }

    #[test]
    fn test_mapper_vm_running() {
        let vm_json = json!({
            "name": "test-vm-1",
            "location": "eastus",
            "powerState": "VM running",
            "hardwareProfile": {
                "vmSize": "Standard_B2s"
            },
            "storageProfile": {
                "imageReference": {
                    "offer": "UbuntuServer"
                },
                "osDisk": {
                    "name": "test-vm-1-osdisk"
                }
            },
            "osProfile": {
                "computerName": "test-host"
            },
            "resourceGroup": "my-rg",
            "timeCreated": "2026-01-15T10:00:00Z",
            "tags": {
                "env": "dev"
            }
        });

        let vm = map_vm(&vm_json, "sub-123");

        assert_eq!(vm.id, "test-vm-1");
        assert_eq!(vm.provider_id, "sub-123");
        assert_eq!(vm.tenant, "my-rg");
        assert_eq!(vm.status, "running");
        assert_eq!(vm.image_id.as_deref(), Some("UbuntuServer"));
        assert_eq!(vm.vcpus, Some(2));
        assert_eq!(vm.memory_mb, Some(4096));
        assert_eq!(vm.hostname.as_deref(), Some("test-host"));
        assert_eq!(vm.created_at, "2026-01-15T10:00:00Z");
        assert!(!vm.is_stale);

        // Check labels include location and vmSize
        let labels = vm.labels.unwrap();
        assert_eq!(labels["location"], "eastus");
        assert_eq!(labels["vmSize"], "Standard_B2s");
        assert_eq!(labels["tags"]["env"], "dev");
    }

    #[test]
    fn test_mapper_vm_deallocated() {
        let vm_json = json!({
            "name": "stopped-vm",
            "location": "westus2",
            "powerState": "VM deallocated",
            "hardwareProfile": {
                "vmSize": "Standard_D4s_v3"
            },
            "storageProfile": {
                "imageReference": {
                    "id": "/subscriptions/.../images/my-image"
                },
                "osDisk": {
                    "name": "stopped-vm-osdisk"
                }
            },
            "osProfile": {
                "computerName": "stopped-host"
            },
            "resourceGroup": "prod-rg",
            "timeCreated": "2026-02-20T08:30:00Z"
        });

        let vm = map_vm(&vm_json, "sub-456");

        assert_eq!(vm.id, "stopped-vm");
        assert_eq!(vm.status, "stopped");
        assert_eq!(vm.vcpus, Some(4));
        assert_eq!(vm.memory_mb, Some(16384));
        assert_eq!(
            vm.image_id.as_deref(),
            Some("/subscriptions/.../images/my-image")
        );
    }

    #[test]
    fn test_mapper_vm_stopped() {
        let vm_json = json!({
            "name": "powered-off-vm",
            "location": "centralus",
            "powerState": "VM stopped",
            "hardwareProfile": { "vmSize": "Standard_B1s" },
            "storageProfile": { "imageReference": {} },
            "resourceGroup": "test-rg",
            "timeCreated": ""
        });

        let vm = map_vm(&vm_json, "sub-789");
        assert_eq!(vm.status, "stopped");
        assert_eq!(vm.vcpus, Some(1));
        assert_eq!(vm.memory_mb, Some(1024));
    }

    #[test]
    fn test_mapper_disk() {
        let disk_json = json!({
            "name": "data-disk-1",
            "diskSizeGb": 128,
            "diskState": "Attached",
            "managedBy": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/my-vm",
            "sku": {
                "name": "Premium_LRS"
            },
            "encryption": {
                "type": "EncryptionAtRestWithPlatformKey"
            },
            "diskIOPSReadWrite": 500,
            "timeCreated": "2026-03-01T12:00:00Z",
            "tags": {
                "team": "infra"
            }
        });

        let vol = map_disk(&disk_json, "sub-disk");

        assert_eq!(vol.id, "data-disk-1");
        assert_eq!(vol.provider_id, "sub-disk");
        assert_eq!(vol.size_gb, 128);
        assert_eq!(vol.status, "attached");
        assert_eq!(vol.vol_type, "Premium_LRS");
        assert!(vol.encrypted);
        assert_eq!(vol.iops, Some(500));
        assert_eq!(vol.microvm_id.as_deref(), Some("my-vm"));
        assert_eq!(vol.created_at, "2026-03-01T12:00:00Z");
        assert_eq!(vol.labels.unwrap()["team"], "infra");
    }

    #[test]
    fn test_mapper_disk_unattached() {
        let disk_json = json!({
            "name": "spare-disk",
            "diskSizeGb": 64,
            "diskState": "Unattached",
            "sku": { "name": "Standard_LRS" },
            "encryption": { "type": "None" },
            "timeCreated": "2026-03-10T09:00:00Z"
        });

        let vol = map_disk(&disk_json, "sub-spare");
        assert_eq!(vol.status, "available");
        assert!(!vol.encrypted);
        assert!(vol.microvm_id.is_none());
    }

    #[test]
    fn test_mapper_snapshot() {
        let snap_json = json!({
            "name": "snap-001",
            "creationData": {
                "sourceResourceId": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/os-disk-1"
            },
            "diskSizeBytes": 34359738368_i64,
            "timeCreated": "2026-03-15T14:00:00Z",
            "tags": {
                "kvmql_tag": "v1.0"
            }
        });

        let snap = map_snapshot(&snap_json, "sub-snap");
        assert_eq!(snap.id, "snap-001");
        assert_eq!(snap.provider_id, "sub-snap");
        assert_eq!(snap.microvm_id, "os-disk-1");
        assert_eq!(snap.size_mb, Some(32768));
        assert_eq!(snap.tag.as_deref(), Some("v1.0"));
        assert_eq!(snap.taken_at, "2026-03-15T14:00:00Z");
    }
}
