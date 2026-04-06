use serde_json::Value;

use crate::types::{MicroVm, Snapshot, Volume};

/// Map an EC2 instance type string to (vcpus, memory_mb).
/// Returns `(None, None)` for unrecognised instance types.
pub fn instance_type_to_specs(instance_type: &str) -> (Option<i32>, Option<i32>) {
    match instance_type {
        "t3.nano" => (Some(2), Some(512)),
        "t3.micro" => (Some(2), Some(1024)),
        "t3.small" => (Some(2), Some(2048)),
        "t3.medium" => (Some(2), Some(4096)),
        "t3.large" => (Some(2), Some(8192)),
        "t3.xlarge" => (Some(4), Some(16384)),
        "t3.2xlarge" => (Some(8), Some(32768)),
        "m5.large" => (Some(2), Some(8192)),
        "m5.xlarge" => (Some(4), Some(16384)),
        "m5.2xlarge" => (Some(8), Some(32768)),
        "c5.large" => (Some(2), Some(4096)),
        "c5.xlarge" => (Some(4), Some(8192)),
        "r5.large" => (Some(2), Some(16384)),
        "r5.xlarge" => (Some(4), Some(32768)),
        _ => (None, None),
    }
}

/// Pick the closest EC2 instance type for the requested vcpus and memory_mb.
/// Falls back to `t3.medium` if nothing is a reasonable match.
pub fn specs_to_instance_type(vcpus: i32, memory_mb: i32) -> &'static str {
    // Table ordered by (vcpus, memory_mb) ascending.
    let table: &[(&str, i32, i32)] = &[
        ("t3.nano", 2, 512),
        ("t3.micro", 2, 1024),
        ("t3.small", 2, 2048),
        ("t3.medium", 2, 4096),
        ("t3.large", 2, 8192),
        ("c5.large", 2, 4096),
        ("m5.large", 2, 8192),
        ("t3.xlarge", 4, 16384),
        ("c5.xlarge", 4, 8192),
        ("m5.xlarge", 4, 16384),
        ("t3.2xlarge", 8, 32768),
        ("m5.2xlarge", 8, 32768),
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

    best.map(|(name, _)| name).unwrap_or("t3.medium")
}

/// Map EC2 instance state name to a KVMQL status string.
fn map_instance_state(state_name: &str) -> &'static str {
    match state_name {
        "running" => "running",
        "stopped" => "stopped",
        "terminated" => "destroyed",
        "pending" => "starting",
        "stopping" => "stopping",
        "shutting-down" => "destroying",
        _ => "unknown",
    }
}

/// Extract the Name tag from an EC2 Tags array.
fn extract_name_tag(json: &Value) -> Option<String> {
    json.get("Tags")
        .and_then(|tags| tags.as_array())
        .and_then(|tags| {
            tags.iter().find_map(|tag| {
                if tag.get("Key").and_then(|k| k.as_str()) == Some("Name") {
                    tag.get("Value").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        })
}

/// Convert EC2 instance JSON (from `aws ec2 describe-instances`) to `MicroVm`.
pub fn map_ec2_instance(json: &Value, provider_id: &str) -> MicroVm {
    let instance_id = json["InstanceId"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    let name = extract_name_tag(json).unwrap_or_else(|| instance_id.clone());

    let state_name = json["State"]["Name"]
        .as_str()
        .unwrap_or("unknown");
    let status = map_instance_state(state_name).to_string();

    let instance_type = json["InstanceType"]
        .as_str()
        .unwrap_or("");
    let (vcpus, memory_mb) = instance_type_to_specs(instance_type);

    let image_id = json["ImageId"]
        .as_str()
        .map(|s| s.to_string());

    let created_at = json["LaunchTime"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let hostname = json["PublicDnsName"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| json["PrivateDnsName"].as_str().filter(|s| !s.is_empty()))
        .map(|s| s.to_string());

    let az = json["Placement"]["AvailabilityZone"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let labels = {
        let mut label_map = serde_json::Map::new();
        label_map.insert(
            "availability_zone".to_string(),
            Value::String(az),
        );
        label_map.insert(
            "instance_type".to_string(),
            Value::String(instance_type.to_string()),
        );
        if let Some(pub_ip) = json.get("PublicIpAddress").and_then(|v| v.as_str()) {
            label_map.insert(
                "public_ip".to_string(),
                Value::String(pub_ip.to_string()),
            );
        }
        if let Some(priv_ip) = json.get("PrivateIpAddress").and_then(|v| v.as_str()) {
            label_map.insert(
                "private_ip".to_string(),
                Value::String(priv_ip.to_string()),
            );
        }
        // Convert Tags array to a map
        if let Some(tags) = json.get("Tags").and_then(|t| t.as_array()) {
            let mut tag_map = serde_json::Map::new();
            for tag in tags {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|k| k.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    tag_map.insert(k.to_string(), Value::String(v.to_string()));
                }
            }
            label_map.insert("tags".to_string(), Value::Object(tag_map));
        }
        Some(Value::Object(label_map))
    };

    MicroVm {
        id: name,
        provider_id: provider_id.to_string(),
        tenant: instance_id,
        status,
        image_id,
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

/// Convert EBS volume JSON (from `aws ec2 describe-volumes`) to `Volume`.
pub fn map_ebs_volume(json: &Value, provider_id: &str) -> Volume {
    let volume_id = json["VolumeId"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    let size_gb = json["Size"].as_i64().unwrap_or(0);

    let state = json["State"]
        .as_str()
        .unwrap_or("unknown");
    let status = match state {
        "in-use" => "attached",
        "available" => "available",
        "creating" => "creating",
        "deleting" => "deleting",
        "deleted" => "deleted",
        _ => "unknown",
    }
    .to_string();

    // Extract attached instance ID if any
    let attached_to = json
        .get("Attachments")
        .and_then(|a| a.as_array())
        .and_then(|arr| arr.first())
        .and_then(|att| att.get("InstanceId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let device_name = json
        .get("Attachments")
        .and_then(|a| a.as_array())
        .and_then(|arr| arr.first())
        .and_then(|att| att.get("Device"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let vol_type = json["VolumeType"]
        .as_str()
        .unwrap_or("gp2")
        .to_string();

    let encrypted = json["Encrypted"].as_bool().unwrap_or(false);

    let created_at = json["CreateTime"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let iops = json["Iops"].as_i64().map(|v| v as i32);

    // Convert Tags array to a map
    let labels = json.get("Tags").and_then(|t| t.as_array()).map(|tags| {
        let mut tag_map = serde_json::Map::new();
        for tag in tags {
            if let (Some(k), Some(v)) = (
                tag.get("Key").and_then(|k| k.as_str()),
                tag.get("Value").and_then(|v| v.as_str()),
            ) {
                tag_map.insert(k.to_string(), Value::String(v.to_string()));
            }
        }
        Value::Object(tag_map)
    });

    Volume {
        id: volume_id,
        provider_id: provider_id.to_string(),
        microvm_id: attached_to,
        vol_type,
        size_gb,
        status,
        device_name,
        iops,
        encrypted,
        created_at,
        labels,
    }
}

/// Convert EBS snapshot JSON to `Snapshot`.
pub fn map_ebs_snapshot(json: &Value, provider_id: &str) -> Snapshot {
    let snapshot_id = json["SnapshotId"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    let volume_id = json["VolumeId"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let size_mb = json["VolumeSize"]
        .as_i64()
        .map(|gb| gb * 1024);

    let taken_at = json["StartTime"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let tag = json
        .get("Tags")
        .and_then(|t| t.as_array())
        .and_then(|tags| {
            tags.iter().find_map(|tag| {
                if tag.get("Key").and_then(|k| k.as_str()) == Some("kvmql_tag") {
                    tag.get("Value").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
        });

    let description = json["Description"]
        .as_str()
        .unwrap_or("")
        .to_string();

    Snapshot {
        id: snapshot_id,
        microvm_id: volume_id,
        provider_id: provider_id.to_string(),
        destination: description,
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
    fn test_instance_type_lookup() {
        assert_eq!(instance_type_to_specs("t3.micro"), (Some(2), Some(1024)));
        assert_eq!(instance_type_to_specs("t3.small"), (Some(2), Some(2048)));
        assert_eq!(instance_type_to_specs("t3.medium"), (Some(2), Some(4096)));
        assert_eq!(instance_type_to_specs("t3.large"), (Some(2), Some(8192)));
        assert_eq!(instance_type_to_specs("t3.xlarge"), (Some(4), Some(16384)));
        assert_eq!(instance_type_to_specs("m5.large"), (Some(2), Some(8192)));
        assert_eq!(instance_type_to_specs("m5.xlarge"), (Some(4), Some(16384)));
        assert_eq!(instance_type_to_specs("SomeCustomType"), (None, None));
    }

    #[test]
    fn test_specs_to_instance_type() {
        assert_eq!(specs_to_instance_type(2, 1024), "t3.micro");
        assert_eq!(specs_to_instance_type(2, 4096), "t3.medium");
        assert_eq!(specs_to_instance_type(4, 16384), "t3.xlarge");
        // Should pick smallest that fits
        assert_eq!(specs_to_instance_type(1, 512), "t3.nano");
        assert_eq!(specs_to_instance_type(2, 2048), "t3.small");
    }

    #[test]
    fn test_map_ec2_running() {
        let instance_json = json!({
            "InstanceId": "i-1234567890abcdef0",
            "InstanceType": "t3.micro",
            "State": {"Name": "running", "Code": 16},
            "PublicIpAddress": "1.2.3.4",
            "PrivateIpAddress": "10.0.1.100",
            "PublicDnsName": "ec2-1-2-3-4.compute-1.amazonaws.com",
            "PrivateDnsName": "ip-10-0-1-100.ec2.internal",
            "ImageId": "ami-12345678",
            "LaunchTime": "2026-01-01T00:00:00Z",
            "Placement": {
                "AvailabilityZone": "us-east-1a"
            },
            "Tags": [
                {"Key": "Name", "Value": "my-vm"},
                {"Key": "env", "Value": "dev"}
            ]
        });

        let vm = map_ec2_instance(&instance_json, "aws-account-123");

        assert_eq!(vm.id, "my-vm");
        assert_eq!(vm.provider_id, "aws-account-123");
        assert_eq!(vm.tenant, "i-1234567890abcdef0");
        assert_eq!(vm.status, "running");
        assert_eq!(vm.image_id.as_deref(), Some("ami-12345678"));
        assert_eq!(vm.vcpus, Some(2));
        assert_eq!(vm.memory_mb, Some(1024));
        assert_eq!(
            vm.hostname.as_deref(),
            Some("ec2-1-2-3-4.compute-1.amazonaws.com")
        );
        assert_eq!(vm.created_at, "2026-01-01T00:00:00Z");
        assert!(!vm.is_stale);

        // Check labels
        let labels = vm.labels.unwrap();
        assert_eq!(labels["availability_zone"], "us-east-1a");
        assert_eq!(labels["instance_type"], "t3.micro");
        assert_eq!(labels["public_ip"], "1.2.3.4");
        assert_eq!(labels["private_ip"], "10.0.1.100");
        assert_eq!(labels["tags"]["Name"], "my-vm");
        assert_eq!(labels["tags"]["env"], "dev");
    }

    #[test]
    fn test_map_ec2_stopped() {
        let instance_json = json!({
            "InstanceId": "i-abcdef1234567890",
            "InstanceType": "t3.large",
            "State": {"Name": "stopped", "Code": 80},
            "PrivateIpAddress": "10.0.2.50",
            "PrivateDnsName": "ip-10-0-2-50.ec2.internal",
            "ImageId": "ami-87654321",
            "LaunchTime": "2026-02-15T12:00:00Z",
            "Placement": {
                "AvailabilityZone": "us-west-2b"
            },
            "Tags": [
                {"Key": "Name", "Value": "stopped-vm"}
            ]
        });

        let vm = map_ec2_instance(&instance_json, "aws-456");

        assert_eq!(vm.id, "stopped-vm");
        assert_eq!(vm.status, "stopped");
        assert_eq!(vm.vcpus, Some(2));
        assert_eq!(vm.memory_mb, Some(8192));
    }

    #[test]
    fn test_map_ec2_terminated() {
        let instance_json = json!({
            "InstanceId": "i-terminated123",
            "InstanceType": "t3.medium",
            "State": {"Name": "terminated", "Code": 48},
            "ImageId": "ami-000",
            "LaunchTime": "2026-03-01T00:00:00Z",
            "Placement": {"AvailabilityZone": "eu-west-1a"}
        });

        let vm = map_ec2_instance(&instance_json, "aws-789");

        assert_eq!(vm.id, "i-terminated123"); // No Name tag, falls back to InstanceId
        assert_eq!(vm.status, "destroyed");
    }

    #[test]
    fn test_map_ec2_no_name_tag() {
        let instance_json = json!({
            "InstanceId": "i-noname",
            "InstanceType": "m5.large",
            "State": {"Name": "running"},
            "LaunchTime": "",
            "Placement": {"AvailabilityZone": "us-east-1a"}
        });

        let vm = map_ec2_instance(&instance_json, "prov-1");
        assert_eq!(vm.id, "i-noname");
    }

    #[test]
    fn test_map_ebs_volume_attached() {
        let vol_json = json!({
            "VolumeId": "vol-12345",
            "Size": 100,
            "State": "in-use",
            "VolumeType": "gp3",
            "Encrypted": true,
            "Iops": 3000,
            "CreateTime": "2026-01-10T08:00:00Z",
            "Attachments": [{
                "InstanceId": "i-abcdef",
                "Device": "/dev/xvdf",
                "State": "attached"
            }],
            "Tags": [
                {"Key": "team", "Value": "infra"}
            ]
        });

        let vol = map_ebs_volume(&vol_json, "aws-vol-prov");

        assert_eq!(vol.id, "vol-12345");
        assert_eq!(vol.provider_id, "aws-vol-prov");
        assert_eq!(vol.size_gb, 100);
        assert_eq!(vol.status, "attached");
        assert_eq!(vol.vol_type, "gp3");
        assert!(vol.encrypted);
        assert_eq!(vol.iops, Some(3000));
        assert_eq!(vol.microvm_id.as_deref(), Some("i-abcdef"));
        assert_eq!(vol.device_name.as_deref(), Some("/dev/xvdf"));
        assert_eq!(vol.created_at, "2026-01-10T08:00:00Z");
        assert_eq!(vol.labels.unwrap()["team"], "infra");
    }

    #[test]
    fn test_map_ebs_volume_available() {
        let vol_json = json!({
            "VolumeId": "vol-spare",
            "Size": 50,
            "State": "available",
            "VolumeType": "gp2",
            "Encrypted": false,
            "CreateTime": "2026-02-01T00:00:00Z"
        });

        let vol = map_ebs_volume(&vol_json, "prov-spare");
        assert_eq!(vol.status, "available");
        assert!(!vol.encrypted);
        assert!(vol.microvm_id.is_none());
        assert!(vol.device_name.is_none());
    }

    #[test]
    fn test_map_ebs_snapshot() {
        let snap_json = json!({
            "SnapshotId": "snap-12345",
            "VolumeId": "vol-source",
            "VolumeSize": 100,
            "StartTime": "2026-03-15T14:00:00Z",
            "Description": "backup of vol-source",
            "Tags": [
                {"Key": "kvmql_tag", "Value": "v2.0"}
            ]
        });

        let snap = map_ebs_snapshot(&snap_json, "aws-snap-prov");
        assert_eq!(snap.id, "snap-12345");
        assert_eq!(snap.provider_id, "aws-snap-prov");
        assert_eq!(snap.microvm_id, "vol-source");
        assert_eq!(snap.size_mb, Some(102400));
        assert_eq!(snap.tag.as_deref(), Some("v2.0"));
        assert_eq!(snap.taken_at, "2026-03-15T14:00:00Z");
        assert_eq!(snap.destination, "backup of vol-source");
    }
}
