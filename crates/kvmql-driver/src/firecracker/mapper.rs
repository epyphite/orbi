use crate::types::*;

use super::client::{ClientError, FirecrackerClient};

/// Default boot arguments when none are supplied via metadata.
const DEFAULT_BOOT_ARGS: &str = "console=ttyS0 reboot=k panic=1";

/// Execute the full VM creation sequence against a Firecracker instance:
///
/// 1. PUT /machine-config  — set vCPUs and memory
/// 2. PUT /boot-source     — set kernel and boot args
/// 3. PUT /drives/rootfs   — attach the root filesystem
/// 4. (optional) PUT /network-interfaces/eth0 — attach a TAP device
/// 5. PUT /actions          — start the instance
///
/// Returns the populated `MicroVm` struct on success.
pub async fn create_vm(
    client: &FirecrackerClient,
    params: &CreateParams,
    image_ref: &ResolvedImage,
    vm_id: &str,
    provider_id: &str,
) -> Result<MicroVm, ClientError> {
    // 1. Machine config
    client
        .set_machine_config(params.vcpus, params.memory_mb)
        .await?;

    // 2. Boot source
    let boot_args = params
        .metadata
        .as_ref()
        .and_then(|m| m.get("boot_args"))
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_BOOT_ARGS);

    client
        .set_boot_source(&image_ref.kernel_path, boot_args)
        .await?;

    // 3. Root drive
    client
        .add_drive("rootfs", &image_ref.rootfs_path, true, false)
        .await?;

    // 4. Network (optional)
    if let Some(ref network) = params.network {
        let guest_mac = params
            .metadata
            .as_ref()
            .and_then(|m| m.get("guest_mac"))
            .and_then(|v| v.as_str());
        client.add_network("eth0", network, guest_mac).await?;
    }

    // 5. Start
    client.start_instance().await?;

    let now = chrono::Utc::now().to_rfc3339();
    Ok(MicroVm {
        id: vm_id.to_string(),
        provider_id: provider_id.to_string(),
        tenant: params.tenant.clone(),
        status: "running".into(),
        image_id: Some(params.image_id.clone()),
        vcpus: Some(params.vcpus),
        memory_mb: Some(params.memory_mb),
        cpu_pct: Some(0.0),
        mem_used_mb: Some(0),
        net_rx_kbps: Some(0.0),
        net_tx_kbps: Some(0.0),
        hostname: params.hostname.clone(),
        metadata: params.metadata.clone(),
        labels: params.labels.clone(),
        created_at: now.clone(),
        last_seen: Some(now),
        is_stale: false,
    })
}

/// Intermediate struct representing a resolved image with kernel and rootfs paths.
/// This decouples the mapper from the image registry lookup mechanism.
#[derive(Debug, Clone)]
pub struct ResolvedImage {
    pub kernel_path: String,
    pub rootfs_path: String,
}

impl ResolvedImage {
    /// Build from an `ImageRef`. Returns `None` if either path is missing.
    pub fn from_image_ref(image_ref: &ImageRef) -> Option<Self> {
        Some(Self {
            kernel_path: image_ref.kernel_path.clone()?,
            rootfs_path: image_ref.rootfs_path.clone()?,
        })
    }
}

/// Build the API call sequence description for a `CreateParams`.
/// This is useful for logging / dry-run / testing without an actual socket.
pub fn describe_create_sequence(
    params: &CreateParams,
    image: &ResolvedImage,
) -> Vec<ApiCallDescription> {
    let boot_args = params
        .metadata
        .as_ref()
        .and_then(|m| m.get("boot_args"))
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_BOOT_ARGS);

    let mut calls = vec![
        ApiCallDescription {
            method: "PUT".into(),
            path: "/machine-config".into(),
            body: serde_json::json!({
                "vcpu_count": params.vcpus,
                "mem_size_mib": params.memory_mb,
            }),
        },
        ApiCallDescription {
            method: "PUT".into(),
            path: "/boot-source".into(),
            body: serde_json::json!({
                "kernel_image_path": image.kernel_path,
                "boot_args": boot_args,
            }),
        },
        ApiCallDescription {
            method: "PUT".into(),
            path: "/drives/rootfs".into(),
            body: serde_json::json!({
                "drive_id": "rootfs",
                "path_on_host": image.rootfs_path,
                "is_root_device": true,
                "is_read_only": false,
            }),
        },
    ];

    if let Some(ref network) = params.network {
        let mut net_body = serde_json::json!({
            "iface_id": "eth0",
            "host_dev_name": network,
        });
        if let Some(mac) = params
            .metadata
            .as_ref()
            .and_then(|m| m.get("guest_mac"))
            .and_then(|v| v.as_str())
        {
            net_body["guest_mac"] = serde_json::Value::String(mac.to_string());
        }
        calls.push(ApiCallDescription {
            method: "PUT".into(),
            path: "/network-interfaces/eth0".into(),
            body: net_body,
        });
    }

    calls.push(ApiCallDescription {
        method: "PUT".into(),
        path: "/actions".into(),
        body: serde_json::json!({
            "action_type": "InstanceStart",
        }),
    });

    calls
}

/// Description of a single Firecracker API call (method, path, JSON body).
#[derive(Debug, Clone)]
pub struct ApiCallDescription {
    pub method: String,
    pub path: String,
    pub body: serde_json::Value,
}

/// Build a `MetricSample` from a machine-config response. Firecracker does
/// not expose per-VM runtime metrics via its REST API, so we return the
/// configured values as a baseline.
pub fn metrics_from_machine_config(
    vm_id: &str,
    _vcpus: i32,
    mem_mib: i32,
) -> MetricSample {
    MetricSample {
        microvm_id: vm_id.to_string(),
        sampled_at: chrono::Utc::now().to_rfc3339(),
        cpu_pct: None,
        mem_used_mb: Some(mem_mib),
        net_rx_kbps: None,
        net_tx_kbps: None,
    }
}

/// Build a `Snapshot` struct from the given paths and VM ID.
pub fn build_snapshot(
    vm_id: &str,
    provider_id: &str,
    destination: &str,
    tag: Option<&str>,
) -> Snapshot {
    Snapshot {
        id: uuid::Uuid::new_v4().to_string(),
        microvm_id: vm_id.to_string(),
        provider_id: provider_id.to_string(),
        destination: destination.to_string(),
        tag: tag.map(|t| t.to_string()),
        size_mb: None,
        taken_at: chrono::Utc::now().to_rfc3339(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_params(with_network: bool) -> CreateParams {
        CreateParams {
            id: Some("vm-1".into()),
            tenant: "acme".into(),
            vcpus: 2,
            memory_mb: 512,
            image_id: "img-test".into(),
            hostname: Some("test-host".into()),
            network: if with_network {
                Some("tap0".into())
            } else {
                None
            },
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

    fn test_image() -> ResolvedImage {
        ResolvedImage {
            kernel_path: "/boot/vmlinux".into(),
            rootfs_path: "/images/rootfs.ext4".into(),
        }
    }

    // ── describe_create_sequence tests ─────────────────────────────

    #[test]
    fn test_create_sequence_without_network() {
        let params = test_params(false);
        let image = test_image();
        let calls = describe_create_sequence(&params, &image);

        // Should have: machine-config, boot-source, drives/rootfs, actions
        assert_eq!(calls.len(), 4);

        assert_eq!(calls[0].method, "PUT");
        assert_eq!(calls[0].path, "/machine-config");
        assert_eq!(calls[0].body["vcpu_count"], 2);
        assert_eq!(calls[0].body["mem_size_mib"], 512);

        assert_eq!(calls[1].method, "PUT");
        assert_eq!(calls[1].path, "/boot-source");
        assert_eq!(calls[1].body["kernel_image_path"], "/boot/vmlinux");
        assert_eq!(
            calls[1].body["boot_args"],
            "console=ttyS0 reboot=k panic=1"
        );

        assert_eq!(calls[2].method, "PUT");
        assert_eq!(calls[2].path, "/drives/rootfs");
        assert_eq!(calls[2].body["drive_id"], "rootfs");
        assert_eq!(calls[2].body["path_on_host"], "/images/rootfs.ext4");
        assert_eq!(calls[2].body["is_root_device"], true);
        assert_eq!(calls[2].body["is_read_only"], false);

        assert_eq!(calls[3].method, "PUT");
        assert_eq!(calls[3].path, "/actions");
        assert_eq!(calls[3].body["action_type"], "InstanceStart");
    }

    #[test]
    fn test_create_sequence_with_network() {
        let params = test_params(true);
        let image = test_image();
        let calls = describe_create_sequence(&params, &image);

        // Should have: machine-config, boot-source, drives/rootfs, network, actions
        assert_eq!(calls.len(), 5);

        assert_eq!(calls[3].method, "PUT");
        assert_eq!(calls[3].path, "/network-interfaces/eth0");
        assert_eq!(calls[3].body["iface_id"], "eth0");
        assert_eq!(calls[3].body["host_dev_name"], "tap0");
        // No guest_mac since metadata is None
        assert!(calls[3].body.get("guest_mac").is_none());

        assert_eq!(calls[4].method, "PUT");
        assert_eq!(calls[4].path, "/actions");
    }

    #[test]
    fn test_create_sequence_with_custom_boot_args() {
        let mut params = test_params(false);
        params.metadata = Some(serde_json::json!({
            "boot_args": "console=ttyS0 init=/sbin/init"
        }));
        let image = test_image();
        let calls = describe_create_sequence(&params, &image);

        assert_eq!(calls[1].body["boot_args"], "console=ttyS0 init=/sbin/init");
    }

    #[test]
    fn test_create_sequence_with_guest_mac() {
        let mut params = test_params(true);
        params.metadata = Some(serde_json::json!({
            "guest_mac": "AA:FC:00:00:00:01"
        }));
        let image = test_image();
        let calls = describe_create_sequence(&params, &image);

        assert_eq!(calls.len(), 5);
        assert_eq!(calls[3].body["guest_mac"], "AA:FC:00:00:00:01");
    }

    // ── ResolvedImage tests ────────────────────────────────────────

    #[test]
    fn test_resolved_image_from_image_ref_success() {
        let image_ref = ImageRef {
            image_id: "img-1".into(),
            provider_id: "fc-img-1".into(),
            resolved_type: "rootfs".into(),
            kernel_path: Some("/boot/vmlinux".into()),
            rootfs_path: Some("/images/rootfs.ext4".into()),
            cloud_ref: None,
        };
        let resolved = ResolvedImage::from_image_ref(&image_ref).unwrap();
        assert_eq!(resolved.kernel_path, "/boot/vmlinux");
        assert_eq!(resolved.rootfs_path, "/images/rootfs.ext4");
    }

    #[test]
    fn test_resolved_image_from_image_ref_missing_kernel() {
        let image_ref = ImageRef {
            image_id: "img-1".into(),
            provider_id: "fc-img-1".into(),
            resolved_type: "rootfs".into(),
            kernel_path: None,
            rootfs_path: Some("/images/rootfs.ext4".into()),
            cloud_ref: None,
        };
        assert!(ResolvedImage::from_image_ref(&image_ref).is_none());
    }

    #[test]
    fn test_resolved_image_from_image_ref_missing_rootfs() {
        let image_ref = ImageRef {
            image_id: "img-1".into(),
            provider_id: "fc-img-1".into(),
            resolved_type: "rootfs".into(),
            kernel_path: Some("/boot/vmlinux".into()),
            rootfs_path: None,
            cloud_ref: None,
        };
        assert!(ResolvedImage::from_image_ref(&image_ref).is_none());
    }

    // ── metrics_from_machine_config ────────────────────────────────

    #[test]
    fn test_metrics_from_machine_config() {
        let sample = metrics_from_machine_config("vm-1", 4, 1024);
        assert_eq!(sample.microvm_id, "vm-1");
        assert_eq!(sample.mem_used_mb, Some(1024));
        assert!(sample.cpu_pct.is_none());
        assert!(!sample.sampled_at.is_empty());
    }

    // ── build_snapshot ─────────────────────────────────────────────

    #[test]
    fn test_build_snapshot() {
        let snap = build_snapshot("vm-1", "fc-vm-1", "/snap/vm1", Some("v1"));
        assert_eq!(snap.microvm_id, "vm-1");
        assert_eq!(snap.provider_id, "fc-vm-1");
        assert_eq!(snap.destination, "/snap/vm1");
        assert_eq!(snap.tag.as_deref(), Some("v1"));
        assert!(!snap.id.is_empty());
        assert!(!snap.taken_at.is_empty());
    }

    #[test]
    fn test_build_snapshot_no_tag() {
        let snap = build_snapshot("vm-2", "fc-vm-2", "/snap/vm2", None);
        assert!(snap.tag.is_none());
    }
}
