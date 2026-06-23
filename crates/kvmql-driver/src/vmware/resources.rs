use serde_json::Value;
use tracing::{debug, error};

use crate::provision::{param_str, param_str_or, ProvisionError, ProvisionResult};

use super::VmwareBackend;

/// Resource provisioner for VMware — dispatches to govc or vmrun backend.
pub struct VmwareResourceProvisioner {
    pub(crate) backend: VmwareBackend,
}

impl VmwareResourceProvisioner {
    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "vmware_vm" => self.create_vm(params),
            "vmware_snapshot" => self.create_snapshot(params),
            other => Err(ProvisionError::UnsupportedType(other.to_string())),
        }
    }

    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), ProvisionError> {
        match resource_type {
            "vmware_vm" => self.delete_vm(id),
            "vmware_snapshot" => self.delete_snapshot(id),
            other => Err(ProvisionError::UnsupportedType(other.to_string())),
        }
    }

    pub fn backup(
        &self,
        _resource_type: &str,
        id: &str,
    ) -> Result<ProvisionResult, ProvisionError> {
        let snap_name = format!("orbi-backup-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                debug!(provider = "vmware", vm = id, snapshot = %snap_name, "creating snapshot via vmrun");
                client
                    .snapshot(id, &snap_name)
                    .map_err(ProvisionError::from)?;
            }
            VmwareBackend::Govc(client) => {
                debug!(provider = "vmware", vm = id, snapshot = %snap_name, "creating snapshot via govc");
                client
                    .snapshot_create(id, &snap_name)
                    .map_err(ProvisionError::from)?;
            }
        }
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({
                "snapshot_name": snap_name,
                "vm": id,
            })),
        })
    }

    pub fn restore(
        &self,
        _resource_type: &str,
        id: &str,
    ) -> Result<ProvisionResult, ProvisionError> {
        // Revert to most recent snapshot
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let snaps = client.list_snapshots(id).map_err(ProvisionError::from)?;
                let snap_name = snaps.last().ok_or_else(|| {
                    ProvisionError::Other(format!("no snapshots found for VM '{id}'"))
                })?;
                debug!(provider = "vmware", vm = id, snapshot = %snap_name, "reverting via vmrun");
                client
                    .revert_to_snapshot(id, snap_name)
                    .map_err(ProvisionError::from)?;
                Ok(ProvisionResult {
                    status: "restored".into(),
                    outputs: Some(serde_json::json!({"reverted_to": snap_name})),
                })
            }
            VmwareBackend::Govc(client) => {
                // govc doesn't have a "list snapshots" — we use a convention name
                let snap_name = "orbi-latest";
                debug!(
                    provider = "vmware",
                    vm = id,
                    snapshot = snap_name,
                    "reverting via govc"
                );
                client
                    .snapshot_revert(id, snap_name)
                    .map_err(ProvisionError::from)?;
                Ok(ProvisionResult {
                    status: "restored".into(),
                    outputs: Some(serde_json::json!({"reverted_to": snap_name})),
                })
            }
        }
    }

    pub fn scale(&self, id: &str, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        match &self.backend {
            VmwareBackend::Govc(client) => {
                let cpus = params
                    .get("cpus")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok());
                let mem = params
                    .get("memory_mb")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok());
                debug!(
                    provider = "vmware",
                    vm = id,
                    ?cpus,
                    ?mem,
                    "scaling via govc"
                );
                client
                    .vm_change(id, cpus, mem)
                    .map_err(ProvisionError::from)?;
                Ok(ProvisionResult {
                    status: "updated".into(),
                    outputs: Some(serde_json::json!({"cpus": cpus, "memory_mb": mem})),
                })
            }
            VmwareBackend::Vmrun(_) => Err(ProvisionError::NotImplemented(
                "vmrun does not support live CPU/memory changes; use vSphere (govc) instead".into(),
            )),
        }
    }

    /// Discover existing VMs.
    pub fn discover(&self) -> Result<Vec<Value>, ProvisionError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                debug!(provider = "vmware", backend = "vmrun", "discovering VMs");
                let vms = client.list().map_err(ProvisionError::from)?;
                Ok(vms
                    .into_iter()
                    .map(|vmx| {
                        let name = std::path::Path::new(&vmx)
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("unknown")
                            .to_string();
                        serde_json::json!({
                            "id": name,
                            "resource_type": "vmware_vm",
                            "status": "running",
                            "config": {"vmx_path": vmx},
                        })
                    })
                    .collect())
            }
            VmwareBackend::Govc(client) => {
                debug!(provider = "vmware", backend = "govc", "discovering VMs");
                let vm_paths = client.find("m").map_err(ProvisionError::from)?;
                Ok(vm_paths
                    .into_iter()
                    .map(|path| {
                        let name = path.rsplit('/').next().unwrap_or(&path).to_string();
                        serde_json::json!({
                            "id": name,
                            "resource_type": "vmware_vm",
                            "status": "discovered",
                            "config": {"vsphere_path": path},
                        })
                    })
                    .collect())
            }
        }
    }

    // ── Internal methods ────────────────────────────────────────────

    fn create_vm(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let id = param_str(params, "id")?;

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                let source = param_str(params, "source")?;
                let dest = param_str(params, "path")?;
                let clone_type = param_str_or(params, "clone_type", "full");

                debug!(provider = "vmware", vm = %id, source = %source, "cloning via vmrun");
                client
                    .clone_vm(&source, &dest, &clone_type)
                    .map_err(ProvisionError::from)?;

                // Start the VM
                client.start(&dest).map_err(ProvisionError::from)?;

                // Try to get guest IP (may fail if VMware Tools not installed)
                let ip = client.get_guest_ip(&dest).ok();

                Ok(ProvisionResult {
                    status: "created".into(),
                    outputs: Some(serde_json::json!({
                        "vmx_path": dest,
                        "source": source,
                        "clone_type": clone_type,
                        "guest_ip": ip,
                    })),
                })
            }
            VmwareBackend::Govc(client) => {
                if let Ok(template) = param_str(params, "template") {
                    // Clone from template
                    debug!(provider = "vmware", vm = %id, template = %template, "cloning via govc");
                    let result = client
                        .vm_clone(&template, &id, true)
                        .map_err(ProvisionError::from)?;
                    Ok(ProvisionResult {
                        status: "created".into(),
                        outputs: Some(result),
                    })
                } else {
                    // Create from scratch
                    let cpus: i32 = param_str_or(params, "cpus", "2").parse().unwrap_or(2);
                    let mem: i32 = param_str_or(params, "memory_mb", "4096")
                        .parse()
                        .unwrap_or(4096);
                    let disk: Option<i32> = params
                        .get("disk_gb")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok());
                    let network = params.get("network").and_then(|v| v.as_str());
                    let guest_os = params.get("guest_os").and_then(|v| v.as_str());

                    debug!(provider = "vmware", vm = %id, cpus, mem, "creating via govc");
                    let result = client
                        .vm_create(&id, cpus, mem, disk, network, guest_os)
                        .map_err(ProvisionError::from)?;

                    // Power on
                    if let Err(e) = client.vm_power(&id, "on") {
                        error!(provider = "vmware", vm = %id, error = %e, "failed to power on after create");
                    }

                    Ok(ProvisionResult {
                        status: "created".into(),
                        outputs: Some(result),
                    })
                }
            }
        }
    }

    fn delete_vm(&self, id: &str) -> Result<(), ProvisionError> {
        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                debug!(provider = "vmware", vm = id, "deleting via vmrun");
                // Stop first, then delete
                let _ = client.stop(id); // ignore error if already stopped
                client.delete_vm(id).map_err(ProvisionError::from)?;
                Ok(())
            }
            VmwareBackend::Govc(client) => {
                debug!(provider = "vmware", vm = id, "destroying via govc");
                client.vm_destroy(id).map_err(ProvisionError::from)?;
                Ok(())
            }
        }
    }

    fn create_snapshot(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let vm = param_str(params, "vm")?;
        let name = param_str(params, "name")?;

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                client.snapshot(&vm, &name).map_err(ProvisionError::from)?;
            }
            VmwareBackend::Govc(client) => {
                client
                    .snapshot_create(&vm, &name)
                    .map_err(ProvisionError::from)?;
            }
        }

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({"vm": vm, "snapshot": name})),
        })
    }

    fn delete_snapshot(&self, id: &str) -> Result<(), ProvisionError> {
        // id format: "vm_name/snapshot_name"
        let parts: Vec<&str> = id.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(ProvisionError::InvalidParam(
                "snapshot id must be 'vm_name/snapshot_name'".into(),
            ));
        }
        let (vm, snap) = (parts[0], parts[1]);

        match &self.backend {
            VmwareBackend::Vmrun(client) => {
                client
                    .delete_snapshot(vm, snap)
                    .map_err(ProvisionError::from)?;
            }
            VmwareBackend::Govc(client) => {
                client
                    .snapshot_remove(vm, snap)
                    .map_err(ProvisionError::from)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::vmrun::VmrunClient;
    use super::*;

    #[test]
    fn test_unsupported_resource_type() {
        let provisioner = VmwareResourceProvisioner {
            backend: VmwareBackend::Vmrun(VmrunClient::new()),
        };
        let err = provisioner
            .create("unknown_type", &serde_json::json!({}))
            .unwrap_err();
        assert!(err.to_string().contains("unsupported resource type"));
    }

    #[test]
    fn test_snapshot_id_parsing() {
        // Valid format
        let parts: Vec<&str> = "my-vm/pre-upgrade".splitn(2, '/').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "my-vm");
        assert_eq!(parts[1], "pre-upgrade");

        // Invalid format
        let parts: Vec<&str> = "no-slash".splitn(2, '/').collect();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn test_vmrun_scale_unsupported() {
        let provisioner = VmwareResourceProvisioner {
            backend: VmwareBackend::Vmrun(VmrunClient::new()),
        };
        let err = provisioner
            .scale("test-vm", &serde_json::json!({"cpus": "4"}))
            .unwrap_err();
        assert!(err.to_string().contains("vmrun does not support"));
    }
}
