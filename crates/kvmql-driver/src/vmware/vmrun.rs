use std::path::PathBuf;
use std::process::Command;

/// Client for VMware Workstation/Fusion via the `vmrun` CLI.
#[derive(Debug, Clone)]
pub struct VmrunClient {
    vmrun_path: PathBuf,
}

impl Default for VmrunClient {
    fn default() -> Self {
        Self::new()
    }
}

impl VmrunClient {
    pub fn new() -> Self {
        Self {
            vmrun_path: find_vmrun().unwrap_or_else(|| PathBuf::from("vmrun")),
        }
    }

    pub fn check_available(&self) -> Result<(), String> {
        let output = Command::new(&self.vmrun_path)
            .arg("list")
            .output()
            .map_err(|e| format!("vmrun not found: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "vmrun failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(())
    }

    /// List running VMs — returns list of .vmx paths.
    pub fn list(&self) -> Result<Vec<String>, String> {
        let output = self
            .run(&["list"])
            .map_err(|e| format!("vmrun list failed: {e}"))?;
        let mut vms = Vec::new();
        for line in output.lines().skip(1) {
            // skip "Total running VMs: N" header
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                vms.push(trimmed.to_string());
            }
        }
        Ok(vms)
    }

    /// Start a VM from its .vmx path.
    pub fn start(&self, vmx_path: &str) -> Result<(), String> {
        self.run(&["start", vmx_path, "nogui"])?;
        Ok(())
    }

    /// Stop a VM (soft shutdown).
    pub fn stop(&self, vmx_path: &str) -> Result<(), String> {
        self.run(&["stop", vmx_path, "soft"])?;
        Ok(())
    }

    /// Suspend (pause) a VM.
    pub fn suspend(&self, vmx_path: &str) -> Result<(), String> {
        self.run(&["suspend", vmx_path, "soft"])?;
        Ok(())
    }

    /// Unpause / resume a suspended VM.
    pub fn unpause(&self, vmx_path: &str) -> Result<(), String> {
        // vmrun doesn't have a separate "resume" — start resumes a suspended VM
        self.run(&["start", vmx_path, "nogui"])?;
        Ok(())
    }

    /// Create a snapshot.
    pub fn snapshot(&self, vmx_path: &str, name: &str) -> Result<(), String> {
        self.run(&["snapshot", vmx_path, name])?;
        Ok(())
    }

    /// Revert to a snapshot.
    pub fn revert_to_snapshot(&self, vmx_path: &str, name: &str) -> Result<(), String> {
        self.run(&["revertToSnapshot", vmx_path, name])?;
        Ok(())
    }

    /// Delete a snapshot.
    pub fn delete_snapshot(&self, vmx_path: &str, name: &str) -> Result<(), String> {
        self.run(&["deleteSnapshot", vmx_path, name])?;
        Ok(())
    }

    /// List snapshots for a VM.
    pub fn list_snapshots(&self, vmx_path: &str) -> Result<Vec<String>, String> {
        let output = self.run(&["listSnapshots", vmx_path])?;
        let mut snaps = Vec::new();
        for line in output.lines().skip(1) {
            // skip "Total snapshots: N" header
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                snaps.push(trimmed.to_string());
            }
        }
        Ok(snaps)
    }

    /// Clone a VM (full or linked).
    pub fn clone_vm(
        &self,
        source_vmx: &str,
        dest_vmx: &str,
        clone_type: &str,
    ) -> Result<(), String> {
        let ct = if clone_type == "linked" {
            "linked"
        } else {
            "full"
        };
        self.run(&["clone", source_vmx, dest_vmx, ct])?;
        Ok(())
    }

    /// Delete a VM entirely.
    pub fn delete_vm(&self, vmx_path: &str) -> Result<(), String> {
        self.run(&["deleteVM", vmx_path])?;
        Ok(())
    }

    /// Get guest IP address (requires VMware Tools).
    pub fn get_guest_ip(&self, vmx_path: &str) -> Result<String, String> {
        let output = self.run(&["getGuestIPAddress", vmx_path, "-wait"])?;
        Ok(output.trim().to_string())
    }

    fn run(&self, args: &[&str]) -> Result<String, String> {
        let output = Command::new(&self.vmrun_path)
            .args(args)
            .output()
            .map_err(|e| format!("failed to run vmrun: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(format!(
                "vmrun {} failed: {}{}",
                args.first().unwrap_or(&""),
                stderr.trim(),
                if stderr.is_empty() {
                    stdout.trim().to_string()
                } else {
                    String::new()
                }
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

/// Find the vmrun binary, checking standard locations + WSL paths.
fn find_vmrun() -> Option<PathBuf> {
    // 1. Check PATH
    if let Ok(output) = Command::new("which").arg("vmrun").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return Some(PathBuf::from(path));
            }
        }
    }

    // 2. Standard locations
    let candidates = [
        // Linux (Workstation)
        "/usr/bin/vmrun",
        // macOS (Fusion)
        "/Applications/VMware Fusion.app/Contents/Library/vmrun",
        // WSL → Windows Workstation
        "/mnt/c/Program Files (x86)/VMware/VMware Workstation/vmrun.exe",
        "/mnt/c/Program Files/VMware/VMware Workstation/vmrun.exe",
    ];

    for path in &candidates {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_vmrun_returns_something_or_none() {
        // Just verify it doesn't panic
        let _ = find_vmrun();
    }

    #[test]
    fn test_default_client() {
        let client = VmrunClient::new();
        // Should have some path set (even if vmrun isn't installed)
        assert!(!client.vmrun_path.as_os_str().is_empty());
    }

    #[test]
    fn test_parse_list_output() {
        // Simulate vmrun list output
        let output =
            "Total running VMs: 2\n/home/user/VMs/ubuntu.vmx\n/home/user/VMs/windows.vmx\n";
        let mut vms = Vec::new();
        for line in output.lines().skip(1) {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                vms.push(trimmed.to_string());
            }
        }
        assert_eq!(vms.len(), 2);
        assert_eq!(vms[0], "/home/user/VMs/ubuntu.vmx");
        assert_eq!(vms[1], "/home/user/VMs/windows.vmx");
    }

    #[test]
    fn test_parse_snapshots_output() {
        let output = "Total snapshots: 2\npre-upgrade\nbaseline\n";
        let mut snaps = Vec::new();
        for line in output.lines().skip(1) {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                snaps.push(trimmed.to_string());
            }
        }
        assert_eq!(snaps, vec!["pre-upgrade", "baseline"]);
    }
}
