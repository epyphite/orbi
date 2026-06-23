use std::process::Command;

use serde_json::Value;

/// Client for VMware vSphere via the `govc` CLI.
#[derive(Debug, Clone)]
pub struct GovcClient {
    datacenter: Option<String>,
}

impl Default for GovcClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GovcClient {
    pub fn new() -> Self {
        Self { datacenter: None }
    }

    pub fn with_datacenter(datacenter: &str) -> Self {
        Self {
            datacenter: Some(datacenter.to_string()),
        }
    }

    pub fn check_available(&self) -> Result<(), String> {
        let output = Command::new("govc")
            .arg("about")
            .arg("-json")
            .output()
            .map_err(|e| format!("govc not found: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "govc about failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(())
    }

    /// Create a VM with the given parameters.
    pub fn vm_create(
        &self,
        name: &str,
        cpus: i32,
        memory_mb: i32,
        disk_gb: Option<i32>,
        network: Option<&str>,
        guest_os: Option<&str>,
    ) -> Result<Value, String> {
        let mut args = vec![
            "vm.create".to_string(),
            "-json".to_string(),
            format!("-c={cpus}"),
            format!("-m={memory_mb}"),
            format!("-g={}", guest_os.unwrap_or("ubuntu64Guest")),
            "-on=false".to_string(),
        ];

        if let Some(gb) = disk_gb {
            args.push(format!("-disk={}GB", gb));
        }
        if let Some(net) = network {
            args.push(format!("-net={net}"));
        }
        args.push(name.to_string());

        self.run_json(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>())
    }

    /// Clone a VM from a template.
    pub fn vm_clone(&self, template: &str, name: &str, power_on: bool) -> Result<Value, String> {
        let on = if power_on { "-on=true" } else { "-on=false" };
        self.run_json(&["vm.clone", "-json", "-vm", template, on, name])
    }

    /// Destroy a VM.
    pub fn vm_destroy(&self, name: &str) -> Result<(), String> {
        self.run(&["vm.destroy", name])?;
        Ok(())
    }

    /// Power on/off/suspend a VM.
    pub fn vm_power(&self, name: &str, action: &str) -> Result<(), String> {
        let flag = match action {
            "on" => "-on",
            "off" => "-off",
            "suspend" => "-suspend",
            "reset" => "-reset",
            _ => return Err(format!("unknown power action: {action}")),
        };
        self.run(&["vm.power", flag, name])?;
        Ok(())
    }

    /// Get VM info as JSON.
    pub fn vm_info(&self, name: &str) -> Result<Value, String> {
        self.run_json(&["vm.info", "-json", name])
    }

    /// Change VM configuration (CPU, memory).
    pub fn vm_change(
        &self,
        name: &str,
        cpus: Option<i32>,
        memory_mb: Option<i32>,
    ) -> Result<(), String> {
        let mut args = vec!["vm.change".to_string(), format!("-vm={name}")];
        if let Some(c) = cpus {
            args.push(format!("-c={c}"));
        }
        if let Some(m) = memory_mb {
            args.push(format!("-m={m}"));
        }
        self.run(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
        Ok(())
    }

    /// Create a snapshot.
    pub fn snapshot_create(&self, vm: &str, name: &str) -> Result<(), String> {
        self.run(&["snapshot.create", format!("-vm={vm}").as_str(), name])?;
        Ok(())
    }

    /// Revert to a snapshot.
    pub fn snapshot_revert(&self, vm: &str, name: &str) -> Result<(), String> {
        self.run(&["snapshot.revert", format!("-vm={vm}").as_str(), name])?;
        Ok(())
    }

    /// Remove a snapshot.
    pub fn snapshot_remove(&self, vm: &str, name: &str) -> Result<(), String> {
        self.run(&["snapshot.remove", format!("-vm={vm}").as_str(), name])?;
        Ok(())
    }

    /// Find all objects of a given type (m=VM, n=network, s=datastore).
    pub fn find(&self, obj_type: &str) -> Result<Vec<String>, String> {
        let output = self.run(&["find", "/", "-type", obj_type])?;
        Ok(output
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect())
    }

    /// Find objects as JSON.
    pub fn find_json(&self, obj_type: &str) -> Result<Value, String> {
        self.run_json(&["find", "/", "-type", obj_type, "-json"])
    }

    fn run(&self, args: &[&str]) -> Result<String, String> {
        let mut cmd = Command::new("govc");
        for arg in args {
            cmd.arg(arg);
        }
        if let Some(ref dc) = self.datacenter {
            cmd.arg(format!("-dc={dc}"));
        }

        let output = cmd
            .output()
            .map_err(|e| format!("failed to run govc: {e}"))?;

        if !output.status.success() {
            return Err(format!(
                "govc {} failed: {}",
                args.first().unwrap_or(&""),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn run_json(&self, args: &[&str]) -> Result<Value, String> {
        let output = self.run(args)?;
        serde_json::from_str(&output).map_err(|e| format!("failed to parse govc JSON output: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client() {
        let client = GovcClient::new();
        assert!(client.datacenter.is_none());
    }

    #[test]
    fn test_with_datacenter() {
        let client = GovcClient::with_datacenter("DC-Kenya");
        assert_eq!(client.datacenter.as_deref(), Some("DC-Kenya"));
    }
}
