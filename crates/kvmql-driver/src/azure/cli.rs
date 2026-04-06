use std::process::Command;

/// Low-level wrapper around the `az` CLI.
///
/// All methods shell out to the `az` command using `std::process::Command` with
/// individual arguments (never shell interpolation). The optional `subscription`
/// and `resource_group` fields are injected as `--subscription` / `--resource-group`
/// flags when set.
#[derive(Debug, Clone)]
pub struct AzureCli {
    pub subscription: Option<String>,
    pub resource_group: Option<String>,
}

impl AzureCli {
    pub fn new() -> Self {
        Self {
            subscription: None,
            resource_group: None,
        }
    }

    pub fn with_subscription(mut self, sub: &str) -> Self {
        self.subscription = Some(sub.to_string());
        self
    }

    pub fn with_resource_group(mut self, rg: &str) -> Self {
        self.resource_group = Some(rg.to_string());
        self
    }

    // ── Availability check ────────────────────────────────────────

    /// Check that the `az` CLI is installed and callable.
    pub fn check_available(&self) -> Result<(), String> {
        let output = Command::new("az")
            .arg("version")
            .arg("--output")
            .arg("json")
            .output()
            .map_err(|e| format!("failed to run az CLI: {e}"))?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("az version failed: {stderr}"))
        }
    }

    // ── Generic runner ────────────────────────────────────────────

    /// Run an arbitrary `az` sub-command with `--output json` and parse the result.
    pub fn run(&self, args: &[&str]) -> Result<serde_json::Value, String> {
        let mut cmd = Command::new("az");
        for arg in args {
            cmd.arg(arg);
        }
        cmd.arg("--output").arg("json");

        if let Some(ref sub) = self.subscription {
            cmd.arg("--subscription").arg(sub);
        }

        let output = cmd
            .output()
            .map_err(|e| format!("failed to run az: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("az command failed: {stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| format!("failed to parse az JSON output: {e}"))
    }

    /// Build the argument list that `run` would use (for testing without execution).
    pub fn build_args(&self, args: &[&str]) -> Vec<String> {
        let mut result: Vec<String> = vec!["az".to_string()];
        for arg in args {
            result.push(arg.to_string());
        }
        result.push("--output".to_string());
        result.push("json".to_string());
        if let Some(ref sub) = self.subscription {
            result.push("--subscription".to_string());
            result.push(sub.clone());
        }
        result
    }

    // ── VM operations ─────────────────────────────────────────────

    /// Create a VM: `az vm create --name <name> --image <image> --size <size> --resource-group <rg> [extra_args]`
    pub fn vm_create(
        &self,
        name: &str,
        image: &str,
        size: &str,
        rg: &str,
        extra_args: &[&str],
    ) -> Result<serde_json::Value, String> {
        let mut args = vec![
            "vm", "create",
            "--name", name,
            "--image", image,
            "--size", size,
            "--resource-group", rg,
        ];
        args.extend_from_slice(extra_args);
        self.run(&args)
    }

    /// Delete a VM: `az vm delete --name <name> --resource-group <rg> --yes [--force-deletion yes]`
    pub fn vm_delete(&self, name: &str, rg: &str, force: bool) -> Result<(), String> {
        let mut args = vec![
            "vm", "delete",
            "--name", name,
            "--resource-group", rg,
            "--yes",
        ];
        if force {
            args.push("--force-deletion");
            args.push("yes");
        }
        self.run(&args)?;
        Ok(())
    }

    /// List VMs: `az vm list --resource-group <rg>`
    pub fn vm_list(&self, rg: &str) -> Result<Vec<serde_json::Value>, String> {
        let args = vec![
            "vm", "list",
            "--resource-group", rg,
            "--show-details",
        ];
        let val = self.run(&args)?;
        match val {
            serde_json::Value::Array(arr) => Ok(arr),
            _ => Err("expected JSON array from az vm list".into()),
        }
    }

    /// Show a single VM: `az vm show --name <name> --resource-group <rg> --show-details`
    pub fn vm_show(&self, name: &str, rg: &str) -> Result<serde_json::Value, String> {
        self.run(&[
            "vm", "show",
            "--name", name,
            "--resource-group", rg,
            "--show-details",
        ])
    }

    /// Start a VM: `az vm start --name <name> --resource-group <rg>`
    pub fn vm_start(&self, name: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "vm", "start",
            "--name", name,
            "--resource-group", rg,
        ])?;
        Ok(())
    }

    /// Stop (power off) a VM: `az vm stop --name <name> --resource-group <rg>`
    pub fn vm_stop(&self, name: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "vm", "stop",
            "--name", name,
            "--resource-group", rg,
        ])?;
        Ok(())
    }

    /// Deallocate a VM: `az vm deallocate --name <name> --resource-group <rg>`
    pub fn vm_deallocate(&self, name: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "vm", "deallocate",
            "--name", name,
            "--resource-group", rg,
        ])?;
        Ok(())
    }

    // ── Disk operations ───────────────────────────────────────────

    /// Create a managed disk: `az disk create --name <name> --size-gb <size> --resource-group <rg>`
    pub fn disk_create(
        &self,
        name: &str,
        size_gb: i64,
        rg: &str,
    ) -> Result<serde_json::Value, String> {
        let size_str = size_gb.to_string();
        self.run(&[
            "disk", "create",
            "--name", name,
            "--size-gb", &size_str,
            "--resource-group", rg,
        ])
    }

    /// Delete a managed disk: `az disk delete --name <name> --resource-group <rg> --yes`
    pub fn disk_delete(&self, name: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "disk", "delete",
            "--name", name,
            "--resource-group", rg,
            "--yes",
        ])?;
        Ok(())
    }

    /// List managed disks: `az disk list --resource-group <rg>`
    pub fn disk_list(&self, rg: &str) -> Result<Vec<serde_json::Value>, String> {
        let val = self.run(&[
            "disk", "list",
            "--resource-group", rg,
        ])?;
        match val {
            serde_json::Value::Array(arr) => Ok(arr),
            _ => Err("expected JSON array from az disk list".into()),
        }
    }

    // ── VM disk attach/detach ─────────────────────────────────────

    /// Attach a disk to a VM: `az vm disk attach --vm-name <vm> --name <disk> --resource-group <rg>`
    pub fn vm_disk_attach(&self, vm: &str, disk: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "vm", "disk", "attach",
            "--vm-name", vm,
            "--name", disk,
            "--resource-group", rg,
        ])?;
        Ok(())
    }

    /// Detach a disk from a VM: `az vm disk detach --vm-name <vm> --name <disk> --resource-group <rg>`
    pub fn vm_disk_detach(&self, vm: &str, disk: &str, rg: &str) -> Result<(), String> {
        self.run(&[
            "vm", "disk", "detach",
            "--vm-name", vm,
            "--name", disk,
            "--resource-group", rg,
        ])?;
        Ok(())
    }

    // ── Snapshot operations ───────────────────────────────────────

    /// Create a snapshot: `az snapshot create --name <name> --source <source_disk> --resource-group <rg>`
    pub fn snapshot_create(
        &self,
        name: &str,
        source_disk: &str,
        rg: &str,
    ) -> Result<serde_json::Value, String> {
        self.run(&[
            "snapshot", "create",
            "--name", name,
            "--source", source_disk,
            "--resource-group", rg,
        ])
    }

    // ── Account ───────────────────────────────────────────────────

    /// Show current account: `az account show`
    pub fn account_show(&self) -> Result<serde_json::Value, String> {
        self.run(&["account", "show"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_construction_vm_list() {
        let cli = AzureCli::new()
            .with_subscription("sub-123")
            .with_resource_group("my-rg");

        let args = cli.build_args(&[
            "vm", "list",
            "--resource-group", "my-rg",
            "--show-details",
        ]);

        assert_eq!(args[0], "az");
        assert_eq!(args[1], "vm");
        assert_eq!(args[2], "list");
        assert_eq!(args[3], "--resource-group");
        assert_eq!(args[4], "my-rg");
        assert_eq!(args[5], "--show-details");
        assert_eq!(args[6], "--output");
        assert_eq!(args[7], "json");
        assert_eq!(args[8], "--subscription");
        assert_eq!(args[9], "sub-123");
    }

    #[test]
    fn test_command_construction_vm_create() {
        let cli = AzureCli::new()
            .with_subscription("sub-456");

        let args = cli.build_args(&[
            "vm", "create",
            "--name", "test-vm",
            "--image", "UbuntuLTS",
            "--size", "Standard_B2s",
            "--resource-group", "test-rg",
        ]);

        assert_eq!(args[0], "az");
        assert_eq!(args[1], "vm");
        assert_eq!(args[2], "create");
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"test-vm".to_string()));
        assert!(args.contains(&"--image".to_string()));
        assert!(args.contains(&"UbuntuLTS".to_string()));
        assert!(args.contains(&"--size".to_string()));
        assert!(args.contains(&"Standard_B2s".to_string()));
        assert!(args.contains(&"--output".to_string()));
        assert!(args.contains(&"json".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-456".to_string()));
    }

    #[test]
    fn test_command_construction_no_subscription() {
        let cli = AzureCli::new();
        let args = cli.build_args(&["account", "show"]);

        assert_eq!(args, vec!["az", "account", "show", "--output", "json"]);
        // Should NOT contain --subscription
        assert!(!args.contains(&"--subscription".to_string()));
    }

    #[test]
    fn test_command_construction_vm_delete_with_force() {
        let cli = AzureCli::new()
            .with_subscription("sub-789");

        let args = cli.build_args(&[
            "vm", "delete",
            "--name", "doomed-vm",
            "--resource-group", "rg",
            "--yes",
            "--force-deletion", "yes",
        ]);

        assert!(args.contains(&"--yes".to_string()));
        assert!(args.contains(&"--force-deletion".to_string()));
        assert!(args.contains(&"doomed-vm".to_string()));
    }

    #[test]
    fn test_command_construction_disk_create() {
        let cli = AzureCli::new()
            .with_subscription("sub-disk");

        let args = cli.build_args(&[
            "disk", "create",
            "--name", "my-disk",
            "--size-gb", "100",
            "--resource-group", "rg",
        ]);

        assert!(args.contains(&"disk".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--size-gb".to_string()));
        assert!(args.contains(&"100".to_string()));
    }

    #[test]
    fn test_command_construction_snapshot() {
        let cli = AzureCli::new();
        let args = cli.build_args(&[
            "snapshot", "create",
            "--name", "snap-1",
            "--source", "os-disk-1",
            "--resource-group", "rg",
        ]);

        assert!(args.contains(&"snapshot".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--source".to_string()));
        assert!(args.contains(&"os-disk-1".to_string()));
    }
}
