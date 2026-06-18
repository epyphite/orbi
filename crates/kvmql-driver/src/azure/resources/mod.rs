mod discover;
mod provisioners;
#[cfg(test)]
mod tests;

use std::process::Command;

use serde_json::Value;

use crate::provision::{param_str, param_str_or, ProvisionError, ProvisionResult};

/// A named collector function used during resource discovery.
pub(crate) type AzureDiscoverCollector = (
    &'static str,
    fn(&AzureResourceProvisioner) -> Result<Vec<Value>, ProvisionError>,
);

/// Azure resource provisioner that maps KVMQL resource types to `az` CLI commands.
///
/// Uses `std::process::Command` with individual arguments (never shell
/// interpolation).  The optional `subscription` and `resource_group` fields are
/// injected automatically where relevant.
#[derive(Debug, Clone)]
pub struct AzureResourceProvisioner {
    pub subscription: Option<String>,
    pub resource_group: Option<String>,
}

impl AzureResourceProvisioner {
    pub fn new(subscription: Option<&str>, resource_group: Option<&str>) -> Self {
        Self {
            subscription: subscription.map(|s| s.to_string()),
            resource_group: resource_group.map(|s| s.to_string()),
        }
    }

    // ── Public dispatch ──────────────────────────────────────────────

    /// Provision a managed resource.  Dispatches to the appropriate `az` command
    /// based on `resource_type`.
    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "postgres" => self.create_postgres(params),
            "redis" => self.create_redis(params),
            "aks" => self.create_aks(params),
            "storage_account" => self.create_storage_account(params),
            "vnet" => self.create_vnet(params),
            "nsg" => self.create_nsg(params),
            "container_registry" => self.create_container_registry(params),
            "dns_zone" => self.create_dns_zone(params),
            "container_app" => self.create_container_app(params),
            "container_job" => self.create_container_job(params),
            "load_balancer" => self.create_load_balancer(params),
            "subnet" => self.create_subnet(params),
            "nsg_rule" => self.create_nsg_rule(params),
            "vnet_peering" => self.create_vnet_peering(params),
            "pg_database" => self.create_pg_database(params),
            "dns_vnet_link" => self.create_dns_vnet_link(params),
            other => Err(format!("unsupported resource type: {other}").into()),
        }
    }

    /// Update a managed resource in-place.
    pub fn update(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "postgres" => {
                let mut args = vec!["postgres", "flexible-server", "update", "--name", id];
                if let Some(rg) = &self.resource_group {
                    args.extend(["--resource-group", rg]);
                }
                if let Some(v) = params.get("sku").and_then(|v| v.as_str()) {
                    args.extend(["--sku-name", v]);
                }
                let result = self.run_az(&args)?;
                Ok(ProvisionResult {
                    status: "updated".into(),
                    outputs: Some(result),
                })
            }
            other => Err(format!("update not yet implemented for resource type: {other}").into()),
        }
    }

    /// Delete a managed resource.
    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), ProvisionError> {
        let rg_args: Vec<String> = self
            .resource_group
            .iter()
            .map(|rg| format!("--resource-group={rg}"))
            .collect();

        let args: Vec<&str> = match resource_type {
            "postgres" => vec![
                "postgres",
                "flexible-server",
                "delete",
                "--name",
                id,
                "--yes",
            ],
            "redis" => vec!["redis", "delete", "--name", id, "--yes"],
            "aks" => vec!["aks", "delete", "--name", id, "--yes"],
            "storage_account" => vec!["storage", "account", "delete", "--name", id, "--yes"],
            "vnet" => vec!["network", "vnet", "delete", "--name", id, "--yes"],
            "nsg" => vec!["network", "nsg", "delete", "--name", id, "--yes"],
            "container_registry" => vec!["acr", "delete", "--name", id, "--yes"],
            "dns_zone" => vec!["network", "dns", "zone", "delete", "--name", id, "--yes"],
            "container_app" => vec!["containerapp", "delete", "--name", id, "--yes"],
            "container_job" => vec!["containerapp", "job", "delete", "--name", id, "--yes"],
            "load_balancer" => vec!["network", "lb", "delete", "--name", id],
            "subnet" | "nsg_rule" | "vnet_peering" => {
                return Err(format!(
                    "sub-resource type '{resource_type}' requires parent context; use delete_with_params()"
                ).into());
            }
            other => return Err(format!("unsupported resource type for delete: {other}").into()),
        };

        let mut full_args = args;
        for a in &rg_args {
            full_args.push(a);
        }
        self.run_az(&full_args)?;
        Ok(())
    }

    /// Delete a sub-resource that requires parent context (e.g. subnet needs vnet name).
    pub fn delete_with_params(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<(), ProvisionError> {
        match resource_type {
            "subnet" => self.delete_subnet(id, params),
            "nsg_rule" => self.delete_nsg_rule(id, params),
            "vnet_peering" => self.delete_vnet_peering(id, params),
            "pg_database" => self.delete_pg_database(id, params),
            "dns_vnet_link" => self.delete_dns_vnet_link(id, params),
            other => self.delete(other, id),
        }
    }

    // ── Day-2 operations ──────────────────────────────────────────────

    /// Trigger a backup for a managed resource.
    pub fn backup(
        &self,
        resource_type: &str,
        id: &str,
        _destination: Option<&str>,
        _tag: Option<&str>,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "postgres" => {
                // Azure PG Flexible Server uses automatic backups with PITR.
                // Manual backup is not directly available via CLI.
                // Return success indicating backup is automatic.
                Ok(ProvisionResult {
                    status: "backed_up".into(),
                    outputs: Some(serde_json::json!({
                        "note": "Azure PostgreSQL Flexible Server uses automatic backups with PITR. Use RESTORE with a point-in-time timestamp.",
                        "server": id,
                    })),
                })
            }
            _ => Err(format!("backup not supported for resource type: {resource_type}").into()),
        }
    }

    /// Restore a managed resource from a point-in-time or backup path.
    pub fn restore_resource(
        &self,
        resource_type: &str,
        id: &str,
        source: &str,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "postgres" => {
                let restored_name = format!("{id}-restored");
                let args = self.build_restore_postgres_args(id, source, &restored_name)?;
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                let result = self.run_az(&refs)?;
                Ok(ProvisionResult {
                    status: "restoring".into(),
                    outputs: Some(result),
                })
            }
            _ => Err(format!("restore not supported for resource type: {resource_type}").into()),
        }
    }

    /// Scale a managed resource (e.g. change node count, replica count).
    pub fn scale(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "aks" => {
                let args = self.build_scale_aks_args(id, params)?;
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                let result = self.run_az(&refs)?;
                Ok(ProvisionResult {
                    status: "scaled".into(),
                    outputs: Some(result),
                })
            }
            "container_app" => {
                let args = self.build_scale_container_app_args(id, params)?;
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                let result = self.run_az(&refs)?;
                Ok(ProvisionResult {
                    status: "scaled".into(),
                    outputs: Some(result),
                })
            }
            _ => Err(format!("scale not supported for resource type: {resource_type}").into()),
        }
    }

    /// Upgrade a managed resource (e.g. Kubernetes version).
    pub fn upgrade(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "aks" => {
                let args = self.build_upgrade_aks_args(id, params)?;
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                let result = self.run_az(&refs)?;
                Ok(ProvisionResult {
                    status: "upgrading".into(),
                    outputs: Some(result),
                })
            }
            _ => Err(format!("upgrade not supported for resource type: {resource_type}").into()),
        }
    }

    // ── Build args (for testing without execution) ───────────────────

    /// Build the `az` argument list that `create()` would use, WITHOUT executing.
    pub fn build_create_args(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let raw = match resource_type {
            "postgres" => self.build_postgres_args(params)?,
            "redis" => self.build_redis_args(params)?,
            "aks" => self.build_aks_args(params)?,
            "storage_account" => self.build_storage_account_args(params)?,
            "vnet" => self.build_vnet_args(params)?,
            "nsg" => self.build_nsg_args(params)?,
            "container_registry" => self.build_container_registry_args(params)?,
            "dns_zone" => self.build_dns_zone_args(params)?,
            "container_app" => self.build_container_app_args(params)?,
            "container_job" => self.build_container_job_args(params)?,
            "load_balancer" => self.build_load_balancer_args(params)?,
            "subnet" => self.build_subnet_args(params)?,
            "nsg_rule" => self.build_nsg_rule_args(params)?,
            "vnet_peering" => self.build_vnet_peering_args(params)?,
            "pg_database" => self.build_pg_database_args(params)?,
            "dns_vnet_link" => self.build_dns_vnet_link_args(params)?,
            other => return Err(format!("unsupported resource type: {other}").into()),
        };
        // Wrap through build_args to add --output json and --subscription
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `az` argument list that `delete()` would use, WITHOUT executing.
    pub fn build_delete_args(
        &self,
        resource_type: &str,
        id: &str,
    ) -> Result<Vec<String>, ProvisionError> {
        let rg_args: Vec<String> = self
            .resource_group
            .iter()
            .map(|rg| format!("--resource-group={rg}"))
            .collect();

        let base: Vec<&str> = match resource_type {
            "postgres" => vec![
                "postgres",
                "flexible-server",
                "delete",
                "--name",
                id,
                "--yes",
            ],
            "redis" => vec!["redis", "delete", "--name", id, "--yes"],
            "aks" => vec!["aks", "delete", "--name", id, "--yes"],
            "storage_account" => vec!["storage", "account", "delete", "--name", id, "--yes"],
            "vnet" => vec!["network", "vnet", "delete", "--name", id, "--yes"],
            "nsg" => vec!["network", "nsg", "delete", "--name", id, "--yes"],
            "container_registry" => vec!["acr", "delete", "--name", id, "--yes"],
            "dns_zone" => vec!["network", "dns", "zone", "delete", "--name", id, "--yes"],
            "container_app" => vec!["containerapp", "delete", "--name", id, "--yes"],
            "container_job" => vec!["containerapp", "job", "delete", "--name", id, "--yes"],
            "load_balancer" => vec!["network", "lb", "delete", "--name", id],
            "subnet" | "nsg_rule" | "vnet_peering" => {
                return Err(format!(
                    "sub-resource type '{resource_type}' requires parent context; use build_delete_args_with_params()"
                ).into());
            }
            other => return Err(format!("unsupported resource type for delete: {other}").into()),
        };

        let mut all: Vec<&str> = base;
        for a in &rg_args {
            all.push(a);
        }
        Ok(self.build_args(&all))
    }

    /// Build the `az` argument list for deleting sub-resources that require parent
    /// context, WITHOUT executing.
    pub fn build_delete_args_with_params(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let raw = match resource_type {
            "subnet" => self.build_subnet_delete_args(id, params)?,
            "nsg_rule" => self.build_nsg_rule_delete_args(id, params)?,
            "vnet_peering" => self.build_vnet_peering_delete_args(id, params)?,
            other => return self.build_delete_args(other, id),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    // ── Day-2 build-args (for testing without execution) ──────────────

    /// Build the argument list for a postgres PITR restore.
    fn build_restore_postgres_args(
        &self,
        source_server: &str,
        restore_time: &str,
        restored_name: &str,
    ) -> Result<Vec<String>, ProvisionError> {
        let mut args = vec![
            "postgres".into(),
            "flexible-server".into(),
            "restore".into(),
            "--name".into(),
            restored_name.to_string(),
            "--source-server".into(),
            source_server.to_string(),
            "--restore-time".into(),
            restore_time.to_string(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    /// Build the argument list for AKS node pool scale.
    fn build_scale_aks_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let node_count = param_str(params, "node_count")?;
        let nodepool = param_str_or(params, "nodepool", "nodepool1");
        let mut args = vec![
            "aks".into(),
            "nodepool".into(),
            "scale".into(),
            "--cluster-name".into(),
            id.to_string(),
            "--name".into(),
            nodepool,
            "--node-count".into(),
            node_count,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    /// Build the argument list for container app scale (replica count).
    fn build_scale_container_app_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let mut args = vec![
            "containerapp".into(),
            "update".into(),
            "--name".into(),
            id.to_string(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("min_replicas") {
            args.push("--min-replicas".into());
            args.push(json_val_to_string(v));
        }
        if let Some(v) = params.get("max_replicas") {
            args.push("--max-replicas".into());
            args.push(json_val_to_string(v));
        }
        Ok(args)
    }

    /// Build the argument list for AKS upgrade.
    fn build_upgrade_aks_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let version = param_str(params, "kubernetes_version")?;
        let mut args = vec![
            "aks".into(),
            "upgrade".into(),
            "--name".into(),
            id.to_string(),
            "--kubernetes-version".into(),
            version,
            "--yes".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    /// Build the `az` argument list that `restore_resource("postgres", ...)` would use,
    /// WITHOUT executing.
    pub fn build_restore_args(
        &self,
        resource_type: &str,
        id: &str,
        source: &str,
    ) -> Result<Vec<String>, ProvisionError> {
        match resource_type {
            "postgres" => {
                let restored_name = format!("{id}-restored");
                let raw = self.build_restore_postgres_args(id, source, &restored_name)?;
                Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
            }
            other => Err(format!("restore not supported for resource type: {other}").into()),
        }
    }

    /// Build the `az` argument list that `scale()` would use, WITHOUT executing.
    pub fn build_scale_args(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let raw = match resource_type {
            "aks" => self.build_scale_aks_args(id, params)?,
            "container_app" => self.build_scale_container_app_args(id, params)?,
            other => return Err(format!("scale not supported for resource type: {other}").into()),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `az` argument list that `upgrade()` would use, WITHOUT executing.
    pub fn build_upgrade_args(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let raw = match resource_type {
            "aks" => self.build_upgrade_aks_args(id, params)?,
            other => return Err(format!("upgrade not supported for resource type: {other}").into()),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    // ── Generic runner ───────────────────────────────────────────────

    /// Run an `az` command and return JSON output.
    fn run_az(&self, args: &[&str]) -> Result<Value, ProvisionError> {
        let mut cmd = Command::new("az");
        for arg in args {
            cmd.arg(arg);
        }
        cmd.arg("--output").arg("json");

        if let Some(ref sub) = self.subscription {
            cmd.arg("--subscription").arg(sub);
        }

        let output = cmd.output().map_err(|e| format!("failed to run az: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("az command failed: {stderr}").into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| ProvisionError::from(format!("failed to parse az JSON output: {e}")))
    }

    /// Build the argument list that `run_az` would use (for testing without
    /// execution).
    fn build_args(&self, args: &[&str]) -> Vec<String> {
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

    // ── Argument builders (testable without execution) ───────────────

    fn build_postgres_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "postgres".into(),
            "flexible-server".into(),
            "create".into(),
            "--name".into(),
            name,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("version").and_then(|v| v.as_str()) {
            args.push("--version".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("sku").and_then(|v| v.as_str()) {
            args.push("--sku-name".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("storage_gb") {
            let val = json_val_to_string(v);
            args.push("--storage-size".into());
            args.push(val);
        }
        if let Some(v) = params.get("backup_retention_days") {
            let val = json_val_to_string(v);
            args.push("--backup-retention".into());
            args.push(val);
        }
        args.push("--yes".into());
        Ok(args)
    }

    fn build_redis_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "redis".into(),
            "create".into(),
            "--name".into(),
            name,
            "--sku".into(),
            sku,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("capacity").and_then(|v| v.as_str()) {
            args.push("--vm-size".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("location").and_then(|v| v.as_str()) {
            args.push("--location".into());
            args.push(v.into());
        }
        Ok(args)
    }

    fn build_aks_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let node_count = param_str_or(params, "node_count", "3");
        let mut args = vec![
            "aks".into(),
            "create".into(),
            "--name".into(),
            name,
            "--node-count".into(),
            node_count,
            "--generate-ssh-keys".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("vm_size").and_then(|v| v.as_str()) {
            args.push("--node-vm-size".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("kubernetes_version").and_then(|v| v.as_str()) {
            args.push("--kubernetes-version".into());
            args.push(v.into());
        }
        Ok(args)
    }

    fn build_storage_account_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard_LRS");
        let mut args = vec![
            "storage".into(),
            "account".into(),
            "create".into(),
            "--name".into(),
            name,
            "--sku".into(),
            sku,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("kind").and_then(|v| v.as_str()) {
            args.push("--kind".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("access_tier").and_then(|v| v.as_str()) {
            args.push("--access-tier".into());
            args.push(v.into());
        }
        Ok(args)
    }

    fn build_vnet_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        // Accept either `address_space` (canonical) or `address_prefix` (subnet terminology)
        let addr = params
            .get("address_space")
            .and_then(|v| v.as_str())
            .or_else(|| params.get("address_prefix").and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .unwrap_or_else(|| "10.0.0.0/16".to_string());
        let mut args = vec![
            "network".into(),
            "vnet".into(),
            "create".into(),
            "--name".into(),
            name,
            "--address-prefix".into(),
            addr,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_nsg_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "network".into(),
            "nsg".into(),
            "create".into(),
            "--name".into(),
            name,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_container_registry_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "acr".into(),
            "create".into(),
            "--name".into(),
            name,
            "--sku".into(),
            sku,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if params.get("admin_enabled").and_then(|v| v.as_bool()) == Some(true) {
            args.push("--admin-enabled".into());
        }
        Ok(args)
    }

    fn build_dns_zone_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "network".into(),
            "dns".into(),
            "zone".into(),
            "create".into(),
            "--name".into(),
            name,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_container_app_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;
        let mut args = vec![
            "containerapp".into(),
            "create".into(),
            "--name".into(),
            name,
            "--image".into(),
            image,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("cpu").and_then(|v| v.as_str()) {
            args.push("--cpu".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("memory").and_then(|v| v.as_str()) {
            args.push("--memory".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("min_replicas") {
            let val = json_val_to_string(v);
            args.push("--min-replicas".into());
            args.push(val);
        }
        if let Some(v) = params.get("max_replicas") {
            let val = json_val_to_string(v);
            args.push("--max-replicas".into());
            args.push(val);
        }
        Ok(args)
    }

    fn build_container_job_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;
        let trigger = param_str_or(params, "trigger_type", "Manual");
        let mut args = vec![
            "containerapp".into(),
            "job".into(),
            "create".into(),
            "--name".into(),
            name,
            "--image".into(),
            image,
            "--trigger-type".into(),
            trigger,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("cpu").and_then(|v| v.as_str()) {
            args.push("--cpu".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("memory").and_then(|v| v.as_str()) {
            args.push("--memory".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("cron_expression").and_then(|v| v.as_str()) {
            args.push("--cron-expression".into());
            args.push(v.into());
        }
        Ok(args)
    }

    fn build_load_balancer_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "network".into(),
            "lb".into(),
            "create".into(),
            "--name".into(),
            name,
            "--sku".into(),
            sku,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_subnet_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let vnet = param_str(params, "vnet")?;
        let prefix = param_str(params, "address_prefix")?;
        let mut args = vec![
            "network".into(),
            "vnet".into(),
            "subnet".into(),
            "create".into(),
            "--name".into(),
            name,
            "--vnet-name".into(),
            vnet,
            "--address-prefixes".into(),
            prefix,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(d) = params.get("delegation").and_then(|v| v.as_str()) {
            args.push("--delegations".into());
            args.push(d.into());
        }
        if let Some(nsg) = params.get("nsg").and_then(|v| v.as_str()) {
            args.push("--network-security-group".into());
            args.push(nsg.into());
        }
        Ok(args)
    }

    fn build_subnet_delete_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let vnet = param_str(params, "vnet")?;
        let mut args: Vec<String> = vec![
            "network".into(),
            "vnet".into(),
            "subnet".into(),
            "delete".into(),
            "--name".into(),
            id.into(),
            "--vnet-name".into(),
            vnet,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        args.push("--yes".into());
        Ok(args)
    }

    fn build_nsg_rule_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let nsg = param_str(params, "nsg")?;
        let priority = param_str(params, "priority")?;
        let direction = param_str_or(params, "direction", "Inbound");
        let access = param_str_or(params, "access", "Allow");
        let protocol = param_str_or(params, "protocol", "Tcp");
        let mut args = vec![
            "network".into(),
            "nsg".into(),
            "rule".into(),
            "create".into(),
            "--name".into(),
            name,
            "--nsg-name".into(),
            nsg,
            "--priority".into(),
            priority,
            "--direction".into(),
            direction,
            "--access".into(),
            access,
            "--protocol".into(),
            protocol,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("source_address").and_then(|v| v.as_str()) {
            args.push("--source-address-prefixes".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("destination_port").and_then(|v| v.as_str()) {
            args.push("--destination-port-ranges".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("source_port").and_then(|v| v.as_str()) {
            args.push("--source-port-ranges".into());
            args.push(v.into());
        } else {
            args.push("--source-port-ranges".into());
            args.push("*".into());
        }
        if let Some(v) = params.get("destination_address").and_then(|v| v.as_str()) {
            args.push("--destination-address-prefixes".into());
            args.push(v.into());
        } else {
            args.push("--destination-address-prefixes".into());
            args.push("*".into());
        }
        Ok(args)
    }

    fn build_nsg_rule_delete_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let nsg = param_str(params, "nsg")?;
        let mut args: Vec<String> = vec![
            "network".into(),
            "nsg".into(),
            "rule".into(),
            "delete".into(),
            "--name".into(),
            id.into(),
            "--nsg-name".into(),
            nsg,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_vnet_peering_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let vnet = param_str(params, "vnet")?;
        let remote_vnet = param_str(params, "remote_vnet")?;
        let mut args = vec![
            "network".into(),
            "vnet".into(),
            "peering".into(),
            "create".into(),
            "--name".into(),
            name,
            "--vnet-name".into(),
            vnet,
            "--remote-vnet".into(),
            remote_vnet,
            "--allow-vnet-access".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if params
            .get("allow_forwarded_traffic")
            .and_then(|v| v.as_bool())
            == Some(true)
        {
            args.push("--allow-forwarded-traffic".into());
        }
        if params
            .get("allow_gateway_transit")
            .and_then(|v| v.as_bool())
            == Some(true)
        {
            args.push("--allow-gateway-transit".into());
        }
        Ok(args)
    }

    fn build_vnet_peering_delete_args(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let vnet = param_str(params, "vnet")?;
        let mut args: Vec<String> = vec![
            "network".into(),
            "vnet".into(),
            "peering".into(),
            "delete".into(),
            "--name".into(),
            id.into(),
            "--vnet-name".into(),
            vnet,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_pg_database_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let server = param_str(params, "server")?;
        let mut args = vec![
            "postgres".into(),
            "flexible-server".into(),
            "db".into(),
            "create".into(),
            "--database-name".into(),
            name,
            "--server-name".into(),
            server,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if let Some(v) = params.get("charset").and_then(|v| v.as_str()) {
            args.push("--charset".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("collation").and_then(|v| v.as_str()) {
            args.push("--collation".into());
            args.push(v.into());
        }
        Ok(args)
    }

    fn build_dns_vnet_link_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let zone = param_str(params, "zone_name")?;
        let vnet = param_str(params, "vnet")?;
        let mut args = vec![
            "network".into(),
            "private-dns".into(),
            "link".into(),
            "vnet".into(),
            "create".into(),
            "--name".into(),
            name,
            "--zone-name".into(),
            zone,
            "--virtual-network".into(),
            vnet,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if params.get("registration_enabled").and_then(|v| v.as_bool()) == Some(true) {
            args.push("--registration-enabled".into());
            args.push("true".into());
        }
        Ok(args)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a string field from a JSON object, returning `""` when missing.
fn str_field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(|s| s.as_str()).unwrap_or("")
}

/// Convert a JSON value to a string suitable for command-line arguments.
fn json_val_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}
