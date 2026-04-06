use std::process::Command;

use serde_json::Value;

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

/// Result of a provisioning operation.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (connection strings, FQDNs, etc.).
    pub outputs: Option<Value>,
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
    pub fn create(&self, resource_type: &str, params: &Value) -> Result<ProvisionResult, String> {
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
            other => Err(format!("unsupported resource type: {other}")),
        }
    }

    /// Update a managed resource in-place.
    pub fn update(&self, resource_type: &str, id: &str, params: &Value) -> Result<ProvisionResult, String> {
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
                Ok(ProvisionResult { status: "updated".into(), outputs: Some(result) })
            }
            other => Err(format!("update not yet implemented for resource type: {other}")),
        }
    }

    /// Delete a managed resource.
    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), String> {
        let rg_args: Vec<String> = self
            .resource_group
            .iter()
            .map(|rg| format!("--resource-group={rg}"))
            .collect();

        let args: Vec<&str> = match resource_type {
            "postgres" => vec!["postgres", "flexible-server", "delete", "--name", id, "--yes"],
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
                ));
            }
            other => return Err(format!("unsupported resource type for delete: {other}")),
        };

        let mut full_args = args;
        for a in &rg_args {
            full_args.push(a);
        }
        self.run_az(&full_args)?;
        Ok(())
    }

    /// Delete a sub-resource that requires parent context (e.g. subnet needs vnet name).
    pub fn delete_with_params(&self, resource_type: &str, id: &str, params: &Value) -> Result<(), String> {
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
    ) -> Result<ProvisionResult, String> {
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
            _ => Err(format!("backup not supported for resource type: {resource_type}")),
        }
    }

    /// Restore a managed resource from a point-in-time or backup path.
    pub fn restore_resource(
        &self,
        resource_type: &str,
        id: &str,
        source: &str,
    ) -> Result<ProvisionResult, String> {
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
            _ => Err(format!("restore not supported for resource type: {resource_type}")),
        }
    }

    /// Scale a managed resource (e.g. change node count, replica count).
    pub fn scale(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
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
            _ => Err(format!("scale not supported for resource type: {resource_type}")),
        }
    }

    /// Upgrade a managed resource (e.g. Kubernetes version).
    pub fn upgrade(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
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
            _ => Err(format!("upgrade not supported for resource type: {resource_type}")),
        }
    }

    // ── Build args (for testing without execution) ───────────────────

    /// Build the `az` argument list that `create()` would use, WITHOUT executing.
    pub fn build_create_args(&self, resource_type: &str, params: &Value) -> Result<Vec<String>, String> {
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
            other => return Err(format!("unsupported resource type: {other}")),
        };
        // Wrap through build_args to add --output json and --subscription
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `az` argument list that `delete()` would use, WITHOUT executing.
    pub fn build_delete_args(&self, resource_type: &str, id: &str) -> Result<Vec<String>, String> {
        let rg_args: Vec<String> = self
            .resource_group
            .iter()
            .map(|rg| format!("--resource-group={rg}"))
            .collect();

        let base: Vec<&str> = match resource_type {
            "postgres" => vec!["postgres", "flexible-server", "delete", "--name", id, "--yes"],
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
                ));
            }
            other => return Err(format!("unsupported resource type for delete: {other}")),
        };

        let mut all: Vec<&str> = base;
        for a in &rg_args {
            all.push(a);
        }
        Ok(self.build_args(&all))
    }

    /// Build the `az` argument list for deleting sub-resources that require parent
    /// context, WITHOUT executing.
    pub fn build_delete_args_with_params(&self, resource_type: &str, id: &str, params: &Value) -> Result<Vec<String>, String> {
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
    ) -> Result<Vec<String>, String> {
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
    fn build_scale_aks_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
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
    fn build_scale_container_app_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
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
    fn build_upgrade_aks_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
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
    ) -> Result<Vec<String>, String> {
        match resource_type {
            "postgres" => {
                let restored_name = format!("{id}-restored");
                let raw = self.build_restore_postgres_args(id, source, &restored_name)?;
                Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
            }
            other => Err(format!("restore not supported for resource type: {other}")),
        }
    }

    /// Build the `az` argument list that `scale()` would use, WITHOUT executing.
    pub fn build_scale_args(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, String> {
        let raw = match resource_type {
            "aks" => self.build_scale_aks_args(id, params)?,
            "container_app" => self.build_scale_container_app_args(id, params)?,
            other => return Err(format!("scale not supported for resource type: {other}")),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `az` argument list that `upgrade()` would use, WITHOUT executing.
    pub fn build_upgrade_args(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<Vec<String>, String> {
        let raw = match resource_type {
            "aks" => self.build_upgrade_aks_args(id, params)?,
            other => return Err(format!("upgrade not supported for resource type: {other}")),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    // ── Generic runner ───────────────────────────────────────────────

    /// Run an `az` command and return JSON output.
    fn run_az(&self, args: &[&str]) -> Result<Value, String> {
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
            return Ok(Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| format!("failed to parse az JSON output: {e}"))
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

    // ── Per-resource create implementations ──────────────────────────

    fn create_postgres(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_postgres_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "fqdn": result.get("fullyQualifiedDomainName"),
            "host": result.get("fullyQualifiedDomainName"),
            "state": result.get("state"),
            "version": result.get("version"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_redis(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_redis_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "host": result.get("hostName"),
            "port": result.get("port"),
            "ssl_port": result.get("sslPort"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_aks(&self, params: &Value) -> Result<ProvisionResult, String> {
        let node_count = param_str_or(params, "node_count", "3");
        let args = self.build_aks_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "fqdn": result.get("fqdn"),
            "kubernetes_version": result.get("kubernetesVersion"),
            "node_count": node_count,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_storage_account(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_storage_account_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "primary_endpoints": result.get("primaryEndpoints"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_vnet(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_vnet_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    fn create_nsg(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_nsg_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    fn create_container_registry(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_container_registry_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "login_server": result.get("loginServer"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_dns_zone(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_dns_zone_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "name_servers": result.get("nameServers"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_container_app(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_container_app_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        let outputs = serde_json::json!({
            "fqdn": result.get("properties")
                .and_then(|p| p.get("configuration"))
                .and_then(|c| c.get("ingress"))
                .and_then(|i| i.get("fqdn")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_container_job(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_container_job_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    fn create_load_balancer(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_load_balancer_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    // ── Argument builders (testable without execution) ───────────────

    fn build_postgres_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "postgres".into(), "flexible-server".into(), "create".into(),
            "--name".into(), name,
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

    fn build_redis_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "redis".into(), "create".into(),
            "--name".into(), name,
            "--sku".into(), sku,
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

    fn build_aks_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let node_count = param_str_or(params, "node_count", "3");
        let mut args = vec![
            "aks".into(), "create".into(),
            "--name".into(), name,
            "--node-count".into(), node_count,
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

    fn build_storage_account_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard_LRS");
        let mut args = vec![
            "storage".into(), "account".into(), "create".into(),
            "--name".into(), name,
            "--sku".into(), sku,
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

    fn build_vnet_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        // Accept either `address_space` (canonical) or `address_prefix` (subnet terminology)
        let addr = params
            .get("address_space")
            .and_then(|v| v.as_str())
            .or_else(|| params.get("address_prefix").and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .unwrap_or_else(|| "10.0.0.0/16".to_string());
        let mut args = vec![
            "network".into(), "vnet".into(), "create".into(),
            "--name".into(), name,
            "--address-prefix".into(), addr,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_nsg_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "network".into(), "nsg".into(), "create".into(),
            "--name".into(), name,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_container_registry_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "acr".into(), "create".into(),
            "--name".into(), name,
            "--sku".into(), sku,
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

    fn build_dns_zone_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let mut args = vec![
            "network".into(), "dns".into(), "zone".into(), "create".into(),
            "--name".into(), name,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    fn build_container_app_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;
        let mut args = vec![
            "containerapp".into(), "create".into(),
            "--name".into(), name,
            "--image".into(), image,
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

    fn build_container_job_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;
        let trigger = param_str_or(params, "trigger_type", "Manual");
        let mut args = vec![
            "containerapp".into(), "job".into(), "create".into(),
            "--name".into(), name,
            "--image".into(), image,
            "--trigger-type".into(), trigger,
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

    fn build_load_balancer_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let sku = param_str_or(params, "sku", "Standard");
        let mut args = vec![
            "network".into(), "lb".into(), "create".into(),
            "--name".into(), name,
            "--sku".into(), sku,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    // ── Subnet (sub-resource of VNet) ──────────────────────────────────

    fn create_subnet(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_subnet_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult { status: "created".into(), outputs: Some(result) })
    }

    fn delete_subnet(&self, id: &str, params: &Value) -> Result<(), String> {
        let args = self.build_subnet_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    fn build_subnet_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let vnet = param_str(params, "vnet")?;
        let prefix = param_str(params, "address_prefix")?;
        let mut args = vec![
            "network".into(), "vnet".into(), "subnet".into(), "create".into(),
            "--name".into(), name,
            "--vnet-name".into(), vnet,
            "--address-prefixes".into(), prefix,
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

    fn build_subnet_delete_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
        let vnet = param_str(params, "vnet")?;
        let mut args: Vec<String> = vec![
            "network".into(), "vnet".into(), "subnet".into(), "delete".into(),
            "--name".into(), id.into(),
            "--vnet-name".into(), vnet,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        args.push("--yes".into());
        Ok(args)
    }

    // ── NSG Rule (sub-resource of NSG) ─────────────────────────────────

    fn create_nsg_rule(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_nsg_rule_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult { status: "created".into(), outputs: Some(result) })
    }

    fn delete_nsg_rule(&self, id: &str, params: &Value) -> Result<(), String> {
        let args = self.build_nsg_rule_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    fn build_nsg_rule_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let nsg = param_str(params, "nsg")?;
        let priority = param_str(params, "priority")?;
        let direction = param_str_or(params, "direction", "Inbound");
        let access = param_str_or(params, "access", "Allow");
        let protocol = param_str_or(params, "protocol", "Tcp");
        let mut args = vec![
            "network".into(), "nsg".into(), "rule".into(), "create".into(),
            "--name".into(), name,
            "--nsg-name".into(), nsg,
            "--priority".into(), priority,
            "--direction".into(), direction,
            "--access".into(), access,
            "--protocol".into(), protocol,
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

    fn build_nsg_rule_delete_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
        let nsg = param_str(params, "nsg")?;
        let mut args: Vec<String> = vec![
            "network".into(), "nsg".into(), "rule".into(), "delete".into(),
            "--name".into(), id.into(),
            "--nsg-name".into(), nsg,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    // ── VNet Peering ───────────────────────────────────────────────────

    fn create_vnet_peering(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_vnet_peering_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult { status: "created".into(), outputs: Some(result) })
    }

    fn delete_vnet_peering(&self, id: &str, params: &Value) -> Result<(), String> {
        let args = self.build_vnet_peering_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    fn build_vnet_peering_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let vnet = param_str(params, "vnet")?;
        let remote_vnet = param_str(params, "remote_vnet")?;
        let mut args = vec![
            "network".into(), "vnet".into(), "peering".into(), "create".into(),
            "--name".into(), name,
            "--vnet-name".into(), vnet,
            "--remote-vnet".into(), remote_vnet,
            "--allow-vnet-access".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        if params.get("allow_forwarded_traffic").and_then(|v| v.as_bool()) == Some(true) {
            args.push("--allow-forwarded-traffic".into());
        }
        if params.get("allow_gateway_transit").and_then(|v| v.as_bool()) == Some(true) {
            args.push("--allow-gateway-transit".into());
        }
        Ok(args)
    }

    fn build_vnet_peering_delete_args(&self, id: &str, params: &Value) -> Result<Vec<String>, String> {
        let vnet = param_str(params, "vnet")?;
        let mut args: Vec<String> = vec![
            "network".into(), "vnet".into(), "peering".into(), "delete".into(),
            "--name".into(), id.into(),
            "--vnet-name".into(), vnet,
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        Ok(args)
    }

    // ── PostgreSQL Database ─────────────────────────────────────────

    fn create_pg_database(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_pg_database_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult { status: "created".into(), outputs: Some(result) })
    }

    fn delete_pg_database(&self, id: &str, params: &Value) -> Result<(), String> {
        let server = param_str(params, "server")?;
        let mut args: Vec<String> = vec![
            "postgres".into(), "flexible-server".into(), "db".into(), "delete".into(),
            "--database-name".into(), id.into(),
            "--server-name".into(), server,
            "--yes".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    fn build_pg_database_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let server = param_str(params, "server")?;
        let mut args = vec![
            "postgres".into(), "flexible-server".into(), "db".into(), "create".into(),
            "--database-name".into(), name,
            "--server-name".into(), server,
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

    // ── Private DNS VNet Link ───────────────────────────────────────

    fn create_dns_vnet_link(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_dns_vnet_link_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult { status: "created".into(), outputs: Some(result) })
    }

    fn delete_dns_vnet_link(&self, id: &str, params: &Value) -> Result<(), String> {
        let zone = param_str(params, "zone_name")?;
        let mut args: Vec<String> = vec![
            "network".into(), "private-dns".into(), "link".into(), "vnet".into(), "delete".into(),
            "--name".into(), id.into(),
            "--zone-name".into(), zone,
            "--yes".into(),
        ];
        if let Some(rg) = &self.resource_group {
            args.push("--resource-group".into());
            args.push(rg.clone());
        }
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    fn build_dns_vnet_link_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let name = param_str(params, "id")?;
        let zone = param_str(params, "zone_name")?;
        let vnet = param_str(params, "vnet")?;
        let mut args = vec![
            "network".into(), "private-dns".into(), "link".into(), "vnet".into(), "create".into(),
            "--name".into(), name,
            "--zone-name".into(), zone,
            "--virtual-network".into(), vnet,
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

fn param_str(params: &Value, key: &str) -> Result<String, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing required parameter: {key}"))
}

fn param_str_or(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_str_present() {
        let params = serde_json::json!({"id": "my-db", "version": "16"});
        assert_eq!(param_str(&params, "id").unwrap(), "my-db");
        assert_eq!(param_str(&params, "version").unwrap(), "16");
    }

    #[test]
    fn test_param_str_missing() {
        let params = serde_json::json!({"id": "my-db"});
        let err = param_str(&params, "version").unwrap_err();
        assert!(err.contains("missing required parameter"));
        assert!(err.contains("version"));
    }

    #[test]
    fn test_param_str_or_default() {
        let params = serde_json::json!({"id": "my-db"});
        assert_eq!(param_str_or(&params, "sku", "Standard"), "Standard");
        // When present, uses the actual value
        let params2 = serde_json::json!({"sku": "Premium"});
        assert_eq!(param_str_or(&params2, "sku", "Standard"), "Premium");
    }

    #[test]
    fn test_create_dispatches_postgres() {
        // We can't actually run az, but we can verify the type is recognized
        // by checking that build_create_args succeeds.
        let p = AzureResourceProvisioner::new(None, Some("test-rg"));
        let params = serde_json::json!({"id": "my-pg", "version": "16"});
        let args = p.build_create_args("postgres", &params).unwrap();
        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"flexible-server".to_string()));
        assert!(args.contains(&"create".to_string()));
    }

    #[test]
    fn test_create_unknown_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "x"});
        let err = p.build_create_args("foobar_unknown", &params).unwrap_err();
        assert!(err.contains("unsupported resource type"));
    }

    #[test]
    fn test_delete_dispatches_redis() {
        let p = AzureResourceProvisioner::new(None, Some("test-rg"));
        let args = p.build_delete_args("redis", "my-cache").unwrap();
        assert!(args.contains(&"redis".to_string()));
        assert!(args.contains(&"delete".to_string()));
        assert!(args.contains(&"my-cache".to_string()));
        assert!(args.contains(&"--yes".to_string()));
    }

    #[test]
    fn test_delete_unknown_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let err = p.build_delete_args("not_a_thing", "x").unwrap_err();
        assert!(err.contains("unsupported resource type for delete"));
    }

    // ── Argument construction tests ─────────────────────────────────

    #[test]
    fn test_postgres_args() {
        let p = AzureResourceProvisioner::new(Some("sub-1"), Some("rg-1"));
        let params = serde_json::json!({
            "id": "acme-db",
            "version": "16",
            "sku": "Standard_B1ms",
            "storage_gb": 128,
            "backup_retention_days": 14
        });
        let args = p.build_create_args("postgres", &params).unwrap();

        assert_eq!(args[0], "az");
        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"flexible-server".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"acme-db".to_string()));
        assert!(args.contains(&"--version".to_string()));
        assert!(args.contains(&"16".to_string()));
        assert!(args.contains(&"--sku-name".to_string()));
        assert!(args.contains(&"Standard_B1ms".to_string()));
        assert!(args.contains(&"--storage-size".to_string()));
        assert!(args.contains(&"128".to_string()));
        assert!(args.contains(&"--backup-retention".to_string()));
        assert!(args.contains(&"14".to_string()));
        assert!(args.contains(&"--yes".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-1".to_string()));
        assert!(args.contains(&"--output".to_string()));
        assert!(args.contains(&"json".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-1".to_string()));
    }

    #[test]
    fn test_aks_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-k8s"));
        let params = serde_json::json!({
            "id": "my-cluster",
            "node_count": "5",
            "vm_size": "Standard_D4s_v3",
            "kubernetes_version": "1.28"
        });
        let args = p.build_create_args("aks", &params).unwrap();

        assert!(args.contains(&"aks".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
        assert!(args.contains(&"--node-count".to_string()));
        assert!(args.contains(&"5".to_string()));
        assert!(args.contains(&"--generate-ssh-keys".to_string()));
        assert!(args.contains(&"--node-vm-size".to_string()));
        assert!(args.contains(&"Standard_D4s_v3".to_string()));
        assert!(args.contains(&"--kubernetes-version".to_string()));
        assert!(args.contains(&"1.28".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-k8s".to_string()));
    }

    #[test]
    fn test_vnet_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "main-vnet",
            "address_space": "10.1.0.0/16"
        });
        let args = p.build_create_args("vnet", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"vnet".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"main-vnet".to_string()));
        assert!(args.contains(&"--address-prefix".to_string()));
        assert!(args.contains(&"10.1.0.0/16".to_string()));
    }

    #[test]
    fn test_vnet_args_default_address_space() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "default-vnet"});
        let args = p.build_create_args("vnet", &params).unwrap();

        assert!(args.contains(&"--address-prefix".to_string()));
        assert!(args.contains(&"10.0.0.0/16".to_string()));
    }

    #[test]
    fn test_storage_account_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-1"));
        let params = serde_json::json!({
            "id": "mystore",
            "sku": "Standard_GRS",
            "kind": "StorageV2",
            "access_tier": "Hot"
        });
        let args = p.build_create_args("storage_account", &params).unwrap();

        assert!(args.contains(&"storage".to_string()));
        assert!(args.contains(&"account".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--sku".to_string()));
        assert!(args.contains(&"Standard_GRS".to_string()));
        assert!(args.contains(&"--kind".to_string()));
        assert!(args.contains(&"StorageV2".to_string()));
        assert!(args.contains(&"--access-tier".to_string()));
        assert!(args.contains(&"Hot".to_string()));
    }

    #[test]
    fn test_container_app_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-app"));
        let params = serde_json::json!({
            "id": "my-app",
            "image": "myregistry.azurecr.io/app:latest",
            "cpu": "0.5",
            "memory": "1.0Gi",
            "min_replicas": 1,
            "max_replicas": 5
        });
        let args = p.build_create_args("container_app", &params).unwrap();

        assert!(args.contains(&"containerapp".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--image".to_string()));
        assert!(args.contains(&"myregistry.azurecr.io/app:latest".to_string()));
        assert!(args.contains(&"--cpu".to_string()));
        assert!(args.contains(&"0.5".to_string()));
        assert!(args.contains(&"--memory".to_string()));
        assert!(args.contains(&"1.0Gi".to_string()));
        assert!(args.contains(&"--min-replicas".to_string()));
        assert!(args.contains(&"--max-replicas".to_string()));
    }

    #[test]
    fn test_container_app_missing_image() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "my-app"});
        let err = p.build_create_args("container_app", &params).unwrap_err();
        assert!(err.contains("missing required parameter"));
        assert!(err.contains("image"));
    }

    #[test]
    fn test_load_balancer_args() {
        let p = AzureResourceProvisioner::new(Some("sub-lb"), Some("rg-lb"));
        let params = serde_json::json!({"id": "my-lb", "sku": "Basic"});
        let args = p.build_create_args("load_balancer", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"lb".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-lb".to_string()));
        assert!(args.contains(&"--sku".to_string()));
        assert!(args.contains(&"Basic".to_string()));
    }

    #[test]
    fn test_nsg_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-sec"));
        let params = serde_json::json!({"id": "my-nsg"});
        let args = p.build_create_args("nsg", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"nsg".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-nsg".to_string()));
    }

    #[test]
    fn test_redis_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-cache"));
        let params = serde_json::json!({
            "id": "my-cache",
            "sku": "Premium",
            "location": "eastus"
        });
        let args = p.build_create_args("redis", &params).unwrap();

        assert!(args.contains(&"redis".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--sku".to_string()));
        assert!(args.contains(&"Premium".to_string()));
        assert!(args.contains(&"--location".to_string()));
        assert!(args.contains(&"eastus".to_string()));
    }

    #[test]
    fn test_container_registry_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-acr"));
        let params = serde_json::json!({
            "id": "myregistry",
            "sku": "Premium",
            "admin_enabled": true
        });
        let args = p.build_create_args("container_registry", &params).unwrap();

        assert!(args.contains(&"acr".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--sku".to_string()));
        assert!(args.contains(&"Premium".to_string()));
        assert!(args.contains(&"--admin-enabled".to_string()));
    }

    #[test]
    fn test_dns_zone_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-dns"));
        let params = serde_json::json!({"id": "example.com"});
        let args = p.build_create_args("dns_zone", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"dns".to_string()));
        assert!(args.contains(&"zone".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"example.com".to_string()));
    }

    #[test]
    fn test_container_job_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-jobs"));
        let params = serde_json::json!({
            "id": "nightly-job",
            "image": "myregistry.azurecr.io/batch:v1",
            "trigger_type": "Schedule",
            "cron_expression": "0 0 * * *",
            "cpu": "1.0",
            "memory": "2.0Gi"
        });
        let args = p.build_create_args("container_job", &params).unwrap();

        assert!(args.contains(&"containerapp".to_string()));
        assert!(args.contains(&"job".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--trigger-type".to_string()));
        assert!(args.contains(&"Schedule".to_string()));
        assert!(args.contains(&"--cron-expression".to_string()));
        assert!(args.contains(&"0 0 * * *".to_string()));
    }

    #[test]
    fn test_no_subscription_no_flag() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "test-nsg"});
        let args = p.build_create_args("nsg", &params).unwrap();
        assert!(!args.contains(&"--subscription".to_string()));
    }

    #[test]
    fn test_subscription_flag_added() {
        let p = AzureResourceProvisioner::new(Some("sub-xyz"), None);
        let params = serde_json::json!({"id": "test-nsg"});
        let args = p.build_create_args("nsg", &params).unwrap();
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-xyz".to_string()));
    }

    #[test]
    fn test_delete_load_balancer_no_yes_flag() {
        // Load balancer delete doesn't use --yes
        let p = AzureResourceProvisioner::new(None, Some("rg-lb"));
        let args = p.build_delete_args("load_balancer", "my-lb").unwrap();
        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"lb".to_string()));
        assert!(args.contains(&"delete".to_string()));
        assert!(!args.contains(&"--yes".to_string()));
    }

    // ── Subnet tests ───────────────────────────────────────────────────

    #[test]
    fn test_subnet_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "app-subnet",
            "vnet": "acme-vnet",
            "address_prefix": "10.0.1.0/24"
        });
        let args = p.build_create_args("subnet", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"vnet".to_string()));
        assert!(args.contains(&"subnet".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"app-subnet".to_string()));
        assert!(args.contains(&"--vnet-name".to_string()));
        assert!(args.contains(&"acme-vnet".to_string()));
        assert!(args.contains(&"--address-prefixes".to_string()));
        assert!(args.contains(&"10.0.1.0/24".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-net".to_string()));
    }

    #[test]
    fn test_subnet_with_delegation() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "pg-subnet",
            "vnet": "acme-vnet",
            "address_prefix": "10.0.2.0/24",
            "delegation": "Microsoft.DBforPostgreSQL/flexibleServers"
        });
        let args = p.build_create_args("subnet", &params).unwrap();

        assert!(args.contains(&"--delegations".to_string()));
        assert!(args.contains(&"Microsoft.DBforPostgreSQL/flexibleServers".to_string()));
    }

    #[test]
    fn test_subnet_with_nsg() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "web-subnet",
            "vnet": "acme-vnet",
            "address_prefix": "10.0.3.0/24",
            "nsg": "web-nsg"
        });
        let args = p.build_create_args("subnet", &params).unwrap();

        assert!(args.contains(&"--network-security-group".to_string()));
        assert!(args.contains(&"web-nsg".to_string()));
    }

    #[test]
    fn test_subnet_delete_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({"vnet": "acme-vnet"});
        let args = p.build_delete_args_with_params("subnet", "app-subnet", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"vnet".to_string()));
        assert!(args.contains(&"subnet".to_string()));
        assert!(args.contains(&"delete".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"app-subnet".to_string()));
        assert!(args.contains(&"--vnet-name".to_string()));
        assert!(args.contains(&"acme-vnet".to_string()));
        assert!(args.contains(&"--yes".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-net".to_string()));
    }

    // ── NSG Rule tests ─────────────────────────────────────────────────

    #[test]
    fn test_nsg_rule_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-sec"));
        let params = serde_json::json!({
            "id": "allow-https",
            "nsg": "web-nsg",
            "priority": "200",
            "direction": "Inbound",
            "access": "Allow",
            "protocol": "Tcp",
            "destination_port": "443"
        });
        let args = p.build_create_args("nsg_rule", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"nsg".to_string()));
        assert!(args.contains(&"rule".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"allow-https".to_string()));
        assert!(args.contains(&"--nsg-name".to_string()));
        assert!(args.contains(&"web-nsg".to_string()));
        assert!(args.contains(&"--priority".to_string()));
        assert!(args.contains(&"200".to_string()));
        assert!(args.contains(&"--direction".to_string()));
        assert!(args.contains(&"Inbound".to_string()));
        assert!(args.contains(&"--access".to_string()));
        assert!(args.contains(&"Allow".to_string()));
        assert!(args.contains(&"--protocol".to_string()));
        assert!(args.contains(&"Tcp".to_string()));
        assert!(args.contains(&"--destination-port-ranges".to_string()));
        assert!(args.contains(&"443".to_string()));
    }

    #[test]
    fn test_nsg_rule_inbound_ssh() {
        let p = AzureResourceProvisioner::new(None, Some("rg-sec"));
        let params = serde_json::json!({
            "id": "allow-ssh",
            "nsg": "acme-nsg",
            "priority": "100",
            "direction": "Inbound",
            "access": "Allow",
            "protocol": "Tcp",
            "source_address": "10.0.0.0/8",
            "destination_port": "22"
        });
        let args = p.build_create_args("nsg_rule", &params).unwrap();

        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"allow-ssh".to_string()));
        assert!(args.contains(&"--nsg-name".to_string()));
        assert!(args.contains(&"acme-nsg".to_string()));
        assert!(args.contains(&"--priority".to_string()));
        assert!(args.contains(&"100".to_string()));
        assert!(args.contains(&"--source-address-prefixes".to_string()));
        assert!(args.contains(&"10.0.0.0/8".to_string()));
        assert!(args.contains(&"--destination-port-ranges".to_string()));
        assert!(args.contains(&"22".to_string()));
        // Default source port should be *
        assert!(args.contains(&"--source-port-ranges".to_string()));
        assert!(args.contains(&"*".to_string()));
    }

    #[test]
    fn test_nsg_rule_delete_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-sec"));
        let params = serde_json::json!({"nsg": "acme-nsg"});
        let args = p.build_delete_args_with_params("nsg_rule", "allow-ssh", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"nsg".to_string()));
        assert!(args.contains(&"rule".to_string()));
        assert!(args.contains(&"delete".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"allow-ssh".to_string()));
        assert!(args.contains(&"--nsg-name".to_string()));
        assert!(args.contains(&"acme-nsg".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-sec".to_string()));
    }

    // ── VNet Peering tests ─────────────────────────────────────────────

    #[test]
    fn test_vnet_peering_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "dev-to-prod",
            "vnet": "dev-vnet",
            "remote_vnet": "/subscriptions/sub-1/resourceGroups/rg-prod/providers/Microsoft.Network/virtualNetworks/prod-vnet"
        });
        let args = p.build_create_args("vnet_peering", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"vnet".to_string()));
        assert!(args.contains(&"peering".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"dev-to-prod".to_string()));
        assert!(args.contains(&"--vnet-name".to_string()));
        assert!(args.contains(&"dev-vnet".to_string()));
        assert!(args.contains(&"--remote-vnet".to_string()));
        assert!(args.contains(&"--allow-vnet-access".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-net".to_string()));
    }

    #[test]
    fn test_vnet_peering_with_forwarding() {
        let p = AzureResourceProvisioner::new(None, Some("rg-net"));
        let params = serde_json::json!({
            "id": "hub-to-spoke",
            "vnet": "hub-vnet",
            "remote_vnet": "spoke-vnet",
            "allow_forwarded_traffic": true,
            "allow_gateway_transit": true
        });
        let args = p.build_create_args("vnet_peering", &params).unwrap();

        assert!(args.contains(&"--allow-forwarded-traffic".to_string()));
        assert!(args.contains(&"--allow-gateway-transit".to_string()));
        assert!(args.contains(&"--allow-vnet-access".to_string()));
    }

    // ── PostgreSQL Database ─────────────────────────────────────────

    #[test]
    fn test_pg_database_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-pg"));
        let params = serde_json::json!({
            "id": "drivelog",
            "server": "acme-pg-server",
            "charset": "UTF8",
            "collation": "en_US.utf8"
        });
        let args = p.build_create_args("pg_database", &params).unwrap();

        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"flexible-server".to_string()));
        assert!(args.contains(&"db".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--database-name".to_string()));
        assert!(args.contains(&"drivelog".to_string()));
        assert!(args.contains(&"--server-name".to_string()));
        assert!(args.contains(&"acme-pg-server".to_string()));
        assert!(args.contains(&"--charset".to_string()));
        assert!(args.contains(&"UTF8".to_string()));
        assert!(args.contains(&"--collation".to_string()));
        assert!(args.contains(&"en_US.utf8".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-pg".to_string()));
    }

    #[test]
    fn test_pg_database_minimal_args() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "mydb",
            "server": "pg-server"
        });
        let args = p.build_create_args("pg_database", &params).unwrap();

        assert!(args.contains(&"--database-name".to_string()));
        assert!(args.contains(&"mydb".to_string()));
        assert!(args.contains(&"--server-name".to_string()));
        assert!(args.contains(&"pg-server".to_string()));
        assert!(!args.contains(&"--charset".to_string()));
        assert!(!args.contains(&"--collation".to_string()));
    }

    #[test]
    fn test_pg_database_missing_server() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "mydb"});
        let err = p.build_create_args("pg_database", &params).unwrap_err();
        assert!(err.contains("missing required parameter"));
        assert!(err.contains("server"));
    }

    // ── Private DNS VNet Link ───────────────────────────────────────

    #[test]
    fn test_dns_vnet_link_args() {
        let p = AzureResourceProvisioner::new(Some("sub-net"), Some("rg-dns"));
        let params = serde_json::json!({
            "id": "pg-dns-link",
            "zone_name": "privatelink.postgres.database.azure.com",
            "vnet": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/my-vnet",
            "registration_enabled": true
        });
        let args = p.build_create_args("dns_vnet_link", &params).unwrap();

        assert!(args.contains(&"network".to_string()));
        assert!(args.contains(&"private-dns".to_string()));
        assert!(args.contains(&"link".to_string()));
        assert!(args.contains(&"vnet".to_string()));
        assert!(args.contains(&"create".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"pg-dns-link".to_string()));
        assert!(args.contains(&"--zone-name".to_string()));
        assert!(args.contains(&"privatelink.postgres.database.azure.com".to_string()));
        assert!(args.contains(&"--virtual-network".to_string()));
        assert!(args.contains(&"--registration-enabled".to_string()));
        assert!(args.contains(&"true".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-dns".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-net".to_string()));
    }

    #[test]
    fn test_dns_vnet_link_no_registration() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "link1",
            "zone_name": "zone",
            "vnet": "vnet1"
        });
        let args = p.build_create_args("dns_vnet_link", &params).unwrap();

        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"link1".to_string()));
        assert!(!args.contains(&"--registration-enabled".to_string()));
    }

    #[test]
    fn test_dns_vnet_link_missing_zone() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "link1", "vnet": "vnet1"});
        let err = p.build_create_args("dns_vnet_link", &params).unwrap_err();
        assert!(err.contains("missing required parameter"));
        assert!(err.contains("zone_name"));
    }

    // ── Day-2 Operations ──────────────────────────────────────────────

    #[test]
    fn test_backup_postgres() {
        let p = AzureResourceProvisioner::new(None, Some("rg-1"));
        let result = p.backup("postgres", "acme-db", None, None).unwrap();
        assert_eq!(result.status, "backed_up");
        assert!(result.outputs.is_some());
        let out = result.outputs.unwrap();
        assert_eq!(out["server"], "acme-db");
    }

    #[test]
    fn test_backup_unsupported_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let err = p.backup("aks", "k8s1", None, None).unwrap_err();
        assert!(err.contains("backup not supported"));
    }

    #[test]
    fn test_restore_postgres_args() {
        let p = AzureResourceProvisioner::new(Some("sub-1"), Some("rg-1"));
        let args = p
            .build_restore_args("postgres", "acme-db", "2026-04-01T10:00:00Z")
            .unwrap();

        assert_eq!(args[0], "az");
        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"flexible-server".to_string()));
        assert!(args.contains(&"restore".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"acme-db-restored".to_string()));
        assert!(args.contains(&"--source-server".to_string()));
        assert!(args.contains(&"acme-db".to_string()));
        assert!(args.contains(&"--restore-time".to_string()));
        assert!(args.contains(&"2026-04-01T10:00:00Z".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-1".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-1".to_string()));
    }

    #[test]
    fn test_restore_unsupported_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let err = p.build_restore_args("aks", "k8s1", "some-source").unwrap_err();
        assert!(err.contains("restore not supported"));
    }

    #[test]
    fn test_scale_aks_args() {
        let p = AzureResourceProvisioner::new(Some("sub-1"), Some("rg-k8s"));
        let params = serde_json::json!({"node_count": "5"});
        let args = p.build_scale_args("aks", "acme-k8s", &params).unwrap();

        assert_eq!(args[0], "az");
        assert!(args.contains(&"aks".to_string()));
        assert!(args.contains(&"nodepool".to_string()));
        assert!(args.contains(&"scale".to_string()));
        assert!(args.contains(&"--cluster-name".to_string()));
        assert!(args.contains(&"acme-k8s".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"nodepool1".to_string()));
        assert!(args.contains(&"--node-count".to_string()));
        assert!(args.contains(&"5".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-k8s".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-1".to_string()));
    }

    #[test]
    fn test_scale_container_app_args() {
        let p = AzureResourceProvisioner::new(None, Some("rg-app"));
        let params = serde_json::json!({"min_replicas": 2, "max_replicas": 10});
        let args = p
            .build_scale_args("container_app", "acme-api", &params)
            .unwrap();

        assert!(args.contains(&"containerapp".to_string()));
        assert!(args.contains(&"update".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"acme-api".to_string()));
        assert!(args.contains(&"--min-replicas".to_string()));
        assert!(args.contains(&"--max-replicas".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-app".to_string()));
    }

    #[test]
    fn test_scale_unsupported_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"node_count": "5"});
        let err = p.build_scale_args("redis", "cache1", &params).unwrap_err();
        assert!(err.contains("scale not supported"));
    }

    #[test]
    fn test_upgrade_aks_args() {
        let p = AzureResourceProvisioner::new(Some("sub-1"), Some("rg-k8s"));
        let params = serde_json::json!({"kubernetes_version": "1.29"});
        let args = p.build_upgrade_args("aks", "acme-k8s", &params).unwrap();

        assert_eq!(args[0], "az");
        assert!(args.contains(&"aks".to_string()));
        assert!(args.contains(&"upgrade".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"acme-k8s".to_string()));
        assert!(args.contains(&"--kubernetes-version".to_string()));
        assert!(args.contains(&"1.29".to_string()));
        assert!(args.contains(&"--yes".to_string()));
        assert!(args.contains(&"--resource-group".to_string()));
        assert!(args.contains(&"rg-k8s".to_string()));
        assert!(args.contains(&"--subscription".to_string()));
        assert!(args.contains(&"sub-1".to_string()));
    }

    #[test]
    fn test_upgrade_unsupported_type() {
        let p = AzureResourceProvisioner::new(None, None);
        let params = serde_json::json!({"kubernetes_version": "1.29"});
        let err = p.build_upgrade_args("redis", "cache1", &params).unwrap_err();
        assert!(err.contains("upgrade not supported"));
    }
}
