use serde_json::Value;
use tracing::{debug, error};

use crate::provision::ProvisionError;

use super::{str_field, AzureDiscoverCollector, AzureResourceProvisioner};

// ── Discovery ───────────────────────────────────────────────────

impl AzureResourceProvisioner {
    /// Discover all Azure resources across supported types by shelling out to
    /// `az` CLI list commands.  Returns a flat `Vec<Value>` where each entry
    /// has at minimum: `id`, `resource_type`, `name`, `config`, `outputs`.
    pub fn discover(&self) -> Result<Vec<Value>, ProvisionError> {
        let mut all: Vec<Value> = Vec::new();

        let collectors: Vec<AzureDiscoverCollector> = vec![
            ("vm", Self::discover_vms),
            ("postgres", Self::discover_postgres),
            ("pg_database", Self::discover_pg_databases),
            ("redis", Self::discover_redis),
            ("aks", Self::discover_aks),
            ("vnet", Self::discover_vnets),
            ("subnet", Self::discover_subnets),
            ("nsg", Self::discover_nsgs),
            ("nsg_rule", Self::discover_nsg_rules),
            ("storage_account", Self::discover_storage_accounts),
            ("keyvault", Self::discover_keyvaults),
            ("container_registry", Self::discover_container_registries),
            ("container_app", Self::discover_container_apps),
            ("container_job", Self::discover_container_jobs),
            ("dns_zone", Self::discover_dns_zones),
            ("load_balancer", Self::discover_load_balancers),
        ];

        for (resource_type, collector) in collectors {
            match collector(self) {
                Ok(items) => all.extend(items),
                Err(e) => {
                    // Log the failure as a warning entry so callers can see
                    // which types failed without aborting the whole discovery.
                    all.push(serde_json::json!({
                        "id": format!("_warning_{resource_type}"),
                        "resource_type": resource_type,
                        "name": format!("_warning_{resource_type}"),
                        "config": {},
                        "outputs": {
                            "warning": format!("discovery failed for {resource_type}: {e}")
                        }
                    }));
                }
            }
        }

        Ok(all)
    }

    /// Run an `az ... list` command and return the parsed JSON array, or an
    /// empty vec when the output is empty/null.  Logs the command and
    /// item count to stderr so discover failures are visible.
    fn run_az_list(&self, args: &[&str]) -> Result<Vec<Value>, ProvisionError> {
        let cmd_str = self.build_args(args).join(" ");
        debug!(provider = "azure", cmd = %cmd_str, "discover running");
        match self.run_az(args) {
            Ok(result) => {
                let items = match result {
                    Value::Array(arr) => arr,
                    Value::Null => Vec::new(),
                    other => vec![other],
                };
                debug!(
                    provider = "azure",
                    count = items.len(),
                    "discover completed"
                );
                Ok(items)
            }
            Err(e) => {
                error!(provider = "azure", error = %e, "discover failed");
                Err(e)
            }
        }
    }

    fn discover_vms(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["vm", "list", "-d"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "vm",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "size": v.get("hardwareProfile")
                            .and_then(|hp| hp.get("vmSize"))
                            .and_then(|s| s.as_str())
                            .unwrap_or(""),
                        "os": v.get("storageProfile")
                            .and_then(|sp| sp.get("osDisk"))
                            .and_then(|od| od.get("osType"))
                            .and_then(|s| s.as_str())
                            .unwrap_or(""),
                        "location": str_field(&v, "location"),
                    },
                    "outputs": {
                        "state": str_field(&v, "powerState"),
                        "public_ip": str_field(&v, "publicIps"),
                        "private_ip": str_field(&v, "privateIps"),
                    }
                })
            })
            .collect())
    }

    fn discover_postgres(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["postgres", "flexible-server", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "postgres",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "version": str_field(&v, "version"),
                        "sku": v.get("sku").and_then(|s| s.get("name"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                        "storage_gb": v.get("storage")
                            .and_then(|s| s.get("storageSizeGb"))
                            .and_then(|s| s.as_u64()).unwrap_or(0),
                    },
                    "outputs": {
                        "state": str_field(&v, "state"),
                        "fqdn": str_field(&v, "fullyQualifiedDomainName"),
                    }
                })
            })
            .collect())
    }

    fn discover_redis(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["redis", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "redis",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "sku": v.get("sku").and_then(|s| s.get("name"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                        "minimum_tls_version": str_field(&v, "minimumTlsVersion"),
                    },
                    "outputs": {
                        "host_name": str_field(&v, "hostName"),
                        "ssl_port": v.get("sslPort").and_then(|s| s.as_u64()).unwrap_or(0),
                    }
                })
            })
            .collect())
    }

    fn discover_aks(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["aks", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                let node_count = v
                    .get("agentPoolProfiles")
                    .and_then(|a| a.as_array())
                    .and_then(|a| a.first())
                    .and_then(|p| p.get("count"))
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0);
                let vm_size = v
                    .get("agentPoolProfiles")
                    .and_then(|a| a.as_array())
                    .and_then(|a| a.first())
                    .and_then(|p| p.get("vmSize"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                serde_json::json!({
                    "id": name,
                    "resource_type": "aks",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "kubernetes_version": str_field(&v, "kubernetesVersion"),
                        "node_count": node_count,
                        "vm_size": vm_size,
                    },
                    "outputs": {
                        "fqdn": str_field(&v, "fqdn"),
                        "state": v.get("powerState")
                            .and_then(|ps| ps.get("code"))
                            .and_then(|s| s.as_str())
                            .unwrap_or(""),
                    }
                })
            })
            .collect())
    }

    fn discover_vnets(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["network", "vnet", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                let address_space = v
                    .get("addressSpace")
                    .and_then(|a| a.get("addressPrefixes"))
                    .cloned()
                    .unwrap_or(Value::Array(Vec::new()));
                let subnet_count = v
                    .get("subnets")
                    .and_then(|s| s.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                serde_json::json!({
                    "id": name,
                    "resource_type": "vnet",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "address_space": address_space,
                        "subnet_count": subnet_count,
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    fn discover_nsgs(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["network", "nsg", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                let rule_count = v
                    .get("securityRules")
                    .and_then(|s| s.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                serde_json::json!({
                    "id": name,
                    "resource_type": "nsg",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "rule_count": rule_count,
                        "location": str_field(&v, "location"),
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    fn discover_storage_accounts(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["storage", "account", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "storage_account",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "sku": v.get("sku").and_then(|s| s.get("name"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                        "location": str_field(&v, "location"),
                        "minimum_tls_version": str_field(&v, "minimumTlsVersion"),
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    fn discover_keyvaults(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["keyvault", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "keyvault",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "location": str_field(&v, "location"),
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    // ── container_registry (ACR) ─────────────────────────────

    fn discover_container_registries(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["acr", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "container_registry",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "sku": v.get("sku").and_then(|s| s.get("name"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                        "location": str_field(&v, "location"),
                        "admin_enabled": v.get("adminUserEnabled")
                            .and_then(|b| b.as_bool()).unwrap_or(false),
                    },
                    "outputs": {
                        "login_server": str_field(&v, "loginServer"),
                    }
                })
            })
            .collect())
    }

    // ── container_app ────────────────────────────────────────

    fn discover_container_apps(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["containerapp", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                let image = v
                    .get("properties")
                    .and_then(|p| p.get("template"))
                    .and_then(|t| t.get("containers"))
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|c| c.get("image"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let fqdn = v
                    .get("properties")
                    .and_then(|p| p.get("configuration"))
                    .and_then(|c| c.get("ingress"))
                    .and_then(|i| i.get("fqdn"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                serde_json::json!({
                    "id": name,
                    "resource_type": "container_app",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "location": str_field(&v, "location"),
                        "image": image,
                        "state": v.get("properties")
                            .and_then(|p| p.get("runningStatus"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                    },
                    "outputs": { "fqdn": fqdn }
                })
            })
            .collect())
    }

    // ── container_job ────────────────────────────────────────

    fn discover_container_jobs(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["containerapp", "job", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                let schedule = v
                    .get("properties")
                    .and_then(|p| p.get("configuration"))
                    .and_then(|c| c.get("scheduleTriggerConfig"))
                    .and_then(|s| s.get("cronExpression"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                serde_json::json!({
                    "id": name,
                    "resource_type": "container_job",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "location": str_field(&v, "location"),
                        "schedule": schedule,
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    // ── dns_zone (public and private) ────────────────────────

    fn discover_dns_zones(&self) -> Result<Vec<Value>, ProvisionError> {
        let mut zones: Vec<Value> = Vec::new();

        // Public DNS
        if let Ok(items) = self.run_az_list(&["network", "dns", "zone", "list"]) {
            for v in items {
                let name = str_field(&v, "name");
                zones.push(serde_json::json!({
                    "id": name,
                    "resource_type": "dns_zone",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "zone_type": "public",
                        "record_count": v.get("numberOfRecordSets")
                            .and_then(|n| n.as_i64()).unwrap_or(0),
                    },
                    "outputs": {}
                }));
            }
        }

        // Private DNS
        if let Ok(items) = self.run_az_list(&["network", "private-dns", "zone", "list"]) {
            for v in items {
                let name = str_field(&v, "name");
                zones.push(serde_json::json!({
                    "id": name,
                    "resource_type": "dns_zone",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "zone_type": "private",
                        "record_count": v.get("numberOfRecordSets")
                            .and_then(|n| n.as_i64()).unwrap_or(0),
                    },
                    "outputs": {}
                }));
            }
        }

        Ok(zones)
    }

    // ── load_balancer ────────────────────────────────────────

    fn discover_load_balancers(&self) -> Result<Vec<Value>, ProvisionError> {
        let items = self.run_az_list(&["network", "lb", "list"])?;
        Ok(items
            .into_iter()
            .map(|v| {
                let name = str_field(&v, "name");
                serde_json::json!({
                    "id": name,
                    "resource_type": "load_balancer",
                    "name": name,
                    "config": {
                        "resource_group": str_field(&v, "resourceGroup"),
                        "sku": v.get("sku").and_then(|s| s.get("name"))
                            .and_then(|s| s.as_str()).unwrap_or(""),
                        "location": str_field(&v, "location"),
                    },
                    "outputs": {}
                })
            })
            .collect())
    }

    // ── Nested: subnets within VNets ─────────────────────────

    fn discover_subnets(&self) -> Result<Vec<Value>, ProvisionError> {
        let vnets = self.run_az_list(&["network", "vnet", "list"])?;
        let mut subnets: Vec<Value> = Vec::new();
        for vnet in &vnets {
            let vnet_name = str_field(vnet, "name");
            let rg = str_field(vnet, "resourceGroup");
            if let Some(arr) = vnet.get("subnets").and_then(|s| s.as_array()) {
                for subnet in arr {
                    let name = str_field(subnet, "name");
                    let id = format!("{vnet_name}/{name}");
                    subnets.push(serde_json::json!({
                        "id": id,
                        "resource_type": "subnet",
                        "name": name,
                        "config": {
                            "resource_group": rg,
                            "vnet": vnet_name,
                            "address_prefix": str_field(subnet, "addressPrefix"),
                        },
                        "outputs": {}
                    }));
                }
            }
        }
        Ok(subnets)
    }

    // ── Nested: NSG rules within NSGs ────────────────────────

    fn discover_nsg_rules(&self) -> Result<Vec<Value>, ProvisionError> {
        let nsgs = self.run_az_list(&["network", "nsg", "list"])?;
        let mut rules: Vec<Value> = Vec::new();
        for nsg in &nsgs {
            let nsg_name = str_field(nsg, "name");
            let rg = str_field(nsg, "resourceGroup");
            if let Some(arr) = nsg.get("securityRules").and_then(|s| s.as_array()) {
                for rule in arr {
                    let name = str_field(rule, "name");
                    let id = format!("{nsg_name}/{name}");
                    rules.push(serde_json::json!({
                        "id": id,
                        "resource_type": "nsg_rule",
                        "name": name,
                        "config": {
                            "resource_group": rg,
                            "nsg": nsg_name,
                            "priority": rule.get("priority").and_then(|p| p.as_i64()).unwrap_or(0),
                            "direction": str_field(rule, "direction"),
                            "protocol": str_field(rule, "protocol"),
                            "source_address_prefix": str_field(rule, "sourceAddressPrefix"),
                            "destination_port_range": str_field(rule, "destinationPortRange"),
                            "access": str_field(rule, "access"),
                        },
                        "outputs": {}
                    }));
                }
            }
        }
        Ok(rules)
    }

    // ── Nested: databases within PG flexible servers ─────────

    fn discover_pg_databases(&self) -> Result<Vec<Value>, ProvisionError> {
        let servers = self.run_az_list(&["postgres", "flexible-server", "list"])?;
        let mut dbs: Vec<Value> = Vec::new();
        for server in &servers {
            let server_name = str_field(server, "name");
            let rg = str_field(server, "resourceGroup");
            if rg.is_empty() || server_name.is_empty() {
                continue;
            }
            // List databases for this server
            let server_dbs = self
                .run_az_list(&[
                    "postgres",
                    "flexible-server",
                    "db",
                    "list",
                    "--server-name",
                    server_name,
                    "--resource-group",
                    rg,
                ])
                .unwrap_or_default();
            for db in server_dbs {
                let name = str_field(&db, "name");
                // Skip system databases
                if name == "azure_maintenance" || name == "azure_sys" {
                    continue;
                }
                let id = format!("{server_name}/{name}");
                dbs.push(serde_json::json!({
                    "id": id,
                    "resource_type": "pg_database",
                    "name": name,
                    "config": {
                        "resource_group": rg,
                        "server": server_name,
                        "charset": str_field(&db, "charset"),
                        "collation": str_field(&db, "collation"),
                    },
                    "outputs": {}
                }));
            }
        }
        Ok(dbs)
    }
}
