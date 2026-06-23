use serde_json::Value;

use crate::provision::{param_str, param_str_or, ProvisionError, ProvisionResult};

use super::AzureResourceProvisioner;

// ── Per-resource create implementations ──────────────────────────

impl AzureResourceProvisioner {
    pub(crate) fn create_postgres(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_redis(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_aks(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_storage_account(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_vnet(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_vnet_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn create_nsg(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_nsg_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn create_container_registry(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_dns_zone(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_container_app(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
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

    pub(crate) fn create_container_job(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_container_job_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn create_load_balancer(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_load_balancer_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    // ── Subnet (sub-resource of VNet) ──────────────────────────────────

    pub(crate) fn create_subnet(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_subnet_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn delete_subnet(&self, id: &str, params: &Value) -> Result<(), ProvisionError> {
        let args = self.build_subnet_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    // ── NSG Rule (sub-resource of NSG) ─────────────────────────────────

    pub(crate) fn create_nsg_rule(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_nsg_rule_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn delete_nsg_rule(&self, id: &str, params: &Value) -> Result<(), ProvisionError> {
        let args = self.build_nsg_rule_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    // ── VNet Peering ───────────────────────────────────────────────────

    pub(crate) fn create_vnet_peering(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_vnet_peering_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn delete_vnet_peering(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<(), ProvisionError> {
        let args = self.build_vnet_peering_delete_args(id, params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_az(&refs)?;
        Ok(())
    }

    // ── PostgreSQL Database ─────────────────────────────────────────

    pub(crate) fn create_pg_database(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_pg_database_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn delete_pg_database(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<(), ProvisionError> {
        let server = param_str(params, "server")?;
        let mut args: Vec<String> = vec![
            "postgres".into(),
            "flexible-server".into(),
            "db".into(),
            "delete".into(),
            "--database-name".into(),
            id.into(),
            "--server-name".into(),
            server,
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

    // ── Private DNS VNet Link ───────────────────────────────────────

    pub(crate) fn create_dns_vnet_link(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_dns_vnet_link_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_az(&refs)?;
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    pub(crate) fn delete_dns_vnet_link(
        &self,
        id: &str,
        params: &Value,
    ) -> Result<(), ProvisionError> {
        let zone = param_str(params, "zone_name")?;
        let mut args: Vec<String> = vec![
            "network".into(),
            "private-dns".into(),
            "link".into(),
            "vnet".into(),
            "delete".into(),
            "--name".into(),
            id.into(),
            "--zone-name".into(),
            zone,
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
}
