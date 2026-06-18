use super::*;

use crate::provision::{param_str, param_str_or};

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
    assert!(err.to_string().contains("missing required parameter"));
    assert!(err.to_string().contains("version"));
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
    assert!(err.to_string().contains("unsupported resource type"));
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
    assert!(err
        .to_string()
        .contains("unsupported resource type for delete"));
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
    assert!(err.to_string().contains("missing required parameter"));
    assert!(err.to_string().contains("image"));
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
    let args = p
        .build_delete_args_with_params("subnet", "app-subnet", &params)
        .unwrap();

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
    let args = p
        .build_delete_args_with_params("nsg_rule", "allow-ssh", &params)
        .unwrap();

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
    assert!(err.to_string().contains("missing required parameter"));
    assert!(err.to_string().contains("server"));
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
    assert!(err.to_string().contains("missing required parameter"));
    assert!(err.to_string().contains("zone_name"));
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
    assert!(err.to_string().contains("backup not supported"));
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
    let err = p
        .build_restore_args("aks", "k8s1", "some-source")
        .unwrap_err();
    assert!(err.to_string().contains("restore not supported"));
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
    assert!(err.to_string().contains("scale not supported"));
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
    let err = p
        .build_upgrade_args("redis", "cache1", &params)
        .unwrap_err();
    assert!(err.to_string().contains("upgrade not supported"));
}

// ── Discovery parsing tests ─────────────────────────────────────

#[test]
fn test_discover_parse_vm_output() {
    let sample = serde_json::json!([
        {
            "name": "web-vm-01",
            "resourceGroup": "rg-prod",
            "hardwareProfile": { "vmSize": "Standard_D4s_v3" },
            "storageProfile": { "osDisk": { "osType": "Linux" } },
            "powerState": "VM running",
            "publicIps": "20.1.2.3",
            "privateIps": "10.0.0.5",
            "location": "eastus2"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "vm",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "size": v.get("hardwareProfile")
                        .and_then(|hp| hp.get("vmSize"))
                        .and_then(|s| s.as_str())
                        .unwrap_or(""),
                    "os": v.get("storageProfile")
                        .and_then(|sp| sp.get("osDisk"))
                        .and_then(|od| od.get("osType"))
                        .and_then(|s| s.as_str())
                        .unwrap_or(""),
                    "location": str_field(v, "location"),
                },
                "outputs": {
                    "state": str_field(v, "powerState"),
                    "public_ip": str_field(v, "publicIps"),
                    "private_ip": str_field(v, "privateIps"),
                }
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], "web-vm-01");
    assert_eq!(items[0]["resource_type"], "vm");
    assert_eq!(items[0]["config"]["size"], "Standard_D4s_v3");
    assert_eq!(items[0]["config"]["os"], "Linux");
    assert_eq!(items[0]["config"]["location"], "eastus2");
    assert_eq!(items[0]["outputs"]["state"], "VM running");
    assert_eq!(items[0]["outputs"]["public_ip"], "20.1.2.3");
    assert_eq!(items[0]["outputs"]["private_ip"], "10.0.0.5");
}

#[test]
fn test_discover_parse_postgres_output() {
    let sample = serde_json::json!([
        {
            "name": "acme-pg",
            "resourceGroup": "rg-data",
            "version": "16",
            "sku": { "name": "Standard_B1ms", "tier": "Burstable" },
            "storage": { "storageSizeGb": 128 },
            "state": "Ready",
            "fullyQualifiedDomainName": "acme-pg.postgres.database.azure.com"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "postgres",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "version": str_field(v, "version"),
                    "sku": v.get("sku").and_then(|s| s.get("name"))
                        .and_then(|s| s.as_str()).unwrap_or(""),
                    "storage_gb": v.get("storage")
                        .and_then(|s| s.get("storageSizeGb"))
                        .and_then(|s| s.as_u64()).unwrap_or(0),
                },
                "outputs": {
                    "state": str_field(v, "state"),
                    "fqdn": str_field(v, "fullyQualifiedDomainName"),
                }
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "acme-pg");
    assert_eq!(items[0]["config"]["version"], "16");
    assert_eq!(items[0]["config"]["sku"], "Standard_B1ms");
    assert_eq!(items[0]["config"]["storage_gb"], 128);
    assert_eq!(items[0]["outputs"]["state"], "Ready");
    assert_eq!(
        items[0]["outputs"]["fqdn"],
        "acme-pg.postgres.database.azure.com"
    );
}

#[test]
fn test_discover_parse_aks_output() {
    let sample = serde_json::json!([
        {
            "name": "prod-k8s",
            "resourceGroup": "rg-k8s",
            "kubernetesVersion": "1.28.5",
            "agentPoolProfiles": [
                { "count": 5, "vmSize": "Standard_D8s_v3" }
            ],
            "fqdn": "prod-k8s-dns-abc123.hcp.eastus.azmk8s.io",
            "powerState": { "code": "Running" }
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
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
                    "resource_group": str_field(v, "resourceGroup"),
                    "kubernetes_version": str_field(v, "kubernetesVersion"),
                    "node_count": node_count,
                    "vm_size": vm_size,
                },
                "outputs": {
                    "fqdn": str_field(v, "fqdn"),
                    "state": v.get("powerState")
                        .and_then(|ps| ps.get("code"))
                        .and_then(|s| s.as_str())
                        .unwrap_or(""),
                }
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "prod-k8s");
    assert_eq!(items[0]["config"]["kubernetes_version"], "1.28.5");
    assert_eq!(items[0]["config"]["node_count"], 5);
    assert_eq!(items[0]["config"]["vm_size"], "Standard_D8s_v3");
    assert_eq!(
        items[0]["outputs"]["fqdn"],
        "prod-k8s-dns-abc123.hcp.eastus.azmk8s.io"
    );
    assert_eq!(items[0]["outputs"]["state"], "Running");
}

#[test]
fn test_discover_parse_vnet_output() {
    let sample = serde_json::json!([
        {
            "name": "acme-vnet",
            "resourceGroup": "rg-net",
            "addressSpace": { "addressPrefixes": ["10.0.0.0/16"] },
            "subnets": [
                { "name": "subnet-a" },
                { "name": "subnet-b" }
            ]
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            let address_space = v
                .get("addressSpace")
                .and_then(|a| a.get("addressPrefixes"))
                .cloned()
                .unwrap_or(serde_json::Value::Array(Vec::new()));
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
                    "resource_group": str_field(v, "resourceGroup"),
                    "address_space": address_space,
                    "subnet_count": subnet_count,
                },
                "outputs": {}
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "acme-vnet");
    assert_eq!(items[0]["config"]["address_space"][0], "10.0.0.0/16");
    assert_eq!(items[0]["config"]["subnet_count"], 2);
}

#[test]
fn test_discover_parse_empty_array() {
    // Simulate empty az output — should produce zero entries.
    let sample: Vec<serde_json::Value> = serde_json::from_str("[]").unwrap();
    assert!(sample.is_empty());
}

#[test]
fn test_discover_parse_nsg_output() {
    let sample = serde_json::json!([
        {
            "name": "web-nsg",
            "resourceGroup": "rg-sec",
            "securityRules": [
                { "name": "allow-http" },
                { "name": "allow-https" },
                { "name": "deny-all" }
            ],
            "location": "westus2"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
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
                    "resource_group": str_field(v, "resourceGroup"),
                    "rule_count": rule_count,
                    "location": str_field(v, "location"),
                },
                "outputs": {}
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "web-nsg");
    assert_eq!(items[0]["config"]["rule_count"], 3);
    assert_eq!(items[0]["config"]["location"], "westus2");
}

#[test]
fn test_discover_parse_storage_account_output() {
    let sample = serde_json::json!([
        {
            "name": "acmestorage",
            "resourceGroup": "rg-data",
            "sku": { "name": "Standard_LRS" },
            "location": "eastus",
            "minimumTlsVersion": "TLS1_2"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "storage_account",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "sku": v.get("sku").and_then(|s| s.get("name"))
                        .and_then(|s| s.as_str()).unwrap_or(""),
                    "location": str_field(v, "location"),
                    "minimum_tls_version": str_field(v, "minimumTlsVersion"),
                },
                "outputs": {}
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "acmestorage");
    assert_eq!(items[0]["config"]["sku"], "Standard_LRS");
    assert_eq!(items[0]["config"]["minimum_tls_version"], "TLS1_2");
}

#[test]
fn test_discover_parse_keyvault_output() {
    let sample = serde_json::json!([
        {
            "name": "acme-kv",
            "resourceGroup": "rg-sec",
            "location": "centralus"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "keyvault",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "location": str_field(v, "location"),
                },
                "outputs": {}
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "acme-kv");
    assert_eq!(items[0]["config"]["resource_group"], "rg-sec");
    assert_eq!(items[0]["config"]["location"], "centralus");
}

#[test]
fn test_discover_parse_redis_output() {
    let sample = serde_json::json!([
        {
            "name": "acme-cache",
            "resourceGroup": "rg-cache",
            "sku": { "name": "Premium" },
            "hostName": "acme-cache.redis.cache.windows.net",
            "sslPort": 6380,
            "minimumTlsVersion": "1.2"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "redis",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "sku": v.get("sku").and_then(|s| s.get("name"))
                        .and_then(|s| s.as_str()).unwrap_or(""),
                    "minimum_tls_version": str_field(v, "minimumTlsVersion"),
                },
                "outputs": {
                    "host_name": str_field(v, "hostName"),
                    "ssl_port": v.get("sslPort").and_then(|s| s.as_u64()).unwrap_or(0),
                }
            })
        })
        .collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "acme-cache");
    assert_eq!(items[0]["config"]["sku"], "Premium");
    assert_eq!(items[0]["config"]["minimum_tls_version"], "1.2");
    assert_eq!(
        items[0]["outputs"]["host_name"],
        "acme-cache.redis.cache.windows.net"
    );
    assert_eq!(items[0]["outputs"]["ssl_port"], 6380);
}

#[test]
fn test_discover_parse_multiple_vms() {
    let sample = serde_json::json!([
        {
            "name": "vm-01",
            "resourceGroup": "rg-prod",
            "hardwareProfile": { "vmSize": "Standard_D2s_v3" },
            "storageProfile": { "osDisk": { "osType": "Linux" } },
            "powerState": "VM running",
            "publicIps": "20.1.2.3",
            "privateIps": "10.0.0.5",
            "location": "eastus"
        },
        {
            "name": "vm-02",
            "resourceGroup": "rg-prod",
            "hardwareProfile": { "vmSize": "Standard_D4s_v3" },
            "storageProfile": { "osDisk": { "osType": "Windows" } },
            "powerState": "VM deallocated",
            "publicIps": "",
            "privateIps": "10.0.0.6",
            "location": "eastus"
        }
    ]);
    let items: Vec<serde_json::Value> = sample
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let name = str_field(v, "name");
            serde_json::json!({
                "id": name,
                "resource_type": "vm",
                "name": name,
                "config": {
                    "resource_group": str_field(v, "resourceGroup"),
                    "size": v.get("hardwareProfile")
                        .and_then(|hp| hp.get("vmSize"))
                        .and_then(|s| s.as_str())
                        .unwrap_or(""),
                    "os": v.get("storageProfile")
                        .and_then(|sp| sp.get("osDisk"))
                        .and_then(|od| od.get("osType"))
                        .and_then(|s| s.as_str())
                        .unwrap_or(""),
                    "location": str_field(v, "location"),
                },
                "outputs": {
                    "state": str_field(v, "powerState"),
                    "public_ip": str_field(v, "publicIps"),
                    "private_ip": str_field(v, "privateIps"),
                }
            })
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["name"], "vm-01");
    assert_eq!(items[0]["config"]["os"], "Linux");
    assert_eq!(items[0]["outputs"]["state"], "VM running");
    assert_eq!(items[1]["name"], "vm-02");
    assert_eq!(items[1]["config"]["os"], "Windows");
    assert_eq!(items[1]["outputs"]["state"], "VM deallocated");
}
