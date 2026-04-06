/// Well-known managed resource types and their expected parameters.
///
/// These definitions are informational — they are not enforced by the parser
/// or engine.  They can be used by tooling (CLI completion, validation
/// warnings, documentation generators) to guide users toward correct usage.
pub struct ResourceTypeDef {
    pub name: &'static str,
    pub description: &'static str,
    pub required_params: &'static [&'static str],
    pub optional_params: &'static [&'static str],
}

pub const RESOURCE_TYPES: &[ResourceTypeDef] = &[
    ResourceTypeDef {
        name: "postgres",
        description: "PostgreSQL Flexible Server",
        required_params: &["id", "version"],
        optional_params: &["sku", "storage_gb", "backup_retention_days", "geo_redundant_backup", "high_availability"],
    },
    ResourceTypeDef {
        name: "redis",
        description: "Redis Cache",
        required_params: &["id", "sku"],
        optional_params: &["capacity", "family", "enable_non_ssl_port"],
    },
    ResourceTypeDef {
        name: "aks",
        description: "Azure Kubernetes Service",
        required_params: &["id", "node_count"],
        optional_params: &["vm_size", "kubernetes_version", "network_plugin", "dns_prefix"],
    },
    ResourceTypeDef {
        name: "storage_account",
        description: "Storage Account (Blob/File/Queue/Table)",
        required_params: &["id", "sku"],
        optional_params: &["kind", "access_tier", "enable_https_only"],
    },
    ResourceTypeDef {
        name: "vnet",
        description: "Virtual Network",
        required_params: &["id", "address_space"],
        optional_params: &["subnets", "dns_servers"],
    },
    ResourceTypeDef {
        name: "nsg",
        description: "Network Security Group",
        required_params: &["id"],
        optional_params: &["rules"],
    },
    ResourceTypeDef {
        name: "container_registry",
        description: "Container Registry (ACR/ECR)",
        required_params: &["id", "sku"],
        optional_params: &["admin_enabled", "geo_replication"],
    },
    ResourceTypeDef {
        name: "dns_zone",
        description: "DNS Zone",
        required_params: &["id"],
        optional_params: &["records"],
    },
    ResourceTypeDef {
        name: "container_app",
        description: "Container App / Serverless Container",
        required_params: &["id", "image"],
        optional_params: &["cpu", "memory", "min_replicas", "max_replicas", "env_vars"],
    },
    ResourceTypeDef {
        name: "container_job",
        description: "Container Job (one-off or scheduled)",
        required_params: &["id", "image"],
        optional_params: &["cpu", "memory", "trigger_type", "cron_expression"],
    },
    ResourceTypeDef {
        name: "load_balancer",
        description: "Load Balancer",
        required_params: &["id"],
        optional_params: &["sku"],
    },
    ResourceTypeDef {
        name: "subnet",
        description: "VNet Subnet",
        required_params: &["id", "vnet", "address_prefix"],
        optional_params: &["delegation", "nsg", "route_table"],
    },
    ResourceTypeDef {
        name: "nsg_rule",
        description: "Network Security Group Rule",
        required_params: &["id", "nsg", "priority"],
        optional_params: &["direction", "access", "protocol", "source_address", "destination_port", "source_port", "destination_address"],
    },
    ResourceTypeDef {
        name: "vnet_peering",
        description: "VNet Peering",
        required_params: &["id", "vnet", "remote_vnet"],
        optional_params: &["allow_forwarded_traffic", "allow_gateway_transit", "use_remote_gateways"],
    },
    ResourceTypeDef {
        name: "pg_database",
        description: "PostgreSQL Database (on a Flexible Server)",
        required_params: &["id", "server"],
        optional_params: &["charset", "collation"],
    },
    ResourceTypeDef {
        name: "dns_vnet_link",
        description: "Private DNS Zone VNet Link",
        required_params: &["id", "zone_name", "vnet"],
        optional_params: &["registration_enabled"],
    },
    // ── AWS resource types ──────────────────────────────────────────
    ResourceTypeDef {
        name: "rds_postgres",
        description: "AWS RDS PostgreSQL",
        required_params: &["id", "instance_class", "engine_version", "master_username", "master_password", "storage_gb"],
        optional_params: &["multi_az", "backup_retention", "vpc_security_group_ids", "db_subnet_group"],
    },
    ResourceTypeDef {
        name: "vpc",
        description: "AWS VPC",
        required_params: &["id", "cidr_block"],
        optional_params: &["enable_dns_support", "enable_dns_hostnames"],
    },
    ResourceTypeDef {
        name: "aws_subnet",
        description: "AWS VPC Subnet",
        required_params: &["id", "vpc_id", "cidr_block"],
        optional_params: &["availability_zone", "map_public_ip"],
    },
    ResourceTypeDef {
        name: "security_group",
        description: "AWS Security Group",
        required_params: &["id", "description", "vpc_id"],
        optional_params: &[],
    },
    ResourceTypeDef {
        name: "sg_rule",
        description: "Security Group Rule",
        required_params: &["id", "security_group_id", "protocol", "port", "cidr"],
        optional_params: &["direction", "description"],
    },
    // ── Cloudflare resource types ───────────────────────────
    ResourceTypeDef {
        name: "cf_zone",
        description: "Cloudflare Zone (domain)",
        required_params: &["id"],
        optional_params: &["type", "plan"],
    },
    ResourceTypeDef {
        name: "cf_dns_record",
        description: "Cloudflare DNS Record",
        required_params: &["id", "zone", "content"],
        optional_params: &["type", "ttl", "proxied", "priority"],
    },
    ResourceTypeDef {
        name: "cf_firewall_rule",
        description: "Cloudflare Firewall Rule",
        required_params: &["id", "zone", "expression"],
        optional_params: &["action", "description"],
    },
    ResourceTypeDef {
        name: "cf_page_rule",
        description: "Cloudflare Page Rule",
        required_params: &["id", "zone", "url"],
        optional_params: &["priority", "cache_level", "ssl"],
    },
    // ── GitHub resource types ───────────────────────────────
    ResourceTypeDef {
        name: "gh_repo",
        description: "GitHub Repository",
        required_params: &["id"], // "org/name" or just "name"
        optional_params: &["visibility", "description", "default_branch"],
    },
    ResourceTypeDef {
        name: "gh_ruleset",
        description: "GitHub Repository Ruleset (branch protection via rulesets API)",
        required_params: &["id", "repo"],
        optional_params: &[
            "target",
            "enforcement",
            "branches",
            "require_pr",
            "required_approvals",
            "linear_history",
        ],
    },
    ResourceTypeDef {
        name: "gh_secret",
        description: "GitHub Actions Secret",
        required_params: &["id", "repo", "value"],
        optional_params: &[],
    },
    ResourceTypeDef {
        name: "gh_variable",
        description: "GitHub Actions Variable",
        required_params: &["id", "repo", "value"],
        optional_params: &[],
    },
    ResourceTypeDef {
        name: "gh_workflow_file",
        description: "GitHub Actions Workflow File",
        required_params: &["id", "repo", "content"],
        optional_params: &["message", "branch"],
    },
    ResourceTypeDef {
        name: "gh_branch_protection",
        description: "GitHub Branch Protection (legacy API, prefer gh_ruleset for new repos)",
        required_params: &["id", "repo"],
        optional_params: &["required_approvals"],
    },
    // ── Kubernetes resource types ───────────────────────────
    ResourceTypeDef {
        name: "k8s_namespace",
        description: "Kubernetes Namespace",
        required_params: &["id"],
        optional_params: &["labels"],
    },
    ResourceTypeDef {
        name: "k8s_deployment",
        description: "Kubernetes Deployment",
        required_params: &["id", "image"],
        optional_params: &["namespace", "replicas", "port", "container_name", "env"],
    },
    ResourceTypeDef {
        name: "k8s_service",
        description: "Kubernetes Service",
        required_params: &["id"],
        optional_params: &["namespace", "type", "selector", "port", "target_port"],
    },
    ResourceTypeDef {
        name: "k8s_ingress",
        description: "Kubernetes Ingress",
        required_params: &["id", "host", "service"],
        optional_params: &["namespace", "port", "path", "tls_secret", "ingress_class"],
    },
    ResourceTypeDef {
        name: "k8s_configmap",
        description: "Kubernetes ConfigMap",
        required_params: &["id", "data"],
        optional_params: &["namespace"],
    },
    ResourceTypeDef {
        name: "k8s_secret",
        description: "Kubernetes Secret",
        required_params: &["id", "data"],
        optional_params: &["namespace", "type"],
    },
];

/// Look up a resource type definition by name.
pub fn get_resource_type(name: &str) -> Option<&'static ResourceTypeDef> {
    RESOURCE_TYPES.iter().find(|rt| rt.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_known_types() {
        assert!(get_resource_type("postgres").is_some());
        assert!(get_resource_type("redis").is_some());
        assert!(get_resource_type("aks").is_some());
        assert!(get_resource_type("storage_account").is_some());
        assert!(get_resource_type("vnet").is_some());
        assert!(get_resource_type("nsg").is_some());
        assert!(get_resource_type("container_registry").is_some());
        assert!(get_resource_type("dns_zone").is_some());
        assert!(get_resource_type("container_app").is_some());
        assert!(get_resource_type("container_job").is_some());
        assert!(get_resource_type("load_balancer").is_some());
        assert!(get_resource_type("subnet").is_some());
        assert!(get_resource_type("nsg_rule").is_some());
        assert!(get_resource_type("vnet_peering").is_some());
        assert!(get_resource_type("pg_database").is_some());
        assert!(get_resource_type("dns_vnet_link").is_some());
        // AWS resource types
        assert!(get_resource_type("rds_postgres").is_some());
        assert!(get_resource_type("vpc").is_some());
        assert!(get_resource_type("aws_subnet").is_some());
        assert!(get_resource_type("security_group").is_some());
        assert!(get_resource_type("sg_rule").is_some());
        // Cloudflare resource types
        assert!(get_resource_type("cf_zone").is_some());
        assert!(get_resource_type("cf_dns_record").is_some());
        assert!(get_resource_type("cf_firewall_rule").is_some());
        assert!(get_resource_type("cf_page_rule").is_some());
        // GitHub resource types
        assert!(get_resource_type("gh_repo").is_some());
        assert!(get_resource_type("gh_ruleset").is_some());
        assert!(get_resource_type("gh_secret").is_some());
        assert!(get_resource_type("gh_variable").is_some());
        assert!(get_resource_type("gh_workflow_file").is_some());
        assert!(get_resource_type("gh_branch_protection").is_some());
        // Kubernetes resource types
        assert!(get_resource_type("k8s_namespace").is_some());
        assert!(get_resource_type("k8s_deployment").is_some());
        assert!(get_resource_type("k8s_service").is_some());
        assert!(get_resource_type("k8s_ingress").is_some());
        assert!(get_resource_type("k8s_configmap").is_some());
        assert!(get_resource_type("k8s_secret").is_some());
    }

    #[test]
    fn k8s_types_have_expected_params() {
        let dep = get_resource_type("k8s_deployment").unwrap();
        assert!(dep.required_params.contains(&"id"));
        assert!(dep.required_params.contains(&"image"));
        assert!(dep.optional_params.contains(&"replicas"));
        assert!(dep.optional_params.contains(&"namespace"));

        let svc = get_resource_type("k8s_service").unwrap();
        assert!(svc.required_params.contains(&"id"));
        assert!(svc.optional_params.contains(&"type"));

        let ing = get_resource_type("k8s_ingress").unwrap();
        assert!(ing.required_params.contains(&"host"));
        assert!(ing.required_params.contains(&"service"));
        assert!(ing.optional_params.contains(&"tls_secret"));

        let secret = get_resource_type("k8s_secret").unwrap();
        assert!(secret.required_params.contains(&"data"));
    }

    #[test]
    fn github_types_have_expected_params() {
        let repo = get_resource_type("gh_repo").unwrap();
        assert!(repo.required_params.contains(&"id"));
        assert!(repo.optional_params.contains(&"visibility"));

        let secret = get_resource_type("gh_secret").unwrap();
        assert!(secret.required_params.contains(&"id"));
        assert!(secret.required_params.contains(&"repo"));
        assert!(secret.required_params.contains(&"value"));

        let ruleset = get_resource_type("gh_ruleset").unwrap();
        assert!(ruleset.required_params.contains(&"id"));
        assert!(ruleset.required_params.contains(&"repo"));
        assert!(ruleset.optional_params.contains(&"require_pr"));

        let workflow = get_resource_type("gh_workflow_file").unwrap();
        assert!(workflow.required_params.contains(&"content"));
        assert!(workflow.optional_params.contains(&"branch"));
    }

    #[test]
    fn cloudflare_types_have_expected_params() {
        let zone = get_resource_type("cf_zone").unwrap();
        assert!(zone.required_params.contains(&"id"));

        let rec = get_resource_type("cf_dns_record").unwrap();
        assert!(rec.required_params.contains(&"id"));
        assert!(rec.required_params.contains(&"zone"));
        assert!(rec.required_params.contains(&"content"));
        assert!(rec.optional_params.contains(&"type"));
        assert!(rec.optional_params.contains(&"proxied"));

        let fw = get_resource_type("cf_firewall_rule").unwrap();
        assert!(fw.required_params.contains(&"expression"));

        let pr = get_resource_type("cf_page_rule").unwrap();
        assert!(pr.required_params.contains(&"url"));
    }

    #[test]
    fn unknown_type_returns_none() {
        assert!(get_resource_type("foobar").is_none());
    }

    #[test]
    fn postgres_has_expected_params() {
        let pg = get_resource_type("postgres").unwrap();
        assert!(pg.required_params.contains(&"id"));
        assert!(pg.required_params.contains(&"version"));
        assert!(pg.optional_params.contains(&"sku"));
    }

    #[test]
    fn rds_postgres_has_expected_params() {
        let rds = get_resource_type("rds_postgres").unwrap();
        assert!(rds.required_params.contains(&"id"));
        assert!(rds.required_params.contains(&"instance_class"));
        assert!(rds.required_params.contains(&"engine_version"));
        assert!(rds.required_params.contains(&"master_username"));
        assert!(rds.required_params.contains(&"master_password"));
        assert!(rds.required_params.contains(&"storage_gb"));
        assert!(rds.optional_params.contains(&"multi_az"));
    }

    #[test]
    fn sg_rule_has_expected_params() {
        let sg = get_resource_type("sg_rule").unwrap();
        assert!(sg.required_params.contains(&"security_group_id"));
        assert!(sg.required_params.contains(&"protocol"));
        assert!(sg.required_params.contains(&"port"));
        assert!(sg.required_params.contains(&"cidr"));
        assert!(sg.optional_params.contains(&"direction"));
    }
}
