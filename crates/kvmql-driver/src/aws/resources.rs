use std::process::Command;

use serde_json::Value;
use tracing::{debug, error};

use crate::provision::{param_str, param_str_or, ProvisionError, ProvisionResult};

/// A named collector function used during resource discovery.
type DiscoverCollector = (&'static str, fn(&AwsResourceProvisioner) -> Vec<Value>);

/// AWS resource provisioner that maps KVMQL resource types to `aws` CLI commands.
///
/// Uses `std::process::Command` with individual arguments (never shell
/// interpolation). The optional `region` and `profile` fields are injected
/// automatically where relevant.
#[derive(Debug, Clone)]
pub struct AwsResourceProvisioner {
    pub region: Option<String>,
    pub profile: Option<String>,
}

impl AwsResourceProvisioner {
    pub fn new(region: Option<&str>, profile: Option<&str>) -> Self {
        Self {
            region: region.map(|s| s.to_string()),
            profile: profile.map(|s| s.to_string()),
        }
    }

    // ── Public dispatch ──────────────────────────────────────────────

    /// Provision a managed resource. Dispatches to the appropriate `aws` command
    /// based on `resource_type`.
    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "rds_postgres" => self.create_rds_postgres(params),
            "vpc" => self.create_vpc(params),
            "aws_subnet" => self.create_aws_subnet(params),
            "security_group" => self.create_security_group(params),
            "sg_rule" => self.create_sg_rule(params),
            "eks_cluster" => self.create_eks_cluster(params),
            "eks_nodegroup" => self.create_eks_nodegroup(params),
            "eks_addon" => self.create_eks_addon(params),
            "s3_bucket" => self.create_s3_bucket(params),
            "kms_key" => self.create_kms_key(params),
            "elasticache_redis" => self.create_elasticache_redis(params),
            "elasticache_replication_group" => self.create_elasticache_replication_group(params),
            "msk_cluster" => self.create_msk_cluster(params),
            "iam_role" => self.create_iam_role(params),
            "iam_policy" => self.create_iam_policy(params),
            "vpc_endpoint" => self.create_vpc_endpoint(params),
            "nat_gateway" => self.create_nat_gateway(params),
            "acm_certificate" => self.create_acm_certificate(params),
            "cloudwatch_alarm" => self.create_cloudwatch_alarm(params),
            "ses_domain" => self.create_ses_domain(params),
            "ses_smtp_user" => self.create_ses_smtp_user(params),
            "backup_vault" => self.create_backup_vault(params),
            "backup_plan" => self.create_backup_plan(params),
            "ecs_cluster" => self.create_ecs_cluster(params),
            "ecs_service" => self.create_ecs_service(params),
            "ecs_task_definition" => self.create_ecs_task_definition(params),
            "ecr_repository" => self.create_ecr_repository(params),
            "alb" => self.create_alb(params),
            "alb_target_group" => self.create_alb_target_group(params),
            "alb_listener" => self.create_alb_listener(params),
            "cloudfront_distribution" => self.create_cloudfront_distribution(params),
            "route53_zone" => self.create_route53_zone(params),
            "route53_record" => self.create_route53_record(params),
            "secrets_manager_secret" => self.create_secrets_manager_secret(params),
            "internet_gateway" => self.create_internet_gateway(params),
            "route_table" => self.create_route_table(params),
            "route_table_association" => self.create_route_table_association(params),
            "route" => self.create_route(params),
            "db_subnet_group" => self.create_db_subnet_group(params),
            "cache_subnet_group" => self.create_cache_subnet_group(params),
            "iam_policy_attachment" => self.create_iam_policy_attachment(params),
            "kms_alias" => self.create_kms_alias(params),
            "cloudwatch_log_group" => self.create_cloudwatch_log_group(params),
            "sns_subscription" => self.create_sns_subscription(params),
            "vpc_flow_log" => self.create_vpc_flow_log(params),
            other => Err(format!("unsupported AWS resource type: {other}").into()),
        }
    }

    /// Delete a managed resource.
    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), ProvisionError> {
        match resource_type {
            "rds_postgres" => {
                self.run_aws(&[
                    "rds", "delete-db-instance",
                    "--db-instance-identifier", id,
                    "--skip-final-snapshot",
                ])?;
                Ok(())
            }
            "vpc" => {
                self.run_aws(&["ec2", "delete-vpc", "--vpc-id", id])?;
                Ok(())
            }
            "aws_subnet" => {
                self.run_aws(&["ec2", "delete-subnet", "--subnet-id", id])?;
                Ok(())
            }
            "security_group" => {
                self.run_aws(&["ec2", "delete-security-group", "--group-id", id])?;
                Ok(())
            }
            "sg_rule" => {
                Err("sg_rule deletion requires params (group_id, protocol, port, cidr); use delete_with_params()".into())
            }
            "eks_cluster" => {
                self.run_aws(&["eks", "delete-cluster", "--name", id])?;
                Ok(())
            }
            "eks_nodegroup" => {
                // eks_nodegroup deletion requires cluster name; use delete_with_params()
                Err("eks_nodegroup deletion requires params (cluster); use delete_with_params()".into())
            }
            "eks_addon" => {
                // eks_addon deletion requires cluster name; use delete_with_params()
                Err("eks_addon deletion requires params (cluster); use delete_with_params()".into())
            }
            "s3_bucket" => {
                self.run_aws(&["s3api", "delete-bucket", "--bucket", id])?;
                Ok(())
            }
            "elasticache_redis" => {
                self.run_aws(&[
                    "elasticache", "delete-cache-cluster",
                    "--cache-cluster-id", id,
                ])?;
                Ok(())
            }
            "elasticache_replication_group" => {
                self.run_aws(&[
                    "elasticache", "delete-replication-group",
                    "--replication-group-id", id,
                ])?;
                Ok(())
            }
            "msk_cluster" => {
                self.run_aws(&[
                    "kafka", "delete-cluster",
                    "--cluster-arn", id,
                ])?;
                Ok(())
            }
            "kms_key" => {
                self.run_aws(&[
                    "kms", "schedule-key-deletion",
                    "--key-id", id,
                    "--pending-window-in-days", "7",
                ])?;
                Ok(())
            }
            "iam_role" => {
                self.run_aws(&["iam", "delete-role", "--role-name", id])?;
                Ok(())
            }
            "iam_policy" => {
                self.run_aws(&["iam", "delete-policy", "--policy-arn", id])?;
                Ok(())
            }
            "vpc_endpoint" => {
                self.run_aws(&["ec2", "delete-vpc-endpoints", "--vpc-endpoint-ids", id])?;
                Ok(())
            }
            "nat_gateway" => {
                self.run_aws(&["ec2", "delete-nat-gateway", "--nat-gateway-id", id])?;
                Ok(())
            }
            "acm_certificate" => {
                self.run_aws(&["acm", "delete-certificate", "--certificate-arn", id])?;
                Ok(())
            }
            "cloudwatch_alarm" => {
                self.run_aws(&["cloudwatch", "delete-alarms", "--alarm-names", id])?;
                Ok(())
            }
            "ses_domain" => {
                self.run_aws(&["ses", "delete-identity", "--identity", id])?;
                Ok(())
            }
            "ses_smtp_user" => {
                self.run_aws(&["iam", "delete-user", "--user-name", id])?;
                Ok(())
            }
            "backup_vault" => {
                self.run_aws(&[
                    "backup", "delete-backup-vault",
                    "--backup-vault-name", id,
                ])?;
                Ok(())
            }
            "backup_plan" => {
                self.run_aws(&[
                    "backup", "delete-backup-plan",
                    "--backup-plan-id", id,
                ])?;
                Ok(())
            }
            "ecs_cluster" => {
                self.run_aws(&["ecs", "delete-cluster", "--cluster", id])?;
                Ok(())
            }
            "ecs_service" => {
                Err("ecs_service deletion requires params (cluster); use delete_with_params()".into())
            }
            "ecs_task_definition" => {
                self.run_aws(&[
                    "ecs", "deregister-task-definition",
                    "--task-definition", id,
                ])?;
                Ok(())
            }
            "ecr_repository" => {
                self.run_aws(&[
                    "ecr", "delete-repository",
                    "--repository-name", id,
                    "--force",
                ])?;
                Ok(())
            }
            "alb" => {
                self.run_aws(&[
                    "elbv2", "delete-load-balancer",
                    "--load-balancer-arn", id,
                ])?;
                Ok(())
            }
            "alb_target_group" => {
                self.run_aws(&[
                    "elbv2", "delete-target-group",
                    "--target-group-arn", id,
                ])?;
                Ok(())
            }
            "alb_listener" => {
                self.run_aws(&[
                    "elbv2", "delete-listener",
                    "--listener-arn", id,
                ])?;
                Ok(())
            }
            "cloudfront_distribution" => {
                // CloudFront distributions must be disabled before deletion.
                // Caller should disable first; we attempt delete directly.
                self.run_aws(&[
                    "cloudfront", "delete-distribution",
                    "--id", id,
                ])?;
                Ok(())
            }
            "route53_zone" => {
                self.run_aws(&["route53", "delete-hosted-zone", "--id", id])?;
                Ok(())
            }
            "route53_record" => {
                Err("route53_record deletion requires params (zone_id, record_type, value, ttl); use delete_with_params()".into())
            }
            "secrets_manager_secret" => {
                self.run_aws(&[
                    "secretsmanager", "delete-secret",
                    "--secret-id", id,
                    "--force-delete-without-recovery",
                ])?;
                Ok(())
            }
            "internet_gateway" => {
                // Detach from VPC before deleting (AWS requires this)
                if let Ok(desc) = self.run_aws(&[
                    "ec2", "describe-internet-gateways",
                    "--internet-gateway-ids", id,
                ]) {
                    if let Some(attachments) = desc
                        .get("InternetGateways")
                        .and_then(|v| v.as_array())
                        .and_then(|a| a.first())
                        .and_then(|igw| igw.get("Attachments"))
                        .and_then(|v| v.as_array())
                    {
                        for att in attachments {
                            if let Some(vpc_id) = att.get("VpcId").and_then(|v| v.as_str()) {
                                let _ = self.run_aws(&[
                                    "ec2", "detach-internet-gateway",
                                    "--internet-gateway-id", id,
                                    "--vpc-id", vpc_id,
                                ]);
                            }
                        }
                    }
                }
                self.run_aws(&["ec2", "delete-internet-gateway", "--internet-gateway-id", id])?;
                Ok(())
            }
            "route_table" => {
                self.run_aws(&["ec2", "delete-route-table", "--route-table-id", id])?;
                Ok(())
            }
            "route_table_association" => {
                self.run_aws(&["ec2", "disassociate-route-table", "--association-id", id])?;
                Ok(())
            }
            "route" => {
                // Route deletion requires route_table_id and destination_cidr via delete_with_params
                Err("route deletion requires params (route_table_id, destination_cidr); use delete_with_params()".into())
            }
            "db_subnet_group" => {
                self.run_aws(&["rds", "delete-db-subnet-group", "--db-subnet-group-name", id])?;
                Ok(())
            }
            "cache_subnet_group" => {
                self.run_aws(&[
                    "elasticache", "delete-cache-subnet-group",
                    "--cache-subnet-group-name", id,
                ])?;
                Ok(())
            }
            "iam_policy_attachment" => {
                // id format: "role_name::policy_arn" — need delete_with_params
                Err("iam_policy_attachment deletion requires params (role_name, policy_arn); use delete_with_params()".into())
            }
            "kms_alias" => {
                let alias_name = if id.starts_with("alias/") {
                    id.to_string()
                } else {
                    format!("alias/{id}")
                };
                self.run_aws(&["kms", "delete-alias", "--alias-name", &alias_name])?;
                Ok(())
            }
            "cloudwatch_log_group" => {
                self.run_aws(&[
                    "logs", "delete-log-group",
                    "--log-group-name", id,
                ])?;
                Ok(())
            }
            "sns_subscription" => {
                self.run_aws(&["sns", "unsubscribe", "--subscription-arn", id])?;
                Ok(())
            }
            "vpc_flow_log" => {
                self.run_aws(&["ec2", "delete-flow-logs", "--flow-log-ids", id])?;
                Ok(())
            }
            other => Err(format!("unsupported AWS resource type for delete: {other}").into()),
        }
    }

    /// Delete a sub-resource that requires extra context (e.g. sg_rule needs group_id).
    pub fn delete_with_params(
        &self,
        resource_type: &str,
        _id: &str,
        params: &Value,
    ) -> Result<(), ProvisionError> {
        match resource_type {
            "sg_rule" => {
                let group_id = param_str(params, "security_group_id")?;
                let protocol = param_str(params, "protocol")?;
                let port = param_str(params, "port")?;
                let cidr = param_str(params, "cidr")?;
                self.run_aws(&[
                    "ec2",
                    "revoke-security-group-ingress",
                    "--group-id",
                    &group_id,
                    "--protocol",
                    &protocol,
                    "--port",
                    &port,
                    "--cidr",
                    &cidr,
                ])?;
                Ok(())
            }
            "eks_nodegroup" => {
                let cluster = param_str(params, "cluster")?;
                self.run_aws(&[
                    "eks",
                    "delete-nodegroup",
                    "--cluster-name",
                    &cluster,
                    "--nodegroup-name",
                    _id,
                ])?;
                Ok(())
            }
            "eks_addon" => {
                let cluster = param_str(params, "cluster")?;
                self.run_aws(&[
                    "eks",
                    "delete-addon",
                    "--cluster-name",
                    &cluster,
                    "--addon-name",
                    _id,
                ])?;
                Ok(())
            }
            "ecs_service" => {
                let cluster = param_str(params, "cluster")?;
                self.run_aws(&[
                    "ecs",
                    "delete-service",
                    "--cluster",
                    &cluster,
                    "--service",
                    _id,
                    "--force",
                ])?;
                Ok(())
            }
            "route53_record" => {
                let zone_id = param_str(params, "zone_id")?;
                let record_type = param_str_or(params, "record_type", "A");
                let value = param_str(params, "value")?;
                let ttl = param_str_or(params, "ttl", "300");
                let change_batch = format!(
                    r#"{{"Changes":[{{"Action":"DELETE","ResourceRecordSet":{{"Name":"{}","Type":"{}","TTL":{},"ResourceRecords":[{{"Value":"{}"}}]}}}}]}}"#,
                    _id, record_type, ttl, value
                );
                self.run_aws(&[
                    "route53",
                    "change-resource-record-sets",
                    "--hosted-zone-id",
                    &zone_id,
                    "--change-batch",
                    &change_batch,
                ])?;
                Ok(())
            }
            other => self.delete(other, _id),
        }
    }

    // ── Build args (for testing without execution) ───────────────────

    /// Build the `aws` argument list that `create()` would use, WITHOUT executing.
    pub fn build_create_args(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let raw = match resource_type {
            "rds_postgres" => self.build_rds_postgres_args(params)?,
            "vpc" => self.build_vpc_args(params)?,
            "aws_subnet" => self.build_aws_subnet_args(params)?,
            "security_group" => self.build_security_group_args(params)?,
            "sg_rule" => self.build_sg_rule_args(params)?,
            "eks_cluster" => self.build_eks_cluster_args(params)?,
            "eks_nodegroup" => self.build_eks_nodegroup_args(params)?,
            "eks_addon" => self.build_eks_addon_args(params)?,
            "s3_bucket" => self.build_s3_bucket_args(params)?,
            "kms_key" => self.build_kms_key_args(params)?,
            "elasticache_redis" => self.build_elasticache_redis_args(params)?,
            "elasticache_replication_group" => {
                self.build_elasticache_replication_group_args(params)?
            }
            "msk_cluster" => self.build_msk_cluster_args(params)?,
            "iam_role" => self.build_iam_role_args(params)?,
            "iam_policy" => self.build_iam_policy_args(params)?,
            "vpc_endpoint" => self.build_vpc_endpoint_args(params)?,
            "nat_gateway" => self.build_nat_gateway_args(params)?,
            "acm_certificate" => self.build_acm_certificate_args(params)?,
            "cloudwatch_alarm" => self.build_cloudwatch_alarm_args(params)?,
            "ses_domain" => self.build_ses_domain_args(params)?,
            "ses_smtp_user" => self.build_ses_smtp_user_args(params)?,
            "backup_vault" => self.build_backup_vault_args(params)?,
            "backup_plan" => self.build_backup_plan_args(params)?,
            "ecs_cluster" => self.build_ecs_cluster_args(params)?,
            "ecs_service" => self.build_ecs_service_args(params)?,
            "ecs_task_definition" => self.build_ecs_task_definition_args(params)?,
            "ecr_repository" => self.build_ecr_repository_args(params)?,
            "alb" => self.build_alb_args(params)?,
            "alb_target_group" => self.build_alb_target_group_args(params)?,
            "alb_listener" => self.build_alb_listener_args(params)?,
            "cloudfront_distribution" => self.build_cloudfront_distribution_args(params)?,
            "route53_zone" => self.build_route53_zone_args(params)?,
            "route53_record" => self.build_route53_record_args(params)?,
            "secrets_manager_secret" => self.build_secrets_manager_secret_args(params)?,
            // Types added in v0.8.1 — inline arg construction for EXPLAIN
            "internet_gateway" => {
                let vpc_id = param_str(params, "vpc_id")?;
                vec!["ec2".into(), "create-internet-gateway".into(),
                     "&&".into(), "ec2".into(), "attach-internet-gateway".into(),
                     "--vpc-id".into(), vpc_id]
            }
            "route_table" => {
                let vpc_id = param_str(params, "vpc_id")?;
                vec!["ec2".into(), "create-route-table".into(), "--vpc-id".into(), vpc_id]
            }
            "route_table_association" => {
                let rt_id = param_str(params, "route_table_id")?;
                let subnet_id = param_str(params, "subnet_id")?;
                vec!["ec2".into(), "associate-route-table".into(),
                     "--route-table-id".into(), rt_id, "--subnet-id".into(), subnet_id]
            }
            "route" => {
                let rt_id = param_str(params, "route_table_id")?;
                let dest = param_str(params, "destination_cidr")?;
                vec!["ec2".into(), "create-route".into(),
                     "--route-table-id".into(), rt_id, "--destination-cidr-block".into(), dest]
            }
            "db_subnet_group" => {
                let name = param_str(params, "id")?;
                let subnets = param_str(params, "subnet_ids")?;
                vec!["rds".into(), "create-db-subnet-group".into(),
                     "--db-subnet-group-name".into(), name, "--subnet-ids".into(), subnets]
            }
            "cache_subnet_group" => {
                let name = param_str(params, "id")?;
                let subnets = param_str(params, "subnet_ids")?;
                vec!["elasticache".into(), "create-cache-subnet-group".into(),
                     "--cache-subnet-group-name".into(), name, "--subnet-ids".into(), subnets]
            }
            "iam_policy_attachment" => {
                let role = param_str(params, "role_name")?;
                let policy = param_str(params, "policy_arn")?;
                vec!["iam".into(), "attach-role-policy".into(),
                     "--role-name".into(), role, "--policy-arn".into(), policy]
            }
            "kms_alias" => {
                let id = param_str(params, "id")?;
                let key = param_str(params, "target_key_id")?;
                let alias = if id.starts_with("alias/") { id } else { format!("alias/{id}") };
                vec!["kms".into(), "create-alias".into(),
                     "--alias-name".into(), alias, "--target-key-id".into(), key]
            }
            "cloudwatch_log_group" => {
                let id = param_str(params, "id")?;
                vec!["logs".into(), "create-log-group".into(), "--log-group-name".into(), id]
            }
            other => return Err(format!("unsupported AWS resource type: {other}").into()),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `aws` argument list that `delete()` would use, WITHOUT executing.
    pub fn build_delete_args(
        &self,
        resource_type: &str,
        id: &str,
    ) -> Result<Vec<String>, ProvisionError> {
        let base: Vec<&str> = match resource_type {
            "rds_postgres" => vec![
                "rds",
                "delete-db-instance",
                "--db-instance-identifier",
                id,
                "--skip-final-snapshot",
            ],
            "vpc" => vec!["ec2", "delete-vpc", "--vpc-id", id],
            "aws_subnet" => vec!["ec2", "delete-subnet", "--subnet-id", id],
            "security_group" => vec!["ec2", "delete-security-group", "--group-id", id],
            "sg_rule" => {
                return Err(
                    "sg_rule deletion requires params; use build_delete_args_with_params()".into(),
                );
            }
            "eks_cluster" => vec!["eks", "delete-cluster", "--name", id],
            "eks_nodegroup" => {
                // eks_nodegroup deletion requires cluster param from registry
                return Err(
                    "eks_nodegroup deletion requires params (cluster); use delete_with_params()"
                        .into(),
                );
            }
            "eks_addon" => {
                // eks_addon deletion requires cluster param from registry
                return Err(
                    "eks_addon deletion requires params (cluster); use delete_with_params()".into(),
                );
            }
            "s3_bucket" => vec!["s3api", "delete-bucket", "--bucket", id],
            "elasticache_redis" => vec![
                "elasticache",
                "delete-cache-cluster",
                "--cache-cluster-id",
                id,
            ],
            "elasticache_replication_group" => vec![
                "elasticache",
                "delete-replication-group",
                "--replication-group-id",
                id,
            ],
            "msk_cluster" => vec!["kafka", "delete-cluster", "--cluster-arn", id],
            "kms_key" => vec![
                "kms",
                "schedule-key-deletion",
                "--key-id",
                id,
                "--pending-window-in-days",
                "7",
            ],
            "iam_role" => vec!["iam", "delete-role", "--role-name", id],
            "iam_policy" => vec!["iam", "delete-policy", "--policy-arn", id],
            "vpc_endpoint" => vec!["ec2", "delete-vpc-endpoints", "--vpc-endpoint-ids", id],
            "nat_gateway" => vec!["ec2", "delete-nat-gateway", "--nat-gateway-id", id],
            "acm_certificate" => vec!["acm", "delete-certificate", "--certificate-arn", id],
            "cloudwatch_alarm" => vec!["cloudwatch", "delete-alarms", "--alarm-names", id],
            "ses_domain" => vec!["ses", "delete-identity", "--identity", id],
            "ses_smtp_user" => vec!["iam", "delete-user", "--user-name", id],
            "backup_vault" => vec!["backup", "delete-backup-vault", "--backup-vault-name", id],
            "backup_plan" => vec!["backup", "delete-backup-plan", "--backup-plan-id", id],
            "ecs_cluster" => vec!["ecs", "delete-cluster", "--cluster", id],
            "ecs_service" => {
                return Err(
                    "ecs_service deletion requires params (cluster); use delete_with_params()"
                        .into(),
                );
            }
            "ecs_task_definition" => {
                vec!["ecs", "deregister-task-definition", "--task-definition", id]
            }
            "ecr_repository" => vec![
                "ecr",
                "delete-repository",
                "--repository-name",
                id,
                "--force",
            ],
            "alb" => vec!["elbv2", "delete-load-balancer", "--load-balancer-arn", id],
            "alb_target_group" => vec!["elbv2", "delete-target-group", "--target-group-arn", id],
            "alb_listener" => vec!["elbv2", "delete-listener", "--listener-arn", id],
            "cloudfront_distribution" => vec!["cloudfront", "delete-distribution", "--id", id],
            "route53_zone" => vec!["route53", "delete-hosted-zone", "--id", id],
            "route53_record" => {
                return Err(
                    "route53_record deletion requires params (zone_id); use delete_with_params()"
                        .into(),
                );
            }
            "secrets_manager_secret" => vec![
                "secretsmanager",
                "delete-secret",
                "--secret-id",
                id,
                "--force-delete-without-recovery",
            ],
            other => {
                return Err(format!("unsupported AWS resource type for delete: {other}").into())
            }
        };
        Ok(self.build_args(&base))
    }

    // ── Generic runner ───────────────────────────────────────────────

    /// Run an `aws` command and return JSON output.
    fn run_aws(&self, args: &[&str]) -> Result<Value, ProvisionError> {
        let mut cmd = Command::new("aws");
        for arg in args {
            cmd.arg(arg);
        }
        cmd.arg("--output").arg("json");
        // Prevent AWS CLI v2 from piping through `less` in non-TTY
        // contexts (which would hang the process).
        cmd.arg("--no-cli-pager");

        if let Some(ref region) = self.region {
            cmd.arg("--region").arg(region);
        }
        if let Some(ref profile) = self.profile {
            cmd.arg("--profile").arg(profile);
        }
        // Also inject AWS_PAGER="" as env to double-guard against pager
        cmd.env("AWS_PAGER", "");
        // Ensure UTF-8 locale so non-ASCII characters in descriptions,
        // tags, and other user-supplied values are handled correctly.
        cmd.env("LC_ALL", "en_US.UTF-8");

        let output = cmd
            .output()
            .map_err(|e| ProvisionError::from(format!("failed to run aws: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("aws command failed: {stderr}").into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| ProvisionError::from(format!("failed to parse aws JSON output: {e}")))
    }

    /// Build the argument list that `run_aws` would use (for testing without
    /// execution).
    fn build_args(&self, args: &[&str]) -> Vec<String> {
        let mut result: Vec<String> = vec!["aws".to_string()];
        for arg in args {
            result.push(arg.to_string());
        }
        result.push("--output".to_string());
        result.push("json".to_string());
        if let Some(ref region) = self.region {
            result.push("--region".to_string());
            result.push(region.clone());
        }
        if let Some(ref profile) = self.profile {
            result.push("--profile".to_string());
            result.push(profile.clone());
        }
        result
    }

    /// Tag a resource with a Name tag. Uses JSON tag syntax which is safe
    /// for non-ASCII characters, commas, equals signs, and other special
    /// characters that break the `Key=k,Value=v` shorthand.
    fn tag_resource(&self, resource_id: &str, name: &str) {
        let tag_json = serde_json::json!([{"Key": "Name", "Value": name}]).to_string();
        let _ = self.run_aws(&[
            "ec2",
            "create-tags",
            "--resources",
            resource_id,
            "--tags",
            &tag_json,
        ]);
    }

    // ── Discovery ────────────────────────────────────────────────────

    /// Discover all supported AWS resources in the configured region/profile.
    ///
    /// Shells out to `aws` CLI commands, parses JSON output, and returns a
    /// flat `Vec` of normalised resource objects.  If a particular resource
    /// type cannot be listed (e.g. insufficient permissions), it is silently
    /// skipped.
    pub fn discover(&self) -> Result<Vec<Value>, ProvisionError> {
        let mut resources: Vec<Value> = Vec::new();

        let collectors: &[DiscoverCollector] = &[
            ("ec2", Self::discover_ec2),
            ("rds_postgres", Self::discover_rds_postgres),
            ("vpc", Self::discover_vpcs),
            ("aws_subnet", Self::discover_subnets),
            ("security_group", Self::discover_security_groups),
            ("s3_bucket", Self::discover_s3_buckets),
            ("lambda", Self::discover_lambda),
            ("elb", Self::discover_elbs),
            ("eks_cluster", Self::discover_eks_clusters),
            ("elasticache_redis", Self::discover_elasticache),
            ("msk_cluster", Self::discover_msk_clusters),
            ("iam_role", Self::discover_iam_roles),
            ("vpc_endpoint", Self::discover_vpc_endpoints),
            ("nat_gateway", Self::discover_nat_gateways),
            ("internet_gateway", Self::discover_internet_gateways),
            ("route_table", Self::discover_route_tables),
            ("db_subnet_group", Self::discover_db_subnet_groups),
            ("cache_subnet_group", Self::discover_cache_subnet_groups),
            ("kms_key", Self::discover_kms_keys),
            ("acm_certificate", Self::discover_acm_certificates),
            ("backup_vault", Self::discover_backup_vaults),
            ("backup_plan", Self::discover_backup_plans),
            ("ses_domain", Self::discover_ses_domains),
            ("cloudwatch_alarm", Self::discover_cloudwatch_alarms),
            ("ecs_cluster", Self::discover_ecs_clusters),
            ("ecr_repository", Self::discover_ecr_repositories),
            ("alb", Self::discover_albs),
            ("route53_zone", Self::discover_route53_zones),
            (
                "secrets_manager_secret",
                Self::discover_secrets_manager_secrets,
            ),
        ];

        for (_, collector) in collectors {
            resources.extend(collector(self));
        }

        Ok(resources)
    }

    // ── Per-resource discovery helpers ───────────────────────────────

    /// Helper: run an aws command for discovery, returning parsed output
    /// or a diagnostic error entry that surfaces in the import results.
    fn discover_run(&self, resource_type: &str, args: &[&str]) -> Result<Value, Vec<Value>> {
        // Log the exact command being run for debugging
        let cmd_str = self.build_args(args).join(" ");
        debug!(provider = "aws", resource_type, cmd = %cmd_str, "discover running");

        match self.run_aws(args) {
            Ok(v) => {
                // Log how many top-level items were found
                let count = match &v {
                    Value::Object(obj) => obj
                        .values()
                        .filter_map(|v| v.as_array())
                        .map(|a| a.len())
                        .next()
                        .unwrap_or(0),
                    _ => 0,
                };
                debug!(provider = "aws", resource_type, count, "discover completed");
                Ok(v)
            }
            Err(e) => {
                error!(provider = "aws", resource_type, error = %e, "discover failed");
                // Surface the error as a diagnostic row so it's visible
                // in the import summary instead of silently returning 0.
                Err(vec![serde_json::json!({
                    "id": format!("_discover_error_{resource_type}"),
                    "resource_type": resource_type,
                    "name": format!("discover error: {resource_type}"),
                    "config": { "error": e.to_string() },
                    "outputs": {},
                })])
            }
        }
    }

    fn discover_ec2(&self) -> Vec<Value> {
        let output = match self.discover_run("ec2", &["ec2", "describe-instances"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_ec2_instances(&output)
    }

    fn parse_ec2_instances(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let reservations = match output.get("Reservations").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for reservation in reservations {
            let instances = match reservation.get("Instances").and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };
            for inst in instances {
                let id = inst
                    .get("InstanceId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let name = extract_name_from_tags(inst).unwrap_or_else(|| id.clone());

                let config = serde_json::json!({
                    "instance_type": inst.get("InstanceType"),
                    "vpc_id": inst.get("VpcId"),
                    "subnet_id": inst.get("SubnetId"),
                    "tags": inst.get("Tags"),
                });
                let outputs = serde_json::json!({
                    "state": inst.get("State").and_then(|s| s.get("Name")),
                    "public_ip": inst.get("PublicIpAddress"),
                    "private_ip": inst.get("PrivateIpAddress"),
                });

                results.push(serde_json::json!({
                    "id": id,
                    "resource_type": "ec2",
                    "name": name,
                    "config": config,
                    "outputs": outputs,
                }));
            }
        }
        results
    }

    fn discover_rds_postgres(&self) -> Vec<Value> {
        let output = match self.discover_run("rds_postgres", &["rds", "describe-db-instances"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_rds_instances(&output)
    }

    fn parse_rds_instances(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let instances = match output.get("DBInstances").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for inst in instances {
            let id = inst
                .get("DBInstanceIdentifier")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "engine": inst.get("Engine"),
                "engine_version": inst.get("EngineVersion"),
                "instance_class": inst.get("DBInstanceClass"),
                "allocated_storage": inst.get("AllocatedStorage"),
                "multi_az": inst.get("MultiAZ"),
            });
            let outputs = serde_json::json!({
                "endpoint": inst.get("Endpoint").and_then(|e| e.get("Address")),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "rds_postgres",
                "name": id,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_vpcs(&self) -> Vec<Value> {
        let output = match self.discover_run("vpc", &["ec2", "describe-vpcs"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_vpcs(&output)
    }

    fn parse_vpcs(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let vpcs = match output.get("Vpcs").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for vpc in vpcs {
            let id = vpc
                .get("VpcId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(vpc).unwrap_or_else(|| id.clone());

            let config = serde_json::json!({
                "cidr_block": vpc.get("CidrBlock"),
                "tags": vpc.get("Tags"),
            });
            let outputs = serde_json::json!({
                "state": vpc.get("State"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "vpc",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_subnets(&self) -> Vec<Value> {
        let output = match self.discover_run("aws_subnet", &["ec2", "describe-subnets"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_subnets(&output)
    }

    fn parse_subnets(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let subnets = match output.get("Subnets").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for subnet in subnets {
            let id = subnet
                .get("SubnetId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(subnet).unwrap_or_else(|| id.clone());

            let config = serde_json::json!({
                "vpc_id": subnet.get("VpcId"),
                "cidr_block": subnet.get("CidrBlock"),
                "availability_zone": subnet.get("AvailabilityZone"),
                "tags": subnet.get("Tags"),
            });
            let outputs = serde_json::json!({});

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "aws_subnet",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_security_groups(&self) -> Vec<Value> {
        let output = match self.discover_run("security_group", &["ec2", "describe-security-groups"])
        {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_security_groups(&output)
    }

    fn parse_security_groups(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let groups = match output.get("SecurityGroups").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for sg in groups {
            let id = sg
                .get("GroupId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = sg
                .get("GroupName")
                .and_then(|v| v.as_str())
                .unwrap_or(&id)
                .to_string();

            let config = serde_json::json!({
                "group_name": sg.get("GroupName"),
                "vpc_id": sg.get("VpcId"),
                "description": sg.get("Description"),
            });
            let outputs = serde_json::json!({});

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "security_group",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_s3_buckets(&self) -> Vec<Value> {
        let output = match self.discover_run("s3_bucket", &["s3api", "list-buckets"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_s3_buckets(&output)
    }

    fn parse_s3_buckets(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let buckets = match output.get("Buckets").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for bucket in buckets {
            let name = bucket
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({});
            let outputs = serde_json::json!({
                "creation_date": bucket.get("CreationDate"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "s3_bucket",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_lambda(&self) -> Vec<Value> {
        let output = match self.discover_run("lambda", &["lambda", "list-functions"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_lambda_functions(&output)
    }

    fn parse_lambda_functions(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let functions = match output.get("Functions").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for func in functions {
            let name = func
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "runtime": func.get("Runtime"),
                "handler": func.get("Handler"),
                "memory_size": func.get("MemorySize"),
                "timeout": func.get("Timeout"),
            });
            let outputs = serde_json::json!({
                "last_modified": func.get("LastModified"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "lambda",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_elbs(&self) -> Vec<Value> {
        let output = match self.discover_run("elb", &["elbv2", "describe-load-balancers"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_elbs(&output)
    }

    fn parse_elbs(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let lbs = match output.get("LoadBalancers").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for lb in lbs {
            let arn = lb
                .get("LoadBalancerArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = lb
                .get("LoadBalancerName")
                .and_then(|v| v.as_str())
                .unwrap_or(&arn)
                .to_string();

            let config = serde_json::json!({
                "type": lb.get("Type"),
                "scheme": lb.get("Scheme"),
                "vpc_id": lb.get("VpcId"),
                "availability_zones": lb.get("AvailabilityZones"),
            });
            let outputs = serde_json::json!({
                "dns_name": lb.get("DNSName"),
                "state": lb.get("State").and_then(|s| s.get("Code")),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "elb",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_eks_clusters(&self) -> Vec<Value> {
        let output = match self.discover_run("eks_cluster", &["eks", "list-clusters"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };

        let mut results = Vec::new();
        let clusters = match output.get("clusters").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };

        for cluster_name in clusters {
            let name = match cluster_name.as_str() {
                Some(n) => n,
                None => continue,
            };
            // Describe each cluster to get full details
            let detail = match self.run_aws(&["eks", "describe-cluster", "--name", name]) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let cluster = match detail.get("cluster") {
                Some(c) => c,
                None => continue,
            };

            let config = serde_json::json!({
                "version": cluster.get("version"),
                "role_arn": cluster.get("roleArn"),
                "platform_version": cluster.get("platformVersion"),
                "vpc_config": cluster.get("resourcesVpcConfig"),
            });
            let outputs = serde_json::json!({
                "endpoint": cluster.get("endpoint"),
                "status": cluster.get("status"),
                "certificate_authority": cluster
                    .get("certificateAuthority")
                    .and_then(|ca| ca.get("data")),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "eks_cluster",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));

            // Discover nodegroups for this cluster
            if let Ok(ng_output) = self.run_aws(&["eks", "list-nodegroups", "--cluster-name", name])
            {
                results.extend(Self::parse_eks_nodegroups(&ng_output, name));
            }

            // Discover addons for this cluster
            if let Ok(addon_output) = self.run_aws(&["eks", "list-addons", "--cluster-name", name])
            {
                results.extend(Self::parse_eks_addons(&addon_output, name));
            }
        }
        results
    }

    fn parse_eks_nodegroups(output: &Value, cluster_name: &str) -> Vec<Value> {
        let mut results = Vec::new();
        let nodegroups = match output.get("nodegroups").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for ng in nodegroups {
            let name = match ng.as_str() {
                Some(n) => n,
                None => continue,
            };

            let config = serde_json::json!({
                "cluster": cluster_name,
            });
            let outputs = serde_json::json!({});

            results.push(serde_json::json!({
                "id": format!("{}/{}", cluster_name, name),
                "resource_type": "eks_nodegroup",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn parse_eks_addons(output: &Value, cluster_name: &str) -> Vec<Value> {
        let mut results = Vec::new();
        let addons = match output.get("addons").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for addon in addons {
            let name = match addon.as_str() {
                Some(n) => n,
                None => continue,
            };

            let config = serde_json::json!({
                "cluster": cluster_name,
            });
            let outputs = serde_json::json!({});

            results.push(serde_json::json!({
                "id": format!("{}/{}", cluster_name, name),
                "resource_type": "eks_addon",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_elasticache(&self) -> Vec<Value> {
        let output = match self.discover_run(
            "elasticache_redis",
            &[
                "elasticache",
                "describe-cache-clusters",
                "--show-cache-node-info",
            ],
        ) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_elasticache_clusters(&output)
    }

    fn parse_elasticache_clusters(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let clusters = match output.get("CacheClusters").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for cluster in clusters {
            let id = cluster
                .get("CacheClusterId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let engine = cluster
                .get("Engine")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let resource_type = if cluster.get("ReplicationGroupId").is_some() {
                "elasticache_replication_group"
            } else {
                "elasticache_redis"
            };

            let config = serde_json::json!({
                "engine": cluster.get("Engine"),
                "engine_version": cluster.get("EngineVersion"),
                "cache_node_type": cluster.get("CacheNodeType"),
                "num_cache_nodes": cluster.get("NumCacheNodes"),
                "replication_group_id": cluster.get("ReplicationGroupId"),
            });
            let outputs = serde_json::json!({
                "cache_cluster_id": &id,
                "status": cluster.get("CacheClusterStatus"),
                "engine": engine,
                "configuration_endpoint": cluster.get("ConfigurationEndpoint"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": resource_type,
                "name": id,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_msk_clusters(&self) -> Vec<Value> {
        let output = match self.discover_run("msk_cluster", &["kafka", "list-clusters-v2"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_msk_clusters(&output)
    }

    fn parse_msk_clusters(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let clusters = match output.get("ClusterInfoList").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for cluster in clusters {
            let arn = cluster
                .get("ClusterArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = cluster
                .get("ClusterName")
                .and_then(|v| v.as_str())
                .unwrap_or(&arn)
                .to_string();

            let config = serde_json::json!({
                "cluster_name": cluster.get("ClusterName"),
                "cluster_type": cluster.get("ClusterType"),
            });
            let outputs = serde_json::json!({
                "cluster_arn": &arn,
                "state": cluster.get("State"),
            });

            results.push(serde_json::json!({
                "id": arn,
                "resource_type": "msk_cluster",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_iam_roles(&self) -> Vec<Value> {
        let output = match self.discover_run("iam_role", &["iam", "list-roles"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_iam_roles(&output)
    }

    fn parse_iam_roles(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let roles = match output.get("Roles").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for role in roles {
            let name = role
                .get("RoleName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "path": role.get("Path"),
                "assume_role_policy_document": role.get("AssumeRolePolicyDocument"),
            });
            let outputs = serde_json::json!({
                "role_name": &name,
                "arn": role.get("Arn"),
                "create_date": role.get("CreateDate"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "iam_role",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_vpc_endpoints(&self) -> Vec<Value> {
        let output = match self.discover_run("vpc_endpoint", &["ec2", "describe-vpc-endpoints"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_vpc_endpoints(&output)
    }

    fn parse_vpc_endpoints(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let endpoints = match output.get("VpcEndpoints").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for ep in endpoints {
            let id = ep
                .get("VpcEndpointId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(ep).unwrap_or_else(|| id.clone());

            let config = serde_json::json!({
                "vpc_id": ep.get("VpcId"),
                "service_name": ep.get("ServiceName"),
                "vpc_endpoint_type": ep.get("VpcEndpointType"),
                "tags": ep.get("Tags"),
            });
            let outputs = serde_json::json!({
                "vpc_endpoint_id": &id,
                "service_name": ep.get("ServiceName"),
                "state": ep.get("State"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "vpc_endpoint",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    // ── Additional discovery helpers (v0.6.2–v0.6.3 resource types) ──

    fn discover_nat_gateways(&self) -> Vec<Value> {
        let output = match self.discover_run("nat_gateway", &["ec2", "describe-nat-gateways"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_nat_gateways(&output)
    }

    fn parse_nat_gateways(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let gateways = match output.get("NatGateways").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for gw in gateways {
            let id = gw
                .get("NatGatewayId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(gw).unwrap_or_else(|| id.clone());

            let config = serde_json::json!({
                "vpc_id": gw.get("VpcId"),
                "subnet_id": gw.get("SubnetId"),
                "connectivity_type": gw.get("ConnectivityType"),
                "tags": gw.get("Tags"),
            });
            let outputs = serde_json::json!({
                "state": gw.get("State"),
                "nat_gateway_addresses": gw.get("NatGatewayAddresses"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "nat_gateway",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_internet_gateways(&self) -> Vec<Value> {
        let output =
            match self.discover_run("internet_gateway", &["ec2", "describe-internet-gateways"]) {
                Ok(v) => v,
                Err(diag) => return diag,
            };
        Self::parse_internet_gateways(&output)
    }

    fn parse_internet_gateways(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let gateways = match output.get("InternetGateways").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for gw in gateways {
            let id = gw
                .get("InternetGatewayId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(gw).unwrap_or_else(|| id.clone());

            let attachments = gw.get("Attachments").and_then(|a| a.as_array());
            let vpc_id = attachments
                .and_then(|arr| arr.first())
                .and_then(|att| att.get("VpcId"));

            let config = serde_json::json!({
                "vpc_id": vpc_id,
                "tags": gw.get("Tags"),
            });
            let outputs = serde_json::json!({
                "attachments": gw.get("Attachments"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "internet_gateway",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_route_tables(&self) -> Vec<Value> {
        let output =
            match self.discover_run("route_table", &["ec2", "describe-route-tables"]) {
                Ok(v) => v,
                Err(diag) => return diag,
            };
        let mut results = Vec::new();
        let tables = match output.get("RouteTables").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for rt in tables {
            let id = rt
                .get("RouteTableId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = extract_name_from_tags(rt).unwrap_or_else(|| id.clone());

            let config = serde_json::json!({
                "vpc_id": rt.get("VpcId"),
                "tags": rt.get("Tags"),
            });
            let outputs = serde_json::json!({
                "routes": rt.get("Routes"),
                "associations": rt.get("Associations"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "route_table",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_db_subnet_groups(&self) -> Vec<Value> {
        let output =
            match self.discover_run("db_subnet_group", &["rds", "describe-db-subnet-groups"]) {
                Ok(v) => v,
                Err(diag) => return diag,
            };
        let mut results = Vec::new();
        let groups = match output.get("DBSubnetGroups").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for g in groups {
            let name = g
                .get("DBSubnetGroupName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let subnet_ids: Vec<&str> = g
                .get("Subnets")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.get("SubnetIdentifier").and_then(|v| v.as_str()))
                        .collect()
                })
                .unwrap_or_default();

            let config = serde_json::json!({
                "description": g.get("DBSubnetGroupDescription"),
                "subnet_ids": subnet_ids.join(" "),
                "vpc_id": g.get("VpcId"),
            });
            let outputs = serde_json::json!({
                "db_subnet_group_arn": g.get("DBSubnetGroupArn"),
                "subnets": g.get("Subnets"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "db_subnet_group",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_cache_subnet_groups(&self) -> Vec<Value> {
        let output = match self.discover_run(
            "cache_subnet_group",
            &["elasticache", "describe-cache-subnet-groups"],
        ) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        let mut results = Vec::new();
        let groups = match output.get("CacheSubnetGroups").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for g in groups {
            let name = g
                .get("CacheSubnetGroupName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let subnet_ids: Vec<&str> = g
                .get("Subnets")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|s| s.get("SubnetIdentifier").and_then(|v| v.as_str()))
                        .collect()
                })
                .unwrap_or_default();

            let config = serde_json::json!({
                "description": g.get("CacheSubnetGroupDescription"),
                "subnet_ids": subnet_ids.join(" "),
                "vpc_id": g.get("VpcId"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "cache_subnet_group",
                "name": name,
                "config": config,
            }));
        }
        results
    }

    fn discover_kms_keys(&self) -> Vec<Value> {
        let output = match self.discover_run("kms_key", &["kms", "list-keys"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_kms_keys(&output)
    }

    fn parse_kms_keys(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let keys = match output.get("Keys").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for key in keys {
            let id = key
                .get("KeyId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let arn = key
                .get("KeyArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({});
            let outputs = serde_json::json!({
                "key_arn": arn,
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "kms_key",
                "name": id,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_acm_certificates(&self) -> Vec<Value> {
        let output = match self.discover_run("acm_certificate", &["acm", "list-certificates"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_acm_certificates(&output)
    }

    fn parse_acm_certificates(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let certs = match output
            .get("CertificateSummaryList")
            .and_then(|v| v.as_array())
        {
            Some(arr) => arr,
            None => return results,
        };
        for cert in certs {
            let arn = cert
                .get("CertificateArn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let domain = cert
                .get("DomainName")
                .and_then(|v| v.as_str())
                .unwrap_or(&arn)
                .to_string();

            let config = serde_json::json!({
                "domain_name": cert.get("DomainName"),
                "type": cert.get("Type"),
            });
            let outputs = serde_json::json!({
                "certificate_arn": &arn,
                "status": cert.get("Status"),
            });

            results.push(serde_json::json!({
                "id": arn,
                "resource_type": "acm_certificate",
                "name": domain,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_backup_vaults(&self) -> Vec<Value> {
        let output = match self.discover_run("backup_vault", &["backup", "list-backup-vaults"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_backup_vaults(&output)
    }

    fn parse_backup_vaults(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let vaults = match output.get("BackupVaultList").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for vault in vaults {
            let name = vault
                .get("BackupVaultName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "encryption_key_arn": vault.get("EncryptionKeyArn"),
            });
            let outputs = serde_json::json!({
                "backup_vault_arn": vault.get("BackupVaultArn"),
                "creation_date": vault.get("CreationDate"),
                "number_of_recovery_points": vault.get("NumberOfRecoveryPoints"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "backup_vault",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_backup_plans(&self) -> Vec<Value> {
        let output = match self.discover_run("backup_plan", &["backup", "list-backup-plans"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_backup_plans(&output)
    }

    fn parse_backup_plans(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let plans = match output.get("BackupPlansList").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for plan in plans {
            let id = plan
                .get("BackupPlanId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = plan
                .get("BackupPlanName")
                .and_then(|v| v.as_str())
                .unwrap_or(&id)
                .to_string();

            let config = serde_json::json!({
                "backup_plan_name": plan.get("BackupPlanName"),
            });
            let outputs = serde_json::json!({
                "backup_plan_arn": plan.get("BackupPlanArn"),
                "backup_plan_id": &id,
                "creation_date": plan.get("CreationDate"),
                "version_id": plan.get("VersionId"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "backup_plan",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_ses_domains(&self) -> Vec<Value> {
        let output = match self.discover_run(
            "ses_domain",
            &["ses", "list-identities", "--identity-type", "Domain"],
        ) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_ses_domains(&output)
    }

    fn parse_ses_domains(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let identities = match output.get("Identities").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for identity in identities {
            let domain = match identity.as_str() {
                Some(s) => s.to_string(),
                None => continue,
            };

            let config = serde_json::json!({});
            let outputs = serde_json::json!({});

            results.push(serde_json::json!({
                "id": domain,
                "resource_type": "ses_domain",
                "name": domain,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_cloudwatch_alarms(&self) -> Vec<Value> {
        let output = match self.discover_run("cloudwatch_alarm", &["cloudwatch", "describe-alarms"])
        {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_cloudwatch_alarms(&output)
    }

    fn parse_cloudwatch_alarms(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let alarms = match output.get("MetricAlarms").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for alarm in alarms {
            let name = alarm
                .get("AlarmName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "metric_name": alarm.get("MetricName"),
                "namespace": alarm.get("Namespace"),
                "comparison_operator": alarm.get("ComparisonOperator"),
                "threshold": alarm.get("Threshold"),
                "period": alarm.get("Period"),
                "evaluation_periods": alarm.get("EvaluationPeriods"),
                "statistic": alarm.get("Statistic"),
            });
            let outputs = serde_json::json!({
                "alarm_arn": alarm.get("AlarmArn"),
                "state_value": alarm.get("StateValue"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "cloudwatch_alarm",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    // ── Additional discovery helpers (v0.7.0 resource types) ─────────

    fn discover_ecs_clusters(&self) -> Vec<Value> {
        let output = match self.discover_run("ecs_cluster", &["ecs", "list-clusters"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };

        let mut results = Vec::new();
        let arns = match output.get("clusterArns").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };

        if arns.is_empty() {
            return results;
        }

        // Describe all clusters in one call
        let arn_strs: Vec<&str> = arns.iter().filter_map(|a| a.as_str()).collect();
        let mut desc_args: Vec<&str> = vec!["ecs", "describe-clusters", "--clusters"];
        desc_args.extend(&arn_strs);

        let detail = match self.run_aws(&desc_args) {
            Ok(v) => v,
            Err(_) => return results,
        };

        let clusters = match detail.get("clusters").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };

        for cluster in clusters {
            let name = cluster
                .get("clusterName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "capacity_providers": cluster.get("capacityProviders"),
                "status": cluster.get("status"),
            });
            let outputs = serde_json::json!({
                "cluster_arn": cluster.get("clusterArn"),
                "status": cluster.get("status"),
                "active_services_count": cluster.get("activeServicesCount"),
                "running_tasks_count": cluster.get("runningTasksCount"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "ecs_cluster",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_ecr_repositories(&self) -> Vec<Value> {
        let output = match self.discover_run("ecr_repository", &["ecr", "describe-repositories"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_ecr_repositories(&output)
    }

    fn parse_ecr_repositories(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let repos = match output.get("repositories").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for repo in repos {
            let name = repo
                .get("repositoryName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "image_scanning": repo.get("imageScanningConfiguration"),
                "encryption": repo.get("encryptionConfiguration"),
            });
            let outputs = serde_json::json!({
                "repository_arn": repo.get("repositoryArn"),
                "repository_uri": repo.get("repositoryUri"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "ecr_repository",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_albs(&self) -> Vec<Value> {
        let output = match self.discover_run("alb", &["elbv2", "describe-load-balancers"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_albs(&output)
    }

    fn parse_albs(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let lbs = match output.get("LoadBalancers").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for lb in lbs {
            let lb_type = lb.get("Type").and_then(|v| v.as_str()).unwrap_or("");
            if lb_type != "application" {
                continue;
            }
            let name = lb
                .get("LoadBalancerName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "scheme": lb.get("Scheme"),
                "vpc_id": lb.get("VpcId"),
                "availability_zones": lb.get("AvailabilityZones"),
            });
            let outputs = serde_json::json!({
                "load_balancer_arn": lb.get("LoadBalancerArn"),
                "dns_name": lb.get("DNSName"),
                "hosted_zone_id": lb.get("CanonicalHostedZoneId"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "alb",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_route53_zones(&self) -> Vec<Value> {
        let output = match self.discover_run("route53_zone", &["route53", "list-hosted-zones"]) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_route53_zones(&output)
    }

    fn parse_route53_zones(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let zones = match output.get("HostedZones").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for zone in zones {
            let id = zone
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = zone
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or(&id)
                .to_string();

            let config = serde_json::json!({
                "name": zone.get("Name"),
                "private_zone": zone.get("Config").and_then(|c| c.get("PrivateZone")),
            });
            let outputs = serde_json::json!({
                "hosted_zone_id": &id,
                "resource_record_set_count": zone.get("ResourceRecordSetCount"),
            });

            results.push(serde_json::json!({
                "id": id,
                "resource_type": "route53_zone",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    fn discover_secrets_manager_secrets(&self) -> Vec<Value> {
        let output = match self.discover_run(
            "secrets_manager_secret",
            &["secretsmanager", "list-secrets"],
        ) {
            Ok(v) => v,
            Err(diag) => return diag,
        };
        Self::parse_secrets_manager_secrets(&output)
    }

    fn parse_secrets_manager_secrets(output: &Value) -> Vec<Value> {
        let mut results = Vec::new();
        let secrets = match output.get("SecretList").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return results,
        };
        for secret in secrets {
            let name = secret
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let config = serde_json::json!({
                "description": secret.get("Description"),
            });
            let outputs = serde_json::json!({
                "arn": secret.get("ARN"),
                "name": &name,
                "last_changed_date": secret.get("LastChangedDate"),
            });

            results.push(serde_json::json!({
                "id": name,
                "resource_type": "secrets_manager_secret",
                "name": name,
                "config": config,
                "outputs": outputs,
            }));
        }
        results
    }

    // ── Per-resource create implementations ──────────────────────────

    /// Create an RDS PostgreSQL instance.
    ///
    /// Note: RDS creates are asynchronous. The instance will be in "creating"
    /// state immediately after this call returns.
    fn create_rds_postgres(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_rds_postgres_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "db_instance_identifier": result.get("DBInstance").and_then(|d| d.get("DBInstanceIdentifier")),
            "endpoint": result.get("DBInstance").and_then(|d| d.get("Endpoint")),
            "status": result.get("DBInstance").and_then(|d| d.get("DBInstanceStatus")),
            "engine": result.get("DBInstance").and_then(|d| d.get("Engine")),
            "note": "RDS creates are asynchronous. Instance will be in 'creating' state.",
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_vpc(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_vpc_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let vpc_id = result
            .get("Vpc")
            .and_then(|v| v.get("VpcId"))
            .and_then(|v| v.as_str());

        // Tag the VPC with its name
        if let (Some(vid), Some(name)) = (vpc_id, params.get("id").and_then(|v| v.as_str())) {
            self.tag_resource(vid, name);
        }

        // Apply DNS attributes via modify-vpc-attribute (not supported on create-vpc)
        if let Some(vid) = vpc_id {
            if params.get("enable_dns_support").and_then(|v| v.as_bool()) == Some(true) {
                let _ = self.run_aws(&[
                    "ec2",
                    "modify-vpc-attribute",
                    "--vpc-id",
                    vid,
                    "--enable-dns-support",
                ]);
            }
            if params.get("enable_dns_hostnames").and_then(|v| v.as_bool()) == Some(true) {
                let _ = self.run_aws(&[
                    "ec2",
                    "modify-vpc-attribute",
                    "--vpc-id",
                    vid,
                    "--enable-dns-hostnames",
                ]);
            }
        }

        let outputs = serde_json::json!({
            "vpc_id": vpc_id,
            "cidr_block": result.get("Vpc").and_then(|v| v.get("CidrBlock")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_aws_subnet(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_aws_subnet_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let subnet_id = result
            .get("Subnet")
            .and_then(|s| s.get("SubnetId"))
            .and_then(|v| v.as_str());

        // Apply map_public_ip via modify-subnet-attribute (not valid on create-subnet)
        if let Some(sid) = subnet_id {
            if params.get("map_public_ip").and_then(|v| v.as_bool()) == Some(true) {
                let _ = self.run_aws(&[
                    "ec2",
                    "modify-subnet-attribute",
                    "--subnet-id",
                    sid,
                    "--map-public-ip-on-launch",
                ]);
            }
        }

        let outputs = serde_json::json!({
            "subnet_id": subnet_id,
            "availability_zone": result.get("Subnet").and_then(|s| s.get("AvailabilityZone")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_security_group(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_security_group_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let group_id = result.get("GroupId").and_then(|v| v.as_str());

        let outputs = serde_json::json!({
            "group_id": group_id,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_sg_rule(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_sg_rule_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    fn create_eks_cluster(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_eks_cluster_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let cluster = result.get("cluster");
        let outputs = serde_json::json!({
            "name": cluster.and_then(|c| c.get("name")),
            "endpoint": cluster.and_then(|c| c.get("endpoint")),
            "status": cluster.and_then(|c| c.get("status")),
            "certificate_authority": cluster
                .and_then(|c| c.get("certificateAuthority"))
                .and_then(|ca| ca.get("data")),
            "note": "EKS cluster creates are asynchronous. Cluster will be in 'CREATING' state.",
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_eks_nodegroup(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_eks_nodegroup_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let ng = result.get("nodegroup");
        let outputs = serde_json::json!({
            "name": ng.and_then(|n| n.get("nodegroupName")),
            "status": ng.and_then(|n| n.get("status")),
            "scaling_config": ng.and_then(|n| n.get("scalingConfig")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_eks_addon(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_eks_addon_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(result),
        })
    }

    fn create_s3_bucket(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_s3_bucket_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let id = param_str(params, "id")?;

        // Apply versioning configuration if requested
        if let Some(versioning) = params.get("versioning").and_then(|v| v.as_str()) {
            let status = if versioning.eq_ignore_ascii_case("enabled") {
                "Enabled"
            } else {
                "Suspended"
            };
            let _ = self.run_aws(&[
                "s3api",
                "put-bucket-versioning",
                "--bucket",
                &id,
                "--versioning-configuration",
                &format!("Status={status}"),
            ]);
        }

        let outputs = serde_json::json!({
            "name": id,
            "location": result.get("Location"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_kms_key(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_kms_key_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let id = param_str(params, "id")?;
        let key_id = result
            .get("KeyMetadata")
            .and_then(|k| k.get("KeyId"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Create alias for the key
        if !key_id.is_empty() {
            let alias_name = format!("alias/{id}");
            let _ = self.run_aws(&[
                "kms",
                "create-alias",
                "--alias-name",
                &alias_name,
                "--target-key-id",
                key_id,
            ]);
        }

        let outputs = serde_json::json!({
            "key_id": result.get("KeyMetadata").and_then(|k| k.get("KeyId")),
            "arn": result.get("KeyMetadata").and_then(|k| k.get("Arn")),
            "alias": format!("alias/{id}"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_elasticache_redis(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_elasticache_redis_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let cc = result.get("CacheCluster");
        let outputs = serde_json::json!({
            "cache_cluster_id": cc.and_then(|c| c.get("CacheClusterId")),
            "status": cc.and_then(|c| c.get("CacheClusterStatus")),
            "engine": cc.and_then(|c| c.get("Engine")),
            "configuration_endpoint": cc.and_then(|c| c.get("ConfigurationEndpoint")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_elasticache_replication_group(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_elasticache_replication_group_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let rg = result.get("ReplicationGroup");
        let outputs = serde_json::json!({
            "replication_group_id": rg.and_then(|r| r.get("ReplicationGroupId")),
            "status": rg.and_then(|r| r.get("Status")),
            "description": rg.and_then(|r| r.get("Description")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_msk_cluster(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_msk_cluster_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "cluster_arn": result.get("ClusterArn"),
            "state": result.get("State"),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_iam_role(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_iam_role_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let role = result.get("Role");
        let outputs = serde_json::json!({
            "role_name": role.and_then(|r| r.get("RoleName")),
            "arn": role.and_then(|r| r.get("Arn")),
            "create_date": role.and_then(|r| r.get("CreateDate")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_iam_policy(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_iam_policy_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let policy = result.get("Policy");
        let outputs = serde_json::json!({
            "policy_name": policy.and_then(|p| p.get("PolicyName")),
            "arn": policy.and_then(|p| p.get("Arn")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_vpc_endpoint(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_vpc_endpoint_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let ep = result.get("VpcEndpoint");
        let outputs = serde_json::json!({
            "vpc_endpoint_id": ep.and_then(|e| e.get("VpcEndpointId")),
            "service_name": ep.and_then(|e| e.get("ServiceName")),
            "state": ep.and_then(|e| e.get("State")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_nat_gateway(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_nat_gateway_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let ng = result.get("NatGateway");
        let outputs = serde_json::json!({
            "nat_gateway_id": ng.and_then(|n| n.get("NatGatewayId")),
            "state": ng.and_then(|n| n.get("State")),
            "subnet_id": ng.and_then(|n| n.get("SubnetId")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_internet_gateway(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        // Internet gateways are created unattached, then attached to a VPC
        let result = self.run_aws(&["ec2", "create-internet-gateway"])?;

        let igw_id = result
            .get("InternetGateway")
            .and_then(|v| v.get("InternetGatewayId"))
            .and_then(|v| v.as_str());

        // Attach to VPC
        if let Some(gw_id) = igw_id {
            let vpc_id = param_str(params, "vpc_id")?;
            let _ = self.run_aws(&[
                "ec2",
                "attach-internet-gateway",
                "--internet-gateway-id",
                gw_id,
                "--vpc-id",
                &vpc_id,
            ]);

            // Tag with name
            if let Some(name) = params.get("id").and_then(|v| v.as_str()) {
                self.tag_resource(gw_id, name);
            }
        }

        let outputs = serde_json::json!({
            "internet_gateway_id": igw_id,
            "vpc_id": params.get("vpc_id"),
        });

        Ok(ProvisionResult {
            status: "available".into(),
            outputs: Some(outputs),
        })
    }

    fn create_route_table(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let vpc_id = param_str(params, "vpc_id")?;
        let result = self.run_aws(&[
            "ec2",
            "create-route-table",
            "--vpc-id",
            &vpc_id,
        ])?;

        let rt_id = result
            .get("RouteTable")
            .and_then(|v| v.get("RouteTableId"))
            .and_then(|v| v.as_str());

        // Tag with name
        if let (Some(rtid), Some(name)) = (rt_id, params.get("id").and_then(|v| v.as_str())) {
            self.tag_resource(rtid, name);
        }

        let outputs = serde_json::json!({
            "route_table_id": rt_id,
            "vpc_id": vpc_id,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_route_table_association(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let route_table_id = param_str(params, "route_table_id")?;
        let subnet_id = param_str(params, "subnet_id")?;
        let result = self.run_aws(&[
            "ec2",
            "associate-route-table",
            "--route-table-id",
            &route_table_id,
            "--subnet-id",
            &subnet_id,
        ])?;

        let assoc_id = result
            .get("AssociationId")
            .and_then(|v| v.as_str());

        let outputs = serde_json::json!({
            "association_id": assoc_id,
            "route_table_id": route_table_id,
            "subnet_id": subnet_id,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_route(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let route_table_id = param_str(params, "route_table_id")?;
        let destination = param_str(params, "destination_cidr")?;

        let mut args = vec![
            "ec2".to_string(),
            "create-route".to_string(),
            "--route-table-id".to_string(),
            route_table_id.clone(),
            "--destination-cidr-block".to_string(),
            destination.clone(),
        ];

        // Exactly one target must be specified
        if let Some(gw) = params.get("gateway_id").and_then(|v| v.as_str()) {
            args.push("--gateway-id".into());
            args.push(gw.into());
        } else if let Some(nat) = params.get("nat_gateway_id").and_then(|v| v.as_str()) {
            args.push("--nat-gateway-id".into());
            args.push(nat.into());
        } else if let Some(ep) = params.get("vpc_endpoint_id").and_then(|v| v.as_str()) {
            args.push("--vpc-endpoint-id".into());
            args.push(ep.into());
        }

        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "route_table_id": route_table_id,
            "destination_cidr": destination,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_db_subnet_group(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let name = param_str(params, "id")?;
        let subnet_ids = param_str(params, "subnet_ids")?;
        let description = param_str_or(params, "description", &format!("Subnet group {name}"));

        let result = self.run_aws(&[
            "rds",
            "create-db-subnet-group",
            "--db-subnet-group-name",
            &name,
            "--db-subnet-group-description",
            &description,
            "--subnet-ids",
            &subnet_ids,
        ])?;

        let group = result.get("DBSubnetGroup");
        let outputs = serde_json::json!({
            "db_subnet_group_name": group.and_then(|g| g.get("DBSubnetGroupName")),
            "db_subnet_group_arn": group.and_then(|g| g.get("DBSubnetGroupArn")),
            "vpc_id": group.and_then(|g| g.get("VpcId")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_cache_subnet_group(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let name = param_str(params, "id")?;
        let subnet_ids = param_str(params, "subnet_ids")?;
        let description = param_str_or(params, "description", &format!("Cache subnet group {name}"));

        let result = self.run_aws(&[
            "elasticache",
            "create-cache-subnet-group",
            "--cache-subnet-group-name",
            &name,
            "--cache-subnet-group-description",
            &description,
            "--subnet-ids",
            &subnet_ids,
        ])?;

        let group = result.get("CacheSubnetGroup");
        let outputs = serde_json::json!({
            "cache_subnet_group_name": group.and_then(|g| g.get("CacheSubnetGroupName")),
            "vpc_id": group.and_then(|g| g.get("VpcId")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_iam_policy_attachment(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let role_name = param_str(params, "role_name")?;
        let policy_arn = param_str(params, "policy_arn")?;

        self.run_aws(&[
            "iam",
            "attach-role-policy",
            "--role-name",
            &role_name,
            "--policy-arn",
            &policy_arn,
        ])?;

        let outputs = serde_json::json!({
            "role_name": role_name,
            "policy_arn": policy_arn,
        });

        Ok(ProvisionResult {
            status: "attached".into(),
            outputs: Some(outputs),
        })
    }

    fn create_kms_alias(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let id = param_str(params, "id")?;
        let target_key_id = param_str(params, "target_key_id")?;

        let alias_name = if id.starts_with("alias/") {
            id.clone()
        } else {
            format!("alias/{id}")
        };

        self.run_aws(&[
            "kms",
            "create-alias",
            "--alias-name",
            &alias_name,
            "--target-key-id",
            &target_key_id,
        ])?;

        let outputs = serde_json::json!({
            "alias_name": alias_name,
            "target_key_id": target_key_id,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_cloudwatch_log_group(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let id = param_str(params, "id")?;

        self.run_aws(&["logs", "create-log-group", "--log-group-name", &id])?;

        // Set retention if specified
        if let Some(retention) = params.get("retention_in_days").and_then(|v| v.as_str()) {
            let _ = self.run_aws(&[
                "logs",
                "put-retention-policy",
                "--log-group-name",
                &id,
                "--retention-in-days",
                retention,
            ]);
        } else if let Some(retention) = params.get("retention_in_days").and_then(|v| v.as_i64()) {
            let ret_str = retention.to_string();
            let _ = self.run_aws(&[
                "logs",
                "put-retention-policy",
                "--log-group-name",
                &id,
                "--retention-in-days",
                &ret_str,
            ]);
        }

        let outputs = serde_json::json!({
            "log_group_name": id,
            "retention_in_days": params.get("retention_in_days"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    // ── sns_subscription ──────────────────────────────────────────────

    fn create_sns_subscription(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let topic_arn = param_str(params, "topic_arn")?;
        let protocol = param_str(params, "protocol")?;
        let endpoint = param_str(params, "endpoint")?;

        let mut args = vec![
            "sns".to_string(),
            "subscribe".to_string(),
            "--topic-arn".to_string(),
            topic_arn.clone(),
            "--protocol".to_string(),
            protocol.clone(),
            "--notification-endpoint".to_string(),
            endpoint.clone(),
            "--return-subscription-arn".to_string(),
        ];

        // Optional filter policy
        if let Some(filter) = params.get("filter_policy").and_then(|v| v.as_str()) {
            args.push("--attributes".to_string());
            args.push(format!("{{\"FilterPolicy\":\"{}\"}}", filter.replace('"', "\\\"")));
        }

        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let subscription_arn = result
            .get("SubscriptionArn")
            .and_then(|v| v.as_str())
            .unwrap_or("pending confirmation")
            .to_string();

        let outputs = serde_json::json!({
            "subscription_arn": subscription_arn,
            "topic_arn": topic_arn,
            "protocol": protocol,
            "endpoint": endpoint,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    // ── vpc_flow_log ─────────────────────────────────────────────────

    fn create_vpc_flow_log(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let resource_id = param_str(params, "resource_id")?;
        let traffic_type = param_str_or(params, "traffic_type", "ALL");
        let resource_type = param_str_or(params, "resource_type", "VPC");
        let log_destination_type =
            param_str_or(params, "log_destination_type", "cloud-watch-logs");

        let mut args = vec![
            "ec2".to_string(),
            "create-flow-logs".to_string(),
            "--resource-ids".to_string(),
            resource_id.clone(),
            "--resource-type".to_string(),
            resource_type,
            "--traffic-type".to_string(),
            traffic_type,
            "--log-destination-type".to_string(),
            log_destination_type.clone(),
        ];

        // For CloudWatch Logs, accept or auto-resolve log group + IAM role
        if log_destination_type == "cloud-watch-logs" {
            let log_group = param_str_or(
                params,
                "log_group_name",
                &format!("/vpc/flow-logs/{resource_id}"),
            );
            args.push("--log-group-name".to_string());
            args.push(log_group);

            // IAM role is required for CloudWatch Logs destination
            if let Some(role_arn) = params.get("role_arn").and_then(|v| v.as_str()) {
                args.push("--deliver-logs-permission-arn".to_string());
                args.push(role_arn.to_string());
            }
        } else if log_destination_type == "s3" {
            if let Some(bucket_arn) = params.get("log_destination").and_then(|v| v.as_str()) {
                args.push("--log-destination".to_string());
                args.push(bucket_arn.to_string());
            }
        }

        // Optional max aggregation interval
        if let Some(interval) = params.get("max_aggregation_interval").and_then(|v| v.as_str()) {
            args.push("--max-aggregation-interval".to_string());
            args.push(interval.to_string());
        }

        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let flow_log_id = result
            .get("FlowLogIds")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let outputs = serde_json::json!({
            "flow_log_id": flow_log_id,
            "resource_id": resource_id,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_acm_certificate(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_acm_certificate_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "certificate_arn": result.get("CertificateArn"),
            "domain_name": params.get("id"),
            "status": "PENDING_VALIDATION",
        });

        Ok(ProvisionResult {
            status: "pending_validation".into(),
            outputs: Some(outputs),
        })
    }

    fn create_cloudwatch_alarm(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_cloudwatch_alarm_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let _result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "alarm_name": params.get("id"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    // ── Argument builders (testable without execution) ───────────────

    fn build_rds_postgres_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let instance_class = param_str(params, "instance_class")?;
        let engine_version = param_str(params, "engine_version")?;
        let master_username = param_str(params, "master_username")?;
        let master_password = param_str(params, "master_password")?;
        let storage_gb = param_str(params, "storage_gb")?;

        let mut args = vec![
            "rds".into(),
            "create-db-instance".into(),
            "--db-instance-identifier".into(),
            id,
            "--db-instance-class".into(),
            instance_class,
            "--engine".into(),
            "postgres".into(),
            "--engine-version".into(),
            engine_version,
            "--master-username".into(),
            master_username,
            "--master-user-password".into(),
            master_password,
            "--allocated-storage".into(),
            storage_gb,
        ];

        if let Some(v) = params.get("multi_az").and_then(|v| v.as_bool()) {
            if v {
                args.push("--multi-az".into());
            } else {
                args.push("--no-multi-az".into());
            }
        }
        if let Some(v) = params.get("backup_retention") {
            args.push("--backup-retention-period".into());
            args.push(json_val_to_string(v));
        }
        if let Some(v) = params
            .get("vpc_security_group_ids")
            .and_then(|v| v.as_str())
        {
            args.push("--vpc-security-group-ids".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("db_subnet_group").and_then(|v| v.as_str()) {
            args.push("--db-subnet-group-name".into());
            args.push(v.into());
        }

        Ok(args)
    }

    fn build_vpc_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let cidr = param_str(params, "cidr_block")?;
        let args = vec![
            "ec2".into(),
            "create-vpc".into(),
            "--cidr-block".into(),
            cidr,
        ];
        Ok(args)
    }

    fn build_aws_subnet_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let vpc_id = param_str(params, "vpc_id")?;
        let cidr = param_str(params, "cidr_block")?;
        let mut args = vec![
            "ec2".into(),
            "create-subnet".into(),
            "--vpc-id".into(),
            vpc_id,
            "--cidr-block".into(),
            cidr,
        ];
        if let Some(az) = params.get("availability_zone").and_then(|v| v.as_str()) {
            args.push("--availability-zone".into());
            args.push(az.into());
        }
        // NOTE: map_public_ip is applied post-creation via modify-subnet-attribute
        // (--map-public-ip-on-launch is not a valid flag for create-subnet)
        Ok(args)
    }

    fn build_security_group_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let name = param_str(params, "id")?;
        let description = param_str(params, "description")?;
        let vpc_id = param_str(params, "vpc_id")?;
        let args = vec![
            "ec2".into(),
            "create-security-group".into(),
            "--group-name".into(),
            name,
            "--description".into(),
            description,
            "--vpc-id".into(),
            vpc_id,
        ];
        Ok(args)
    }

    fn build_sg_rule_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let group_id = param_str(params, "security_group_id")?;
        let protocol = param_str(params, "protocol")?;
        let port = param_str(params, "port")?;

        let direction = param_str_or(params, "direction", "ingress");

        let cmd = if direction == "egress" {
            "authorize-security-group-egress"
        } else {
            "authorize-security-group-ingress"
        };

        let mut args = vec![
            "ec2".into(),
            cmd.into(),
            "--group-id".into(),
            group_id,
            "--protocol".into(),
            protocol,
            "--port".into(),
            port,
        ];

        // Support either CIDR block or source security group (SG-to-SG)
        if let Some(source_group) = params.get("source_group").and_then(|v| v.as_str()) {
            args.push("--source-group".into());
            args.push(source_group.into());
        } else {
            let cidr = param_str(params, "cidr")?;
            args.push("--cidr".into());
            args.push(cidr);
        }

        if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
            args.push("--description".into());
            args.push(desc.into());
        }
        Ok(args)
    }

    fn build_eks_cluster_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let role_arn = param_str(params, "role_arn")?;
        let subnets = param_str(params, "subnets")?;
        let security_groups = param_str(params, "security_groups")?;
        let version = param_str_or(params, "version", "1.30");

        let vpc_config = format!("subnetIds={subnets},securityGroupIds={security_groups}");

        let args = vec![
            "eks".into(),
            "create-cluster".into(),
            "--name".into(),
            id,
            "--role-arn".into(),
            role_arn,
            "--resources-vpc-config".into(),
            vpc_config,
            "--kubernetes-version".into(),
            version,
        ];
        Ok(args)
    }

    fn build_eks_nodegroup_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let cluster = param_str(params, "cluster")?;
        let node_role = param_str(params, "node_role")?;
        let subnets = param_str(params, "subnets")?;
        let instance_types = param_str_or(params, "instance_types", "t3.medium");
        let min = param_str_or(params, "min", "1");
        let max = param_str_or(params, "max", "3");
        let desired = param_str_or(params, "desired", "2");

        let scaling_config = format!("minSize={min},maxSize={max},desiredSize={desired}");

        let args = vec![
            "eks".into(),
            "create-nodegroup".into(),
            "--cluster-name".into(),
            cluster,
            "--nodegroup-name".into(),
            id,
            "--node-role".into(),
            node_role,
            "--subnets".into(),
            subnets,
            "--instance-types".into(),
            instance_types,
            "--scaling-config".into(),
            scaling_config,
        ];
        Ok(args)
    }

    fn build_eks_addon_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let cluster = param_str(params, "cluster")?;

        let mut args = vec![
            "eks".into(),
            "create-addon".into(),
            "--cluster-name".into(),
            cluster,
            "--addon-name".into(),
            id,
        ];
        if let Some(version) = params.get("version").and_then(|v| v.as_str()) {
            args.push("--addon-version".into());
            args.push(version.into());
        }
        Ok(args)
    }

    fn build_s3_bucket_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;

        let mut args = vec![
            "s3api".into(),
            "create-bucket".into(),
            "--bucket".into(),
            id,
        ];

        // Only add LocationConstraint if region is set and is not us-east-1
        if let Some(ref region) = self.region {
            if region != "us-east-1" {
                args.push("--create-bucket-configuration".into());
                args.push(format!("LocationConstraint={region}"));
            }
        }

        Ok(args)
    }

    fn build_kms_key_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let usage = param_str_or(params, "usage", "ENCRYPT_DECRYPT");

        let mut args = vec![
            "kms".into(),
            "create-key".into(),
            "--key-usage".into(),
            usage,
        ];
        if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
            args.push("--description".into());
            args.push(desc.into());
        }
        Ok(args)
    }

    fn build_elasticache_redis_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let node_type = param_str_or(params, "node_type", "cache.t3.micro");
        let num_nodes = param_str_or(params, "num_nodes", "1");

        let mut args = vec![
            "elasticache".into(),
            "create-cache-cluster".into(),
            "--cache-cluster-id".into(),
            id,
            "--engine".into(),
            "redis".into(),
            "--cache-node-type".into(),
            node_type,
            "--num-cache-nodes".into(),
            num_nodes,
        ];
        if let Some(version) = params.get("version").and_then(|v| v.as_str()) {
            args.push("--engine-version".into());
            args.push(version.into());
        }
        Ok(args)
    }

    fn build_elasticache_replication_group_args(
        &self,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let description = param_str_or(params, "description", "Orbi managed");
        let node_type = param_str_or(params, "node_type", "cache.t3.micro");
        let num_shards = param_str_or(params, "num_shards", "1");
        let replicas = param_str_or(params, "replicas", "1");

        let mut args = vec![
            "elasticache".into(),
            "create-replication-group".into(),
            "--replication-group-id".into(),
            id,
            "--replication-group-description".into(),
            description,
            "--cache-node-type".into(),
            node_type,
            "--num-node-groups".into(),
            num_shards,
            "--replicas-per-node-group".into(),
            replicas,
        ];
        if let Some(version) = params.get("version").and_then(|v| v.as_str()) {
            args.push("--engine-version".into());
            args.push(version.into());
        }
        Ok(args)
    }

    fn build_msk_cluster_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let version = param_str_or(params, "version", "3.6.0");
        let broker_count = param_str_or(params, "broker_count", "3");
        let subnets = param_str(params, "subnets")?;
        let instance_type = param_str_or(params, "instance_type", "kafka.m5.large");
        let security_groups = param_str(params, "security_groups")?;

        // Parse comma-separated values into JSON arrays
        let subnet_list: Vec<String> = subnets.split(',').map(|s| s.trim().to_string()).collect();
        let sg_list: Vec<String> = security_groups
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let broker_info = serde_json::json!({
            "ClientSubnets": subnet_list,
            "InstanceType": instance_type,
            "SecurityGroups": sg_list,
        });

        let mut args = vec![
            "kafka".into(),
            "create-cluster".into(),
            "--cluster-name".into(),
            id,
            "--kafka-version".into(),
            version,
            "--number-of-broker-nodes".into(),
            broker_count,
            "--broker-node-group-info".into(),
            broker_info.to_string(),
        ];
        if let Some(monitoring) = params.get("monitoring").and_then(|v| v.as_str()) {
            args.push("--enhanced-monitoring".into());
            args.push(monitoring.into());
        }
        Ok(args)
    }

    fn build_iam_role_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let trust_policy = param_str(params, "trust_policy")?;

        let mut args = vec![
            "iam".into(),
            "create-role".into(),
            "--role-name".into(),
            id,
            "--assume-role-policy-document".into(),
            trust_policy,
        ];
        if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
            args.push("--description".into());
            args.push(desc.into());
        }
        Ok(args)
    }

    fn build_iam_policy_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let policy_document = param_str(params, "policy_document")?;

        let mut args = vec![
            "iam".into(),
            "create-policy".into(),
            "--policy-name".into(),
            id,
            "--policy-document".into(),
            policy_document,
        ];
        if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
            args.push("--description".into());
            args.push(desc.into());
        }
        Ok(args)
    }

    fn build_vpc_endpoint_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let vpc_id = param_str(params, "vpc_id")?;
        let service_name = param_str(params, "service_name")?;
        let ep_type = param_str_or(params, "type", "Gateway");

        let mut args = vec![
            "ec2".into(),
            "create-vpc-endpoint".into(),
            "--vpc-id".into(),
            vpc_id,
            "--service-name".into(),
            service_name,
            "--vpc-endpoint-type".into(),
            ep_type.clone(),
        ];
        // Interface endpoints require subnets and security groups
        if let Some(subnets) = params.get("subnet_ids").and_then(|v| v.as_str()) {
            args.push("--subnet-ids".into());
            args.push(subnets.into());
        }
        if let Some(sgs) = params.get("security_group_ids").and_then(|v| v.as_str()) {
            args.push("--security-group-ids".into());
            args.push(sgs.into());
        }
        // Private DNS (Interface endpoints only, defaults to true)
        if ep_type.eq_ignore_ascii_case("Interface") {
            let dns_enabled = params
                .get("private_dns_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if dns_enabled {
                args.push("--private-dns-enabled".into());
            } else {
                args.push("--no-private-dns-enabled".into());
            }
        }
        // Route table IDs (Gateway endpoints only)
        if let Some(rt_ids) = params.get("route_table_ids").and_then(|v| v.as_str()) {
            args.push("--route-table-ids".into());
            args.push(rt_ids.into());
        }
        Ok(args)
    }

    fn build_nat_gateway_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let subnet_id = param_str(params, "subnet_id")?;
        let allocation_id = param_str(params, "allocation_id")?;

        let args = vec![
            "ec2".into(),
            "create-nat-gateway".into(),
            "--subnet-id".into(),
            subnet_id,
            "--allocation-id".into(),
            allocation_id,
        ];
        Ok(args)
    }

    fn build_acm_certificate_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let validation = param_str_or(params, "validation", "DNS");

        let mut args = vec![
            "acm".into(),
            "request-certificate".into(),
            "--domain-name".into(),
            id,
            "--validation-method".into(),
            validation,
        ];
        if let Some(san) = params.get("san").and_then(|v| v.as_str()) {
            args.push("--subject-alternative-names".into());
            args.push(san.into());
        }
        Ok(args)
    }

    fn build_cloudwatch_alarm_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let metric = param_str(params, "metric")?;
        let namespace = param_str(params, "namespace")?;
        let threshold = param_str(params, "threshold")?;
        let statistic = param_str_or(params, "statistic", "Average");
        let period = param_str_or(params, "period", "300");
        let eval_periods = param_str_or(params, "eval_periods", "1");
        let operator = param_str_or(params, "operator", "GreaterThanThreshold");

        let args = vec![
            "cloudwatch".into(),
            "put-metric-alarm".into(),
            "--alarm-name".into(),
            id,
            "--metric-name".into(),
            metric,
            "--namespace".into(),
            namespace,
            "--statistic".into(),
            statistic,
            "--period".into(),
            period,
            "--evaluation-periods".into(),
            eval_periods,
            "--threshold".into(),
            threshold,
            "--comparison-operator".into(),
            operator,
        ];
        Ok(args)
    }

    // ── SES / Backup create implementations ────────────────────────

    fn create_ses_domain(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ses_domain_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let id = param_str(params, "id")?;

        let verification_token = result
            .get("VerificationToken")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Optionally enable DKIM
        let dkim_tokens = if param_str_or(params, "dkim", "false") == "true" {
            let dkim_result = self.run_aws(&["ses", "verify-domain-dkim", "--domain", &id])?;
            dkim_result.get("DkimTokens").cloned()
        } else {
            None
        };

        let outputs = serde_json::json!({
            "domain": id,
            "verification_token": verification_token,
            "dkim_tokens": dkim_tokens,
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_ses_smtp_user(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ses_smtp_user_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

        let id = param_str(params, "id")?;

        // Step 1: Create the IAM user
        self.run_aws(&refs)?;

        // Step 2: Attach SES send policy
        let ses_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ses:SendRawEmail","Resource":"*"}]}"#;
        self.run_aws(&[
            "iam",
            "put-user-policy",
            "--user-name",
            &id,
            "--policy-name",
            "SesSendPolicy",
            "--policy-document",
            ses_policy,
        ])?;

        // Step 3: Create access key
        let key_result = self.run_aws(&["iam", "create-access-key", "--user-name", &id])?;

        let ak = key_result.get("AccessKey");
        let outputs = serde_json::json!({
            "user_name": id,
            "access_key_id": ak.and_then(|a| a.get("AccessKeyId")),
            "secret_access_key": ak.and_then(|a| a.get("SecretAccessKey")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_backup_vault(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_backup_vault_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "backup_vault_name": result.get("BackupVaultName"),
            "backup_vault_arn": result.get("BackupVaultArn"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_backup_plan(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_backup_plan_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "backup_plan_id": result.get("BackupPlanId"),
            "backup_plan_arn": result.get("BackupPlanArn"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    // ── ECS / ECR / ALB / CloudFront / Route53 / SecretsManager create ──

    fn create_ecs_cluster(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ecs_cluster_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let cluster = result.get("cluster");
        let outputs = serde_json::json!({
            "cluster_arn": cluster.and_then(|c| c.get("clusterArn")),
            "status": cluster.and_then(|c| c.get("status")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_ecs_service(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ecs_service_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let service = result.get("service");
        let outputs = serde_json::json!({
            "service_arn": service.and_then(|s| s.get("serviceArn")),
            "status": service.and_then(|s| s.get("status")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_ecs_task_definition(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ecs_task_definition_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let td = result.get("taskDefinition");
        let outputs = serde_json::json!({
            "task_definition_arn": td.and_then(|t| t.get("taskDefinitionArn")),
            "revision": td.and_then(|t| t.get("revision")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_ecr_repository(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_ecr_repository_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let repo = result.get("repository");
        let outputs = serde_json::json!({
            "repository_arn": repo.and_then(|r| r.get("repositoryArn")),
            "repository_uri": repo.and_then(|r| r.get("repositoryUri")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_alb(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_alb_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let lb = result
            .get("LoadBalancers")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first());
        let outputs = serde_json::json!({
            "load_balancer_arn": lb.and_then(|l| l.get("LoadBalancerArn")),
            "dns_name": lb.and_then(|l| l.get("DNSName")),
            "hosted_zone_id": lb.and_then(|l| l.get("CanonicalHostedZoneId")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_alb_target_group(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_alb_target_group_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let tg = result
            .get("TargetGroups")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first());
        let outputs = serde_json::json!({
            "target_group_arn": tg.and_then(|t| t.get("TargetGroupArn")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_alb_listener(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_alb_listener_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let listener = result
            .get("Listeners")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first());
        let outputs = serde_json::json!({
            "listener_arn": listener.and_then(|l| l.get("ListenerArn")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_cloudfront_distribution(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_cloudfront_distribution_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let dist = result.get("Distribution");
        let outputs = serde_json::json!({
            "distribution_id": dist.and_then(|d| d.get("Id")),
            "domain_name": dist.and_then(|d| d.get("DomainName")),
            "arn": dist.and_then(|d| d.get("ARN")),
        });

        Ok(ProvisionResult {
            status: "creating".into(),
            outputs: Some(outputs),
        })
    }

    fn create_route53_zone(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_route53_zone_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let zone = result.get("HostedZone");
        let outputs = serde_json::json!({
            "hosted_zone_id": zone.and_then(|z| z.get("Id")),
            "name_servers": result.get("DelegationSet").and_then(|d| d.get("NameServers")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_route53_record(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_route53_record_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let change_info = result.get("ChangeInfo");
        let outputs = serde_json::json!({
            "change_id": change_info.and_then(|c| c.get("Id")),
            "status": change_info.and_then(|c| c.get("Status")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn create_secrets_manager_secret(
        &self,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        let args = self.build_secrets_manager_secret_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        let outputs = serde_json::json!({
            "arn": result.get("ARN"),
            "name": result.get("Name"),
            "version_id": result.get("VersionId"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    // ── SES / Backup argument builders ─────────────────────────────

    fn build_ses_domain_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let args = vec![
            "ses".into(),
            "verify-domain-identity".into(),
            "--domain".into(),
            id,
        ];
        Ok(args)
    }

    fn build_ses_smtp_user_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let args = vec!["iam".into(), "create-user".into(), "--user-name".into(), id];
        Ok(args)
    }

    fn build_backup_vault_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let mut args = vec![
            "backup".into(),
            "create-backup-vault".into(),
            "--backup-vault-name".into(),
            id,
        ];
        if let Some(key) = params.get("encryption_key").and_then(|v| v.as_str()) {
            args.push("--encryption-key-arn".into());
            args.push(key.into());
        }
        Ok(args)
    }

    fn build_backup_plan_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let vault = param_str(params, "vault")?;
        let retention_days = param_str_or(params, "retention_days", "30");
        let schedule = param_str_or(params, "schedule", "cron(0 5 ? * * *)");

        let plan_json = format!(
            r#"{{"BackupPlanName":"{}","Rules":[{{"RuleName":"daily","TargetBackupVaultName":"{}","ScheduleExpression":"{}","Lifecycle":{{"DeleteAfterDays":{}}}}}]}}"#,
            id, vault, schedule, retention_days
        );

        let args = vec![
            "backup".into(),
            "create-backup-plan".into(),
            "--backup-plan".into(),
            plan_json,
        ];
        Ok(args)
    }
    // ── ECS / ECR / ALB / CloudFront / Route53 / SecretsManager arg builders ──

    fn build_ecs_cluster_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let capacity_providers = param_str_or(params, "capacity_providers", "FARGATE");

        let mut args = vec![
            "ecs".into(),
            "create-cluster".into(),
            "--cluster-name".into(),
            id,
            "--capacity-providers".into(),
            capacity_providers,
        ];
        // Allow passing additional settings tags etc
        if let Some(tags) = params.get("tags").and_then(|v| v.as_str()) {
            args.push("--tags".into());
            args.push(tags.into());
        }
        Ok(args)
    }

    fn build_ecs_service_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let cluster = param_str(params, "cluster")?;
        let task_definition = param_str(params, "task_definition")?;
        let desired_count = param_str_or(params, "desired_count", "1");
        let subnets = param_str(params, "subnets")?;
        let security_groups = param_str(params, "security_groups")?;
        let public_ip = param_str_or(params, "public_ip", "DISABLED");
        let platform_version = param_str_or(params, "platform_version", "LATEST");

        let network_config = format!(
            "awsvpcConfiguration={{subnets=[{subnets}],securityGroups=[{security_groups}],assignPublicIp={public_ip}}}"
        );

        let mut args = vec![
            "ecs".into(),
            "create-service".into(),
            "--cluster".into(),
            cluster,
            "--service-name".into(),
            id.clone(),
            "--task-definition".into(),
            task_definition,
            "--desired-count".into(),
            desired_count,
            "--launch-type".into(),
            "FARGATE".into(),
            "--platform-version".into(),
            platform_version,
            "--network-configuration".into(),
            network_config,
        ];

        // Load balancer configuration (target_group_arn + container_name + container_port)
        if let Some(tg_arn) = params.get("target_group_arn").and_then(|v| v.as_str()) {
            let lb_container = params
                .get("lb_container_name")
                .and_then(|v| v.as_str())
                .unwrap_or(&id);
            let lb_port = params
                .get("lb_container_port")
                .and_then(|v| v.as_str())
                .or_else(|| params.get("lb_container_port").and_then(|v| v.as_i64()).map(|_| ""))
                .unwrap_or("80");
            let lb_port_val = params
                .get("lb_container_port")
                .and_then(|v| v.as_i64())
                .map(|n| n.to_string())
                .unwrap_or_else(|| lb_port.to_string());
            let lb_json = format!(
                "targetGroupArn={tg_arn},containerName={lb_container},containerPort={lb_port_val}"
            );
            args.push("--load-balancers".into());
            args.push(lb_json);
        }

        // Service registries (for Cloud Map)
        if let Some(registry_arn) = params.get("service_registry_arn").and_then(|v| v.as_str()) {
            args.push("--service-registries".into());
            args.push(format!("registryArn={registry_arn}"));
        }

        Ok(args)
    }

    fn build_ecs_task_definition_args(
        &self,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let cpu = param_str_or(params, "cpu", "256");
        let memory = param_str_or(params, "memory", "512");

        // If user passes raw container_definitions JSON, use it directly
        // (full-control escape hatch for complex multi-container tasks)
        let container_defs = if let Some(raw) = params.get("container_definitions").and_then(|v| v.as_str()) {
            raw.to_string()
        } else {
            // Build single-container definition from params
            let container_name = param_str_or(params, "container_name", &id);
            let image = param_str(params, "image")?;

            // Port mappings: port = '80' or ports = '80,443,8080'
            let port_mappings: Vec<serde_json::Value> = if let Some(ports) = params.get("ports").and_then(|v| v.as_str()) {
                ports.split(',').map(|p| {
                    let port_num = p.trim().parse::<i64>().unwrap_or(80);
                    serde_json::json!({"containerPort": port_num, "protocol": "tcp"})
                }).collect()
            } else {
                let port = param_str_or(params, "port", "80");
                vec![serde_json::json!({"containerPort": port.parse::<i64>().unwrap_or(80), "protocol": "tcp"})]
            };

            let mut container = serde_json::json!({
                "name": container_name,
                "image": image,
                "portMappings": port_mappings,
                "essential": true,
            });

            if let Some(obj) = container.as_object_mut() {
                // Command override: command = 'node,server.js' (comma-separated)
                if let Some(cmd) = params.get("command").and_then(|v| v.as_str()) {
                    let parts: Vec<serde_json::Value> = cmd.split(',')
                        .map(|s| serde_json::Value::String(s.trim().to_string()))
                        .collect();
                    obj.insert("command".into(), serde_json::Value::Array(parts));
                }

                // Entrypoint override: entrypoint = '/bin/sh,-c'
                if let Some(ep) = params.get("entrypoint").and_then(|v| v.as_str()) {
                    let parts: Vec<serde_json::Value> = ep.split(',')
                        .map(|s| serde_json::Value::String(s.trim().to_string()))
                        .collect();
                    obj.insert("entryPoint".into(), serde_json::Value::Array(parts));
                }

                // Working directory
                if let Some(wd) = params.get("working_directory").and_then(|v| v.as_str()) {
                    obj.insert("workingDirectory".into(), serde_json::Value::String(wd.to_string()));
                }

                // Container-level memory limits
                if let Some(mem_limit) = params.get("memory_reservation").and_then(|v| {
                    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                }) {
                    obj.insert("memoryReservation".into(), serde_json::json!(mem_limit));
                }

                // Environment variables: env_vars = 'KEY1=val1,KEY2=val2'
                if let Some(env_str) = params.get("env_vars").and_then(|v| v.as_str()) {
                    let env: Vec<serde_json::Value> = env_str
                        .split(',')
                        .filter_map(|pair| {
                            let mut parts = pair.splitn(2, '=');
                            let k = parts.next()?.trim();
                            let v = parts.next()?.trim();
                            Some(serde_json::json!({"name": k, "value": v}))
                        })
                        .collect();
                    if !env.is_empty() {
                        obj.insert("environment".into(), serde_json::Value::Array(env));
                    }
                }

                // Log configuration: log_group = '/ecs/my-service'
                if let Some(log_group) = params.get("log_group").and_then(|v| v.as_str()) {
                    let region = self.region.as_deref().unwrap_or("us-east-1");
                    let stream_prefix = params.get("log_stream_prefix")
                        .and_then(|v| v.as_str())
                        .unwrap_or("ecs");
                    obj.insert(
                        "logConfiguration".into(),
                        serde_json::json!({
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": log_group,
                                "awslogs-region": region,
                                "awslogs-stream-prefix": stream_prefix
                            }
                        }),
                    );
                }

                // Health check: health_check_cmd = 'CMD-SHELL,curl -f http://localhost/ || exit 1'
                if let Some(hc) = params.get("health_check_cmd").and_then(|v| v.as_str()) {
                    let parts: Vec<&str> = hc.splitn(2, ',').collect();
                    let (cmd_type, cmd) = if parts.len() == 2 {
                        (parts[0], parts[1])
                    } else {
                        ("CMD-SHELL", hc)
                    };
                    let interval: i64 = params.get("health_check_interval")
                        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                        .unwrap_or(30);
                    let retries: i64 = params.get("health_check_retries")
                        .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                        .unwrap_or(3);
                    obj.insert(
                        "healthCheck".into(),
                        serde_json::json!({
                            "command": [cmd_type, cmd],
                            "interval": interval,
                            "timeout": 5,
                            "retries": retries,
                            "startPeriod": 60
                        }),
                    );
                }

                // Secrets: secrets = 'ENV_VAR=arn:aws:secretsmanager:...,OTHER=arn:...'
                if let Some(secrets_str) = params.get("secrets").and_then(|v| v.as_str()) {
                    let secrets: Vec<serde_json::Value> = secrets_str
                        .split(',')
                        .filter_map(|pair| {
                            let mut parts = pair.splitn(2, '=');
                            let k = parts.next()?.trim();
                            let v = parts.next()?.trim();
                            Some(serde_json::json!({"name": k, "valueFrom": v}))
                        })
                        .collect();
                    if !secrets.is_empty() {
                        obj.insert("secrets".into(), serde_json::Value::Array(secrets));
                    }
                }

                // Mount points: mount_points = 'source_vol:/container/path,data:/app/data'
                if let Some(mounts_str) = params.get("mount_points").and_then(|v| v.as_str()) {
                    let mounts: Vec<serde_json::Value> = mounts_str
                        .split(',')
                        .filter_map(|pair| {
                            let mut parts = pair.splitn(2, ':');
                            let vol = parts.next()?.trim();
                            let path = parts.next()?.trim();
                            Some(serde_json::json!({
                                "sourceVolume": vol,
                                "containerPath": path,
                                "readOnly": false
                            }))
                        })
                        .collect();
                    if !mounts.is_empty() {
                        obj.insert("mountPoints".into(), serde_json::Value::Array(mounts));
                    }
                }
            }

            serde_json::json!([container]).to_string()
        };

        let mut args = vec![
            "ecs".into(),
            "register-task-definition".into(),
            "--family".into(),
            id,
            "--network-mode".into(),
            "awsvpc".into(),
            "--requires-compatibilities".into(),
            "FARGATE".into(),
            "--cpu".into(),
            cpu,
            "--memory".into(),
            memory,
            "--container-definitions".into(),
            container_defs,
        ];

        // Execution role (required for log drivers and secrets)
        if let Some(exec_role) = params.get("execution_role_arn").and_then(|v| v.as_str()) {
            args.push("--execution-role-arn".into());
            args.push(exec_role.into());
        }
        // Task role (for application-level AWS API access)
        if let Some(task_role) = params.get("task_role_arn").and_then(|v| v.as_str()) {
            args.push("--task-role-arn".into());
            args.push(task_role.into());
        }
        // EFS volumes: volumes = 'name:fs-id:path,data:fs-456:/data'
        if let Some(vols_str) = params.get("volumes").and_then(|v| v.as_str()) {
            let volumes: Vec<serde_json::Value> = vols_str
                .split(',')
                .filter_map(|vol| {
                    let parts: Vec<&str> = vol.splitn(3, ':').collect();
                    if parts.len() >= 2 {
                        let name = parts[0].trim();
                        let fs_id = parts[1].trim();
                        let root_dir = if parts.len() == 3 { parts[2].trim() } else { "/" };
                        Some(serde_json::json!({
                            "name": name,
                            "efsVolumeConfiguration": {
                                "fileSystemId": fs_id,
                                "rootDirectory": root_dir,
                                "transitEncryption": "ENABLED"
                            }
                        }))
                    } else {
                        None
                    }
                })
                .collect();
            if !volumes.is_empty() {
                let volumes_json = serde_json::json!(volumes).to_string();
                args.push("--volumes".into());
                args.push(volumes_json);
            }
        }

        Ok(args)
    }

    fn build_ecr_repository_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let scan = param_str_or(params, "scan", "true");
        let encryption = param_str_or(params, "encryption", "AES256");

        let scan_config = format!("scanOnPush={scan}");
        let encryption_config = format!("encryptionType={encryption}");

        let args = vec![
            "ecr".into(),
            "create-repository".into(),
            "--repository-name".into(),
            id,
            "--image-scanning-configuration".into(),
            scan_config,
            "--encryption-configuration".into(),
            encryption_config,
        ];
        Ok(args)
    }

    fn build_alb_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let subnets = param_str(params, "subnets")?;
        let security_groups = param_str(params, "security_groups")?;
        let scheme = param_str_or(params, "scheme", "internet-facing");

        let args = vec![
            "elbv2".into(),
            "create-load-balancer".into(),
            "--name".into(),
            id,
            "--type".into(),
            "application".into(),
            "--subnets".into(),
            subnets,
            "--security-groups".into(),
            security_groups,
            "--scheme".into(),
            scheme,
        ];
        Ok(args)
    }

    fn build_alb_target_group_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let protocol = param_str_or(params, "protocol", "HTTP");
        let port = param_str(params, "port")?;
        let vpc_id = param_str(params, "vpc_id")?;
        let target_type = param_str_or(params, "target_type", "ip");
        let health_check = param_str_or(params, "health_check", "/");

        let args = vec![
            "elbv2".into(),
            "create-target-group".into(),
            "--name".into(),
            id,
            "--protocol".into(),
            protocol,
            "--port".into(),
            port,
            "--vpc-id".into(),
            vpc_id,
            "--target-type".into(),
            target_type,
            "--health-check-path".into(),
            health_check,
        ];
        Ok(args)
    }

    fn build_alb_listener_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let alb_arn = param_str(params, "alb_arn")?;
        let protocol = param_str_or(params, "protocol", "HTTP");
        let port = param_str_or(params, "port", "80");
        let target_group_arn = param_str(params, "target_group_arn")?;

        let action = format!("Type=forward,TargetGroupArn={target_group_arn}");

        let args = vec![
            "elbv2".into(),
            "create-listener".into(),
            "--load-balancer-arn".into(),
            alb_arn,
            "--protocol".into(),
            protocol,
            "--port".into(),
            port,
            "--default-actions".into(),
            action,
        ];
        Ok(args)
    }

    fn build_cloudfront_distribution_args(
        &self,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let origin_domain = param_str(params, "origin_domain")?;
        let origin_id = param_str_or(params, "origin_id", &id);
        let viewer_protocol = param_str_or(params, "viewer_protocol", "redirect-to-https");
        let comment = param_str_or(params, "comment", "");

        let config_json = format!(
            r#"{{"CallerReference":"{}","Origins":{{"Quantity":1,"Items":[{{"Id":"{}","DomainName":"{}","S3OriginConfig":{{"OriginAccessIdentity":""}}}}]}},"DefaultCacheBehavior":{{"TargetOriginId":"{}","ViewerProtocolPolicy":"{}","ForwardedValues":{{"QueryString":false,"Cookies":{{"Forward":"none"}}}}}},"Enabled":true,"Comment":"{}"}}"#,
            id, origin_id, origin_domain, origin_id, viewer_protocol, comment
        );

        let args = vec![
            "cloudfront".into(),
            "create-distribution".into(),
            "--distribution-config".into(),
            config_json,
        ];
        Ok(args)
    }

    fn build_route53_zone_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let caller_ref = param_str_or(params, "caller_ref", &id);

        let args = vec![
            "route53".into(),
            "create-hosted-zone".into(),
            "--name".into(),
            id,
            "--caller-reference".into(),
            caller_ref,
        ];
        Ok(args)
    }

    fn build_route53_record_args(&self, params: &Value) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let zone_id = param_str(params, "zone_id")?;
        let record_type = param_str_or(params, "record_type", "A");
        let value = param_str(params, "value")?;
        let ttl = param_str_or(params, "ttl", "300");

        let change_batch = format!(
            r#"{{"Changes":[{{"Action":"UPSERT","ResourceRecordSet":{{"Name":"{}","Type":"{}","TTL":{},"ResourceRecords":[{{"Value":"{}"}}]}}}}]}}"#,
            id, record_type, ttl, value
        );

        let args = vec![
            "route53".into(),
            "change-resource-record-sets".into(),
            "--hosted-zone-id".into(),
            zone_id,
            "--change-batch".into(),
            change_batch,
        ];
        Ok(args)
    }

    fn build_secrets_manager_secret_args(
        &self,
        params: &Value,
    ) -> Result<Vec<String>, ProvisionError> {
        let id = param_str(params, "id")?;
        let value = param_str(params, "value")?;

        let mut args = vec![
            "secretsmanager".into(),
            "create-secret".into(),
            "--name".into(),
            id,
            "--secret-string".into(),
            value,
        ];
        if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
            args.push("--description".into());
            args.push(desc.into());
        }
        Ok(args)
    }

    // ── UPDATE (ALTER RESOURCE) ────────────────────────────────────

    /// Update an existing AWS resource. Returns updated outputs on success.
    pub fn update(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "secrets_manager_secret" => {
                let mut args = vec![
                    "secretsmanager".to_string(),
                    "update-secret".to_string(),
                    "--secret-id".to_string(),
                    id.to_string(),
                ];
                if let Some(value) = params.get("value").and_then(|v| v.as_str()) {
                    args.push("--secret-string".into());
                    args.push(value.into());
                }
                if let Some(desc) = params.get("description").and_then(|v| v.as_str()) {
                    args.push("--description".into());
                    args.push(desc.into());
                }
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                let result = self.run_aws(&refs)?;

                let outputs = serde_json::json!({
                    "arn": result.get("ARN"),
                    "name": result.get("Name"),
                    "version_id": result.get("VersionId"),
                });
                Ok(ProvisionResult {
                    status: "updated".into(),
                    outputs: Some(outputs),
                })
            }
            "cloudwatch_log_group" => {
                if let Some(retention) = params.get("retention_in_days") {
                    let ret_str = if let Some(n) = retention.as_i64() {
                        n.to_string()
                    } else if let Some(s) = retention.as_str() {
                        s.to_string()
                    } else {
                        return Err("invalid retention_in_days value".into());
                    };
                    self.run_aws(&[
                        "logs",
                        "put-retention-policy",
                        "--log-group-name",
                        id,
                        "--retention-in-days",
                        &ret_str,
                    ])?;
                }
                Ok(ProvisionResult {
                    status: "updated".into(),
                    outputs: None,
                })
            }
            other => Err(format!("ALTER not supported for AWS resource type: {other}").into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the `Name` tag from a Tags array, if present.
fn extract_name_from_tags(resource: &Value) -> Option<String> {
    resource
        .get("Tags")?
        .as_array()?
        .iter()
        .find(|tag| tag.get("Key").and_then(|k| k.as_str()) == Some("Name"))
        .and_then(|tag| tag.get("Value"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
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
        let params = serde_json::json!({"id": "my-db", "instance_class": "db.t3.micro"});
        assert_eq!(param_str(&params, "id").unwrap(), "my-db");
        assert_eq!(param_str(&params, "instance_class").unwrap(), "db.t3.micro");
    }

    #[test]
    fn test_param_str_missing() {
        let params = serde_json::json!({"id": "my-db"});
        let err = param_str(&params, "engine_version").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing required parameter") || msg.contains("engine_version"));
        assert!(msg.contains("engine_version"));
    }

    #[test]
    fn test_create_dispatches_rds_postgres() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-pg",
            "instance_class": "db.t3.micro",
            "engine_version": "16",
            "master_username": "admin",
            "master_password": "secret",
            "storage_gb": "20"
        });
        let args = p.build_create_args("rds_postgres", &params).unwrap();
        assert!(args.contains(&"rds".to_string()));
        assert!(args.contains(&"create-db-instance".to_string()));
        assert!(args.contains(&"--engine".to_string()));
        assert!(args.contains(&"postgres".to_string()));
    }

    #[test]
    fn test_create_unknown_type() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({"id": "x"});
        let err = p.build_create_args("foobar_unknown", &params).unwrap_err();
        assert!(err.to_string().contains("unsupported AWS resource type"));
    }

    #[test]
    fn test_rds_postgres_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "acme-db",
            "instance_class": "db.t3.micro",
            "engine_version": "16",
            "master_username": "pgadmin",
            "master_password": "supersecret",
            "storage_gb": "50",
            "backup_retention": 7,
            "multi_az": true
        });
        let args = p.build_create_args("rds_postgres", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"rds".to_string()));
        assert!(args.contains(&"create-db-instance".to_string()));
        assert!(args.contains(&"--db-instance-identifier".to_string()));
        assert!(args.contains(&"acme-db".to_string()));
        assert!(args.contains(&"--db-instance-class".to_string()));
        assert!(args.contains(&"db.t3.micro".to_string()));
        assert!(args.contains(&"--engine".to_string()));
        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"--engine-version".to_string()));
        assert!(args.contains(&"16".to_string()));
        assert!(args.contains(&"--master-username".to_string()));
        assert!(args.contains(&"pgadmin".to_string()));
        assert!(args.contains(&"--master-user-password".to_string()));
        assert!(args.contains(&"supersecret".to_string()));
        assert!(args.contains(&"--allocated-storage".to_string()));
        assert!(args.contains(&"50".to_string()));
        assert!(args.contains(&"--multi-az".to_string()));
        assert!(args.contains(&"--backup-retention-period".to_string()));
        assert!(args.contains(&"7".to_string()));
        assert!(args.contains(&"--output".to_string()));
        assert!(args.contains(&"json".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
    }

    #[test]
    fn test_vpc_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-west-2"), None);
        let params = serde_json::json!({
            "id": "main-vpc",
            "cidr_block": "10.0.0.0/16"
        });
        let args = p.build_create_args("vpc", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-vpc".to_string()));
        assert!(args.contains(&"--cidr-block".to_string()));
        assert!(args.contains(&"10.0.0.0/16".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-west-2".to_string()));
    }

    #[test]
    fn test_aws_subnet_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "pub-subnet-1",
            "vpc_id": "vpc-12345",
            "cidr_block": "10.0.1.0/24",
            "availability_zone": "us-east-1a"
        });
        let args = p.build_create_args("aws_subnet", &params).unwrap();

        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-subnet".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-12345".to_string()));
        assert!(args.contains(&"--cidr-block".to_string()));
        assert!(args.contains(&"10.0.1.0/24".to_string()));
        assert!(args.contains(&"--availability-zone".to_string()));
        assert!(args.contains(&"us-east-1a".to_string()));
    }

    #[test]
    fn test_security_group_build_args() {
        let p = AwsResourceProvisioner::new(Some("eu-west-1"), None);
        let params = serde_json::json!({
            "id": "web-sg",
            "description": "Web server security group",
            "vpc_id": "vpc-abc123"
        });
        let args = p.build_create_args("security_group", &params).unwrap();

        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-security-group".to_string()));
        assert!(args.contains(&"--group-name".to_string()));
        assert!(args.contains(&"web-sg".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"Web server security group".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-abc123".to_string()));
    }

    #[test]
    fn test_sg_rule_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "allow-https",
            "security_group_id": "sg-12345",
            "protocol": "tcp",
            "port": "443",
            "cidr": "0.0.0.0/0"
        });
        let args = p.build_create_args("sg_rule", &params).unwrap();

        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"authorize-security-group-ingress".to_string()));
        assert!(args.contains(&"--group-id".to_string()));
        assert!(args.contains(&"sg-12345".to_string()));
        assert!(args.contains(&"--protocol".to_string()));
        assert!(args.contains(&"tcp".to_string()));
        assert!(args.contains(&"--port".to_string()));
        assert!(args.contains(&"443".to_string()));
        assert!(args.contains(&"--cidr".to_string()));
        assert!(args.contains(&"0.0.0.0/0".to_string()));
    }

    #[test]
    fn test_sg_rule_egress() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "allow-outbound",
            "security_group_id": "sg-99999",
            "protocol": "tcp",
            "port": "80",
            "cidr": "0.0.0.0/0",
            "direction": "egress"
        });
        let args = p.build_create_args("sg_rule", &params).unwrap();

        assert!(args.contains(&"authorize-security-group-egress".to_string()));
        assert!(!args.contains(&"authorize-security-group-ingress".to_string()));
    }

    #[test]
    fn test_delete_rds_postgres() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let args = p.build_delete_args("rds_postgres", "my-db").unwrap();
        assert!(args.contains(&"rds".to_string()));
        assert!(args.contains(&"delete-db-instance".to_string()));
        assert!(args.contains(&"--db-instance-identifier".to_string()));
        assert!(args.contains(&"my-db".to_string()));
        assert!(args.contains(&"--skip-final-snapshot".to_string()));
    }

    #[test]
    fn test_delete_vpc() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("vpc", "vpc-12345").unwrap();
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"delete-vpc".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-12345".to_string()));
    }

    #[test]
    fn test_delete_security_group() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("security_group", "sg-abc").unwrap();
        assert!(args.contains(&"delete-security-group".to_string()));
        assert!(args.contains(&"--group-id".to_string()));
        assert!(args.contains(&"sg-abc".to_string()));
    }

    #[test]
    fn test_delete_unknown_type() {
        let p = AwsResourceProvisioner::new(None, None);
        let err = p.build_delete_args("not_a_thing", "x").unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported AWS resource type for delete"));
    }

    // ── Discovery parsing tests ─────────────────────────────────────

    #[test]
    fn test_parse_ec2_instances() {
        let json = serde_json::json!({
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-abc123",
                    "InstanceType": "t3.micro",
                    "State": { "Name": "running" },
                    "PublicIpAddress": "1.2.3.4",
                    "PrivateIpAddress": "10.0.0.5",
                    "VpcId": "vpc-111",
                    "SubnetId": "subnet-222",
                    "Tags": [
                        { "Key": "Name", "Value": "web-server" },
                        { "Key": "Env", "Value": "prod" }
                    ]
                }]
            }]
        });

        let results = AwsResourceProvisioner::parse_ec2_instances(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "i-abc123");
        assert_eq!(r["resource_type"], "ec2");
        assert_eq!(r["name"], "web-server");
        assert_eq!(r["config"]["instance_type"], "t3.micro");
        assert_eq!(r["config"]["vpc_id"], "vpc-111");
        assert_eq!(r["outputs"]["state"], "running");
        assert_eq!(r["outputs"]["public_ip"], "1.2.3.4");
        assert_eq!(r["outputs"]["private_ip"], "10.0.0.5");
    }

    #[test]
    fn test_parse_ec2_instances_no_tags() {
        let json = serde_json::json!({
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-notag",
                    "InstanceType": "t2.nano",
                    "State": { "Name": "stopped" }
                }]
            }]
        });

        let results = AwsResourceProvisioner::parse_ec2_instances(&json);
        assert_eq!(results.len(), 1);
        // name falls back to id when there are no tags
        assert_eq!(results[0]["name"], "i-notag");
    }

    #[test]
    fn test_parse_ec2_empty_reservations() {
        let json = serde_json::json!({ "Reservations": [] });
        let results = AwsResourceProvisioner::parse_ec2_instances(&json);
        assert!(results.is_empty());
    }

    #[test]
    fn test_parse_rds_instances() {
        let json = serde_json::json!({
            "DBInstances": [{
                "DBInstanceIdentifier": "prod-db",
                "Engine": "postgres",
                "EngineVersion": "16.1",
                "DBInstanceClass": "db.r6g.large",
                "Endpoint": { "Address": "prod-db.abc.us-east-1.rds.amazonaws.com" },
                "AllocatedStorage": 100,
                "MultiAZ": true
            }]
        });

        let results = AwsResourceProvisioner::parse_rds_instances(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "prod-db");
        assert_eq!(r["resource_type"], "rds_postgres");
        assert_eq!(r["name"], "prod-db");
        assert_eq!(r["config"]["engine"], "postgres");
        assert_eq!(r["config"]["engine_version"], "16.1");
        assert_eq!(r["config"]["instance_class"], "db.r6g.large");
        assert_eq!(r["config"]["allocated_storage"], 100);
        assert_eq!(r["config"]["multi_az"], true);
        assert_eq!(
            r["outputs"]["endpoint"],
            "prod-db.abc.us-east-1.rds.amazonaws.com"
        );
    }

    #[test]
    fn test_parse_vpcs() {
        let json = serde_json::json!({
            "Vpcs": [{
                "VpcId": "vpc-main",
                "CidrBlock": "10.0.0.0/16",
                "State": "available",
                "Tags": [{ "Key": "Name", "Value": "production" }]
            }]
        });

        let results = AwsResourceProvisioner::parse_vpcs(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "vpc-main");
        assert_eq!(r["resource_type"], "vpc");
        assert_eq!(r["name"], "production");
        assert_eq!(r["config"]["cidr_block"], "10.0.0.0/16");
        assert_eq!(r["outputs"]["state"], "available");
    }

    #[test]
    fn test_parse_subnets() {
        let json = serde_json::json!({
            "Subnets": [{
                "SubnetId": "subnet-aaa",
                "VpcId": "vpc-111",
                "CidrBlock": "10.0.1.0/24",
                "AvailabilityZone": "us-east-1a",
                "Tags": [{ "Key": "Name", "Value": "public-1a" }]
            }]
        });

        let results = AwsResourceProvisioner::parse_subnets(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "subnet-aaa");
        assert_eq!(r["resource_type"], "aws_subnet");
        assert_eq!(r["name"], "public-1a");
        assert_eq!(r["config"]["vpc_id"], "vpc-111");
        assert_eq!(r["config"]["cidr_block"], "10.0.1.0/24");
        assert_eq!(r["config"]["availability_zone"], "us-east-1a");
    }

    #[test]
    fn test_parse_security_groups() {
        let json = serde_json::json!({
            "SecurityGroups": [{
                "GroupId": "sg-web",
                "GroupName": "web-sg",
                "VpcId": "vpc-111",
                "Description": "Web traffic"
            }]
        });

        let results = AwsResourceProvisioner::parse_security_groups(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "sg-web");
        assert_eq!(r["resource_type"], "security_group");
        assert_eq!(r["name"], "web-sg");
        assert_eq!(r["config"]["group_name"], "web-sg");
        assert_eq!(r["config"]["vpc_id"], "vpc-111");
        assert_eq!(r["config"]["description"], "Web traffic");
    }

    #[test]
    fn test_parse_s3_buckets() {
        let json = serde_json::json!({
            "Buckets": [
                { "Name": "my-app-assets", "CreationDate": "2024-01-15T10:30:00Z" },
                { "Name": "logs-bucket", "CreationDate": "2023-06-01T00:00:00Z" }
            ]
        });

        let results = AwsResourceProvisioner::parse_s3_buckets(&json);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["id"], "my-app-assets");
        assert_eq!(results[0]["resource_type"], "s3_bucket");
        assert_eq!(results[0]["name"], "my-app-assets");
        assert_eq!(
            results[0]["outputs"]["creation_date"],
            "2024-01-15T10:30:00Z"
        );
        assert_eq!(results[1]["id"], "logs-bucket");
    }

    #[test]
    fn test_parse_lambda_functions() {
        let json = serde_json::json!({
            "Functions": [{
                "FunctionName": "api-handler",
                "Runtime": "python3.12",
                "Handler": "main.handler",
                "MemorySize": 256,
                "Timeout": 30,
                "LastModified": "2025-03-10T12:00:00Z"
            }]
        });

        let results = AwsResourceProvisioner::parse_lambda_functions(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "api-handler");
        assert_eq!(r["resource_type"], "lambda");
        assert_eq!(r["name"], "api-handler");
        assert_eq!(r["config"]["runtime"], "python3.12");
        assert_eq!(r["config"]["handler"], "main.handler");
        assert_eq!(r["config"]["memory_size"], 256);
        assert_eq!(r["config"]["timeout"], 30);
        assert_eq!(r["outputs"]["last_modified"], "2025-03-10T12:00:00Z");
    }

    #[test]
    fn test_parse_missing_top_level_key() {
        // All parsers should return empty vec when the expected key is absent.
        let empty = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_ec2_instances(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_rds_instances(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_vpcs(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_subnets(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_security_groups(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_s3_buckets(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_lambda_functions(&empty).is_empty());
    }

    #[test]
    fn test_extract_name_from_tags() {
        let with_name = serde_json::json!({
            "Tags": [
                { "Key": "Env", "Value": "prod" },
                { "Key": "Name", "Value": "my-resource" }
            ]
        });
        assert_eq!(
            extract_name_from_tags(&with_name),
            Some("my-resource".to_string())
        );

        let without_name = serde_json::json!({
            "Tags": [{ "Key": "Env", "Value": "prod" }]
        });
        assert_eq!(extract_name_from_tags(&without_name), None);

        let no_tags = serde_json::json!({ "InstanceId": "i-123" });
        assert_eq!(extract_name_from_tags(&no_tags), None);
    }

    // ── EKS build-args tests ───────────────────────────────────────

    #[test]
    fn test_eks_cluster_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-cluster",
            "role_arn": "arn:aws:iam::123456789012:role/eks-role",
            "subnets": "subnet-aaa,subnet-bbb",
            "security_groups": "sg-111",
            "version": "1.29"
        });
        let args = p.build_create_args("eks_cluster", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"eks".to_string()));
        assert!(args.contains(&"create-cluster".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
        assert!(args.contains(&"--role-arn".to_string()));
        assert!(args.contains(&"arn:aws:iam::123456789012:role/eks-role".to_string()));
        assert!(args.contains(&"--resources-vpc-config".to_string()));
        assert!(
            args.contains(&"subnetIds=subnet-aaa,subnet-bbb,securityGroupIds=sg-111".to_string())
        );
        assert!(args.contains(&"--kubernetes-version".to_string()));
        assert!(args.contains(&"1.29".to_string()));
    }

    #[test]
    fn test_eks_cluster_build_args_default_version() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-ver",
            "role_arn": "arn:aws:iam::123456789012:role/eks-role",
            "subnets": "subnet-aaa",
            "security_groups": "sg-111"
        });
        let args = p.build_create_args("eks_cluster", &params).unwrap();

        assert!(args.contains(&"--kubernetes-version".to_string()));
        assert!(args.contains(&"1.30".to_string()));
    }

    #[test]
    fn test_eks_nodegroup_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-west-2"), None);
        let params = serde_json::json!({
            "id": "workers",
            "cluster": "my-cluster",
            "node_role": "arn:aws:iam::123456789012:role/node-role",
            "subnets": "subnet-aaa,subnet-bbb",
            "instance_types": "t3.large",
            "min": "2",
            "max": "5",
            "desired": "3"
        });
        let args = p.build_create_args("eks_nodegroup", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"eks".to_string()));
        assert!(args.contains(&"create-nodegroup".to_string()));
        assert!(args.contains(&"--cluster-name".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
        assert!(args.contains(&"--nodegroup-name".to_string()));
        assert!(args.contains(&"workers".to_string()));
        assert!(args.contains(&"--node-role".to_string()));
        assert!(args.contains(&"--subnets".to_string()));
        assert!(args.contains(&"subnet-aaa,subnet-bbb".to_string()));
        assert!(args.contains(&"--instance-types".to_string()));
        assert!(args.contains(&"t3.large".to_string()));
        assert!(args.contains(&"--scaling-config".to_string()));
        assert!(args.contains(&"minSize=2,maxSize=5,desiredSize=3".to_string()));
    }

    #[test]
    fn test_eks_nodegroup_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "ng-default",
            "cluster": "my-cluster",
            "node_role": "arn:aws:iam::123456789012:role/node-role",
            "subnets": "subnet-aaa"
        });
        let args = p.build_create_args("eks_nodegroup", &params).unwrap();

        assert!(args.contains(&"t3.medium".to_string()));
        assert!(args.contains(&"minSize=1,maxSize=3,desiredSize=2".to_string()));
    }

    #[test]
    fn test_eks_addon_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "vpc-cni",
            "cluster": "my-cluster",
            "version": "v1.15.0"
        });
        let args = p.build_create_args("eks_addon", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"eks".to_string()));
        assert!(args.contains(&"create-addon".to_string()));
        assert!(args.contains(&"--cluster-name".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
        assert!(args.contains(&"--addon-name".to_string()));
        assert!(args.contains(&"vpc-cni".to_string()));
        assert!(args.contains(&"--addon-version".to_string()));
        assert!(args.contains(&"v1.15.0".to_string()));
    }

    #[test]
    fn test_eks_addon_build_args_no_version() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "coredns",
            "cluster": "my-cluster"
        });
        let args = p.build_create_args("eks_addon", &params).unwrap();

        assert!(args.contains(&"create-addon".to_string()));
        assert!(args.contains(&"coredns".to_string()));
        assert!(!args.contains(&"--addon-version".to_string()));
    }

    // ── S3 / KMS build-args tests ──────────────────────────────────

    #[test]
    fn test_s3_bucket_build_args_us_east_1() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-bucket"
        });
        let args = p.build_create_args("s3_bucket", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"s3api".to_string()));
        assert!(args.contains(&"create-bucket".to_string()));
        assert!(args.contains(&"--bucket".to_string()));
        assert!(args.contains(&"my-bucket".to_string()));
        // us-east-1 should NOT have LocationConstraint
        assert!(!args.iter().any(|a| a.contains("LocationConstraint")));
    }

    #[test]
    fn test_s3_bucket_build_args_non_us_east_1() {
        let p = AwsResourceProvisioner::new(Some("eu-west-1"), None);
        let params = serde_json::json!({
            "id": "eu-bucket"
        });
        let args = p.build_create_args("s3_bucket", &params).unwrap();

        assert!(args.contains(&"s3api".to_string()));
        assert!(args.contains(&"create-bucket".to_string()));
        assert!(args.contains(&"--bucket".to_string()));
        assert!(args.contains(&"eu-bucket".to_string()));
        assert!(args.contains(&"--create-bucket-configuration".to_string()));
        assert!(args.contains(&"LocationConstraint=eu-west-1".to_string()));
    }

    #[test]
    fn test_s3_bucket_build_args_no_region() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "no-region-bucket"
        });
        let args = p.build_create_args("s3_bucket", &params).unwrap();

        assert!(args.contains(&"create-bucket".to_string()));
        assert!(args.contains(&"no-region-bucket".to_string()));
        // No region set, so no LocationConstraint
        assert!(!args.iter().any(|a| a.contains("LocationConstraint")));
    }

    #[test]
    fn test_kms_key_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-key",
            "description": "Encryption key for app data",
            "usage": "ENCRYPT_DECRYPT"
        });
        let args = p.build_create_args("kms_key", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"kms".to_string()));
        assert!(args.contains(&"create-key".to_string()));
        assert!(args.contains(&"--key-usage".to_string()));
        assert!(args.contains(&"ENCRYPT_DECRYPT".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"Encryption key for app data".to_string()));
    }

    #[test]
    fn test_kms_key_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-key"
        });
        let args = p.build_create_args("kms_key", &params).unwrap();

        assert!(args.contains(&"create-key".to_string()));
        assert!(args.contains(&"--key-usage".to_string()));
        assert!(args.contains(&"ENCRYPT_DECRYPT".to_string()));
        // No description param, so --description should not appear
        assert!(!args.contains(&"--description".to_string()));
    }

    // ── Delete-args tests for new types ─────────────────────────────

    #[test]
    fn test_delete_eks_cluster() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let args = p.build_delete_args("eks_cluster", "my-cluster").unwrap();
        assert!(args.contains(&"eks".to_string()));
        assert!(args.contains(&"delete-cluster".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
    }

    #[test]
    fn test_delete_eks_nodegroup_requires_params() {
        let p = AwsResourceProvisioner::new(None, None);
        let err = p.build_delete_args("eks_nodegroup", "ng-1").unwrap_err();
        assert!(err.to_string().contains("cluster"));
    }

    #[test]
    fn test_delete_eks_addon_requires_params() {
        let p = AwsResourceProvisioner::new(None, None);
        let err = p.build_delete_args("eks_addon", "vpc-cni").unwrap_err();
        assert!(err.to_string().contains("cluster"));
    }

    #[test]
    fn test_delete_s3_bucket() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("s3_bucket", "my-bucket").unwrap();
        assert!(args.contains(&"s3api".to_string()));
        assert!(args.contains(&"delete-bucket".to_string()));
        assert!(args.contains(&"--bucket".to_string()));
        assert!(args.contains(&"my-bucket".to_string()));
    }

    #[test]
    fn test_delete_kms_key() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("kms_key", "key-123").unwrap();
        assert!(args.contains(&"kms".to_string()));
        assert!(args.contains(&"schedule-key-deletion".to_string()));
        assert!(args.contains(&"--key-id".to_string()));
        assert!(args.contains(&"key-123".to_string()));
        assert!(args.contains(&"--pending-window-in-days".to_string()));
        assert!(args.contains(&"7".to_string()));
    }

    // ── ElastiCache / MSK build-args tests ────────────────────────────

    #[test]
    fn test_elasticache_redis_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-redis",
            "node_type": "cache.r6g.large",
            "num_nodes": "2",
            "version": "7.0"
        });
        let args = p.build_create_args("elasticache_redis", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"elasticache".to_string()));
        assert!(args.contains(&"create-cache-cluster".to_string()));
        assert!(args.contains(&"--cache-cluster-id".to_string()));
        assert!(args.contains(&"my-redis".to_string()));
        assert!(args.contains(&"--engine".to_string()));
        assert!(args.contains(&"redis".to_string()));
        assert!(args.contains(&"--cache-node-type".to_string()));
        assert!(args.contains(&"cache.r6g.large".to_string()));
        assert!(args.contains(&"--num-cache-nodes".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"--engine-version".to_string()));
        assert!(args.contains(&"7.0".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
    }

    #[test]
    fn test_elasticache_redis_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-redis"
        });
        let args = p.build_create_args("elasticache_redis", &params).unwrap();

        assert!(args.contains(&"create-cache-cluster".to_string()));
        assert!(args.contains(&"default-redis".to_string()));
        assert!(args.contains(&"cache.t3.micro".to_string()));
        assert!(args.contains(&"1".to_string()));
        // No version param, so --engine-version should not appear
        assert!(!args.contains(&"--engine-version".to_string()));
    }

    #[test]
    fn test_elasticache_replication_group_build_args() {
        let p = AwsResourceProvisioner::new(Some("eu-west-1"), None);
        let params = serde_json::json!({
            "id": "my-repl-group",
            "description": "Production Redis cluster",
            "node_type": "cache.r6g.xlarge",
            "num_shards": "3",
            "replicas": "2",
            "version": "7.0"
        });
        let args = p
            .build_create_args("elasticache_replication_group", &params)
            .unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"elasticache".to_string()));
        assert!(args.contains(&"create-replication-group".to_string()));
        assert!(args.contains(&"--replication-group-id".to_string()));
        assert!(args.contains(&"my-repl-group".to_string()));
        assert!(args.contains(&"--replication-group-description".to_string()));
        assert!(args.contains(&"Production Redis cluster".to_string()));
        assert!(args.contains(&"--cache-node-type".to_string()));
        assert!(args.contains(&"cache.r6g.xlarge".to_string()));
        assert!(args.contains(&"--num-node-groups".to_string()));
        assert!(args.contains(&"3".to_string()));
        assert!(args.contains(&"--replicas-per-node-group".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"--engine-version".to_string()));
        assert!(args.contains(&"7.0".to_string()));
    }

    #[test]
    fn test_elasticache_replication_group_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-repl"
        });
        let args = p
            .build_create_args("elasticache_replication_group", &params)
            .unwrap();

        assert!(args.contains(&"create-replication-group".to_string()));
        assert!(args.contains(&"default-repl".to_string()));
        assert!(args.contains(&"Orbi managed".to_string()));
        assert!(args.contains(&"cache.t3.micro".to_string()));
        assert!(args.contains(&"--num-node-groups".to_string()));
        assert!(args.contains(&"--replicas-per-node-group".to_string()));
        assert!(!args.contains(&"--engine-version".to_string()));
    }

    #[test]
    fn test_msk_cluster_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-kafka",
            "version": "3.5.1",
            "broker_count": "6",
            "subnets": "subnet-aaa,subnet-bbb,subnet-ccc",
            "instance_type": "kafka.m5.2xlarge",
            "security_groups": "sg-111,sg-222",
            "monitoring": "PER_TOPIC_PER_BROKER"
        });
        let args = p.build_create_args("msk_cluster", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"kafka".to_string()));
        assert!(args.contains(&"create-cluster".to_string()));
        assert!(args.contains(&"--cluster-name".to_string()));
        assert!(args.contains(&"my-kafka".to_string()));
        assert!(args.contains(&"--kafka-version".to_string()));
        assert!(args.contains(&"3.5.1".to_string()));
        assert!(args.contains(&"--number-of-broker-nodes".to_string()));
        assert!(args.contains(&"6".to_string()));
        assert!(args.contains(&"--broker-node-group-info".to_string()));
        assert!(args.contains(&"--enhanced-monitoring".to_string()));
        assert!(args.contains(&"PER_TOPIC_PER_BROKER".to_string()));

        // Verify broker-node-group-info contains the expected JSON fields
        let broker_info_arg = args
            .iter()
            .find(|a| a.contains("ClientSubnets"))
            .expect("broker-node-group-info JSON not found");
        let broker_info: Value = serde_json::from_str(broker_info_arg).unwrap();
        assert_eq!(
            broker_info["ClientSubnets"],
            serde_json::json!(["subnet-aaa", "subnet-bbb", "subnet-ccc"])
        );
        assert_eq!(broker_info["InstanceType"], "kafka.m5.2xlarge");
        assert_eq!(
            broker_info["SecurityGroups"],
            serde_json::json!(["sg-111", "sg-222"])
        );
    }

    #[test]
    fn test_msk_cluster_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-kafka",
            "subnets": "subnet-aaa",
            "security_groups": "sg-111"
        });
        let args = p.build_create_args("msk_cluster", &params).unwrap();

        assert!(args.contains(&"create-cluster".to_string()));
        assert!(args.contains(&"default-kafka".to_string()));
        assert!(args.contains(&"3.6.0".to_string()));
        assert!(args.contains(&"3".to_string()));
        // No monitoring param, so --enhanced-monitoring should not appear
        assert!(!args.contains(&"--enhanced-monitoring".to_string()));

        let broker_info_arg = args
            .iter()
            .find(|a| a.contains("ClientSubnets"))
            .expect("broker-node-group-info JSON not found");
        let broker_info: Value = serde_json::from_str(broker_info_arg).unwrap();
        assert_eq!(broker_info["InstanceType"], "kafka.m5.large");
    }

    #[test]
    fn test_delete_elasticache() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let args = p
            .build_delete_args("elasticache_redis", "my-redis")
            .unwrap();
        assert!(args.contains(&"elasticache".to_string()));
        assert!(args.contains(&"delete-cache-cluster".to_string()));
        assert!(args.contains(&"--cache-cluster-id".to_string()));
        assert!(args.contains(&"my-redis".to_string()));

        let args = p
            .build_delete_args("elasticache_replication_group", "my-repl")
            .unwrap();
        assert!(args.contains(&"elasticache".to_string()));
        assert!(args.contains(&"delete-replication-group".to_string()));
        assert!(args.contains(&"--replication-group-id".to_string()));
        assert!(args.contains(&"my-repl".to_string()));
    }

    #[test]
    fn test_delete_msk() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args(
                "msk_cluster",
                "arn:aws:kafka:us-east-1:123:cluster/my-kafka/abc",
            )
            .unwrap();
        assert!(args.contains(&"kafka".to_string()));
        assert!(args.contains(&"delete-cluster".to_string()));
        assert!(args.contains(&"--cluster-arn".to_string()));
        assert!(args.contains(&"arn:aws:kafka:us-east-1:123:cluster/my-kafka/abc".to_string()));
    }

    // ── ElastiCache / MSK discovery parsing tests ─────────────────────

    #[test]
    fn test_parse_elasticache_clusters() {
        let json = serde_json::json!({
            "CacheClusters": [{
                "CacheClusterId": "prod-redis",
                "Engine": "redis",
                "EngineVersion": "7.0.7",
                "CacheNodeType": "cache.r6g.large",
                "CacheClusterStatus": "available",
                "NumCacheNodes": 1,
                "ConfigurationEndpoint": {
                    "Address": "prod-redis.abc.cfg.use1.cache.amazonaws.com",
                    "Port": 6379
                }
            }]
        });

        let results = AwsResourceProvisioner::parse_elasticache_clusters(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "prod-redis");
        assert_eq!(r["resource_type"], "elasticache_redis");
        assert_eq!(r["name"], "prod-redis");
        assert_eq!(r["config"]["engine"], "redis");
        assert_eq!(r["config"]["engine_version"], "7.0.7");
        assert_eq!(r["config"]["cache_node_type"], "cache.r6g.large");
        assert_eq!(r["outputs"]["status"], "available");
    }

    #[test]
    fn test_parse_elasticache_replication_group() {
        let json = serde_json::json!({
            "CacheClusters": [{
                "CacheClusterId": "repl-001",
                "Engine": "redis",
                "EngineVersion": "7.0.7",
                "CacheNodeType": "cache.r6g.large",
                "CacheClusterStatus": "available",
                "NumCacheNodes": 1,
                "ReplicationGroupId": "my-repl-group"
            }]
        });

        let results = AwsResourceProvisioner::parse_elasticache_clusters(&json);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["resource_type"], "elasticache_replication_group");
        assert_eq!(
            results[0]["config"]["replication_group_id"],
            "my-repl-group"
        );
    }

    #[test]
    fn test_parse_msk_clusters() {
        let json = serde_json::json!({
            "ClusterInfoList": [{
                "ClusterArn": "arn:aws:kafka:us-east-1:123:cluster/prod-kafka/abc-123",
                "ClusterName": "prod-kafka",
                "ClusterType": "PROVISIONED",
                "State": "ACTIVE"
            }]
        });

        let results = AwsResourceProvisioner::parse_msk_clusters(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(
            r["id"],
            "arn:aws:kafka:us-east-1:123:cluster/prod-kafka/abc-123"
        );
        assert_eq!(r["resource_type"], "msk_cluster");
        assert_eq!(r["name"], "prod-kafka");
        assert_eq!(r["config"]["cluster_name"], "prod-kafka");
        assert_eq!(r["config"]["cluster_type"], "PROVISIONED");
        assert_eq!(r["outputs"]["state"], "ACTIVE");
    }

    #[test]
    fn test_parse_elasticache_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_elasticache_clusters(&json).is_empty());
    }

    #[test]
    fn test_parse_msk_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_msk_clusters(&json).is_empty());
    }

    // ── IAM build-args tests ──────────────────────────────────────────

    #[test]
    fn test_iam_role_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-role",
            "trust_policy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            "description": "Test role"
        });
        let args = p.build_create_args("iam_role", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"create-role".to_string()));
        assert!(args.contains(&"--role-name".to_string()));
        assert!(args.contains(&"my-role".to_string()));
        assert!(args.contains(&"--assume-role-policy-document".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"Test role".to_string()));
    }

    #[test]
    fn test_iam_role_build_args_no_description() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "minimal-role",
            "trust_policy": "{}"
        });
        let args = p.build_create_args("iam_role", &params).unwrap();

        assert!(args.contains(&"create-role".to_string()));
        assert!(args.contains(&"minimal-role".to_string()));
        assert!(!args.contains(&"--description".to_string()));
    }

    #[test]
    fn test_iam_policy_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-policy",
            "policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            "description": "Test policy"
        });
        let args = p.build_create_args("iam_policy", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"create-policy".to_string()));
        assert!(args.contains(&"--policy-name".to_string()));
        assert!(args.contains(&"my-policy".to_string()));
        assert!(args.contains(&"--policy-document".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"Test policy".to_string()));
    }

    #[test]
    fn test_iam_policy_build_args_no_description() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "minimal-policy",
            "policy_document": "{}"
        });
        let args = p.build_create_args("iam_policy", &params).unwrap();

        assert!(args.contains(&"create-policy".to_string()));
        assert!(args.contains(&"minimal-policy".to_string()));
        assert!(!args.contains(&"--description".to_string()));
    }

    #[test]
    fn test_delete_iam_role() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("iam_role", "my-role").unwrap();
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"delete-role".to_string()));
        assert!(args.contains(&"--role-name".to_string()));
        assert!(args.contains(&"my-role".to_string()));
    }

    #[test]
    fn test_delete_iam_policy() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("iam_policy", "arn:aws:iam::123456789012:policy/my-policy")
            .unwrap();
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"delete-policy".to_string()));
        assert!(args.contains(&"--policy-arn".to_string()));
        assert!(args.contains(&"arn:aws:iam::123456789012:policy/my-policy".to_string()));
    }

    // ── Network build-args tests ──────────────────────────────────────

    #[test]
    fn test_vpc_endpoint_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "s3-endpoint",
            "vpc_id": "vpc-12345",
            "service_name": "com.amazonaws.us-east-1.s3",
            "type": "Gateway"
        });
        let args = p.build_create_args("vpc_endpoint", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-vpc-endpoint".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-12345".to_string()));
        assert!(args.contains(&"--service-name".to_string()));
        assert!(args.contains(&"com.amazonaws.us-east-1.s3".to_string()));
        assert!(args.contains(&"--vpc-endpoint-type".to_string()));
        assert!(args.contains(&"Gateway".to_string()));
    }

    #[test]
    fn test_vpc_endpoint_build_args_interface() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "ssm-endpoint",
            "vpc_id": "vpc-999",
            "service_name": "com.amazonaws.us-east-1.ssm",
            "type": "Interface",
            "subnet_ids": "subnet-aaa,subnet-bbb",
            "security_group_ids": "sg-111"
        });
        let args = p.build_create_args("vpc_endpoint", &params).unwrap();

        assert!(args.contains(&"Interface".to_string()));
        assert!(args.contains(&"--subnet-ids".to_string()));
        assert!(args.contains(&"subnet-aaa,subnet-bbb".to_string()));
        assert!(args.contains(&"--security-group-ids".to_string()));
        assert!(args.contains(&"sg-111".to_string()));
    }

    #[test]
    fn test_vpc_endpoint_build_args_default_type() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "ep-default",
            "vpc_id": "vpc-111",
            "service_name": "com.amazonaws.us-east-1.s3"
        });
        let args = p.build_create_args("vpc_endpoint", &params).unwrap();

        assert!(args.contains(&"Gateway".to_string()));
        assert!(!args.contains(&"--subnet-ids".to_string()));
        assert!(!args.contains(&"--security-group-ids".to_string()));
    }

    #[test]
    fn test_nat_gateway_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-west-2"), None);
        let params = serde_json::json!({
            "id": "nat-pub",
            "subnet_id": "subnet-pub-1a",
            "allocation_id": "eipalloc-abc123"
        });
        let args = p.build_create_args("nat_gateway", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-nat-gateway".to_string()));
        assert!(args.contains(&"--subnet-id".to_string()));
        assert!(args.contains(&"subnet-pub-1a".to_string()));
        assert!(args.contains(&"--allocation-id".to_string()));
        assert!(args.contains(&"eipalloc-abc123".to_string()));
    }

    #[test]
    fn test_delete_vpc_endpoint() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("vpc_endpoint", "vpce-12345").unwrap();
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"delete-vpc-endpoints".to_string()));
        assert!(args.contains(&"--vpc-endpoint-ids".to_string()));
        assert!(args.contains(&"vpce-12345".to_string()));
    }

    #[test]
    fn test_delete_nat_gateway() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("nat_gateway", "nat-abc123").unwrap();
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"delete-nat-gateway".to_string()));
        assert!(args.contains(&"--nat-gateway-id".to_string()));
        assert!(args.contains(&"nat-abc123".to_string()));
    }

    // ── ACM build-args tests ──────────────────────────────────────────

    #[test]
    fn test_acm_certificate_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "example.com",
            "validation": "DNS",
            "san": "*.example.com,api.example.com"
        });
        let args = p.build_create_args("acm_certificate", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"acm".to_string()));
        assert!(args.contains(&"request-certificate".to_string()));
        assert!(args.contains(&"--domain-name".to_string()));
        assert!(args.contains(&"example.com".to_string()));
        assert!(args.contains(&"--validation-method".to_string()));
        assert!(args.contains(&"DNS".to_string()));
        assert!(args.contains(&"--subject-alternative-names".to_string()));
        assert!(args.contains(&"*.example.com,api.example.com".to_string()));
    }

    #[test]
    fn test_acm_certificate_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "simple.com"
        });
        let args = p.build_create_args("acm_certificate", &params).unwrap();

        assert!(args.contains(&"request-certificate".to_string()));
        assert!(args.contains(&"simple.com".to_string()));
        assert!(args.contains(&"DNS".to_string()));
        assert!(!args.contains(&"--subject-alternative-names".to_string()));
    }

    #[test]
    fn test_delete_acm_certificate() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args(
                "acm_certificate",
                "arn:aws:acm:us-east-1:123:certificate/abc-123",
            )
            .unwrap();
        assert!(args.contains(&"acm".to_string()));
        assert!(args.contains(&"delete-certificate".to_string()));
        assert!(args.contains(&"--certificate-arn".to_string()));
        assert!(args.contains(&"arn:aws:acm:us-east-1:123:certificate/abc-123".to_string()));
    }

    // ── CloudWatch build-args tests ───────────────────────────────────

    #[test]
    fn test_cloudwatch_alarm_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "high-cpu",
            "metric": "CPUUtilization",
            "namespace": "AWS/EC2",
            "statistic": "Average",
            "period": "300",
            "eval_periods": "2",
            "threshold": "80",
            "operator": "GreaterThanOrEqualToThreshold"
        });
        let args = p.build_create_args("cloudwatch_alarm", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"cloudwatch".to_string()));
        assert!(args.contains(&"put-metric-alarm".to_string()));
        assert!(args.contains(&"--alarm-name".to_string()));
        assert!(args.contains(&"high-cpu".to_string()));
        assert!(args.contains(&"--metric-name".to_string()));
        assert!(args.contains(&"CPUUtilization".to_string()));
        assert!(args.contains(&"--namespace".to_string()));
        assert!(args.contains(&"AWS/EC2".to_string()));
        assert!(args.contains(&"--statistic".to_string()));
        assert!(args.contains(&"Average".to_string()));
        assert!(args.contains(&"--period".to_string()));
        assert!(args.contains(&"300".to_string()));
        assert!(args.contains(&"--evaluation-periods".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"--threshold".to_string()));
        assert!(args.contains(&"80".to_string()));
        assert!(args.contains(&"--comparison-operator".to_string()));
        assert!(args.contains(&"GreaterThanOrEqualToThreshold".to_string()));
    }

    #[test]
    fn test_cloudwatch_alarm_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-alarm",
            "metric": "Errors",
            "namespace": "AWS/Lambda",
            "threshold": "5"
        });
        let args = p.build_create_args("cloudwatch_alarm", &params).unwrap();

        assert!(args.contains(&"put-metric-alarm".to_string()));
        assert!(args.contains(&"default-alarm".to_string()));
        assert!(args.contains(&"Average".to_string()));
        assert!(args.contains(&"300".to_string()));
        assert!(args.contains(&"1".to_string()));
        assert!(args.contains(&"GreaterThanThreshold".to_string()));
    }

    #[test]
    fn test_delete_cloudwatch_alarm() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("cloudwatch_alarm", "high-cpu").unwrap();
        assert!(args.contains(&"cloudwatch".to_string()));
        assert!(args.contains(&"delete-alarms".to_string()));
        assert!(args.contains(&"--alarm-names".to_string()));
        assert!(args.contains(&"high-cpu".to_string()));
    }

    // ── IAM / VPC endpoint discovery parsing tests ────────────────────

    #[test]
    fn test_parse_iam_roles() {
        let json = serde_json::json!({
            "Roles": [{
                "RoleName": "eks-cluster-role",
                "Path": "/",
                "Arn": "arn:aws:iam::123456789012:role/eks-cluster-role",
                "CreateDate": "2025-01-10T12:00:00Z",
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": []
                }
            }]
        });

        let results = AwsResourceProvisioner::parse_iam_roles(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "eks-cluster-role");
        assert_eq!(r["resource_type"], "iam_role");
        assert_eq!(r["name"], "eks-cluster-role");
        assert_eq!(
            r["outputs"]["arn"],
            "arn:aws:iam::123456789012:role/eks-cluster-role"
        );
        assert_eq!(r["outputs"]["create_date"], "2025-01-10T12:00:00Z");
    }

    #[test]
    fn test_parse_iam_roles_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_iam_roles(&json).is_empty());
    }

    #[test]
    fn test_parse_vpc_endpoints() {
        let json = serde_json::json!({
            "VpcEndpoints": [{
                "VpcEndpointId": "vpce-abc123",
                "VpcId": "vpc-111",
                "ServiceName": "com.amazonaws.us-east-1.s3",
                "VpcEndpointType": "Gateway",
                "State": "available",
                "Tags": [{ "Key": "Name", "Value": "s3-gateway" }]
            }]
        });

        let results = AwsResourceProvisioner::parse_vpc_endpoints(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "vpce-abc123");
        assert_eq!(r["resource_type"], "vpc_endpoint");
        assert_eq!(r["name"], "s3-gateway");
        assert_eq!(r["config"]["vpc_id"], "vpc-111");
        assert_eq!(r["config"]["service_name"], "com.amazonaws.us-east-1.s3");
        assert_eq!(r["outputs"]["state"], "available");
    }

    #[test]
    fn test_parse_vpc_endpoints_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_vpc_endpoints(&json).is_empty());
    }

    #[test]
    fn test_parse_missing_top_level_key_new_types() {
        let empty = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_iam_roles(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_vpc_endpoints(&empty).is_empty());
    }

    // ── SES build-args tests ─────────────────────────────────────────

    #[test]
    fn test_ses_domain_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "example.com"
        });
        let args = p.build_create_args("ses_domain", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ses".to_string()));
        assert!(args.contains(&"verify-domain-identity".to_string()));
        assert!(args.contains(&"--domain".to_string()));
        assert!(args.contains(&"example.com".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
    }

    #[test]
    fn test_delete_ses_domain() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("ses_domain", "example.com").unwrap();
        assert!(args.contains(&"ses".to_string()));
        assert!(args.contains(&"delete-identity".to_string()));
        assert!(args.contains(&"--identity".to_string()));
        assert!(args.contains(&"example.com".to_string()));
    }

    #[test]
    fn test_ses_smtp_user_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "ses-smtp-prod"
        });
        let args = p.build_create_args("ses_smtp_user", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"create-user".to_string()));
        assert!(args.contains(&"--user-name".to_string()));
        assert!(args.contains(&"ses-smtp-prod".to_string()));
    }

    #[test]
    fn test_delete_ses_smtp_user() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("ses_smtp_user", "ses-smtp-prod")
            .unwrap();
        assert!(args.contains(&"iam".to_string()));
        assert!(args.contains(&"delete-user".to_string()));
        assert!(args.contains(&"--user-name".to_string()));
        assert!(args.contains(&"ses-smtp-prod".to_string()));
    }

    // ── Backup build-args tests ──────────────────────────────────────

    #[test]
    fn test_backup_vault_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-vault",
            "encryption_key": "arn:aws:kms:us-east-1:123:key/abc-123"
        });
        let args = p.build_create_args("backup_vault", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"backup".to_string()));
        assert!(args.contains(&"create-backup-vault".to_string()));
        assert!(args.contains(&"--backup-vault-name".to_string()));
        assert!(args.contains(&"my-vault".to_string()));
        assert!(args.contains(&"--encryption-key-arn".to_string()));
        assert!(args.contains(&"arn:aws:kms:us-east-1:123:key/abc-123".to_string()));
    }

    #[test]
    fn test_backup_vault_build_args_no_encryption() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "simple-vault"
        });
        let args = p.build_create_args("backup_vault", &params).unwrap();

        assert!(args.contains(&"create-backup-vault".to_string()));
        assert!(args.contains(&"simple-vault".to_string()));
        assert!(!args.contains(&"--encryption-key-arn".to_string()));
    }

    #[test]
    fn test_delete_backup_vault() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("backup_vault", "my-vault").unwrap();
        assert!(args.contains(&"backup".to_string()));
        assert!(args.contains(&"delete-backup-vault".to_string()));
        assert!(args.contains(&"--backup-vault-name".to_string()));
        assert!(args.contains(&"my-vault".to_string()));
    }

    #[test]
    fn test_backup_plan_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "daily-backup",
            "vault": "my-vault",
            "retention_days": "90",
            "schedule": "cron(0 12 ? * * *)"
        });
        let args = p.build_create_args("backup_plan", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"backup".to_string()));
        assert!(args.contains(&"create-backup-plan".to_string()));
        assert!(args.contains(&"--backup-plan".to_string()));

        // Verify the plan JSON contains expected fields
        let plan_arg = args
            .iter()
            .find(|a| a.contains("BackupPlanName"))
            .expect("backup plan JSON not found");
        let plan: Value = serde_json::from_str(plan_arg).unwrap();
        assert_eq!(plan["BackupPlanName"], "daily-backup");
        let rule = &plan["Rules"][0];
        assert_eq!(rule["RuleName"], "daily");
        assert_eq!(rule["TargetBackupVaultName"], "my-vault");
        assert_eq!(rule["ScheduleExpression"], "cron(0 12 ? * * *)");
        assert_eq!(rule["Lifecycle"]["DeleteAfterDays"], 90);
    }

    #[test]
    fn test_backup_plan_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-plan",
            "vault": "default-vault"
        });
        let args = p.build_create_args("backup_plan", &params).unwrap();

        assert!(args.contains(&"create-backup-plan".to_string()));

        let plan_arg = args
            .iter()
            .find(|a| a.contains("BackupPlanName"))
            .expect("backup plan JSON not found");
        let plan: Value = serde_json::from_str(plan_arg).unwrap();
        assert_eq!(plan["BackupPlanName"], "default-plan");
        let rule = &plan["Rules"][0];
        assert_eq!(rule["TargetBackupVaultName"], "default-vault");
        assert_eq!(rule["ScheduleExpression"], "cron(0 5 ? * * *)");
        assert_eq!(rule["Lifecycle"]["DeleteAfterDays"], 30);
    }

    #[test]
    fn test_delete_backup_plan() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("backup_plan", "plan-abc-123").unwrap();
        assert!(args.contains(&"backup".to_string()));
        assert!(args.contains(&"delete-backup-plan".to_string()));
        assert!(args.contains(&"--backup-plan-id".to_string()));
        assert!(args.contains(&"plan-abc-123".to_string()));
    }

    // ── Discovery parse tests for v0.6.2–v0.6.3 resource types ──────

    #[test]
    fn test_parse_nat_gateways() {
        let json = serde_json::json!({
            "NatGateways": [{
                "NatGatewayId": "nat-0123456789abcdef0",
                "VpcId": "vpc-111",
                "SubnetId": "subnet-222",
                "ConnectivityType": "public",
                "State": "available",
                "NatGatewayAddresses": [{"PublicIp": "52.1.2.3"}],
                "Tags": [{ "Key": "Name", "Value": "main-nat" }]
            }]
        });

        let results = AwsResourceProvisioner::parse_nat_gateways(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "nat-0123456789abcdef0");
        assert_eq!(r["resource_type"], "nat_gateway");
        assert_eq!(r["name"], "main-nat");
        assert_eq!(r["config"]["vpc_id"], "vpc-111");
        assert_eq!(r["config"]["subnet_id"], "subnet-222");
        assert_eq!(r["outputs"]["state"], "available");
    }

    #[test]
    fn test_parse_nat_gateways_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_nat_gateways(&json).is_empty());
    }

    #[test]
    fn test_parse_kms_keys() {
        let json = serde_json::json!({
            "Keys": [{
                "KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab",
                "KeyArn": "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
            }]
        });

        let results = AwsResourceProvisioner::parse_kms_keys(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "1234abcd-12ab-34cd-56ef-1234567890ab");
        assert_eq!(r["resource_type"], "kms_key");
        assert_eq!(
            r["outputs"]["key_arn"],
            "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
        );
    }

    #[test]
    fn test_parse_kms_keys_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_kms_keys(&json).is_empty());
    }

    #[test]
    fn test_parse_eks_nodegroups() {
        let json = serde_json::json!({
            "nodegroups": ["workers", "gpu-pool"]
        });

        let results = AwsResourceProvisioner::parse_eks_nodegroups(&json, "prod-cluster");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["id"], "prod-cluster/workers");
        assert_eq!(results[0]["resource_type"], "eks_nodegroup");
        assert_eq!(results[0]["name"], "workers");
        assert_eq!(results[0]["config"]["cluster"], "prod-cluster");
        assert_eq!(results[1]["id"], "prod-cluster/gpu-pool");
        assert_eq!(results[1]["name"], "gpu-pool");
    }

    #[test]
    fn test_parse_eks_nodegroups_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_eks_nodegroups(&json, "c").is_empty());
    }

    #[test]
    fn test_parse_eks_addons() {
        let json = serde_json::json!({
            "addons": ["vpc-cni", "coredns", "kube-proxy"]
        });

        let results = AwsResourceProvisioner::parse_eks_addons(&json, "my-cluster");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["id"], "my-cluster/vpc-cni");
        assert_eq!(results[0]["resource_type"], "eks_addon");
        assert_eq!(results[0]["name"], "vpc-cni");
        assert_eq!(results[0]["config"]["cluster"], "my-cluster");
        assert_eq!(results[2]["name"], "kube-proxy");
    }

    #[test]
    fn test_parse_eks_addons_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_eks_addons(&json, "c").is_empty());
    }

    #[test]
    fn test_parse_acm_certificates() {
        let json = serde_json::json!({
            "CertificateSummaryList": [{
                "CertificateArn": "arn:aws:acm:us-east-1:123:certificate/abc-123",
                "DomainName": "example.com",
                "Status": "ISSUED",
                "Type": "AMAZON_ISSUED"
            }]
        });

        let results = AwsResourceProvisioner::parse_acm_certificates(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "arn:aws:acm:us-east-1:123:certificate/abc-123");
        assert_eq!(r["resource_type"], "acm_certificate");
        assert_eq!(r["name"], "example.com");
        assert_eq!(r["config"]["domain_name"], "example.com");
        assert_eq!(r["outputs"]["status"], "ISSUED");
    }

    #[test]
    fn test_parse_acm_certificates_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_acm_certificates(&json).is_empty());
    }

    #[test]
    fn test_parse_backup_vaults() {
        let json = serde_json::json!({
            "BackupVaultList": [{
                "BackupVaultName": "default-vault",
                "BackupVaultArn": "arn:aws:backup:us-east-1:123:backup-vault:default-vault",
                "EncryptionKeyArn": "arn:aws:kms:us-east-1:123:key/abc",
                "CreationDate": "2025-01-01T00:00:00Z",
                "NumberOfRecoveryPoints": 42
            }]
        });

        let results = AwsResourceProvisioner::parse_backup_vaults(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "default-vault");
        assert_eq!(r["resource_type"], "backup_vault");
        assert_eq!(r["name"], "default-vault");
        assert_eq!(
            r["config"]["encryption_key_arn"],
            "arn:aws:kms:us-east-1:123:key/abc"
        );
        assert_eq!(r["outputs"]["number_of_recovery_points"], 42);
    }

    #[test]
    fn test_parse_backup_vaults_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_backup_vaults(&json).is_empty());
    }

    #[test]
    fn test_parse_backup_plans() {
        let json = serde_json::json!({
            "BackupPlansList": [{
                "BackupPlanId": "plan-abc-123",
                "BackupPlanName": "daily-backups",
                "BackupPlanArn": "arn:aws:backup:us-east-1:123:backup-plan:plan-abc-123",
                "CreationDate": "2025-06-01T00:00:00Z",
                "VersionId": "v1"
            }]
        });

        let results = AwsResourceProvisioner::parse_backup_plans(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "plan-abc-123");
        assert_eq!(r["resource_type"], "backup_plan");
        assert_eq!(r["name"], "daily-backups");
        assert_eq!(r["outputs"]["backup_plan_id"], "plan-abc-123");
        assert_eq!(r["outputs"]["version_id"], "v1");
    }

    #[test]
    fn test_parse_backup_plans_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_backup_plans(&json).is_empty());
    }

    #[test]
    fn test_parse_ses_domains() {
        let json = serde_json::json!({
            "Identities": ["example.com", "notifications.example.com"]
        });

        let results = AwsResourceProvisioner::parse_ses_domains(&json);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["id"], "example.com");
        assert_eq!(results[0]["resource_type"], "ses_domain");
        assert_eq!(results[0]["name"], "example.com");
        assert_eq!(results[1]["id"], "notifications.example.com");
    }

    #[test]
    fn test_parse_ses_domains_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_ses_domains(&json).is_empty());
    }

    #[test]
    fn test_parse_cloudwatch_alarms() {
        let json = serde_json::json!({
            "MetricAlarms": [{
                "AlarmName": "high-cpu",
                "AlarmArn": "arn:aws:cloudwatch:us-east-1:123:alarm:high-cpu",
                "MetricName": "CPUUtilization",
                "Namespace": "AWS/EC2",
                "ComparisonOperator": "GreaterThanThreshold",
                "Threshold": 80.0,
                "Period": 300,
                "EvaluationPeriods": 2,
                "Statistic": "Average",
                "StateValue": "OK"
            }]
        });

        let results = AwsResourceProvisioner::parse_cloudwatch_alarms(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "high-cpu");
        assert_eq!(r["resource_type"], "cloudwatch_alarm");
        assert_eq!(r["name"], "high-cpu");
        assert_eq!(r["config"]["metric_name"], "CPUUtilization");
        assert_eq!(r["config"]["namespace"], "AWS/EC2");
        assert_eq!(r["config"]["threshold"], 80.0);
        assert_eq!(r["outputs"]["state_value"], "OK");
    }

    #[test]
    fn test_parse_cloudwatch_alarms_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_cloudwatch_alarms(&json).is_empty());
    }

    #[test]
    fn test_parse_missing_top_level_key_v063_types() {
        let empty = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_nat_gateways(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_kms_keys(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_acm_certificates(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_backup_vaults(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_backup_plans(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_ses_domains(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_cloudwatch_alarms(&empty).is_empty());
    }

    // ── ECS build-args tests ─────────────────────────────────────────

    #[test]
    fn test_ecs_cluster_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-ecs-cluster",
            "capacity_providers": "FARGATE FARGATE_SPOT"
        });
        let args = p.build_create_args("ecs_cluster", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ecs".to_string()));
        assert!(args.contains(&"create-cluster".to_string()));
        assert!(args.contains(&"--cluster-name".to_string()));
        assert!(args.contains(&"my-ecs-cluster".to_string()));
        assert!(args.contains(&"--capacity-providers".to_string()));
        assert!(args.contains(&"FARGATE FARGATE_SPOT".to_string()));
    }

    #[test]
    fn test_ecs_cluster_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-cluster"
        });
        let args = p.build_create_args("ecs_cluster", &params).unwrap();

        assert!(args.contains(&"create-cluster".to_string()));
        assert!(args.contains(&"default-cluster".to_string()));
        assert!(args.contains(&"FARGATE".to_string()));
    }

    #[test]
    fn test_delete_ecs_cluster() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("ecs_cluster", "my-cluster").unwrap();
        assert!(args.contains(&"ecs".to_string()));
        assert!(args.contains(&"delete-cluster".to_string()));
        assert!(args.contains(&"--cluster".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
    }

    #[test]
    fn test_ecs_service_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "web-service",
            "cluster": "my-cluster",
            "task_definition": "web-task:1",
            "desired_count": "2",
            "subnets": "subnet-aaa,subnet-bbb",
            "security_groups": "sg-111",
            "public_ip": "ENABLED"
        });
        let args = p.build_create_args("ecs_service", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ecs".to_string()));
        assert!(args.contains(&"create-service".to_string()));
        assert!(args.contains(&"--cluster".to_string()));
        assert!(args.contains(&"my-cluster".to_string()));
        assert!(args.contains(&"--service-name".to_string()));
        assert!(args.contains(&"web-service".to_string()));
        assert!(args.contains(&"--task-definition".to_string()));
        assert!(args.contains(&"web-task:1".to_string()));
        assert!(args.contains(&"--desired-count".to_string()));
        assert!(args.contains(&"2".to_string()));
        assert!(args.contains(&"--launch-type".to_string()));
        assert!(args.contains(&"FARGATE".to_string()));
        assert!(args.contains(&"--network-configuration".to_string()));
        // Verify network config contains expected values
        let net_arg = args
            .iter()
            .find(|a| a.contains("awsvpcConfiguration"))
            .expect("network config not found");
        assert!(net_arg.contains("subnet-aaa,subnet-bbb"));
        assert!(net_arg.contains("sg-111"));
        assert!(net_arg.contains("ENABLED"));
    }

    #[test]
    fn test_ecs_service_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "api",
            "cluster": "prod",
            "task_definition": "api:1",
            "subnets": "subnet-aaa",
            "security_groups": "sg-111"
        });
        let args = p.build_create_args("ecs_service", &params).unwrap();

        assert!(args.contains(&"1".to_string())); // default desired_count
        let net_arg = args
            .iter()
            .find(|a| a.contains("awsvpcConfiguration"))
            .expect("network config not found");
        assert!(net_arg.contains("DISABLED")); // default public_ip
    }

    #[test]
    fn test_delete_ecs_service_requires_params() {
        let p = AwsResourceProvisioner::new(None, None);
        let err = p.build_delete_args("ecs_service", "web-svc").unwrap_err();
        assert!(err.to_string().contains("cluster"));
    }

    #[test]
    fn test_ecs_task_definition_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "web-task",
            "cpu": "512",
            "memory": "1024",
            "container_name": "web",
            "image": "nginx:latest",
            "port": "8080"
        });
        let args = p.build_create_args("ecs_task_definition", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ecs".to_string()));
        assert!(args.contains(&"register-task-definition".to_string()));
        assert!(args.contains(&"--family".to_string()));
        assert!(args.contains(&"web-task".to_string()));
        assert!(args.contains(&"--network-mode".to_string()));
        assert!(args.contains(&"awsvpc".to_string()));
        assert!(args.contains(&"--requires-compatibilities".to_string()));
        assert!(args.contains(&"FARGATE".to_string()));
        assert!(args.contains(&"--cpu".to_string()));
        assert!(args.contains(&"512".to_string()));
        assert!(args.contains(&"--memory".to_string()));
        assert!(args.contains(&"1024".to_string()));
        assert!(args.contains(&"--container-definitions".to_string()));

        let cd_arg = args
            .iter()
            .find(|a| a.contains("containerPort"))
            .expect("container definitions not found");
        let cd: Value = serde_json::from_str(cd_arg).unwrap();
        assert_eq!(cd[0]["name"], "web");
        assert_eq!(cd[0]["image"], "nginx:latest");
        assert_eq!(cd[0]["portMappings"][0]["containerPort"], 8080);
        assert_eq!(cd[0]["essential"], true);
    }

    #[test]
    fn test_ecs_task_definition_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "api-task",
            "image": "myapp:v1"
        });
        let args = p.build_create_args("ecs_task_definition", &params).unwrap();

        assert!(args.contains(&"256".to_string())); // default cpu
        assert!(args.contains(&"512".to_string())); // default memory

        let cd_arg = args
            .iter()
            .find(|a| a.contains("containerPort"))
            .expect("container definitions not found");
        let cd: Value = serde_json::from_str(cd_arg).unwrap();
        assert_eq!(cd[0]["name"], "api-task"); // default container_name = id
        assert_eq!(cd[0]["portMappings"][0]["containerPort"], 80); // default port
    }

    #[test]
    fn test_delete_ecs_task_definition() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("ecs_task_definition", "web-task:1")
            .unwrap();
        assert!(args.contains(&"ecs".to_string()));
        assert!(args.contains(&"deregister-task-definition".to_string()));
        assert!(args.contains(&"--task-definition".to_string()));
        assert!(args.contains(&"web-task:1".to_string()));
    }

    // ── ECR build-args tests ─────────────────────────────────────────

    #[test]
    fn test_ecr_repository_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-app",
            "scan": "true",
            "encryption": "AES256"
        });
        let args = p.build_create_args("ecr_repository", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ecr".to_string()));
        assert!(args.contains(&"create-repository".to_string()));
        assert!(args.contains(&"--repository-name".to_string()));
        assert!(args.contains(&"my-app".to_string()));
        assert!(args.contains(&"--image-scanning-configuration".to_string()));
        assert!(args.contains(&"scanOnPush=true".to_string()));
        assert!(args.contains(&"--encryption-configuration".to_string()));
        assert!(args.contains(&"encryptionType=AES256".to_string()));
    }

    #[test]
    fn test_ecr_repository_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-repo"
        });
        let args = p.build_create_args("ecr_repository", &params).unwrap();

        assert!(args.contains(&"create-repository".to_string()));
        assert!(args.contains(&"default-repo".to_string()));
        assert!(args.contains(&"scanOnPush=true".to_string()));
        assert!(args.contains(&"encryptionType=AES256".to_string()));
    }

    #[test]
    fn test_delete_ecr_repository() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p.build_delete_args("ecr_repository", "my-app").unwrap();
        assert!(args.contains(&"ecr".to_string()));
        assert!(args.contains(&"delete-repository".to_string()));
        assert!(args.contains(&"--repository-name".to_string()));
        assert!(args.contains(&"my-app".to_string()));
        assert!(args.contains(&"--force".to_string()));
    }

    // ── ALB build-args tests ─────────────────────────────────────────

    #[test]
    fn test_alb_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-alb",
            "subnets": "subnet-aaa subnet-bbb",
            "security_groups": "sg-111 sg-222",
            "scheme": "internal"
        });
        let args = p.build_create_args("alb", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"create-load-balancer".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-alb".to_string()));
        assert!(args.contains(&"--type".to_string()));
        assert!(args.contains(&"application".to_string()));
        assert!(args.contains(&"--subnets".to_string()));
        assert!(args.contains(&"subnet-aaa subnet-bbb".to_string()));
        assert!(args.contains(&"--security-groups".to_string()));
        assert!(args.contains(&"sg-111 sg-222".to_string()));
        assert!(args.contains(&"--scheme".to_string()));
        assert!(args.contains(&"internal".to_string()));
    }

    #[test]
    fn test_alb_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-alb",
            "subnets": "subnet-aaa",
            "security_groups": "sg-111"
        });
        let args = p.build_create_args("alb", &params).unwrap();

        assert!(args.contains(&"create-load-balancer".to_string()));
        assert!(args.contains(&"internet-facing".to_string()));
    }

    #[test]
    fn test_delete_alb() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args(
                "alb",
                "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc",
            )
            .unwrap();
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"delete-load-balancer".to_string()));
        assert!(args.contains(&"--load-balancer-arn".to_string()));
    }

    #[test]
    fn test_alb_target_group_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-tg",
            "protocol": "HTTPS",
            "port": "443",
            "vpc_id": "vpc-12345",
            "target_type": "instance",
            "health_check": "/health"
        });
        let args = p.build_create_args("alb_target_group", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"create-target-group".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"my-tg".to_string()));
        assert!(args.contains(&"--protocol".to_string()));
        assert!(args.contains(&"HTTPS".to_string()));
        assert!(args.contains(&"--port".to_string()));
        assert!(args.contains(&"443".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-12345".to_string()));
        assert!(args.contains(&"--target-type".to_string()));
        assert!(args.contains(&"instance".to_string()));
        assert!(args.contains(&"--health-check-path".to_string()));
        assert!(args.contains(&"/health".to_string()));
    }

    #[test]
    fn test_alb_target_group_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-tg",
            "port": "80",
            "vpc_id": "vpc-111"
        });
        let args = p.build_create_args("alb_target_group", &params).unwrap();

        assert!(args.contains(&"create-target-group".to_string()));
        assert!(args.contains(&"HTTP".to_string())); // default protocol
        assert!(args.contains(&"ip".to_string())); // default target_type
        assert!(args.contains(&"/".to_string())); // default health_check
    }

    #[test]
    fn test_delete_alb_target_group() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args(
                "alb_target_group",
                "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/abc",
            )
            .unwrap();
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"delete-target-group".to_string()));
        assert!(args.contains(&"--target-group-arn".to_string()));
    }

    #[test]
    fn test_alb_listener_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-listener",
            "alb_arn": "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc",
            "protocol": "HTTPS",
            "port": "443",
            "target_group_arn": "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/abc"
        });
        let args = p.build_create_args("alb_listener", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"create-listener".to_string()));
        assert!(args.contains(&"--load-balancer-arn".to_string()));
        assert!(args.contains(&"--protocol".to_string()));
        assert!(args.contains(&"HTTPS".to_string()));
        assert!(args.contains(&"--port".to_string()));
        assert!(args.contains(&"443".to_string()));
        assert!(args.contains(&"--default-actions".to_string()));

        let action_arg = args
            .iter()
            .find(|a| a.contains("Type=forward"))
            .expect("action not found");
        assert!(action_arg.contains("TargetGroupArn="));
    }

    #[test]
    fn test_alb_listener_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-listener",
            "alb_arn": "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/alb/abc",
            "target_group_arn": "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc"
        });
        let args = p.build_create_args("alb_listener", &params).unwrap();

        assert!(args.contains(&"HTTP".to_string())); // default protocol
        assert!(args.contains(&"80".to_string())); // default port
    }

    #[test]
    fn test_delete_alb_listener() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args(
                "alb_listener",
                "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/alb/abc/def",
            )
            .unwrap();
        assert!(args.contains(&"elbv2".to_string()));
        assert!(args.contains(&"delete-listener".to_string()));
        assert!(args.contains(&"--listener-arn".to_string()));
    }

    // ── CloudFront build-args tests ──────────────────────────────────

    #[test]
    fn test_cloudfront_distribution_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "my-dist",
            "origin_domain": "my-bucket.s3.amazonaws.com",
            "origin_id": "s3-origin",
            "viewer_protocol": "allow-all",
            "comment": "My distribution"
        });
        let args = p
            .build_create_args("cloudfront_distribution", &params)
            .unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"cloudfront".to_string()));
        assert!(args.contains(&"create-distribution".to_string()));
        assert!(args.contains(&"--distribution-config".to_string()));

        let config_arg = args
            .iter()
            .find(|a| a.contains("CallerReference"))
            .expect("distribution config not found");
        let config: Value = serde_json::from_str(config_arg).unwrap();
        assert_eq!(config["CallerReference"], "my-dist");
        assert_eq!(config["Origins"]["Items"][0]["Id"], "s3-origin");
        assert_eq!(
            config["Origins"]["Items"][0]["DomainName"],
            "my-bucket.s3.amazonaws.com"
        );
        assert_eq!(
            config["DefaultCacheBehavior"]["ViewerProtocolPolicy"],
            "allow-all"
        );
        assert_eq!(config["Enabled"], true);
        assert_eq!(config["Comment"], "My distribution");
    }

    #[test]
    fn test_cloudfront_distribution_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "default-dist",
            "origin_domain": "example.s3.amazonaws.com"
        });
        let args = p
            .build_create_args("cloudfront_distribution", &params)
            .unwrap();

        let config_arg = args
            .iter()
            .find(|a| a.contains("CallerReference"))
            .expect("distribution config not found");
        let config: Value = serde_json::from_str(config_arg).unwrap();
        assert_eq!(config["Origins"]["Items"][0]["Id"], "default-dist"); // default origin_id = id
        assert_eq!(
            config["DefaultCacheBehavior"]["ViewerProtocolPolicy"],
            "redirect-to-https"
        ); // default
    }

    #[test]
    fn test_delete_cloudfront_distribution() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("cloudfront_distribution", "E1234567890AB")
            .unwrap();
        assert!(args.contains(&"cloudfront".to_string()));
        assert!(args.contains(&"delete-distribution".to_string()));
        assert!(args.contains(&"--id".to_string()));
        assert!(args.contains(&"E1234567890AB".to_string()));
    }

    // ── Route53 build-args tests ─────────────────────────────────────

    #[test]
    fn test_route53_zone_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "example.com",
            "caller_ref": "unique-ref-123"
        });
        let args = p.build_create_args("route53_zone", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"route53".to_string()));
        assert!(args.contains(&"create-hosted-zone".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"example.com".to_string()));
        assert!(args.contains(&"--caller-reference".to_string()));
        assert!(args.contains(&"unique-ref-123".to_string()));
    }

    #[test]
    fn test_route53_zone_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "example.org"
        });
        let args = p.build_create_args("route53_zone", &params).unwrap();

        assert!(args.contains(&"create-hosted-zone".to_string()));
        assert!(args.contains(&"example.org".to_string()));
        // default caller_ref = id
        let ref_idx = args.iter().position(|a| a == "--caller-reference").unwrap();
        assert_eq!(args[ref_idx + 1], "example.org");
    }

    #[test]
    fn test_delete_route53_zone() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("route53_zone", "/hostedzone/Z12345")
            .unwrap();
        assert!(args.contains(&"route53".to_string()));
        assert!(args.contains(&"delete-hosted-zone".to_string()));
        assert!(args.contains(&"--id".to_string()));
        assert!(args.contains(&"/hostedzone/Z12345".to_string()));
    }

    #[test]
    fn test_route53_record_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "api.example.com",
            "zone_id": "Z12345",
            "record_type": "CNAME",
            "value": "lb.example.com",
            "ttl": "600"
        });
        let args = p.build_create_args("route53_record", &params).unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"route53".to_string()));
        assert!(args.contains(&"change-resource-record-sets".to_string()));
        assert!(args.contains(&"--hosted-zone-id".to_string()));
        assert!(args.contains(&"Z12345".to_string()));
        assert!(args.contains(&"--change-batch".to_string()));

        let batch_arg = args
            .iter()
            .find(|a| a.contains("UPSERT"))
            .expect("change batch not found");
        let batch: Value = serde_json::from_str(batch_arg).unwrap();
        let rrs = &batch["Changes"][0]["ResourceRecordSet"];
        assert_eq!(rrs["Name"], "api.example.com");
        assert_eq!(rrs["Type"], "CNAME");
        assert_eq!(rrs["TTL"], 600);
        assert_eq!(rrs["ResourceRecords"][0]["Value"], "lb.example.com");
    }

    #[test]
    fn test_route53_record_build_args_defaults() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "web.example.com",
            "zone_id": "Z99999",
            "value": "1.2.3.4"
        });
        let args = p.build_create_args("route53_record", &params).unwrap();

        let batch_arg = args
            .iter()
            .find(|a| a.contains("UPSERT"))
            .expect("change batch not found");
        let batch: Value = serde_json::from_str(batch_arg).unwrap();
        let rrs = &batch["Changes"][0]["ResourceRecordSet"];
        assert_eq!(rrs["Type"], "A"); // default record_type
        assert_eq!(rrs["TTL"], 300); // default ttl
    }

    #[test]
    fn test_delete_route53_record_requires_params() {
        let p = AwsResourceProvisioner::new(None, None);
        let err = p
            .build_delete_args("route53_record", "api.example.com")
            .unwrap_err();
        assert!(err.to_string().contains("zone_id"));
    }

    // ── Secrets Manager build-args tests ─────────────────────────────

    #[test]
    fn test_secrets_manager_secret_build_args() {
        let p = AwsResourceProvisioner::new(Some("us-east-1"), None);
        let params = serde_json::json!({
            "id": "prod/db-password",
            "value": "supersecret123",
            "description": "Production database password"
        });
        let args = p
            .build_create_args("secrets_manager_secret", &params)
            .unwrap();

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"secretsmanager".to_string()));
        assert!(args.contains(&"create-secret".to_string()));
        assert!(args.contains(&"--name".to_string()));
        assert!(args.contains(&"prod/db-password".to_string()));
        assert!(args.contains(&"--secret-string".to_string()));
        assert!(args.contains(&"supersecret123".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"Production database password".to_string()));
    }

    #[test]
    fn test_secrets_manager_secret_build_args_no_description() {
        let p = AwsResourceProvisioner::new(None, None);
        let params = serde_json::json!({
            "id": "api-key",
            "value": "key123"
        });
        let args = p
            .build_create_args("secrets_manager_secret", &params)
            .unwrap();

        assert!(args.contains(&"create-secret".to_string()));
        assert!(args.contains(&"api-key".to_string()));
        assert!(args.contains(&"key123".to_string()));
        assert!(!args.contains(&"--description".to_string()));
    }

    #[test]
    fn test_delete_secrets_manager_secret() {
        let p = AwsResourceProvisioner::new(None, None);
        let args = p
            .build_delete_args("secrets_manager_secret", "prod/db-password")
            .unwrap();
        assert!(args.contains(&"secretsmanager".to_string()));
        assert!(args.contains(&"delete-secret".to_string()));
        assert!(args.contains(&"--secret-id".to_string()));
        assert!(args.contains(&"prod/db-password".to_string()));
        assert!(args.contains(&"--force-delete-without-recovery".to_string()));
    }

    // ── Discovery parse tests for v0.7.0 resource types ─────────────

    #[test]
    fn test_parse_ecr_repositories() {
        let json = serde_json::json!({
            "repositories": [{
                "repositoryName": "my-app",
                "repositoryArn": "arn:aws:ecr:us-east-1:123:repository/my-app",
                "repositoryUri": "123.dkr.ecr.us-east-1.amazonaws.com/my-app",
                "imageScanningConfiguration": { "scanOnPush": true },
                "encryptionConfiguration": { "encryptionType": "AES256" }
            }]
        });

        let results = AwsResourceProvisioner::parse_ecr_repositories(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "my-app");
        assert_eq!(r["resource_type"], "ecr_repository");
        assert_eq!(r["name"], "my-app");
        assert_eq!(
            r["outputs"]["repository_arn"],
            "arn:aws:ecr:us-east-1:123:repository/my-app"
        );
        assert_eq!(
            r["outputs"]["repository_uri"],
            "123.dkr.ecr.us-east-1.amazonaws.com/my-app"
        );
    }

    #[test]
    fn test_parse_ecr_repositories_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_ecr_repositories(&json).is_empty());
    }

    #[test]
    fn test_parse_albs() {
        let json = serde_json::json!({
            "LoadBalancers": [{
                "LoadBalancerName": "web-alb",
                "LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/web-alb/abc",
                "DNSName": "web-alb-123.us-east-1.elb.amazonaws.com",
                "CanonicalHostedZoneId": "Z35SXDOTRQ7X7K",
                "Type": "application",
                "Scheme": "internet-facing",
                "VpcId": "vpc-111",
                "AvailabilityZones": [{"ZoneName": "us-east-1a"}]
            }, {
                "LoadBalancerName": "nlb-internal",
                "Type": "network",
                "Scheme": "internal"
            }]
        });

        let results = AwsResourceProvisioner::parse_albs(&json);
        // Only application type should be included
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "web-alb");
        assert_eq!(r["resource_type"], "alb");
        assert_eq!(r["name"], "web-alb");
        assert_eq!(r["config"]["scheme"], "internet-facing");
        assert_eq!(
            r["outputs"]["dns_name"],
            "web-alb-123.us-east-1.elb.amazonaws.com"
        );
        assert_eq!(r["outputs"]["hosted_zone_id"], "Z35SXDOTRQ7X7K");
    }

    #[test]
    fn test_parse_albs_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_albs(&json).is_empty());
    }

    #[test]
    fn test_parse_route53_zones() {
        let json = serde_json::json!({
            "HostedZones": [{
                "Id": "/hostedzone/Z12345",
                "Name": "example.com.",
                "Config": { "PrivateZone": false },
                "ResourceRecordSetCount": 10
            }]
        });

        let results = AwsResourceProvisioner::parse_route53_zones(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "/hostedzone/Z12345");
        assert_eq!(r["resource_type"], "route53_zone");
        assert_eq!(r["name"], "example.com.");
        assert_eq!(r["config"]["private_zone"], false);
        assert_eq!(r["outputs"]["resource_record_set_count"], 10);
    }

    #[test]
    fn test_parse_route53_zones_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_route53_zones(&json).is_empty());
    }

    #[test]
    fn test_parse_secrets_manager_secrets() {
        let json = serde_json::json!({
            "SecretList": [{
                "Name": "prod/db-password",
                "ARN": "arn:aws:secretsmanager:us-east-1:123:secret:prod/db-password-abc123",
                "Description": "Production DB password",
                "LastChangedDate": "2025-06-01T00:00:00Z"
            }]
        });

        let results = AwsResourceProvisioner::parse_secrets_manager_secrets(&json);
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r["id"], "prod/db-password");
        assert_eq!(r["resource_type"], "secrets_manager_secret");
        assert_eq!(r["name"], "prod/db-password");
        assert_eq!(
            r["outputs"]["arn"],
            "arn:aws:secretsmanager:us-east-1:123:secret:prod/db-password-abc123"
        );
        assert_eq!(r["config"]["description"], "Production DB password");
    }

    #[test]
    fn test_parse_secrets_manager_secrets_empty() {
        let json = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_secrets_manager_secrets(&json).is_empty());
    }

    #[test]
    fn test_parse_missing_top_level_key_v070_types() {
        let empty = serde_json::json!({});
        assert!(AwsResourceProvisioner::parse_ecr_repositories(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_albs(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_route53_zones(&empty).is_empty());
        assert!(AwsResourceProvisioner::parse_secrets_manager_secrets(&empty).is_empty());
    }
}
