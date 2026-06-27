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
            let _ = self.run_aws(&[
                "ec2",
                "create-tags",
                "--resources",
                vid,
                "--tags",
                &format!("Key=Name,Value={name}"),
            ]);
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

        let outputs = serde_json::json!({
            "subnet_id": result.get("Subnet").and_then(|s| s.get("SubnetId")),
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
        if params.get("map_public_ip").and_then(|v| v.as_bool()) == Some(true) {
            args.push("--map-public-ip-on-launch".into());
        }
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
        let cidr = param_str(params, "cidr")?;

        let direction = param_str_or(params, "direction", "ingress");

        let cmd = if direction == "egress" {
            "authorize-security-group-egress"
        } else {
            "authorize-security-group-ingress"
        };

        let args = vec![
            "ec2".into(),
            cmd.into(),
            "--group-id".into(),
            group_id,
            "--protocol".into(),
            protocol,
            "--port".into(),
            port,
            "--cidr".into(),
            cidr,
        ];
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
            ep_type,
        ];
        if let Some(subnets) = params.get("subnet_ids").and_then(|v| v.as_str()) {
            args.push("--subnet-ids".into());
            args.push(subnets.into());
        }
        if let Some(sgs) = params.get("security_group_ids").and_then(|v| v.as_str()) {
            args.push("--security-group-ids".into());
            args.push(sgs.into());
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
}
