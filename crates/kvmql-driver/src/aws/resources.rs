use std::process::Command;

use serde_json::Value;

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

/// Result of a provisioning operation.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "creating", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (endpoints, IDs, etc.).
    pub outputs: Option<Value>,
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
    pub fn create(&self, resource_type: &str, params: &Value) -> Result<ProvisionResult, String> {
        match resource_type {
            "rds_postgres" => self.create_rds_postgres(params),
            "vpc" => self.create_vpc(params),
            "aws_subnet" => self.create_aws_subnet(params),
            "security_group" => self.create_security_group(params),
            "sg_rule" => self.create_sg_rule(params),
            other => Err(format!("unsupported AWS resource type: {other}")),
        }
    }

    /// Delete a managed resource.
    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), String> {
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
            other => Err(format!("unsupported AWS resource type for delete: {other}")),
        }
    }

    /// Delete a sub-resource that requires extra context (e.g. sg_rule needs group_id).
    pub fn delete_with_params(&self, resource_type: &str, _id: &str, params: &Value) -> Result<(), String> {
        match resource_type {
            "sg_rule" => {
                let group_id = param_str(params, "security_group_id")?;
                let protocol = param_str(params, "protocol")?;
                let port = param_str(params, "port")?;
                let cidr = param_str(params, "cidr")?;
                self.run_aws(&[
                    "ec2", "revoke-security-group-ingress",
                    "--group-id", &group_id,
                    "--protocol", &protocol,
                    "--port", &port,
                    "--cidr", &cidr,
                ])?;
                Ok(())
            }
            other => self.delete(other, _id),
        }
    }

    // ── Build args (for testing without execution) ───────────────────

    /// Build the `aws` argument list that `create()` would use, WITHOUT executing.
    pub fn build_create_args(&self, resource_type: &str, params: &Value) -> Result<Vec<String>, String> {
        let raw = match resource_type {
            "rds_postgres" => self.build_rds_postgres_args(params)?,
            "vpc" => self.build_vpc_args(params)?,
            "aws_subnet" => self.build_aws_subnet_args(params)?,
            "security_group" => self.build_security_group_args(params)?,
            "sg_rule" => self.build_sg_rule_args(params)?,
            other => return Err(format!("unsupported AWS resource type: {other}")),
        };
        Ok(self.build_args(&raw.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    }

    /// Build the `aws` argument list that `delete()` would use, WITHOUT executing.
    pub fn build_delete_args(&self, resource_type: &str, id: &str) -> Result<Vec<String>, String> {
        let base: Vec<&str> = match resource_type {
            "rds_postgres" => vec![
                "rds", "delete-db-instance",
                "--db-instance-identifier", id,
                "--skip-final-snapshot",
            ],
            "vpc" => vec!["ec2", "delete-vpc", "--vpc-id", id],
            "aws_subnet" => vec!["ec2", "delete-subnet", "--subnet-id", id],
            "security_group" => vec!["ec2", "delete-security-group", "--group-id", id],
            "sg_rule" => {
                return Err("sg_rule deletion requires params; use build_delete_args_with_params()".into());
            }
            other => return Err(format!("unsupported AWS resource type for delete: {other}")),
        };
        Ok(self.build_args(&base))
    }

    // ── Generic runner ───────────────────────────────────────────────

    /// Run an `aws` command and return JSON output.
    fn run_aws(&self, args: &[&str]) -> Result<Value, String> {
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
            .map_err(|e| format!("failed to run aws: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("aws command failed: {stderr}"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| format!("failed to parse aws JSON output: {e}"))
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
    pub fn discover(&self) -> Result<Vec<Value>, String> {
        let mut resources: Vec<Value> = Vec::new();

        let collectors: &[(&str, fn(&Self) -> Vec<Value>)] = &[
            ("ec2", Self::discover_ec2),
            ("rds_postgres", Self::discover_rds_postgres),
            ("vpc", Self::discover_vpcs),
            ("aws_subnet", Self::discover_subnets),
            ("security_group", Self::discover_security_groups),
            ("s3_bucket", Self::discover_s3_buckets),
            ("lambda", Self::discover_lambda),
            ("elb", Self::discover_elbs),
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
        match self.run_aws(args) {
            Ok(v) => Ok(v),
            Err(e) => {
                // Surface the error as a diagnostic row so it's visible
                // in the import summary instead of silently returning 0.
                Err(vec![serde_json::json!({
                    "id": format!("_discover_error_{resource_type}"),
                    "resource_type": resource_type,
                    "name": format!("discover error: {resource_type}"),
                    "config": { "error": e },
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
        let output = match self.discover_run("security_group", &["ec2", "describe-security-groups"]) {
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

    // ── Per-resource create implementations ──────────────────────────

    /// Create an RDS PostgreSQL instance.
    ///
    /// Note: RDS creates are asynchronous. The instance will be in "creating"
    /// state immediately after this call returns.
    fn create_rds_postgres(&self, params: &Value) -> Result<ProvisionResult, String> {
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

    fn create_vpc(&self, params: &Value) -> Result<ProvisionResult, String> {
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
                "ec2", "create-tags",
                "--resources", vid,
                "--tags", &format!("Key=Name,Value={name}"),
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

    fn create_aws_subnet(&self, params: &Value) -> Result<ProvisionResult, String> {
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

    fn create_security_group(&self, params: &Value) -> Result<ProvisionResult, String> {
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

    fn create_sg_rule(&self, params: &Value) -> Result<ProvisionResult, String> {
        let args = self.build_sg_rule_args(params)?;
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let result = self.run_aws(&refs)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(result),
        })
    }

    // ── Argument builders (testable without execution) ───────────────

    fn build_rds_postgres_args(&self, params: &Value) -> Result<Vec<String>, String> {
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
        if let Some(v) = params.get("vpc_security_group_ids").and_then(|v| v.as_str()) {
            args.push("--vpc-security-group-ids".into());
            args.push(v.into());
        }
        if let Some(v) = params.get("db_subnet_group").and_then(|v| v.as_str()) {
            args.push("--db-subnet-group-name".into());
            args.push(v.into());
        }

        Ok(args)
    }

    fn build_vpc_args(&self, params: &Value) -> Result<Vec<String>, String> {
        let cidr = param_str(params, "cidr_block")?;
        let args = vec![
            "ec2".into(),
            "create-vpc".into(),
            "--cidr-block".into(),
            cidr,
        ];
        Ok(args)
    }

    fn build_aws_subnet_args(&self, params: &Value) -> Result<Vec<String>, String> {
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

    fn build_security_group_args(&self, params: &Value) -> Result<Vec<String>, String> {
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

    fn build_sg_rule_args(&self, params: &Value) -> Result<Vec<String>, String> {
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
        let params = serde_json::json!({"id": "my-db", "instance_class": "db.t3.micro"});
        assert_eq!(param_str(&params, "id").unwrap(), "my-db");
        assert_eq!(param_str(&params, "instance_class").unwrap(), "db.t3.micro");
    }

    #[test]
    fn test_param_str_missing() {
        let params = serde_json::json!({"id": "my-db"});
        let err = param_str(&params, "engine_version").unwrap_err();
        assert!(err.contains("missing required parameter"));
        assert!(err.contains("engine_version"));
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
        assert!(err.contains("unsupported AWS resource type"));
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
        assert!(err.contains("unsupported AWS resource type for delete"));
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
        assert_eq!(r["outputs"]["endpoint"], "prod-db.abc.us-east-1.rds.amazonaws.com");
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
        assert_eq!(results[0]["outputs"]["creation_date"], "2024-01-15T10:30:00Z");
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
        assert_eq!(extract_name_from_tags(&with_name), Some("my-resource".to_string()));

        let without_name = serde_json::json!({
            "Tags": [{ "Key": "Env", "Value": "prod" }]
        });
        assert_eq!(extract_name_from_tags(&without_name), None);

        let no_tags = serde_json::json!({ "InstanceId": "i-123" });
        assert_eq!(extract_name_from_tags(&no_tags), None);
    }
}
