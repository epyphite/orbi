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

        if let Some(ref region) = self.region {
            cmd.arg("--region").arg(region);
        }
        if let Some(ref profile) = self.profile {
            cmd.arg("--profile").arg(profile);
        }

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
}
