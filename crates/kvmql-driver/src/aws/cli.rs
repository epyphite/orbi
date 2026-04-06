use std::process::Command;

/// Low-level wrapper around the `aws` CLI.
///
/// All methods shell out to the `aws` command using `std::process::Command` with
/// individual arguments (never shell interpolation). The optional `region` and
/// `profile` fields are injected as `--region` / `--profile` flags when set.
/// Output is always requested as JSON via `--output json`.
#[derive(Debug, Clone)]
pub struct AwsCli {
    pub region: Option<String>,
    pub profile: Option<String>,
}

impl AwsCli {
    pub fn new() -> Self {
        Self {
            region: None,
            profile: None,
        }
    }

    pub fn with_region(region: &str) -> Self {
        Self {
            region: Some(region.to_string()),
            profile: None,
        }
    }

    pub fn with_profile(mut self, profile: &str) -> Self {
        self.profile = Some(profile.to_string());
        self
    }

    // ── Availability check ────────────────────────────────────────

    /// Check that the `aws` CLI is installed and callable.
    pub fn check_available() -> Result<(), String> {
        let output = Command::new("aws")
            .arg("--version")
            .output()
            .map_err(|e| format!("failed to run aws CLI: {e}"))?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("aws --version failed: {stderr}"))
        }
    }

    // ── Generic runner ────────────────────────────────────────────

    /// Run an arbitrary `aws` sub-command with `--output json` and parse the result.
    fn run(&self, args: &[&str]) -> Result<serde_json::Value, String> {
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
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(stdout.trim())
            .map_err(|e| format!("failed to parse aws JSON output: {e}"))
    }

    /// Build the argument list that `run` would use (for testing without execution).
    pub fn build_args(&self, args: &[&str]) -> Vec<String> {
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

    // ── EC2 Instance operations ───────────────────────────────────

    /// Launch EC2 instances: `aws ec2 run-instances [params]`
    pub fn ec2_run_instances(&self, params: &[(&str, &str)]) -> Result<serde_json::Value, String> {
        let mut args = vec!["ec2", "run-instances"];
        // We need owned strings for the --key value pairs
        let kv: Vec<String> = params
            .iter()
            .flat_map(|(k, v)| vec![format!("--{k}"), v.to_string()])
            .collect();
        let kv_refs: Vec<&str> = kv.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&kv_refs);
        self.run(&args)
    }

    /// Terminate EC2 instances: `aws ec2 terminate-instances --instance-ids <id>`
    pub fn ec2_terminate_instances(&self, instance_id: &str) -> Result<(), String> {
        self.run(&["ec2", "terminate-instances", "--instance-ids", instance_id])?;
        Ok(())
    }

    /// Describe EC2 instances, optionally filtered.
    ///
    /// Returns a flattened list of instances (AWS wraps results in
    /// `Reservations[].Instances[]`).
    pub fn ec2_describe_instances(
        &self,
        filters: Option<&[(&str, &str)]>,
    ) -> Result<Vec<serde_json::Value>, String> {
        let mut args = vec!["ec2", "describe-instances"];
        let filter_strs: Vec<String>;
        let filter_refs: Vec<&str>;
        if let Some(f) = filters {
            filter_strs = f
                .iter()
                .map(|(name, values)| format!("Name={name},Values={values}"))
                .collect();
            let mut joined = Vec::new();
            joined.push("--filters");
            filter_refs = filter_strs.iter().map(|s| s.as_str()).collect();
            args.push(joined[0]);
            args.extend_from_slice(&filter_refs);
        }

        let val = self.run(&args)?;

        // Flatten Reservations[].Instances[]
        let mut instances = Vec::new();
        if let Some(reservations) = val.get("Reservations").and_then(|v| v.as_array()) {
            for reservation in reservations {
                if let Some(insts) = reservation.get("Instances").and_then(|v| v.as_array()) {
                    instances.extend(insts.iter().cloned());
                }
            }
        }
        Ok(instances)
    }

    /// Start EC2 instances: `aws ec2 start-instances --instance-ids <id>`
    pub fn ec2_start_instances(&self, instance_id: &str) -> Result<(), String> {
        self.run(&["ec2", "start-instances", "--instance-ids", instance_id])?;
        Ok(())
    }

    /// Stop EC2 instances: `aws ec2 stop-instances --instance-ids <id>`
    pub fn ec2_stop_instances(&self, instance_id: &str) -> Result<(), String> {
        self.run(&["ec2", "stop-instances", "--instance-ids", instance_id])?;
        Ok(())
    }

    /// Create an EBS snapshot: `aws ec2 create-snapshot --volume-id <id> --description <desc>`
    pub fn ec2_create_snapshot(
        &self,
        volume_id: &str,
        description: &str,
    ) -> Result<serde_json::Value, String> {
        self.run(&[
            "ec2",
            "create-snapshot",
            "--volume-id",
            volume_id,
            "--description",
            description,
        ])
    }

    // ── EBS Volume operations ─────────────────────────────────────

    /// Create an EBS volume: `aws ec2 create-volume --size <gb> --availability-zone <az> [params]`
    pub fn ec2_create_volume(
        &self,
        size_gb: i64,
        az: &str,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value, String> {
        let size_str = size_gb.to_string();
        let mut args = vec![
            "ec2",
            "create-volume",
            "--size",
            &size_str,
            "--availability-zone",
            az,
        ];
        let kv: Vec<String> = params
            .iter()
            .flat_map(|(k, v)| vec![format!("--{k}"), v.to_string()])
            .collect();
        let kv_refs: Vec<&str> = kv.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&kv_refs);
        self.run(&args)
    }

    /// Delete an EBS volume: `aws ec2 delete-volume --volume-id <id>`
    pub fn ec2_delete_volume(&self, volume_id: &str) -> Result<(), String> {
        self.run(&["ec2", "delete-volume", "--volume-id", volume_id])?;
        Ok(())
    }

    /// Attach an EBS volume: `aws ec2 attach-volume --volume-id <vol> --instance-id <inst> --device <dev>`
    pub fn ec2_attach_volume(
        &self,
        volume_id: &str,
        instance_id: &str,
        device: &str,
    ) -> Result<(), String> {
        self.run(&[
            "ec2",
            "attach-volume",
            "--volume-id",
            volume_id,
            "--instance-id",
            instance_id,
            "--device",
            device,
        ])?;
        Ok(())
    }

    /// Detach an EBS volume: `aws ec2 detach-volume --volume-id <vol>`
    pub fn ec2_detach_volume(&self, volume_id: &str) -> Result<(), String> {
        self.run(&["ec2", "detach-volume", "--volume-id", volume_id])?;
        Ok(())
    }

    /// Describe EBS volumes: `aws ec2 describe-volumes`
    pub fn ec2_describe_volumes(&self) -> Result<Vec<serde_json::Value>, String> {
        let val = self.run(&["ec2", "describe-volumes"])?;
        match val.get("Volumes").and_then(|v| v.as_array()) {
            Some(arr) => Ok(arr.clone()),
            None => Ok(Vec::new()),
        }
    }

    // ── VPC operations ────────────────────────────────────────────

    /// Create a VPC: `aws ec2 create-vpc --cidr-block <cidr> [params]`
    pub fn ec2_create_vpc(
        &self,
        cidr: &str,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value, String> {
        let mut args = vec!["ec2", "create-vpc", "--cidr-block", cidr];
        let kv: Vec<String> = params
            .iter()
            .flat_map(|(k, v)| vec![format!("--{k}"), v.to_string()])
            .collect();
        let kv_refs: Vec<&str> = kv.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&kv_refs);
        self.run(&args)
    }

    /// Delete a VPC: `aws ec2 delete-vpc --vpc-id <id>`
    pub fn ec2_delete_vpc(&self, vpc_id: &str) -> Result<(), String> {
        self.run(&["ec2", "delete-vpc", "--vpc-id", vpc_id])?;
        Ok(())
    }

    /// Create a subnet: `aws ec2 create-subnet --vpc-id <vpc> --cidr-block <cidr> --availability-zone <az>`
    pub fn ec2_create_subnet(
        &self,
        vpc_id: &str,
        cidr: &str,
        az: &str,
    ) -> Result<serde_json::Value, String> {
        self.run(&[
            "ec2",
            "create-subnet",
            "--vpc-id",
            vpc_id,
            "--cidr-block",
            cidr,
            "--availability-zone",
            az,
        ])
    }

    /// Delete a subnet: `aws ec2 delete-subnet --subnet-id <id>`
    pub fn ec2_delete_subnet(&self, subnet_id: &str) -> Result<(), String> {
        self.run(&["ec2", "delete-subnet", "--subnet-id", subnet_id])?;
        Ok(())
    }

    // ── Security Group operations ─────────────────────────────────

    /// Create a security group: `aws ec2 create-security-group --group-name <name> --description <desc> --vpc-id <vpc>`
    pub fn ec2_create_security_group(
        &self,
        name: &str,
        description: &str,
        vpc_id: &str,
    ) -> Result<serde_json::Value, String> {
        self.run(&[
            "ec2",
            "create-security-group",
            "--group-name",
            name,
            "--description",
            description,
            "--vpc-id",
            vpc_id,
        ])
    }

    /// Delete a security group: `aws ec2 delete-security-group --group-id <id>`
    pub fn ec2_delete_security_group(&self, group_id: &str) -> Result<(), String> {
        self.run(&["ec2", "delete-security-group", "--group-id", group_id])?;
        Ok(())
    }

    /// Authorize ingress on a security group:
    /// `aws ec2 authorize-security-group-ingress --group-id <id> --protocol <proto> --port <port> --cidr <cidr>`
    pub fn ec2_authorize_ingress(
        &self,
        group_id: &str,
        protocol: &str,
        port: &str,
        cidr: &str,
    ) -> Result<(), String> {
        self.run(&[
            "ec2",
            "authorize-security-group-ingress",
            "--group-id",
            group_id,
            "--protocol",
            protocol,
            "--port",
            port,
            "--cidr",
            cidr,
        ])?;
        Ok(())
    }

    /// Revoke ingress on a security group:
    /// `aws ec2 revoke-security-group-ingress --group-id <id> --protocol <proto> --port <port> --cidr <cidr>`
    pub fn ec2_revoke_ingress(
        &self,
        group_id: &str,
        protocol: &str,
        port: &str,
        cidr: &str,
    ) -> Result<(), String> {
        self.run(&[
            "ec2",
            "revoke-security-group-ingress",
            "--group-id",
            group_id,
            "--protocol",
            protocol,
            "--port",
            port,
            "--cidr",
            cidr,
        ])?;
        Ok(())
    }

    // ── RDS operations ────────────────────────────────────────────

    /// Create an RDS instance: `aws rds create-db-instance [params]`
    ///
    /// Note: RDS creates are asynchronous. The instance will be in "creating"
    /// state immediately after this call returns.
    pub fn rds_create_db_instance(
        &self,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value, String> {
        let mut args = vec!["rds", "create-db-instance"];
        let kv: Vec<String> = params
            .iter()
            .flat_map(|(k, v)| vec![format!("--{k}"), v.to_string()])
            .collect();
        let kv_refs: Vec<&str> = kv.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&kv_refs);
        self.run(&args)
    }

    /// Delete an RDS instance: `aws rds delete-db-instance --db-instance-identifier <id>`
    pub fn rds_delete_db_instance(
        &self,
        instance_id: &str,
        skip_snapshot: bool,
    ) -> Result<(), String> {
        let mut args = vec![
            "rds",
            "delete-db-instance",
            "--db-instance-identifier",
            instance_id,
        ];
        if skip_snapshot {
            args.push("--skip-final-snapshot");
        }
        self.run(&args)?;
        Ok(())
    }

    /// Describe RDS instances: `aws rds describe-db-instances`
    pub fn rds_describe_db_instances(&self) -> Result<Vec<serde_json::Value>, String> {
        let val = self.run(&["rds", "describe-db-instances"])?;
        match val.get("DBInstances").and_then(|v| v.as_array()) {
            Some(arr) => Ok(arr.clone()),
            None => Ok(Vec::new()),
        }
    }

    /// Modify an RDS instance: `aws rds modify-db-instance --db-instance-identifier <id> [params]`
    pub fn rds_modify_db_instance(
        &self,
        instance_id: &str,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value, String> {
        let mut args = vec![
            "rds",
            "modify-db-instance",
            "--db-instance-identifier",
            instance_id,
        ];
        let kv: Vec<String> = params
            .iter()
            .flat_map(|(k, v)| vec![format!("--{k}"), v.to_string()])
            .collect();
        let kv_refs: Vec<&str> = kv.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&kv_refs);
        self.run(&args)
    }

    // ── Key pair operations ────────────────────────────────────────

    /// Import a public SSH key as an EC2 key pair:
    /// `aws ec2 import-key-pair --key-name <name> --public-key-material <key>`
    pub fn ec2_import_key_pair(
        &self,
        key_name: &str,
        public_key: &str,
    ) -> Result<serde_json::Value, String> {
        self.run(&[
            "ec2",
            "import-key-pair",
            "--key-name",
            key_name,
            "--public-key-material",
            public_key,
        ])
    }

    // ── STS / health check ────────────────────────────────────────

    /// Get caller identity: `aws sts get-caller-identity`
    pub fn sts_get_caller_identity(&self) -> Result<serde_json::Value, String> {
        self.run(&["sts", "get-caller-identity"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_construction_ec2_run_instances() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "run-instances",
            "--image-id", "ami-12345678",
            "--instance-type", "t3.micro",
            "--min-count", "1",
            "--max-count", "1",
        ]);

        assert_eq!(args[0], "aws");
        assert_eq!(args[1], "ec2");
        assert_eq!(args[2], "run-instances");
        assert!(args.contains(&"--image-id".to_string()));
        assert!(args.contains(&"ami-12345678".to_string()));
        assert!(args.contains(&"--instance-type".to_string()));
        assert!(args.contains(&"t3.micro".to_string()));
        assert!(args.contains(&"--output".to_string()));
        assert!(args.contains(&"json".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
    }

    #[test]
    fn test_command_construction_ec2_terminate() {
        let cli = AwsCli::with_region("us-west-2");
        let args = cli.build_args(&[
            "ec2", "terminate-instances",
            "--instance-ids", "i-1234567890abcdef0",
        ]);

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"terminate-instances".to_string()));
        assert!(args.contains(&"--instance-ids".to_string()));
        assert!(args.contains(&"i-1234567890abcdef0".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-west-2".to_string()));
    }

    #[test]
    fn test_command_construction_rds_create() {
        let cli = AwsCli::with_region("eu-west-1");
        let args = cli.build_args(&[
            "rds", "create-db-instance",
            "--db-instance-identifier", "my-pg",
            "--db-instance-class", "db.t3.micro",
            "--engine", "postgres",
            "--engine-version", "16",
            "--master-username", "admin",
            "--master-user-password", "secret123",
            "--allocated-storage", "20",
        ]);

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"rds".to_string()));
        assert!(args.contains(&"create-db-instance".to_string()));
        assert!(args.contains(&"--db-instance-identifier".to_string()));
        assert!(args.contains(&"my-pg".to_string()));
        assert!(args.contains(&"--engine".to_string()));
        assert!(args.contains(&"postgres".to_string()));
        assert!(args.contains(&"--engine-version".to_string()));
        assert!(args.contains(&"16".to_string()));
        assert!(args.contains(&"--allocated-storage".to_string()));
        assert!(args.contains(&"20".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"eu-west-1".to_string()));
    }

    #[test]
    fn test_command_construction_vpc_create() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "create-vpc",
            "--cidr-block", "10.0.0.0/16",
        ]);

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-vpc".to_string()));
        assert!(args.contains(&"--cidr-block".to_string()));
        assert!(args.contains(&"10.0.0.0/16".to_string()));
    }

    #[test]
    fn test_command_construction_security_group_create() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "create-security-group",
            "--group-name", "my-sg",
            "--description", "My security group",
            "--vpc-id", "vpc-12345",
        ]);

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"create-security-group".to_string()));
        assert!(args.contains(&"--group-name".to_string()));
        assert!(args.contains(&"my-sg".to_string()));
        assert!(args.contains(&"--description".to_string()));
        assert!(args.contains(&"My security group".to_string()));
        assert!(args.contains(&"--vpc-id".to_string()));
        assert!(args.contains(&"vpc-12345".to_string()));
    }

    #[test]
    fn test_command_construction_sg_rule() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "authorize-security-group-ingress",
            "--group-id", "sg-12345",
            "--protocol", "tcp",
            "--port", "443",
            "--cidr", "0.0.0.0/0",
        ]);

        assert_eq!(args[0], "aws");
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
    fn test_command_construction_no_region() {
        let cli = AwsCli::new();
        let args = cli.build_args(&["sts", "get-caller-identity"]);

        assert_eq!(
            args,
            vec!["aws", "sts", "get-caller-identity", "--output", "json"]
        );
        // Should NOT contain --region
        assert!(!args.contains(&"--region".to_string()));
    }

    #[test]
    fn test_command_construction_with_profile() {
        let cli = AwsCli::with_region("us-east-1").with_profile("staging");
        let args = cli.build_args(&["sts", "get-caller-identity"]);

        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
        assert!(args.contains(&"--profile".to_string()));
        assert!(args.contains(&"staging".to_string()));
    }

    #[test]
    fn test_command_construction_ebs_create_volume() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "create-volume",
            "--size", "100",
            "--availability-zone", "us-east-1a",
            "--volume-type", "gp3",
        ]);

        assert!(args.contains(&"create-volume".to_string()));
        assert!(args.contains(&"--size".to_string()));
        assert!(args.contains(&"100".to_string()));
        assert!(args.contains(&"--availability-zone".to_string()));
        assert!(args.contains(&"us-east-1a".to_string()));
    }

    #[test]
    fn test_command_construction_describe_instances() {
        let cli = AwsCli::with_region("us-west-2");
        let args = cli.build_args(&["ec2", "describe-instances"]);

        assert_eq!(args[0], "aws");
        assert_eq!(args[1], "ec2");
        assert_eq!(args[2], "describe-instances");
        assert!(args.contains(&"--output".to_string()));
        assert!(args.contains(&"json".to_string()));
    }

    #[test]
    fn test_command_construction_import_key_pair() {
        let cli = AwsCli::with_region("us-east-1");
        let args = cli.build_args(&[
            "ec2", "import-key-pair",
            "--key-name", "kvmql-my-vm",
            "--public-key-material", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample",
        ]);

        assert_eq!(args[0], "aws");
        assert!(args.contains(&"ec2".to_string()));
        assert!(args.contains(&"import-key-pair".to_string()));
        assert!(args.contains(&"--key-name".to_string()));
        assert!(args.contains(&"kvmql-my-vm".to_string()));
        assert!(args.contains(&"--public-key-material".to_string()));
        assert!(args.contains(&"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample".to_string()));
        assert!(args.contains(&"--region".to_string()));
        assert!(args.contains(&"us-east-1".to_string()));
    }
}
