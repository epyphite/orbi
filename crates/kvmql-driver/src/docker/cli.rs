use std::process::Command;

use serde_json::Value;

/// Client for the `docker` and `docker compose` CLIs.
#[derive(Debug, Clone)]
pub struct DockerCli {
    host: Option<String>,
}

impl Default for DockerCli {
    fn default() -> Self {
        Self::new()
    }
}

impl DockerCli {
    pub fn new() -> Self {
        Self { host: None }
    }

    pub fn with_host(host: &str) -> Self {
        Self {
            host: Some(host.to_string()),
        }
    }

    pub fn check_available(&self) -> Result<(), String> {
        let output = self
            .cmd(&["version", "--format", "json"])
            .map_err(|e| format!("docker not found: {e}"))?;
        if output.is_empty() {
            return Err("docker returned empty output".into());
        }
        Ok(())
    }

    // ── Container operations ────────────────────────────────────────

    pub fn run(&self, name: &str, image: &str, extra_args: &[&str]) -> Result<String, String> {
        let mut args = vec!["run", "-d", "--name", name];
        args.extend_from_slice(extra_args);
        args.push(image);
        let output = self.cmd(&args)?;
        Ok(output.trim().to_string()) // returns container ID
    }

    pub fn stop(&self, name: &str) -> Result<(), String> {
        self.cmd(&["stop", name])?;
        Ok(())
    }

    pub fn start(&self, name: &str) -> Result<(), String> {
        self.cmd(&["start", name])?;
        Ok(())
    }

    pub fn rm(&self, name: &str, force: bool) -> Result<(), String> {
        if force {
            self.cmd(&["rm", "-f", name])?;
        } else {
            self.cmd(&["rm", name])?;
        }
        Ok(())
    }

    pub fn inspect(&self, name: &str) -> Result<Value, String> {
        let output = self.cmd(&["inspect", name, "--format", "{{json .}}"])?;
        serde_json::from_str(output.trim())
            .map_err(|e| format!("failed to parse docker inspect: {e}"))
    }

    pub fn ps_json(&self) -> Result<Vec<Value>, String> {
        let output = self.cmd(&["ps", "-a", "--format", "{{json .}}"])?;
        let mut containers = Vec::new();
        for line in output.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                    containers.push(v);
                }
            }
        }
        Ok(containers)
    }

    // ── Network operations ──────────────────────────────────────────

    pub fn network_create(
        &self,
        name: &str,
        driver: &str,
        subnet: Option<&str>,
    ) -> Result<String, String> {
        let mut args = vec!["network", "create", "--driver", driver];
        let subnet_flag;
        if let Some(s) = subnet {
            subnet_flag = format!("--subnet={s}");
            args.push(&subnet_flag);
        }
        args.push(name);
        let output = self.cmd(&args)?;
        Ok(output.trim().to_string())
    }

    pub fn network_rm(&self, name: &str) -> Result<(), String> {
        self.cmd(&["network", "rm", name])?;
        Ok(())
    }

    pub fn network_ls_json(&self) -> Result<Vec<Value>, String> {
        let output = self.cmd(&["network", "ls", "--format", "{{json .}}"])?;
        let mut networks = Vec::new();
        for line in output.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                    networks.push(v);
                }
            }
        }
        Ok(networks)
    }

    // ── Volume operations ───────────────────────────────────────────

    pub fn volume_create(&self, name: &str) -> Result<String, String> {
        let output = self.cmd(&["volume", "create", name])?;
        Ok(output.trim().to_string())
    }

    pub fn volume_rm(&self, name: &str) -> Result<(), String> {
        self.cmd(&["volume", "rm", name])?;
        Ok(())
    }

    pub fn volume_ls_json(&self) -> Result<Vec<Value>, String> {
        let output = self.cmd(&["volume", "ls", "--format", "{{json .}}"])?;
        let mut volumes = Vec::new();
        for line in output.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                    volumes.push(v);
                }
            }
        }
        Ok(volumes)
    }

    // ── Compose operations ──────────────────────────────────────────

    pub fn compose_up(
        &self,
        project_dir: &str,
        project_name: Option<&str>,
    ) -> Result<String, String> {
        let mut args = vec!["compose"];
        let pn_flag;
        if let Some(pn) = project_name {
            pn_flag = format!("-p={pn}");
            args.push(&pn_flag);
        }
        args.extend_from_slice(&["up", "-d", "--wait"]);
        self.cmd_in_dir(&args, project_dir)
    }

    pub fn compose_down(
        &self,
        project_dir: &str,
        project_name: Option<&str>,
    ) -> Result<(), String> {
        let mut args = vec!["compose"];
        let pn_flag;
        if let Some(pn) = project_name {
            pn_flag = format!("-p={pn}");
            args.push(&pn_flag);
        }
        args.extend_from_slice(&["down", "--remove-orphans"]);
        self.cmd_in_dir(&args, project_dir)?;
        Ok(())
    }

    pub fn compose_ps_json(
        &self,
        project_dir: &str,
        project_name: Option<&str>,
    ) -> Result<Vec<Value>, String> {
        let mut args = vec!["compose"];
        let pn_flag;
        if let Some(pn) = project_name {
            pn_flag = format!("-p={pn}");
            args.push(&pn_flag);
        }
        args.extend_from_slice(&["ps", "--format", "json"]);
        let output = self.cmd_in_dir(&args, project_dir)?;
        let mut services = Vec::new();
        for line in output.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                    services.push(v);
                }
            }
        }
        Ok(services)
    }

    pub fn compose_scale(
        &self,
        project_dir: &str,
        service: &str,
        count: u32,
    ) -> Result<(), String> {
        self.cmd_in_dir(
            &[
                "compose",
                "up",
                "-d",
                "--scale",
                &format!("{service}={count}"),
            ],
            project_dir,
        )?;
        Ok(())
    }

    // ── Internal ────────────────────────────────────────────────────

    fn cmd(&self, args: &[&str]) -> Result<String, String> {
        self.cmd_in_dir(args, ".")
    }

    fn cmd_in_dir(&self, args: &[&str], dir: &str) -> Result<String, String> {
        let mut cmd = Command::new("docker");
        if let Some(ref host) = self.host {
            cmd.arg("-H").arg(host);
        }
        for arg in args {
            cmd.arg(arg);
        }
        cmd.current_dir(dir);

        let output = cmd
            .output()
            .map_err(|e| format!("failed to run docker: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "docker {} failed: {}",
                args.first().unwrap_or(&""),
                stderr.trim()
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client() {
        let cli = DockerCli::new();
        assert!(cli.host.is_none());
    }

    #[test]
    fn test_with_host() {
        let cli = DockerCli::with_host("ssh://user@remote");
        assert_eq!(cli.host.as_deref(), Some("ssh://user@remote"));
    }

    #[test]
    fn test_parse_ps_json_output() {
        let output = r#"{"ID":"abc123","Names":"web","Image":"nginx","Status":"Up 2 hours"}
{"ID":"def456","Names":"db","Image":"postgres:16","Status":"Up 2 hours"}
"#;
        let mut containers = Vec::new();
        for line in output.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                    containers.push(v);
                }
            }
        }
        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0]["Names"], "web");
        assert_eq!(containers[1]["Image"], "postgres:16");
    }
}
