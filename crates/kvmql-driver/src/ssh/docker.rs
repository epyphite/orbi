//! Docker resource management over SSH.
//!
//! Resource types:
//! - `docker_container` — single container via `docker run`.  Params:
//!   `image`, `ports` (array), `volumes` (array), `env` (string or
//!   credential ref), `restart_policy`.
//! - `docker_volume` — named volume via `docker volume create`.
//! - `docker_network` — network via `docker network create`.  Params:
//!   `driver` (default `bridge`).
//! - `docker_compose` — multi-container app via `docker compose`.
//!   Params: `project_name`, `file` (compose YAML content, pre-resolved),
//!   `env_file` (optional env content, pre-resolved).
//!
//! Query function `docker_containers` returns one row per running
//! container on the remote host.

use serde_json::{json, Value};

use super::client::SshClient;

pub struct DockerProvisioner<'a> {
    pub client: &'a SshClient,
}

#[derive(Debug)]
pub struct ProvisionResult {
    pub status: String,
    pub outputs: Option<Value>,
}

impl<'a> DockerProvisioner<'a> {
    pub fn new(client: &'a SshClient) -> Self {
        Self { client }
    }

    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        match resource_type {
            "docker_container" => self.create_container(params),
            "docker_volume" => self.create_volume(params),
            "docker_network" => self.create_network(params),
            "docker_compose" => self.create_compose(params),
            other => Err(format!("unsupported docker resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "docker_container" => {
                let _ = self.docker(&["stop", id]);
                self.docker(&["rm", "-f", id])
                    .map(|_| ())
                    .map_err(|e| format!("docker rm failed: {e}"))
            }
            "docker_volume" => self
                .docker(&["volume", "rm", id])
                .map(|_| ())
                .map_err(|e| format!("docker volume rm failed: {e}")),
            "docker_network" => self
                .docker(&["network", "rm", id])
                .map(|_| ())
                .map_err(|e| format!("docker network rm failed: {e}")),
            "docker_compose" => {
                let project = params
                    .get("project_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(id);
                let qp = super::client::shell_single_quote(project);
                self.client
                    .exec_checked(&format!("docker compose -p {qp} down"))
                    .map(|_| ())
                    .map_err(|e| format!("docker compose down failed: {e}"))
            }
            other => Err(format!("unsupported docker resource type: {other}")),
        }
    }

    // ── docker_container ─────────────────────────────────────

    fn create_container(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;
        let restart = param_str_or(params, "restart_policy", "unless-stopped");

        // Check if already running
        if self.container_exists(&name) {
            return Ok(ProvisionResult {
                status: "unchanged".into(),
                outputs: Some(json!({"container": name, "image": image})),
            });
        }

        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.clone(),
            format!("--restart={restart}"),
        ];

        // Ports
        if let Some(ports) = params.get("ports").and_then(|v| v.as_array()) {
            for p in ports {
                if let Some(s) = p.as_str() {
                    args.push("-p".into());
                    args.push(s.to_string());
                }
            }
        }

        // Volumes
        if let Some(vols) = params.get("volumes").and_then(|v| v.as_array()) {
            for v in vols {
                if let Some(s) = v.as_str() {
                    args.push("-v".into());
                    args.push(s.to_string());
                }
            }
        }

        // Network
        if let Some(net) = params.get("network").and_then(|v| v.as_str()) {
            args.push("--network".into());
            args.push(net.to_string());
        }

        // Environment — single string "KEY=value" or pre-resolved secret
        if let Some(env_str) = params.get("env").and_then(|v| v.as_str()) {
            // May contain multiple KEY=VALUE lines (e.g. from an .env file)
            for line in env_str.lines() {
                let l = line.trim();
                if !l.is_empty() && !l.starts_with('#') {
                    args.push("-e".into());
                    args.push(l.to_string());
                }
            }
        }

        args.push(image.clone());

        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.docker(&refs)
            .map_err(|e| format!("docker run failed: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "container": name,
                "image": image,
                "restart_policy": restart,
            })),
        })
    }

    // ── docker_volume ────────────────────────────────────────

    fn create_volume(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;

        if self.volume_exists(&name) {
            return Ok(ProvisionResult {
                status: "unchanged".into(),
                outputs: Some(json!({"volume": name})),
            });
        }

        self.docker(&["volume", "create", &name])
            .map_err(|e| format!("docker volume create failed: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({"volume": name})),
        })
    }

    // ── docker_network ───────────────────────────────────────

    fn create_network(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;
        let driver = param_str_or(params, "driver", "bridge");

        if self.network_exists(&name) {
            return Ok(ProvisionResult {
                status: "unchanged".into(),
                outputs: Some(json!({"network": name})),
            });
        }

        self.docker(&["network", "create", "--driver", &driver, &name])
            .map_err(|e| format!("docker network create failed: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({"network": name, "driver": driver})),
        })
    }

    // ── docker_compose ───────────────────────────────────────

    fn create_compose(&self, params: &Value) -> Result<ProvisionResult, String> {
        let project = param_str(params, "project_name")?;
        // The compose file content is pre-resolved in __content_bytes or file
        let compose_content = params
            .get("__content_bytes")
            .or_else(|| params.get("file"))
            .and_then(|v| v.as_str())
            .ok_or("docker_compose requires file (compose YAML content)")?;

        // Write compose file to a project directory on the remote
        let dir = format!("/opt/compose/{project}");
        self.client
            .mkdir_p(&dir)
            .map_err(|e| format!("mkdir compose dir failed: {e}"))?;

        let compose_path = format!("{dir}/docker-compose.yml");
        self.client
            .upload(compose_content.as_bytes(), &compose_path)
            .map_err(|e| format!("upload compose file failed: {e}"))?;

        // Optional env file
        if let Some(env_content) = params
            .get("env_file")
            .or_else(|| params.get("__env_bytes"))
            .and_then(|v| v.as_str())
        {
            let env_path = format!("{dir}/.env");
            self.client
                .upload(env_content.as_bytes(), &env_path)
                .map_err(|e| format!("upload env file failed: {e}"))?;
        }

        // docker compose up -d
        let qp = super::client::shell_single_quote(&project);
        let qd = super::client::shell_single_quote(&dir);
        self.client
            .exec_checked(&format!(
                "docker compose -p {qp} -f {qd}/docker-compose.yml up -d"
            ))
            .map_err(|e| format!("docker compose up failed: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "project": project,
                "compose_file": compose_path,
            })),
        })
    }

    // ── query: docker_containers ─────────────────────────────

    /// List running containers via `docker ps`.
    pub fn list_containers(&self) -> Result<Vec<Value>, String> {
        // Use --format with Go template for consistent parsing
        let out = self
            .client
            .exec(
                "docker ps -a --format '{{.Names}}|{{.Image}}|{{.State}}|{{.Status}}|{{.Ports}}' --no-trunc 2>/dev/null || true",
            )
            .map_err(|e| format!("docker ps failed: {e}"))?;

        let mut rows = Vec::new();
        for line in out.stdout.lines() {
            let line = line.trim().trim_matches('\'');
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.splitn(5, '|').collect();
            if parts.len() < 5 {
                continue;
            }
            // Health status is embedded in the Status field
            let status_str = parts[3];
            let health = if status_str.contains("healthy") {
                "healthy"
            } else if status_str.contains("unhealthy") {
                "unhealthy"
            } else {
                "none"
            };

            rows.push(json!({
                "name": parts[0],
                "image": parts[1],
                "state": parts[2],
                "status": status_str,
                "ports": parts[4],
                "health": health,
            }));
        }
        Ok(rows)
    }

    // ── helpers ──────────────────────────────────────────────

    fn docker(&self, args: &[&str]) -> Result<String, String> {
        let cmd = format!(
            "docker {}",
            args.iter()
                .map(|a| super::client::shell_single_quote(a))
                .collect::<Vec<_>>()
                .join(" ")
        );
        self.client
            .exec_checked(&cmd)
            .map_err(|e| e.to_string())
    }

    fn container_exists(&self, name: &str) -> bool {
        let q = super::client::shell_single_quote(name);
        self.client
            .exec(&format!("docker inspect {q} >/dev/null 2>&1 && echo yes"))
            .map(|o| o.stdout.trim() == "yes")
            .unwrap_or(false)
    }

    fn volume_exists(&self, name: &str) -> bool {
        let q = super::client::shell_single_quote(name);
        self.client
            .exec(&format!(
                "docker volume inspect {q} >/dev/null 2>&1 && echo yes"
            ))
            .map(|o| o.stdout.trim() == "yes")
            .unwrap_or(false)
    }

    fn network_exists(&self, name: &str) -> bool {
        let q = super::client::shell_single_quote(name);
        self.client
            .exec(&format!(
                "docker network inspect {q} >/dev/null 2>&1 && echo yes"
            ))
            .map(|o| o.stdout.trim() == "yes")
            .unwrap_or(false)
    }
}

fn param_str(params: &Value, key: &str) -> Result<String, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| format!("missing required parameter: {key}"))
}

fn param_str_or(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssh::client::{ExecOutput, SshError, SshExec};
    use std::sync::Mutex;

    struct FakeExec {
        responses: Mutex<Vec<(String, ExecOutput)>>,
        seen_stdin: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl FakeExec {
        fn new(script: Vec<(&str, ExecOutput)>) -> Self {
            Self {
                responses: Mutex::new(
                    script
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                ),
                seen_stdin: Mutex::new(vec![]),
            }
        }
        fn respond(&self, cmd: &str) -> ExecOutput {
            let r = self.responses.lock().unwrap();
            for (needle, out) in r.iter() {
                if cmd.contains(needle.as_str()) {
                    return out.clone();
                }
            }
            ExecOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
            }
        }
    }

    impl SshExec for FakeExec {
        fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
            Ok(self.respond(cmd))
        }
        fn exec_with_stdin(&self, cmd: &str, stdin: &[u8]) -> Result<ExecOutput, SshError> {
            self.seen_stdin
                .lock()
                .unwrap()
                .push((cmd.to_string(), stdin.to_vec()));
            Ok(self.respond(cmd))
        }
    }

    fn ok(s: &str) -> ExecOutput {
        ExecOutput {
            stdout: s.to_string(),
            stderr: String::new(),
            exit_code: 0,
        }
    }

    #[test]
    fn container_created_when_not_existing() {
        let fake = FakeExec::new(vec![
            // container_exists returns no
            ("docker inspect", ok("")),
            // docker run succeeds
            ("docker 'run'", ok("abc123\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let params = json!({
            "id": "redis",
            "image": "redis:7-alpine",
            "ports": ["6380:6379"],
            "restart_policy": "unless-stopped",
        });
        let r = p.create("docker_container", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn container_unchanged_when_existing() {
        let fake = FakeExec::new(vec![
            ("docker inspect", ok("yes\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let params = json!({
            "id": "redis",
            "image": "redis:7-alpine",
        });
        let r = p.create("docker_container", &params).unwrap();
        assert_eq!(r.status, "unchanged");
    }

    #[test]
    fn volume_created() {
        let fake = FakeExec::new(vec![
            ("volume inspect", ok("")),
            ("volume 'create'", ok("my-vol\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let params = json!({"id": "my-vol"});
        let r = p.create("docker_volume", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn network_created_with_driver() {
        let fake = FakeExec::new(vec![
            ("network inspect", ok("")),
            ("network 'create'", ok("net-id\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let params = json!({"id": "passport-net", "driver": "bridge"});
        let r = p.create("docker_network", &params).unwrap();
        assert_eq!(r.status, "created");
        assert_eq!(r.outputs.unwrap()["driver"], "bridge");
    }

    #[test]
    fn compose_uploads_and_starts() {
        let fake = FakeExec::new(vec![
            ("docker compose", ok("")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let params = json!({
            "id": "passport",
            "project_name": "passport",
            "file": "version: '3'\nservices:\n  web:\n    image: nginx",
        });
        let r = p.create("docker_compose", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn list_containers_parses_docker_ps() {
        let output = "redis|redis:7-alpine|running|Up 2 days|0.0.0.0:6380->6379/tcp\n\
                       pg|postgis/postgis:15|running|Up 2 days (healthy)|0.0.0.0:5433->5432/tcp\n";
        let fake = FakeExec::new(vec![("docker ps", ok(output))]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        let rows = p.list_containers().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "redis");
        assert_eq!(rows[0]["health"], "none");
        assert_eq!(rows[1]["name"], "pg");
        assert_eq!(rows[1]["health"], "healthy");
    }

    #[test]
    fn unsupported_type() {
        let fake = FakeExec::new(vec![]);
        let client = SshClient::new(Box::new(fake));
        let p = DockerProvisioner::new(&client);
        assert!(p.create("docker_unknown", &json!({})).is_err());
    }
}
