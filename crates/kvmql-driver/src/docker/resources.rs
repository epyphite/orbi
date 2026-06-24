use serde_json::Value;
use tracing::debug;

use crate::provision::{param_str, param_str_or, ProvisionError, ProvisionResult};

use super::cli::DockerCli;

/// Resource provisioner for Docker containers, networks, volumes, and Compose stacks.
pub struct DockerResourceProvisioner {
    pub(crate) cli: DockerCli,
}

impl DockerResourceProvisioner {
    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "docker_container" => self.create_container(params),
            "docker_network" => self.create_network(params),
            "docker_volume" => self.create_volume(params),
            "compose_stack" => self.compose_up(params),
            other => Err(ProvisionError::UnsupportedType(other.to_string())),
        }
    }

    pub fn delete(&self, resource_type: &str, id: &str) -> Result<(), ProvisionError> {
        match resource_type {
            "docker_container" => {
                debug!(provider = "docker", container = id, "removing container");
                self.cli.rm(id, true).map_err(ProvisionError::from)
            }
            "docker_network" => {
                debug!(provider = "docker", network = id, "removing network");
                self.cli.network_rm(id).map_err(ProvisionError::from)
            }
            "docker_volume" => {
                debug!(provider = "docker", volume = id, "removing volume");
                self.cli.volume_rm(id).map_err(ProvisionError::from)
            }
            "compose_stack" => self.compose_down(id),
            other => Err(ProvisionError::UnsupportedType(other.to_string())),
        }
    }

    pub fn update(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<ProvisionResult, ProvisionError> {
        match resource_type {
            "compose_stack" => self.compose_scale(id, params),
            _ => Err(ProvisionError::NotImplemented(format!(
                "update not supported for {resource_type}"
            ))),
        }
    }

    /// Discover running containers, networks, and volumes.
    pub fn discover(&self) -> Result<Vec<Value>, ProvisionError> {
        debug!(
            provider = "docker",
            "discovering containers, networks, volumes"
        );
        let mut entries = Vec::new();

        // Containers
        if let Ok(containers) = self.cli.ps_json() {
            for c in containers {
                let name = c["Names"]
                    .as_str()
                    .unwrap_or(c["ID"].as_str().unwrap_or("unknown"));
                entries.push(serde_json::json!({
                    "id": name,
                    "resource_type": "docker_container",
                    "status": c["Status"].as_str().unwrap_or("unknown"),
                    "config": {
                        "image": c["Image"].as_str().unwrap_or(""),
                        "ports": c.get("Ports").cloned().unwrap_or(Value::Null),
                    },
                }));
            }
        }

        // Networks (skip defaults)
        if let Ok(networks) = self.cli.network_ls_json() {
            for n in networks {
                let name = n["Name"].as_str().unwrap_or("");
                if matches!(name, "bridge" | "host" | "none") {
                    continue;
                }
                entries.push(serde_json::json!({
                    "id": name,
                    "resource_type": "docker_network",
                    "status": "active",
                    "config": {
                        "driver": n["Driver"].as_str().unwrap_or("bridge"),
                    },
                }));
            }
        }

        // Volumes
        if let Ok(volumes) = self.cli.volume_ls_json() {
            for v in volumes {
                let name = v["Name"].as_str().unwrap_or("");
                entries.push(serde_json::json!({
                    "id": name,
                    "resource_type": "docker_volume",
                    "status": "available",
                    "config": {
                        "driver": v["Driver"].as_str().unwrap_or("local"),
                    },
                }));
            }
        }

        Ok(entries)
    }

    // ── Internal ────────────────────────────────────────────────────

    fn create_container(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let name = param_str(params, "id")?;
        let image = param_str(params, "image")?;

        let mut extra_args: Vec<String> = Vec::new();

        // Port mappings: "ports" = "8080:80,443:443"
        if let Some(ports) = params.get("ports").and_then(|v| v.as_str()) {
            for mapping in ports.split(',') {
                extra_args.push("-p".into());
                extra_args.push(mapping.trim().into());
            }
        }

        // Environment variables: "env" = "KEY=val,KEY2=val2"
        if let Some(env) = params.get("env").and_then(|v| v.as_str()) {
            for pair in env.split(',') {
                extra_args.push("-e".into());
                extra_args.push(pair.trim().into());
            }
        }

        // Network
        if let Some(net) = params.get("network").and_then(|v| v.as_str()) {
            extra_args.push("--network".into());
            extra_args.push(net.into());
        }

        // Volumes: "volumes" = "/host:/container,named_vol:/data"
        if let Some(vols) = params.get("volumes").and_then(|v| v.as_str()) {
            for vol in vols.split(',') {
                extra_args.push("-v".into());
                extra_args.push(vol.trim().into());
            }
        }

        // Restart policy
        let restart = param_str_or(params, "restart", "unless-stopped");
        extra_args.push("--restart".into());
        extra_args.push(restart);

        // Command (optional)
        let command = params.get("command").and_then(|v| v.as_str());

        debug!(provider = "docker", container = %name, image = %image, "creating container");

        let refs: Vec<&str> = extra_args.iter().map(|s| s.as_str()).collect();
        let container_id = self
            .cli
            .run(&name, &image, &refs)
            .map_err(ProvisionError::from)?;

        // Get the IP if possible
        let ip = self.cli.inspect(&name).ok().and_then(|info| {
            info.pointer("/NetworkSettings/IPAddress")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        });

        let _ = command; // TODO: append command args to docker run

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({
                "container_id": container_id,
                "name": name,
                "image": image,
                "ip_address": ip,
            })),
        })
    }

    fn create_network(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let name = param_str(params, "id")?;
        let driver = param_str_or(params, "driver", "bridge");
        let subnet = params.get("subnet").and_then(|v| v.as_str());

        debug!(provider = "docker", network = %name, driver = %driver, "creating network");
        let net_id = self
            .cli
            .network_create(&name, &driver, subnet)
            .map_err(ProvisionError::from)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({
                "network_id": net_id,
                "name": name,
                "driver": driver,
            })),
        })
    }

    fn create_volume(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let name = param_str(params, "id")?;

        debug!(provider = "docker", volume = %name, "creating volume");
        let vol_name = self
            .cli
            .volume_create(&name)
            .map_err(ProvisionError::from)?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({
                "name": vol_name,
            })),
        })
    }

    fn compose_up(&self, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let project_dir = param_str(params, "path")?;
        let project_name = params.get("id").and_then(|v| v.as_str());

        debug!(provider = "docker", path = %project_dir, name = ?project_name, "compose up");
        let output = self
            .cli
            .compose_up(&project_dir, project_name)
            .map_err(ProvisionError::from)?;

        // List services after up
        let services = self
            .cli
            .compose_ps_json(&project_dir, project_name)
            .unwrap_or_default();

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(serde_json::json!({
                "path": project_dir,
                "services": services.len(),
                "output": output.trim(),
            })),
        })
    }

    fn compose_down(&self, id: &str) -> Result<(), ProvisionError> {
        // id could be a path or project name — try as path first
        let path = if std::path::Path::new(id).exists() {
            id.to_string()
        } else {
            // Assume current dir with project name
            ".".to_string()
        };

        debug!(provider = "docker", stack = id, "compose down");
        self.cli
            .compose_down(&path, Some(id))
            .map_err(ProvisionError::from)
    }

    fn compose_scale(&self, id: &str, params: &Value) -> Result<ProvisionResult, ProvisionError> {
        let service = param_str(params, "service")?;
        let count: u32 = param_str(params, "count")?
            .parse()
            .map_err(|_| ProvisionError::InvalidParam("count must be a positive integer".into()))?;

        let path = if std::path::Path::new(id).exists() {
            id.to_string()
        } else {
            ".".to_string()
        };

        debug!(provider = "docker", stack = id, service = %service, count, "compose scale");
        self.cli
            .compose_scale(&path, &service, count)
            .map_err(ProvisionError::from)?;

        Ok(ProvisionResult {
            status: "updated".into(),
            outputs: Some(serde_json::json!({
                "service": service,
                "replicas": count,
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported_resource_type() {
        let p = DockerResourceProvisioner {
            cli: DockerCli::new(),
        };
        let err = p.create("unknown", &serde_json::json!({})).unwrap_err();
        assert!(err.to_string().contains("unsupported resource type"));
    }

    #[test]
    fn test_delete_unsupported_type() {
        let p = DockerResourceProvisioner {
            cli: DockerCli::new(),
        };
        let err = p.delete("unknown", "test").unwrap_err();
        assert!(err.to_string().contains("unsupported resource type"));
    }

    #[test]
    fn test_update_unsupported() {
        let p = DockerResourceProvisioner {
            cli: DockerCli::new(),
        };
        let err = p
            .update("docker_container", "c1", &serde_json::json!({}))
            .unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_create_container_missing_image() {
        let p = DockerResourceProvisioner {
            cli: DockerCli::new(),
        };
        let err = p
            .create("docker_container", &serde_json::json!({"id": "test"}))
            .unwrap_err();
        assert!(err.to_string().contains("image"));
    }

    #[test]
    fn test_compose_scale_invalid_count() {
        let p = DockerResourceProvisioner {
            cli: DockerCli::new(),
        };
        let err = p
            .update(
                "compose_stack",
                "myapp",
                &serde_json::json!({"service": "web", "count": "abc"}),
            )
            .unwrap_err();
        assert!(err.to_string().contains("count"));
    }
}
