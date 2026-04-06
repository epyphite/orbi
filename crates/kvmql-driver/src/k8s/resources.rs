//! Kubernetes resource provisioner.
//!
//! Dispatches KVMQL `k8s_*` resource types to a live cluster by way of
//! `kubectl apply -f -` (with the YAML built in-process from the params).
//! Built from an optional context name; when absent, falls back to the
//! ambient `kubectl` current context if `kubectl` is on PATH.
//!
//! Supported resource types:
//! - `k8s_namespace`
//! - `k8s_deployment`
//! - `k8s_service`
//! - `k8s_ingress`
//! - `k8s_configmap`
//! - `k8s_secret`
//!
//! YAML is built via straight string concatenation (no `serde_yaml` dep) —
//! the resource shapes we emit are small enough that this is simpler and
//! avoids pulling in another crate.

use serde_json::{json, Value};

use super::cli::KubectlCli;

#[derive(Debug, Clone)]
pub struct KubernetesResourceProvisioner {
    cli: Option<KubectlCli>,
}

/// Result of a provisioning operation.  Mirrors the GitHub/Cloudflare/Azure
/// shape so the executor can route k8s alongside the others uniformly.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (kind, name, namespace, uid, ...).
    pub outputs: Option<Value>,
}

impl KubernetesResourceProvisioner {
    pub fn new(context: Option<&str>) -> Self {
        let cli = if KubectlCli::check_available().is_ok() {
            Some(match context {
                Some(ctx) => KubectlCli::with_context(ctx),
                None => KubectlCli::new(),
            })
        } else {
            None
        };
        Self { cli }
    }

    fn cli(&self) -> Result<&KubectlCli, String> {
        self.cli.as_ref().ok_or_else(|| {
            "kubectl CLI not found. Install from https://kubernetes.io/docs/tasks/tools/ \
             and ensure your kubeconfig is configured."
                .to_string()
        })
    }

    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        match resource_type {
            "k8s_namespace" => {
                let yaml = self.build_namespace(params)?;
                self.apply_yaml(yaml, "namespace", params)
            }
            "k8s_deployment" => {
                let yaml = self.build_deployment(params)?;
                self.apply_yaml(yaml, "deployment", params)
            }
            "k8s_service" => {
                let yaml = self.build_service(params)?;
                self.apply_yaml(yaml, "service", params)
            }
            "k8s_ingress" => {
                let yaml = self.build_ingress(params)?;
                self.apply_yaml(yaml, "ingress", params)
            }
            "k8s_configmap" => {
                let yaml = self.build_configmap(params)?;
                self.apply_yaml(yaml, "configmap", params)
            }
            "k8s_secret" => {
                let yaml = self.build_secret(params)?;
                self.apply_yaml(yaml, "secret", params)
            }
            other => Err(format!("unsupported kubernetes resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<(), String> {
        let kind = match resource_type {
            "k8s_namespace" => "namespace",
            "k8s_deployment" => "deployment",
            "k8s_service" => "service",
            "k8s_ingress" => "ingress",
            "k8s_configmap" => "configmap",
            "k8s_secret" => "secret",
            other => return Err(format!("unsupported kubernetes resource type: {other}")),
        };
        let ns = params.get("namespace").and_then(|v| v.as_str());
        // Cluster-scoped: namespaces ignore the -n flag.
        let ns_arg = if resource_type == "k8s_namespace" {
            None
        } else {
            ns
        };
        self.cli()?
            .delete(kind, id, ns_arg)
            .map_err(|e| format!("failed to delete {kind} '{id}': {e}"))?;
        Ok(())
    }

    fn apply_yaml(
        &self,
        yaml: String,
        kind: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let result = cli
            .apply_yaml(&yaml)
            .map_err(|e| format!("kubectl apply failed: {e}"))?;
        let name = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let ns = params
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "kind": kind,
                "name": name,
                "namespace": ns,
                "uid": result.get("metadata").and_then(|m| m.get("uid")),
                "apiVersion": result.get("apiVersion"),
            })),
        })
    }

    // ── YAML builders ────────────────────────────────────────
    // Each builder returns a YAML string ready for `kubectl apply -f -`.
    // We deliberately avoid `serde_yaml` — these shapes are small and the
    // builders fit on a screen each.

    fn build_namespace(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        Ok(format!(
            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {name}\n"
        ))
    }

    fn build_deployment(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        let namespace = param_str_or(params, "namespace", "default");
        let image = param_str(params, "image")?;
        let replicas = params
            .get("replicas")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let port = params.get("port").and_then(|v| v.as_i64());
        let container_name = param_str_or(params, "container_name", &name);

        let mut yaml = String::new();
        yaml.push_str("apiVersion: apps/v1\n");
        yaml.push_str("kind: Deployment\n");
        yaml.push_str(&format!(
            "metadata:\n  name: {name}\n  namespace: {namespace}\n"
        ));
        yaml.push_str(&format!("  labels:\n    app: {name}\n"));
        yaml.push_str("spec:\n");
        yaml.push_str(&format!("  replicas: {replicas}\n"));
        yaml.push_str(&format!(
            "  selector:\n    matchLabels:\n      app: {name}\n"
        ));
        yaml.push_str("  template:\n");
        yaml.push_str(&format!(
            "    metadata:\n      labels:\n        app: {name}\n"
        ));
        yaml.push_str("    spec:\n      containers:\n");
        yaml.push_str(&format!("      - name: {container_name}\n"));
        yaml.push_str(&format!("        image: {image}\n"));
        if let Some(p) = port {
            yaml.push_str(&format!(
                "        ports:\n        - containerPort: {p}\n"
            ));
        }
        // env vars from params.env (object)
        if let Some(env) = params.get("env").and_then(|v| v.as_object()) {
            yaml.push_str("        env:\n");
            for (k, v) in env {
                let val = v.as_str().unwrap_or("");
                yaml.push_str(&format!(
                    "        - name: {k}\n          value: \"{val}\"\n"
                ));
            }
        }
        Ok(yaml)
    }

    fn build_service(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        let namespace = param_str_or(params, "namespace", "default");
        let selector_app = param_str_or(params, "selector", &name);
        let svc_type = param_str_or(params, "type", "ClusterIP");
        let port = params
            .get("port")
            .and_then(|v| v.as_i64())
            .unwrap_or(80);
        let target_port = params
            .get("target_port")
            .and_then(|v| v.as_i64())
            .unwrap_or(port);

        let mut yaml = String::new();
        yaml.push_str("apiVersion: v1\n");
        yaml.push_str("kind: Service\n");
        yaml.push_str(&format!(
            "metadata:\n  name: {name}\n  namespace: {namespace}\n"
        ));
        yaml.push_str("spec:\n");
        yaml.push_str(&format!("  type: {svc_type}\n"));
        yaml.push_str(&format!(
            "  selector:\n    app: {selector_app}\n"
        ));
        yaml.push_str("  ports:\n");
        yaml.push_str(&format!(
            "  - port: {port}\n    targetPort: {target_port}\n    protocol: TCP\n"
        ));
        Ok(yaml)
    }

    fn build_ingress(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        let namespace = param_str_or(params, "namespace", "default");
        let host = param_str(params, "host")?;
        let service = param_str(params, "service")?;
        let port = params
            .get("port")
            .and_then(|v| v.as_i64())
            .unwrap_or(80);
        let path = param_str_or(params, "path", "/");
        let tls_secret = params.get("tls_secret").and_then(|v| v.as_str());
        let ingress_class = params.get("ingress_class").and_then(|v| v.as_str());

        let mut yaml = String::new();
        yaml.push_str("apiVersion: networking.k8s.io/v1\n");
        yaml.push_str("kind: Ingress\n");
        yaml.push_str(&format!(
            "metadata:\n  name: {name}\n  namespace: {namespace}\n"
        ));
        yaml.push_str("spec:\n");
        if let Some(class) = ingress_class {
            yaml.push_str(&format!("  ingressClassName: {class}\n"));
        }
        if let Some(secret) = tls_secret {
            yaml.push_str("  tls:\n");
            yaml.push_str(&format!(
                "  - hosts:\n    - {host}\n    secretName: {secret}\n"
            ));
        }
        yaml.push_str("  rules:\n");
        yaml.push_str(&format!(
            "  - host: {host}\n    http:\n      paths:\n"
        ));
        yaml.push_str(&format!(
            "      - path: {path}\n        pathType: Prefix\n"
        ));
        yaml.push_str(&format!(
            "        backend:\n          service:\n            name: {service}\n            port:\n              number: {port}\n"
        ));
        Ok(yaml)
    }

    fn build_configmap(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        let namespace = param_str_or(params, "namespace", "default");
        let data = params
            .get("data")
            .and_then(|v| v.as_object())
            .ok_or("configmap requires 'data' object")?;

        let mut yaml = String::new();
        yaml.push_str("apiVersion: v1\n");
        yaml.push_str("kind: ConfigMap\n");
        yaml.push_str(&format!(
            "metadata:\n  name: {name}\n  namespace: {namespace}\n"
        ));
        yaml.push_str("data:\n");
        for (k, v) in data {
            let val = v.as_str().unwrap_or("");
            // Use literal block scalar so multi-line values pass through.
            yaml.push_str(&format!("  {k}: |\n    {val}\n"));
        }
        Ok(yaml)
    }

    fn build_secret(&self, params: &Value) -> Result<String, String> {
        let name = param_str(params, "id")?;
        let namespace = param_str_or(params, "namespace", "default");
        let data = params
            .get("data")
            .and_then(|v| v.as_object())
            .ok_or("secret requires 'data' object")?;

        let mut yaml = String::new();
        yaml.push_str("apiVersion: v1\n");
        yaml.push_str("kind: Secret\n");
        yaml.push_str(&format!(
            "metadata:\n  name: {name}\n  namespace: {namespace}\n"
        ));
        yaml.push_str("type: Opaque\n");
        // stringData — kubectl auto-encodes to base64 server-side, which
        // means we never have to ship a base64 dep just for secrets.
        yaml.push_str("stringData:\n");
        for (k, v) in data {
            let val = v.as_str().unwrap_or("");
            yaml.push_str(&format!("  {k}: \"{val}\"\n"));
        }
        Ok(yaml)
    }

    // ── EXPLAIN support ──────────────────────────────────────

    /// Build a human-readable description of the kubectl call(s) a create
    /// would emit, without actually executing them.  Used by EXPLAIN and
    /// dry-run.  Secret data is NEVER included in the output.
    pub fn build_create_args(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<Vec<String>, String> {
        let (kind, yaml) = match resource_type {
            "k8s_namespace" => ("Namespace", self.build_namespace(params)?),
            "k8s_deployment" => ("Deployment", self.build_deployment(params)?),
            "k8s_service" => ("Service", self.build_service(params)?),
            "k8s_ingress" => ("Ingress", self.build_ingress(params)?),
            "k8s_configmap" => ("ConfigMap", self.build_configmap(params)?),
            "k8s_secret" => ("Secret", self.build_secret(params)?),
            other => return Err(format!("unsupported kubernetes resource type: {other}")),
        };
        let name = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let ns = params
            .get("namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        // Redact secret data in the EXPLAIN preview entirely — never let
        // any value from a `k8s_secret` leak into the rendered command.
        let yaml_preview = if resource_type == "k8s_secret" {
            "kind: Secret (data redacted)".to_string()
        } else {
            yaml.lines().take(5).collect::<Vec<_>>().join(" | ")
        };
        Ok(vec![
            "apply -f -".into(),
            format!("kind={kind}"),
            format!("name={name}"),
            format!("namespace={ns}"),
            yaml_preview,
        ])
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

    fn p() -> KubernetesResourceProvisioner {
        KubernetesResourceProvisioner::new(None)
    }

    #[test]
    fn test_build_namespace() {
        let params = json!({"id": "orbital-pay"});
        let yaml = p().build_namespace(&params).unwrap();
        assert!(yaml.contains("kind: Namespace"));
        assert!(yaml.contains("name: orbital-pay"));
    }

    #[test]
    fn test_build_deployment_basic() {
        let params = json!({
            "id": "api",
            "namespace": "orbital-pay",
            "image": "acr.azurecr.io/api:latest",
            "replicas": 3,
            "port": 8080
        });
        let yaml = p().build_deployment(&params).unwrap();
        assert!(yaml.contains("kind: Deployment"));
        assert!(yaml.contains("name: api"));
        assert!(yaml.contains("namespace: orbital-pay"));
        assert!(yaml.contains("replicas: 3"));
        assert!(yaml.contains("containerPort: 8080"));
        assert!(yaml.contains("image: acr.azurecr.io/api:latest"));
    }

    #[test]
    fn test_build_deployment_with_env() {
        let params = json!({
            "id": "api",
            "image": "nginx",
            "env": { "LOG_LEVEL": "info", "PORT": "8080" }
        });
        let yaml = p().build_deployment(&params).unwrap();
        assert!(yaml.contains("env:"));
        assert!(yaml.contains("name: LOG_LEVEL"));
        assert!(yaml.contains("value: \"info\""));
    }

    #[test]
    fn test_build_deployment_default_replicas() {
        let params = json!({"id": "api", "image": "nginx"});
        let yaml = p().build_deployment(&params).unwrap();
        assert!(yaml.contains("replicas: 1"));
        // No port specified -> no ports stanza
        assert!(!yaml.contains("containerPort"));
    }

    #[test]
    fn test_build_service_loadbalancer() {
        let params = json!({
            "id": "api",
            "type": "LoadBalancer",
            "port": 80,
            "target_port": 8080
        });
        let yaml = p().build_service(&params).unwrap();
        assert!(yaml.contains("kind: Service"));
        assert!(yaml.contains("type: LoadBalancer"));
        assert!(yaml.contains("port: 80"));
        assert!(yaml.contains("targetPort: 8080"));
    }

    #[test]
    fn test_build_service_default_clusterip() {
        let params = json!({"id": "api"});
        let yaml = p().build_service(&params).unwrap();
        assert!(yaml.contains("type: ClusterIP"));
        assert!(yaml.contains("port: 80"));
        assert!(yaml.contains("targetPort: 80"));
    }

    #[test]
    fn test_build_ingress_with_tls() {
        let params = json!({
            "id": "api",
            "host": "api.orbital-pay.com",
            "service": "api",
            "port": 80,
            "tls_secret": "api-tls"
        });
        let yaml = p().build_ingress(&params).unwrap();
        assert!(yaml.contains("kind: Ingress"));
        assert!(yaml.contains("host: api.orbital-pay.com"));
        assert!(yaml.contains("secretName: api-tls"));
    }

    #[test]
    fn test_build_ingress_with_class() {
        let params = json!({
            "id": "api",
            "host": "api.example.com",
            "service": "api",
            "ingress_class": "nginx"
        });
        let yaml = p().build_ingress(&params).unwrap();
        assert!(yaml.contains("ingressClassName: nginx"));
    }

    #[test]
    fn test_build_configmap() {
        let params = json!({
            "id": "app-config",
            "data": { "DATABASE_URL": "postgres://...", "LOG_LEVEL": "info" }
        });
        let yaml = p().build_configmap(&params).unwrap();
        assert!(yaml.contains("kind: ConfigMap"));
        assert!(yaml.contains("DATABASE_URL:"));
        assert!(yaml.contains("LOG_LEVEL:"));
    }

    #[test]
    fn test_build_configmap_requires_data() {
        let params = json!({"id": "app-config"});
        assert!(p().build_configmap(&params).is_err());
    }

    #[test]
    fn test_build_secret_uses_stringdata() {
        let params = json!({
            "id": "api-tls",
            "data": { "tls.crt": "---BEGIN CERT---", "tls.key": "---BEGIN KEY---" }
        });
        let yaml = p().build_secret(&params).unwrap();
        assert!(yaml.contains("kind: Secret"));
        assert!(yaml.contains("type: Opaque"));
        assert!(yaml.contains("stringData:"));
    }

    #[test]
    fn test_explain_secret_redacted() {
        let params = json!({
            "id": "api-tls",
            "data": { "password": "super-secret" }
        });
        let args = p().build_create_args("k8s_secret", &params).unwrap();
        // Ensure the secret value is not anywhere in the EXPLAIN output.
        assert!(args.iter().all(|a| !a.contains("super-secret")));
        assert!(args.iter().any(|a| a.contains("redacted")));
    }

    #[test]
    fn test_explain_namespace_args() {
        let params = json!({"id": "orbital-pay"});
        let args = p().build_create_args("k8s_namespace", &params).unwrap();
        assert!(args.iter().any(|a| a == "apply -f -"));
        assert!(args.iter().any(|a| a == "kind=Namespace"));
        assert!(args.iter().any(|a| a == "name=orbital-pay"));
    }

    #[test]
    fn test_unsupported_resource_type() {
        let result = p().create("k8s_unknown", &json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_param_str_missing() {
        let params = json!({});
        assert!(param_str(&params, "id").is_err());
    }
}
