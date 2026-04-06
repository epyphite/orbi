//! Live-query engine for Kubernetes cluster state.
//!
//! Unlike the registry-backed `SELECT` paths in the engine, this module
//! executes `kubectl get <kind> -o json` against a real cluster and
//! returns flattened rows that can be filtered with the existing WHERE
//! evaluator.
//!
//! This is the moat: `SELECT * FROM k8s_pods WHERE status = 'CrashLoopBackOff';`
//! against a live AKS/EKS/GKE/k3s cluster, no exporters, no Prometheus,
//! no kubectl piping into jq.

use serde_json::{json, Value};

use super::cli::KubectlCli;

#[derive(Debug, Clone)]
pub struct KubernetesQueryEngine {
    cli: Option<KubectlCli>,
}

impl KubernetesQueryEngine {
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

    /// Query a Kubernetes resource type and return rows as JSON.
    ///
    /// `noun` is the normalised KVMQL noun (e.g. `"k8s_pods"`).
    /// `namespace` is optional; `None` (or `Some("*")`/`Some("all")`)
    /// queries every namespace.
    pub fn query(
        &self,
        noun: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Value>, String> {
        let cli = self
            .cli
            .as_ref()
            .ok_or("kubectl not available — install kubectl and configure a kubeconfig")?;
        let kind = noun_to_kind(noun)?;
        let list = cli
            .get_list(kind, namespace)
            .map_err(|e| format!("kubectl get {kind} failed: {e}"))?;

        let items = list
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let rows = items
            .iter()
            .map(|item| flatten_k8s_item(item, kind))
            .collect();
        Ok(rows)
    }
}

/// Map a KVMQL noun to a `kubectl` kind name.
fn noun_to_kind(noun: &str) -> Result<&'static str, String> {
    match noun {
        "k8s_pods" => Ok("pods"),
        "k8s_deployments" => Ok("deployments"),
        "k8s_services" => Ok("services"),
        "k8s_ingresses" => Ok("ingresses"),
        "k8s_configmaps" => Ok("configmaps"),
        "k8s_secrets" => Ok("secrets"),
        "k8s_namespaces" => Ok("namespaces"),
        "k8s_nodes" => Ok("nodes"),
        other => Err(format!("unknown kubernetes noun: {other}")),
    }
}

/// Flatten a Kubernetes API object into a row that the existing
/// `eval_predicate` can filter on.  We extract the "interesting" fields
/// per kind so users can write WHERE on `status`, `replicas`, `ready`, etc.
fn flatten_k8s_item(item: &Value, kind: &str) -> Value {
    let name = item
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let namespace = item
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let created = item
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let uid = item
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let mut row = json!({
        "name": name,
        "namespace": namespace,
        "created_at": created,
        "uid": uid,
    });

    match kind {
        "pods" => {
            let phase = item
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let node = item
                .get("spec")
                .and_then(|s| s.get("nodeName"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let container_statuses = item
                .get("status")
                .and_then(|s| s.get("containerStatuses"))
                .and_then(|v| v.as_array());
            let (ready, restarts, reason) = container_statuses
                .map(|cs| {
                    let ready = !cs.is_empty()
                        && cs.iter().all(|c| {
                            c.get("ready").and_then(|v| v.as_bool()).unwrap_or(false)
                        });
                    let restarts: i64 = cs
                        .iter()
                        .map(|c| {
                            c.get("restartCount")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(0)
                        })
                        .sum();
                    // Surface waiting-state reasons (CrashLoopBackOff,
                    // ImagePullBackOff, ...) so users can filter on them
                    // directly via WHERE status = 'CrashLoopBackOff'.
                    let reason = cs
                        .iter()
                        .find_map(|c| {
                            c.get("state")
                                .and_then(|s| s.get("waiting"))
                                .and_then(|w| w.get("reason"))
                                .and_then(|v| v.as_str())
                        })
                        .unwrap_or("")
                        .to_string();
                    (ready, restarts, reason)
                })
                .unwrap_or((false, 0, String::new()));

            let effective_status = if !reason.is_empty() {
                reason
            } else {
                phase.to_string()
            };
            row["status"] = json!(effective_status);
            row["ready"] = json!(ready);
            row["restarts"] = json!(restarts);
            row["node"] = json!(node);
        }
        "deployments" => {
            let spec_replicas = item
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let ready_replicas = item
                .get("status")
                .and_then(|s| s.get("readyReplicas"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let available_replicas = item
                .get("status")
                .and_then(|s| s.get("availableReplicas"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            row["replicas"] = json!(spec_replicas);
            row["ready_replicas"] = json!(ready_replicas);
            row["available_replicas"] = json!(available_replicas);
        }
        "services" => {
            let svc_type = item
                .get("spec")
                .and_then(|s| s.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let cluster_ip = item
                .get("spec")
                .and_then(|s| s.get("clusterIP"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            row["type"] = json!(svc_type);
            row["cluster_ip"] = json!(cluster_ip);
        }
        "ingresses" => {
            let hosts = item
                .get("spec")
                .and_then(|s| s.get("rules"))
                .and_then(|v| v.as_array())
                .map(|rules| {
                    rules
                        .iter()
                        .filter_map(|r| {
                            r.get("host").and_then(|h| h.as_str()).map(String::from)
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            row["hosts"] = json!(hosts);
        }
        "nodes" => {
            let ready = item
                .get("status")
                .and_then(|s| s.get("conditions"))
                .and_then(|v| v.as_array())
                .and_then(|conds| {
                    conds.iter().find(|c| {
                        c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    })
                })
                .and_then(|c| c.get("status").and_then(|s| s.as_str()))
                .map(|s| s == "True")
                .unwrap_or(false);
            row["ready"] = json!(ready);
        }
        _ => {}
    }

    row
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noun_to_kind() {
        assert_eq!(noun_to_kind("k8s_pods").unwrap(), "pods");
        assert_eq!(noun_to_kind("k8s_deployments").unwrap(), "deployments");
        assert_eq!(noun_to_kind("k8s_services").unwrap(), "services");
        assert_eq!(noun_to_kind("k8s_ingresses").unwrap(), "ingresses");
        assert_eq!(noun_to_kind("k8s_configmaps").unwrap(), "configmaps");
        assert_eq!(noun_to_kind("k8s_secrets").unwrap(), "secrets");
        assert_eq!(noun_to_kind("k8s_namespaces").unwrap(), "namespaces");
        assert_eq!(noun_to_kind("k8s_nodes").unwrap(), "nodes");
        assert!(noun_to_kind("invalid").is_err());
    }

    #[test]
    fn test_flatten_pod_running() {
        let pod = json!({
            "metadata": {
                "name": "api-1",
                "namespace": "default",
                "uid": "abc",
                "creationTimestamp": "2026-01-01T00:00:00Z"
            },
            "spec": { "nodeName": "node-1" },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    { "ready": true, "restartCount": 0 }
                ]
            }
        });
        let row = flatten_k8s_item(&pod, "pods");
        assert_eq!(row["name"], "api-1");
        assert_eq!(row["namespace"], "default");
        assert_eq!(row["status"], "Running");
        assert_eq!(row["ready"], true);
        assert_eq!(row["restarts"], 0);
        assert_eq!(row["node"], "node-1");
    }

    #[test]
    fn test_flatten_pod_crashloop() {
        let pod = json!({
            "metadata": {
                "name": "api-1",
                "namespace": "default",
                "uid": "abc",
                "creationTimestamp": ""
            },
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {
                        "ready": false,
                        "restartCount": 15,
                        "state": { "waiting": { "reason": "CrashLoopBackOff" } }
                    }
                ]
            }
        });
        let row = flatten_k8s_item(&pod, "pods");
        // The waiting reason takes precedence over the bare phase so users
        // can filter `WHERE status = 'CrashLoopBackOff'` directly.
        assert_eq!(row["status"], "CrashLoopBackOff");
        assert_eq!(row["restarts"], 15);
        assert_eq!(row["ready"], false);
    }

    #[test]
    fn test_flatten_deployment() {
        let dep = json!({
            "metadata": {
                "name": "api",
                "namespace": "default",
                "uid": "d1",
                "creationTimestamp": ""
            },
            "spec": { "replicas": 3 },
            "status": { "readyReplicas": 2, "availableReplicas": 2 }
        });
        let row = flatten_k8s_item(&dep, "deployments");
        assert_eq!(row["replicas"], 3);
        assert_eq!(row["ready_replicas"], 2);
        assert_eq!(row["available_replicas"], 2);
    }

    #[test]
    fn test_flatten_service() {
        let svc = json!({
            "metadata": { "name": "api", "namespace": "default" },
            "spec": { "type": "LoadBalancer", "clusterIP": "10.0.0.42" }
        });
        let row = flatten_k8s_item(&svc, "services");
        assert_eq!(row["type"], "LoadBalancer");
        assert_eq!(row["cluster_ip"], "10.0.0.42");
    }

    #[test]
    fn test_flatten_node_ready() {
        let node = json!({
            "metadata": { "name": "node-1" },
            "status": {
                "conditions": [
                    { "type": "MemoryPressure", "status": "False" },
                    { "type": "Ready", "status": "True" }
                ]
            }
        });
        let row = flatten_k8s_item(&node, "nodes");
        assert_eq!(row["ready"], true);
    }

    #[test]
    fn test_flatten_ingress_hosts() {
        let ing = json!({
            "metadata": { "name": "api", "namespace": "default" },
            "spec": {
                "rules": [
                    { "host": "api.example.com" },
                    { "host": "api2.example.com" }
                ]
            }
        });
        let row = flatten_k8s_item(&ing, "ingresses");
        assert_eq!(row["hosts"], "api.example.com,api2.example.com");
    }
}
