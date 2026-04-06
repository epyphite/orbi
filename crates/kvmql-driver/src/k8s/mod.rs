//! Kubernetes provider for KVMQL/Orbi.
//!
//! Maps KVMQL resource types (`k8s_namespace`, `k8s_deployment`, `k8s_service`,
//! `k8s_ingress`, `k8s_configmap`, `k8s_secret`) to Kubernetes manifests by
//! shelling out to `kubectl`.  Same pattern as the GitHub driver shelling to
//! `gh` and the Azure driver shelling to `az` — keeps Kubernetes API client
//! state out of our codebase by delegating to `kubectl`.
//!
//! Unlike the other providers, the k8s module also exposes a live-query
//! engine that powers `SELECT * FROM k8s_pods WHERE ...` against a real
//! cluster (rather than the local registry).

pub mod cli;
pub mod query;
pub mod resources;

pub use cli::{KubectlCli, KubectlError};
pub use query::KubernetesQueryEngine;
pub use resources::KubernetesResourceProvisioner;
