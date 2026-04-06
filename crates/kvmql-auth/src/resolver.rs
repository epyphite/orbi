use std::fs;
use std::os::unix::fs::PermissionsExt;
use thiserror::Error;

use crate::vault_aws::AwsSecretsManagerResolver;
use crate::vault_azure::AzureKeyVaultResolver;
use crate::vault_gcp::GcpSecretManagerResolver;
use crate::vault_hashicorp::HashicorpVaultResolver;
use crate::vault_k8s::K8sSecretResolver;
use crate::vault_onepassword::OnePasswordResolver;
use crate::vault_sops::SopsResolver;

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("unknown credential scheme: {0}")]
    UnknownScheme(String),
    #[error("environment variable '{0}' not set")]
    EnvVarNotFound(String),
    #[error("credential file not found: {0}")]
    FileNotFound(String),
    #[error("credential file '{0}' has insecure permissions (world-readable)")]
    InsecurePermissions(String),
    #[error("failed to read credential file: {0}")]
    FileReadError(String),
    #[error("invalid credential reference: {0}")]
    InvalidReference(String),
    #[error("vault connection failed: {0}")]
    VaultConnectionFailed(String),
    #[error("vault authentication failed: {0}")]
    VaultAuthFailed(String),
    #[error("vault secret not found: {0}")]
    VaultSecretNotFound(String),
    #[error("external tool '{0}' not found in PATH")]
    ExternalToolNotFound(String),
    #[error("external tool '{tool}' failed: {stderr}")]
    ExternalToolFailed { tool: String, stderr: String },
}

pub struct CredentialResolver;

impl CredentialResolver {
    /// Resolve a credential reference to its secret value.
    ///
    /// Formats:
    /// - `env:VAR_NAME` — single env var
    /// - `env:VAR1,VAR2` — returns "val1,val2" (comma-joined)
    /// - `file:/path/to/secret` — read file contents, fail if world-readable
    /// - `vault:mount/path#field` — HashiCorp Vault KV v2
    /// - `aws-sm:secret-name#field` — AWS Secrets Manager
    /// - `gcp-sm:secret-ref` — GCP Secret Manager
    /// - `azure-kv:vault-name/secret-name` — Azure Key Vault
    /// - `op:vault/item#field` — 1Password CLI
    /// - `sops:/path/to/file#key.subkey` — Mozilla SOPS
    /// - `k8s:namespace/secret-name#key` — Kubernetes Secrets
    pub fn resolve(reference: &str) -> Result<String, CredentialError> {
        let (scheme, rest) = reference
            .split_once(':')
            .ok_or_else(|| CredentialError::InvalidReference(reference.to_string()))?;

        match scheme {
            "env" => Self::resolve_env(rest),
            "file" => Self::resolve_file(rest),
            "vault" => Self::resolve_vault(rest),
            "aws-sm" => Self::resolve_aws_sm(rest),
            "gcp-sm" => Self::resolve_gcp_sm(rest),
            "azure-kv" => Self::resolve_azure_kv(rest),
            "op" => Self::resolve_onepassword(rest),
            "sops" => Self::resolve_sops(rest),
            "k8s" => Self::resolve_k8s(rest),
            other => Err(CredentialError::UnknownScheme(other.to_string())),
        }
    }

    /// Return all credential scheme names supported by this resolver.
    pub fn supported_schemes() -> Vec<&'static str> {
        vec![
            "env", "file", "vault", "aws-sm", "gcp-sm", "azure-kv", "op", "sops", "k8s",
        ]
    }

    // ------------------------------------------------------------------
    // Env
    // ------------------------------------------------------------------

    fn resolve_env(var_spec: &str) -> Result<String, CredentialError> {
        // Handle comma-separated env vars: env:VAR1,VAR2
        let vars: Vec<&str> = var_spec.split(',').collect();
        let mut values = Vec::new();
        for var in &vars {
            let var = var.trim();
            let val = std::env::var(var)
                .map_err(|_| CredentialError::EnvVarNotFound(var.to_string()))?;
            values.push(val);
        }
        Ok(values.join(","))
    }

    // ------------------------------------------------------------------
    // File
    // ------------------------------------------------------------------

    fn resolve_file(path: &str) -> Result<String, CredentialError> {
        let metadata =
            fs::metadata(path).map_err(|_| CredentialError::FileNotFound(path.to_string()))?;

        // Check not world-readable (unix permissions)
        let mode = metadata.permissions().mode();
        if mode & 0o004 != 0 {
            return Err(CredentialError::InsecurePermissions(path.to_string()));
        }

        fs::read_to_string(path)
            .map(|s| s.trim().to_string())
            .map_err(|e| CredentialError::FileReadError(e.to_string()))
    }

    // ------------------------------------------------------------------
    // HashiCorp Vault
    // ------------------------------------------------------------------

    fn resolve_vault(rest: &str) -> Result<String, CredentialError> {
        // Format: mount/path#field  or  mount/path
        let (path_part, field) = match rest.split_once('#') {
            Some((p, f)) => (p, Some(f)),
            None => (rest, None),
        };

        // Split mount from path: first segment is mount, remainder is path
        let (mount, path) = path_part.split_once('/').ok_or_else(|| {
            CredentialError::InvalidReference(format!(
                "vault reference must be 'mount/path[#field]', got: {rest}"
            ))
        })?;

        let resolver = HashicorpVaultResolver::from_env()?;
        resolver.resolve(mount, path, field)
    }

    // ------------------------------------------------------------------
    // AWS Secrets Manager
    // ------------------------------------------------------------------

    fn resolve_aws_sm(rest: &str) -> Result<String, CredentialError> {
        let (secret_name, field) = match rest.split_once('#') {
            Some((s, f)) => (s, Some(f)),
            None => (rest, None),
        };
        AwsSecretsManagerResolver::resolve(secret_name, field)
    }

    // ------------------------------------------------------------------
    // GCP Secret Manager
    // ------------------------------------------------------------------

    fn resolve_gcp_sm(rest: &str) -> Result<String, CredentialError> {
        GcpSecretManagerResolver::resolve(rest)
    }

    // ------------------------------------------------------------------
    // Azure Key Vault
    // ------------------------------------------------------------------

    fn resolve_azure_kv(rest: &str) -> Result<String, CredentialError> {
        let (vault_name, secret_name) = AzureKeyVaultResolver::parse_ref(rest)?;
        AzureKeyVaultResolver::resolve(vault_name, secret_name)
    }

    // ------------------------------------------------------------------
    // 1Password
    // ------------------------------------------------------------------

    fn resolve_onepassword(rest: &str) -> Result<String, CredentialError> {
        let (vault, item, field) = OnePasswordResolver::parse_ref(rest)?;
        OnePasswordResolver::resolve(vault, item, field)
    }

    // ------------------------------------------------------------------
    // SOPS
    // ------------------------------------------------------------------

    fn resolve_sops(rest: &str) -> Result<String, CredentialError> {
        let (file_path, key_path) = SopsResolver::parse_ref(rest);
        SopsResolver::resolve(file_path, key_path)
    }

    // ------------------------------------------------------------------
    // Kubernetes
    // ------------------------------------------------------------------

    fn resolve_k8s(rest: &str) -> Result<String, CredentialError> {
        let (namespace, secret_name, key) = K8sSecretResolver::parse_ref(rest)?;
        K8sSecretResolver::resolve(namespace, secret_name, key)
    }
}

/// Return all credential scheme names supported by the resolver.
pub fn list_supported_schemes() -> Vec<&'static str> {
    CredentialResolver::supported_schemes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, Permissions};
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::NamedTempFile;

    // ==================================================================
    // Env tests
    // ==================================================================

    #[test]
    fn resolve_env_single() {
        std::env::set_var("KVMQL_TEST_CRED_SINGLE", "secret123");
        let result = CredentialResolver::resolve("env:KVMQL_TEST_CRED_SINGLE").unwrap();
        assert_eq!(result, "secret123");
    }

    #[test]
    fn resolve_env_multiple() {
        std::env::set_var("KVMQL_TEST_CRED_A", "alpha");
        std::env::set_var("KVMQL_TEST_CRED_B", "beta");
        let result =
            CredentialResolver::resolve("env:KVMQL_TEST_CRED_A,KVMQL_TEST_CRED_B").unwrap();
        assert_eq!(result, "alpha,beta");
    }

    #[test]
    fn resolve_env_missing() {
        std::env::remove_var("KVMQL_TEST_CRED_MISSING");
        let result = CredentialResolver::resolve("env:KVMQL_TEST_CRED_MISSING");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::EnvVarNotFound(ref v) if v == "KVMQL_TEST_CRED_MISSING"),
            "expected EnvVarNotFound, got: {err:?}"
        );
    }

    // ==================================================================
    // File tests
    // ==================================================================

    #[test]
    fn resolve_file_success() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "  file_secret  ").unwrap();
        let path = file.path().to_str().unwrap().to_string();

        fs::set_permissions(&path, Permissions::from_mode(0o600)).unwrap();

        let result = CredentialResolver::resolve(&format!("file:{path}")).unwrap();
        assert_eq!(result, "file_secret");
    }

    #[test]
    fn resolve_file_not_found() {
        let result = CredentialResolver::resolve("file:/tmp/kvmql_nonexistent_credential_file");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::FileNotFound(_)),
            "expected FileNotFound, got: {err:?}"
        );
    }

    #[test]
    fn resolve_file_insecure() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "leaked_secret").unwrap();
        let path = file.path().to_str().unwrap().to_string();

        fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap();

        let result = CredentialResolver::resolve(&format!("file:{path}"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::InsecurePermissions(_)),
            "expected InsecurePermissions, got: {err:?}"
        );
    }

    // ==================================================================
    // Vault dispatch tests
    // ==================================================================

    #[test]
    fn resolve_vault_without_running_server() {
        // If VAULT_ADDR/VAULT_TOKEN are not set we get VaultAuthFailed;
        // if they happen to be set (from a parallel test) we get a
        // connection error.  Either way, it must not succeed.
        let result = CredentialResolver::resolve("vault:secret/myapp#password");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::VaultAuthFailed(_) | CredentialError::VaultConnectionFailed(_)
            ),
            "expected VaultAuthFailed or VaultConnectionFailed, got: {err:?}"
        );
    }

    #[test]
    fn resolve_vault_invalid_ref_no_slash() {
        let result = CredentialResolver::resolve("vault:noseparator");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::InvalidReference(_)),
            "expected InvalidReference for vault ref without slash"
        );
    }

    // ==================================================================
    // Unknown scheme
    // ==================================================================

    #[test]
    fn resolve_unknown_scheme() {
        let result = CredentialResolver::resolve("s3:bucket/key");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::UnknownScheme(ref s) if s == "s3"),
            "expected UnknownScheme(\"s3\"), got: {err:?}"
        );
    }

    // ==================================================================
    // Invalid format
    // ==================================================================

    #[test]
    fn resolve_invalid_format() {
        let result = CredentialResolver::resolve("no-colon-here");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CredentialError::InvalidReference(_)),
            "expected InvalidReference, got: {err:?}"
        );
    }

    // ==================================================================
    // Dispatch routing tests (verifies correct scheme → backend mapping)
    // ==================================================================

    #[test]
    fn dispatch_aws_sm_returns_external_tool_error() {
        // aws CLI is unlikely to be named this way, so we expect either
        // ExternalToolNotFound (if aws not in PATH) or ExternalToolFailed
        let result = CredentialResolver::resolve("aws-sm:my-secret#password");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_) | CredentialError::ExternalToolFailed { .. }
            ),
            "expected external tool error for aws-sm, got: {err:?}"
        );
    }

    #[test]
    fn dispatch_gcp_sm_returns_external_tool_error() {
        let result = CredentialResolver::resolve("gcp-sm:my-project/my-secret");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_) | CredentialError::ExternalToolFailed { .. }
            ),
            "expected external tool error for gcp-sm, got: {err:?}"
        );
    }

    #[test]
    fn dispatch_azure_kv_returns_external_tool_error() {
        let result = CredentialResolver::resolve("azure-kv:myvault/mysecret");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_)
                    | CredentialError::ExternalToolFailed { .. }
                    | CredentialError::InvalidReference(_)
            ),
            "expected external tool or invalid ref error for azure-kv, got: {err:?}"
        );
    }

    #[test]
    fn dispatch_op_returns_external_tool_error() {
        let result = CredentialResolver::resolve("op:Personal/login#password");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_) | CredentialError::ExternalToolFailed { .. }
            ),
            "expected external tool error for op, got: {err:?}"
        );
    }

    #[test]
    fn dispatch_sops_returns_external_tool_error() {
        let result = CredentialResolver::resolve("sops:/etc/secrets/db.yaml#db.password");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_) | CredentialError::ExternalToolFailed { .. }
            ),
            "expected external tool error for sops, got: {err:?}"
        );
    }

    #[test]
    fn dispatch_k8s_returns_external_tool_error() {
        let result = CredentialResolver::resolve("k8s:default/my-secret#password");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CredentialError::ExternalToolNotFound(_) | CredentialError::ExternalToolFailed { .. }
            ),
            "expected external tool error for k8s, got: {err:?}"
        );
    }

    // ==================================================================
    // Reference parsing validation
    // ==================================================================

    #[test]
    fn azure_kv_invalid_ref_no_slash() {
        let result = CredentialResolver::resolve("azure-kv:novault");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::InvalidReference(_)),
            "expected InvalidReference for azure-kv without slash"
        );
    }

    #[test]
    fn op_invalid_ref_no_slash() {
        let result = CredentialResolver::resolve("op:noslash");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::InvalidReference(_)),
            "expected InvalidReference for op without slash"
        );
    }

    #[test]
    fn k8s_invalid_ref_no_key() {
        let result = CredentialResolver::resolve("k8s:default/my-secret");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::InvalidReference(_)),
            "expected InvalidReference for k8s without #key"
        );
    }

    #[test]
    fn k8s_invalid_ref_no_namespace() {
        let result = CredentialResolver::resolve("k8s:secretonly#key");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::InvalidReference(_)),
            "expected InvalidReference for k8s without namespace"
        );
    }

    // ==================================================================
    // list_supported_schemes
    // ==================================================================

    #[test]
    fn test_list_supported_schemes() {
        let schemes = list_supported_schemes();
        assert!(schemes.contains(&"env"));
        assert!(schemes.contains(&"file"));
        assert!(schemes.contains(&"vault"));
        assert!(schemes.contains(&"aws-sm"));
        assert!(schemes.contains(&"gcp-sm"));
        assert!(schemes.contains(&"azure-kv"));
        assert!(schemes.contains(&"op"));
        assert!(schemes.contains(&"sops"));
        assert!(schemes.contains(&"k8s"));
        assert_eq!(schemes.len(), 9);
    }
}
