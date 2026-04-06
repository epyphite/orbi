//! Kubernetes Secrets credential resolver.
//!
//! Reference format: `k8s:namespace/secret-name#key`
//!
//! Uses `kubectl` to retrieve secrets and base64-decodes the values.

use crate::resolver::CredentialError;
use crate::vault_aws::which_tool;
use std::process::Command;

pub struct K8sSecretResolver;

impl K8sSecretResolver {
    /// Resolve a secret from a Kubernetes Secret resource.
    ///
    /// `namespace`   — the Kubernetes namespace
    /// `secret_name` — the Secret resource name
    /// `key`         — the data key within the Secret
    pub fn resolve(
        namespace: &str,
        secret_name: &str,
        key: &str,
    ) -> Result<String, CredentialError> {
        which_tool("kubectl")?;

        // Use jsonpath to get the base64-encoded value directly
        let jsonpath = format!("{{.data.{key}}}");

        let output = Command::new("kubectl")
            .arg("get")
            .arg("secret")
            .arg(secret_name)
            .arg("-n")
            .arg(namespace)
            .arg("-o")
            .arg(format!("jsonpath={jsonpath}"))
            .output()
            .map_err(|e| CredentialError::ExternalToolFailed {
                tool: "kubectl".to_string(),
                stderr: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "kubectl".to_string(),
                stderr,
            });
        }

        let encoded = String::from_utf8_lossy(&output.stdout).trim().to_string();

        if encoded.is_empty() {
            return Err(CredentialError::VaultSecretNotFound(format!(
                "key '{key}' not found in secret '{namespace}/{secret_name}'"
            )));
        }

        // Base64-decode the value
        Self::base64_decode(&encoded).map_err(|e| CredentialError::ExternalToolFailed {
            tool: "kubectl".to_string(),
            stderr: format!("failed to base64-decode secret value: {e}"),
        })
    }

    /// Parse a `k8s:namespace/secret-name#key` reference.
    ///
    /// Returns `(namespace, secret_name, key)`.
    pub(crate) fn parse_ref(rest: &str) -> Result<(&str, &str, &str), CredentialError> {
        let (path_part, key) = rest.split_once('#').ok_or_else(|| {
            CredentialError::InvalidReference(format!(
                "k8s reference must include #key, got: {rest}"
            ))
        })?;

        let (namespace, secret_name) = path_part.split_once('/').ok_or_else(|| {
            CredentialError::InvalidReference(format!(
                "k8s reference must be 'namespace/secret-name#key', got: {rest}"
            ))
        })?;

        Ok((namespace, secret_name, key))
    }

    /// Decode a base64 string (standard alphabet, with or without padding).
    fn base64_decode(input: &str) -> Result<String, String> {
        // Use a simple base64 decoder to avoid an extra dependency.
        // We shell out to `base64 --decode` for portability.
        let output = Command::new("base64")
            .arg("--decode")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                if let Some(ref mut stdin) = child.stdin {
                    stdin.write_all(input.as_bytes()).ok();
                }
                child.wait_with_output()
            })
            .map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }

        String::from_utf8(output.stdout).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ref_valid() {
        let (ns, name, key) =
            K8sSecretResolver::parse_ref("default/my-secret#password").unwrap();
        assert_eq!(ns, "default");
        assert_eq!(name, "my-secret");
        assert_eq!(key, "password");
    }

    #[test]
    fn test_parse_ref_missing_key() {
        let result = K8sSecretResolver::parse_ref("default/my-secret");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CredentialError::InvalidReference(_)
        ));
    }

    #[test]
    fn test_parse_ref_missing_namespace() {
        let result = K8sSecretResolver::parse_ref("my-secret#password");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CredentialError::InvalidReference(_)
        ));
    }

    #[test]
    fn test_base64_decode() {
        // "hello" in base64 is "aGVsbG8="
        let decoded = K8sSecretResolver::base64_decode("aGVsbG8=").unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn test_kubectl_not_installed() {
        let result = which_tool("kvmql_nonexistent_kubectl_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_kubectl_12345"
            ),
        );
    }
}
