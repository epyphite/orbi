//! Azure Key Vault credential resolver.
//!
//! Reference format: `azure-kv:vault-name/secret-name`
//!
//! Uses the `az` CLI so that standard Azure authentication is honoured.

use crate::resolver::CredentialError;
use crate::vault_aws::which_tool;
use std::process::Command;

pub struct AzureKeyVaultResolver;

impl AzureKeyVaultResolver {
    /// Resolve a secret from Azure Key Vault via the az CLI.
    ///
    /// `vault_name`  — the Key Vault name
    /// `secret_name` — the secret name within that vault
    pub fn resolve(vault_name: &str, secret_name: &str) -> Result<String, CredentialError> {
        which_tool("az")?;

        let output = Command::new("az")
            .arg("keyvault")
            .arg("secret")
            .arg("show")
            .arg("--vault-name")
            .arg(vault_name)
            .arg("--name")
            .arg(secret_name)
            .arg("--query")
            .arg("value")
            .arg("-o")
            .arg("tsv")
            .output()
            .map_err(|e| CredentialError::ExternalToolFailed {
                tool: "az".to_string(),
                stderr: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "az".to_string(),
                stderr,
            });
        }

        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(value)
    }

    /// Parse an `azure-kv:vault-name/secret-name` reference.
    ///
    /// Returns `(vault_name, secret_name)`.
    pub(crate) fn parse_ref(rest: &str) -> Result<(&str, &str), CredentialError> {
        rest.split_once('/').ok_or_else(|| {
            CredentialError::InvalidReference(format!(
                "azure-kv reference must be 'vault-name/secret-name', got: {rest}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ref_valid() {
        let (vault, secret) = AzureKeyVaultResolver::parse_ref("myvault/mysecret").unwrap();
        assert_eq!(vault, "myvault");
        assert_eq!(secret, "mysecret");
    }

    #[test]
    fn test_parse_ref_invalid_no_slash() {
        let result = AzureKeyVaultResolver::parse_ref("novaultslash");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CredentialError::InvalidReference(_)
        ));
    }

    #[test]
    fn test_az_not_installed() {
        let result = which_tool("kvmql_nonexistent_az_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_az_12345"
            ),
        );
    }
}
