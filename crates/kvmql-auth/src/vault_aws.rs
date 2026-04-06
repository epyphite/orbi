//! AWS Secrets Manager credential resolver.
//!
//! Reference format: `aws-sm:secret-name#field` or `aws-sm:secret-name`
//!
//! Uses the `aws` CLI (`aws secretsmanager get-secret-value`) so that
//! standard AWS credential chains (env vars, profiles, IMDS) are honoured
//! without pulling in the full AWS SDK.

use crate::resolver::CredentialError;
use serde_json::Value;
use std::process::Command;

pub struct AwsSecretsManagerResolver;

impl AwsSecretsManagerResolver {
    /// Resolve a secret from AWS Secrets Manager via the AWS CLI.
    ///
    /// `secret_name` — the secret name or ARN
    /// `field`       — optional JSON key to extract from the secret string
    pub fn resolve(secret_name: &str, field: Option<&str>) -> Result<String, CredentialError> {
        Self::check_cli_available()?;

        let output = Command::new("aws")
            .arg("secretsmanager")
            .arg("get-secret-value")
            .arg("--secret-id")
            .arg(secret_name)
            .arg("--query")
            .arg("SecretString")
            .arg("--output")
            .arg("text")
            .output()
            .map_err(|e| CredentialError::ExternalToolFailed {
                tool: "aws".to_string(),
                stderr: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "aws".to_string(),
                stderr,
            });
        }

        let secret_string = String::from_utf8_lossy(&output.stdout).trim().to_string();

        match field {
            Some(f) => {
                let parsed: Value = serde_json::from_str(&secret_string).map_err(|e| {
                    CredentialError::ExternalToolFailed {
                        tool: "aws".to_string(),
                        stderr: format!("failed to parse secret as JSON: {e}"),
                    }
                })?;
                let value = parsed.get(f).ok_or_else(|| {
                    CredentialError::VaultSecretNotFound(format!(
                        "field '{f}' not found in secret '{secret_name}'"
                    ))
                })?;
                match value {
                    Value::String(s) => Ok(s.clone()),
                    other => Ok(other.to_string()),
                }
            }
            None => Ok(secret_string),
        }
    }

    fn check_cli_available() -> Result<(), CredentialError> {
        which_tool("aws")
    }
}

/// Check whether a CLI tool is available in `PATH`.
pub(crate) fn which_tool(name: &str) -> Result<(), CredentialError> {
    match Command::new("which").arg(name).output() {
        Ok(output) if output.status.success() => Ok(()),
        _ => Err(CredentialError::ExternalToolNotFound(name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_reference_with_field() {
        // Test that our parsing logic (done in resolver.rs) works for aws-sm references.
        let reference = "my-database-creds#password";
        let (secret, field) = match reference.split_once('#') {
            Some((s, f)) => (s, Some(f)),
            None => (reference, None),
        };
        assert_eq!(secret, "my-database-creds");
        assert_eq!(field, Some("password"));
    }

    #[test]
    fn test_parse_reference_without_field() {
        let reference = "my-simple-secret";
        let (secret, field) = match reference.split_once('#') {
            Some((s, f)) => (s, Some(f)),
            None => (reference, None),
        };
        assert_eq!(secret, "my-simple-secret");
        assert_eq!(field, None);
    }

    #[test]
    fn test_aws_cli_not_installed() {
        // This test checks the which_tool function with a non-existent tool
        let result = which_tool("kvmql_nonexistent_cli_tool_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_cli_tool_12345"
            ),
            "expected ExternalToolNotFound"
        );
    }
}
