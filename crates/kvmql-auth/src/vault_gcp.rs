//! GCP Secret Manager credential resolver.
//!
//! Reference format: `gcp-sm:project/secret-name/versions/latest`
//!
//! Uses the `gcloud` CLI so that standard GCP authentication is honoured.

use crate::resolver::CredentialError;
use crate::vault_aws::which_tool;
use std::process::Command;

pub struct GcpSecretManagerResolver;

impl GcpSecretManagerResolver {
    /// Resolve a secret from GCP Secret Manager via the gcloud CLI.
    ///
    /// `secret_ref` — a reference of the form `project/secret-name/versions/latest`
    ///   or just `secret-name` (in which case gcloud uses the default project
    ///   and the `latest` version).
    pub fn resolve(secret_ref: &str) -> Result<String, CredentialError> {
        which_tool("gcloud")?;

        // Parse the reference. Supported forms:
        //   project/secret-name/versions/VERSION
        //   secret-name  (use default project, version = latest)
        let (secret_name, version, project) = Self::parse_ref(secret_ref);

        let mut cmd = Command::new("gcloud");
        cmd.arg("secrets")
            .arg("versions")
            .arg("access")
            .arg(&version);

        cmd.arg("--secret").arg(&secret_name);

        if let Some(proj) = project {
            cmd.arg("--project").arg(proj);
        }

        let output = cmd.output().map_err(|e| CredentialError::ExternalToolFailed {
            tool: "gcloud".to_string(),
            stderr: e.to_string(),
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "gcloud".to_string(),
                stderr,
            });
        }

        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(value)
    }

    /// Parse a GCP secret reference.
    ///
    /// Returns `(secret_name, version, optional_project)`.
    fn parse_ref(secret_ref: &str) -> (String, String, Option<String>) {
        let parts: Vec<&str> = secret_ref.split('/').collect();
        match parts.len() {
            // "secret-name" → default project, latest
            1 => (parts[0].to_string(), "latest".to_string(), None),
            // "project/secret-name" → explicit project, latest
            2 => (
                parts[1].to_string(),
                "latest".to_string(),
                Some(parts[0].to_string()),
            ),
            // "project/secret-name/versions/VERSION"
            4 if parts[2] == "versions" => (
                parts[1].to_string(),
                parts[3].to_string(),
                Some(parts[0].to_string()),
            ),
            // Fallback: treat entire string as secret name
            _ => (secret_ref.to_string(), "latest".to_string(), None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ref_simple() {
        let (name, version, project) = GcpSecretManagerResolver::parse_ref("my-secret");
        assert_eq!(name, "my-secret");
        assert_eq!(version, "latest");
        assert!(project.is_none());
    }

    #[test]
    fn test_parse_ref_with_project() {
        let (name, version, project) =
            GcpSecretManagerResolver::parse_ref("my-project/my-secret");
        assert_eq!(name, "my-secret");
        assert_eq!(version, "latest");
        assert_eq!(project, Some("my-project".to_string()));
    }

    #[test]
    fn test_parse_ref_full() {
        let (name, version, project) =
            GcpSecretManagerResolver::parse_ref("my-project/my-secret/versions/3");
        assert_eq!(name, "my-secret");
        assert_eq!(version, "3");
        assert_eq!(project, Some("my-project".to_string()));
    }

    #[test]
    fn test_gcloud_not_installed() {
        let result = which_tool("kvmql_nonexistent_gcloud_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_gcloud_12345"
            ),
        );
    }
}
