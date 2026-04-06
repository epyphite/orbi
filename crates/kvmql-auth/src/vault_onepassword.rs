//! 1Password CLI credential resolver.
//!
//! Reference format: `op:vault/item#field`
//!
//! Uses the `op` CLI (1Password CLI v2+).

use crate::resolver::CredentialError;
use crate::vault_aws::which_tool;
use std::process::Command;

pub struct OnePasswordResolver;

impl OnePasswordResolver {
    /// Resolve a secret from 1Password via the op CLI.
    ///
    /// `vault` — the 1Password vault name
    /// `item`  — the item name or ID
    /// `field` — optional field label to extract; if `None`, returns the full
    ///           item as JSON
    pub fn resolve(
        vault: &str,
        item: &str,
        field: Option<&str>,
    ) -> Result<String, CredentialError> {
        which_tool("op")?;

        let mut cmd = Command::new("op");
        cmd.arg("item").arg("get").arg(item).arg("--vault").arg(vault);

        match field {
            Some(f) => {
                cmd.arg("--fields").arg(f);
            }
            None => {
                cmd.arg("--format").arg("json");
            }
        }

        let output = cmd.output().map_err(|e| CredentialError::ExternalToolFailed {
            tool: "op".to_string(),
            stderr: e.to_string(),
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "op".to_string(),
                stderr,
            });
        }

        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(value)
    }

    /// Parse an `op:vault/item#field` reference.
    ///
    /// Returns `(vault, item, Option<field>)`.
    pub(crate) fn parse_ref(rest: &str) -> Result<(&str, &str, Option<&str>), CredentialError> {
        // Split off optional #field first
        let (path_part, field) = match rest.split_once('#') {
            Some((p, f)) => (p, Some(f)),
            None => (rest, None),
        };

        let (vault, item) = path_part.split_once('/').ok_or_else(|| {
            CredentialError::InvalidReference(format!(
                "op reference must be 'vault/item[#field]', got: {rest}"
            ))
        })?;

        Ok((vault, item, field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ref_with_field() {
        let (vault, item, field) = OnePasswordResolver::parse_ref("Personal/login#password").unwrap();
        assert_eq!(vault, "Personal");
        assert_eq!(item, "login");
        assert_eq!(field, Some("password"));
    }

    #[test]
    fn test_parse_ref_without_field() {
        let (vault, item, field) = OnePasswordResolver::parse_ref("Work/api-key").unwrap();
        assert_eq!(vault, "Work");
        assert_eq!(item, "api-key");
        assert_eq!(field, None);
    }

    #[test]
    fn test_parse_ref_invalid() {
        let result = OnePasswordResolver::parse_ref("no-slash");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CredentialError::InvalidReference(_)
        ));
    }

    #[test]
    fn test_op_not_installed() {
        let result = which_tool("kvmql_nonexistent_op_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_op_12345"
            ),
        );
    }
}
