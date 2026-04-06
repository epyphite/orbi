//! Mozilla SOPS encrypted-file credential resolver.
//!
//! Reference format: `sops:/path/to/file.yaml#key.subkey`
//!
//! Uses the `sops` CLI to decrypt files or extract specific keys.

use crate::resolver::CredentialError;
use crate::vault_aws::which_tool;
use std::process::Command;

pub struct SopsResolver;

impl SopsResolver {
    /// Resolve a secret from a SOPS-encrypted file.
    ///
    /// `file_path` — path to the encrypted file (YAML, JSON, etc.)
    /// `key_path`  — optional dotted key path (e.g. `"db.password"`);
    ///               if `None`, the entire decrypted file is returned.
    pub fn resolve(file_path: &str, key_path: Option<&str>) -> Result<String, CredentialError> {
        which_tool("sops")?;

        let mut cmd = Command::new("sops");
        cmd.arg("--decrypt");

        if let Some(kp) = key_path {
            // Convert dotted path `a.b.c` → sops extract expression `["a"]["b"]["c"]`
            let extract_expr = Self::dotted_to_extract(kp);
            cmd.arg("--extract").arg(&extract_expr);
        }

        cmd.arg(file_path);

        let output = cmd.output().map_err(|e| CredentialError::ExternalToolFailed {
            tool: "sops".to_string(),
            stderr: e.to_string(),
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(CredentialError::ExternalToolFailed {
                tool: "sops".to_string(),
                stderr,
            });
        }

        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(value)
    }

    /// Parse a `sops:/path/to/file#key.subkey` reference.
    ///
    /// Returns `(file_path, Option<key_path>)`.
    pub(crate) fn parse_ref(rest: &str) -> (&str, Option<&str>) {
        match rest.split_once('#') {
            Some((path, key)) => (path, Some(key)),
            None => (rest, None),
        }
    }

    /// Convert a dotted key path into the sops `--extract` expression format.
    ///
    /// `"db.password"` → `'["db"]["password"]'`
    fn dotted_to_extract(dotted: &str) -> String {
        dotted
            .split('.')
            .map(|part| format!("[\"{part}\"]"))
            .collect::<String>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dotted_to_extract_single() {
        assert_eq!(SopsResolver::dotted_to_extract("password"), "[\"password\"]");
    }

    #[test]
    fn test_dotted_to_extract_nested() {
        assert_eq!(
            SopsResolver::dotted_to_extract("db.password"),
            "[\"db\"][\"password\"]"
        );
    }

    #[test]
    fn test_dotted_to_extract_deep() {
        assert_eq!(
            SopsResolver::dotted_to_extract("a.b.c.d"),
            "[\"a\"][\"b\"][\"c\"][\"d\"]"
        );
    }

    #[test]
    fn test_parse_ref_with_key() {
        let (path, key) = SopsResolver::parse_ref("/etc/secrets/db.yaml#db.password");
        assert_eq!(path, "/etc/secrets/db.yaml");
        assert_eq!(key, Some("db.password"));
    }

    #[test]
    fn test_parse_ref_without_key() {
        let (path, key) = SopsResolver::parse_ref("/etc/secrets/db.yaml");
        assert_eq!(path, "/etc/secrets/db.yaml");
        assert_eq!(key, None);
    }

    #[test]
    fn test_sops_not_installed() {
        let result = which_tool("kvmql_nonexistent_sops_12345");
        assert!(result.is_err());
        assert!(
            matches!(
                result.unwrap_err(),
                CredentialError::ExternalToolNotFound(ref t) if t == "kvmql_nonexistent_sops_12345"
            ),
        );
    }
}
