//! HashiCorp Vault KV v2 credential resolver.
//!
//! Reference format: `vault:mount/path#field`
//!
//! Reads `VAULT_ADDR` and `VAULT_TOKEN` from environment variables
//! and makes an HTTP GET to `{VAULT_ADDR}/v1/{mount}/data/{path}`.

use crate::resolver::CredentialError;
use reqwest::blocking::Client;
use serde_json::Value;

#[derive(Debug)]
pub struct HashicorpVaultResolver {
    address: String,
    token: String,
}

impl HashicorpVaultResolver {
    /// Create a resolver from `VAULT_ADDR` and `VAULT_TOKEN` environment variables.
    pub fn from_env() -> Result<Self, CredentialError> {
        let address = std::env::var("VAULT_ADDR").map_err(|_| {
            CredentialError::VaultAuthFailed("VAULT_ADDR environment variable not set".to_string())
        })?;
        let token = std::env::var("VAULT_TOKEN").map_err(|_| {
            CredentialError::VaultAuthFailed(
                "VAULT_TOKEN environment variable not set".to_string(),
            )
        })?;
        Ok(Self { address, token })
    }

    /// Resolve a secret from Vault KV v2.
    ///
    /// `mount` — the secrets engine mount path (e.g. "secret")
    /// `path`  — the secret path within that mount (e.g. "myapp/db")
    /// `field` — optional field name within the secret data
    pub fn resolve(
        &self,
        mount: &str,
        path: &str,
        field: Option<&str>,
    ) -> Result<String, CredentialError> {
        let url = format!(
            "{}/v1/{}/data/{}",
            self.address.trim_end_matches('/'),
            mount,
            path
        );

        let client = Client::new();
        let response = client
            .get(&url)
            .header("X-Vault-Token", &self.token)
            .send()
            .map_err(|e| CredentialError::VaultConnectionFailed(e.to_string()))?;

        let status = response.status();
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(CredentialError::VaultAuthFailed(format!(
                "403 Forbidden for {url}"
            )));
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(CredentialError::VaultSecretNotFound(format!(
                "{mount}/{path}"
            )));
        }
        if !status.is_success() {
            return Err(CredentialError::VaultConnectionFailed(format!(
                "Vault returned HTTP {status} for {url}"
            )));
        }

        let body: Value = response
            .json()
            .map_err(|e| CredentialError::VaultConnectionFailed(e.to_string()))?;

        // KV v2 response shape: { "data": { "data": { ... }, "metadata": { ... } } }
        let data = body
            .get("data")
            .and_then(|d| d.get("data"))
            .ok_or_else(|| {
                CredentialError::VaultSecretNotFound(format!(
                    "unexpected response structure for {mount}/{path}"
                ))
            })?;

        match field {
            Some(f) => {
                let value = data.get(f).ok_or_else(|| {
                    CredentialError::VaultSecretNotFound(format!(
                        "field '{f}' not found in {mount}/{path}"
                    ))
                })?;
                // Return string values unquoted, everything else as JSON
                match value {
                    Value::String(s) => Ok(s.clone()),
                    other => Ok(other.to_string()),
                }
            }
            None => {
                // Return the entire data object as a JSON string
                Ok(data.to_string())
            }
        }
    }
}

/// Build the Vault URL for a given mount and path.
pub fn build_vault_url(address: &str, mount: &str, path: &str) -> String {
    format!(
        "{}/v1/{}/data/{}",
        address.trim_end_matches('/'),
        mount,
        path
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vault_url() {
        assert_eq!(
            build_vault_url("https://vault.example.com", "secret", "myapp/db"),
            "https://vault.example.com/v1/secret/data/myapp/db"
        );
    }

    #[test]
    fn test_build_vault_url_trailing_slash() {
        assert_eq!(
            build_vault_url("https://vault.example.com/", "secret", "myapp"),
            "https://vault.example.com/v1/secret/data/myapp"
        );
    }

    /// All env-var-dependent tests are combined into one test function to
    /// avoid races with parallel tests that mutate the same process-global
    /// environment variables.
    #[test]
    fn test_from_env_scenarios() {
        // 1. Missing both vars → VaultAuthFailed
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
        let result = HashicorpVaultResolver::from_env();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::VaultAuthFailed(ref msg) if msg.contains("VAULT_ADDR")),
            "expected VaultAuthFailed for VAULT_ADDR"
        );

        // 2. VAULT_ADDR set, VAULT_TOKEN missing → VaultAuthFailed for token
        std::env::set_var("VAULT_ADDR", "https://vault.test:8200");
        std::env::remove_var("VAULT_TOKEN");
        let result = HashicorpVaultResolver::from_env();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::VaultAuthFailed(ref msg) if msg.contains("VAULT_TOKEN")),
            "expected VaultAuthFailed for VAULT_TOKEN"
        );

        // 3. Both set → success
        std::env::set_var("VAULT_ADDR", "https://vault.test:8200");
        std::env::set_var("VAULT_TOKEN", "s.test-token");
        let resolver = HashicorpVaultResolver::from_env().unwrap();
        assert_eq!(resolver.address, "https://vault.test:8200");
        assert_eq!(resolver.token, "s.test-token");

        // Clean up
        std::env::remove_var("VAULT_ADDR");
        std::env::remove_var("VAULT_TOKEN");
    }

    #[test]
    fn test_resolve_connection_refused() {
        let resolver = HashicorpVaultResolver {
            address: "http://127.0.0.1:1".to_string(), // almost certainly refused
            token: "test".to_string(),
        };
        let result = resolver.resolve("secret", "test", None);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), CredentialError::VaultConnectionFailed(_)),
            "expected VaultConnectionFailed"
        );
    }
}
