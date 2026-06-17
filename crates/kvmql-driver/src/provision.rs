use serde_json::Value;

/// Error type for resource provisioning operations across all providers.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("missing required parameter: {0}")]
    MissingParam(String),

    #[error("invalid parameter: {0}")]
    InvalidParam(String),

    #[error("unsupported resource type: {0}")]
    UnsupportedType(String),

    #[error("command execution failed: {0}")]
    CommandFailed(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("{0}")]
    Other(String),
}

impl ProvisionError {
    /// Check whether the error message contains the given substring.
    /// Convenience method for assertions and diagnostics.
    pub fn contains(&self, needle: &str) -> bool {
        self.to_string().contains(needle)
    }
}

impl From<String> for ProvisionError {
    fn from(s: String) -> Self {
        ProvisionError::Other(s)
    }
}

impl From<&str> for ProvisionError {
    fn from(s: &str) -> Self {
        ProvisionError::Other(s.to_string())
    }
}

/// Result of a provisioning operation.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "creating", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (endpoints, IDs, connection strings, etc.).
    pub outputs: Option<Value>,
}

/// Extract a required string parameter from a JSON object, returning
/// `ProvisionError::MissingParam` when absent.
pub fn param_str(params: &Value, key: &str) -> Result<String, ProvisionError> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ProvisionError::MissingParam(key.to_string()))
}

/// Extract an optional string parameter, returning `default` when absent.
pub fn param_str_or(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}
