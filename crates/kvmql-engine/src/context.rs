use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use kvmql_driver::traits::Driver;
use kvmql_registry::Registry;

/// The execution mode controls how the engine handles partial failures.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExecutionMode {
    /// Abort on first error.
    Strict,
    /// Continue past non-fatal errors, collecting notifications.
    Permissive,
}

/// Holds all state needed to execute KVMQL statements: the registry, driver
/// map, and engine configuration.
pub struct EngineContext {
    pub registry: Registry,
    pub drivers: RwLock<HashMap<String, Arc<dyn Driver>>>,
    pub execution_mode: ExecutionMode,
    /// When `true`, every statement is checked against the principal's grants
    /// before execution.  Defaults to `false` for backward compatibility.
    pub auth_enabled: bool,
    /// The id of the principal executing statements.  Must be set when
    /// `auth_enabled` is `true`.
    pub current_principal: Option<String>,
    /// User-defined variables set via `SET @name = value`.
    pub variables: RwLock<HashMap<String, String>>,
    /// When `true`, mutation statements are automatically wrapped in EXPLAIN
    /// (dry-run mode).  Defaults to `false`.
    pub dry_run: bool,
    /// When `true`, all operations return realistic fake responses without
    /// calling any cloud provider.  Defaults to `false`.
    pub simulate: bool,
}

impl EngineContext {
    /// Create a new context backed by the given registry. Starts in permissive
    /// mode with no drivers registered and auth disabled.
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            drivers: RwLock::new(HashMap::new()),
            execution_mode: ExecutionMode::Permissive,
            auth_enabled: false,
            current_principal: None,
            variables: RwLock::new(HashMap::new()),
            dry_run: false,
            simulate: false,
        }
    }

    /// Bootstrap the admin principal and global grant if the principals table
    /// is empty.  This is idempotent — subsequent calls are no-ops.
    pub fn ensure_bootstrap_admin(&self) {
        // Check whether any principals exist yet.
        let existing = self.registry.get_grants_for_principal("admin");
        let has_admin_grant = matches!(&existing, Ok(v) if !v.is_empty());

        // If there's already a grant for "admin" we're done.
        if has_admin_grant {
            return;
        }

        // Insert principal (ignore AlreadyExists).
        let _ = self.registry.insert_principal(
            "admin",
            "user",
            "env:ORBI_ADMIN_TOKEN",
            true,
        );

        // Insert a global grant covering all verbs.
        let all_verbs = serde_json::json!([
            "SELECT", "CREATE", "ALTER", "DESTROY", "PAUSE", "RESUME",
            "SNAPSHOT", "RESTORE", "ATTACH", "DETACH", "RESIZE", "WATCH",
            "IMPORT", "PUBLISH", "ADD", "REMOVE", "GRANT", "REVOKE", "SHOW", "SET"
        ]);
        let _ = self.registry.insert_grant(
            "grant-admin-global",
            "admin",
            &all_verbs.to_string(),
            "global",
            None,
            None,
            Some("bootstrap"),
        );
    }

    /// Register a driver for a given provider id.
    pub fn register_driver(&self, provider_id: String, driver: Arc<dyn Driver>) {
        self.drivers.write().unwrap().insert(provider_id, driver);
    }

    /// Look up the driver for a provider.
    pub fn get_driver(&self, provider_id: &str) -> Option<Arc<dyn Driver>> {
        self.drivers.read().unwrap().get(provider_id).cloned()
    }
}
