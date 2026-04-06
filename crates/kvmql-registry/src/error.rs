#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("not found: {entity} '{id}'")]
    NotFound { entity: String, id: String },

    #[error("already exists: {entity} '{id}'")]
    AlreadyExists { entity: String, id: String },

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
