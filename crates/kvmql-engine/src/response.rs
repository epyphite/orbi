use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultEnvelope {
    pub request_id: String,
    pub status: ResultStatus,
    pub notifications: Vec<Notification>,
    pub result: Option<serde_json::Value>,
    pub rows_affected: Option<i64>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub level: String,
    pub code: String,
    pub provider_id: Option<String>,
    pub message: String,
}
