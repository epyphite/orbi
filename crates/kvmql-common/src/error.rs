use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    ParseError,
    AuthDenied,
    NotFound,
    ImageNotFound,
    VolumeNotFound,
    CapUnsupported,
    NoEligibleProvider,
    ImageNotOnProvider,
    VolumeAttached,
    ChecksumMismatch,
    ProviderOffline,
    AgentTimeout,
    AuditWriteFailed,
    CredentialFailed,
    InternalError,
}

impl ErrorCode {
    pub fn http_status(&self) -> u16 {
        match self {
            ErrorCode::ParseError => 400,
            ErrorCode::AuthDenied => 403,
            ErrorCode::NotFound | ErrorCode::ImageNotFound | ErrorCode::VolumeNotFound => 404,
            ErrorCode::CapUnsupported
            | ErrorCode::NoEligibleProvider
            | ErrorCode::ImageNotOnProvider
            | ErrorCode::VolumeAttached
            | ErrorCode::ChecksumMismatch => 422,
            ErrorCode::ProviderOffline => 503,
            ErrorCode::AgentTimeout => 504,
            ErrorCode::AuditWriteFailed
            | ErrorCode::CredentialFailed
            | ErrorCode::InternalError => 500,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorCode::ParseError => "PARSE_ERROR",
            ErrorCode::AuthDenied => "AUTH_DENIED",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::ImageNotFound => "IMAGE_NOT_FOUND",
            ErrorCode::VolumeNotFound => "VOLUME_NOT_FOUND",
            ErrorCode::CapUnsupported => "CAP_UNSUPPORTED",
            ErrorCode::NoEligibleProvider => "NO_ELIGIBLE_PROVIDER",
            ErrorCode::ImageNotOnProvider => "IMAGE_NOT_ON_PROVIDER",
            ErrorCode::VolumeAttached => "VOLUME_ATTACHED",
            ErrorCode::ChecksumMismatch => "CHECKSUM_MISMATCH",
            ErrorCode::ProviderOffline => "PROVIDER_OFFLINE",
            ErrorCode::AgentTimeout => "AGENT_TIMEOUT",
            ErrorCode::AuditWriteFailed => "AUDIT_WRITE_FAILED",
            ErrorCode::CredentialFailed => "CREDENTIAL_FAILED",
            ErrorCode::InternalError => "INTERNAL_ERROR",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvmqlError {
    pub code: ErrorCode,
    pub message: String,
    pub request_id: Option<String>,
}

impl fmt::Display for KvmqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for KvmqlError {}
