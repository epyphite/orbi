use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotificationLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotificationCode {
    // Capability
    Cap001, // INFO  Parameter ignored (not applicable)
    Cap002, // WARN  Parameter dropped (unsupported, permissive mode)
    Cap003, // WARN  Operation approximate on provider
    Cap004, // ERROR Verb unsupported on provider
    Cap005, // ERROR No eligible provider for REQUIRE constraints
    Cap006, // WARN  Live operation not supported; stop/start cycle
    Cap007, // ERROR Image not available on target provider

    // Routing
    Rte001, // INFO  Request routed to provider
    Rte002, // WARN  Provider unreachable; partial results
    Rte003, // ERROR Owning provider offline

    // State
    Sta001, // INFO  VM state synced from live provider
    Sta002, // WARN  VM state is stale
    Sta003, // WARN  Registry TTL exceeded

    // Image
    Img001, // INFO  Image resolved to cloud_ref
    Img002, // WARN  Image not in catalog
    Img003, // ERROR Image not found in registry
    Img004, // ERROR Checksum mismatch on import
    Img005, // INFO  Image import complete
    Img006, // INFO  Image published to provider

    // Volume
    Vol001, // INFO  Volume created
    Vol002, // WARN  Hotplug not supported
    Vol003, // ERROR Cannot destroy attached volume without FORCE
    Vol004, // INFO  Volume detached

    // Auth
    Auth001, // ERROR Principal not found
    Auth002, // ERROR Verb not permitted
    Auth003, // ERROR Condition check failed

    // Credential
    Cred001, // INFO  Credential resolved
    Cred002, // ERROR Credential reference unresolvable
    Cred003, // ERROR Credential file insecure permissions
    Cred004, // ERROR Vault lease renewal failed

    // Agent
    Agt001, // INFO  Agent connected
    Agt002, // WARN  Agent degraded
    Agt003, // ERROR Agent offline

    // Execution
    Exe001, // ERROR Query timeout exceeded
    Exe002, // ERROR Parse error
    Exe003, // ERROR Audit write failed
}

impl NotificationCode {
    pub fn level(&self) -> NotificationLevel {
        use NotificationCode::*;
        match self {
            Cap001 | Rte001 | Sta001 | Img001 | Img005 | Img006 | Vol001 | Vol004 | Cred001
            | Agt001 => NotificationLevel::Info,

            Cap002 | Cap003 | Cap006 | Rte002 | Sta002 | Sta003 | Img002 | Vol002 | Agt002 => {
                NotificationLevel::Warn
            }

            Cap004 | Cap005 | Cap007 | Rte003 | Img003 | Img004 | Vol003 | Auth001 | Auth002
            | Auth003 | Cred002 | Cred003 | Cred004 | Agt003 | Exe001 | Exe002 | Exe003 => {
                NotificationLevel::Error
            }
        }
    }

    pub fn as_str(&self) -> &'static str {
        use NotificationCode::*;
        match self {
            Cap001 => "CAP_001",
            Cap002 => "CAP_002",
            Cap003 => "CAP_003",
            Cap004 => "CAP_004",
            Cap005 => "CAP_005",
            Cap006 => "CAP_006",
            Cap007 => "CAP_007",
            Rte001 => "RTE_001",
            Rte002 => "RTE_002",
            Rte003 => "RTE_003",
            Sta001 => "STA_001",
            Sta002 => "STA_002",
            Sta003 => "STA_003",
            Img001 => "IMG_001",
            Img002 => "IMG_002",
            Img003 => "IMG_003",
            Img004 => "IMG_004",
            Img005 => "IMG_005",
            Img006 => "IMG_006",
            Vol001 => "VOL_001",
            Vol002 => "VOL_002",
            Vol003 => "VOL_003",
            Vol004 => "VOL_004",
            Auth001 => "AUTH_001",
            Auth002 => "AUTH_002",
            Auth003 => "AUTH_003",
            Cred001 => "CRED_001",
            Cred002 => "CRED_002",
            Cred003 => "CRED_003",
            Cred004 => "CRED_004",
            Agt001 => "AGT_001",
            Agt002 => "AGT_002",
            Agt003 => "AGT_003",
            Exe001 => "EXE_001",
            Exe002 => "EXE_002",
            Exe003 => "EXE_003",
        }
    }
}

impl fmt::Display for NotificationCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub level: NotificationLevel,
    pub code: NotificationCode,
    pub provider_id: Option<String>,
    pub message: String,
}

impl fmt::Display for Notification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.level {
            NotificationLevel::Info => "INFO ",
            NotificationLevel::Warn => "WARN ",
            NotificationLevel::Error => "ERROR",
        };
        write!(f, "{}  {}  {}", level, self.code, self.message)
    }
}
