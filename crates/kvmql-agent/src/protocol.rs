use serde::{Deserialize, Serialize};

/// Messages sent from agent -> control plane
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentMessage {
    #[serde(rename = "register")]
    Register {
        agent_id: String,
        driver_type: String,
        protocol_version: u32,
        image_store_path: Option<String>,
        image_store_free_gb: Option<u64>,
    },
    #[serde(rename = "heartbeat")]
    Heartbeat {
        agent_id: String,
        timestamp: String,
        load: AgentLoad,
    },
    #[serde(rename = "state_push")]
    StatePush {
        agent_id: String,
        timestamp: String,
        microvms: Vec<serde_json::Value>,
        volumes: Vec<serde_json::Value>,
        images: Vec<serde_json::Value>,
    },
    #[serde(rename = "execute_response")]
    ExecuteResponse {
        request_id: String,
        success: bool,
        result: Option<serde_json::Value>,
        error: Option<String>,
    },
}

/// Messages sent from control plane -> agent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlMessage {
    #[serde(rename = "ack")]
    Ack,
    #[serde(rename = "reject")]
    Reject { reason: String },
    #[serde(rename = "execute_request")]
    ExecuteRequest {
        request_id: String,
        verb: String,
        params: serde_json::Value,
    },
    #[serde(rename = "shutdown")]
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLoad {
    pub cpu_pct: f64,
    pub mem_used_mb: u64,
    pub vm_count: u32,
    pub volume_count: u32,
    pub image_store_used_gb: Option<u64>,
}

pub const PROTOCOL_VERSION: u32 = 1;

/// Encode a message as length-prefixed JSON (4-byte big-endian + JSON payload).
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, serde_json::Error> {
    let json = serde_json::to_vec(msg)?;
    let len = (json.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&json);
    Ok(buf)
}

/// Decode a length-prefixed JSON message.
pub fn decode_message<T: for<'de> Deserialize<'de>>(
    buf: &[u8],
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    if buf.len() < 4 {
        return Err("buffer too short".into());
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Err("incomplete message".into());
    }
    Ok(serde_json::from_slice(&buf[4..4 + len])?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_register() {
        let msg = AgentMessage::Register {
            agent_id: "node-1".into(),
            driver_type: "firecracker".into(),
            protocol_version: PROTOCOL_VERSION,
            image_store_path: Some("/var/images".into()),
            image_store_free_gb: Some(100),
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: AgentMessage = decode_message(&encoded).unwrap();
        match decoded {
            AgentMessage::Register {
                agent_id,
                driver_type,
                protocol_version,
                image_store_path,
                image_store_free_gb,
            } => {
                assert_eq!(agent_id, "node-1");
                assert_eq!(driver_type, "firecracker");
                assert_eq!(protocol_version, PROTOCOL_VERSION);
                assert_eq!(image_store_path.as_deref(), Some("/var/images"));
                assert_eq!(image_store_free_gb, Some(100));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_heartbeat() {
        let msg = AgentMessage::Heartbeat {
            agent_id: "node-1".into(),
            timestamp: "2026-01-01T00:00:00Z".into(),
            load: AgentLoad {
                cpu_pct: 42.5,
                mem_used_mb: 2048,
                vm_count: 3,
                volume_count: 5,
                image_store_used_gb: Some(20),
            },
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: AgentMessage = decode_message(&encoded).unwrap();
        match decoded {
            AgentMessage::Heartbeat {
                agent_id, load, ..
            } => {
                assert_eq!(agent_id, "node-1");
                assert!((load.cpu_pct - 42.5).abs() < f64::EPSILON);
                assert_eq!(load.mem_used_mb, 2048);
                assert_eq!(load.vm_count, 3);
                assert_eq!(load.volume_count, 5);
                assert_eq!(load.image_store_used_gb, Some(20));
            }
            other => panic!("expected Heartbeat, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_state_push() {
        let msg = AgentMessage::StatePush {
            agent_id: "node-1".into(),
            timestamp: "2026-01-01T00:00:00Z".into(),
            microvms: vec![serde_json::json!({"id": "vm-1"})],
            volumes: vec![],
            images: vec![serde_json::json!({"id": "img-1"})],
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: AgentMessage = decode_message(&encoded).unwrap();
        match decoded {
            AgentMessage::StatePush {
                microvms, images, ..
            } => {
                assert_eq!(microvms.len(), 1);
                assert_eq!(images.len(), 1);
            }
            other => panic!("expected StatePush, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_execute_response() {
        let msg = AgentMessage::ExecuteResponse {
            request_id: "req-42".into(),
            success: true,
            result: Some(serde_json::json!({"status": "done"})),
            error: None,
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: AgentMessage = decode_message(&encoded).unwrap();
        match decoded {
            AgentMessage::ExecuteResponse {
                request_id,
                success,
                error,
                ..
            } => {
                assert_eq!(request_id, "req-42");
                assert!(success);
                assert!(error.is_none());
            }
            other => panic!("expected ExecuteResponse, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_control_ack() {
        let msg = ControlMessage::Ack;
        let encoded = encode_message(&msg).unwrap();
        let decoded: ControlMessage = decode_message(&encoded).unwrap();
        assert!(matches!(decoded, ControlMessage::Ack));
    }

    #[test]
    fn test_encode_decode_control_reject() {
        let msg = ControlMessage::Reject {
            reason: "version mismatch".into(),
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: ControlMessage = decode_message(&encoded).unwrap();
        match decoded {
            ControlMessage::Reject { reason } => {
                assert_eq!(reason, "version mismatch");
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_control_execute_request() {
        let msg = ControlMessage::ExecuteRequest {
            request_id: "req-99".into(),
            verb: "create".into(),
            params: serde_json::json!({"vcpus": 4}),
        };
        let encoded = encode_message(&msg).unwrap();
        let decoded: ControlMessage = decode_message(&encoded).unwrap();
        match decoded {
            ControlMessage::ExecuteRequest {
                request_id,
                verb,
                params,
            } => {
                assert_eq!(request_id, "req-99");
                assert_eq!(verb, "create");
                assert_eq!(params["vcpus"], 4);
            }
            other => panic!("expected ExecuteRequest, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_control_shutdown() {
        let msg = ControlMessage::Shutdown;
        let encoded = encode_message(&msg).unwrap();
        let decoded: ControlMessage = decode_message(&encoded).unwrap();
        assert!(matches!(decoded, ControlMessage::Shutdown));
    }

    #[test]
    fn test_decode_buffer_too_short() {
        let result: Result<AgentMessage, _> = decode_message(&[0, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_incomplete_message() {
        // Length says 100 bytes but only 4 bytes of header
        let buf = [0u8, 0, 0, 100];
        let result: Result<AgentMessage, _> = decode_message(&buf);
        assert!(result.is_err());
    }
}
