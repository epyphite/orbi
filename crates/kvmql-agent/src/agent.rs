use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

use kvmql_driver::traits::Driver;

use crate::protocol::{AgentLoad, AgentMessage, PROTOCOL_VERSION};

pub struct AgentConfig {
    pub agent_id: String,
    pub control_plane_address: String,
    pub driver_type: String,
    pub heartbeat_interval_s: u64,
    pub state_push_interval_s: u64,
    pub reconnect_max_s: u64,
    pub image_store_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AgentState {
    Disconnected,
    Connecting,
    Registered,
    Running,
    ShuttingDown,
}

pub struct Agent {
    config: AgentConfig,
    driver: Arc<dyn Driver>,
    state: RwLock<AgentState>,
}

impl Agent {
    pub fn new(config: AgentConfig, driver: Arc<dyn Driver>) -> Self {
        Self {
            config,
            driver,
            state: RwLock::new(AgentState::Disconnected),
        }
    }

    pub async fn state(&self) -> AgentState {
        *self.state.read().await
    }

    /// Run the agent lifecycle (connect, register, heartbeat loop, state push loop).
    /// This is a placeholder that simulates the lifecycle without actual TCP connections.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 1. Set state to Connecting
        {
            let mut s = self.state.write().await;
            *s = AgentState::Connecting;
        }
        info!(agent_id = %self.config.agent_id, "connecting to control plane");

        // 2. Build Register message
        let _register_msg = AgentMessage::Register {
            agent_id: self.config.agent_id.clone(),
            driver_type: self.config.driver_type.clone(),
            protocol_version: PROTOCOL_VERSION,
            image_store_path: self.config.image_store_path.clone(),
            image_store_free_gb: None,
        };

        // 3. (Would send to control plane -- placeholder for now)
        info!("registration message built (not sent -- no TCP yet)");

        // 4. Set state to Registered
        {
            let mut s = self.state.write().await;
            *s = AgentState::Registered;
        }
        info!("agent registered");

        // 5. Set state to Running
        {
            let mut s = self.state.write().await;
            *s = AgentState::Running;
        }
        info!("agent running");

        // 6. In a real agent, we would start heartbeat and state push loops
        //    using tokio::select!. For now, just return to indicate success.

        Ok(())
    }

    /// Build a heartbeat message using current driver state.
    pub async fn build_heartbeat(&self) -> AgentMessage {
        let vms = self.driver.list().await.unwrap_or_default();
        let volumes = self.driver.list_volumes().await.unwrap_or_default();
        AgentMessage::Heartbeat {
            agent_id: self.config.agent_id.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            load: AgentLoad {
                cpu_pct: 0.0, // Would come from system metrics
                mem_used_mb: 0,
                vm_count: vms.len() as u32,
                volume_count: volumes.len() as u32,
                image_store_used_gb: None,
            },
        }
    }

    /// Build a state push message with current VM/volume/image state.
    pub async fn build_state_push(&self) -> AgentMessage {
        let vms = self.driver.list().await.unwrap_or_default();
        let volumes = self.driver.list_volumes().await.unwrap_or_default();
        let images = self.driver.list_images().await.unwrap_or_default();
        AgentMessage::StatePush {
            agent_id: self.config.agent_id.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            microvms: vms
                .into_iter()
                .map(|v| serde_json::to_value(v).unwrap_or_default())
                .collect(),
            volumes: volumes
                .into_iter()
                .map(|v| serde_json::to_value(v).unwrap_or_default())
                .collect(),
            images: images
                .into_iter()
                .map(|v| serde_json::to_value(v).unwrap_or_default())
                .collect(),
        }
    }

    /// Handle an execute request from the control plane.
    pub async fn handle_execute(
        &self,
        request_id: &str,
        _verb: &str,
        _params: serde_json::Value,
    ) -> AgentMessage {
        // Dispatch to driver based on verb
        // For now, return a placeholder response
        AgentMessage::ExecuteResponse {
            request_id: request_id.to_string(),
            success: true,
            result: Some(serde_json::json!({"status": "executed"})),
            error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvmql_driver::mock::MockDriver;
    use kvmql_driver::types::{CreateParams, VolumeParams};

    fn test_config() -> AgentConfig {
        AgentConfig {
            agent_id: "test-agent".into(),
            control_plane_address: "127.0.0.1:9090".into(),
            driver_type: "mock".into(),
            heartbeat_interval_s: 5,
            state_push_interval_s: 10,
            reconnect_max_s: 60,
            image_store_path: None,
        }
    }

    fn test_agent() -> Agent {
        let driver = Arc::new(MockDriver::new());
        Agent::new(test_config(), driver)
    }

    fn test_agent_with_driver() -> (Agent, Arc<MockDriver>) {
        let driver = Arc::new(MockDriver::new());
        let agent = Agent::new(test_config(), driver.clone());
        (agent, driver)
    }

    #[tokio::test]
    async fn test_agent_state_transitions() {
        let agent = test_agent();

        // Starts Disconnected
        assert_eq!(agent.state().await, AgentState::Disconnected);

        // Run transitions through Connecting -> Registered -> Running
        agent.run().await.unwrap();
        assert_eq!(agent.state().await, AgentState::Running);
    }

    #[tokio::test]
    async fn test_build_heartbeat() {
        let agent = test_agent();

        let hb = agent.build_heartbeat().await;
        match hb {
            AgentMessage::Heartbeat {
                agent_id, load, ..
            } => {
                assert_eq!(agent_id, "test-agent");
                assert_eq!(load.vm_count, 0);
                assert_eq!(load.volume_count, 0);
                assert!((load.cpu_pct - 0.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Heartbeat, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_build_heartbeat_with_vms() {
        let (agent, driver) = test_agent_with_driver();

        // Create some VMs via the driver
        driver
            .create(CreateParams {
                id: Some("vm-1".into()),
                tenant: "acme".into(),
                vcpus: 2,
                memory_mb: 512,
                image_id: "img-test".into(),
                hostname: None,
                network: None,
                metadata: None,
                labels: None,
                ssh_key: None,
                ssh_key_ref: None,
                admin_user: None,
                cloud_init: None,
                cloud_init_ref: None,
                password: None,
            })
            .await
            .unwrap();

        driver
            .create(CreateParams {
                id: Some("vm-2".into()),
                tenant: "acme".into(),
                vcpus: 1,
                memory_mb: 256,
                image_id: "img-test".into(),
                hostname: None,
                network: None,
                metadata: None,
                labels: None,
                ssh_key: None,
                ssh_key_ref: None,
                admin_user: None,
                cloud_init: None,
                cloud_init_ref: None,
                password: None,
            })
            .await
            .unwrap();

        let hb = agent.build_heartbeat().await;
        match hb {
            AgentMessage::Heartbeat { load, .. } => {
                assert_eq!(load.vm_count, 2);
            }
            other => panic!("expected Heartbeat, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_build_state_push() {
        let (agent, driver) = test_agent_with_driver();

        // Create a VM
        driver
            .create(CreateParams {
                id: Some("vm-sp".into()),
                tenant: "acme".into(),
                vcpus: 2,
                memory_mb: 512,
                image_id: "img-test".into(),
                hostname: None,
                network: None,
                metadata: None,
                labels: None,
                ssh_key: None,
                ssh_key_ref: None,
                admin_user: None,
                cloud_init: None,
                cloud_init_ref: None,
                password: None,
            })
            .await
            .unwrap();

        // Create a volume
        driver
            .create_volume(VolumeParams {
                id: Some("vol-sp".into()),
                size_gb: 10,
                vol_type: "virtio-blk".into(),
                encrypted: false,
                iops: None,
                labels: None,
            })
            .await
            .unwrap();

        let sp = agent.build_state_push().await;
        match sp {
            AgentMessage::StatePush {
                agent_id,
                microvms,
                volumes,
                images,
                ..
            } => {
                assert_eq!(agent_id, "test-agent");
                assert_eq!(microvms.len(), 1);
                assert_eq!(volumes.len(), 1);
                assert_eq!(images.len(), 0);
            }
            other => panic!("expected StatePush, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_execute() {
        let agent = test_agent();

        let resp = agent
            .handle_execute("req-1", "create", serde_json::json!({"vcpus": 2}))
            .await;
        match resp {
            AgentMessage::ExecuteResponse {
                request_id,
                success,
                result,
                error,
            } => {
                assert_eq!(request_id, "req-1");
                assert!(success);
                assert!(result.is_some());
                assert!(error.is_none());
            }
            other => panic!("expected ExecuteResponse, got {:?}", other),
        }
    }
}
