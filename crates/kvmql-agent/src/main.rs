mod agent;
mod protocol;

use std::sync::Arc;

use kvmql_driver::mock::MockDriver;

use agent::{Agent, AgentConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Default config (would load from agent.toml)
    let config = AgentConfig {
        agent_id: "kvm.local".to_string(),
        control_plane_address: "127.0.0.1:9090".to_string(),
        driver_type: "firecracker".to_string(),
        heartbeat_interval_s: 5,
        state_push_interval_s: 10,
        reconnect_max_s: 60,
        image_store_path: None,
    };

    let driver = Arc::new(MockDriver::new());
    let agent = Agent::new(config, driver);

    println!("KVMQL Agent starting (id: kvm.local)");
    if let Err(e) = agent.run().await {
        eprintln!("Agent error: {e}");
        std::process::exit(1);
    }
}
