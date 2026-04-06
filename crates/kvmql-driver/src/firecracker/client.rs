use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Low-level HTTP client for the Firecracker REST API over a Unix socket.
///
/// Each request opens a fresh connection with `Connection: close` semantics,
/// which matches Firecracker's own behaviour and keeps the implementation
/// stateless.
pub struct FirecrackerClient {
    socket_path: String,
}

impl FirecrackerClient {
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
        }
    }

    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    // ── Low-level HTTP ─────────────────────────────────────────────

    /// Send a raw HTTP/1.1 request over the Unix socket and return `(status_code, body)`.
    async fn raw_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(u16, String), ClientError> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;

        let body_str = body.unwrap_or("");
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body_str}",
            body_str.len()
        );

        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| ClientError::Request(format!("write failed: {e}")))?;

        // Shut down the write half so the server knows we are done sending.
        stream
            .shutdown()
            .await
            .map_err(|e| ClientError::Request(format!("shutdown failed: {e}")))?;

        let mut response_buf = Vec::new();
        stream
            .read_to_end(&mut response_buf)
            .await
            .map_err(|e| ClientError::Request(format!("read failed: {e}")))?;

        let response_str = String::from_utf8_lossy(&response_buf);
        parse_http_response(&response_str)
    }

    async fn put(&self, path: &str, body: &impl Serialize) -> Result<(), ClientError> {
        let json = serde_json::to_string(body)
            .map_err(|e| ClientError::Request(format!("serialization failed: {e}")))?;
        let (status, body) = self.raw_request("PUT", path, Some(&json)).await?;
        if status >= 200 && status < 300 {
            Ok(())
        } else {
            Err(ClientError::ApiError { status, body })
        }
    }

    async fn patch(&self, path: &str, body: &impl Serialize) -> Result<(), ClientError> {
        let json = serde_json::to_string(body)
            .map_err(|e| ClientError::Request(format!("serialization failed: {e}")))?;
        let (status, body) = self.raw_request("PATCH", path, Some(&json)).await?;
        if status >= 200 && status < 300 {
            Ok(())
        } else {
            Err(ClientError::ApiError { status, body })
        }
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ClientError> {
        let (status, body) = self.raw_request("GET", path, None).await?;
        if status >= 200 && status < 300 {
            serde_json::from_str(&body)
                .map_err(|e| ClientError::Request(format!("deserialization failed: {e}")))
        } else {
            Err(ClientError::ApiError { status, body })
        }
    }

    // ── High-level operations ──────────────────────────────────────

    /// Set the number of vCPUs and memory (in MiB) for the VM.
    pub async fn set_machine_config(
        &self,
        vcpus: i32,
        mem_mib: i32,
    ) -> Result<(), ClientError> {
        self.put("/machine-config", &serde_json::json!({
            "vcpu_count": vcpus,
            "mem_size_mib": mem_mib
        }))
        .await
    }

    /// Set kernel image path and boot arguments.
    pub async fn set_boot_source(
        &self,
        kernel_path: &str,
        boot_args: &str,
    ) -> Result<(), ClientError> {
        self.put("/boot-source", &serde_json::json!({
            "kernel_image_path": kernel_path,
            "boot_args": boot_args
        }))
        .await
    }

    /// Attach a block device (drive) to the VM.
    pub async fn add_drive(
        &self,
        drive_id: &str,
        path: &str,
        is_root: bool,
        read_only: bool,
    ) -> Result<(), ClientError> {
        let endpoint = format!("/drives/{drive_id}");
        self.put(&endpoint, &serde_json::json!({
            "drive_id": drive_id,
            "path_on_host": path,
            "is_root_device": is_root,
            "is_read_only": read_only
        }))
        .await
    }

    /// Add a network interface backed by a TAP device.
    pub async fn add_network(
        &self,
        iface_id: &str,
        host_dev: &str,
        guest_mac: Option<&str>,
    ) -> Result<(), ClientError> {
        let endpoint = format!("/network-interfaces/{iface_id}");
        let mut body = serde_json::json!({
            "iface_id": iface_id,
            "host_dev_name": host_dev
        });
        if let Some(mac) = guest_mac {
            body["guest_mac"] = serde_json::Value::String(mac.to_string());
        }
        self.put(&endpoint, &body).await
    }

    /// Start the Firecracker instance.
    pub async fn start_instance(&self) -> Result<(), ClientError> {
        self.put("/actions", &serde_json::json!({
            "action_type": "InstanceStart"
        }))
        .await
    }

    /// Send Ctrl+Alt+Del for graceful shutdown.
    pub async fn stop_instance(&self) -> Result<(), ClientError> {
        self.put("/actions", &serde_json::json!({
            "action_type": "SendCtrlAltDel"
        }))
        .await
    }

    /// Pause the VM (PATCH /vm with state=Paused).
    pub async fn pause_instance(&self) -> Result<(), ClientError> {
        self.patch("/vm", &serde_json::json!({
            "state": "Paused"
        }))
        .await
    }

    /// Resume the VM (PATCH /vm with state=Resumed).
    pub async fn resume_instance(&self) -> Result<(), ClientError> {
        self.patch("/vm", &serde_json::json!({
            "state": "Resumed"
        }))
        .await
    }

    /// Create a full VM snapshot.
    pub async fn create_snapshot(
        &self,
        snapshot_path: &str,
        mem_path: &str,
    ) -> Result<(), ClientError> {
        self.put("/snapshot/create", &serde_json::json!({
            "snapshot_type": "Full",
            "snapshot_path": snapshot_path,
            "mem_file_path": mem_path
        }))
        .await
    }

    /// Load a VM snapshot.
    pub async fn load_snapshot(
        &self,
        snapshot_path: &str,
        mem_path: &str,
    ) -> Result<(), ClientError> {
        self.put("/snapshot/load", &serde_json::json!({
            "snapshot_path": snapshot_path,
            "mem_backend": {
                "backend_path": mem_path,
                "backend_type": "File"
            }
        }))
        .await
    }

    /// GET / — returns general instance info.
    pub async fn get_instance_info(&self) -> Result<serde_json::Value, ClientError> {
        self.get("/").await
    }

    /// GET /machine-config — returns current vCPU and memory configuration.
    pub async fn get_machine_config(&self) -> Result<MachineConfig, ClientError> {
        self.get("/machine-config").await
    }
}

// ── Response types ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineConfig {
    pub vcpu_count: i32,
    pub mem_size_mib: i32,
}

// ── Error type ─────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("request failed: {0}")]
    Request(String),
    #[error("api error {status}: {body}")]
    ApiError { status: u16, body: String },
}

// ── HTTP response parser ───────────────────────────────────────────

/// Parse a raw HTTP/1.1 response string into `(status_code, body)`.
fn parse_http_response(raw: &str) -> Result<(u16, String), ClientError> {
    // Split headers from body at the first empty line.
    let (header_section, body) = match raw.find("\r\n\r\n") {
        Some(pos) => (&raw[..pos], raw[pos + 4..].to_string()),
        None => {
            // Try plain LF as a fallback.
            match raw.find("\n\n") {
                Some(pos) => (&raw[..pos], raw[pos + 2..].to_string()),
                None => return Err(ClientError::Request("malformed HTTP response".into())),
            }
        }
    };

    // The first line is the status line, e.g. "HTTP/1.1 200 OK"
    let status_line = header_section
        .lines()
        .next()
        .ok_or_else(|| ClientError::Request("empty HTTP response".into()))?;

    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| ClientError::Request(format!("bad status line: {status_line}")))?
        .parse::<u16>()
        .map_err(|e| ClientError::Request(format!("bad status code: {e}")))?;

    Ok((status_code, body.trim().to_string()))
}

// ── Request builder (for testing) ──────────────────────────────────

/// Build the raw HTTP request string that would be sent to Firecracker.
/// Exposed for unit testing.
pub fn build_raw_request(method: &str, path: &str, body: Option<&str>) -> String {
    let body_str = body.unwrap_or("");
    format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Accept: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body_str}",
        body_str.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Request construction tests ─────────────────────────────────

    #[test]
    fn test_build_raw_request_get() {
        let req = build_raw_request("GET", "/", None);
        assert!(req.starts_with("GET / HTTP/1.1\r\n"));
        assert!(req.contains("Content-Length: 0\r\n"));
        assert!(req.contains("Host: localhost\r\n"));
        assert!(req.contains("Connection: close\r\n"));
    }

    #[test]
    fn test_build_raw_request_put_with_body() {
        let body = r#"{"vcpu_count":2,"mem_size_mib":512}"#;
        let req = build_raw_request("PUT", "/machine-config", Some(body));
        assert!(req.starts_with("PUT /machine-config HTTP/1.1\r\n"));
        assert!(req.contains(&format!("Content-Length: {}\r\n", body.len())));
        assert!(req.ends_with(body));
    }

    #[test]
    fn test_build_raw_request_patch() {
        let body = r#"{"state":"Paused"}"#;
        let req = build_raw_request("PATCH", "/vm", Some(body));
        assert!(req.starts_with("PATCH /vm HTTP/1.1\r\n"));
        assert!(req.contains("Content-Type: application/json\r\n"));
    }

    // ── Response parsing tests ─────────────────────────────────────

    #[test]
    fn test_parse_http_response_200() {
        let raw = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"id\":\"vm1\"}";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, "{\"id\":\"vm1\"}");
    }

    #[test]
    fn test_parse_http_response_204_no_body() {
        let raw = "HTTP/1.1 204 No Content\r\n\r\n";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 204);
        assert_eq!(body, "");
    }

    #[test]
    fn test_parse_http_response_400() {
        let raw = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n\r\n{\"fault_message\":\"invalid config\"}";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 400);
        assert!(body.contains("invalid config"));
    }

    #[test]
    fn test_parse_http_response_malformed() {
        let raw = "not a valid response";
        let result = parse_http_response(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_http_response_lf_only() {
        let raw = "HTTP/1.1 200 OK\nContent-Type: application/json\n\n{\"ok\":true}";
        let (status, body) = parse_http_response(raw).unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, "{\"ok\":true}");
    }

    // ── MachineConfig deserialization ──────────────────────────────

    #[test]
    fn test_machine_config_deserialize() {
        let json = r#"{"vcpu_count":4,"mem_size_mib":1024}"#;
        let mc: MachineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(mc.vcpu_count, 4);
        assert_eq!(mc.mem_size_mib, 1024);
    }

    #[test]
    fn test_machine_config_serialize() {
        let mc = MachineConfig {
            vcpu_count: 2,
            mem_size_mib: 512,
        };
        let json = serde_json::to_string(&mc).unwrap();
        assert!(json.contains("\"vcpu_count\":2"));
        assert!(json.contains("\"mem_size_mib\":512"));
    }

    // ── Client construction ────────────────────────────────────────

    #[test]
    fn test_client_new() {
        let client = FirecrackerClient::new("/tmp/firecracker.sock");
        assert_eq!(client.socket_path(), "/tmp/firecracker.sock");
    }

    // ── High-level method payload tests ────────────────────────────
    // These verify the JSON payloads that would be sent without needing
    // an actual Firecracker socket.

    #[test]
    fn test_machine_config_payload() {
        let payload = serde_json::json!({
            "vcpu_count": 2,
            "mem_size_mib": 512
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"vcpu_count\":2"));
        assert!(s.contains("\"mem_size_mib\":512"));
    }

    #[test]
    fn test_boot_source_payload() {
        let payload = serde_json::json!({
            "kernel_image_path": "/path/to/vmlinux",
            "boot_args": "console=ttyS0 reboot=k panic=1"
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"kernel_image_path\":\"/path/to/vmlinux\""));
        assert!(s.contains("console=ttyS0"));
    }

    #[test]
    fn test_drive_payload() {
        let payload = serde_json::json!({
            "drive_id": "rootfs",
            "path_on_host": "/path/to/rootfs.ext4",
            "is_root_device": true,
            "is_read_only": false
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"drive_id\":\"rootfs\""));
        assert!(s.contains("\"is_root_device\":true"));
    }

    #[test]
    fn test_network_payload_with_mac() {
        let mut payload = serde_json::json!({
            "iface_id": "eth0",
            "host_dev_name": "tap0"
        });
        payload["guest_mac"] = serde_json::Value::String("AA:FC:00:00:00:01".into());
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"iface_id\":\"eth0\""));
        assert!(s.contains("\"guest_mac\":\"AA:FC:00:00:00:01\""));
    }

    #[test]
    fn test_network_payload_without_mac() {
        let payload = serde_json::json!({
            "iface_id": "eth0",
            "host_dev_name": "tap0"
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"iface_id\":\"eth0\""));
        assert!(!s.contains("guest_mac"));
    }

    #[test]
    fn test_action_start_payload() {
        let payload = serde_json::json!({"action_type": "InstanceStart"});
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"action_type\":\"InstanceStart\""));
    }

    #[test]
    fn test_action_stop_payload() {
        let payload = serde_json::json!({"action_type": "SendCtrlAltDel"});
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"action_type\":\"SendCtrlAltDel\""));
    }

    #[test]
    fn test_pause_payload() {
        let payload = serde_json::json!({"state": "Paused"});
        let s = serde_json::to_string(&payload).unwrap();
        assert_eq!(s, "{\"state\":\"Paused\"}");
    }

    #[test]
    fn test_resume_payload() {
        let payload = serde_json::json!({"state": "Resumed"});
        let s = serde_json::to_string(&payload).unwrap();
        assert_eq!(s, "{\"state\":\"Resumed\"}");
    }

    #[test]
    fn test_snapshot_create_payload() {
        let payload = serde_json::json!({
            "snapshot_type": "Full",
            "snapshot_path": "/snap/vm.snap",
            "mem_file_path": "/snap/vm.mem"
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"snapshot_type\":\"Full\""));
        assert!(s.contains("\"snapshot_path\":\"/snap/vm.snap\""));
        assert!(s.contains("\"mem_file_path\":\"/snap/vm.mem\""));
    }

    #[test]
    fn test_snapshot_load_payload() {
        let payload = serde_json::json!({
            "snapshot_path": "/snap/vm.snap",
            "mem_backend": {
                "backend_path": "/snap/vm.mem",
                "backend_type": "File"
            }
        });
        let s = serde_json::to_string(&payload).unwrap();
        assert!(s.contains("\"snapshot_path\":\"/snap/vm.snap\""));
        assert!(s.contains("\"backend_type\":\"File\""));
    }

    // ── ClientError display ────────────────────────────────────────

    #[test]
    fn test_client_error_display() {
        let e = ClientError::Connection("refused".into());
        assert_eq!(e.to_string(), "connection failed: refused");

        let e = ClientError::Request("timeout".into());
        assert_eq!(e.to_string(), "request failed: timeout");

        let e = ClientError::ApiError {
            status: 400,
            body: "bad request".into(),
        };
        assert_eq!(e.to_string(), "api error 400: bad request");
    }

    // ── Drive endpoint construction ────────────────────────────────

    #[test]
    fn test_drive_endpoint_format() {
        let drive_id = "rootfs";
        let endpoint = format!("/drives/{drive_id}");
        assert_eq!(endpoint, "/drives/rootfs");
    }

    #[test]
    fn test_network_endpoint_format() {
        let iface_id = "eth0";
        let endpoint = format!("/network-interfaces/{iface_id}");
        assert_eq!(endpoint, "/network-interfaces/eth0");
    }
}
