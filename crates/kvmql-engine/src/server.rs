use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::context::EngineContext;
use crate::executor::Executor;
use crate::response::ResultEnvelope;

/// Length-prefixed JSON protocol server for KVMQL.
///
/// # Protocol
///
/// Each message is framed as a 4-byte big-endian length header followed by
/// a JSON payload of that length.
///
/// Request:  `{"statement": "SELECT * FROM microvms;"}`
/// Response: the `ResultEnvelope` JSON.
///
/// # Concurrency
///
/// `EngineContext` contains a `Registry` backed by `rusqlite::Connection`
/// which is `Send` but not `Sync`.  To share across connections we wrap the
/// context in `Arc<Mutex<EngineContext>>`.  Statement execution is
/// dispatched to a blocking thread via `spawn_blocking` so the tokio Mutex
/// guard (which is not `Send`) never crosses an `.await` point on the async
/// task.  This serializes execution but keeps the implementation simple.  A
/// future iteration can use per-connection registry handles or a connection
/// pool.
pub struct Server {
    ctx: Arc<Mutex<EngineContext>>,
}

impl Server {
    /// Create a new server wrapping the given engine context.
    pub fn new(ctx: Arc<Mutex<EngineContext>>) -> Self {
        Self { ctx }
    }

    /// Start listening on TCP.
    ///
    /// When `tcp_addr` is `None` the function returns immediately.
    ///
    /// For Unix socket support, see [`run_with_unix`] (unix-only).
    pub async fn run(
        &self,
        tcp_addr: Option<&str>,
        _unix_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tcp_listener = match tcp_addr {
            Some(addr) => TcpListener::bind(addr).await?,
            None => return Ok(()),
        };

        loop {
            let (stream, _addr) = tcp_listener.accept().await?;
            let ctx = Arc::clone(&self.ctx);
            tokio::spawn(handle_tcp_connection(ctx, stream));
        }
    }

    /// Start listening on both TCP and a Unix domain socket.
    ///
    /// Either or both addresses may be provided.  If both are `None` the
    /// function returns immediately.
    #[cfg(unix)]
    pub async fn run_with_unix(
        &self,
        tcp_addr: Option<&str>,
        unix_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tcp_listener = match tcp_addr {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };

        let unix_listener = match unix_path {
            Some(path) => {
                // Remove stale socket file if present.
                let _ = std::fs::remove_file(path);
                Some(tokio::net::UnixListener::bind(path)?)
            }
            None => None,
        };

        if tcp_listener.is_none() && unix_listener.is_none() {
            return Ok(());
        }

        loop {
            tokio::select! {
                result = async {
                    match &tcp_listener {
                        Some(l) => l.accept().await.map(|(s, a)| Some((s, a))),
                        None => std::future::pending().await,
                    }
                } => {
                    if let Ok(Some((stream, _addr))) = result {
                        let ctx = Arc::clone(&self.ctx);
                        tokio::spawn(handle_tcp_connection(ctx, stream));
                    }
                }

                result = async {
                    match &unix_listener {
                        Some(l) => l.accept().await.map(|(s, _)| Some(s)),
                        None => std::future::pending().await,
                    }
                } => {
                    if let Ok(Some(stream)) = result {
                        let ctx = Arc::clone(&self.ctx);
                        tokio::spawn(handle_unix_connection(ctx, stream));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

/// Read a single length-prefixed frame.
///
/// Returns `Ok(None)` on clean EOF (client disconnected).
pub async fn read_frame(
    stream: &mut (impl AsyncReadExt + Unpin),
) -> Result<Option<Vec<u8>>, std::io::Error> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let len = u32::from_be_bytes(len_buf) as usize;

    // Guard against absurdly large frames (16 MiB limit).
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

/// Write a single length-prefixed frame.
pub async fn write_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    data: &[u8],
) -> Result<(), std::io::Error> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Connection handlers (concrete types so they are Send + 'static)
// ---------------------------------------------------------------------------

/// Handle a single TCP client connection.
async fn handle_tcp_connection(
    ctx: Arc<Mutex<EngineContext>>,
    mut stream: tokio::net::TcpStream,
) {
    loop {
        // 1. Read a frame
        let frame = match read_frame(&mut stream).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(_) => break,
        };

        // 2. Parse + execute + respond
        let response_bytes = process_frame(&ctx, &frame).await;

        if write_frame(&mut stream, &response_bytes).await.is_err() {
            break;
        }
    }
}

/// Handle a single Unix socket client connection.
#[cfg(unix)]
async fn handle_unix_connection(
    ctx: Arc<Mutex<EngineContext>>,
    mut stream: tokio::net::UnixStream,
) {
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(_) => break,
        };

        let response_bytes = process_frame(&ctx, &frame).await;

        if write_frame(&mut stream, &response_bytes).await.is_err() {
            break;
        }
    }
}

/// Parse a request frame, execute the statement, and return the serialized
/// response bytes.
async fn process_frame(
    ctx: &Arc<Mutex<EngineContext>>,
    frame: &[u8],
) -> Vec<u8> {
    let stmt_str = match parse_request(frame) {
        Ok(s) => s,
        Err(err_json) => return err_json.into_bytes(),
    };

    // Execute on a blocking thread so the Mutex guard (which borrows a
    // non-Sync EngineContext) never lives across an async .await point
    // inside the spawned task.
    let ctx = Arc::clone(ctx);
    let result = tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async {
            let ctx_guard = ctx.lock().await;
            let executor = Executor::new(&ctx_guard);
            executor.execute(&stmt_str).await
        })
    })
    .await;

    let envelope: ResultEnvelope = match result {
        Ok(env) => env,
        Err(e) => {
            return format!(r#"{{"error":"execution panicked: {e}"}}"#).into_bytes();
        }
    };

    serde_json::to_vec(&envelope).unwrap_or_else(|e| {
        format!(r#"{{"error":"serialization failed: {e}"}}"#).into_bytes()
    })
}

/// Parse a JSON request frame and extract the `statement` field.
/// On error, returns a JSON error string suitable for sending back.
fn parse_request(frame: &[u8]) -> Result<String, String> {
    let v: serde_json::Value = serde_json::from_slice(frame)
        .map_err(|e| format!(r#"{{"error":"invalid JSON: {e}"}}"#))?;
    v.get("statement")
        .and_then(|s| s.as_str())
        .map(|s| s.to_owned())
        .ok_or_else(|| r#"{"error":"missing 'statement' field"}"#.to_string())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::EngineContext;
    use crate::response::ResultStatus;
    use kvmql_driver::mock::MockDriver;
    use kvmql_registry::Registry;

    fn make_ctx() -> Arc<Mutex<EngineContext>> {
        let registry = Registry::open_in_memory().unwrap();
        let ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);
        Arc::new(Mutex::new(ctx))
    }

    // ── Frame encode / decode round-trip ─────────────────────────────

    #[tokio::test]
    async fn test_frame_round_trip() {
        let payload = b"hello world";

        // Write into a buffer
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, payload).await.unwrap();

        // Read back from the buffer
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn test_frame_empty_payload() {
        let payload = b"";
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, payload).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_frame(&mut cursor).await.unwrap().unwrap();
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn test_frame_eof_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    // ── parse_request ────────────────────────────────────────────────

    #[test]
    fn test_parse_request_valid() {
        let req = r#"{"statement": "SHOW VERSION"}"#;
        let s = parse_request(req.as_bytes()).unwrap();
        assert_eq!(s, "SHOW VERSION");
    }

    #[test]
    fn test_parse_request_missing_field() {
        let req = r#"{"foo": "bar"}"#;
        let err = parse_request(req.as_bytes()).unwrap_err();
        assert!(err.contains("missing"));
    }

    #[test]
    fn test_parse_request_invalid_json() {
        let req = b"not json";
        let err = parse_request(req).unwrap_err();
        assert!(err.contains("invalid JSON"));
    }

    // ── TCP server integration test ──────────────────────────────────

    #[tokio::test]
    async fn test_tcp_server_execute_statement() {
        let ctx = make_ctx();
        let server = Server::new(Arc::clone(&ctx));

        // Bind to port 0 to get a random available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Free the port so the server can bind to it

        let addr_str = addr.to_string();

        // Spawn the server
        let server_handle = tokio::spawn(async move {
            let _ = server.run(Some(&addr_str), None).await;
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect as a client
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

        // Send a SHOW VERSION request
        let req = serde_json::json!({"statement": "SHOW VERSION"});
        let req_bytes = serde_json::to_vec(&req).unwrap();
        write_frame(&mut stream, &req_bytes).await.unwrap();

        // Read the response
        let response_bytes = read_frame(&mut stream).await.unwrap().unwrap();
        let envelope: ResultEnvelope =
            serde_json::from_slice(&response_bytes).unwrap();

        assert_eq!(envelope.status, ResultStatus::Ok);
        assert!(envelope.result.is_some());

        // Clean up
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_tcp_server_invalid_json() {
        let ctx = make_ctx();
        let server = Server::new(Arc::clone(&ctx));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let addr_str = addr.to_string();

        let server_handle = tokio::spawn(async move {
            let _ = server.run(Some(&addr_str), None).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

        // Send garbage
        write_frame(&mut stream, b"this is not json").await.unwrap();

        // Should get an error response
        let response_bytes = read_frame(&mut stream).await.unwrap().unwrap();
        let response_str = String::from_utf8_lossy(&response_bytes);
        assert!(response_str.contains("error"));

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_tcp_server_multiple_requests() {
        let ctx = make_ctx();
        let server = Server::new(Arc::clone(&ctx));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let addr_str = addr.to_string();

        let server_handle = tokio::spawn(async move {
            let _ = server.run(Some(&addr_str), None).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();

        // First request: SHOW VERSION
        let req1 = serde_json::json!({"statement": "SHOW VERSION"});
        write_frame(&mut stream, &serde_json::to_vec(&req1).unwrap())
            .await
            .unwrap();
        let resp1 = read_frame(&mut stream).await.unwrap().unwrap();
        let env1: ResultEnvelope =
            serde_json::from_slice(&resp1).unwrap();
        assert_eq!(env1.status, ResultStatus::Ok);

        // Second request: SHOW PROVIDERS
        let req2 = serde_json::json!({"statement": "SHOW PROVIDERS"});
        write_frame(&mut stream, &serde_json::to_vec(&req2).unwrap())
            .await
            .unwrap();
        let resp2 = read_frame(&mut stream).await.unwrap().unwrap();
        let env2: ResultEnvelope =
            serde_json::from_slice(&resp2).unwrap();
        assert_eq!(env2.status, ResultStatus::Ok);

        server_handle.abort();
    }
}
