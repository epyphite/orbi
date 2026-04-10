//! Network verification primitives.
//!
//! Table-valued functions callable from `SELECT ... FROM <fn>(...)` clauses.
//! Used together with `ASSERT` to verify infrastructure in the same DSL that
//! declared it.

use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub struct NetworkFunctions;

impl NetworkFunctions {
    /// `dns_lookup(name)` — system-resolver lookup. Returns A and AAAA rows.
    /// `dns_lookup(name, type)` — same but filtered by record type ("A" or "AAAA").
    ///
    /// Note: tokio's `lookup_host` only exposes A/AAAA. MX/TXT/NS/CNAME require
    /// `hickory-resolver` or shelling out to `dig`. Out of MVP scope.
    pub async fn dns_lookup(name: &str, record_type: Option<&str>) -> Result<Vec<Value>, String> {
        let target = format!("{}:0", name);
        let addrs = tokio::net::lookup_host(&target)
            .await
            .map_err(|e| format!("dns lookup failed for '{}': {}", name, e))?;

        let mut rows = Vec::new();
        for addr in addrs {
            let (ty, content) = match addr {
                std::net::SocketAddr::V4(v4) => ("A", v4.ip().to_string()),
                std::net::SocketAddr::V6(v6) => ("AAAA", v6.ip().to_string()),
            };
            if let Some(rt) = record_type {
                if !rt.eq_ignore_ascii_case(ty) {
                    continue;
                }
            }
            rows.push(json!({
                "name": name,
                "type": ty,
                "content": content,
                "ttl": 0,
            }));
        }
        Ok(rows)
    }

    /// `reverse_dns(ip)` — MVP stub. Reverse DNS without `hickory-resolver` or
    /// platform-specific `getnameinfo` is non-trivial. Returns a stub row that
    /// surfaces the limitation rather than silently returning empty.
    pub async fn reverse_dns(ip: &str) -> Result<Vec<Value>, String> {
        // Validate the IP first so callers get a real error on bad input
        let _: std::net::IpAddr = ip
            .parse()
            .map_err(|e| format!("invalid IP '{}': {}", ip, e))?;
        Ok(vec![json!({
            "ip": ip,
            "hostname": Value::Null,
            "note": "reverse_dns is MVP stub — requires hickory-resolver or system getnameinfo",
        })])
    }

    /// `tcp_probe(host, port)` — TCP connect with default 5s timeout.
    /// `tcp_probe(host, port, timeout_ms)` — custom timeout.
    pub async fn tcp_probe(
        host: &str,
        port: u16,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<Value>, String> {
        let timeout_dur = Duration::from_millis(timeout_ms.unwrap_or(5000));
        let target = format!("{}:{}", host, port);
        let start = Instant::now();

        let result = timeout(timeout_dur, TcpStream::connect(&target)).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let (status, latency) = match result {
            Ok(Ok(_stream)) => ("open", Some(elapsed_ms)),
            Ok(Err(_)) => ("closed", Some(elapsed_ms)),
            Err(_) => ("timeout", None),
        };

        Ok(vec![json!({
            "host": host,
            "port": port,
            "status": status,
            "latency_ms": latency,
        })])
    }

    /// `http_probe(url)` — HTTP GET with 10s timeout. Returns status code and timing.
    pub async fn http_probe(url: &str) -> Result<Vec<Value>, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("http client init failed: {}", e))?;

        let start = Instant::now();
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("http request failed: {}", e))?;
        let elapsed_ms = start.elapsed().as_millis() as u64;

        let status = resp.status().as_u16();
        let headers: serde_json::Map<String, Value> = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|s| (k.to_string(), Value::String(s.to_string())))
            })
            .collect();

        Ok(vec![json!({
            "url": url,
            "status_code": status,
            "response_time_ms": elapsed_ms,
            "headers": Value::Object(headers),
        })])
    }

    /// `tls_cert(host, port)` — peer certificate inspection via `openssl s_client`.
    /// Falls back to a clear error if openssl isn't available.
    pub async fn tls_cert(host: &str, port: u16) -> Result<Vec<Value>, String> {
        let host = host.to_string();
        let target = format!("{}:{}", host, port);

        // Run blocking openssl in a worker thread
        tokio::task::spawn_blocking(move || -> Result<Vec<Value>, String> {
            use std::io::Write;
            use std::process::{Command, Stdio};

            let s_client = Command::new("openssl")
                .args([
                    "s_client",
                    "-connect",
                    &target,
                    "-servername",
                    &host,
                    "-showcerts",
                ])
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
                .map_err(|e| format!("openssl not available: {}", e))?;

            if !s_client.status.success() {
                return Err(format!("openssl s_client failed for {}", target));
            }

            let stdout = String::from_utf8_lossy(&s_client.stdout);
            let cert_start = stdout
                .find("-----BEGIN CERTIFICATE-----")
                .ok_or("no certificate in openssl output")?;
            let cert_end = stdout[cert_start..]
                .find("-----END CERTIFICATE-----")
                .ok_or("incomplete certificate")?
                + cert_start
                + "-----END CERTIFICATE-----".len();
            let cert_pem = &stdout[cert_start..cert_end];

            let mut child = Command::new("openssl")
                .args([
                    "x509",
                    "-noout",
                    "-subject",
                    "-issuer",
                    "-startdate",
                    "-enddate",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|e| format!("openssl x509 failed: {}", e))?;

            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(cert_pem.as_bytes())
                    .map_err(|e| format!("write to openssl: {}", e))?;
            }

            let cert_output = child
                .wait_with_output()
                .map_err(|e| format!("openssl x509 wait: {}", e))?;
            let cert_text = String::from_utf8_lossy(&cert_output.stdout);

            let mut subject = String::new();
            let mut issuer = String::new();
            let mut not_before = String::new();
            let mut not_after = String::new();

            for line in cert_text.lines() {
                if let Some(s) = line.strip_prefix("subject=") {
                    subject = s.trim().to_string();
                } else if let Some(s) = line.strip_prefix("issuer=") {
                    issuer = s.trim().to_string();
                } else if let Some(s) = line.strip_prefix("notBefore=") {
                    not_before = s.trim().to_string();
                } else if let Some(s) = line.strip_prefix("notAfter=") {
                    not_after = s.trim().to_string();
                }
            }

            Ok(vec![json!({
                "host": host,
                "port": port,
                "subject": subject,
                "issuer": issuer,
                "not_before": not_before,
                "not_after": not_after,
                "sans": [],
                "fingerprint_sha256": Value::Null,
            })])
        })
        .await
        .map_err(|e| format!("openssl task panicked: {}", e))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dns_lookup_localhost() {
        let rows = NetworkFunctions::dns_lookup("localhost", None).await.unwrap();
        assert!(!rows.is_empty(), "localhost should resolve to at least one address");
        // localhost typically resolves to 127.0.0.1 or ::1
        let has_loopback = rows.iter().any(|r| {
            let content = r["content"].as_str().unwrap_or("");
            content == "127.0.0.1" || content == "::1"
        });
        assert!(has_loopback, "localhost should resolve to a loopback address, got: {:?}", rows);
    }

    #[tokio::test]
    async fn test_dns_lookup_filtered_by_type() {
        let rows = NetworkFunctions::dns_lookup("localhost", Some("A")).await.unwrap();
        // Every row should be type A
        for row in &rows {
            assert_eq!(row["type"].as_str().unwrap(), "A");
        }
    }

    #[tokio::test]
    async fn test_tcp_probe_unreachable_port() {
        // Port 1 on localhost is virtually never open
        let rows = NetworkFunctions::tcp_probe("127.0.0.1", 1, Some(500))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let status = rows[0]["status"].as_str().unwrap();
        assert!(
            status == "closed" || status == "timeout",
            "expected closed/timeout, got: {}",
            status
        );
    }

    #[tokio::test]
    async fn test_reverse_dns_invalid_ip() {
        let result = NetworkFunctions::reverse_dns("not-an-ip").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_reverse_dns_valid_ip_returns_stub() {
        let rows = NetworkFunctions::reverse_dns("127.0.0.1").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["ip"].as_str().unwrap(), "127.0.0.1");
    }
}
