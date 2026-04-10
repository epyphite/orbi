//! nginx resource management over SSH.
//!
//! Resource types:
//! - `nginx_vhost` — full vhost configuration with explicit locations.
//!   Renders config → writes to `/etc/nginx/sites-available/<id>` →
//!   symlinks into `sites-enabled` → `nginx -t` → `systemctl reload
//!   nginx`.  Rolls back on config-test failure.
//! - `nginx_proxy` — shortcut for a reverse-proxy vhost.  Generates a
//!   server block from `server_name`, `upstream`, `tls`, etc.
//!
//! Query functions:
//! - `nginx_vhosts` — lists enabled vhosts from `sites-enabled/`.
//! - `nginx_config_test` — runs `nginx -t` and returns valid/errors.

use serde_json::{json, Value};

use super::client::SshClient;

pub struct NginxProvisioner<'a> {
    pub client: &'a SshClient,
}

#[derive(Debug)]
pub struct ProvisionResult {
    pub status: String,
    pub outputs: Option<Value>,
}

impl<'a> NginxProvisioner<'a> {
    pub fn new(client: &'a SshClient) -> Self {
        Self { client }
    }

    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        match resource_type {
            "nginx_vhost" => self.create_vhost(params),
            "nginx_proxy" => self.create_proxy(params),
            other => Err(format!("unsupported nginx resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        _params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "nginx_vhost" | "nginx_proxy" => {
                let enabled = format!("/etc/nginx/sites-enabled/{id}");
                let available = format!("/etc/nginx/sites-available/{id}");
                let _ = self.client.remove(&enabled);
                let _ = self.client.remove(&available);
                self.reload_nginx()?;
                Ok(())
            }
            other => Err(format!("unsupported nginx resource type: {other}")),
        }
    }

    // ── nginx_vhost ──────────────────────────────────────────

    fn create_vhost(&self, params: &Value) -> Result<ProvisionResult, String> {
        let id = param_str(params, "id")?;
        let server_name = param_str(params, "server_name")?;
        let listen = param_str_or(params, "listen", "80");

        // Build the config from params.  If `locations` is set (as a
        // pre-resolved string from file:), use that as the location
        // block body; otherwise generate a minimal root.
        let locations = params
            .get("locations")
            .or_else(|| params.get("__content_bytes"))
            .and_then(|v| v.as_str())
            .unwrap_or("    location / {\n        return 200 'ok';\n    }");

        let ssl_cert = params.get("ssl_certificate").and_then(|v| v.as_str());
        let ssl_key = params.get("ssl_certificate_key").and_then(|v| v.as_str());

        let mut config = format!("server {{\n    listen {listen};\n    server_name {server_name};\n\n");
        if let (Some(cert), Some(key)) = (ssl_cert, ssl_key) {
            config.push_str(&format!("    ssl_certificate {cert};\n"));
            config.push_str(&format!("    ssl_certificate_key {key};\n\n"));
        }
        config.push_str(locations);
        config.push_str("\n}\n");

        self.write_and_enable(&id, &config)
    }

    // ── nginx_proxy ──────────────────────────────────────────

    fn create_proxy(&self, params: &Value) -> Result<ProvisionResult, String> {
        let id = param_str(params, "id")?;
        let server_name = param_str(params, "server_name")?;
        let upstream = param_str(params, "upstream")?;
        let tls = params
            .get("tls")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let listen = if tls { "443 ssl http2" } else { "80" };

        let mut config = format!(
            "server {{\n    listen {listen};\n    server_name {server_name};\n\n"
        );

        if tls {
            // tls_cert_from convention: 'letsencrypt:<domain>' → default cert paths
            let cert_from = params.get("tls_cert_from").and_then(|v| v.as_str());
            let (cert, key) = if let Some(ref cf) = cert_from {
                if let Some(domain) = cf.strip_prefix("letsencrypt:") {
                    (
                        format!("/etc/letsencrypt/live/{domain}/fullchain.pem"),
                        format!("/etc/letsencrypt/live/{domain}/privkey.pem"),
                    )
                } else {
                    // Fallback: expect explicit cert/key params
                    (
                        param_str_or(params, "ssl_certificate", "/etc/ssl/certs/ssl-cert-snakeoil.pem"),
                        param_str_or(params, "ssl_certificate_key", "/etc/ssl/private/ssl-cert-snakeoil.key"),
                    )
                }
            } else {
                (
                    param_str_or(params, "ssl_certificate", "/etc/ssl/certs/ssl-cert-snakeoil.pem"),
                    param_str_or(params, "ssl_certificate_key", "/etc/ssl/private/ssl-cert-snakeoil.key"),
                )
            };
            config.push_str(&format!("    ssl_certificate {cert};\n"));
            config.push_str(&format!("    ssl_certificate_key {key};\n\n"));
        }

        config.push_str("    location / {\n");
        config.push_str(&format!("        proxy_pass {upstream};\n"));
        config.push_str("        proxy_set_header Host $host;\n");
        config.push_str("        proxy_set_header X-Real-IP $remote_addr;\n");
        config.push_str("        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n");
        config.push_str("        proxy_set_header X-Forwarded-Proto $scheme;\n");
        config.push_str("    }\n}\n");

        self.write_and_enable(&id, &config)
    }

    // ── shared logic ─────────────────────────────────────────

    fn write_and_enable(&self, id: &str, config: &str) -> Result<ProvisionResult, String> {
        let available = format!("/etc/nginx/sites-available/{id}");
        let enabled = format!("/etc/nginx/sites-enabled/{id}");

        // Write config
        self.client
            .upload(config.as_bytes(), &available)
            .map_err(|e| format!("failed to write nginx config: {e}"))?;

        // Symlink into sites-enabled
        self.client
            .symlink_create(&available, &enabled)
            .map_err(|e| format!("failed to enable site: {e}"))?;

        // Test config
        match self.config_test() {
            Ok(true) => {}
            Ok(false) => {
                // Roll back: remove the symlink and the config
                let _ = self.client.remove(&enabled);
                let _ = self.client.remove(&available);
                return Err(format!(
                    "nginx config test failed for '{id}'; changes rolled back"
                ));
            }
            Err(e) => {
                let _ = self.client.remove(&enabled);
                let _ = self.client.remove(&available);
                return Err(format!(
                    "nginx config test error for '{id}': {e}; changes rolled back"
                ));
            }
        }

        // Reload
        self.reload_nginx()?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "vhost": id,
                "available": available,
                "enabled": enabled,
            })),
        })
    }

    fn config_test(&self) -> Result<bool, String> {
        let out = self
            .client
            .exec("nginx -t 2>&1")
            .map_err(|e| e.to_string())?;
        Ok(out.exit_code == 0)
    }

    fn reload_nginx(&self) -> Result<(), String> {
        self.client
            .exec_checked("systemctl reload nginx")
            .map(|_| ())
            .map_err(|e| format!("nginx reload failed: {e}"))
    }

    // ── query: nginx_vhosts ──────────────────────────────────

    /// List enabled vhosts from `/etc/nginx/sites-enabled/`.
    pub fn list_vhosts(&self) -> Result<Vec<Value>, String> {
        let out = self
            .client
            .exec("ls -1 /etc/nginx/sites-enabled/ 2>/dev/null || true")
            .map_err(|e| format!("ls sites-enabled failed: {e}"))?;
        let mut rows = Vec::new();
        for line in out.stdout.lines() {
            let name = line.trim();
            if name.is_empty() {
                continue;
            }
            let target = self
                .client
                .readlink(&format!("/etc/nginx/sites-enabled/{name}"))
                .ok()
                .flatten();
            rows.push(json!({
                "name": name,
                "enabled": true,
                "config_path": target.unwrap_or_else(|| format!("/etc/nginx/sites-enabled/{name}")),
            }));
        }
        Ok(rows)
    }

    /// Run `nginx -t` and return a row with `valid` (bool) and `errors`.
    pub fn config_test_row(&self) -> Result<Vec<Value>, String> {
        let out = self
            .client
            .exec("nginx -t 2>&1")
            .map_err(|e| e.to_string())?;
        let valid = out.exit_code == 0;
        let output = format!("{}{}", out.stdout, out.stderr);
        Ok(vec![json!({
            "valid": valid,
            "errors": if valid { Value::Null } else { Value::String(output.trim().to_string()) },
        })])
    }
}

fn param_str(params: &Value, key: &str) -> Result<String, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| format!("missing required parameter: {key}"))
}

fn param_str_or(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssh::client::{ExecOutput, SshError, SshExec};
    use std::sync::{Arc, Mutex};

    struct FakeExecInner {
        responses: Vec<(String, ExecOutput)>,
        seen_stdin: Mutex<Vec<(String, Vec<u8>)>>,
    }

    #[derive(Clone)]
    struct FakeExec(Arc<FakeExecInner>);

    impl FakeExec {
        fn new(script: Vec<(&str, ExecOutput)>) -> Self {
            Self(Arc::new(FakeExecInner {
                responses: script
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
                seen_stdin: Mutex::new(vec![]),
            }))
        }
        fn respond(&self, cmd: &str) -> ExecOutput {
            for (needle, out) in &self.0.responses {
                if cmd.contains(needle.as_str()) {
                    return out.clone();
                }
            }
            ExecOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
            }
        }
        fn seen_stdin(&self) -> std::sync::MutexGuard<'_, Vec<(String, Vec<u8>)>> {
            self.0.seen_stdin.lock().unwrap()
        }
    }

    impl SshExec for FakeExec {
        fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
            Ok(self.respond(cmd))
        }
        fn exec_with_stdin(&self, cmd: &str, stdin: &[u8]) -> Result<ExecOutput, SshError> {
            self.0
                .seen_stdin
                .lock()
                .unwrap()
                .push((cmd.to_string(), stdin.to_vec()));
            Ok(self.respond(cmd))
        }
    }

    fn ok(s: &str) -> ExecOutput {
        ExecOutput {
            stdout: s.to_string(),
            stderr: String::new(),
            exit_code: 0,
        }
    }

    fn fail(stderr: &str) -> ExecOutput {
        ExecOutput {
            stdout: String::new(),
            stderr: stderr.to_string(),
            exit_code: 1,
        }
    }

    #[test]
    fn proxy_creates_config_and_reloads() {
        let fake = FakeExec::new(vec![
            ("nginx -t", ok("nginx: configuration file /etc/nginx/nginx.conf test is successful\n")),
            ("systemctl reload", ok("")),
        ]);
        let fake2 = fake.clone();
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "server_name": "earth.epy.digital",
            "upstream": "http://127.0.0.1:3001",
            "tls": false,
        });
        let r = p.create("nginx_proxy", &params).unwrap();
        assert_eq!(r.status, "created");

        // Verify the rendered config was uploaded
        let seen = fake2.seen_stdin();
        assert!(!seen.is_empty(), "should have uploaded config via stdin");
        let config = String::from_utf8_lossy(&seen[0].1);
        assert!(config.contains("proxy_pass http://127.0.0.1:3001"));
        assert!(config.contains("server_name earth.epy.digital"));
    }

    #[test]
    fn proxy_rolls_back_on_config_test_failure() {
        let fake = FakeExec::new(vec![
            ("nginx -t", fail("nginx: emerg] unknown directive\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let params = json!({
            "id": "bad.conf",
            "server_name": "bad.conf",
            "upstream": "http://127.0.0.1:3001",
        });
        let r = p.create("nginx_proxy", &params);
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("rolled back"));
    }

    #[test]
    fn proxy_with_tls_and_letsencrypt() {
        let fake = FakeExec::new(vec![
            ("nginx -t", ok("test is successful\n")),
            ("systemctl reload", ok("")),
        ]);
        let fake2 = fake.clone();
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "server_name": "earth.epy.digital",
            "upstream": "http://127.0.0.1:3001",
            "tls": true,
            "tls_cert_from": "letsencrypt:earth.epy.digital",
        });
        let r = p.create("nginx_proxy", &params).unwrap();
        assert_eq!(r.status, "created");

        let seen = fake2.seen_stdin();
        let config = String::from_utf8_lossy(&seen[0].1);
        assert!(config.contains("listen 443 ssl http2"));
        assert!(config.contains("/etc/letsencrypt/live/earth.epy.digital/fullchain.pem"));
    }

    #[test]
    fn list_vhosts_parses_ls() {
        let fake = FakeExec::new(vec![
            ("ls -1", ok("default\nearth.epy.digital\n")),
            ("readlink", ok("/etc/nginx/sites-available/default\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let rows = p.list_vhosts().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "default");
        assert_eq!(rows[1]["name"], "earth.epy.digital");
    }

    #[test]
    fn config_test_row_valid() {
        let fake = FakeExec::new(vec![
            ("nginx -t", ok("test is successful\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let rows = p.config_test_row().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["valid"], true);
        assert!(rows[0]["errors"].is_null());
    }

    #[test]
    fn config_test_row_invalid() {
        let fake = FakeExec::new(vec![
            ("nginx -t", fail("nginx: [emerg] broken\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        let rows = p.config_test_row().unwrap();
        assert_eq!(rows[0]["valid"], false);
        assert!(rows[0]["errors"].as_str().unwrap().contains("broken"));
    }

    #[test]
    fn unsupported_type() {
        let fake = FakeExec::new(vec![]);
        let client = SshClient::new(Box::new(fake));
        let p = NginxProvisioner::new(&client);
        assert!(p.create("nginx_unknown", &json!({})).is_err());
    }
}
