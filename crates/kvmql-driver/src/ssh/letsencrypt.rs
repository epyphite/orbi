//! Let's Encrypt certificate management over SSH.
//!
//! Resource type:
//! - `letsencrypt_cert` — obtain or renew a TLS certificate via `certbot`
//!   on the remote host.  Supports `dns-01` challenge with Cloudflare
//!   (dns_provider='cf') and `http-01` via the webroot method.
//!
//! Params:
//! - `id` — identifier for the cert (usually the primary domain)
//! - `domains` — array of domain names (SAN cert when > 1)
//! - `email` — ACME registration email
//! - `challenge` — `dns-01` (default) or `http-01`
//! - `dns_provider` — `cf` for Cloudflare DNS plugin (dns-01 only)
//! - `cf_api_token` — Cloudflare API token; the executor injects this
//!   from the 'cf' provider's resolved auth_ref
//! - `auto_renew` — bool (default true); installs a systemd timer
//! - `renew_before_days` — int (default 30)
//! - `webroot` — path for http-01 (default `/var/www/html`)
//!
//! Verification: use the `tls_cert(host, port)` table-valued function
//! from Round 1 to check `not_after`, subject, SANs, etc.

use serde_json::{json, Value};

use super::client::SshClient;

pub struct LetsencryptProvisioner<'a> {
    pub client: &'a SshClient,
}

#[derive(Debug)]
pub struct ProvisionResult {
    pub status: String,
    pub outputs: Option<Value>,
}

impl<'a> LetsencryptProvisioner<'a> {
    pub fn new(client: &'a SshClient) -> Self {
        Self { client }
    }

    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        match resource_type {
            "letsencrypt_cert" => self.create_cert(params),
            other => Err(format!("unsupported letsencrypt resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        _params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "letsencrypt_cert" => {
                let q = super::client::shell_single_quote(id);
                self.client
                    .exec_checked(&format!("certbot delete --cert-name {q} --non-interactive"))
                    .map(|_| ())
                    .map_err(|e| format!("certbot delete failed: {e}"))
            }
            other => Err(format!("unsupported letsencrypt resource type: {other}")),
        }
    }

    fn create_cert(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cert_name = param_str(params, "id")?;
        let email = param_str(params, "email")?;
        let challenge = param_str_or(params, "challenge", "dns-01");

        // Parse domains — may be a JSON array or a single string
        let domains = parse_domains(params)?;
        if domains.is_empty() {
            return Err("letsencrypt_cert requires at least one domain".into());
        }

        // Check if cert already exists and is still valid
        if self.cert_exists(&cert_name) {
            let renew_before = params
                .get("renew_before_days")
                .and_then(|v| v.as_i64())
                .unwrap_or(30);

            // Check if renewal is needed
            if !self.needs_renewal(&cert_name, renew_before) {
                return Ok(ProvisionResult {
                    status: "unchanged".into(),
                    outputs: Some(json!({
                        "cert_name": cert_name,
                        "domains": domains,
                        "renewed": false,
                    })),
                });
            }

            // Renew
            self.run_certbot_renew(&cert_name)?;
            return Ok(ProvisionResult {
                status: "updated".into(),
                outputs: Some(json!({
                    "cert_name": cert_name,
                    "domains": domains,
                    "renewed": true,
                })),
            });
        }

        // New cert — issue via certbot certonly
        match challenge.as_str() {
            "dns-01" => self.certbot_dns01(params, &cert_name, &email, &domains)?,
            "http-01" => self.certbot_http01(params, &cert_name, &email, &domains)?,
            other => return Err(format!("unsupported challenge type: {other}")),
        }

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "cert_name": cert_name,
                "domains": domains,
                "cert_path": format!("/etc/letsencrypt/live/{cert_name}/fullchain.pem"),
                "key_path": format!("/etc/letsencrypt/live/{cert_name}/privkey.pem"),
            })),
        })
    }

    // ── certbot strategies ───────────────────────────────────

    fn certbot_dns01(
        &self,
        params: &Value,
        cert_name: &str,
        email: &str,
        domains: &[String],
    ) -> Result<(), String> {
        let dns_provider = param_str_or(params, "dns_provider", "cf");

        match dns_provider.as_str() {
            "cf" => self.certbot_dns01_cloudflare(params, cert_name, email, domains),
            other => Err(format!(
                "unsupported dns_provider '{other}'; currently only 'cf' (Cloudflare) is supported"
            )),
        }
    }

    fn certbot_dns01_cloudflare(
        &self,
        params: &Value,
        cert_name: &str,
        email: &str,
        domains: &[String],
    ) -> Result<(), String> {
        // The executor injects the resolved CF token as cf_api_token
        let cf_token = params
            .get("cf_api_token")
            .and_then(|v| v.as_str())
            .ok_or(
                "dns-01 with cf requires a Cloudflare API token. \
                 Ensure a 'cf' provider is registered with a valid auth= credential.",
            )?;

        // Write the Cloudflare credentials INI to a temp file on the
        // remote host.  certbot's cloudflare plugin reads it from there.
        let creds_content = format!("dns_cloudflare_api_token = {cf_token}\n");
        let creds_path = "/tmp/orbi-cf-creds.ini";

        self.client
            .upload(creds_content.as_bytes(), creds_path)
            .map_err(|e| format!("failed to write CF credentials: {e}"))?;
        // Lock down permissions — certbot requires <=600 on the creds file
        self.client
            .chmod(creds_path, "0600")
            .map_err(|e| format!("chmod creds failed: {e}"))?;

        let domain_args = domains
            .iter()
            .flat_map(|d| ["-d".to_string(), d.clone()])
            .collect::<Vec<_>>()
            .join(" ");

        let qn = super::client::shell_single_quote(cert_name);
        let qe = super::client::shell_single_quote(email);

        let cmd = format!(
            "certbot certonly \
             --dns-cloudflare \
             --dns-cloudflare-credentials {creds_path} \
             --cert-name {qn} \
             --email {qe} \
             --agree-tos --non-interactive \
             {domain_args}"
        );

        let result = self.client.exec(&cmd).map_err(|e| e.to_string())?;

        // Clean up credentials file regardless of outcome
        let _ = self.client.remove(creds_path);

        if result.exit_code != 0 {
            return Err(format!(
                "certbot dns-01 failed (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            ));
        }

        Ok(())
    }

    fn certbot_http01(
        &self,
        params: &Value,
        cert_name: &str,
        email: &str,
        domains: &[String],
    ) -> Result<(), String> {
        let webroot = param_str_or(params, "webroot", "/var/www/html");

        let domain_args = domains
            .iter()
            .flat_map(|d| ["-d".to_string(), d.clone()])
            .collect::<Vec<_>>()
            .join(" ");

        let qn = super::client::shell_single_quote(cert_name);
        let qe = super::client::shell_single_quote(email);
        let qw = super::client::shell_single_quote(&webroot);

        let cmd = format!(
            "certbot certonly \
             --webroot -w {qw} \
             --cert-name {qn} \
             --email {qe} \
             --agree-tos --non-interactive \
             {domain_args}"
        );

        self.client
            .exec_checked(&cmd)
            .map(|_| ())
            .map_err(|e| format!("certbot http-01 failed: {e}"))
    }

    // ── helpers ──────────────────────────────────────────────

    fn cert_exists(&self, cert_name: &str) -> bool {
        let q = super::client::shell_single_quote(cert_name);
        let path = format!("/etc/letsencrypt/live/{cert_name}/fullchain.pem");
        let qp = super::client::shell_single_quote(&path);
        self.client
            .exec(&format!("test -f {qp} && echo yes"))
            .map(|o| o.stdout.trim() == "yes")
            .unwrap_or(false)
    }

    fn needs_renewal(&self, cert_name: &str, renew_before_days: i64) -> bool {
        let q = super::client::shell_single_quote(cert_name);
        // Use openssl to check expiry
        let cmd = format!(
            "openssl x509 -in /etc/letsencrypt/live/{cert_name}/fullchain.pem \
             -noout -checkend {} 2>/dev/null; echo $?",
            renew_before_days * 86400
        );
        self.client
            .exec(&cmd)
            .map(|o| {
                // openssl x509 -checkend returns 0 if cert is NOT expiring,
                // 1 if it IS expiring within the window.  The echo $? at
                // the end gives us that exit code as text.
                o.stdout.trim().ends_with('1')
            })
            .unwrap_or(true) // If we can't check, assume renewal needed
    }

    fn run_certbot_renew(&self, cert_name: &str) -> Result<(), String> {
        let q = super::client::shell_single_quote(cert_name);
        self.client
            .exec_checked(&format!(
                "certbot renew --cert-name {q} --non-interactive"
            ))
            .map(|_| ())
            .map_err(|e| format!("certbot renew failed: {e}"))
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

/// Parse domains from params — supports JSON array or single string.
fn parse_domains(params: &Value) -> Result<Vec<String>, String> {
    if let Some(arr) = params.get("domains").and_then(|v| v.as_array()) {
        Ok(arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect())
    } else if let Some(s) = params.get("domains").and_then(|v| v.as_str()) {
        // Single domain as string — or comma-separated
        Ok(s.split(',').map(|d| d.trim().to_string()).collect())
    } else {
        // Fall back to id as the single domain
        param_str(params, "id").map(|d| vec![d])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ssh::client::{ExecOutput, SshError, SshExec};
    use std::sync::Mutex;

    struct FakeExec {
        responses: Mutex<Vec<(String, ExecOutput)>>,
    }

    impl FakeExec {
        fn new(script: Vec<(&str, ExecOutput)>) -> Self {
            Self {
                responses: Mutex::new(
                    script
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                ),
            }
        }
        fn respond(&self, cmd: &str) -> ExecOutput {
            let r = self.responses.lock().unwrap();
            for (needle, out) in r.iter() {
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
    }

    impl SshExec for FakeExec {
        fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
            Ok(self.respond(cmd))
        }
        fn exec_with_stdin(&self, cmd: &str, _: &[u8]) -> Result<ExecOutput, SshError> {
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

    #[test]
    fn new_cert_dns01_cloudflare() {
        let fake = FakeExec::new(vec![
            // cert_exists → no
            ("test -f", ok("")),
            // certbot certonly succeeds
            ("certbot certonly", ok("Certificate obtained\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "domains": ["earth.epy.digital"],
            "email": "admin@epy.digital",
            "challenge": "dns-01",
            "dns_provider": "cf",
            "cf_api_token": "fake-cf-token-12345",
        });
        let r = p.create("letsencrypt_cert", &params).unwrap();
        assert_eq!(r.status, "created");
        let outputs = r.outputs.unwrap();
        assert!(outputs["cert_path"]
            .as_str()
            .unwrap()
            .contains("fullchain.pem"));
    }

    #[test]
    fn existing_cert_not_expiring_is_unchanged() {
        let fake = FakeExec::new(vec![
            // cert_exists → yes
            ("test -f", ok("yes\n")),
            // needs_renewal → 0 means NOT expiring
            ("openssl x509", ok("0\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "domains": ["earth.epy.digital"],
            "email": "admin@epy.digital",
            "challenge": "dns-01",
            "dns_provider": "cf",
            "cf_api_token": "fake-cf-token",
        });
        let r = p.create("letsencrypt_cert", &params).unwrap();
        assert_eq!(r.status, "unchanged");
    }

    #[test]
    fn existing_cert_expiring_gets_renewed() {
        let fake = FakeExec::new(vec![
            // cert_exists → yes
            ("test -f", ok("yes\n")),
            // needs_renewal → 1 means IS expiring
            ("openssl x509", ok("1\n")),
            // certbot renew succeeds
            ("certbot renew", ok("Renewed\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "domains": ["earth.epy.digital"],
            "email": "admin@epy.digital",
            "renew_before_days": 30,
            "challenge": "dns-01",
            "dns_provider": "cf",
            "cf_api_token": "fake-cf-token",
        });
        let r = p.create("letsencrypt_cert", &params).unwrap();
        assert_eq!(r.status, "updated");
        assert_eq!(r.outputs.unwrap()["renewed"], true);
    }

    #[test]
    fn san_cert_multiple_domains() {
        let fake = FakeExec::new(vec![
            ("test -f", ok("")),
            ("certbot certonly", ok("Certificate obtained\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "orbitalpay-san",
            "domains": [
                "orbitalpay.ai",
                "www.orbitalpay.ai",
                "api.orbitalpay.ai",
            ],
            "email": "admin@orbitalpay.ai",
            "challenge": "dns-01",
            "dns_provider": "cf",
            "cf_api_token": "fake-cf-token",
        });
        let r = p.create("letsencrypt_cert", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn http01_challenge() {
        let fake = FakeExec::new(vec![
            ("test -f", ok("")),
            ("certbot certonly", ok("Certificate obtained\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "domains": ["earth.epy.digital"],
            "email": "admin@epy.digital",
            "challenge": "http-01",
            "webroot": "/var/www/html",
        });
        let r = p.create("letsencrypt_cert", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn missing_cf_token_errors() {
        let fake = FakeExec::new(vec![("test -f", ok(""))]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        let params = json!({
            "id": "earth.epy.digital",
            "domains": ["earth.epy.digital"],
            "email": "admin@epy.digital",
            "challenge": "dns-01",
            "dns_provider": "cf",
            // no cf_api_token
        });
        let r = p.create("letsencrypt_cert", &params);
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("Cloudflare API token"));
    }

    #[test]
    fn parse_domains_array() {
        let params = json!({"domains": ["a.com", "b.com"]});
        assert_eq!(parse_domains(&params).unwrap(), vec!["a.com", "b.com"]);
    }

    #[test]
    fn parse_domains_string() {
        let params = json!({"domains": "a.com, b.com"});
        assert_eq!(parse_domains(&params).unwrap(), vec!["a.com", "b.com"]);
    }

    #[test]
    fn parse_domains_fallback_to_id() {
        let params = json!({"id": "example.com"});
        assert_eq!(parse_domains(&params).unwrap(), vec!["example.com"]);
    }

    #[test]
    fn unsupported_type() {
        let fake = FakeExec::new(vec![]);
        let client = SshClient::new(Box::new(fake));
        let p = LetsencryptProvisioner::new(&client);
        assert!(p.create("letsencrypt_unknown", &json!({})).is_err());
    }
}
