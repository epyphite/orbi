//! systemd resource management over SSH.
//!
//! Resource types:
//! - `systemd_service` — manage a systemd service unit.  Params: `id`
//!   (unit name), `enabled` (bool), `started` (bool), `after_file`
//!   (optional — if set, triggers `daemon-reload` before any state
//!   transition so unit-file changes take effect immediately).
//! - `systemd_timer` — manage a systemd timer.  Params: `id` (timer
//!   name), `schedule` (OnCalendar value), `unit` (service to trigger),
//!   `enabled` (bool).
//!
//! Query function `systemd_services` returns one row per service unit
//! on the remote host.

use serde_json::{json, Value};

use super::client::SshClient;

/// Provision systemd resources via an SSH connection.
pub struct SystemdProvisioner<'a> {
    pub client: &'a SshClient,
}

#[derive(Debug)]
pub struct ProvisionResult {
    pub status: String,
    pub outputs: Option<Value>,
}

impl<'a> SystemdProvisioner<'a> {
    pub fn new(client: &'a SshClient) -> Self {
        Self { client }
    }

    pub fn create(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<ProvisionResult, String> {
        match resource_type {
            "systemd_service" => self.create_service(params),
            "systemd_timer" => self.create_timer(params),
            other => Err(format!("unsupported systemd resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        _params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "systemd_service" => {
                let _ = self.systemctl(&["stop", id]);
                let _ = self.systemctl(&["disable", id]);
                Ok(())
            }
            "systemd_timer" => {
                let timer = if id.ends_with(".timer") {
                    id.to_string()
                } else {
                    format!("{id}.timer")
                };
                let _ = self.systemctl(&["stop", &timer]);
                let _ = self.systemctl(&["disable", &timer]);
                Ok(())
            }
            other => Err(format!("unsupported systemd resource type: {other}")),
        }
    }

    // ── systemd_service ──────────────────────────────────────

    fn create_service(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;
        let unit = if name.ends_with(".service") {
            name.clone()
        } else {
            format!("{name}.service")
        };
        let want_enabled = params
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let want_started = params
            .get("started")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let after_file = params.get("after_file").and_then(|v| v.as_str());

        let mut changes: Vec<&str> = Vec::new();

        // If after_file is set, daemon-reload first so the unit file
        // changes take effect before enable/start.
        if after_file.is_some() {
            self.systemctl(&["daemon-reload"])
                .map_err(|e| format!("daemon-reload failed: {e}"))?;
            changes.push("daemon-reload");
        }

        // Current state
        let is_enabled = self.is_enabled(&unit);
        let is_active = self.is_active(&unit);

        // Enable / disable
        if want_enabled && !is_enabled {
            self.systemctl(&["enable", &unit])
                .map_err(|e| format!("enable {unit} failed: {e}"))?;
            changes.push("enabled");
        } else if !want_enabled && is_enabled {
            self.systemctl(&["disable", &unit])
                .map_err(|e| format!("disable {unit} failed: {e}"))?;
            changes.push("disabled");
        }

        // Start / stop
        if want_started && !is_active {
            self.systemctl(&["start", &unit])
                .map_err(|e| format!("start {unit} failed: {e}"))?;
            changes.push("started");
        } else if !want_started && is_active {
            self.systemctl(&["stop", &unit])
                .map_err(|e| format!("stop {unit} failed: {e}"))?;
            changes.push("stopped");
        }

        let status = if changes.is_empty() {
            "unchanged"
        } else {
            "created"
        };
        Ok(ProvisionResult {
            status: status.into(),
            outputs: Some(json!({
                "unit": unit,
                "enabled": want_enabled,
                "started": want_started,
                "changes": changes,
            })),
        })
    }

    // ── systemd_timer ────────────────────────────────────────

    fn create_timer(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;
        let timer = if name.ends_with(".timer") {
            name.clone()
        } else {
            format!("{name}.timer")
        };
        let want_enabled = params
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let mut changes: Vec<&str> = Vec::new();

        // daemon-reload in case the timer file was just written
        self.systemctl(&["daemon-reload"])
            .map_err(|e| format!("daemon-reload failed: {e}"))?;

        let is_enabled = self.is_enabled(&timer);

        if want_enabled && !is_enabled {
            self.systemctl(&["enable", "--now", &timer])
                .map_err(|e| format!("enable {timer} failed: {e}"))?;
            changes.push("enabled");
            changes.push("started");
        } else if !want_enabled && is_enabled {
            self.systemctl(&["disable", "--now", &timer])
                .map_err(|e| format!("disable {timer} failed: {e}"))?;
            changes.push("disabled");
            changes.push("stopped");
        }

        let status = if changes.is_empty() {
            "unchanged"
        } else {
            "created"
        };
        Ok(ProvisionResult {
            status: status.into(),
            outputs: Some(json!({
                "timer": timer,
                "enabled": want_enabled,
                "changes": changes,
            })),
        })
    }

    // ── query: systemd_services ──────────────────────────────

    /// List all service units via `systemctl list-units`.  Returns one
    /// JSON row per unit.
    pub fn list_services(&self) -> Result<Vec<Value>, String> {
        // --output=json is available since systemd 248+; for older systems
        // we fall back to plain-text parsing.
        let out = self
            .client
            .exec("systemctl list-units --type=service --all --no-pager --plain --no-legend")
            .map_err(|e| format!("systemctl list-units failed: {e}"))?;
        if out.exit_code != 0 {
            return Err(format!(
                "systemctl list-units failed (exit {}): {}",
                out.exit_code,
                out.stderr.trim()
            ));
        }

        let mut rows = Vec::new();
        for line in out.stdout.lines() {
            // Format: UNIT LOAD ACTIVE SUB DESCRIPTION...
            let parts: Vec<&str> = line.splitn(5, char::is_whitespace).collect();
            if parts.len() < 4 {
                continue;
            }
            let unit_name = parts[0].trim_end_matches(".service");
            let is_enabled = self.is_enabled(parts[0]);
            rows.push(json!({
                "name": unit_name,
                "load_state": parts[1],
                "active_state": parts[2],
                "sub_state": parts[3],
                "enabled": is_enabled,
                "description": if parts.len() > 4 { parts[4].trim() } else { "" },
            }));
        }
        Ok(rows)
    }

    // ── helpers ──────────────────────────────────────────────

    fn systemctl(&self, args: &[&str]) -> Result<String, String> {
        let cmd = format!(
            "systemctl {}",
            args.iter()
                .map(|a| super::client::shell_single_quote(a))
                .collect::<Vec<_>>()
                .join(" ")
        );
        self.client
            .exec_checked(&cmd)
            .map_err(|e| e.to_string())
    }

    fn is_enabled(&self, unit: &str) -> bool {
        let q = super::client::shell_single_quote(unit);
        self.client
            .exec(&format!("systemctl is-enabled {q}"))
            .map(|o| o.stdout.trim() == "enabled")
            .unwrap_or(false)
    }

    fn is_active(&self, unit: &str) -> bool {
        let q = super::client::shell_single_quote(unit);
        self.client
            .exec(&format!("systemctl is-active {q}"))
            .map(|o| o.stdout.trim() == "active")
            .unwrap_or(false)
    }
}

fn param_str(params: &Value, key: &str) -> Result<String, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| format!("missing required parameter: {key}"))
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
    fn service_already_running_is_unchanged() {
        let fake = FakeExec::new(vec![
            ("is-enabled", ok("enabled\n")),
            ("is-active", ok("active\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        let params = json!({"id": "nginx", "enabled": true, "started": true});
        let r = p.create("systemd_service", &params).unwrap();
        assert_eq!(r.status, "unchanged");
    }

    #[test]
    fn service_starts_when_inactive() {
        let fake = FakeExec::new(vec![
            ("is-enabled", ok("enabled\n")),
            ("is-active", ok("inactive\n")),
            ("start", ok("")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        let params = json!({"id": "nginx", "enabled": true, "started": true});
        let r = p.create("systemd_service", &params).unwrap();
        assert_eq!(r.status, "created");
        let changes: Vec<String> = r.outputs.unwrap()["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(changes.contains(&"started".to_string()));
    }

    #[test]
    fn service_with_after_file_triggers_daemon_reload() {
        let fake = FakeExec::new(vec![
            ("daemon-reload", ok("")),
            ("is-enabled", ok("enabled\n")),
            ("is-active", ok("active\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        let params = json!({
            "id": "gee-api",
            "enabled": true,
            "started": true,
            "after_file": "/etc/systemd/system/gee-api.service",
        });
        let r = p.create("systemd_service", &params).unwrap();
        // daemon-reload was called but service was already running
        let changes: Vec<String> = r.outputs.unwrap()["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(changes.contains(&"daemon-reload".to_string()));
    }

    #[test]
    fn timer_enable() {
        let fake = FakeExec::new(vec![
            ("daemon-reload", ok("")),
            ("is-enabled", ok("disabled\n")),
            ("enable", ok("")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        let params = json!({
            "id": "cert-renew",
            "schedule": "daily",
            "unit": "cert-renew.service",
            "enabled": true,
        });
        let r = p.create("systemd_timer", &params).unwrap();
        assert_eq!(r.status, "created");
    }

    #[test]
    fn list_services_parses_plain_output() {
        let output = "\
nginx.service loaded active running A high performance web server
ssh.service loaded active running OpenBSD Secure Shell server
cron.service loaded active running Regular background program processing daemon";
        let fake = FakeExec::new(vec![
            ("list-units", ok(output)),
            ("is-enabled", ok("enabled\n")),
        ]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        let rows = p.list_services().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["name"], "nginx");
        assert_eq!(rows[0]["active_state"], "active");
        assert_eq!(rows[1]["name"], "ssh");
        assert_eq!(rows[2]["name"], "cron");
    }

    #[test]
    fn unsupported_resource_type() {
        let fake = FakeExec::new(vec![]);
        let client = SshClient::new(Box::new(fake));
        let p = SystemdProvisioner::new(&client);
        assert!(p.create("systemd_unknown", &json!({})).is_err());
    }
}
