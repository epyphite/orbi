//! Low-level wrapper around the `kubectl` CLI.
//!
//! Every method shells out to `kubectl` via `std::process::Command` with
//! individual arguments — no shell interpolation, ever.  Optional `context`
//! and `kubeconfig` fields let different providers route to different
//! clusters via `--context` / `--kubeconfig` flags.

use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum KubectlError {
    #[error("kubectl CLI not found in PATH -- install from https://kubernetes.io/docs/tasks/tools/")]
    NotInstalled,
    #[error("kubectl command failed: {0}")]
    CommandFailed(String),
    #[error("parse error: {0}")]
    ParseError(String),
}

#[derive(Debug, Clone)]
pub struct KubectlCli {
    /// Optional context name (`--context` flag).  When set, every command
    /// is pinned to this context regardless of the user's current default.
    pub context: Option<String>,
    /// Optional kubeconfig path (`--kubeconfig` flag).
    pub kubeconfig: Option<String>,
}

impl Default for KubectlCli {
    fn default() -> Self {
        Self::new()
    }
}

impl KubectlCli {
    pub fn new() -> Self {
        Self {
            context: None,
            kubeconfig: None,
        }
    }

    pub fn with_context(context: &str) -> Self {
        Self {
            context: Some(context.to_string()),
            kubeconfig: None,
        }
    }

    /// Check if the `kubectl` CLI is available on PATH.
    pub fn check_available() -> Result<(), KubectlError> {
        let out = Command::new("kubectl")
            .arg("version")
            .arg("--client")
            .arg("-o")
            .arg("json")
            .output()
            .map_err(|_| KubectlError::NotInstalled)?;
        if !out.status.success() {
            return Err(KubectlError::NotInstalled);
        }
        Ok(())
    }

    /// Inject `--context` / `--kubeconfig` into a command if configured.
    fn apply_context(&self, cmd: &mut Command) {
        if let Some(ref ctx) = self.context {
            cmd.arg("--context").arg(ctx);
        }
        if let Some(ref cfg) = self.kubeconfig {
            cmd.arg("--kubeconfig").arg(cfg);
        }
    }

    /// Run `kubectl <args>` with no stdin and return stdout parsed as JSON
    /// when possible, otherwise wrapped as a `Value::String`.
    pub fn run(&self, args: &[&str]) -> Result<Value, KubectlError> {
        let mut cmd = Command::new("kubectl");
        cmd.args(args);
        self.apply_context(&mut cmd);
        let out = cmd.output().map_err(|_| KubectlError::NotInstalled)?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(KubectlError::CommandFailed(stderr.to_string()));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&stdout).or_else(|_| Ok(Value::String(stdout.to_string())))
    }

    /// Run `kubectl <args>` with stdin (used for `apply -f -` and
    /// `delete -f -`).
    pub fn run_with_stdin(
        &self,
        args: &[&str],
        stdin_content: &str,
    ) -> Result<Value, KubectlError> {
        let mut cmd = Command::new("kubectl");
        cmd.args(args);
        self.apply_context(&mut cmd);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|_| KubectlError::NotInstalled)?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(stdin_content.as_bytes())
                .map_err(|e| KubectlError::CommandFailed(format!("stdin write: {e}")))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| KubectlError::CommandFailed(format!("wait: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(KubectlError::CommandFailed(stderr.to_string()));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&stdout).or_else(|_| Ok(Value::String(stdout.to_string())))
    }

    // ── High-level operations ──────────────────────────────

    /// `kubectl apply -f - -o json` with the given YAML on stdin.
    pub fn apply_yaml(&self, yaml: &str) -> Result<Value, KubectlError> {
        self.run_with_stdin(&["apply", "-f", "-", "-o", "json"], yaml)
    }

    /// `kubectl delete <kind> <name> [-n <ns>] --ignore-not-found=true`.
    pub fn delete(
        &self,
        kind: &str,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<Value, KubectlError> {
        let mut args = vec!["delete", kind, name, "--ignore-not-found=true"];
        if let Some(ns) = namespace {
            args.push("-n");
            args.push(ns);
        }
        self.run(&args)
    }

    /// `kubectl get <kind> <name> -o json [-n <ns>]`.
    pub fn get_one(
        &self,
        kind: &str,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<Value, KubectlError> {
        let mut args = vec!["get", kind, name, "-o", "json"];
        if let Some(ns) = namespace {
            args.push("-n");
            args.push(ns);
        }
        self.run(&args)
    }

    /// `kubectl get <kind> -o json [-n <ns> | --all-namespaces]`.
    ///
    /// `namespace` semantics:
    /// - `Some("*")` or `Some("all")` → `--all-namespaces`
    /// - `Some(ns)` → `-n <ns>`
    /// - `None` → `--all-namespaces` (the default for cluster-wide queries)
    pub fn get_list(
        &self,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Value, KubectlError> {
        let mut args = vec!["get", kind, "-o", "json"];
        match namespace {
            Some(ns) if ns == "*" || ns == "all" => args.push("--all-namespaces"),
            Some(ns) => {
                args.push("-n");
                args.push(ns);
            }
            None => args.push("--all-namespaces"),
        }
        self.run(&args)
    }

    /// `kubectl config current-context`.
    pub fn current_context(&self) -> Result<String, KubectlError> {
        let v = self.run(&["config", "current-context"])?;
        Ok(v.as_str().unwrap_or("").trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_new() {
        let cli = KubectlCli::new();
        assert!(cli.context.is_none());
        assert!(cli.kubeconfig.is_none());
    }

    #[test]
    fn test_cli_with_context() {
        let cli = KubectlCli::with_context("prod-aks");
        assert_eq!(cli.context.unwrap(), "prod-aks");
        assert!(cli.kubeconfig.is_none());
    }

    #[test]
    fn test_cli_default_matches_new() {
        let a = KubectlCli::default();
        let b = KubectlCli::new();
        assert_eq!(a.context, b.context);
        assert_eq!(a.kubeconfig, b.kubeconfig);
    }
}
