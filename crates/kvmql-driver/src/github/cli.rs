//! Low-level wrapper around the `gh` CLI.
//!
//! Every method shells out to `gh` via `std::process::Command` with individual
//! arguments — no shell interpolation, ever.  An optional `token` field is
//! injected as the `GH_TOKEN` env var so different providers can route to
//! different GitHub accounts.

use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum GhError {
    #[error("gh CLI not found in PATH -- install from https://cli.github.com/")]
    NotInstalled,
    #[error("gh not authenticated -- run 'gh auth login'")]
    NotAuthenticated,
    #[error("gh command failed: {0}")]
    CommandFailed(String),
    #[error("parse error: {0}")]
    ParseError(String),
}

#[derive(Debug, Clone)]
pub struct GhCli {
    /// Optional token to inject via `GH_TOKEN` for this process tree.
    /// Allows routing different providers to different GitHub accounts.
    pub token: Option<String>,
}

impl Default for GhCli {
    fn default() -> Self {
        Self::new()
    }
}

impl GhCli {
    pub fn new() -> Self {
        Self { token: None }
    }

    pub fn with_token(token: &str) -> Self {
        Self {
            token: Some(token.to_string()),
        }
    }

    /// Check if the `gh` CLI is available on PATH.
    pub fn check_available() -> Result<(), GhError> {
        let out = Command::new("gh")
            .arg("--version")
            .output()
            .map_err(|_| GhError::NotInstalled)?;
        if !out.status.success() {
            return Err(GhError::NotInstalled);
        }
        Ok(())
    }

    /// Run `gh <args>` and return stdout parsed as JSON when possible,
    /// otherwise wrapped as a `Value::String`.
    pub fn run(&self, args: &[&str]) -> Result<Value, GhError> {
        let mut cmd = Command::new("gh");
        cmd.args(args);
        if let Some(ref token) = self.token {
            cmd.env("GH_TOKEN", token);
        }
        let out = cmd.output().map_err(|_| GhError::NotInstalled)?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("not logged in") || stderr.contains("authentication") {
                return Err(GhError::NotAuthenticated);
            }
            return Err(GhError::CommandFailed(stderr.to_string()));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&stdout).or_else(|_| Ok(Value::String(stdout.to_string())))
    }

    /// Run `gh <args>` with stdin piped from the given JSON body.  Used for
    /// `gh api --input -` calls with complex payloads (rulesets, branch
    /// protection, contents API).
    pub fn run_with_stdin(&self, args: &[&str], stdin_json: &Value) -> Result<Value, GhError> {
        let mut cmd = Command::new("gh");
        cmd.args(args);
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        if let Some(ref token) = self.token {
            cmd.env("GH_TOKEN", token);
        }
        let mut child = cmd.spawn().map_err(|_| GhError::NotInstalled)?;
        if let Some(mut stdin) = child.stdin.take() {
            let body = serde_json::to_vec(stdin_json)
                .map_err(|e| GhError::ParseError(e.to_string()))?;
            stdin
                .write_all(&body)
                .map_err(|e| GhError::CommandFailed(format!("stdin write: {e}")))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| GhError::CommandFailed(format!("wait: {e}")))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(GhError::CommandFailed(stderr.to_string()));
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&stdout).or_else(|_| Ok(Value::String(stdout.to_string())))
    }

    // ── High-level operations ──────────────────────────────

    pub fn repo_create(
        &self,
        name: &str,
        visibility: &str,
        description: Option<&str>,
    ) -> Result<Value, GhError> {
        let visibility_flag = match visibility {
            "public" => "--public",
            "internal" => "--internal",
            _ => "--private",
        };
        let mut args = vec!["repo", "create", name, visibility_flag];
        if let Some(desc) = description {
            args.push("--description");
            args.push(desc);
        }
        // Non-interactive mode
        args.push("--confirm");
        self.run(&args)
    }

    pub fn repo_delete(&self, full_name: &str) -> Result<Value, GhError> {
        self.run(&["repo", "delete", full_name, "--yes"])
    }

    pub fn repo_view(&self, full_name: &str) -> Result<Value, GhError> {
        self.run(&[
            "repo",
            "view",
            full_name,
            "--json",
            "id,name,owner,url,visibility,defaultBranchRef,description",
        ])
    }

    pub fn secret_set(&self, name: &str, repo: &str, value: &str) -> Result<Value, GhError> {
        // Pipe the value via --body to avoid leaking it through shell history,
        // but still keep it out of argv when possible.  `gh secret set --body`
        // is the documented form; gh handles libsodium encryption internally.
        let mut cmd = Command::new("gh");
        cmd.args(["secret", "set", name, "--repo", repo, "--body", value]);
        if let Some(ref token) = self.token {
            cmd.env("GH_TOKEN", token);
        }
        let out = cmd.output().map_err(|_| GhError::NotInstalled)?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(GhError::CommandFailed(stderr.to_string()));
        }
        Ok(Value::Null)
    }

    pub fn secret_delete(&self, name: &str, repo: &str) -> Result<Value, GhError> {
        self.run(&["secret", "delete", name, "--repo", repo])
    }

    pub fn variable_set(&self, name: &str, repo: &str, value: &str) -> Result<Value, GhError> {
        let mut cmd = Command::new("gh");
        cmd.args(["variable", "set", name, "--repo", repo, "--body", value]);
        if let Some(ref token) = self.token {
            cmd.env("GH_TOKEN", token);
        }
        let out = cmd.output().map_err(|_| GhError::NotInstalled)?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(GhError::CommandFailed(stderr.to_string()));
        }
        Ok(Value::Null)
    }

    pub fn variable_delete(&self, name: &str, repo: &str) -> Result<Value, GhError> {
        self.run(&["variable", "delete", name, "--repo", repo])
    }

    /// Create or update a file via the GitHub contents API.
    /// Used for workflow files in `.github/workflows/`.
    pub fn file_put(
        &self,
        repo: &str,
        path: &str,
        content: &str,
        message: &str,
        branch: Option<&str>,
    ) -> Result<Value, GhError> {
        let encoded = base64_encode(content.as_bytes());

        // Try to fetch the existing file's SHA so we update instead of creating.
        let sha_check = self.run(&[
            "api",
            &format!("/repos/{repo}/contents/{path}"),
            "--jq",
            ".sha",
        ]);

        let mut body = serde_json::json!({
            "message": message,
            "content": encoded,
        });
        if let Some(b) = branch {
            body["branch"] = Value::String(b.to_string());
        }
        if let Ok(Value::String(sha)) = sha_check {
            let sha = sha.trim().to_string();
            if !sha.is_empty() {
                body["sha"] = Value::String(sha);
            }
        }

        self.run_with_stdin(
            &[
                "api",
                "-X",
                "PUT",
                &format!("/repos/{repo}/contents/{path}"),
                "--input",
                "-",
            ],
            &body,
        )
    }

    pub fn ruleset_create(&self, repo: &str, body: &Value) -> Result<Value, GhError> {
        self.run_with_stdin(
            &[
                "api",
                "-X",
                "POST",
                &format!("/repos/{repo}/rulesets"),
                "-H",
                "Accept: application/vnd.github+json",
                "--input",
                "-",
            ],
            body,
        )
    }

    pub fn ruleset_delete(&self, repo: &str, ruleset_id: &str) -> Result<Value, GhError> {
        self.run(&[
            "api",
            "-X",
            "DELETE",
            &format!("/repos/{repo}/rulesets/{ruleset_id}"),
        ])
    }

    pub fn branch_protection_set(
        &self,
        repo: &str,
        branch: &str,
        body: &Value,
    ) -> Result<Value, GhError> {
        self.run_with_stdin(
            &[
                "api",
                "-X",
                "PUT",
                &format!("/repos/{repo}/branches/{branch}/protection"),
                "-H",
                "Accept: application/vnd.github+json",
                "--input",
                "-",
            ],
            body,
        )
    }

    pub fn branch_protection_delete(&self, repo: &str, branch: &str) -> Result<Value, GhError> {
        self.run(&[
            "api",
            "-X",
            "DELETE",
            &format!("/repos/{repo}/branches/{branch}/protection"),
        ])
    }

    pub fn auth_status(&self) -> Result<Value, GhError> {
        self.run(&["auth", "status"])
    }
}

/// Trivial base64 encoder for workflow file contents.
///
/// We need exactly one base64 encode (workflow file PUT) and don't want to
/// pull in a base64 crate just for that.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= input.len() {
        let b0 = input[i] as usize;
        let b1 = input[i + 1] as usize;
        let b2 = input[i + 2] as usize;
        out.push(TABLE[(b0 >> 2) & 0x3F] as char);
        out.push(TABLE[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
        out.push(TABLE[((b1 << 2) | (b2 >> 6)) & 0x3F] as char);
        out.push(TABLE[b2 & 0x3F] as char);
        i += 3;
    }
    let rem = input.len() - i;
    if rem == 1 {
        let b0 = input[i] as usize;
        out.push(TABLE[(b0 >> 2) & 0x3F] as char);
        out.push(TABLE[(b0 << 4) & 0x3F] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b0 = input[i] as usize;
        let b1 = input[i + 1] as usize;
        out.push(TABLE[(b0 >> 2) & 0x3F] as char);
        out.push(TABLE[((b0 << 4) | (b1 >> 4)) & 0x3F] as char);
        out.push(TABLE[(b1 << 2) & 0x3F] as char);
        out.push('=');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_encode_basic() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b"hello world"), "aGVsbG8gd29ybGQ=");
    }

    #[test]
    fn test_gh_cli_new() {
        let cli = GhCli::new();
        assert!(cli.token.is_none());
    }

    #[test]
    fn test_gh_cli_with_token() {
        let cli = GhCli::with_token("ghp_fake");
        assert_eq!(cli.token.unwrap(), "ghp_fake");
    }
}
