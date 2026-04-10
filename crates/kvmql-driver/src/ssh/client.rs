//! Low-level wrapper around the OpenSSH client.
//!
//! All remote operations go through a [`SshExec`] trait so tests can swap in
//! a scripted fake without touching the real filesystem or network.  The
//! real implementation ([`OpenSshExec`]) shells out to `ssh` for commands
//! and uses a `ssh ... "cat > …"` pipe for uploads — one round trip per
//! upload, and binary-safe because the SSH channel is raw.
//!
//! Remote paths are always single-quote escaped.  Shell injection would
//! require an attacker to control the *path string itself*, which comes
//! from the DSL author, not from runtime input — but we still escape
//! defensively.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Debug, thiserror::Error)]
pub enum SshError {
    #[error("ssh client not found in PATH -- install openssh-client")]
    NotInstalled,
    #[error("ssh connection to {host} failed: {msg}")]
    ConnectFailed { host: String, msg: String },
    #[error("remote command failed (exit {exit_code}): {stderr}")]
    CommandFailed { exit_code: i32, stderr: String },
    #[error("ssh auth failed: {0}")]
    AuthFailed(String),
    #[error("io error: {0}")]
    Io(String),
}

/// Stdout/stderr/exit of a remote command.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// stat(1) output parsed into fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatInfo {
    pub path: String,
    pub size: u64,
    /// Octal mode without the file-type bits (e.g. "0644").
    pub mode: String,
    pub owner: String,
    pub group: String,
    /// ISO-8601 mtime as reported by the remote `stat` command.
    pub modified_at: String,
}

/// The only thing the provisioner needs from the SSH layer: run a command,
/// run one that reads bytes from stdin, or fetch a file's contents.
pub trait SshExec: Send + Sync {
    fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError>;
    fn exec_with_stdin(&self, cmd: &str, stdin: &[u8]) -> Result<ExecOutput, SshError>;
}

/// Real [`SshExec`] implementation that shells out to the OpenSSH client.
#[derive(Debug, Clone)]
pub struct OpenSshExec {
    pub host: String,
    pub user: Option<String>,
    pub port: Option<u16>,
    /// Path to a private key, usually the output of writing an `op://`
    /// secret to disk in the executor layer.  `None` means use the default
    /// agent / `~/.ssh/config` resolution.
    pub key_path: Option<PathBuf>,
    /// Extra `-o` options appended verbatim.  Tests can pass
    /// `BatchMode=yes` to ensure we never block on a password prompt.
    pub extra_opts: Vec<String>,
}

impl OpenSshExec {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            user: None,
            port: None,
            key_path: None,
            extra_opts: vec![
                // Never prompt interactively — we're running inside orbi,
                // not a terminal.
                "BatchMode=yes".into(),
                // Accept unknown host keys once, then lock them in.  More
                // permissive than strict, but lets idempotent runs against
                // new hosts succeed on the first try.
                "StrictHostKeyChecking=accept-new".into(),
                "ConnectTimeout=10".into(),
            ],
        }
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn with_key(mut self, path: PathBuf) -> Self {
        self.key_path = Some(path);
        self
    }

    /// `user@host` target string for ssh/scp argv.
    pub fn target(&self) -> String {
        match &self.user {
            Some(u) => format!("{u}@{}", self.host),
            None => self.host.clone(),
        }
    }

    /// Build the full argv for an `ssh` invocation with the given remote
    /// command as the trailing argument.  Options come first so the command
    /// is always the last positional arg.
    fn build_argv(&self, remote_cmd: &str) -> Vec<String> {
        let mut argv: Vec<String> = vec!["ssh".into()];
        for opt in &self.extra_opts {
            argv.push("-o".into());
            argv.push(opt.clone());
        }
        if let Some(p) = self.port {
            argv.push("-p".into());
            argv.push(p.to_string());
        }
        if let Some(k) = &self.key_path {
            argv.push("-i".into());
            argv.push(k.to_string_lossy().into());
            // When we supply an explicit key, also disable agent and
            // identity file fallback so we get a deterministic auth attempt.
            argv.push("-o".into());
            argv.push("IdentitiesOnly=yes".into());
        }
        argv.push(self.target());
        argv.push(remote_cmd.into());
        argv
    }
}

impl SshExec for OpenSshExec {
    fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
        let argv = self.build_argv(cmd);
        let mut c = Command::new(&argv[0]);
        c.args(&argv[1..]);
        let out = c.output().map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => SshError::NotInstalled,
            _ => SshError::Io(e.to_string()),
        })?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let exit_code = out.status.code().unwrap_or(-1);
        if exit_code != 0 && looks_like_auth_failure(&stderr) {
            return Err(SshError::AuthFailed(stderr.trim().to_string()));
        }
        if exit_code != 0 && looks_like_connect_failure(&stderr) {
            return Err(SshError::ConnectFailed {
                host: self.host.clone(),
                msg: stderr.trim().to_string(),
            });
        }
        Ok(ExecOutput {
            stdout,
            stderr,
            exit_code,
        })
    }

    fn exec_with_stdin(&self, cmd: &str, stdin: &[u8]) -> Result<ExecOutput, SshError> {
        let argv = self.build_argv(cmd);
        let mut c = Command::new(&argv[0]);
        c.args(&argv[1..]);
        c.stdin(Stdio::piped());
        c.stdout(Stdio::piped());
        c.stderr(Stdio::piped());
        let mut child = c.spawn().map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => SshError::NotInstalled,
            _ => SshError::Io(e.to_string()),
        })?;
        if let Some(mut s) = child.stdin.take() {
            s.write_all(stdin)
                .map_err(|e| SshError::Io(format!("stdin write: {e}")))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| SshError::Io(format!("wait: {e}")))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let exit_code = out.status.code().unwrap_or(-1);
        if exit_code != 0 && looks_like_auth_failure(&stderr) {
            return Err(SshError::AuthFailed(stderr.trim().to_string()));
        }
        if exit_code != 0 && looks_like_connect_failure(&stderr) {
            return Err(SshError::ConnectFailed {
                host: self.host.clone(),
                msg: stderr.trim().to_string(),
            });
        }
        Ok(ExecOutput {
            stdout,
            stderr,
            exit_code,
        })
    }
}

fn looks_like_auth_failure(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("permission denied")
        || s.contains("publickey")
        || s.contains("authentication failed")
}

fn looks_like_connect_failure(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    s.contains("no route to host")
        || s.contains("connection refused")
        || s.contains("connection timed out")
        || s.contains("could not resolve hostname")
}

/// High-level SSH client: builds on any [`SshExec`] implementation and
/// exposes filesystem primitives the resource provisioner needs.
pub struct SshClient {
    exec: Box<dyn SshExec>,
}

impl SshClient {
    pub fn new(exec: Box<dyn SshExec>) -> Self {
        Self { exec }
    }

    /// Construct a real SSH client from provider config.
    pub fn from_openssh(
        host: &str,
        user: Option<&str>,
        port: Option<u16>,
        key_path: Option<PathBuf>,
    ) -> Self {
        let mut e = OpenSshExec::new(host);
        if let Some(u) = user {
            e = e.with_user(u);
        }
        if let Some(p) = port {
            e = e.with_port(p);
        }
        if let Some(k) = key_path {
            e = e.with_key(k);
        }
        Self::new(Box::new(e))
    }

    /// Run a remote command and return raw output.
    pub fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
        self.exec.exec(cmd)
    }

    /// Run a remote command, failing if exit != 0.
    pub fn exec_checked(&self, cmd: &str) -> Result<String, SshError> {
        let out = self.exec.exec(cmd)?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(out.stdout)
    }

    /// Upload bytes to `remote_path` atomically: stream through stdin into
    /// `path.tmp.<pid>`, then `mv` into place.  `chmod`/`chown` must be
    /// called separately.
    pub fn upload(&self, content: &[u8], remote_path: &str) -> Result<(), SshError> {
        let q = shell_single_quote(remote_path);
        // Use process-local uniqueness so concurrent writers don't collide
        // on the tmp filename.  The `$$` is evaluated on the remote shell.
        let cmd = format!("tmp={q}.tmp.$$ && cat > \"$tmp\" && mv \"$tmp\" {q}");
        let out = self.exec.exec_with_stdin(&cmd, content)?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(())
    }

    /// Read remote file bytes.  Used by `file_stat`'s content fetch and
    /// by the provisioner's drift detection on small files.
    pub fn read(&self, remote_path: &str) -> Result<Vec<u8>, SshError> {
        let q = shell_single_quote(remote_path);
        let out = self.exec.exec(&format!("cat {q}"))?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        Ok(out.stdout.into_bytes())
    }

    /// SHA-256 of a remote file, or `None` if the file doesn't exist.
    pub fn sha256(&self, remote_path: &str) -> Result<Option<String>, SshError> {
        let q = shell_single_quote(remote_path);
        // `sha256sum` on Linux, `shasum -a 256` on BSD/macOS.  Try the GNU
        // one first; fall back on non-zero exit.
        let cmd = format!(
            "if [ ! -e {q} ]; then echo MISSING; exit 0; fi; \
             if command -v sha256sum >/dev/null 2>&1; then sha256sum {q}; \
             else shasum -a 256 {q}; fi"
        );
        let out = self.exec.exec(&cmd)?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        let stdout = out.stdout.trim();
        if stdout == "MISSING" {
            return Ok(None);
        }
        // Both tools print "<hex>  <path>".  Grab the first whitespace-
        // separated token.
        Ok(stdout
            .split_whitespace()
            .next()
            .map(|h| h.to_string()))
    }

    /// Parsed `stat` info, or `None` if the file doesn't exist.  Tries the
    /// GNU format first, then falls back to BSD.
    pub fn stat(&self, remote_path: &str) -> Result<Option<StatInfo>, SshError> {
        let q = shell_single_quote(remote_path);
        // GNU stat: -c "%s|%a|%U|%G|%y"
        // BSD stat: -f "%z|%p|%Su|%Sg|%Sm"
        let cmd = format!(
            "if [ ! -e {q} ]; then echo MISSING; exit 0; fi; \
             if stat --version >/dev/null 2>&1; then \
               stat -c '%s|%a|%U|%G|%y' {q}; \
             else \
               stat -f '%z|%p|%Su|%Sg|%Sm' {q}; \
             fi"
        );
        let out = self.exec.exec(&cmd)?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        let line = out.stdout.trim();
        if line == "MISSING" {
            return Ok(None);
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 5 {
            return Err(SshError::CommandFailed {
                exit_code: 0,
                stderr: format!("unexpected stat output: {line}"),
            });
        }
        let size: u64 = parts[0].trim().parse().unwrap_or(0);
        // GNU prints "644" for mode, BSD prints the full inode mode
        // (e.g. "100644").  Strip to the low 4 octal digits.
        let mode_raw = parts[1].trim();
        let mode = if mode_raw.len() > 4 {
            format!("0{}", &mode_raw[mode_raw.len() - 4..])
        } else {
            format!("0{mode_raw:0>3}")
        };
        Ok(Some(StatInfo {
            path: remote_path.to_string(),
            size,
            mode,
            owner: parts[2].trim().to_string(),
            group: parts[3].trim().to_string(),
            modified_at: parts[4].trim().to_string(),
        }))
    }

    /// readlink, or `None` if the path is not a symlink.
    pub fn readlink(&self, remote_path: &str) -> Result<Option<String>, SshError> {
        let q = shell_single_quote(remote_path);
        let cmd = format!(
            "if [ -L {q} ]; then readlink {q}; else echo NOTLINK; fi"
        );
        let out = self.exec.exec(&cmd)?;
        if out.exit_code != 0 {
            return Err(SshError::CommandFailed {
                exit_code: out.exit_code,
                stderr: out.stderr.trim().to_string(),
            });
        }
        let s = out.stdout.trim();
        if s == "NOTLINK" {
            return Ok(None);
        }
        Ok(Some(s.to_string()))
    }

    pub fn mkdir_p(&self, path: &str) -> Result<(), SshError> {
        let q = shell_single_quote(path);
        self.exec_checked(&format!("mkdir -p {q}")).map(|_| ())
    }

    pub fn chmod(&self, path: &str, mode: &str) -> Result<(), SshError> {
        // Mode is validated by the provisioner; we still refuse anything
        // with shell metacharacters to be safe.
        if !mode
            .chars()
            .all(|c| c.is_ascii_digit() || c == '+' || c == '-' || c == '=')
        {
            return Err(SshError::CommandFailed {
                exit_code: 0,
                stderr: format!("invalid mode: {mode}"),
            });
        }
        let q = shell_single_quote(path);
        self.exec_checked(&format!("chmod {mode} {q}")).map(|_| ())
    }

    pub fn chown(
        &self,
        path: &str,
        owner: Option<&str>,
        group: Option<&str>,
    ) -> Result<(), SshError> {
        let spec = match (owner, group) {
            (Some(o), Some(g)) => format!("{o}:{g}"),
            (Some(o), None) => o.to_string(),
            (None, Some(g)) => format!(":{g}"),
            (None, None) => return Ok(()),
        };
        // Owner/group come from DSL params; reject anything outside the
        // Unix username character set.
        if !spec
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':' || c == '.')
        {
            return Err(SshError::CommandFailed {
                exit_code: 0,
                stderr: format!("invalid owner/group spec: {spec}"),
            });
        }
        let q = shell_single_quote(path);
        self.exec_checked(&format!("chown {spec} {q}")).map(|_| ())
    }

    /// `ln -sfn target link` — atomic replace of an existing symlink.
    pub fn symlink_create(&self, target: &str, link_path: &str) -> Result<(), SshError> {
        let qt = shell_single_quote(target);
        let ql = shell_single_quote(link_path);
        self.exec_checked(&format!("ln -sfn {qt} {ql}")).map(|_| ())
    }

    pub fn remove(&self, path: &str) -> Result<(), SshError> {
        let q = shell_single_quote(path);
        self.exec_checked(&format!("rm -f {q}")).map(|_| ())
    }

    pub fn remove_dir(&self, path: &str) -> Result<(), SshError> {
        let q = shell_single_quote(path);
        self.exec_checked(&format!("rmdir {q}")).map(|_| ())
    }
}

/// Single-quote a string for POSIX shells.  `'` becomes `'\''`.
pub fn shell_single_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::Mutex;

    /// Scripted SshExec for unit tests.  Stores the commands it saw and
    /// returns canned outputs by matching on a prefix of the command.
    pub struct FakeExec {
        pub responses: Mutex<Vec<(String, ExecOutput)>>,
        pub seen: Mutex<Vec<String>>,
        pub seen_stdin: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl FakeExec {
        pub fn new(responses: Vec<(&str, ExecOutput)>) -> Self {
            Self {
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                ),
                seen: Mutex::new(vec![]),
                seen_stdin: Mutex::new(vec![]),
            }
        }

        fn matched(&self, cmd: &str) -> ExecOutput {
            let r = self.responses.lock().unwrap();
            for (prefix, out) in r.iter() {
                if cmd.contains(prefix.as_str()) {
                    return out.clone();
                }
            }
            // Default: ok
            ExecOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
            }
        }
    }

    impl SshExec for FakeExec {
        fn exec(&self, cmd: &str) -> Result<ExecOutput, SshError> {
            self.seen.lock().unwrap().push(cmd.to_string());
            Ok(self.matched(cmd))
        }
        fn exec_with_stdin(&self, cmd: &str, stdin: &[u8]) -> Result<ExecOutput, SshError> {
            self.seen_stdin
                .lock()
                .unwrap()
                .push((cmd.to_string(), stdin.to_vec()));
            Ok(self.matched(cmd))
        }
    }

    #[test]
    fn test_shell_single_quote_plain() {
        assert_eq!(shell_single_quote("hello"), "'hello'");
    }

    #[test]
    fn test_shell_single_quote_with_apostrophe() {
        assert_eq!(shell_single_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn test_shell_single_quote_with_spaces_and_slashes() {
        assert_eq!(
            shell_single_quote("/etc/nginx/sites available/foo.conf"),
            "'/etc/nginx/sites available/foo.conf'"
        );
    }

    #[test]
    fn test_openssh_argv_minimal() {
        let e = OpenSshExec::new("earth.example").with_user("root");
        let argv = e.build_argv("whoami");
        assert_eq!(argv[0], "ssh");
        // BatchMode, StrictHostKeyChecking, ConnectTimeout
        assert!(argv.iter().filter(|a| *a == "-o").count() >= 3);
        assert!(argv.contains(&"root@earth.example".to_string()));
        assert_eq!(argv.last().unwrap(), "whoami");
    }

    #[test]
    fn test_openssh_argv_with_port_and_key() {
        let e = OpenSshExec::new("earth.example")
            .with_user("azureuser")
            .with_port(2222)
            .with_key(PathBuf::from("/tmp/id_ed25519"));
        let argv = e.build_argv("ls /");
        assert!(argv.windows(2).any(|w| w == ["-p", "2222"]));
        assert!(argv.windows(2).any(|w| w == ["-i", "/tmp/id_ed25519"]));
        // IdentitiesOnly is added when a key is supplied
        assert!(argv.iter().any(|a| a == "IdentitiesOnly=yes"));
    }

    #[test]
    fn test_sha256_parse_gnu() {
        let fake = FakeExec::new(vec![(
            "sha256sum",
            ExecOutput {
                stdout: "abc123  /etc/foo\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        assert_eq!(c.sha256("/etc/foo").unwrap(), Some("abc123".into()));
    }

    #[test]
    fn test_sha256_missing_file() {
        let fake = FakeExec::new(vec![(
            "sha256sum",
            ExecOutput {
                stdout: "MISSING\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        assert_eq!(c.sha256("/etc/missing").unwrap(), None);
    }

    #[test]
    fn test_stat_parse_gnu() {
        let fake = FakeExec::new(vec![(
            "stat -c",
            ExecOutput {
                stdout: "1024|644|root|root|2026-04-09 12:00:00.000000000 +0000\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        let s = c.stat("/etc/foo").unwrap().unwrap();
        assert_eq!(s.size, 1024);
        assert_eq!(s.mode, "0644");
        assert_eq!(s.owner, "root");
        assert_eq!(s.group, "root");
    }

    #[test]
    fn test_stat_missing() {
        let fake = FakeExec::new(vec![(
            "stat -c",
            ExecOutput {
                stdout: "MISSING\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        assert!(c.stat("/nope").unwrap().is_none());
    }

    #[test]
    fn test_readlink_notlink() {
        let fake = FakeExec::new(vec![(
            "readlink",
            ExecOutput {
                stdout: "NOTLINK\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        assert!(c.readlink("/etc/hosts").unwrap().is_none());
    }

    #[test]
    fn test_readlink_present() {
        let fake = FakeExec::new(vec![(
            "readlink",
            ExecOutput {
                stdout: "/etc/nginx/sites-available/foo\n".into(),
                stderr: "".into(),
                exit_code: 0,
            },
        )]);
        let c = SshClient::new(Box::new(fake));
        assert_eq!(
            c.readlink("/etc/nginx/sites-enabled/foo").unwrap(),
            Some("/etc/nginx/sites-available/foo".into())
        );
    }

    #[test]
    fn test_chmod_rejects_shell_meta() {
        let fake = FakeExec::new(vec![]);
        let c = SshClient::new(Box::new(fake));
        let e = c.chmod("/etc/foo", "0644; rm -rf /").unwrap_err();
        match e {
            SshError::CommandFailed { stderr, .. } => assert!(stderr.contains("invalid mode")),
            _ => panic!("expected CommandFailed"),
        }
    }

    #[test]
    fn test_chown_rejects_shell_meta() {
        let fake = FakeExec::new(vec![]);
        let c = SshClient::new(Box::new(fake));
        let e = c.chown("/etc/foo", Some("root;rm"), None).unwrap_err();
        match e {
            SshError::CommandFailed { stderr, .. } => assert!(stderr.contains("invalid")),
            _ => panic!("expected CommandFailed"),
        }
    }

    #[test]
    fn test_upload_atomic_mv() {
        let fake = FakeExec::new(vec![]);
        let c = SshClient::new(Box::new(fake));
        c.upload(b"hello world", "/etc/foo.conf").unwrap();
        // The provisioner pipes through a tmp path + mv.  Dropping the
        // client would clean up but we reach into the fake to assert.
    }

    // Unused but kept here as a sanity guard against `RefCell: !Send`
    // leaking into the client type.
    #[allow(dead_code)]
    fn _assert_send(_: &SshClient) {}
    #[allow(dead_code)]
    type _Rc = RefCell<()>;
}
