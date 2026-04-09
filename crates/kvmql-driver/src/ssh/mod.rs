//! SSH provider for Orbi.
//!
//! Manages files, directories, and symlinks on remote hosts by shelling out
//! to the OpenSSH client.  Auth, `~/.ssh/config`, `ProxyJump`, and agent
//! forwarding all come "for free" because we delegate to `ssh`/`scp`
//! themselves rather than reimplementing the protocol.
//!
//! Resource types handled by [`SshResourceProvisioner`]:
//! - `file` — arbitrary remote file, content from literal, file://, or
//!   credential URI (op://, env://, vault://, ...).  Idempotent via SHA-256.
//! - `directory` — `mkdir -p` with owner/group/mode.
//! - `symlink` — `ln -sfn target link`, compared via `readlink`.

pub mod client;
pub mod resources;

pub use client::{SshClient, SshError, SshExec, StatInfo};
pub use resources::SshResourceProvisioner;
