//! SSH provider for Orbi.
//!
//! Manages files, directories, symlinks, systemd units, nginx vhosts,
//! and Docker containers on remote hosts by shelling out to the OpenSSH
//! client.  Auth, `~/.ssh/config`, `ProxyJump`, and agent forwarding
//! all come "for free" because we delegate to `ssh`/`scp` themselves
//! rather than reimplementing the protocol.
//!
//! Resource types handled by [`SshResourceProvisioner`]:
//! - `file`, `directory`, `symlink` — filesystem primitives (see `resources.rs`)
//! - `systemd_service`, `systemd_timer` — systemd unit management
//! - `nginx_vhost`, `nginx_proxy` — nginx site management
//! - `docker_container`, `docker_volume`, `docker_network`, `docker_compose`

pub mod client;
pub mod docker;
pub mod letsencrypt;
pub mod nginx;
pub mod resources;
pub mod systemd;

pub use client::{SshClient, SshError, SshExec, StatInfo};
pub use resources::SshResourceProvisioner;
