//! GitHub provider for KVMQL/Orbi.
//!
//! Maps KVMQL resource types (`gh_repo`, `gh_ruleset`, `gh_secret`,
//! `gh_variable`, `gh_workflow_file`, `gh_branch_protection`) to the GitHub
//! API by shelling out to the `gh` CLI.  Same pattern as the Azure driver
//! shelling to `az` — keeps libsodium / OAuth flows out of our codebase by
//! delegating to `gh`.

pub mod cli;
pub mod resources;

pub use cli::{GhCli, GhError};
pub use resources::GithubResourceProvisioner;
