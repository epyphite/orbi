//! GitHub resource provisioner.
//!
//! Dispatches KVMQL `gh_*` resource types to the GitHub API by way of the
//! `gh` CLI.  Built from an optional token; when absent, falls back to the
//! ambient `gh auth login` session if `gh` is on PATH.
//!
//! Supported resource types:
//! - `gh_repo` -- repository (create/delete)
//! - `gh_ruleset` -- branch ruleset (newer API, recommended)
//! - `gh_secret` -- Actions secret (gh handles libsodium encryption)
//! - `gh_variable` -- Actions variable
//! - `gh_workflow_file` -- file in `.github/workflows/`
//! - `gh_branch_protection` -- legacy branch protection API

use serde_json::{json, Value};

use super::cli::GhCli;

#[derive(Debug, Clone)]
pub struct GithubResourceProvisioner {
    cli: Option<GhCli>,
}

/// Result of a provisioning operation.  Mirrors the Cloudflare/Azure shape.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (repo URL, ruleset_id, commit SHA, ...).
    pub outputs: Option<Value>,
}

impl GithubResourceProvisioner {
    pub fn new(token: Option<&str>) -> Self {
        let cli = match token {
            Some(t) => Some(GhCli::with_token(t)),
            None => {
                // Fall back to ambient `gh auth` if the CLI is installed.
                if GhCli::check_available().is_ok() {
                    Some(GhCli::new())
                } else {
                    None
                }
            }
        };
        Self { cli }
    }

    fn cli(&self) -> Result<&GhCli, String> {
        self.cli.as_ref().ok_or_else(|| {
            "gh CLI not configured. Install from https://cli.github.com/ and run 'gh auth login', \
             or set auth='env:GITHUB_TOKEN' on the provider."
                .to_string()
        })
    }

    pub fn create(&self, resource_type: &str, params: &Value) -> Result<ProvisionResult, String> {
        match resource_type {
            "gh_repo" => self.create_repo(params),
            "gh_ruleset" => self.create_ruleset(params),
            "gh_secret" => self.create_secret(params),
            "gh_variable" => self.create_variable(params),
            "gh_workflow_file" => self.create_workflow_file(params),
            "gh_branch_protection" => self.create_branch_protection(params),
            other => Err(format!("unsupported github resource type: {other}")),
        }
    }

    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "gh_repo" => self.delete_repo(id, params),
            "gh_ruleset" => self.delete_ruleset(id, params),
            "gh_secret" => self.delete_secret(id, params),
            "gh_variable" => self.delete_variable(id, params),
            "gh_workflow_file" => self.delete_workflow_file(id, params),
            "gh_branch_protection" => self.delete_branch_protection(id, params),
            other => Err(format!("unsupported github resource type: {other}")),
        }
    }

    // ── Repo ──────────────────────────────────────────────────

    fn create_repo(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let id = param_str(params, "id")?; // "org/name" or just "name"
        let visibility = param_str_or(params, "visibility", "private");
        let description = params.get("description").and_then(|v| v.as_str());

        let result = cli
            .repo_create(&id, &visibility, description)
            .map_err(|e| format!("failed to create repo: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": id,
                "visibility": visibility,
                "result": result,
            })),
        })
    }

    fn delete_repo(&self, id: &str, _params: &Value) -> Result<(), String> {
        let cli = self.cli()?;
        cli.repo_delete(id)
            .map_err(|e| format!("failed to delete repo: {e}"))?;
        Ok(())
    }

    // ── Ruleset ──────────────────────────────────────────────

    fn create_ruleset(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        let name = param_str(params, "id")?;
        let target = param_str_or(params, "target", "branch");
        let enforcement = param_str_or(params, "enforcement", "active");
        let require_pr = params
            .get("require_pr")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let required_approvals = params
            .get("required_approvals")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let branches_pattern = param_str_or(params, "branches", "~DEFAULT_BRANCH");

        let mut rules = vec![];
        if require_pr {
            rules.push(json!({
                "type": "pull_request",
                "parameters": {
                    "required_approving_review_count": required_approvals,
                    "dismiss_stale_reviews_on_push": true,
                    "require_code_owner_review": false,
                    "require_last_push_approval": false,
                    "required_review_thread_resolution": false
                }
            }));
        }
        // Disallow branch deletion.
        rules.push(json!({ "type": "deletion" }));
        // Optional linear history.
        if params
            .get("linear_history")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            rules.push(json!({ "type": "required_linear_history" }));
        }

        let body = json!({
            "name": name,
            "target": target,
            "enforcement": enforcement,
            "conditions": {
                "ref_name": {
                    "include": [branches_pattern],
                    "exclude": []
                }
            },
            "rules": rules,
        });

        let result = cli
            .ruleset_create(&repo, &body)
            .map_err(|e| format!("failed to create ruleset: {e}"))?;

        let ruleset_id = result
            .get("id")
            .and_then(|v| v.as_i64())
            .map(|n| n.to_string());
        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": repo,
                "ruleset_id": ruleset_id,
                "name": name,
                "enforcement": enforcement,
            })),
        })
    }

    fn delete_ruleset(&self, _id: &str, params: &Value) -> Result<(), String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        // The friendly id is the rule name; the real numeric ruleset_id lives
        // in outputs (passed in here via params after the executor merges them).
        let ruleset_id = params
            .get("ruleset_id")
            .and_then(|v| v.as_str())
            .ok_or("ruleset_id missing from params; required for delete")?;
        cli.ruleset_delete(&repo, ruleset_id)
            .map_err(|e| format!("failed to delete ruleset: {e}"))?;
        Ok(())
    }

    // ── Secret ───────────────────────────────────────────────

    fn create_secret(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let name = param_str(params, "id")?;
        let repo = param_str(params, "repo")?;
        let value = param_str(params, "value")?;

        cli.secret_set(&name, &repo, &value)
            .map_err(|e| format!("failed to set secret: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": repo,
                "name": name,
                // Do NOT include the value in outputs.
            })),
        })
    }

    fn delete_secret(&self, id: &str, params: &Value) -> Result<(), String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        cli.secret_delete(id, &repo)
            .map_err(|e| format!("failed to delete secret: {e}"))?;
        Ok(())
    }

    // ── Variable ─────────────────────────────────────────────

    fn create_variable(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let name = param_str(params, "id")?;
        let repo = param_str(params, "repo")?;
        let value = param_str(params, "value")?;

        cli.variable_set(&name, &repo, &value)
            .map_err(|e| format!("failed to set variable: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": repo,
                "name": name,
                "value": value,
            })),
        })
    }

    fn delete_variable(&self, id: &str, params: &Value) -> Result<(), String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        cli.variable_delete(id, &repo)
            .map_err(|e| format!("failed to delete variable: {e}"))?;
        Ok(())
    }

    // ── Workflow File ────────────────────────────────────────

    fn create_workflow_file(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let name = param_str(params, "id")?; // e.g. "claude-review.yml"
        let repo = param_str(params, "repo")?;
        let content = param_str(params, "content")?;
        let default_msg = format!("Add/update {name}");
        let message = param_str_or(params, "message", &default_msg);
        let branch = params.get("branch").and_then(|v| v.as_str());

        let path = format!(".github/workflows/{name}");

        let result = cli
            .file_put(&repo, &path, &content, &message, branch)
            .map_err(|e| format!("failed to put workflow file: {e}"))?;

        let commit_sha = result
            .get("commit")
            .and_then(|c| c.get("sha"))
            .and_then(|s| s.as_str())
            .map(String::from);

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": repo,
                "path": path,
                "commit_sha": commit_sha,
            })),
        })
    }

    fn delete_workflow_file(&self, _id: &str, _params: &Value) -> Result<(), String> {
        // Deleting a file via the contents API requires the SHA AND a commit;
        // the recommended path is to remove the file via a normal git commit.
        Err("workflow file deletion not supported via API; remove the file via a git commit".into())
    }

    // ── Branch Protection (legacy) ───────────────────────────

    fn create_branch_protection(&self, params: &Value) -> Result<ProvisionResult, String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        let branch = param_str_or(params, "id", "main");
        let required_approvals = params
            .get("required_approvals")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);

        let body = json!({
            "required_status_checks": null,
            "enforce_admins": false,
            "required_pull_request_reviews": {
                "dismiss_stale_reviews": true,
                "require_code_owner_reviews": false,
                "required_approving_review_count": required_approvals
            },
            "restrictions": null,
            "allow_force_pushes": false,
            "allow_deletions": false
        });

        cli.branch_protection_set(&repo, &branch, &body)
            .map_err(|e| format!("failed to set branch protection: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "repo": repo,
                "branch": branch,
            })),
        })
    }

    fn delete_branch_protection(&self, id: &str, params: &Value) -> Result<(), String> {
        let cli = self.cli()?;
        let repo = param_str(params, "repo")?;
        cli.branch_protection_delete(&repo, id)
            .map_err(|e| format!("failed to delete branch protection: {e}"))?;
        Ok(())
    }

    // ── EXPLAIN support ──────────────────────────────────────

    /// Build a human-readable description of the gh CLI calls a create would
    /// emit, without actually executing them.  Used by EXPLAIN and dry-run.
    /// Secret values are NEVER included in the output.
    pub fn build_create_args(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<Vec<String>, String> {
        match resource_type {
            "gh_repo" => {
                let id = param_str(params, "id")?;
                let vis = param_str_or(params, "visibility", "private");
                Ok(vec![
                    "gh repo create".into(),
                    id,
                    format!("--{vis}"),
                    "--confirm".into(),
                ])
            }
            "gh_ruleset" => {
                let repo = param_str(params, "repo")?;
                Ok(vec![
                    "gh api POST".into(),
                    format!("/repos/{repo}/rulesets"),
                ])
            }
            "gh_secret" => {
                let name = param_str(params, "id")?;
                let repo = param_str(params, "repo")?;
                Ok(vec![
                    "gh secret set".into(),
                    name,
                    format!("--repo {repo}"),
                    "--body <redacted>".into(),
                ])
            }
            "gh_variable" => {
                let name = param_str(params, "id")?;
                let repo = param_str(params, "repo")?;
                Ok(vec![
                    "gh variable set".into(),
                    name,
                    format!("--repo {repo}"),
                ])
            }
            "gh_workflow_file" => {
                let name = param_str(params, "id")?;
                let repo = param_str(params, "repo")?;
                Ok(vec![
                    "gh api PUT".into(),
                    format!("/repos/{repo}/contents/.github/workflows/{name}"),
                ])
            }
            "gh_branch_protection" => {
                let repo = param_str(params, "repo")?;
                let branch = param_str_or(params, "id", "main");
                Ok(vec![
                    "gh api PUT".into(),
                    format!("/repos/{repo}/branches/{branch}/protection"),
                ])
            }
            other => Err(format!("unsupported: {other}")),
        }
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

    #[test]
    fn test_param_str_present() {
        let params = json!({"id": "my-repo"});
        assert_eq!(param_str(&params, "id").unwrap(), "my-repo");
    }

    #[test]
    fn test_param_str_missing() {
        let params = json!({});
        assert!(param_str(&params, "id").is_err());
    }

    #[test]
    fn test_build_create_args_repo() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let params = json!({"id": "org/repo", "visibility": "public"});
        let args = p.build_create_args("gh_repo", &params).unwrap();
        assert!(args.iter().any(|a| a.contains("gh repo create")));
        assert!(args.iter().any(|a| a == "org/repo"));
        assert!(args.iter().any(|a| a == "--public"));
    }

    #[test]
    fn test_build_create_args_secret_redacted() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let params = json!({"id": "API_KEY", "repo": "org/repo", "value": "secret-value"});
        let args = p.build_create_args("gh_secret", &params).unwrap();
        // The actual value must NOT appear in the rendered command args.
        assert!(args.iter().all(|a| !a.contains("secret-value")));
        assert!(args.iter().any(|a| a.contains("<redacted>")));
    }

    #[test]
    fn test_build_create_args_ruleset() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let params = json!({"id": "main-protection", "repo": "org/repo"});
        let args = p.build_create_args("gh_ruleset", &params).unwrap();
        assert!(args.iter().any(|a| a.contains("rulesets")));
    }

    #[test]
    fn test_build_create_args_workflow() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let params = json!({
            "id": "claude-review.yml",
            "repo": "org/repo",
            "content": "name: review"
        });
        let args = p.build_create_args("gh_workflow_file", &params).unwrap();
        assert!(args
            .iter()
            .any(|a| a.contains(".github/workflows/claude-review.yml")));
    }

    #[test]
    fn test_build_create_args_branch_protection() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let params = json!({"id": "main", "repo": "org/repo"});
        let args = p
            .build_create_args("gh_branch_protection", &params)
            .unwrap();
        assert!(args.iter().any(|a| a.contains("branches/main/protection")));
    }

    #[test]
    fn test_unsupported_resource_type() {
        let p = GithubResourceProvisioner::new(Some("fake-token"));
        let result = p.create("gh_unknown", &json!({}));
        assert!(result.is_err());
    }
}
