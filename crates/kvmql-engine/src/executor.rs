use std::sync::Arc;
use std::time::Instant;

use kvmql_auth::access::{AccessChecker, AuthDecision, Grant};
use kvmql_driver::types::{CreateParams, ImageParams, VolumeParams};
use kvmql_parser::ast::*;
use kvmql_parser::parser::Parser;

use crate::context::{EngineContext, ExecutionMode};
use crate::errors::{with_remediation, ErrorContext};
use crate::response::*;

// ---------------------------------------------------------------------------
// Statement normalization — replaces literals with `?`
// ---------------------------------------------------------------------------

/// Replace all string literals (`'...'`) and numeric literals with `?`.
/// This is used to produce a normalized statement for query history so that
/// sensitive parameter values are never persisted.
fn normalize_statement(stmt: &str) -> String {
    let mut result = String::with_capacity(stmt.len());
    let mut chars = stmt.chars().peekable();

    while let Some(&ch) = chars.peek() {
        if ch == '\'' {
            // Consume the entire single-quoted string
            chars.next(); // skip opening quote
            loop {
                match chars.next() {
                    Some('\'') => break,
                    Some('\\') => { chars.next(); } // skip escaped char
                    None => break,
                    _ => {}
                }
            }
            result.push('?');
        } else if ch.is_ascii_digit() || (ch == '-' && {
            // Check if minus sign is a numeric prefix (preceded by operator context)
            let trimmed = result.trim_end();
            trimmed.is_empty()
                || trimmed.ends_with('=')
                || trimmed.ends_with('>')
                || trimmed.ends_with('<')
                || trimmed.ends_with(',')
                || trimmed.ends_with('(')
        }) {
            // Consume the entire numeric literal (integer or float)
            if ch == '-' {
                chars.next();
            }
            let mut has_digits = false;
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() || c == '.' {
                    has_digits = true;
                    chars.next();
                } else {
                    break;
                }
            }
            if has_digits {
                result.push('?');
            } else if ch == '-' {
                // Was just a minus sign, not a numeric literal
                result.push('-');
            }
        } else {
            result.push(ch);
            chars.next();
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Audit event mapping
// ---------------------------------------------------------------------------

/// Information about an audit event derived from a statement.
struct AuditEvent {
    action: &'static str,
    target_type: &'static str,
    target_id: Option<String>,
}

/// Determine the audit event for a mutation statement, if applicable.
/// Returns `None` for non-mutation statements (SELECT, SHOW, SET, WATCH).
fn audit_event_for_statement(stmt: &Statement) -> Option<AuditEvent> {
    match stmt {
        Statement::CreateMicrovm(s) => Some(AuditEvent {
            action: "VM_CREATED",
            target_type: "microvm",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::Destroy(s) => match s.target {
            DestroyTarget::Microvm => Some(AuditEvent {
                action: "VM_DESTROYED",
                target_type: "microvm",
                target_id: Some(s.id.clone()),
            }),
            DestroyTarget::Volume => Some(AuditEvent {
                action: "VOLUME_DESTROYED",
                target_type: "volume",
                target_id: Some(s.id.clone()),
            }),
        },
        Statement::AlterMicrovm(s) => Some(AuditEvent {
            action: "VM_ALTERED",
            target_type: "microvm",
            target_id: Some(s.id.clone()),
        }),
        Statement::AlterVolume(s) => Some(AuditEvent {
            action: "VOLUME_RESIZED",
            target_type: "volume",
            target_id: Some(s.id.clone()),
        }),
        Statement::Pause(s) => Some(AuditEvent {
            action: "VM_PAUSED",
            target_type: "microvm",
            target_id: Some(s.id.clone()),
        }),
        Statement::Resume(s) => Some(AuditEvent {
            action: "VM_RESUMED",
            target_type: "microvm",
            target_id: Some(s.id.clone()),
        }),
        Statement::Snapshot(s) => Some(AuditEvent {
            action: "SNAPSHOT_TAKEN",
            target_type: "microvm",
            target_id: Some(s.id.clone()),
        }),
        Statement::Restore(s) => Some(AuditEvent {
            action: "SNAPSHOT_RESTORED",
            target_type: "microvm",
            target_id: Some(s.id.clone()),
        }),
        Statement::CreateVolume(s) => Some(AuditEvent {
            action: "VOLUME_CREATED",
            target_type: "volume",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::Attach(s) => Some(AuditEvent {
            action: "VOLUME_ATTACHED",
            target_type: "volume",
            target_id: Some(s.volume_id.clone()),
        }),
        Statement::Detach(s) => Some(AuditEvent {
            action: "VOLUME_DETACHED",
            target_type: "volume",
            target_id: Some(s.volume_id.clone()),
        }),
        Statement::Resize(s) => Some(AuditEvent {
            action: "VOLUME_RESIZED",
            target_type: "volume",
            target_id: Some(s.volume_id.clone()),
        }),
        Statement::ImportImage(s) => Some(AuditEvent {
            action: "IMAGE_IMPORTED",
            target_type: "image",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::RemoveImage(s) => Some(AuditEvent {
            action: "IMAGE_REMOVED",
            target_type: "image",
            target_id: Some(s.image_id.clone()),
        }),
        Statement::AddProvider(s) => Some(AuditEvent {
            action: "PROVIDER_ADDED",
            target_type: "provider",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::RemoveProvider(s) => Some(AuditEvent {
            action: "PROVIDER_REMOVED",
            target_type: "provider",
            target_id: Some(s.name.clone()),
        }),
        Statement::AlterProvider(s) => Some(AuditEvent {
            action: "PROVIDER_ALTERED",
            target_type: "provider",
            target_id: Some(s.name.clone()),
        }),
        Statement::AddCluster(s) => Some(AuditEvent {
            action: "CLUSTER_CREATED",
            target_type: "cluster",
            target_id: Some(s.name.clone()),
        }),
        Statement::RemoveCluster(s) => Some(AuditEvent {
            action: "CLUSTER_REMOVED",
            target_type: "cluster",
            target_id: Some(s.name.clone()),
        }),
        Statement::AlterCluster(s) => Some(AuditEvent {
            action: "CLUSTER_ALTERED",
            target_type: "cluster",
            target_id: Some(s.name.clone()),
        }),
        Statement::AddPrincipal(s) => Some(AuditEvent {
            action: "PRINCIPAL_ADDED",
            target_type: "principal",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::Grant(s) => Some(AuditEvent {
            action: "GRANT_ADDED",
            target_type: "grant",
            target_id: Some(s.principal.clone()),
        }),
        Statement::Revoke(s) => Some(AuditEvent {
            action: "GRANT_REVOKED",
            target_type: "grant",
            target_id: Some(s.principal.clone()),
        }),
        Statement::CreateResource(s) => Some(AuditEvent {
            action: "RESOURCE_CREATED",
            target_type: "resource",
            target_id: get_param(&s.params, "id"),
        }),
        Statement::AlterResource(s) => Some(AuditEvent {
            action: "RESOURCE_ALTERED",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        Statement::DestroyResource(s) => Some(AuditEvent {
            action: "RESOURCE_DESTROYED",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        Statement::Backup(s) => Some(AuditEvent {
            action: "RESOURCE_BACKED_UP",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        Statement::RestoreResource(s) => Some(AuditEvent {
            action: "RESOURCE_RESTORED",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        Statement::Scale(s) => Some(AuditEvent {
            action: "RESOURCE_SCALED",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        Statement::Upgrade(s) => Some(AuditEvent {
            action: "RESOURCE_UPGRADED",
            target_type: "resource",
            target_id: Some(s.id.clone()),
        }),
        // Non-mutation statements
        Statement::Select(_)
        | Statement::Show(_)
        | Statement::Set(_)
        | Statement::Watch(_)
        | Statement::PublishImage(_) => None,
        // EXPLAIN is introspection, never a mutation
        Statement::Explain(_) => None,
        Statement::Rollback(s) => Some(AuditEvent {
            action: "ROLLBACK",
            target_type: "resource",
            target_id: match &s.target {
                kvmql_parser::ast::RollbackTarget::Resource { id, .. } => Some(id.clone()),
                _ => None,
            },
        }),
        // ASSERT is verification, never a mutation — no audit entry.
        Statement::Assert(_) => None,
    }
}

/// Extract the primary verb from a statement.
fn verb_for_statement(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Select(_) => "SELECT",
        Statement::CreateMicrovm(_) => "CREATE",
        Statement::CreateVolume(_) => "CREATE",
        Statement::AlterMicrovm(_) => "ALTER",
        Statement::AlterVolume(_) => "ALTER",
        Statement::Destroy(_) => "DESTROY",
        Statement::Pause(_) => "PAUSE",
        Statement::Resume(_) => "RESUME",
        Statement::Snapshot(_) => "SNAPSHOT",
        Statement::Restore(_) => "RESTORE",
        Statement::Watch(_) => "WATCH",
        Statement::Attach(_) => "ATTACH",
        Statement::Detach(_) => "DETACH",
        Statement::Resize(_) => "RESIZE",
        Statement::ImportImage(_) => "IMPORT",
        Statement::PublishImage(_) => "PUBLISH",
        Statement::RemoveImage(_) => "REMOVE",
        Statement::AddProvider(_) => "ADD",
        Statement::RemoveProvider(_) => "REMOVE",
        Statement::AlterProvider(_) => "ALTER",
        Statement::AddCluster(_) => "ADD",
        Statement::AlterCluster(_) => "ALTER",
        Statement::RemoveCluster(_) => "REMOVE",
        Statement::AddPrincipal(_) => "ADD",
        Statement::Grant(_) => "GRANT",
        Statement::Revoke(_) => "REVOKE",
        Statement::Set(_) => "SET",
        Statement::Show(_) => "SHOW",
        Statement::CreateResource(_) => "CREATE",
        Statement::AlterResource(_) => "ALTER",
        Statement::DestroyResource(_) => "DESTROY",
        Statement::Backup(_) => "BACKUP",
        Statement::RestoreResource(_) => "RESTORE",
        Statement::Scale(_) => "SCALE",
        Statement::Upgrade(_) => "UPGRADE",
        Statement::Explain(_) => "EXPLAIN",
        Statement::Rollback(_) => "ROLLBACK",
        Statement::Assert(_) => "ASSERT",
    }
}

/// Returns true if this statement type should skip query-history recording.
fn skip_history(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Set(_) | Statement::Show(_) | Statement::Explain(_) | Statement::Assert(_)
    )
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

pub struct Executor<'a> {
    ctx: &'a EngineContext,
}

impl<'a> Executor<'a> {
    pub fn new(ctx: &'a EngineContext) -> Self {
        Self { ctx }
    }

    /// Parse and execute one or more KVMQL statements.  Returns a single
    /// [`ResultEnvelope`] covering all statements in the batch.
    pub async fn execute(&self, source: &str) -> ResultEnvelope {
        let start = Instant::now();
        let request_id = uuid::Uuid::new_v4().to_string();

        // 1. Parse -----------------------------------------------------------
        let program = match Parser::parse(source) {
            Ok(p) => p,
            Err(e) => return error_envelope(request_id, &e.to_string(), start),
        };

        // 2. Execute each statement -----------------------------------------
        let mut notifications: Vec<Notification> = Vec::new();
        let mut last_result: Option<serde_json::Value> = None;
        let mut total_rows: i64 = 0;

        for stmt in &program.statements {
            // --- Auth check (Phase 11) ------------------------------------
            if self.ctx.auth_enabled {
                if let Some(ref principal_id) = self.ctx.current_principal {
                    let verb = verb_for_statement(stmt);
                    match self.check_auth(principal_id, verb) {
                        AuthDecision::Permitted => { /* proceed */ }
                        AuthDecision::Denied { reason } => {
                            // Write audit log with outcome "denied"
                            let audit = audit_event_for_statement(stmt);
                            if let Some(ref ev) = audit {
                                let _ = self.ctx.registry.insert_audit_log(
                                    Some(principal_id),
                                    ev.action,
                                    Some(ev.target_type),
                                    ev.target_id.as_deref(),
                                    "denied",
                                    Some(&reason),
                                    None,
                                );
                            }
                            let auth_msg = with_remediation(
                                "AUTH_DENIED",
                                &format!("AUTH_DENIED: {reason}"),
                                &ErrorContext {
                                    resource_id: Some(principal_id.clone()),
                                    ..Default::default()
                                },
                            );
                            return error_envelope_with(
                                request_id,
                                &auth_msg,
                                notifications,
                                last_result,
                                start,
                            );
                        }
                    }
                }
            }

            // --- Audit log (mutations only, BEFORE execution per spec) ---
            let audit = audit_event_for_statement(stmt);
            if let Some(ref ev) = audit {
                let detail = serde_json::json!({
                    "source": source,
                })
                .to_string();
                if let Err(e) = self.ctx.registry.insert_audit_log(
                    self.ctx.current_principal.as_deref(),
                    ev.action,
                    Some(ev.target_type),
                    ev.target_id.as_deref(),
                    "permitted",
                    None,           // reason
                    Some(&detail),
                ) {
                    // Per spec: if audit write fails, operation is aborted
                    return error_envelope_with(
                        request_id,
                        &format!("audit log write failed, operation aborted: {e}"),
                        notifications,
                        last_result,
                        start,
                    );
                }
            }

            // Capture state snapshot before mutations for ROLLBACK support
            self.capture_state_snapshot(stmt, source);

            match self.execute_statement(stmt).await {
                Ok(outcome) => {
                    notifications.extend(outcome.notifications);
                    if outcome.result.is_some() {
                        last_result = outcome.result;
                    }
                    total_rows += outcome.rows_affected;
                }
                Err(msg) => {
                    if self.ctx.execution_mode == ExecutionMode::Strict {
                        // Record query history even for errors (unless SET/SHOW)
                        if !skip_history(stmt) {
                            self.record_query_history(
                                source,
                                verb_for_statement(stmt),
                                &ResultStatus::Error,
                                &notifications,
                                total_rows,
                                &start,
                            );
                        }
                        return error_envelope_with(
                            request_id,
                            &msg,
                            notifications,
                            last_result,
                            start,
                        );
                    }
                    notifications.push(Notification {
                        level: "ERROR".into(),
                        code: "RTE_001".into(),
                        provider_id: None,
                        message: msg,
                    });
                }
            }
        }

        let status = if notifications.iter().any(|n| n.level == "ERROR") {
            ResultStatus::Warn
        } else {
            ResultStatus::Ok
        };

        // 3. Record query history (skip SET and SHOW) ----------------------
        let should_record = program
            .statements
            .iter()
            .any(|s| !skip_history(s));
        if should_record {
            let verb = program
                .statements
                .first()
                .map(|s| verb_for_statement(s))
                .unwrap_or("UNKNOWN");
            self.record_query_history(source, verb, &status, &notifications, total_rows, &start);
        }

        ResultEnvelope {
            request_id,
            status,
            notifications,
            result: last_result,
            rows_affected: Some(total_rows),
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Best-effort write of a query history row.
    fn record_query_history(
        &self,
        source: &str,
        verb: &str,
        status: &ResultStatus,
        notifications: &[Notification],
        rows_affected: i64,
        start: &Instant,
    ) {
        let normalized = normalize_statement(source);
        let status_str = match status {
            ResultStatus::Ok => "ok",
            ResultStatus::Warn => "warn",
            ResultStatus::Error => "error",
        };
        let notif_json = if notifications.is_empty() {
            None
        } else {
            serde_json::to_string(notifications).ok()
        };
        let duration = start.elapsed().as_millis() as i64;
        let _ = self.ctx.registry.insert_query_history(
            self.ctx.current_principal.as_deref(),
            source,
            Some(&normalized),
            verb,
            None,                       // targets
            Some(duration),
            status_str,
            notif_json.as_deref(),
            Some(rows_affected),
            None,                       // result_hash
        );
    }

    /// Perform the auth check for a principal + verb.  Loads grants from the
    /// registry, converts `GrantRow` → `Grant`, and delegates to
    /// `AccessChecker::check`.
    fn check_auth(&self, principal_id: &str, verb: &str) -> AuthDecision {
        let grant_rows = match self.ctx.registry.get_grants_for_principal(principal_id) {
            Ok(rows) => rows,
            Err(_) => {
                return AuthDecision::Denied {
                    reason: format!("failed to load grants for principal '{principal_id}'"),
                };
            }
        };

        let grants: Vec<Grant> = grant_rows
            .into_iter()
            .map(|row| {
                let verbs: Vec<String> = serde_json::from_str(&row.verbs)
                    .unwrap_or_default();
                Grant {
                    id: row.id,
                    principal_id: row.principal_id,
                    verbs,
                    scope_type: row.scope_type,
                    scope_id: row.scope_id,
                    conditions: row.conditions,
                }
            })
            .collect();

        // For now we don't extract a specific scope from the statement —
        // we pass None so the checker only enforces verb + scope_type.
        AccessChecker::check(&grants, verb, None, None)
    }
}

// ---------------------------------------------------------------------------
// Internal outcome type for a single statement
// ---------------------------------------------------------------------------

struct StmtOutcome {
    result: Option<serde_json::Value>,
    rows_affected: i64,
    notifications: Vec<Notification>,
}

impl StmtOutcome {
    fn ok_val(val: serde_json::Value) -> Self {
        Self {
            result: Some(val),
            rows_affected: 1,
            notifications: Vec::new(),
        }
    }

    fn ok_rows(val: serde_json::Value, n: i64) -> Self {
        Self {
            result: Some(val),
            rows_affected: n,
            notifications: Vec::new(),
        }
    }

    fn ok_empty() -> Self {
        Self {
            result: None,
            rows_affected: 1,
            notifications: Vec::new(),
        }
    }

    fn not_implemented(stmt_name: &str) -> Self {
        Self {
            result: None,
            rows_affected: 0,
            notifications: vec![Notification {
                level: "INFO".into(),
                code: "NYI_001".into(),
                provider_id: None,
                message: format!("{stmt_name} is not yet implemented"),
            }],
        }
    }
}

// ---------------------------------------------------------------------------
// Per-statement dispatch
// ---------------------------------------------------------------------------

impl<'a> Executor<'a> {
    async fn execute_statement(&self, stmt: &Statement) -> Result<StmtOutcome, String> {
        // Dry-run mode: wrap mutations in EXPLAIN automatically, but let
        // read-only statements (SELECT, SHOW, SET, EXPLAIN) through.
        if self.ctx.dry_run
            && !matches!(
                stmt,
                Statement::Explain(_)
                    | Statement::Select(_)
                    | Statement::Show(_)
                    | Statement::Set(_)
                    | Statement::Assert(_)
            )
        {
            return self.exec_explain(stmt).await;
        }

        match stmt {
            Statement::Explain(inner) => self.exec_explain(inner).await,
            Statement::AddProvider(s) => self.exec_add_provider(s),
            Statement::RemoveProvider(s) => self.exec_remove_provider(s),
            Statement::CreateMicrovm(s) => self.exec_create_microvm(s).await,
            Statement::Destroy(s) => self.exec_destroy(s).await,
            Statement::Pause(s) => self.exec_pause(s).await,
            Statement::Resume(s) => self.exec_resume(s).await,
            Statement::Snapshot(s) => self.exec_snapshot(s).await,
            Statement::Restore(s) => self.exec_restore(s).await,
            Statement::CreateVolume(s) => self.exec_create_volume(s).await,
            Statement::Attach(s) => self.exec_attach(s).await,
            Statement::Detach(s) => self.exec_detach(s).await,
            Statement::Resize(s) => self.exec_resize(s).await,
            Statement::AlterMicrovm(s) => self.exec_alter_microvm(s).await,
            Statement::AlterVolume(s) => self.exec_alter_volume(s).await,
            Statement::ImportImage(s) => self.exec_import_image(s).await,
            Statement::RemoveImage(s) => self.exec_remove_image(s).await,
            Statement::Select(s) => self.exec_select(s).await,
            Statement::Assert(s) => self.exec_assert(s).await,
            Statement::Show(s) => self.exec_show(s),
            Statement::Set(s) => self.exec_set(s),
            Statement::AddCluster(s) => self.exec_add_cluster(s),
            Statement::RemoveCluster(s) => self.exec_remove_cluster(s),
            Statement::AlterCluster(s) => self.exec_alter_cluster(s),
            Statement::AlterProvider(s) => self.exec_alter_provider(s),
            Statement::AddPrincipal(s) => self.exec_add_principal(s),
            Statement::Grant(s) => self.exec_grant(s),
            Statement::Revoke(s) => self.exec_revoke(s),
            Statement::Watch(s) => self.exec_watch(s).await,
            Statement::PublishImage(s) => self.exec_publish_image(s),
            Statement::CreateResource(s) => self.exec_create_resource(s),
            Statement::AlterResource(s) => self.exec_alter_resource(s),
            Statement::DestroyResource(s) => self.exec_destroy_resource(s),
            Statement::Backup(s) => self.exec_backup(s),
            Statement::RestoreResource(s) => self.exec_restore_resource(s),
            Statement::Scale(s) => self.exec_scale(s),
            Statement::Upgrade(s) => self.exec_upgrade(s),
            Statement::Rollback(s) => self.exec_rollback(s),
        }
    }

    // =======================================================================
    // EXPLAIN
    // =======================================================================

    async fn exec_explain(&self, stmt: &Statement) -> Result<StmtOutcome, String> {
        let mut steps = Vec::new();

        match stmt {
            Statement::CreateResource(s) => {
                let config = self.params_to_json(&s.params);
                let rtype = s.resource_type.as_str();

                let is_aws = matches!(
                    rtype,
                    "rds_postgres" | "vpc" | "aws_subnet" | "security_group" | "sg_rule"
                );
                let is_cloudflare = rtype.starts_with("cf_");
                let is_github = rtype.starts_with("gh_");
                let is_k8s = rtype.starts_with("k8s_");

                let (command_prefix, args_result) = if is_k8s {
                    let p = self.get_k8s_provisioner("default");
                    ("kubectl", p.build_create_args(rtype, &config))
                } else if is_cloudflare {
                    let p = self.get_cloudflare_provisioner("default");
                    ("cloudflare-api", p.build_create_args(rtype, &config))
                } else if is_github {
                    let p = self.get_github_provisioner("default");
                    ("gh", p.build_create_args(rtype, &config))
                } else if is_aws {
                    let p = self.get_aws_provisioner("default");
                    ("aws", p.build_create_args(rtype, &config))
                } else {
                    let p = self.get_azure_provisioner("default");
                    ("az", p.build_create_args(rtype, &config))
                };

                match args_result {
                    Ok(args) => {
                        let cmd_args = if is_cloudflare || is_github || is_k8s {
                            args.join(" ")
                        } else {
                            args[1..].iter().map(|a| a.as_str()).collect::<Vec<_>>().join(" ")
                        };
                        steps.push(serde_json::json!({
                            "step": 1,
                            "action": format!("CREATE RESOURCE '{}'", rtype),
                            "command": format!("{} {}", command_prefix, cmd_args),
                            "parameters": config,
                            "registry_action": "INSERT INTO resources",
                        }));
                    }
                    Err(e) => {
                        steps.push(serde_json::json!({
                            "step": 1,
                            "action": format!("CREATE RESOURCE '{}'", rtype),
                            "error": e,
                        }));
                    }
                }
            }
            Statement::DestroyResource(s) => {
                let rtype = s.resource_type.as_str();
                let is_aws = matches!(
                    rtype,
                    "rds_postgres" | "vpc" | "aws_subnet" | "security_group" | "sg_rule"
                );
                let is_cloudflare = rtype.starts_with("cf_");
                let is_github = rtype.starts_with("gh_");
                let is_k8s = rtype.starts_with("k8s_");

                // For Cloudflare/GitHub/Kubernetes and Azure sub-resources,
                // delete requires params/context that aren't available in
                // EXPLAIN mode — show a generic plan step.
                if is_k8s {
                    let kind = rtype.strip_prefix("k8s_").unwrap_or(rtype);
                    steps.push(serde_json::json!({
                        "step": 1,
                        "action": format!("DESTROY RESOURCE '{}' '{}'", rtype, s.id),
                        "command": format!("kubectl delete {} {} --ignore-not-found=true", kind, s.id),
                        "registry_action": "DELETE FROM resources",
                    }));
                } else if is_cloudflare {
                    steps.push(serde_json::json!({
                        "step": 1,
                        "action": format!("DESTROY RESOURCE '{}' '{}'", rtype, s.id),
                        "command": format!("cloudflare-api DELETE /zones/{{zone_id}}/.../{}", s.id),
                        "registry_action": "DELETE FROM resources",
                    }));
                } else if is_github {
                    steps.push(serde_json::json!({
                        "step": 1,
                        "action": format!("DESTROY RESOURCE '{}' '{}'", rtype, s.id),
                        "command": format!("gh api DELETE .../{}", s.id),
                        "registry_action": "DELETE FROM resources",
                    }));
                } else {
                    let args_result = if is_aws {
                        self.get_aws_provisioner("default").build_delete_args(rtype, &s.id)
                    } else {
                        self.get_azure_provisioner("default").build_delete_args(rtype, &s.id)
                    };
                    let prefix = if is_aws { "aws" } else { "az" };
                    match args_result {
                        Ok(args) => {
                            steps.push(serde_json::json!({
                                "step": 1,
                                "action": format!("DESTROY RESOURCE '{}' '{}'", rtype, s.id),
                                "command": format!("{} {}", prefix, args[1..].iter().map(|a| a.as_str()).collect::<Vec<_>>().join(" ")),
                                "registry_action": "DELETE FROM resources",
                            }));
                        }
                        Err(e) => {
                            steps.push(serde_json::json!({
                                "step": 1,
                                "action": format!("DESTROY RESOURCE '{}' '{}'", rtype, s.id),
                                "error": e,
                            }));
                        }
                    }
                }
            }
            Statement::CreateMicrovm(s) => {
                let params_json = self.params_to_json(&s.params);
                steps.push(serde_json::json!({
                    "step": 1,
                    "action": "CREATE MICROVM",
                    "driver_call": "driver.create()",
                    "parameters": params_json,
                    "registry_action": "INSERT INTO microvms",
                }));
            }
            Statement::Select(s) => {
                steps.push(serde_json::json!({
                    "step": 1,
                    "action": "SELECT",
                    "from": format!("{}", s.from),
                    "registry_action": format!("SELECT FROM {}", s.from),
                    "filters": s.where_clause.as_ref().map(|w| format!("{w}")),
                }));
            }
            Statement::Destroy(s) => {
                steps.push(serde_json::json!({
                    "step": 1,
                    "action": format!("DESTROY {} '{}'", s.target, s.id),
                    "driver_call": "driver.destroy()",
                    "registry_action": format!("DELETE FROM {}", match s.target {
                        DestroyTarget::Microvm => "microvms",
                        DestroyTarget::Volume => "volumes",
                    }),
                }));
            }
            Statement::AlterResource(s) => {
                steps.push(serde_json::json!({
                    "step": 1,
                    "action": format!("ALTER RESOURCE '{}' '{}'", s.resource_type, s.id),
                    "registry_action": "UPDATE resources",
                }));
            }
            other => {
                steps.push(serde_json::json!({
                    "step": 1,
                    "action": format!("{}", other),
                    "type": "would execute as normal",
                }));
            }
        }

        let plan = serde_json::json!({
            "explain": true,
            "statement": format!("{stmt}"),
            "steps": steps,
            "estimated_calls": steps.len(),
        });

        Ok(StmtOutcome::ok_val(plan))
    }

    fn get_azure_provisioner(&self, provider_id: &str) -> kvmql_driver::azure::resources::AzureResourceProvisioner {
        // Look up provider from registry to get subscription and resource group
        let (sub, rg) = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            // auth_ref may contain the subscription ID (for azure-kv or env ref)
            // region field often holds the resource group in Azure convention
            // The provider's own id or auth_ref can carry the subscription
            let subscription = if p.provider_type == "azure" {
                // Try to resolve the auth_ref to get subscription ID
                kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref).ok()
            } else {
                None
            };
            (subscription, p.region.clone())
        } else {
            (None, None)
        };
        kvmql_driver::azure::resources::AzureResourceProvisioner::new(
            sub.as_deref(),
            rg.as_deref(),
        )
    }

    fn get_aws_provisioner(&self, provider_id: &str) -> kvmql_driver::aws::resources::AwsResourceProvisioner {
        let (region, profile) = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            let prof = kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref).ok();
            (p.region.clone(), prof)
        } else {
            (None, None)
        };
        kvmql_driver::aws::resources::AwsResourceProvisioner::new(
            region.as_deref(),
            profile.as_deref(),
        )
    }

    fn get_cloudflare_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::cloudflare::CloudflareResourceProvisioner {
        let token = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref).ok()
        } else {
            // Fall back to the standard CLOUDFLARE_API_TOKEN env var.
            std::env::var("CLOUDFLARE_API_TOKEN").ok()
        };
        kvmql_driver::cloudflare::CloudflareResourceProvisioner::new(token.as_deref())
    }

    fn get_github_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::github::GithubResourceProvisioner {
        let token = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref).ok()
        } else {
            // Fall back to GITHUB_TOKEN, then GH_TOKEN — both are recognised
            // by `gh` itself, so we honour either as ambient credentials.
            std::env::var("GITHUB_TOKEN")
                .ok()
                .or_else(|| std::env::var("GH_TOKEN").ok())
        };
        kvmql_driver::github::GithubResourceProvisioner::new(token.as_deref())
    }

    /// Build an [`SshResourceProvisioner`] for the given provider.
    ///
    /// Resolves the provider's `auth_ref` (expected to be a credential URI
    /// pointing at a private key) and writes the key to a mode-0600
    /// tempfile so OpenSSH can consume it via `-i`.  The tempfile is
    /// leaked intentionally — its path is handed to `ssh`, and we want
    /// it to outlive this helper.  Callers should treat the path as
    /// process-scoped and never pass it elsewhere.
    ///
    /// When `auth_ref` is absent, empty, or "none", we fall back on the
    /// user's ambient SSH config (`~/.ssh/config`, agent, default keys).
    fn get_ssh_provisioner(
        &self,
        provider_id: &str,
    ) -> Result<kvmql_driver::ssh::SshResourceProvisioner, String> {
        let p = self
            .ctx
            .registry
            .get_provider(provider_id)
            .map_err(|e| format!("ssh provider '{provider_id}' not found: {e}"))?;
        let host = p
            .host
            .clone()
            .ok_or_else(|| format!("ssh provider '{provider_id}' is missing host="))?;
        // Labels JSON may hold user/port overrides; parse conservatively.
        let (user, port) = parse_ssh_connection_hints(p.labels.as_deref());

        let key_path = if p.auth_ref.is_empty() || p.auth_ref == "none" {
            None
        } else {
            match kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref) {
                Ok(key_material) => Some(write_ephemeral_key(&key_material)?),
                Err(e) => {
                    return Err(format!(
                        "failed to resolve ssh key '{}' for provider '{}': {}",
                        p.auth_ref, provider_id, e
                    ));
                }
            }
        };

        let client =
            kvmql_driver::ssh::SshClient::from_openssh(&host, user.as_deref(), port, key_path);
        Ok(kvmql_driver::ssh::SshResourceProvisioner::new(client))
    }

    fn get_k8s_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::k8s::KubernetesResourceProvisioner {
        // For k8s, "auth_ref" carries the kubeconfig context name (or an
        // env: ref pointing at one).  Fall back to the provider's `region`
        // field which we also document as a place to stash the context
        // name.  When neither is set, the provisioner uses the user's
        // current kubectl context.
        let context = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                .ok()
                .or_else(|| p.region.clone())
        } else {
            std::env::var("KUBECONTEXT").ok()
        };
        kvmql_driver::k8s::KubernetesResourceProvisioner::new(context.as_deref())
    }

    fn get_k8s_query_engine(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::k8s::KubernetesQueryEngine {
        let context = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                .ok()
                .or_else(|| p.region.clone())
        } else {
            std::env::var("KUBECONTEXT").ok()
        };
        kvmql_driver::k8s::KubernetesQueryEngine::new(context.as_deref())
    }

    /// Convert params to a `serde_json::Value` object.
    fn params_to_json(&self, params: &[Param]) -> serde_json::Value {
        let resolved = self.resolve_params(params);
        let mut map = serde_json::Map::new();
        for p in &resolved {
            let v = match &p.value {
                Value::String(s) => serde_json::Value::String(s.clone()),
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::Float(f) => serde_json::json!(f),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                _ => serde_json::Value::String(format!("{}", p.value)),
            };
            map.insert(p.key.clone(), v);
        }
        serde_json::Value::Object(map)
    }

    // =======================================================================
    // Provider management
    // =======================================================================

    fn exec_add_provider(&self, s: &AddProviderStmt) -> Result<StmtOutcome, String> {
        let id = get_param(&s.params, "id").ok_or("ADD PROVIDER requires id param")?;

        // IF NOT EXISTS: skip silently if provider already exists
        if s.if_not_exists {
            if let Ok(_existing) = self.ctx.registry.get_provider(&id) {
                let val = serde_json::json!({
                    "id": id,
                    "status": "already_exists",
                    "skipped": true,
                });
                let mut outcome = StmtOutcome::ok_val(val);
                outcome.notifications.push(Notification {
                    level: "INFO".into(),
                    code: "PROV_EXISTS".into(),
                    provider_id: None,
                    message: format!("Provider '{}' already exists -- skipped", id),
                });
                return Ok(outcome);
            }
        }

        let ptype =
            get_param(&s.params, "type").unwrap_or_else(|| "kvm".into());
        let driver =
            get_param(&s.params, "driver").unwrap_or_else(|| "firecracker".into());
        let auth = get_param(&s.params, "auth").unwrap_or_else(|| "none".into());
        let host = get_param(&s.params, "host");
        let region = get_param(&s.params, "region");
        let labels = get_param(&s.params, "labels");

        self.ctx
            .registry
            .insert_provider(
                &id,
                &ptype,
                &driver,
                "healthy",
                true,
                host.as_deref(),
                region.as_deref(),
                &auth,
                labels.as_deref(),
                None,
            )
            .map_err(|e| format!("failed to add provider: {e}"))?;

        // Register the appropriate driver based on provider type and driver field
        let driver: Arc<dyn kvmql_driver::traits::Driver> = if self.ctx.simulate {
            Arc::new(kvmql_driver::simulate::SimulationDriver::new(&ptype))
        } else { match (ptype.as_str(), driver.as_str()) {
            ("kvm", "firecracker") => {
                let socket = get_param(&s.params, "api_socket")
                    .or_else(|| host.as_ref().map(|h| format!("/run/firecracker/{h}.sock")))
                    .unwrap_or_else(|| "/run/firecracker.sock".into());
                Arc::new(kvmql_driver::firecracker::FirecrackerDriver::new(&socket))
            }
            ("aws", _) => {
                let region_str = region.as_deref().unwrap_or("us-east-1");
                Arc::new(kvmql_driver::aws::AwsEc2Driver::new(region_str))
            }
            ("azure", _) => {
                Arc::new(kvmql_driver::azure::AzureVmDriver::new(&id))
            }
            ("gcp", _) => {
                Arc::new(kvmql_driver::gcp::GcpComputeDriver::new(&id))
            }
            _ => {
                // Fallback to mock for unknown types
                Arc::new(kvmql_driver::mock::MockDriver::new())
            }
        } };
        self.ctx.register_driver(id.clone(), driver);

        let row = self
            .ctx
            .registry
            .get_provider(&id)
            .map_err(|e| format!("provider inserted but read-back failed: {e}"))?;
        let val = serde_json::json!({
            "id": row.id,
            "type": row.provider_type,
            "driver": row.driver,
            "status": row.status,
            "enabled": row.enabled,
            "host": row.host,
            "region": row.region,
            "auth_ref": row.auth_ref,
            "labels": row.labels,
        });
        Ok(StmtOutcome::ok_val(val))
    }

    fn exec_remove_provider(&self, s: &RemoveProviderStmt) -> Result<StmtOutcome, String> {
        self.ctx
            .registry
            .delete_provider(&s.name)
            .map_err(|e| format!("failed to remove provider: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    fn exec_alter_provider(&self, s: &AlterProviderStmt) -> Result<StmtOutcome, String> {
        // Only supports SET status = '...' for now
        for item in &s.set_items {
            if item.key == "status" {
                if let Value::String(ref v) = item.value {
                    self.ctx
                        .registry
                        .update_provider_status(&s.name, v)
                        .map_err(|e| format!("failed to alter provider: {e}"))?;
                }
            }
        }
        Ok(StmtOutcome::ok_empty())
    }

    // =======================================================================
    // CREATE MICROVM
    // =======================================================================

    async fn exec_create_microvm(
        &self,
        s: &CreateMicrovmStmt,
    ) -> Result<StmtOutcome, String> {
        // Determine target provider
        let provider_id = self.resolve_provider(&s.on)?;
        let driver = self
            .ctx
            .get_driver(&provider_id)
            .ok_or_else(|| format!("no driver registered for provider '{provider_id}'"))?
            .clone();

        let id = get_param(&s.params, "id");

        // IF NOT EXISTS: skip silently if microvm already exists
        if s.if_not_exists {
            if let Some(ref vm_id) = id {
                if let Ok(_existing) = self.ctx.registry.get_microvm(vm_id) {
                    let val = serde_json::json!({
                        "id": vm_id,
                        "status": "already_exists",
                        "skipped": true,
                    });
                    let mut outcome = StmtOutcome::ok_val(val);
                    outcome.notifications.push(Notification {
                        level: "INFO".into(),
                        code: "VM_EXISTS".into(),
                        provider_id: None,
                        message: format!("Microvm '{}' already exists -- skipped", vm_id),
                    });
                    return Ok(outcome);
                }
            }
        }

        let tenant = get_param(&s.params, "tenant").unwrap_or_else(|| "default".into());
        let vcpus = get_param_i64(&s.params, "vcpus").unwrap_or(1) as i32;
        let memory_mb = get_param_i64(&s.params, "memory_mb").unwrap_or(512) as i32;
        let image_id = get_param(&s.params, "image").unwrap_or_else(|| "default".into());
        let hostname = get_param(&s.params, "hostname");
        let network = get_param(&s.params, "network");
        let metadata = get_param(&s.params, "metadata")
            .and_then(|s| serde_json::from_str(&s).ok());
        let labels = get_param(&s.params, "labels")
            .and_then(|s| serde_json::from_str(&s).ok());

        // ── SSH / access params ──────────────────────────────────────
        let mut notifications: Vec<Notification> = Vec::new();

        // Resolve ssh_key through credential backend
        let ssh_key_ref = get_param(&s.params, "ssh_key");
        let ssh_key = if let Some(ref key_ref) = ssh_key_ref {
            if key_ref == "generate" {
                Some("generate".to_string())
            } else {
                match kvmql_auth::resolver::CredentialResolver::resolve(key_ref) {
                    Ok(resolved) => Some(resolved),
                    Err(_e) => {
                        // If it's not a credential reference, treat as literal value
                        // (e.g., direct public key content)
                        if key_ref.contains("ssh-rsa")
                            || key_ref.contains("ssh-ed25519")
                            || key_ref.contains("ecdsa-")
                        {
                            Some(key_ref.clone())
                        } else {
                            return Err(format!(
                                "failed to resolve ssh_key '{}': {}", key_ref, _e
                            ));
                        }
                    }
                }
            }
        } else {
            None
        };

        // Resolve cloud_init
        let cloud_init_ref = get_param(&s.params, "cloud_init");
        let cloud_init = if let Some(ref init_ref) = cloud_init_ref {
            match kvmql_auth::resolver::CredentialResolver::resolve(init_ref) {
                Ok(resolved) => Some(resolved),
                Err(_) => {
                    // If not a credential reference, treat as literal content
                    Some(init_ref.clone())
                }
            }
        } else {
            None
        };

        // Resolve password (discouraged)
        let password_ref = get_param(&s.params, "password");
        let password = if let Some(ref pass_ref) = password_ref {
            match kvmql_auth::resolver::CredentialResolver::resolve(pass_ref) {
                Ok(resolved) => Some(resolved),
                Err(_) => {
                    // Treat as literal password value
                    Some(pass_ref.clone())
                }
            }
        } else {
            None
        };

        if password.is_some() {
            notifications.push(Notification {
                level: "WARN".into(),
                code: "SEC_001".into(),
                provider_id: None,
                message: "Password authentication is discouraged. Use ssh_key for secure access."
                    .into(),
            });
        }

        let admin_user = get_param(&s.params, "admin_user");

        let params = CreateParams {
            id,
            tenant: tenant.clone(),
            vcpus,
            memory_mb,
            image_id: image_id.clone(),
            hostname: hostname.clone(),
            network,
            metadata,
            labels,
            ssh_key,
            ssh_key_ref: ssh_key_ref.clone(),
            admin_user,
            cloud_init,
            cloud_init_ref: cloud_init_ref.clone(),
            password,
        };

        let vm = driver
            .create(params)
            .await
            .map_err(|e| format!("driver create failed: {e}"))?;

        // Only reference image_id in registry if the image actually exists
        let registry_image_id = match self.ctx.registry.get_image(&image_id) {
            Ok(_) => Some(image_id.as_str()),
            Err(_) => None,
        };

        // Write to registry
        self.ctx.registry.insert_microvm(
            &vm.id,
            &provider_id,
            &tenant,
            &vm.status,
            registry_image_id,
            Some(vcpus as i64),
            Some(memory_mb as i64),
            hostname.as_deref(),
            None,
            None,
        ).map_err(|e| format!("registry insert_microvm failed: {e}"))?;

        let val = serde_json::to_value(&vm)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome {
            result: Some(val),
            rows_affected: 1,
            notifications,
        })
    }

    // =======================================================================
    // DESTROY MICROVM / VOLUME
    // =======================================================================

    async fn exec_destroy(&self, s: &DestroyStmt) -> Result<StmtOutcome, String> {
        match s.target {
            DestroyTarget::Microvm => {
                // Look up VM in registry to find its provider
                let row = self
                    .ctx
                    .registry
                    .get_microvm(&s.id)
                    .map_err(|e| format!("microvm lookup failed: {e}"))?;
                let driver = self
                    .ctx
                    .get_driver(&row.provider_id)
                    .ok_or_else(|| {
                        format!("no driver for provider '{}'", row.provider_id)
                    })?
                    .clone();
                driver
                    .destroy(&s.id, s.force)
                    .await
                    .map_err(|e| format!("driver destroy failed: {e}"))?;
                self.ctx
                    .registry
                    .delete_microvm(&s.id)
                    .map_err(|e| format!("registry delete failed: {e}"))?;
                Ok(StmtOutcome::ok_empty())
            }
            DestroyTarget::Volume => {
                let row = self
                    .ctx
                    .registry
                    .get_volume(&s.id)
                    .map_err(|e| format!("volume lookup failed: {e}"))?;
                let driver = self
                    .ctx
                    .get_driver(&row.provider_id)
                    .ok_or_else(|| {
                        format!("no driver for provider '{}'", row.provider_id)
                    })?
                    .clone();
                driver
                    .destroy_volume(&s.id, s.force)
                    .await
                    .map_err(|e| format!("driver destroy_volume failed: {e}"))?;
                self.ctx
                    .registry
                    .delete_volume(&s.id)
                    .map_err(|e| format!("registry delete failed: {e}"))?;
                Ok(StmtOutcome::ok_empty())
            }
        }
    }

    // =======================================================================
    // PAUSE / RESUME
    // =======================================================================

    async fn exec_pause(&self, s: &PauseStmt) -> Result<StmtOutcome, String> {
        let row = self
            .ctx
            .registry
            .get_microvm(&s.id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
            .clone();
        driver
            .pause(&s.id)
            .await
            .map_err(|e| format!("driver pause failed: {e}"))?;
        self.ctx
            .registry
            .update_microvm_status(&s.id, "paused")
            .map_err(|e| format!("registry update failed: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    async fn exec_resume(&self, s: &ResumeStmt) -> Result<StmtOutcome, String> {
        let row = self
            .ctx
            .registry
            .get_microvm(&s.id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
            .clone();
        driver
            .resume(&s.id)
            .await
            .map_err(|e| format!("driver resume failed: {e}"))?;
        self.ctx
            .registry
            .update_microvm_status(&s.id, "running")
            .map_err(|e| format!("registry update failed: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    // =======================================================================
    // SNAPSHOT / RESTORE
    // =======================================================================

    async fn exec_snapshot(&self, s: &SnapshotStmt) -> Result<StmtOutcome, String> {
        let row = self
            .ctx
            .registry
            .get_microvm(&s.id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
            .clone();
        let snap = driver
            .snapshot(&s.id, &s.destination, s.tag.as_deref())
            .await
            .map_err(|e| format!("driver snapshot failed: {e}"))?;
        let val = serde_json::to_value(&snap)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    async fn exec_restore(&self, s: &RestoreStmt) -> Result<StmtOutcome, String> {
        // Find any driver (use first available or the one that holds a snapshot)
        let (provider_id, driver) = self.any_driver()?;
        let vm = driver
            .restore(&s.id, &s.source)
            .await
            .map_err(|e| format!("driver restore failed: {e}"))?;

        // Only reference image_id in registry if the image actually exists
        let registry_image_id = vm.image_id.as_deref().and_then(|iid| {
            self.ctx.registry.get_image(iid).ok().map(|_| iid)
        });

        self.ctx.registry.insert_microvm(
            &vm.id,
            &provider_id,
            &vm.tenant,
            &vm.status,
            registry_image_id,
            vm.vcpus.map(|v| v as i64),
            vm.memory_mb.map(|v| v as i64),
            vm.hostname.as_deref(),
            None,
            None,
        ).map_err(|e| format!("registry insert_microvm failed: {e}"))?;

        let val = serde_json::to_value(&vm)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // CREATE VOLUME
    // =======================================================================

    async fn exec_create_volume(
        &self,
        s: &CreateVolumeStmt,
    ) -> Result<StmtOutcome, String> {
        let provider_id = self.resolve_provider(&s.on)?;
        let driver = self
            .ctx
            .get_driver(&provider_id)
            .ok_or_else(|| format!("no driver for provider '{provider_id}'"))?
            .clone();

        let id = get_param(&s.params, "id");

        // IF NOT EXISTS: skip silently if volume already exists
        if s.if_not_exists {
            if let Some(ref vol_id) = id {
                if let Ok(_existing) = self.ctx.registry.get_volume(vol_id) {
                    let val = serde_json::json!({
                        "id": vol_id,
                        "status": "already_exists",
                        "skipped": true,
                    });
                    let mut outcome = StmtOutcome::ok_val(val);
                    outcome.notifications.push(Notification {
                        level: "INFO".into(),
                        code: "VOL_EXISTS".into(),
                        provider_id: None,
                        message: format!("Volume '{}' already exists -- skipped", vol_id),
                    });
                    return Ok(outcome);
                }
            }
        }

        let size_gb = get_param_i64(&s.params, "size_gb").unwrap_or(10);
        let vol_type =
            get_param(&s.params, "type").unwrap_or_else(|| "virtio-blk".into());
        let encrypted = get_param_bool(&s.params, "encrypted").unwrap_or(false);
        let iops = get_param_i64(&s.params, "iops").map(|v| v as i32);
        let labels = get_param(&s.params, "labels")
            .and_then(|s| serde_json::from_str(&s).ok());

        let params = VolumeParams {
            id,
            size_gb,
            vol_type: vol_type.clone(),
            encrypted,
            iops,
            labels,
        };

        let vol = driver
            .create_volume(params)
            .await
            .map_err(|e| format!("driver create_volume failed: {e}"))?;

        self.ctx.registry.insert_volume(
            &vol.id,
            &provider_id,
            &vol_type,
            size_gb,
            &vol.status,
            iops.map(|v| v as i64),
            encrypted,
            None,
        ).map_err(|e| format!("registry insert_volume failed: {e}"))?;

        let val = serde_json::to_value(&vol)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // ATTACH / DETACH VOLUME
    // =======================================================================

    async fn exec_attach(&self, s: &AttachStmt) -> Result<StmtOutcome, String> {
        // Look up the VM to find its provider
        let vm_row = self
            .ctx
            .registry
            .get_microvm(&s.microvm_id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&vm_row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", vm_row.provider_id))?
            .clone();

        let device = s.device_name.as_deref();
        driver
            .attach_volume(&s.volume_id, &s.microvm_id, device)
            .await
            .map_err(|e| format!("driver attach_volume failed: {e}"))?;

        let device_str = device.unwrap_or("/dev/vdb");
        self.ctx
            .registry
            .attach_volume(&s.volume_id, &s.microvm_id, device_str)
            .map_err(|e| format!("registry attach failed: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    async fn exec_detach(&self, s: &DetachStmt) -> Result<StmtOutcome, String> {
        let vm_row = self
            .ctx
            .registry
            .get_microvm(&s.microvm_id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&vm_row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", vm_row.provider_id))?
            .clone();
        driver
            .detach_volume(&s.volume_id, &s.microvm_id)
            .await
            .map_err(|e| format!("driver detach_volume failed: {e}"))?;
        self.ctx
            .registry
            .detach_volume(&s.volume_id)
            .map_err(|e| format!("registry detach failed: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    // =======================================================================
    // RESIZE VOLUME
    // =======================================================================

    async fn exec_resize(&self, s: &ResizeStmt) -> Result<StmtOutcome, String> {
        let vol_row = self
            .ctx
            .registry
            .get_volume(&s.volume_id)
            .map_err(|e| format!("volume lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&vol_row.provider_id)
            .ok_or_else(|| {
                format!("no driver for provider '{}'", vol_row.provider_id)
            })?
            .clone();
        let vol = driver
            .resize_volume(&s.volume_id, s.new_size_gb)
            .await
            .map_err(|e| format!("driver resize_volume failed: {e}"))?;
        let val = serde_json::to_value(&vol)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // ALTER MICROVM / VOLUME
    // =======================================================================

    async fn exec_alter_microvm(
        &self,
        s: &AlterMicrovmStmt,
    ) -> Result<StmtOutcome, String> {
        let row = self
            .ctx
            .registry
            .get_microvm(&s.id)
            .map_err(|e| format!("microvm lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
            .clone();

        let mut json_params = serde_json::Map::new();
        for item in &s.set_items {
            let v = match &item.value {
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::String(s) => serde_json::Value::String(s.clone()),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                Value::Float(f) => serde_json::json!(f),
                _ => continue,
            };
            json_params.insert(item.key.clone(), v);
        }

        let vm = driver
            .alter(&s.id, serde_json::Value::Object(json_params))
            .await
            .map_err(|e| format!("driver alter failed: {e}"))?;
        let val = serde_json::to_value(&vm)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    async fn exec_alter_volume(
        &self,
        s: &AlterVolumeStmt,
    ) -> Result<StmtOutcome, String> {
        let vol_row = self
            .ctx
            .registry
            .get_volume(&s.id)
            .map_err(|e| format!("volume '{}' not found: {e}", s.id))?;

        let mut notifications = Vec::new();

        for item in &s.set_items {
            match item.key.as_str() {
                "size_gb" => {
                    if let Value::Integer(new_size) = &item.value {
                        let driver = self
                            .ctx
                            .get_driver(&vol_row.provider_id)
                            .ok_or_else(|| {
                                format!("no driver for provider '{}'", vol_row.provider_id)
                            })?
                            .clone();
                        driver
                            .resize_volume(&s.id, *new_size)
                            .await
                            .map_err(|e| format!("driver resize_volume failed: {e}"))?;
                    }
                }
                "labels" | "iops" | "encrypted" => {
                    // Extract the string representation of the value
                    let val_str = match &item.value {
                        Value::String(s) => s.clone(),
                        Value::Integer(n) => n.to_string(),
                        Value::Boolean(b) => if *b { "1".into() } else { "0".into() },
                        other => format!("{other:?}"),
                    };
                    self.ctx
                        .registry
                        .update_volume_field(&s.id, &item.key, &val_str)
                        .map_err(|e| format!("failed to update volume field '{}': {e}", item.key))?;
                    notifications.push(Notification {
                        level: "INFO".into(),
                        code: "CAP_001".into(),
                        provider_id: Some(vol_row.provider_id.clone()),
                        message: format!(
                            "'{}' updated in registry (provider update not supported for this field)",
                            item.key
                        ),
                    });
                }
                other => {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "CAP_002".into(),
                        provider_id: None,
                        message: format!("unknown volume field '{}' -- ignored", other),
                    });
                }
            }
        }

        // Re-read the volume to return the updated state
        let updated = self
            .ctx
            .registry
            .get_volume(&s.id)
            .map_err(|e| format!("volume lookup after update failed: {e}"))?;
        let val = serde_json::json!({
            "id": updated.id,
            "provider_id": updated.provider_id,
            "microvm_id": updated.microvm_id,
            "type": updated.volume_type,
            "size_gb": updated.size_gb,
            "status": updated.status,
            "device_name": updated.device_name,
            "iops": updated.iops,
            "encrypted": updated.encrypted,
            "created_at": updated.created_at,
            "labels": updated.labels,
        });

        Ok(StmtOutcome {
            result: Some(val),
            rows_affected: 1,
            notifications,
        })
    }

    // =======================================================================
    // IMPORT / REMOVE IMAGE
    // =======================================================================

    async fn exec_import_image(
        &self,
        s: &ImportImageStmt,
    ) -> Result<StmtOutcome, String> {
        let (provider_id, driver) = self.any_driver()?;

        let id = get_param(&s.params, "id")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let name = get_param(&s.params, "name")
            .unwrap_or_else(|| "unnamed".into());
        let os = get_param(&s.params, "os").unwrap_or_else(|| "linux".into());
        let distro = get_param(&s.params, "distro")
            .unwrap_or_else(|| "unknown".into());
        let version = get_param(&s.params, "version")
            .unwrap_or_else(|| "latest".into());
        let arch = get_param(&s.params, "arch")
            .unwrap_or_else(|| "x86_64".into());
        let image_type = get_param(&s.params, "type")
            .unwrap_or_else(|| "rootfs".into());
        let source = get_param(&s.params, "source")
            .unwrap_or_else(|| "local".into());
        let kernel = get_param(&s.params, "kernel");
        let rootfs = get_param(&s.params, "rootfs");
        let checksum = get_param(&s.params, "checksum");

        let params = ImageParams {
            id: id.clone(),
            name: name.clone(),
            os: os.clone(),
            distro: distro.clone(),
            version: version.clone(),
            arch: arch.clone(),
            image_type: image_type.clone(),
            source: source.clone(),
            kernel: kernel.clone(),
            rootfs: rootfs.clone(),
            checksum,
            labels: None,
        };

        let img = driver
            .import_image(params)
            .await
            .map_err(|e| format!("driver import_image failed: {e}"))?;

        // Best-effort registry insert — schema CHECK constraints may reject
        // non-standard values (e.g. image type must be in the enum).
        let _ = self.ctx.registry.insert_image(
            &id,
            &name,
            &os,
            &distro,
            &version,
            &arch,
            &image_type,
            Some(provider_id.as_str()),
            kernel.as_deref(),
            rootfs.as_deref(),
            None,   // disk_path
            None,   // cloud_ref
            &source,
            None,   // checksum_sha256
            None,   // size_mb
            "available",
            None,   // labels
        );

        let val = serde_json::to_value(&img)
            .map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    async fn exec_remove_image(
        &self,
        s: &RemoveImageStmt,
    ) -> Result<StmtOutcome, String> {
        let (_provider_id, driver) = self.any_driver()?;
        driver
            .remove_image(&s.image_id, s.force)
            .await
            .map_err(|e| format!("driver remove_image failed: {e}"))?;
        self.ctx
            .registry
            .delete_image(&s.image_id)
            .map_err(|e| format!("registry delete failed: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    // =======================================================================
    // SELECT
    // =======================================================================

    async fn exec_select(&self, s: &SelectStmt) -> Result<StmtOutcome, String> {
        // Table-valued function sources (dns_lookup, tcp_probe, ...) live in
        // a separate code path — no registry involvement.
        let noun = match &s.from {
            SelectSource::Noun(n) => n.clone(),
            SelectSource::Function(fc) => {
                return self.exec_select_function(s, fc).await;
            }
        };
        let rows: Vec<serde_json::Value> = match noun {
            Noun::Microvms => {
                let list = self
                    .ctx
                    .registry
                    .list_microvms()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "provider_id": r.provider_id,
                            "tenant": r.tenant,
                            "status": r.status,
                            "image_id": r.image_id,
                            "vcpus": r.vcpus,
                            "memory_mb": r.memory_mb,
                            "hostname": r.hostname,
                            "labels": r.labels,
                            "created_at": r.created_at,
                        })
                    })
                    .collect()
            }
            Noun::Volumes => {
                let list = self
                    .ctx
                    .registry
                    .list_volumes()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "provider_id": r.provider_id,
                            "microvm_id": r.microvm_id,
                            "type": r.volume_type,
                            "size_gb": r.size_gb,
                            "status": r.status,
                            "device_name": r.device_name,
                            "iops": r.iops,
                            "encrypted": r.encrypted,
                            "created_at": r.created_at,
                            "labels": r.labels,
                        })
                    })
                    .collect()
            }
            Noun::Images => {
                let list = self
                    .ctx
                    .registry
                    .list_images()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "name": r.name,
                            "os": r.os,
                            "distro": r.distro,
                            "version": r.version,
                            "arch": r.arch,
                            "type": r.image_type,
                            "status": r.status,
                            "labels": r.labels,
                        })
                    })
                    .collect()
            }
            Noun::Providers => {
                let list = self
                    .ctx
                    .registry
                    .list_providers()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "type": r.provider_type,
                            "driver": r.driver,
                            "status": r.status,
                            "enabled": r.enabled,
                            "host": r.host,
                            "region": r.region,
                            "auth_ref": r.auth_ref,
                            "labels": r.labels,
                        })
                    })
                    .collect()
            }
            Noun::Resources => {
                let list = self
                    .ctx
                    .registry
                    .list_resources()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "resource_type": r.resource_type,
                            "provider_id": r.provider_id,
                            "name": r.name,
                            "status": r.status,
                            "config": r.config,
                            "outputs": r.outputs,
                            "created_at": r.created_at,
                            "updated_at": r.updated_at,
                            "labels": r.labels,
                        })
                    })
                    .collect()
            }
            Noun::AuditLog => {
                let list = self
                    .ctx
                    .registry
                    .list_audit_log(None)
                    .map_err(|e| format!("failed to query audit_log: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "event_time": r.event_time,
                            "principal": r.principal,
                            "action": r.action,
                            "target_type": r.target_type,
                            "target_id": r.target_id,
                            "outcome": r.outcome,
                            "reason": r.reason,
                            "detail": r.detail,
                        })
                    })
                    .collect()
            }
            Noun::QueryHistory => {
                let list = self
                    .ctx
                    .registry
                    .list_query_history(None)
                    .map_err(|e| format!("failed to query query_history: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "executed_at": r.executed_at,
                            "principal": r.principal,
                            "statement": r.statement,
                            "normalized_stmt": r.normalized_stmt,
                            "verb": r.verb,
                            "targets": r.targets,
                            "duration_ms": r.duration_ms,
                            "status": r.status,
                            "rows_affected": r.rows_affected,
                        })
                    })
                    .collect()
            }
            Noun::Principals => {
                let list = self
                    .ctx
                    .registry
                    .list_principals()
                    .map_err(|e| format!("failed to query principals: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "type": r.principal_type,
                            "auth_ref": r.auth_ref,
                            "enabled": r.enabled,
                            "created_at": r.created_at,
                        })
                    })
                    .collect()
            }
            Noun::Grants => {
                let list = self
                    .ctx
                    .registry
                    .list_all_grants()
                    .map_err(|e| format!("failed to query grants: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "principal_id": r.principal_id,
                            "verbs": r.verbs,
                            "scope_type": r.scope_type,
                            "scope_id": r.scope_id,
                            "conditions": r.conditions,
                            "granted_at": r.granted_at,
                            "granted_by": r.granted_by,
                        })
                    })
                    .collect()
            }
            Noun::Clusters => {
                let list = self
                    .ctx
                    .registry
                    .list_clusters()
                    .map_err(|e| format!("failed to query clusters: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "name": r.name,
                            "created_at": r.created_at,
                        })
                    })
                    .collect()
            }
            Noun::ClusterMembers => {
                let list = self
                    .ctx
                    .registry
                    .list_cluster_members()
                    .map_err(|e| format!("failed to query cluster_members: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "cluster_id": r.cluster_id,
                            "provider_id": r.provider_id,
                        })
                    })
                    .collect()
            }
            Noun::Snapshots => {
                let list = self
                    .ctx
                    .registry
                    .list_snapshots_table()
                    .map_err(|e| format!("failed to query snapshots: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "microvm_id": r.microvm_id,
                            "provider_id": r.provider_id,
                            "destination": r.destination,
                            "tag": r.tag,
                            "size_mb": r.size_mb,
                            "taken_at": r.taken_at,
                        })
                    })
                    .collect()
            }
            Noun::Capabilities => {
                let mut all_rows = Vec::new();
                let drivers = self.ctx.drivers.read().unwrap();
                for (pid, driver) in drivers.iter() {
                    let caps = driver.capabilities();
                    let mut entries: Vec<_> = caps.capabilities.iter().collect();
                    entries.sort_by_key(|(k, _)| format!("{:?}", k));
                    for (cap, entry) in entries {
                        all_rows.push(serde_json::json!({
                            "provider_id": pid,
                            "capability": format!("{:?}", cap),
                            "supported": entry.supported,
                            "notes": entry.notes,
                        }));
                    }
                }
                all_rows
            }
            Noun::Events => {
                let list = self
                    .ctx
                    .registry
                    .list_events()
                    .map_err(|e| format!("failed to query events: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "event_time": r.event_time,
                            "event_type": r.event_type,
                            "microvm_id": r.microvm_id,
                            "volume_id": r.volume_id,
                            "image_id": r.image_id,
                            "provider_id": r.provider_id,
                            "principal": r.principal,
                            "detail": r.detail,
                        })
                    })
                    .collect()
            }
            Noun::Metrics => {
                let list = self
                    .ctx
                    .registry
                    .list_metrics()
                    .map_err(|e| format!("failed to query metrics: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "microvm_id": r.microvm_id,
                            "sampled_at": r.sampled_at,
                            "cpu_pct": r.cpu_pct,
                            "mem_used_mb": r.mem_used_mb,
                            "net_rx_kbps": r.net_rx_kbps,
                            "net_tx_kbps": r.net_tx_kbps,
                        })
                    })
                    .collect()
            }
            Noun::Plans => {
                let list = self
                    .ctx
                    .registry
                    .list_plans(None)
                    .map_err(|e| format!("failed to query plans: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "name": r.name,
                            "checksum": r.checksum,
                            "status": r.status,
                            "created_at": r.created_at,
                            "created_by": r.created_by,
                            "approved_at": r.approved_at,
                            "approved_by": r.approved_by,
                            "applied_at": r.applied_at,
                            "applied_by": r.applied_by,
                            "error": r.error,
                            "environment": r.environment,
                        })
                    })
                    .collect()
            }
            Noun::AppliedFiles => {
                let list = self
                    .ctx
                    .registry
                    .list_applied_files()
                    .map_err(|e| format!("failed to query applied_files: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "file_path": r.file_path,
                            "file_hash": r.file_hash,
                            "statements_count": r.statements_count,
                            "applied_at": r.applied_at,
                            "applied_by": r.applied_by,
                            "environment": r.environment,
                            "status": r.status,
                        })
                    })
                    .collect()
            }
            // ── Kubernetes live-query nouns ───────────────────────
            // These don't hit the registry — they shell out to `kubectl
            // get` against the live cluster.  WHERE / LIMIT are still
            // applied client-side via the existing eval_predicate path.
            Noun::K8sPods
            | Noun::K8sDeployments
            | Noun::K8sServices
            | Noun::K8sIngresses
            | Noun::K8sConfigmaps
            | Noun::K8sSecrets
            | Noun::K8sNamespaces
            | Noun::K8sNodes => {
                // In simulation mode we never touch a real cluster — just
                // return an empty result set so demos and tests don't
                // require kubectl/a live cluster.
                if self.ctx.simulate {
                    Vec::new()
                } else {
                    // Resolve provider: explicit ON PROVIDER 'id' wins;
                    // else we look for any registered provider of type
                    // "kubernetes" and use the first one; else "default".
                    let provider_id = if let Some(spec) = s.on.as_ref() {
                        match &spec.target {
                            TargetKind::Provider(id) => id.clone(),
                            _ => "default".to_string(),
                        }
                    } else {
                        self.ctx
                            .registry
                            .list_providers()
                            .ok()
                            .and_then(|list| {
                                list.into_iter()
                                    .find(|p| p.provider_type == "kubernetes")
                                    .map(|p| p.id)
                            })
                            .unwrap_or_else(|| "default".to_string())
                    };

                    let engine = self.get_k8s_query_engine(&provider_id);
                    let noun_str = format!("{}", noun);
                    // Future: extract a `namespace = '...'` filter from the
                    // WHERE clause and push it down via -n; for now we list
                    // across all namespaces and filter client-side.
                    let namespace: Option<&str> = None;

                    engine
                        .query(&noun_str, namespace)
                        .map_err(|e| format!("k8s query failed: {e}"))?
                }
            }
        };

        // Apply WHERE filtering
        let rows = if let Some(ref pred) = s.where_clause {
            rows.into_iter()
                .filter(|row| eval_predicate(pred, row))
                .collect()
        } else {
            rows
        };

        // Apply LIMIT
        let rows = if let Some(limit) = s.limit {
            rows.into_iter().take(limit as usize).collect()
        } else {
            rows
        };

        let count = rows.len() as i64;
        let val = serde_json::Value::Array(rows);
        Ok(StmtOutcome::ok_rows(val, count))
    }

    // =======================================================================
    // SHOW
    // =======================================================================

    fn exec_show(&self, s: &ShowStmt) -> Result<StmtOutcome, String> {
        match &s.target {
            ShowTarget::Providers => {
                let list = self
                    .ctx
                    .registry
                    .list_providers()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                let vals: Vec<serde_json::Value> = list
                    .into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "type": r.provider_type,
                            "driver": r.driver,
                            "status": r.status,
                            "enabled": r.enabled,
                            "host": r.host,
                            "region": r.region,
                        })
                    })
                    .collect();
                let n = vals.len() as i64;
                Ok(StmtOutcome::ok_rows(serde_json::Value::Array(vals), n))
            }
            ShowTarget::Images => {
                let list = self
                    .ctx
                    .registry
                    .list_images()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                let vals: Vec<serde_json::Value> = list
                    .into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "name": r.name,
                            "os": r.os,
                            "status": r.status,
                        })
                    })
                    .collect();
                let n = vals.len() as i64;
                Ok(StmtOutcome::ok_rows(serde_json::Value::Array(vals), n))
            }
            ShowTarget::Version => {
                let val = serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "engine": "kvmql-engine",
                });
                Ok(StmtOutcome::ok_val(val))
            }
            ShowTarget::Capabilities { for_provider } => {
                if let Some(pid) = for_provider {
                    if let Some(driver) = self.ctx.get_driver(pid) {
                        let caps = driver.capabilities();
                        let mut rows = Vec::new();
                        let mut entries: Vec<_> = caps.capabilities.iter().collect();
                        entries.sort_by_key(|(k, _)| format!("{:?}", k));
                        for (cap, entry) in entries {
                            rows.push(serde_json::json!({
                                "provider_id": pid,
                                "capability": format!("{:?}", cap),
                                "supported": entry.supported,
                                "notes": entry.notes,
                            }));
                        }
                        let n = rows.len() as i64;
                        return Ok(StmtOutcome::ok_rows(serde_json::Value::Array(rows), n));
                    }
                    return Err(with_remediation(
                        "PROVIDER_NOT_FOUND",
                        &format!("provider '{}' not found", pid),
                        &ErrorContext {
                            provider_id: Some(pid.clone()),
                            ..Default::default()
                        },
                    ));
                }
                // No provider specified — show all capabilities across all drivers
                let mut rows = Vec::new();
                let drivers = self.ctx.drivers.read().unwrap();
                for (pid, driver) in drivers.iter() {
                    let caps = driver.capabilities();
                    let mut entries: Vec<_> = caps.capabilities.iter().collect();
                    entries.sort_by_key(|(k, _)| format!("{:?}", k));
                    for (cap, entry) in entries {
                        rows.push(serde_json::json!({
                            "provider_id": pid,
                            "capability": format!("{:?}", cap),
                            "supported": entry.supported,
                            "notes": entry.notes,
                        }));
                    }
                }
                let n = rows.len() as i64;
                Ok(StmtOutcome::ok_rows(serde_json::Value::Array(rows), n))
            }
            ShowTarget::Clusters => {
                let list = self
                    .ctx
                    .registry
                    .list_clusters()
                    .map_err(|e| format!("registry query failed: {e}"))?;
                let vals: Vec<serde_json::Value> = list
                    .into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "name": r.name,
                            "created_at": r.created_at,
                        })
                    })
                    .collect();
                let n = vals.len() as i64;
                Ok(StmtOutcome::ok_rows(serde_json::Value::Array(vals), n))
            }
            ShowTarget::Grants { for_principal } => {
                if let Some(pid) = for_principal {
                    match self.ctx.registry.get_grants_for_principal(pid) {
                        Ok(grants) => {
                            let vals: Vec<serde_json::Value> = grants
                                .iter()
                                .map(|g| {
                                    serde_json::json!({
                                        "id": g.id,
                                        "principal_id": g.principal_id,
                                        "verbs": g.verbs,
                                        "scope_type": g.scope_type,
                                        "scope_id": g.scope_id,
                                        "granted_at": g.granted_at,
                                    })
                                })
                                .collect();
                            let n = vals.len() as i64;
                            Ok(StmtOutcome::ok_rows(serde_json::Value::Array(vals), n))
                        }
                        Err(e) => Err(format!("failed to list grants: {e}")),
                    }
                } else {
                    match self.ctx.registry.list_all_grants() {
                        Ok(grants) => {
                            let vals: Vec<serde_json::Value> = grants
                                .iter()
                                .map(|g| {
                                    serde_json::json!({
                                        "id": g.id,
                                        "principal_id": g.principal_id,
                                        "verbs": g.verbs,
                                        "scope_type": g.scope_type,
                                        "scope_id": g.scope_id,
                                        "granted_at": g.granted_at,
                                    })
                                })
                                .collect();
                            let n = vals.len() as i64;
                            Ok(StmtOutcome::ok_rows(serde_json::Value::Array(vals), n))
                        }
                        Err(e) => Err(format!("failed to list grants: {e}")),
                    }
                }
            }
        }
    }

    // =======================================================================
    // SET
    // =======================================================================

    fn exec_set(&self, s: &SetStmt) -> Result<StmtOutcome, String> {
        // Handle @variable assignment
        if s.key.starts_with('@') {
            let var_name = s.key[1..].to_string();
            let val = match &s.value {
                Value::String(sv) => sv.clone(),
                Value::Integer(n) => n.to_string(),
                Value::Float(fv) => fv.to_string(),
                Value::Boolean(b) => b.to_string(),
                _ => return Err("variable value must be a string, integer, float, or boolean".into()),
            };
            self.ctx.variables.write().unwrap().insert(var_name.clone(), val.clone());
            return Ok(StmtOutcome::ok_val(serde_json::json!({
                "variable": &s.key,
                "value": val
            })));
        }

        match s.key.as_str() {
            "execution_mode" => {
                if let Value::String(ref v) = s.value {
                    let _mode = match v.as_str() {
                        "strict" => ExecutionMode::Strict,
                        "permissive" => ExecutionMode::Permissive,
                        other => {
                            return Err(format!(
                                "unknown execution mode: '{other}'; expected 'strict' or 'permissive'"
                            ));
                        }
                    };
                    // NOTE: EngineContext is borrowed immutably, so we can't
                    // mutate it here. The SET is acknowledged but the context
                    // owner is responsible for applying it. We return success
                    // with an info notification.
                    let mut outcome = StmtOutcome::ok_val(serde_json::json!({
                        "execution_mode": v,
                    }));
                    outcome.notifications.push(Notification {
                        level: "INFO".into(),
                        code: "SET_001".into(),
                        provider_id: None,
                        message: format!("execution_mode acknowledged as '{v}'"),
                    });
                    return Ok(outcome);
                }
                Err("execution_mode value must be a string".into())
            }
            _ => Ok(StmtOutcome::not_implemented(&format!("SET {}", s.key))),
        }
    }

    // =======================================================================
    // CLUSTER management
    // =======================================================================

    fn exec_add_cluster(&self, s: &AddClusterStmt) -> Result<StmtOutcome, String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.ctx
            .registry
            .insert_cluster(&id, &s.name)
            .map_err(|e| format!("failed to add cluster: {e}"))?;

        for member in &s.members {
            let _ = self.ctx.registry.add_cluster_member(&id, member);
        }

        Ok(StmtOutcome::ok_val(serde_json::json!({
            "id": id,
            "name": s.name,
            "members": s.members,
        })))
    }

    fn exec_remove_cluster(&self, s: &RemoveClusterStmt) -> Result<StmtOutcome, String> {
        self.ctx
            .registry
            .delete_cluster(&s.name)
            .map_err(|e| format!("failed to remove cluster: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    fn exec_alter_cluster(&self, s: &AlterClusterStmt) -> Result<StmtOutcome, String> {
        match &s.action {
            ClusterAlterAction::AddMember(provider_id) => {
                self.ctx
                    .registry
                    .add_cluster_member(&s.name, provider_id)
                    .map_err(|e| format!("failed to add cluster member: {e}"))?;
            }
            ClusterAlterAction::RemoveMember(provider_id) => {
                self.ctx
                    .registry
                    .remove_cluster_member(&s.name, provider_id)
                    .map_err(|e| format!("failed to remove cluster member: {e}"))?;
            }
        }
        Ok(StmtOutcome::ok_empty())
    }

    // =======================================================================
    // ADD PRINCIPAL / GRANT / REVOKE
    // =======================================================================

    fn exec_add_principal(&self, s: &AddPrincipalStmt) -> Result<StmtOutcome, String> {
        let id = get_param(&s.params, "id").ok_or("ADD PRINCIPAL requires id")?;
        let ptype = get_param(&s.params, "type").unwrap_or_else(|| "user".into());
        let auth = get_param(&s.params, "auth").ok_or("ADD PRINCIPAL requires auth")?;

        self.ctx
            .registry
            .insert_principal(&id, &ptype, &auth, true)
            .map_err(|e| format!("failed to add principal: {e}"))?;

        let val = serde_json::json!({
            "id": id,
            "type": ptype,
            "auth_ref": auth,
            "enabled": true,
        });
        Ok(StmtOutcome::ok_val(val))
    }

    fn exec_grant(&self, s: &GrantStmt) -> Result<StmtOutcome, String> {
        // 1. Verify principal exists
        self.ctx
            .registry
            .get_principal(&s.principal)
            .map_err(|e| format!("principal '{}' not found: {e}", s.principal))?;

        // 2. Serialize verbs as JSON array
        let verb_strings: Vec<String> = s.verbs.iter().map(|v| format!("{v}")).collect();
        let verbs_json = serde_json::to_string(&verb_strings)
            .map_err(|e| format!("failed to serialize verbs: {e}"))?;

        // 3. Determine scope_type and scope_id from GrantScope
        let (scope_type, scope_id): (&str, Option<&str>) = match &s.scope {
            GrantScope::Cluster(id) => ("cluster", Some(id.as_str())),
            GrantScope::Provider(id) => ("provider", Some(id.as_str())),
            GrantScope::Microvms => ("global", None),
            GrantScope::Volumes => ("global", None),
            GrantScope::Images => ("global", None),
        };

        // 4. Serialize WHERE condition if present
        let conditions = s.where_clause.as_ref().map(|w| format!("{w}"));

        // 5. Insert grant via registry
        let grant_id = uuid::Uuid::new_v4().to_string();
        self.ctx
            .registry
            .insert_grant(
                &grant_id,
                &s.principal,
                &verbs_json,
                scope_type,
                scope_id,
                conditions.as_deref(),
                self.ctx.current_principal.as_deref(),
            )
            .map_err(|e| format!("failed to insert grant: {e}"))?;

        // 6. Return the created grant as JSON
        let val = serde_json::json!({
            "id": grant_id,
            "principal_id": s.principal,
            "verbs": verb_strings,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "conditions": conditions,
        });
        Ok(StmtOutcome::ok_val(val))
    }

    fn exec_revoke(&self, s: &RevokeStmt) -> Result<StmtOutcome, String> {
        // 1. Find grants for the principal
        let grants = self
            .ctx
            .registry
            .get_grants_for_principal(&s.principal)
            .map_err(|e| format!("failed to look up grants: {e}"))?;

        // 2. Build the set of verbs and scope to match
        let revoke_verbs: Vec<String> = s.verbs.iter().map(|v| format!("{v}")).collect();
        let (revoke_scope_type, revoke_scope_id): (&str, Option<&str>) = match &s.scope {
            GrantScope::Cluster(id) => ("cluster", Some(id.as_str())),
            GrantScope::Provider(id) => ("provider", Some(id.as_str())),
            GrantScope::Microvms => ("global", None),
            GrantScope::Volumes => ("global", None),
            GrantScope::Images => ("global", None),
        };

        // 3. Delete matching grants
        let mut revoked = 0i64;
        for grant in &grants {
            // Parse verbs from the grant
            let grant_verbs: Vec<String> =
                serde_json::from_str(&grant.verbs).unwrap_or_default();

            // Check if scope matches
            let scope_matches = grant.scope_type == revoke_scope_type
                && grant.scope_id.as_deref() == revoke_scope_id;

            if !scope_matches {
                continue;
            }

            // Check if the revoke verbs are a subset of (or equal to) the grant verbs
            let verb_overlap = revoke_verbs
                .iter()
                .any(|rv| grant_verbs.iter().any(|gv| gv.eq_ignore_ascii_case(rv)));

            if verb_overlap {
                let _ = self.ctx.registry.delete_grant(&grant.id);
                revoked += 1;
            }
        }

        let val = serde_json::json!({
            "revoked_count": revoked,
            "principal": s.principal,
        });
        Ok(StmtOutcome::ok_rows(val, revoked))
    }

    // =======================================================================
    // CREATE / ALTER / DESTROY RESOURCE
    // =======================================================================

    /// Resolve credential-like parameter values in a JSON object.
    ///
    /// Walks known credential keys (`ssh_key`, `cloud_init`, `password`, `auth`)
    /// and replaces any credential-scheme references with resolved values.
    /// Values that are not valid credential references are left unchanged.
    fn resolve_credential_params(&self, params: &serde_json::Value) -> serde_json::Value {
        let mut resolved = params.clone();
        if let Some(obj) = resolved.as_object_mut() {
            let keys_to_resolve = ["ssh_key", "cloud_init", "password", "auth"];
            for key in &keys_to_resolve {
                if let Some(val) = obj.get(*key).and_then(|v| v.as_str()).map(|s| s.to_string()) {
                    if val == "generate" {
                        continue;
                    }
                    if let Ok(resolved_val) =
                        kvmql_auth::resolver::CredentialResolver::resolve(&val)
                    {
                        obj.insert(key.to_string(), serde_json::Value::String(resolved_val));
                    }
                }
            }
        }
        resolved
    }

    fn exec_create_resource(
        &self,
        s: &CreateResourceStmt,
    ) -> Result<StmtOutcome, String> {
        if self.ctx.simulate {
            return self.exec_create_resource_simulated(s);
        }

        // Resolve variable references in params
        let params = self.resolve_params(&s.params);

        let id = get_param(&params, "id")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // IF NOT EXISTS: skip silently if resource already exists
        if s.if_not_exists {
            if let Ok(_existing) = self.ctx.registry.get_resource(&id) {
                let val = serde_json::json!({
                    "id": id,
                    "status": "already_exists",
                    "skipped": true,
                });
                let mut outcome = StmtOutcome::ok_val(val);
                outcome.notifications.push(Notification {
                    level: "INFO".into(),
                    code: "RES_EXISTS".into(),
                    provider_id: None,
                    message: format!("Resource '{}' '{}' already exists -- skipped", s.resource_type, id),
                });
                return Ok(outcome);
            }
        }
        let name = get_param(&params, "name");
        let labels = get_param(&params, "labels");

        // Resolve provider
        let provider_id = self.resolve_provider(&s.on)?;

        // Build config JSON from all params
        let config_value = {
            let mut map = serde_json::Map::new();
            for p in &params {
                let v = match &p.value {
                    Value::String(s) => serde_json::Value::String(s.clone()),
                    Value::Integer(n) => serde_json::Value::Number((*n).into()),
                    Value::Float(f) => serde_json::json!(f),
                    Value::Boolean(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::String(format!("{}", p.value)),
                };
                map.insert(p.key.clone(), v);
            }
            serde_json::Value::Object(map)
        };
        // Resolve credential references before passing to the provisioner
        let config_value = self.resolve_credential_params(&config_value);
        let config = config_value.to_string();

        // Attempt real provisioning via the appropriate cloud CLI.
        // Determine whether to use the AWS or Azure provisioner based on
        // the resource type or the provider type from the registry.
        let mut notifications: Vec<Notification> = Vec::new();
        let (status, outputs) = {
            let provider_type = self.ctx.registry.get_provider(&provider_id)
                .ok()
                .map(|p| p.provider_type.clone())
                .unwrap_or_default();

            let is_aws = provider_type == "aws"
                || matches!(
                    s.resource_type.as_str(),
                    "rds_postgres" | "vpc" | "aws_subnet" | "security_group" | "sg_rule"
                );

            let is_cloudflare = provider_type == "cloudflare"
                || matches!(
                    s.resource_type.as_str(),
                    "cf_zone" | "cf_dns_record" | "cf_firewall_rule" | "cf_page_rule"
                );

            let is_github = provider_type == "github"
                || s.resource_type.starts_with("gh_");

            let is_k8s = provider_type == "kubernetes"
                || s.resource_type.starts_with("k8s_");

            let is_ssh = provider_type == "ssh"
                || matches!(
                    s.resource_type.as_str(),
                    "file"
                        | "directory"
                        | "symlink"
                        | "systemd_service"
                        | "systemd_timer"
                        | "nginx_vhost"
                        | "nginx_proxy"
                        | "docker_container"
                        | "docker_volume"
                        | "docker_network"
                        | "docker_compose"
                        | "letsencrypt_cert"
                );

            if is_ssh {
                // Pre-resolve `content` if it's a credential/file reference
                // so the provisioner gets raw bytes.  We do this here (not
                // in resolve_credential_params) because the `content` key
                // is file-provider-specific and the generic credential
                // walker skips unknown keys on purpose.
                let mut cfg_with_content = config_value.clone();
                if s.resource_type == "file" {
                    if let Some(raw_content) = cfg_with_content
                        .get("content")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                    {
                        match resolve_content_reference(&raw_content) {
                            Ok(bytes) => {
                                if let Some(obj) = cfg_with_content.as_object_mut() {
                                    obj.insert(
                                        "__content_bytes".into(),
                                        serde_json::Value::String(bytes),
                                    );
                                }
                            }
                            Err(e) => {
                                notifications.push(Notification {
                                    level: "ERROR".into(),
                                    code: "SSH_CONTENT_RESOLVE_FAILED".into(),
                                    provider_id: Some(provider_id.clone()),
                                    message: format!(
                                        "failed to resolve content for '{}': {}",
                                        id, e
                                    ),
                                });
                                return Err(format!(
                                    "failed to resolve content reference: {e}"
                                ));
                            }
                        }
                    }
                }

                // For letsencrypt_cert with dns_provider='cf', resolve the
                // Cloudflare API token from the 'cf' provider and inject
                // it so the certbot plugin can authenticate.
                if s.resource_type == "letsencrypt_cert" {
                    let dns_prov = cfg_with_content
                        .get("dns_provider")
                        .and_then(|v| v.as_str())
                        .unwrap_or("cf");
                    if dns_prov == "cf" {
                        if let Some(obj) = cfg_with_content.as_object_mut() {
                            if !obj.contains_key("cf_api_token") {
                                // Look up the 'cf' provider's auth_ref
                                let token = self
                                    .ctx
                                    .registry
                                    .get_provider("cf")
                                    .ok()
                                    .map(|p| p.auth_ref.clone())
                                    .and_then(|a| {
                                        kvmql_auth::resolver::CredentialResolver::resolve(&a).ok()
                                    });
                                if let Some(t) = token {
                                    obj.insert(
                                        "cf_api_token".into(),
                                        serde_json::Value::String(t),
                                    );
                                }
                            }
                        }
                    }
                }

                let provisioner = self
                    .get_ssh_provisioner(&provider_id)
                    .map_err(|e| format!("ssh provisioner setup failed: {e}"))?;

                match provisioner.create(&s.resource_type, &cfg_with_content) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        let remediation_msg = with_remediation(
                            "SSH_PROVISION_FAILED",
                            &format!(
                                "SSH provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "SSH_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            } else if is_k8s {
                let provisioner = self.get_k8s_provisioner(&provider_id);

                match provisioner.create(&s.resource_type, &config_value) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        let remediation_msg = with_remediation(
                            "K8S_PROVISION_FAILED",
                            &format!(
                                "Kubernetes provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "K8S_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            } else if is_cloudflare {
                let provisioner = self.get_cloudflare_provisioner(&provider_id);

                match provisioner.create(&s.resource_type, &config_value) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        let remediation_msg = with_remediation(
                            "CF_PROVISION_FAILED",
                            &format!(
                                "Cloudflare provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "CF_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            } else if is_github {
                let provisioner = self.get_github_provisioner(&provider_id);

                match provisioner.create(&s.resource_type, &config_value) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        let remediation_msg = with_remediation(
                            "GH_PROVISION_FAILED",
                            &format!(
                                "GitHub provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "GH_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            } else if is_aws {
                let provisioner = self.get_aws_provisioner(&provider_id);

                match provisioner.create(&s.resource_type, &config_value) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        let remediation_msg = with_remediation(
                            "AWS_PROVISION_FAILED",
                            &format!(
                                "AWS provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "AWS_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            } else {
                let provisioner = self.get_azure_provisioner(&provider_id);

                match provisioner.create(&s.resource_type, &config_value) {
                    Ok(result) => (
                        result.status,
                        result.outputs.map(|o| o.to_string()),
                    ),
                    Err(e) => {
                        // If az CLI fails, still write to registry but with status "pending"
                        let remediation_msg = with_remediation(
                            "AZ_PROVISION_FAILED",
                            &format!(
                                "Azure provisioning failed, resource registered as pending: {e}"
                            ),
                            &ErrorContext {
                                resource_id: Some(id.clone()),
                                resource_type: Some(s.resource_type.clone()),
                                provider_id: Some(provider_id.clone()),
                                ..Default::default()
                            },
                        );
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "AZ_PROVISION_FAILED".into(),
                            provider_id: Some(provider_id.clone()),
                            message: remediation_msg,
                        });
                        ("pending".into(), None)
                    }
                }
            }
        };

        self.ctx
            .registry
            .insert_resource(
                &id,
                &s.resource_type,
                &provider_id,
                name.as_deref(),
                &status,
                Some(&config),
                labels.as_deref(),
            )
            .map_err(|e| format!("failed to create resource: {e}"))?;

        // Update outputs if provisioning returned them
        if let Some(ref out) = outputs {
            let _ = self.ctx.registry.update_resource_outputs(&id, out);
        }

        let row = self
            .ctx
            .registry
            .get_resource(&id)
            .map_err(|e| format!("resource inserted but read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "provider_id": row.provider_id,
            "name": row.name,
            "status": row.status,
            "config": row.config,
            "outputs": row.outputs,
            "created_at": row.created_at,
            "labels": row.labels,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    fn exec_alter_resource(
        &self,
        s: &AlterResourceStmt,
    ) -> Result<StmtOutcome, String> {
        if self.ctx.simulate {
            return self.exec_alter_resource_simulated(s);
        }

        // Get existing resource
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        // Verify resource type matches
        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        // Resolve variable references in set_items
        let set_items = self.resolve_params(&s.set_items);

        // Merge set_items into existing config
        let mut config: serde_json::Value = existing
            .config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or_else(|| serde_json::json!({}));

        for item in &set_items {
            let v = match &item.value {
                Value::String(s) => serde_json::Value::String(s.clone()),
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::Float(f) => serde_json::json!(f),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                _ => serde_json::Value::String(format!("{}", item.value)),
            };
            config[&item.key] = v;
        }

        // Attempt real update via the appropriate cloud CLI
        let mut notifications: Vec<Notification> = Vec::new();
        {
            let is_aws = matches!(
                s.resource_type.as_str(),
                "rds_postgres" | "vpc" | "aws_subnet" | "security_group" | "sg_rule"
            );

            let is_cloudflare = matches!(
                s.resource_type.as_str(),
                "cf_zone" | "cf_dns_record" | "cf_firewall_rule" | "cf_page_rule"
            );

            let is_github = s.resource_type.starts_with("gh_");

            let is_k8s = s.resource_type.starts_with("k8s_");

            if is_k8s {
                // Kubernetes ALTER is conceptually a re-apply: we could
                // just call create() with the merged config since `kubectl
                // apply` is idempotent. For now we mirror the
                // cloudflare/aws pattern and update the registry only;
                // users can run a CREATE again to re-apply.
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "K8S_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "Kubernetes resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_cloudflare {
                // Cloudflare update is a PATCH-per-resource story that
                // requires looking up record IDs first; for now we simply
                // update the config in the registry.  Executing a CREATE
                // after DESTROY is the recommended pattern until PATCH is
                // wired up.
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "CF_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "Cloudflare resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_github {
                // GitHub resources currently support create/delete only.
                // ALTER could rewire to gh secret set / gh variable set in a
                // future patch; for now mirror the cloudflare/aws pattern.
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "GH_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "GitHub resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_aws {
                // AWS resource types do not yet support ALTER/update
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "AWS_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "AWS resource update not yet implemented, config updated in registry only".into(),
                });
            } else {
                let provisioner = self.get_azure_provisioner(&existing.provider_id);
                match provisioner.update(&s.resource_type, &s.id, &config) {
                    Ok(result) => {
                        if let Some(outputs) = result.outputs {
                            let _ = self.ctx.registry.update_resource_outputs(&s.id, &outputs.to_string());
                        }
                    }
                    Err(e) => {
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "AZ_UPDATE_FAILED".into(),
                            provider_id: Some(existing.provider_id.clone()),
                            message: format!(
                                "Azure update failed, config updated in registry only: {e}"
                            ),
                        });
                    }
                }
            }
        }

        let config_str = config.to_string();
        self.ctx
            .registry
            .update_resource_config(&s.id, &config_str)
            .map_err(|e| format!("failed to update resource config: {e}"))?;

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "provider_id": row.provider_id,
            "name": row.name,
            "status": row.status,
            "config": row.config,
            "outputs": row.outputs,
            "updated_at": row.updated_at,
            "labels": row.labels,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    fn exec_destroy_resource(
        &self,
        s: &DestroyResourceStmt,
    ) -> Result<StmtOutcome, String> {
        if self.ctx.simulate {
            return self.exec_destroy_resource_simulated(s);
        }

        // Verify resource exists and type matches
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        // Attempt real deletion via the appropriate cloud CLI
        let mut notifications: Vec<Notification> = Vec::new();
        {
            let is_aws = matches!(
                s.resource_type.as_str(),
                "rds_postgres" | "vpc" | "aws_subnet" | "security_group" | "sg_rule"
            );

            let is_cloudflare = matches!(
                s.resource_type.as_str(),
                "cf_zone" | "cf_dns_record" | "cf_firewall_rule" | "cf_page_rule"
            );

            let is_github = s.resource_type.starts_with("gh_");

            let is_k8s = s.resource_type.starts_with("k8s_");

            if is_k8s {
                let provisioner = self.get_k8s_provisioner(&existing.provider_id);
                let cfg: serde_json::Value = existing
                    .config
                    .as_deref()
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Err(e) = provisioner.delete(&s.resource_type, &s.id, &cfg) {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "K8S_DELETE_FAILED".into(),
                        provider_id: Some(existing.provider_id.clone()),
                        message: format!(
                            "Kubernetes deletion failed, removing from registry anyway: {e}"
                        ),
                    });
                }
            } else if is_cloudflare {
                let provisioner = self.get_cloudflare_provisioner(&existing.provider_id);
                // Cloudflare delete needs zone context from the stored config
                // (for DNS records, firewall rules, page rules).  For
                // cf_dns_record the `id` stored in the registry is the
                // subdomain name — the actual Cloudflare record_id lives in
                // the outputs JSON under "record_id".
                let cfg: serde_json::Value = existing
                    .config
                    .as_deref()
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                let target_id = match s.resource_type.as_str() {
                    "cf_zone" => s.id.clone(),
                    _ => existing
                        .outputs
                        .as_deref()
                        .and_then(|o| serde_json::from_str::<serde_json::Value>(o).ok())
                        .and_then(|v| {
                            v.get("record_id")
                                .or_else(|| v.get("rule_id"))
                                .and_then(|x| x.as_str())
                                .map(String::from)
                        })
                        .unwrap_or_else(|| s.id.clone()),
                };
                if let Err(e) = provisioner.delete(&s.resource_type, &target_id, &cfg) {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "CF_DELETE_FAILED".into(),
                        provider_id: Some(existing.provider_id.clone()),
                        message: format!(
                            "Cloudflare deletion failed, removing from registry anyway: {e}"
                        ),
                    });
                }
            } else if is_github {
                let provisioner = self.get_github_provisioner(&existing.provider_id);
                // GitHub delete needs the stored config (for `repo`) and the
                // numeric `ruleset_id` (which lives in outputs, not the
                // friendly id). Merge config + outputs into a single params
                // object so the provisioner can pull whichever it needs.
                let mut merged: serde_json::Value = existing
                    .config
                    .as_deref()
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Some(outputs_str) = existing.outputs.as_deref() {
                    if let Ok(outputs_val) =
                        serde_json::from_str::<serde_json::Value>(outputs_str)
                    {
                        if let (Some(merged_obj), Some(out_obj)) =
                            (merged.as_object_mut(), outputs_val.as_object())
                        {
                            for (k, v) in out_obj {
                                // Only fill in missing keys; do not overwrite
                                // explicit config values.
                                merged_obj.entry(k.clone()).or_insert_with(|| v.clone());
                            }
                        }
                    }
                }
                if let Err(e) = provisioner.delete(&s.resource_type, &s.id, &merged) {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "GH_DELETE_FAILED".into(),
                        provider_id: Some(existing.provider_id.clone()),
                        message: format!(
                            "GitHub deletion failed, removing from registry anyway: {e}"
                        ),
                    });
                }
            } else if is_aws {
                let provisioner = self.get_aws_provisioner(&existing.provider_id);
                if let Err(e) = provisioner.delete(&s.resource_type, &s.id) {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "AWS_DELETE_FAILED".into(),
                        provider_id: Some(existing.provider_id.clone()),
                        message: format!(
                            "AWS deletion failed, removing from registry anyway: {e}"
                        ),
                    });
                }
            } else {
                let provisioner = self.get_azure_provisioner(&existing.provider_id);
                if let Err(e) = provisioner.delete(&s.resource_type, &s.id) {
                    notifications.push(Notification {
                        level: "WARN".into(),
                        code: "AZ_DELETE_FAILED".into(),
                        provider_id: Some(existing.provider_id.clone()),
                        message: format!(
                            "Azure deletion failed, removing from registry anyway: {e}"
                        ),
                    });
                }
            }
        }

        // If not force and status is "creating", still allow deletion
        // If not force and resource has a protected status, could block — but
        // for now we allow all deletions (force just signals intent)
        self.ctx
            .registry
            .delete_resource(&s.id)
            .map_err(|e| format!("failed to destroy resource: {e}"))?;

        let mut outcome = StmtOutcome::ok_empty();
        outcome.notifications = notifications;
        Ok(outcome)
    }

    // =======================================================================
    // Simulated resource operations (no cloud calls)
    // =======================================================================

    fn exec_create_resource_simulated(
        &self,
        s: &CreateResourceStmt,
    ) -> Result<StmtOutcome, String> {
        let params = self.resolve_params(&s.params);
        let id = get_param(&params, "id")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // IF NOT EXISTS: skip silently if resource already exists
        if s.if_not_exists {
            if let Ok(_existing) = self.ctx.registry.get_resource(&id) {
                let val = serde_json::json!({
                    "id": id,
                    "status": "already_exists",
                    "skipped": true,
                });
                let mut outcome = StmtOutcome::ok_val(val);
                outcome.notifications.push(Notification {
                    level: "INFO".into(),
                    code: "RES_EXISTS".into(),
                    provider_id: None,
                    message: format!("Resource '{}' '{}' already exists -- skipped", s.resource_type, id),
                });
                return Ok(outcome);
            }
        }
        let name = get_param(&params, "name");
        let provider_id = self.resolve_provider(&s.on)
            .unwrap_or_else(|_| "simulate".into());

        let config_value = self.params_to_json(&s.params);
        let config = config_value.to_string();
        let labels = get_param(&params, "labels");

        // Generate realistic fake outputs based on resource type
        let outputs = simulate_outputs(&s.resource_type, &id, &config_value);

        self.ctx
            .registry
            .insert_resource(
                &id,
                &s.resource_type,
                &provider_id,
                name.as_deref(),
                "simulated",
                Some(&config),
                labels.as_deref(),
            )
            .map_err(|e| format!("failed to create resource (simulated): {e}"))?;

        if let Some(ref out) = outputs {
            let _ = self.ctx.registry.update_resource_outputs(&id, &out.to_string());
        }

        let val = serde_json::json!({
            "id": id,
            "resource_type": s.resource_type,
            "provider_id": provider_id,
            "status": "simulated",
            "config": config_value,
            "outputs": outputs,
            "_simulated": true,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications.push(Notification {
            level: "INFO".into(),
            code: "SIM_001".into(),
            provider_id: Some(provider_id),
            message: format!(
                "Resource '{}' '{}' created in simulation mode -- no cloud resources provisioned",
                s.resource_type, id
            ),
        });
        Ok(outcome)
    }

    fn exec_alter_resource_simulated(
        &self,
        s: &AlterResourceStmt,
    ) -> Result<StmtOutcome, String> {
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        let set_items = self.resolve_params(&s.set_items);
        let mut config: serde_json::Value = existing
            .config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or_else(|| serde_json::json!({}));

        for item in &set_items {
            let v = match &item.value {
                Value::String(s) => serde_json::Value::String(s.clone()),
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::Float(f) => serde_json::json!(f),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                _ => serde_json::Value::String(format!("{}", item.value)),
            };
            config[&item.key] = v;
        }

        let config_str = config.to_string();
        self.ctx
            .registry
            .update_resource_config(&s.id, &config_str)
            .map_err(|e| format!("failed to update resource config: {e}"))?;

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "provider_id": row.provider_id,
            "name": row.name,
            "status": "simulated",
            "config": row.config,
            "outputs": row.outputs,
            "updated_at": row.updated_at,
            "labels": row.labels,
        });
        Ok(StmtOutcome::ok_val(val))
    }

    fn exec_destroy_resource_simulated(
        &self,
        s: &DestroyResourceStmt,
    ) -> Result<StmtOutcome, String> {
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        self.ctx
            .registry
            .delete_resource(&s.id)
            .map_err(|e| format!("failed to destroy resource: {e}"))?;

        let mut outcome = StmtOutcome::ok_val(serde_json::json!({
            "destroyed": s.id,
            "_simulated": true,
        }));
        outcome.notifications.push(Notification {
            level: "INFO".into(),
            code: "SIM_001".into(),
            provider_id: Some(existing.provider_id),
            message: format!(
                "Resource '{}' '{}' destroyed in simulation mode -- no cloud resources affected",
                s.resource_type, s.id
            ),
        });
        Ok(outcome)
    }

    // =======================================================================
    // Day-2 Operations: BACKUP, RESTORE RESOURCE, SCALE, UPGRADE
    // =======================================================================

    fn exec_backup(
        &self,
        s: &BackupStmt,
    ) -> Result<StmtOutcome, String> {
        // Verify resource exists and type matches
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        if self.ctx.simulate {
            let val = serde_json::json!({
                "id": existing.id,
                "resource_type": existing.resource_type,
                "status": "simulated",
                "outputs": existing.outputs,
                "_simulated": true,
            });
            let mut outcome = StmtOutcome::ok_val(val);
            outcome.notifications.push(Notification {
                level: "INFO".into(),
                code: "SIM_001".into(),
                provider_id: Some(existing.provider_id.clone()),
                message: format!("BACKUP '{}' '{}' simulated -- no cloud calls made", s.resource_type, s.id),
            });
            return Ok(outcome);
        }

        let mut notifications: Vec<Notification> = Vec::new();
        let provisioner = self.get_azure_provisioner(&existing.provider_id);

        match provisioner.backup(
            &s.resource_type,
            &s.id,
            s.destination.as_deref(),
            s.tag.as_deref(),
        ) {
            Ok(result) => {
                if let Some(ref outputs) = result.outputs {
                    let _ = self.ctx.registry.update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self.ctx.registry.update_resource_status(&s.id, &result.status);
            }
            Err(e) => {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "AZ_BACKUP_FAILED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: format!("Azure backup failed: {e}"),
                });
            }
        }

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "status": row.status,
            "outputs": row.outputs,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    fn exec_restore_resource(
        &self,
        s: &RestoreResourceStmt,
    ) -> Result<StmtOutcome, String> {
        // Verify resource exists and type matches
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        if self.ctx.simulate {
            let val = serde_json::json!({
                "id": existing.id,
                "resource_type": existing.resource_type,
                "status": "simulated",
                "outputs": existing.outputs,
                "_simulated": true,
            });
            let mut outcome = StmtOutcome::ok_val(val);
            outcome.notifications.push(Notification {
                level: "INFO".into(),
                code: "SIM_001".into(),
                provider_id: Some(existing.provider_id.clone()),
                message: format!("RESTORE '{}' '{}' simulated -- no cloud calls made", s.resource_type, s.id),
            });
            return Ok(outcome);
        }

        let mut notifications: Vec<Notification> = Vec::new();
        let provisioner = self.get_azure_provisioner(&existing.provider_id);

        match provisioner.restore_resource(&s.resource_type, &s.id, &s.source) {
            Ok(result) => {
                if let Some(ref outputs) = result.outputs {
                    let _ = self.ctx.registry.update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self.ctx.registry.update_resource_status(&s.id, &result.status);
            }
            Err(e) => {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "AZ_RESTORE_FAILED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: format!("Azure restore failed: {e}"),
                });
            }
        }

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "status": row.status,
            "outputs": row.outputs,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    fn exec_scale(
        &self,
        s: &ScaleStmt,
    ) -> Result<StmtOutcome, String> {
        // Verify resource exists and type matches
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        if self.ctx.simulate {
            let val = serde_json::json!({
                "id": existing.id,
                "resource_type": existing.resource_type,
                "status": "simulated",
                "config": existing.config,
                "outputs": existing.outputs,
                "_simulated": true,
            });
            let mut outcome = StmtOutcome::ok_val(val);
            outcome.notifications.push(Notification {
                level: "INFO".into(),
                code: "SIM_001".into(),
                provider_id: Some(existing.provider_id.clone()),
                message: format!("SCALE '{}' '{}' simulated -- no cloud calls made", s.resource_type, s.id),
            });
            return Ok(outcome);
        }

        // Build params JSON
        let params_value = {
            let mut map = serde_json::Map::new();
            for p in &s.params {
                let v = match &p.value {
                    Value::String(sv) => serde_json::Value::String(sv.clone()),
                    Value::Integer(n) => serde_json::Value::Number((*n).into()),
                    Value::Float(fv) => serde_json::json!(fv),
                    Value::Boolean(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::String(format!("{}", p.value)),
                };
                map.insert(p.key.clone(), v);
            }
            serde_json::Value::Object(map)
        };

        let mut notifications: Vec<Notification> = Vec::new();
        let provisioner = self.get_azure_provisioner(&existing.provider_id);

        match provisioner.scale(&s.resource_type, &s.id, &params_value) {
            Ok(result) => {
                if let Some(ref outputs) = result.outputs {
                    let _ = self.ctx.registry.update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self.ctx.registry.update_resource_status(&s.id, &result.status);
            }
            Err(e) => {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "AZ_SCALE_FAILED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: format!("Azure scale failed: {e}"),
                });
            }
        }

        // Update config to reflect new params
        let mut config: serde_json::Value = existing
            .config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        for p in &s.params {
            let v = match &p.value {
                Value::String(sv) => serde_json::Value::String(sv.clone()),
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::Float(fv) => serde_json::json!(fv),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                _ => serde_json::Value::String(format!("{}", p.value)),
            };
            config[&p.key] = v;
        }
        let _ = self.ctx.registry.update_resource_config(&s.id, &config.to_string());

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "status": row.status,
            "config": row.config,
            "outputs": row.outputs,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    fn exec_upgrade(
        &self,
        s: &UpgradeStmt,
    ) -> Result<StmtOutcome, String> {
        // Verify resource exists and type matches
        let existing = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            ))?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            ));
        }

        if self.ctx.simulate {
            let val = serde_json::json!({
                "id": existing.id,
                "resource_type": existing.resource_type,
                "status": "simulated",
                "config": existing.config,
                "outputs": existing.outputs,
                "_simulated": true,
            });
            let mut outcome = StmtOutcome::ok_val(val);
            outcome.notifications.push(Notification {
                level: "INFO".into(),
                code: "SIM_001".into(),
                provider_id: Some(existing.provider_id.clone()),
                message: format!("UPGRADE '{}' '{}' simulated -- no cloud calls made", s.resource_type, s.id),
            });
            return Ok(outcome);
        }

        // Build params JSON
        let params_value = {
            let mut map = serde_json::Map::new();
            for p in &s.params {
                let v = match &p.value {
                    Value::String(sv) => serde_json::Value::String(sv.clone()),
                    Value::Integer(n) => serde_json::Value::Number((*n).into()),
                    Value::Float(fv) => serde_json::json!(fv),
                    Value::Boolean(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::String(format!("{}", p.value)),
                };
                map.insert(p.key.clone(), v);
            }
            serde_json::Value::Object(map)
        };

        let mut notifications: Vec<Notification> = Vec::new();
        let provisioner = self.get_azure_provisioner(&existing.provider_id);

        match provisioner.upgrade(&s.resource_type, &s.id, &params_value) {
            Ok(result) => {
                if let Some(ref outputs) = result.outputs {
                    let _ = self.ctx.registry.update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self.ctx.registry.update_resource_status(&s.id, &result.status);
            }
            Err(e) => {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "AZ_UPGRADE_FAILED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: format!("Azure upgrade failed: {e}"),
                });
            }
        }

        // Update config to reflect upgrade params
        let mut config: serde_json::Value = existing
            .config
            .as_deref()
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        for p in &s.params {
            let v = match &p.value {
                Value::String(sv) => serde_json::Value::String(sv.clone()),
                Value::Integer(n) => serde_json::Value::Number((*n).into()),
                Value::Float(fv) => serde_json::json!(fv),
                Value::Boolean(b) => serde_json::Value::Bool(*b),
                _ => serde_json::Value::String(format!("{}", p.value)),
            };
            config[&p.key] = v;
        }
        let _ = self.ctx.registry.update_resource_config(&s.id, &config.to_string());

        let row = self
            .ctx
            .registry
            .get_resource(&s.id)
            .map_err(|e| format!("resource read-back failed: {e}"))?;

        let val = serde_json::json!({
            "id": row.id,
            "resource_type": row.resource_type,
            "status": row.status,
            "config": row.config,
            "outputs": row.outputs,
        });
        let mut outcome = StmtOutcome::ok_val(val);
        outcome.notifications = notifications;
        Ok(outcome)
    }

    // =======================================================================
    // Helpers
    // =======================================================================

    /// Resolve which provider to target. If an `ON PROVIDER 'x'` clause is
    /// present we use that, otherwise we pick the first registered driver.
    // =======================================================================
    // WATCH
    // =======================================================================

    async fn exec_watch(&self, s: &WatchStmt) -> Result<StmtOutcome, String> {
        // WATCH is fundamentally a streaming operation.  In the single-shot
        // executor context (CLI / REST) we execute one sample of the
        // underlying SELECT query and return it with an informational
        // notification telling the user to use the TCP server for
        // continuous streaming.
        let select = SelectStmt {
            fields: s.metrics.clone(),
            from: SelectSource::Noun(s.from.clone()),
            on: None,
            where_clause: s.where_clause.clone(),
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        let mut outcome = self.exec_select(&select).await?;
        outcome.notifications.push(Notification {
            level: "INFO".into(),
            code: "STA_001".into(),
            provider_id: None,
            message: format!(
                "Single sample returned. For continuous streaming, use the TCP server with WATCH INTERVAL {}.",
                s.interval,
            ),
        });
        Ok(outcome)
    }

    // =======================================================================
    // PUBLISH IMAGE
    // =======================================================================

    fn exec_publish_image(&self, s: &PublishImageStmt) -> Result<StmtOutcome, String> {
        // 1. Verify the image exists in the registry
        let image = self
            .ctx
            .registry
            .get_image(&s.image_id)
            .map_err(|e| format!("image '{}' not found: {e}", s.image_id))?;

        let mut notifications = Vec::new();

        // 2. Look up the target provider driver (optional)
        let driver = self.ctx.get_driver(&s.provider);

        // 3. Check capability if driver exists
        if let Some(ref d) = driver {
            if !d
                .capabilities()
                .supports(&kvmql_driver::capability::Capability::ImagePublish)
            {
                notifications.push(Notification {
                    level: "INFO".into(),
                    code: "CAP_001".into(),
                    provider_id: Some(s.provider.clone()),
                    message: format!(
                        "provider '{}' does not support native ImagePublish; using registry-only publish",
                        s.provider
                    ),
                });
            }
        }

        // 4. Generate a cloud_ref for the published image.
        //    The Driver trait has no publish_image method, so we generate
        //    a reference and store it in the registry.
        let cloud_ref = format!("cloud-ref-{}-{}", s.provider, s.image_id);

        // 5. Update the registry with the cloud_ref
        self.ctx
            .registry
            .update_image_cloud_ref(&s.image_id, &cloud_ref)
            .map_err(|e| format!("failed to update image cloud_ref: {e}"))?;

        let val = serde_json::json!({
            "image_id": image.id,
            "provider": s.provider,
            "cloud_ref": cloud_ref,
            "status": "published",
        });

        Ok(StmtOutcome {
            result: Some(val),
            rows_affected: 1,
            notifications,
        })
    }

    // =======================================================================
    // Provider resolution helpers
    // =======================================================================

    fn resolve_provider(&self, on: &Option<TargetSpec>) -> Result<String, String> {
        if let Some(spec) = on {
            match &spec.target {
                TargetKind::Provider(id) => return Ok(id.clone()),
                TargetKind::Cluster(_name) => {
                    return Err("cluster targeting not yet supported".into());
                }
            }
        }
        // Fall back to first registered driver
        let drivers = self.ctx.drivers.read().unwrap();
        drivers
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| {
                with_remediation(
                    "NO_DRIVERS",
                    "no drivers registered",
                    &ErrorContext::default(),
                )
            })
    }

    /// Return any available driver (first in the map).
    fn any_driver(&self) -> Result<(String, Arc<dyn kvmql_driver::traits::Driver>), String> {
        let drivers = self.ctx.drivers.read().unwrap();
        drivers
            .iter()
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .ok_or_else(|| {
                with_remediation(
                    "NO_DRIVERS",
                    "no drivers registered",
                    &ErrorContext::default(),
                )
            })
    }

    // =======================================================================
    // Variable resolution
    // =======================================================================

    /// Resolve a `Value::Variable` reference to the stored string value.
    /// Non-variable values are returned as-is.
    fn resolve_value(&self, value: &Value) -> Value {
        match value {
            Value::Variable(name) => {
                let vars = self.ctx.variables.read().unwrap();
                if let Some(val) = vars.get(name) {
                    Value::String(val.clone())
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }

    /// Resolve all variable references in a parameter list.
    fn resolve_params(&self, params: &[Param]) -> Vec<Param> {
        params.iter().map(|p| Param {
            key: p.key.clone(),
            value: self.resolve_value(&p.value),
        }).collect()
    }

    // =======================================================================
    // State snapshots — capture before mutations
    // =======================================================================

    /// Capture the current state of the resource targeted by a mutation
    /// statement, so it can be restored later via ROLLBACK.
    fn capture_state_snapshot(&self, stmt: &Statement, source: &str) {
        // Check if a snapshot_tag variable is set
        let tag: Option<String> = {
            let vars = self.ctx.variables.read().unwrap();
            vars.get("snapshot_tag").cloned()
        };
        // Clear the tag after use (one-shot)
        if tag.is_some() {
            let mut vars = self.ctx.variables.write().unwrap();
            vars.remove("snapshot_tag");
        }

        match stmt {
            Statement::DestroyResource(s) => {
                if let Ok(row) = self.ctx.registry.get_resource(&s.id) {
                    let state_json = serde_json::json!({
                        "id": row.id,
                        "resource_type": row.resource_type,
                        "provider_id": row.provider_id,
                        "name": row.name,
                        "status": row.status,
                        "config": row.config,
                        "outputs": row.outputs,
                        "labels": row.labels,
                    }).to_string();
                    let _ = self.ctx.registry.insert_state_snapshot(
                        &uuid::Uuid::new_v4().to_string(),
                        tag.as_deref(),
                        source,
                        "resource",
                        &s.id,
                        Some(&state_json),
                    );
                }
            }
            Statement::Destroy(s) => {
                let target_type = match s.target {
                    DestroyTarget::Microvm => "microvm",
                    DestroyTarget::Volume => "volume",
                };
                let state = match s.target {
                    DestroyTarget::Microvm => self.ctx.registry.get_microvm(&s.id).ok().map(|r| {
                        serde_json::json!({
                            "id": r.id, "provider_id": r.provider_id, "tenant": r.tenant,
                            "status": r.status, "image_id": r.image_id,
                            "vcpus": r.vcpus, "memory_mb": r.memory_mb,
                            "hostname": r.hostname, "metadata": r.metadata, "labels": r.labels,
                        }).to_string()
                    }),
                    DestroyTarget::Volume => self.ctx.registry.get_volume(&s.id).ok().map(|r| {
                        serde_json::json!({
                            "id": r.id, "provider_id": r.provider_id, "size_gb": r.size_gb,
                            "status": r.status, "volume_type": r.volume_type,
                            "iops": r.iops, "encrypted": r.encrypted, "labels": r.labels,
                        }).to_string()
                    }),
                };
                let _ = self.ctx.registry.insert_state_snapshot(
                    &uuid::Uuid::new_v4().to_string(),
                    tag.as_deref(),
                    source,
                    target_type,
                    &s.id,
                    state.as_deref(),
                );
            }
            Statement::AlterResource(s) => {
                if let Ok(row) = self.ctx.registry.get_resource(&s.id) {
                    let state_json = serde_json::json!({
                        "id": row.id, "resource_type": row.resource_type,
                        "provider_id": row.provider_id, "name": row.name,
                        "config": row.config, "status": row.status,
                        "outputs": row.outputs, "labels": row.labels,
                    }).to_string();
                    let _ = self.ctx.registry.insert_state_snapshot(
                        &uuid::Uuid::new_v4().to_string(),
                        tag.as_deref(),
                        source,
                        "resource",
                        &s.id,
                        Some(&state_json),
                    );
                }
            }
            Statement::AlterMicrovm(s) => {
                if let Ok(row) = self.ctx.registry.get_microvm(&s.id) {
                    let state_json = serde_json::json!({
                        "id": row.id, "provider_id": row.provider_id, "tenant": row.tenant,
                        "status": row.status, "vcpus": row.vcpus, "memory_mb": row.memory_mb,
                        "hostname": row.hostname, "metadata": row.metadata, "labels": row.labels,
                    }).to_string();
                    let _ = self.ctx.registry.insert_state_snapshot(
                        &uuid::Uuid::new_v4().to_string(),
                        tag.as_deref(),
                        source,
                        "microvm",
                        &s.id,
                        Some(&state_json),
                    );
                }
            }
            Statement::AlterVolume(s) => {
                if let Ok(row) = self.ctx.registry.get_volume(&s.id) {
                    let state_json = serde_json::json!({
                        "id": row.id, "provider_id": row.provider_id,
                        "size_gb": row.size_gb, "status": row.status,
                        "volume_type": row.volume_type, "iops": row.iops,
                        "encrypted": row.encrypted, "labels": row.labels,
                    }).to_string();
                    let _ = self.ctx.registry.insert_state_snapshot(
                        &uuid::Uuid::new_v4().to_string(),
                        tag.as_deref(),
                        source,
                        "volume",
                        &s.id,
                        Some(&state_json),
                    );
                }
            }
            _ => {} // Non-destructive/alter mutations don't need snapshots
        }
    }

    // =======================================================================
    // ROLLBACK
    // =======================================================================

    fn exec_rollback(&self, s: &RollbackStmt) -> Result<StmtOutcome, String> {
        let snapshot = match &s.target {
            RollbackTarget::Last => {
                self.ctx.registry.get_last_snapshot()
                    .map_err(|e| format!("failed to get last snapshot: {e}"))?
                    .ok_or_else(|| with_remediation(
                        "ROLLBACK_NO_SNAPSHOTS",
                        "no snapshots available for rollback",
                        &ErrorContext::default(),
                    ))?
            }
            RollbackTarget::Tag(tag) => {
                self.ctx.registry.get_snapshot_by_tag(tag)
                    .map_err(|e| format!("failed to get snapshot: {e}"))?
                    .ok_or_else(|| format!("no snapshot found with tag '{tag}'"))?
            }
            RollbackTarget::Resource { resource_type, id } => {
                self.ctx.registry.get_snapshot_for_resource(resource_type, id)
                    .map_err(|e| format!("failed to get snapshot: {e}"))?
                    .ok_or_else(|| format!("no snapshot found for {} '{}'", resource_type, id))?
            }
        };

        // Parse the previous state
        let previous = snapshot.previous_state
            .as_deref()
            .ok_or("snapshot has no previous state -- cannot rollback")?;
        let state: serde_json::Value = serde_json::from_str(previous)
            .map_err(|e| format!("failed to parse snapshot state: {e}"))?;

        // Re-create the resource from the saved state
        match snapshot.target_type.as_str() {
            "resource" => {
                // Delete current (if exists) and re-insert from snapshot
                let _ = self.ctx.registry.delete_resource(&snapshot.target_id);
                let id = state["id"].as_str().unwrap_or(&snapshot.target_id);
                let rtype = state["resource_type"].as_str().unwrap_or("unknown");
                let provider = state["provider_id"].as_str().unwrap_or("local");
                let name = state["name"].as_str();
                let status = state["status"].as_str().unwrap_or("available");
                let config = state.get("config").and_then(|c| {
                    if c.is_null() { None } else { Some(c.to_string()) }
                });
                let labels = state.get("labels").and_then(|l| {
                    if l.is_null() { None } else { Some(l.to_string()) }
                });

                self.ctx.registry.insert_resource(
                    id, rtype, provider, name, status,
                    config.as_deref(), labels.as_deref(),
                ).map_err(|e| format!("rollback failed: {e}"))?;

                if let Some(outputs) = state.get("outputs") {
                    if !outputs.is_null() {
                        let _ = self.ctx.registry.update_resource_outputs(id, &outputs.to_string());
                    }
                }
            }
            "microvm" => {
                let _ = self.ctx.registry.delete_microvm(&snapshot.target_id);
                let id = state["id"].as_str().unwrap_or(&snapshot.target_id);
                let provider = state["provider_id"].as_str().unwrap_or("local");
                let tenant = state["tenant"].as_str().unwrap_or("unknown");
                let status = state["status"].as_str().unwrap_or("unknown");
                let image_id = state["image_id"].as_str();
                let vcpus = state["vcpus"].as_i64();
                let memory_mb = state["memory_mb"].as_i64();
                let hostname = state["hostname"].as_str();
                let metadata = state.get("metadata").and_then(|m| {
                    if m.is_null() { None } else { Some(m.to_string()) }
                });
                let labels = state.get("labels").and_then(|l| {
                    if l.is_null() { None } else { Some(l.to_string()) }
                });

                self.ctx.registry.insert_microvm(
                    id, provider, tenant, status, image_id,
                    vcpus, memory_mb, hostname,
                    metadata.as_deref(), labels.as_deref(),
                ).map_err(|e| format!("rollback failed: {e}"))?;
            }
            "volume" => {
                let _ = self.ctx.registry.delete_volume(&snapshot.target_id);
                let id = state["id"].as_str().unwrap_or(&snapshot.target_id);
                let provider = state["provider_id"].as_str().unwrap_or("local");
                let vol_type = state["volume_type"].as_str().unwrap_or("gp2");
                let size_gb = state["size_gb"].as_i64().unwrap_or(10);
                let status = state["status"].as_str().unwrap_or("available");
                let iops = state["iops"].as_i64();
                let encrypted = state["encrypted"].as_bool().unwrap_or(false);
                let labels = state.get("labels").and_then(|l| {
                    if l.is_null() { None } else { Some(l.to_string()) }
                });

                self.ctx.registry.insert_volume(
                    id, provider, vol_type, size_gb, status,
                    iops, encrypted, labels.as_deref(),
                ).map_err(|e| format!("rollback failed: {e}"))?;
            }
            other => return Err(format!("rollback not supported for type: {other}")),
        }

        let val = serde_json::json!({
            "rolled_back": true,
            "target_type": snapshot.target_type,
            "target_id": snapshot.target_id,
            "restored_from": snapshot.id,
            "original_statement": snapshot.statement,
            "note": "Registry state restored. Cloud resource may need to be re-created.",
        });

        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // SELECT FROM <table-valued function>(...)
    // =======================================================================

    /// Execute `SELECT ... FROM <fn>(...)` against a network verification
    /// function. Applies WHERE/LIMIT in-memory after the function returns.
    async fn exec_select_function(
        &self,
        s: &SelectStmt,
        fc: &kvmql_parser::ast::FunctionCall,
    ) -> Result<StmtOutcome, String> {
        // Resolve any `@var` references in the args against the session
        // variable pool so functions only ever see concrete literals.
        let fc_resolved = self.resolve_function_call_args(fc);

        // Host-aware functions need access to the provider registry, so
        // we handle them here before falling through to the free-function
        // dispatcher for network-only functions.
        let rows = match fc_resolved.name.as_str() {
            "file_stat" => self.run_file_stat(&fc_resolved)?,
            "systemd_services" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::systemd::SystemdProvisioner::new(&p.client)
                    .list_services()
            })?,
            "nginx_vhosts" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::nginx::NginxProvisioner::new(&p.client)
                    .list_vhosts()
            })?,
            "nginx_config_test" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::nginx::NginxProvisioner::new(&p.client)
                    .config_test_row()
            })?,
            "docker_containers" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::docker::DockerProvisioner::new(&p.client)
                    .list_containers()
            })?,
            _ => run_table_function(&fc_resolved).await?,
        };

        // WHERE filter
        let filtered: Vec<serde_json::Value> = if let Some(ref pred) = s.where_clause {
            rows.into_iter()
                .filter(|row| eval_predicate(pred, row))
                .collect()
        } else {
            rows
        };

        // LIMIT
        let limited: Vec<serde_json::Value> = if let Some(limit) = s.limit {
            filtered.into_iter().take(limit as usize).collect()
        } else {
            filtered
        };

        // Projection — apply the SELECT field list.  Supports `*`, simple
        // column names, and a small set of aggregate functions used in
        // ASSERT expressions (`count(*)`).
        let projected = project_rows(&s.fields, limited)?;

        let n = projected.len() as i64;
        Ok(StmtOutcome::ok_rows(serde_json::Value::Array(projected), n))
    }

    /// Generic SSH query dispatcher.  Takes a function call and a closure
    /// that runs on each SSH provisioner.  If the call has one string arg,
    /// treat it as an explicit `provider_id`; if zero args, fan out across
    /// every SSH provider.  Each call's rows get a `host` and `provider_id`
    /// column injected.
    fn run_ssh_query(
        &self,
        fc: &kvmql_parser::ast::FunctionCall,
        query_fn: impl Fn(
            &kvmql_driver::ssh::SshResourceProvisioner,
        ) -> Result<Vec<serde_json::Value>, String>,
    ) -> Result<Vec<serde_json::Value>, String> {
        use kvmql_parser::ast::Expr;

        let provider_ids: Vec<String> = match fc.args.first() {
            Some(Expr::StringLit(pid)) => vec![pid.clone()],
            None | Some(_) => {
                // Fan-out across every ssh provider
                self.ctx
                    .registry
                    .list_providers()
                    .map_err(|e| format!("list providers: {e}"))?
                    .into_iter()
                    .filter(|r| r.provider_type == "ssh")
                    .map(|r| r.id)
                    .collect()
            }
        };

        if self.ctx.simulate {
            // In simulate mode, return empty — these queries require a
            // real SSH connection.
            return Ok(vec![]);
        }

        let mut all_rows = Vec::new();
        for pid in &provider_ids {
            let provisioner = self
                .get_ssh_provisioner(pid)
                .map_err(|e| format!("ssh provisioner for {pid}: {e}"))?;
            let host = self
                .ctx
                .registry
                .get_provider(pid)
                .ok()
                .and_then(|p| p.host.clone());
            match query_fn(&provisioner) {
                Ok(mut rows) => {
                    // Inject host/provider_id into each row
                    for row in &mut rows {
                        if let Some(obj) = row.as_object_mut() {
                            obj.insert(
                                "provider_id".into(),
                                serde_json::Value::String(pid.clone()),
                            );
                            obj.insert(
                                "host".into(),
                                host.as_ref()
                                    .map(|h| serde_json::Value::String(h.clone()))
                                    .unwrap_or(serde_json::Value::Null),
                            );
                        }
                    }
                    all_rows.extend(rows);
                }
                Err(e) => {
                    all_rows.push(serde_json::json!({
                        "provider_id": pid,
                        "host": host,
                        "error": e,
                    }));
                }
            }
        }
        Ok(all_rows)
    }

    /// Clone a `FunctionCall` with every `Expr::Variable(@name)`
    /// replaced by a concrete literal resolved from the session variable
    /// pool.  Variables that don't exist are left alone so the downstream
    /// dispatcher can report a proper error.
    fn resolve_function_call_args(
        &self,
        fc: &kvmql_parser::ast::FunctionCall,
    ) -> kvmql_parser::ast::FunctionCall {
        use kvmql_parser::ast::{Expr, FunctionCall};
        let vars = self.ctx.variables.read().unwrap();
        let mut new_args = Vec::with_capacity(fc.args.len());
        for a in &fc.args {
            let replaced = match a {
                Expr::Variable(name) => match vars.get(name) {
                    Some(v) => {
                        // Prefer integer if the value parses cleanly,
                        // otherwise treat as a string literal.
                        if let Ok(n) = v.parse::<i64>() {
                            Expr::Integer(n)
                        } else {
                            Expr::StringLit(v.clone())
                        }
                    }
                    None => a.clone(),
                },
                other => other.clone(),
            };
            new_args.push(replaced);
        }
        FunctionCall {
            name: fc.name.clone(),
            args: new_args,
        }
    }

    /// Host-aware file_stat table function.
    ///
    /// Signatures:
    /// - `file_stat(provider_id, path)` — stat a single file on a single
    ///   SSH host and return one row.
    /// - `file_stat(path)` — iterate every SSH provider in the registry
    ///   and return one row per host.  Useful with a `WHERE host='...'`
    ///   predicate or with `count(*)` to check rollout.
    ///
    /// Returns columns: `path`, `provider_id`, `host`, `present`, `size`,
    /// `owner`, `group`, `mode`, `sha256`, `modified_at`.  Missing files
    /// produce a row with `present = false` and nulls for the rest.
    /// (The column is `present` rather than `exists` because `EXISTS` is
    /// a reserved keyword used in subqueries.)
    fn run_file_stat(
        &self,
        fc: &kvmql_parser::ast::FunctionCall,
    ) -> Result<Vec<serde_json::Value>, String> {
        use kvmql_parser::ast::Expr;

        fn arg_str(args: &[Expr], idx: usize) -> Option<String> {
            match args.get(idx) {
                Some(Expr::StringLit(s)) => Some(s.clone()),
                _ => None,
            }
        }

        let (provider_ids, path) = match fc.args.len() {
            1 => {
                // file_stat(path) — fan out across all ssh providers
                let p = arg_str(&fc.args, 0)
                    .ok_or("file_stat(path): path must be a string literal")?;
                let providers = self
                    .ctx
                    .registry
                    .list_providers()
                    .map_err(|e| format!("failed to list providers: {e}"))?;
                let ssh: Vec<String> = providers
                    .into_iter()
                    .filter(|row| row.provider_type == "ssh")
                    .map(|row| row.id)
                    .collect();
                if ssh.is_empty() {
                    return Ok(vec![]);
                }
                (ssh, p)
            }
            2 => {
                let pid = arg_str(&fc.args, 0)
                    .ok_or("file_stat(provider_id, path): first arg must be a string literal")?;
                let p = arg_str(&fc.args, 1)
                    .ok_or("file_stat(provider_id, path): second arg must be a string literal")?;
                (vec![pid], p)
            }
            n => {
                return Err(format!(
                    "file_stat expects 1 or 2 args, got {n}"
                ))
            }
        };

        // In simulate mode we never reach a real SSH host.  Return one
        // deterministic fake row per provider_id so assertions in example
        // scripts still evaluate, and so dry-runs stay offline.
        if self.ctx.simulate {
            let rows = provider_ids
                .into_iter()
                .map(|pid| {
                    let host = self
                        .ctx
                        .registry
                        .get_provider(&pid)
                        .ok()
                        .and_then(|p| p.host.clone());
                    serde_json::json!({
                        "provider_id": pid,
                        "host": host,
                        "path": path,
                        "present": true,
                        "size": 0_i64,
                        "owner": "root",
                        "group": "root",
                        "mode": "0600",
                        "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                        "modified_at": "1970-01-01T00:00:00Z",
                        "simulated": true,
                    })
                })
                .collect();
            return Ok(rows);
        }

        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(provider_ids.len());
        for pid in provider_ids {
            let provisioner = match self.get_ssh_provisioner(&pid) {
                Ok(p) => p,
                Err(e) => {
                    // Surface the error as a row with an error field so
                    // the caller can filter on it if they want; avoids
                    // aborting the whole SELECT.
                    rows.push(serde_json::json!({
                        "provider_id": pid,
                        "host": null,
                        "path": path,
                        "present": false,
                        "error": e,
                    }));
                    continue;
                }
            };
            let host = self
                .ctx
                .registry
                .get_provider(&pid)
                .ok()
                .and_then(|p| p.host.clone());
            rows.push(build_file_stat_row(&provisioner, &pid, host.as_deref(), &path));
        }
        Ok(rows)
    }

    // =======================================================================
    // ASSERT
    // =======================================================================

    /// Execute an `ASSERT <predicate>[, '<message>']` statement.
    /// Pass → returns ok. Fail → returns Err carrying the failure message.
    async fn exec_assert(
        &self,
        s: &kvmql_parser::ast::AssertStmt,
    ) -> Result<StmtOutcome, String> {
        let passed = self.eval_assertion_predicate(&s.condition).await?;
        if passed {
            Ok(StmtOutcome::ok_val(serde_json::json!({
                "assertion": "passed",
            })))
        } else {
            let msg = s
                .message
                .clone()
                .unwrap_or_else(|| "assertion failed".to_string());
            Err(format!("ASSERTION FAILED: {}", msg))
        }
    }

    /// Async predicate evaluation that supports `EXISTS (SELECT ...)` and
    /// scalar subqueries on either side of a comparison. Used by `ASSERT`.
    fn eval_assertion_predicate<'b>(
        &'b self,
        pred: &'b Predicate,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + 'b>>
    {
        Box::pin(async move {
            match pred {
                Predicate::And(a, b) => Ok(self.eval_assertion_predicate(a).await?
                    && self.eval_assertion_predicate(b).await?),
                Predicate::Or(a, b) => Ok(self.eval_assertion_predicate(a).await?
                    || self.eval_assertion_predicate(b).await?),
                Predicate::Not(inner) => Ok(!self.eval_assertion_predicate(inner).await?),
                Predicate::Grouped(inner) => self.eval_assertion_predicate(inner).await,
                Predicate::Exists(select) => {
                    let outcome = self.exec_select(select).await?;
                    if let Some(serde_json::Value::Array(rows)) = outcome.result {
                        Ok(!rows.is_empty())
                    } else {
                        Ok(false)
                    }
                }
                Predicate::Comparison(cmp) => self.eval_assertion_comparison(cmp).await,
            }
        })
    }

    /// Evaluate a comparison where either side may be a scalar subquery.
    async fn eval_assertion_comparison(
        &self,
        cmp: &Comparison,
    ) -> Result<bool, String> {
        let lhs = self.eval_expr_value(&cmp.left).await?;
        let rhs = self.eval_expr_value(&cmp.right).await?;
        Ok(compare_json(&lhs, &cmp.op, &rhs))
    }

    /// Evaluate an `Expr` to a JSON value, with subquery support.
    fn eval_expr_value<'b>(
        &'b self,
        expr: &'b kvmql_parser::ast::Expr,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + 'b>,
    > {
        use kvmql_parser::ast::Expr;
        Box::pin(async move {
            match expr {
                Expr::Subquery(select) => {
                    let outcome = self.exec_select(select).await?;
                    // Take the first row's first column (or the whole scalar
                    // if the result is already a single value).
                    match outcome.result {
                        Some(serde_json::Value::Array(rows)) => {
                            if let Some(first) = rows.first() {
                                if let serde_json::Value::Object(obj) = first {
                                    Ok(obj
                                        .values()
                                        .next()
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null))
                                } else {
                                    Ok(first.clone())
                                }
                            } else {
                                Ok(serde_json::Value::Null)
                            }
                        }
                        Some(other) => Ok(other),
                        None => Ok(serde_json::Value::Null),
                    }
                }
                Expr::Integer(n) => Ok(serde_json::json!(n)),
                Expr::Float(f) => Ok(serde_json::json!(f)),
                Expr::StringLit(s) => Ok(serde_json::json!(s)),
                Expr::Boolean(b) => Ok(serde_json::json!(b)),
                Expr::Null => Ok(serde_json::Value::Null),
                Expr::Variable(name) => {
                    let vars = self.ctx.variables.read().unwrap();
                    Ok(vars
                        .get(name)
                        .map(|v| serde_json::Value::String(v.clone()))
                        .unwrap_or(serde_json::Value::Null))
                }
                Expr::BinaryOp { left, op, right } => {
                    let l = self.eval_expr_value(left).await?;
                    let r = self.eval_expr_value(right).await?;
                    Ok(eval_binary_op(&l, op, &r))
                }
                Expr::Grouped(inner) => self.eval_expr_value(inner).await,
                Expr::Identifier(name) => {
                    // Bare identifiers in ASSERT context are treated as
                    // string literals (column refs only make sense in WHERE).
                    Ok(serde_json::Value::String(name.clone()))
                }
                Expr::FunctionCall(_) | Expr::Duration(_) => Err(format!(
                    "expression not supported in ASSERT context: {:?}",
                    expr
                )),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Simulation: realistic fake outputs per resource type
// ---------------------------------------------------------------------------

fn simulate_outputs(resource_type: &str, id: &str, config: &serde_json::Value) -> Option<serde_json::Value> {
    match resource_type {
        "postgres" => Some(serde_json::json!({
            "fqdn": format!("{id}.postgres.database.azure.com"),
            "host": format!("{id}.postgres.database.azure.com"),
            "port": 5432,
            "state": "Ready",
            "version": config.get("version").and_then(|v| v.as_str()).unwrap_or("16"),
            "connection_string": format!("postgresql://admin@{id}.postgres.database.azure.com:5432/postgres?sslmode=require"),
        })),
        "redis" => Some(serde_json::json!({
            "host": format!("{id}.redis.cache.windows.net"),
            "port": 6380,
            "ssl_port": 6380,
            "connection_string": format!("{id}.redis.cache.windows.net:6380,ssl=True,abortConnect=False"),
        })),
        "aks" => Some(serde_json::json!({
            "fqdn": format!("{id}-dns-abc123.hcp.eastus.azmk8s.io"),
            "kubernetes_version": config.get("kubernetes_version").and_then(|v| v.as_str()).unwrap_or("1.28"),
            "node_count": config.get("node_count").and_then(|v| v.as_str()).unwrap_or("3"),
            "kubeconfig_command": format!("az aks get-credentials --name {id} --resource-group <rg>"),
        })),
        "storage_account" => Some(serde_json::json!({
            "primary_blob_endpoint": format!("https://{id}.blob.core.windows.net/"),
            "primary_file_endpoint": format!("https://{id}.file.core.windows.net/"),
            "primary_table_endpoint": format!("https://{id}.table.core.windows.net/"),
            "primary_queue_endpoint": format!("https://{id}.queue.core.windows.net/"),
        })),
        "vnet" => Some(serde_json::json!({
            "address_space": config.get("address_space").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/16"),
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/virtualNetworks/{id}"),
        })),
        "subnet" => Some(serde_json::json!({
            "address_prefix": config.get("address_prefix").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/24"),
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/virtualNetworks/{}/subnets/{id}",
                config.get("vnet").and_then(|v| v.as_str()).unwrap_or("sim-vnet")),
        })),
        "nsg" => Some(serde_json::json!({
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/networkSecurityGroups/{id}"),
        })),
        "container_registry" => Some(serde_json::json!({
            "login_server": format!("{id}.azurecr.io"),
            "admin_username": id,
        })),
        "dns_zone" => Some(serde_json::json!({
            "name_servers": ["ns1-sim.azure-dns.com", "ns2-sim.azure-dns.net"],
        })),
        "container_app" => Some(serde_json::json!({
            "fqdn": format!("{id}.happyforest-abc123.eastus.azurecontainerapps.io"),
            "latest_revision": format!("{id}--initial"),
        })),
        "rds_postgres" => Some(serde_json::json!({
            "endpoint": format!("{id}.abc123.us-east-1.rds.amazonaws.com"),
            "port": 5432,
            "engine": "postgres",
            "status": "available",
            "connection_string": format!("postgresql://admin@{id}.abc123.us-east-1.rds.amazonaws.com:5432/postgres"),
        })),
        "vpc" => {
            let id_trunc = &id[..id.len().min(8)];
            Some(serde_json::json!({
                "vpc_id": format!("vpc-sim-{id_trunc}"),
                "cidr_block": config.get("cidr_block").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/16"),
                "state": "available",
            }))
        }
        "security_group" => {
            let id_trunc = &id[..id.len().min(8)];
            Some(serde_json::json!({
                "group_id": format!("sg-sim-{id_trunc}"),
                "vpc_id": config.get("vpc_id").and_then(|v| v.as_str()),
            }))
        }
        _ => Some(serde_json::json!({
            "id": id,
            "status": "simulated",
        })),
    }
}

// ---------------------------------------------------------------------------
// Helpers: extract params
// ---------------------------------------------------------------------------

/// Apply a SELECT field list to a set of rows returned from a
/// table-valued function.
///
/// Supported shapes:
/// - `*` — pass rows through untouched.
/// - a list of simple/qualified column names — pluck those keys out of
///   each row; missing keys become `null`.
/// - an aggregate like `count(*)` — returns a single row containing one
///   field named after the aggregate.
///
/// Projections that mix aggregates and plain columns are not supported
/// and produce an error pointing at the specific field.
fn project_rows(
    fields: &kvmql_parser::ast::FieldList,
    rows: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    use kvmql_parser::ast::{Field, FieldList};

    match fields {
        FieldList::All => Ok(rows),
        FieldList::Fields(fs) => {
            // Detect aggregate mode: any FnCall whose name is a known
            // aggregate.  In that mode we return exactly one row.
            let has_aggregate = fs
                .iter()
                .any(|f| matches!(f, Field::FnCall { name, .. } if is_aggregate(name)));

            if has_aggregate {
                let mut row = serde_json::Map::new();
                for f in fs {
                    match f {
                        Field::FnCall { name, star, args } => {
                            let val = eval_aggregate(name, *star, args, &rows)?;
                            row.insert(name.clone(), val);
                        }
                        other => {
                            return Err(format!(
                                "mixing aggregate and non-aggregate projections is not supported (got {other:?})"
                            ));
                        }
                    }
                }
                return Ok(vec![serde_json::Value::Object(row)]);
            }

            // Non-aggregate projection: pluck fields from every row.
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let obj = row.as_object().cloned().unwrap_or_default();
                let mut new_row = serde_json::Map::new();
                for f in fs {
                    match f {
                        Field::Simple(name) => {
                            let v = obj.get(name).cloned().unwrap_or(serde_json::Value::Null);
                            new_row.insert(name.clone(), v);
                        }
                        Field::Qualified(_, name) => {
                            let v = obj.get(name).cloned().unwrap_or(serde_json::Value::Null);
                            new_row.insert(name.clone(), v);
                        }
                        Field::FnCall { name, .. } => {
                            return Err(format!(
                                "function '{name}' is not supported in non-aggregate projection"
                            ));
                        }
                    }
                }
                out.push(serde_json::Value::Object(new_row));
            }
            Ok(out)
        }
    }
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

fn eval_aggregate(
    name: &str,
    star: bool,
    args: &[kvmql_parser::ast::Expr],
    rows: &[serde_json::Value],
) -> Result<serde_json::Value, String> {
    use kvmql_parser::ast::Expr;

    let lname = name.to_ascii_lowercase();
    if lname == "count" {
        if star || args.is_empty() {
            return Ok(serde_json::json!(rows.len() as i64));
        }
        // count(column) — count non-null values
        if let Expr::Identifier(col) = &args[0] {
            let n = rows
                .iter()
                .filter_map(|r| r.get(col))
                .filter(|v| !v.is_null())
                .count();
            return Ok(serde_json::json!(n as i64));
        }
        return Err("count(expr) only supports count(*) and count(column)".into());
    }

    // For sum/avg/min/max we need a column reference.
    let col = match args.first() {
        Some(Expr::Identifier(name)) => name.clone(),
        Some(other) => {
            return Err(format!(
                "aggregate {lname}() requires a column name, got {other:?}"
            ))
        }
        None => return Err(format!("aggregate {lname}() requires one argument")),
    };

    let values: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.get(&col))
        .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))
        .collect();

    if values.is_empty() {
        return Ok(serde_json::Value::Null);
    }

    let result = match lname.as_str() {
        "sum" => values.iter().sum::<f64>(),
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
        "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        _ => unreachable!(),
    };
    Ok(serde_json::json!(result))
}

/// Run `stat` + `sha256sum` on a remote path via the given provisioner
/// and produce one JSON row.  Errors at either step produce a row with
/// `exists=false` and an `error` field — individual failures never abort
/// the whole `SELECT`.
fn build_file_stat_row(
    provisioner: &kvmql_driver::ssh::SshResourceProvisioner,
    provider_id: &str,
    host: Option<&str>,
    path: &str,
) -> serde_json::Value {
    let stat_res = provisioner.client.stat(path);
    match stat_res {
        Err(e) => serde_json::json!({
            "provider_id": provider_id,
            "host": host,
            "path": path,
            "present": false,
            "error": format!("stat failed: {e}"),
        }),
        Ok(None) => serde_json::json!({
            "provider_id": provider_id,
            "host": host,
            "path": path,
            "present": false,
            "size": null,
            "owner": null,
            "group": null,
            "mode": null,
            "sha256": null,
            "modified_at": null,
        }),
        Ok(Some(stat)) => {
            // SHA-256 is best-effort — a permission denied on a file we
            // can stat but not read shouldn't turn the whole row into an
            // error.  Propagate it as a null with a warning on the row.
            let sha = provisioner
                .client
                .sha256(path)
                .ok()
                .flatten();
            serde_json::json!({
                "provider_id": provider_id,
                "host": host,
                "path": path,
                "present": true,
                "size": stat.size,
                "owner": stat.owner,
                "group": stat.group,
                "mode": stat.mode,
                "sha256": sha,
                "modified_at": stat.modified_at,
            })
        }
    }
}

/// Resolve a `file` resource `content` parameter into raw bytes (returned
/// as a String because all our tracked content is UTF-8 right now).
///
/// Three cases, disambiguated by the leading scheme:
/// - `file:./local/path` or `file:/abs/path` — read the local file
///   verbatim, NO trim and NO world-readable check.  This is the common
///   case for config files checked into the repo.
/// - `env:...`, `op:...`, `vault:...`, etc. — delegate to
///   [`CredentialResolver`], which handles secrets (trimmed, permission-
///   checked for `file:` scheme, but we reach it only for non-file
///   schemes here).
/// - anything else — treat as a literal string and pass through.
fn resolve_content_reference(raw: &str) -> Result<String, String> {
    if let Some(rest) = raw.strip_prefix("file:") {
        return std::fs::read_to_string(rest)
            .map_err(|e| format!("failed to read {rest}: {e}"));
    }
    // Any other recognised credential scheme goes through the resolver.
    let is_credential_scheme = ["env:", "op:", "vault:", "aws-sm:", "gcp-sm:", "azure-kv:", "sops:", "k8s:"]
        .iter()
        .any(|p| raw.starts_with(p));
    if is_credential_scheme {
        return kvmql_auth::resolver::CredentialResolver::resolve(raw)
            .map_err(|e| format!("credential resolve failed: {e}"));
    }
    // Literal string (nginx conf inlined in the .kvmql file, etc.).
    Ok(raw.to_string())
}

/// Parse optional SSH connection hints out of the provider's `labels`
/// JSON (the same column we reuse for provider metadata across drivers).
/// Recognised keys: `ssh_user`, `ssh_port`.  The DSL can set them via
/// `ADD PROVIDER ... labels='{"ssh_user":"azureuser","ssh_port":22}'`.
fn parse_ssh_connection_hints(labels: Option<&str>) -> (Option<String>, Option<u16>) {
    let Some(raw) = labels else {
        return (None, None);
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return (None, None);
    };
    let user = v
        .get("ssh_user")
        .and_then(|x| x.as_str())
        .map(String::from);
    let port = v
        .get("ssh_port")
        .and_then(|x| x.as_u64())
        .and_then(|p| u16::try_from(p).ok());
    (user, port)
}

/// Write resolved private-key material to a mode-0600 tempfile and return
/// its path.  The file is leaked intentionally — OpenSSH needs a real
/// path, and callers want the key to stay valid for the lifetime of the
/// SSH client.
fn write_ephemeral_key(key_material: &str) -> Result<std::path::PathBuf, String> {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir();
    let filename = format!("orbi-ssh-{}.pem", uuid::Uuid::new_v4());
    let path = dir.join(filename);

    // Create with 0600 up-front so no other user ever sees the bytes.
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(&path)
        .map_err(|e| format!("failed to create ssh key tempfile: {e}"))?;
    f.write_all(key_material.as_bytes())
        .map_err(|e| format!("failed to write ssh key tempfile: {e}"))?;
    // OpenSSH also requires the file to end with a newline for some
    // key formats; add one if missing.
    if !key_material.ends_with('\n') {
        f.write_all(b"\n")
            .map_err(|e| format!("failed to write ssh key tempfile newline: {e}"))?;
    }
    // Belt-and-braces: re-apply 0600 in case the create_new path ran
    // without OpenOptionsExt on a platform that silently ignored it.
    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    Ok(path)
}

fn get_param(params: &[Param], key: &str) -> Option<String> {
    params.iter().find(|p| p.key == key).and_then(|p| match &p.value {
        Value::String(s) => Some(s.clone()),
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Boolean(b) => Some(b.to_string()),
        _ => None,
    })
}

fn get_param_i64(params: &[Param], key: &str) -> Option<i64> {
    params.iter().find(|p| p.key == key).and_then(|p| match &p.value {
        Value::Integer(n) => Some(*n),
        Value::String(s) => s.parse().ok(),
        _ => None,
    })
}

fn get_param_bool(params: &[Param], key: &str) -> Option<bool> {
    params.iter().find(|p| p.key == key).and_then(|p| match &p.value {
        Value::Boolean(b) => Some(*b),
        Value::String(s) => match s.as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// Predicate evaluation (simple in-memory WHERE filter)
// ---------------------------------------------------------------------------

fn eval_predicate(pred: &Predicate, row: &serde_json::Value) -> bool {
    match pred {
        Predicate::And(a, b) => eval_predicate(a, row) && eval_predicate(b, row),
        Predicate::Or(a, b) => eval_predicate(a, row) || eval_predicate(b, row),
        Predicate::Not(a) => !eval_predicate(a, row),
        Predicate::Grouped(inner) => eval_predicate(inner, row),
        Predicate::Comparison(cmp) => eval_comparison(cmp, row),
        // EXISTS subqueries don't have a meaningful row-level interpretation.
        // They are evaluated in the ASSERT path via the async helper; in the
        // WHERE-clause path we conservatively treat them as true (no-op).
        Predicate::Exists(_) => true,
    }
}

fn eval_comparison(cmp: &Comparison, row: &serde_json::Value) -> bool {
    let field_name = match &cmp.left {
        Expr::Identifier(name) => name.as_str(),
        _ => return false,
    };

    let row_val = match row.get(field_name) {
        Some(v) => v,
        None => return cmp.op == ComparisonOp::IsNull,
    };

    match &cmp.op {
        ComparisonOp::IsNull => row_val.is_null(),
        ComparisonOp::IsNotNull => !row_val.is_null(),
        ComparisonOp::Eq => expr_matches_json(&cmp.right, row_val),
        ComparisonOp::NotEq => !expr_matches_json(&cmp.right, row_val),
        ComparisonOp::Gt => compare_numeric(row_val, &cmp.right, |a, b| a > b),
        ComparisonOp::Lt => compare_numeric(row_val, &cmp.right, |a, b| a < b),
        ComparisonOp::GtEq => compare_numeric(row_val, &cmp.right, |a, b| a >= b),
        ComparisonOp::LtEq => compare_numeric(row_val, &cmp.right, |a, b| a <= b),
        ComparisonOp::Like => {
            if let (Some(hay), Expr::StringLit(pat)) =
                (row_val.as_str(), &cmp.right)
            {
                simple_like(hay, pat)
            } else {
                false
            }
        }
        ComparisonOp::In | ComparisonOp::NotIn => {
            // Not yet supported — always true/false
            cmp.op == ComparisonOp::NotIn
        }
    }
}

fn expr_matches_json(expr: &Expr, val: &serde_json::Value) -> bool {
    match expr {
        Expr::StringLit(s) => val.as_str() == Some(s.as_str()),
        Expr::Integer(n) => val.as_i64() == Some(*n),
        Expr::Float(f) => val.as_f64() == Some(*f),
        Expr::Boolean(b) => val.as_bool() == Some(*b),
        Expr::Null => val.is_null(),
        _ => false,
    }
}

fn compare_numeric(
    val: &serde_json::Value,
    expr: &Expr,
    f: fn(f64, f64) -> bool,
) -> bool {
    let lhs = val.as_f64().or_else(|| val.as_i64().map(|n| n as f64));
    let rhs = match expr {
        Expr::Integer(n) => Some(*n as f64),
        Expr::Float(f) => Some(*f),
        _ => None,
    };
    match (lhs, rhs) {
        (Some(a), Some(b)) => f(a, b),
        _ => false,
    }
}

/// Very simple LIKE matching supporting `%` as wildcard.
fn simple_like(hay: &str, pattern: &str) -> bool {
    if pattern == "%" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix('%') {
        if let Some(prefix) = suffix.strip_suffix('%') {
            return hay.contains(prefix);
        }
        return hay.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('%') {
        return hay.starts_with(prefix);
    }
    hay == pattern
}

// ---------------------------------------------------------------------------
// Table-valued functions (network verification)
// ---------------------------------------------------------------------------

/// Dispatch a `SELECT * FROM <fn>(...)` call to the network verification
/// module. Each function returns a `Vec<serde_json::Value>` of rows.
async fn run_table_function(
    fc: &kvmql_parser::ast::FunctionCall,
) -> Result<Vec<serde_json::Value>, String> {
    use crate::network::NetworkFunctions;

    fn arg_str(args: &[Expr], idx: usize) -> Result<String, String> {
        match args.get(idx) {
            Some(Expr::StringLit(s)) => Ok(s.clone()),
            Some(other) => Err(format!(
                "expected string literal at arg {}, got {:?}",
                idx, other
            )),
            None => Err(format!("missing required arg {}", idx)),
        }
    }

    fn arg_int(args: &[Expr], idx: usize) -> Result<i64, String> {
        match args.get(idx) {
            Some(Expr::Integer(n)) => Ok(*n),
            Some(other) => Err(format!(
                "expected integer at arg {}, got {:?}",
                idx, other
            )),
            None => Err(format!("missing required arg {}", idx)),
        }
    }

    fn arg_str_opt(args: &[Expr], idx: usize) -> Option<String> {
        match args.get(idx) {
            Some(Expr::StringLit(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn arg_int_opt(args: &[Expr], idx: usize) -> Option<i64> {
        match args.get(idx) {
            Some(Expr::Integer(n)) => Some(*n),
            _ => None,
        }
    }

    match fc.name.as_str() {
        "dns_lookup" => {
            let name = arg_str(&fc.args, 0)?;
            let record_type = arg_str_opt(&fc.args, 1);
            // 3rd arg (resolver override) is reserved for future hickory-resolver work.
            NetworkFunctions::dns_lookup(&name, record_type.as_deref()).await
        }
        "reverse_dns" => {
            let ip = arg_str(&fc.args, 0)?;
            NetworkFunctions::reverse_dns(&ip).await
        }
        "tcp_probe" => {
            let host = arg_str(&fc.args, 0)?;
            let port = arg_int(&fc.args, 1)? as u16;
            let timeout_ms = arg_int_opt(&fc.args, 2).map(|n| n as u64);
            NetworkFunctions::tcp_probe(&host, port, timeout_ms).await
        }
        "http_probe" => {
            let url = arg_str(&fc.args, 0)?;
            NetworkFunctions::http_probe(&url).await
        }
        "tls_cert" => {
            let host = arg_str(&fc.args, 0)?;
            let port = arg_int(&fc.args, 1)? as u16;
            NetworkFunctions::tls_cert(&host, port).await
        }
        other => Err(format!("unknown table function: {}", other)),
    }
}

/// Compare two JSON scalar values using a `ComparisonOp`. Used by ASSERT
/// when both sides of a comparison have been resolved to concrete values.
fn compare_json(lhs: &serde_json::Value, op: &ComparisonOp, rhs: &serde_json::Value) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::NotEq => lhs != rhs,
        ComparisonOp::IsNull => lhs.is_null(),
        ComparisonOp::IsNotNull => !lhs.is_null(),
        ComparisonOp::Gt | ComparisonOp::Lt | ComparisonOp::GtEq | ComparisonOp::LtEq => {
            let l = lhs.as_f64();
            let r = rhs.as_f64();
            // Fall back to string comparison for date-like strings
            if let (Some(a), Some(b)) = (l, r) {
                match op {
                    ComparisonOp::Gt => a > b,
                    ComparisonOp::Lt => a < b,
                    ComparisonOp::GtEq => a >= b,
                    ComparisonOp::LtEq => a <= b,
                    _ => unreachable!(),
                }
            } else if let (Some(a), Some(b)) = (lhs.as_str(), rhs.as_str()) {
                match op {
                    ComparisonOp::Gt => a > b,
                    ComparisonOp::Lt => a < b,
                    ComparisonOp::GtEq => a >= b,
                    ComparisonOp::LtEq => a <= b,
                    _ => unreachable!(),
                }
            } else {
                false
            }
        }
        ComparisonOp::Like => {
            if let (Some(h), Some(p)) = (lhs.as_str(), rhs.as_str()) {
                simple_like(h, p)
            } else {
                false
            }
        }
        ComparisonOp::In | ComparisonOp::NotIn => {
            // IN/NOT IN against a scalar RHS reduces to equality
            let eq = lhs == rhs;
            if matches!(op, ComparisonOp::In) {
                eq
            } else {
                !eq
            }
        }
    }
}

/// Apply a binary operator (`+`, `-`, `||`) to two resolved JSON values.
fn eval_binary_op(
    lhs: &serde_json::Value,
    op: &kvmql_parser::ast::BinaryOp,
    rhs: &serde_json::Value,
) -> serde_json::Value {
    use kvmql_parser::ast::BinaryOp;
    match op {
        BinaryOp::Concat => {
            let ls = json_to_concat_string(lhs);
            let rs = json_to_concat_string(rhs);
            serde_json::Value::String(format!("{}{}", ls, rs))
        }
        BinaryOp::Add => {
            if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_f64()) {
                serde_json::json!(a + b)
            } else {
                serde_json::Value::Null
            }
        }
        BinaryOp::Subtract => {
            if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_f64()) {
                serde_json::json!(a - b)
            } else {
                serde_json::Value::Null
            }
        }
    }
}

fn json_to_concat_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Envelope constructors
// ---------------------------------------------------------------------------

fn error_envelope(request_id: String, message: &str, start: Instant) -> ResultEnvelope {
    ResultEnvelope {
        request_id,
        status: ResultStatus::Error,
        notifications: vec![Notification {
            level: "ERROR".into(),
            code: "PARSE_001".into(),
            provider_id: None,
            message: message.to_string(),
        }],
        result: None,
        rows_affected: None,
        duration_ms: start.elapsed().as_millis() as u64,
    }
}

fn error_envelope_with(
    request_id: String,
    message: &str,
    mut notifications: Vec<Notification>,
    result: Option<serde_json::Value>,
    start: Instant,
) -> ResultEnvelope {
    notifications.push(Notification {
        level: "ERROR".into(),
        code: "RTE_001".into(),
        provider_id: None,
        message: message.to_string(),
    });
    ResultEnvelope {
        request_id,
        status: ResultStatus::Error,
        notifications,
        result,
        rows_affected: None,
        duration_ms: start.elapsed().as_millis() as u64,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::EngineContext;
    use kvmql_driver::mock::MockDriver;
    use kvmql_registry::Registry;
    use std::sync::Arc;

    fn setup() -> EngineContext {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);
        ctx
    }

    fn setup_with_provider() -> EngineContext {
        let ctx = setup();
        ctx.registry
            .insert_provider(
                "test-provider",
                "kvm",
                "firecracker",
                "healthy",
                true,
                Some("localhost"),
                None,
                "env:X",
                None,
                None,
            )
            .unwrap();
        ctx
    }

    // ── ADD PROVIDER + SHOW PROVIDERS ──────────────────────────────────

    #[tokio::test]
    async fn add_provider_and_show() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("ADD PROVIDER id='p1' type='kvm' driver='firecracker' auth='env:X'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "envelope: {r:?}");
        assert!(r.result.is_some());

        let r2 = exec.execute("SHOW PROVIDERS").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let providers = arr.as_array().unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0]["id"], "p1");
    }

    // ── CREATE + SELECT MICROVM ───────────────────────────────────────

    #[tokio::test]
    async fn create_and_select_microvm() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM tenant='acme' vcpus=2 memory_mb=512 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create envelope: {r:?}");

        let r2 = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let vms = arr.as_array().unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0]["tenant"], "acme");
    }

    // ── CREATE + DESTROY MICROVM ──────────────────────────────────────

    #[tokio::test]
    async fn create_and_destroy_microvm() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='vm-del' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok);

        let r2 = exec.execute("DESTROY MICROVM 'vm-del'").await;
        assert_eq!(r2.status, ResultStatus::Ok, "destroy: {r2:?}");

        let r3 = exec.execute("SELECT * FROM microvms").await;
        let vms = r3.result.unwrap().as_array().unwrap().len();
        assert_eq!(vms, 0);
    }

    // ── PAUSE / RESUME ────────────────────────────────────────────────

    #[tokio::test]
    async fn pause_resume() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='vm-pr' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec.execute("PAUSE MICROVM 'vm-pr'").await;
        assert_eq!(r.status, ResultStatus::Ok, "pause: {r:?}");

        // Verify status in registry
        let row = ctx.registry.get_microvm("vm-pr").unwrap();
        assert_eq!(row.status, "paused");

        let r2 = exec.execute("RESUME MICROVM 'vm-pr'").await;
        assert_eq!(r2.status, ResultStatus::Ok, "resume: {r2:?}");

        let row = ctx.registry.get_microvm("vm-pr").unwrap();
        assert_eq!(row.status, "running");
    }

    // ── VOLUME lifecycle ──────────────────────────────────────────────

    #[tokio::test]
    async fn create_destroy_volume() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE VOLUME id='vol-1' size_gb=20 type='virtio-blk' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create vol: {r:?}");

        let r2 = exec.execute("SELECT * FROM volumes").await;
        let vols = r2.result.unwrap().as_array().unwrap().len();
        assert_eq!(vols, 1);

        let r3 = exec.execute("DESTROY VOLUME 'vol-1'").await;
        assert_eq!(r3.status, ResultStatus::Ok, "destroy vol: {r3:?}");

        let r4 = exec.execute("SELECT * FROM volumes").await;
        let vols = r4.result.unwrap().as_array().unwrap().len();
        assert_eq!(vols, 0);
    }

    // ── ATTACH / DETACH VOLUME ────────────────────────────────────────

    #[tokio::test]
    async fn attach_detach_volume() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='vm-att' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute("CREATE VOLUME id='vol-att' size_gb=10 ON PROVIDER 'test-provider'")
            .await;

        let r = exec
            .execute("ATTACH VOLUME 'vol-att' TO MICROVM 'vm-att'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "attach: {r:?}");

        let vol_row = ctx.registry.get_volume("vol-att").unwrap();
        assert_eq!(vol_row.status, "attached");
        assert_eq!(vol_row.microvm_id.as_deref(), Some("vm-att"));

        let r2 = exec
            .execute("DETACH VOLUME 'vol-att' FROM MICROVM 'vm-att'")
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "detach: {r2:?}");

        let vol_row = ctx.registry.get_volume("vol-att").unwrap();
        assert_eq!(vol_row.status, "available");
        assert!(vol_row.microvm_id.is_none());
    }

    // ── SHOW VERSION ──────────────────────────────────────────────────

    #[tokio::test]
    async fn show_version() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec.execute("SHOW VERSION").await;
        assert_eq!(r.status, ResultStatus::Ok);
        let val = r.result.unwrap();
        assert_eq!(val["version"], env!("CARGO_PKG_VERSION"));
    }

    // ── SET execution_mode ────────────────────────────────────────────

    #[tokio::test]
    async fn set_execution_mode() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec.execute("SET execution_mode = 'strict'").await;
        assert_eq!(r.status, ResultStatus::Ok, "set: {r:?}");
        let val = r.result.unwrap();
        assert_eq!(val["execution_mode"], "strict");
    }

    // ── Parse error returns error envelope ────────────────────────────

    #[tokio::test]
    async fn parse_error_returns_error_envelope() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec.execute("BLARGH FOOBAR").await;
        assert_eq!(r.status, ResultStatus::Error);
        assert!(!r.notifications.is_empty());
        assert_eq!(r.notifications[0].code, "PARSE_001");
    }

    // ── Multi-statement execution ─────────────────────────────────────

    #[tokio::test]
    async fn multi_statement() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "ADD PROVIDER id='p1' type='kvm' driver='firecracker' auth='env:X'; \
                 SHOW PROVIDERS",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "multi: {r:?}");
        // The last result should be SHOW PROVIDERS
        let arr = r.result.unwrap();
        let providers = arr.as_array().unwrap();
        assert_eq!(providers.len(), 1);
    }

    // ── SNAPSHOT / RESTORE ────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_and_restore() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='vm-snap' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SNAPSHOT MICROVM 'vm-snap' INTO '/snaps/s1' TAG 'v1'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "snapshot: {r:?}");
        let snap_val = r.result.unwrap();
        assert_eq!(snap_val["microvm_id"], "vm-snap");
        assert_eq!(snap_val["destination"], "/snaps/s1");

        // Destroy original, then restore
        exec.execute("DESTROY MICROVM 'vm-snap' FORCE").await;

        let r2 = exec
            .execute("RESTORE MICROVM 'vm-restored' FROM '/snaps/s1'")
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "restore: {r2:?}");
    }

    // ── SELECT with WHERE ─────────────────────────────────────────────

    #[tokio::test]
    async fn select_with_where() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='vm-a' tenant='alpha' vcpus=1 memory_mb=256 image='img-1' ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='vm-b' tenant='beta' vcpus=2 memory_mb=512 image='img-1' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SELECT * FROM microvms WHERE tenant = 'alpha'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok);
        let arr = r.result.unwrap();
        let vms = arr.as_array().unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0]["tenant"], "alpha");
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 10: Query history & audit log integration tests
    // ══════════════════════════════════════════════════════════════════

    // ── test_query_history_recorded ──────────────────────────────────

    #[tokio::test]
    async fn test_query_history_recorded() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r.status, ResultStatus::Ok);

        let history = ctx.registry.list_query_history(None).unwrap();
        assert!(
            !history.is_empty(),
            "expected at least one query_history row after SELECT"
        );
        let row = &history[0];
        assert_eq!(row.verb, "SELECT");
        assert_eq!(row.status, "ok");
        assert!(row.statement.contains("SELECT"));
        assert!(row.normalized_stmt.is_some());
    }

    // ── test_audit_log_on_create ────────────────────────────────────

    #[tokio::test]
    async fn test_audit_log_on_create() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='vm-audit' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create: {r:?}");

        let logs = ctx.registry.list_audit_log(None).unwrap();
        let vm_created = logs.iter().find(|l| l.action == "VM_CREATED");
        assert!(
            vm_created.is_some(),
            "expected VM_CREATED audit log entry; got: {logs:?}"
        );
        let entry = vm_created.unwrap();
        assert_eq!(entry.target_type.as_deref(), Some("microvm"));
        assert_eq!(entry.target_id.as_deref(), Some("vm-audit"));
        assert_eq!(entry.outcome, "permitted");
    }

    // ── test_audit_log_on_destroy ───────────────────────────────────

    #[tokio::test]
    async fn test_audit_log_on_destroy() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // First create a VM so we can destroy it
        exec.execute(
            "CREATE MICROVM id='vm-aud-del' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec.execute("DESTROY MICROVM 'vm-aud-del'").await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy: {r:?}");

        let logs = ctx.registry.list_audit_log(None).unwrap();
        let vm_destroyed = logs.iter().find(|l| l.action == "VM_DESTROYED");
        assert!(
            vm_destroyed.is_some(),
            "expected VM_DESTROYED audit log entry; got: {logs:?}"
        );
        let entry = vm_destroyed.unwrap();
        assert_eq!(entry.target_type.as_deref(), Some("microvm"));
        assert_eq!(entry.target_id.as_deref(), Some("vm-aud-del"));
    }

    // ── test_normalized_statement ───────────────────────────────────

    #[test]
    fn test_normalized_statement() {
        let input = "SELECT * FROM microvms WHERE tenant = 'acme' AND cpu_pct > 50";
        let normalized = normalize_statement(input);
        assert_eq!(
            normalized,
            "SELECT * FROM microvms WHERE tenant = ? AND cpu_pct > ?"
        );

        // Strings with special characters
        let input2 = "CREATE MICROVM id='vm-1' tenant='my tenant' vcpus=2 memory_mb=512";
        let normalized2 = normalize_statement(input2);
        assert_eq!(
            normalized2,
            "CREATE MICROVM id=? tenant=? vcpus=? memory_mb=?"
        );

        // Auth param should be masked
        let input3 = "ADD PROVIDER id='p1' auth='s3cr3t-k3y'";
        let normalized3 = normalize_statement(input3);
        assert_eq!(normalized3, "ADD PROVIDER id=? auth=?");
        assert!(
            !normalized3.contains("s3cr3t"),
            "normalized statement must not contain credential values"
        );
    }

    // ── test_no_credentials_in_history ──────────────────────────────

    #[tokio::test]
    async fn test_no_credentials_in_history() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("ADD PROVIDER id='prov-secret' type='kvm' driver='firecracker' auth='super-secret-key-123'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "add provider: {r:?}");

        let history = ctx.registry.list_query_history(None).unwrap();
        assert!(!history.is_empty(), "expected query_history row");

        let row = &history[0];
        // The normalized_stmt must NOT contain the auth value
        let norm = row.normalized_stmt.as_deref().unwrap_or("");
        assert!(
            !norm.contains("super-secret-key-123"),
            "normalized statement must not contain auth credential; got: {norm}"
        );
        assert!(
            norm.contains("auth=?"),
            "expected auth=? in normalized statement; got: {norm}"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 11: Access control integration tests
    // ══════════════════════════════════════════════════════════════════

    fn setup_with_auth(principal_id: &str, verbs_json: &str) -> EngineContext {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);

        // Insert provider for FK
        ctx.registry
            .insert_provider(
                "test-provider",
                "kvm",
                "firecracker",
                "healthy",
                true,
                Some("localhost"),
                None,
                "env:X",
                None,
                None,
            )
            .unwrap();

        // Insert principal and grant
        ctx.registry
            .insert_principal(principal_id, "user", "env:TOKEN", true)
            .unwrap();
        ctx.registry
            .insert_grant(
                "grant-test",
                principal_id,
                verbs_json,
                "global",
                None,
                None,
                Some("test"),
            )
            .unwrap();

        ctx.auth_enabled = true;
        ctx.current_principal = Some(principal_id.to_string());
        ctx
    }

    // ── test_auth_enabled_blocks_unauthorized ────────────────────────

    #[tokio::test]
    async fn test_auth_enabled_blocks_unauthorized() {
        // Principal has SELECT only — DESTROY should be denied
        let ctx = setup_with_auth("usr-ro", r#"["SELECT"]"#);
        let exec = Executor::new(&ctx);

        // First create a VM (will be denied since CREATE is not granted)
        let r = exec
            .execute("DESTROY MICROVM 'vm-nonexistent'")
            .await;
        assert_eq!(r.status, ResultStatus::Error, "expected auth denial: {r:?}");
        assert!(
            r.notifications
                .iter()
                .any(|n| n.message.contains("AUTH_DENIED")),
            "expected AUTH_DENIED in notifications: {:?}",
            r.notifications,
        );
    }

    // ── test_auth_enabled_permits_authorized ─────────────────────────

    #[tokio::test]
    async fn test_auth_enabled_permits_authorized() {
        // Principal has DESTROY + CREATE + SELECT
        let ctx = setup_with_auth(
            "usr-admin",
            r#"["SELECT","CREATE","DESTROY"]"#,
        );
        let exec = Executor::new(&ctx);

        // Create a VM first
        let r = exec
            .execute(
                "CREATE MICROVM id='vm-auth' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create should succeed: {r:?}");

        // Destroy it — should be permitted
        let r2 = exec.execute("DESTROY MICROVM 'vm-auth'").await;
        assert_eq!(r2.status, ResultStatus::Ok, "destroy should succeed: {r2:?}");
    }

    // ── test_auth_disabled_permits_all ────────────────────────────────

    #[tokio::test]
    async fn test_auth_disabled_permits_all() {
        // Default setup: auth_enabled = false, no principal set
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='vm-noauth' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "with auth disabled, any op should succeed: {r:?}"
        );
    }

    // ── WATCH returns single sample with INFO notification ───────────

    #[tokio::test]
    async fn watch_returns_single_sample_with_notification() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a VM so the SELECT underlying WATCH has data
        exec.execute(
            "CREATE MICROVM id='vm-w1' tenant='acme' vcpus=2 memory_mb=512 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("WATCH METRIC * FROM microvms INTERVAL 5s")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "watch should return Ok: {r:?}");
        // Should have at least one row in the result
        assert!(r.result.is_some(), "watch should return result data");

        // Should contain the STA_001 streaming notification
        let has_streaming_note = r
            .notifications
            .iter()
            .any(|n| n.code == "STA_001" && n.message.contains("INTERVAL 5s"));
        assert!(
            has_streaming_note,
            "expected STA_001 notification about streaming: {:?}",
            r.notifications
        );
    }

    // ── PUBLISH IMAGE succeeds on mock driver ──────────────────────

    #[tokio::test]
    async fn test_publish_image_mock() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Insert the image directly into the registry with a schema-valid type
        ctx.registry
            .insert_image(
                "img-pub",
                "Ubuntu",
                "linux",
                "ubuntu",
                "22.04",
                "x86_64",
                "kernel+rootfs",
                Some("test-provider"),
                Some("/boot/vmlinux"),
                Some("/images/ubuntu.ext4"),
                None,
                None,
                "local",
                None,
                None,
                "available",
                None,
            )
            .unwrap();

        let r = exec
            .execute("PUBLISH IMAGE 'img-pub' TO PROVIDER 'test-provider'")
            .await;

        // Mock driver now supports ImagePublish — should succeed
        assert_eq!(r.status, ResultStatus::Ok, "publish should succeed: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["image_id"], "img-pub");
        assert_eq!(result["provider"], "test-provider");
        assert_eq!(result["status"], "published");
        assert!(
            result["cloud_ref"].as_str().unwrap().contains("img-pub"),
            "cloud_ref should reference image id"
        );

        // Verify the registry was updated
        let image = ctx.registry.get_image("img-pub").unwrap();
        assert!(image.cloud_ref.is_some(), "cloud_ref should be set in registry");
        assert!(
            image.cloud_ref.unwrap().contains("img-pub"),
            "cloud_ref should reference image id"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Managed Resource tests
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn create_resource_and_select() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'db-1' version = '16' sku = 'Standard_B1ms' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create resource: {r:?}");
        let val = r.result.unwrap();
        assert_eq!(val["id"], "db-1");
        assert_eq!(val["resource_type"], "postgres");
        // Status is "created" when az CLI succeeds, or "pending" when it
        // is not available (e.g. in CI / test environments).
        let status = val["status"].as_str().unwrap();
        assert!(
            status == "created" || status == "pending",
            "expected status 'created' or 'pending', got '{status}'"
        );

        // Select from resources
        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0]["id"], "db-1");
        assert_eq!(resources[0]["resource_type"], "postgres");
    }

    #[tokio::test]
    async fn create_resource_and_filter_by_type() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-1' version = '16' ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE RESOURCE 'redis' id = 'cache-1' sku = 'Standard' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SELECT * FROM resources WHERE resource_type = 'postgres'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok);
        let arr = r.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0]["resource_type"], "postgres");
    }

    #[tokio::test]
    async fn alter_resource_updates_config() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-alt' version = '16' sku = 'Standard_B1ms' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("ALTER RESOURCE 'postgres' 'db-alt' SET sku = 'Standard_B2s', storage_gb = 64")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "alter resource: {r:?}");

        // Verify the config was updated
        let row = ctx.registry.get_resource("db-alt").unwrap();
        let config: serde_json::Value = serde_json::from_str(row.config.as_deref().unwrap()).unwrap();
        assert_eq!(config["sku"], "Standard_B2s");
        assert_eq!(config["storage_gb"], 64);
        // Original values should still be there
        assert_eq!(config["version"], "16");
    }

    #[tokio::test]
    async fn destroy_resource_removes_from_registry() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'redis' id = 'cache-del' sku = 'Standard' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("DESTROY RESOURCE 'redis' 'cache-del' FORCE")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy resource: {r:?}");

        // Verify it's gone
        let r2 = exec.execute("SELECT * FROM resources").await;
        let arr = r2.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 0);
    }

    #[tokio::test]
    async fn resource_create_select_destroy_lifecycle() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create
        let r1 = exec
            .execute(
                "CREATE RESOURCE 'aks' id = 'k8s-1' node_count = 3 vm_size = 'Standard_DS2_v2' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r1.status, ResultStatus::Ok, "create: {r1:?}");

        // Select
        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0]["id"], "k8s-1");
        assert_eq!(resources[0]["resource_type"], "aks");

        // Destroy
        let r3 = exec
            .execute("DESTROY RESOURCE 'aks' 'k8s-1'")
            .await;
        assert_eq!(r3.status, ResultStatus::Ok, "destroy: {r3:?}");

        // Verify empty
        let r4 = exec.execute("SELECT * FROM resources").await;
        let arr = r4.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 0);
    }

    #[tokio::test]
    async fn resource_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-aud' version = '16' ON PROVIDER 'test-provider'",
        )
        .await;

        let logs = ctx.registry.list_audit_log(None).unwrap();
        let created = logs.iter().find(|l| l.action == "RESOURCE_CREATED");
        assert!(
            created.is_some(),
            "expected RESOURCE_CREATED audit log entry; got: {logs:?}"
        );
        let entry = created.unwrap();
        assert_eq!(entry.target_type.as_deref(), Some("resource"));
        assert_eq!(entry.target_id.as_deref(), Some("db-aud"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Day-2 Operations: BACKUP, RESTORE RESOURCE, SCALE, UPGRADE
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn backup_writes_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a postgres resource first
        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-bak' version = '16' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("BACKUP RESOURCE 'postgres' 'db-bak'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "backup: {r:?}");

        let logs = ctx.registry.list_audit_log(None).unwrap();
        let backed_up = logs.iter().find(|l| l.action == "RESOURCE_BACKED_UP");
        assert!(
            backed_up.is_some(),
            "expected RESOURCE_BACKED_UP audit log entry; got: {logs:?}"
        );
        let entry = backed_up.unwrap();
        assert_eq!(entry.target_type.as_deref(), Some("resource"));
        assert_eq!(entry.target_id.as_deref(), Some("db-bak"));
    }

    #[tokio::test]
    async fn scale_updates_registry() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create an AKS resource first
        exec.execute(
            "CREATE RESOURCE 'aks' id = 'k8s-scale' node_count = 3 ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SCALE RESOURCE 'aks' 'k8s-scale' node_count = 5")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "scale: {r:?}");

        // Verify config was updated in registry
        let row = ctx.registry.get_resource("k8s-scale").unwrap();
        let config: serde_json::Value =
            serde_json::from_str(row.config.as_deref().unwrap_or("{}")).unwrap();
        // node_count should have been updated to 5
        assert_eq!(config["node_count"], 5);

        // Verify audit log
        let logs = ctx.registry.list_audit_log(None).unwrap();
        let scaled = logs.iter().find(|l| l.action == "RESOURCE_SCALED");
        assert!(
            scaled.is_some(),
            "expected RESOURCE_SCALED audit log entry; got: {logs:?}"
        );
    }

    #[tokio::test]
    async fn upgrade_updates_registry() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create an AKS resource first
        exec.execute(
            "CREATE RESOURCE 'aks' id = 'k8s-upg' kubernetes_version = '1.28' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("UPGRADE RESOURCE 'aks' 'k8s-upg' kubernetes_version = '1.29'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "upgrade: {r:?}");

        // Verify config was updated in registry
        let row = ctx.registry.get_resource("k8s-upg").unwrap();
        let config: serde_json::Value =
            serde_json::from_str(row.config.as_deref().unwrap_or("{}")).unwrap();
        assert_eq!(config["kubernetes_version"], "1.29");

        // Verify audit log
        let logs = ctx.registry.list_audit_log(None).unwrap();
        let upgraded = logs.iter().find(|l| l.action == "RESOURCE_UPGRADED");
        assert!(
            upgraded.is_some(),
            "expected RESOURCE_UPGRADED audit log entry; got: {logs:?}"
        );
    }

    #[tokio::test]
    async fn restore_resource_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a postgres resource first
        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-rst' version = '16' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("RESTORE RESOURCE 'postgres' 'db-rst' FROM '2026-04-01T10:00:00Z'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "restore resource: {r:?}");

        let logs = ctx.registry.list_audit_log(None).unwrap();
        let restored = logs.iter().find(|l| l.action == "RESOURCE_RESTORED");
        assert!(
            restored.is_some(),
            "expected RESOURCE_RESTORED audit log entry; got: {logs:?}"
        );
    }

    #[tokio::test]
    async fn backup_wrong_resource_type() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'aks' id = 'k8s-bak' node_count = 3 ON PROVIDER 'test-provider'",
        )
        .await;

        // Backup with wrong resource type — in permissive mode, errors produce Warn
        let r = exec
            .execute("BACKUP RESOURCE 'postgres' 'k8s-bak'")
            .await;
        assert!(
            r.status == ResultStatus::Error || r.status == ResultStatus::Warn,
            "should fail with type mismatch: {r:?}"
        );
        // Verify an error notification was produced
        assert!(
            r.notifications.iter().any(|n| n.level == "ERROR"),
            "expected error notification about type mismatch: {:?}",
            r.notifications
        );
    }

    // ── EXPLAIN ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_explain_shows_plan() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN CREATE MICROVM tenant='acme' vcpus=2 memory_mb=512 image='img-1' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain envelope: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["explain"], true);
        assert!(result["steps"].is_array());
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0]["action"], "CREATE MICROVM");
        assert_eq!(steps[0]["registry_action"], "INSERT INTO microvms");

        // Verify nothing was actually created
        let r2 = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let vms = r2.result.unwrap();
        let vms = vms.as_array().unwrap();
        assert_eq!(vms.len(), 0, "EXPLAIN should not create any microvms");
    }

    #[tokio::test]
    async fn test_explain_resource_shows_az_command() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "EXPLAIN CREATE RESOURCE 'postgres' id='db1' version='16' ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain envelope: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["explain"], true);
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 1);
        assert!(
            steps[0]["command"].as_str().unwrap().contains("postgres"),
            "should show az postgres command: {:?}",
            steps[0]["command"]
        );
        assert!(
            steps[0]["command"].as_str().unwrap().contains("flexible-server"),
            "should show flexible-server subcommand"
        );

        // Verify nothing was actually created
        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let resources = r2.result.unwrap();
        let resources = resources.as_array().unwrap();
        assert_eq!(resources.len(), 0, "EXPLAIN should not create any resources");
    }

    #[tokio::test]
    async fn test_explain_destroy_resource_shows_az_command() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN DESTROY RESOURCE 'redis' 'cache-1'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain envelope: {r:?}");

        let result = r.result.unwrap();
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 1);
        assert!(
            steps[0]["command"].as_str().unwrap().contains("redis"),
            "should show az redis command"
        );
        assert!(
            steps[0]["command"].as_str().unwrap().contains("delete"),
            "should show delete subcommand"
        );
    }

    #[tokio::test]
    async fn test_explain_select() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN SELECT * FROM microvms WHERE status = 'running'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain envelope: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["explain"], true);
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps[0]["action"], "SELECT");
        assert_eq!(steps[0]["from"], "microvms");
        assert!(steps[0]["filters"].as_str().unwrap().contains("status"));
    }

    // ── DRY-RUN MODE ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_dry_run_mode() {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);
        ctx.registry
            .insert_provider(
                "test-provider",
                "kvm",
                "firecracker",
                "healthy",
                true,
                Some("localhost"),
                None,
                "env:X",
                None,
                None,
            )
            .unwrap();

        // Enable dry-run mode
        ctx.dry_run = true;

        let exec = Executor::new(&ctx);

        // Creating a resource in dry-run mode should NOT actually create it
        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id='db1' version='16' ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "dry-run envelope: {r:?}");

        // Should return an explain-style plan
        let result = r.result.unwrap();
        assert_eq!(result["explain"], true);

        // Verify the registry is empty
        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let resources = r2.result.unwrap();
        let resources = resources.as_array().unwrap();
        assert_eq!(resources.len(), 0, "dry-run should not create any resources");
    }

    #[tokio::test]
    async fn test_dry_run_allows_select() {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);

        // Enable dry-run mode
        ctx.dry_run = true;

        let exec = Executor::new(&ctx);

        // SELECT should still work normally in dry-run mode
        let r = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r.status, ResultStatus::Ok, "select in dry-run: {r:?}");
        // Result should be an array (actual execution), not an explain plan
        let result = r.result.unwrap();
        assert!(result.is_array(), "SELECT should return array, not explain plan");
    }

    #[tokio::test]
    async fn test_dry_run_allows_show() {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);

        ctx.dry_run = true;
        let exec = Executor::new(&ctx);

        let r = exec.execute("SHOW VERSION").await;
        assert_eq!(r.status, ResultStatus::Ok, "show in dry-run: {r:?}");
        let result = r.result.unwrap();
        // SHOW VERSION returns a version object, not an explain plan
        assert!(result.get("explain").is_none(), "SHOW should not be wrapped in explain");
    }

    // ── ROLLBACK ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rollback_restores_destroyed_resource() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a resource
        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='db1' name='prod-db' status='available' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create resource: {r:?}");

        // Destroy it (this should capture a snapshot)
        let r = exec
            .execute("DESTROY RESOURCE 'postgres' 'db1'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy resource: {r:?}");

        // Verify it's gone
        assert!(ctx.registry.get_resource("db1").is_err());

        // Rollback last
        let r = exec.execute("ROLLBACK LAST").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["rolled_back"], true);
        assert_eq!(result["target_type"], "resource");
        assert_eq!(result["target_id"], "db1");
        assert!(result["note"].as_str().unwrap().contains("Registry state restored"));

        // Verify it's back
        let row = ctx.registry.get_resource("db1").unwrap();
        assert_eq!(row.resource_type, "postgres");
        // Status is "pending" because the mock provider fails provisioning
        assert_eq!(row.status, "pending");
    }

    #[tokio::test]
    async fn test_rollback_by_tag() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a resource
        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='db2' name='staging-db' status='available' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create: {r:?}");

        // Set a snapshot tag via variable
        let r = exec.execute("SET @snapshot_tag = 'pre-migration'").await;
        assert_eq!(r.status, ResultStatus::Ok, "set tag: {r:?}");

        // Destroy it (this should capture a snapshot with the tag)
        let r = exec
            .execute("DESTROY RESOURCE 'postgres' 'db2'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy: {r:?}");

        // Rollback by tag
        let r = exec.execute("ROLLBACK TO TAG 'pre-migration'").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["rolled_back"], true);

        // Verify resource is back
        let row = ctx.registry.get_resource("db2").unwrap();
        assert_eq!(row.resource_type, "postgres");
    }

    #[tokio::test]
    async fn test_rollback_no_snapshots() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec.execute("ROLLBACK LAST").await;
        // Should fail because there are no snapshots
        assert!(
            r.status == ResultStatus::Error || r.notifications.iter().any(|n| n.level == "ERROR"),
            "should error when no snapshots: {r:?}"
        );
    }

    #[tokio::test]
    async fn test_rollback_resource_specific() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create two resources
        exec.execute("CREATE RESOURCE 'postgres' id='r1' status='available' ON PROVIDER 'test-provider'").await;
        exec.execute("CREATE RESOURCE 'redis' id='r2' status='available' ON PROVIDER 'test-provider'").await;

        // Destroy both
        exec.execute("DESTROY RESOURCE 'postgres' 'r1'").await;
        exec.execute("DESTROY RESOURCE 'redis' 'r2'").await;

        // Rollback only the postgres resource
        let r = exec.execute("ROLLBACK RESOURCE 'resource' 'r1'").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");

        // r1 should be back, r2 should still be gone
        assert!(ctx.registry.get_resource("r1").is_ok());
        assert!(ctx.registry.get_resource("r2").is_err());
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 12: GRANT / REVOKE / ADD PRINCIPAL & SELECT FROM nouns
    // ══════════════════════════════════════════════════════════════════

    // ── test_add_principal_and_grant ─────────────────────────────────

    #[tokio::test]
    async fn test_add_principal_and_grant() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        // Add a principal
        let r = exec
            .execute("ADD PRINCIPAL id='alice' type='user' auth='env:ALICE_TOKEN'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "add principal: {r:?}");
        let val = r.result.unwrap();
        assert_eq!(val["id"], "alice");
        assert_eq!(val["type"], "user");
        assert_eq!(val["enabled"], true);

        // Grant SELECT on microvms
        let r2 = exec
            .execute("GRANT SELECT ON MICROVMS TO 'alice'")
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "grant: {r2:?}");
        let grant_val = r2.result.unwrap();
        assert_eq!(grant_val["principal_id"], "alice");
        assert_eq!(grant_val["scope_type"], "global");

        // Verify via SHOW GRANTS FOR 'alice'
        let r3 = exec
            .execute("SHOW GRANTS FOR 'alice'")
            .await;
        assert_eq!(r3.status, ResultStatus::Ok, "show grants: {r3:?}");
        let grants = r3.result.unwrap();
        let grants = grants.as_array().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0]["principal_id"], "alice");
        assert!(grants[0]["verbs"].as_str().unwrap().contains("SELECT"));
    }

    // ── test_revoke_removes_grant ───────────────────────────────────

    #[tokio::test]
    async fn test_revoke_removes_grant() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        // Add principal and grant
        exec.execute("ADD PRINCIPAL id='bob' type='user' auth='env:BOB_TOKEN'")
            .await;
        exec.execute("GRANT SELECT, CREATE ON MICROVMS TO 'bob'")
            .await;

        // Verify grant exists
        let r = exec.execute("SHOW GRANTS FOR 'bob'").await;
        let grants = r.result.unwrap();
        assert_eq!(grants.as_array().unwrap().len(), 1);

        // Revoke
        let r2 = exec
            .execute("REVOKE SELECT ON MICROVMS FROM 'bob'")
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "revoke: {r2:?}");
        let revoke_val = r2.result.unwrap();
        assert_eq!(revoke_val["revoked_count"], 1);

        // Verify grant is gone
        let r3 = exec.execute("SHOW GRANTS FOR 'bob'").await;
        let grants = r3.result.unwrap();
        assert_eq!(grants.as_array().unwrap().len(), 0);
    }

    // ── test_grant_enforced_when_auth_enabled ───────────────────────

    #[tokio::test]
    async fn test_grant_enforced_when_auth_enabled() {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);

        ctx.registry
            .insert_provider(
                "test-provider", "kvm", "firecracker", "healthy", true,
                Some("localhost"), None, "env:X", None, None,
            )
            .unwrap();

        // Add principal with only SELECT permission
        ctx.registry
            .insert_principal("carol", "user", "env:TOKEN", true)
            .unwrap();
        ctx.registry
            .insert_grant(
                "grant-carol",
                "carol",
                r#"["SELECT"]"#,
                "global",
                None,
                None,
                Some("test"),
            )
            .unwrap();

        ctx.auth_enabled = true;
        ctx.current_principal = Some("carol".to_string());

        let exec = Executor::new(&ctx);

        // SELECT should be permitted
        let r = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r.status, ResultStatus::Ok, "select should succeed: {r:?}");

        // CREATE should be denied
        let r2 = exec
            .execute(
                "CREATE MICROVM id='vm-x' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r2.status, ResultStatus::Error, "create should be denied: {r2:?}");
        assert!(
            r2.notifications
                .iter()
                .any(|n| n.message.contains("AUTH_DENIED")),
            "expected AUTH_DENIED: {:?}",
            r2.notifications,
        );
    }

    // ── test_select_from_audit_log ──────────────────────────────────

    #[tokio::test]
    async fn test_select_from_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Execute a mutation to generate audit log entries
        exec.execute(
            "CREATE MICROVM id='vm-aud-sel' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec.execute("SELECT * FROM audit_log").await;
        assert_eq!(r.status, ResultStatus::Ok, "select audit_log: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert!(
            !rows.is_empty(),
            "expected at least one audit_log row after CREATE"
        );
        // Verify structure
        assert!(rows[0].get("action").is_some());
        assert!(rows[0].get("event_time").is_some());
    }

    // ── test_select_from_query_history ──────────────────────────────

    #[tokio::test]
    async fn test_select_from_query_history() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Execute a SELECT to generate query history
        exec.execute("SELECT * FROM microvms").await;

        let r = exec.execute("SELECT * FROM query_history").await;
        assert_eq!(r.status, ResultStatus::Ok, "select query_history: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert!(
            !rows.is_empty(),
            "expected at least one query_history row"
        );
        assert!(rows[0].get("verb").is_some());
        assert!(rows[0].get("executed_at").is_some());
    }

    // ── test_select_from_principals ─────────────────────────────────

    #[tokio::test]
    async fn test_select_from_principals() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        exec.execute("ADD PRINCIPAL id='dave' type='user' auth='env:DAVE_TOKEN'")
            .await;

        let r = exec.execute("SELECT * FROM principals").await;
        assert_eq!(r.status, ResultStatus::Ok, "select principals: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "dave");
        assert_eq!(rows[0]["type"], "user");
        assert_eq!(rows[0]["enabled"], true);
    }

    // ── test_select_from_grants ─────────────────────────────────────

    #[tokio::test]
    async fn test_select_from_grants() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        exec.execute("ADD PRINCIPAL id='eve' type='user' auth='env:EVE_TOKEN'")
            .await;
        exec.execute("GRANT SELECT, CREATE ON MICROVMS TO 'eve'")
            .await;

        let r = exec.execute("SELECT * FROM grants").await;
        assert_eq!(r.status, ResultStatus::Ok, "select grants: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["principal_id"], "eve");
        assert!(rows[0]["verbs"].as_str().unwrap().contains("SELECT"));
    }

    // ── test_show_clusters ──────────────────────────────────────────

    #[tokio::test]
    async fn test_show_clusters() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute("ADD CLUSTER 'prod-cluster' MEMBERS = ['test-provider']")
            .await;

        let r = exec.execute("SHOW CLUSTERS").await;
        assert_eq!(r.status, ResultStatus::Ok, "show clusters: {r:?}");
        let arr = r.result.unwrap();
        let clusters = arr.as_array().unwrap();
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0]["name"], "prod-cluster");
    }

    // ══════════════════════════════════════════════════════════════════
    // NYI-5: ALTER VOLUME tests
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_alter_volume_labels() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a volume
        let r = exec
            .execute("CREATE VOLUME type='virtio-blk' size_gb=10 ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create volume: {r:?}");
        let vol_id = r.result.unwrap()["id"].as_str().unwrap().to_string();

        // Alter labels
        let r = exec
            .execute(&format!(
                "ALTER VOLUME '{}' SET labels = 'env=prod,tier=fast'",
                vol_id
            ))
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "alter volume labels: {r:?}");

        // Verify label was updated in registry
        let vol = ctx.registry.get_volume(&vol_id).unwrap();
        assert_eq!(vol.labels.as_deref(), Some("env=prod,tier=fast"));

        // Should have a CAP_001 notification
        let has_cap_note = r.notifications.iter().any(|n| n.code == "CAP_001");
        assert!(has_cap_note, "expected CAP_001 notification: {:?}", r.notifications);
    }

    #[tokio::test]
    async fn test_alter_volume_unknown_field() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create a volume
        let r = exec
            .execute("CREATE VOLUME type='virtio-blk' size_gb=10 ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create volume: {r:?}");
        let vol_id = r.result.unwrap()["id"].as_str().unwrap().to_string();

        // Alter with unknown field
        let r = exec
            .execute(&format!(
                "ALTER VOLUME '{}' SET foobar = 'something'",
                vol_id
            ))
            .await;

        // Should succeed (permissive mode) but with a WARN notification
        assert_eq!(r.status, ResultStatus::Ok, "alter unknown: {r:?}");
        let has_warn = r
            .notifications
            .iter()
            .any(|n| n.code == "CAP_002" && n.message.contains("foobar"));
        assert!(has_warn, "expected CAP_002 warning: {:?}", r.notifications);
    }

    // ══════════════════════════════════════════════════════════════════
    // NYI-6: SELECT FROM capabilities
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_select_from_capabilities() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec.execute("SELECT * FROM capabilities").await;
        assert_eq!(r.status, ResultStatus::Ok, "select capabilities: {r:?}");
        let result = r.result.unwrap();
        let rows = result.as_array().unwrap();
        // Mock driver has many capabilities
        assert!(
            !rows.is_empty(),
            "expected capability rows, got none"
        );
        // Each row should have provider_id, capability, supported
        let first = &rows[0];
        assert!(first.get("provider_id").is_some());
        assert!(first.get("capability").is_some());
        assert!(first.get("supported").is_some());
    }

    // ══════════════════════════════════════════════════════════════════
    // Remediation error messages
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_actual_error_has_remediation() {
        // Execute a DESTROY RESOURCE on a nonexistent resource — the error
        // message should contain remediation advice (SELECT * FROM resources).
        // Use strict mode so the error propagates as ResultStatus::Error.
        let mut ctx = setup_with_provider();
        ctx.execution_mode = ExecutionMode::Strict;
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("DESTROY RESOURCE 'postgres' 'nonexistent-db'")
            .await;
        assert_eq!(r.status, ResultStatus::Error, "should fail: {r:?}");

        let error_msg = r
            .notifications
            .iter()
            .find(|n| n.level == "ERROR")
            .map(|n| n.message.clone())
            .unwrap_or_default();

        assert!(
            error_msg.contains("SELECT * FROM resources"),
            "error should include remediation with SELECT query: {error_msg}"
        );
        assert!(
            error_msg.contains("CREATE RESOURCE"),
            "error should include remediation with CREATE RESOURCE: {error_msg}"
        );
    }

    // ── SIMULATION MODE TESTS ─────────────────────────────────────────

    fn setup_simulate() -> EngineContext {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        ctx.simulate = true;
        let driver = Arc::new(kvmql_driver::simulate::SimulationDriver::new("azure"));
        ctx.register_driver("simulate".into(), driver);
        // Also register a provider in the registry so resolve_provider works
        ctx.registry
            .insert_provider(
                "simulate",
                "azure",
                "simulation",
                "healthy",
                true,
                None,
                None,
                "none",
                None,
                None,
            )
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_simulate_create_resource() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id='mydb' name='mydb' version='16' \
                 ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "envelope: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["status"], "simulated");
        assert_eq!(result["_simulated"], true);

        // Verify outputs have realistic FQDN
        let outputs = &result["outputs"];
        assert!(
            outputs["fqdn"].as_str().unwrap().contains("postgres.database.azure.com"),
            "expected realistic FQDN, got: {outputs}"
        );
        assert_eq!(outputs["port"], 5432);
        assert!(outputs["connection_string"].as_str().unwrap().contains("postgresql://"));
    }

    #[tokio::test]
    async fn test_simulate_registry_tracks_simulated() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id='simdb' name='simdb' ON PROVIDER 'simulate'",
        )
        .await;

        // Use SELECT FROM resources to verify status
        let r = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r.status, ResultStatus::Ok);
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], "simulated");
        assert_eq!(rows[0]["id"], "simdb");
    }

    #[tokio::test]
    async fn test_simulate_create_microvm() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='sim-vm-1' tenant='acme' vcpus=2 memory_mb=1024 \
                 image='img-1' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create envelope: {r:?}");

        // Verify it appears in registry
        let r2 = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let vms = arr.as_array().unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0]["id"], "sim-vm-1");
    }

    #[tokio::test]
    async fn test_simulate_no_az_calls() {
        // This test verifies that simulation mode doesn't fail even
        // without az CLI installed (because it never calls it).
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        // Create several resource types -- none should fail
        let types = [
            ("postgres", "pg1"),
            ("redis", "redis1"),
            ("aks", "aks1"),
            ("storage_account", "sa1"),
            ("vnet", "vnet1"),
            ("container_registry", "cr1"),
        ];

        for (rtype, id) in &types {
            let stmt = format!(
                "CREATE RESOURCE '{rtype}' id='{id}' name='{id}' ON PROVIDER 'simulate'"
            );
            let r = exec.execute(&stmt).await;
            assert_eq!(
                r.status,
                ResultStatus::Ok,
                "failed for resource type {rtype}: {r:?}"
            );

            let result = r.result.unwrap();
            assert_eq!(result["status"], "simulated");
            assert_eq!(result["_simulated"], true);
        }
    }

    #[tokio::test]
    async fn test_simulate_destroy() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        // Create a resource
        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id='delsim' name='delsim' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok);

        // Verify it exists
        let r2 = exec.execute("SELECT * FROM resources").await;
        let rows = r2.result.unwrap().as_array().unwrap().len();
        assert_eq!(rows, 1);

        // Destroy it
        let r3 = exec.execute("DESTROY RESOURCE 'postgres' 'delsim'").await;
        assert_eq!(r3.status, ResultStatus::Ok, "destroy: {r3:?}");

        let result = r3.result.unwrap();
        assert_eq!(result["_simulated"], true);

        // Verify resource removed from registry
        let r4 = exec.execute("SELECT * FROM resources").await;
        let rows = r4.result.unwrap().as_array().unwrap().len();
        assert_eq!(rows, 0);
    }

    // ── SELECT FROM plans ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_select_from_plans() {
        let ctx = setup();

        // Insert a plan into the registry
        ctx.registry
            .insert_plan(
                "plan-test1",
                Some("my-plan"),
                "CREATE MICROVM vcpus=2;",
                "{}",
                "abc123",
                None,
            )
            .unwrap();

        let exec = Executor::new(&ctx);
        let result = exec.execute("SELECT * FROM plans;").await;
        assert_eq!(result.status, ResultStatus::Ok);

        let rows = result.result.unwrap();
        let arr = rows.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], "plan-test1");
        assert_eq!(arr[0]["name"], "my-plan");
        assert_eq!(arr[0]["status"], "pending");
    }

    // ── SSH / VM ACCESS CONFIGURATION TESTS ──────────────────────────

    #[tokio::test]
    async fn test_create_microvm_with_ssh_key() {
        std::env::set_var("KVMQL_TEST_SSH_KEY", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleKey test@kvmql");
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='ssh-vm' tenant='acme' vcpus=2 memory_mb=1024 \
                 image='img-1' ssh_key='env:KVMQL_TEST_SSH_KEY' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create with ssh_key failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "ssh-vm");
        assert_eq!(result["status"], "running");
    }

    #[tokio::test]
    async fn test_create_microvm_with_admin_user() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='admin-vm' tenant='acme' vcpus=1 memory_mb=512 \
                 image='img-1' admin_user='deployer' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create with admin_user failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "admin-vm");
    }

    #[tokio::test]
    async fn test_ssh_key_credential_resolution() {
        let key_content = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ test@example";
        std::env::set_var("KVMQL_TEST_SSH_RESOLVE", key_content);

        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='resolve-vm' tenant='acme' vcpus=1 memory_mb=512 \
                 image='img-1' ssh_key='env:KVMQL_TEST_SSH_RESOLVE' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "credential resolution failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "resolve-vm");
        assert_eq!(result["status"], "running");
    }

    #[tokio::test]
    async fn test_password_warning() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='pw-vm' tenant='acme' vcpus=1 memory_mb=512 \
                 image='img-1' password='insecure123' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create with password failed: {r:?}");

        // Should have a WARN notification about password usage
        assert!(
            r.notifications.iter().any(|n| n.code == "SEC_001" && n.level == "WARN"),
            "expected SEC_001 warning for password auth, got: {:?}",
            r.notifications
        );
    }

    #[tokio::test]
    async fn test_cloud_init_from_file() {
        use std::os::unix::fs::PermissionsExt;

        // Create a temp file with cloud-init content
        let path = std::env::temp_dir().join("kvmql-test-cloud-init.yaml");
        std::fs::write(&path, "#cloud-config\npackages:\n  - nginx\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let stmt = format!(
            "CREATE MICROVM id='ci-vm' tenant='acme' vcpus=1 memory_mb=512 \
             image='img-1' cloud_init='file:{}' ON PROVIDER 'simulate'",
            path.to_str().unwrap()
        );
        let r = exec.execute(&stmt).await;
        assert_eq!(r.status, ResultStatus::Ok, "create with cloud_init file ref failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "ci-vm");

        // Clean up
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_create_microvm_with_generate_ssh_key() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='gen-vm' tenant='acme' vcpus=1 memory_mb=512 \
                 image='img-1' ssh_key='generate' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create with ssh_key=generate failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "gen-vm");
    }

    #[tokio::test]
    async fn test_create_microvm_ssh_key_literal_pubkey() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        // A literal SSH public key (not a credential reference) should pass through
        let r = exec
            .execute(
                "CREATE MICROVM id='literal-vm' tenant='acme' vcpus=1 memory_mb=512 \
                 image='img-1' ssh_key='ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDirect user@host' \
                 ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "literal pubkey failed: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["id"], "literal-vm");
    }

    // ── IF NOT EXISTS ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_if_not_exists_skips_duplicate_resource() {
        let ctx = setup_with_provider();
        ctx.simulate.then(|| ()); // Not needed, just using registry directly
        let exec = Executor::new(&ctx);

        // First create succeeds
        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'prod-db' name = 'Production DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

        // Second create WITH IF NOT EXISTS should skip silently
        let r2 = exec
            .execute(
                "CREATE IF NOT EXISTS RESOURCE 'postgres' id = 'prod-db' name = 'Production DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "if not exists: {r2:?}");
        let result = r2.result.unwrap();
        assert_eq!(result["skipped"], true);
        assert_eq!(result["status"], "already_exists");
    }

    #[tokio::test]
    async fn test_without_if_not_exists_errors_on_duplicate_resource() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // First create succeeds
        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'dup-db' name = 'DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

        // Second create WITHOUT IF NOT EXISTS should fail (Warn with error notification)
        let r2 = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'dup-db' name = 'DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        // The engine wraps single-statement errors as Warn with error notifications
        assert_ne!(r2.status, ResultStatus::Ok, "duplicate should not be Ok: {r2:?}");
        assert!(
            r2.notifications.iter().any(|n| n.level == "ERROR"),
            "should have error notification: {r2:?}"
        );
    }

    #[tokio::test]
    async fn test_add_provider_if_not_exists_skips() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        // First add succeeds
        let r = exec
            .execute("ADD PROVIDER id='prov-ine' type='kvm' driver='firecracker' auth='env:X'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first add: {r:?}");

        // Second add WITH IF NOT EXISTS should skip
        let r2 = exec
            .execute("ADD IF NOT EXISTS PROVIDER id='prov-ine' type='kvm' driver='firecracker' auth='env:X'")
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "if not exists: {r2:?}");
        let result = r2.result.unwrap();
        assert_eq!(result["skipped"], true);
        assert_eq!(result["status"], "already_exists");
    }

    #[tokio::test]
    async fn test_if_not_exists_skips_duplicate_microvm() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // First create succeeds
        let r = exec
            .execute(
                "CREATE MICROVM id = 'vm-ine' tenant = 'acme' vcpus = 1 memory_mb = 256 \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

        // Second create WITH IF NOT EXISTS should skip
        let r2 = exec
            .execute(
                "CREATE IF NOT EXISTS MICROVM id = 'vm-ine' tenant = 'acme' vcpus = 1 memory_mb = 256 \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r2.status, ResultStatus::Ok, "if not exists: {r2:?}");
        let result = r2.result.unwrap();
        assert_eq!(result["skipped"], true);
        assert_eq!(result["status"], "already_exists");
    }

    #[tokio::test]
    async fn test_select_from_applied_files() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Insert an applied file record directly via registry
        ctx.registry
            .insert_applied_file("af-test", "demo.kvmql", "somehash", 3, None)
            .unwrap();

        let r = exec.execute("SELECT * FROM applied_files").await;
        assert_eq!(r.status, ResultStatus::Ok, "select: {r:?}");
        let arr = r.result.unwrap();
        let files = arr.as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["file_path"], "demo.kvmql");
        assert_eq!(files[0]["file_hash"], "somehash");
    }
}
