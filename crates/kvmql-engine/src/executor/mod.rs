mod admin;
mod cost;
mod helpers;
mod query;
mod resource;
mod vm;
mod volume;

use std::time::Instant;

use kvmql_auth::access::{AccessChecker, AuthDecision, Grant};
use kvmql_parser::ast::*;
use kvmql_parser::parser::Parser;

use crate::context::{EngineContext, ExecutionMode};
use crate::errors::{with_remediation, EngineError, ErrorContext};
use crate::response::*;

use helpers::{error_envelope, error_envelope_with};

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
                    Some('\\') => {
                        chars.next();
                    } // skip escaped char
                    None => break,
                    _ => {}
                }
            }
            result.push('?');
        } else if ch.is_ascii_digit()
            || (ch == '-' && {
                // Check if minus sign is a numeric prefix (preceded by operator context)
                let trimmed = result.trim_end();
                trimmed.is_empty()
                    || trimmed.ends_with('=')
                    || trimmed.ends_with('>')
                    || trimmed.ends_with('<')
                    || trimmed.ends_with(',')
                    || trimmed.ends_with('(')
            })
        {
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
        // EXPLAIN / EXPLAIN COST / EXEC are introspection or meta, never a mutation
        Statement::Explain(_) | Statement::ExplainCost(_) | Statement::ExecFile(_) => None,
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
        Statement::ImportResources(_) => Some(AuditEvent {
            action: "IMPORT",
            target_type: "resources",
            target_id: None,
        }),
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
        Statement::ExplainCost(_) => "EXPLAIN_COST",
        Statement::Rollback(_) => "ROLLBACK",
        Statement::Assert(_) => "ASSERT",
        Statement::ImportResources(_) => "IMPORT",
        Statement::ExecFile(_) => "EXEC",
    }
}

/// Returns true if this statement type should skip query-history recording.
fn skip_history(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Set(_)
            | Statement::Show(_)
            | Statement::Explain(_)
            | Statement::ExplainCost(_)
            | Statement::Assert(_)
    )
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

pub struct Executor<'a> {
    pub(super) ctx: &'a EngineContext,
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
                    None, // reason
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
                Err(e) => {
                    let msg = e.to_string();
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
        let should_record = program.statements.iter().any(|s| !skip_history(s));
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
            None, // targets
            Some(duration),
            status_str,
            notif_json.as_deref(),
            Some(rows_affected),
            None, // result_hash
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
                let verbs: Vec<String> = serde_json::from_str(&row.verbs).unwrap_or_default();
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

pub(super) struct StmtOutcome {
    pub(super) result: Option<serde_json::Value>,
    pub(super) rows_affected: i64,
    pub(super) notifications: Vec<Notification>,
}

impl StmtOutcome {
    pub(super) fn ok_val(val: serde_json::Value) -> Self {
        Self {
            result: Some(val),
            rows_affected: 1,
            notifications: Vec::new(),
        }
    }

    pub(super) fn ok_rows(val: serde_json::Value, n: i64) -> Self {
        Self {
            result: Some(val),
            rows_affected: n,
            notifications: Vec::new(),
        }
    }

    pub(super) fn ok_empty() -> Self {
        Self {
            result: None,
            rows_affected: 1,
            notifications: Vec::new(),
        }
    }

    pub(super) fn not_implemented(stmt_name: &str) -> Self {
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
    async fn execute_statement(&self, stmt: &Statement) -> Result<StmtOutcome, EngineError> {
        // Dry-run mode: wrap mutations in EXPLAIN automatically, but let
        // read-only statements (SELECT, SHOW, SET, EXPLAIN) through.
        if self.ctx.dry_run
            && !matches!(
                stmt,
                Statement::Explain(_)
                    | Statement::ExplainCost(_)
                    | Statement::Select(_)
                    | Statement::Show(_)
                    | Statement::Set(_)
                    | Statement::Assert(_)
                    | Statement::ImportResources(_)
                    | Statement::ExecFile(_)
            )
        {
            return self.exec_explain(stmt).await;
        }

        match stmt {
            Statement::Explain(inner) => self.exec_explain(inner).await,
            Statement::ExplainCost(inner) => self.exec_explain_cost(inner).await,
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
            Statement::ImportResources(s) => self.exec_import_resources(s),
            Statement::ExecFile(path) => self.exec_file(path).await,
        }
    }

    // =======================================================================
    // EXEC FILE
    // =======================================================================

    fn exec_file(
        &self,
        path: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<StmtOutcome, EngineError>> + '_>>
    {
        let path = path.to_string();
        Box::pin(async move {
            let source = std::fs::read_to_string(&path).map_err(|e| -> EngineError {
                format!("failed to read file '{path}': {e}").into()
            })?;
            let result = self.execute(&source).await;
            Ok(StmtOutcome::ok_val(serde_json::json!({
                "file": path,
                "status": result.status,
                "rows_affected": result.rows_affected,
                "notifications": result.notifications.len(),
            })))
        })
    }

    // =======================================================================
    // EXPLAIN
    // =======================================================================

    async fn exec_explain(&self, stmt: &Statement) -> Result<StmtOutcome, EngineError> {
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
                            args[1..]
                                .iter()
                                .map(|a| a.as_str())
                                .collect::<Vec<_>>()
                                .join(" ")
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
                            "error": e.to_string(),
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
                        self.get_aws_provisioner("default")
                            .build_delete_args(rtype, &s.id)
                    } else {
                        self.get_azure_provisioner("default")
                            .build_delete_args(rtype, &s.id)
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
                                "error": e.to_string(),
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
                    })
                    .to_string();
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
                        })
                        .to_string()
                    }),
                    DestroyTarget::Volume => self.ctx.registry.get_volume(&s.id).ok().map(|r| {
                        serde_json::json!({
                            "id": r.id, "provider_id": r.provider_id, "size_gb": r.size_gb,
                            "status": r.status, "volume_type": r.volume_type,
                            "iops": r.iops, "encrypted": r.encrypted, "labels": r.labels,
                        })
                        .to_string()
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
                    })
                    .to_string();
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
                    })
                    .to_string();
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
                    })
                    .to_string();
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
}

// ---------------------------------------------------------------------------
// Helpers: extract params — used across multiple sub-modules
// ---------------------------------------------------------------------------

pub(super) fn get_param(params: &[Param], key: &str) -> Option<String> {
    params
        .iter()
        .find(|p| p.key == key)
        .and_then(|p| match &p.value {
            Value::String(s) => Some(s.clone()),
            Value::Integer(n) => Some(n.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Boolean(b) => Some(b.to_string()),
            _ => None,
        })
}

pub(super) fn get_param_i64(params: &[Param], key: &str) -> Option<i64> {
    params
        .iter()
        .find(|p| p.key == key)
        .and_then(|p| match &p.value {
            Value::Integer(n) => Some(*n),
            Value::String(s) => s.parse().ok(),
            _ => None,
        })
}

pub(super) fn get_param_bool(params: &[Param], key: &str) -> Option<bool> {
    params
        .iter()
        .find(|p| p.key == key)
        .and_then(|p| match &p.value {
            Value::Boolean(b) => Some(*b),
            Value::String(s) => match s.as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        })
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
        let ctx = EngineContext::new(registry);
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
            .execute(
                "CREATE VOLUME id='vol-1' size_gb=20 type='virtio-blk' ON PROVIDER 'test-provider'",
            )
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

    #[tokio::test]
    async fn test_audit_log_on_destroy() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

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

    #[test]
    fn test_normalized_statement() {
        let input = "SELECT * FROM microvms WHERE tenant = 'acme' AND cpu_pct > 50";
        let normalized = normalize_statement(input);
        assert_eq!(
            normalized,
            "SELECT * FROM microvms WHERE tenant = ? AND cpu_pct > ?"
        );

        let input2 = "CREATE MICROVM id='vm-1' tenant='my tenant' vcpus=2 memory_mb=512";
        let normalized2 = normalize_statement(input2);
        assert_eq!(
            normalized2,
            "CREATE MICROVM id=? tenant=? vcpus=? memory_mb=?"
        );

        let input3 = "ADD PROVIDER id='p1' auth='s3cr3t-k3y'";
        let normalized3 = normalize_statement(input3);
        assert_eq!(normalized3, "ADD PROVIDER id=? auth=?");
        assert!(
            !normalized3.contains("s3cr3t"),
            "normalized statement must not contain credential values"
        );
    }

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

    #[tokio::test]
    async fn test_auth_enabled_blocks_unauthorized() {
        let ctx = setup_with_auth("usr-ro", r#"["SELECT"]"#);
        let exec = Executor::new(&ctx);

        let r = exec.execute("DESTROY MICROVM 'vm-nonexistent'").await;
        assert_eq!(r.status, ResultStatus::Error, "expected auth denial: {r:?}");
        assert!(
            r.notifications
                .iter()
                .any(|n| n.message.contains("AUTH_DENIED")),
            "expected AUTH_DENIED in notifications: {:?}",
            r.notifications,
        );
    }

    #[tokio::test]
    async fn test_auth_enabled_permits_authorized() {
        let ctx = setup_with_auth("usr-admin", r#"["SELECT","CREATE","DESTROY"]"#);
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='vm-auth' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create should succeed: {r:?}");

        let r2 = exec.execute("DESTROY MICROVM 'vm-auth'").await;
        assert_eq!(
            r2.status,
            ResultStatus::Ok,
            "destroy should succeed: {r2:?}"
        );
    }

    #[tokio::test]
    async fn test_auth_disabled_permits_all() {
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

    #[tokio::test]
    async fn watch_returns_single_sample_with_notification() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='vm-w1' tenant='acme' vcpus=2 memory_mb=512 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("WATCH METRIC * FROM microvms INTERVAL 5s")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "watch should return Ok: {r:?}");
        assert!(r.result.is_some(), "watch should return result data");

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

    #[tokio::test]
    async fn test_publish_image_mock() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

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

        assert_eq!(r.status, ResultStatus::Ok, "publish should succeed: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["image_id"], "img-pub");
        assert_eq!(result["provider"], "test-provider");
        assert_eq!(result["status"], "published");
        assert!(
            result["cloud_ref"].as_str().unwrap().contains("img-pub"),
            "cloud_ref should reference image id"
        );

        let image = ctx.registry.get_image("img-pub").unwrap();
        assert!(
            image.cloud_ref.is_some(),
            "cloud_ref should be set in registry"
        );
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
        let status = val["status"].as_str().unwrap();
        assert!(
            status == "created" || status == "pending",
            "expected status 'created' or 'pending', got '{status}'"
        );

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

        let row = ctx.registry.get_resource("db-alt").unwrap();
        let config: serde_json::Value =
            serde_json::from_str(row.config.as_deref().unwrap()).unwrap();
        assert_eq!(config["sku"], "Standard_B2s");
        assert_eq!(config["storage_gb"], 64);
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

        let r2 = exec.execute("SELECT * FROM resources").await;
        let arr = r2.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 0);
    }

    #[tokio::test]
    async fn resource_create_select_destroy_lifecycle() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r1 = exec
            .execute(
                "CREATE RESOURCE 'aks' id = 'k8s-1' node_count = 3 vm_size = 'Standard_DS2_v2' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r1.status, ResultStatus::Ok, "create: {r1:?}");

        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let resources = arr.as_array().unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0]["id"], "k8s-1");
        assert_eq!(resources[0]["resource_type"], "aks");

        let r3 = exec.execute("DESTROY RESOURCE 'aks' 'k8s-1'").await;
        assert_eq!(r3.status, ResultStatus::Ok, "destroy: {r3:?}");

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

    #[tokio::test]
    async fn backup_writes_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id = 'db-bak' version = '16' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec.execute("BACKUP RESOURCE 'postgres' 'db-bak'").await;
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

        exec.execute(
            "CREATE RESOURCE 'aks' id = 'k8s-scale' node_count = 3 ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SCALE RESOURCE 'aks' 'k8s-scale' node_count = 5")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "scale: {r:?}");

        let row = ctx.registry.get_resource("k8s-scale").unwrap();
        let config: serde_json::Value =
            serde_json::from_str(row.config.as_deref().unwrap_or("{}")).unwrap();
        assert_eq!(config["node_count"], 5);

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

        exec.execute(
            "CREATE RESOURCE 'aks' id = 'k8s-upg' kubernetes_version = '1.28' ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("UPGRADE RESOURCE 'aks' 'k8s-upg' kubernetes_version = '1.29'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "upgrade: {r:?}");

        let row = ctx.registry.get_resource("k8s-upg").unwrap();
        let config: serde_json::Value =
            serde_json::from_str(row.config.as_deref().unwrap_or("{}")).unwrap();
        assert_eq!(config["kubernetes_version"], "1.29");

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

        let r = exec.execute("BACKUP RESOURCE 'postgres' 'k8s-bak'").await;
        assert!(
            r.status == ResultStatus::Error || r.status == ResultStatus::Warn,
            "should fail with type mismatch: {r:?}"
        );
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
            steps[0]["command"]
                .as_str()
                .unwrap()
                .contains("flexible-server"),
            "should show flexible-server subcommand"
        );

        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let resources = r2.result.unwrap();
        let resources = resources.as_array().unwrap();
        assert_eq!(
            resources.len(),
            0,
            "EXPLAIN should not create any resources"
        );
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

        ctx.dry_run = true;

        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='db1' version='16' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "dry-run envelope: {r:?}");

        let result = r.result.unwrap();
        assert_eq!(result["explain"], true);

        let r2 = exec.execute("SELECT * FROM resources").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let resources = r2.result.unwrap();
        let resources = resources.as_array().unwrap();
        assert_eq!(
            resources.len(),
            0,
            "dry-run should not create any resources"
        );
    }

    #[tokio::test]
    async fn test_dry_run_allows_select() {
        let registry = Registry::open_in_memory().unwrap();
        let mut ctx = EngineContext::new(registry);
        let driver = Arc::new(MockDriver::new());
        ctx.register_driver("test-provider".into(), driver);

        ctx.dry_run = true;

        let exec = Executor::new(&ctx);

        let r = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r.status, ResultStatus::Ok, "select in dry-run: {r:?}");
        let result = r.result.unwrap();
        assert!(
            result.is_array(),
            "SELECT should return array, not explain plan"
        );
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
        assert!(
            result.get("explain").is_none(),
            "SHOW should not be wrapped in explain"
        );
    }

    // ── ROLLBACK ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rollback_restores_destroyed_resource() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='db1' name='prod-db' status='available' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create resource: {r:?}");

        let r = exec.execute("DESTROY RESOURCE 'postgres' 'db1'").await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy resource: {r:?}");

        assert!(ctx.registry.get_resource("db1").is_err());

        let r = exec.execute("ROLLBACK LAST").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["rolled_back"], true);
        assert_eq!(result["target_type"], "resource");
        assert_eq!(result["target_id"], "db1");
        assert!(result["note"]
            .as_str()
            .unwrap()
            .contains("Registry state restored"));

        let row = ctx.registry.get_resource("db1").unwrap();
        assert_eq!(row.resource_type, "postgres");
        assert_eq!(row.status, "pending");
    }

    #[tokio::test]
    async fn test_rollback_by_tag() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='db2' name='staging-db' status='available' ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create: {r:?}");

        let r = exec.execute("SET @snapshot_tag = 'pre-migration'").await;
        assert_eq!(r.status, ResultStatus::Ok, "set tag: {r:?}");

        let r = exec.execute("DESTROY RESOURCE 'postgres' 'db2'").await;
        assert_eq!(r.status, ResultStatus::Ok, "destroy: {r:?}");

        let r = exec.execute("ROLLBACK TO TAG 'pre-migration'").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");
        let result = r.result.unwrap();
        assert_eq!(result["rolled_back"], true);

        let row = ctx.registry.get_resource("db2").unwrap();
        assert_eq!(row.resource_type, "postgres");
    }

    #[tokio::test]
    async fn test_rollback_no_snapshots() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec.execute("ROLLBACK LAST").await;
        assert!(
            r.status == ResultStatus::Error || r.notifications.iter().any(|n| n.level == "ERROR"),
            "should error when no snapshots: {r:?}"
        );
    }

    #[tokio::test]
    async fn test_rollback_resource_specific() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE RESOURCE 'postgres' id='r1' status='available' ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE RESOURCE 'redis' id='r2' status='available' ON PROVIDER 'test-provider'",
        )
        .await;

        exec.execute("DESTROY RESOURCE 'postgres' 'r1'").await;
        exec.execute("DESTROY RESOURCE 'redis' 'r2'").await;

        let r = exec.execute("ROLLBACK RESOURCE 'resource' 'r1'").await;
        assert_eq!(r.status, ResultStatus::Ok, "rollback: {r:?}");

        assert!(ctx.registry.get_resource("r1").is_ok());
        assert!(ctx.registry.get_resource("r2").is_err());
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 12: GRANT / REVOKE / ADD PRINCIPAL & SELECT FROM nouns
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_add_principal_and_grant() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("ADD PRINCIPAL id='alice' type='user' auth='env:ALICE_TOKEN'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "add principal: {r:?}");
        let val = r.result.unwrap();
        assert_eq!(val["id"], "alice");
        assert_eq!(val["type"], "user");
        assert_eq!(val["enabled"], true);

        let r2 = exec.execute("GRANT SELECT ON MICROVMS TO 'alice'").await;
        assert_eq!(r2.status, ResultStatus::Ok, "grant: {r2:?}");
        let grant_val = r2.result.unwrap();
        assert_eq!(grant_val["principal_id"], "alice");
        assert_eq!(grant_val["scope_type"], "global");

        let r3 = exec.execute("SHOW GRANTS FOR 'alice'").await;
        assert_eq!(r3.status, ResultStatus::Ok, "show grants: {r3:?}");
        let grants = r3.result.unwrap();
        let grants = grants.as_array().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0]["principal_id"], "alice");
        assert!(grants[0]["verbs"].as_str().unwrap().contains("SELECT"));
    }

    #[tokio::test]
    async fn test_revoke_removes_grant() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        exec.execute("ADD PRINCIPAL id='bob' type='user' auth='env:BOB_TOKEN'")
            .await;
        exec.execute("GRANT SELECT, CREATE ON MICROVMS TO 'bob'")
            .await;

        let r = exec.execute("SHOW GRANTS FOR 'bob'").await;
        let grants = r.result.unwrap();
        assert_eq!(grants.as_array().unwrap().len(), 1);

        let r2 = exec.execute("REVOKE SELECT ON MICROVMS FROM 'bob'").await;
        assert_eq!(r2.status, ResultStatus::Ok, "revoke: {r2:?}");
        let revoke_val = r2.result.unwrap();
        assert_eq!(revoke_val["revoked_count"], 1);

        let r3 = exec.execute("SHOW GRANTS FOR 'bob'").await;
        let grants = r3.result.unwrap();
        assert_eq!(grants.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_grant_enforced_when_auth_enabled() {
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

        let r = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r.status, ResultStatus::Ok, "select should succeed: {r:?}");

        let r2 = exec
            .execute(
                "CREATE MICROVM id='vm-x' tenant='acme' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(
            r2.status,
            ResultStatus::Error,
            "create should be denied: {r2:?}"
        );
        assert!(
            r2.notifications
                .iter()
                .any(|n| n.message.contains("AUTH_DENIED")),
            "expected AUTH_DENIED: {:?}",
            r2.notifications,
        );
    }

    #[tokio::test]
    async fn test_select_from_audit_log() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

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
        assert!(rows[0].get("action").is_some());
        assert!(rows[0].get("event_time").is_some());
    }

    #[tokio::test]
    async fn test_select_from_query_history() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute("SELECT * FROM microvms").await;

        let r = exec.execute("SELECT * FROM query_history").await;
        assert_eq!(r.status, ResultStatus::Ok, "select query_history: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert!(!rows.is_empty(), "expected at least one query_history row");
        assert!(rows[0].get("verb").is_some());
        assert!(rows[0].get("executed_at").is_some());
    }

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

    #[tokio::test]
    async fn test_alter_volume_labels() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE VOLUME type='virtio-blk' size_gb=10 ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create volume: {r:?}");
        let vol_id = r.result.unwrap()["id"].as_str().unwrap().to_string();

        let r = exec
            .execute(&format!(
                "ALTER VOLUME '{}' SET labels = 'env=prod,tier=fast'",
                vol_id
            ))
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "alter volume labels: {r:?}");

        let vol = ctx.registry.get_volume(&vol_id).unwrap();
        assert_eq!(vol.labels.as_deref(), Some("env=prod,tier=fast"));

        let has_cap_note = r.notifications.iter().any(|n| n.code == "CAP_001");
        assert!(
            has_cap_note,
            "expected CAP_001 notification: {:?}",
            r.notifications
        );
    }

    #[tokio::test]
    async fn test_alter_volume_unknown_field() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("CREATE VOLUME type='virtio-blk' size_gb=10 ON PROVIDER 'test-provider'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "create volume: {r:?}");
        let vol_id = r.result.unwrap()["id"].as_str().unwrap().to_string();

        let r = exec
            .execute(&format!(
                "ALTER VOLUME '{}' SET foobar = 'something'",
                vol_id
            ))
            .await;

        assert_eq!(r.status, ResultStatus::Ok, "alter unknown: {r:?}");
        let has_warn = r
            .notifications
            .iter()
            .any(|n| n.code == "CAP_002" && n.message.contains("foobar"));
        assert!(has_warn, "expected CAP_002 warning: {:?}", r.notifications);
    }

    #[tokio::test]
    async fn test_select_from_capabilities() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        let r = exec.execute("SELECT * FROM capabilities").await;
        assert_eq!(r.status, ResultStatus::Ok, "select capabilities: {r:?}");
        let result = r.result.unwrap();
        let rows = result.as_array().unwrap();
        assert!(!rows.is_empty(), "expected capability rows, got none");
        let first = &rows[0];
        assert!(first.get("provider_id").is_some());
        assert!(first.get("capability").is_some());
        assert!(first.get("supported").is_some());
    }

    #[tokio::test]
    async fn test_actual_error_has_remediation() {
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

        let outputs = &result["outputs"];
        assert!(
            outputs["fqdn"]
                .as_str()
                .unwrap()
                .contains("postgres.database.azure.com"),
            "expected realistic FQDN, got: {outputs}"
        );
        assert_eq!(outputs["port"], 5432);
        assert!(outputs["connection_string"]
            .as_str()
            .unwrap()
            .contains("postgresql://"));
    }

    #[tokio::test]
    async fn test_simulate_registry_tracks_simulated() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        exec.execute("CREATE RESOURCE 'postgres' id='simdb' name='simdb' ON PROVIDER 'simulate'")
            .await;

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

        let r2 = exec.execute("SELECT * FROM microvms").await;
        assert_eq!(r2.status, ResultStatus::Ok);
        let arr = r2.result.unwrap();
        let vms = arr.as_array().unwrap();
        assert_eq!(vms.len(), 1);
        assert_eq!(vms[0]["id"], "sim-vm-1");
    }

    #[tokio::test]
    async fn test_simulate_no_az_calls() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let types = [
            ("postgres", "pg1"),
            ("redis", "redis1"),
            ("aks", "aks1"),
            ("storage_account", "sa1"),
            ("vnet", "vnet1"),
            ("container_registry", "cr1"),
        ];

        for (rtype, id) in &types {
            let stmt =
                format!("CREATE RESOURCE '{rtype}' id='{id}' name='{id}' ON PROVIDER 'simulate'");
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

        let r = exec
            .execute("CREATE RESOURCE 'postgres' id='delsim' name='delsim' ON PROVIDER 'simulate'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok);

        let r2 = exec.execute("SELECT * FROM resources").await;
        let rows = r2.result.unwrap().as_array().unwrap().len();
        assert_eq!(rows, 1);

        let r3 = exec.execute("DESTROY RESOURCE 'postgres' 'delsim'").await;
        assert_eq!(r3.status, ResultStatus::Ok, "destroy: {r3:?}");

        let result = r3.result.unwrap();
        assert_eq!(result["_simulated"], true);

        let r4 = exec.execute("SELECT * FROM resources").await;
        let rows = r4.result.unwrap().as_array().unwrap().len();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn test_select_from_plans() {
        let ctx = setup();

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

    #[tokio::test]
    async fn test_create_microvm_with_ssh_key() {
        std::env::set_var(
            "KVMQL_TEST_SSH_KEY",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleKey test@kvmql",
        );
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE MICROVM id='ssh-vm' tenant='acme' vcpus=2 memory_mb=1024 \
                 image='img-1' ssh_key='env:KVMQL_TEST_SSH_KEY' ON PROVIDER 'simulate'",
            )
            .await;
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "create with ssh_key failed: {r:?}"
        );

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
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "create with admin_user failed: {r:?}"
        );

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
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "credential resolution failed: {r:?}"
        );

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
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "create with password failed: {r:?}"
        );

        assert!(
            r.notifications
                .iter()
                .any(|n| n.code == "SEC_001" && n.level == "WARN"),
            "expected SEC_001 warning for password auth, got: {:?}",
            r.notifications
        );
    }

    #[tokio::test]
    async fn test_cloud_init_from_file() {
        use std::os::unix::fs::PermissionsExt;

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
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "create with cloud_init file ref failed: {r:?}"
        );

        let result = r.result.unwrap();
        assert_eq!(result["id"], "ci-vm");

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
        assert_eq!(
            r.status,
            ResultStatus::Ok,
            "create with ssh_key=generate failed: {r:?}"
        );

        let result = r.result.unwrap();
        assert_eq!(result["id"], "gen-vm");
    }

    #[tokio::test]
    async fn test_create_microvm_ssh_key_literal_pubkey() {
        let ctx = setup_simulate();
        let exec = Executor::new(&ctx);

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
        ctx.simulate.then_some(());
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'prod-db' name = 'Production DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

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

        let r = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'dup-db' name = 'DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

        let r2 = exec
            .execute(
                "CREATE RESOURCE 'postgres' id = 'dup-db' name = 'DB' \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_ne!(
            r2.status,
            ResultStatus::Ok,
            "duplicate should not be Ok: {r2:?}"
        );
        assert!(
            r2.notifications.iter().any(|n| n.level == "ERROR"),
            "should have error notification: {r2:?}"
        );
    }

    #[tokio::test]
    async fn test_add_provider_if_not_exists_skips() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("ADD PROVIDER id='prov-ine' type='kvm' driver='firecracker' auth='env:X'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first add: {r:?}");

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

        let r = exec
            .execute(
                "CREATE MICROVM id = 'vm-ine' tenant = 'acme' vcpus = 1 memory_mb = 256 \
                 ON PROVIDER 'test-provider'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "first create: {r:?}");

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

    // ── EXPLAIN COST ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_explain_cost_eks_cluster() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN COST CREATE RESOURCE 'eks_cluster' id = 'my-cluster' name = 'prod'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain cost envelope: {r:?}");

        let result = r.result.unwrap();
        let rows = result.as_array().unwrap();
        // Should have the resource row + TOTAL row
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["resource"], "my-cluster");
        assert_eq!(rows[0]["type"], "eks_cluster");
        assert_eq!(rows[0]["hourly"], "0.100");
        assert_eq!(rows[0]["monthly"], "73.00");
        // TOTAL row
        assert_eq!(rows[1]["resource"], "TOTAL");
        assert_eq!(rows[1]["hourly"], "0.100");
        assert_eq!(rows[1]["monthly"], "73.00");
    }

    #[tokio::test]
    async fn test_explain_cost_free_resource() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN COST CREATE RESOURCE 'vpc' id = 'my-vpc'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain cost vpc: {r:?}");

        let result = r.result.unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["resource"], "my-vpc");
        assert_eq!(rows[0]["hourly"], "0.000");
        assert_eq!(rows[0]["monthly"], "0.00");
    }

    #[tokio::test]
    async fn test_explain_cost_rds_postgres() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute(
                "EXPLAIN COST CREATE RESOURCE 'rds_postgres' id = 'db1' instance_class = 'db.r5.large'",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain cost rds: {r:?}");

        let result = r.result.unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["resource"], "db1");
        assert_eq!(rows[0]["type"], "rds_postgres");
        assert_eq!(rows[0]["hourly"], "0.240");
        assert_eq!(rows[0]["monthly"], "175.20");
    }

    #[tokio::test]
    async fn test_explain_cost_populates_cost_estimate_table() {
        let ctx = setup();
        let exec = Executor::new(&ctx);

        let r = exec
            .execute("EXPLAIN COST CREATE RESOURCE 'nat_gateway' id = 'nat1'")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "explain cost: {r:?}");

        // The cost_estimate table should now be queryable
        let r2 = exec.execute("SELECT * FROM cost_estimate").await;
        assert_eq!(r2.status, ResultStatus::Ok, "select cost_estimate: {r2:?}");
        let arr = r2.result.unwrap();
        let estimates = arr.as_array().unwrap();
        assert_eq!(estimates.len(), 1);
        assert_eq!(estimates[0]["resource_id"], "nat1");
        assert_eq!(estimates[0]["resource_type"], "nat_gateway");
    }

    // ══════════════════════════════════════════════════════════════════
    // GROUP BY, ORDER BY, OFFSET tests
    // ══════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn select_group_by_with_count() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        // Create microvms across two tenants
        for id in &["ga1", "ga2", "ga3"] {
            exec.execute(&format!(
                "CREATE MICROVM id='{}' tenant='alpha' vcpus=1 memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
                id
            ))
            .await;
        }
        for id in &["gb1", "gb2"] {
            exec.execute(&format!(
                "CREATE MICROVM id='{}' tenant='beta' vcpus=2 memory_mb=512 image='img-1' \
                 ON PROVIDER 'test-provider'",
                id
            ))
            .await;
        }

        let r = exec
            .execute("SELECT tenant, count(*) AS count FROM microvms GROUP BY tenant")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "group by: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 2, "expected 2 groups, got: {rows:?}");

        // Find alpha and beta groups
        let alpha = rows.iter().find(|r| r["tenant"] == "alpha").unwrap();
        let beta = rows.iter().find(|r| r["tenant"] == "beta").unwrap();
        assert_eq!(alpha["count"], 3);
        assert_eq!(beta["count"], 2);
    }

    #[tokio::test]
    async fn select_group_by_with_sum_avg() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='s1' tenant='alpha' vcpus=2 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='s2' tenant='alpha' vcpus=4 memory_mb=512 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='s3' tenant='beta' vcpus=8 memory_mb=1024 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute(
                "SELECT tenant, sum(vcpus) AS total_vcpus, avg(memory_mb) AS avg_mem \
                 FROM microvms GROUP BY tenant",
            )
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "group by sum/avg: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 2);

        let alpha = rows.iter().find(|r| r["tenant"] == "alpha").unwrap();
        assert_eq!(alpha["total_vcpus"], 6.0);
        assert_eq!(alpha["avg_mem"], 384.0);

        let beta = rows.iter().find(|r| r["tenant"] == "beta").unwrap();
        assert_eq!(beta["total_vcpus"], 8.0);
        assert_eq!(beta["avg_mem"], 1024.0);
    }

    #[tokio::test]
    async fn select_order_by_asc() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='ob1' tenant='charlie' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='ob2' tenant='alpha' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='ob3' tenant='beta' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SELECT * FROM microvms ORDER BY tenant ASC")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "order by: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["tenant"], "alpha");
        assert_eq!(rows[1]["tenant"], "beta");
        assert_eq!(rows[2]["tenant"], "charlie");
    }

    #[tokio::test]
    async fn select_order_by_desc() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='od1' tenant='t' vcpus=1 memory_mb=128 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='od2' tenant='t' vcpus=4 memory_mb=512 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='od3' tenant='t' vcpus=2 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        let r = exec
            .execute("SELECT * FROM microvms ORDER BY vcpus DESC")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "order by desc: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["vcpus"], 4);
        assert_eq!(rows[1]["vcpus"], 2);
        assert_eq!(rows[2]["vcpus"], 1);
    }

    #[tokio::test]
    async fn select_with_offset_and_limit() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        for i in 1..=5 {
            exec.execute(&format!(
                "CREATE MICROVM id='ol{}' tenant='t' vcpus={} memory_mb=256 image='img-1' \
                 ON PROVIDER 'test-provider'",
                i, i
            ))
            .await;
        }

        let r = exec
            .execute("SELECT * FROM microvms ORDER BY vcpus ASC LIMIT 2 OFFSET 1")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "offset+limit: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["vcpus"], 2);
        assert_eq!(rows[1]["vcpus"], 3);
    }

    #[tokio::test]
    async fn select_group_by_without_aggregates() {
        let ctx = setup_with_provider();
        let exec = Executor::new(&ctx);

        exec.execute(
            "CREATE MICROVM id='ng1' tenant='alpha' vcpus=1 memory_mb=256 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='ng2' tenant='alpha' vcpus=2 memory_mb=512 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;
        exec.execute(
            "CREATE MICROVM id='ng3' tenant='beta' vcpus=4 memory_mb=1024 image='img-1' \
             ON PROVIDER 'test-provider'",
        )
        .await;

        // GROUP BY without aggregates should still deduplicate by tenant
        let r = exec
            .execute("SELECT tenant FROM microvms GROUP BY tenant")
            .await;
        assert_eq!(r.status, ResultStatus::Ok, "group by no agg: {r:?}");
        let arr = r.result.unwrap();
        let rows = arr.as_array().unwrap();
        assert_eq!(rows.len(), 2, "expected 2 groups: {rows:?}");
    }
}
