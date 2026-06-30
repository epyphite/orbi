use kvmql_parser::ast::*;

use crate::errors::{with_remediation, EngineError, ErrorContext};
use crate::response::*;

use super::helpers::{
    build_file_stat_row, compare_json, eval_binary_op, eval_predicate, project_rows,
    run_table_function,
};
use super::{Executor, StmtOutcome};

impl<'a> Executor<'a> {
    // =======================================================================
    // SELECT
    // =======================================================================

    pub(super) async fn exec_select(&self, s: &SelectStmt) -> Result<StmtOutcome, EngineError> {
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
            Noun::K8sPods
            | Noun::K8sDeployments
            | Noun::K8sServices
            | Noun::K8sIngresses
            | Noun::K8sConfigmaps
            | Noun::K8sSecrets
            | Noun::K8sNamespaces
            | Noun::K8sNodes => {
                if self.ctx.simulate {
                    Vec::new()
                } else {
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
                    let namespace: Option<&str> = None;

                    engine
                        .query(&noun_str, namespace)
                        .map_err(|e| format!("k8s query failed: {e}"))?
                }
            }
            Noun::ImportLog => {
                let list = self
                    .ctx
                    .registry
                    .list_import_log()
                    .map_err(|e| format!("failed to query import_log: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "provider_id": r.provider_id,
                            "resource_type": r.resource_type,
                            "resource_id": r.resource_id,
                            "action": r.action,
                            "details": r.details,
                            "imported_at": r.imported_at,
                        })
                    })
                    .collect()
            }
            Noun::CostEstimate => {
                let list = self
                    .ctx
                    .registry
                    .list_cost_estimates()
                    .map_err(|e| format!("failed to query cost_estimate: {e}"))?;
                list.into_iter()
                    .map(|r| {
                        serde_json::json!({
                            "id": r.id,
                            "resource_id": r.resource_id,
                            "resource_type": r.resource_type,
                            "provider": r.provider,
                            "description": r.description,
                            "quantity": r.quantity,
                            "hourly": r.hourly,
                            "monthly": r.monthly,
                            "estimated_at": r.estimated_at,
                        })
                    })
                    .collect()
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

        // Apply GROUP BY
        let rows = if let Some(ref group_fields) = s.group_by {
            apply_group_by(&rows, group_fields, &s.fields)
        } else {
            rows
        };

        // Apply ORDER BY
        let mut rows = rows;
        if let Some(ref order) = s.order_by {
            rows.sort_by(|a, b| {
                for item in order {
                    let key = item.field.as_str();
                    let va = a.get(key);
                    let vb = b.get(key);
                    let cmp = compare_json_values(va, vb);
                    let cmp = if item.direction == SortDirection::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply OFFSET
        let rows: Vec<serde_json::Value> = if let Some(offset) = s.offset {
            rows.into_iter().skip(offset as usize).collect()
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

    pub(super) fn exec_show(&self, s: &ShowStmt) -> Result<StmtOutcome, EngineError> {
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
                    )
                    .into());
                }
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
                        Err(e) => Err(format!("failed to list grants: {e}").into()),
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
                        Err(e) => Err(format!("failed to list grants: {e}").into()),
                    }
                }
            }
        }
    }

    // =======================================================================
    // WATCH
    // =======================================================================

    pub(super) async fn exec_watch(&self, s: &WatchStmt) -> Result<StmtOutcome, EngineError> {
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
    // SELECT FROM <table-valued function>(...)
    // =======================================================================

    pub(super) async fn exec_select_function(
        &self,
        s: &SelectStmt,
        fc: &kvmql_parser::ast::FunctionCall,
    ) -> Result<StmtOutcome, EngineError> {
        let fc_resolved = self.resolve_function_call_args(fc);

        let rows = match fc_resolved.name.as_str() {
            "file_stat" => self.run_file_stat(&fc_resolved)?,
            "systemd_services" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::systemd::SystemdProvisioner::new(&p.client)
                    .list_services()
                    .map_err(|e| e.to_string())
            })?,
            "nginx_vhosts" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::nginx::NginxProvisioner::new(&p.client)
                    .list_vhosts()
                    .map_err(|e| e.to_string())
            })?,
            "nginx_config_test" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::nginx::NginxProvisioner::new(&p.client)
                    .config_test_row()
                    .map_err(|e| e.to_string())
            })?,
            "docker_containers" => self.run_ssh_query(&fc_resolved, |p| {
                kvmql_driver::ssh::docker::DockerProvisioner::new(&p.client)
                    .list_containers()
                    .map_err(|e| e.to_string())
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

        // Projection
        let projected = project_rows(&s.fields, limited)?;

        let n = projected.len() as i64;
        Ok(StmtOutcome::ok_rows(serde_json::Value::Array(projected), n))
    }

    /// Generic SSH query dispatcher.
    pub(super) fn run_ssh_query(
        &self,
        fc: &kvmql_parser::ast::FunctionCall,
        query_fn: impl Fn(
            &kvmql_driver::ssh::SshResourceProvisioner,
        ) -> Result<Vec<serde_json::Value>, String>,
    ) -> Result<Vec<serde_json::Value>, String> {
        use kvmql_parser::ast::Expr;

        let provider_ids: Vec<String> = match fc.args.first() {
            Some(Expr::StringLit(pid)) => vec![pid.clone()],
            None | Some(_) => self
                .ctx
                .registry
                .list_providers()
                .map_err(|e| format!("list providers: {e}"))?
                .into_iter()
                .filter(|r| r.provider_type == "ssh")
                .map(|r| r.id)
                .collect(),
        };

        if self.ctx.simulate {
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

    /// Clone a FunctionCall with variable references resolved.
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
                let p =
                    arg_str(&fc.args, 0).ok_or("file_stat(path): path must be a string literal")?;
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
            n => return Err(format!("file_stat expects 1 or 2 args, got {n}")),
        };

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
                    rows.push(serde_json::json!({
                        "provider_id": pid,
                        "host": null,
                        "path": path,
                        "present": false,
                        "error": e.to_string(),
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
            rows.push(build_file_stat_row(
                &provisioner,
                &pid,
                host.as_deref(),
                &path,
            ));
        }
        Ok(rows)
    }

    // =======================================================================
    // ASSERT
    // =======================================================================

    pub(super) async fn exec_assert(
        &self,
        s: &kvmql_parser::ast::AssertStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
            Err(format!("ASSERTION FAILED: {}", msg).into())
        }
    }

    /// Async predicate evaluation that supports `EXISTS (SELECT ...)` and
    /// scalar subqueries on either side of a comparison. Used by `ASSERT`.
    pub(super) fn eval_assertion_predicate<'b>(
        &'b self,
        pred: &'b Predicate,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + 'b>> {
        Box::pin(async move {
            match pred {
                Predicate::And(a, b) => Ok(self.eval_assertion_predicate(a).await?
                    && self.eval_assertion_predicate(b).await?),
                Predicate::Or(a, b) => Ok(self.eval_assertion_predicate(a).await?
                    || self.eval_assertion_predicate(b).await?),
                Predicate::Not(inner) => Ok(!self.eval_assertion_predicate(inner).await?),
                Predicate::Grouped(inner) => self.eval_assertion_predicate(inner).await,
                Predicate::Exists(select) => {
                    let outcome = self.exec_select(select).await.map_err(|e| e.to_string())?;
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
    async fn eval_assertion_comparison(&self, cmp: &Comparison) -> Result<bool, String> {
        let lhs = self.eval_expr_value(&cmp.left).await?;
        let rhs = self.eval_expr_value(&cmp.right).await?;
        Ok(compare_json(&lhs, &cmp.op, &rhs))
    }

    /// Evaluate an `Expr` to a JSON value, with subquery support.
    pub(super) fn eval_expr_value<'b>(
        &'b self,
        expr: &'b kvmql_parser::ast::Expr,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + 'b>>
    {
        use kvmql_parser::ast::Expr;
        Box::pin(async move {
            match expr {
                Expr::Subquery(select) => {
                    let outcome = self.exec_select(select).await.map_err(|e| e.to_string())?;
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
                Expr::Identifier(name) => Ok(serde_json::Value::String(name.clone())),
                Expr::FunctionCall(_) | Expr::Duration(_) => Err(format!(
                    "expression not supported in ASSERT context: {:?}",
                    expr
                )),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// GROUP BY helper
// ---------------------------------------------------------------------------

/// Group rows by the specified fields, computing aggregates for any
/// `FnCall` fields in the SELECT list (count, sum, avg, min, max).
fn apply_group_by(
    rows: &[serde_json::Value],
    group_fields: &FieldList,
    select_fields: &FieldList,
) -> Vec<serde_json::Value> {
    use std::collections::BTreeMap;

    // Extract group-by field names
    let group_keys: Vec<String> = match group_fields {
        FieldList::Fields(fields) => fields
            .iter()
            .filter_map(|f| match f {
                Field::Simple(name) => Some(name.clone()),
                Field::Aliased { field, .. } => match field.as_ref() {
                    Field::Simple(name) => Some(name.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        FieldList::All => return rows.to_vec(),
    };

    // Group rows by key values
    let mut groups: BTreeMap<Vec<String>, Vec<&serde_json::Value>> = BTreeMap::new();
    for row in rows {
        let key: Vec<String> = group_keys
            .iter()
            .map(|k| {
                row.get(k)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => "null".to_string(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect();
        groups.entry(key).or_default().push(row);
    }

    // Collect aggregate descriptors from the SELECT field list
    let agg_fields: Vec<(String, String, String)> = collect_aggregates(select_fields);

    // Build output rows
    groups
        .into_iter()
        .map(|(key_vals, group_rows)| {
            let mut result = serde_json::Map::new();

            // Add group-by fields (preserve original JSON types)
            for (i, k) in group_keys.iter().enumerate() {
                if let Some(first_row) = group_rows.first() {
                    if let Some(val) = first_row.get(k) {
                        result.insert(k.clone(), val.clone());
                    } else {
                        result.insert(k.clone(), serde_json::Value::String(key_vals[i].clone()));
                    }
                } else {
                    result.insert(k.clone(), serde_json::Value::String(key_vals[i].clone()));
                }
            }

            // Compute aggregates
            for (func, arg, alias) in &agg_fields {
                let val = compute_aggregate(func, arg, &group_rows);
                result.insert(alias.clone(), val);
            }

            // For non-aggregate, non-group-by selected fields, take first row value
            if let FieldList::Fields(fields) = select_fields {
                collect_passthrough_fields(fields, &group_keys, &group_rows, &mut result);
            }

            serde_json::Value::Object(result)
        })
        .collect()
}

/// Extract aggregate function descriptors: (func_name, arg_name, output_alias).
fn collect_aggregates(select_fields: &FieldList) -> Vec<(String, String, String)> {
    let fields = match select_fields {
        FieldList::Fields(f) => f,
        FieldList::All => return vec![],
    };

    let mut aggs = Vec::new();
    for f in fields {
        match f {
            Field::FnCall { name, star, args } => {
                let func_name = name.to_lowercase();
                let arg_name = if *star {
                    "*".to_string()
                } else {
                    args.first()
                        .map(|a| match a {
                            Expr::Identifier(n) => n.clone(),
                            _ => format!("{:?}", a),
                        })
                        .unwrap_or_default()
                };
                let alias = if func_name == "count" {
                    "count".to_string()
                } else {
                    format!("{}_{}", func_name, arg_name)
                };
                aggs.push((func_name, arg_name, alias));
            }
            Field::Aliased { field, alias } => {
                if let Field::FnCall { name, star, args } = field.as_ref() {
                    let func_name = name.to_lowercase();
                    let arg_name = if *star {
                        "*".to_string()
                    } else {
                        args.first()
                            .map(|a| match a {
                                Expr::Identifier(n) => n.clone(),
                                _ => format!("{:?}", a),
                            })
                            .unwrap_or_default()
                    };
                    aggs.push((func_name, arg_name, alias.clone()));
                }
            }
            _ => {}
        }
    }
    aggs
}

/// Compute a single aggregate value for a group of rows.
fn compute_aggregate(
    func: &str,
    arg: &str,
    group_rows: &[&serde_json::Value],
) -> serde_json::Value {
    match func {
        "count" => serde_json::json!(group_rows.len()),
        "sum" => {
            let sum: f64 = group_rows
                .iter()
                .filter_map(|r| r.get(arg))
                .filter_map(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .sum();
            serde_json::json!(sum)
        }
        "avg" => {
            let vals: Vec<f64> = group_rows
                .iter()
                .filter_map(|r| r.get(arg))
                .filter_map(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .collect();
            if vals.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        }
        "min" => group_rows
            .iter()
            .filter_map(|r| r.get(arg))
            .filter_map(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        "max" => group_rows
            .iter()
            .filter_map(|r| r.get(arg))
            .filter_map(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| serde_json::json!(v))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

/// For non-aggregate, non-group-by selected fields, take first row's value.
fn collect_passthrough_fields(
    fields: &[Field],
    group_keys: &[String],
    group_rows: &[&serde_json::Value],
    result: &mut serde_json::Map<String, serde_json::Value>,
) {
    for f in fields {
        match f {
            Field::Simple(name) => {
                if !result.contains_key(name) && !group_keys.contains(name) {
                    if let Some(val) = group_rows.first().and_then(|r| r.get(name.as_str())) {
                        result.insert(name.clone(), val.clone());
                    }
                }
            }
            Field::Aliased { field, alias } => {
                if let Field::Simple(name) = field.as_ref() {
                    if !result.contains_key(alias) && !group_keys.contains(name) {
                        if let Some(val) = group_rows.first().and_then(|r| r.get(name.as_str())) {
                            result.insert(alias.clone(), val.clone());
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// ORDER BY helper
// ---------------------------------------------------------------------------

fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            // Try numeric comparison first
            if let (Some(na), Some(nb)) = (a.as_f64(), b.as_f64()) {
                return na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal);
            }
            // Fall back to string comparison
            let sa = match a {
                serde_json::Value::String(s) => s.as_str(),
                _ => "",
            };
            let sb = match b {
                serde_json::Value::String(s) => s.as_str(),
                _ => "",
            };
            sa.cmp(sb)
        }
    }
}
