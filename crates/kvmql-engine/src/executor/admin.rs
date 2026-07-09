use std::sync::Arc;

use kvmql_parser::ast::*;

use crate::context::ExecutionMode;
use crate::errors::EngineError;
use crate::response::*;

use super::{get_param, Executor, StmtOutcome};

impl<'a> Executor<'a> {
    // =======================================================================
    // Provider management
    // =======================================================================

    pub(super) fn exec_add_provider(
        &self,
        s: &AddProviderStmt,
    ) -> Result<StmtOutcome, EngineError> {
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

        let ptype = get_param(&s.params, "type").unwrap_or_else(|| "kvm".into());
        let driver = get_param(&s.params, "driver").unwrap_or_else(|| "firecracker".into());
        let auth = get_param(&s.params, "auth").unwrap_or_else(|| "none".into());
        let host = get_param(&s.params, "host");
        let region = get_param(&s.params, "region");
        let mut labels = get_param(&s.params, "labels");

        // Merge USER, PORT, and SUDO params into labels for SSH providers so
        // they survive the round-trip through the registry (labels is the
        // only extensible field on the providers table).
        let ssh_user = get_param(&s.params, "user");
        let ssh_port = get_param(&s.params, "port");
        let ssh_sudo = get_param(&s.params, "sudo");
        if ssh_user.is_some() || ssh_port.is_some() || ssh_sudo.is_some() {
            let mut obj: serde_json::Map<String, serde_json::Value> = labels
                .as_deref()
                .and_then(|l| serde_json::from_str(l).ok())
                .unwrap_or_default();
            if let Some(u) = &ssh_user {
                obj.insert("ssh_user".into(), serde_json::Value::String(u.clone()));
            }
            if let Some(p) = &ssh_port {
                obj.insert("ssh_port".into(), serde_json::Value::String(p.clone()));
            }
            if let Some(s) = &ssh_sudo {
                obj.insert("sudo".into(), serde_json::Value::String(s.clone()));
            }
            labels = Some(serde_json::Value::Object(obj).to_string());
        }

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
        } else {
            match (ptype.as_str(), driver.as_str()) {
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
                ("azure", _) => Arc::new(kvmql_driver::azure::AzureVmDriver::new(&id)),
                ("gcp", _) => Arc::new(kvmql_driver::gcp::GcpComputeDriver::new(&id)),
                _ => {
                    // Fallback to mock for unknown types
                    Arc::new(kvmql_driver::mock::MockDriver::new())
                }
            }
        };
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

    pub(super) fn exec_remove_provider(
        &self,
        s: &RemoveProviderStmt,
    ) -> Result<StmtOutcome, EngineError> {
        self.ctx
            .registry
            .delete_provider(&s.name)
            .map_err(|e| format!("failed to remove provider: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    pub(super) fn exec_alter_provider(
        &self,
        s: &AlterProviderStmt,
    ) -> Result<StmtOutcome, EngineError> {
        let mut changed = Vec::new();
        for item in &s.set_items {
            let val = match &item.value {
                Value::String(v) => v.clone(),
                Value::Integer(n) => n.to_string(),
                Value::Boolean(b) => b.to_string(),
                _ => format!("{}", item.value),
            };
            self.ctx
                .registry
                .update_provider_field(&s.name, &item.key, &val)
                .map_err(|e| format!("failed to alter provider '{}': {e}", s.name))?;
            changed.push(format!("{} = '{val}'", item.key));
        }
        let msg = if changed.is_empty() {
            "no changes".to_string()
        } else {
            changed.join(", ")
        };
        Ok(StmtOutcome::ok_val(serde_json::json!({
            "provider": s.name,
            "altered": msg,
        })))
    }

    // =======================================================================
    // SET
    // =======================================================================

    pub(super) fn exec_set(&self, s: &SetStmt) -> Result<StmtOutcome, EngineError> {
        // Handle @variable assignment
        if s.key.starts_with('@') {
            let var_name = s.key[1..].to_string();
            let val = match &s.value {
                Value::String(sv) => sv.clone(),
                Value::Integer(n) => n.to_string(),
                Value::Float(fv) => fv.to_string(),
                Value::Boolean(b) => b.to_string(),
                _ => {
                    return Err(
                        "variable value must be a string, integer, float, or boolean".into(),
                    )
                }
            };
            self.ctx
                .variables
                .write()
                .unwrap()
                .insert(var_name.clone(), val.clone());
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
                            ).into());
                        }
                    };
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

    pub(super) fn exec_add_cluster(&self, s: &AddClusterStmt) -> Result<StmtOutcome, EngineError> {
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

    pub(super) fn exec_remove_cluster(
        &self,
        s: &RemoveClusterStmt,
    ) -> Result<StmtOutcome, EngineError> {
        self.ctx
            .registry
            .delete_cluster(&s.name)
            .map_err(|e| format!("failed to remove cluster: {e}"))?;
        Ok(StmtOutcome::ok_empty())
    }

    pub(super) fn exec_alter_cluster(
        &self,
        s: &AlterClusterStmt,
    ) -> Result<StmtOutcome, EngineError> {
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

    pub(super) fn exec_add_principal(
        &self,
        s: &AddPrincipalStmt,
    ) -> Result<StmtOutcome, EngineError> {
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

    pub(super) fn exec_grant(&self, s: &GrantStmt) -> Result<StmtOutcome, EngineError> {
        self.ctx
            .registry
            .get_principal(&s.principal)
            .map_err(|e| format!("principal '{}' not found: {e}", s.principal))?;

        let verb_strings: Vec<String> = s.verbs.iter().map(|v| format!("{v}")).collect();
        let verbs_json = serde_json::to_string(&verb_strings)
            .map_err(|e| format!("failed to serialize verbs: {e}"))?;

        let (scope_type, scope_id): (&str, Option<&str>) = match &s.scope {
            GrantScope::Cluster(id) => ("cluster", Some(id.as_str())),
            GrantScope::Provider(id) => ("provider", Some(id.as_str())),
            GrantScope::Microvms => ("global", None),
            GrantScope::Volumes => ("global", None),
            GrantScope::Images => ("global", None),
        };

        let conditions = s.where_clause.as_ref().map(|w| format!("{w}"));

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

    pub(super) fn exec_revoke(&self, s: &RevokeStmt) -> Result<StmtOutcome, EngineError> {
        let grants = self
            .ctx
            .registry
            .get_grants_for_principal(&s.principal)
            .map_err(|e| format!("failed to look up grants: {e}"))?;

        let revoke_verbs: Vec<String> = s.verbs.iter().map(|v| format!("{v}")).collect();
        let (revoke_scope_type, revoke_scope_id): (&str, Option<&str>) = match &s.scope {
            GrantScope::Cluster(id) => ("cluster", Some(id.as_str())),
            GrantScope::Provider(id) => ("provider", Some(id.as_str())),
            GrantScope::Microvms => ("global", None),
            GrantScope::Volumes => ("global", None),
            GrantScope::Images => ("global", None),
        };

        let mut revoked = 0i64;
        for grant in &grants {
            let grant_verbs: Vec<String> = serde_json::from_str(&grant.verbs).unwrap_or_default();

            let scope_matches = grant.scope_type == revoke_scope_type
                && grant.scope_id.as_deref() == revoke_scope_id;

            if !scope_matches {
                continue;
            }

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
    // PUBLISH IMAGE
    // =======================================================================

    pub(super) fn exec_publish_image(
        &self,
        s: &PublishImageStmt,
    ) -> Result<StmtOutcome, EngineError> {
        let image = self
            .ctx
            .registry
            .get_image(&s.image_id)
            .map_err(|e| format!("image '{}' not found: {e}", s.image_id))?;

        let mut notifications = Vec::new();

        let driver = self.ctx.get_driver(&s.provider);

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

        let cloud_ref = format!("cloud-ref-{}-{}", s.provider, s.image_id);

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
}
