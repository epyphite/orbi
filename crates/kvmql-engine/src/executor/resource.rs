use kvmql_parser::ast::*;

use crate::errors::{with_remediation, EngineError, ErrorContext};
use crate::response::*;

use super::helpers::{resolve_content_reference, simulate_outputs};
use super::{get_param, Executor, StmtOutcome};

impl<'a> Executor<'a> {
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
                if let Some(val) = obj
                    .get(*key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
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

    /// Resolve cross-resource reference parameters.
    ///
    /// When a parameter like `vpc_id = 'my-vpc'` refers to a resource managed by
    /// Orbi, the actual cloud-assigned ID (e.g. `vpc-0abc123`) must be passed to
    /// the cloud CLI.  This method checks known reference keys against the
    /// registry and replaces logical IDs with the cloud ID from `outputs`.
    fn resolve_resource_refs(&self, params: &serde_json::Value) -> serde_json::Value {
        /// Map of parameter key → output field that contains the cloud ID
        const REF_KEYS: &[(&str, &str)] = &[
            ("vpc_id", "vpc_id"),
            ("subnet_id", "subnet_id"),
            ("security_group_id", "group_id"),
            ("allocation_id", "allocation_id"),
            ("target_group_arn", "target_group_arn"),
            ("alb_arn", "load_balancer_arn"),
            ("internet_gateway_id", "internet_gateway_id"),
            ("route_table_id", "route_table_id"),
            ("gateway_id", "internet_gateway_id"),
            ("nat_gateway_id", "nat_gateway_id"),
        ];

        let mut resolved = params.clone();
        let obj = match resolved.as_object_mut() {
            Some(o) => o,
            None => return resolved,
        };

        for &(param_key, output_key) in REF_KEYS {
            let logical_id = match obj.get(param_key).and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };

            // Skip if the value already looks like a cloud-assigned ID
            if logical_id.contains('-')
                && (logical_id.starts_with("vpc-")
                    || logical_id.starts_with("subnet-")
                    || logical_id.starts_with("sg-")
                    || logical_id.starts_with("igw-")
                    || logical_id.starts_with("eipalloc-")
                    || logical_id.starts_with("arn:")
                    || logical_id.starts_with("nat-")
                    || logical_id.starts_with("vpce-")
                    || logical_id.starts_with("rtb-"))
            {
                continue;
            }

            // Look up the referenced resource in the registry
            if let Ok(resource) = self.ctx.registry.get_resource(&logical_id) {
                if let Some(outputs_json) = &resource.outputs {
                    if let Ok(outputs) =
                        serde_json::from_str::<serde_json::Value>(outputs_json)
                    {
                        if let Some(cloud_id) = outputs.get(output_key).and_then(|v| v.as_str()) {
                            obj.insert(
                                param_key.to_string(),
                                serde_json::Value::String(cloud_id.to_string()),
                            );
                            tracing::debug!(
                                param = param_key,
                                logical = %logical_id,
                                resolved = cloud_id,
                                "resolved resource cross-reference"
                            );
                        }
                    }
                }
            }
        }

        resolved
    }

    pub(super) fn exec_create_resource(
        &self,
        s: &CreateResourceStmt,
    ) -> Result<StmtOutcome, EngineError> {
        if self.ctx.simulate {
            return self.exec_create_resource_simulated(s);
        }

        // Resolve variable references in params
        let params = self.resolve_params(&s.params);

        let id = get_param(&params, "id").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

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
                    message: format!(
                        "Resource '{}' '{}' already exists -- skipped",
                        s.resource_type, id
                    ),
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
        // Resolve cross-resource references (logical ID → AWS/cloud ID)
        let config_value = self.resolve_resource_refs(&config_value);
        let config = config_value.to_string();

        // Attempt real provisioning via the appropriate cloud CLI.
        let mut notifications: Vec<Notification> = Vec::new();
        let (status, outputs) = {
            let provider_type = self
                .ctx
                .registry
                .get_provider(&provider_id)
                .ok()
                .map(|p| p.provider_type.clone())
                .unwrap_or_default();

            let is_aws = provider_type == "aws"
                || matches!(
                    s.resource_type.as_str(),
                    "rds_postgres"
                        | "vpc"
                        | "aws_subnet"
                        | "security_group"
                        | "sg_rule"
                        | "internet_gateway"
                );

            let is_cloudflare = provider_type == "cloudflare"
                || matches!(
                    s.resource_type.as_str(),
                    "cf_zone" | "cf_dns_record" | "cf_firewall_rule" | "cf_page_rule"
                );

            let is_github = provider_type == "github" || s.resource_type.starts_with("gh_");

            let is_k8s = provider_type == "kubernetes" || s.resource_type.starts_with("k8s_");

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
                                return Err(
                                    format!("failed to resolve content reference: {e}").into()
                                );
                            }
                        }
                    }
                }

                // For letsencrypt_cert with dns_provider='cf', resolve the
                // Cloudflare API token
                if s.resource_type == "letsencrypt_cert" {
                    let dns_prov = cfg_with_content
                        .get("dns_provider")
                        .and_then(|v| v.as_str())
                        .unwrap_or("cf");
                    if dns_prov == "cf" {
                        if let Some(obj) = cfg_with_content.as_object_mut() {
                            if !obj.contains_key("cf_api_token") {
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
                                    obj.insert("cf_api_token".into(), serde_json::Value::String(t));
                                }
                            }
                        }
                    }
                }

                let provisioner = self
                    .get_ssh_provisioner(&provider_id)
                    .map_err(|e| format!("ssh provisioner setup failed: {e}"))?;

                match provisioner.create(&s.resource_type, &cfg_with_content) {
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
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
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
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
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
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
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
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
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
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
                    Ok(result) => (result.status, result.outputs.map(|o| o.to_string())),
                    Err(e) => {
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

    pub(super) fn exec_alter_resource(
        &self,
        s: &AlterResourceStmt,
    ) -> Result<StmtOutcome, EngineError> {
        if self.ctx.simulate {
            return self.exec_alter_resource_simulated(s);
        }

        // Get existing resource
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        // Verify resource type matches
        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
            let is_aws = Self::is_aws_resource_type(s.resource_type.as_str());

            let is_cloudflare = matches!(
                s.resource_type.as_str(),
                "cf_zone" | "cf_dns_record" | "cf_firewall_rule" | "cf_page_rule"
            );

            let is_github = s.resource_type.starts_with("gh_");

            let is_k8s = s.resource_type.starts_with("k8s_");

            if is_k8s {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "K8S_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "Kubernetes resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_cloudflare {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "CF_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "Cloudflare resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_github {
                notifications.push(Notification {
                    level: "WARN".into(),
                    code: "GH_UPDATE_NOT_IMPLEMENTED".into(),
                    provider_id: Some(existing.provider_id.clone()),
                    message: "GitHub resource update not yet implemented, config updated in registry only".into(),
                });
            } else if is_aws {
                let provisioner = self.get_aws_provisioner(&existing.provider_id);
                match provisioner.update(&s.resource_type, &s.id, &config) {
                    Ok(result) => {
                        if let Some(outputs) = result.outputs {
                            let _ = self
                                .ctx
                                .registry
                                .update_resource_outputs(&s.id, &outputs.to_string());
                        }
                    }
                    Err(e) => {
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "AWS_UPDATE_PARTIAL".into(),
                            provider_id: Some(existing.provider_id.clone()),
                            message: format!(
                                "AWS update not supported for this resource type, config updated in registry only: {e}"
                            ),
                        });
                    }
                }
            } else {
                let provisioner = self.get_azure_provisioner(&existing.provider_id);
                match provisioner.update(&s.resource_type, &s.id, &config) {
                    Ok(result) => {
                        if let Some(outputs) = result.outputs {
                            let _ = self
                                .ctx
                                .registry
                                .update_resource_outputs(&s.id, &outputs.to_string());
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

    pub(super) fn exec_destroy_resource(
        &self,
        s: &DestroyResourceStmt,
    ) -> Result<StmtOutcome, EngineError> {
        if self.ctx.simulate {
            return self.exec_destroy_resource_simulated(s);
        }

        // Verify resource exists and type matches
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
        }

        // If the resource is pending (never actually provisioned), skip the
        // cloud deletion and just remove it from the registry.
        if existing.status == "pending" {
            self.ctx
                .registry
                .delete_resource(&s.id)
                .map_err(|e| format!("failed to delete pending resource: {e}"))?;
            return Ok(StmtOutcome::ok_val(serde_json::json!({
                "id": s.id,
                "resource_type": s.resource_type,
                "status": "removed",
                "note": "pending resource removed from registry (was never provisioned)",
            })));
        }

        // Attempt real deletion via the appropriate cloud CLI
        let mut notifications: Vec<Notification> = Vec::new();
        {
            let is_aws = Self::is_aws_resource_type(s.resource_type.as_str());

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
                let mut merged: serde_json::Value = existing
                    .config
                    .as_deref()
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Some(outputs_str) = existing.outputs.as_deref() {
                    if let Ok(outputs_val) = serde_json::from_str::<serde_json::Value>(outputs_str)
                    {
                        if let (Some(merged_obj), Some(out_obj)) =
                            (merged.as_object_mut(), outputs_val.as_object())
                        {
                            for (k, v) in out_obj {
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
                        message: format!("AWS deletion failed, removing from registry anyway: {e}"),
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
    ) -> Result<StmtOutcome, EngineError> {
        let params = self.resolve_params(&s.params);
        let id = get_param(&params, "id").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

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
                    message: format!(
                        "Resource '{}' '{}' already exists -- skipped",
                        s.resource_type, id
                    ),
                });
                return Ok(outcome);
            }
        }
        let name = get_param(&params, "name");
        let provider_id = self
            .resolve_provider(&s.on)
            .unwrap_or_else(|_| "simulate".into());

        let config_value = self.params_to_json(&s.params);
        let config = config_value.to_string();
        let labels = get_param(&params, "labels");

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
            let _ = self
                .ctx
                .registry
                .update_resource_outputs(&id, &out.to_string());
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
    ) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
    ) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
    // Day-2 Operations: BACKUP, RESTORE RESOURCE, SCALE, UPGRADE, ROLLBACK
    // =======================================================================

    pub(super) fn exec_backup(&self, s: &BackupStmt) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
                message: format!(
                    "BACKUP '{}' '{}' simulated -- no cloud calls made",
                    s.resource_type, s.id
                ),
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
                    let _ = self
                        .ctx
                        .registry
                        .update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self
                    .ctx
                    .registry
                    .update_resource_status(&s.id, &result.status);
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

    pub(super) fn exec_restore_resource(
        &self,
        s: &RestoreResourceStmt,
    ) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
                message: format!(
                    "RESTORE '{}' '{}' simulated -- no cloud calls made",
                    s.resource_type, s.id
                ),
            });
            return Ok(outcome);
        }

        let mut notifications: Vec<Notification> = Vec::new();
        let provisioner = self.get_azure_provisioner(&existing.provider_id);

        match provisioner.restore_resource(&s.resource_type, &s.id, &s.source) {
            Ok(result) => {
                if let Some(ref outputs) = result.outputs {
                    let _ = self
                        .ctx
                        .registry
                        .update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self
                    .ctx
                    .registry
                    .update_resource_status(&s.id, &result.status);
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

    pub(super) fn exec_scale(&self, s: &ScaleStmt) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
                message: format!(
                    "SCALE '{}' '{}' simulated -- no cloud calls made",
                    s.resource_type, s.id
                ),
            });
            return Ok(outcome);
        }

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
                    let _ = self
                        .ctx
                        .registry
                        .update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self
                    .ctx
                    .registry
                    .update_resource_status(&s.id, &result.status);
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
        let _ = self
            .ctx
            .registry
            .update_resource_config(&s.id, &config.to_string());

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

    pub(super) fn exec_upgrade(&self, s: &UpgradeStmt) -> Result<StmtOutcome, EngineError> {
        let existing = self.ctx.registry.get_resource(&s.id).map_err(|e| {
            with_remediation(
                "RESOURCE_NOT_FOUND",
                &format!("resource lookup failed: {e}"),
                &ErrorContext {
                    resource_id: Some(s.id.clone()),
                    resource_type: Some(s.resource_type.clone()),
                    ..Default::default()
                },
            )
        })?;

        if existing.resource_type != s.resource_type {
            return Err(format!(
                "resource '{}' is of type '{}', not '{}'",
                s.id, existing.resource_type, s.resource_type
            )
            .into());
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
                message: format!(
                    "UPGRADE '{}' '{}' simulated -- no cloud calls made",
                    s.resource_type, s.id
                ),
            });
            return Ok(outcome);
        }

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
                    let _ = self
                        .ctx
                        .registry
                        .update_resource_outputs(&s.id, &outputs.to_string());
                }
                let _ = self
                    .ctx
                    .registry
                    .update_resource_status(&s.id, &result.status);
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
        let _ = self
            .ctx
            .registry
            .update_resource_config(&s.id, &config.to_string());

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
    // ROLLBACK
    // =======================================================================

    pub(super) fn exec_rollback(&self, s: &RollbackStmt) -> Result<StmtOutcome, EngineError> {
        let snapshot = match &s.target {
            RollbackTarget::Last => self
                .ctx
                .registry
                .get_last_snapshot()
                .map_err(|e| format!("failed to get last snapshot: {e}"))?
                .ok_or_else(|| {
                    with_remediation(
                        "ROLLBACK_NO_SNAPSHOTS",
                        "no snapshots available for rollback",
                        &ErrorContext::default(),
                    )
                })?,
            RollbackTarget::Tag(tag) => self
                .ctx
                .registry
                .get_snapshot_by_tag(tag)
                .map_err(|e| format!("failed to get snapshot: {e}"))?
                .ok_or_else(|| format!("no snapshot found with tag '{tag}'"))?,
            RollbackTarget::Resource { resource_type, id } => self
                .ctx
                .registry
                .get_snapshot_for_resource(resource_type, id)
                .map_err(|e| format!("failed to get snapshot: {e}"))?
                .ok_or_else(|| format!("no snapshot found for {} '{}'", resource_type, id))?,
        };

        let previous = snapshot
            .previous_state
            .as_deref()
            .ok_or("snapshot has no previous state -- cannot rollback")?;
        let state: serde_json::Value = serde_json::from_str(previous)
            .map_err(|e| format!("failed to parse snapshot state: {e}"))?;

        match snapshot.target_type.as_str() {
            "resource" => {
                let _ = self.ctx.registry.delete_resource(&snapshot.target_id);
                let id = state["id"].as_str().unwrap_or(&snapshot.target_id);
                let rtype = state["resource_type"].as_str().unwrap_or("unknown");
                let provider = state["provider_id"].as_str().unwrap_or("local");
                let name = state["name"].as_str();
                let status = state["status"].as_str().unwrap_or("available");
                let config = state.get("config").and_then(|c| {
                    if c.is_null() {
                        None
                    } else {
                        Some(c.to_string())
                    }
                });
                let labels = state.get("labels").and_then(|l| {
                    if l.is_null() {
                        None
                    } else {
                        Some(l.to_string())
                    }
                });

                self.ctx
                    .registry
                    .insert_resource(
                        id,
                        rtype,
                        provider,
                        name,
                        status,
                        config.as_deref(),
                        labels.as_deref(),
                    )
                    .map_err(|e| format!("rollback failed: {e}"))?;

                if let Some(outputs) = state.get("outputs") {
                    if !outputs.is_null() {
                        let _ = self
                            .ctx
                            .registry
                            .update_resource_outputs(id, &outputs.to_string());
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
                    if m.is_null() {
                        None
                    } else {
                        Some(m.to_string())
                    }
                });
                let labels = state.get("labels").and_then(|l| {
                    if l.is_null() {
                        None
                    } else {
                        Some(l.to_string())
                    }
                });

                self.ctx
                    .registry
                    .insert_microvm(
                        id,
                        provider,
                        tenant,
                        status,
                        image_id,
                        vcpus,
                        memory_mb,
                        hostname,
                        metadata.as_deref(),
                        labels.as_deref(),
                    )
                    .map_err(|e| format!("rollback failed: {e}"))?;
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
                    if l.is_null() {
                        None
                    } else {
                        Some(l.to_string())
                    }
                });

                self.ctx
                    .registry
                    .insert_volume(
                        id,
                        provider,
                        vol_type,
                        size_gb,
                        status,
                        iops,
                        encrypted,
                        labels.as_deref(),
                    )
                    .map_err(|e| format!("rollback failed: {e}"))?;
            }
            other => return Err(format!("rollback not supported for type: {other}").into()),
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
    // IMPORT RESOURCES
    // =======================================================================

    pub(super) fn exec_import_resources(
        &self,
        s: &kvmql_parser::ast::ImportResourcesStmt,
    ) -> Result<StmtOutcome, EngineError> {
        use kvmql_parser::ast::ImportSource;

        let provider_ids: Vec<String> = match &s.source {
            ImportSource::SingleProvider(id) => vec![id.clone()],
            ImportSource::ProvidersByType(ptype) => self
                .ctx
                .registry
                .list_providers()
                .map_err(|e| format!("list providers: {e}"))?
                .into_iter()
                .filter(|p| p.provider_type == *ptype)
                .map(|p| p.id)
                .collect(),
            ImportSource::AllProviders => self
                .ctx
                .registry
                .list_providers()
                .map_err(|e| format!("list providers: {e}"))?
                .into_iter()
                .map(|p| p.id)
                .collect(),
        };

        if provider_ids.is_empty() {
            return Ok(StmtOutcome::ok_val(serde_json::json!({
                "imported": 0,
                "skipped": 0,
                "errors": 0,
                "message": "no matching providers found",
            })));
        }

        let mut total_imported: i64 = 0;
        let mut total_skipped: i64 = 0;
        let mut total_errors: i64 = 0;
        let mut notifications: Vec<Notification> = Vec::new();
        let mut all_rows: Vec<serde_json::Value> = Vec::new();

        for pid in &provider_ids {
            let provider = match self.ctx.registry.get_provider(pid) {
                Ok(p) => p,
                Err(e) => {
                    notifications.push(Notification {
                        level: "ERROR".into(),
                        code: "IMPORT_PROVIDER_ERR".into(),
                        provider_id: Some(pid.clone()),
                        message: format!("provider '{pid}' not found: {e}"),
                    });
                    total_errors += 1;
                    continue;
                }
            };

            let discovered =
                match self.discover_resources(&provider, s.resource_type_filter.as_deref()) {
                    Ok(d) => d,
                    Err(e) => {
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "IMPORT_DISCOVER_ERR".into(),
                            provider_id: Some(pid.clone()),
                            message: format!("discover failed for '{pid}': {e}"),
                        });
                        total_errors += 1;
                        continue;
                    }
                };

            for entry in discovered {
                let resource_id = entry["id"].as_str().unwrap_or_default().to_string();
                let resource_type = entry["resource_type"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();

                if resource_id.is_empty() {
                    total_errors += 1;
                    continue;
                }

                if let Ok(_existing) = self.ctx.registry.get_resource(&resource_id) {
                    if let Some(config) = entry.get("config") {
                        let _ = self
                            .ctx
                            .registry
                            .update_resource_config(&resource_id, &config.to_string());
                    }
                    total_skipped += 1;

                    let _ = self.ctx.registry.insert_import_log(
                        pid,
                        &resource_type,
                        &resource_id,
                        "existing",
                        Some(&entry.to_string()),
                    );
                    continue;
                }

                let name = entry["name"].as_str();
                let config = entry.get("config").map(|c| c.to_string());
                let labels = entry.get("labels").and_then(|l| {
                    if l.is_null() {
                        None
                    } else {
                        Some(l.to_string())
                    }
                });

                match self.ctx.registry.insert_resource(
                    &resource_id,
                    &resource_type,
                    pid,
                    name,
                    "imported",
                    config.as_deref(),
                    labels.as_deref(),
                ) {
                    Ok(_) => {
                        if let Some(outputs) = entry.get("outputs") {
                            let _ = self
                                .ctx
                                .registry
                                .update_resource_outputs(&resource_id, &outputs.to_string());
                        }
                        total_imported += 1;
                        let _ = self.ctx.registry.insert_import_log(
                            pid,
                            &resource_type,
                            &resource_id,
                            "new",
                            Some(&entry.to_string()),
                        );
                        all_rows.push(serde_json::json!({
                            "id": resource_id,
                            "resource_type": resource_type,
                            "provider_id": pid,
                            "action": "imported",
                        }));
                    }
                    Err(e) => {
                        notifications.push(Notification {
                            level: "WARN".into(),
                            code: "IMPORT_INSERT_ERR".into(),
                            provider_id: Some(pid.clone()),
                            message: format!("failed to import '{resource_id}': {e}"),
                        });
                        total_errors += 1;
                    }
                }
            }
        }

        let summary = serde_json::json!({
            "imported": total_imported,
            "skipped": total_skipped,
            "errors": total_errors,
            "providers_scanned": provider_ids.len(),
        });
        let mut outcome = StmtOutcome::ok_val(summary);
        outcome.notifications = notifications;
        outcome.rows_affected = total_imported;
        Ok(outcome)
    }

    /// Call the appropriate discover implementation for the given provider.
    fn discover_resources(
        &self,
        provider: &kvmql_registry::registry::ProviderRow,
        type_filter: Option<&[String]>,
    ) -> Result<Vec<serde_json::Value>, String> {
        if self.ctx.simulate {
            return Ok(vec![]);
        }

        let entries = match provider.provider_type.as_str() {
            "azure" => {
                let p = self.get_azure_provisioner(&provider.id);
                p.discover().map_err(|e| e.to_string())?
            }
            "aws" => {
                let p = self.get_aws_provisioner(&provider.id);
                p.discover().map_err(|e| e.to_string())?
            }
            "cloudflare" => {
                let p = self.get_cloudflare_provisioner(&provider.id);
                p.discover().map_err(|e| e.to_string())?
            }
            "github" => {
                let p = self.get_github_provisioner(&provider.id);
                p.discover().map_err(|e| e.to_string())?
            }
            "kubernetes" => {
                let p = self.get_k8s_provisioner(&provider.id);
                p.discover().map_err(|e| e.to_string())?
            }
            "ssh" => {
                let p = self
                    .get_ssh_provisioner(&provider.id)
                    .map_err(|e| e.to_string())?;
                p.discover().map_err(|e| e.to_string())?
            }
            other => {
                return Err(format!(
                    "discover not supported for provider type '{other}'"
                ));
            }
        };

        match type_filter {
            Some(types) => Ok(entries
                .into_iter()
                .filter(|e| {
                    e["resource_type"]
                        .as_str()
                        .map(|rt| types.iter().any(|t| t == rt))
                        .unwrap_or(false)
                })
                .collect()),
            None => Ok(entries),
        }
    }
}
