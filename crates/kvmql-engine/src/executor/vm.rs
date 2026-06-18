use kvmql_driver::types::CreateParams;
use kvmql_parser::ast::*;

use crate::errors::EngineError;
use crate::response::*;

use super::{get_param, get_param_i64, Executor, StmtOutcome};

impl<'a> Executor<'a> {
    // =======================================================================
    // CREATE MICROVM
    // =======================================================================

    pub(super) async fn exec_create_microvm(
        &self,
        s: &CreateMicrovmStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
        let metadata = get_param(&s.params, "metadata").and_then(|s| serde_json::from_str(&s).ok());
        let labels = get_param(&s.params, "labels").and_then(|s| serde_json::from_str(&s).ok());

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
                            return Err(
                                format!("failed to resolve ssh_key '{}': {}", key_ref, _e).into()
                            );
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
        self.ctx
            .registry
            .insert_microvm(
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
            )
            .map_err(|e| format!("registry insert_microvm failed: {e}"))?;

        let val = serde_json::to_value(&vm).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome {
            result: Some(val),
            rows_affected: 1,
            notifications,
        })
    }

    // =======================================================================
    // DESTROY MICROVM / VOLUME
    // =======================================================================

    pub(super) async fn exec_destroy(
        &self,
        s: &DestroyStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
                    .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
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
                    .ok_or_else(|| format!("no driver for provider '{}'", row.provider_id))?
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

    pub(super) async fn exec_pause(
        &self,
        s: &PauseStmt,
    ) -> Result<StmtOutcome, EngineError> {
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

    pub(super) async fn exec_resume(
        &self,
        s: &ResumeStmt,
    ) -> Result<StmtOutcome, EngineError> {
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

    pub(super) async fn exec_snapshot(
        &self,
        s: &SnapshotStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
        let val = serde_json::to_value(&snap).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    pub(super) async fn exec_restore(
        &self,
        s: &RestoreStmt,
    ) -> Result<StmtOutcome, EngineError> {
        // Find any driver (use first available or the one that holds a snapshot)
        let (provider_id, driver) = self.any_driver()?;
        let vm = driver
            .restore(&s.id, &s.source)
            .await
            .map_err(|e| format!("driver restore failed: {e}"))?;

        // Only reference image_id in registry if the image actually exists
        let registry_image_id = vm
            .image_id
            .as_deref()
            .and_then(|iid| self.ctx.registry.get_image(iid).ok().map(|_| iid));

        self.ctx
            .registry
            .insert_microvm(
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
            )
            .map_err(|e| format!("registry insert_microvm failed: {e}"))?;

        let val = serde_json::to_value(&vm).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // ALTER MICROVM
    // =======================================================================

    pub(super) async fn exec_alter_microvm(
        &self,
        s: &AlterMicrovmStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
        let val = serde_json::to_value(&vm).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // ALTER VOLUME
    // =======================================================================

    pub(super) async fn exec_alter_volume(
        &self,
        s: &AlterVolumeStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
                        Value::Boolean(b) => {
                            if *b {
                                "1".into()
                            } else {
                                "0".into()
                            }
                        }
                        other => format!("{other:?}"),
                    };
                    self.ctx
                        .registry
                        .update_volume_field(&s.id, &item.key, &val_str)
                        .map_err(|e| {
                            format!("failed to update volume field '{}': {e}", item.key)
                        })?;
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

    pub(super) async fn exec_import_image(
        &self,
        s: &ImportImageStmt,
    ) -> Result<StmtOutcome, EngineError> {
        let (provider_id, driver) = self.any_driver()?;

        let id = get_param(&s.params, "id").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let name = get_param(&s.params, "name").unwrap_or_else(|| "unnamed".into());
        let os = get_param(&s.params, "os").unwrap_or_else(|| "linux".into());
        let distro = get_param(&s.params, "distro").unwrap_or_else(|| "unknown".into());
        let version = get_param(&s.params, "version").unwrap_or_else(|| "latest".into());
        let arch = get_param(&s.params, "arch").unwrap_or_else(|| "x86_64".into());
        let image_type = get_param(&s.params, "type").unwrap_or_else(|| "rootfs".into());
        let source = get_param(&s.params, "source").unwrap_or_else(|| "local".into());
        let kernel = get_param(&s.params, "kernel");
        let rootfs = get_param(&s.params, "rootfs");
        let checksum = get_param(&s.params, "checksum");

        let params = kvmql_driver::types::ImageParams {
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
            None, // disk_path
            None, // cloud_ref
            &source,
            None, // checksum_sha256
            None, // size_mb
            "available",
            None, // labels
        );

        let val = serde_json::to_value(&img).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    pub(super) async fn exec_remove_image(
        &self,
        s: &RemoveImageStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
}
