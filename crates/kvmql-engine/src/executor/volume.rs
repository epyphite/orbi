use kvmql_driver::types::VolumeParams;
use kvmql_parser::ast::*;

use crate::errors::EngineError;
use crate::response::*;

use super::{get_param, get_param_bool, get_param_i64, Executor, StmtOutcome};

impl<'a> Executor<'a> {
    // =======================================================================
    // CREATE VOLUME
    // =======================================================================

    pub(super) async fn exec_create_volume(
        &self,
        s: &CreateVolumeStmt,
    ) -> Result<StmtOutcome, EngineError> {
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
        let vol_type = get_param(&s.params, "type").unwrap_or_else(|| "virtio-blk".into());
        let encrypted = get_param_bool(&s.params, "encrypted").unwrap_or(false);
        let iops = get_param_i64(&s.params, "iops").map(|v| v as i32);
        let labels = get_param(&s.params, "labels").and_then(|s| serde_json::from_str(&s).ok());

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

        self.ctx
            .registry
            .insert_volume(
                &vol.id,
                &provider_id,
                &vol_type,
                size_gb,
                &vol.status,
                iops.map(|v| v as i64),
                encrypted,
                None,
            )
            .map_err(|e| format!("registry insert_volume failed: {e}"))?;

        let val = serde_json::to_value(&vol).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }

    // =======================================================================
    // ATTACH / DETACH VOLUME
    // =======================================================================

    pub(super) async fn exec_attach(&self, s: &AttachStmt) -> Result<StmtOutcome, EngineError> {
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

    pub(super) async fn exec_detach(&self, s: &DetachStmt) -> Result<StmtOutcome, EngineError> {
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

    pub(super) async fn exec_resize(&self, s: &ResizeStmt) -> Result<StmtOutcome, EngineError> {
        let vol_row = self
            .ctx
            .registry
            .get_volume(&s.volume_id)
            .map_err(|e| format!("volume lookup failed: {e}"))?;
        let driver = self
            .ctx
            .get_driver(&vol_row.provider_id)
            .ok_or_else(|| format!("no driver for provider '{}'", vol_row.provider_id))?
            .clone();
        let vol = driver
            .resize_volume(&s.volume_id, s.new_size_gb)
            .await
            .map_err(|e| format!("driver resize_volume failed: {e}"))?;
        let val = serde_json::to_value(&vol).map_err(|e| format!("serialization error: {e}"))?;
        Ok(StmtOutcome::ok_val(val))
    }
}
