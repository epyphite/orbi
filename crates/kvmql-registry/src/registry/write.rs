use rusqlite::params;
use uuid::Uuid;

use crate::error::RegistryError;

use super::Registry;

impl Registry {
    // -----------------------------------------------------------------------
    // Provider writes
    // -----------------------------------------------------------------------

    pub fn insert_provider(
        &self,
        id: &str,
        provider_type: &str,
        driver: &str,
        status: &str,
        enabled: bool,
        host: Option<&str>,
        region: Option<&str>,
        auth_ref: &str,
        labels: Option<&str>,
        latency_ms: Option<i64>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO providers (id, type, driver, status, enabled, host, region, auth_ref, labels, latency_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![id, provider_type, driver, status, enabled as i64, host, region, auth_ref, labels, latency_ms],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, ref msg)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                let msg_str = msg.as_deref().unwrap_or("");
                // UNIQUE constraint → already exists
                if msg_str.contains("UNIQUE") || msg_str.contains("PRIMARY KEY") {
                    RegistryError::AlreadyExists { entity: "provider".into(), id: id.into() }
                } else {
                    // CHECK or other constraint violation
                    RegistryError::ConstraintViolation(format!(
                        "provider '{}': {}", id, msg_str
                    ))
                }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn update_provider_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE providers SET status = ?1, last_seen = datetime('now') WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "provider".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_provider(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM providers WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "provider".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MicroVM writes
    // -----------------------------------------------------------------------

    pub fn insert_microvm(
        &self,
        id: &str,
        provider_id: &str,
        tenant: &str,
        status: &str,
        image_id: Option<&str>,
        vcpus: Option<i64>,
        memory_mb: Option<i64>,
        hostname: Option<&str>,
        metadata: Option<&str>,
        labels: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO microvms (id, provider_id, tenant, status, image_id, vcpus, memory_mb, hostname, metadata, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![id, provider_id, tenant, status, image_id, vcpus, memory_mb, hostname, metadata, labels],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "microvm".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn update_microvm_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE microvms SET status = ?1, last_seen = datetime('now') WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "microvm".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_microvm(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM microvms WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "microvm".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Volume writes
    // -----------------------------------------------------------------------

    pub fn insert_volume(
        &self,
        id: &str,
        provider_id: &str,
        volume_type: &str,
        size_gb: i64,
        status: &str,
        iops: Option<i64>,
        encrypted: bool,
        labels: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO volumes (id, provider_id, type, size_gb, status, iops, encrypted, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, provider_id, volume_type, size_gb, status, iops, encrypted as i64, labels],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "volume".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn update_volume_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE volumes SET status = ?1 WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    /// Update a single field on a volume row.
    ///
    /// Supported fields: `labels`, `iops`, `encrypted`.
    pub fn update_volume_field(
        &self,
        id: &str,
        field: &str,
        value: &str,
    ) -> Result<(), RegistryError> {
        let sql = match field {
            "labels" => "UPDATE volumes SET labels = ?1 WHERE id = ?2",
            "iops" => "UPDATE volumes SET iops = ?1 WHERE id = ?2",
            "encrypted" => "UPDATE volumes SET encrypted = ?1 WHERE id = ?2",
            other => {
                return Err(RegistryError::ConstraintViolation(format!(
                    "unsupported volume field: {other}"
                )));
            }
        };
        let changed = self.conn.execute(sql, params![value, id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn attach_volume(
        &self,
        vol_id: &str,
        vm_id: &str,
        device: &str,
    ) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE volumes SET microvm_id = ?1, device_name = ?2, status = 'attached' WHERE id = ?3",
            params![vm_id, device, vol_id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: vol_id.into(),
            });
        }
        Ok(())
    }

    pub fn detach_volume(&self, vol_id: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE volumes SET microvm_id = NULL, device_name = NULL, status = 'available' WHERE id = ?1",
            params![vol_id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: vol_id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_volume(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM volumes WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Image writes
    // -----------------------------------------------------------------------

    pub fn insert_image(
        &self,
        id: &str,
        name: &str,
        os: &str,
        distro: &str,
        version: &str,
        arch: &str,
        image_type: &str,
        provider_id: Option<&str>,
        kernel_path: Option<&str>,
        rootfs_path: Option<&str>,
        disk_path: Option<&str>,
        cloud_ref: Option<&str>,
        source: &str,
        checksum_sha256: Option<&str>,
        size_mb: Option<i64>,
        status: &str,
        labels: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn
            .execute(
                "INSERT INTO images (id, name, os, distro, version, arch, type, provider_id,
                                 kernel_path, rootfs_path, disk_path, cloud_ref, source,
                                 checksum_sha256, size_mb, status, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                params![
                    id,
                    name,
                    os,
                    distro,
                    version,
                    arch,
                    image_type,
                    provider_id,
                    kernel_path,
                    rootfs_path,
                    disk_path,
                    cloud_ref,
                    source,
                    checksum_sha256,
                    size_mb,
                    status,
                    labels,
                ],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref sql_err, _)
                    if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    RegistryError::AlreadyExists {
                        entity: "image".into(),
                        id: id.into(),
                    }
                }
                other => RegistryError::Database(other),
            })?;
        Ok(())
    }

    pub fn update_image_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE images SET status = ?1 WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "image".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    /// Update the cloud_ref field on an image (used by PUBLISH IMAGE).
    pub fn update_image_cloud_ref(&self, id: &str, cloud_ref: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE images SET cloud_ref = ?1 WHERE id = ?2",
            params![cloud_ref, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "image".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_image(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM images WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "image".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Audit log writes (append-only)
    // -----------------------------------------------------------------------

    pub fn insert_audit_log(
        &self,
        principal: Option<&str>,
        action: &str,
        target_type: Option<&str>,
        target_id: Option<&str>,
        outcome: &str,
        reason: Option<&str>,
        detail: Option<&str>,
    ) -> Result<(), RegistryError> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO audit_log (id, principal, action, target_type, target_id, outcome, reason, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, principal, action, target_type, target_id, outcome, reason, detail],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Query history writes
    // -----------------------------------------------------------------------

    pub fn insert_query_history(
        &self,
        principal: Option<&str>,
        statement: &str,
        normalized_stmt: Option<&str>,
        verb: &str,
        targets: Option<&str>,
        duration_ms: Option<i64>,
        status: &str,
        notifications: Option<&str>,
        rows_affected: Option<i64>,
        result_hash: Option<&str>,
    ) -> Result<(), RegistryError> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO query_history (id, principal, statement, normalized_stmt, verb, targets,
                                        duration_ms, status, notifications, rows_affected, result_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                id, principal, statement, normalized_stmt, verb, targets,
                duration_ms, status, notifications, rows_affected, result_hash,
            ],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event writes
    // -----------------------------------------------------------------------

    pub fn insert_event(
        &self,
        event_type: &str,
        microvm_id: Option<&str>,
        volume_id: Option<&str>,
        image_id: Option<&str>,
        provider_id: Option<&str>,
        principal: Option<&str>,
        detail: Option<&str>,
    ) -> Result<(), RegistryError> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO events (id, event_type, microvm_id, volume_id, image_id, provider_id, principal, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, event_type, microvm_id, volume_id, image_id, provider_id, principal, detail],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Metric writes
    // -----------------------------------------------------------------------

    pub fn insert_metric(
        &self,
        microvm_id: &str,
        cpu_pct: Option<f64>,
        mem_used_mb: Option<i64>,
        net_rx_kbps: Option<f64>,
        net_tx_kbps: Option<f64>,
    ) -> Result<(), RegistryError> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO metrics (id, microvm_id, cpu_pct, mem_used_mb, net_rx_kbps, net_tx_kbps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id,
                microvm_id,
                cpu_pct,
                mem_used_mb,
                net_rx_kbps,
                net_tx_kbps
            ],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cluster writes
    // -----------------------------------------------------------------------

    pub fn insert_cluster(&self, id: &str, name: &str) -> Result<(), RegistryError> {
        self.conn
            .execute(
                "INSERT INTO clusters (id, name) VALUES (?1, ?2)",
                params![id, name],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref sql_err, _)
                    if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    RegistryError::AlreadyExists {
                        entity: "cluster".into(),
                        id: id.into(),
                    }
                }
                other => RegistryError::Database(other),
            })?;
        Ok(())
    }

    pub fn add_cluster_member(
        &self,
        cluster_id: &str,
        provider_id: &str,
    ) -> Result<(), RegistryError> {
        self.conn
            .execute(
                "INSERT INTO cluster_members (cluster_id, provider_id) VALUES (?1, ?2)",
                params![cluster_id, provider_id],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref sql_err, _)
                    if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    RegistryError::ConstraintViolation(format!(
                        "cannot add provider '{provider_id}' to cluster '{cluster_id}': {e}"
                    ))
                }
                other => RegistryError::Database(other),
            })?;
        Ok(())
    }

    pub fn remove_cluster_member(
        &self,
        cluster_id: &str,
        provider_id: &str,
    ) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "DELETE FROM cluster_members WHERE cluster_id = ?1 AND provider_id = ?2",
            params![cluster_id, provider_id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "cluster_member".into(),
                id: format!("{cluster_id}/{provider_id}"),
            });
        }
        Ok(())
    }

    pub fn delete_cluster(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM clusters WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "cluster".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Principals & Grants writes
    // -----------------------------------------------------------------------

    pub fn insert_principal(
        &self,
        id: &str,
        principal_type: &str,
        auth_ref: &str,
        enabled: bool,
    ) -> Result<(), RegistryError> {
        self.conn
            .execute(
                "INSERT INTO principals (id, type, auth_ref, enabled) VALUES (?1, ?2, ?3, ?4)",
                params![id, principal_type, auth_ref, enabled as i64],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref sql_err, _)
                    if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    RegistryError::AlreadyExists {
                        entity: "principal".into(),
                        id: id.into(),
                    }
                }
                other => RegistryError::Database(other),
            })?;
        Ok(())
    }

    pub fn insert_grant(
        &self,
        id: &str,
        principal_id: &str,
        verbs: &str,
        scope_type: &str,
        scope_id: Option<&str>,
        conditions: Option<&str>,
        granted_by: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO grants (id, principal_id, verbs, scope_type, scope_id, conditions, granted_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, principal_id, verbs, scope_type, scope_id, conditions, granted_by],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::ConstraintViolation(format!("grant constraint violation: {e}"))
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn delete_grant(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM grants WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "grant".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Resource writes
    // -----------------------------------------------------------------------

    pub fn insert_resource(
        &self,
        id: &str,
        resource_type: &str,
        provider_id: &str,
        name: Option<&str>,
        status: &str,
        config: Option<&str>,
        labels: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO resources (id, resource_type, provider_id, name, status, config, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, resource_type, provider_id, name, status, config, labels],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "resource".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn update_resource_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE resources SET status = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn update_resource_outputs(&self, id: &str, outputs: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE resources SET outputs = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![outputs, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn update_resource_config(&self, id: &str, config: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE resources SET config = ?1, updated_at = datetime('now') WHERE id = ?2",
            params![config, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_resource(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM resources WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // State Snapshot writes
    // -----------------------------------------------------------------------

    pub fn insert_state_snapshot(
        &self,
        id: &str,
        tag: Option<&str>,
        statement: &str,
        target_type: &str,
        target_id: &str,
        previous_state: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO state_snapshots (id, tag, statement, target_type, target_id, previous_state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, tag, statement, target_type, target_id, previous_state],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Plan writes
    // -----------------------------------------------------------------------

    pub fn insert_plan(
        &self,
        id: &str,
        name: Option<&str>,
        source: &str,
        plan_output: &str,
        checksum: &str,
        environment: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn
            .execute(
                "INSERT INTO plans (id, name, source, plan_output, checksum, environment)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![id, name, source, plan_output, checksum, environment],
            )
            .map_err(|e| match e {
                rusqlite::Error::SqliteFailure(ref sql_err, _)
                    if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    RegistryError::AlreadyExists {
                        entity: "plan".into(),
                        id: id.into(),
                    }
                }
                other => RegistryError::Database(other),
            })?;
        Ok(())
    }

    pub fn update_plan_status(
        &self,
        id: &str,
        status: &str,
        by: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), RegistryError> {
        let changed = if status == "applied" {
            self.conn.execute(
                "UPDATE plans SET status = ?1, applied_at = datetime('now'), applied_by = ?2, error = ?3 WHERE id = ?4",
                params![status, by, error, id],
            )?
        } else {
            self.conn.execute(
                "UPDATE plans SET status = ?1, error = ?2 WHERE id = ?3",
                params![status, error, id],
            )?
        };
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "plan".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn approve_plan(&self, id: &str, by: Option<&str>) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE plans SET status = 'approved', approved_at = datetime('now'), approved_by = ?1 WHERE id = ?2",
            params![by, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "plan".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn delete_plan(&self, id: &str) -> Result<(), RegistryError> {
        let changed = self
            .conn
            .execute("DELETE FROM plans WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "plan".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Applied Files writes
    // -----------------------------------------------------------------------

    pub fn insert_applied_file(
        &self,
        id: &str,
        file_path: &str,
        file_hash: &str,
        statements_count: i64,
        environment: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO applied_files (id, file_path, file_hash, statements_count, environment)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, file_path, file_hash, statements_count, environment],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "applied_file".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn update_applied_file_status(&self, id: &str, status: &str) -> Result<(), RegistryError> {
        let changed = self.conn.execute(
            "UPDATE applied_files SET status = ?1 WHERE id = ?2",
            params![status, id],
        )?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "applied_file".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Import log writes
    // -----------------------------------------------------------------------

    pub fn insert_import_log(
        &self,
        provider_id: &str,
        resource_type: &str,
        resource_id: &str,
        action: &str,
        details: Option<&str>,
    ) -> Result<(), RegistryError> {
        let id = Uuid::new_v4().to_string();
        self.conn.execute(
            "INSERT INTO import_log (id, provider_id, resource_type, resource_id, action, details)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, provider_id, resource_type, resource_id, action, details],
        )?;
        Ok(())
    }
}
