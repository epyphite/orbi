use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use crate::error::RegistryError;
use crate::migration::run_migrations;

// ---------------------------------------------------------------------------
// Row structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProviderRow {
    pub id: String,
    pub provider_type: String,
    pub driver: String,
    pub status: String,
    pub enabled: bool,
    pub host: Option<String>,
    pub region: Option<String>,
    pub auth_ref: String,
    pub labels: Option<String>,
    pub latency_ms: Option<i64>,
    pub added_at: String,
    pub last_seen: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MicrovmRow {
    pub id: String,
    pub provider_id: String,
    pub tenant: String,
    pub status: String,
    pub image_id: Option<String>,
    pub vcpus: Option<i64>,
    pub memory_mb: Option<i64>,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i64>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
    pub hostname: Option<String>,
    pub metadata: Option<String>,
    pub labels: Option<String>,
    pub created_at: String,
    pub last_seen: Option<String>,
    pub is_stale: bool,
}

#[derive(Debug, Clone)]
pub struct VolumeRow {
    pub id: String,
    pub provider_id: String,
    pub microvm_id: Option<String>,
    pub volume_type: String,
    pub size_gb: i64,
    pub status: String,
    pub device_name: Option<String>,
    pub iops: Option<i64>,
    pub encrypted: bool,
    pub created_at: String,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ImageRow {
    pub id: String,
    pub name: String,
    pub os: String,
    pub distro: String,
    pub version: String,
    pub arch: String,
    pub image_type: String,
    pub provider_id: Option<String>,
    pub kernel_path: Option<String>,
    pub rootfs_path: Option<String>,
    pub disk_path: Option<String>,
    pub cloud_ref: Option<String>,
    pub source: String,
    pub checksum_sha256: Option<String>,
    pub size_mb: Option<i64>,
    pub status: String,
    pub imported_at: String,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuditLogRow {
    pub id: String,
    pub event_time: String,
    pub principal: Option<String>,
    pub action: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub outcome: String,
    pub reason: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryHistoryRow {
    pub id: String,
    pub executed_at: String,
    pub principal: Option<String>,
    pub statement: String,
    pub normalized_stmt: Option<String>,
    pub verb: String,
    pub targets: Option<String>,
    pub duration_ms: Option<i64>,
    pub status: String,
    pub notifications: Option<String>,
    pub rows_affected: Option<i64>,
    pub result_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterRow {
    pub id: String,
    pub name: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct PrincipalRow {
    pub id: String,
    pub principal_type: String,
    pub auth_ref: String,
    pub created_at: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct GrantRow {
    pub id: String,
    pub principal_id: String,
    pub verbs: String,
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub conditions: Option<String>,
    pub granted_at: String,
    pub granted_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResourceRow {
    pub id: String,
    pub resource_type: String,
    pub provider_id: String,
    pub name: Option<String>,
    pub status: String,
    pub config: Option<String>,
    pub outputs: Option<String>,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub labels: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterMemberRow {
    pub cluster_id: String,
    pub provider_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub id: String,
    pub microvm_id: String,
    pub provider_id: String,
    pub destination: String,
    pub tag: Option<String>,
    pub size_mb: Option<i64>,
    pub taken_at: String,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: String,
    pub event_time: String,
    pub event_type: String,
    pub microvm_id: Option<String>,
    pub volume_id: Option<String>,
    pub image_id: Option<String>,
    pub provider_id: Option<String>,
    pub principal: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetricRow {
    pub id: String,
    pub microvm_id: String,
    pub sampled_at: String,
    pub cpu_pct: Option<f64>,
    pub mem_used_mb: Option<i64>,
    pub net_rx_kbps: Option<f64>,
    pub net_tx_kbps: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StateSnapshotRow {
    pub id: String,
    pub tag: Option<String>,
    pub statement: String,
    pub target_type: String,
    pub target_id: String,
    pub previous_state: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct PlanRow {
    pub id: String,
    pub name: Option<String>,
    pub source: String,
    pub plan_output: String,
    pub checksum: String,
    pub status: String,
    pub created_at: String,
    pub created_by: Option<String>,
    pub approved_at: Option<String>,
    pub approved_by: Option<String>,
    pub applied_at: Option<String>,
    pub applied_by: Option<String>,
    pub error: Option<String>,
    pub environment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppliedFileRow {
    pub id: String,
    pub file_path: String,
    pub file_hash: String,
    pub statements_count: i64,
    pub applied_at: String,
    pub applied_by: Option<String>,
    pub environment: Option<String>,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub struct Registry {
    conn: Connection,
}

impl Registry {
    /// Open (or create) a registry backed by a file on disk.
    pub fn open(path: &str) -> Result<Self, RegistryError> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// Open an in-memory registry (useful for tests).
    pub fn open_in_memory() -> Result<Self, RegistryError> {
        let conn = Connection::open_in_memory()?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self, RegistryError> {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        run_migrations(&conn)?;
        Ok(Self { conn })
    }

    // -----------------------------------------------------------------------
    // Provider CRUD
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

    pub fn get_provider(&self, id: &str) -> Result<ProviderRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, type, driver, status, enabled, host, region, auth_ref, labels, latency_ms, added_at, last_seen
                 FROM providers WHERE id = ?1",
                params![id],
                |row| {
                    Ok(ProviderRow {
                        id: row.get(0)?,
                        provider_type: row.get(1)?,
                        driver: row.get(2)?,
                        status: row.get(3)?,
                        enabled: row.get::<_, i64>(4)? != 0,
                        host: row.get(5)?,
                        region: row.get(6)?,
                        auth_ref: row.get(7)?,
                        labels: row.get(8)?,
                        latency_ms: row.get(9)?,
                        added_at: row.get(10)?,
                        last_seen: row.get(11)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "provider".into(),
                id: id.into(),
            })
    }

    pub fn list_providers(&self) -> Result<Vec<ProviderRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, type, driver, status, enabled, host, region, auth_ref, labels, latency_ms, added_at, last_seen
             FROM providers ORDER BY added_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ProviderRow {
                id: row.get(0)?,
                provider_type: row.get(1)?,
                driver: row.get(2)?,
                status: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
                host: row.get(5)?,
                region: row.get(6)?,
                auth_ref: row.get(7)?,
                labels: row.get(8)?,
                latency_ms: row.get(9)?,
                added_at: row.get(10)?,
                last_seen: row.get(11)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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
        let changed = self.conn.execute("DELETE FROM providers WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "provider".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MicroVM CRUD
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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

    pub fn get_microvm(&self, id: &str) -> Result<MicrovmRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb,
                        cpu_pct, mem_used_mb, net_rx_kbps, net_tx_kbps, hostname,
                        metadata, labels, created_at, last_seen, is_stale
                 FROM microvms WHERE id = ?1",
                params![id],
                |row| {
                    Ok(MicrovmRow {
                        id: row.get(0)?,
                        provider_id: row.get(1)?,
                        tenant: row.get(2)?,
                        status: row.get(3)?,
                        image_id: row.get(4)?,
                        vcpus: row.get(5)?,
                        memory_mb: row.get(6)?,
                        cpu_pct: row.get(7)?,
                        mem_used_mb: row.get(8)?,
                        net_rx_kbps: row.get(9)?,
                        net_tx_kbps: row.get(10)?,
                        hostname: row.get(11)?,
                        metadata: row.get(12)?,
                        labels: row.get(13)?,
                        created_at: row.get(14)?,
                        last_seen: row.get(15)?,
                        is_stale: row.get::<_, i64>(16)? != 0,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "microvm".into(),
                id: id.into(),
            })
    }

    pub fn list_microvms(&self) -> Result<Vec<MicrovmRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb,
                    cpu_pct, mem_used_mb, net_rx_kbps, net_tx_kbps, hostname,
                    metadata, labels, created_at, last_seen, is_stale
             FROM microvms ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MicrovmRow {
                id: row.get(0)?,
                provider_id: row.get(1)?,
                tenant: row.get(2)?,
                status: row.get(3)?,
                image_id: row.get(4)?,
                vcpus: row.get(5)?,
                memory_mb: row.get(6)?,
                cpu_pct: row.get(7)?,
                mem_used_mb: row.get(8)?,
                net_rx_kbps: row.get(9)?,
                net_tx_kbps: row.get(10)?,
                hostname: row.get(11)?,
                metadata: row.get(12)?,
                labels: row.get(13)?,
                created_at: row.get(14)?,
                last_seen: row.get(15)?,
                is_stale: row.get::<_, i64>(16)? != 0,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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
        let changed = self.conn.execute("DELETE FROM microvms WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "microvm".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Volume CRUD
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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

    pub fn get_volume(&self, id: &str) -> Result<VolumeRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, provider_id, microvm_id, type, size_gb, status, device_name, iops, encrypted, created_at, labels
                 FROM volumes WHERE id = ?1",
                params![id],
                |row| {
                    Ok(VolumeRow {
                        id: row.get(0)?,
                        provider_id: row.get(1)?,
                        microvm_id: row.get(2)?,
                        volume_type: row.get(3)?,
                        size_gb: row.get(4)?,
                        status: row.get(5)?,
                        device_name: row.get(6)?,
                        iops: row.get(7)?,
                        encrypted: row.get::<_, i64>(8)? != 0,
                        created_at: row.get(9)?,
                        labels: row.get(10)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "volume".into(),
                id: id.into(),
            })
    }

    pub fn list_volumes(&self) -> Result<Vec<VolumeRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider_id, microvm_id, type, size_gb, status, device_name, iops, encrypted, created_at, labels
             FROM volumes ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(VolumeRow {
                id: row.get(0)?,
                provider_id: row.get(1)?,
                microvm_id: row.get(2)?,
                volume_type: row.get(3)?,
                size_gb: row.get(4)?,
                status: row.get(5)?,
                device_name: row.get(6)?,
                iops: row.get(7)?,
                encrypted: row.get::<_, i64>(8)? != 0,
                created_at: row.get(9)?,
                labels: row.get(10)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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
    pub fn update_volume_field(&self, id: &str, field: &str, value: &str) -> Result<(), RegistryError> {
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
        let changed = self.conn.execute("DELETE FROM volumes WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "volume".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Image CRUD
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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
        self.conn.execute(
            "INSERT INTO images (id, name, os, distro, version, arch, type, provider_id,
                                 kernel_path, rootfs_path, disk_path, cloud_ref, source,
                                 checksum_sha256, size_mb, status, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                id, name, os, distro, version, arch, image_type, provider_id,
                kernel_path, rootfs_path, disk_path, cloud_ref, source,
                checksum_sha256, size_mb, status, labels,
            ],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "image".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn get_image(&self, id: &str) -> Result<ImageRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, name, os, distro, version, arch, type, provider_id,
                        kernel_path, rootfs_path, disk_path, cloud_ref, source,
                        checksum_sha256, size_mb, status, imported_at, labels
                 FROM images WHERE id = ?1",
                params![id],
                |row| {
                    Ok(ImageRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        os: row.get(2)?,
                        distro: row.get(3)?,
                        version: row.get(4)?,
                        arch: row.get(5)?,
                        image_type: row.get(6)?,
                        provider_id: row.get(7)?,
                        kernel_path: row.get(8)?,
                        rootfs_path: row.get(9)?,
                        disk_path: row.get(10)?,
                        cloud_ref: row.get(11)?,
                        source: row.get(12)?,
                        checksum_sha256: row.get(13)?,
                        size_mb: row.get(14)?,
                        status: row.get(15)?,
                        imported_at: row.get(16)?,
                        labels: row.get(17)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "image".into(),
                id: id.into(),
            })
    }

    pub fn list_images(&self) -> Result<Vec<ImageRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, os, distro, version, arch, type, provider_id,
                    kernel_path, rootfs_path, disk_path, cloud_ref, source,
                    checksum_sha256, size_mb, status, imported_at, labels
             FROM images ORDER BY imported_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ImageRow {
                id: row.get(0)?,
                name: row.get(1)?,
                os: row.get(2)?,
                distro: row.get(3)?,
                version: row.get(4)?,
                arch: row.get(5)?,
                image_type: row.get(6)?,
                provider_id: row.get(7)?,
                kernel_path: row.get(8)?,
                rootfs_path: row.get(9)?,
                disk_path: row.get(10)?,
                cloud_ref: row.get(11)?,
                source: row.get(12)?,
                checksum_sha256: row.get(13)?,
                size_mb: row.get(14)?,
                status: row.get(15)?,
                imported_at: row.get(16)?,
                labels: row.get(17)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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
        let changed = self.conn.execute("DELETE FROM images WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "image".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Audit log (append-only)
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

    pub fn list_audit_log(&self, limit: Option<i64>) -> Result<Vec<AuditLogRow>, RegistryError> {
        let sql = match limit {
            Some(n) => format!(
                "SELECT id, event_time, principal, action, target_type, target_id, outcome, reason, detail
                 FROM audit_log ORDER BY event_time DESC LIMIT {n}"
            ),
            None => "SELECT id, event_time, principal, action, target_type, target_id, outcome, reason, detail
                     FROM audit_log ORDER BY event_time DESC".to_string(),
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(AuditLogRow {
                id: row.get(0)?,
                event_time: row.get(1)?,
                principal: row.get(2)?,
                action: row.get(3)?,
                target_type: row.get(4)?,
                target_id: row.get(5)?,
                outcome: row.get(6)?,
                reason: row.get(7)?,
                detail: row.get(8)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Query history
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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

    pub fn list_query_history(&self, limit: Option<i64>) -> Result<Vec<QueryHistoryRow>, RegistryError> {
        let sql = match limit {
            Some(n) => format!(
                "SELECT id, executed_at, principal, statement, normalized_stmt, verb, targets,
                        duration_ms, status, notifications, rows_affected, result_hash
                 FROM query_history ORDER BY executed_at DESC LIMIT {n}"
            ),
            None => "SELECT id, executed_at, principal, statement, normalized_stmt, verb, targets,
                            duration_ms, status, notifications, rows_affected, result_hash
                     FROM query_history ORDER BY executed_at DESC".to_string(),
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(QueryHistoryRow {
                id: row.get(0)?,
                executed_at: row.get(1)?,
                principal: row.get(2)?,
                statement: row.get(3)?,
                normalized_stmt: row.get(4)?,
                verb: row.get(5)?,
                targets: row.get(6)?,
                duration_ms: row.get(7)?,
                status: row.get(8)?,
                notifications: row.get(9)?,
                rows_affected: row.get(10)?,
                result_hash: row.get(11)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Events
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
    // Metrics
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
            params![id, microvm_id, cpu_pct, mem_used_mb, net_rx_kbps, net_tx_kbps],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cluster CRUD
    // -----------------------------------------------------------------------

    pub fn insert_cluster(&self, id: &str, name: &str) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO clusters (id, name) VALUES (?1, ?2)",
            params![id, name],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "cluster".into(), id: id.into() }
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
        self.conn.execute(
            "INSERT INTO cluster_members (cluster_id, provider_id) VALUES (?1, ?2)",
            params![cluster_id, provider_id],
        ).map_err(|e| match e {
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
        let changed = self.conn.execute("DELETE FROM clusters WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "cluster".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Principals & Grants
    // -----------------------------------------------------------------------

    pub fn insert_principal(
        &self,
        id: &str,
        principal_type: &str,
        auth_ref: &str,
        enabled: bool,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO principals (id, type, auth_ref, enabled) VALUES (?1, ?2, ?3, ?4)",
            params![id, principal_type, auth_ref, enabled as i64],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "principal".into(), id: id.into() }
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
        let changed = self.conn.execute("DELETE FROM grants WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "grant".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    pub fn get_grants_for_principal(
        &self,
        principal_id: &str,
    ) -> Result<Vec<GrantRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, principal_id, verbs, scope_type, scope_id, conditions, granted_at, granted_by
             FROM grants WHERE principal_id = ?1 ORDER BY granted_at",
        )?;
        let rows = stmt.query_map(params![principal_id], |row| {
            Ok(GrantRow {
                id: row.get(0)?,
                principal_id: row.get(1)?,
                verbs: row.get(2)?,
                scope_type: row.get(3)?,
                scope_id: row.get(4)?,
                conditions: row.get(5)?,
                granted_at: row.get(6)?,
                granted_by: row.get(7)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn get_principal(&self, id: &str) -> Result<PrincipalRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, type, auth_ref, created_at, enabled FROM principals WHERE id = ?1",
                params![id],
                |row| {
                    Ok(PrincipalRow {
                        id: row.get(0)?,
                        principal_type: row.get(1)?,
                        auth_ref: row.get(2)?,
                        created_at: row.get(3)?,
                        enabled: row.get::<_, i64>(4)? != 0,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "principal".into(),
                id: id.into(),
            })
    }

    pub fn list_principals(&self) -> Result<Vec<PrincipalRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, type, auth_ref, created_at, enabled FROM principals ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PrincipalRow {
                id: row.get(0)?,
                principal_type: row.get(1)?,
                auth_ref: row.get(2)?,
                created_at: row.get(3)?,
                enabled: row.get::<_, i64>(4)? != 0,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_all_grants(&self) -> Result<Vec<GrantRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, principal_id, verbs, scope_type, scope_id, conditions, granted_at, granted_by
             FROM grants ORDER BY granted_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(GrantRow {
                id: row.get(0)?,
                principal_id: row.get(1)?,
                verbs: row.get(2)?,
                scope_type: row.get(3)?,
                scope_id: row.get(4)?,
                conditions: row.get(5)?,
                granted_at: row.get(6)?,
                granted_by: row.get(7)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_clusters(&self) -> Result<Vec<ClusterRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, created_at FROM clusters ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ClusterRow {
                id: row.get(0)?,
                name: row.get(1)?,
                created_at: row.get(2)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_cluster_members(&self) -> Result<Vec<ClusterMemberRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT cluster_id, provider_id FROM cluster_members ORDER BY cluster_id, provider_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ClusterMemberRow {
                cluster_id: row.get(0)?,
                provider_id: row.get(1)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_snapshots_table(&self) -> Result<Vec<SnapshotRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, microvm_id, provider_id, destination, tag, size_mb, taken_at
             FROM snapshots ORDER BY taken_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SnapshotRow {
                id: row.get(0)?,
                microvm_id: row.get(1)?,
                provider_id: row.get(2)?,
                destination: row.get(3)?,
                tag: row.get(4)?,
                size_mb: row.get(5)?,
                taken_at: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_events(&self) -> Result<Vec<EventRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_time, event_type, microvm_id, volume_id, image_id, provider_id, principal, detail
             FROM events ORDER BY event_time DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(EventRow {
                id: row.get(0)?,
                event_time: row.get(1)?,
                event_type: row.get(2)?,
                microvm_id: row.get(3)?,
                volume_id: row.get(4)?,
                image_id: row.get(5)?,
                provider_id: row.get(6)?,
                principal: row.get(7)?,
                detail: row.get(8)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_metrics(&self) -> Result<Vec<MetricRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, microvm_id, sampled_at, cpu_pct, mem_used_mb, net_rx_kbps, net_tx_kbps
             FROM metrics ORDER BY sampled_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MetricRow {
                id: row.get(0)?,
                microvm_id: row.get(1)?,
                sampled_at: row.get(2)?,
                cpu_pct: row.get(3)?,
                mem_used_mb: row.get(4)?,
                net_rx_kbps: row.get(5)?,
                net_tx_kbps: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Resource CRUD
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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

    pub fn get_resource(&self, id: &str) -> Result<ResourceRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, resource_type, provider_id, name, status, config, outputs, created_at, updated_at, labels
                 FROM resources WHERE id = ?1",
                params![id],
                |row| {
                    Ok(ResourceRow {
                        id: row.get(0)?,
                        resource_type: row.get(1)?,
                        provider_id: row.get(2)?,
                        name: row.get(3)?,
                        status: row.get(4)?,
                        config: row.get(5)?,
                        outputs: row.get(6)?,
                        created_at: row.get(7)?,
                        updated_at: row.get(8)?,
                        labels: row.get(9)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            })
    }

    pub fn list_resources(&self) -> Result<Vec<ResourceRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, resource_type, provider_id, name, status, config, outputs, created_at, updated_at, labels
             FROM resources ORDER BY created_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ResourceRow {
                id: row.get(0)?,
                resource_type: row.get(1)?,
                provider_id: row.get(2)?,
                name: row.get(3)?,
                status: row.get(4)?,
                config: row.get(5)?,
                outputs: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
                labels: row.get(9)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    pub fn list_resources_by_type(&self, resource_type: &str) -> Result<Vec<ResourceRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, resource_type, provider_id, name, status, config, outputs, created_at, updated_at, labels
             FROM resources WHERE resource_type = ?1 ORDER BY created_at",
        )?;
        let rows = stmt.query_map(params![resource_type], |row| {
            Ok(ResourceRow {
                id: row.get(0)?,
                resource_type: row.get(1)?,
                provider_id: row.get(2)?,
                name: row.get(3)?,
                status: row.get(4)?,
                config: row.get(5)?,
                outputs: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
                labels: row.get(9)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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
        let changed = self.conn.execute("DELETE FROM resources WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "resource".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // State Snapshot CRUD
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

    pub fn get_last_snapshot(&self) -> Result<Option<StateSnapshotRow>, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, tag, statement, target_type, target_id, previous_state, created_at
                 FROM state_snapshots ORDER BY created_at DESC, rowid DESC LIMIT 1",
                [],
                |row| {
                    Ok(StateSnapshotRow {
                        id: row.get(0)?,
                        tag: row.get(1)?,
                        statement: row.get(2)?,
                        target_type: row.get(3)?,
                        target_id: row.get(4)?,
                        previous_state: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(RegistryError::Database)
    }

    pub fn get_snapshot_by_tag(&self, tag: &str) -> Result<Option<StateSnapshotRow>, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, tag, statement, target_type, target_id, previous_state, created_at
                 FROM state_snapshots WHERE tag = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                params![tag],
                |row| {
                    Ok(StateSnapshotRow {
                        id: row.get(0)?,
                        tag: row.get(1)?,
                        statement: row.get(2)?,
                        target_type: row.get(3)?,
                        target_id: row.get(4)?,
                        previous_state: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(RegistryError::Database)
    }

    pub fn get_snapshot_for_resource(
        &self,
        target_type: &str,
        target_id: &str,
    ) -> Result<Option<StateSnapshotRow>, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, tag, statement, target_type, target_id, previous_state, created_at
                 FROM state_snapshots WHERE target_type = ?1 AND target_id = ?2
                 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                params![target_type, target_id],
                |row| {
                    Ok(StateSnapshotRow {
                        id: row.get(0)?,
                        tag: row.get(1)?,
                        statement: row.get(2)?,
                        target_type: row.get(3)?,
                        target_id: row.get(4)?,
                        previous_state: row.get(5)?,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(RegistryError::Database)
    }

    pub fn list_snapshots(&self, limit: Option<i64>) -> Result<Vec<StateSnapshotRow>, RegistryError> {
        let limit_val = limit.unwrap_or(100);
        let mut stmt = self.conn.prepare(
            "SELECT id, tag, statement, target_type, target_id, previous_state, created_at
             FROM state_snapshots ORDER BY created_at DESC, rowid DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit_val], |row| {
                Ok(StateSnapshotRow {
                    id: row.get(0)?,
                    tag: row.get(1)?,
                    statement: row.get(2)?,
                    target_type: row.get(3)?,
                    target_id: row.get(4)?,
                    previous_state: row.get(5)?,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Plan CRUD
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub fn insert_plan(
        &self,
        id: &str,
        name: Option<&str>,
        source: &str,
        plan_output: &str,
        checksum: &str,
        environment: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.conn.execute(
            "INSERT INTO plans (id, name, source, plan_output, checksum, environment)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, name, source, plan_output, checksum, environment],
        ).map_err(|e| match e {
            rusqlite::Error::SqliteFailure(ref sql_err, _)
                if sql_err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                RegistryError::AlreadyExists { entity: "plan".into(), id: id.into() }
            }
            other => RegistryError::Database(other),
        })?;
        Ok(())
    }

    pub fn get_plan(&self, id: &str) -> Result<PlanRow, RegistryError> {
        self.conn
            .query_row(
                "SELECT id, name, source, plan_output, checksum, status,
                        created_at, created_by, approved_at, approved_by,
                        applied_at, applied_by, error, environment
                 FROM plans WHERE id = ?1",
                params![id],
                |row| {
                    Ok(PlanRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        source: row.get(2)?,
                        plan_output: row.get(3)?,
                        checksum: row.get(4)?,
                        status: row.get(5)?,
                        created_at: row.get(6)?,
                        created_by: row.get(7)?,
                        approved_at: row.get(8)?,
                        approved_by: row.get(9)?,
                        applied_at: row.get(10)?,
                        applied_by: row.get(11)?,
                        error: row.get(12)?,
                        environment: row.get(13)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| RegistryError::NotFound {
                entity: "plan".into(),
                id: id.into(),
            })
    }

    pub fn list_plans(&self, status: Option<&str>) -> Result<Vec<PlanRow>, RegistryError> {
        if let Some(st) = status {
            let mut stmt = self.conn.prepare(
                "SELECT id, name, source, plan_output, checksum, status,
                        created_at, created_by, approved_at, approved_by,
                        applied_at, applied_by, error, environment
                 FROM plans WHERE status = ?1 ORDER BY created_at DESC",
            )?;
            let rows = stmt.query_map(params![st], |row| {
                Ok(PlanRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    source: row.get(2)?,
                    plan_output: row.get(3)?,
                    checksum: row.get(4)?,
                    status: row.get(5)?,
                    created_at: row.get(6)?,
                    created_by: row.get(7)?,
                    approved_at: row.get(8)?,
                    approved_by: row.get(9)?,
                    applied_at: row.get(10)?,
                    applied_by: row.get(11)?,
                    error: row.get(12)?,
                    environment: row.get(13)?,
                })
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT id, name, source, plan_output, checksum, status,
                        created_at, created_by, approved_at, approved_by,
                        applied_at, applied_by, error, environment
                 FROM plans ORDER BY created_at DESC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(PlanRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    source: row.get(2)?,
                    plan_output: row.get(3)?,
                    checksum: row.get(4)?,
                    status: row.get(5)?,
                    created_at: row.get(6)?,
                    created_by: row.get(7)?,
                    approved_at: row.get(8)?,
                    approved_by: row.get(9)?,
                    applied_at: row.get(10)?,
                    applied_by: row.get(11)?,
                    error: row.get(12)?,
                    environment: row.get(13)?,
                })
            })?;
            let mut result = Vec::new();
            for r in rows {
                result.push(r?);
            }
            Ok(result)
        }
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
        let changed = self.conn.execute("DELETE FROM plans WHERE id = ?1", params![id])?;
        if changed == 0 {
            return Err(RegistryError::NotFound {
                entity: "plan".into(),
                id: id.into(),
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Applied Files (migration tracking)
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
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

    pub fn get_applied_file_by_hash(&self, file_hash: &str) -> Result<Option<AppliedFileRow>, RegistryError> {
        let row = self.conn
            .query_row(
                "SELECT id, file_path, file_hash, statements_count, applied_at, applied_by, environment, status
                 FROM applied_files WHERE file_hash = ?1 ORDER BY applied_at DESC LIMIT 1",
                params![file_hash],
                |row| {
                    Ok(AppliedFileRow {
                        id: row.get(0)?,
                        file_path: row.get(1)?,
                        file_hash: row.get(2)?,
                        statements_count: row.get(3)?,
                        applied_at: row.get(4)?,
                        applied_by: row.get(5)?,
                        environment: row.get(6)?,
                        status: row.get(7)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn list_applied_files(&self) -> Result<Vec<AppliedFileRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, file_path, file_hash, statements_count, applied_at, applied_by, environment, status
             FROM applied_files ORDER BY applied_at",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AppliedFileRow {
                id: row.get(0)?,
                file_path: row.get(1)?,
                file_hash: row.get(2)?,
                statements_count: row.get(3)?,
                applied_at: row.get(4)?,
                applied_by: row.get(5)?,
                environment: row.get(6)?,
                status: row.get(7)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
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

    // ── Import log ──────────────────────────────────────────

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a provider for FK satisfaction.
    fn seed_provider(reg: &Registry, id: &str) {
        reg.insert_provider(id, "kvm", "firecracker", "healthy", true, Some("h1"), None, "auth-1", None, None)
            .unwrap();
    }

    // -- 1. Fresh init creates all tables -----------------------------------

    #[test]
    fn fresh_init_creates_all_tables() {
        let reg = Registry::open_in_memory().unwrap();

        let expected_tables = [
            "schema_version",
            "providers",
            "capabilities",
            "clusters",
            "cluster_members",
            "images",
            "volumes",
            "microvms",
            "snapshots",
            "events",
            "metrics",
            "query_history",
            "audit_log",
            "principals",
            "grants",
            "resources",
            "state_snapshots",
            "plans",
        ];

        let mut stmt = reg
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        for t in &expected_tables {
            assert!(tables.contains(&t.to_string()), "missing table: {t}");
        }
    }

    // -- 2. Provider CRUD ---------------------------------------------------

    #[test]
    fn provider_insert_get_list_delete() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-1");

        // Get
        let p = reg.get_provider("prov-1").unwrap();
        assert_eq!(p.id, "prov-1");
        assert_eq!(p.provider_type, "kvm");
        assert_eq!(p.driver, "firecracker");
        assert_eq!(p.status, "healthy");
        assert!(p.enabled);
        assert_eq!(p.host.as_deref(), Some("h1"));
        assert_eq!(p.auth_ref, "auth-1");

        // List
        seed_provider(&reg, "prov-2");
        let all = reg.list_providers().unwrap();
        assert_eq!(all.len(), 2);

        // Update status
        reg.update_provider_status("prov-1", "degraded").unwrap();
        let p2 = reg.get_provider("prov-1").unwrap();
        assert_eq!(p2.status, "degraded");

        // Delete
        reg.delete_provider("prov-1").unwrap();
        assert!(reg.get_provider("prov-1").is_err());
        assert_eq!(reg.list_providers().unwrap().len(), 1);
    }

    #[test]
    fn provider_not_found() {
        let reg = Registry::open_in_memory().unwrap();
        let err = reg.get_provider("no-such").unwrap_err();
        assert!(matches!(err, RegistryError::NotFound { .. }));
    }

    // -- 3. MicroVM CRUD ----------------------------------------------------

    #[test]
    fn microvm_insert_get_list_delete() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-1");

        reg.insert_microvm("vm-1", "prov-1", "tenant-a", "running", None, Some(2), Some(512), Some("host1"), None, None)
            .unwrap();

        let vm = reg.get_microvm("vm-1").unwrap();
        assert_eq!(vm.id, "vm-1");
        assert_eq!(vm.provider_id, "prov-1");
        assert_eq!(vm.tenant, "tenant-a");
        assert_eq!(vm.status, "running");
        assert_eq!(vm.vcpus, Some(2));
        assert_eq!(vm.memory_mb, Some(512));

        // List
        reg.insert_microvm("vm-2", "prov-1", "tenant-b", "creating", None, Some(4), Some(1024), None, None, None)
            .unwrap();
        assert_eq!(reg.list_microvms().unwrap().len(), 2);

        // Update status
        reg.update_microvm_status("vm-1", "stopped").unwrap();
        assert_eq!(reg.get_microvm("vm-1").unwrap().status, "stopped");

        // Delete
        reg.delete_microvm("vm-1").unwrap();
        assert!(reg.get_microvm("vm-1").is_err());
        assert_eq!(reg.list_microvms().unwrap().len(), 1);
    }

    // -- 4. Volume CRUD + attach/detach -------------------------------------

    #[test]
    fn volume_insert_get_list_delete() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-1");

        reg.insert_volume("vol-1", "prov-1", "ssd", 100, "available", Some(3000), false, None)
            .unwrap();

        let v = reg.get_volume("vol-1").unwrap();
        assert_eq!(v.id, "vol-1");
        assert_eq!(v.volume_type, "ssd");
        assert_eq!(v.size_gb, 100);
        assert_eq!(v.status, "available");
        assert!(!v.encrypted);

        // List
        reg.insert_volume("vol-2", "prov-1", "hdd", 500, "available", None, true, None)
            .unwrap();
        assert_eq!(reg.list_volumes().unwrap().len(), 2);

        // Update status
        reg.update_volume_status("vol-1", "creating").unwrap();
        assert_eq!(reg.get_volume("vol-1").unwrap().status, "creating");

        // Delete
        reg.delete_volume("vol-2").unwrap();
        assert_eq!(reg.list_volumes().unwrap().len(), 1);
    }

    #[test]
    fn volume_attach_detach() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-1");
        reg.insert_volume("vol-1", "prov-1", "ssd", 50, "available", None, false, None)
            .unwrap();

        // Attach
        reg.attach_volume("vol-1", "vm-99", "/dev/vdb").unwrap();
        let v = reg.get_volume("vol-1").unwrap();
        assert_eq!(v.status, "attached");
        assert_eq!(v.microvm_id.as_deref(), Some("vm-99"));
        assert_eq!(v.device_name.as_deref(), Some("/dev/vdb"));

        // Detach
        reg.detach_volume("vol-1").unwrap();
        let v2 = reg.get_volume("vol-1").unwrap();
        assert_eq!(v2.status, "available");
        assert!(v2.microvm_id.is_none());
        assert!(v2.device_name.is_none());
    }

    // -- 5. Image CRUD ------------------------------------------------------

    #[test]
    fn image_insert_get_list_delete() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_image(
            "img-1", "ubuntu-22", "linux", "ubuntu", "22.04", "x86_64",
            "kernel+rootfs", None, Some("/boot/vmlinux"), Some("/root.ext4"),
            None, None, "local", Some("abc123"), Some(256), "available", None,
        )
        .unwrap();

        let img = reg.get_image("img-1").unwrap();
        assert_eq!(img.id, "img-1");
        assert_eq!(img.name, "ubuntu-22");
        assert_eq!(img.os, "linux");
        assert_eq!(img.arch, "x86_64");
        assert_eq!(img.status, "available");

        // List
        reg.insert_image(
            "img-2", "alpine", "linux", "alpine", "3.18", "aarch64",
            "disk", None, None, None, Some("/disk.qcow2"), None,
            "registry", None, Some(64), "importing", None,
        )
        .unwrap();
        assert_eq!(reg.list_images().unwrap().len(), 2);

        // Update status
        reg.update_image_status("img-2", "available").unwrap();
        assert_eq!(reg.get_image("img-2").unwrap().status, "available");

        // Delete
        reg.delete_image("img-1").unwrap();
        assert!(reg.get_image("img-1").is_err());
        assert_eq!(reg.list_images().unwrap().len(), 1);
    }

    // -- 6. Audit log (append-only) -----------------------------------------

    #[test]
    fn audit_log_append_and_list() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_audit_log(Some("admin"), "CREATE", Some("microvm"), Some("vm-1"), "permitted", None, None)
            .unwrap();
        reg.insert_audit_log(Some("admin"), "DELETE", Some("microvm"), Some("vm-1"), "denied", Some("no permission"), None)
            .unwrap();
        reg.insert_audit_log(None, "SELECT", None, None, "permitted", None, None)
            .unwrap();

        // List all
        let all = reg.list_audit_log(None).unwrap();
        assert_eq!(all.len(), 3);

        // List with limit
        let limited = reg.list_audit_log(Some(2)).unwrap();
        assert_eq!(limited.len(), 2);

        // Verify no DELETE on audit_log table exists in our API — append-only by design.
    }

    // -- 7. State survives close/reopen (temp file) -------------------------

    #[test]
    fn state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_str = db_path.to_str().unwrap();

        // Open, write, drop (close).
        {
            let reg = Registry::open(db_str).unwrap();
            seed_provider(&reg, "prov-persist");
            reg.insert_microvm(
                "vm-persist", "prov-persist", "t1", "running",
                None, Some(1), Some(256), None, None, None,
            )
            .unwrap();
        }

        // Re-open and verify data is still there.
        {
            let reg = Registry::open(db_str).unwrap();
            let p = reg.get_provider("prov-persist").unwrap();
            assert_eq!(p.id, "prov-persist");

            let vm = reg.get_microvm("vm-persist").unwrap();
            assert_eq!(vm.id, "vm-persist");
            assert_eq!(vm.status, "running");

            assert_eq!(reg.list_providers().unwrap().len(), 1);
            assert_eq!(reg.list_microvms().unwrap().len(), 1);
        }
    }

    // -- 8. Clusters --------------------------------------------------------

    #[test]
    fn cluster_crud() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-1");

        reg.insert_cluster("cl-1", "my-cluster").unwrap();
        reg.add_cluster_member("cl-1", "prov-1").unwrap();

        // Remove member
        reg.remove_cluster_member("cl-1", "prov-1").unwrap();
        assert!(reg.remove_cluster_member("cl-1", "prov-1").is_err());

        // Delete cluster
        reg.delete_cluster("cl-1").unwrap();
        assert!(reg.delete_cluster("cl-1").is_err());
    }

    // -- 9. Principals & Grants ---------------------------------------------

    #[test]
    fn principal_and_grant_crud() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_principal("usr-1", "user", "oidc:alice", true).unwrap();
        reg.insert_grant("grant-1", "usr-1", r#"["SELECT","CREATE"]"#, "global", None, None, Some("root"))
            .unwrap();

        let grants = reg.get_grants_for_principal("usr-1").unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].verbs, r#"["SELECT","CREATE"]"#);
        assert_eq!(grants[0].scope_type, "global");

        reg.delete_grant("grant-1").unwrap();
        assert!(reg.get_grants_for_principal("usr-1").unwrap().is_empty());
    }

    // -- 10. Query history --------------------------------------------------

    #[test]
    fn query_history_insert() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_query_history(
            Some("admin"), "SELECT * FROM microvms", Some("SELECT * FROM microvms"),
            "SELECT", Some(r#"["microvms"]"#), Some(5), "ok", None, Some(10), None,
        )
        .unwrap();

        // Verify a row landed (query directly since we only have insert in the API).
        let count: i64 = reg
            .conn
            .query_row("SELECT COUNT(*) FROM query_history", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    // -- 11. Duplicate insert returns AlreadyExists -------------------------

    #[test]
    fn duplicate_provider_returns_already_exists() {
        let reg = Registry::open_in_memory().unwrap();
        seed_provider(&reg, "prov-dup");
        let err = reg
            .insert_provider("prov-dup", "kvm", "firecracker", "healthy", true, None, None, "auth", None, None)
            .unwrap_err();
        assert!(matches!(err, RegistryError::AlreadyExists { .. }));
    }

    // -- 12. Resource CRUD ----------------------------------------------------

    #[test]
    fn resource_insert_get_list_delete() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_resource(
            "db-1", "postgres", "azure.eastus", Some("acme-db"),
            "creating", Some(r#"{"version":"16","sku":"Standard_B1ms"}"#), None,
        )
        .unwrap();

        // Get
        let r = reg.get_resource("db-1").unwrap();
        assert_eq!(r.id, "db-1");
        assert_eq!(r.resource_type, "postgres");
        assert_eq!(r.provider_id, "azure.eastus");
        assert_eq!(r.name.as_deref(), Some("acme-db"));
        assert_eq!(r.status, "creating");
        assert!(r.config.is_some());

        // List all
        reg.insert_resource(
            "cache-1", "redis", "azure.eastus", None,
            "creating", Some(r#"{"sku":"Standard"}"#), None,
        )
        .unwrap();
        let all = reg.list_resources().unwrap();
        assert_eq!(all.len(), 2);

        // List by type
        let pg_only = reg.list_resources_by_type("postgres").unwrap();
        assert_eq!(pg_only.len(), 1);
        assert_eq!(pg_only[0].id, "db-1");

        let redis_only = reg.list_resources_by_type("redis").unwrap();
        assert_eq!(redis_only.len(), 1);
        assert_eq!(redis_only[0].id, "cache-1");

        // Update status
        reg.update_resource_status("db-1", "running").unwrap();
        let r2 = reg.get_resource("db-1").unwrap();
        assert_eq!(r2.status, "running");
        assert!(r2.updated_at.is_some());

        // Update outputs
        reg.update_resource_outputs("db-1", r#"{"connection_string":"host=..."}"#).unwrap();
        let r3 = reg.get_resource("db-1").unwrap();
        assert!(r3.outputs.is_some());
        assert!(r3.outputs.unwrap().contains("connection_string"));

        // Update config
        reg.update_resource_config("db-1", r#"{"version":"16","sku":"Standard_B2s"}"#).unwrap();
        let r4 = reg.get_resource("db-1").unwrap();
        assert!(r4.config.unwrap().contains("Standard_B2s"));

        // Delete
        reg.delete_resource("cache-1").unwrap();
        assert_eq!(reg.list_resources().unwrap().len(), 1);
        assert!(reg.get_resource("cache-1").is_err());
    }

    #[test]
    fn resource_not_found() {
        let reg = Registry::open_in_memory().unwrap();
        let err = reg.get_resource("no-such").unwrap_err();
        assert!(matches!(err, RegistryError::NotFound { .. }));
    }

    #[test]
    fn resource_duplicate_returns_already_exists() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_resource("dup-1", "postgres", "prov-1", None, "creating", None, None)
            .unwrap();
        let err = reg
            .insert_resource("dup-1", "postgres", "prov-1", None, "creating", None, None)
            .unwrap_err();
        assert!(matches!(err, RegistryError::AlreadyExists { .. }));
    }

    // -- State Snapshot tests ------------------------------------------------

    #[test]
    fn test_state_snapshot_insert_and_get_last() {
        let reg = Registry::open_in_memory().unwrap();

        // No snapshots initially
        assert!(reg.get_last_snapshot().unwrap().is_none());

        reg.insert_state_snapshot(
            "snap-1",
            None,
            "DESTROY RESOURCE 'postgres' 'db1'",
            "resource",
            "db1",
            Some(r#"{"id":"db1","resource_type":"postgres","status":"available"}"#),
        )
        .unwrap();

        let snap = reg.get_last_snapshot().unwrap().unwrap();
        assert_eq!(snap.id, "snap-1");
        assert_eq!(snap.target_type, "resource");
        assert_eq!(snap.target_id, "db1");
        assert!(snap.tag.is_none());
        assert!(snap.previous_state.is_some());
    }

    #[test]
    fn test_state_snapshot_by_tag() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_state_snapshot(
            "snap-1",
            Some("pre-migration"),
            "ALTER RESOURCE 'postgres' 'db1' SET status = 'upgrading'",
            "resource",
            "db1",
            Some(r#"{"id":"db1","status":"available"}"#),
        )
        .unwrap();

        reg.insert_state_snapshot(
            "snap-2",
            None,
            "DESTROY RESOURCE 'redis' 'cache1'",
            "resource",
            "cache1",
            Some(r#"{"id":"cache1","status":"available"}"#),
        )
        .unwrap();

        let by_tag = reg.get_snapshot_by_tag("pre-migration").unwrap().unwrap();
        assert_eq!(by_tag.id, "snap-1");
        assert_eq!(by_tag.tag.as_deref(), Some("pre-migration"));

        // Non-existent tag
        assert!(reg.get_snapshot_by_tag("no-such-tag").unwrap().is_none());
    }

    #[test]
    fn test_state_snapshot_for_resource() {
        let reg = Registry::open_in_memory().unwrap();

        reg.insert_state_snapshot(
            "snap-1",
            None,
            "DESTROY RESOURCE 'postgres' 'db1'",
            "resource",
            "db1",
            Some(r#"{"id":"db1","resource_type":"postgres"}"#),
        )
        .unwrap();

        reg.insert_state_snapshot(
            "snap-2",
            None,
            "DESTROY MICROVM 'vm-1'",
            "microvm",
            "vm-1",
            Some(r#"{"id":"vm-1","status":"running"}"#),
        )
        .unwrap();

        let snap = reg
            .get_snapshot_for_resource("resource", "db1")
            .unwrap()
            .unwrap();
        assert_eq!(snap.id, "snap-1");

        let snap_vm = reg
            .get_snapshot_for_resource("microvm", "vm-1")
            .unwrap()
            .unwrap();
        assert_eq!(snap_vm.id, "snap-2");

        // Non-existent
        assert!(reg
            .get_snapshot_for_resource("resource", "nope")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_list_snapshots() {
        let reg = Registry::open_in_memory().unwrap();

        for i in 0..5 {
            reg.insert_state_snapshot(
                &format!("snap-{i}"),
                None,
                &format!("stmt-{i}"),
                "resource",
                &format!("res-{i}"),
                None,
            )
            .unwrap();
        }

        let all = reg.list_snapshots(None).unwrap();
        assert_eq!(all.len(), 5);

        let limited = reg.list_snapshots(Some(2)).unwrap();
        assert_eq!(limited.len(), 2);
    }

    // -- Plan tests ---------------------------------------------------------

    #[test]
    fn test_plan_insert_and_get() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_plan(
            "plan-abc12345",
            Some("deploy-vms"),
            "CREATE MICROVM vcpus=2 memory_mb=512;",
            r#"{"actions":["create"]}"#,
            "sha256checksum",
            None,
        )
        .unwrap();

        let plan = reg.get_plan("plan-abc12345").unwrap();
        assert_eq!(plan.id, "plan-abc12345");
        assert_eq!(plan.name.as_deref(), Some("deploy-vms"));
        assert_eq!(plan.source, "CREATE MICROVM vcpus=2 memory_mb=512;");
        assert_eq!(plan.plan_output, r#"{"actions":["create"]}"#);
        assert_eq!(plan.checksum, "sha256checksum");
        assert_eq!(plan.status, "pending");
        assert!(plan.approved_at.is_none());
        assert!(plan.applied_at.is_none());
        assert!(plan.error.is_none());
        assert!(plan.environment.is_none());
    }

    #[test]
    fn test_plan_approve() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_plan(
            "plan-approve",
            None,
            "SELECT * FROM microvms;",
            "{}",
            "checksum1",
            None,
        )
        .unwrap();

        reg.approve_plan("plan-approve", Some("alice")).unwrap();
        let plan = reg.get_plan("plan-approve").unwrap();
        assert_eq!(plan.status, "approved");
        assert!(plan.approved_at.is_some());
        assert_eq!(plan.approved_by.as_deref(), Some("alice"));
    }

    #[test]
    fn test_plan_apply_status() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_plan(
            "plan-apply",
            None,
            "CREATE MICROVM vcpus=4;",
            "{}",
            "checksum2",
            None,
        )
        .unwrap();

        reg.approve_plan("plan-apply", Some("bob")).unwrap();
        reg.update_plan_status("plan-apply", "applied", Some("deployer"), None)
            .unwrap();

        let plan = reg.get_plan("plan-apply").unwrap();
        assert_eq!(plan.status, "applied");
        assert!(plan.applied_at.is_some());
        assert_eq!(plan.applied_by.as_deref(), Some("deployer"));
    }

    #[test]
    fn test_plan_list_by_status() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_plan("plan-1", None, "s1", "{}", "c1", None).unwrap();
        reg.insert_plan("plan-2", None, "s2", "{}", "c2", None).unwrap();
        reg.insert_plan("plan-3", None, "s3", "{}", "c3", None).unwrap();

        reg.approve_plan("plan-2", None).unwrap();

        let all = reg.list_plans(None).unwrap();
        assert_eq!(all.len(), 3);

        let pending = reg.list_plans(Some("pending")).unwrap();
        assert_eq!(pending.len(), 2);

        let approved = reg.list_plans(Some("approved")).unwrap();
        assert_eq!(approved.len(), 1);
        assert_eq!(approved[0].id, "plan-2");
    }

    // ── Applied Files (migration tracking) ──────────────────────────

    #[test]
    fn test_applied_file_tracking() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_applied_file("af-1", "infra.kvmql", "abc123hash", 5, None)
            .unwrap();

        let found = reg.get_applied_file_by_hash("abc123hash").unwrap();
        assert!(found.is_some());
        let row = found.unwrap();
        assert_eq!(row.id, "af-1");
        assert_eq!(row.file_path, "infra.kvmql");
        assert_eq!(row.file_hash, "abc123hash");
        assert_eq!(row.statements_count, 5);
        assert_eq!(row.status, "applied");

        let all = reg.list_applied_files().unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_applied_file_prevents_rerun() {
        let reg = Registry::open_in_memory().unwrap();
        let hash = "deadbeef1234567890abcdef";
        reg.insert_applied_file("af-2", "setup.kvmql", hash, 3, Some("prod"))
            .unwrap();

        // Looking up by hash should return the record
        let found = reg.get_applied_file_by_hash(hash).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().environment, Some("prod".into()));

        // A different hash should not match
        let not_found = reg.get_applied_file_by_hash("different-hash").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_applied_file_status_update() {
        let reg = Registry::open_in_memory().unwrap();
        reg.insert_applied_file("af-3", "failing.kvmql", "failhash", 2, None)
            .unwrap();

        reg.update_applied_file_status("af-3", "failed").unwrap();

        let row = reg.get_applied_file_by_hash("failhash").unwrap().unwrap();
        assert_eq!(row.status, "failed");
    }
}
