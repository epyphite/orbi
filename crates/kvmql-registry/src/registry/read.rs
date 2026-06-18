use rusqlite::{params, OptionalExtension};

use crate::error::RegistryError;

use super::{
    AppliedFileRow, AuditLogRow, ClusterMemberRow, ClusterRow, EventRow, GrantRow, ImageRow,
    ImportLogRow, MetricRow, MicrovmRow, PlanRow, PrincipalRow, ProviderRow, QueryHistoryRow,
    Registry, ResourceRow, SnapshotRow, StateSnapshotRow, VolumeRow,
};

impl Registry {
    // -----------------------------------------------------------------------
    // Provider reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // MicroVM reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Volume reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Image reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Audit log reads
    // -----------------------------------------------------------------------

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
    // Query history reads
    // -----------------------------------------------------------------------

    pub fn list_query_history(
        &self,
        limit: Option<i64>,
    ) -> Result<Vec<QueryHistoryRow>, RegistryError> {
        let sql = match limit {
            Some(n) => format!(
                "SELECT id, executed_at, principal, statement, normalized_stmt, verb, targets,
                        duration_ms, status, notifications, rows_affected, result_hash
                 FROM query_history ORDER BY executed_at DESC LIMIT {n}"
            ),
            None => "SELECT id, executed_at, principal, statement, normalized_stmt, verb, targets,
                            duration_ms, status, notifications, rows_affected, result_hash
                     FROM query_history ORDER BY executed_at DESC"
                .to_string(),
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
    // Principals & Grants reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Cluster reads
    // -----------------------------------------------------------------------

    pub fn list_clusters(&self) -> Result<Vec<ClusterRow>, RegistryError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, created_at FROM clusters ORDER BY created_at")?;
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

    // -----------------------------------------------------------------------
    // Snapshot reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Event reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Metric reads
    // -----------------------------------------------------------------------

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
    // Resource reads
    // -----------------------------------------------------------------------

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

    pub fn list_resources_by_type(
        &self,
        resource_type: &str,
    ) -> Result<Vec<ResourceRow>, RegistryError> {
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

    // -----------------------------------------------------------------------
    // State Snapshot reads
    // -----------------------------------------------------------------------

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

    pub fn get_snapshot_by_tag(
        &self,
        tag: &str,
    ) -> Result<Option<StateSnapshotRow>, RegistryError> {
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

    pub fn list_snapshots(
        &self,
        limit: Option<i64>,
    ) -> Result<Vec<StateSnapshotRow>, RegistryError> {
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
    // Plan reads
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Applied Files reads
    // -----------------------------------------------------------------------

    pub fn get_applied_file_by_hash(
        &self,
        file_hash: &str,
    ) -> Result<Option<AppliedFileRow>, RegistryError> {
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

    // -----------------------------------------------------------------------
    // Import log reads
    // -----------------------------------------------------------------------

    pub fn list_import_log(&self) -> Result<Vec<ImportLogRow>, RegistryError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider_id, resource_type, resource_id, action, details, imported_at
             FROM import_log ORDER BY imported_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ImportLogRow {
                    id: row.get(0)?,
                    provider_id: row.get(1)?,
                    resource_type: row.get(2)?,
                    resource_id: row.get(3)?,
                    action: row.get(4)?,
                    details: row.get(5)?,
                    imported_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
