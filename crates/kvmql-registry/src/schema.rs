/// DDL for all registry tables — schema version 1.
pub const SCHEMA_V1: &str = r#"
-- 1. schema_version
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
);

-- 2. providers
CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('kvm','aws','gcp','azure','cloudflare','github','kubernetes','ssh')),
    driver TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown' CHECK (status IN ('healthy','degraded','offline','unknown')),
    enabled INTEGER NOT NULL DEFAULT 1,
    host TEXT,
    region TEXT,
    auth_ref TEXT NOT NULL,
    labels TEXT,
    latency_ms INTEGER,
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen TEXT
);

-- 3. capabilities
CREATE TABLE IF NOT EXISTS capabilities (
    provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    supported INTEGER NOT NULL,
    notes TEXT,
    PRIMARY KEY (provider_id, capability)
);

-- 4. clusters
CREATE TABLE IF NOT EXISTS clusters (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 5. cluster_members
CREATE TABLE IF NOT EXISTS cluster_members (
    cluster_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id, provider_id)
);

-- 6. images
CREATE TABLE IF NOT EXISTS images (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    os TEXT NOT NULL CHECK (os IN ('linux','windows')),
    distro TEXT NOT NULL,
    version TEXT NOT NULL,
    arch TEXT NOT NULL CHECK (arch IN ('x86_64','aarch64')),
    type TEXT NOT NULL CHECK (type IN ('kernel+rootfs','disk','ami','machine_image')),
    provider_id TEXT REFERENCES providers(id),
    kernel_path TEXT,
    rootfs_path TEXT,
    disk_path TEXT,
    cloud_ref TEXT,
    source TEXT NOT NULL,
    checksum_sha256 TEXT,
    size_mb INTEGER,
    status TEXT NOT NULL DEFAULT 'importing' CHECK (status IN ('available','importing','publishing','removing','error')),
    imported_at TEXT NOT NULL DEFAULT (datetime('now')),
    labels TEXT
);

-- 7. volumes
CREATE TABLE IF NOT EXISTS volumes (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES providers(id),
    microvm_id TEXT,
    type TEXT NOT NULL,
    size_gb INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'available' CHECK (status IN ('creating','available','attaching','attached','detaching','resizing','deleting','error')),
    device_name TEXT,
    iops INTEGER,
    encrypted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    labels TEXT
);

-- 8. microvms
CREATE TABLE IF NOT EXISTS microvms (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES providers(id),
    tenant TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('creating','running','stopped','paused','stopping','destroying','snapshotting','error','unknown')),
    image_id TEXT REFERENCES images(id),
    vcpus INTEGER,
    memory_mb INTEGER,
    cpu_pct REAL,
    mem_used_mb INTEGER,
    net_rx_kbps REAL,
    net_tx_kbps REAL,
    hostname TEXT,
    metadata TEXT,
    labels TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen TEXT,
    is_stale INTEGER NOT NULL DEFAULT 0
);

-- 9. snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    microvm_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    tag TEXT,
    size_mb INTEGER,
    taken_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 10. events
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_time TEXT NOT NULL DEFAULT (datetime('now')),
    event_type TEXT NOT NULL,
    microvm_id TEXT,
    volume_id TEXT,
    image_id TEXT,
    provider_id TEXT,
    principal TEXT,
    detail TEXT
);

-- 11. metrics
CREATE TABLE IF NOT EXISTS metrics (
    id TEXT PRIMARY KEY,
    microvm_id TEXT NOT NULL,
    sampled_at TEXT NOT NULL DEFAULT (datetime('now')),
    cpu_pct REAL,
    mem_used_mb INTEGER,
    net_rx_kbps REAL,
    net_tx_kbps REAL
);

-- 12. query_history
CREATE TABLE IF NOT EXISTS query_history (
    id TEXT PRIMARY KEY,
    executed_at TEXT NOT NULL DEFAULT (datetime('now')),
    principal TEXT,
    statement TEXT NOT NULL,
    normalized_stmt TEXT,
    verb TEXT NOT NULL,
    targets TEXT,
    duration_ms INTEGER,
    status TEXT NOT NULL CHECK (status IN ('ok','warn','error')),
    notifications TEXT,
    rows_affected INTEGER,
    result_hash TEXT
);

-- 13. audit_log
CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    event_time TEXT NOT NULL DEFAULT (datetime('now')),
    principal TEXT,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('permitted','denied')),
    reason TEXT,
    detail TEXT
);

-- 14. principals
CREATE TABLE IF NOT EXISTS principals (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('user','service','token')),
    auth_ref TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    enabled INTEGER NOT NULL DEFAULT 1
);

-- 15. resources
CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'creating',
    config TEXT,
    outputs TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT,
    labels TEXT
);

-- 16. import_log
CREATE TABLE IF NOT EXISTS import_log (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('new','existing','missing','error')),
    details TEXT,
    imported_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_import_log_provider ON import_log(provider_id);
CREATE INDEX IF NOT EXISTS idx_import_log_action ON import_log(action);

-- 17. grants
CREATE TABLE IF NOT EXISTS grants (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    verbs TEXT NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('cluster','provider','global')),
    scope_id TEXT,
    conditions TEXT,
    granted_at TEXT NOT NULL DEFAULT (datetime('now')),
    granted_by TEXT
);
"#;
