use super::*;

/// Helper: create a provider for FK satisfaction.
fn seed_provider(reg: &Registry, id: &str) {
    reg.insert_provider(
        id,
        "kvm",
        "firecracker",
        "healthy",
        true,
        Some("h1"),
        None,
        "auth-1",
        None,
        None,
    )
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

    reg.insert_microvm(
        "vm-1",
        "prov-1",
        "tenant-a",
        "running",
        None,
        Some(2),
        Some(512),
        Some("host1"),
        None,
        None,
    )
    .unwrap();

    let vm = reg.get_microvm("vm-1").unwrap();
    assert_eq!(vm.id, "vm-1");
    assert_eq!(vm.provider_id, "prov-1");
    assert_eq!(vm.tenant, "tenant-a");
    assert_eq!(vm.status, "running");
    assert_eq!(vm.vcpus, Some(2));
    assert_eq!(vm.memory_mb, Some(512));

    // List
    reg.insert_microvm(
        "vm-2",
        "prov-1",
        "tenant-b",
        "creating",
        None,
        Some(4),
        Some(1024),
        None,
        None,
        None,
    )
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

    reg.insert_volume(
        "vol-1",
        "prov-1",
        "ssd",
        100,
        "available",
        Some(3000),
        false,
        None,
    )
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
        "img-1",
        "ubuntu-22",
        "linux",
        "ubuntu",
        "22.04",
        "x86_64",
        "kernel+rootfs",
        None,
        Some("/boot/vmlinux"),
        Some("/root.ext4"),
        None,
        None,
        "local",
        Some("abc123"),
        Some(256),
        "available",
        None,
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
        "img-2",
        "alpine",
        "linux",
        "alpine",
        "3.18",
        "aarch64",
        "disk",
        None,
        None,
        None,
        Some("/disk.qcow2"),
        None,
        "registry",
        None,
        Some(64),
        "importing",
        None,
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

    reg.insert_audit_log(
        Some("admin"),
        "CREATE",
        Some("microvm"),
        Some("vm-1"),
        "permitted",
        None,
        None,
    )
    .unwrap();
    reg.insert_audit_log(
        Some("admin"),
        "DELETE",
        Some("microvm"),
        Some("vm-1"),
        "denied",
        Some("no permission"),
        None,
    )
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
            "vm-persist",
            "prov-persist",
            "t1",
            "running",
            None,
            Some(1),
            Some(256),
            None,
            None,
            None,
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

    reg.insert_principal("usr-1", "user", "oidc:alice", true)
        .unwrap();
    reg.insert_grant(
        "grant-1",
        "usr-1",
        r#"["SELECT","CREATE"]"#,
        "global",
        None,
        None,
        Some("root"),
    )
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
        Some("admin"),
        "SELECT * FROM microvms",
        Some("SELECT * FROM microvms"),
        "SELECT",
        Some(r#"["microvms"]"#),
        Some(5),
        "ok",
        None,
        Some(10),
        None,
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
        .insert_provider(
            "prov-dup",
            "kvm",
            "firecracker",
            "healthy",
            true,
            None,
            None,
            "auth",
            None,
            None,
        )
        .unwrap_err();
    assert!(matches!(err, RegistryError::AlreadyExists { .. }));
}

// -- 12. Resource CRUD ----------------------------------------------------

#[test]
fn resource_insert_get_list_delete() {
    let reg = Registry::open_in_memory().unwrap();

    reg.insert_resource(
        "db-1",
        "postgres",
        "azure.eastus",
        Some("acme-db"),
        "creating",
        Some(r#"{"version":"16","sku":"Standard_B1ms"}"#),
        None,
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
        "cache-1",
        "redis",
        "azure.eastus",
        None,
        "creating",
        Some(r#"{"sku":"Standard"}"#),
        None,
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
    reg.update_resource_outputs("db-1", r#"{"connection_string":"host=..."}"#)
        .unwrap();
    let r3 = reg.get_resource("db-1").unwrap();
    assert!(r3.outputs.is_some());
    assert!(r3.outputs.unwrap().contains("connection_string"));

    // Update config
    reg.update_resource_config("db-1", r#"{"version":"16","sku":"Standard_B2s"}"#)
        .unwrap();
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
    reg.insert_plan("plan-1", None, "s1", "{}", "c1", None)
        .unwrap();
    reg.insert_plan("plan-2", None, "s2", "{}", "c2", None)
        .unwrap();
    reg.insert_plan("plan-3", None, "s3", "{}", "c3", None)
        .unwrap();

    reg.approve_plan("plan-2", None).unwrap();

    let all = reg.list_plans(None).unwrap();
    assert_eq!(all.len(), 3);

    let pending = reg.list_plans(Some("pending")).unwrap();
    assert_eq!(pending.len(), 2);

    let approved = reg.list_plans(Some("approved")).unwrap();
    assert_eq!(approved.len(), 1);
    assert_eq!(approved[0].id, "plan-2");
}

// -- Applied Files (migration tracking) ----------------------------------

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
