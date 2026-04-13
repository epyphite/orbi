use kvmql_parser::ast::*;
use kvmql_parser::parser::Parser;

fn parse(input: &str) -> Program {
    Parser::parse(input).unwrap_or_else(|e| panic!("Parse failed: {e}"))
}

fn first_stmt(input: &str) -> Statement {
    let p = parse(input);
    assert_eq!(p.statements.len(), 1, "Expected 1 statement");
    p.statements.into_iter().next().unwrap()
}

// ── SELECT ──────────────────────────────────────────────────────────

#[test]
fn select_star() {
    let stmt = first_stmt("SELECT * FROM microvms;");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.fields, FieldList::All);
            assert_eq!(s.from, SelectSource::Noun(Noun::Microvms));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_fields_with_where() {
    let stmt = first_stmt(
        "SELECT id, tenant, status FROM microvms WHERE status = 'running';",
    );
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(s.fields, FieldList::Fields(ref f) if f.len() == 3));
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_count_star_in_projection() {
    let stmt = first_stmt("SELECT count(*) FROM microvms;");
    match stmt {
        Statement::Select(s) => match s.fields {
            FieldList::Fields(ref fs) => {
                assert_eq!(fs.len(), 1);
                match &fs[0] {
                    kvmql_parser::ast::Field::FnCall { name, star, args } => {
                        assert_eq!(name, "count");
                        assert!(*star);
                        assert!(args.is_empty());
                    }
                    other => panic!("expected FnCall, got {other:?}"),
                }
            }
            _ => panic!("expected Fields"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_function_call_with_arg() {
    let stmt = first_stmt("SELECT sum(size) FROM volumes;");
    match stmt {
        Statement::Select(s) => match s.fields {
            FieldList::Fields(ref fs) => match &fs[0] {
                kvmql_parser::ast::Field::FnCall { name, star, args } => {
                    assert_eq!(name, "sum");
                    assert!(!*star);
                    assert_eq!(args.len(), 1);
                }
                other => panic!("expected FnCall, got {other:?}"),
            },
            _ => panic!("expected Fields"),
        },
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_on_cluster_order_limit() {
    let stmt = first_stmt(
        "SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb \
         FROM microvms ON CLUSTER 'prod' \
         WHERE tenant = 'acme' ORDER BY cpu_pct DESC;",
    );
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.on,
                Some(TargetSpec {
                    target: TargetKind::Cluster(ref c),
                    ..
                }) if c == "prod"
            ));
            assert!(s.order_by.is_some());
            let order = s.order_by.unwrap();
            assert_eq!(order[0].field, "cpu_pct");
            assert_eq!(order[0].direction, SortDirection::Desc);
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_from_audit_log() {
    let stmt = first_stmt(
        "SELECT event_time, principal, action, target_type, target_id, outcome \
         FROM audit_log ORDER BY event_time DESC;",
    );
    match stmt {
        Statement::Select(s) => assert_eq!(s.from, SelectSource::Noun(Noun::AuditLog)),
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_from_query_history_with_limit() {
    let stmt = first_stmt(
        "SELECT executed_at, principal, verb, statement, duration_ms \
         FROM query_history WHERE status = 'error' ORDER BY executed_at DESC LIMIT 20;",
    );
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::QueryHistory));
            assert_eq!(s.limit, Some(20));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_with_group_by() {
    let stmt = first_stmt(
        "SELECT tenant, status FROM microvms GROUP BY tenant, status;",
    );
    match stmt {
        Statement::Select(s) => {
            assert!(s.group_by.is_some());
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_on_provider_live() {
    let stmt = first_stmt(
        "SELECT * FROM microvms ON PROVIDER 'kvm.host-a' LIVE;",
    );
    match stmt {
        Statement::Select(s) => {
            let on = s.on.unwrap();
            assert!(on.live);
            assert!(matches!(on.target, TargetKind::Provider(ref p) if p == "kvm.host-a"));
        }
        _ => panic!("Expected Select"),
    }
}

// ── CREATE MICROVM ──────────────────────────────────────────────────

#[test]
fn create_microvm_full() {
    let stmt = first_stmt(
        "CREATE MICROVM \
         tenant = 'acme' vcpus = 2 memory_mb = 1024 image = 'ubuntu-22.04-lts' \
         hostname = 'acme-web-01' \
         VOLUME (size_gb = 10 type = 'virtio-blk') \
         VOLUME (id = 'vol-acme-data') \
         ON PROVIDER 'kvm.host-a';",
    );
    match stmt {
        Statement::CreateMicrovm(s) => {
            assert!(s.params.len() >= 4);
            assert_eq!(s.volumes.len(), 2);
            assert!(s.on.is_some());
        }
        _ => panic!("Expected CreateMicrovm"),
    }
}

#[test]
fn create_microvm_with_metadata_labels() {
    let stmt = first_stmt(
        "CREATE MICROVM \
         tenant = 'acme' vcpus = 2 memory_mb = 512 image = 'ubuntu-22.04-lts' \
         metadata = { region: 'sg', tier: 'compute' } \
         labels = { env: 'prod' };",
    );
    match stmt {
        Statement::CreateMicrovm(s) => {
            let meta = s.params.iter().find(|p| p.key == "metadata").unwrap();
            assert!(matches!(meta.value, Value::Map(_)));
        }
        _ => panic!("Expected CreateMicrovm"),
    }
}

#[test]
fn create_microvm_with_placement_require() {
    let stmt = first_stmt(
        "CREATE MICROVM \
         tenant = 'acme' vcpus = 2 memory_mb = 512 image = 'ubuntu-22.04-lts' \
         PLACEMENT POLICY = 'least_loaded' \
         REQUIRE capability = 'hotplug_volume';",
    );
    match stmt {
        Statement::CreateMicrovm(s) => {
            assert_eq!(s.placement_policy, Some("least_loaded".to_string()));
            assert_eq!(s.require.len(), 1);
            assert!(matches!(s.require[0], RequireClause::Capability(ref c) if c == "hotplug_volume"));
        }
        _ => panic!("Expected CreateMicrovm"),
    }
}

// ── CREATE VOLUME ───────────────────────────────────────────────────

#[test]
fn create_volume() {
    let stmt = first_stmt(
        "CREATE VOLUME id = 'vol-acme-data' size_gb = 20 type = 'virtio-blk' \
         ON PROVIDER 'kvm.host-a';",
    );
    match stmt {
        Statement::CreateVolume(s) => {
            assert_eq!(s.params.len(), 3);
            assert!(s.on.is_some());
        }
        _ => panic!("Expected CreateVolume"),
    }
}

// ── ALTER ───────────────────────────────────────────────────────────

#[test]
fn alter_microvm() {
    let stmt = first_stmt("ALTER MICROVM 'vm-abc' SET memory_mb = 2048, vcpus = 4;");
    match stmt {
        Statement::AlterMicrovm(s) => {
            assert_eq!(s.id, "vm-abc");
            assert_eq!(s.set_items.len(), 2);
        }
        _ => panic!("Expected AlterMicrovm"),
    }
}

#[test]
fn alter_volume_labels() {
    let stmt = first_stmt("ALTER VOLUME 'vol-001' SET labels = { env: 'staging' };");
    match stmt {
        Statement::AlterVolume(s) => {
            assert_eq!(s.id, "vol-001");
        }
        _ => panic!("Expected AlterVolume"),
    }
}

#[test]
fn alter_provider() {
    let stmt = first_stmt("ALTER PROVIDER 'aws.us-east-1' SET enabled = true;");
    match stmt {
        Statement::AlterProvider(s) => {
            assert_eq!(s.name, "aws.us-east-1");
        }
        _ => panic!("Expected AlterProvider"),
    }
}

#[test]
fn alter_cluster_add_member() {
    let stmt = first_stmt("ALTER CLUSTER 'prod' ADD MEMBER 'kvm.host-c';");
    match stmt {
        Statement::AlterCluster(s) => {
            assert_eq!(s.name, "prod");
            assert!(matches!(s.action, ClusterAlterAction::AddMember(ref m) if m == "kvm.host-c"));
        }
        _ => panic!("Expected AlterCluster"),
    }
}

#[test]
fn alter_cluster_remove_member() {
    let stmt = first_stmt("ALTER CLUSTER 'prod' REMOVE MEMBER 'kvm.host-b';");
    match stmt {
        Statement::AlterCluster(s) => {
            assert!(matches!(s.action, ClusterAlterAction::RemoveMember(ref m) if m == "kvm.host-b"));
        }
        _ => panic!("Expected AlterCluster"),
    }
}

// ── DESTROY ─────────────────────────────────────────────────────────

#[test]
fn destroy_microvm() {
    let stmt = first_stmt("DESTROY MICROVM 'vm-abc';");
    match stmt {
        Statement::Destroy(s) => {
            assert_eq!(s.target, DestroyTarget::Microvm);
            assert_eq!(s.id, "vm-abc");
            assert!(!s.force);
        }
        _ => panic!("Expected Destroy"),
    }
}

#[test]
fn destroy_volume_force() {
    let stmt = first_stmt("DESTROY VOLUME 'vol-001' FORCE;");
    match stmt {
        Statement::Destroy(s) => {
            assert_eq!(s.target, DestroyTarget::Volume);
            assert!(s.force);
        }
        _ => panic!("Expected Destroy"),
    }
}

// ── Lifecycle ───────────────────────────────────────────────────────

#[test]
fn pause_resume() {
    let p = first_stmt("PAUSE MICROVM 'vm-abc';");
    assert!(matches!(p, Statement::Pause(PauseStmt { id }) if id == "vm-abc"));

    let r = first_stmt("RESUME MICROVM 'vm-abc';");
    assert!(matches!(r, Statement::Resume(ResumeStmt { id }) if id == "vm-abc"));
}

#[test]
fn snapshot_with_tag() {
    let stmt = first_stmt(
        "SNAPSHOT MICROVM 'vm-abc' INTO 's3://snapshots/vm-abc' TAG 'pre-upgrade';",
    );
    match stmt {
        Statement::Snapshot(s) => {
            assert_eq!(s.id, "vm-abc");
            assert_eq!(s.destination, "s3://snapshots/vm-abc");
            assert_eq!(s.tag, Some("pre-upgrade".to_string()));
        }
        _ => panic!("Expected Snapshot"),
    }
}

#[test]
fn restore() {
    let stmt = first_stmt("RESTORE MICROVM 'vm-abc' FROM 's3://snapshots/vm-abc';");
    match stmt {
        Statement::Restore(s) => {
            assert_eq!(s.id, "vm-abc");
            assert_eq!(s.source, "s3://snapshots/vm-abc");
        }
        _ => panic!("Expected Restore"),
    }
}

// ── WATCH ───────────────────────────────────────────────────────────

#[test]
fn watch_metrics() {
    let stmt = first_stmt(
        "WATCH METRIC cpu_pct, mem_used_mb, net_rx_kbps \
         FROM microvms WHERE tenant = 'acme' INTERVAL 5s;",
    );
    match stmt {
        Statement::Watch(s) => {
            assert!(matches!(s.metrics, FieldList::Fields(ref f) if f.len() == 3));
            assert_eq!(s.from, Noun::Microvms);
            assert!(s.where_clause.is_some());
            assert_eq!(s.interval.magnitude, 5);
            assert_eq!(s.interval.unit, DurationUnit::Seconds);
        }
        _ => panic!("Expected Watch"),
    }
}

// ── Volume Ops ──────────────────────────────────────────────────────

#[test]
fn attach_volume() {
    let stmt = first_stmt(
        "ATTACH VOLUME 'vol-acme-logs' TO MICROVM 'vm-abc' AS '/dev/vdc';",
    );
    match stmt {
        Statement::Attach(s) => {
            assert_eq!(s.volume_id, "vol-acme-logs");
            assert_eq!(s.microvm_id, "vm-abc");
            assert_eq!(s.device_name, Some("/dev/vdc".to_string()));
        }
        _ => panic!("Expected Attach"),
    }
}

#[test]
fn detach_volume() {
    let stmt = first_stmt("DETACH VOLUME 'vol-acme-logs' FROM MICROVM 'vm-abc';");
    match stmt {
        Statement::Detach(s) => {
            assert_eq!(s.volume_id, "vol-acme-logs");
            assert_eq!(s.microvm_id, "vm-abc");
        }
        _ => panic!("Expected Detach"),
    }
}

#[test]
fn resize_volume() {
    let stmt = first_stmt("RESIZE VOLUME 'vol-acme-data' TO 40 GB;");
    match stmt {
        Statement::Resize(s) => {
            assert_eq!(s.volume_id, "vol-acme-data");
            assert_eq!(s.new_size_gb, 40);
        }
        _ => panic!("Expected Resize"),
    }
}

// ── Image Management ────────────────────────────────────────────────

#[test]
fn import_image() {
    let stmt = first_stmt(
        "IMPORT IMAGE id = 'ubuntu-22.04-lts' source = 'catalog:ubuntu-22.04-lts';",
    );
    match stmt {
        Statement::ImportImage(s) => {
            assert!(s.params.len() >= 2);
        }
        _ => panic!("Expected ImportImage"),
    }
}

#[test]
fn publish_image() {
    let stmt = first_stmt(
        "PUBLISH IMAGE 'ubuntu-22.04-lts' TO PROVIDER 'aws.ap-southeast-1';",
    );
    match stmt {
        Statement::PublishImage(s) => {
            assert_eq!(s.image_id, "ubuntu-22.04-lts");
            assert_eq!(s.provider, "aws.ap-southeast-1");
        }
        _ => panic!("Expected PublishImage"),
    }
}

#[test]
fn remove_image_force() {
    let stmt = first_stmt("REMOVE IMAGE 'ubuntu-22.04-lts' FORCE;");
    match stmt {
        Statement::RemoveImage(s) => {
            assert_eq!(s.image_id, "ubuntu-22.04-lts");
            assert!(s.force);
        }
        _ => panic!("Expected RemoveImage"),
    }
}

// ── Provider / Cluster Management ───────────────────────────────────

#[test]
fn add_provider() {
    let stmt = first_stmt(
        "ADD PROVIDER id = 'kvm.host-a' type = 'kvm' driver = 'firecracker' \
         host = '192.168.1.10' auth = 'file:/etc/kvmql/creds/host-a.env';",
    );
    match stmt {
        Statement::AddProvider(s) => {
            assert!(s.params.len() >= 4);
        }
        _ => panic!("Expected AddProvider"),
    }
}

#[test]
fn remove_provider() {
    let stmt = first_stmt("REMOVE PROVIDER 'kvm.host-a';");
    match stmt {
        Statement::RemoveProvider(s) => assert_eq!(s.name, "kvm.host-a"),
        _ => panic!("Expected RemoveProvider"),
    }
}

#[test]
fn add_cluster() {
    let stmt = first_stmt(
        "ADD CLUSTER 'prod' MEMBERS = ['kvm.host-a', 'aws.ap-southeast-1'];",
    );
    match stmt {
        Statement::AddCluster(s) => {
            assert_eq!(s.name, "prod");
            assert_eq!(s.members.len(), 2);
        }
        _ => panic!("Expected AddCluster"),
    }
}

#[test]
fn remove_cluster() {
    let stmt = first_stmt("REMOVE CLUSTER 'prod';");
    match stmt {
        Statement::RemoveCluster(s) => assert_eq!(s.name, "prod"),
        _ => panic!("Expected RemoveCluster"),
    }
}

// ── Access Control ──────────────────────────────────────────────────

#[test]
fn add_principal() {
    let stmt = first_stmt(
        "ADD PRINCIPAL id = 'ops@acme.com' type = 'user' auth = 'env:ACME_OPS_TOKEN';",
    );
    assert!(matches!(stmt, Statement::AddPrincipal(_)));
}

#[test]
fn grant_with_where() {
    let stmt = first_stmt(
        "GRANT SELECT, SNAPSHOT ON microvms WHERE tenant = 'acme' TO 'ops@acme.com';",
    );
    match stmt {
        Statement::Grant(s) => {
            assert_eq!(s.verbs.len(), 2);
            assert_eq!(s.verbs[0], Verb::Select);
            assert_eq!(s.verbs[1], Verb::Snapshot);
            assert!(matches!(s.scope, GrantScope::Microvms));
            assert!(s.where_clause.is_some());
            assert_eq!(s.principal, "ops@acme.com");
        }
        _ => panic!("Expected Grant"),
    }
}

#[test]
fn grant_on_volumes() {
    let stmt = first_stmt(
        "GRANT SELECT ON volumes WHERE tenant = 'acme' TO 'ops@acme.com';",
    );
    match stmt {
        Statement::Grant(s) => {
            assert!(matches!(s.scope, GrantScope::Volumes));
        }
        _ => panic!("Expected Grant"),
    }
}

#[test]
fn revoke() {
    let stmt = first_stmt(
        "REVOKE SELECT, DESTROY ON CLUSTER 'prod' FROM 'ops@acme.com';",
    );
    match stmt {
        Statement::Revoke(s) => {
            assert_eq!(s.verbs.len(), 2);
            assert!(matches!(s.scope, GrantScope::Cluster(ref c) if c == "prod"));
            assert_eq!(s.principal, "ops@acme.com");
        }
        _ => panic!("Expected Revoke"),
    }
}

// ── SET / SHOW ──────────────────────────────────────────────────────

#[test]
fn set_execution_mode() {
    let stmt = first_stmt("SET execution_mode = 'strict';");
    match stmt {
        Statement::Set(s) => {
            assert_eq!(s.key, "execution_mode");
            assert_eq!(s.value, Value::String("strict".to_string()));
        }
        _ => panic!("Expected Set"),
    }
}

#[test]
fn show_variants() {
    assert!(matches!(first_stmt("SHOW PROVIDERS;"), Statement::Show(ShowStmt { target: ShowTarget::Providers })));
    assert!(matches!(first_stmt("SHOW CLUSTERS;"), Statement::Show(ShowStmt { target: ShowTarget::Clusters })));
    assert!(matches!(first_stmt("SHOW IMAGES;"), Statement::Show(ShowStmt { target: ShowTarget::Images })));
    assert!(matches!(first_stmt("SHOW VERSION;"), Statement::Show(ShowStmt { target: ShowTarget::Version })));
}

#[test]
fn show_capabilities_for_provider() {
    let stmt = first_stmt("SHOW CAPABILITIES FOR PROVIDER 'kvm.host-a';");
    match stmt {
        Statement::Show(s) => {
            assert!(matches!(
                s.target,
                ShowTarget::Capabilities { for_provider: Some(ref p) } if p == "kvm.host-a"
            ));
        }
        _ => panic!("Expected Show"),
    }
}

#[test]
fn show_grants_for_principal() {
    let stmt = first_stmt("SHOW GRANTS FOR 'ops@acme.com';");
    match stmt {
        Statement::Show(s) => {
            assert!(matches!(
                s.target,
                ShowTarget::Grants { for_principal: Some(ref p) } if p == "ops@acme.com"
            ));
        }
        _ => panic!("Expected Show"),
    }
}

// ── NULL / IS NULL / IS NOT NULL ─────────────────────────────────

#[test]
fn null_value() {
    let stmt = first_stmt("ALTER MICROVM 'vm-abc' SET metadata = null;");
    match stmt {
        Statement::AlterMicrovm(s) => {
            assert_eq!(s.set_items[0].value, Value::Null);
        }
        _ => panic!("Expected AlterMicrovm"),
    }
}

#[test]
fn is_null_predicate() {
    let stmt = first_stmt("SELECT * FROM volumes WHERE microvm_id IS NULL;");
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert_eq!(c.op, ComparisonOp::IsNull);
                assert_eq!(c.left, Expr::Identifier("microvm_id".to_string()));
            } else {
                panic!("Expected IS NULL comparison");
            }
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn is_not_null_predicate() {
    let stmt = first_stmt("SELECT * FROM volumes WHERE microvm_id IS NOT NULL;");
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert_eq!(c.op, ComparisonOp::IsNotNull);
            } else {
                panic!("Expected IS NOT NULL comparison");
            }
        }
        _ => panic!("Expected Select"),
    }
}

// ── OFFSET ──────────────────────────────────────────────────────────

#[test]
fn select_with_offset() {
    let stmt = first_stmt("SELECT * FROM microvms LIMIT 10 OFFSET 20;");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.limit, Some(10));
            assert_eq!(s.offset, Some(20));
        }
        _ => panic!("Expected Select"),
    }
}

// ── Arithmetic Expressions ──────────────────────────────────────────

#[test]
fn arithmetic_subtract() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE cpu_pct > 100 - 10;",
    );
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert!(matches!(c.right, Expr::BinaryOp { op: BinaryOp::Subtract, .. }));
            } else {
                panic!("Expected comparison with arithmetic");
            }
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn arithmetic_add() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE vcpus = 2 + 2;",
    );
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert!(matches!(c.right, Expr::BinaryOp { op: BinaryOp::Add, .. }));
            } else {
                panic!("Expected comparison with arithmetic");
            }
        }
        _ => panic!("Expected Select"),
    }
}

// ── Round-Trip for new features ─────────────────────────────────────

#[test]
fn round_trip_is_null() {
    let input = "SELECT * FROM volumes WHERE microvm_id IS NULL;";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_is_not_null() {
    let input = "SELECT * FROM volumes WHERE microvm_id IS NOT NULL;";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

// ── Multi-Statement ─────────────────────────────────────────────────

#[test]
fn multi_statement_program() {
    let p = parse(
        "SELECT * FROM microvms; \
         DESTROY MICROVM 'vm-abc'; \
         SHOW VERSION;",
    );
    assert_eq!(p.statements.len(), 3);
}

// ── Predicates ──────────────────────────────────────────────────────

#[test]
fn predicate_and_or() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE status = 'running' AND tenant = 'acme';",
    );
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(s.where_clause, Some(Predicate::And(_, _))));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn predicate_grouped() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE (status = 'running' OR status = 'paused') AND tenant = 'acme';",
    );
    match stmt {
        Statement::Select(s) => {
            // Should parse as: AND( Grouped(OR(..)), comparison )
            assert!(matches!(s.where_clause, Some(Predicate::And(_, _))));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn predicate_not() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE NOT status = 'error';",
    );
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(s.where_clause, Some(Predicate::Not(_))));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn predicate_in() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE status IN 'running';",
    );
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert_eq!(c.op, ComparisonOp::In);
            } else {
                panic!("Expected comparison with IN");
            }
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn predicate_like() {
    let stmt = first_stmt(
        "SELECT * FROM microvms WHERE tenant LIKE 'acme%';",
    );
    match stmt {
        Statement::Select(s) => {
            if let Some(Predicate::Comparison(c)) = s.where_clause {
                assert_eq!(c.op, ComparisonOp::Like);
            } else {
                panic!("Expected comparison with LIKE");
            }
        }
        _ => panic!("Expected Select"),
    }
}

// ── Error Cases ─────────────────────────────────────────────────────

#[test]
fn error_unknown_statement() {
    let err = Parser::parse("FOOBAR;").unwrap_err();
    assert!(matches!(err.kind, kvmql_parser::error::ParseErrorKind::UnexpectedToken));
}

#[test]
fn error_missing_from() {
    let err = Parser::parse("SELECT * WHERE status = 'running';").unwrap_err();
    // Should error about expecting FROM
    assert!(err.expected.is_some());
}

#[test]
fn error_invalid_noun() {
    let err = Parser::parse("SELECT * FROM foobar;").unwrap_err();
    assert!(matches!(err.kind, kvmql_parser::error::ParseErrorKind::InvalidNoun { .. }));
}

// ── Round-Trip ──────────────────────────────────────────────────────

#[test]
fn round_trip_select() {
    let input = "SELECT id, tenant, status FROM microvms WHERE status = 'running' ORDER BY cpu_pct DESC LIMIT 10;";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_create_microvm() {
    let input = "CREATE MICROVM tenant = 'acme' vcpus = 2 memory_mb = 512 image = 'ubuntu-22.04-lts' \
                 VOLUME (size_gb = 10 type = 'virtio-blk') \
                 ON PROVIDER 'kvm.host-a';";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_grant() {
    let input = "GRANT SELECT, SNAPSHOT ON microvms WHERE tenant = 'acme' TO 'ops@acme.com';";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

// ── Case Insensitivity ──────────────────────────────────────────────

#[test]
fn keywords_case_insensitive() {
    let stmt = first_stmt("select * from microvms where status = 'running';");
    assert!(matches!(stmt, Statement::Select(_)));
}

// ── Empty Map and Array ─────────────────────────────────────────────

#[test]
fn empty_map() {
    let stmt = first_stmt("ALTER MICROVM 'vm-abc' SET labels = {};");
    match stmt {
        Statement::AlterMicrovm(s) => {
            assert_eq!(s.set_items[0].value, Value::Map(vec![]));
        }
        _ => panic!("Expected AlterMicrovm"),
    }
}

#[test]
fn empty_array() {
    let stmt = first_stmt("ADD CLUSTER 'test' MEMBERS = [];");
    match stmt {
        Statement::AddCluster(s) => {
            assert_eq!(s.members.len(), 0);
        }
        _ => panic!("Expected AddCluster"),
    }
}

// ── Comments ────────────────────────────────────────────────────────

#[test]
fn comments_ignored() {
    let p = parse(
        "-- This is a comment\n\
         SELECT * FROM microvms; -- trailing comment\n\
         -- another comment\n\
         SHOW VERSION;",
    );
    assert_eq!(p.statements.len(), 2);
}

// ── Appendix A Full Session ─────────────────────────────────────────

#[test]
fn appendix_a_full_session() {
    let session = r#"
        ADD PROVIDER
          id     = 'kvm.host-a'
          type   = 'kvm'
          driver = 'firecracker'
          host   = '192.168.1.10'
          auth   = 'file:/etc/kvmql/creds/host-a.env';

        ADD PROVIDER
          id     = 'aws.ap-southeast-1'
          type   = 'aws'
          region = 'ap-southeast-1'
          auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';

        ADD CLUSTER 'prod'
          MEMBERS = ['kvm.host-a', 'aws.ap-southeast-1'];

        IMPORT IMAGE
          id      = 'ubuntu-22.04-lts'
          source  = 'catalog:ubuntu-22.04-lts';

        SELECT id, status, size_mb, arch
        FROM   images
        WHERE  id = 'ubuntu-22.04-lts';

        CREATE VOLUME
          id      = 'vol-acme-data'
          size_gb = 20
          type    = 'virtio-blk'
          ON PROVIDER 'kvm.host-a';

        CREATE MICROVM
          tenant    = 'acme'
          vcpus     = 2
          memory_mb = 1024
          image     = 'ubuntu-22.04-lts'
          hostname  = 'acme-web-01'
          VOLUME (
            size_gb = 10
            type    = 'virtio-blk'
          )
          VOLUME (
            id = 'vol-acme-data'
          )
          ON PROVIDER 'kvm.host-a';

        SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb
        FROM   microvms
        ON CLUSTER 'prod'
        WHERE  tenant = 'acme'
        ORDER BY cpu_pct DESC;

        SELECT id, microvm_id, size_gb, status, device_name
        FROM   volumes
        WHERE  microvm_id = 'vm-abc';

        CREATE VOLUME
          id      = 'vol-acme-logs'
          size_gb = 50
          type    = 'virtio-blk'
          ON PROVIDER 'kvm.host-a';

        ATTACH VOLUME 'vol-acme-logs'
          TO MICROVM 'vm-abc'
          AS '/dev/vdc';

        WATCH METRIC cpu_pct, mem_used_mb, net_rx_kbps
        FROM  microvms
        WHERE tenant = 'acme'
        INTERVAL 5s;

        SNAPSHOT MICROVM 'vm-abc'
          INTO 's3://epiphyte-snapshots/acme/vm-abc-20260318'
          TAG  'pre-upgrade';

        ADD PRINCIPAL
          id   = 'ops@acme.com'
          type = 'user'
          auth = 'env:ACME_OPS_TOKEN';

        GRANT SELECT, SNAPSHOT ON microvms
          WHERE tenant = 'acme'
          TO 'ops@acme.com';

        GRANT SELECT ON volumes
          WHERE tenant = 'acme'
          TO 'ops@acme.com';

        PUBLISH IMAGE 'ubuntu-22.04-lts'
          TO PROVIDER 'aws.ap-southeast-1';

        CREATE MICROVM
          tenant    = 'acme'
          vcpus     = 2
          memory_mb = 1024
          image     = 'ubuntu-22.04-lts'
          VOLUME ( size_gb = 20 type = 'ebs' )
          ON PROVIDER 'aws.ap-southeast-1';

        RESIZE VOLUME 'vol-acme-data' TO 40 GB;

        DETACH VOLUME 'vol-acme-logs' FROM MICROVM 'vm-abc';
        DESTROY MICROVM 'vm-abc';
        DESTROY VOLUME 'vol-acme-data' FORCE;
        DESTROY VOLUME 'vol-acme-logs';
        REMOVE IMAGE 'ubuntu-22.04-lts';
    "#;
    let program = parse(session);
    assert_eq!(program.statements.len(), 24);
}

// ── RESOURCE statements ────────────────────────────────────────────

#[test]
fn create_resource_postgres() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'postgres' id = 'db1' version = '16';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.params.len(), 2);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("db1".into()));
            assert_eq!(s.params[1].key, "version");
            assert_eq!(s.params[1].value, Value::String("16".into()));
            assert!(s.on.is_none());
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn create_resource_with_on_provider() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'postgres' \
         id = 'acme-db' sku = 'Standard_B1ms' storage_gb = 32 version = '16' \
         ON PROVIDER 'azure.eastus';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.params.len(), 4);
            assert!(s.on.is_some());
            let on = s.on.unwrap();
            assert!(matches!(on.target, TargetKind::Provider(ref p) if p == "azure.eastus"));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn create_resource_redis() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'redis' id = 'cache1' sku = 'Standard' capacity = 2;",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "redis");
            assert_eq!(s.params.len(), 3);
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn alter_resource() {
    let stmt = first_stmt(
        "ALTER RESOURCE 'postgres' 'acme-db' SET sku = 'Standard_B2s', storage_gb = 64;",
    );
    match stmt {
        Statement::AlterResource(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "acme-db");
            assert_eq!(s.set_items.len(), 2);
            assert_eq!(s.set_items[0].key, "sku");
            assert_eq!(s.set_items[0].value, Value::String("Standard_B2s".into()));
            assert_eq!(s.set_items[1].key, "storage_gb");
            assert_eq!(s.set_items[1].value, Value::Integer(64));
        }
        _ => panic!("Expected AlterResource"),
    }
}

#[test]
fn destroy_resource_force() {
    let stmt = first_stmt(
        "DESTROY RESOURCE 'redis' 'cache1' FORCE;",
    );
    match stmt {
        Statement::DestroyResource(s) => {
            assert_eq!(s.resource_type, "redis");
            assert_eq!(s.id, "cache1");
            assert!(s.force);
        }
        _ => panic!("Expected DestroyResource"),
    }
}

#[test]
fn destroy_resource_no_force() {
    let stmt = first_stmt(
        "DESTROY RESOURCE 'postgres' 'acme-db';",
    );
    match stmt {
        Statement::DestroyResource(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "acme-db");
            assert!(!s.force);
        }
        _ => panic!("Expected DestroyResource"),
    }
}

#[test]
fn select_from_resources() {
    let stmt = first_stmt(
        "SELECT * FROM resources WHERE resource_type = 'postgres';",
    );
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::Resources));
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_from_resources_no_where() {
    let stmt = first_stmt("SELECT * FROM resources;");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::Resources));
            assert!(s.where_clause.is_none());
        }
        _ => panic!("Expected Select"),
    }
}

// ── RESOURCE round-trip ────────────────────────────────────────────

#[test]
fn round_trip_create_resource() {
    let input = "CREATE RESOURCE 'postgres' id = 'db1' version = '16' ON PROVIDER 'azure.eastus';";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_alter_resource() {
    let input = "ALTER RESOURCE 'postgres' 'acme-db' SET sku = 'Standard_B2s', storage_gb = 64;";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_destroy_resource() {
    let input = "DESTROY RESOURCE 'redis' 'cache1' FORCE;";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn round_trip_destroy_resource_no_force() {
    let input = "DESTROY RESOURCE 'postgres' 'acme-db';";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

// ── RESOURCE case insensitivity ────────────────────────────────────

#[test]
fn create_resource_case_insensitive() {
    let stmt = first_stmt(
        "create resource 'aks' id = 'k8s-1' node_count = 3;",
    );
    assert!(matches!(stmt, Statement::CreateResource(_)));
}

// ── RESOURCE mixed with existing statements ────────────────────────

#[test]
fn resource_in_multi_statement() {
    let p = parse(
        "CREATE RESOURCE 'postgres' id = 'db1' version = '16'; \
         SELECT * FROM resources; \
         DESTROY RESOURCE 'postgres' 'db1';",
    );
    assert_eq!(p.statements.len(), 3);
    assert!(matches!(p.statements[0], Statement::CreateResource(_)));
    assert!(matches!(p.statements[1], Statement::Select(_)));
    assert!(matches!(p.statements[2], Statement::DestroyResource(_)));
}

// ── Networking sub-resources ───────────────────────────────────────

#[test]
fn test_create_subnet() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'subnet' id = 'app' vnet = 'acme-vnet' address_prefix = '10.0.0.0/24';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "subnet");
            assert_eq!(s.params.len(), 3);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("app".into()));
            assert_eq!(s.params[1].key, "vnet");
            assert_eq!(s.params[1].value, Value::String("acme-vnet".into()));
            assert_eq!(s.params[2].key, "address_prefix");
            assert_eq!(s.params[2].value, Value::String("10.0.0.0/24".into()));
            assert!(s.on.is_none());
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn test_create_nsg_rule() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'nsg_rule' \
         id = 'allow-ssh' \
         nsg = 'acme-nsg' \
         priority = 100 \
         direction = 'Inbound' \
         access = 'Allow' \
         protocol = 'Tcp' \
         source_address = '10.0.0.0/8' \
         destination_port = 22;",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "nsg_rule");
            assert_eq!(s.params.len(), 8);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("allow-ssh".into()));
            assert_eq!(s.params[1].key, "nsg");
            assert_eq!(s.params[1].value, Value::String("acme-nsg".into()));
            assert_eq!(s.params[2].key, "priority");
            assert_eq!(s.params[2].value, Value::Integer(100));
            assert_eq!(s.params[3].key, "direction");
            assert_eq!(s.params[4].key, "access");
            assert_eq!(s.params[5].key, "protocol");
            assert_eq!(s.params[6].key, "source_address");
            assert_eq!(s.params[7].key, "destination_port");
            assert_eq!(s.params[7].value, Value::Integer(22));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn test_create_vnet_peering() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'vnet_peering' \
         id = 'dev-to-prod' \
         vnet = 'dev-vnet' \
         remote_vnet = '/subscriptions/sub-1/resourceGroups/rg-prod/providers/Microsoft.Network/virtualNetworks/prod-vnet' \
         allow_forwarded_traffic = true;",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "vnet_peering");
            assert_eq!(s.params.len(), 4);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("dev-to-prod".into()));
            assert_eq!(s.params[1].key, "vnet");
            assert_eq!(s.params[1].value, Value::String("dev-vnet".into()));
            assert_eq!(s.params[2].key, "remote_vnet");
            assert_eq!(s.params[3].key, "allow_forwarded_traffic");
            assert_eq!(s.params[3].value, Value::Boolean(true));
        }
        _ => panic!("Expected CreateResource"),
    }
}

// ── Variable Substitution ──────────────────────────────────────────

#[test]
fn set_variable() {
    let stmt = first_stmt("SET @env = 'staging';");
    match stmt {
        Statement::Set(s) => {
            assert_eq!(s.key, "@env");
            assert_eq!(s.value, Value::String("staging".into()));
        }
        _ => panic!("Expected Set"),
    }
}

#[test]
fn set_variable_integer() {
    let stmt = first_stmt("SET @count = 42;");
    match stmt {
        Statement::Set(s) => {
            assert_eq!(s.key, "@count");
            assert_eq!(s.value, Value::Integer(42));
        }
        _ => panic!("Expected Set"),
    }
}

#[test]
fn variable_in_create_resource_param() {
    let stmt = first_stmt("CREATE RESOURCE 'vnet' id = @env;");
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "vnet");
            assert_eq!(s.params.len(), 1);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::Variable("env".into()));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn variable_in_where_clause() {
    let stmt = first_stmt("SELECT * FROM microvms WHERE tenant = @tenant;");
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
            match s.where_clause.unwrap() {
                Predicate::Comparison(c) => {
                    assert_eq!(c.right, Expr::Variable("tenant".into()));
                }
                _ => panic!("Expected comparison"),
            }
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn multiple_variables_in_program() {
    let prog = parse(
        "SET @env = 'staging'; SET @region = 'eastus'; \
         CREATE RESOURCE 'vnet' id = @env address_space = '10.1.0.0/16';",
    );
    assert_eq!(prog.statements.len(), 3);
    match &prog.statements[2] {
        Statement::CreateResource(s) => {
            assert_eq!(s.params[0].value, Value::Variable("env".into()));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn variable_display_roundtrip() {
    let val = Value::Variable("env".into());
    assert_eq!(val.to_string(), "@env");

    let expr = Expr::Variable("region".into());
    assert_eq!(expr.to_string(), "@region");
}

// ── PostgreSQL Database ────────────────────────────────────────────

#[test]
fn test_create_pg_database() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'pg_database' id = 'drivelog' server = 'acme-pg-server';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "pg_database");
            assert_eq!(s.params.len(), 2);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("drivelog".into()));
            assert_eq!(s.params[1].key, "server");
            assert_eq!(s.params[1].value, Value::String("acme-pg-server".into()));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn test_create_pg_database_with_charset() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'pg_database' id = 'userdb' server = 'acme-pg-server' charset = 'UTF8';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "pg_database");
            assert_eq!(s.params.len(), 3);
            assert_eq!(s.params[2].key, "charset");
            assert_eq!(s.params[2].value, Value::String("UTF8".into()));
        }
        _ => panic!("Expected CreateResource"),
    }
}

// ── DNS VNet Link ──────────────────────────────────────────────────

#[test]
fn test_create_dns_vnet_link() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'dns_vnet_link' \
         id = 'pg-dns-link' \
         zone_name = 'privatelink.postgres.database.azure.com' \
         vnet = '/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/acme-vnet' \
         registration_enabled = true;",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "dns_vnet_link");
            assert_eq!(s.params.len(), 4);
            assert_eq!(s.params[0].key, "id");
            assert_eq!(s.params[0].value, Value::String("pg-dns-link".into()));
            assert_eq!(s.params[1].key, "zone_name");
            assert_eq!(s.params[2].key, "vnet");
            assert_eq!(s.params[3].key, "registration_enabled");
            assert_eq!(s.params[3].value, Value::Boolean(true));
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn test_create_dns_vnet_link_minimal() {
    let stmt = first_stmt(
        "CREATE RESOURCE 'dns_vnet_link' id = 'link1' zone_name = 'zone' vnet = 'vnet1';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert_eq!(s.resource_type, "dns_vnet_link");
            assert_eq!(s.params.len(), 3);
        }
        _ => panic!("Expected CreateResource"),
    }
}

// ── Day-2 Operations: BACKUP, RESTORE RESOURCE, SCALE, UPGRADE ────

#[test]
fn test_backup_resource() {
    let stmt = first_stmt("BACKUP RESOURCE 'postgres' 'db1';");
    match stmt {
        Statement::Backup(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "db1");
            assert!(s.destination.is_none());
            assert!(s.tag.is_none());
        }
        _ => panic!("Expected Backup"),
    }
}

#[test]
fn test_backup_with_destination_and_tag() {
    let stmt = first_stmt(
        "BACKUP RESOURCE 'postgres' 'acme-db' INTO 's3://backups/' TAG 'pre-migration';",
    );
    match stmt {
        Statement::Backup(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "acme-db");
            assert_eq!(s.destination.as_deref(), Some("s3://backups/"));
            assert_eq!(s.tag.as_deref(), Some("pre-migration"));
        }
        _ => panic!("Expected Backup"),
    }
}

#[test]
fn test_backup_with_tag_only() {
    let stmt = first_stmt("BACKUP RESOURCE 'postgres' 'db1' TAG 'v1';");
    match stmt {
        Statement::Backup(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "db1");
            assert!(s.destination.is_none());
            assert_eq!(s.tag.as_deref(), Some("v1"));
        }
        _ => panic!("Expected Backup"),
    }
}

#[test]
fn test_restore_resource() {
    let stmt = first_stmt(
        "RESTORE RESOURCE 'postgres' 'acme-db' FROM '2026-04-01T10:00:00Z';",
    );
    match stmt {
        Statement::RestoreResource(s) => {
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.id, "acme-db");
            assert_eq!(s.source, "2026-04-01T10:00:00Z");
        }
        _ => panic!("Expected RestoreResource"),
    }
}

#[test]
fn test_restore_microvm_still_works() {
    // Ensure existing RESTORE MICROVM syntax is not broken
    let stmt = first_stmt("RESTORE MICROVM 'vm-abc' FROM 's3://snapshots/vm-abc';");
    match stmt {
        Statement::Restore(s) => {
            assert_eq!(s.id, "vm-abc");
            assert_eq!(s.source, "s3://snapshots/vm-abc");
        }
        _ => panic!("Expected Restore (MICROVM)"),
    }
}

#[test]
fn test_scale_aks() {
    let stmt = first_stmt("SCALE RESOURCE 'aks' 'acme-k8s' node_count = 5;");
    match stmt {
        Statement::Scale(s) => {
            assert_eq!(s.resource_type, "aks");
            assert_eq!(s.id, "acme-k8s");
            assert_eq!(s.params.len(), 1);
            assert_eq!(s.params[0].key, "node_count");
            assert_eq!(s.params[0].value, Value::Integer(5));
        }
        _ => panic!("Expected Scale"),
    }
}

#[test]
fn test_scale_container_app() {
    let stmt = first_stmt(
        "SCALE RESOURCE 'container_app' 'acme-api' min_replicas = 2 max_replicas = 10;",
    );
    match stmt {
        Statement::Scale(s) => {
            assert_eq!(s.resource_type, "container_app");
            assert_eq!(s.id, "acme-api");
            assert_eq!(s.params.len(), 2);
            assert_eq!(s.params[0].key, "min_replicas");
            assert_eq!(s.params[0].value, Value::Integer(2));
            assert_eq!(s.params[1].key, "max_replicas");
            assert_eq!(s.params[1].value, Value::Integer(10));
        }
        _ => panic!("Expected Scale"),
    }
}

#[test]
fn test_upgrade_aks() {
    let stmt = first_stmt("UPGRADE RESOURCE 'aks' 'acme-k8s' kubernetes_version = '1.29';");
    match stmt {
        Statement::Upgrade(s) => {
            assert_eq!(s.resource_type, "aks");
            assert_eq!(s.id, "acme-k8s");
            assert_eq!(s.params.len(), 1);
            assert_eq!(s.params[0].key, "kubernetes_version");
            assert_eq!(s.params[0].value, Value::String("1.29".into()));
        }
        _ => panic!("Expected Upgrade"),
    }
}

// ── Day-2 round-trip tests ────────────────────────────────────────

#[test]
fn round_trip_backup() {
    let input = "BACKUP RESOURCE 'postgres' 'db1';";
    let p = parse(input);
    let output = format!("{}", p);
    let p2 = parse(&output);
    assert_eq!(p, p2);
}

#[test]
fn round_trip_backup_with_tag() {
    let input = "BACKUP RESOURCE 'postgres' 'db1' INTO '/backups' TAG 'v1';";
    let p = parse(input);
    let output = format!("{}", p);
    let p2 = parse(&output);
    assert_eq!(p, p2);
}

#[test]
fn round_trip_restore_resource() {
    let input = "RESTORE RESOURCE 'postgres' 'acme-db' FROM '2026-04-01T10:00:00Z';";
    let p = parse(input);
    let output = format!("{}", p);
    let p2 = parse(&output);
    assert_eq!(p, p2);
}

#[test]
fn round_trip_scale() {
    let input = "SCALE RESOURCE 'aks' 'k8s1' node_count = 5;";
    let p = parse(input);
    let output = format!("{}", p);
    let p2 = parse(&output);
    assert_eq!(p, p2);
}

#[test]
fn round_trip_upgrade() {
    let input = "UPGRADE RESOURCE 'aks' 'k8s1' kubernetes_version = '1.29';";
    let p = parse(input);
    let output = format!("{}", p);
    let p2 = parse(&output);
    assert_eq!(p, p2);
}

// ── Day-2 case insensitivity ──────────────────────────────────────

#[test]
fn backup_case_insensitive() {
    let stmt = first_stmt("backup resource 'postgres' 'db1';");
    assert!(matches!(stmt, Statement::Backup(_)));
}

#[test]
fn scale_case_insensitive() {
    let stmt = first_stmt("scale resource 'aks' 'k8s1' node_count = 3;");
    assert!(matches!(stmt, Statement::Scale(_)));
}

#[test]
fn upgrade_case_insensitive() {
    let stmt = first_stmt("upgrade resource 'aks' 'k8s1' kubernetes_version = '1.29';");
    assert!(matches!(stmt, Statement::Upgrade(_)));
}

// ── Day-2 mixed with other statements ─────────────────────────────

#[test]
fn day2_in_multi_statement() {
    let p = parse(
        "BACKUP RESOURCE 'postgres' 'db1'; \
         SCALE RESOURCE 'aks' 'k8s1' node_count = 5; \
         UPGRADE RESOURCE 'aks' 'k8s1' kubernetes_version = '1.29';",
    );
    assert_eq!(p.statements.len(), 3);
    assert!(matches!(p.statements[0], Statement::Backup(_)));
    assert!(matches!(p.statements[1], Statement::Scale(_)));
    assert!(matches!(p.statements[2], Statement::Upgrade(_)));
}

// ── EXPLAIN ──────────────────────────────────────────────────────────

#[test]
fn test_explain_create_resource() {
    let stmt = first_stmt("EXPLAIN CREATE RESOURCE 'postgres' id='db1';");
    match stmt {
        Statement::Explain(inner) => match *inner {
            Statement::CreateResource(s) => {
                assert_eq!(s.resource_type, "postgres");
                assert_eq!(s.params.len(), 1);
                assert_eq!(s.params[0].key, "id");
            }
            _ => panic!("Expected CreateResource inside Explain"),
        },
        _ => panic!("Expected Explain"),
    }
}

#[test]
fn test_explain_select() {
    let stmt = first_stmt("EXPLAIN SELECT * FROM microvms;");
    match stmt {
        Statement::Explain(inner) => match *inner {
            Statement::Select(s) => {
                assert_eq!(s.fields, FieldList::All);
                assert_eq!(s.from, SelectSource::Noun(Noun::Microvms));
            }
            _ => panic!("Expected Select inside Explain"),
        },
        _ => panic!("Expected Explain"),
    }
}

#[test]
fn test_explain_destroy() {
    let stmt = first_stmt("EXPLAIN DESTROY MICROVM 'vm-1';");
    match stmt {
        Statement::Explain(inner) => match *inner {
            Statement::Destroy(s) => {
                assert_eq!(s.target, DestroyTarget::Microvm);
                assert_eq!(s.id, "vm-1");
            }
            _ => panic!("Expected Destroy inside Explain"),
        },
        _ => panic!("Expected Explain"),
    }
}

#[test]
fn test_explain_destroy_resource() {
    let stmt = first_stmt("EXPLAIN DESTROY RESOURCE 'postgres' 'db1';");
    match stmt {
        Statement::Explain(inner) => match *inner {
            Statement::DestroyResource(s) => {
                assert_eq!(s.resource_type, "postgres");
                assert_eq!(s.id, "db1");
            }
            _ => panic!("Expected DestroyResource inside Explain"),
        },
        _ => panic!("Expected Explain"),
    }
}

#[test]
fn round_trip_explain_create_resource() {
    let input = "EXPLAIN CREATE RESOURCE 'postgres' id = 'db1'";
    let stmt = first_stmt(input);
    let displayed = format!("{stmt}");
    assert_eq!(displayed, input);
    // Re-parse the displayed string
    let reparsed = first_stmt(&displayed);
    assert_eq!(reparsed, stmt);
}

#[test]
fn test_explain_case_insensitive() {
    let stmt = first_stmt("explain select * from microvms;");
    assert!(matches!(stmt, Statement::Explain(_)));
}

#[test]
fn test_explain_create_microvm() {
    let stmt = first_stmt(
        "EXPLAIN CREATE MICROVM tenant='acme' vcpus=2 memory_mb=512 image='img-1';",
    );
    match stmt {
        Statement::Explain(inner) => {
            assert!(matches!(*inner, Statement::CreateMicrovm(_)));
        }
        _ => panic!("Expected Explain"),
    }
}

// ── ROLLBACK ──────────────────────────────────────────────────────────

#[test]
fn test_rollback_last() {
    let stmt = first_stmt("ROLLBACK LAST;");
    match stmt {
        Statement::Rollback(s) => {
            assert_eq!(s.target, RollbackTarget::Last);
        }
        _ => panic!("Expected Rollback"),
    }
}

#[test]
fn test_rollback_tag() {
    let stmt = first_stmt("ROLLBACK TO TAG 'pre-migration';");
    match stmt {
        Statement::Rollback(s) => {
            assert_eq!(s.target, RollbackTarget::Tag("pre-migration".into()));
        }
        _ => panic!("Expected Rollback"),
    }
}

#[test]
fn test_rollback_resource() {
    let stmt = first_stmt("ROLLBACK RESOURCE 'postgres' 'db1';");
    match stmt {
        Statement::Rollback(s) => {
            assert_eq!(
                s.target,
                RollbackTarget::Resource {
                    resource_type: "postgres".into(),
                    id: "db1".into(),
                }
            );
        }
        _ => panic!("Expected Rollback"),
    }
}

#[test]
fn test_rollback_case_insensitive() {
    let stmt = first_stmt("rollback last;");
    assert!(matches!(stmt, Statement::Rollback(_)));
}

#[test]
fn test_rollback_roundtrip_last() {
    let input = "ROLLBACK LAST;";
    let program = parse(input);
    let output = format!("{program}");
    assert_eq!(output, "ROLLBACK LAST;");
}

#[test]
fn test_rollback_roundtrip_tag() {
    let input = "ROLLBACK TO TAG 'pre-migration';";
    let program = parse(input);
    let output = format!("{program}");
    assert_eq!(output, "ROLLBACK TO TAG 'pre-migration';");
}

#[test]
fn test_rollback_roundtrip_resource() {
    let input = "ROLLBACK RESOURCE 'postgres' 'db1';";
    let program = parse(input);
    let output = format!("{program}");
    assert_eq!(output, "ROLLBACK RESOURCE 'postgres' 'db1';");
}

// ── Example file parsing ────────────────────────────────────────────

#[test]
fn test_demo_file_parses() {
    let demo = include_str!("../../../examples/demo.kvmql");
    Parser::parse(demo).expect("demo.kvmql should parse");
}

#[test]
fn test_azure_stack_file_parses() {
    let demo = include_str!("../../../examples/azure-stack.kvmql");
    Parser::parse(demo).expect("azure-stack.kvmql should parse");
}

#[test]
fn test_aws_stack_file_parses() {
    let demo = include_str!("../../../examples/aws-stack.kvmql");
    Parser::parse(demo).expect("aws-stack.kvmql should parse");
}

#[test]
fn test_github_project_setup_file_parses() {
    let demo = include_str!("../../../examples/github-project-setup.kvmql");
    Parser::parse(demo).expect("github-project-setup.kvmql should parse");
}

// ── IF NOT EXISTS ──────────────────────────────────────────────────

#[test]
fn test_create_resource_if_not_exists() {
    let stmt = first_stmt(
        "CREATE IF NOT EXISTS RESOURCE 'postgres' id = 'prod-db' version = '16';",
    );
    match stmt {
        Statement::CreateResource(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.resource_type, "postgres");
            assert_eq!(s.params.len(), 2);
        }
        _ => panic!("Expected CreateResource"),
    }
}

#[test]
fn test_create_microvm_if_not_exists() {
    let stmt = first_stmt(
        "CREATE IF NOT EXISTS MICROVM id = 'vm-1' vcpus = 2 memory_mb = 512;",
    );
    match stmt {
        Statement::CreateMicrovm(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.params.len(), 3);
        }
        _ => panic!("Expected CreateMicrovm"),
    }
}

#[test]
fn test_create_volume_if_not_exists() {
    let stmt = first_stmt(
        "CREATE IF NOT EXISTS VOLUME id = 'vol-1' size_gb = 20;",
    );
    match stmt {
        Statement::CreateVolume(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.params.len(), 2);
        }
        _ => panic!("Expected CreateVolume"),
    }
}

#[test]
fn test_add_provider_if_not_exists() {
    let stmt = first_stmt(
        "ADD IF NOT EXISTS PROVIDER id = 'azure-prod' type = 'azure' auth = 'env:AZURE_SUB';",
    );
    match stmt {
        Statement::AddProvider(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.params.len(), 3);
        }
        _ => panic!("Expected AddProvider"),
    }
}

#[test]
fn test_add_cluster_if_not_exists() {
    let stmt = first_stmt(
        "ADD IF NOT EXISTS CLUSTER 'prod' MEMBERS = ['prov-1', 'prov-2'];",
    );
    match stmt {
        Statement::AddCluster(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.name, "prod");
        }
        _ => panic!("Expected AddCluster"),
    }
}

#[test]
fn test_add_principal_if_not_exists() {
    let stmt = first_stmt(
        "ADD IF NOT EXISTS PRINCIPAL id = 'svc-1' type = 'service' auth_ref = 'key-1';",
    );
    match stmt {
        Statement::AddPrincipal(s) => {
            assert!(s.if_not_exists);
            assert_eq!(s.params.len(), 3);
        }
        _ => panic!("Expected AddPrincipal"),
    }
}

#[test]
fn test_if_not_exists_round_trip() {
    let input = "CREATE IF NOT EXISTS RESOURCE 'postgres' id = 'prod-db' version = '16';";
    let stmt = first_stmt(input);
    let displayed = format!("{stmt};");
    assert_eq!(displayed, input);

    // Re-parse the displayed output and check it matches
    let reparsed = first_stmt(&displayed);
    assert_eq!(stmt, reparsed);
}

#[test]
fn test_create_without_if_not_exists() {
    // Regression: ensure existing syntax still works without IF NOT EXISTS
    let stmt = first_stmt("CREATE RESOURCE 'postgres' id = 'prod-db';");
    match stmt {
        Statement::CreateResource(s) => {
            assert!(!s.if_not_exists);
            assert_eq!(s.resource_type, "postgres");
        }
        _ => panic!("Expected CreateResource"),
    }

    let stmt = first_stmt("CREATE MICROVM vcpus = 2;");
    match stmt {
        Statement::CreateMicrovm(s) => {
            assert!(!s.if_not_exists);
        }
        _ => panic!("Expected CreateMicrovm"),
    }

    let stmt = first_stmt("ADD PROVIDER id = 'prov-1' type = 'kvm' auth = 'none';");
    match stmt {
        Statement::AddProvider(s) => {
            assert!(!s.if_not_exists);
        }
        _ => panic!("Expected AddProvider"),
    }
}

#[test]
fn test_select_from_applied_files() {
    let stmt = first_stmt("SELECT * FROM applied_files;");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::AppliedFiles));
            assert_eq!(s.fields, FieldList::All);
        }
        _ => panic!("Expected Select"),
    }
}

// ── Kubernetes live-query nouns ────────────────────────────────────

#[test]
fn select_from_k8s_pods_with_where() {
    let stmt = first_stmt(
        "SELECT * FROM k8s_pods WHERE status = 'CrashLoopBackOff';",
    );
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::K8sPods));
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_from_k8s_deployments() {
    let stmt = first_stmt(
        "SELECT name, replicas, ready_replicas FROM k8s_deployments;",
    );
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::K8sDeployments));
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn select_from_k8s_nodes() {
    let stmt = first_stmt("SELECT name, ready FROM k8s_nodes WHERE ready = false;");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.from, SelectSource::Noun(Noun::K8sNodes));
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected Select"),
    }
}

#[test]
fn round_trip_select_k8s_pods() {
    let input = "SELECT * FROM k8s_pods WHERE namespace = 'orbital-pay';";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

// ── IMPORT RESOURCES tests ───────────────────────────────────────

#[test]
fn import_resources_single_provider() {
    let stmt = first_stmt("IMPORT RESOURCES FROM PROVIDER 'aws-prod';");
    match stmt {
        Statement::ImportResources(s) => {
            assert_eq!(s.source, ImportSource::SingleProvider("aws-prod".into()));
            assert!(s.resource_type_filter.is_none());
        }
        _ => panic!("expected ImportResources"),
    }
}

#[test]
fn import_resources_with_type_filter() {
    let stmt = first_stmt(
        "IMPORT RESOURCES FROM PROVIDER 'aws-prod' WHERE resource_type = 'ec2';",
    );
    match stmt {
        Statement::ImportResources(s) => {
            assert_eq!(s.source, ImportSource::SingleProvider("aws-prod".into()));
            assert_eq!(s.resource_type_filter, Some(vec!["ec2".into()]));
        }
        _ => panic!("expected ImportResources"),
    }
}

#[test]
fn import_resources_with_in_filter() {
    let stmt = first_stmt(
        "IMPORT RESOURCES FROM PROVIDER 'aws-prod' WHERE resource_type IN ('ec2', 'rds_postgres', 'vpc');",
    );
    match stmt {
        Statement::ImportResources(s) => {
            assert_eq!(s.resource_type_filter, Some(vec![
                "ec2".into(),
                "rds_postgres".into(),
                "vpc".into(),
            ]));
        }
        _ => panic!("expected ImportResources"),
    }
}

#[test]
fn import_resources_providers_by_type() {
    let stmt = first_stmt("IMPORT RESOURCES FROM PROVIDERS WHERE type = 'aws';");
    match stmt {
        Statement::ImportResources(s) => {
            assert_eq!(s.source, ImportSource::ProvidersByType("aws".into()));
        }
        _ => panic!("expected ImportResources"),
    }
}

#[test]
fn import_resources_all_providers() {
    let stmt = first_stmt("IMPORT RESOURCES FROM ALL PROVIDERS;");
    match stmt {
        Statement::ImportResources(s) => {
            assert_eq!(s.source, ImportSource::AllProviders);
            assert!(s.resource_type_filter.is_none());
        }
        _ => panic!("expected ImportResources"),
    }
}

#[test]
fn import_resources_case_insensitive() {
    let stmt = first_stmt("import resources from all providers;");
    assert!(matches!(stmt, Statement::ImportResources(_)));
}

#[test]
fn import_resources_round_trip() {
    let input = "IMPORT RESOURCES FROM PROVIDER 'azure' WHERE resource_type IN ('vm', 'postgres');";
    let ast1 = parse(input);
    let text = format!("{}", ast1.statements[0]);
    let ast2 = parse(&format!("{text};"));
    assert_eq!(ast1, ast2);
}

#[test]
fn import_image_still_works() {
    // Verify IMPORT IMAGE didn't break
    let stmt = first_stmt("IMPORT IMAGE source='https://example.com/image.qcow2';");
    assert!(matches!(stmt, Statement::ImportImage(_)));
}
