use std::fmt;

use crate::ast::*;

// ── Program ────────────────────────────────────────────────────────

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, stmt) in self.statements.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{stmt};")?;
        }
        Ok(())
    }
}

// ── Statement ──────────────────────────────────────────────────────

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(s) => write!(f, "{s}"),
            Statement::CreateMicrovm(s) => write!(f, "{s}"),
            Statement::CreateVolume(s) => write!(f, "{s}"),
            Statement::AlterMicrovm(s) => write!(f, "{s}"),
            Statement::AlterVolume(s) => write!(f, "{s}"),
            Statement::Destroy(s) => write!(f, "{s}"),
            Statement::Pause(s) => write!(f, "{s}"),
            Statement::Resume(s) => write!(f, "{s}"),
            Statement::Snapshot(s) => write!(f, "{s}"),
            Statement::Restore(s) => write!(f, "{s}"),
            Statement::Watch(s) => write!(f, "{s}"),
            Statement::Attach(s) => write!(f, "{s}"),
            Statement::Detach(s) => write!(f, "{s}"),
            Statement::Resize(s) => write!(f, "{s}"),
            Statement::ImportImage(s) => write!(f, "{s}"),
            Statement::PublishImage(s) => write!(f, "{s}"),
            Statement::RemoveImage(s) => write!(f, "{s}"),
            Statement::AddProvider(s) => write!(f, "{s}"),
            Statement::RemoveProvider(s) => write!(f, "{s}"),
            Statement::AlterProvider(s) => write!(f, "{s}"),
            Statement::AddCluster(s) => write!(f, "{s}"),
            Statement::AlterCluster(s) => write!(f, "{s}"),
            Statement::RemoveCluster(s) => write!(f, "{s}"),
            Statement::AddPrincipal(s) => write!(f, "{s}"),
            Statement::Grant(s) => write!(f, "{s}"),
            Statement::Revoke(s) => write!(f, "{s}"),
            Statement::Set(s) => write!(f, "{s}"),
            Statement::Show(s) => write!(f, "{s}"),
            Statement::CreateResource(s) => write!(f, "{s}"),
            Statement::AlterResource(s) => write!(f, "{s}"),
            Statement::DestroyResource(s) => write!(f, "{s}"),
            Statement::Backup(s) => write!(f, "{s}"),
            Statement::RestoreResource(s) => write!(f, "{s}"),
            Statement::Scale(s) => write!(f, "{s}"),
            Statement::Upgrade(s) => write!(f, "{s}"),
            Statement::Explain(inner) => write!(f, "EXPLAIN {inner}"),
            Statement::Rollback(s) => write!(f, "{s}"),
            Statement::Assert(s) => write!(f, "{s}"),
        }
    }
}

// ── Fields and Selection ───────────────────────────────────────────

impl fmt::Display for FieldList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldList::All => write!(f, "*"),
            FieldList::Fields(fields) => {
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{field}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::Simple(name) => write!(f, "{name}"),
            Field::Qualified(table, field) => write!(f, "{table}.{field}"),
            Field::FnCall { name, star, args } => {
                write!(f, "{name}(")?;
                if *star {
                    write!(f, "*")?;
                } else {
                    for (i, a) in args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{a}")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl fmt::Display for OrderItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field, self.direction)
    }
}

impl fmt::Display for SortDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "ASC"),
            SortDirection::Desc => write!(f, "DESC"),
        }
    }
}

// ── Nouns ──────────────────────────────────────────────────────────

impl fmt::Display for Noun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Noun::Microvms => write!(f, "microvms"),
            Noun::Volumes => write!(f, "volumes"),
            Noun::Images => write!(f, "images"),
            Noun::Providers => write!(f, "providers"),
            Noun::Clusters => write!(f, "clusters"),
            Noun::Capabilities => write!(f, "capabilities"),
            Noun::Snapshots => write!(f, "snapshots"),
            Noun::Metrics => write!(f, "metrics"),
            Noun::Events => write!(f, "events"),
            Noun::QueryHistory => write!(f, "query_history"),
            Noun::AuditLog => write!(f, "audit_log"),
            Noun::Principals => write!(f, "principals"),
            Noun::Grants => write!(f, "grants"),
            Noun::ClusterMembers => write!(f, "cluster_members"),
            Noun::Resources => write!(f, "resources"),
            Noun::Plans => write!(f, "plans"),
            Noun::AppliedFiles => write!(f, "applied_files"),
            Noun::K8sPods => write!(f, "k8s_pods"),
            Noun::K8sDeployments => write!(f, "k8s_deployments"),
            Noun::K8sServices => write!(f, "k8s_services"),
            Noun::K8sIngresses => write!(f, "k8s_ingresses"),
            Noun::K8sConfigmaps => write!(f, "k8s_configmaps"),
            Noun::K8sSecrets => write!(f, "k8s_secrets"),
            Noun::K8sNamespaces => write!(f, "k8s_namespaces"),
            Noun::K8sNodes => write!(f, "k8s_nodes"),
        }
    }
}

// ── Target Spec ────────────────────────────────────────────────────

impl fmt::Display for TargetSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ON ")?;
        if self.live {
            write!(f, "LIVE ")?;
        }
        write!(f, "{}", self.target)
    }
}

impl fmt::Display for TargetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetKind::Provider(name) => write!(f, "PROVIDER {}", quote_string(name)),
            TargetKind::Cluster(name) => write!(f, "CLUSTER {}", quote_string(name)),
        }
    }
}

// ── Predicates ─────────────────────────────────────────────────────

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::And(left, right) => write!(f, "{left} AND {right}"),
            Predicate::Or(left, right) => write!(f, "{left} OR {right}"),
            Predicate::Not(inner) => write!(f, "NOT {inner}"),
            Predicate::Comparison(cmp) => write!(f, "{cmp}"),
            Predicate::Grouped(inner) => write!(f, "({inner})"),
            Predicate::Exists(select) => write!(f, "EXISTS ({select})"),
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.op {
            ComparisonOp::IsNull | ComparisonOp::IsNotNull => {
                write!(f, "{} {}", self.left, self.op)
            }
            _ => write!(f, "{} {} {}", self.left, self.op, self.right),
        }
    }
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "="),
            ComparisonOp::NotEq => write!(f, "!="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::GtEq => write!(f, ">="),
            ComparisonOp::LtEq => write!(f, "<="),
            ComparisonOp::In => write!(f, "IN"),
            ComparisonOp::NotIn => write!(f, "NOT IN"),
            ComparisonOp::Like => write!(f, "LIKE"),
            ComparisonOp::IsNull => write!(f, "IS NULL"),
            ComparisonOp::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

// ── Expressions ────────────────────────────────────────────────────

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Identifier(name) => write!(f, "{name}"),
            Expr::StringLit(s) => write!(f, "{}", quote_string(s)),
            Expr::Integer(n) => write!(f, "{n}"),
            Expr::Float(v) => write!(f, "{v}"),
            Expr::Boolean(b) => write!(f, "{b}"),
            Expr::Null => write!(f, "NULL"),
            Expr::Duration(d) => write!(f, "{d}"),
            Expr::FunctionCall(fc) => write!(f, "{fc}"),
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            Expr::Grouped(inner) => write!(f, "({inner})"),
            Expr::Variable(s) => write!(f, "@{s}"),
            Expr::Subquery(select) => write!(f, "({select})"),
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Subtract => write!(f, "-"),
            BinaryOp::Concat => write!(f, "||"),
        }
    }
}

impl fmt::Display for DurationValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.magnitude, self.unit)
    }
}

impl fmt::Display for DurationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DurationUnit::Seconds => write!(f, "s"),
            DurationUnit::Minutes => write!(f, "m"),
            DurationUnit::Hours => write!(f, "h"),
            DurationUnit::Days => write!(f, "d"),
        }
    }
}

impl fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

// ── Values ─────────────────────────────────────────────────────────

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", quote_string(s)),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Duration(d) => write!(f, "{d}"),
            Value::Map(entries) => {
                write!(f, "{{")?;
                for (i, entry) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{entry}")?;
                }
                write!(f, "}}")
            }
            Value::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::Null => write!(f, "null"),
            Value::Variable(s) => write!(f, "@{s}"),
        }
    }
}

impl fmt::Display for MapEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.value)
    }
}

impl fmt::Display for Param {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.key, self.value)
    }
}

// ── Statement Display Impls ────────────────────────────────────────

impl fmt::Display for SelectSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectSource::Noun(n) => write!(f, "{n}"),
            SelectSource::Function(fc) => write!(f, "{fc}"),
        }
    }
}

impl fmt::Display for AssertStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ASSERT {}", self.condition)?;
        if let Some(ref msg) = self.message {
            write!(f, ", {}", quote_string(msg))?;
        }
        Ok(())
    }
}

impl fmt::Display for SelectStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT {} FROM {}", self.fields, self.from)?;
        if let Some(ref on) = self.on {
            write!(f, " {on}")?;
        }
        if let Some(ref pred) = self.where_clause {
            write!(f, " WHERE {pred}")?;
        }
        if let Some(ref gb) = self.group_by {
            write!(f, " GROUP BY {gb}")?;
        }
        if let Some(ref ob) = self.order_by {
            write!(f, " ORDER BY ")?;
            for (i, item) in ob.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{item}")?;
            }
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateMicrovmStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "CREATE IF NOT EXISTS MICROVM")?;
        } else {
            write!(f, "CREATE MICROVM")?;
        }
        write_params(f, &self.params)?;
        for vol in &self.volumes {
            write!(f, " VOLUME {vol}")?;
        }
        if let Some(ref on) = self.on {
            write!(f, " {on}")?;
        }
        if let Some(ref policy) = self.placement_policy {
            write!(f, " PLACEMENT POLICY {}", quote_string(policy))?;
        }
        for req in &self.require {
            write!(f, " REQUIRE {req}")?;
        }
        Ok(())
    }
}

impl fmt::Display for VolumeInline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        write_param_list(f, &self.params)?;
        write!(f, ")")
    }
}

impl fmt::Display for RequireClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequireClause::Capability(cap) => write!(f, "CAPABILITY {}", quote_string(cap)),
            RequireClause::Provider(prov) => write!(f, "PROVIDER {}", quote_string(prov)),
            RequireClause::Label { key, value } => {
                write!(f, "LABEL {} = {}", quote_string(key), quote_string(value))
            }
        }
    }
}

impl fmt::Display for CreateVolumeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "CREATE IF NOT EXISTS VOLUME")?;
        } else {
            write!(f, "CREATE VOLUME")?;
        }
        write_params(f, &self.params)?;
        if let Some(ref on) = self.on {
            write!(f, " {on}")?;
        }
        Ok(())
    }
}

impl fmt::Display for AlterMicrovmStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER MICROVM {}", quote_string(&self.id))?;
        write!(f, " SET")?;
        write_params(f, &self.set_items)
    }
}

impl fmt::Display for AlterVolumeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER VOLUME {}", quote_string(&self.id))?;
        write!(f, " SET")?;
        write_params(f, &self.set_items)
    }
}

impl fmt::Display for DestroyStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DESTROY {} {}", self.target, quote_string(&self.id))?;
        if self.force {
            write!(f, " FORCE")?;
        }
        Ok(())
    }
}

impl fmt::Display for DestroyTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DestroyTarget::Microvm => write!(f, "MICROVM"),
            DestroyTarget::Volume => write!(f, "VOLUME"),
        }
    }
}

impl fmt::Display for PauseStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PAUSE MICROVM {}", quote_string(&self.id))
    }
}

impl fmt::Display for ResumeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RESUME MICROVM {}", quote_string(&self.id))
    }
}

impl fmt::Display for SnapshotStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SNAPSHOT MICROVM {} TO {}",
            quote_string(&self.id),
            quote_string(&self.destination),
        )?;
        if let Some(ref tag) = self.tag {
            write!(f, " TAG {}", quote_string(tag))?;
        }
        Ok(())
    }
}

impl fmt::Display for RestoreStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RESTORE MICROVM {} FROM {}",
            quote_string(&self.id),
            quote_string(&self.source),
        )
    }
}

impl fmt::Display for WatchStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WATCH {} FROM {}", self.metrics, self.from)?;
        if let Some(ref pred) = self.where_clause {
            write!(f, " WHERE {pred}")?;
        }
        write!(f, " INTERVAL {}", self.interval)
    }
}

impl fmt::Display for AttachStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ATTACH VOLUME {} TO MICROVM {}",
            quote_string(&self.volume_id),
            quote_string(&self.microvm_id),
        )?;
        if let Some(ref dev) = self.device_name {
            write!(f, " AS {}", quote_string(dev))?;
        }
        Ok(())
    }
}

impl fmt::Display for DetachStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DETACH VOLUME {} FROM MICROVM {}",
            quote_string(&self.volume_id),
            quote_string(&self.microvm_id),
        )
    }
}

impl fmt::Display for ResizeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RESIZE VOLUME {} {} GB",
            quote_string(&self.volume_id),
            self.new_size_gb,
        )
    }
}

impl fmt::Display for ImportImageStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IMPORT IMAGE")?;
        write_params(f, &self.params)
    }
}

impl fmt::Display for PublishImageStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PUBLISH IMAGE {} TO PROVIDER {}",
            quote_string(&self.image_id),
            quote_string(&self.provider),
        )
    }
}

impl fmt::Display for RemoveImageStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REMOVE IMAGE {}", quote_string(&self.image_id))?;
        if self.force {
            write!(f, " FORCE")?;
        }
        Ok(())
    }
}

// ── Resource Management ────────────────────────────────────────────

impl fmt::Display for CreateResourceStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "CREATE IF NOT EXISTS RESOURCE {}", quote_string(&self.resource_type))?;
        } else {
            write!(f, "CREATE RESOURCE {}", quote_string(&self.resource_type))?;
        }
        write_params(f, &self.params)?;
        if let Some(ref on) = self.on {
            write!(f, " {on}")?;
        }
        Ok(())
    }
}

impl fmt::Display for AlterResourceStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ALTER RESOURCE {} {}",
            quote_string(&self.resource_type),
            quote_string(&self.id),
        )?;
        write!(f, " SET")?;
        for (i, item) in self.set_items.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, " {item}")?;
        }
        Ok(())
    }
}

impl fmt::Display for DestroyResourceStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DESTROY RESOURCE {} {}",
            quote_string(&self.resource_type),
            quote_string(&self.id),
        )?;
        if self.force {
            write!(f, " FORCE")?;
        }
        Ok(())
    }
}

// ── Day-2 Operations ──────────────────────────────────────────────

impl fmt::Display for BackupStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BACKUP RESOURCE {} {}", quote_string(&self.resource_type), quote_string(&self.id))?;
        if let Some(ref dest) = self.destination {
            write!(f, " INTO {}", quote_string(dest))?;
        }
        if let Some(ref tag) = self.tag {
            write!(f, " TAG {}", quote_string(tag))?;
        }
        Ok(())
    }
}

impl fmt::Display for RestoreResourceStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RESTORE RESOURCE {} {} FROM {}",
            quote_string(&self.resource_type),
            quote_string(&self.id),
            quote_string(&self.source),
        )
    }
}

impl fmt::Display for ScaleStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SCALE RESOURCE {} {}", quote_string(&self.resource_type), quote_string(&self.id))?;
        write_params(f, &self.params)
    }
}

impl fmt::Display for UpgradeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UPGRADE RESOURCE {} {}", quote_string(&self.resource_type), quote_string(&self.id))?;
        write_params(f, &self.params)
    }
}

// ── Rollback ──────────────────────────────────────────────────────

impl fmt::Display for RollbackStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROLLBACK {}", self.target)
    }
}

impl fmt::Display for RollbackTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RollbackTarget::Last => write!(f, "LAST"),
            RollbackTarget::Tag(tag) => write!(f, "TO TAG {}", quote_string(tag)),
            RollbackTarget::Resource { resource_type, id } => {
                write!(f, "RESOURCE {} {}", quote_string(resource_type), quote_string(id))
            }
        }
    }
}

// ── Provider Management ────────────────────────────────────────────

impl fmt::Display for AddProviderStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "ADD IF NOT EXISTS PROVIDER")?;
        } else {
            write!(f, "ADD PROVIDER")?;
        }
        write_params(f, &self.params)
    }
}

impl fmt::Display for RemoveProviderStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REMOVE PROVIDER {}", quote_string(&self.name))
    }
}

impl fmt::Display for AlterProviderStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER PROVIDER {}", quote_string(&self.name))?;
        write!(f, " SET")?;
        write_params(f, &self.set_items)
    }
}

// ── Cluster Management ─────────────────────────────────────────────

impl fmt::Display for AddClusterStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "ADD IF NOT EXISTS CLUSTER {}", quote_string(&self.name))?;
        } else {
            write!(f, "ADD CLUSTER {}", quote_string(&self.name))?;
        }
        write!(f, " MEMBERS")?;
        for (i, member) in self.members.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, " {}", quote_string(member))?;
        }
        Ok(())
    }
}

impl fmt::Display for AlterClusterStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER CLUSTER {} {}", quote_string(&self.name), self.action)
    }
}

impl fmt::Display for ClusterAlterAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterAlterAction::AddMember(m) => write!(f, "ADD MEMBER {}", quote_string(m)),
            ClusterAlterAction::RemoveMember(m) => write!(f, "REMOVE MEMBER {}", quote_string(m)),
        }
    }
}

impl fmt::Display for RemoveClusterStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REMOVE CLUSTER {}", quote_string(&self.name))
    }
}

// ── Access Control ─────────────────────────────────────────────────

impl fmt::Display for AddPrincipalStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.if_not_exists {
            write!(f, "ADD IF NOT EXISTS PRINCIPAL")?;
        } else {
            write!(f, "ADD PRINCIPAL")?;
        }
        write_params(f, &self.params)
    }
}

impl fmt::Display for GrantStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GRANT ")?;
        write_verb_list(f, &self.verbs)?;
        write!(f, " ON {}", self.scope)?;
        if let Some(ref pred) = self.where_clause {
            write!(f, " WHERE {pred}")?;
        }
        write!(f, " TO {}", quote_string(&self.principal))
    }
}

impl fmt::Display for RevokeStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "REVOKE ")?;
        write_verb_list(f, &self.verbs)?;
        write!(f, " ON {}", self.scope)?;
        write!(f, " FROM {}", quote_string(&self.principal))
    }
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verb::Select => write!(f, "SELECT"),
            Verb::Create => write!(f, "CREATE"),
            Verb::Alter => write!(f, "ALTER"),
            Verb::Destroy => write!(f, "DESTROY"),
            Verb::Pause => write!(f, "PAUSE"),
            Verb::Resume => write!(f, "RESUME"),
            Verb::Snapshot => write!(f, "SNAPSHOT"),
            Verb::Restore => write!(f, "RESTORE"),
            Verb::Attach => write!(f, "ATTACH"),
            Verb::Detach => write!(f, "DETACH"),
            Verb::Resize => write!(f, "RESIZE"),
            Verb::Watch => write!(f, "WATCH"),
            Verb::Import => write!(f, "IMPORT"),
            Verb::Publish => write!(f, "PUBLISH"),
        }
    }
}

impl fmt::Display for GrantScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantScope::Cluster(name) => write!(f, "CLUSTER {}", quote_string(name)),
            GrantScope::Provider(name) => write!(f, "PROVIDER {}", quote_string(name)),
            GrantScope::Microvms => write!(f, "MICROVMS"),
            GrantScope::Volumes => write!(f, "VOLUMES"),
            GrantScope::Images => write!(f, "IMAGES"),
        }
    }
}

// ── Config ─────────────────────────────────────────────────────────

impl fmt::Display for SetStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {} = {}", self.key, self.value)
    }
}

impl fmt::Display for ShowStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW {}", self.target)
    }
}

impl fmt::Display for ShowTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShowTarget::Providers => write!(f, "PROVIDERS"),
            ShowTarget::Clusters => write!(f, "CLUSTERS"),
            ShowTarget::Capabilities { for_provider } => {
                write!(f, "CAPABILITIES")?;
                if let Some(ref provider) = for_provider {
                    write!(f, " FOR PROVIDER {}", quote_string(provider))?;
                }
                Ok(())
            }
            ShowTarget::Grants { for_principal } => {
                write!(f, "GRANTS")?;
                if let Some(ref principal) = for_principal {
                    write!(f, " FOR {}", quote_string(principal))?;
                }
                Ok(())
            }
            ShowTarget::Images => write!(f, "IMAGES"),
            ShowTarget::Version => write!(f, "VERSION"),
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn write_params(f: &mut fmt::Formatter<'_>, params: &[Param]) -> fmt::Result {
    for param in params {
        write!(f, " {param}")?;
    }
    Ok(())
}

fn write_param_list(f: &mut fmt::Formatter<'_>, params: &[Param]) -> fmt::Result {
    for (i, param) in params.iter().enumerate() {
        if i > 0 {
            write!(f, " ")?;
        }
        write!(f, "{param}")?;
    }
    Ok(())
}

fn write_verb_list(f: &mut fmt::Formatter<'_>, verbs: &[Verb]) -> fmt::Result {
    for (i, verb) in verbs.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{verb}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_select_simple() {
        let stmt = SelectStmt {
            fields: FieldList::All,
            from: SelectSource::Noun(Noun::Microvms),
            on: None,
            where_clause: None,
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
        };
        assert_eq!(stmt.to_string(), "SELECT * FROM microvms");
    }

    #[test]
    fn display_select_full() {
        let stmt = SelectStmt {
            fields: FieldList::Fields(vec![
                Field::Simple("id".into()),
                Field::Simple("status".into()),
            ]),
            from: SelectSource::Noun(Noun::Microvms),
            on: Some(TargetSpec {
                target: TargetKind::Provider("aws-east".into()),
                live: true,
            }),
            where_clause: Some(Predicate::Comparison(Comparison {
                left: Expr::Identifier("status".into()),
                op: ComparisonOp::Eq,
                right: Expr::StringLit("running".into()),
            })),
            group_by: None,
            order_by: Some(vec![OrderItem {
                field: "id".into(),
                direction: SortDirection::Desc,
            }]),
            limit: Some(10),
            offset: Some(5),
        };
        assert_eq!(
            stmt.to_string(),
            "SELECT id, status FROM microvms ON LIVE PROVIDER 'aws-east' WHERE status = 'running' ORDER BY id DESC LIMIT 10 OFFSET 5"
        );
    }

    #[test]
    fn display_create_microvm() {
        let stmt = CreateMicrovmStmt {
            if_not_exists: false,
            params: vec![
                Param { key: "vcpus".into(), value: Value::Integer(2) },
                Param { key: "mem_mb".into(), value: Value::Integer(512) },
            ],
            volumes: vec![],
            on: None,
            placement_policy: None,
            require: vec![],
        };
        assert_eq!(stmt.to_string(), "CREATE MICROVM vcpus = 2 mem_mb = 512");
    }

    #[test]
    fn display_destroy_force() {
        let stmt = DestroyStmt {
            target: DestroyTarget::Microvm,
            id: "vm-123".into(),
            force: true,
        };
        assert_eq!(stmt.to_string(), "DESTROY MICROVM 'vm-123' FORCE");
    }

    #[test]
    fn display_predicate_and_or() {
        let pred = Predicate::And(
            Box::new(Predicate::Comparison(Comparison {
                left: Expr::Identifier("a".into()),
                op: ComparisonOp::Eq,
                right: Expr::Integer(1),
            })),
            Box::new(Predicate::Or(
                Box::new(Predicate::Comparison(Comparison {
                    left: Expr::Identifier("b".into()),
                    op: ComparisonOp::Gt,
                    right: Expr::Integer(2),
                })),
                Box::new(Predicate::Comparison(Comparison {
                    left: Expr::Identifier("c".into()),
                    op: ComparisonOp::Lt,
                    right: Expr::Integer(3),
                })),
            )),
        );
        assert_eq!(pred.to_string(), "a = 1 AND b > 2 OR c < 3");
    }

    #[test]
    fn display_value_map() {
        let val = Value::Map(vec![
            MapEntry { key: "a".into(), value: Value::Integer(1) },
            MapEntry { key: "b".into(), value: Value::String("hello".into()) },
        ]);
        assert_eq!(val.to_string(), "{a: 1, b: 'hello'}");
    }

    #[test]
    fn display_value_array() {
        let val = Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        assert_eq!(val.to_string(), "[1, 2, 3]");
    }

    #[test]
    fn display_duration() {
        let d = DurationValue { magnitude: 30, unit: DurationUnit::Seconds };
        assert_eq!(d.to_string(), "30s");
        let d = DurationValue { magnitude: 5, unit: DurationUnit::Minutes };
        assert_eq!(d.to_string(), "5m");
    }

    #[test]
    fn display_grant() {
        let stmt = GrantStmt {
            verbs: vec![Verb::Select, Verb::Create],
            scope: GrantScope::Microvms,
            where_clause: None,
            principal: "admin".into(),
        };
        assert_eq!(stmt.to_string(), "GRANT SELECT, CREATE ON MICROVMS TO 'admin'");
    }

    #[test]
    fn display_show_capabilities() {
        let stmt = ShowStmt {
            target: ShowTarget::Capabilities {
                for_provider: Some("aws".into()),
            },
        };
        assert_eq!(stmt.to_string(), "SHOW CAPABILITIES FOR PROVIDER 'aws'");
    }

    #[test]
    fn display_program() {
        let program = Program {
            statements: vec![
                Statement::Show(ShowStmt { target: ShowTarget::Version }),
                Statement::Show(ShowStmt { target: ShowTarget::Providers }),
            ],
        };
        assert_eq!(program.to_string(), "SHOW VERSION;\nSHOW PROVIDERS;");
    }

    #[test]
    fn quote_string_escapes_quotes() {
        assert_eq!(quote_string("it's"), "'it''s'");
    }

    #[test]
    fn display_attach_with_device() {
        let stmt = AttachStmt {
            volume_id: "vol-1".into(),
            microvm_id: "vm-1".into(),
            device_name: Some("vdb".into()),
        };
        assert_eq!(
            stmt.to_string(),
            "ATTACH VOLUME 'vol-1' TO MICROVM 'vm-1' AS 'vdb'"
        );
    }

    #[test]
    fn display_watch() {
        let stmt = WatchStmt {
            metrics: FieldList::Fields(vec![Field::Simple("cpu".into())]),
            from: Noun::Metrics,
            where_clause: None,
            interval: DurationValue { magnitude: 5, unit: DurationUnit::Seconds },
        };
        assert_eq!(stmt.to_string(), "WATCH cpu FROM metrics INTERVAL 5s");
    }

    #[test]
    fn display_function_call() {
        let expr = Expr::FunctionCall(FunctionCall {
            name: "avg".into(),
            args: vec![Expr::Identifier("cpu_pct".into())],
        });
        assert_eq!(expr.to_string(), "avg(cpu_pct)");
    }

    #[test]
    fn test_select_from_plans() {
        use crate::parser::Parser;
        let program = Parser::parse("SELECT * FROM plans;").unwrap();
        assert_eq!(program.statements.len(), 1);
        if let Statement::Select(ref s) = program.statements[0] {
            assert_eq!(s.from, SelectSource::Noun(Noun::Plans));
            assert_eq!(s.to_string(), "SELECT * FROM plans");
        } else {
            panic!("expected SELECT statement");
        }
    }
}
