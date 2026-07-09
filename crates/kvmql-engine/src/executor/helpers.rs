use std::sync::Arc;
use std::time::Instant;

use kvmql_parser::ast::*;

use crate::errors::{with_remediation, EngineError, ErrorContext};
use crate::response::*;

use super::Executor;

// ---------------------------------------------------------------------------
// Provider resolution helpers (Executor methods)
// ---------------------------------------------------------------------------

impl<'a> Executor<'a> {
    pub(super) fn resolve_provider(&self, on: &Option<TargetSpec>) -> Result<String, EngineError> {
        if let Some(spec) = on {
            match &spec.target {
                TargetKind::Provider(id) => return Ok(id.clone()),
                TargetKind::Cluster(_name) => {
                    return Err("cluster targeting not yet supported".into());
                }
            }
        }
        // Fall back to first registered driver
        let drivers = self.ctx.drivers.read().unwrap();
        drivers.keys().next().cloned().ok_or_else(|| {
            with_remediation(
                "NO_DRIVERS",
                "no drivers registered",
                &ErrorContext::default(),
            )
            .into()
        })
    }

    /// Return any available driver (first in the map).
    pub(super) fn any_driver(
        &self,
    ) -> Result<(String, Arc<dyn kvmql_driver::traits::Driver>), EngineError> {
        let drivers = self.ctx.drivers.read().unwrap();
        drivers
            .iter()
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .ok_or_else(|| {
                with_remediation(
                    "NO_DRIVERS",
                    "no drivers registered",
                    &ErrorContext::default(),
                )
                .into()
            })
    }

    // =======================================================================
    // Variable resolution
    // =======================================================================

    /// Resolve a `Value::Variable` reference to the stored string value.
    pub(super) fn resolve_value(&self, value: &Value) -> Value {
        match value {
            Value::Variable(name) => {
                let vars = self.ctx.variables.read().unwrap();
                if let Some(val) = vars.get(name) {
                    Value::String(val.clone())
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }

    /// Resolve all variable references in a parameter list.
    pub(super) fn resolve_params(&self, params: &[Param]) -> Vec<Param> {
        params
            .iter()
            .map(|p| Param {
                key: p.key.clone(),
                value: self.resolve_value(&p.value),
            })
            .collect()
    }

    /// Convert params to a `serde_json::Value` object.
    pub(super) fn params_to_json(&self, params: &[Param]) -> serde_json::Value {
        let resolved = self.resolve_params(params);
        let mut map = serde_json::Map::new();
        for p in &resolved {
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
    }

    // =======================================================================
    // Provider type detection
    // =======================================================================

    /// Returns true if the resource type belongs to the AWS provider.
    ///
    /// Used by EXPLAIN, dry-run, and ALTER to route to the correct provisioner
    /// without a registry lookup. Must be kept in sync with the AWS driver's
    /// `create()` dispatch.
    pub(super) fn is_aws_resource_type(rtype: &str) -> bool {
        matches!(
            rtype,
            "rds_postgres"
                | "vpc"
                | "aws_subnet"
                | "security_group"
                | "sg_rule"
                | "eks_cluster"
                | "eks_nodegroup"
                | "eks_addon"
                | "s3_bucket"
                | "kms_key"
                | "kms_alias"
                | "elasticache_redis"
                | "elasticache_replication_group"
                | "msk_cluster"
                | "iam_role"
                | "iam_policy"
                | "iam_policy_attachment"
                | "vpc_endpoint"
                | "nat_gateway"
                | "internet_gateway"
                | "route_table"
                | "route_table_association"
                | "route"
                | "db_subnet_group"
                | "cache_subnet_group"
                | "acm_certificate"
                | "cloudwatch_alarm"
                | "cloudwatch_log_group"
                | "ses_domain"
                | "ses_smtp_user"
                | "backup_vault"
                | "backup_plan"
                | "ecs_cluster"
                | "ecs_service"
                | "ecs_task_definition"
                | "ecr_repository"
                | "alb"
                | "alb_target_group"
                | "alb_listener"
                | "cloudfront_distribution"
                | "route53_zone"
                | "route53_record"
                | "secrets_manager_secret"
        )
    }

    // =======================================================================
    // Provisioner getters
    // =======================================================================

    pub(super) fn get_azure_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::azure::resources::AzureResourceProvisioner {
        let (sub, rg) = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            let subscription = if p.provider_type == "azure" {
                kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref).ok()
            } else {
                None
            };
            (subscription, p.region.clone())
        } else {
            (None, None)
        };
        kvmql_driver::azure::resources::AzureResourceProvisioner::new(sub.as_deref(), rg.as_deref())
    }

    pub(super) fn get_aws_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::aws::resources::AwsResourceProvisioner {
        let (region, profile) = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            let prof = resolve_aws_profile(&p.auth_ref);
            (p.region.clone(), prof)
        } else {
            (None, None)
        };
        kvmql_driver::aws::resources::AwsResourceProvisioner::new(
            region.as_deref(),
            profile.as_deref(),
        )
    }

    pub(super) fn get_cloudflare_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::cloudflare::CloudflareResourceProvisioner {
        let token = self
            .resolve_provider_token(provider_id, "cloudflare", "CLOUDFLARE_API_TOKEN");
        kvmql_driver::cloudflare::CloudflareResourceProvisioner::new(token.as_deref())
    }

    /// Resolve an API token for a provider, trying multiple strategies:
    /// 1. Resolve auth_ref from the provider registered with `provider_id`
    /// 2. If auth_ref has no scheme (raw value), use it directly
    /// 3. Search for any provider of `provider_type` and resolve its auth_ref
    /// 4. Fall back to `fallback_env` environment variable
    fn resolve_provider_token(
        &self,
        provider_id: &str,
        provider_type: &str,
        fallback_env: &str,
    ) -> Option<String> {
        // Strategy 1: lookup by exact provider ID
        if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            if let Ok(val) = kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref) {
                return Some(val);
            }
            // If auth_ref has no scheme (no ':'), treat it as a raw token value
            if !p.auth_ref.is_empty() && !p.auth_ref.contains(':') {
                return Some(p.auth_ref.clone());
            }
        }

        // Strategy 2: find any provider of the matching type
        if let Ok(providers) = self.ctx.registry.list_providers() {
            for p in &providers {
                if p.provider_type == provider_type && p.id != provider_id {
                    if let Ok(val) =
                        kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                    {
                        return Some(val);
                    }
                    if !p.auth_ref.is_empty() && !p.auth_ref.contains(':') {
                        return Some(p.auth_ref.clone());
                    }
                }
            }
        }

        // Strategy 3: environment variable fallback
        std::env::var(fallback_env).ok()
    }

    pub(super) fn get_github_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::github::GithubResourceProvisioner {
        let token = self
            .resolve_provider_token(provider_id, "github", "GITHUB_TOKEN")
            .or_else(|| std::env::var("GH_TOKEN").ok());
        kvmql_driver::github::GithubResourceProvisioner::new(token.as_deref())
    }

    pub(super) fn get_ssh_provisioner(
        &self,
        provider_id: &str,
    ) -> Result<kvmql_driver::ssh::SshResourceProvisioner, EngineError> {
        let p = self
            .ctx
            .registry
            .get_provider(provider_id)
            .map_err(|e| format!("ssh provider '{provider_id}' not found: {e}"))?;
        let host = p
            .host
            .clone()
            .ok_or_else(|| format!("ssh provider '{provider_id}' is missing host="))?;
        let (user, port) = parse_ssh_connection_hints(p.labels.as_deref());

        let key_path = if p.auth_ref.is_empty() || p.auth_ref == "none" {
            None
        } else {
            match kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref) {
                Ok(key_material) => Some(write_ephemeral_key(&key_material)?),
                Err(e) => {
                    return Err(format!(
                        "failed to resolve ssh key '{}' for provider '{}': {}",
                        p.auth_ref, provider_id, e
                    )
                    .into());
                }
            }
        };

        let client =
            kvmql_driver::ssh::SshClient::from_openssh(&host, user.as_deref(), port, key_path);
        Ok(kvmql_driver::ssh::SshResourceProvisioner::new(client))
    }

    pub(super) fn get_k8s_provisioner(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::k8s::KubernetesResourceProvisioner {
        let context = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                .ok()
                .or_else(|| p.region.clone())
        } else {
            std::env::var("KUBECONTEXT").ok()
        };
        kvmql_driver::k8s::KubernetesResourceProvisioner::new(context.as_deref())
    }

    pub(super) fn get_k8s_query_engine(
        &self,
        provider_id: &str,
    ) -> kvmql_driver::k8s::KubernetesQueryEngine {
        let context = if let Ok(p) = self.ctx.registry.get_provider(provider_id) {
            kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                .ok()
                .or_else(|| p.region.clone())
        } else {
            std::env::var("KUBECONTEXT").ok()
        };
        kvmql_driver::k8s::KubernetesQueryEngine::new(context.as_deref())
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Extract an AWS profile name from a provider's `auth_ref`.
pub(super) fn resolve_aws_profile(auth_ref: &str) -> Option<String> {
    if auth_ref.is_empty() || auth_ref == "none" {
        return None;
    }

    if let Some(rest) = auth_ref.strip_prefix("env:") {
        if let Some((_var, value)) = rest.split_once('=') {
            return Some(value.to_string());
        }
        return std::env::var(rest).ok();
    }

    if auth_ref.contains(':') {
        return kvmql_auth::resolver::CredentialResolver::resolve(auth_ref).ok();
    }

    Some(auth_ref.to_string())
}

/// Resolve a `file` resource `content` parameter into raw bytes.
pub(super) fn resolve_content_reference(raw: &str) -> Result<String, String> {
    if let Some(rest) = raw.strip_prefix("file:") {
        return std::fs::read_to_string(rest).map_err(|e| format!("failed to read {rest}: {e}"));
    }
    let is_credential_scheme = [
        "env:",
        "op:",
        "vault:",
        "aws-sm:",
        "gcp-sm:",
        "azure-kv:",
        "sops:",
        "k8s:",
    ]
    .iter()
    .any(|p| raw.starts_with(p));
    if is_credential_scheme {
        return kvmql_auth::resolver::CredentialResolver::resolve(raw)
            .map_err(|e| format!("credential resolve failed: {e}"));
    }
    Ok(raw.to_string())
}

/// Parse optional SSH connection hints out of the provider's `labels` JSON.
pub(super) fn parse_ssh_connection_hints(labels: Option<&str>) -> (Option<String>, Option<u16>) {
    let Some(raw) = labels else {
        return (None, None);
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return (None, None);
    };
    let user = v.get("ssh_user").and_then(|x| x.as_str()).map(String::from);
    let port = v.get("ssh_port").and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_str().and_then(|s| s.parse::<u64>().ok()))
    }).and_then(|p| u16::try_from(p).ok());
    (user, port)
}

/// Write resolved private-key material to a mode-0600 tempfile and return
/// its path.
///
/// Handles both OpenSSH format (`BEGIN OPENSSH PRIVATE KEY`) and PEM format
/// (`BEGIN RSA PRIVATE KEY` / `BEGIN EC PRIVATE KEY`).  Credential resolvers
/// sometimes return keys with mangled whitespace (e.g. literal `\n` instead
/// of real newlines, or a single long line from env vars / secrets managers).
/// We normalise the content before writing.
pub(super) fn write_ephemeral_key(key_material: &str) -> Result<std::path::PathBuf, String> {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;

    // Normalise key material: replace literal "\n" sequences with real
    // newlines (common when keys come from env vars or JSON secrets).
    let normalised = if key_material.contains("\\n") && !key_material.contains('\n') {
        key_material.replace("\\n", "\n")
    } else if key_material.contains("\\n") {
        // Mixed — some real newlines and some escaped.  Replace escaped ones.
        key_material.replace("\\n", "\n")
    } else {
        key_material.to_string()
    };

    // Validate that the key looks like a private key
    let trimmed = normalised.trim();
    if !trimmed.starts_with("-----BEGIN ") {
        return Err(format!(
            "SSH key does not look like a PEM or OpenSSH private key (starts with {:?}…). \
             Supported formats: OpenSSH (BEGIN OPENSSH PRIVATE KEY) and PEM (BEGIN RSA/EC/DSA PRIVATE KEY).",
            &trimmed[..trimmed.len().min(30)]
        ));
    }

    let dir = std::env::temp_dir();
    let filename = format!("orbi-ssh-{}.pem", uuid::Uuid::new_v4());
    let path = dir.join(filename);

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(&path)
        .map_err(|e| format!("failed to create ssh key tempfile: {e}"))?;
    f.write_all(trimmed.as_bytes())
        .map_err(|e| format!("failed to write ssh key tempfile: {e}"))?;
    if !trimmed.ends_with('\n') {
        f.write_all(b"\n")
            .map_err(|e| format!("failed to write ssh key tempfile newline: {e}"))?;
    }
    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    Ok(path)
}

// ---------------------------------------------------------------------------
// Predicate evaluation (simple in-memory WHERE filter)
// ---------------------------------------------------------------------------

pub(super) fn eval_predicate(pred: &Predicate, row: &serde_json::Value) -> bool {
    match pred {
        Predicate::And(a, b) => eval_predicate(a, row) && eval_predicate(b, row),
        Predicate::Or(a, b) => eval_predicate(a, row) || eval_predicate(b, row),
        Predicate::Not(a) => !eval_predicate(a, row),
        Predicate::Grouped(inner) => eval_predicate(inner, row),
        Predicate::Comparison(cmp) => eval_comparison(cmp, row),
        Predicate::Exists(_) => true,
    }
}

fn eval_comparison(cmp: &Comparison, row: &serde_json::Value) -> bool {
    let field_name = match &cmp.left {
        Expr::Identifier(name) => name.as_str(),
        _ => return false,
    };

    let row_val = match row.get(field_name) {
        Some(v) => v,
        None => return cmp.op == ComparisonOp::IsNull,
    };

    match &cmp.op {
        ComparisonOp::IsNull => row_val.is_null(),
        ComparisonOp::IsNotNull => !row_val.is_null(),
        ComparisonOp::Eq => expr_matches_json(&cmp.right, row_val),
        ComparisonOp::NotEq => !expr_matches_json(&cmp.right, row_val),
        ComparisonOp::Gt => compare_numeric(row_val, &cmp.right, |a, b| a > b),
        ComparisonOp::Lt => compare_numeric(row_val, &cmp.right, |a, b| a < b),
        ComparisonOp::GtEq => compare_numeric(row_val, &cmp.right, |a, b| a >= b),
        ComparisonOp::LtEq => compare_numeric(row_val, &cmp.right, |a, b| a <= b),
        ComparisonOp::Like => {
            if let (Some(hay), Expr::StringLit(pat)) = (row_val.as_str(), &cmp.right) {
                simple_like(hay, pat)
            } else {
                false
            }
        }
        ComparisonOp::In | ComparisonOp::NotIn => cmp.op == ComparisonOp::NotIn,
    }
}

fn expr_matches_json(expr: &Expr, val: &serde_json::Value) -> bool {
    match expr {
        Expr::StringLit(s) => val.as_str() == Some(s.as_str()),
        Expr::Integer(n) => val.as_i64() == Some(*n),
        Expr::Float(f) => val.as_f64() == Some(*f),
        Expr::Boolean(b) => val.as_bool() == Some(*b),
        Expr::Null => val.is_null(),
        _ => false,
    }
}

fn compare_numeric(val: &serde_json::Value, expr: &Expr, f: fn(f64, f64) -> bool) -> bool {
    let lhs = val.as_f64().or_else(|| val.as_i64().map(|n| n as f64));
    let rhs = match expr {
        Expr::Integer(n) => Some(*n as f64),
        Expr::Float(f) => Some(*f),
        _ => None,
    };
    match (lhs, rhs) {
        (Some(a), Some(b)) => f(a, b),
        _ => false,
    }
}

/// Very simple LIKE matching supporting `%` as wildcard.
fn simple_like(hay: &str, pattern: &str) -> bool {
    if pattern == "%" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix('%') {
        if let Some(prefix) = suffix.strip_suffix('%') {
            return hay.contains(prefix);
        }
        return hay.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('%') {
        return hay.starts_with(prefix);
    }
    hay == pattern
}

// ---------------------------------------------------------------------------
// Table-valued functions (network verification)
// ---------------------------------------------------------------------------

pub(super) async fn run_table_function(
    fc: &kvmql_parser::ast::FunctionCall,
) -> Result<Vec<serde_json::Value>, String> {
    use crate::network::NetworkFunctions;

    fn arg_str(args: &[Expr], idx: usize) -> Result<String, String> {
        match args.get(idx) {
            Some(Expr::StringLit(s)) => Ok(s.clone()),
            Some(other) => Err(format!(
                "expected string literal at arg {}, got {:?}",
                idx, other
            )),
            None => Err(format!("missing required arg {}", idx)),
        }
    }

    fn arg_int(args: &[Expr], idx: usize) -> Result<i64, String> {
        match args.get(idx) {
            Some(Expr::Integer(n)) => Ok(*n),
            Some(other) => Err(format!("expected integer at arg {}, got {:?}", idx, other)),
            None => Err(format!("missing required arg {}", idx)),
        }
    }

    fn arg_str_opt(args: &[Expr], idx: usize) -> Option<String> {
        match args.get(idx) {
            Some(Expr::StringLit(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn arg_int_opt(args: &[Expr], idx: usize) -> Option<i64> {
        match args.get(idx) {
            Some(Expr::Integer(n)) => Some(*n),
            _ => None,
        }
    }

    match fc.name.as_str() {
        "dns_lookup" => {
            let name = arg_str(&fc.args, 0)?;
            let record_type = arg_str_opt(&fc.args, 1);
            NetworkFunctions::dns_lookup(&name, record_type.as_deref()).await
        }
        "reverse_dns" => {
            let ip = arg_str(&fc.args, 0)?;
            NetworkFunctions::reverse_dns(&ip).await
        }
        "tcp_probe" => {
            let host = arg_str(&fc.args, 0)?;
            let port = arg_int(&fc.args, 1)? as u16;
            let timeout_ms = arg_int_opt(&fc.args, 2).map(|n| n as u64);
            NetworkFunctions::tcp_probe(&host, port, timeout_ms).await
        }
        "http_probe" => {
            let url = arg_str(&fc.args, 0)?;
            NetworkFunctions::http_probe(&url).await
        }
        "tls_cert" => {
            let host = arg_str(&fc.args, 0)?;
            let port = arg_int(&fc.args, 1)? as u16;
            NetworkFunctions::tls_cert(&host, port).await
        }
        other => Err(format!("unknown table function: {}", other)),
    }
}

/// Compare two JSON scalar values using a `ComparisonOp`. Used by ASSERT.
pub(super) fn compare_json(
    lhs: &serde_json::Value,
    op: &ComparisonOp,
    rhs: &serde_json::Value,
) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::NotEq => lhs != rhs,
        ComparisonOp::IsNull => lhs.is_null(),
        ComparisonOp::IsNotNull => !lhs.is_null(),
        ComparisonOp::Gt | ComparisonOp::Lt | ComparisonOp::GtEq | ComparisonOp::LtEq => {
            let l = lhs.as_f64();
            let r = rhs.as_f64();
            if let (Some(a), Some(b)) = (l, r) {
                match op {
                    ComparisonOp::Gt => a > b,
                    ComparisonOp::Lt => a < b,
                    ComparisonOp::GtEq => a >= b,
                    ComparisonOp::LtEq => a <= b,
                    _ => unreachable!(),
                }
            } else if let (Some(a), Some(b)) = (lhs.as_str(), rhs.as_str()) {
                match op {
                    ComparisonOp::Gt => a > b,
                    ComparisonOp::Lt => a < b,
                    ComparisonOp::GtEq => a >= b,
                    ComparisonOp::LtEq => a <= b,
                    _ => unreachable!(),
                }
            } else {
                false
            }
        }
        ComparisonOp::Like => {
            if let (Some(h), Some(p)) = (lhs.as_str(), rhs.as_str()) {
                simple_like(h, p)
            } else {
                false
            }
        }
        ComparisonOp::In | ComparisonOp::NotIn => {
            let eq = lhs == rhs;
            if matches!(op, ComparisonOp::In) {
                eq
            } else {
                !eq
            }
        }
    }
}

/// Apply a binary operator (`+`, `-`, `||`) to two resolved JSON values.
pub(super) fn eval_binary_op(
    lhs: &serde_json::Value,
    op: &kvmql_parser::ast::BinaryOp,
    rhs: &serde_json::Value,
) -> serde_json::Value {
    use kvmql_parser::ast::BinaryOp;
    match op {
        BinaryOp::Concat => {
            let ls = json_to_concat_string(lhs);
            let rs = json_to_concat_string(rhs);
            serde_json::Value::String(format!("{}{}", ls, rs))
        }
        BinaryOp::Add => {
            if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_f64()) {
                serde_json::json!(a + b)
            } else {
                serde_json::Value::Null
            }
        }
        BinaryOp::Subtract => {
            if let (Some(a), Some(b)) = (lhs.as_f64(), rhs.as_f64()) {
                serde_json::json!(a - b)
            } else {
                serde_json::Value::Null
            }
        }
    }
}

fn json_to_concat_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Projection helpers for SELECT field lists
// ---------------------------------------------------------------------------

pub(super) fn project_rows(
    fields: &kvmql_parser::ast::FieldList,
    rows: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    use kvmql_parser::ast::{Field, FieldList};

    match fields {
        FieldList::All => Ok(rows),
        FieldList::Fields(fs) => {
            let has_aggregate = fs.iter().any(field_is_aggregate);

            if has_aggregate {
                let mut row = serde_json::Map::new();
                for f in fs {
                    match f {
                        Field::FnCall { name, star, args } => {
                            let val = eval_aggregate(name, *star, args, &rows)?;
                            row.insert(name.clone(), val);
                        }
                        Field::Aliased { field, alias } => match field.as_ref() {
                            Field::FnCall { name, star, args } => {
                                let val = eval_aggregate(name, *star, args, &rows)?;
                                row.insert(alias.clone(), val);
                            }
                            other => {
                                return Err(format!(
                                    "mixing aggregate and non-aggregate projections is not supported (got {other:?})"
                                ));
                            }
                        },
                        other => {
                            return Err(format!(
                                "mixing aggregate and non-aggregate projections is not supported (got {other:?})"
                            ));
                        }
                    }
                }
                return Ok(vec![serde_json::Value::Object(row)]);
            }

            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let obj = row.as_object().cloned().unwrap_or_default();
                let mut new_row = serde_json::Map::new();
                for f in fs {
                    project_field(f, &obj, &mut new_row)?;
                }
                out.push(serde_json::Value::Object(new_row));
            }
            Ok(out)
        }
    }
}

fn field_is_aggregate(f: &kvmql_parser::ast::Field) -> bool {
    use kvmql_parser::ast::Field;
    match f {
        Field::FnCall { name, .. } => is_aggregate(name),
        Field::Aliased { field, .. } => field_is_aggregate(field),
        _ => false,
    }
}

fn project_field(
    f: &kvmql_parser::ast::Field,
    obj: &serde_json::Map<String, serde_json::Value>,
    out: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    use kvmql_parser::ast::Field;
    match f {
        Field::Simple(name) => {
            let v = obj.get(name).cloned().unwrap_or(serde_json::Value::Null);
            out.insert(name.clone(), v);
        }
        Field::Qualified(_, name) => {
            let v = obj.get(name).cloned().unwrap_or(serde_json::Value::Null);
            out.insert(name.clone(), v);
        }
        Field::FnCall { name, .. } => {
            return Err(format!(
                "function '{name}' is not supported in non-aggregate projection"
            ));
        }
        Field::Aliased { field, alias } => {
            let mut tmp = serde_json::Map::new();
            project_field(field, obj, &mut tmp)?;
            if let Some((_, v)) = tmp.into_iter().next() {
                out.insert(alias.clone(), v);
            }
        }
    }
    Ok(())
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max"
    )
}

fn eval_aggregate(
    name: &str,
    star: bool,
    args: &[kvmql_parser::ast::Expr],
    rows: &[serde_json::Value],
) -> Result<serde_json::Value, String> {
    use kvmql_parser::ast::Expr;

    let lname = name.to_ascii_lowercase();
    if lname == "count" {
        if star || args.is_empty() {
            return Ok(serde_json::json!(rows.len() as i64));
        }
        if let Expr::Identifier(col) = &args[0] {
            let n = rows
                .iter()
                .filter_map(|r| r.get(col))
                .filter(|v| !v.is_null())
                .count();
            return Ok(serde_json::json!(n as i64));
        }
        return Err("count(expr) only supports count(*) and count(column)".into());
    }

    let col = match args.first() {
        Some(Expr::Identifier(name)) => name.clone(),
        Some(other) => {
            return Err(format!(
                "aggregate {lname}() requires a column name, got {other:?}"
            ))
        }
        None => return Err(format!("aggregate {lname}() requires one argument")),
    };

    let values: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.get(&col))
        .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))
        .collect();

    if values.is_empty() {
        return Ok(serde_json::Value::Null);
    }

    let result = match lname.as_str() {
        "sum" => values.iter().sum::<f64>(),
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
        "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        _ => unreachable!(),
    };
    Ok(serde_json::json!(result))
}

/// Run `stat` + `sha256sum` on a remote path via the given provisioner.
pub(super) fn build_file_stat_row(
    provisioner: &kvmql_driver::ssh::SshResourceProvisioner,
    provider_id: &str,
    host: Option<&str>,
    path: &str,
) -> serde_json::Value {
    let stat_res = provisioner.client.stat(path);
    match stat_res {
        Err(e) => serde_json::json!({
            "provider_id": provider_id,
            "host": host,
            "path": path,
            "present": false,
            "error": format!("stat failed: {e}"),
        }),
        Ok(None) => serde_json::json!({
            "provider_id": provider_id,
            "host": host,
            "path": path,
            "present": false,
            "size": null,
            "owner": null,
            "group": null,
            "mode": null,
            "sha256": null,
            "modified_at": null,
        }),
        Ok(Some(stat)) => {
            let sha = provisioner.client.sha256(path).ok().flatten();
            serde_json::json!({
                "provider_id": provider_id,
                "host": host,
                "path": path,
                "present": true,
                "size": stat.size,
                "owner": stat.owner,
                "group": stat.group,
                "mode": stat.mode,
                "sha256": sha,
                "modified_at": stat.modified_at,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Simulation: realistic fake outputs per resource type
// ---------------------------------------------------------------------------

pub(super) fn simulate_outputs(
    resource_type: &str,
    id: &str,
    config: &serde_json::Value,
) -> Option<serde_json::Value> {
    match resource_type {
        "postgres" => Some(serde_json::json!({
            "fqdn": format!("{id}.postgres.database.azure.com"),
            "host": format!("{id}.postgres.database.azure.com"),
            "port": 5432,
            "state": "Ready",
            "version": config.get("version").and_then(|v| v.as_str()).unwrap_or("16"),
            "connection_string": format!("postgresql://admin@{id}.postgres.database.azure.com:5432/postgres?sslmode=require"),
        })),
        "redis" => Some(serde_json::json!({
            "host": format!("{id}.redis.cache.windows.net"),
            "port": 6380,
            "ssl_port": 6380,
            "connection_string": format!("{id}.redis.cache.windows.net:6380,ssl=True,abortConnect=False"),
        })),
        "aks" => Some(serde_json::json!({
            "fqdn": format!("{id}-dns-abc123.hcp.eastus.azmk8s.io"),
            "kubernetes_version": config.get("kubernetes_version").and_then(|v| v.as_str()).unwrap_or("1.28"),
            "node_count": config.get("node_count").and_then(|v| v.as_str()).unwrap_or("3"),
            "kubeconfig_command": format!("az aks get-credentials --name {id} --resource-group <rg>"),
        })),
        "storage_account" => Some(serde_json::json!({
            "primary_blob_endpoint": format!("https://{id}.blob.core.windows.net/"),
            "primary_file_endpoint": format!("https://{id}.file.core.windows.net/"),
            "primary_table_endpoint": format!("https://{id}.table.core.windows.net/"),
            "primary_queue_endpoint": format!("https://{id}.queue.core.windows.net/"),
        })),
        "vnet" => Some(serde_json::json!({
            "address_space": config.get("address_space").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/16"),
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/virtualNetworks/{id}"),
        })),
        "subnet" => Some(serde_json::json!({
            "address_prefix": config.get("address_prefix").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/24"),
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/virtualNetworks/{}/subnets/{id}",
                config.get("vnet").and_then(|v| v.as_str()).unwrap_or("sim-vnet")),
        })),
        "nsg" => Some(serde_json::json!({
            "resource_id": format!("/subscriptions/sim-sub/resourceGroups/sim-rg/providers/Microsoft.Network/networkSecurityGroups/{id}"),
        })),
        "container_registry" => Some(serde_json::json!({
            "login_server": format!("{id}.azurecr.io"),
            "admin_username": id,
        })),
        "dns_zone" => Some(serde_json::json!({
            "name_servers": ["ns1-sim.azure-dns.com", "ns2-sim.azure-dns.net"],
        })),
        "container_app" => Some(serde_json::json!({
            "fqdn": format!("{id}.happyforest-abc123.eastus.azurecontainerapps.io"),
            "latest_revision": format!("{id}--initial"),
        })),
        "rds_postgres" => Some(serde_json::json!({
            "endpoint": format!("{id}.abc123.us-east-1.rds.amazonaws.com"),
            "port": 5432,
            "engine": "postgres",
            "status": "available",
            "connection_string": format!("postgresql://admin@{id}.abc123.us-east-1.rds.amazonaws.com:5432/postgres"),
        })),
        "vpc" => {
            let id_trunc = &id[..id.len().min(8)];
            Some(serde_json::json!({
                "vpc_id": format!("vpc-sim-{id_trunc}"),
                "cidr_block": config.get("cidr_block").and_then(|v| v.as_str()).unwrap_or("10.0.0.0/16"),
                "state": "available",
            }))
        }
        "security_group" => {
            let id_trunc = &id[..id.len().min(8)];
            Some(serde_json::json!({
                "group_id": format!("sg-sim-{id_trunc}"),
                "vpc_id": config.get("vpc_id").and_then(|v| v.as_str()),
            }))
        }
        _ => Some(serde_json::json!({
            "id": id,
            "status": "simulated",
        })),
    }
}

// ---------------------------------------------------------------------------
// Envelope constructors
// ---------------------------------------------------------------------------

pub(super) fn error_envelope(request_id: String, message: &str, start: Instant) -> ResultEnvelope {
    ResultEnvelope {
        request_id,
        status: ResultStatus::Error,
        notifications: vec![Notification {
            level: "ERROR".into(),
            code: "PARSE_001".into(),
            provider_id: None,
            message: message.to_string(),
        }],
        result: None,
        rows_affected: None,
        duration_ms: start.elapsed().as_millis() as u64,
    }
}

pub(super) fn error_envelope_with(
    request_id: String,
    message: &str,
    mut notifications: Vec<Notification>,
    result: Option<serde_json::Value>,
    start: Instant,
) -> ResultEnvelope {
    notifications.push(Notification {
        level: "ERROR".into(),
        code: "RTE_001".into(),
        provider_id: None,
        message: message.to_string(),
    });
    ResultEnvelope {
        request_id,
        status: ResultStatus::Error,
        notifications,
        result,
        rows_affected: None,
        duration_ms: start.elapsed().as_millis() as u64,
    }
}
