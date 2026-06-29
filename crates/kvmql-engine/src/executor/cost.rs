use kvmql_parser::ast::*;

use crate::errors::EngineError;

use super::{Executor, StmtOutcome};

// ---------------------------------------------------------------------------
// Local helper — not a registry row
// ---------------------------------------------------------------------------

struct CostEstimate {
    resource_id: String,
    resource_type: String,
    provider: String,
    description: Option<String>,
    quantity: i64,
    hourly: f64,
    monthly: f64,
}

impl<'a> Executor<'a> {
    // =======================================================================
    // EXPLAIN COST
    // =======================================================================

    pub(super) async fn exec_explain_cost(
        &self,
        stmt: &Statement,
    ) -> Result<StmtOutcome, EngineError> {
        // 1. Clear previous cost estimates
        self.ctx
            .registry
            .clear_cost_estimates()
            .map_err(|e| -> EngineError {
                format!("failed to clear cost estimates: {e}").into()
            })?;

        // 2. Collect cost rows from the statement
        let mut cost_rows = Vec::new();

        match stmt {
            Statement::CreateResource(s) => {
                let params = self.params_to_json(&s.params);
                if let Some(c) = self.estimate_resource_cost(&s.resource_type, &params)? {
                    cost_rows.push(c);
                }
            }
            _ => {
                // For non-CREATE statements, fall through to EXPLAIN
                return self.exec_explain(stmt).await;
            }
        }

        // 3. Insert cost estimates into the registry
        for row in &cost_rows {
            let _ = self.ctx.registry.insert_cost_estimate(
                &uuid::Uuid::new_v4().to_string(),
                &row.resource_id,
                &row.resource_type,
                &row.provider,
                row.description.as_deref(),
                row.quantity,
                row.hourly,
                row.monthly,
            );
        }

        // 4. Build result table
        let mut result: Vec<serde_json::Value> = cost_rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "resource": r.resource_id,
                    "type": r.resource_type,
                    "description": r.description,
                    "quantity": r.quantity,
                    "hourly": format!("{:.3}", r.hourly),
                    "monthly": format!("{:.2}", r.monthly),
                })
            })
            .collect();

        // Add total row
        let total_hourly: f64 = cost_rows.iter().map(|r| r.hourly).sum();
        let total_monthly: f64 = cost_rows.iter().map(|r| r.monthly).sum();
        result.push(serde_json::json!({
            "resource": "TOTAL",
            "type": "",
            "description": "",
            "quantity": "",
            "hourly": format!("{:.3}", total_hourly),
            "monthly": format!("{:.2}", total_monthly),
        }));

        let n = result.len() as i64;
        Ok(StmtOutcome::ok_rows(serde_json::Value::Array(result), n))
    }

    // =======================================================================
    // Cost estimation helper
    // =======================================================================

    fn estimate_resource_cost(
        &self,
        resource_type: &str,
        params: &serde_json::Value,
    ) -> Result<Option<CostEstimate>, EngineError> {
        let region = self
            .ctx
            .registry
            .list_providers()
            .ok()
            .and_then(|ps| ps.first().and_then(|p| p.region.clone()))
            .unwrap_or_else(|| "us-east-1".to_string());

        let (lookup_type, param_key, quantity) = match resource_type {
            "eks_cluster" => ("eks_cluster", String::new(), 1_i64),
            "eks_nodegroup" => {
                let instance_type = params
                    .get("instance_types")
                    .and_then(|v| v.as_str())
                    .unwrap_or("t3.medium");
                let desired: i64 = params
                    .get("desired")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(2);
                ("eks_nodegroup", instance_type.to_string(), desired)
            }
            "rds_postgres" => {
                let instance_class = params
                    .get("instance_class")
                    .and_then(|v| v.as_str())
                    .unwrap_or("db.t3.medium");
                ("rds_postgres", instance_class.to_string(), 1)
            }
            "elasticache_redis" => {
                let node_type = params
                    .get("node_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("cache.t3.micro");
                let num_nodes: i64 = params
                    .get("num_nodes")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(1);
                ("elasticache_redis", node_type.to_string(), num_nodes)
            }
            "elasticache_replication_group" => {
                let node_type = params
                    .get("node_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("cache.t3.micro");
                let shards: i64 = params
                    .get("num_shards")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(1);
                let replicas: i64 = params
                    .get("replicas")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(1);
                let total_nodes = shards * (1 + replicas);
                (
                    "elasticache_replication_group",
                    node_type.to_string(),
                    total_nodes,
                )
            }
            "msk_cluster" => {
                let instance_type = params
                    .get("instance_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("kafka.m5.large");
                let broker_count: i64 = params
                    .get("broker_count")
                    .and_then(|v| {
                        v.as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| v.as_i64())
                    })
                    .unwrap_or(3);
                ("msk_cluster", instance_type.to_string(), broker_count)
            }
            "nat_gateway" => ("nat_gateway", String::new(), 1),
            "vpc_endpoint" => {
                let ep_type = params
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Gateway");
                ("vpc_endpoint", ep_type.to_string(), 1)
            }
            "kms_key" => ("kms_key", String::new(), 1),
            "s3_bucket" => ("s3_bucket", String::new(), 1),
            // Free resources
            "vpc" | "aws_subnet" | "security_group" | "sg_rule" | "iam_role" | "iam_policy"
            | "ses_domain" | "ses_smtp_user" | "acm_certificate" | "eks_addon" | "backup_vault"
            | "backup_plan" | "cloudwatch_alarm" => {
                return Ok(Some(CostEstimate {
                    resource_id: params
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?")
                        .to_string(),
                    resource_type: resource_type.to_string(),
                    provider: "aws".to_string(),
                    description: Some(format!("{resource_type} (no hourly cost)")),
                    quantity: 1,
                    hourly: 0.0,
                    monthly: 0.0,
                }));
            }
            _ => return Ok(None), // Unknown type, skip
        };

        // Look up pricing
        let pricing = self
            .ctx
            .registry
            .get_pricing("aws", &region, lookup_type, &param_key)
            .map_err(|e| -> EngineError { format!("pricing lookup failed: {e}").into() })?;

        let (base_hourly, base_monthly) = pricing.unwrap_or((0.0, 0.0));
        let resource_id = params
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();

        let description = if param_key.is_empty() {
            resource_type.to_string()
        } else if quantity > 1 {
            format!("{quantity}x {param_key}")
        } else {
            param_key.clone()
        };

        Ok(Some(CostEstimate {
            resource_id,
            resource_type: resource_type.to_string(),
            provider: "aws".to_string(),
            description: Some(description),
            quantity,
            hourly: base_hourly * quantity as f64,
            monthly: base_monthly * quantity as f64,
        }))
    }
}
