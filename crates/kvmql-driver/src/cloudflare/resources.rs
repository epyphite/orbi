//! Cloudflare resource provisioner.
//!
//! Dispatches KVMQL `cf_*` resource types to the Cloudflare API.  The
//! provisioner is constructed from an optional API token; if absent, every
//! create/delete operation returns a helpful error instructing the user to
//! configure `auth='env:CLOUDFLARE_API_TOKEN'` on the provider.
//!
//! Supported resource types:
//! - `cf_zone` — Cloudflare zone (domain)
//! - `cf_dns_record` — A/AAAA/CNAME/MX/TXT/etc.
//! - `cf_firewall_rule` — custom firewall rule
//! - `cf_page_rule` — page rule (cache, SSL, etc.)

use serde_json::{json, Value};

use super::api::CloudflareClient;

#[derive(Debug, Clone)]
pub struct CloudflareResourceProvisioner {
    client: Option<CloudflareClient>,
}

/// Result of a provisioning operation.  Mirrors Azure/AWS provisioner shape.
#[derive(Debug)]
pub struct ProvisionResult {
    /// One of "created", "updated", "deleted".
    pub status: String,
    /// Provider-specific outputs (zone_id, record_id, name_servers, etc.).
    pub outputs: Option<Value>,
}

impl CloudflareResourceProvisioner {
    pub fn new(token: Option<&str>) -> Self {
        Self {
            client: token.map(CloudflareClient::new),
        }
    }

    /// Create a Cloudflare resource.  Dispatches by `resource_type`.
    pub fn create(&self, resource_type: &str, params: &Value) -> Result<ProvisionResult, String> {
        match resource_type {
            "cf_zone" => self.create_zone(params),
            "cf_dns_record" => self.create_dns_record(params),
            "cf_firewall_rule" => self.create_firewall_rule(params),
            "cf_page_rule" => self.create_page_rule(params),
            other => Err(format!("unsupported cloudflare resource type: {other}")),
        }
    }

    /// Delete a Cloudflare resource.  `id` is typically the record/rule ID
    /// returned in `outputs` at create time; for zones it may be the zone name
    /// or the 32-char zone ID.
    pub fn delete(
        &self,
        resource_type: &str,
        id: &str,
        params: &Value,
    ) -> Result<(), String> {
        match resource_type {
            "cf_zone" => self.delete_zone(id),
            "cf_dns_record" => self.delete_dns_record(id, params),
            "cf_firewall_rule" => self.delete_firewall_rule(id, params),
            "cf_page_rule" => self.delete_page_rule(id, params),
            other => Err(format!("unsupported cloudflare resource type: {other}")),
        }
    }

    fn client(&self) -> Result<&CloudflareClient, String> {
        self.client.as_ref().ok_or_else(|| {
            "Cloudflare API token not configured. Set auth='env:CLOUDFLARE_API_TOKEN' on provider."
                .to_string()
        })
    }

    // ── Zone operations ──────────────────────────────────────

    fn create_zone(&self, params: &Value) -> Result<ProvisionResult, String> {
        let name = param_str(params, "id")?;
        let zone_type = params
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("full");

        let body = json!({
            "name": name,
            "type": zone_type,
        });

        let result = self
            .client()?
            .post("/zones", &body)
            .map_err(|e| format!("failed to create zone: {e}"))?;

        let outputs = json!({
            "zone_id": result.get("id"),
            "name_servers": result.get("name_servers"),
            "status": result.get("status"),
            "plan": result.get("plan").and_then(|p| p.get("name")),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn delete_zone(&self, id: &str) -> Result<(), String> {
        // `id` might be the zone name or the 32-char zone id.
        let client = self.client()?;
        let zone_id = if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) {
            id.to_string()
        } else {
            client
                .resolve_zone_id(id)
                .map_err(|e| e.to_string())?
        };
        client
            .delete(&format!("/zones/{}", zone_id))
            .map_err(|e| format!("failed to delete zone: {e}"))?;
        Ok(())
    }

    // ── DNS Record operations ────────────────────────────────

    fn create_dns_record(&self, params: &Value) -> Result<ProvisionResult, String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone '{}': {e}", zone_name))?;

        let record_type = param_str_or(params, "type", "A");
        let name = param_str(params, "id")?;
        let content = param_str(params, "content")?;
        let ttl = params.get("ttl").and_then(|v| v.as_i64()).unwrap_or(1);
        let proxied = params
            .get("proxied")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut body = json!({
            "type": record_type,
            "name": name,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        });

        // MX records need a priority.
        if record_type == "MX" {
            if let Some(priority) = params.get("priority").and_then(|v| v.as_i64()) {
                body["priority"] = json!(priority);
            }
        }

        let result = client
            .post(&format!("/zones/{}/dns_records", zone_id), &body)
            .map_err(|e| format!("failed to create dns record: {e}"))?;

        let outputs = json!({
            "record_id": result.get("id"),
            "zone_id": zone_id,
            "name": result.get("name"),
            "type": result.get("type"),
            "content": result.get("content"),
            "proxied": result.get("proxied"),
        });

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(outputs),
        })
    }

    fn delete_dns_record(&self, id: &str, params: &Value) -> Result<(), String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone: {e}"))?;
        // `id` here is expected to be the record_id from outputs.
        client
            .delete(&format!("/zones/{}/dns_records/{}", zone_id, id))
            .map_err(|e| format!("failed to delete dns record: {e}"))?;
        Ok(())
    }

    // ── Firewall Rule operations ────────────────────────────

    fn create_firewall_rule(&self, params: &Value) -> Result<ProvisionResult, String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone: {e}"))?;

        let expression = param_str(params, "expression")?;
        let action = param_str_or(params, "action", "block");
        let description = params
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let _rule_name = param_str(params, "id")?;

        // Legacy firewall rules API — simpler than rulesets for one-off rules.
        let body = json!([{
            "filter": {
                "expression": expression,
            },
            "action": action,
            "description": description,
        }]);

        let result = client
            .post(&format!("/zones/{}/firewall/rules", zone_id), &body)
            .map_err(|e| format!("failed to create firewall rule: {e}"))?;

        // Response is an array; take the first element.
        let rule = result
            .as_array()
            .and_then(|a| a.first())
            .cloned()
            .unwrap_or(Value::Null);

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "rule_id": rule.get("id"),
                "zone_id": zone_id,
                "action": action,
            })),
        })
    }

    fn delete_firewall_rule(&self, id: &str, params: &Value) -> Result<(), String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone: {e}"))?;
        client
            .delete(&format!("/zones/{}/firewall/rules/{}", zone_id, id))
            .map_err(|e| format!("failed to delete firewall rule: {e}"))?;
        Ok(())
    }

    // ── Page Rule operations ──────────────────────────────────

    fn create_page_rule(&self, params: &Value) -> Result<ProvisionResult, String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone: {e}"))?;

        let url_pattern = param_str(params, "url")?;
        let priority = params
            .get("priority")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);

        // Build actions from params — cache_level, ssl, etc.
        let mut actions = Vec::new();
        if let Some(cache_level) = params.get("cache_level").and_then(|v| v.as_str()) {
            actions.push(json!({ "id": "cache_level", "value": cache_level }));
        }
        if let Some(ssl) = params.get("ssl").and_then(|v| v.as_str()) {
            actions.push(json!({ "id": "ssl", "value": ssl }));
        }
        if actions.is_empty() {
            return Err(
                "page rule requires at least one action (cache_level, ssl, etc.)".into(),
            );
        }

        let body = json!({
            "targets": [{
                "target": "url",
                "constraint": { "operator": "matches", "value": url_pattern }
            }],
            "actions": actions,
            "priority": priority,
            "status": "active",
        });

        let result = client
            .post(&format!("/zones/{}/pagerules", zone_id), &body)
            .map_err(|e| format!("failed to create page rule: {e}"))?;

        Ok(ProvisionResult {
            status: "created".into(),
            outputs: Some(json!({
                "rule_id": result.get("id"),
                "zone_id": zone_id,
                "priority": priority,
            })),
        })
    }

    fn delete_page_rule(&self, id: &str, params: &Value) -> Result<(), String> {
        let client = self.client()?;
        let zone_name = param_str(params, "zone")?;
        let zone_id = client
            .resolve_zone_id(&zone_name)
            .map_err(|e| format!("failed to resolve zone: {e}"))?;
        client
            .delete(&format!("/zones/{}/pagerules/{}", zone_id, id))
            .map_err(|e| format!("failed to delete page rule: {e}"))?;
        Ok(())
    }

    // ── Discovery ────────────────────────────────────────────

    /// Discover existing Cloudflare resources: zones and their DNS records.
    pub fn discover(&self) -> Result<Vec<Value>, String> {
        let client = self.client()?;
        let mut results = Vec::new();

        // Discover zones
        let zones_raw = client
            .get("/zones?per_page=50")
            .map_err(|e| format!("failed to list zones: {e}"))?;

        let zones = match zones_raw.as_array() {
            Some(arr) => arr.clone(),
            None => return Ok(results),
        };

        for zone in &zones {
            let zone_id = zone.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let zone_name = zone.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if zone_id.is_empty() || zone_name.is_empty() {
                continue;
            }

            let plan_name = zone
                .get("plan")
                .and_then(|p| p.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            results.push(json!({
                "id": zone_name,
                "resource_type": "cf_zone",
                "name": zone_name,
                "config": {
                    "type": zone.get("type").and_then(|v| v.as_str()).unwrap_or("full"),
                },
                "outputs": {
                    "zone_id": zone_id,
                    "name": zone_name,
                    "status": zone.get("status"),
                    "plan": plan_name,
                    "name_servers": zone.get("name_servers"),
                },
            }));

            // Discover DNS records for this zone
            let records_raw = client
                .get(&format!("/zones/{}/dns_records?per_page=100", zone_id))
                .map_err(|e| format!("failed to list dns records for {}: {e}", zone_name))?;

            if let Some(records) = records_raw.as_array() {
                for record in records {
                    let record_id = record.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let record_name = record.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let record_type = record.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if record_id.is_empty() {
                        continue;
                    }

                    results.push(json!({
                        "id": record_id,
                        "resource_type": "cf_dns_record",
                        "name": record_name,
                        "config": {
                            "zone": zone_name,
                            "type": record_type,
                            "content": record.get("content"),
                            "ttl": record.get("ttl"),
                            "proxied": record.get("proxied"),
                        },
                        "outputs": {
                            "record_id": record_id,
                            "zone_id": zone_id,
                            "name": record_name,
                            "type": record_type,
                            "content": record.get("content"),
                            "ttl": record.get("ttl"),
                            "proxied": record.get("proxied"),
                        },
                    }));
                }
            }
        }

        Ok(results)
    }

    // ── build_create_args (for EXPLAIN / dry-run) ────────────

    /// Build a human-readable description of the API calls a create would
    /// emit, without actually executing them.  Used by EXPLAIN and dry-run.
    pub fn build_create_args(
        &self,
        resource_type: &str,
        params: &Value,
    ) -> Result<Vec<String>, String> {
        match resource_type {
            "cf_zone" => {
                let name = param_str(params, "id")?;
                Ok(vec!["POST /zones".to_string(), format!("name={name}")])
            }
            "cf_dns_record" => {
                let zone = param_str(params, "zone")?;
                let rtype = param_str_or(params, "type", "A");
                let name = param_str(params, "id")?;
                let content = param_str(params, "content")?;
                Ok(vec![
                    format!("POST /zones/{{{zone}_id}}/dns_records"),
                    format!("type={rtype}"),
                    format!("name={name}"),
                    format!("content={content}"),
                ])
            }
            "cf_firewall_rule" => {
                let zone = param_str(params, "zone")?;
                let expr = param_str(params, "expression")?;
                Ok(vec![
                    format!("POST /zones/{{{zone}_id}}/firewall/rules"),
                    format!("expression={expr}"),
                ])
            }
            "cf_page_rule" => {
                let zone = param_str(params, "zone")?;
                let url = param_str(params, "url")?;
                Ok(vec![
                    format!("POST /zones/{{{zone}_id}}/pagerules"),
                    format!("url={url}"),
                ])
            }
            other => Err(format!("unsupported: {other}")),
        }
    }
}

fn param_str(params: &Value, key: &str) -> Result<String, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| format!("missing required parameter: {key}"))
}

fn param_str_or(params: &Value, key: &str, default: &str) -> String {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_str_present() {
        let params = json!({"id": "test"});
        assert_eq!(param_str(&params, "id").unwrap(), "test");
    }

    #[test]
    fn test_param_str_missing() {
        let params = json!({});
        assert!(param_str(&params, "id").is_err());
    }

    #[test]
    fn test_build_create_args_dns_record() {
        let p = CloudflareResourceProvisioner::new(None);
        let params = json!({
            "id": "api.example.com",
            "zone": "example.com",
            "type": "A",
            "content": "1.2.3.4"
        });
        let args = p.build_create_args("cf_dns_record", &params).unwrap();
        assert!(args.iter().any(|a| a.contains("dns_records")));
        assert!(args.iter().any(|a| a.contains("api.example.com")));
    }

    #[test]
    fn test_build_create_args_firewall_rule() {
        let p = CloudflareResourceProvisioner::new(None);
        let params = json!({
            "id": "block-china",
            "zone": "example.com",
            "expression": "(ip.geoip.country eq \"CN\")"
        });
        let args = p.build_create_args("cf_firewall_rule", &params).unwrap();
        assert!(args.iter().any(|a| a.contains("firewall")));
    }

    #[test]
    fn test_create_without_token_errors() {
        let p = CloudflareResourceProvisioner::new(None);
        let params = json!({"id": "test.com"});
        let result = p.create("cf_zone", &params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("token not configured"));
    }

    #[test]
    fn test_unsupported_resource_type() {
        let p = CloudflareResourceProvisioner::new(Some("fake-token"));
        let result = p.create("cf_unknown", &json!({}));
        assert!(result.is_err());
    }
}
