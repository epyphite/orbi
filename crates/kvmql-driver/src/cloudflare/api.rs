//! Cloudflare API client — thin wrapper over the REST API.
//!
//! All Cloudflare responses are wrapped in a uniform envelope:
//! ```json
//! { "success": true, "errors": [], "messages": [], "result": { ... } }
//! ```
//!
//! This client unwraps the envelope and returns the `result` field directly,
//! mapping non-success responses to [`CloudflareError::Api`].

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;
use std::time::Duration;

const API_BASE: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug, thiserror::Error)]
pub enum CloudflareError {
    #[error("http error: {0}")]
    Http(String),
    #[error("api error: {0}")]
    Api(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid token")]
    InvalidToken,
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone)]
pub struct CloudflareClient {
    token: String,
    client: Client,
}

impl CloudflareClient {
    pub fn new(token: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build http client");
        Self {
            token: token.to_string(),
            client,
        }
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.token))
                .unwrap_or_else(|_| HeaderValue::from_static("Bearer invalid")),
        );
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h
    }

    /// GET `/<path>`
    pub fn get(&self, path: &str) -> Result<Value, CloudflareError> {
        let url = format!("{}{}", API_BASE, path);
        let resp = self
            .client
            .get(&url)
            .headers(self.headers())
            .send()
            .map_err(|e| CloudflareError::Http(e.to_string()))?;
        self.parse_response(resp)
    }

    /// POST `/<path>` with JSON body
    pub fn post(&self, path: &str, body: &Value) -> Result<Value, CloudflareError> {
        let url = format!("{}{}", API_BASE, path);
        let resp = self
            .client
            .post(&url)
            .headers(self.headers())
            .json(body)
            .send()
            .map_err(|e| CloudflareError::Http(e.to_string()))?;
        self.parse_response(resp)
    }

    /// PUT `/<path>`
    pub fn put(&self, path: &str, body: &Value) -> Result<Value, CloudflareError> {
        let url = format!("{}{}", API_BASE, path);
        let resp = self
            .client
            .put(&url)
            .headers(self.headers())
            .json(body)
            .send()
            .map_err(|e| CloudflareError::Http(e.to_string()))?;
        self.parse_response(resp)
    }

    /// DELETE `/<path>`
    pub fn delete(&self, path: &str) -> Result<Value, CloudflareError> {
        let url = format!("{}{}", API_BASE, path);
        let resp = self
            .client
            .delete(&url)
            .headers(self.headers())
            .send()
            .map_err(|e| CloudflareError::Http(e.to_string()))?;
        self.parse_response(resp)
    }

    fn parse_response(
        &self,
        resp: reqwest::blocking::Response,
    ) -> Result<Value, CloudflareError> {
        let status = resp.status();
        if status == 401 || status == 403 {
            return Err(CloudflareError::InvalidToken);
        }
        if status == 404 {
            return Err(CloudflareError::NotFound("resource not found".into()));
        }
        let json: Value = resp
            .json()
            .map_err(|e| CloudflareError::Parse(e.to_string()))?;

        let success = json
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !success {
            let errors = json
                .get("errors")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "unknown error".into());
            return Err(CloudflareError::Api(errors));
        }

        Ok(json.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Look up a zone ID by name.  Returns [`CloudflareError::NotFound`] when
    /// the named zone cannot be located on the account.
    pub fn resolve_zone_id(&self, zone_name: &str) -> Result<String, CloudflareError> {
        let result = self.get(&format!("/zones?name={}", zone_name))?;
        let zones = result
            .as_array()
            .ok_or_else(|| CloudflareError::Parse("expected zones array".into()))?;
        let zone = zones.first().ok_or_else(|| {
            CloudflareError::NotFound(format!("zone '{}' not found", zone_name))
        })?;
        zone.get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| CloudflareError::Parse("zone missing id".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_new() {
        let c = CloudflareClient::new("test-token");
        assert_eq!(c.token, "test-token");
    }

    #[test]
    fn test_headers_contain_bearer() {
        let c = CloudflareClient::new("abc123");
        let headers = c.headers();
        let auth = headers.get(AUTHORIZATION).unwrap();
        assert_eq!(auth.to_str().unwrap(), "Bearer abc123");
    }
}
