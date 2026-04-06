//! Cloudflare provider for KVMQL/Orbi.
//!
//! Maps KVMQL resource types (`cf_zone`, `cf_dns_record`, `cf_firewall_rule`,
//! `cf_page_rule`) to the Cloudflare API.  Closes the DNS loop — every
//! provisioned VM can now have a DNS record in one statement.

pub mod api;
pub mod resources;

pub use api::{CloudflareClient, CloudflareError};
pub use resources::CloudflareResourceProvisioner;
