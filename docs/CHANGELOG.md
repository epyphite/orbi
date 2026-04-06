# Changelog

## v0.3.0 (2026-04-06)

### New Providers
- **Cloudflare** (4 resource types): cf_zone, cf_dns_record, cf_firewall_rule, cf_page_rule
  - REST API client via reqwest::blocking, Bearer token auth
  - Zone name → zone ID resolution per request
  - Closes the DNS loop: provision VM → point domain at it in one file
- **GitHub** (6 resource types): gh_repo, gh_ruleset, gh_secret, gh_variable, gh_workflow_file, gh_branch_protection
  - Uses `gh` CLI as backend (no libsodium dependency for secrets)
  - Supports multi-account via GH_TOKEN env injection
  - Secret values never appear in EXPLAIN output
  - Recreates manual repo setup as a single .kvmql file
- **Kubernetes** (6 resource types): k8s_namespace, k8s_deployment, k8s_service, k8s_ingress, k8s_configmap, k8s_secret
  - Uses `kubectl` CLI as backend, supports --context for multi-cluster
  - YAML generation via string concatenation (no serde_yaml dep)
  - `kubectl apply -f -` is idempotent, enabling clean re-runs

### Live Cluster Queries (Kubernetes)
- **New pattern**: SELECT against live cluster state, not the registry
- 8 new nouns: k8s_pods, k8s_deployments, k8s_services, k8s_ingresses,
  k8s_configmaps, k8s_secrets, k8s_namespaces, k8s_nodes
- Pod status surfaces CrashLoopBackOff / ImagePullBackOff from container waiting state
- The killer query: `SELECT * FROM k8s_pods WHERE status = 'CrashLoopBackOff'`
- Deployments expose replicas, ready_replicas, available_replicas as columns

### Engine improvements
- Provisioner dispatch via prefix matching (cf_, gh_, k8s_, aws_, azure_)
- EXPLAIN routes to the correct provisioner per resource type
- Provider CHECK constraint expanded to include cloudflare, github, kubernetes (v6 migration)
- Fixed: insert_provider now distinguishes UNIQUE violations from CHECK violations
- Fixed: VNet address_space / address_prefix both accepted

### Stats
- 641 tests (up from 553)
- 33,341 lines of Rust (up from ~29,000)
- 37 total resource types across 6 providers
- 8 crates in the workspace

---

## v0.2.0 (2026-04-02)

### Core
- SQL-like DSL with 35+ statement types
- Recursive descent parser with error recovery
- Full SELECT with WHERE, ORDER BY, GROUP BY, LIMIT, OFFSET
- IS NULL / IS NOT NULL, LIKE, arithmetic operators (+, -)
- Variable substitution: SET @var = 'value'

### Providers
- Azure VM driver (real, via az CLI)
- AWS EC2 driver (real, via aws CLI)
- GCP Compute Engine (skeleton)
- Firecracker (real, via Unix socket HTTP)
- Simulation driver (--simulate, no credentials needed)

### Managed Resources (21 types)
- Azure: postgres, redis, aks, storage_account, vnet, subnet, nsg, nsg_rule,
  vnet_peering, container_registry, dns_zone, dns_vnet_link, container_app,
  container_job, load_balancer, pg_database
- AWS: rds_postgres, vpc, aws_subnet, security_group, sg_rule

### Operations
- EXPLAIN -- show what would happen without executing
- --dry-run -- plan mode for all mutations
- ROLLBACK LAST / TO TAG / RESOURCE -- undo mutations
- BACKUP / RESTORE / SCALE / UPGRADE for managed resources
- Plan files with SHA-256 integrity verification
- DB-backed plan workflow: plan -> approve -> apply

### Security
- 9 credential backends (env, file, Vault, AWS SM, GCP SM, Azure KV, 1Password, SOPS, K8s)
- SSH/VM access: admin_user, ssh_key, cloud_init params
- GRANT/REVOKE with WHERE conditions
- Append-only audit log
- Credential scrubbing in history and plans

### CLI
- Interactive REPL with 16 meta-commands
- 4 output formats (table, json, csv, raw)
- Environment management (--env, orbi env create/copy/export)
- Tab completion, multi-line input, session scoping

### Registry
- SQLite-backed with 18 tables
- Persisted across invocations
- Driver re-hydration from providers table
- Environment isolation via separate .db files
