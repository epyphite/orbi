# Orbi

**What if `kubectl` spoke SQL?**

[![asciicast](https://asciinema.org/a/5fd4E7b0lYHQQ53h.svg)](https://asciinema.org/a/5fd4E7b0lYHQQ53h)

**A SQL-like DSL for managing infrastructure across clouds.**

Orbi replaces Terraform for VM, database, networking, and managed service lifecycle management. Write infrastructure as SQL-like statements, execute against any cloud provider with a single binary.

```sql
-- One file. Any cloud.
CREATE RESOURCE 'postgres' id = 'prod-db'
  version = '16' sku = 'Standard_B2s' storage_gb = 64
  ON PROVIDER 'azure-prod';

CREATE RESOURCE 'rds_postgres' id = 'dr-db'
  instance_class = 'db.t3.medium' engine_version = '16'
  master_username = 'admin' master_password = 'env:DR_DB_PASSWORD'
  storage_gb = 64
  ON PROVIDER 'aws-dr';

SELECT * FROM resources;
```

## Why Orbi?

| | Terraform | Orbi |
|---|---|---|
| Language | HCL (custom) | SQL-like (familiar) |
| State | Separate .tfstate file | Embedded SQLite (queryable) |
| Plan/Apply | `terraform plan` | `EXPLAIN`, `--dry-run`, `orbi plan` |
| Rollback | Manual | `ROLLBACK LAST` |
| Query state | External tools | `SELECT * FROM resources WHERE ...` |
| Multi-cloud | Per-provider blocks | `ON PROVIDER 'aws'` / `ON PROVIDER 'azure'` |
| Credentials | Per-provider config | 9 backends (env, Vault, Azure KV, AWS SM, 1Password, SOPS, ...) |

## Quick Start

```bash
# Install
curl -fsSL https://raw.githubusercontent.com/epyphite/orbi/main/install.sh | sh

# Try immediately -- no credentials needed
orbi --simulate "CREATE RESOURCE 'postgres' id = 'test-db' version = '16';"

# Interactive shell
orbi shell
```

## Features

### Infrastructure as SQL

```sql
-- Create a managed Postgres database
CREATE RESOURCE 'postgres'
  id = 'prod-db' version = '16' sku = 'Standard_B2s' storage_gb = 64
  ON PROVIDER 'azure';

-- Query all infrastructure
SELECT id, resource_type, status FROM resources
  WHERE status = 'running' ORDER BY resource_type;

-- Preview changes before applying
EXPLAIN CREATE RESOURCE 'redis' id = 'cache' sku = 'Standard';

-- Undo the last mutation
ROLLBACK LAST;
```

### 37 Managed Resource Types across 6 providers

#### Azure (16 types)

| Type | Description |
|------|-------------|
| `postgres` | PostgreSQL Flexible Server |
| `redis` | Redis Cache |
| `aks` | Azure Kubernetes Service |
| `storage_account` | Storage Account (Blob/File/Queue/Table) |
| `vnet` | Virtual Network |
| `subnet` | VNet Subnet |
| `nsg` | Network Security Group |
| `nsg_rule` | NSG Rule |
| `vnet_peering` | VNet Peering |
| `container_registry` | Container Registry (ACR) |
| `dns_zone` | DNS Zone |
| `dns_vnet_link` | Private DNS Zone VNet Link |
| `container_app` | Container App / Serverless Container |
| `container_job` | Container Job (one-off or scheduled) |
| `load_balancer` | Load Balancer |
| `pg_database` | PostgreSQL Database (on a Flexible Server) |

#### AWS (5 types)

| Type | Description |
|------|-------------|
| `rds_postgres` | RDS PostgreSQL |
| `vpc` | VPC |
| `aws_subnet` | VPC Subnet |
| `security_group` | Security Group |
| `sg_rule` | Security Group Rule |

#### Cloudflare (4 types)

| Type | Description |
|------|-------------|
| `cf_zone` | Cloudflare Zone (domain) |
| `cf_dns_record` | DNS Record (A/AAAA/CNAME/MX/TXT/...) |
| `cf_firewall_rule` | Custom Firewall Rule |
| `cf_page_rule` | Page Rule (cache, SSL, etc.) |

#### GitHub (6 types)

| Type | Description |
|------|-------------|
| `gh_repo` | GitHub Repository |
| `gh_ruleset` | Repository Ruleset (modern branch protection) |
| `gh_secret` | GitHub Actions Secret (gh handles encryption) |
| `gh_variable` | GitHub Actions Variable |
| `gh_workflow_file` | Workflow YAML file via contents API |
| `gh_branch_protection` | Legacy branch protection |

#### Kubernetes (6 types + live queries)

| Type | Description |
|------|-------------|
| `k8s_namespace` | Namespace |
| `k8s_deployment` | Deployment with replicas, image, env |
| `k8s_service` | Service (ClusterIP, LoadBalancer, NodePort) |
| `k8s_ingress` | Ingress with TLS and host rules |
| `k8s_configmap` | ConfigMap |
| `k8s_secret` | Secret (stringData auto-encoded) |

Plus **live cluster queries** against real cluster state:

```sql
-- Find crashing pods across all namespaces
SELECT name, namespace, status, restarts FROM k8s_pods
  WHERE status = 'CrashLoopBackOff';

-- Find under-replicated deployments
SELECT name, replicas, ready_replicas FROM k8s_deployments
  WHERE ready_replicas < replicas;
```

Nothing else gives you SQL over live Kubernetes state.

### 9 Credential Backends

| Scheme | Backend |
|--------|---------|
| `env:VAR_NAME` | Environment variable |
| `file:/path/to/secret` | File (permission-checked) |
| `vault:mount/path#field` | HashiCorp Vault KV v2 |
| `aws-sm:secret-name#field` | AWS Secrets Manager |
| `gcp-sm:secret-ref` | GCP Secret Manager |
| `azure-kv:vault-name/secret-name` | Azure Key Vault |
| `op:vault/item#field` | 1Password CLI |
| `sops:/path/to/file#key.subkey` | Mozilla SOPS |
| `k8s:namespace/secret-name#key` | Kubernetes Secrets |

### VM Access Configuration

```sql
CREATE MICROVM
  tenant    = 'acme'
  vcpus     = 2
  memory_mb = 4096
  image     = 'ubuntu-24.04-lts'
  hostname  = 'acme-api'
  admin_user = 'ops'
  ssh_key    = 'file:~/.ssh/id_ed25519.pub'
  cloud_init = 'file:/etc/orbi/cloud-init/base.yaml'
  ON PROVIDER 'kvm.host-a';
```

### Plan, Review, Apply Workflow

```bash
# Generate a plan from a DSL file
orbi plan examples/azure-stack.kvmql --name "staging deploy"

# Review pending plans
orbi plans --status pending

# Approve a plan
orbi approve <plan-id>

# Apply an approved plan
orbi apply <plan-id>
```

### Environment Management

```bash
# Run against a named environment
orbi --env staging "SELECT * FROM resources;"

# Create, list, copy, export environments
orbi env create production
orbi env list
orbi env copy staging production
orbi env export staging > staging-snapshot.json
orbi env import snapshot.json dr-recovery
```

### Full SQL Query Engine

```sql
-- WHERE with AND, OR, NOT, parentheses
SELECT * FROM microvms
  WHERE (status = 'running' OR status = 'paused') AND tenant = 'acme';

-- ORDER BY, GROUP BY, LIMIT, OFFSET
SELECT tenant, status FROM microvms GROUP BY tenant, status;

-- Pattern matching
SELECT * FROM resources WHERE id LIKE 'prod%';

-- NULL checks
SELECT * FROM volumes WHERE microvm_id IS NULL;

-- Arithmetic
SELECT * FROM microvms WHERE vcpus = 2 + 2;
```

### Access Control

```sql
-- Grant scoped permissions with conditions
GRANT SELECT, SNAPSHOT ON microvms WHERE tenant = 'acme' TO 'ops@acme.com';

-- Revoke access
REVOKE DESTROY ON CLUSTER 'prod' FROM 'ops@acme.com';
```

### Audit Trail

```sql
-- Every mutation is logged
SELECT * FROM audit_log
  WHERE action = 'RESOURCE_CREATED'
  ORDER BY timestamp DESC LIMIT 20;
```

## Architecture

```
+---------------------------------------------+
|                    CLI                       |
|  orbi "DSL"  |  orbi shell  |  --env      |
+-----------------------+---------------------+
                        |
+-----------------------v---------------------+
|                  Engine                      |
|  Parse -> Auth -> Plan -> Execute -> Audit   |
+-----------------------+---------------------+
                        |
+---------+---------+---------+------------+---------------+
| Azure   |  AWS    |  GCP    | Cloudflare |  Firecracker  |
| az CLI  | aws CLI | gcloud  |  REST API  |  Unix socket  |
+---------+---------+---------+------------+---------------+
                        |
+-----------------------v---------------------+
|            Registry (SQLite)                 |
|  providers, resources, microvms,             |
|  audit_log, plans, state_snapshots           |
+---------------------------------------------+
```

## Documentation

- [Getting Started](docs/getting-started.md) -- installation, first commands, registry concept
- [Concepts](docs/concepts.md) -- providers, resources, registry, credentials, simulate mode
- [Provider Reference](docs/providers.md) -- all 7 providers with resource types and auth patterns
- [IMPORT / Discover](docs/import-discover.md) -- auto-populate registry from live cloud state
- [Network Verification](docs/network-verification.md) -- table-valued functions, ASSERT, deploy-and-verify
- [Recipes](docs/recipes.md) -- 7 complete cookbook patterns (web stack, multi-cloud DB, Docker, K8s, ...)
- [User Manual](MANUAL.md) -- complete DSL reference (17 sections)
- [Examples](examples/) -- ready-to-run `.kvmql` files
- [Specification](docs/SPEC.md) -- original DSL specification
- [Changelog](docs/CHANGELOG.md) -- release history

## Building from Source

```bash
git clone https://github.com/epyphite/orbi.git
cd orbi
cargo build --release
./target/release/orbi version
```

## Project Structure

```
orbi/
├── crates/
│   ├── kvmql-common/      # Shared types, notification codes, config
│   ├── kvmql-parser/      # Lexer (logos), AST, recursive descent parser
│   ├── kvmql-registry/    # SQLite registry, migrations, CRUD
│   ├── kvmql-driver/      # Driver trait + Azure/AWS/Cloudflare/GitHub/K8s/Firecracker
│   ├── kvmql-auth/        # 9 credential backends, access control
│   ├── kvmql-engine/      # Execution pipeline, EXPLAIN, ROLLBACK
│   ├── kvmql-agent/       # Per-host agent (heartbeat, state push)
│   └── kvmql-cli/         # CLI binary (orbi), REPL shell, output formats
├── examples/              # Demo .kvmql files
├── scripts/               # Build and release scripts
├── docs/                  # Specification and changelog
├── MANUAL.md              # User manual
├── CONTRIBUTING.md        # Contribution guide
├── LICENSE                # Apache 2.0
└── install.sh             # One-command installer
```

The internal crates are prefixed `kvmql-*` because the engine name is KVMQL — the SQL-like DSL parser/runtime that powers Orbi. The user-facing binary is `orbi`.

## Stats

| Metric | Value |
|--------|-------|
| Lines of Rust | ~29,000 |
| Tests | 572 |
| Crates | 8 |
| Statement types | 35+ |
| Resource types | 21 |
| Credential backends | 9 |

## License

Apache 2.0 — see [LICENSE](LICENSE)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
