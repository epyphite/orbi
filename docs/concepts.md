# Orbi Concepts

## Providers

A **provider** is a cloud backend that Orbi manages infrastructure against. Supported provider types:

| Type | Backend |
|------|---------|
| `aws` | Amazon Web Services |
| `azure` | Microsoft Azure |
| `cloudflare` | Cloudflare |
| `github` | GitHub |
| `kubernetes` | Kubernetes cluster |
| `ssh` | Remote host via SSH |

Register a provider with `ADD PROVIDER`:

```sql
ADD PROVIDER
  id     = 'aws-prod'
  type   = 'aws'
  region = 'us-east-1'
  auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';
```

The `auth` parameter references one of 9 credential backends (see [Credential resolution](#credential-resolution) below). Credentials are resolved at execution time and never stored in the registry.

---

## Resources

A **resource** is anything managed by a provider -- a database, cache, VM, DNS record, Kubernetes deployment, or any other cloud-managed object.

Create a resource with `CREATE RESOURCE`:

```sql
CREATE RESOURCE 'rds_postgres'
  id              = 'analytics-db'
  instance_class  = 'db.t3.medium'
  engine_version  = '16'
  master_username = 'admin'
  master_password = 'env:DB_PASSWORD'
  storage_gb      = 100
  ON PROVIDER 'aws-prod';
```

The resource type (first argument) determines which cloud API is invoked. The `ON PROVIDER` clause routes the operation to the correct backend.

---

## Registry

The **registry** is an embedded SQLite database that stores all Orbi state:

- **providers** -- registered cloud backends
- **resources** -- managed cloud resources
- **microvms** -- virtual machines
- **volumes** -- block storage
- **images** -- OS/boot images
- **audit_log** -- every mutation with timestamp, action, and details
- **query_history** -- every statement executed
- **import_log** -- records from `IMPORT RESOURCES` discovery
- **applied_files** -- files executed via `orbi exec`

Each registry file represents one infrastructure state. You can maintain separate registries for different environments:

```bash
orbi --registry ./staging.db "SELECT * FROM resources;"
orbi --registry ./production.db "SELECT * FROM resources;"
```

Because the registry is SQLite, all state is queryable with standard `SELECT`:

```sql
SELECT * FROM audit_log
  WHERE action = 'RESOURCE_CREATED'
  ORDER BY timestamp DESC
  LIMIT 10;
```

---

## Nouns

**Nouns** are the queryable tables in the registry. Use them in `SELECT`, `SHOW`, and `WATCH` statements:

| Noun | Description |
|------|-------------|
| `microvms` | Virtual machines |
| `volumes` | Block storage devices |
| `images` | OS and boot images |
| `providers` | Registered cloud backends |
| `clusters` | Logical groups of providers |
| `resources` | Managed cloud resources |
| `audit_log` | Mutation history |
| `query_history` | Statement execution history |
| `import_log` | Discovery import records |
| `applied_files` | Files executed via `orbi exec` |
| `k8s_pods` | Live Kubernetes pods (from cluster) |
| `k8s_deployments` | Live Kubernetes deployments |
| `k8s_services` | Live Kubernetes services |

Examples:

```sql
SELECT * FROM resources WHERE resource_type = 'postgres';

SELECT name, namespace, status FROM k8s_pods
  WHERE status = 'CrashLoopBackOff';

SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 20;
```

---

## Idempotency

Orbi provides two mechanisms for idempotent operations:

### IF NOT EXISTS

Use `IF NOT EXISTS` on `CREATE` and `ADD` statements. If the object already exists, the statement is a no-op:

```sql
ADD IF NOT EXISTS PROVIDER id='aws-prod'
  type='aws'
  auth='env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';
```

### Applied files tracking

When `orbi exec` runs a `.kvmql` file, it records the file path and content hash in the `applied_files` table. This enables pipelines to detect and skip files that have already been applied:

```sql
SELECT file_path, file_hash, applied_at, status
  FROM applied_files;
```

---

## Credential resolution

The `auth` parameter on providers uses a URI scheme to specify one of 9 credential backends. Credentials are resolved at execution time and are never persisted in the registry -- only the scheme reference is stored.

| Scheme | Backend | Example |
|--------|---------|---------|
| `env:` | Environment variable | `env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY` |
| `file:` | File (permission-checked) | `file:/etc/orbi/creds/token` |
| `op:` | 1Password CLI | `op:Infrastructure/Cloudflare-API-Token` |
| `vault:` | HashiCorp Vault KV v2 | `vault:secret/myapp#password` |
| `aws-sm:` | AWS Secrets Manager | `aws-sm:prod/db-password#password` |
| `gcp-sm:` | GCP Secret Manager | `gcp-sm:projects/myproj/secrets/key` |
| `azure-kv:` | Azure Key Vault | `azure-kv:my-vault/db-connection-string` |
| `sops:` | Mozilla SOPS | `sops:/path/to/secrets.yaml#db.password` |
| `k8s:` | Kubernetes Secrets | `k8s:default/my-secret#api-key` |

Multiple environment variables can be comma-separated:

```sql
ADD PROVIDER id='aws-prod' type='aws'
  auth='env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';
```

File-based credentials are verified for safe permissions (must not be world-readable).

---

## Simulate mode

The `--simulate` flag executes all statements without making real cloud API calls. The engine processes the DSL normally -- parsing, validation, registry writes -- but skips the provider driver call. Resources are recorded in the registry with a simulated status.

```bash
orbi --simulate "CREATE RESOURCE 'postgres' id='test' version='16';"
orbi --simulate exec examples/azure-stack.kvmql
```

Simulate mode requires no credentials and is safe to run against any registry. It is useful for:

- Validating DSL syntax and semantics before real execution.
- Exploring Orbi without cloud accounts.
- CI/CD pre-merge validation of infrastructure files.
