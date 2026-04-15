# Import and Discover

## What IMPORT RESOURCES does

`IMPORT RESOURCES` reads live cloud state from one or more providers and registers discovered resources in the local registry. It is the inverse of `CREATE` -- instead of "make this exist," it asks "tell me what's already there."

IMPORT is **read-only**. It never modifies, creates, or deletes cloud resources. It only writes to the local registry.

---

## Syntax

### Import from a single provider

```sql
IMPORT RESOURCES FROM PROVIDER 'azure';
```

### Import from providers matching a filter

```sql
IMPORT RESOURCES FROM PROVIDERS WHERE type = 'aws';
```

### Import from all registered providers

```sql
IMPORT RESOURCES FROM ALL PROVIDERS;
```

---

## Filtering by resource type

Use a `WHERE resource_type` clause to limit discovery to specific resource types:

```sql
-- Single type
IMPORT RESOURCES FROM PROVIDER 'aws-prod'
  WHERE resource_type = 'ec2';

-- Multiple types
IMPORT RESOURCES FROM PROVIDER 'aws-prod'
  WHERE resource_type IN ('ec2', 'rds_postgres', 's3_bucket');
```

Without a filter, all discoverable resource types for the provider are imported.

---

## Discoverable resource types by provider

### Azure

| Resource type | Description |
|---------------|-------------|
| `vm` | Virtual Machine |
| `postgres` | PostgreSQL Flexible Server |
| `redis` | Redis Cache |
| `aks` | Azure Kubernetes Service |
| `vnet` | Virtual Network |
| `nsg` | Network Security Group |
| `storage_account` | Storage Account |
| `keyvault` | Key Vault |

### AWS

| Resource type | Description |
|---------------|-------------|
| `ec2` | EC2 Instance |
| `rds_postgres` | RDS PostgreSQL |
| `vpc` | VPC |
| `aws_subnet` | VPC Subnet |
| `security_group` | Security Group |
| `s3_bucket` | S3 Bucket |
| `lambda` | Lambda Function |
| `elb` | Elastic Load Balancer |

### GitHub

| Resource type | Description |
|---------------|-------------|
| `gh_repo` | GitHub Repository |

### Cloudflare

| Resource type | Description |
|---------------|-------------|
| `cf_zone` | Cloudflare Zone (domain) |
| `cf_dns_record` | DNS Record |

### Kubernetes

| Resource type | Description |
|---------------|-------------|
| `k8s_namespace` | Namespace |
| `k8s_deployment` | Deployment |
| `k8s_service` | Service |

### SSH

| Resource type | Description |
|---------------|-------------|
| `systemd_service` | systemd unit |
| `docker_container` | Docker container |
| `docker_volume` | Docker volume |
| `nginx_vhost` | Nginx virtual host |
| `letsencrypt_cert` | Let's Encrypt certificate |

---

## The import_log table

Every `IMPORT RESOURCES` execution writes entries to the `import_log` table. Each row records one discovered resource along with whether it was newly added or already known.

```sql
SELECT * FROM import_log
  WHERE action = 'new'
  ORDER BY imported_at DESC;
```

Columns: `id`, `provider_id`, `resource_type`, `resource_id`, `action`, `details`, `imported_at`.

The `action` field is `new` for first-time discoveries and `existing` for resources already in the registry.

---

## Multi-account inventory example

A common use case is registering all your cloud accounts and running a single discovery pass to build a complete inventory.

### 1. Register providers

```sql
-- AWS accounts
ADD IF NOT EXISTS PROVIDER id='aws-prod'
  type='aws' region='us-east-1'
  auth='env:AWS_PROD_PROFILE';

ADD IF NOT EXISTS PROVIDER id='aws-staging'
  type='aws' region='us-west-2'
  auth='env:AWS_STAGING_PROFILE';

-- Azure
ADD IF NOT EXISTS PROVIDER id='azure'
  type='azure'
  auth='env:AZURE_SUBSCRIPTION_ID';

-- Cloudflare
ADD IF NOT EXISTS PROVIDER id='cf'
  type='cloudflare'
  auth='op:Infrastructure/Cloudflare-API-Token';

-- GitHub
ADD IF NOT EXISTS PROVIDER id='gh'
  type='github'
  auth='env:GITHUB_TOKEN';

-- Kubernetes
ADD IF NOT EXISTS PROVIDER id='k8s-prod'
  type='kubernetes'
  auth='file:~/.kube/config';

-- SSH host
ADD IF NOT EXISTS PROVIDER id='bastion'
  type='ssh'
  host='bastion.example.com'
  auth='op:Infrastructure/bastion-ssh-key'
  labels='{"ssh_user":"ops"}';
```

### 2. Import from all providers

```sql
IMPORT RESOURCES FROM ALL PROVIDERS;
```

### 3. Query the estate

```sql
-- Total resources per provider
SELECT provider_id, count(*) FROM resources
  GROUP BY provider_id
  ORDER BY count(*) DESC;

-- All VMs across AWS and Azure
SELECT id, provider_id, status FROM resources
  WHERE resource_type IN ('vm', 'ec2');

-- All databases
SELECT id, resource_type, provider_id FROM resources
  WHERE resource_type IN ('postgres', 'rds_postgres');

-- Docker containers on SSH hosts
SELECT id, provider_id FROM resources
  WHERE resource_type = 'docker_container';
```

---

## Querying imported resources

Imported resources live in the same `resources` table as resources created via `CREATE RESOURCE`. All standard query features work:

```sql
-- Filter by type
SELECT * FROM resources WHERE resource_type = 'ec2';

-- Filter by provider
SELECT * FROM resources WHERE provider_id = 'aws-prod';

-- Pattern matching
SELECT * FROM resources WHERE id LIKE 'prod-%';

-- Combine filters
SELECT id, resource_type, status FROM resources
  WHERE provider_id = 'azure' AND status = 'running'
  ORDER BY resource_type;
```

---

## Read-only guarantee

`IMPORT RESOURCES` never modifies cloud resources. It issues read-only API calls (list/describe operations) against each provider and writes the results to the local registry. No infrastructure is created, updated, or deleted.

To test discovery without even writing to the registry, combine with simulate mode:

```bash
orbi --simulate "IMPORT RESOURCES FROM ALL PROVIDERS;"
```
