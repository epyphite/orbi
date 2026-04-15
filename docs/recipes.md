# Recipes -- Common Patterns

Complete, runnable `.kvmql` examples for common infrastructure tasks. Each recipe
can be executed with `orbi exec <file>` or tested in simulation mode with
`orbi --simulate exec <file>`.

---

## 1. Deploy a web app stack

Full stack on a remote host over SSH: directories, config files, systemd
services, nginx reverse proxy, and a TLS certificate via Let's Encrypt with
DNS-01 challenge through Cloudflare.

```sql
-- deploy-webapp.kvmql
-- Usage: orbi exec deploy-webapp.kvmql

SET @host = 'app.example.com';
SET @ssh_user = 'deploy';

-- ── Providers ────────────────────────────────────────────────────

ADD IF NOT EXISTS PROVIDER id='app-ssh'
  type='ssh'
  host='app.example.com'
  auth='file:~/.ssh/id_ed25519'
  labels='{"ssh_user":"deploy"}';

ADD IF NOT EXISTS PROVIDER id='cf'
  type='cloudflare'
  auth='env:CLOUDFLARE_API_TOKEN';

-- ── Foundation: directories ──────────────────────────────────────

CREATE RESOURCE 'directory' id='/opt/myapp'
  owner='deploy' group='deploy' mode='0755'
  ON PROVIDER 'app-ssh';

CREATE RESOURCE 'directory' id='/opt/myapp/bin'
  owner='deploy' group='deploy' mode='0755'
  ON PROVIDER 'app-ssh';

CREATE RESOURCE 'directory' id='/opt/myapp/config'
  owner='deploy' group='deploy' mode='0755'
  ON PROVIDER 'app-ssh';

-- ── Config files ─────────────────────────────────────────────────

CREATE RESOURCE 'file' id='/opt/myapp/config/.env'
  content='op:Infrastructure/myapp-prod-env'
  owner='deploy' group='deploy' mode='0600'
  ON PROVIDER 'app-ssh';

CREATE RESOURCE 'file' id='/etc/systemd/system/myapp.service'
  content='file:./units/myapp.service'
  mode='0644'
  ON PROVIDER 'app-ssh';

-- ── systemd service ──────────────────────────────────────────────

CREATE RESOURCE 'systemd_service' id='myapp'
  enabled=true started=true
  after_file='/etc/systemd/system/myapp.service'
  ON PROVIDER 'app-ssh';

-- ── TLS certificate (DNS-01 via Cloudflare) ──────────────────────

CREATE RESOURCE 'letsencrypt_cert' id='cert-app'
  domains=['app.example.com']
  email='ops@example.com'
  challenge='dns-01'
  dns_provider='cf'
  auto_renew=true
  renew_before_days=30
  ON PROVIDER 'app-ssh';

CREATE RESOURCE 'systemd_timer' id='cert-renew'
  schedule='daily'
  unit='cert-renew.service'
  enabled=true
  ON PROVIDER 'app-ssh';

-- ── nginx reverse proxy ──────────────────────────────────────────

CREATE RESOURCE 'nginx_proxy' id='app.example.com'
  server_name='app.example.com'
  upstream='http://127.0.0.1:3000'
  tls=true
  tls_cert_from='letsencrypt:app.example.com'
  ON PROVIDER 'app-ssh';

-- ── Cloudflare DNS ───────────────────────────────────────────────

CREATE RESOURCE 'cf_dns_record' id='app'
  zone='example.com'
  type='A'
  content='203.0.113.10'
  proxied=true
  ON PROVIDER 'cf';

-- ── Verify ───────────────────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM dns_lookup(@host, 'A')
), 'DNS does not resolve for app.example.com';

ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 443) WHERE status = 'open'
), 'port 443 not reachable';

ASSERT (
  SELECT status_code FROM http_probe('https://' || @host || '/health')
) = 200, 'health endpoint not returning 200';

ASSERT EXISTS (
  SELECT 1 FROM file_stat('app-ssh', '/opt/myapp/config/.env')
  WHERE present = true AND mode = '0600'
), '.env missing or has wrong permissions';

ASSERT EXISTS (
  SELECT 1 FROM tls_cert(@host, 443)
  WHERE days_remaining > 7
), 'TLS certificate expiring within 7 days';
```

---

## 2. Multi-cloud database setup

Azure PostgreSQL for production, AWS RDS PostgreSQL for disaster recovery, in a
single file. Both share the same schema version.

```sql
-- multi-cloud-db.kvmql
-- Usage: orbi exec multi-cloud-db.kvmql

SET @pg_version = '16';
SET @storage = 64;

-- ── Providers ────────────────────────────────────────────────────

ADD PROVIDER
  id     = 'azure-prod'
  type   = 'azure'
  region = 'eastus'
  auth   = 'azure-kv:infra-vault/deploy-creds';

ADD PROVIDER
  id     = 'aws-dr'
  type   = 'aws'
  region = 'us-west-2'
  auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';

-- ── Primary: Azure PostgreSQL ────────────────────────────────────

CREATE RESOURCE 'postgres' id = 'prod-db'
  version    = @pg_version
  sku        = 'Standard_B2s'
  storage_gb = @storage
  backup_retention_days = 14
  ON PROVIDER 'azure-prod';

CREATE RESOURCE 'pg_database' id = 'app'      server = 'prod-db';
CREATE RESOURCE 'pg_database' id = 'analytics' server = 'prod-db';

-- ── DR replica: AWS RDS ──────────────────────────────────────────

CREATE RESOURCE 'rds_postgres' id = 'dr-db'
  instance_class  = 'db.t3.medium'
  engine_version  = @pg_version
  master_username = 'admin'
  master_password = 'aws-sm:prod/dr-db-password#password'
  storage_gb      = @storage
  multi_az        = true
  ON PROVIDER 'aws-dr';

-- ── Query both ───────────────────────────────────────────────────

SELECT id, resource_type, provider_id, status FROM resources
  WHERE resource_type IN ('postgres', 'rds_postgres')
  ORDER BY provider_id;
```

---

## 3. Docker deployment

Deploy a PostgreSQL database and a Redis cache as Docker containers on a remote
host, each with persistent volumes.

```sql
-- docker-stack.kvmql
-- Usage: orbi exec docker-stack.kvmql

ADD IF NOT EXISTS PROVIDER id='web-host'
  type='ssh'
  host='web.example.com'
  auth='op:Infrastructure/web-host-ssh-key'
  labels='{"ssh_user":"ops"}';

-- ── Network ──────────────────────────────────────────────────────

CREATE RESOURCE 'docker_network' id='app-net'
  driver='bridge'
  ON PROVIDER 'web-host';

-- ── PostgreSQL ───────────────────────────────────────────────────

CREATE RESOURCE 'docker_volume' id='pg-data'
  ON PROVIDER 'web-host';

CREATE RESOURCE 'docker_container' id='app-postgres'
  image='postgres:16-alpine'
  ports=['5432:5432']
  volumes=['pg-data:/var/lib/postgresql/data']
  env=['POSTGRES_USER=app', 'POSTGRES_PASSWORD=env:PG_PASSWORD', 'POSTGRES_DB=appdb']
  restart_policy='unless-stopped'
  ON PROVIDER 'web-host';

-- ── Redis ────────────────────────────────────────────────────────

CREATE RESOURCE 'docker_volume' id='redis-data'
  ON PROVIDER 'web-host';

CREATE RESOURCE 'docker_container' id='app-redis'
  image='redis:7-alpine'
  ports=['6379:6379']
  volumes=['redis-data:/data']
  restart_policy='unless-stopped'
  ON PROVIDER 'web-host';

-- ── Verify ───────────────────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM tcp_probe('web.example.com', 5432) WHERE status = 'open'
), 'PostgreSQL port not reachable';

ASSERT EXISTS (
  SELECT 1 FROM tcp_probe('web.example.com', 6379) WHERE status = 'open'
), 'Redis port not reachable';

-- Inspect running containers on the host
-- SELECT * FROM docker_containers('web-host');
```

---

## 4. DNS + TLS verification

After deploying infrastructure, verify DNS resolution, TCP reachability, HTTP
health, and TLS certificate validity using table-valued functions and ASSERT.

```sql
-- verify-deployment.kvmql
-- Usage: orbi exec verify-deployment.kvmql

SET @host = 'api.example.com';
SET @ip   = '203.0.113.42';

-- ── DNS verification ─────────────────────────────────────────────

-- Confirm the A record resolves to the expected IP
ASSERT (
  SELECT content FROM dns_lookup(@host, 'A')
) = @ip, 'A record does not point to expected IP';

-- Confirm at least one record exists
ASSERT (
  SELECT count(*) FROM dns_lookup(@host)
) >= 1, 'no DNS records found';

-- Check reverse DNS
SELECT * FROM reverse_dns(@ip);

-- ── TCP probe ────────────────────────────────────────────────────

-- Port 443 must be open
ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 443) WHERE status = 'open'
), 'HTTPS port closed';

-- Port 80 must be open (redirect to HTTPS)
ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 80) WHERE status = 'open'
), 'HTTP port closed';

-- SSH should not be exposed to the internet
ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 22) WHERE status != 'open'
), 'SSH port unexpectedly open to the public';

-- ── HTTP probe ───────────────────────────────────────────────────

-- Health endpoint returns 200
ASSERT (
  SELECT status_code FROM http_probe('https://' || @host || '/health')
) = 200, 'health check failed';

-- Inspect full response headers
SELECT status_code, content_type, headers
  FROM http_probe('https://' || @host || '/');

-- ── TLS certificate ──────────────────────────────────────────────

-- Certificate must be valid for at least 14 days
ASSERT EXISTS (
  SELECT 1 FROM tls_cert(@host, 443)
  WHERE days_remaining > 14
), 'TLS certificate expires in fewer than 14 days';

-- Inspect certificate details
SELECT subject, issuer, not_before, not_after, days_remaining
  FROM tls_cert(@host, 443);
```

---

## 5. Multi-account AWS inventory

Register multiple AWS profiles (e.g., per-team or per-environment accounts),
import all resources, then query across every account from a single registry.

```sql
-- aws-inventory.kvmql
-- Usage: orbi exec aws-inventory.kvmql

-- ── Register five AWS accounts ───────────────────────────────────

ADD IF NOT EXISTS PROVIDER id='aws-prod'
  type='aws'
  region='us-east-1'
  auth='env:AWS_PROD_ACCESS_KEY_ID,AWS_PROD_SECRET_ACCESS_KEY';

ADD IF NOT EXISTS PROVIDER id='aws-staging'
  type='aws'
  region='us-east-1'
  auth='env:AWS_STAGING_ACCESS_KEY_ID,AWS_STAGING_SECRET_ACCESS_KEY';

ADD IF NOT EXISTS PROVIDER id='aws-dev'
  type='aws'
  region='us-west-2'
  auth='env:AWS_DEV_ACCESS_KEY_ID,AWS_DEV_SECRET_ACCESS_KEY';

ADD IF NOT EXISTS PROVIDER id='aws-data'
  type='aws'
  region='eu-west-1'
  auth='env:AWS_DATA_ACCESS_KEY_ID,AWS_DATA_SECRET_ACCESS_KEY';

ADD IF NOT EXISTS PROVIDER id='aws-security'
  type='aws'
  region='us-east-1'
  auth='env:AWS_SEC_ACCESS_KEY_ID,AWS_SEC_SECRET_ACCESS_KEY';

-- ── Import everything ────────────────────────────────────────────

IMPORT RESOURCES FROM ALL PROVIDERS;

-- ── Cross-account queries ────────────────────────────────────────

-- All RDS instances across every account
SELECT id, provider_id, status FROM resources
  WHERE resource_type = 'rds_postgres'
  ORDER BY provider_id;

-- All VPCs
SELECT id, provider_id, status FROM resources
  WHERE resource_type = 'vpc';

-- Count resources per account
-- SELECT provider_id, count(*) AS total FROM resources
--   GROUP BY provider_id
--   ORDER BY total DESC;

-- Security groups with overly permissive rules
SELECT id, provider_id FROM resources
  WHERE resource_type = 'sg_rule';

SHOW PROVIDERS;
```

---

## 6. Manage GitHub repos

Create a repository, protect the default branch with a modern ruleset, add
CI/CD secrets, and set an Actions variable -- all in one file.

```sql
-- github-setup.kvmql
-- Usage: orbi exec github-setup.kvmql
-- Prerequisites: gh CLI authenticated (gh auth login)

ADD IF NOT EXISTS PROVIDER
  id   = 'github'
  type = 'github'
  auth = 'env:GITHUB_TOKEN';

-- ── Create the repository ────────────────────────────────────────

CREATE RESOURCE 'gh_repo' id = 'acme-corp/billing-service'
  visibility  = 'private'
  description = 'Billing microservice managed by Orbi'
  ON PROVIDER 'github';

-- ── Branch protection (rulesets API) ─────────────────────────────

CREATE RESOURCE 'gh_ruleset' id = 'main-protection'
  repo               = 'acme-corp/billing-service'
  target             = 'branch'
  enforcement        = 'active'
  branches           = '~DEFAULT_BRANCH'
  require_pr         = true
  required_approvals = 2
  linear_history     = true
  ON PROVIDER 'github';

-- ── Secrets (values from external backends) ──────────────────────

CREATE RESOURCE 'gh_secret' id = 'DATABASE_URL'
  repo  = 'acme-corp/billing-service'
  value = 'vault:secret/billing#database_url'
  ON PROVIDER 'github';

CREATE RESOURCE 'gh_secret' id = 'STRIPE_SECRET_KEY'
  repo  = 'acme-corp/billing-service'
  value = 'op:Payments/Stripe#live-secret-key'
  ON PROVIDER 'github';

CREATE RESOURCE 'gh_secret' id = 'DEPLOY_SSH_KEY'
  repo  = 'acme-corp/billing-service'
  value = 'file:~/.ssh/deploy_ed25519'
  ON PROVIDER 'github';

-- ── Variables ────────────────────────────────────────────────────

CREATE RESOURCE 'gh_variable' id = 'DEPLOY_ENV'
  repo  = 'acme-corp/billing-service'
  value = 'production'
  ON PROVIDER 'github';

CREATE RESOURCE 'gh_variable' id = 'NOTIFY_CHANNEL'
  repo  = 'acme-corp/billing-service'
  value = '#billing-deploys'
  ON PROVIDER 'github';

-- ── Workflow file ────────────────────────────────────────────────

CREATE RESOURCE 'gh_workflow_file' id = 'ci.yml'
  repo    = 'acme-corp/billing-service'
  content = 'file:./workflows/ci.yml'
  message = 'Add CI workflow'
  ON PROVIDER 'github';

-- ── Verify ───────────────────────────────────────────────────────

SELECT id, resource_type, status FROM resources
  WHERE resource_type LIKE 'gh_%'
  ORDER BY resource_type;
```

---

## 7. Kubernetes resources

Declare a namespace, deployment, and service, then query live cluster state.

```sql
-- k8s-deploy.kvmql
-- Usage: orbi exec k8s-deploy.kvmql
-- Prerequisites: kubectl configured with a valid context

ADD IF NOT EXISTS PROVIDER
  id     = 'k8s'
  type   = 'kubernetes'
  auth   = 'env:KUBECONTEXT'
  region = 'prod-cluster';

-- ── Namespace ────────────────────────────────────────────────────

CREATE RESOURCE 'k8s_namespace' id = 'billing'
  ON PROVIDER 'k8s';

-- ── ConfigMap and Secret ─────────────────────────────────────────

CREATE RESOURCE 'k8s_configmap' id = 'billing-config'
  namespace = 'billing'
  data      = 'LOG_LEVEL=info,APP_PORT=8080'
  ON PROVIDER 'k8s';

CREATE RESOURCE 'k8s_secret' id = 'billing-secrets'
  namespace = 'billing'
  data      = 'DATABASE_URL=vault:secret/billing#url,API_KEY=op:Payments/Stripe#key'
  ON PROVIDER 'k8s';

-- ── Deployment ───────────────────────────────────────────────────

CREATE RESOURCE 'k8s_deployment' id = 'billing-api'
  namespace = 'billing'
  image     = 'registry.example.com/billing-api:v1.4.2'
  replicas  = 3
  port      = 8080
  ON PROVIDER 'k8s';

-- ── Service ──────────────────────────────────────────────────────

CREATE RESOURCE 'k8s_service' id = 'billing-svc'
  namespace   = 'billing'
  type        = 'ClusterIP'
  port        = 80
  target_port = 8080
  ON PROVIDER 'k8s';

-- ── Ingress ──────────────────────────────────────────────────────

CREATE RESOURCE 'k8s_ingress' id = 'billing-ing'
  namespace     = 'billing'
  host          = 'billing.example.com'
  service       = 'billing-svc'
  port          = 80
  tls_secret    = 'billing-tls'
  ingress_class = 'nginx'
  ON PROVIDER 'k8s';

-- ── Live cluster queries ─────────────────────────────────────────

-- All pods in the billing namespace
SELECT name, status, restarts FROM k8s_pods
  WHERE namespace = 'billing';

-- Deployments with unhealthy replicas
SELECT name, replicas, ready_replicas FROM k8s_deployments
  WHERE namespace = 'billing' AND ready_replicas < replicas;

-- Services and their cluster IPs
SELECT name, type, cluster_ip FROM k8s_services
  WHERE namespace = 'billing';

-- Cluster-wide: nodes that are not Ready
SELECT name, ready FROM k8s_nodes WHERE ready = false;

-- All pods in CrashLoopBackOff across the cluster
SELECT name, namespace, status, restarts FROM k8s_pods
  WHERE status = 'CrashLoopBackOff';
```
