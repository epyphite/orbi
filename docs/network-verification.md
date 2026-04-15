# Network Verification -- Table-Valued Functions and ASSERT

Orbi provides table-valued functions for network probing, DNS resolution, TLS
inspection, and host-level queries. Combined with ASSERT, these let you declare
infrastructure and verify it works in the same `.kvmql` file. When any assertion
fails, the script exits non-zero with the failure message.

---

## The deploy-and-verify pattern

The core idea: a single `.kvmql` file both provisions resources and asserts that
they are functioning correctly. Infrastructure creation and validation live
together, so drift is caught at deploy time rather than in a separate monitoring
system.

```sql
-- 1. Declare the DNS record
CREATE RESOURCE 'cf_dns_record' id = 'api'
  zone    = 'example.com'
  type    = 'A'
  content = '203.0.113.42'
  proxied = true
  ON PROVIDER 'cloudflare';

-- 2. Assert it propagated
ASSERT EXISTS (
  SELECT 1 FROM dns_lookup('api.example.com', 'A')
  WHERE content = '203.0.113.42'
), 'DNS record did not propagate';

-- 3. Assert the service is healthy
ASSERT (
  SELECT status_code FROM http_probe('https://api.example.com/health')
) = 200, 'API health check failed';
```

Every table-valued function is callable in a `SELECT ... FROM <fn>(...)` clause,
the same way PostgreSQL exposes `generate_series()` or `unnest()`.

---

## Table-valued functions

### dns_lookup(name, [type])

Resolves DNS records for a given hostname. The optional second argument filters
by record type (A, AAAA, CNAME, MX, TXT, etc.). When omitted, all record types
are returned.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `name` | string | Queried hostname |
| `type` | string | Record type (A, AAAA, CNAME, ...) |
| `content` | string | Record value (IP address, hostname, text) |
| `ttl` | integer | Time to live in seconds |

**Examples:**

```sql
-- All records for a hostname
SELECT * FROM dns_lookup('example.com');

-- Filter to A records only
SELECT content FROM dns_lookup('example.com', 'A');

-- Check MX records
SELECT content, ttl FROM dns_lookup('example.com', 'MX');
```

---

### reverse_dns(ip)

Performs a reverse DNS lookup (PTR record) for an IP address.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ip` | string | Queried IP address |
| `hostname` | string | Resolved hostname (if any) |
| `status` | string | `resolved` or `not_found` |

**Example:**

```sql
SELECT * FROM reverse_dns('203.0.113.42');
```

---

### tcp_probe(host, port, [timeout_ms])

Tests TCP connectivity to a host and port. The optional third argument sets a
timeout in milliseconds (default: 3000).

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `host` | string | Target hostname or IP |
| `port` | integer | Target port |
| `status` | string | `open`, `closed`, or `timeout` |
| `latency_ms` | float | Connection time in milliseconds |

**Examples:**

```sql
-- Default timeout
SELECT * FROM tcp_probe('example.com', 443);

-- Custom timeout (5 seconds)
SELECT status, latency_ms FROM tcp_probe('example.com', 443, 5000);

-- Assert a port is reachable
ASSERT EXISTS (
  SELECT 1 FROM tcp_probe('example.com', 443)
  WHERE status = 'open'
), 'port 443 not reachable';
```

---

### http_probe(url)

Issues an HTTP GET request to a URL and returns response metadata.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `url` | string | Requested URL |
| `status_code` | integer | HTTP status code |
| `content_type` | string | Content-Type header value |
| `headers` | string | Response headers (JSON) |
| `latency_ms` | float | Round-trip time in milliseconds |

**Examples:**

```sql
-- Inspect a health endpoint
SELECT status_code, latency_ms FROM http_probe('https://example.com/health');

-- Assert a 200 response
ASSERT (
  SELECT status_code FROM http_probe('https://example.com/health')
) = 200, 'health endpoint not returning 200';
```

---

### tls_cert(host, port)

Connects to a TLS endpoint and returns certificate details.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `host` | string | Target hostname |
| `port` | integer | Target port |
| `subject` | string | Certificate subject (CN) |
| `issuer` | string | Certificate issuer |
| `not_before` | string | Validity start (ISO 8601) |
| `not_after` | string | Validity end (ISO 8601) |
| `days_remaining` | integer | Days until expiration |

**Examples:**

```sql
-- Inspect certificate
SELECT subject, issuer, not_after, days_remaining
  FROM tls_cert('example.com', 443);

-- Assert at least 30 days remaining
ASSERT EXISTS (
  SELECT 1 FROM tls_cert('example.com', 443)
  WHERE days_remaining > 30
), 'TLS certificate expires within 30 days';
```

---

### file_stat(provider_id, path) / file_stat(path)

Inspects a file on a remote host via the SSH provider. Two signatures:

- `file_stat(provider_id, path)` -- query a single host.
- `file_stat(path)` -- fan out across every registered SSH provider.

**Return columns:**

| Column | Type | Description |
|--------|------|-------------|
| `provider_id` | string | SSH provider that was queried |
| `host` | string | Hostname of the provider |
| `path` | string | Absolute file path |
| `present` | boolean | Whether the file exists |
| `mode` | string | Unix permission mode (e.g. `0644`) |
| `owner` | string | File owner |
| `group` | string | File group |
| `size` | integer | File size in bytes |

**Examples:**

```sql
-- Single host
SELECT * FROM file_stat('web-ssh', '/etc/nginx/nginx.conf');

-- Check permissions
ASSERT EXISTS (
  SELECT 1 FROM file_stat('web-ssh', '/opt/app/.env')
  WHERE present = true AND mode = '0600'
), '.env missing or has wrong permissions';

-- Fan out: find hosts where the file has drifted
SELECT provider_id, host, mode, owner FROM file_stat('/opt/app/.env')
  WHERE present = true AND mode != '0600';

-- Fleet-wide presence check
ASSERT (
  SELECT count(*) FROM file_stat('/opt/app/.env')
  WHERE present = true
) >= 1, '.env not present on any host';
```

---

## Host-aware query functions

These functions query the state of services and containers on a remote host.
Each accepts an optional `provider_id` argument. When omitted, the function fans
out across all SSH providers.

### systemd_services([provider_id])

Lists systemd units on a host.

**Return columns:** `provider_id`, `unit`, `load_state`, `active_state`, `sub_state`, `description`

```sql
-- All failed services on a specific host
SELECT unit, active_state FROM systemd_services('web-ssh')
  WHERE active_state = 'failed';

-- Find inactive timers across the fleet
SELECT provider_id, unit FROM systemd_services()
  WHERE unit LIKE '%.timer' AND active_state != 'active';
```

### nginx_vhosts([provider_id])

Lists nginx virtual hosts (parsed from sites-enabled).

**Return columns:** `provider_id`, `server_name`, `listen`, `root`, `upstream`

```sql
SELECT server_name, upstream FROM nginx_vhosts('web-ssh');
```

### nginx_config_test([provider_id])

Runs `nginx -t` and returns the result.

**Return columns:** `provider_id`, `valid`, `errors`

```sql
-- Assert nginx config is valid before reloading
ASSERT (
  SELECT valid FROM nginx_config_test('web-ssh')
) = true, 'nginx config has errors';
```

### docker_containers([provider_id])

Lists Docker containers on a host.

**Return columns:** `provider_id`, `container_id`, `name`, `image`, `status`, `ports`

```sql
-- All containers on a host
SELECT name, image, status FROM docker_containers('web-ssh');

-- Find stopped containers across the fleet
SELECT provider_id, name, status FROM docker_containers()
  WHERE status != 'running';
```

---

## ASSERT syntax

ASSERT evaluates a condition and fails the script if the condition is false.
Two forms are supported:

```
ASSERT <condition>;
ASSERT <condition>, '<message>';
```

The condition can be a comparison, an EXISTS subquery, or a scalar subquery
comparison. When the optional message is provided, it is included in the error
output on failure.

---

### EXISTS subqueries

Pass if the inner SELECT returns at least one row.

```sql
-- DNS record exists
ASSERT EXISTS (
  SELECT 1 FROM dns_lookup('example.com', 'A')
), 'no A record found for example.com';

-- Port is open
ASSERT EXISTS (
  SELECT 1 FROM tcp_probe('example.com', 443)
  WHERE status = 'open'
), 'HTTPS port not reachable';

-- File exists with correct permissions
ASSERT EXISTS (
  SELECT 1 FROM file_stat('web-ssh', '/etc/nginx/nginx.conf')
  WHERE present = true AND owner = 'root'
), 'nginx.conf missing or not owned by root';
```

---

### Scalar subquery comparisons

Extract a single value from a subquery and compare it to a literal or variable.

```sql
-- HTTP status code
ASSERT (
  SELECT status_code FROM http_probe('https://example.com/health')
) = 200, 'health check failed';

-- DNS resolves to expected IP
ASSERT (
  SELECT content FROM dns_lookup('example.com', 'A')
) = '203.0.113.42', 'A record does not match expected IP';

-- File ownership
ASSERT (
  SELECT owner FROM file_stat('web-ssh', '/etc/nginx/nginx.conf')
) = 'root', 'nginx.conf not owned by root';

-- nginx config validity
ASSERT (
  SELECT valid FROM nginx_config_test('web-ssh')
) = true, 'nginx configuration test failed';
```

---

### Aggregate assertions

Use aggregate functions in a scalar subquery to assert counts, minimums, or
other computed values.

```sql
-- At least one DNS record exists
ASSERT (
  SELECT count(*) FROM dns_lookup('example.com')
) >= 1, 'no DNS records found';

-- All SSH hosts have the config file
ASSERT (
  SELECT count(*) FROM file_stat('/opt/app/.env')
  WHERE present = true
) >= 3, 'expected .env on at least 3 hosts';
```

---

## Variables

Use `SET @name = value;` to define session variables. Variables can be
referenced in function arguments and comparisons with `@name` syntax.

```sql
SET @host = 'api.example.com';
SET @expected_ip = '203.0.113.42';

ASSERT (
  SELECT content FROM dns_lookup(@host, 'A')
) = @expected_ip, 'DNS mismatch';

ASSERT (
  SELECT status_code FROM http_probe('https://' || @host || '/health')
) = 200, 'health check failed';
```

---

## String concatenation

The `||` operator concatenates strings. This is useful for building URLs or
hostnames dynamically from variables.

```sql
SET @domain = 'example.com';
SET @subdomain = 'api';

-- Build a URL from parts
SELECT * FROM http_probe('https://' || @subdomain || '.' || @domain || '/status');

-- Build a hostname
SELECT * FROM dns_lookup(@subdomain || '.' || @domain, 'A');
```

---

## AS aliases

Use `AS` to name columns in SELECT output. This is particularly useful with
aggregate functions and when piping output to other tools.

```sql
SELECT count(*) AS total FROM dns_lookup('example.com');

SELECT provider_id, count(*) AS host_count
  FROM file_stat('/opt/app/.env')
  WHERE present = true;
```

---

## Complete example: deploy and verify

A full deploy-and-verify workflow in a single file. The infrastructure section
provisions resources; the verification section asserts they are working.

```sql
-- full-verify.kvmql
-- Usage: orbi exec full-verify.kvmql

SET @host = 'app.example.com';
SET @ip   = '203.0.113.42';

-- ── Providers ────────────────────────────────────────────────────

ADD IF NOT EXISTS PROVIDER id='web-ssh'
  type='ssh'
  host=@host
  auth='op:Infrastructure/web-ssh-key'
  labels='{"ssh_user":"deploy"}';

ADD IF NOT EXISTS PROVIDER id='cf'
  type='cloudflare'
  auth='env:CLOUDFLARE_API_TOKEN';

-- ── Deploy ───────────────────────────────────────────────────────

CREATE RESOURCE 'cf_dns_record' id='app'
  zone='example.com'
  type='A'
  content=@ip
  proxied=true
  ON PROVIDER 'cf';

CREATE RESOURCE 'file' id='/opt/app/.env'
  content='op:Infrastructure/app-prod-env'
  owner='deploy' group='deploy' mode='0600'
  ON PROVIDER 'web-ssh';

CREATE RESOURCE 'file' id='/etc/systemd/system/app.service'
  content='file:./units/app.service'
  mode='0644'
  ON PROVIDER 'web-ssh';

CREATE RESOURCE 'systemd_service' id='app'
  enabled=true started=true
  after_file='/etc/systemd/system/app.service'
  ON PROVIDER 'web-ssh';

CREATE RESOURCE 'nginx_proxy' id=@host
  server_name=@host
  upstream='http://127.0.0.1:3000'
  tls=true
  tls_cert_from='letsencrypt:app.example.com'
  ON PROVIDER 'web-ssh';

-- ── Verify: DNS ──────────────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM dns_lookup(@host, 'A')
  WHERE content = @ip
), 'A record does not point to expected IP';

ASSERT (
  SELECT count(*) FROM dns_lookup(@host)
) >= 1, 'no DNS records found for host';

-- ── Verify: connectivity ─────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 443) WHERE status = 'open'
), 'port 443 not reachable';

ASSERT EXISTS (
  SELECT 1 FROM tcp_probe(@host, 80) WHERE status = 'open'
), 'port 80 not reachable';

-- ── Verify: HTTP ─────────────────────────────────────────────────

ASSERT (
  SELECT status_code FROM http_probe('https://' || @host || '/health')
) = 200, 'health endpoint returned non-200';

-- ── Verify: TLS ──────────────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM tls_cert(@host, 443)
  WHERE days_remaining > 14
), 'TLS certificate expires within 14 days';

SELECT subject, issuer, not_after, days_remaining AS days_left
  FROM tls_cert(@host, 443);

-- ── Verify: host state ───────────────────────────────────────────

ASSERT EXISTS (
  SELECT 1 FROM file_stat('web-ssh', '/opt/app/.env')
  WHERE present = true AND mode = '0600'
), '.env missing or has wrong permissions';

ASSERT (
  SELECT valid FROM nginx_config_test('web-ssh')
) = true, 'nginx config invalid';

-- Show deployed state
SELECT id, resource_type, status FROM resources ORDER BY resource_type;
```
