# KVMQL User Manual

**Version 0.2.0**

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation & Setup](#2-installation--setup)
3. [Quick Start](#3-quick-start)
4. [CLI Reference](#4-cli-reference)
5. [DSL Reference -- VM Lifecycle](#5-dsl-reference----vm-lifecycle)
6. [DSL Reference -- Volumes](#6-dsl-reference----volumes)
7. [DSL Reference -- Images](#7-dsl-reference----images)
8. [DSL Reference -- Managed Resources](#8-dsl-reference----managed-resources)
9. [DSL Reference -- Day-2 Operations](#9-dsl-reference----day-2-operations)
   - [EXPLAIN](#explain)
   - [ROLLBACK](#rollback)
   - [Plan Management](#9a-plan-management)
   - [Environment Management](#9b-environment-management)
10. [DSL Reference -- Infrastructure](#10-dsl-reference----infrastructure)
11. [DSL Reference -- Queries](#11-dsl-reference----queries)
12. [DSL Reference -- Access Control](#12-dsl-reference----access-control)
13. [DSL Reference -- Variables & Config](#13-dsl-reference----variables--config)
14. [Shell Reference](#14-shell-reference)
15. [Security](#15-security)
16. [Agent & Server](#16-agent--server)
17. [Error & Notification Reference](#17-error--notification-reference)

---

## 1. Introduction

### What KVMQL Is

KVMQL is a declarative, SQL-like DSL for managing virtual machines, OS images, block volumes, and cloud-managed resources across heterogeneous infrastructure. It provides:

- A unified language for both queries (SELECT, SHOW, WATCH) and mutations (CREATE, DESTROY, ALTER, etc.) across microVMs, images, volumes, and managed cloud resources.
- A driver model abstracting KVM/Firecracker and cloud providers behind a single interface.
- An embedded SQLite registry as the single source of truth for topology, images, volumes, state, query history, audit log, and access control.
- Cloud resource provisioning via `az` CLI (16 Azure types) and `aws` CLI (5 AWS types) with graceful fallback to registry-only when CLIs are unavailable.
- A thin agent per host that executes local operations and syncs state to the control plane.
- A capability model that reports provider-specific limitations explicitly.
- A psql-style interactive shell for human operators.

### What KVMQL Is Not

- **Not a hypervisor.** KVMQL manages VMs; KVM and Firecracker are the hypervisor layer underneath.
- **Not a container orchestrator.** MicroVMs are an alternative isolation primitive to containers.
- **Not Terraform.** KVMQL manages VM, image, volume, and managed resource lifecycle with a live, always-queryable registry. There is no separate state file.
- **Not HA by default.** Single control plane. HA is a documented upgrade path.

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Provider** | A registered backend (KVM host, cloud region) that can run VMs and store volumes. |
| **Cluster** | A logical group of providers for multi-host targeting. |
| **MicroVM** | A lightweight virtual machine managed by a provider's driver. |
| **Volume** | A block storage device that can be attached to a microVM. |
| **Image** | A bootable OS image (rootfs, disk, or cloud image). |
| **Resource** | A managed cloud resource (database, cache, Kubernetes cluster, etc.). |
| **Principal** | An identity (user or service account) for access control. |
| **Grant** | A permission associating verbs with a scope for a principal. |
| **Registry** | The embedded SQLite database storing all KVMQL state. |
| **Driver** | A plugin that translates KVMQL operations to provider-specific API calls. |

### Language Basics

All keywords are **case-insensitive**. String literals use single quotes (`'...'`). Escaped quotes use `''`. Statements are terminated with `;`. Comments use `--`.

```
-- This is a comment
SELECT * FROM microvms WHERE status = 'running';
```

Multiple statements can be batched in a single execution:

```
SELECT * FROM microvms;
DESTROY MICROVM 'vm-abc';
SHOW VERSION;
```

---

## 2. Installation & Setup

### Building from Source

```bash
cargo build --release
```

The binary is built at `target/release/kvmql`.

### Initializing the Registry

```bash
orbi init
```

This creates the SQLite registry at `~/.kvmql/state.db` (default path). You can override the path:

```bash
orbi --registry /path/to/state.db init
```

### Directory Structure

```
~/.kvmql/
  state.db          # SQLite registry (topology, state, history, audit)
```

---

## 3. Quick Start

This walkthrough uses actual KVMQL syntax verified against the parser.

```sql
-- 1. Register a provider
ADD PROVIDER
  id     = 'kvm.host-a'
  type   = 'kvm'
  driver = 'firecracker'
  host   = '192.168.1.10'
  auth   = 'file:/etc/kvmql/creds/host-a.env';

-- 2. Import an image
IMPORT IMAGE
  id     = 'ubuntu-22.04-lts'
  source = 'catalog:ubuntu-22.04-lts';

-- 3. Create a volume
CREATE VOLUME
  id      = 'vol-acme-data'
  size_gb = 20
  type    = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';

-- 4. Create a microVM with inline volumes
CREATE MICROVM
  tenant    = 'acme'
  vcpus     = 2
  memory_mb = 1024
  image     = 'ubuntu-22.04-lts'
  hostname  = 'acme-web-01'
  VOLUME (size_gb = 10 type = 'virtio-blk')
  VOLUME (id = 'vol-acme-data')
  ON PROVIDER 'kvm.host-a';

-- 5. Query your VMs
SELECT id, provider_id, tenant, status, vcpus, memory_mb
FROM microvms
WHERE tenant = 'acme';

-- 6. Attach a new volume
CREATE VOLUME id = 'vol-logs' size_gb = 50 type = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';
ATTACH VOLUME 'vol-logs' TO MICROVM 'vm-abc' AS '/dev/vdc';

-- 7. Take a snapshot
SNAPSHOT MICROVM 'vm-abc'
  INTO 's3://snapshots/vm-abc-20260318'
  TAG 'pre-upgrade';

-- 8. Clean up
DETACH VOLUME 'vol-logs' FROM MICROVM 'vm-abc';
DESTROY MICROVM 'vm-abc';
DESTROY VOLUME 'vol-acme-data' FORCE;

-- 9. Create a managed database
CREATE RESOURCE 'postgres'
  id = 'acme-db' version = '16' sku = 'Standard_B1ms' storage_gb = 128
  ON PROVIDER 'kvm.host-a';
```

---

## 4. CLI Reference

### Usage

```
kvmql [OPTIONS] [STATEMENT] [COMMAND]
```

### Positional Argument

- `STATEMENT` -- A DSL statement to execute directly. If omitted and no subcommand is given, the interactive shell launches.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--format <FORMAT>` | `table` | Output format: `table`, `json`, `csv`, `raw` |
| `--registry <PATH>` | `~/.kvmql/state.db` | Path to the SQLite registry file |
| `--dry-run` | off | Show what would happen without executing |
| `--simulate` | off | Run in simulation mode -- no cloud calls, realistic fake responses |
| `--env <NAME>` | none | Use a named environment (shortcut for `--registry ~/.kvmql/envs/<name>.db`) |

### Subcommands

| Command | Description |
|---------|-------------|
| `exec <FILE>` | Execute a `.kvmql` DSL file |
| `shell` | Launch the interactive shell |
| `version` | Print version (`orbi v0.2.0`) |
| `init` | Initialize a new registry at the configured path |
| `plan <SOURCE>` | Generate an execution plan from a file or statement |
| `apply <TARGET>` | Apply a plan (from registry ID or file) |
| `approve <ID>` | Approve a pending plan |
| `plans` | List plans (optional: `--status pending\|approved\|applied\|failed`) |
| `env create <NAME>` | Create a new environment |
| `env list` | List all environments |
| `env current` | Show current environment |
| `env copy <FROM> <TO>` | Copy an environment |
| `env export [NAME]` | Export environment as JSON |
| `env import <FILE> <NAME>` | Import environment from JSON |

### Examples

```bash
# Direct statement execution
orbi "SELECT * FROM microvms;"

# Execute with JSON output
orbi --format json "SHOW PROVIDERS;"

# Execute a file
orbi exec deploy.kvmql

# Launch shell
orbi shell

# Initialize registry at custom path
orbi --registry /data/kvmql.db init

# Dry-run (show what would happen)
orbi --dry-run "CREATE RESOURCE 'postgres' id = 'test' version = '16';"

# Simulation mode (no credentials needed)
orbi --simulate exec examples/demo.kvmql

# Named environment
orbi --env staging "SELECT * FROM resources;"

# Plan workflow
orbi plan examples/azure-stack.kvmql --name "staging deploy"
orbi plans --status pending
orbi approve <plan-id>
orbi apply <plan-id>

# Environment management
orbi env create production
orbi env list
orbi env copy staging production
orbi env export staging > snapshot.json
orbi env import snapshot.json dr-recovery
```

### Output Formats

- **table** -- ASCII table with column headers (default).
- **json** -- Pretty-printed JSON of the full `ResultEnvelope`.
- **csv** -- Comma-separated values with header row.
- **raw** -- Raw JSON value without envelope metadata.

### Exit Codes

- `0` -- Success.
- `1` -- Error (parse failure, execution error, file not found).

---

## 5. DSL Reference -- VM Lifecycle

### CREATE MICROVM

Creates a new microVM on a provider.

**Syntax:**

```
CREATE MICROVM
  <param> = <value> ...
  [VOLUME (<param> = <value> ...)] ...
  [ON PROVIDER '<provider_id>']
  [PLACEMENT POLICY = '<policy>']
  [REQUIRE <clause>, ...]
;
```

**Parameters (read by executor):**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | No | auto-generated UUID | VM identifier |
| `tenant` | string | No | `'default'` | Tenant/owner |
| `vcpus` | integer | No | `1` | Virtual CPU count |
| `memory_mb` | integer | No | `512` | Memory in megabytes |
| `image` | string | No | `'default'` | Image ID to boot from |
| `hostname` | string | No | null | Hostname |
| `network` | string | No | null | Network configuration |
| `metadata` | map | No | null | Key-value metadata (JSON) |
| `labels` | map | No | null | Key-value labels (JSON) |

**Inline volumes:**

Each `VOLUME (...)` block creates or references a volume to attach. The parenthesized block accepts the same params as `CREATE VOLUME`.

**REQUIRE clauses:**

```
REQUIRE capability = 'hotplug_volume'
REQUIRE provider = 'kvm.host-a'
REQUIRE label zone = 'us-east-1a'
```

Multiple REQUIRE clauses can be comma-separated after a single `REQUIRE` keyword.

**Example:**

```sql
CREATE MICROVM
  tenant = 'acme' vcpus = 2 memory_mb = 1024 image = 'ubuntu-22.04-lts'
  hostname = 'acme-web-01'
  metadata = { region: 'sg', tier: 'compute' }
  labels = { env: 'prod' }
  VOLUME (size_gb = 10 type = 'virtio-blk')
  VOLUME (id = 'vol-acme-data')
  ON PROVIDER 'kvm.host-a'
  PLACEMENT POLICY = 'least_loaded'
  REQUIRE capability = 'hotplug_volume';
```

**Returns:** JSON object with fields from the driver: `id`, `status`, `tenant`, `vcpus`, `memory_mb`, `image_id`, `hostname`.

### DESTROY MICROVM

```
DESTROY MICROVM '<id>' [FORCE];
```

Looks up the VM in the registry to find its provider, calls the driver's destroy method, then removes the registry entry.

**Example:**

```sql
DESTROY MICROVM 'vm-abc';
DESTROY MICROVM 'vm-abc' FORCE;
```

### ALTER MICROVM

Modifies a running microVM's parameters.

```
ALTER MICROVM '<id>' SET <key> = <value> [, <key> = <value> ...];
```

Sends all SET key-value pairs as a JSON object to the driver's `alter` method. Common keys: `vcpus`, `memory_mb`.

**Example:**

```sql
ALTER MICROVM 'vm-abc' SET memory_mb = 2048, vcpus = 4;
ALTER MICROVM 'vm-abc' SET metadata = null;
ALTER MICROVM 'vm-abc' SET labels = {};
```

**Returns:** Updated VM object from the driver.

### PAUSE MICROVM

```
PAUSE MICROVM '<id>';
```

Suspends a running microVM. Updates registry status to `paused`.

### RESUME MICROVM

```
RESUME MICROVM '<id>';
```

Wakes a paused microVM. Updates registry status to `running`.

### SNAPSHOT MICROVM

Captures microVM state to a snapshot destination.

```
SNAPSHOT MICROVM '<id>' INTO '<destination>' [TAG '<tag>'];
```

**Example:**

```sql
SNAPSHOT MICROVM 'vm-abc' INTO 's3://snapshots/vm-abc' TAG 'pre-upgrade';
```

**Returns:** Snapshot object from the driver with `id`, `microvm_id`, `destination`, `tag`, `size_mb`, `taken_at`.

### RESTORE MICROVM

Restores a microVM from a snapshot.

```
RESTORE MICROVM '<id>' FROM '<source>';
```

**Example:**

```sql
RESTORE MICROVM 'vm-abc' FROM 's3://snapshots/vm-abc';
```

**Returns:** Restored VM object registered in the registry.

---

## 6. DSL Reference -- Volumes

### CREATE VOLUME

```
CREATE VOLUME
  <param> = <value> ...
  [ON PROVIDER '<provider_id>']
;
```

**Parameters (read by executor):**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | No | auto-generated | Volume identifier |
| `size_gb` | integer | No | `10` | Size in gigabytes |
| `type` | string | No | `'virtio-blk'` | Volume type |
| `encrypted` | boolean | No | `false` | Enable encryption |
| `iops` | integer | No | null | Provisioned IOPS |
| `labels` | map | No | null | Key-value labels (JSON) |

**Example:**

```sql
CREATE VOLUME id = 'vol-acme-data' size_gb = 20 type = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';
```

**Returns:** Volume object with `id`, `size_gb`, `status`, `vol_type`, `encrypted`, `iops`.

### DESTROY VOLUME

```
DESTROY VOLUME '<id>' [FORCE];
```

**Example:**

```sql
DESTROY VOLUME 'vol-001' FORCE;
```

### ATTACH VOLUME

Attaches a volume to a microVM.

```
ATTACH VOLUME '<volume_id>' TO MICROVM '<microvm_id>' [AS '<device_name>'];
```

If `AS` is omitted, the default device name is `/dev/vdb`.

**Example:**

```sql
ATTACH VOLUME 'vol-acme-logs' TO MICROVM 'vm-abc' AS '/dev/vdc';
```

### DETACH VOLUME

```
DETACH VOLUME '<volume_id>' FROM MICROVM '<microvm_id>';
```

**Example:**

```sql
DETACH VOLUME 'vol-acme-logs' FROM MICROVM 'vm-abc';
```

### RESIZE VOLUME

```
RESIZE VOLUME '<volume_id>' TO <size> GB;
```

**Example:**

```sql
RESIZE VOLUME 'vol-acme-data' TO 40 GB;
```

**Returns:** Updated volume object from the driver.

### ALTER VOLUME

```
ALTER VOLUME '<id>' SET <key> = <value> [, <key> = <value> ...];
```

Currently, only `size_gb` triggers actual resize via the driver. Other SET items return `NYI_001` (not yet implemented).

**Example:**

```sql
ALTER VOLUME 'vol-001' SET size_gb = 100;
ALTER VOLUME 'vol-001' SET labels = { env: 'staging' };
```

---

## 7. DSL Reference -- Images

### IMPORT IMAGE

```
IMPORT IMAGE <param> = <value> ...;
```

**Parameters (read by executor):**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | No | auto-generated UUID | Image identifier |
| `name` | string | No | `'unnamed'` | Human-readable name |
| `os` | string | No | `'linux'` | Operating system |
| `distro` | string | No | `'unknown'` | Distribution |
| `version` | string | No | `'latest'` | OS version |
| `arch` | string | No | `'x86_64'` | Architecture |
| `type` | string | No | `'rootfs'` | Image type: `rootfs`, `disk`, `cloud` |
| `source` | string | No | `'local'` | Import source URI |
| `kernel` | string | No | null | Kernel path |
| `rootfs` | string | No | null | Root filesystem path |
| `checksum` | string | No | null | Checksum for verification |

**Example:**

```sql
IMPORT IMAGE id = 'ubuntu-22.04-lts' source = 'catalog:ubuntu-22.04-lts';

IMPORT IMAGE
  id = 'alpine-edge'
  name = 'Alpine Edge'
  os = 'linux'
  distro = 'alpine'
  version = 'edge'
  arch = 'x86_64'
  type = 'rootfs'
  source = '/images/alpine.ext4';
```

**Returns:** Image object from the driver.

### PUBLISH IMAGE

Publishes an image to a target provider. Currently, no driver trait method implements publish, so this will return an error even if the provider advertises the `ImagePublish` capability.

```
PUBLISH IMAGE '<image_id>' TO PROVIDER '<provider_id>';
```

**Example:**

```sql
PUBLISH IMAGE 'ubuntu-22.04-lts' TO PROVIDER 'aws.ap-southeast-1';
```

**Status:** Returns an error -- driver trait `publish_image` method not yet implemented.

### REMOVE IMAGE

```
REMOVE IMAGE '<image_id>' [FORCE];
```

**Example:**

```sql
REMOVE IMAGE 'ubuntu-22.04-lts';
REMOVE IMAGE 'ubuntu-22.04-lts' FORCE;
```

---

## 8. DSL Reference -- Managed Resources

Managed resources represent cloud-hosted services (databases, caches, Kubernetes clusters, networking, etc.) provisioned via the Azure CLI (`az`).

### How Provisioning Works

When you execute `CREATE RESOURCE`, the engine:

1. Resolves the target provider (from `ON PROVIDER` clause or first registered provider).
2. Builds the config JSON from all parameters.
3. Attempts real provisioning via `az` CLI commands.
4. If `az` succeeds: records the resource with status from Azure (typically `"created"`).
5. If `az` fails (not installed, auth error, etc.): records the resource with status `"pending"` and emits a `WARN` notification with code `AZ_PROVISION_FAILED`.

This means resources are always recorded in the registry, even when Azure provisioning fails. This is by design -- you can retry provisioning later or use the registry as a desired-state manifest.

### CREATE RESOURCE

```
CREATE RESOURCE '<type>'
  <param> = <value> ...
  [ON PROVIDER '<provider_id>']
;
```

**Returns:** JSON object with fields: `id`, `resource_type`, `provider_id`, `name`, `status`, `config`, `outputs`, `created_at`, `labels`.

### ALTER RESOURCE

```
ALTER RESOURCE '<type>' '<id>' SET <key> = <value> [, <key> = <value> ...];
```

Merges the SET items into the existing config. Attempts real update via `az` CLI. If the az update fails, the config is updated in the registry only, and a `WARN` notification with code `AZ_UPDATE_FAILED` is emitted.

**Example:**

```sql
ALTER RESOURCE 'postgres' 'acme-db' SET sku = 'Standard_B2s', storage_gb = 64;
```

**Returns:** Updated resource JSON with `id`, `resource_type`, `provider_id`, `name`, `status`, `config`, `outputs`, `updated_at`, `labels`.

**Note:** Only `postgres` update is currently wired to `az postgres flexible-server update`. Other resource types return `AZ_UPDATE_FAILED`.

### DESTROY RESOURCE

```
DESTROY RESOURCE '<type>' '<id>' [FORCE];
```

Attempts deletion via `az` CLI. If deletion fails, the resource is still removed from the registry and a `WARN` notification with code `AZ_DELETE_FAILED` is emitted.

**Example:**

```sql
DESTROY RESOURCE 'redis' 'cache1' FORCE;
DESTROY RESOURCE 'postgres' 'acme-db';
```

### All 25 Resource Types

Each resource type maps to specific provider APIs (Azure via `az`, AWS via `aws`, Cloudflare via REST). Below is every type with its required and optional parameters as defined in `resource_types.rs`.

---

#### `postgres` -- PostgreSQL Flexible Server

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `version` | Yes | -- | `--version` |
| `sku` | No | -- | `--sku-name` |
| `storage_gb` | No | -- | `--storage-size` |
| `backup_retention_days` | No | -- | `--backup-retention` |
| `geo_redundant_backup` | No | -- | (informational only) |
| `high_availability` | No | -- | (informational only) |

**Outputs on success:** `fqdn`, `host`, `state`, `version`

**Day-2 support:** BACKUP (automatic PITR -- returns advisory note), RESTORE (point-in-time restore), ALTER (update sku via `az postgres flexible-server update`).

```sql
CREATE RESOURCE 'postgres'
  id = 'acme-db' version = '16' sku = 'Standard_B1ms' storage_gb = 128
  backup_retention_days = 14
  ON PROVIDER 'azure.eastus';
```

---

#### `redis` -- Redis Cache

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `sku` | Yes | `'Standard'` | `--sku` |
| `capacity` | No | -- | `--vm-size` |
| `family` | No | -- | (informational only) |
| `enable_non_ssl_port` | No | -- | (informational only) |

**Outputs on success:** `host`, `port`, `ssl_port`

```sql
CREATE RESOURCE 'redis' id = 'cache1' sku = 'Standard' capacity = 2;
```

---

#### `aks` -- Azure Kubernetes Service

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `node_count` | Yes | `3` | `--node-count` |
| `vm_size` | No | -- | `--node-vm-size` |
| `kubernetes_version` | No | -- | `--kubernetes-version` |
| `network_plugin` | No | -- | (informational only) |
| `dns_prefix` | No | -- | (informational only) |

**Outputs on success:** `fqdn`, `kubernetes_version`, `node_count`

**Day-2 support:** SCALE (change `node_count` per nodepool), UPGRADE (change `kubernetes_version`).

```sql
CREATE RESOURCE 'aks' id = 'k8s-1' node_count = 3 vm_size = 'Standard_DS2_v2';
```

---

#### `storage_account` -- Storage Account

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `sku` | Yes | `'Standard_LRS'` | `--sku` |
| `kind` | No | -- | `--kind` |
| `access_tier` | No | -- | `--access-tier` |
| `enable_https_only` | No | -- | (informational only) |

**Outputs on success:** `primary_endpoints`

```sql
CREATE RESOURCE 'storage_account' id = 'acmeblobs' sku = 'Standard_LRS';
```

---

#### `vnet` -- Virtual Network

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `address_space` | Yes | `'10.0.0.0/16'` | `--address-prefix` |
| `subnets` | No | -- | (informational only) |
| `dns_servers` | No | -- | (informational only) |

```sql
CREATE RESOURCE 'vnet' id = 'acme-vnet' address_space = '10.0.0.0/16';
```

---

#### `nsg` -- Network Security Group

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `rules` | No | -- | (informational only) |

```sql
CREATE RESOURCE 'nsg' id = 'acme-nsg';
```

---

#### `container_registry` -- Container Registry (ACR)

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `sku` | Yes | `'Standard'` | `--sku` |
| `admin_enabled` | No | -- | `--admin-enabled` (flag, set if `true`) |
| `geo_replication` | No | -- | (informational only) |

**Outputs on success:** `login_server`

```sql
CREATE RESOURCE 'container_registry' id = 'acmeregistry' sku = 'Standard' admin_enabled = true;
```

---

#### `dns_zone` -- DNS Zone

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `records` | No | -- | (informational only) |

**Outputs on success:** `name_servers`

```sql
CREATE RESOURCE 'dns_zone' id = 'acme.com';
```

---

#### `container_app` -- Container App

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `image` | Yes | -- | `--image` |
| `cpu` | No | -- | `--cpu` |
| `memory` | No | -- | `--memory` |
| `min_replicas` | No | -- | `--min-replicas` |
| `max_replicas` | No | -- | `--max-replicas` |
| `env_vars` | No | -- | (informational only) |

**Outputs on success:** `fqdn`

**Day-2 support:** SCALE (change `min_replicas`, `max_replicas`).

```sql
CREATE RESOURCE 'container_app'
  id = 'api' image = 'acmeregistry.azurecr.io/api:latest'
  cpu = '0.5' memory = '1Gi' min_replicas = 1 max_replicas = 5;
```

---

#### `container_job` -- Container Job

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `image` | Yes | -- | `--image` |
| `cpu` | No | -- | `--cpu` |
| `memory` | No | -- | `--memory` |
| `trigger_type` | No | `'Manual'` | `--trigger-type` |
| `cron_expression` | No | -- | `--cron-expression` |

```sql
CREATE RESOURCE 'container_job'
  id = 'nightly-report' image = 'acmeregistry.azurecr.io/report:latest'
  trigger_type = 'Schedule' cron_expression = '0 2 * * *';
```

---

#### `load_balancer` -- Load Balancer

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `sku` | No | `'Standard'` | `--sku` |

```sql
CREATE RESOURCE 'load_balancer' id = 'acme-lb' sku = 'Standard';
```

---

#### `subnet` -- VNet Subnet (sub-resource)

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `vnet` | Yes | -- | `--vnet-name` |
| `address_prefix` | Yes | -- | `--address-prefixes` |
| `delegation` | No | -- | `--delegations` |
| `nsg` | No | -- | `--network-security-group` |
| `route_table` | No | -- | (informational only) |

**Note:** Deletion requires parent context (`vnet` param). Use `DESTROY RESOURCE` -- the engine handles this.

```sql
CREATE RESOURCE 'subnet'
  id = 'app' vnet = 'acme-vnet' address_prefix = '10.0.0.0/24'
  nsg = 'acme-nsg';
```

---

#### `nsg_rule` -- NSG Rule (sub-resource)

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `nsg` | Yes | -- | `--nsg-name` |
| `priority` | Yes | -- | `--priority` |
| `direction` | No | `'Inbound'` | `--direction` |
| `access` | No | `'Allow'` | `--access` |
| `protocol` | No | `'Tcp'` | `--protocol` |
| `source_address` | No | -- | `--source-address-prefixes` |
| `destination_port` | No | -- | `--destination-port-ranges` |
| `source_port` | No | `'*'` | `--source-port-ranges` |
| `destination_address` | No | `'*'` | `--destination-address-prefixes` |

```sql
CREATE RESOURCE 'nsg_rule'
  id = 'allow-ssh' nsg = 'acme-nsg' priority = 100
  direction = 'Inbound' access = 'Allow' protocol = 'Tcp'
  destination_port = '22' source_address = '10.0.0.0/8';
```

---

#### `vnet_peering` -- VNet Peering (sub-resource)

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `vnet` | Yes | -- | `--vnet-name` |
| `remote_vnet` | Yes | -- | `--remote-vnet` |
| `allow_forwarded_traffic` | No | -- | `--allow-forwarded-traffic` (flag if `true`) |
| `allow_gateway_transit` | No | -- | `--allow-gateway-transit` (flag if `true`) |
| `use_remote_gateways` | No | -- | (informational only) |

```sql
CREATE RESOURCE 'vnet_peering'
  id = 'hub-spoke' vnet = 'hub-vnet' remote_vnet = 'spoke-vnet'
  allow_forwarded_traffic = true;
```

---

#### `pg_database` -- PostgreSQL Database (on a Flexible Server)

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--database-name` |
| `server` | Yes | -- | `--server-name` |
| `charset` | No | -- | `--charset` |
| `collation` | No | -- | `--collation` |

```sql
CREATE RESOURCE 'pg_database' id = 'myapp' server = 'acme-db';
```

---

#### `dns_vnet_link` -- Private DNS Zone VNet Link

| Param | Required | Default | az flag |
|-------|----------|---------|---------|
| `id` | Yes | -- | `--name` |
| `zone_name` | Yes | -- | `--zone-name` |
| `vnet` | Yes | -- | `--virtual-network` |
| `registration_enabled` | No | -- | `--registration-enabled true` (if `true`) |

```sql
CREATE RESOURCE 'dns_vnet_link'
  id = 'link-hub' zone_name = 'privatelink.postgres.database.azure.com'
  vnet = 'hub-vnet' registration_enabled = true;
```

---

#### `rds_postgres` -- AWS RDS PostgreSQL

| Param | Required | Default | aws flag |
|-------|----------|---------|----------|
| `id` | Yes | -- | `--db-instance-identifier` |
| `instance_class` | Yes | -- | `--db-instance-class` |
| `engine_version` | Yes | -- | `--engine-version` |
| `master_username` | Yes | -- | `--master-username` |
| `master_password` | Yes | -- | `--master-user-password` |
| `storage_gb` | Yes | -- | `--allocated-storage` |
| `multi_az` | No | -- | `--multi-az` |
| `backup_retention` | No | -- | `--backup-retention-period` |
| `vpc_security_group_ids` | No | -- | `--vpc-security-group-ids` |
| `db_subnet_group` | No | -- | `--db-subnet-group-name` |

```sql
CREATE RESOURCE 'rds_postgres'
  id = 'prod-db' instance_class = 'db.t3.medium'
  engine_version = '16' master_username = 'admin'
  master_password = 'env:DB_PASSWORD' storage_gb = 100
  ON PROVIDER 'aws.us-east-1';
```

---

#### `vpc` -- AWS VPC

| Param | Required | Default | aws flag |
|-------|----------|---------|----------|
| `id` | Yes | -- | tag `Name` |
| `cidr_block` | Yes | -- | `--cidr-block` |
| `enable_dns_support` | No | -- | `--enable-dns-support` |
| `enable_dns_hostnames` | No | -- | `--enable-dns-hostnames` |

```sql
CREATE RESOURCE 'vpc' id = 'prod-vpc' cidr_block = '10.0.0.0/16'
  ON PROVIDER 'aws.us-east-1';
```

---

#### `aws_subnet` -- AWS VPC Subnet

| Param | Required | Default | aws flag |
|-------|----------|---------|----------|
| `id` | Yes | -- | tag `Name` |
| `vpc_id` | Yes | -- | `--vpc-id` |
| `cidr_block` | Yes | -- | `--cidr-block` |
| `availability_zone` | No | -- | `--availability-zone` |
| `map_public_ip` | No | -- | `--map-public-ip-on-launch` |

```sql
CREATE RESOURCE 'aws_subnet'
  id = 'app-subnet' vpc_id = 'prod-vpc' cidr_block = '10.0.1.0/24'
  ON PROVIDER 'aws.us-east-1';
```

---

#### `security_group` -- AWS Security Group

| Param | Required | Default | aws flag |
|-------|----------|---------|----------|
| `id` | Yes | -- | `--group-name` |
| `description` | Yes | -- | `--description` |
| `vpc_id` | Yes | -- | `--vpc-id` |

```sql
CREATE RESOURCE 'security_group'
  id = 'web-sg' description = 'Web tier SG' vpc_id = 'prod-vpc'
  ON PROVIDER 'aws.us-east-1';
```

---

#### `sg_rule` -- Security Group Rule

| Param | Required | Default | aws flag |
|-------|----------|---------|----------|
| `id` | Yes | -- | tag `Name` |
| `security_group_id` | Yes | -- | `--group-id` |
| `protocol` | Yes | -- | `--protocol` |
| `port` | Yes | -- | `--port` |
| `cidr` | Yes | -- | `--cidr` |
| `direction` | No | `'ingress'` | ingress/egress |
| `description` | No | -- | `--description` |

```sql
CREATE RESOURCE 'sg_rule'
  id = 'allow-https' security_group_id = 'web-sg'
  protocol = 'tcp' port = 443 cidr = '0.0.0.0/0';
```

#### Cloudflare Resource Types

Cloudflare resource types target the Cloudflare REST API (`https://api.cloudflare.com/client/v4`). Authentication is via an API token resolved through the provider's `auth` credential reference (e.g. `auth='env:CLOUDFLARE_API_TOKEN'`). Zone-name-to-ID lookup happens per-request; no cache yet.

#### `cf_zone` -- Cloudflare Zone (domain)

| Param | Required | Default | Cloudflare field |
|-------|----------|---------|------------------|
| `id` | Yes | -- | `name` (the domain) |
| `type` | No | `'full'` | `type` (`full` or `partial`) |
| `plan` | No | -- | informational |

```sql
CREATE RESOURCE 'cf_zone' id = 'example.com' ON PROVIDER 'cloudflare';
```

#### `cf_dns_record` -- Cloudflare DNS Record

| Param | Required | Default | Cloudflare field |
|-------|----------|---------|------------------|
| `id` | Yes | -- | `name` (subdomain or `@`) |
| `zone` | Yes | -- | resolved to `zone_id` |
| `content` | Yes | -- | `content` (IP, hostname, etc.) |
| `type` | No | `'A'` | `type` (A/AAAA/CNAME/MX/TXT/...) |
| `ttl` | No | `1` (auto) | `ttl` |
| `proxied` | No | `false` | `proxied` |
| `priority` | No | -- | `priority` (MX only) |

```sql
CREATE RESOURCE 'cf_dns_record' id = 'api'
  zone = 'example.com' type = 'A' content = '1.2.3.4' proxied = true
  ON PROVIDER 'cloudflare';
```

#### `cf_firewall_rule` -- Cloudflare Firewall Rule

| Param | Required | Default | Cloudflare field |
|-------|----------|---------|------------------|
| `id` | Yes | -- | local name (not sent) |
| `zone` | Yes | -- | resolved to `zone_id` |
| `expression` | Yes | -- | `filter.expression` |
| `action` | No | `'block'` | `action` |
| `description` | No | -- | `description` |

```sql
CREATE RESOURCE 'cf_firewall_rule' id = 'block-tor'
  zone = 'example.com'
  expression = '(ip.geoip.asnum eq 396507)'
  action = 'block' description = 'Block Tor exit nodes'
  ON PROVIDER 'cloudflare';
```

#### `cf_page_rule` -- Cloudflare Page Rule

| Param | Required | Default | Cloudflare field |
|-------|----------|---------|------------------|
| `id` | Yes | -- | local name (not sent) |
| `zone` | Yes | -- | resolved to `zone_id` |
| `url` | Yes | -- | `targets[0].constraint.value` |
| `priority` | No | `1` | `priority` |
| `cache_level` | No | -- | action `cache_level` |
| `ssl` | No | -- | action `ssl` |

```sql
CREATE RESOURCE 'cf_page_rule' id = 'cache-static'
  zone = 'example.com' url = '*example.com/static/*'
  cache_level = 'cache_everything'
  ON PROVIDER 'cloudflare';
```

---

## 9. DSL Reference -- Day-2 Operations

Day-2 operations (BACKUP, RESTORE RESOURCE, SCALE, UPGRADE) target existing managed resources. All follow the same pattern: verify the resource exists and its type matches, attempt the operation via `az` CLI, update the registry.

### BACKUP

```
BACKUP RESOURCE '<type>' '<id>' [INTO '<destination>'] [TAG '<tag>'];
```

**Supported types:** `postgres` (uses automatic PITR -- returns an advisory note about point-in-time recovery).

Unsupported types return an error.

**Example:**

```sql
BACKUP RESOURCE 'postgres' 'acme-db';
BACKUP RESOURCE 'postgres' 'acme-db' INTO 'archive' TAG 'pre-migration';
```

**Returns:** JSON with `id`, `resource_type`, `status`, `outputs`.

### RESTORE RESOURCE

```
RESTORE RESOURCE '<type>' '<id>' FROM '<source>';
```

For `postgres`, `<source>` is a point-in-time timestamp. Creates a restored server named `<id>-restored`.

**Supported types:** `postgres`

**Example:**

```sql
RESTORE RESOURCE 'postgres' 'acme-db' FROM '2026-03-18T10:00:00Z';
```

**Returns:** JSON with `id`, `resource_type`, `status`, `outputs`.

### SCALE

```
SCALE RESOURCE '<type>' '<id>' <param> = <value> ...;
```

**Supported types and params:**

- `aks` -- Requires `node_count`. Optional `nodepool` (default: `'nodepool1'`).
- `container_app` -- Optional `min_replicas`, `max_replicas`.

Updates the resource config in the registry to reflect the new params.

**Example:**

```sql
SCALE RESOURCE 'aks' 'k8s-1' node_count = 5;
SCALE RESOURCE 'aks' 'k8s-1' node_count = 10 nodepool = 'workload';
SCALE RESOURCE 'container_app' 'api' min_replicas = 2 max_replicas = 10;
```

**Returns:** JSON with `id`, `resource_type`, `status`, `config`, `outputs`.

### UPGRADE

```
UPGRADE RESOURCE '<type>' '<id>' <param> = <value> ...;
```

**Supported types and params:**

- `aks` -- Requires `kubernetes_version`.

**Example:**

```sql
UPGRADE RESOURCE 'aks' 'k8s-1' kubernetes_version = '1.29';
```

**Returns:** JSON with `id`, `resource_type`, `status`, `config`, `outputs`.

### EXPLAIN

Wraps any mutation statement to preview what would happen without executing it. Returns a plan showing the actions, parameters, and affected resources.

```
EXPLAIN <statement>;
```

**Example:**

```sql
EXPLAIN CREATE RESOURCE 'postgres' id = 'test-db' version = '16'
  sku = 'Standard_B2s' storage_gb = 64 ON PROVIDER 'azure';

EXPLAIN DESTROY MICROVM 'vm-abc';

EXPLAIN ALTER RESOURCE 'postgres' 'prod-db' SET sku = 'Standard_B4s';
```

**Returns:** JSON plan object with the operation details, target resource, and parameters that would be applied.

### ROLLBACK

Undo mutations using state snapshots. The engine records state snapshots before each mutation, enabling rollback.

```
ROLLBACK LAST;
ROLLBACK TO TAG '<tag>';
ROLLBACK RESOURCE '<type>' '<id>';
```

**Variants:**

- `ROLLBACK LAST` -- Undo the most recent mutation.
- `ROLLBACK TO TAG '<tag>'` -- Restore state to a tagged snapshot.
- `ROLLBACK RESOURCE '<type>' '<id>'` -- Undo the last mutation to a specific resource.

**Example:**

```sql
-- Undo the last change
ROLLBACK LAST;

-- Rollback to a named checkpoint
ROLLBACK TO TAG 'pre-migration';

-- Rollback a specific resource
ROLLBACK RESOURCE 'postgres' 'prod-db';
```

**Returns:** JSON with rollback details and the restored state.

---

## 9a. Plan Management

KVMQL supports a database-backed plan workflow: generate a plan, review it, approve it, then apply it. Plans are stored in the registry with SHA-256 integrity verification.

### Generating a Plan

```bash
orbi plan examples/azure-stack.kvmql --name "staging deploy"
orbi plan "CREATE RESOURCE 'postgres' id = 'test' version = '16';" --output plan.json
```

Plans are stored in the registry with status `pending`.

### Reviewing Plans

```bash
orbi plans                    # list all plans
orbi plans --status pending   # filter by status
```

Or from DSL:

```sql
SELECT * FROM plans WHERE status = 'pending';
```

### Approving a Plan

```bash
orbi approve <plan-id>
```

Changes the plan status from `pending` to `approved`.

### Applying a Plan

```bash
orbi apply <plan-id>          # from registry
orbi apply plan.json          # from file
```

Executes an approved plan. The plan is verified against its SHA-256 checksum before execution.

### Plan Lifecycle

```
plan (pending) --> approve (approved) --> apply (applied)
                                     \-> (failed)
```

---

## 9b. Environment Management

Environments provide isolated registry databases for separating infrastructure state across stages (dev, staging, production).

### Using Environments

```bash
# Run a command against a specific environment
orbi --env staging "SELECT * FROM resources;"
orbi --env staging exec examples/azure-stack.kvmql
```

The `--env` flag is a shortcut for `--registry ~/.kvmql/envs/<name>.db`.

### Managing Environments

```bash
# Create a new environment
orbi env create production

# List all environments
orbi env list

# Show current environment
orbi env current

# Copy an environment (snapshot for DR or testing)
orbi env copy staging production

# Export environment state as JSON
orbi env export staging > staging-snapshot.json

# Import environment from JSON
orbi env import snapshot.json dr-recovery
```

---

## 10. DSL Reference -- Infrastructure

### ADD PROVIDER

Registers a new provider (hypervisor host or cloud region).

```
ADD PROVIDER <param> = <value> ...;
```

**Parameters (read by executor):**

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `id` | Yes | -- | Provider identifier |
| `type` | No | `'kvm'` | Provider type |
| `driver` | No | `'firecracker'` | Driver name |
| `auth` | No | `'none'` | Credential reference (see Section 15) |
| `host` | No | null | Hostname or endpoint |
| `region` | No | null | Region or zone |
| `labels` | No | null | JSON key-value labels |

**Returns:** JSON object with `id`, `type`, `driver`, `status`, `enabled`, `host`, `region`, `auth_ref`, `labels`.

**Example:**

```sql
ADD PROVIDER
  id     = 'kvm.host-a'
  type   = 'kvm'
  driver = 'firecracker'
  host   = '192.168.1.10'
  auth   = 'file:/etc/kvmql/creds/host-a.env';

ADD PROVIDER
  id     = 'aws.ap-southeast-1'
  type   = 'aws'
  region = 'ap-southeast-1'
  auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';
```

### REMOVE PROVIDER

```
REMOVE PROVIDER '<name>';
```

### ALTER PROVIDER

```
ALTER PROVIDER '<name>' SET <key> = <value> [, <key> = <value> ...];
```

Currently supports: `SET status = '<status>'`.

**Example:**

```sql
ALTER PROVIDER 'aws.us-east-1' SET enabled = true;
ALTER PROVIDER 'kvm.host-a' SET status = 'offline';
```

### ADD CLUSTER

Creates a logical cluster of providers.

```
ADD CLUSTER '<name>' MEMBERS = ['<provider1>', '<provider2>', ...];
```

**Example:**

```sql
ADD CLUSTER 'prod' MEMBERS = ['kvm.host-a', 'aws.ap-southeast-1'];
ADD CLUSTER 'test' MEMBERS = [];
```

**Returns:** JSON with `id`, `name`, `members`.

### ALTER CLUSTER

```
ALTER CLUSTER '<name>' ADD MEMBER '<provider_id>';
ALTER CLUSTER '<name>' REMOVE MEMBER '<provider_id>';
```

**Example:**

```sql
ALTER CLUSTER 'prod' ADD MEMBER 'kvm.host-c';
ALTER CLUSTER 'prod' REMOVE MEMBER 'kvm.host-b';
```

### REMOVE CLUSTER

```
REMOVE CLUSTER '<name>';
```

---

## 11. DSL Reference -- Queries

### SELECT

Full SQL-like query syntax for reading from the registry.

```
SELECT <fields> FROM <noun>
  [ON [LIVE] PROVIDER '<id>' | CLUSTER '<id>']
  [WHERE <predicate>]
  [GROUP BY <fields>]
  [ORDER BY <field> [ASC|DESC] [, ...]]
  [LIMIT <n>]
  [OFFSET <n>]
;
```

**Fields:**

- `*` -- All fields.
- `field1, field2, ...` -- Named fields.
- `table.field` -- Qualified field (dot notation).

**Nouns (FROM targets):**

| Noun | Description | SELECT implemented |
|------|-------------|-------------------|
| `microvms` | All microVMs | Yes |
| `volumes` | All volumes | Yes |
| `images` | All images | Yes |
| `providers` | All providers | Yes |
| `resources` | All managed resources | Yes |
| `plans` | Execution plans | Yes |
| `state_snapshots` | State snapshots for rollback | Yes |
| `clusters` | All clusters | Not yet |
| `capabilities` | Provider capabilities | Not yet |
| `snapshots` | VM snapshots | Not yet |
| `metrics` | Live metrics | Not yet |
| `events` | System events | Not yet |
| `query_history` | Past query executions | Not yet |
| `audit_log` | Audit trail | Not yet |
| `principals` | Access control principals | Not yet |
| `grants` | Access control grants | Not yet |
| `cluster_members` | Cluster membership | Not yet |

**ON clause:**

Target a specific provider or cluster. Adding `LIVE` after the provider/cluster name instructs the engine to query the provider directly rather than using the registry cache. (Note: LIVE is parsed but execution behavior depends on driver support.)

```sql
SELECT * FROM microvms ON PROVIDER 'kvm.host-a';
SELECT * FROM microvms ON PROVIDER 'kvm.host-a' LIVE;
SELECT * FROM microvms ON CLUSTER 'prod';
```

**WHERE predicates:**

| Operator | Syntax | Example |
|----------|--------|---------|
| Equal | `=` | `status = 'running'` |
| Not equal | `!=` | `status != 'error'` |
| Greater than | `>` | `vcpus > 2` |
| Less than | `<` | `memory_mb < 1024` |
| Greater or equal | `>=` | `size_gb >= 10` |
| Less or equal | `<=` | `cpu_pct <= 80.0` |
| IN | `IN` | `status IN 'running'` |
| NOT IN | `NOT IN` | `status NOT IN 'error'` |
| LIKE | `LIKE` | `tenant LIKE 'acme%'` |
| IS NULL | `IS NULL` | `microvm_id IS NULL` |
| IS NOT NULL | `IS NOT NULL` | `hostname IS NOT NULL` |

Predicates can be combined with `AND`, `OR`, `NOT`, and parenthesized grouping:

```sql
SELECT * FROM microvms
WHERE (status = 'running' OR status = 'paused') AND tenant = 'acme';

SELECT * FROM microvms WHERE NOT status = 'error';
```

**LIKE pattern matching:** `%` is the wildcard character.
- `'acme%'` -- starts with "acme"
- `'%acme'` -- ends with "acme"
- `'%acme%'` -- contains "acme"

**Arithmetic in expressions:** `+` and `-` are supported in comparisons:

```sql
SELECT * FROM microvms WHERE cpu_pct > 100 - 10;
SELECT * FROM microvms WHERE vcpus = 2 + 2;
```

**Return fields per noun:**

MicroVM rows: `id`, `provider_id`, `tenant`, `status`, `image_id`, `vcpus`, `memory_mb`, `hostname`, `labels`, `created_at`.

Volume rows: `id`, `provider_id`, `microvm_id`, `type`, `size_gb`, `status`, `device_name`, `iops`, `encrypted`, `created_at`, `labels`.

Image rows: `id`, `name`, `os`, `distro`, `version`, `arch`, `type`, `status`, `labels`.

Provider rows: `id`, `type`, `driver`, `status`, `enabled`, `host`, `region`, `auth_ref`, `labels`.

Resource rows: `id`, `resource_type`, `provider_id`, `name`, `status`, `config`, `outputs`, `created_at`, `updated_at`, `labels`.

**Examples:**

```sql
SELECT * FROM microvms;

SELECT id, tenant, status FROM microvms
WHERE status = 'running' ORDER BY cpu_pct DESC LIMIT 10;

SELECT * FROM volumes WHERE microvm_id IS NULL;

SELECT * FROM resources WHERE resource_type = 'postgres';

SELECT id, status, size_mb, arch FROM images WHERE id = 'ubuntu-22.04-lts';

SELECT * FROM microvms LIMIT 10 OFFSET 20;

SELECT tenant, status FROM microvms GROUP BY tenant, status;
```

### SHOW

Shorthand queries for common inspection tasks.

```
SHOW PROVIDERS;
SHOW CLUSTERS;
SHOW IMAGES;
SHOW VERSION;
SHOW CAPABILITIES [FOR PROVIDER '<provider_id>'];
SHOW GRANTS [FOR '<principal_id>'];
```

**SHOW PROVIDERS** -- Lists all providers with `id`, `type`, `driver`, `status`, `enabled`, `host`, `region`.

**SHOW CLUSTERS** -- Not yet implemented (returns `NYI_001`).

**SHOW IMAGES** -- Lists images with `id`, `name`, `os`, `status`.

**SHOW VERSION** -- Returns `{"version": "0.2.0", "engine": "kvmql-engine"}`.

**SHOW CAPABILITIES** -- Lists capability entries per provider with `provider_id`, `capability`, `supported`, `notes`. Without `FOR`, lists all capabilities across all registered drivers.

**SHOW GRANTS FOR '<principal>'** -- Lists grants for a principal with `id`, `principal_id`, `verbs`, `scope_type`, `scope_id`, `granted_at`. Without `FOR`, returns `NYI_001`.

### WATCH

Streams live metrics at a given interval.

```
WATCH METRIC <fields> FROM <noun> [WHERE <predicate>] INTERVAL <duration>;
```

Duration literals: `5s` (seconds), `1m` (minutes), `1h` (hours), `1d` (days).

In the single-shot executor (CLI/REST), WATCH executes one sample of the underlying SELECT and returns it with an `INFO` notification (`STA_001`) instructing the user to use the TCP server for continuous streaming.

**Example:**

```sql
WATCH METRIC cpu_pct, mem_used_mb, net_rx_kbps
FROM microvms
WHERE tenant = 'acme'
INTERVAL 5s;
```

---

## 12. DSL Reference -- Access Control

Access control uses principals, grants, and verbs. When `auth_enabled` is `true` on the `EngineContext`, every statement is checked against the executing principal's grants before execution.

**Note:** `ADD PRINCIPAL`, `GRANT`, and `REVOKE` are fully parsed but their executor implementations currently return `NYI_001` (not yet implemented). The parser and AST support is complete, and the auth checking framework is wired up for when execution is implemented.

### ADD PRINCIPAL

```
ADD PRINCIPAL <param> = <value> ...;
```

**Parameters:**

| Param | Description |
|-------|-------------|
| `id` | Principal identifier (e.g., email) |
| `type` | `'user'` or `'service'` |
| `auth` | Credential reference (see Section 15) |

**Example:**

```sql
ADD PRINCIPAL id = 'ops@acme.com' type = 'user' auth = 'env:ACME_OPS_TOKEN';
```

### GRANT

```
GRANT <verb> [, <verb> ...] ON <scope> [WHERE <predicate>] TO '<principal_id>';
```

**Verbs:** `SELECT`, `CREATE`, `ALTER`, `DESTROY`, `PAUSE`, `RESUME`, `SNAPSHOT`, `RESTORE`, `ATTACH`, `DETACH`, `RESIZE`, `WATCH`, `IMPORT`, `PUBLISH`

**Scopes:**
- `CLUSTER '<name>'`
- `PROVIDER '<name>'`
- `microvms`
- `volumes`
- `images`

**Examples:**

```sql
GRANT SELECT, SNAPSHOT ON microvms WHERE tenant = 'acme' TO 'ops@acme.com';
GRANT SELECT ON volumes WHERE tenant = 'acme' TO 'ops@acme.com';
GRANT SELECT, DESTROY ON CLUSTER 'prod' TO 'admin@acme.com';
```

### REVOKE

```
REVOKE <verb> [, <verb> ...] ON <scope> FROM '<principal_id>';
```

**Example:**

```sql
REVOKE SELECT, DESTROY ON CLUSTER 'prod' FROM 'ops@acme.com';
```

---

## 13. DSL Reference -- Variables & Config

### SET @variable

User-defined variables for value reuse across statements.

```
SET @<name> = <value>;
```

Variables can hold strings, integers, floats, or booleans. They are stored in-memory for the session.

Variable references use `@name` syntax and can appear as parameter values:

```sql
SET @tenant = 'acme';
SET @vcpus = 4;

CREATE MICROVM tenant = @tenant vcpus = @vcpus memory_mb = 1024 image = 'ubuntu-22.04-lts';

CREATE RESOURCE 'postgres' id = @tenant version = '16';
```

Variable resolution happens at execution time. If a variable is not set, the `@name` reference is passed through as-is.

**Returns:** JSON with `variable` and `value` fields.

### SET config

Engine configuration settings.

```
SET <key> = <value>;
```

**Supported config keys:**

| Key | Values | Description |
|-----|--------|-------------|
| `execution_mode` | `'strict'`, `'permissive'` | Controls error handling |

- **strict** -- Abort on first error.
- **permissive** -- Continue past non-fatal errors, collecting notifications (default).

**Example:**

```sql
SET execution_mode = 'strict';
```

Other config keys are acknowledged but return `NYI_001`.

---

## 14. Shell Reference

The interactive shell is launched with `orbi shell` or by running `kvmql` with no arguments.

### Backslash Commands

| Command | Description |
|---------|-------------|
| `\q` or `\quit` | Quit the shell |
| `\h` or `\help` | Show all DSL verbs |
| `\h <VERB>` | Show help for a specific verb |
| `\d` | List all nouns (VM, VOLUME, IMAGE, etc.) |
| `\d <NOUN>` | Show field schema for a noun |
| `\c` | Show current connection context (cluster, provider, expanded, timing) |
| `\cluster <id>` | Set session cluster context |
| `\cluster` | Show current connection (same as `\c`) |
| `\provider <id>` | Set session provider context |
| `\provider` | Show current connection (same as `\c`) |
| `\timing` | Toggle execution timing display on/off |
| `\x` | Toggle expanded display mode on/off |
| `\images` | Shortcut for `SHOW IMAGES;` |
| `\providers` | Shortcut for `SHOW PROVIDERS;` |
| `\clusters` | Shortcut for `SHOW CLUSTERS;` |

### Expanded Display Mode

When enabled with `\x`, results display vertically (one field per line) instead of as a table:

```
-[ RECORD 1 ]---
id       | vm-abc
status   | running
vcpus    | 2
```

### Timing Mode

When enabled with `\timing`, output includes row count and execution time:

```
3 row(s) (42ms)
```

---

## 15. Security

### Credential Backends

KVMQL supports 9 credential backend schemes. The `auth` parameter on providers and principals uses a URI scheme to specify the backend.

#### `env:VAR_NAME` -- Environment Variable

Read a secret from one or more environment variables.

```
auth = 'env:KVMQL_ADMIN_TOKEN'
auth = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY'
```

Multiple variables are comma-separated; their values are joined with commas.

#### `file:/path/to/secret` -- File

Read a secret from a file. The file must NOT be world-readable (Unix permission check: mode `& 0o004` must be 0). Contents are trimmed of whitespace.

```
auth = 'file:/etc/kvmql/creds/host-a.env'
```

#### `vault:mount/path[#field]` -- HashiCorp Vault KV v2

Read a secret from HashiCorp Vault. Requires `VAULT_ADDR` and `VAULT_TOKEN` environment variables.

```
auth = 'vault:secret/myapp#password'
auth = 'vault:kv/database/prod#connection_string'
```

#### `aws-sm:secret-name[#field]` -- AWS Secrets Manager

Read a secret from AWS Secrets Manager. Uses the `aws` CLI.

```
auth = 'aws-sm:prod/db-password#password'
```

#### `gcp-sm:secret-ref` -- GCP Secret Manager

Read a secret from Google Cloud Secret Manager. Uses the `gcloud` CLI.

```
auth = 'gcp-sm:my-project/my-secret'
```

#### `azure-kv:vault-name/secret-name` -- Azure Key Vault

Read a secret from Azure Key Vault. Uses the `az` CLI.

```
auth = 'azure-kv:myvault/mysecret'
```

#### `op:vault/item[#field]` -- 1Password CLI

Read a secret from 1Password. Uses the `op` CLI.

```
auth = 'op:Personal/login#password'
```

#### `sops:/path/to/file#key.subkey` -- Mozilla SOPS

Decrypt and extract a value from a SOPS-encrypted file. Uses the `sops` CLI.

```
auth = 'sops:/etc/secrets/db.yaml#db.password'
```

#### `k8s:namespace/secret-name#key` -- Kubernetes Secrets

Read a secret from a Kubernetes cluster. Uses `kubectl`. All three components (namespace, secret name, key) are required.

```
auth = 'k8s:default/my-secret#password'
```

### Access Control Model

KVMQL uses a principal-grant model:

1. **Principals** are identities (users or services) with an `id`, `type`, and `auth` reference.
2. **Grants** associate a list of verbs with a scope (global, cluster, provider, microvms, volumes, images) and optionally conditions (WHERE clause).
3. When `auth_enabled` is `true`, every statement is checked against the principal's grants before execution.

**Bootstrap admin:** On first use, `ensure_bootstrap_admin()` creates an `admin` principal with credential `env:KVMQL_ADMIN_TOKEN` and a global grant covering all verbs.

**Auth check flow:**
1. Load grants for the current principal from the registry.
2. Check if any grant permits the statement's verb.
3. If denied, write an audit log entry with outcome `"denied"` and return `AUTH_DENIED`.

### Audit Log

Every mutation statement generates an audit log entry BEFORE execution. The audit log records:

- `principal` -- Who executed the statement.
- `action` -- The action code (e.g., `VM_CREATED`, `RESOURCE_DESTROYED`).
- `target_type` -- The type of object (`microvm`, `volume`, `image`, `provider`, `cluster`, `principal`, `grant`, `resource`).
- `target_id` -- The identifier of the affected object.
- `outcome` -- `"permitted"` or `"denied"`.
- `reason` -- Why denied (if applicable).
- `detail` -- JSON with the source statement.

If the audit log write fails, the operation is aborted (fail-closed).

**Audit action codes:**

| Code | Trigger |
|------|---------|
| `VM_CREATED` | CREATE MICROVM |
| `VM_DESTROYED` | DESTROY MICROVM |
| `VM_ALTERED` | ALTER MICROVM |
| `VM_PAUSED` | PAUSE MICROVM |
| `VM_RESUMED` | RESUME MICROVM |
| `SNAPSHOT_TAKEN` | SNAPSHOT MICROVM |
| `SNAPSHOT_RESTORED` | RESTORE MICROVM |
| `VOLUME_CREATED` | CREATE VOLUME |
| `VOLUME_DESTROYED` | DESTROY VOLUME |
| `VOLUME_ATTACHED` | ATTACH VOLUME |
| `VOLUME_DETACHED` | DETACH VOLUME |
| `VOLUME_RESIZED` | RESIZE VOLUME or ALTER VOLUME |
| `IMAGE_IMPORTED` | IMPORT IMAGE |
| `IMAGE_REMOVED` | REMOVE IMAGE |
| `PROVIDER_ADDED` | ADD PROVIDER |
| `PROVIDER_REMOVED` | REMOVE PROVIDER |
| `PROVIDER_ALTERED` | ALTER PROVIDER |
| `CLUSTER_CREATED` | ADD CLUSTER |
| `CLUSTER_REMOVED` | REMOVE CLUSTER |
| `CLUSTER_ALTERED` | ALTER CLUSTER |
| `PRINCIPAL_ADDED` | ADD PRINCIPAL |
| `GRANT_ADDED` | GRANT |
| `GRANT_REVOKED` | REVOKE |
| `RESOURCE_CREATED` | CREATE RESOURCE |
| `RESOURCE_ALTERED` | ALTER RESOURCE |
| `RESOURCE_DESTROYED` | DESTROY RESOURCE |
| `RESOURCE_BACKED_UP` | BACKUP RESOURCE |
| `RESOURCE_RESTORED` | RESTORE RESOURCE |
| `RESOURCE_SCALED` | SCALE RESOURCE |
| `RESOURCE_UPGRADED` | UPGRADE RESOURCE |

**Non-audited statements:** SELECT, SHOW, SET, WATCH, PUBLISH IMAGE.

### Query History

Every executed statement (except SET and SHOW) is recorded in query history with:

- `principal` -- Who executed.
- `statement` -- Original source.
- `normalized` -- Statement with literals replaced by `?` (for privacy).
- `verb` -- Primary verb (SELECT, CREATE, etc.).
- `duration_ms` -- Execution time.
- `status` -- `ok`, `warn`, or `error`.
- `rows_affected` -- Number of affected rows.

---

## 16. Agent & Server

### TCP Server Protocol

The KVMQL server uses a length-prefixed JSON protocol over TCP (and optionally Unix domain sockets).

**Frame format:** 4-byte big-endian length header followed by JSON payload.

**Maximum frame size:** 16 MiB.

**Request format:**

```json
{"statement": "SELECT * FROM microvms;"}
```

**Response format:** A serialized `ResultEnvelope`:

```json
{
  "request_id": "uuid",
  "status": "ok",
  "notifications": [],
  "result": [...],
  "rows_affected": 5,
  "duration_ms": 12
}
```

**Connection handling:** Each TCP connection can send multiple request frames. The server processes them sequentially per connection. Execution is serialized across connections (single mutex over `EngineContext`).

**Listening:**

- TCP: Bind to `host:port` (e.g., `127.0.0.1:9090`).
- Unix socket: Bind to a filesystem path (unix-only, via `run_with_unix`). Stale socket files are automatically removed.

### Agent Protocol

The agent protocol is a length-prefixed JSON protocol (same frame format as the server) with typed messages.

**Protocol version:** 1

**Agent -> Control Plane messages:**

| Type | Fields | Description |
|------|--------|-------------|
| `register` | `agent_id`, `driver_type`, `protocol_version`, `image_store_path`, `image_store_free_gb` | Agent registration |
| `heartbeat` | `agent_id`, `timestamp`, `load` | Periodic health report |
| `state_push` | `agent_id`, `timestamp`, `microvms`, `volumes`, `images` | Full state sync |
| `execute_response` | `request_id`, `success`, `result`, `error` | Response to an execute request |

**AgentLoad fields:** `cpu_pct` (f64), `mem_used_mb` (u64), `vm_count` (u32), `volume_count` (u32), `image_store_used_gb` (Option<u64>).

**Control Plane -> Agent messages:**

| Type | Fields | Description |
|------|--------|-------------|
| `ack` | (none) | Acknowledge registration/heartbeat |
| `reject` | `reason` | Reject registration |
| `execute_request` | `request_id`, `verb`, `params` | Execute an operation |
| `shutdown` | (none) | Graceful shutdown |

**Encoding/Decoding:**

```rust
// Encode
let bytes = encode_message(&msg)?;  // Returns Vec<u8> with 4-byte length prefix

// Decode
let msg: AgentMessage = decode_message(&bytes)?;
```

---

## 17. Error & Notification Reference

### ResultEnvelope

Every execution returns a `ResultEnvelope`:

```json
{
  "request_id": "uuid-string",
  "status": "ok" | "warn" | "error",
  "notifications": [...],
  "result": <value or null>,
  "rows_affected": <integer or null>,
  "duration_ms": <integer>
}
```

**Status values:**

- `ok` -- All statements succeeded with no errors.
- `warn` -- Some statements succeeded but errors occurred in permissive mode.
- `error` -- Execution failed (in strict mode, on first error; in permissive mode, after collecting errors).

### Notification Codes

Each notification has a `level`, `code`, `provider_id` (optional), and `message`.

| Code | Level | Description |
|------|-------|-------------|
| `PARSE_001` | ERROR | Parse error -- invalid DSL syntax |
| `RTE_001` | ERROR | Runtime execution error |
| `NYI_001` | INFO | Feature not yet implemented |
| `SET_001` | INFO | SET acknowledged (e.g., execution_mode) |
| `STA_001` | INFO | WATCH returned a single sample; use TCP server for streaming |
| `CAP_002` | WARN | Capability warning (e.g., unsupported parameter) |
| `AZ_PROVISION_FAILED` | WARN | Azure resource creation failed; registered as "pending" |
| `AZ_UPDATE_FAILED` | WARN | Azure resource update failed; config updated in registry only |
| `AZ_DELETE_FAILED` | WARN | Azure resource deletion failed; removed from registry anyway |
| `AZ_BACKUP_FAILED` | WARN | Azure backup operation failed |
| `AZ_RESTORE_FAILED` | WARN | Azure restore operation failed |
| `AZ_SCALE_FAILED` | WARN | Azure scale operation failed |
| `AZ_UPGRADE_FAILED` | WARN | Azure upgrade operation failed |
| `AUTH_DENIED` | ERROR | Principal lacks required grant for this verb |
| `SIM_001` | INFO | Operating in simulation mode -- no real cloud calls made |
| `SEC_001` | WARN | Security advisory (e.g., insecure credential handling detected) |

### Not-Yet-Implemented Features

The following features are fully parsed (valid DSL syntax) but return `NYI_001` at execution time:

- `ADD PRINCIPAL` -- Parsed, audit-logged, but executor returns NYI.
- `GRANT` -- Parsed, audit-logged, but executor returns NYI.
- `REVOKE` -- Parsed, audit-logged, but executor returns NYI.
- `PUBLISH IMAGE` -- Parsed, but no driver trait method exists. Returns an error if the provider advertises the capability.
- `ALTER VOLUME` with non-`size_gb` fields -- Only `size_gb` triggers actual resize.
- `SHOW CLUSTERS` -- Returns NYI.
- `SHOW GRANTS` without `FOR` -- Returns NYI.
- `SELECT FROM` with nouns: `clusters`, `capabilities`, `snapshots`, `metrics`, `events`, `query_history`, `audit_log`, `principals`, `grants`, `cluster_members` -- Returns NYI.

### Value Types

| Type | Syntax | Examples |
|------|--------|---------|
| String | `'...'` | `'hello'`, `'it''s'` (escaped quote) |
| Integer | digits | `42`, `1024` |
| Float | digits.digits | `3.14`, `0.5` |
| Boolean | `true` / `false` | `true`, `false` |
| Null | `null` / `NULL` | `null` |
| Duration | digits + unit | `5s`, `10m`, `1h`, `7d` |
| Map | `{ key: value, ... }` | `{ env: 'prod', tier: 'compute' }` |
| Array | `[value, ...]` | `['a', 'b', 'c']` |
| Variable | `@name` | `@tenant`, `@vcpus` |

Duration units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

---

## Appendix A: Full Session Example

This complete session is verified to parse correctly (from parser test suite):

```sql
ADD PROVIDER
  id     = 'kvm.host-a'
  type   = 'kvm'
  driver = 'firecracker'
  host   = '192.168.1.10'
  auth   = 'file:/etc/kvmql/creds/host-a.env';

ADD PROVIDER
  id     = 'aws.ap-southeast-1'
  type   = 'aws'
  region = 'ap-southeast-1'
  auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';

ADD CLUSTER 'prod'
  MEMBERS = ['kvm.host-a', 'aws.ap-southeast-1'];

IMPORT IMAGE
  id      = 'ubuntu-22.04-lts'
  source  = 'catalog:ubuntu-22.04-lts';

SELECT id, status, size_mb, arch
FROM   images
WHERE  id = 'ubuntu-22.04-lts';

CREATE VOLUME
  id      = 'vol-acme-data'
  size_gb = 20
  type    = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';

CREATE MICROVM
  tenant    = 'acme'
  vcpus     = 2
  memory_mb = 1024
  image     = 'ubuntu-22.04-lts'
  hostname  = 'acme-web-01'
  VOLUME (
    size_gb = 10
    type    = 'virtio-blk'
  )
  VOLUME (
    id = 'vol-acme-data'
  )
  ON PROVIDER 'kvm.host-a';

SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb
FROM   microvms
ON CLUSTER 'prod'
WHERE  tenant = 'acme'
ORDER BY cpu_pct DESC;

SELECT id, microvm_id, size_gb, status, device_name
FROM   volumes
WHERE  microvm_id = 'vm-abc';

CREATE VOLUME
  id      = 'vol-acme-logs'
  size_gb = 50
  type    = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';

ATTACH VOLUME 'vol-acme-logs'
  TO MICROVM 'vm-abc'
  AS '/dev/vdc';

WATCH METRIC cpu_pct, mem_used_mb, net_rx_kbps
FROM  microvms
WHERE tenant = 'acme'
INTERVAL 5s;

SNAPSHOT MICROVM 'vm-abc'
  INTO 's3://epiphyte-snapshots/acme/vm-abc-20260318'
  TAG  'pre-upgrade';

ADD PRINCIPAL
  id   = 'ops@acme.com'
  type = 'user'
  auth = 'env:ACME_OPS_TOKEN';

GRANT SELECT, SNAPSHOT ON microvms
  WHERE tenant = 'acme'
  TO 'ops@acme.com';

GRANT SELECT ON volumes
  WHERE tenant = 'acme'
  TO 'ops@acme.com';

PUBLISH IMAGE 'ubuntu-22.04-lts'
  TO PROVIDER 'aws.ap-southeast-1';

CREATE MICROVM
  tenant    = 'acme'
  vcpus     = 2
  memory_mb = 1024
  image     = 'ubuntu-22.04-lts'
  VOLUME ( size_gb = 20 type = 'ebs' )
  ON PROVIDER 'aws.ap-southeast-1';

RESIZE VOLUME 'vol-acme-data' TO 40 GB;

DETACH VOLUME 'vol-acme-logs' FROM MICROVM 'vm-abc';
DESTROY MICROVM 'vm-abc';
DESTROY VOLUME 'vol-acme-data' FORCE;
DESTROY VOLUME 'vol-acme-logs';
REMOVE IMAGE 'ubuntu-22.04-lts';
```

## Appendix B: Managed Resource Lifecycle Example

```sql
-- Set up provider
ADD PROVIDER id = 'azure.eastus' type = 'azure' region = 'eastus'
  auth = 'env:AZURE_SUBSCRIPTION_ID';

-- Create networking
CREATE RESOURCE 'vnet' id = 'acme-vnet' address_space = '10.0.0.0/16'
  ON PROVIDER 'azure.eastus';

CREATE RESOURCE 'nsg' id = 'acme-nsg'
  ON PROVIDER 'azure.eastus';

CREATE RESOURCE 'subnet'
  id = 'app' vnet = 'acme-vnet' address_prefix = '10.0.1.0/24'
  nsg = 'acme-nsg'
  ON PROVIDER 'azure.eastus';

-- Create database
CREATE RESOURCE 'postgres'
  id = 'acme-db' version = '16' sku = 'Standard_B1ms'
  storage_gb = 128 backup_retention_days = 14
  ON PROVIDER 'azure.eastus';

CREATE RESOURCE 'pg_database'
  id = 'myapp' server = 'acme-db'
  ON PROVIDER 'azure.eastus';

-- Create container registry and app
CREATE RESOURCE 'container_registry'
  id = 'acmereg' sku = 'Standard'
  ON PROVIDER 'azure.eastus';

CREATE RESOURCE 'container_app'
  id = 'api' image = 'acmereg.azurecr.io/api:v1'
  cpu = '0.5' memory = '1Gi' min_replicas = 1 max_replicas = 5
  ON PROVIDER 'azure.eastus';

-- Day-2: Scale up
SCALE RESOURCE 'container_app' 'api' min_replicas = 2 max_replicas = 10;

-- Day-2: Upgrade database
ALTER RESOURCE 'postgres' 'acme-db' SET sku = 'Standard_B2s';

-- Day-2: Backup
BACKUP RESOURCE 'postgres' 'acme-db';

-- Query all resources
SELECT * FROM resources;

-- Clean up
DESTROY RESOURCE 'container_app' 'api';
DESTROY RESOURCE 'postgres' 'acme-db';
DESTROY RESOURCE 'vnet' 'acme-vnet';
```

## Appendix C: Grammar Quick Reference

```
-- Queries
SELECT <fields|*> FROM <noun> [ON [LIVE] PROVIDER|CLUSTER '<id>']
  [WHERE <pred>] [GROUP BY <fields>] [ORDER BY <field> [ASC|DESC], ...]
  [LIMIT <n>] [OFFSET <n>];
SHOW PROVIDERS|CLUSTERS|IMAGES|VERSION;
SHOW CAPABILITIES [FOR PROVIDER '<id>'];
SHOW GRANTS [FOR '<principal>'];
WATCH METRIC <fields> FROM <noun> [WHERE <pred>] INTERVAL <dur>;

-- VM lifecycle
CREATE MICROVM <params> [VOLUME (<params>)]... [ON PROVIDER '<id>']
  [PLACEMENT POLICY = '<p>'] [REQUIRE <clause>, ...];
DESTROY MICROVM '<id>' [FORCE];
ALTER MICROVM '<id>' SET <k>=<v>, ...;
PAUSE MICROVM '<id>';
RESUME MICROVM '<id>';
SNAPSHOT MICROVM '<id>' INTO '<dest>' [TAG '<tag>'];
RESTORE MICROVM '<id>' FROM '<source>';

-- Volumes
CREATE VOLUME <params> [ON PROVIDER '<id>'];
DESTROY VOLUME '<id>' [FORCE];
ALTER VOLUME '<id>' SET <k>=<v>, ...;
ATTACH VOLUME '<vid>' TO MICROVM '<mid>' [AS '<dev>'];
DETACH VOLUME '<vid>' FROM MICROVM '<mid>';
RESIZE VOLUME '<vid>' TO <n> GB;

-- Images
IMPORT IMAGE <params>;
PUBLISH IMAGE '<id>' TO PROVIDER '<pid>';
REMOVE IMAGE '<id>' [FORCE];

-- Managed resources
CREATE RESOURCE '<type>' <params> [ON PROVIDER '<id>'];
ALTER RESOURCE '<type>' '<id>' SET <k>=<v>, ...;
DESTROY RESOURCE '<type>' '<id>' [FORCE];
BACKUP RESOURCE '<type>' '<id>' [INTO '<dest>'] [TAG '<tag>'];
RESTORE RESOURCE '<type>' '<id>' FROM '<source>';
SCALE RESOURCE '<type>' '<id>' <params>;
UPGRADE RESOURCE '<type>' '<id>' <params>;

-- Infrastructure
ADD PROVIDER <params>;
REMOVE PROVIDER '<name>';
ALTER PROVIDER '<name>' SET <k>=<v>, ...;
ADD CLUSTER '<name>' MEMBERS = ['<p1>', '<p2>', ...];
ALTER CLUSTER '<name>' ADD|REMOVE MEMBER '<pid>';
REMOVE CLUSTER '<name>';

-- Access control
ADD PRINCIPAL <params>;
GRANT <verbs> ON <scope> [WHERE <pred>] TO '<principal>';
REVOKE <verbs> ON <scope> FROM '<principal>';

-- EXPLAIN / ROLLBACK
EXPLAIN <any-mutation-statement>;
ROLLBACK LAST;
ROLLBACK TO TAG '<tag>';
ROLLBACK RESOURCE '<type>' '<id>';

-- Config
SET <key> = <value>;
SET @<var> = <value>;
```
