# KVMQL — Universal VM Control DSL
## Technical Specification v0.2
### Epiphyte Corp — Internal Engineering Spec

---

## Document Purpose

This document is the authoritative specification for KVMQL — a declarative, queryable control plane DSL for managing virtual machines, images, and volumes across KVM/Firecracker hosts and cloud providers. It is written for AI agents and engineers implementing the system. Every design decision, build order constraint, grammar rule, schema definition, and interface contract is specified here.

Do not deviate from the build order. Do not implement layer N+1 before layer N is verified working.

**Changes from v0.1:**
- Image catalog added (OS images, kernel images, custom images)
- Volume management added (create, attach, detach, destroy)
- CREATE MICROVM updated to reference images and volumes
- Shell (REPL) fully specified
- Terraform relationship clarified — KVMQL replaces it for VM layer
- Container/microVM relationship corrected
- Cost estimation added as planned post-v1 feature
- What KVMQL Is Not corrected throughout

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Build Order](#2-build-order)
3. [DSL Grammar Specification](#3-dsl-grammar-specification)
4. [Type System](#4-type-system)
5. [Noun Reference](#5-noun-reference)
6. [Verb Reference](#6-verb-reference)
7. [Control Plane Architecture](#7-control-plane-architecture)
8. [Agent Architecture](#8-agent-architecture)
9. [Driver Interface](#9-driver-interface)
10. [Registry Schema](#10-registry-schema)
11. [Credential Resolver](#11-credential-resolver)
12. [Capability Model](#12-capability-model)
13. [Image Catalog](#13-image-catalog)
14. [Volume Management](#14-volume-management)
15. [Query History](#15-query-history)
16. [Audit Log](#16-audit-log)
17. [Access Control](#17-access-control)
18. [Configuration Bootstrap](#18-configuration-bootstrap)
19. [CLI and Shell Specification](#19-cli-and-shell-specification)
20. [Notification System](#20-notification-system)
21. [Streaming Protocol](#21-streaming-protocol)
22. [Error Reference](#22-error-reference)
23. [Testing Requirements](#23-testing-requirements)

---

## 1. System Overview

### What KVMQL Is

KVMQL is a DSL and control plane for managing virtual machines, OS images, and block volumes across heterogeneous infrastructure. It provides:

- A SQL-like language for both read (query) and write (mutation) operations across VMs, images, and volumes
- An image catalog that tracks available OS images, kernel images, and custom images across providers
- Volume lifecycle management (create, attach, detach, resize, destroy)
- A driver model abstracting KVM/Firecracker, AWS, GCP, and Azure behind a unified interface
- An embedded DuckDB registry as the single source of truth for topology, images, volumes, state, history, and access control
- A thin agent per KVM host that executes local operations and syncs state to the control plane
- A capability model that reports provider-specific limitations explicitly rather than failing silently
- A psql-style interactive shell for human operators

### What KVMQL Is Not

- **Not a hypervisor.** KVMQL manages VMs; KVM and Firecracker are the hypervisor layer underneath it.
- **Not a container orchestrator** (Docker, Kubernetes). However, microVMs are a valid alternative isolation primitive to containers. KVMQL manages the microVM layer; container runtimes can run inside microVMs if needed. The two models are adjacent, not mutually exclusive.
- **Not HA by default in v1.** Single control plane. HA is a documented upgrade path for v2.

### Relationship to Terraform

KVMQL does not need Terraform for VM layer management. Terraform solves desired-state reconciliation declared in HCL files against a statefile. KVMQL solves the same problem differently: operations are expressed in DSL, and the live registry is the statefile — always current, always queryable. If you can CREATE, ALTER, DESTROY, and SELECT infrastructure through KVMQL, the Terraform layer for VM management is redundant.

KVMQL is not a general-purpose IaC tool (DNS records, IAM policies, managed databases are out of scope), but for VM, image, and volume lifecycle it is a direct and simpler alternative to Terraform.

### Cost Visibility (Planned, Post-v1)

KVMQL has sufficient information to provide cost estimates for cloud-hosted microVMs:

- Provider type known at CREATE time
- vcpus, memory_mb, and disk_gb known
- created_at and status track uptime
- Cloud provider on-demand pricing is public and cacheable

A `pricing` table will be added to the registry post-v1, seeded from provider pricing APIs and refreshed periodically. This enables:

```sql
SELECT   tenant,
         SUM(COST_ESTIMATE()) as usd_est_month
FROM     microvms
WHERE    status = 'running'
GROUP BY tenant
ORDER BY usd_est_month DESC;
```

`COST_ESTIMATE()` returns null for KVM (fixed infrastructure cost not tracked by KVMQL) and hourly-rate × uptime for cloud providers. This is an operational estimate, not real billing.

### Core Design Principles

1. **DSL is the contract.** Everything — topology, images, volumes, VM operations, history, audit, access control — is expressed in KVMQL. The CLI and shell are thin DSL executors.
2. **One TOML file.** Bootstrap configuration only. Everything else is managed through the DSL.
3. **Explicit over silent.** Unsupported operations emit structured notifications (INFO/WARN/ERROR). Nothing fails silently.
4. **Registry is the equalizer.** All providers normalize state into DuckDB. Query layer never touches provider APIs directly except when `LIVE` is specified.
5. **Credentials never touch the registry.** Credential resolver handles secrets. The registry stores only references.
6. **Strict build order.** Multi-host before multi-cloud. Single host before multi-host. Images before volumes. Volumes before advanced CREATE.

---

## 2. Build Order

Agents must complete each phase before starting the next. Each phase has a defined verification gate.

### Phase 1 — DSL Parser and Grammar

Deliverables:
- EBNF grammar implemented in Rust (preferred) or Go
- Parser producing a typed AST from DSL text
- Lexer with token definitions for all keywords, types, and operators
- Parse error messages with line/column and suggestion

Verification gate:
- All grammar examples in Section 3 parse without error
- Malformed inputs return structured parse errors
- Round-trip: AST serializes back to canonical DSL text

### Phase 2 — KVM/Firecracker Driver (Single Host)

Deliverables:
- Firecracker REST API driver implementing the Driver Interface (Section 9)
- Capability manifest for Firecracker driver
- Driver unit tests against Firecracker mock

Verification gate:
- CREATE, SELECT, DESTROY, PAUSE, RESUME, SNAPSHOT, RESTORE verified against a running Firecracker instance
- Capability manifest accurately reflects Firecracker capabilities

### Phase 3 — Local DuckDB Registry (Single Host)

Deliverables:
- DuckDB schema (Section 10) initialized on first run
- Registry read/write layer
- State sync from driver to registry on every operation
- Schema migration system (versioned, forward-only)

Verification gate:
- All tables created correctly on fresh init
- SELECT queries return correct state after CREATE/DESTROY
- Registry survives control plane restart with state intact

### Phase 4 — Control Plane (Single Host)

Deliverables:
- Control plane binary accepting DSL over Unix socket or TCP
- Query planner (single-host mode)
- Execution engine: parser → planner → driver → registry → response
- Bootstrap config loader (Section 18)

Verification gate:
- Full DSL round-trip: client sends DSL text, control plane executes, returns result
- All Phase 2 verbs work end-to-end through the control plane

### Phase 5 — Image Catalog

Deliverables:
- `images` table in registry
- `IMPORT IMAGE`, `PUBLISH IMAGE`, `REMOVE IMAGE` verbs
- Image source resolvers: local path, HTTP/HTTPS, S3
- Image driver interface: download, verify checksum, store locally
- Cloud image mapping: AMI IDs (AWS), machine images (GCP), VM images (Azure)
- `CREATE MICROVM` updated to accept `image` reference instead of raw `kernel`/`rootfs` paths

Verification gate:
- `IMPORT IMAGE` from local path populates registry
- `IMPORT IMAGE` from HTTP URL downloads, checksums, and stores
- `CREATE MICROVM image = 'ubuntu-22.04-lts'` resolves image and boots VM
- `SELECT * FROM images` returns correct catalog

### Phase 6 — Volume Management

Deliverables:
- `volumes` table in registry
- `CREATE VOLUME`, `DESTROY VOLUME`, `ATTACH VOLUME`, `DETACH VOLUME`, `RESIZE VOLUME` verbs
- Volume driver interface per provider
- `CREATE MICROVM` updated to accept inline volume spec or reference existing volume

Verification gate:
- `CREATE VOLUME` creates a block device on KVM host
- `ATTACH VOLUME` hotplugs to a running VM (where capability exists)
- `DETACH VOLUME` cleanly removes device
- `DESTROY VOLUME` fails if volume is attached (explicit FORCE required)

### Phase 7 — Shell (REPL)

Deliverables:
- `kvmql shell` command launches interactive REPL
- readline with history persisted to `~/.kvmql/history`
- Multi-line statement accumulation until `;`
- Tab completion: nouns, verbs, provider IDs, image IDs, VM IDs from registry
- Backslash meta-commands (Section 19)
- `\cluster` and `\provider` session context commands
- Notification rendering inline above results
- `\x` expanded/vertical output mode
- `\o` output redirection
- `\e` open buffer in $EDITOR

Verification gate:
- Multi-line statement executes on `;`
- Tab completion returns correct candidates from live registry
- `\cluster prod` scopes subsequent queries to cluster prod
- `\o output.csv` redirects result to file

### Phase 8 — Agent + Multi-Host

Deliverables:
- Agent binary (thin, per-host)
- Agent heartbeat and state push to control plane
- Control plane fan-out for SELECT queries across multiple agents
- Mutation routing: control plane routes to owning host
- Global identity model: all IDs globally unique
- `PLACEMENT POLICY` primitive for CREATE

Verification gate:
- Two-host cluster: SELECT returns VMs from both hosts
- CREATE with `PLACEMENT POLICY = 'least_loaded'` places correctly
- DESTROY routes to owning host
- Agent disconnect marks VMs as stale

### Phase 9 — Credential Resolver

Deliverables:
- `env:` resolver
- `file:` resolver
- `vault:` resolver (HashiCorp Vault KV v2)
- Credential never written to registry, query history, or logs

Verification gate:
- All three resolver types work
- Credential values absent from DuckDB under all tables
- Vault lease renewal functions correctly

### Phase 10 — Query History and Audit Log

Deliverables:
- `query_history` table populated on every DSL execution
- `audit_log` table populated on every lifecycle event and auth decision
- Both queryable via DSL

Verification gate:
- Every verb creates a query_history row
- Every VM/image/volume lifecycle event creates an audit_log row
- No credential values in either table

### Phase 11 — Access Control

Deliverables:
- `principals` and `grants` tables
- Auth check on every execution before planner
- `ADD PRINCIPAL`, `GRANT`, `REVOKE` verbs
- Tenant-scoped grants enforced at query rewrite layer

Verification gate:
- SELECT-only principal cannot DESTROY
- Tenant-scoped grant sees only own tenant's resources
- REVOKE removes access immediately

### Phase 12 — Cloud Drivers

Deliverables (in order): AWS EC2, GCP Compute Engine, Azure VM
- Per-driver capability manifest
- Image mapping: cloud images → catalog entries
- Volume mapping: EBS / persistent disk / managed disk
- Notification engine for unsupported parameters

Verification gate:
- CREATE on AWS with unsupported KVM param emits WARN in PERMISSIVE, ERROR in STRICT
- SELECT returns VMs across KVM + AWS in single result
- Images from AWS AMI catalog queryable in `images` table

---

## 3. DSL Grammar Specification

### EBNF Grammar

```ebnf
program         ::= statement (';' statement)* ';'?

statement       ::= select_stmt
                  | create_microvm_stmt
                  | create_volume_stmt
                  | alter_microvm_stmt
                  | alter_volume_stmt
                  | destroy_stmt
                  | pause_stmt
                  | resume_stmt
                  | snapshot_stmt
                  | restore_stmt
                  | watch_stmt
                  | attach_stmt
                  | detach_stmt
                  | resize_stmt
                  | import_image_stmt
                  | publish_image_stmt
                  | remove_image_stmt
                  | add_provider_stmt
                  | remove_provider_stmt
                  | alter_provider_stmt
                  | add_cluster_stmt
                  | alter_cluster_stmt
                  | remove_cluster_stmt
                  | add_principal_stmt
                  | grant_stmt
                  | revoke_stmt
                  | set_stmt
                  | show_stmt

select_stmt     ::= 'SELECT' field_list
                    'FROM' noun
                    ('ON' target_spec)?
                    ('WHERE' predicate)?
                    ('GROUP BY' field_list)?
                    ('ORDER BY' order_list)?
                    ('LIMIT' INTEGER)?

create_microvm_stmt ::= 'CREATE' 'MICROVM'
                        param_list
                        ('VOLUME' volume_inline (',' volume_inline)*)?
                        ('ON' target_spec)?
                        ('PLACEMENT' 'POLICY' '=' STRING)?
                        ('REQUIRE' require_clause (',' require_clause)*)?

create_volume_stmt  ::= 'CREATE' 'VOLUME'
                        param_list
                        ('ON' target_spec)?

alter_microvm_stmt  ::= 'ALTER' 'MICROVM' id_expr
                        'SET' set_list

alter_volume_stmt   ::= 'ALTER' 'VOLUME' id_expr
                        'SET' set_list

destroy_stmt    ::= 'DESTROY' ('MICROVM' | 'VOLUME') id_expr ('FORCE')?

pause_stmt      ::= 'PAUSE' 'MICROVM' id_expr

resume_stmt     ::= 'RESUME' 'MICROVM' id_expr

snapshot_stmt   ::= 'SNAPSHOT' 'MICROVM' id_expr
                    'INTO' STRING
                    ('TAG' STRING)?

restore_stmt    ::= 'RESTORE' 'MICROVM' id_expr
                    'FROM' STRING

watch_stmt      ::= 'WATCH' 'METRIC' field_list
                    'FROM' noun
                    ('WHERE' predicate)?
                    'INTERVAL' DURATION

attach_stmt     ::= 'ATTACH' 'VOLUME' id_expr
                    'TO' 'MICROVM' id_expr
                    ('AS' STRING)?

detach_stmt     ::= 'DETACH' 'VOLUME' id_expr
                    'FROM' 'MICROVM' id_expr

resize_stmt     ::= 'RESIZE' 'VOLUME' id_expr
                    'TO' INTEGER 'GB'

import_image_stmt ::= 'IMPORT' 'IMAGE'
                      param_list

publish_image_stmt ::= 'PUBLISH' 'IMAGE' id_expr
                       'TO' 'PROVIDER' STRING

remove_image_stmt  ::= 'REMOVE' 'IMAGE' id_expr ('FORCE')?

add_provider_stmt   ::= 'ADD' 'PROVIDER' param_list
remove_provider_stmt ::= 'REMOVE' 'PROVIDER' STRING
alter_provider_stmt ::= 'ALTER' 'PROVIDER' STRING 'SET' set_list

add_cluster_stmt    ::= 'ADD' 'CLUSTER' STRING
                        'MEMBERS' '=' '[' string_list ']'
alter_cluster_stmt  ::= 'ALTER' 'CLUSTER' STRING
                        ( 'ADD' 'MEMBER' STRING
                        | 'REMOVE' 'MEMBER' STRING )
remove_cluster_stmt ::= 'REMOVE' 'CLUSTER' STRING

add_principal_stmt  ::= 'ADD' 'PRINCIPAL' param_list
grant_stmt          ::= 'GRANT' verb_list
                        'ON' grant_scope
                        ('WHERE' predicate)?
                        'TO' STRING
revoke_stmt         ::= 'REVOKE' verb_list
                        'ON' grant_scope
                        'FROM' STRING

set_stmt        ::= 'SET' IDENTIFIER '=' value
show_stmt       ::= 'SHOW' show_target

volume_inline   ::= '(' param_list ')'

noun            ::= 'microvms'
                  | 'volumes'
                  | 'images'
                  | 'providers'
                  | 'clusters'
                  | 'capabilities'
                  | 'snapshots'
                  | 'metrics'
                  | 'events'
                  | 'query_history'
                  | 'audit_log'
                  | 'principals'
                  | 'grants'
                  | 'cluster_members'

target_spec     ::= 'PROVIDER' STRING
                  | 'CLUSTER' STRING
                  | target_spec 'LIVE'

field_list      ::= '*' | field (',' field)*
field           ::= IDENTIFIER | IDENTIFIER '.' IDENTIFIER

predicate       ::= predicate 'AND' predicate
                  | predicate 'OR' predicate
                  | 'NOT' predicate
                  | '(' predicate ')'
                  | comparison

comparison      ::= expr operator expr
operator        ::= '=' | '!=' | '>' | '<' | '>=' | '<=' | 'IN' | 'NOT IN' | 'LIKE'

expr            ::= IDENTIFIER | STRING | INTEGER | FLOAT | BOOLEAN | DURATION
                  | function_call | '(' expr ')'

function_call   ::= IDENTIFIER '(' (expr (',' expr)*)? ')'

param_list      ::= param (param)*
param           ::= IDENTIFIER '=' value

set_list        ::= set_item (',' set_item)*
set_item        ::= IDENTIFIER '=' value

require_clause  ::= 'capability' '=' STRING
                  | 'provider'   '=' STRING
                  | 'label' IDENTIFIER '=' STRING

verb_list       ::= verb (',' verb)*
verb            ::= 'SELECT' | 'CREATE' | 'ALTER' | 'DESTROY'
                  | 'PAUSE' | 'RESUME' | 'SNAPSHOT' | 'RESTORE'
                  | 'ATTACH' | 'DETACH' | 'RESIZE' | 'WATCH'
                  | 'IMPORT' | 'PUBLISH'

grant_scope     ::= 'CLUSTER' STRING | 'PROVIDER' STRING | 'microvms' | 'volumes' | 'images'

show_target     ::= 'PROVIDERS'
                  | 'CLUSTERS'
                  | 'CAPABILITIES' ('FOR' 'PROVIDER' STRING)?
                  | 'GRANTS' ('FOR' STRING)?
                  | 'IMAGES'
                  | 'VERSION'

order_list      ::= order_item (',' order_item)*
order_item      ::= IDENTIFIER ('ASC' | 'DESC')?

string_list     ::= STRING (',' STRING)*
id_expr         ::= STRING

value           ::= STRING | INTEGER | FLOAT | BOOLEAN | DURATION
                  | map_literal | array_literal
map_literal     ::= '{' map_entry (',' map_entry)* '}'
map_entry       ::= IDENTIFIER ':' value
array_literal   ::= '[' value (',' value)* ']'
```

### Lexer Tokens

```
STRING      ::= '\'' [^']* '\''
INTEGER     ::= [0-9]+
FLOAT       ::= [0-9]+ '.' [0-9]+
BOOLEAN     ::= 'true' | 'false'
DURATION    ::= INTEGER ('s' | 'm' | 'h' | 'd')
IDENTIFIER  ::= [a-zA-Z_][a-zA-Z0-9_]*
COMMENT     ::= '--' [^\n]* '\n'        (ignored)
WHITESPACE  ::= [ \t\n\r]+             (ignored)
```

### Reserved Keywords

```
SELECT FROM WHERE ORDER BY LIMIT GROUP
CREATE ALTER DESTROY PAUSE RESUME SNAPSHOT RESTORE WATCH
ATTACH DETACH RESIZE IMPORT PUBLISH REMOVE
ADD REMOVE GRANT REVOKE SET SHOW
MICROVM VOLUME IMAGE PROVIDER CLUSTER PRINCIPAL
ON LIVE FORCE INTO FROM TAG AS TO
MEMBERS MEMBER POLICY PLACEMENT REQUIRE
AND OR NOT IN LIKE ASC DESC
METRIC INTERVAL GB
```

---

## 4. Type System

| Type | Description | Example |
|------|-------------|---------|
| `STRING` | Quoted string | `'acme'` |
| `INTEGER` | Unquoted integer | `512` |
| `FLOAT` | Decimal number | `82.5` |
| `BOOLEAN` | true or false | `true` |
| `DURATION` | Time value | `'10m'`, `'5s'`, `'1h'` |
| `PATH` | File or object store path | `'/images/vmlinux'` |
| `MAP` | Key-value pairs | `{ region: 'sg', tier: 'compute' }` |
| `ARRAY` | Ordered list | `['kvm.host-a', 'kvm.host-b']` |
| `CREDENTIAL_REF` | Credential reference | `'env:AWS_ACCESS_KEY_ID'` |

---

## 5. Noun Reference

### microvms

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Globally unique VM identifier |
| `provider_id` | STRING | Owning provider |
| `host` | STRING | Host or region |
| `tenant` | STRING | Tenant identifier |
| `status` | STRING | `running`, `stopped`, `paused`, `error` |
| `image_id` | STRING | Image used to boot (references images table) |
| `vcpus` | INTEGER | Number of vCPUs |
| `memory_mb` | INTEGER | Memory in MB |
| `cpu_pct` | FLOAT | CPU utilisation % |
| `mem_used_mb` | INTEGER | Memory usage MB |
| `net_rx_kbps` | FLOAT | Network receive rate |
| `net_tx_kbps` | FLOAT | Network transmit rate |
| `created_at` | STRING | ISO 8601 creation timestamp |
| `last_seen` | STRING | Last state sync timestamp |
| `is_stale` | BOOLEAN | True if agent missed N heartbeats |
| `metadata` | MAP | Arbitrary key-value metadata |
| `labels` | MAP | Searchable labels |

### images

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Image identifier (e.g. `ubuntu-22.04-lts`) |
| `name` | STRING | Human-readable name |
| `os` | STRING | `linux`, `windows` |
| `distro` | STRING | `ubuntu`, `debian`, `alpine`, `fedora`, `custom` |
| `version` | STRING | OS version string |
| `arch` | STRING | `x86_64`, `aarch64` |
| `type` | STRING | `kernel+rootfs`, `disk`, `ami`, `machine_image` |
| `provider_id` | STRING | Which provider this image is available on (null = all) |
| `kernel_path` | PATH | Kernel image path (KVM type only) |
| `rootfs_path` | PATH | Root filesystem path (KVM type only) |
| `disk_path` | PATH | Disk image path (disk type only) |
| `cloud_ref` | STRING | AMI ID / GCP image name / Azure image URN |
| `source` | STRING | Where the image was imported from |
| `checksum_sha256` | STRING | SHA-256 of image file |
| `size_mb` | INTEGER | Image size in MB |
| `status` | STRING | `available`, `importing`, `error` |
| `imported_at` | STRING | Import timestamp |
| `labels` | MAP | Searchable labels |

### volumes

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Globally unique volume identifier |
| `provider_id` | STRING | Owning provider |
| `microvm_id` | STRING | Attached VM (null if detached) |
| `type` | STRING | `virtio-blk`, `ebs`, `persistent-disk`, `managed-disk` |
| `size_gb` | INTEGER | Volume size in GB |
| `status` | STRING | `available`, `attached`, `detaching`, `error` |
| `device_name` | STRING | Device name inside VM (e.g. `/dev/vdb`) |
| `iops` | INTEGER | Provisioned IOPS (cloud only) |
| `encrypted` | BOOLEAN | Whether volume is encrypted |
| `created_at` | STRING | Creation timestamp |
| `labels` | MAP | Searchable labels |

### providers

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Provider identifier |
| `type` | STRING | `kvm`, `aws`, `gcp`, `azure` |
| `driver` | STRING | `firecracker`, `libvirt`, `ec2`, `compute`, `azure_vm` |
| `status` | STRING | `healthy`, `degraded`, `offline`, `unknown` |
| `enabled` | BOOLEAN | Whether provider accepts operations |
| `latency_ms` | INTEGER | Last health check RTT |
| `labels` | MAP | Labels |
| `added_at` | STRING | When provider was registered |

### capabilities

| Field | Type | Description |
|-------|------|-------------|
| `provider_id` | STRING | Provider |
| `capability` | STRING | Capability name |
| `supported` | BOOLEAN | Whether supported |
| `notes` | STRING | Human-readable notes |

Defined capability names:
```
create                destroy               pause
resume                snapshot              restore
alter_cpu_live        alter_memory_live     watch_metric
placement             custom_kernel         vsock
balloon               hotplug_volume        hotplug_network
live_migration        nested_virt           gpu_passthrough
volume_resize_live    volume_encrypt        image_import
image_publish
```

### snapshots

| Field | Type | Description |
|-------|------|-------------|
| `id` | STRING | Snapshot identifier |
| `microvm_id` | STRING | Source VM |
| `provider_id` | STRING | Storage provider |
| `destination` | PATH | Storage path |
| `tag` | STRING | Optional label |
| `taken_at` | STRING | Timestamp |
| `size_mb` | INTEGER | Size |

### clusters, cluster_members, events, metrics, query_history, audit_log, principals, grants

Unchanged from v0.1. See Section 10 (Registry Schema) for full DDL.

---

## 6. Verb Reference

### SELECT

```sql
SELECT <field_list>
FROM   <noun>
[ON PROVIDER '<id>' | ON CLUSTER '<id>'] [LIVE]
[WHERE <predicate>]
[GROUP BY <field_list>]
[ORDER BY <field> ASC|DESC]
[LIMIT <n>]
```

Works on all nouns including `images` and `volumes`.

### CREATE MICROVM

```sql
CREATE MICROVM
  id          = '<id>'              -- optional, auto UUID if omitted
  tenant      = '<tenant>'
  vcpus       = <n>
  memory_mb   = <n>
  image       = '<image_id>'        -- references images table
  [hostname   = '<hostname>']
  [network    = '<tap_iface>']
  [metadata   = { key: value }]
  [labels     = { key: value }]
  VOLUME (                           -- inline root volume
    size_gb   = <n>
    type      = 'virtio-blk'
    [encrypted = true]
  )
  [VOLUME (                          -- additional volumes
    id        = '<existing_vol_id>'  -- attach existing, OR
    size_gb   = <n>                  -- create new
    type      = 'virtio-blk'
  )]
  [ON PROVIDER '<provider_id>']
  [PLACEMENT POLICY = '<policy>']
  [REQUIRE capability = '<cap>' [, ...]]
```

Image resolution:
1. Look up `image_id` in registry `images` table
2. If `provider_id` matches target provider: use directly
3. If image is universal (provider_id null): resolve to provider-specific format
4. If image not available on target provider: ERROR unless `PUBLISH IMAGE` was run first

Placement policies:
```
least_loaded      -- provider with lowest average CPU
least_memory      -- provider with most free memory
least_cost        -- post-v1 only
latency_to:<addr> -- lowest latency to address
explicit          -- ON PROVIDER required
```

### CREATE VOLUME

```sql
CREATE VOLUME
  id          = '<id>'              -- optional
  size_gb     = <n>
  type        = 'virtio-blk'        -- KVM default
              | 'ebs'               -- AWS
              | 'persistent-disk'   -- GCP
              | 'managed-disk'      -- Azure
  [encrypted  = true]
  [iops       = <n>]                -- cloud only
  [labels     = { key: value }]
  [ON PROVIDER '<provider_id>']
```

### ATTACH VOLUME

```sql
ATTACH VOLUME '<volume_id>'
  TO MICROVM '<microvm_id>'
  [AS '/dev/vdb']                   -- device name inside VM
```

Requires `hotplug_volume` capability for live attach. Without it in PERMISSIVE mode: emits WARN, requires VM stop/start cycle.

### DETACH VOLUME

```sql
DETACH VOLUME '<volume_id>'
  FROM MICROVM '<microvm_id>'
```

Graceful detach. Fails if volume is the root volume (FORCE not permitted on root volumes).

### RESIZE VOLUME

```sql
RESIZE VOLUME '<volume_id>' TO <n> GB
```

Online resize requires `volume_resize_live` capability. Without it: VM must be stopped.

### DESTROY VOLUME

```sql
DESTROY VOLUME '<volume_id>' [FORCE]
```

Fails if volume is attached unless FORCE is specified. FORCE detaches then destroys. Data is permanently lost. Audit log entry always written.

### IMPORT IMAGE

```sql
IMPORT IMAGE
  id          = '<image_id>'
  name        = '<human name>'
  os          = 'linux' | 'windows'
  distro      = 'ubuntu' | 'debian' | 'alpine' | 'fedora' | 'custom'
  version     = '<version>'
  arch        = 'x86_64' | 'aarch64'
  type        = 'kernel+rootfs' | 'disk'
  source      = '<uri>'             -- local path, http/https, s3://, gs://, az://
  [kernel     = '<path>']           -- for kernel+rootfs type: kernel within source
  [rootfs     = '<path>']           -- for kernel+rootfs type: rootfs within source
  [checksum   = 'sha256:<hash>']    -- optional, verified after download
  [labels     = { key: value }]
```

Source URI formats:
```
/local/absolute/path/image.img
http://mirror.example.com/ubuntu-22.04-server.img
https://...
s3://bucket/path/image.img
gs://bucket/path/image.img
az://container/path/image.img
```

Import process:
1. Resolve source URI
2. Download to local image store (`/var/kvmql/images/`)
3. Verify checksum if provided (fail hard if mismatch)
4. Write metadata to registry `images` table with `status = 'available'`
5. `status = 'importing'` during download; `status = 'error'` on failure

### PUBLISH IMAGE

```sql
PUBLISH IMAGE '<image_id>'
  TO PROVIDER '<provider_id>'
```

Makes a locally imported image available on a cloud provider. For AWS: creates an AMI. For GCP: creates a machine image. For Azure: creates a VM image. Updates `images` table with the cloud reference after successful publish.

### REMOVE IMAGE

```sql
REMOVE IMAGE '<image_id>' [FORCE]
```

Fails if any running VM was created from this image unless FORCE is specified. Does not destroy running VMs — it removes the catalog entry only. Local image files are deleted. Cloud referces (AMIs etc.) are deregistered.

### ALTER MICROVM

```sql
ALTER MICROVM '<id>'
  SET memory_mb = <n>
  [, vcpus     = <n>]
  [, labels    = { key: value }]
  [, metadata  = { key: value }]
```

### ALTER VOLUME

```sql
ALTER VOLUME '<id>'
  SET labels   = { key: value }
  [, iops      = <n>]
```

### DESTROY MICROVM

```sql
DESTROY MICROVM '<id>' [FORCE]
```

Does not destroy attached volumes. Volumes are detached and remain in `available` state. To destroy volumes with the VM:

```sql
-- Query volumes first
SELECT id FROM volumes WHERE microvm_id = 'vm-abc';

-- Then destroy
DESTROY MICROVM 'vm-abc';
DESTROY VOLUME 'vol-001';
DESTROY VOLUME 'vol-002';
```

### PAUSE / RESUME

```sql
PAUSE  MICROVM '<id>'
RESUME MICROVM '<id>'
```

### SNAPSHOT / RESTORE

```sql
SNAPSHOT MICROVM '<id>'
  INTO '<destination>'
  [TAG '<label>']

RESTORE MICROVM '<id>' FROM '<snapshot_path>'
```

### WATCH

```sql
WATCH METRIC <field_list>
FROM  microvms
[WHERE <predicate>]
INTERVAL <duration>
```

### ADD/REMOVE/ALTER PROVIDER, ADD/ALTER/REMOVE CLUSTER

Unchanged from v0.1. See Section 6 in v0.1 for full syntax.

### ADD PRINCIPAL / GRANT / REVOKE

```sql
ADD PRINCIPAL
  id   = '<identifier>'
  type = 'user' | 'service' | 'token'
  auth = '<credential_ref>'

GRANT <verb_list>
  ON CLUSTER '<id>' | ON PROVIDER '<id>' | ON microvms | ON volumes | ON images
  [WHERE <predicate>]
  TO '<principal_id>'

REVOKE <verb_list>
  ON CLUSTER '<id>' | ON PROVIDER '<id>' | ON microvms | ON volumes | ON images
  FROM '<principal_id>'
```

### SET

```sql
SET execution_mode    = 'strict' | 'permissive'
SET query_timeout_ms  = <n>
SET fanout_concurrency = <n>
SET state_ttl_seconds = <n>
SET image_store_path  = '<path>'
```

### SHOW

```sql
SHOW PROVIDERS
SHOW CLUSTERS
SHOW CAPABILITIES [FOR PROVIDER '<id>']
SHOW GRANTS [FOR '<principal_id>']
SHOW IMAGES
SHOW VERSION
```

---

## 7. Control Plane Architecture

### Components

```
┌────────────────────────────────────────────┐
│                     Control Plane                         │
│                                                           │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────────┐  │
│  │ Listener │→ │  Parser  │→ │      Auth Check        │  │
│  │ TCP/Unix │  │  AST     │  │   (grants table)       │  │
│  └────────────────┘  └───────────┬────────────┘  │
│                                           ↓               │
│                                 ┌─────────────────┐       │
│                                 │     Planner     │       │
│                                 │ route / fan-out │       │
│                                 │ placement       │       │
│                                 └────────┬────────┘       │
│                                          ↓                │
│                                 ┌─────────────────┐       │
│                                 │  Notif Engine   │       │
│                                 │ INFO/WARN/ERROR │       │
│                                 └────────┬────────┘       │
│                                      ↓                │
│                                 ┌─────────────────┐       │
│                                 │  Image Resolver │       │
│                                 │ catalog lookup  │       │
│                                 └────────┬────────┘       │
│                                          ↓                │
│                                 ┌─â─────────┐       │
│                                 │     Router      │       │
│                                 │  agent fanout   │       │
│                                 └────────┬────────┘       │
│                                          ↓                │
│  ┌─────────────────────────────────────────────────â              DuckDB Registry                   │    │
│  │  providers, capabilities, clusters,                │    │
│  │  microvms, images, volumes, events, snapshots,    │    │
│  │  metrics, query_history, audit_log,               │    │
│  │  principals, grants                               │    │
│  └───────────────────────────────────────────────────┘    â──────────────────────────────────────────────────────────┘
```

### Request Lifecycle

```
1.  Client sends DSL text
2.  Listener receives, assigns request_id
3.  Parser produces AST
4.  Auth check: does principal's grants permit this verb on this scope?
    → DENIED: write audit_log, return AUTH_DENIED, stop
5.  Planner determines execution strategy
6.  Notification engine checks capapatibility per target provider
    → STRICT + unsupported: ERROR, stop
    → PERMISSIVE + unsupported: WARN, continue with supported subset
7.  Image resolver: if CREATE MICROVM, resolve image_id to provider-specific ref
    → Image not found: NOT_FOUND error, stop
    → Image not available on provider: suggest PUBLISH IMAGE, stop
8.  Router dispatches to agent(s)
9.  Agent executes against local KVM/Firecracker or cloud API
10. Agent returns result + state update
11. Registry updated
12. query_histwritten
13. audit_log row written (mutations only)
14. Response returned with result + notifications
```

---

## 8. Agent Architecture

### Responsibilities

- Execute DSL operations locally against KVM/Firecracker
- Manage local image store (download, store, verify)
- Push VM and volume state to control plane
- Report health via heartbeat
- Never make decisions — control plane plans, agent executes

### Agent Protocol

Transport: mTLS over TCP
Message format: JSON, length-prefixed (4-byte big-endian)

## Heartbeat (agent → control plane, every 5s)

```json
{
  "type": "heartbeat",
  "agent_id": "kvm.host-a",
  "timestamp": "2026-03-18T10:00:00Z",
  "load": {
    "cpu_pct": 42.1,
    "mem_used_mb": 16384,
    "vm_count": 12,
    "volume_count": 24,
    "image_store_used_gb": 180
  }
}
```

#### State Push (agent → control plane, every 10s or on event)

```json
{
  "type": "state_push",
  "agent_id": "kvm.host-a",
  "timestamp": "2026-03-18T10:00:00Z",
  "microvms": [ { "id": "vm-abc", "status": "runnin"cpu_pct": 22.4 } ],
  "volumes": [ { "id": "vol-001", "status": "attached", "microvm_id": "vm-abc" } ],
  "images": [ { "id": "ubuntu-22.04-lts", "status": "available", "size_mb": 512 } ]
}
```

#### Execute Request / Response

Unchanged from v0.1 with addition of `IMPORT_IMAGE`, `CREATE_VOLUME`, `ATTACH_VOLUME`, `DETACH_VOLUME`, `RESIZE_VOLUME`, `DESTROY_VOLUME` verb types.

### Agent Startup Sequence

```
1. Load agent.toml
2. Resolve credentials for control plane mTLS
3. Connect to control plane
4. Send REGISTER with agent_id, driver type, image store path, image store free space
5. Control plane replies ACK or REJECT
6. Sync local image catalog to control plane (list of available images + checksums)
7. Begin heartbeat loop (5s)
8. Begin state push loop (10s)
9. Begin listening for execute requests
```

### Agent Failure Handling

- Miss 1 heartbeat: no action
- Miss 3 heartbeats: mark provider `degraded`
- Miss 5 heartbeats: mark provider `offline`, all VMs and volumes `is_stale = true`

---

## 9. Driver Interface

```
Driver interface:

  capabilities()  → CapabilityManifest

  -- MicroVM operations
  create(params: CreateParams)        → Result<MicroVm, DriverError>
  destroy(id: String, force: bool)    → Result<(), DriverError>
  pause(id: String)                   → Result<(), DriverError>
  resume(id: String)                  → Result<(), DriverError>
  alter(id: String, params)           → Result<MicroVm, DriverError>
  snapshot(id, destination, tag)      → Result<Snapshot, DriverError, source)                 → Result<MicroVm, DriverError>
  list()                              → Result<Vec<MicroVm>, DriverError>
  get(id: String)                     → Result<MicroVm, DriverError>
  metrics(id: String)                 → Result<MetricSample, DriverError>

  -- Volume operations
  create_volume(params: VolumeParams) → Result<Volume, DriverError>
  destroy_volume(id, force: bool)     → Result<(), DriverError>
  attach_volume(vol_id, vm_id, dev)   → Result<(), DriverError>
  del_id, vm_id)        → Result<(), DriverError>
  resize_volume(id, size_gb: u64)     → Result<Volume, DriverError>
  list_volumes()                      → Result<Vec<Volume>, DriverError>

  -- Image operations
  import_image(params: ImageParams)   → Result<Image, DriverError>
  publish_image(image_id, provider)   → Result<String, DriverError>  -- returns cloud ref
  remove_image(id, force: bool)       → Result<(), DriverError>
  list_images()                       → Result<Vec<Image>, DriverEr_image(image_id)             → Result<ImageRef, DriverError>

  -- Health
  health_check()                      → Result<HealthStatus, DriverError>
```

### ImageRef

The concrete, provider-specific reference to an image used at VM boot time:

```json
{
  "image_id": "ubuntu-22.04-lts",
  "provider_id": "kvm.host-a",
  "resolved_type": "kernel+rootfs",
  "kernel_path": "/var/kvmql/images/vmlinux-5.10",
  "rootfs_path": "/var/kvmql/images/ubuntu-22-rootfs.ext4"
}
```

For AWS:
```json
{
  "image_id": "ub-22.04-lts",
  "provider_id": "aws.ap-southeast-1",
  "resolved_type": "ami",
  "cloud_ref": "ami-0abcdef1234567890"
}
```

---

## 10. Registry Schema

```sql
CREATE TABLE schema_version (
  version     INTEGER PRIMARY KEY,
  applied_at  TIMESTAMP NOT NULL,
  description TEXT
);

CREATE TABLE providers (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL CHECK (type IN ('kvm','aws','gcp','azure')),
  driver      TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'unknown'
              CHECK (status IN ('healthy','degraded','offline','unknown')),
  enabled     BOOLEAN NOT NULL DEFAULT true,
  host        TEXT,
  region      TEXT,
  auth_ref    TEXT NOT NULL,
  labels      JSON,
  latency_ms  INTEGER,
  added_at    TIMESTAMP NOT NULL DEFAULT NOW(),
  last_seen   TIMESTAMP
);

CREATE TABLE capabilities (
  provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  capability  TEXT NOT NULL,
  supported   BOOLEAN NOT NULL,
  notes       TEXT,
  PRIMARY KEY (provider_id, capability)
);

CREATE TABLE clusters (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cluster_members (
  cluster_id  TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
  provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  PRIMARY KEY (cluster_id, provider_id)
);

CREATE TABLE images (
  id               TEXT PRIMARY KEY,
  name             TEXT NOT NULL,
  os               TEXT NOT NULL CHECK (os IN ('linux','windows')),
  distro           TEXT NOT NULL,
  version          TEXT NOT NULL,
  arch             TEXT NOT NULL CHECK (arch IN ('x86_64','aarch64')),
  type             TEXT NOT NULL CHECK (type IN ('kernel+rootfs','disk','ami','machine_image')),
  provider_id      TEXT REFERENCES providers(id),  -- null = universal
  kernel_path      TEXT,
  rootfs_path      TEXT,
  disk_path        TEXT,
  cloud_ref        TEXT,
  source           TEXT NOT NULL,
  checksum_sha256  TEXT,
  size_mb          INTEGER,
  status           TEXT NOT NULL DEFAULT 'importing'
                   CHECK (status IN ('available','importing','error')),
  imported_at      TIMESTAMP NOT NULL DEFAULT NOW(),
  labels           JSON
);

CREATE TABLE volumes (
  id           TEXT PRIMARY KEY,
  provider_id  TEXT NOT NULL REFERENCES providers(id),
  microvm_id   TEXT,
  type         TEXT NOT NULL,
  size_gb      INTEGER NOT NULL,
  status       TEXT NOT NULL DEFAULT 'available'
               CHECK (status IN ('available','attached','detaching','error')),
  device_name  TEXT,
  iops         INTEGER,
  encrypted    BOOLEAN NOT NULL DEFAULT false,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
  labels       JSON
);

CREATE TABLE microvms (
  id            TEXT PRIMARY KEY,
  provider_id   TEXT NOT NULL REFERENCES providers(id),
  tenant        TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('running','stopped','paused','error','unknown')),
  image_id      TEXT REFERENCES images(id),
  vcpus         INTEGER,
  memory_mb     INTEGER,
  cpu_pct       REAL,
  mem_used_mb   INTEGER,
  net_rx_kbps   REAL,
  net_tx_kbps   REAL,
  hostname      TEXT,
  metadata      JSON,
  labels        JSON,
  created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
  last_seen     TIMESTAMP,
  is_stale      BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE snapshots (
  id           TEXT PRIMARY KEY,
  microvm_id   TEXT NOT NULL,
  provider_id  TEXT NOT NULL,
  destination  TEXT NOT NULL,
  tag          TEXT,
  size_mb      INTEGER,
  taken_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE events (
  id           TEXT PRIMARY KEY,
  event_time   TIMESTAMP NOT NULL DEFAULT NOW(),
  event_type   TEXT NOT NULL,
  microvm_id   TEXT,
  volume_id    TEXT,
  image_id     TEXT,
  provider_id  TEXT,
  principal    TEXT,
  detail       JSON
);

CREATE TABLE metrics (
  id           TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  microvm_id   TEXT NOT NULL,
  sampled_at   TIMESTAMP NOT NULL DEFAULT NOW(),
  cpu_pct      REAL,
  mem_used_mb  INTEGER,
  net_rx_kbps  REAL,
  net_tx_kbps  REAL
);

CREATE TABLE query_history (
  id              TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  executed_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  principal       TEXT,
  statement       TEXT NOT NULL,
  normalized_stmt TEXT,
  verb            TEXT NOT NULL,
  targets         JSON,
  duration_ms     INTEGER,
  status          TEXT NOT NULL CHECK (status IN ('ok','warn','error')),
  notifications   JSON,
  rows_affected   INTEGER,
  result_hash     TEXT
);

CREATE TABLE audit_log (
  id           TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  event_time   TIMESTAMP NOT NULL DEFAULT NOW(),
  principal    TEXT,
  action       TEXT NOT NULL,
  target_type  TEXT,
  target_id    TEXT,
  outcome      TEXT NOT NULL CHECK (outcome IN ('permitted','denied')),
  reason       TEXT,
  detail       JSON
);

CREATE TABLE principals (
  id          TEXT PRIMARY KEY,
  type        TEXT NOT NULL CHECK (type IN ('user','service','token')),
  auth_ref    TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
  enabled     BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE grants (
  id           TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
  verbs        JSON NOT NULL,
  scope_type   TEXT NOT NULL CHECK (scope_type IN ('cluster','provider','global')),
  scope_id     TEXT,
  conditions   TEXT,
  granted_at   TIMESTAMP NOT NULL DEFAULT NOW(),
  granted_by   TEXT
);
```

---

## 11. Credential Resolver

Three resolver types. Unchanged from v0.1. Reference formats:

```
env:<VAR>[,<VAR>...]
file:<absolute_path>
vault:<mount>/<path>[#<field>]
```

Security rules:
- Credentials never written to DuckDB under any table
- Credentials not logged at any level
- File resolver: fails if file is world-readable
- Vault: lease monitored and renewed; failure → provider marked `degraded`
- Resolution failure is a hard error; no fallback

---

## 12. Capability Model

### Parameter-to-Capability Map (Updated)

| Parameter / Operation | Required Capability |
|--------------------|-------------------|
| `kernel` in CREATE | `custom_kernel` |
| `ATTACH VOLUME` to running VM | `hotplug_volume` |
| `ALTER vcpus` on running VM | `alter_cpu_live` |
| `ALTER memory_mb` on running VM | `alter_memory_live` |
| `RESIZE VOLUME` on running VM | `volume_resize_live` |
| `RESTORE` with memory state | `snapshot` |
| `IMPORT IMAGE` | `image_import` |
| `PUBLISH IMAGE` | `image_publish` |

### Notification Codes

```
CAP_001  INFO   Parameter ignored (not applicable on provider)
CAP_002  WARN   Parameter dropped (unsupported, permissive mode)
CAP_003  WARN   Operation approximate on this provider
CAP_004  ERROR  Verb unsupported on provider
CAP_005  ERROR  No provider satisfies REQUIRE constraints
CAP_006  WARN   Live operation not supported; stop/start cycle will be used
CAP_007  ERROR  Image not available on target provider; run PUBLISH IMAGE first
```

---

## 13. Image Catalog

### Image Store

Images are stored locally on each KVM host at a configurable path (default `/var/kvmql/images/`). The agent manages the local store. The control plane registry tracks metadata.

Directory structure:
```
/var/kvmql/images/
├── ubuntu-22.04-lts/
│   ├── vmlinux-5.10.bin
│   ├── rootfs.ext4
│   └── manifest.json
├── alpine-3.19/
│   ├── vmlinux.bin
│   ├── rootfs.ext4
│   └── manifest.json
└── custom-acme-v2/
    ├── disk.img
    └── manifest.json
```

`manifest.json` per image:
```json
{
  "id": "ubuntu-22.04-lts",
  "typetfs",
  "kernel": "vmlinux-5.10.bin",
  "rootfs": "rootfs.ext4",
  "checksum_sha256": "abc123...",
  "size_mb": 512,
  "imported_at": "2026-03-18T10:00:00Z"
}
```

### Built-in Image Sources

The control plane ships with a curated image index referencing public mirrors for common distributions. An agent can be instructed to import from these without specifying a full URI:

```sql
-- Short-form import using built-in index
IMPORT IMAGE
  id     = 'ubuntu-22.04-lts'
  source = 'catalog:ubuntu-22.04-lts';

-- Explicit URI import
IMPORT IMAGE
  id     = 'ubuntu-22.04-lts'
  source = 'https://cloud-images.ubuntu.com/releases/22.04/release/ubuntu-22.04-server-cloudimg-amd64.img'
  checksum = 'sha256:abc123...';
```

Built-in catalog entries (initial set):

| Catalog ID | OS | Arch | Type |
|-----------|-----|------|------|
| `ubuntu-22.04-lts` | Ubuntu 22.04 | x86_64 | kernel+rootfs |
| `ubuntu-24.04-lts` | Ubuntu 24.04 | x86_64 | kernel+rootfs |
| `debian-12` | Debian 12 Bookworm | x86_64 | kernel+rootfs |
| `alpine-3.19` | Alpine Linux 3.19 | x86_64 | kernel+rootfs |
| `fedora-39` | Fedora 39 | x86_64 | kernel+rootfs |
| `ubuntu-22.04-lts-arm` | Ubuntu 22.04 | aarch64 | kernel+rootfs |
| `alpine-3.19-arm` | Alpine Linux 3.19 | aarch64 | kernel+rootfs |

### Cloud Image Mapping

When a universally-cataloged image is used with a cloud provider, the driver resolves it to the provider's equivalent:

| Catalog ID | AWS (ap-southeast-1) | GCP | Azure |
|-----------|---------------------|-----|-------|
| `ubuntu-22.04-lts` | `ami-0abcdef...` | `ubuntu-2204-lts` | `Canonical:UbuntuServer:22_04-lts:latest` |
| `debian-12` | `ami-0fedcba...` | `debian-12` | `Debian:debian-12:12:latest` |
| `alpine-3.19` | Not available — WARN | Not available — WARN | Not available — WARN |

If a catalog image has no cloud mapping and the provider is a cloud provider: ERROR with suggestion to use IMPORT IMAGE + PUBLISH IMAGE to make it available.

### Image Lifecycle

```
IMPORT IMAGE → status: importing
    ↓ download + verify
status:
    ↓
PUBLISH IMAGE TO PROVIDER 'aws...'
    ↓ cloud upload + registration
cloud_ref populated in images table
    ↓
CREATE MICROVM image = 'ubuntu-22.04-lts'
    ↓ image resolved to provider-specific ref
VM boots
    ↓
REMOVE IMAGE (fails if running VMs used this image, unless FORCE)
```

---

## 14. Volume Management

### Volume Types Per Provider

| DSL type | KVM | AWS | GCP | Azure |
|----------|-----|-----|-----|-------|
| `virtio-blk` | ✅ native | ❌ | ❌ | ❌ |
| `ebs` | ❌ | ✅ na ❌ | ❌ |
| `persistent-disk` | ❌ | ❌ | ✅ native | ❌ |
| `managed-disk` | ❌ | ❌ | ❌ | ✅ native |

In PERMISSIVE mode, specifying a KVM-specific type on AWS maps to `ebs` with WARN notification. In STRICT mode: ERROR.

### Root Volume vs Data Volume

The first VOLUME clause in CREATE MICROVM is the root volume. It is created alongside the VM and destroyed only when `DESTROY MICROVM FORCE` is used with explicit volume inclusion. By default `DESTROY MICROVM` detaches volumes and leaves them ilable`.

### Volume Naming

Device names inside the VM:

| KVM | Cloud |
|-----|-------|
| `/dev/vda` (root) | `/dev/xvda` or `/dev/sda` (root) |
| `/dev/vdb` (first data) | `/dev/xvdb` |
| `/dev/vdc` etc. | `/dev/xvdc` etc. |

If `AS '/dev/vdb'` is omitted from ATTACH, the next available device name is assigned automatically.

---

## 15. Query History

Every DSL execution (except SET, SHOW, internal health checks) writes one row to `query_history` after completion.

`normalized_stmt` strips literal values to `?` placeholders:
```
SELECT * FROM microvms WHERE tenant = ?  AND cpu_pct > ?
```

Credentials never appear in `statement` or `normalized_stmt`. Auth references stored as the reference string only.

Default retention: unlimited. Configurable:
```sql
SET query_history_retention = '90d'
```

---

## 16. Audit Log

Append-only. Never updated or deleted in place. If retention limits set, rows archived to file not deleted.

### Audit Event Types

```
VM_CREATED              VM_DESTROYED            VM_ALTERED
VM_PAUSED               VM_RESUMED
SNAPSHOT_TAKEN          SNAPSHOT_RESTORED
VOLUME_CREATED          VOLUME_DESTROYED        VOLUME_ATTACHED
VOLUME_DETACHED         VOLUME_RESIZED
IMAGE_IMPORTED          IMAGE_PUBLISHED         IMAGE_REMOVED
PROVIDER_ADDED          PROVIDER_REMOVED        PROVIDER_ALTERED
CLUSTER_CREATED         CLUSTER_ALTERED         CLUSTER_REMOVED
PRINCIPAL_ADDED         PRINCIPAL_REMOVED
GRANT_ADDED             GRANT_REVOKED
AUTH_SUCCESS            AUTH_DENIED
CREDENTIAL_RESOLVED     CREDENTIAL_FAILED
AGENT_CONNECTED         AGENT_DISCONNECTED      AGENT_UNHEALTHY
CONFIG_CHANGED
```

### Guaranteed Write

Audit log write occurs before response is returned to client. If audit write fails: operation is aborted, `AUDIT_WRITE_FAILED` error returned. No operation completes without an audit record.

---

## 17. Access Control

### Model

Direct verb grants on scopes with optional WHERE conditions. No roles.

Grant scopes now include `volumes` and `images` in addition to `microvms`, `CLUSTER`, and `PROVIDER`.

### Auth Check Algorithm

```
Given: principal_id, verb, target

1. Load all grants for principal_id from grants table
2. For each grant:
   a. Does grant.verbs contain verb?
   b. Does grant.scope_type match target type?
   c. Does grant.scope_id match target id (or scope_type = 'global')?
   d. If grant has conditions: evaluate condition against target fields
   e. All pass → PERMIT
3. No grant permits → DENY
```

### Bootstrap Admin

On first run (empty principals table) bootstrapncipal auto-created:
```
id:   admin
type: user
auth: env:KVMQL_ADMIN_TOKEN
```

Disable after real principals are provisioned:
```sql
ALTER PROVIDER ...  -- or
ALTER PRINCIPAL 'admin' SET enabled = false;  -- post-v1 verb
```

---

## 18. Configuration Bootstrap

### kvmql.toml

```toml
[control_plane]
bind              = "0.0.0.0:9090"
unix_socket       = "/var/run/kvmql/control.sock"
advertise_addr    = ""

[registry]
path              = "/var/kvmql/state.db"

[images]
store_path        = "/var/kvmql/images"
catalog_url       = "https://catalog.kvmql.io/v1/index.json"  -- built-in image index

[auth]
ca                = "/etc/kvmql/certs/ca.crt"
cert              = "/etc/kvmql/certs/server.crt"
key               = "/etc/kvmql/certs/server.key"

[vault]
address           = ""
auth              = ""

[runtime]
execution_mode        = "permissive"
state_ttl_seconds     = 30
query_timeout_ms      = 5000
fanout_concurrency    = 10
agent_heartbeat_s     = 5
agent_stale_threshold = 5
metrics_retention_h   = 1

[logging]
level             = "info"
format            = "json"
path              = "/var/log/kvmql/kvmql.log"
```

### agent.toml

```toml
[agent]
id                = "kvm.host-a"

[control_plane]
address           = "10.0.0.1:9090"
ca                = "/etc/kvmql/certs/ca.crt"
cert              = "/etc/kvmql/certs/agent.crt"
key               = "/etc/kvmql/certs/agent.key"

[driver]
type              = "firecracker"
api_socket        = "/run/firecracker.sock"

[images]
store_path        = "/var/kvmql/images"
max_store_gb      = 500

[runtime]
heartbeat_s       = 5
state_push_s      = 10
reconnect_max_s   = 60

[logging]
level             = "info"
format            = "json"
path              = "/var/log/kvmql/agent.log"
```

### Environment Variable Overrides

```
KVMQL_CONFIG
KVMQL_BIND
KVMQL_UNIX_SOCKET
KVMQL_REGISTRY_PATH
KVMQL_IMAGE_STORE_PATH
KVMQL_EXECUTION_MODE
KVMQL_LOG_LEVEL
KVMQL_ADMIN_TOKEN
KVMQL_VAULT_ADDRESS
KVMQL_VAULT_TOKEN
```

---

## 19. CLI and Shell Specification

### CLI Commands

```bash
# Execute a single DSL statement
kvmql "<statement>"

# Execute a DSL file
kvmql exec <file.kvmql>

# Launch interactive shell (REPL)
kvmql shell

# Shortcuts
kvmql cluster status <cluster_id>
kvmql provider status <provider_id>
kvmql images [--provider <id>]
kvmql version
kvmql init
```

### Output Formats

```bash
kvmql --format table  "<statement>"    # default
kvmql --format json   "<statement>"
kvmql --format csv    "<statement>"
kvmql --format raw    "<statement>"
```

### Connection

```bash
kvmql --socket /var/run/kvmql/control.sock "<statement>"
kvmql --host 10.0.0.1 --port 9090 "<statement>"
```

### Shell (REPL)

The shell is a psql-style interactive REPL. It is a readline loop that accumulates input until `;`, sends DSL to the control plane over a socket, receives a result envelope, renders it, and loops.

```
$ kvmql shell

KVMQL v0.2 | unix:/var/run/kvmql/control.sock | cluster: none
Type \h for help, \q to quit

kvmql> SELECT id, tenant, status, image_id
     > FROM microvms
     > WHERE status = 'running';

┌──────────┬────────┬─────────┬────────────────────┐
│ id       │ tenant │ status  │ image_id           │
├──────────┼────────┼─────────┼────────────────────┤
│ vm-abc   │ acme   │ running │ ubuntu-22.04-lts   │
│ vm-def   │ acme   │ running │ ubuntu-22.04-lts    │ beta   │ running │ alpine-3.19        │
└──────────┴────────┴─────────┴────────────────────┘
3 rows (12ms)

kvmql>
```

Notifications appear above the result table:

```
kvmql> CREATE MICROVM image = 'ubuntu-22.04-lts' vcpus = 2 memory_mb = 512
     > tenant = 'acme' ON PROVIDER 'aws.ap-southeast-1';

WARN  CAP_002  kernel parameter not applicable on 'aws'; ignored
INFO  RTE_001  Request routed to provider 'aws.ap-southeast-1'
OK    VM_CREATED  vm-new123 created (1847ms)

1 row affected
```

### Backslash Meta-Commands

```
\q              Quit
\h              List all verbs
\h <verb>       Help for specific verb (e.g. \h CREATE)
\d              List all nouns
\d <noun>       Describe noun (fields + types)
\c              Show current connection and session context
\cluster <id>   Set default cluster context for all queries
\provider <id>  Set default provider context for all queries
\timing         Toggle query timing display
\x              Toggle expanded (vertical) output mode
\o <file>       Redirect output to file (append)
\o              Reset output to stdout
\i <file>       Execute DSL file
\e              Open current buffer in $EDITOR; execute on save+close
\f <char>       Set field separator (default: pipe for table mode)
\images         Shortcut for SHOW IMAGES
\providers      Shortcut for SHOW PROVIDERS
\clusters       Shortcut for SHOW CLUSTERS
```

### Session Context (\cluster / \provider)

Setting a cluster or provider context scopes all subsequent queries that lack an explicit `ON` clause:

```
kvmql> \cluster prod
Context set: cluster 'prod'
Subsequent queries will use ON CLUSTER 'prod' unless overridden

kvmql> SELECT * FROM microvms WHERE tenant = 'acme';
-- Executes as: SELECT * FROM microvms ON CLUSTER 'prod' WHERE tenant = 'acme'

kvmql> SELECT * FROM microvms ON PROVIDER 'kvm.host-a' WHERE tenant = 'acme';
-- ON PROVIDER overrides the cluster context for this statement only
```

### Expanded Output (\x)

```
kvmql> \x
Expanded output on

kvmql> SELECT * FROM microvms LIMIT 1;

─[ row 1 ]──────────────────────────────
id          │ vm-abc
tenant      │ acme
status      │ running
image_id    │ ubuntu-22.04-lts
vcpus       │ 2
memory_mb   │ 512
cpu_pct     │ 22.4
mem_used_mb │ 480
created_at  │ 2026-03-18T08:00:00Z
is_stale    │ false
```

### Shell History

Persisted to `~/.kvmql/history`. Maximu10,000 entries. Statements with credential values are excluded from history (detected by presence of `auth =` parameter).

### Tab Completion

Completion candidates sourced from live registry:

| Context | Candidates |
|---------|-----------|
| First token | All verbs + `\` commands |
| After `FROM` | All noun names |
| After `ON PROVIDER` | provider IDs from registry |
| After `ON CLUSTER` | cluster IDs from registry |
| After `MICROVM` (id position) | microvm IDs from registry |
| After `VOLUME` (id position) | volume IDs from registry |
| After `image =` | image IDs from registry |
| After `WHERE <field>` | `=`, `!=`, `>`, `<`, `IN` |

### Non-Interactive Usage (Scripting)

```bash
# Single statement
kvmql "SELECT id FROM microvms WHERE status = 'running';"

# Pipe
echo "SELECT count(*) FROM microvms;" | kvmql

# File
kvmql exec provision.kvmql

# Script usage
RUNNING=$(kvmql --format csv "SELECT count(*) FROM microvms WHERE status='running';")
echo "Running VMs: $RUNNING"

# Combine with jq
kvmql --format json "SELECT * FROM microvms WHERE tenant='acme';" | jq '.[] | .id'
```

### cluster status Output

```bash
$ kvmql cluster status prod

CLUSTER: prod
┌─────────────────────┬──────────┬────────────┬───────────┬──────────────────────────┐
│ Provider            │ Status   │ Latency    │ VMs       │ Capabilities             │
â───────────┼──────────┼────────────┼───────────┼──────────────────────────┤
│ kvm.host-a          │ healthy  │ 2ms        │ 12        │ full                     │
│ kvm.host-b          │ healthy  │ 3ms        │ 8         │ full                     │
│ aws.ap-southeast-1  │ healthy  │ 87ms       │ 4         │ partial (no cus│
│ gcp.asia-east1      │ degraded │ timeout    │ unknown   │ unknown                  │
└─────────────────────┴──────────┴────────────┴───────────┴──────────────────────────┘
```

---

## 20. Notification System

Every DSL execution returns a result envelope:

```json
{
  "request_id": "req-uuid",
  "status": "ok | warn | enotifications": [
    {
      "level": "INFO | WARN | ERROR",
      "code": "CAP_002",
      "provider_id": "aws.ap-southeast-1",
      "message": "Parameter 'kernel' not supported on 'aws'; ignored"
    }
  ],
  "result": { ... },
  "rows_affected": 1,
  "duration_ms": 142
}
```

Status rules:
- `ok`: completed without WARN or ERROR
- `warn`: at least one WARN; operation completed with degraded behaviour
- `error`: at least one ERROR; operation did not complete

### Full Notification Code Reference

```
-- Capability
CAP_001  INFO   Parameter ignored (not applicable)
CAP_002  WARN   Parameter dropped (unsupported, permissive mode)
CAP_003  WARN   Operation approximate on provider
CAP_004  ERROR  Verb unsupported on provider
CAP_005  ERROR  No eligible provider for REQUIRE constraints
CAP_006  WARN   Live operation not supported; stop/start cycle will be used
CAP_007  ERROR  Image not available on target provider

-- Routing
RTE_001  INFO   Request routed to provider '<id>'
RTE_002  WARN   Provider '<id>' unreachable; partial results returned
RTE_003  ERROR  Owning provider for VM '<id>' is offline

-- State
STA_001  INFO   VM state synced from live provider
STA_002  WARN   VM state is stale (last seen: <timestamp>)
STA_003  WARN   Registry TTL exceeded; result may be outdated

-- Image
IMG_001  INFO   Image '<id>' resolved to '<cloud_ref>'
IMG_002  WARN   Image not in catalog; proceeding with explicit paths
IMG_003  ERROR  Image '<id>' not found in registry
IMG_004  ERROR  Checksum mismatch on import
IMG_005  INFO   Image import complete
IMG_006  INFO   Image published to provider as '<cloud_ref>'

-- Volume
VOL_001  INFO   Volume '<id>' created
VOL_002  WARN   Hotplug not supported; stop/start required for attach
VOL_003  ERROR  Cannot destroy attached volume without FORCE
VOL_004  INFO   Volume detached successfully

-- Auth
AUTH_001 ERROR  Principal '<id>' not found
AUTH_002 ERROR  Verb not permitted for principal on this scope
AUTH_003 ERROR  Condition check failed for grant

-- Credential
CRED_001 INFO   Credential resolved
CRED_002 ERROR  Credential reference unresolvable
CRED_003 ERROR  Credential file has insecure permissions
CRED_004 ERROR  Vault lease renewal failed

-- Agent
AGT_001  INFO   Agent '<id>' connected
AGT_002  WARN   Agent '<id>' degraded
AGT_003  ERROR  Agent '<id>' offline

-- Execution
EXE_001  ERROR  Query timeout exceeded
EXE_002  ERROR  Parse error: <detail>
EXE_003  ERROR  Audit write failed; operation aborted
```

---

## 21. Streaming Protocol

`WATCH` is the only streaming verb.

### SSE Format (HTTP interface)

```
Content-Type: text/event-stream

data: {"microvm_id":"vm-abc","sampled_at":"2026-03-18T10:00:00Z","cpu_pct":42.1,"mem_used_mb":512}
data: {"microvm_id":"vm-abc","sampled_at":"2026-03-18T10:00:05Z","cpu_pct":44.3,"mem_used_mb":516}
```

### Native Frame Protocol (TCP/Unix)

4-byte big-endian length header + JSON payload.

```json
{
  "type": "metric_sample",
  "microvm_id": "vm-abc",
  "sampled_at": "2026-03-18T10:00:00Z",
  "cpu_pct": 42.1,
  "mem_used_mb": 512,
  "net_rx_kbps": 100.2,
  "net_tx_kbps": 55.1
}
```

Stream termination frame:
```json
{ "type": "stream_end", "reason": "vm_destroyed | provider_offline | shutdown | client_disconnect" }
```

---

## 22. Error Reference

| Error | Code | HTTP |
|-------|------|------|
| Parse error | PARSE_ERROR | 400 |
| Auth denied | AUTH_DENIED | 403 |
| Not found | NOT_FOUND | 404 |
| Image not found | IMAGE_NOT_FOUND | 404 |
| Volume not found | VOLUME_NOT_FOUND | 404 |
| Capability unsupported | CAP_UNSUPPORTED | 422 |
| No eligible provider | NO_ELIGIBLE_PROVIDER | 422 |
| Image not on provider | IMAGE_NOT_ON_PROVIDER | 422 |
| Volume attached | VOLUME_ATTACHED | 422 |
| Checksum mismatch | CHECKSUM_MISMATCH | 422 |
| Provider offline | PROVIDER_OFFLINE | 503 |
| Agent timeout | AGENT_TIMEOUT | 504 |
| Audit write failed | AUDIT_WRITE_FAILED | 500 |
| Credential failure | CREDENTIAL_FAILED | 500 |
| Internal error | INTERNAL_ERROR | 500 |

All errors return:
```json
{
  "error": {
    "code": "IMAGE_NOT_ON_PROVIDER",
    "message": "Image 'alpine-3.19' is not available on 'aws.ap-southeast-1'. Run PUBLISH IMAGE 'alpine-3.19' TO PROVIDER 'aws.ap-southeast-1' first.",
    "request_id": "req-uuid",
    "notifications": []
  }
}
```

---

## 23. Testing Requirements

### Unit Tests

- Parser: all grammar examples; malformed inputs return structured errors
- Image resolver: local path, HTTP, S3, catalog short-form, cloud mapping, missing image
- Volume driver: create, attach, detach, resize, destroy; destroy-while-attached rejection
- Credential resolver: all three types; insecure permissions; missing env var; Vault unreachable
- Capability resolver: STRICT abort; PERMISSIVE continue with WARN; image capability check
- Auth check: permit; deny; tenant-scoped; revoked grant; volume/image scope

### Integration Tests

**Single host, full lifecycle:**
```
IMPORT IMAGE → SELECT FROM images → CREATE MICROVM (with image + volumes)
→ SELECT FROM microvms → SELECT FROM volumes
→ ATTACH VOLUME → DETACH VOLUME
→ ALTER MICROVM → PAUSE → RESUME
→ SNAPSHOT → RESTORE
→ DESTROY MICROVM → DESTROY VOLUME
→ REMOVE IMAGE
```

**Multi-host:**
- SELECT returns VMs from both hosts
- PLACEMENT POLICY routes CREATE to correct host
- DESTROY routes to owning host
- Agent disconnect: VMs and volumes marked stale

**Image catalog:**
- Import from local path: registry populated, file in store
- Import from HTTP: download + checksum verify + registry
- Import with wrong checksum: hard fail, no registry entry
- PUBLISH IMAGE to AWS: cloud_to images table
- CREATE MICROVM with catalog image on cloud: resolved to cloud_ref

**Cloud drivers (AWS mock):**
- CREATE with `kernel` emits WARN in PERMISSIVE, ERROR in STRICT
- SELECT returns VMs across KVM + AWS
- Images from AWS AMI catalog queryable

**Shell:**
- Multi-line statement executes on `;`
- Tab completion returns candidates from registry
- `\cluster` scopes subsequent queries
- `\x` switches to expanded output
- `\o file.csv` redirects output

### Load Tests

- 100 concurrent clients SELECT against 10,000 microvms: p99 < 200ms
- Fan-out across 10 agents: results merged within 500ms
- Agent state push: 1,000 VMs + 2,000 volumes per agent, 10s interval, p99 write < 50ms
- Image import: 10 concurrent imports from HTTP, no corruption in local store

### Security Tests

- Credential values absent from query_history and audit_log
- Principal with no grants receives AUTH_DENIED on any verb
- Tenant-scoped grant: SELECT returns only tenant's VMs, volumes, images
- DESTROY VOLUME without FORCE on attached volume: rejected
- REMOVE IMAGE in use by running VM without FORCE: rejected

---

## Appendix A — Complete Example DSL Session

```sql
-- 1. Add providers
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

-- 2. Group into cluster
ADD CLUSTER 'prod'
 EMBERS = ['kvm.host-a', 'aws.ap-southeast-1'];

-- 3. Import an OS image
IMPORT IMAGE
  id      = 'ubuntu-22.04-lts'
  source  = 'catalog:ubuntu-22.04-lts';

-- 4. Check image is ready
SELECT id, status, size_mb, arch
FROM   images
WHERE  id = 'ubuntu-22.04-lts';

-- 5. Create a standalone data volume
CREATE VOLUME
  id      = 'vol-acme-data'
  size_gb = 20
  type    = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';

-- 6. Create a VM with image + root volume + data volume
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

-- 7. Query across cluster
SELECT id, provider_id, tenant, status, image_id, vcpus, memory_mb
FROM   microvms
ON CLUSTER 'prod'
WHERE  tenant = 'acme'
ORDER BY cpu_pct DESC;

-- 8. Check volumes
SELECT id, microvm_id, size_gb, status, device_name
FROM   volumes
WHERE  microvm_id = 'vm-abc';

-- 9. Add another volume live (requires hotplug_volume capability)
CREATE VOLUME
  id      = 'vol-acme-logs'
  size_gb = 50
  type    = 'virtio-blk'
  ON PROVIDER 'kvm.host-a';

ATTACH VOLUME 'vol-acme-logs'
  TO MICROVM 'vm-abc'
  AS '/dev/vdc';

-- 10. Watch metrics
WATCH METRIC cpu_pct, mem_used_mb, net_rx_kbps
FROM  microvms
WHERE tenant = 'acme'
INTERVAL 5s;

-- 11. Snapshot before upgrade
SNAPSHOT MICROVM 'vm-abc'
  INTO 's3://epiphyte-snapshots/acme/vm-abc-20260318'
  TAG  'pre-upgrade';

-- 12. Access control for a tenant operator
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

-- 13. Publish image to AWS for burst workloads
PUBLISH IMAGE 'ubuntu-22.04-lts'
  TO PROVIDER 'aws.ap-southeast-1';

-- 14. Create a VM on AWS using the same image
CREATE MICROVM
  tenant    = 'acme'
  vcpus     = 2
  memory_mb = 1024
  image     = 'ubuntu-22.04-lts'
  VOLUME ( size_gb = 20 type = 'ebs' )
  ON PROVIDER 'aws.ap-southeast-1';

-- 15. Audit trail
SELECT event_time, principal, action, target_type, target_id, outcome
FROM   audit_log
WHERE  event_time > NOW() - INTERVAL '24h'
ORDER BY event_time DESC;

-- 16. Query history: recent errors
SELECT executed_at, principal, verb, statement, duration_ms
FROM   query_history
WHERE  status = 'error'
ORDER BY executed_at DESC
LIMIT  20;

-- 17. Resize a volume
RESIZE VOLUME 'vol-acme-data' TO 40 GB;

-- 18. Clean up
DETACH VOLUME 'vol-acme-logs' FROM MICROVM 'vm-abc';
DESTROY MICROVM 'vm-abc';
DESTROY VOLUME 'vol-acme-data' FORCE;
DESTROY VOLUME 'vol-acme-logs';
REMOVE IMAGE 'ubuntu-22.04-lts';
```

---

## Appendix B — Known Limitations in v1

1. **No HA control plane.** Single control plane. Raft-based HA is v2.
2. **No live migration.** Moving a running VM between hosts is not supported.
3. **Metric retention is rolling window.** Historical metric analysis requires external pipeline.
4. *ost estimation absent.** `COST_ESTIMATE()` not implemented. `PLACEMENT POLICY = 'least_cost'` returns `NOT_IMPLEMENTED`.
5. **DuckDB single-writer.** Control plane serializes all registry writes. Not a bottleneck at expected v1 scale.
6. **Vault only.** AWS Secrets Manager, GCP Secret Manager, Azure Key Vault are post-v1.
7. **Built-in image catalog is static.** Catalog index is bundled with the binary. Live catalog sync is post-v1.

---

## Appendix C — Deliberately Deferred

### SDK / Client Library —t-v1

The DSL over TCP/Unix socket is the programmatic interface. Build an SDK when a concrete external consumer requires it.

### Export — Not a Feature

The DSL and CLI `--format csv/json` are the export mechanism. Integration adapters (Prometheus, SIEM) are a separate post-v1 concern.

### Backup and Restore of the Registry — Ops Procedure

DuckDB is a single file. Backup is a file copy. Document the procedure; do not build tooling.

### Rate Limiting — Defer Until Multi-Tenant Public Access

All vcipals are explicitly provisioned. Rate limiting applies when the control plane is exposed to external or anonymous consumers.

### Cost Estimation — Post-v1

Pricing table, COST_ESTIMATE() function, and cloud pricing API sync are post-v1. Data model is sketched in Section 1.

---

*End of Specification*

*Version: 0.2 | Status: Draft — Ready for Implementation | Owner: Epiphyte Corp Engineering*
