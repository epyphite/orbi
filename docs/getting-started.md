# Getting Started with Orbi

## Installation

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/epyphite/orbi/main/install.sh | sh
```

### GitHub Releases

Download a prebuilt binary from the [Releases page](https://github.com/epyphite/orbi/releases). Extract it and place the `orbi` binary somewhere on your `$PATH`.

### Build from source

```bash
git clone https://github.com/epyphite/orbi.git
cd orbi
cargo build --release
./target/release/orbi version
```

The compiled binary is at `target/release/orbi`.

---

## First commands

Verify the installation:

```bash
orbi version
```

Run a statement directly (no credentials needed in simulate mode):

```bash
orbi --simulate "SELECT * FROM providers;"
```

Launch the interactive shell:

```bash
orbi shell
```

---

## The registry

Orbi stores all state -- providers, resources, audit log, query history, applied files -- in an embedded SQLite database called the **registry**. Each `.db` file is a self-contained snapshot of an infrastructure.

The default registry lives at `~/.orbi/registry.db`. Override it per-invocation:

```bash
# Environment variable
ORBI_REGISTRY=./mystack.db orbi "SELECT * FROM providers;"

# CLI flag
orbi --registry ./mystack.db "SELECT * FROM providers;"
```

Because the registry is plain SQLite, you can copy it, back it up, or inspect it with any SQLite client.

---

## Adding your first provider

A **provider** is a cloud backend that Orbi manages resources against. Register one with `ADD PROVIDER`:

### Azure

```sql
ADD PROVIDER
  id   = 'azure-prod'
  type = 'azure'
  auth = 'env:AZURE_SUBSCRIPTION_ID';
```

### AWS

```sql
ADD PROVIDER
  id     = 'aws-prod'
  type   = 'aws'
  region = 'us-east-1'
  auth   = 'env:AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY';
```

### Cloudflare

```sql
ADD PROVIDER
  id   = 'cf'
  type = 'cloudflare'
  auth = 'env:CLOUDFLARE_API_TOKEN';
```

Use `IF NOT EXISTS` to make provider registration idempotent:

```sql
ADD IF NOT EXISTS PROVIDER id='azure-prod'
  type='azure'
  auth='env:AZURE_SUBSCRIPTION_ID';
```

Verify your providers:

```sql
SELECT * FROM providers;
```

---

## Creating your first resource

Once a provider is registered, create a managed resource on it:

```sql
CREATE RESOURCE 'postgres'
  id         = 'my-db'
  version    = '16'
  sku        = 'Standard_B2s'
  storage_gb = 64
  ON PROVIDER 'azure-prod';
```

Query it back:

```sql
SELECT id, resource_type, status FROM resources;
```

---

## Simulate mode

The `--simulate` flag runs every statement without making real cloud API calls. Resources are created in the registry with a simulated status. This is useful for:

- Testing DSL files before applying them to real infrastructure.
- Exploring Orbi without cloud credentials.
- CI/CD dry-run validation.

```bash
# Simulate a single statement
orbi --simulate "CREATE RESOURCE 'postgres' id='test-db' version='16';"

# Simulate an entire file
orbi --simulate exec examples/azure-stack.kvmql
```

No credentials are required in simulate mode.

---

## The exec command

`orbi exec` runs a `.kvmql` file containing one or more DSL statements:

```bash
orbi exec myfile.kvmql
```

### Idempotency with IF NOT EXISTS

Use `IF NOT EXISTS` on `CREATE` and `ADD` statements so re-running a file does not produce duplicates:

```sql
ADD IF NOT EXISTS PROVIDER id='azure-prod'
  type='azure'
  auth='env:AZURE_SUBSCRIPTION_ID';

CREATE RESOURCE 'postgres'
  id = 'prod-db'
  version = '16'
  ON PROVIDER 'azure-prod';
```

### Applied files tracking

Orbi records every executed file in the `applied_files` table, including the file hash and timestamp. Query it to see what has been applied:

```sql
SELECT * FROM applied_files;
```

This lets you build deployment pipelines that skip files already applied to a given registry.

---

## Interactive shell

Launch the shell with:

```bash
orbi shell
```

The shell provides tab completion for keywords, nouns, and backslash commands. Key features:

| Command | Description |
|---------|-------------|
| `\q` | Quit the shell |
| `\h` | List all DSL verbs |
| `\h <VERB>` | Help for a specific verb |
| `\d` | List all queryable nouns |
| `\d <NOUN>` | Show field schema for a noun |
| `\providers` | Shortcut for `SHOW PROVIDERS;` |
| `\timing` | Toggle execution timing display |
| `\x` | Toggle expanded (vertical) display |

Multiple statements can be entered on separate lines; each is executed when terminated with `;`.

Output formats are configurable at launch:

```bash
orbi --format json shell
orbi --format csv shell
```
