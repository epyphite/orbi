# Contributing to Orbi

Thank you for your interest in contributing to Orbi. This guide explains how to set up the project, understand the codebase, and submit changes.

## Development Setup

```bash
git clone https://github.com/epyphite/orbi.git
cd orbi
cargo build
cargo test --workspace
```

**Requirements:**
- Rust 1.75+ (2021 edition)
- SQLite (bundled via `rusqlite`)
- Optional: `az` CLI (for Azure integration tests), `aws` CLI (for AWS)

## Project Structure

Orbi is organized as a Cargo workspace with 8 crates (the internal crates keep their historical `kvmql-*` prefix because KVMQL is the name of the DSL engine):

| Crate | Purpose |
|-------|---------|
| `kvmql-common` | Shared types, notification codes, resource type definitions, config |
| `kvmql-parser` | Lexer (logos), AST definitions, recursive descent parser |
| `kvmql-registry` | SQLite registry, schema migrations, CRUD operations |
| `kvmql-driver` | Driver trait + implementations (Azure, AWS, GCP, Firecracker, Mock, Simulation) |
| `kvmql-auth` | 9 credential backends, access control checks |
| `kvmql-engine` | Execution pipeline, EXPLAIN, ROLLBACK, plan management |
| `kvmql-agent` | Per-host agent (heartbeat, state push protocol) |
| `kvmql-cli` | CLI binary, interactive REPL shell, output formatters |

**Dependency flow:** `common <- parser <- registry <- driver <- auth <- engine <- cli`

## Adding a New Resource Type

1. **Define the type** in `crates/kvmql-common/src/resource_types.rs`:
   ```rust
   ResourceTypeDef {
       name: "my_resource",
       description: "My New Resource",
       required_params: &["id", "size"],
       optional_params: &["tags"],
   },
   ```

2. **Add provisioning logic** in the appropriate driver under `crates/kvmql-driver/src/`. For Azure resources, add to `azure/resources.rs`. For AWS, add to `aws/resources.rs`.

3. **Add tests** for the new type:
   - Parser test: verify `CREATE RESOURCE 'my_resource' id = 'test' size = 10;` parses correctly
   - Resource type test: verify `get_resource_type("my_resource").is_some()`
   - Engine test: verify end-to-end creation with the mock driver

4. **Update documentation** in `MANUAL.md` under "DSL Reference -- Managed Resources".

## Adding a New Credential Backend

1. **Create the resolver** in `crates/kvmql-auth/src/vault_<name>.rs`:
   - Implement a struct with a `pub fn resolve(reference: &str) -> Result<String, CredentialError>` method
   - Handle errors with descriptive messages and remediation advice

2. **Add the scheme** to the dispatch in `crates/kvmql-auth/src/resolver.rs`:
   ```rust
   "my-scheme" => MyResolver::resolve(rest),
   ```

3. **Add tests** in the resolver module.

4. **Update documentation** in `MANUAL.md` Section 15 (Security).

## Adding a New Cloud Driver

1. **Create a module** under `crates/kvmql-driver/src/<cloud>/`:
   - `mod.rs` -- Driver struct implementing the `Driver` trait
   - `resources.rs` -- Resource provisioning via CLI or SDK

2. **Implement the `Driver` trait** with at least:
   - `create_vm`, `destroy_vm`, `list_vms`
   - `create_resource`, `destroy_resource`
   - `capabilities` -- report what the driver supports

3. **Register the driver** in the driver factory (`crates/kvmql-driver/src/lib.rs`).

4. **Add tests** using the mock infrastructure.

## Running Tests

```bash
# All tests
cargo test --workspace

# Individual crates
cargo test -p kvmql-parser
cargo test -p kvmql-engine
cargo test -p kvmql-driver
cargo test -p kvmql-auth
cargo test -p kvmql-registry
cargo test -p kvmql-common

# With output
cargo test --workspace -- --nocapture
```

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` -- there should be no warnings
- Every new feature needs tests
- Error messages must include remediation advice (tell the user what to do, not just what went wrong)

## Pull Request Process

1. Fork the repository and create a branch from `main`
2. Add tests for new features or bug fixes
3. Run `cargo test --workspace` and confirm all tests pass
4. Run `cargo clippy` and `cargo fmt --check`
5. Update `MANUAL.md` if adding user-facing features
6. Submit a PR with a clear description of what changed and why

## Commit Messages

Follow conventional commit style:

```
feat: add GCP Compute Engine driver
fix: credential file permission check on macOS
docs: update MANUAL.md with new resource types
test: add parser tests for ROLLBACK variants
chore: update rusqlite to 0.33
```

## Reporting Issues

When filing a bug report, include:

- Orbi version (`orbi version`)
- OS and architecture
- The DSL statement that failed
- Full error output (with notification codes)
- Expected behavior

## License

By contributing, you agree that your contributions will be licensed under the Apache License, Version 2.0.
