mod formatter;
mod meta_commands;
mod shell;

use std::sync::Arc;

use clap::{Parser, Subcommand};

use kvmql_driver::mock::MockDriver;
use kvmql_driver::simulate::SimulationDriver;
use kvmql_engine::context::EngineContext;
use kvmql_engine::executor::Executor;
use kvmql_registry::Registry;

use formatter::{print_result, OutputFormat};

#[derive(Parser)]
#[command(name = "orbi", version = env!("CARGO_PKG_VERSION"), about = "Orbi — SQL-like DSL for infrastructure management (engine: kvmql)")]
struct Cli {
    /// DSL statement to execute
    statement: Option<String>,

    /// Output format (table, json, csv, raw)
    #[arg(long, default_value = "table")]
    format: String,

    /// Registry path
    #[arg(long, default_value = "~/.kvmql/state.db")]
    registry: String,

    /// Execute in dry-run mode (show what would happen without doing it)
    #[arg(long)]
    dry_run: bool,

    /// Run in simulation mode — no cloud calls, realistic fake responses
    #[arg(long)]
    simulate: bool,

    /// Environment name (shortcut for --registry ~/.kvmql/envs/<name>.db)
    #[arg(long)]
    env: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a DSL file
    Exec {
        /// Path to the .kvmql file
        file: String,
        /// Force re-execution even if file was already applied
        #[arg(long)]
        force: bool,
    },
    /// Launch interactive shell
    Shell,
    /// Show version
    Version,
    /// Initialize a new KVMQL instance
    Init,
    /// Generate an execution plan
    Plan {
        /// DSL file or inline statement to plan
        source: String,
        /// Output to file (in addition to storing in registry)
        #[arg(short, long)]
        output: Option<String>,
        /// Plan name for identification
        #[arg(long)]
        name: Option<String>,
    },
    /// Apply a plan (from registry ID or file)
    Apply {
        /// Plan ID (from registry) or path to plan file
        target: String,
    },
    /// Approve a pending plan
    Approve {
        /// Plan ID to approve
        id: String,
    },
    /// List plans
    Plans {
        /// Filter by status: pending, approved, applied, failed
        #[arg(long)]
        status: Option<String>,
    },
    /// Manage environments (registry files)
    Env {
        #[command(subcommand)]
        action: EnvAction,
    },
}

#[derive(Subcommand)]
enum EnvAction {
    /// Create a new environment
    Create {
        /// Environment name
        name: String,
    },
    /// List environments
    List,
    /// Show current environment
    Current,
    /// Copy an environment
    Copy {
        /// Source environment name
        from: String,
        /// Destination environment name
        to: String,
    },
    /// Export environment as JSON
    Export {
        /// Environment name (default: current)
        name: Option<String>,
    },
    /// Import environment from JSON
    Import {
        /// JSON file to import
        file: String,
        /// Environment name
        name: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let format = OutputFormat::from_str_loose(&cli.format);

    // Expand ~ in registry path, with --env shortcut
    let registry_path = if let Some(ref env_name) = cli.env {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        format!("{home}/.kvmql/envs/{env_name}.db")
    } else {
        expand_tilde(&cli.registry)
    };

    // Open the persisted registry (or in-memory for simulate mode)
    let registry = if cli.simulate {
        Registry::open_in_memory().expect("failed to open in-memory registry")
    } else {
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&registry_path).parent() {
            if !parent.exists() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        Registry::open(&registry_path).unwrap_or_else(|e| {
            eprintln!("Failed to open registry at {registry_path}: {e}");
            eprintln!("Run 'orbi init' first, or use --simulate for demo mode.");
            std::process::exit(1);
        })
    };
    let mut ctx = EngineContext::new(registry);

    // Re-hydrate drivers from persisted providers table
    if let Ok(providers) = ctx.registry.list_providers() {
        for p in &providers {
            let driver: Arc<dyn kvmql_driver::traits::Driver> = if cli.simulate {
                Arc::new(kvmql_driver::simulate::SimulationDriver::new(&p.provider_type))
            } else {
                match (p.provider_type.as_str(), p.driver.as_str()) {
                    ("kvm", "firecracker") => {
                        let socket = p.host.as_deref().unwrap_or("/run/firecracker.sock");
                        Arc::new(kvmql_driver::firecracker::FirecrackerDriver::new(socket))
                    }
                    ("aws", _) => {
                        let region = p.region.as_deref().unwrap_or("us-east-1");
                        // Try to resolve auth_ref as AWS profile name
                        if let Ok(profile) = kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref) {
                            Arc::new(kvmql_driver::aws::AwsEc2Driver::with_profile(region, &profile))
                        } else {
                            Arc::new(kvmql_driver::aws::AwsEc2Driver::new(region))
                        }
                    }
                    ("azure", _) => {
                        // Try to resolve auth_ref as subscription ID
                        let sub = kvmql_auth::resolver::CredentialResolver::resolve(&p.auth_ref)
                            .unwrap_or_else(|_| p.id.clone());
                        if let Some(ref rg) = p.region {
                            Arc::new(kvmql_driver::azure::AzureVmDriver::with_resource_group(&sub, rg))
                        } else {
                            Arc::new(kvmql_driver::azure::AzureVmDriver::new(&sub))
                        }
                    }
                    ("gcp", _) => Arc::new(kvmql_driver::gcp::GcpComputeDriver::new(&p.id)),
                    _ => Arc::new(MockDriver::new()),
                }
            };
            ctx.register_driver(p.id.clone(), driver);
        }
    }

    if cli.dry_run {
        ctx.dry_run = true;
    }

    if cli.simulate {
        ctx.simulate = true;
        ctx.register_driver("simulate".into(), Arc::new(SimulationDriver::new("azure")));
        eprintln!("[SIMULATE] Running in simulation mode \u{2014} no cloud resources will be created");
    }

    // Dispatch based on arguments.
    if let Some(command) = &cli.command {
        match command {
            Commands::Shell => {
                if let Err(e) = shell::run_shell(&ctx, format).await {
                    eprintln!("Shell error: {e}");
                    std::process::exit(1);
                }
            }
            Commands::Exec { file, force } => {
                match std::fs::read_to_string(file) {
                    Ok(source) => {
                        let file_hash = sha256_hex(&source);

                        // Check if this exact file was already applied
                        if !force {
                            if let Ok(Some(applied)) = ctx.registry.get_applied_file_by_hash(&file_hash) {
                                match applied.status.as_str() {
                                    "applied" => {
                                        eprintln!(
                                            "File '{}' already applied at {} (hash: {}...)",
                                            file, applied.applied_at, &file_hash[..12]
                                        );
                                        eprintln!("Use --force to re-apply.");
                                        return;
                                    }
                                    "partial" | "failed" => {
                                        eprintln!(
                                            "File '{}' previously {} at {}. Re-running with IF NOT EXISTS protection.",
                                            file, applied.status, applied.applied_at
                                        );
                                        eprintln!("Tip: use IF NOT EXISTS on CREATE/ADD statements for safe re-runs.");
                                        // Fall through — allow re-execution
                                    }
                                    _ => {} // unknown status, allow execution
                                }
                            }
                        }

                        // Execute in Permissive mode when re-running a failed file,
                        // so we skip past already-completed statements
                        let executor = Executor::new(&ctx);
                        let result = executor.execute(&source).await;
                        print_result(&result, format, false, false);

                        // Determine status: applied (all ok), partial (some errors), failed (all errors)
                        let has_errors = result.notifications.iter().any(|n| n.level == "ERROR");
                        let has_success = result.rows_affected.unwrap_or(0) > 0;
                        let status = match (has_errors, has_success) {
                            (false, _) => "applied",
                            (true, true) => "partial",
                            (true, false) => "failed",
                        };

                        // Count statements from the source
                        let stmt_count = source.matches(';').count() as i64;

                        let _ = ctx.registry.insert_applied_file(
                            &uuid::Uuid::new_v4().to_string(),
                            file,
                            &file_hash,
                            stmt_count,
                            cli.env.as_deref(),
                        );
                        if status != "applied" {
                            if let Ok(Some(row)) = ctx.registry.get_applied_file_by_hash(&file_hash) {
                                let _ = ctx.registry.update_applied_file_status(&row.id, status);
                            }
                        }

                        if result.status == kvmql_engine::response::ResultStatus::Error {
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading file '{file}': {e}");
                        std::process::exit(1);
                    }
                }
            }
            Commands::Version => {
                println!("orbi v{} (engine: kvmql)", env!("CARGO_PKG_VERSION"));
            }
            Commands::Init => {
                let path = &registry_path;
                if let Some(parent) = std::path::Path::new(path).parent() {
                    if !parent.exists() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!("Failed to create directory {}: {e}", parent.display());
                            std::process::exit(1);
                        }
                    }
                }
                match Registry::open(path) {
                    Ok(_) => println!("Registry initialised at {path}"),
                    Err(e) => {
                        eprintln!("Failed to initialise registry at {path}: {e}");
                        std::process::exit(1);
                    }
                }
            }
            Commands::Plan { source, output, name } => {
                // Read source (file or inline)
                let dsl = if std::path::Path::new(source).exists() {
                    std::fs::read_to_string(source).expect("failed to read file")
                } else {
                    source.clone()
                };

                // Execute in dry-run mode to get the plan
                ctx.dry_run = true;
                let executor = Executor::new(&ctx);
                let result = executor.execute(&dsl).await;

                let checksum = sha256_hex(&dsl);
                let plan_id = format!("plan-{}", &uuid::Uuid::new_v4().to_string()[..8]);
                let plan_json = serde_json::to_string_pretty(&result).unwrap();

                // Store in registry
                ctx.registry.insert_plan(
                    &plan_id,
                    name.as_deref(),
                    &dsl,
                    &plan_json,
                    &checksum,
                    cli.env.as_deref(),
                ).expect("failed to store plan");

                println!("Plan {} created (status: pending)", plan_id);
                println!();
                print_result(&result, format, false, false);

                // Also write to file if requested
                if let Some(path) = output {
                    let file_plan = serde_json::json!({
                        "plan_id": plan_id,
                        "version": "0.2.0",
                        "created_at": chrono::Utc::now().to_rfc3339(),
                        "source": dsl,
                        "checksum": checksum,
                        "plan": result,
                    });
                    std::fs::write(&path, serde_json::to_string_pretty(&file_plan).unwrap())
                        .expect("failed to write plan file");
                    println!("Plan also written to {path}");
                }
            }
            Commands::Apply { target } => {
                // Check if target is a file path or a plan ID
                if std::path::Path::new(target).exists() {
                    // File-based apply (existing behavior)
                    let plan_json = std::fs::read_to_string(target).expect("failed to read plan file");
                    let plan: serde_json::Value =
                        serde_json::from_str(&plan_json).expect("invalid plan file");

                    let source = plan["source"]
                        .as_str()
                        .expect("plan missing source");
                    let expected_checksum = plan["checksum"]
                        .as_str()
                        .expect("plan missing checksum");
                    let actual_checksum = sha256_hex(source);

                    if actual_checksum != expected_checksum {
                        eprintln!("Plan file has been modified since generation. Refusing to apply.");
                        eprintln!("Expected checksum: {expected_checksum}");
                        eprintln!("Actual checksum:   {actual_checksum}");
                        std::process::exit(1);
                    }

                    println!("Applying plan from {target}...");
                    let executor = Executor::new(&ctx);
                    let result = executor.execute(source).await;
                    print_result(&result, format, false, false);
                    if result.status == kvmql_engine::response::ResultStatus::Error {
                        std::process::exit(1);
                    }
                } else {
                    // Registry-based apply
                    let plan = ctx.registry.get_plan(target)
                        .unwrap_or_else(|_| {
                            eprintln!("Plan '{}' not found. List plans: orbi plans", target);
                            std::process::exit(1);
                        });

                    if plan.status == "pending" {
                        eprintln!("Plan '{}' has not been approved yet.", target);
                        eprintln!("Approve it: orbi approve {}", target);
                        std::process::exit(1);
                    }

                    if plan.status == "applied" {
                        eprintln!("Plan '{}' has already been applied.", target);
                        std::process::exit(1);
                    }

                    if plan.status != "approved" {
                        eprintln!("Plan '{}' cannot be applied (status: {}).", target, plan.status);
                        std::process::exit(1);
                    }

                    println!("Applying plan {}...", target);
                    let executor = Executor::new(&ctx);
                    let result = executor.execute(&plan.source).await;

                    if result.status == kvmql_engine::response::ResultStatus::Error {
                        ctx.registry.update_plan_status(target, "failed", None,
                            Some(&format!("{:?}", result.notifications))).ok();
                        print_result(&result, format, false, false);
                        std::process::exit(1);
                    } else {
                        ctx.registry.update_plan_status(target, "applied", None, None).ok();
                        print_result(&result, format, false, false);
                        println!("Plan {} applied successfully.", target);
                    }
                }
            }
            Commands::Approve { id } => {
                let plan = ctx.registry.get_plan(id).unwrap_or_else(|_| {
                    eprintln!("Plan '{}' not found.", id);
                    std::process::exit(1);
                });

                if plan.status != "pending" {
                    eprintln!("Plan '{}' is not pending (status: {}).", id, plan.status);
                    std::process::exit(1);
                }

                ctx.registry.approve_plan(id, None).unwrap();
                println!("Plan {} approved. Apply it: orbi apply {}", id, id);
            }
            Commands::Plans { status } => {
                let plans = ctx.registry.list_plans(status.as_deref())
                    .expect("failed to list plans");
                if plans.is_empty() {
                    println!("No plans found.");
                } else {
                    for p in &plans {
                        println!("{} | {} | {} | {}",
                            p.id,
                            p.status,
                            p.name.as_deref().unwrap_or("-"),
                            p.created_at
                        );
                    }
                }
            }
            Commands::Env { action } => {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                let envs_dir = format!("{home}/.kvmql/envs");

                match action {
                    EnvAction::Create { name } => {
                        let _ = std::fs::create_dir_all(&envs_dir);
                        let path = format!("{envs_dir}/{name}.db");
                        if std::path::Path::new(&path).exists() {
                            eprintln!("Environment '{name}' already exists at {path}");
                            std::process::exit(1);
                        }
                        Registry::open(&path).expect("failed to create environment");
                        println!("Environment '{name}' created at {path}");
                        println!("Use: orbi --env {name} \"SHOW VERSION;\"");
                    }
                    EnvAction::List => {
                        let _ = std::fs::create_dir_all(&envs_dir);
                        let entries = std::fs::read_dir(&envs_dir).unwrap();
                        println!("Environments:");
                        println!("  default  ~/.kvmql/state.db");
                        for entry in entries.flatten() {
                            if let Some(name) = entry.file_name().to_str() {
                                if name.ends_with(".db") {
                                    let env_name = name.trim_end_matches(".db");
                                    let meta = entry.metadata().ok();
                                    let size = meta.as_ref().map(|m| m.len()).unwrap_or(0);
                                    println!("  {:<8} {} ({} KB)", env_name, entry.path().display(), size / 1024);
                                }
                            }
                        }
                    }
                    EnvAction::Current => {
                        println!("Current registry: {registry_path}");
                    }
                    EnvAction::Copy { from, to } => {
                        let src = format!("{envs_dir}/{from}.db");
                        let dst = format!("{envs_dir}/{to}.db");
                        if !std::path::Path::new(&src).exists() {
                            eprintln!("Source environment '{from}' not found");
                            std::process::exit(1);
                        }
                        std::fs::copy(&src, &dst).expect("failed to copy environment");
                        println!("Environment '{from}' copied to '{to}'");
                    }
                    EnvAction::Export { name } => {
                        // Open the environment DB and export all tables as JSON
                        let path = if let Some(n) = name {
                            format!("{envs_dir}/{n}.db")
                        } else {
                            registry_path.clone()
                        };
                        let reg = Registry::open(&path).expect("failed to open environment");
                        let providers = reg.list_providers().unwrap_or_default();
                        let resources = reg.list_resources().unwrap_or_default();
                        let export = serde_json::json!({
                            "version": "0.2.0",
                            "exported_at": chrono::Utc::now().to_rfc3339(),
                            "providers": providers.iter().map(|p| serde_json::json!({
                                "id": p.id, "type": p.provider_type, "driver": p.driver,
                                "region": p.region, "host": p.host, "auth_ref": p.auth_ref,
                            })).collect::<Vec<_>>(),
                            "resources": resources.iter().map(|r| serde_json::json!({
                                "id": r.id, "resource_type": r.resource_type, "provider_id": r.provider_id,
                                "status": r.status, "config": r.config, "outputs": r.outputs,
                            })).collect::<Vec<_>>(),
                        });
                        println!("{}", serde_json::to_string_pretty(&export).unwrap());
                    }
                    EnvAction::Import { file, name } => {
                        let json = std::fs::read_to_string(&file).expect("failed to read file");
                        let data: serde_json::Value = serde_json::from_str(&json).expect("invalid JSON");
                        let path = format!("{envs_dir}/{name}.db");
                        let _ = std::fs::create_dir_all(&envs_dir);
                        let reg = Registry::open(&path).expect("failed to create environment");

                        // Import providers
                        if let Some(providers) = data["providers"].as_array() {
                            for p in providers {
                                let _ = reg.insert_provider(
                                    p["id"].as_str().unwrap_or(""),
                                    p["type"].as_str().unwrap_or("kvm"),
                                    p["driver"].as_str().unwrap_or("mock"),
                                    "unknown", true,
                                    p["host"].as_str(),
                                    p["region"].as_str(),
                                    p["auth_ref"].as_str().unwrap_or("none"),
                                    None, None,
                                );
                            }
                        }
                        // Import resources
                        if let Some(resources) = data["resources"].as_array() {
                            for r in resources {
                                let _ = reg.insert_resource(
                                    r["id"].as_str().unwrap_or(""),
                                    r["resource_type"].as_str().unwrap_or(""),
                                    r["provider_id"].as_str().unwrap_or(""),
                                    None,
                                    r["status"].as_str().unwrap_or("imported"),
                                    r["config"].as_str(),
                                    None,
                                );
                            }
                        }
                        println!("Environment '{name}' imported from {file}");
                    }
                }
            }
        }
    } else if let Some(ref stmt) = cli.statement {
        // Direct statement execution.
        let executor = Executor::new(&ctx);
        let result = executor.execute(stmt).await;
        print_result(&result, format, false, false);
        if result.status == kvmql_engine::response::ResultStatus::Error {
            std::process::exit(1);
        }
    } else {
        // No command and no statement: launch the shell by default.
        if let Err(e) = shell::run_shell(&ctx, format).await {
            eprintln!("Shell error: {e}");
            std::process::exit(1);
        }
    }
}

fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}/{}", home.to_string_lossy(), rest);
        }
    }
    path.to_string()
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}
