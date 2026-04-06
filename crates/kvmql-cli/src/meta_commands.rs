use kvmql_engine::context::EngineContext;
use kvmql_engine::executor::Executor;

use crate::formatter::{print_result, OutputFormat};

/// The result of parsing a meta-command line.
#[derive(Debug, PartialEq)]
pub enum MetaAction {
    Quit,
    Help(Option<String>),
    DescribeAll,
    DescribeNoun(String),
    ShowConnection,
    SetCluster(String),
    SetProvider(String),
    ToggleTiming,
    ToggleExpanded,
    Shortcut(String),
    Unknown(String),
}

/// Parse a backslash command string into a `MetaAction`.
pub fn parse_meta_command(input: &str) -> MetaAction {
    let trimmed = input.trim();
    let parts: Vec<&str> = trimmed.splitn(2, char::is_whitespace).collect();
    let cmd = parts[0];
    let arg = parts.get(1).map(|s| s.trim().to_string());

    match cmd {
        "\\q" | "\\quit" => MetaAction::Quit,
        "\\h" | "\\help" => MetaAction::Help(arg),
        "\\d" => {
            if let Some(noun) = arg {
                MetaAction::DescribeNoun(noun)
            } else {
                MetaAction::DescribeAll
            }
        }
        "\\c" => MetaAction::ShowConnection,
        "\\cluster" => {
            if let Some(id) = arg {
                MetaAction::SetCluster(id)
            } else {
                MetaAction::ShowConnection
            }
        }
        "\\provider" => {
            if let Some(id) = arg {
                MetaAction::SetProvider(id)
            } else {
                MetaAction::ShowConnection
            }
        }
        "\\timing" => MetaAction::ToggleTiming,
        "\\x" => MetaAction::ToggleExpanded,
        "\\images" => MetaAction::Shortcut("SHOW IMAGES;".into()),
        "\\providers" => MetaAction::Shortcut("SHOW PROVIDERS;".into()),
        "\\clusters" => MetaAction::Shortcut("SHOW CLUSTERS;".into()),
        other => MetaAction::Unknown(other.into()),
    }
}

/// Execute a meta-command, mutating session state as needed.
pub async fn handle_meta_command(
    input: &str,
    session_cluster: &mut Option<String>,
    session_provider: &mut Option<String>,
    expanded_mode: &mut bool,
    timing: &mut bool,
    ctx: &EngineContext,
    format: OutputFormat,
) {
    let action = parse_meta_command(input);

    match action {
        MetaAction::Quit => {
            // Handled in the shell loop — should not reach here.
        }
        MetaAction::Help(None) => print_verb_list(),
        MetaAction::Help(Some(verb)) => print_verb_help(&verb),
        MetaAction::DescribeAll => print_noun_list(),
        MetaAction::DescribeNoun(noun) => print_noun_schema(&noun),
        MetaAction::ShowConnection => {
            let cluster = session_cluster
                .as_deref()
                .unwrap_or("(none)");
            let provider = session_provider
                .as_deref()
                .unwrap_or("(none)");
            println!("cluster:  {cluster}");
            println!("provider: {provider}");
            println!("expanded: {}", if *expanded_mode { "on" } else { "off" });
            println!("timing:   {}", if *timing { "on" } else { "off" });
        }
        MetaAction::SetCluster(id) => {
            println!("Cluster context set to: {id}");
            *session_cluster = Some(id);
        }
        MetaAction::SetProvider(id) => {
            println!("Provider context set to: {id}");
            *session_provider = Some(id);
        }
        MetaAction::ToggleTiming => {
            *timing = !*timing;
            println!("Timing is {}", if *timing { "on" } else { "off" });
        }
        MetaAction::ToggleExpanded => {
            *expanded_mode = !*expanded_mode;
            println!(
                "Expanded display is {}",
                if *expanded_mode { "on" } else { "off" }
            );
        }
        MetaAction::Shortcut(stmt) => {
            let executor = Executor::new(ctx);
            let result = executor.execute(&stmt).await;
            print_result(&result, format, *expanded_mode, *timing);
        }
        MetaAction::Unknown(cmd) => {
            eprintln!("Unknown command: {cmd}");
            eprintln!("Type \\h for help.");
        }
    }
}

// ---------------------------------------------------------------------------
// Informational output
// ---------------------------------------------------------------------------

fn print_verb_list() {
    println!("KVMQL Verbs:");
    println!("  CREATE       Create a new microVM, volume, image, or cluster");
    println!("  DESTROY      Remove a microVM, volume, image, or cluster");
    println!("  SHOW         List or inspect resources");
    println!("  ALTER        Modify a running microVM (vCPUs, memory)");
    println!("  PAUSE        Suspend a running microVM");
    println!("  RESUME       Wake a paused microVM");
    println!("  SNAPSHOT     Capture microVM state to a snapshot");
    println!("  RESTORE      Restore a microVM from a snapshot");
    println!("  ATTACH       Attach a volume to a microVM");
    println!("  DETACH       Detach a volume from a microVM");
    println!("  WATCH        Stream live metrics for a microVM");
    println!("  IMPORT       Import an image from a source");
    println!("  REGISTER     Register a provider");
    println!();
    println!("Type \\h <verb> for details on a specific verb.");
}

fn print_verb_help(verb: &str) {
    match verb.to_uppercase().as_str() {
        "CREATE" => {
            println!("CREATE <noun> <id> [WITH <key>=<value>, ...]");
            println!();
            println!("  CREATE VM my-vm WITH image='ubuntu-22.04', vcpus=2, memory_mb=512;");
            println!("  CREATE VOLUME vol-1 WITH size_gb=20;");
        }
        "DESTROY" => {
            println!("DESTROY <noun> <id> [FORCE]");
            println!();
            println!("  DESTROY VM my-vm;");
            println!("  DESTROY VM my-vm FORCE;");
        }
        "SHOW" => {
            println!("SHOW <noun> [<id>] [WHERE <filter>] [LIMIT n]");
            println!();
            println!("  SHOW VMS;");
            println!("  SHOW VM my-vm;");
            println!("  SHOW VOLUMES WHERE status='attached';");
        }
        "ALTER" => {
            println!("ALTER VM <id> SET <key>=<value>, ...");
            println!();
            println!("  ALTER VM my-vm SET vcpus=4, memory_mb=1024;");
        }
        "PAUSE" => {
            println!("PAUSE VM <id>;");
            println!();
            println!("  PAUSE VM my-vm;");
        }
        "RESUME" => {
            println!("RESUME VM <id>;");
            println!();
            println!("  RESUME VM my-vm;");
        }
        "SNAPSHOT" => {
            println!("SNAPSHOT VM <id> TO <destination> [TAG <tag>];");
            println!();
            println!("  SNAPSHOT VM my-vm TO '/snapshots/snap1' TAG 'v1';");
        }
        "RESTORE" => {
            println!("RESTORE VM <id> FROM <source>;");
            println!();
            println!("  RESTORE VM my-vm FROM '/snapshots/snap1';");
        }
        "ATTACH" => {
            println!("ATTACH VOLUME <vol-id> TO <vm-id> [DEVICE <device>];");
            println!();
            println!("  ATTACH VOLUME vol-1 TO my-vm;");
        }
        "DETACH" => {
            println!("DETACH VOLUME <vol-id> FROM <vm-id>;");
            println!();
            println!("  DETACH VOLUME vol-1 FROM my-vm;");
        }
        "WATCH" => {
            println!("WATCH VM <id> [INTERVAL <seconds>];");
            println!();
            println!("  WATCH VM my-vm INTERVAL 5;");
        }
        "IMPORT" => {
            println!("IMPORT IMAGE <id> FROM <source> [WITH <key>=<value>, ...];");
            println!();
            println!("  IMPORT IMAGE ubuntu FROM '/images/ubuntu.ext4' WITH os='linux';");
        }
        "REGISTER" => {
            println!("REGISTER PROVIDER <id> DRIVER <driver> [WITH <key>=<value>, ...];");
            println!();
            println!("  REGISTER PROVIDER local DRIVER firecracker WITH host='localhost';");
        }
        _ => {
            eprintln!("Unknown verb: {verb}");
            eprintln!("Type \\h to see all verbs.");
        }
    }
}

fn print_noun_list() {
    println!("KVMQL Nouns:");
    println!("  VM (MICROVM)    A micro virtual machine");
    println!("  VOLUME          A block storage volume");
    println!("  IMAGE           A bootable OS image");
    println!("  SNAPSHOT        A point-in-time VM capture");
    println!("  PROVIDER        A registered hypervisor/cloud backend");
    println!("  CLUSTER         A logical grouping of providers");
    println!();
    println!("Type \\d <noun> for field details.");
}

fn print_noun_schema(noun: &str) {
    match noun.to_uppercase().as_str() {
        "VM" | "MICROVM" | "VMS" => {
            println!("VM (MicroVM)");
            println!("  id            String    Unique identifier");
            println!("  provider_id   String    Provider that hosts this VM");
            println!("  tenant        String    Tenant / owner");
            println!("  status        String    running | paused | stopped | error");
            println!("  image_id      String?   Boot image");
            println!("  vcpus         Int?      Virtual CPU count");
            println!("  memory_mb     Int?      Memory in MB");
            println!("  cpu_pct       Float?    CPU usage percentage");
            println!("  mem_used_mb   Int?      Memory used in MB");
            println!("  hostname      String?   Hostname");
            println!("  labels        JSON?     Key-value labels");
            println!("  created_at    DateTime  Creation timestamp");
        }
        "VOLUME" | "VOLUMES" => {
            println!("VOLUME");
            println!("  id            String    Unique identifier");
            println!("  provider_id   String    Provider that hosts this volume");
            println!("  microvm_id    String?   Attached VM (if any)");
            println!("  vol_type      String    virtio-blk | nvme | ...");
            println!("  size_gb       Int       Size in GB");
            println!("  status        String    available | attached | error");
            println!("  device_name   String?   Device path when attached");
            println!("  iops          Int?      Provisioned IOPS");
            println!("  encrypted     Bool      Encryption enabled");
            println!("  labels        JSON?     Key-value labels");
            println!("  created_at    DateTime  Creation timestamp");
        }
        "IMAGE" | "IMAGES" => {
            println!("IMAGE");
            println!("  id            String    Unique identifier");
            println!("  name          String    Human-readable name");
            println!("  os            String    Operating system (linux, ...)");
            println!("  distro        String    Distribution (ubuntu, alpine, ...)");
            println!("  version       String    OS version");
            println!("  arch          String    Architecture (x86_64, aarch64)");
            println!("  image_type    String    rootfs | disk | cloud");
            println!("  source        String    Import source");
            println!("  size_mb       Int?      Image size in MB");
            println!("  status        String    available | importing | error");
            println!("  labels        JSON?     Key-value labels");
            println!("  imported_at   DateTime  Import timestamp");
        }
        "SNAPSHOT" | "SNAPSHOTS" => {
            println!("SNAPSHOT");
            println!("  id            String    Unique identifier");
            println!("  microvm_id    String    Source VM");
            println!("  provider_id   String    Provider that stores it");
            println!("  destination   String    Storage path");
            println!("  tag           String?   User-defined tag");
            println!("  size_mb       Int?      Snapshot size in MB");
            println!("  taken_at      DateTime  Capture timestamp");
        }
        "PROVIDER" | "PROVIDERS" => {
            println!("PROVIDER");
            println!("  id            String    Unique identifier");
            println!("  provider_type String    Type (firecracker, cloud-hypervisor, ...)");
            println!("  driver        String    Driver name");
            println!("  status        String    online | offline | degraded");
            println!("  enabled       Bool      Whether provider accepts work");
            println!("  host          String?   Hostname / endpoint");
            println!("  region        String?   Region / zone");
            println!("  labels        JSON?     Key-value labels");
            println!("  added_at      DateTime  Registration timestamp");
        }
        "CLUSTER" | "CLUSTERS" => {
            println!("CLUSTER");
            println!("  id            String    Unique identifier");
            println!("  name          String    Human-readable name");
            println!("  providers     [String]  Member provider IDs");
            println!("  status        String    active | degraded | offline");
        }
        _ => {
            eprintln!("Unknown noun: {noun}");
            eprintln!("Type \\d to see all nouns.");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        assert_eq!(parse_meta_command("\\q"), MetaAction::Quit);
        assert_eq!(parse_meta_command("\\quit"), MetaAction::Quit);
    }

    #[test]
    fn test_parse_help_no_arg() {
        assert_eq!(parse_meta_command("\\h"), MetaAction::Help(None));
    }

    #[test]
    fn test_parse_help_with_arg() {
        assert_eq!(
            parse_meta_command("\\h CREATE"),
            MetaAction::Help(Some("CREATE".into()))
        );
    }

    #[test]
    fn test_parse_describe_all() {
        assert_eq!(parse_meta_command("\\d"), MetaAction::DescribeAll);
    }

    #[test]
    fn test_parse_describe_noun() {
        assert_eq!(
            parse_meta_command("\\d VM"),
            MetaAction::DescribeNoun("VM".into())
        );
    }

    #[test]
    fn test_parse_connection() {
        assert_eq!(parse_meta_command("\\c"), MetaAction::ShowConnection);
    }

    #[test]
    fn test_parse_set_cluster() {
        assert_eq!(
            parse_meta_command("\\cluster prod"),
            MetaAction::SetCluster("prod".into())
        );
    }

    #[test]
    fn test_parse_set_provider() {
        assert_eq!(
            parse_meta_command("\\provider local"),
            MetaAction::SetProvider("local".into())
        );
    }

    #[test]
    fn test_parse_timing() {
        assert_eq!(parse_meta_command("\\timing"), MetaAction::ToggleTiming);
    }

    #[test]
    fn test_parse_expanded() {
        assert_eq!(parse_meta_command("\\x"), MetaAction::ToggleExpanded);
    }

    #[test]
    fn test_parse_shortcuts() {
        assert_eq!(
            parse_meta_command("\\images"),
            MetaAction::Shortcut("SHOW IMAGES;".into())
        );
        assert_eq!(
            parse_meta_command("\\providers"),
            MetaAction::Shortcut("SHOW PROVIDERS;".into())
        );
        assert_eq!(
            parse_meta_command("\\clusters"),
            MetaAction::Shortcut("SHOW CLUSTERS;".into())
        );
    }

    #[test]
    fn test_parse_unknown() {
        assert_eq!(
            parse_meta_command("\\foo"),
            MetaAction::Unknown("\\foo".into())
        );
    }

    #[test]
    fn test_parse_cluster_no_arg_shows_connection() {
        assert_eq!(
            parse_meta_command("\\cluster"),
            MetaAction::ShowConnection
        );
    }

    #[test]
    fn test_parse_provider_no_arg_shows_connection() {
        assert_eq!(
            parse_meta_command("\\provider"),
            MetaAction::ShowConnection
        );
    }
}
