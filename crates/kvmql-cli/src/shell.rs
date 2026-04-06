use rustyline::error::ReadlineError;
use rustyline::Editor;

use kvmql_engine::context::EngineContext;
use kvmql_engine::executor::Executor;

use crate::formatter::{print_result, OutputFormat};
use crate::meta_commands::{handle_meta_command, parse_meta_command, MetaAction};

/// Run the interactive Orbi REPL.
pub async fn run_shell(
    ctx: &EngineContext,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Orbi v{} | engine: kvmql | type \\h for help, \\q to quit\n",
        env!("CARGO_PKG_VERSION")
    );

    let mut rl = Editor::<(), rustyline::history::DefaultHistory>::new()?;
    let history_path = history_file_path();
    if let Some(ref p) = history_path {
        let _ = rl.load_history(p);
    }

    let mut buffer = String::new();
    let mut session_cluster: Option<String> = None;
    let mut session_provider: Option<String> = None;
    let mut expanded_mode = false;
    let mut timing = false;

    let executor = Executor::new(ctx);

    loop {
        let prompt = if buffer.is_empty() {
            "orbi> "
        } else {
            "    > "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle backslash meta-commands (only when buffer is empty).
                if buffer.is_empty() && trimmed.starts_with('\\') {
                    let action = parse_meta_command(trimmed);
                    if action == MetaAction::Quit {
                        break;
                    }
                    handle_meta_command(
                        trimmed,
                        &mut session_cluster,
                        &mut session_provider,
                        &mut expanded_mode,
                        &mut timing,
                        ctx,
                        format,
                    )
                    .await;
                    continue;
                }

                buffer.push_str(&line);
                buffer.push(' ');

                // Execute when we see a semicolon at end of input.
                if buffer.trim_end().ends_with(';') {
                    let stmt = buffer.trim().to_string();
                    let _ = rl.add_history_entry(&stmt);

                    // Check if this is a WATCH statement — stream continuously
                    let is_watch = stmt.trim().to_uppercase().starts_with("WATCH");

                    if is_watch {
                        let interval_secs = extract_watch_interval(&stmt).unwrap_or(5);
                        let select_stmt = watch_to_select(&stmt);

                        println!(
                            "Streaming every {}s. Press Ctrl+C to stop.\n",
                            interval_secs
                        );

                        loop {
                            let result = executor.execute(&select_stmt).await;
                            println!(
                                "--- {} ---",
                                chrono::Utc::now().format("%H:%M:%S")
                            );
                            print_result(&result, format, expanded_mode, timing);

                            // Sleep, but check for Ctrl+C
                            tokio::select! {
                                _ = tokio::time::sleep(
                                    std::time::Duration::from_secs(interval_secs)
                                ) => {}
                                _ = tokio::signal::ctrl_c() => {
                                    println!("\nWatch stopped.");
                                    break;
                                }
                            }
                        }
                    } else {
                        let result = executor.execute(&stmt).await;
                        print_result(&result, format, expanded_mode, timing);
                    }

                    buffer.clear();
                }
            }
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    if let Some(ref p) = history_path {
        // Ensure parent directory exists.
        if let Some(parent) = std::path::Path::new(p.as_str()).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.save_history(p);
    }

    Ok(())
}

/// Extract the INTERVAL duration in seconds from a WATCH statement.
///
/// Looks for `INTERVAL` followed by a duration like `5s`, `1m`, `2h`.
/// Returns `None` if no interval clause is found.
pub fn extract_watch_interval(stmt: &str) -> Option<u64> {
    let upper = stmt.to_uppercase();
    if let Some(idx) = upper.find("INTERVAL") {
        let rest = stmt[idx + 8..].trim();
        // Parse duration: "5s" -> 5, "1m" -> 60, "10s" -> 10
        let num_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        if num_end == 0 {
            return None;
        }
        let num: u64 = rest[..num_end].parse().ok()?;
        let unit = rest.get(num_end..num_end + 1).unwrap_or("s");
        match unit {
            "s" | "S" => Some(num),
            "m" | "M" => Some(num * 60),
            "h" | "H" => Some(num * 3600),
            _ => Some(num),
        }
    } else {
        None
    }
}

/// Convert a WATCH statement into an equivalent SELECT statement.
///
/// Example:
///   `WATCH METRIC cpu_pct, mem_used_mb FROM microvms WHERE tenant = 'acme' INTERVAL 5s;`
/// becomes:
///   `SELECT cpu_pct, mem_used_mb FROM microvms WHERE tenant = 'acme';`
pub fn watch_to_select(stmt: &str) -> String {
    let upper = stmt.to_uppercase();
    // Strip trailing INTERVAL clause
    let stmt_clean = if let Some(idx) = upper.find("INTERVAL") {
        stmt[..idx].trim()
    } else {
        stmt.trim_end_matches(';').trim()
    };
    // Replace WATCH with SELECT, strip METRIC keyword
    let result = if let Some(rest) = strip_prefix_case_insensitive(stmt_clean, "WATCH") {
        let rest = rest.trim();
        let rest = if let Some(after_metric) = strip_prefix_case_insensitive(rest, "METRIC") {
            after_metric.trim()
        } else {
            rest
        };
        format!("SELECT {}", rest)
    } else {
        stmt_clean.to_string()
    };
    format!("{};", result.trim().trim_end_matches(';').trim())
}

/// Case-insensitive prefix strip. Returns the remainder if prefix matches.
fn strip_prefix_case_insensitive<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len()
        && s[..prefix.len()].eq_ignore_ascii_case(prefix)
    {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Resolve the history file path (~/.kvmql/history).
fn history_file_path() -> Option<String> {
    #[cfg(unix)]
    {
        if let Ok(home) = std::env::var("HOME") {
            return Some(format!("{home}/.kvmql/history"));
        }
    }
    #[cfg(windows)]
    {
        if let Ok(profile) = std::env::var("USERPROFILE") {
            return Some(format!("{profile}\\.kvmql\\history"));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_to_select_basic() {
        let input = "WATCH METRIC cpu_pct, mem_used_mb FROM microvms WHERE tenant = 'acme' INTERVAL 5s;";
        let result = watch_to_select(input);
        assert_eq!(
            result,
            "SELECT cpu_pct, mem_used_mb FROM microvms WHERE tenant = 'acme';"
        );
    }

    #[test]
    fn test_watch_to_select_star() {
        let input = "WATCH METRIC * FROM microvms INTERVAL 10s;";
        let result = watch_to_select(input);
        assert_eq!(result, "SELECT * FROM microvms;");
    }

    #[test]
    fn test_watch_to_select_no_interval() {
        let input = "WATCH METRIC cpu_pct FROM microvms;";
        let result = watch_to_select(input);
        assert_eq!(result, "SELECT cpu_pct FROM microvms;");
    }

    #[test]
    fn test_watch_to_select_lowercase() {
        let input = "watch metric cpu_pct from microvms interval 5s;";
        let result = watch_to_select(input);
        assert_eq!(result, "SELECT cpu_pct from microvms;");
    }

    #[test]
    fn test_extract_watch_interval_seconds() {
        assert_eq!(extract_watch_interval("WATCH METRIC * FROM microvms INTERVAL 5s;"), Some(5));
    }

    #[test]
    fn test_extract_watch_interval_minutes() {
        assert_eq!(extract_watch_interval("WATCH METRIC * FROM microvms INTERVAL 2m;"), Some(120));
    }

    #[test]
    fn test_extract_watch_interval_hours() {
        assert_eq!(extract_watch_interval("WATCH METRIC * FROM microvms INTERVAL 1h;"), Some(3600));
    }

    #[test]
    fn test_extract_watch_interval_none() {
        assert_eq!(extract_watch_interval("WATCH METRIC * FROM microvms;"), None);
    }

    #[test]
    fn test_extract_watch_interval_ten_seconds() {
        assert_eq!(extract_watch_interval("WATCH METRIC * FROM microvms INTERVAL 10s;"), Some(10));
    }
}
