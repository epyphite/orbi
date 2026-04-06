use comfy_table::{Cell, Table};
use kvmql_engine::response::{ResultEnvelope, ResultStatus};

/// Supported output formats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Raw,
}

impl OutputFormat {
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            "csv" => Self::Csv,
            "raw" => Self::Raw,
            _ => Self::Table,
        }
    }
}

/// Render the full result envelope according to the chosen format.
pub fn print_result(envelope: &ResultEnvelope, format: OutputFormat, expanded: bool, timing: bool) {
    // Print notifications first.
    for n in &envelope.notifications {
        let level = n.level.to_uppercase();
        let provider = n
            .provider_id
            .as_deref()
            .map(|p| format!(" [{p}]"))
            .unwrap_or_default();
        eprintln!("{level}  {}{provider}  {}", n.code, n.message);
    }

    if envelope.status == ResultStatus::Error && envelope.result.is_none() {
        // Nothing more to print — the notification above carries the error.
        if timing {
            eprintln!("Time: {}ms", envelope.duration_ms);
        }
        return;
    }

    match format {
        OutputFormat::Table => print_table(envelope, expanded),
        OutputFormat::Json => print_json(envelope),
        OutputFormat::Csv => print_csv(envelope),
        OutputFormat::Raw => print_raw(envelope),
    }

    // Row count + timing footer.
    let row_count = row_count_from_result(&envelope.result);
    if timing {
        println!("{row_count} row(s) ({}ms)", envelope.duration_ms);
    } else if row_count > 0 {
        println!("({row_count} row(s))");
    }
}

// ---------------------------------------------------------------------------
// Format implementations
// ---------------------------------------------------------------------------

fn print_table(envelope: &ResultEnvelope, expanded: bool) {
    let Some(value) = &envelope.result else {
        return;
    };

    if expanded {
        print_expanded(value);
        return;
    }

    let rows = match value {
        serde_json::Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    };

    if rows.is_empty() {
        return;
    }

    let headers = collect_headers(&rows);
    if headers.is_empty() {
        // Scalar result — just print it.
        println!("{}", format_value(&rows[0]));
        return;
    }

    let mut table = Table::new();
    table.set_header(headers.iter().map(|h| Cell::new(h)));

    for row in &rows {
        let cells: Vec<Cell> = headers
            .iter()
            .map(|h| {
                let val = row.get(h.as_str()).unwrap_or(&serde_json::Value::Null);
                Cell::new(format_value(val))
            })
            .collect();
        table.add_row(cells);
    }

    println!("{table}");
}

fn print_expanded(value: &serde_json::Value) {
    let rows = match value {
        serde_json::Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    };

    for (i, row) in rows.iter().enumerate() {
        println!("-[ RECORD {} ]---", i + 1);
        if let serde_json::Value::Object(map) = row {
            let max_key_len = map.keys().map(|k| k.len()).max().unwrap_or(0);
            for (k, v) in map {
                println!("{:width$} | {}", k, format_value(v), width = max_key_len);
            }
        } else {
            println!("{}", format_value(row));
        }
    }
}

fn print_json(envelope: &ResultEnvelope) {
    match serde_json::to_string_pretty(envelope) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("JSON serialisation error: {e}"),
    }
}

fn print_csv(envelope: &ResultEnvelope) {
    let Some(value) = &envelope.result else {
        return;
    };

    let rows = match value {
        serde_json::Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    };

    if rows.is_empty() {
        return;
    }

    let headers = collect_headers(&rows);
    if headers.is_empty() {
        println!("{}", format_value(&rows[0]));
        return;
    }

    // Header line
    println!("{}", headers.join(","));

    // Data lines
    for row in &rows {
        let line: Vec<String> = headers
            .iter()
            .map(|h| {
                let val = row.get(h.as_str()).unwrap_or(&serde_json::Value::Null);
                csv_escape(&format_value(val))
            })
            .collect();
        println!("{}", line.join(","));
    }
}

fn print_raw(envelope: &ResultEnvelope) {
    let Some(value) = &envelope.result else {
        return;
    };
    println!("{}", format_value(value));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn collect_headers(rows: &[serde_json::Value]) -> Vec<String> {
    // Use insertion order from the first object.
    let mut headers = Vec::new();
    for row in rows {
        if let serde_json::Value::Object(map) = row {
            for key in map.keys() {
                if !headers.contains(key) {
                    headers.push(key.clone());
                }
            }
        }
    }
    headers
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        // For nested objects/arrays, fall back to compact JSON.
        other => other.to_string(),
    }
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn row_count_from_result(result: &Option<serde_json::Value>) -> usize {
    match result {
        Some(serde_json::Value::Array(arr)) => arr.len(),
        Some(_) => 1,
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use kvmql_engine::response::Notification;

    fn sample_envelope(result: serde_json::Value) -> ResultEnvelope {
        ResultEnvelope {
            request_id: "test-req".into(),
            status: ResultStatus::Ok,
            notifications: vec![],
            result: Some(result),
            rows_affected: Some(0),
            duration_ms: 42,
        }
    }

    #[test]
    fn test_output_format_parsing() {
        assert_eq!(OutputFormat::from_str_loose("table"), OutputFormat::Table);
        assert_eq!(OutputFormat::from_str_loose("json"), OutputFormat::Json);
        assert_eq!(OutputFormat::from_str_loose("JSON"), OutputFormat::Json);
        assert_eq!(OutputFormat::from_str_loose("csv"), OutputFormat::Csv);
        assert_eq!(OutputFormat::from_str_loose("raw"), OutputFormat::Raw);
        assert_eq!(OutputFormat::from_str_loose("unknown"), OutputFormat::Table);
    }

    #[test]
    fn test_collect_headers() {
        let rows = vec![
            serde_json::json!({"id": "vm-1", "status": "running"}),
            serde_json::json!({"id": "vm-2", "status": "paused", "extra": true}),
        ];
        let headers = collect_headers(&rows);
        assert_eq!(headers, vec!["id", "status", "extra"]);
    }

    #[test]
    fn test_collect_headers_empty() {
        let rows: Vec<serde_json::Value> = vec![serde_json::json!(42)];
        let headers = collect_headers(&rows);
        assert!(headers.is_empty());
    }

    #[test]
    fn test_format_value_types() {
        assert_eq!(format_value(&serde_json::Value::Null), "");
        assert_eq!(format_value(&serde_json::json!("hello")), "hello");
        assert_eq!(format_value(&serde_json::json!(true)), "true");
        assert_eq!(format_value(&serde_json::json!(42)), "42");
        assert_eq!(format_value(&serde_json::json!(3.14)), "3.14");
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape("simple"), "simple");
        assert_eq!(csv_escape("has,comma"), "\"has,comma\"");
        assert_eq!(csv_escape("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(csv_escape("has\nnewline"), "\"has\nnewline\"");
    }

    #[test]
    fn test_row_count_from_result() {
        assert_eq!(row_count_from_result(&None), 0);
        assert_eq!(
            row_count_from_result(&Some(serde_json::json!("scalar"))),
            1
        );
        assert_eq!(
            row_count_from_result(&Some(serde_json::json!([1, 2, 3]))),
            3
        );
    }

    #[test]
    fn test_table_rendering_captures_output() {
        // Verify table rendering doesn't panic on typical input.
        let envelope = sample_envelope(serde_json::json!([
            {"id": "vm-1", "status": "running", "vcpus": 2},
            {"id": "vm-2", "status": "paused", "vcpus": 4},
        ]));
        // This just ensures no panic during rendering.
        print_table(&envelope, false);
    }

    #[test]
    fn test_expanded_rendering() {
        let value = serde_json::json!([
            {"id": "vm-1", "status": "running"},
        ]);
        // Ensure no panic.
        print_expanded(&value);
    }

    #[test]
    fn test_csv_rendering() {
        let envelope = sample_envelope(serde_json::json!([
            {"name": "alpha", "count": 10},
            {"name": "beta", "count": 20},
        ]));
        // Ensure no panic.
        print_csv(&envelope);
    }

    #[test]
    fn test_raw_rendering() {
        let envelope = sample_envelope(serde_json::json!("hello world"));
        print_raw(&envelope);
    }

    #[test]
    fn test_notifications_printed() {
        let envelope = ResultEnvelope {
            request_id: "test".into(),
            status: ResultStatus::Warn,
            notifications: vec![Notification {
                level: "WARN".into(),
                code: "CAP_002".into(),
                provider_id: Some("mock".into()),
                message: "kernel parameter not applicable".into(),
            }],
            result: Some(serde_json::json!({"ok": true})),
            rows_affected: Some(0),
            duration_ms: 5,
        };
        // Ensure print_result doesn't panic with notifications.
        print_result(&envelope, OutputFormat::Table, false, true);
    }
}
