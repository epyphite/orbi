use std::fmt;

/// A source location within KVMQL input text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// 1-based line number.
    pub line: usize,
    /// 1-based column number.
    pub column: usize,
    /// 0-based byte offset into the source string.
    pub offset: usize,
}

/// The category of parse error that occurred.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseErrorKind {
    #[error("invalid token")]
    InvalidToken,
    #[error("unexpected token")]
    UnexpectedToken,
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("expected keyword '{keyword}'")]
    ExpectedKeyword { keyword: String },
    #[error("expected string literal")]
    ExpectedString,
    #[error("expected identifier")]
    ExpectedIdentifier,
    #[error("expected integer")]
    ExpectedInteger,
    #[error("expected duration")]
    ExpectedDuration,
    #[error("expected expression")]
    ExpectedExpression,
    #[error("invalid noun '{found}'; expected one of: microvms, volumes, images, providers, clusters, capabilities, snapshots, metrics, events, query_history, audit_log, principals, grants, cluster_members, resources")]
    InvalidNoun { found: String },
    #[error("expected operator")]
    ExpectedOperator,
    #[error("expected value")]
    ExpectedValue,
    #[error("unclosed string literal")]
    UnclosedString,
    #[error("unclosed parenthesis")]
    UnclosedParen,
    #[error("unclosed brace")]
    UnclosedBrace,
    #[error("unclosed bracket")]
    UnclosedBracket,
    #[error("integer overflow")]
    IntegerOverflow,
    #[error("duplicate parameter '{key}'")]
    DuplicateParam { key: String },
    #[error("unknown SHOW target '{found}'")]
    UnknownShowTarget { found: String },
}

/// A parse error with full diagnostic context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    pub location: Location,
    pub found: Option<String>,
    pub expected: Option<String>,
    pub suggestion: Option<String>,
    pub source_line: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error at line {}, column {}: {}",
            self.location.line, self.location.column, self.kind,
        )?;

        if let Some(ref found) = self.found {
            write!(f, "\n  found: {found}")?;
        }

        if let Some(ref expected) = self.expected {
            write!(f, "\n  expected: {expected}")?;
        }

        let caret_offset = self.location.column.saturating_sub(1);
        write!(f, "\n  | {}", self.source_line)?;
        write!(f, "\n  | {:>width$}", "^", width = caret_offset + 1)?;

        if let Some(ref suggestion) = self.suggestion {
            write!(f, "\n  suggestion: {suggestion}")?;
        }

        Ok(())
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.kind)
    }
}

/// Converts a 0-based byte offset into a [`Location`] with 1-based line and column.
pub fn offset_to_location(source: &str, offset: usize) -> Location {
    let offset = offset.min(source.len());
    let mut line = 1;
    let mut line_start = 0;

    for (i, byte) in source.bytes().enumerate() {
        if i == offset {
            break;
        }
        if byte == b'\n' {
            line += 1;
            line_start = i + 1;
        }
    }

    Location {
        line,
        column: offset - line_start + 1,
        offset,
    }
}

/// Extracts the given 1-based line from source text.
pub fn get_source_line(source: &str, line: usize) -> String {
    if line == 0 {
        return String::new();
    }
    source.lines().nth(line - 1).unwrap_or("").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_to_location_single_line() {
        let src = "CREATE MICROVM";
        let loc = offset_to_location(src, 7);
        assert_eq!(loc.line, 1);
        assert_eq!(loc.column, 8);
        assert_eq!(loc.offset, 7);
    }

    #[test]
    fn offset_to_location_multiline() {
        let src = "SELECT *\nFROM microvms\nWHERE id = 1";
        // "SELECT *\n" = 9 bytes, "FROM microvms\n" = 14 bytes => line 3 starts at offset 23
        let loc = offset_to_location(src, 23);
        assert_eq!(loc.line, 3);
        assert_eq!(loc.column, 1);
    }

    #[test]
    fn offset_to_location_start() {
        let loc = offset_to_location("hello", 0);
        assert_eq!(loc.line, 1);
        assert_eq!(loc.column, 1);
        assert_eq!(loc.offset, 0);
    }

    #[test]
    fn offset_to_location_beyond_end() {
        let src = "abc";
        let loc = offset_to_location(src, 100);
        assert_eq!(loc.offset, 3);
    }

    #[test]
    fn get_source_line_basic() {
        let src = "line one\nline two\nline three";
        assert_eq!(get_source_line(src, 1), "line one");
        assert_eq!(get_source_line(src, 2), "line two");
        assert_eq!(get_source_line(src, 3), "line three");
    }

    #[test]
    fn get_source_line_out_of_bounds() {
        assert_eq!(get_source_line("hello", 0), "");
        assert_eq!(get_source_line("hello", 99), "");
    }

    #[test]
    fn parse_error_display_full() {
        let err = ParseError {
            kind: ParseErrorKind::UnexpectedToken,
            location: Location {
                line: 3,
                column: 12,
                offset: 30,
            },
            found: Some("MICROVM".to_string()),
            expected: Some("VOLUME or MICROVM after CREATE".to_string()),
            suggestion: Some("Did you mean CREATE MICROVM?".to_string()),
            source_line: "CREATE MICROVM tenant = 'acme'".to_string(),
        };

        let output = err.to_string();
        assert!(output.contains("error at line 3, column 12: unexpected token"));
        assert!(output.contains("found: MICROVM"));
        assert!(output.contains("expected: VOLUME or MICROVM after CREATE"));
        assert!(output.contains("| CREATE MICROVM tenant = 'acme'"));
        assert!(output.contains("^"));
        assert!(output.contains("suggestion: Did you mean CREATE MICROVM?"));
    }

    #[test]
    fn parse_error_display_minimal() {
        let err = ParseError {
            kind: ParseErrorKind::UnclosedString,
            location: Location {
                line: 1,
                column: 5,
                offset: 4,
            },
            found: None,
            expected: None,
            suggestion: None,
            source_line: "foo 'bar".to_string(),
        };

        let output = err.to_string();
        assert!(output.contains("unclosed string literal"));
        assert!(!output.contains("found:"));
        assert!(!output.contains("expected:"));
        assert!(!output.contains("suggestion:"));
    }

    #[test]
    fn parse_error_kind_display() {
        assert_eq!(ParseErrorKind::InvalidToken.to_string(), "invalid token");
        assert_eq!(
            ParseErrorKind::ExpectedKeyword {
                keyword: "WHERE".to_string()
            }
            .to_string(),
            "expected keyword 'WHERE'"
        );
        assert_eq!(
            ParseErrorKind::InvalidNoun {
                found: "foobar".to_string()
            }
            .to_string(),
            "invalid noun 'foobar'; expected one of: microvms, volumes, images, providers, clusters, capabilities, snapshots, metrics, events, query_history, audit_log, principals, grants, cluster_members, resources"
        );
        assert_eq!(
            ParseErrorKind::DuplicateParam {
                key: "vcpus".to_string()
            }
            .to_string(),
            "duplicate parameter 'vcpus'"
        );
    }
}
