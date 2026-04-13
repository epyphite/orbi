use logos::Logos;
use std::fmt;
use std::ops::Range;

#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n]+")]
#[logos(skip r"--[^\n]*")]
pub enum Token {
    // ── Punctuation & Operators ──────────────────────────────────────
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(",")]
    Comma,
    #[token(";")]
    Semicolon,
    #[token(":")]
    Colon,
    #[token(".")]
    Dot,
    #[token("*")]
    Star,

    #[token("=")]
    Eq,
    #[token("!=")]
    Neq,
    #[token(">=")]
    Gte,
    #[token("<=")]
    Lte,
    #[token(">")]
    Gt,
    #[token("<")]
    Lt,

    // ── Statement verbs ──────────────────────────────────────────────
    #[token("SELECT", ignore(ascii_case))]
    Select,
    #[token("CREATE", ignore(ascii_case))]
    Create,
    #[token("ALTER", ignore(ascii_case))]
    Alter,
    #[token("DESTROY", ignore(ascii_case))]
    Destroy,
    #[token("PAUSE", ignore(ascii_case))]
    Pause,
    #[token("RESUME", ignore(ascii_case))]
    Resume,
    #[token("SNAPSHOT", ignore(ascii_case))]
    Snapshot,
    #[token("RESTORE", ignore(ascii_case))]
    Restore,
    #[token("WATCH", ignore(ascii_case))]
    Watch,
    #[token("ATTACH", ignore(ascii_case))]
    Attach,
    #[token("DETACH", ignore(ascii_case))]
    Detach,
    #[token("RESIZE", ignore(ascii_case))]
    Resize,
    #[token("IMPORT", ignore(ascii_case))]
    Import,
    #[token("PUBLISH", ignore(ascii_case))]
    Publish,
    #[token("REMOVE", ignore(ascii_case))]
    Remove,
    #[token("ADD", ignore(ascii_case))]
    Add,
    #[token("GRANT", ignore(ascii_case))]
    Grant,
    #[token("REVOKE", ignore(ascii_case))]
    Revoke,
    #[token("SET", ignore(ascii_case))]
    Set,
    #[token("SHOW", ignore(ascii_case))]
    Show,
    #[token("BACKUP", ignore(ascii_case))]
    Backup,
    #[token("SCALE", ignore(ascii_case))]
    Scale,
    #[token("UPGRADE", ignore(ascii_case))]
    Upgrade,
    #[token("EXPLAIN", ignore(ascii_case))]
    Explain,
    #[token("ROLLBACK", ignore(ascii_case))]
    Rollback,
    #[token("ASSERT", ignore(ascii_case))]
    Assert,

    // ── Clause keywords ──────────────────────────────────────────────
    #[token("FROM", ignore(ascii_case))]
    From,
    #[token("WHERE", ignore(ascii_case))]
    Where,
    #[token("ORDER", ignore(ascii_case))]
    Order,
    #[token("BY", ignore(ascii_case))]
    By,
    #[token("LIMIT", ignore(ascii_case))]
    Limit,
    #[token("GROUP", ignore(ascii_case))]
    Group,
    #[token("ON", ignore(ascii_case))]
    On,
    #[token("LIVE", ignore(ascii_case))]
    Live,
    #[token("FORCE", ignore(ascii_case))]
    Force,
    #[token("INTO", ignore(ascii_case))]
    Into,
    #[token("TAG", ignore(ascii_case))]
    Tag,
    #[token("AS", ignore(ascii_case))]
    As,
    #[token("TO", ignore(ascii_case))]
    To,
    #[token("FOR", ignore(ascii_case))]
    For,
    #[token("LAST", ignore(ascii_case))]
    Last,

    // ── Noun-related ─────────────────────────────────────────────────
    #[token("MICROVM", ignore(ascii_case))]
    MicroVm,
    #[token("VOLUME", ignore(ascii_case))]
    Volume,
    #[token("IMAGE", ignore(ascii_case))]
    Image,
    #[token("PROVIDER", ignore(ascii_case))]
    Provider,
    #[token("CLUSTER", ignore(ascii_case))]
    Cluster,
    #[token("PRINCIPAL", ignore(ascii_case))]
    Principal,
    #[token("RESOURCE", ignore(ascii_case))]
    Resource,

    // ── IMPORT RESOURCES keywords ────────────────────────────────────
    #[token("RESOURCES", ignore(ascii_case))]
    Resources,
    #[token("ALL", ignore(ascii_case))]
    All,

    // ── Additional keywords ──────────────────────────────────────────
    #[token("MEMBERS", ignore(ascii_case))]
    Members,
    #[token("MEMBER", ignore(ascii_case))]
    Member,
    #[token("POLICY", ignore(ascii_case))]
    Policy,
    #[token("PLACEMENT", ignore(ascii_case))]
    Placement,
    #[token("REQUIRE", ignore(ascii_case))]
    Require,
    #[token("METRIC", ignore(ascii_case))]
    Metric,
    #[token("INTERVAL", ignore(ascii_case))]
    Interval,
    #[token("GB", ignore(ascii_case))]
    Gb,

    // ── Conditional / idempotency keywords ─────────────────────────────
    #[token("IF", ignore(ascii_case))]
    If,
    #[token("EXISTS", ignore(ascii_case))]
    Exists,

    // ── Logical operators ────────────────────────────────────────────
    #[token("AND", ignore(ascii_case))]
    And,
    #[token("OR", ignore(ascii_case))]
    Or,
    #[token("NOT", ignore(ascii_case))]
    Not,
    #[token("IN", ignore(ascii_case))]
    In,
    #[token("LIKE", ignore(ascii_case))]
    Like,

    // ── Sort direction ───────────────────────────────────────────────
    #[token("ASC", ignore(ascii_case))]
    Asc,
    #[token("DESC", ignore(ascii_case))]
    Desc,

    // ── SHOW targets (plural forms) ─────────────────────────────────
    #[token("PROVIDERS", ignore(ascii_case))]
    Providers,
    #[token("CLUSTERS", ignore(ascii_case))]
    Clusters,
    #[token("CAPABILITIES", ignore(ascii_case))]
    Capabilities,
    #[token("GRANTS", ignore(ascii_case))]
    Grants,
    #[token("IMAGES", ignore(ascii_case))]
    Images,
    #[token("VERSION", ignore(ascii_case))]
    Version,

    // ── Boolean / null literals ───────────────────────────────────────
    #[token("true", ignore(ascii_case))]
    BoolTrue,
    #[token("false", ignore(ascii_case))]
    BoolFalse,
    #[token("NULL", ignore(ascii_case))]
    Null,

    // ── IS keyword (for IS NULL / IS NOT NULL) ────────────────────────
    #[token("IS", ignore(ascii_case))]
    Is,

    // ── Arithmetic operators ──────────────────────────────────────────
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,

    // ── String concatenation (must come before any other `|` token) ───
    #[token("||")]
    Concat,

    // ── OFFSET keyword ────────────────────────────────────────────────
    #[token("OFFSET", ignore(ascii_case))]
    Offset,

    // ── Numeric / duration literals ──────────────────────────────────
    #[regex(r"[0-9]+[smhd]", |lex| lex.slice().to_string(), priority = 3)]
    Duration(String),

    #[regex(r"[0-9]+\.[0-9]+", |lex| lex.slice().parse::<f64>().unwrap(), priority = 2)]
    Float(f64),

    #[regex(r"[0-9]+", |lex| lex.slice().parse::<i64>().unwrap())]
    Integer(i64),

    // ── String literal ───────────────────────────────────────────────
    #[regex(r"'([^']|'')*'", parse_string_lit)]
    StringLit(String),

    // ── Variable reference ────────────────────────────────────────────
    #[regex(r"@[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice()[1..].to_string())]
    Variable(String),

    // ── Identifier (lowest priority) ─────────────────────────────────
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice().to_string(), priority = 1)]
    Ident(String),
}

fn parse_string_lit(lex: &mut logos::Lexer<Token>) -> String {
    let slice = lex.slice();
    // Strip surrounding quotes and unescape ''
    slice[1..slice.len() - 1].replace("''", "'")
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Comma => write!(f, ","),
            Token::Semicolon => write!(f, ";"),
            Token::Colon => write!(f, ":"),
            Token::Dot => write!(f, "."),
            Token::Star => write!(f, "*"),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "!="),
            Token::Gte => write!(f, ">="),
            Token::Lte => write!(f, "<="),
            Token::Gt => write!(f, ">"),
            Token::Lt => write!(f, "<"),
            Token::Select => write!(f, "SELECT"),
            Token::Create => write!(f, "CREATE"),
            Token::Alter => write!(f, "ALTER"),
            Token::Destroy => write!(f, "DESTROY"),
            Token::Pause => write!(f, "PAUSE"),
            Token::Resume => write!(f, "RESUME"),
            Token::Snapshot => write!(f, "SNAPSHOT"),
            Token::Restore => write!(f, "RESTORE"),
            Token::Watch => write!(f, "WATCH"),
            Token::Attach => write!(f, "ATTACH"),
            Token::Detach => write!(f, "DETACH"),
            Token::Resize => write!(f, "RESIZE"),
            Token::Import => write!(f, "IMPORT"),
            Token::Publish => write!(f, "PUBLISH"),
            Token::Remove => write!(f, "REMOVE"),
            Token::Add => write!(f, "ADD"),
            Token::Grant => write!(f, "GRANT"),
            Token::Revoke => write!(f, "REVOKE"),
            Token::Set => write!(f, "SET"),
            Token::Backup => write!(f, "BACKUP"),
            Token::Scale => write!(f, "SCALE"),
            Token::Upgrade => write!(f, "UPGRADE"),
            Token::Explain => write!(f, "EXPLAIN"),
            Token::Rollback => write!(f, "ROLLBACK"),
            Token::Show => write!(f, "SHOW"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Order => write!(f, "ORDER"),
            Token::By => write!(f, "BY"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Group => write!(f, "GROUP"),
            Token::On => write!(f, "ON"),
            Token::Live => write!(f, "LIVE"),
            Token::Force => write!(f, "FORCE"),
            Token::Into => write!(f, "INTO"),
            Token::Tag => write!(f, "TAG"),
            Token::As => write!(f, "AS"),
            Token::To => write!(f, "TO"),
            Token::For => write!(f, "FOR"),
            Token::Last => write!(f, "LAST"),
            Token::MicroVm => write!(f, "MICROVM"),
            Token::Volume => write!(f, "VOLUME"),
            Token::Image => write!(f, "IMAGE"),
            Token::Provider => write!(f, "PROVIDER"),
            Token::Cluster => write!(f, "CLUSTER"),
            Token::Principal => write!(f, "PRINCIPAL"),
            Token::Resource => write!(f, "RESOURCE"),
            Token::Resources => write!(f, "RESOURCES"),
            Token::All => write!(f, "ALL"),
            Token::Members => write!(f, "MEMBERS"),
            Token::Member => write!(f, "MEMBER"),
            Token::Policy => write!(f, "POLICY"),
            Token::Placement => write!(f, "PLACEMENT"),
            Token::Require => write!(f, "REQUIRE"),
            Token::Metric => write!(f, "METRIC"),
            Token::Interval => write!(f, "INTERVAL"),
            Token::Gb => write!(f, "GB"),
            Token::If => write!(f, "IF"),
            Token::Exists => write!(f, "EXISTS"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::In => write!(f, "IN"),
            Token::Like => write!(f, "LIKE"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::Providers => write!(f, "PROVIDERS"),
            Token::Clusters => write!(f, "CLUSTERS"),
            Token::Capabilities => write!(f, "CAPABILITIES"),
            Token::Grants => write!(f, "GRANTS"),
            Token::Images => write!(f, "IMAGES"),
            Token::Version => write!(f, "VERSION"),
            Token::BoolTrue => write!(f, "true"),
            Token::BoolFalse => write!(f, "false"),
            Token::Null => write!(f, "NULL"),
            Token::Is => write!(f, "IS"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Concat => write!(f, "||"),
            Token::Assert => write!(f, "ASSERT"),
            Token::Offset => write!(f, "OFFSET"),
            Token::Duration(s) => write!(f, "{s}"),
            Token::Float(v) => write!(f, "{v}"),
            Token::Integer(v) => write!(f, "{v}"),
            Token::StringLit(s) => write!(f, "'{s}'"),
            Token::Variable(s) => write!(f, "@{s}"),
            Token::Ident(s) => write!(f, "{s}"),
        }
    }
}

// ── SpannedToken & tokenize ──────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Range<usize>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("unexpected token at position {position}")]
pub struct LexError {
    pub position: usize,
}

pub fn tokenize(source: &str) -> Result<Vec<SpannedToken>, LexError> {
    let mut tokens = Vec::new();
    let mut lexer = Token::lexer(source);

    while let Some(result) = lexer.next() {
        match result {
            Ok(token) => {
                tokens.push(SpannedToken {
                    token,
                    span: lexer.span(),
                });
            }
            Err(()) => {
                return Err(LexError {
                    position: lexer.span().start,
                });
            }
        }
    }

    Ok(tokens)
}
