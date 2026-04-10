use crate::ast::*;
use crate::error::*;
use crate::token::*;

pub struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
    source: String,
}

// ── Public API ──────────────────────────────────────────────────────

impl Parser {
    pub fn new(source: &str) -> Result<Self, ParseError> {
        let tokens = tokenize(source).map_err(|e| {
            let loc = offset_to_location(source, e.position);
            ParseError {
                kind: ParseErrorKind::InvalidToken,
                location: loc,
                found: None,
                expected: None,
                suggestion: None,
                source_line: get_source_line(source, loc.line),
            }
        })?;
        Ok(Self {
            tokens,
            pos: 0,
            source: source.to_string(),
        })
    }

    pub fn parse_program(&mut self) -> Result<Program, ParseError> {
        let mut statements = Vec::new();
        while !self.at_end() {
            if self.check(&Token::Semicolon) {
                self.advance();
                continue;
            }
            statements.push(self.parse_statement()?);
            self.eat(&Token::Semicolon);
        }
        Ok(Program { statements })
    }

    pub fn parse(source: &str) -> Result<Program, ParseError> {
        let mut parser = Self::new(source)?;
        parser.parse_program()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|st| &st.token)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset).map(|st| &st.token)
    }

    fn current_span(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|st| st.span.start)
            .unwrap_or(self.source.len())
    }

    fn advance(&mut self) -> Option<&SpannedToken> {
        if self.pos < self.tokens.len() {
            let t = &self.tokens[self.pos];
            self.pos += 1;
            Some(t)
        } else {
            None
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn check(&self, token: &Token) -> bool {
        self.peek()
            .map_or(false, |t| std::mem::discriminant(t) == std::mem::discriminant(token))
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.check(token) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: &Token) -> Result<(), ParseError> {
        if self.check(token) {
            self.advance();
            Ok(())
        } else {
            Err(self.error_expected(&format!("{}", token)))
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.peek().cloned() {
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error_expected("string literal")),
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.peek().cloned() {
            Some(Token::Ident(s)) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error_expected("identifier")),
        }
    }

    fn expect_integer(&mut self) -> Result<i64, ParseError> {
        match self.peek().cloned() {
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(n)
            }
            _ => Err(self.error_expected("integer")),
        }
    }

    fn expect_id_expr(&mut self) -> Result<String, ParseError> {
        self.expect_string()
    }

    fn error_here(&self, kind: ParseErrorKind) -> ParseError {
        let offset = self.current_span();
        let loc = offset_to_location(&self.source, offset);
        ParseError {
            kind,
            location: loc,
            found: self.peek().map(|t| format!("{}", t)),
            expected: None,
            suggestion: None,
            source_line: get_source_line(&self.source, loc.line),
        }
    }

    fn error_expected(&self, expected: &str) -> ParseError {
        let offset = self.current_span();
        let loc = offset_to_location(&self.source, offset);
        ParseError {
            kind: ParseErrorKind::UnexpectedToken,
            location: loc,
            found: self.peek().map(|t| format!("{}", t)),
            expected: Some(expected.to_string()),
            suggestion: None,
            source_line: get_source_line(&self.source, loc.line),
        }
    }

    /// Try to consume the current token as an identifier name.
    /// This handles keywords that are also valid identifier names in context
    /// (e.g., `image`, `type`, `status`, `principal` can be param keys or field names).
    fn try_ident_like(&mut self) -> Option<String> {
        let name = self.peek_ident_like()?;
        self.advance();
        Some(name)
    }

    /// Peek at the current token as an identifier name without consuming.
    fn peek_ident_like(&self) -> Option<String> {
        match self.peek() {
            Some(Token::Ident(s)) => Some(s.clone()),
            // Keywords that commonly appear as param keys or field names
            Some(t) => {
                let s = format!("{}", t);
                // If it looks like a word (not punctuation/operator), treat as ident
                if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    Some(s.to_lowercase())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn expect_ident_like(&mut self) -> Result<String, ParseError> {
        self.try_ident_like()
            .ok_or_else(|| self.error_expected("identifier"))
    }
}

// ── Statement Dispatch ───────────────────────────────────────────────

impl Parser {
    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Some(Token::Explain) => {
                self.advance();
                let inner = self.parse_statement()?;
                Ok(Statement::Explain(Box::new(inner)))
            }
            Some(Token::Select) => self.parse_select().map(Statement::Select),
            Some(Token::Create) => self.parse_create(),
            Some(Token::Alter) => self.parse_alter(),
            Some(Token::Destroy) => self.parse_destroy_dispatch(),
            Some(Token::Pause) => self.parse_pause().map(Statement::Pause),
            Some(Token::Resume) => self.parse_resume().map(Statement::Resume),
            Some(Token::Snapshot) => self.parse_snapshot().map(Statement::Snapshot),
            Some(Token::Restore) => self.parse_restore(),
            Some(Token::Watch) => self.parse_watch().map(Statement::Watch),
            Some(Token::Attach) => self.parse_attach().map(Statement::Attach),
            Some(Token::Detach) => self.parse_detach().map(Statement::Detach),
            Some(Token::Resize) => self.parse_resize().map(Statement::Resize),
            Some(Token::Import) => self.parse_import_image().map(Statement::ImportImage),
            Some(Token::Publish) => self.parse_publish_image().map(Statement::PublishImage),
            Some(Token::Remove) => self.parse_remove(),
            Some(Token::Add) => self.parse_add(),
            Some(Token::Grant) => self.parse_grant().map(Statement::Grant),
            Some(Token::Revoke) => self.parse_revoke().map(Statement::Revoke),
            Some(Token::Set) => self.parse_set_stmt().map(Statement::Set),
            Some(Token::Show) => self.parse_show().map(Statement::Show),
            Some(Token::Backup) => self.parse_backup().map(Statement::Backup),
            Some(Token::Scale) => self.parse_scale().map(Statement::Scale),
            Some(Token::Upgrade) => self.parse_upgrade().map(Statement::Upgrade),
            Some(Token::Rollback) => self.parse_rollback().map(Statement::Rollback),
            Some(Token::Assert) => self.parse_assert().map(Statement::Assert),
            Some(_) => Err(self.error_expected(
                "statement keyword (SELECT, CREATE, ALTER, DESTROY, BACKUP, SCALE, UPGRADE, ROLLBACK, ASSERT, ...)",
            )),
            None => Err(self.error_here(ParseErrorKind::UnexpectedEof)),
        }
    }

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Create)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.check(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Not)?; // NOT
            self.expect(&Token::Exists)?; // EXISTS
            true
        } else {
            false
        };

        match self.peek() {
            Some(Token::MicroVm) => {
                self.advance();
                let mut stmt = self.parse_create_microvm()?;
                stmt.if_not_exists = if_not_exists;
                Ok(Statement::CreateMicrovm(stmt))
            }
            Some(Token::Volume) => {
                self.advance();
                let mut stmt = self.parse_create_volume()?;
                stmt.if_not_exists = if_not_exists;
                Ok(Statement::CreateVolume(stmt))
            }
            Some(Token::Resource) => {
                self.advance();
                let mut stmt = self.parse_create_resource()?;
                stmt.if_not_exists = if_not_exists;
                Ok(Statement::CreateResource(stmt))
            }
            _ => Err(self.error_expected("MICROVM, VOLUME, or RESOURCE after CREATE")),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Alter)?;
        match self.peek() {
            Some(Token::MicroVm) => {
                self.advance();
                self.parse_alter_microvm().map(Statement::AlterMicrovm)
            }
            Some(Token::Volume) => {
                self.advance();
                self.parse_alter_volume().map(Statement::AlterVolume)
            }
            Some(Token::Provider) => {
                self.advance();
                self.parse_alter_provider().map(Statement::AlterProvider)
            }
            Some(Token::Cluster) => {
                self.advance();
                self.parse_alter_cluster().map(Statement::AlterCluster)
            }
            Some(Token::Resource) => {
                self.advance();
                self.parse_alter_resource().map(Statement::AlterResource)
            }
            _ => Err(self.error_expected(
                "MICROVM, VOLUME, PROVIDER, CLUSTER, or RESOURCE after ALTER",
            )),
        }
    }

    fn parse_remove(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Remove)?;
        match self.peek() {
            Some(Token::Image) => {
                self.advance();
                self.parse_remove_image().map(Statement::RemoveImage)
            }
            Some(Token::Provider) => {
                self.advance();
                let name = self.expect_string()?;
                Ok(Statement::RemoveProvider(RemoveProviderStmt { name }))
            }
            Some(Token::Cluster) => {
                self.advance();
                let name = self.expect_string()?;
                Ok(Statement::RemoveCluster(RemoveClusterStmt { name }))
            }
            _ => Err(self.error_expected("IMAGE, PROVIDER, or CLUSTER after REMOVE")),
        }
    }

    fn parse_add(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Add)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.check(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Not)?; // NOT
            self.expect(&Token::Exists)?; // EXISTS
            true
        } else {
            false
        };

        match self.peek() {
            Some(Token::Provider) => {
                self.advance();
                let params = self.parse_param_list()?;
                Ok(Statement::AddProvider(AddProviderStmt { if_not_exists, params }))
            }
            Some(Token::Cluster) => {
                self.advance();
                let mut stmt = self.parse_add_cluster()?;
                stmt.if_not_exists = if_not_exists;
                Ok(Statement::AddCluster(stmt))
            }
            Some(Token::Principal) => {
                self.advance();
                let params = self.parse_param_list()?;
                Ok(Statement::AddPrincipal(AddPrincipalStmt { if_not_exists, params }))
            }
            _ => Err(self.error_expected(
                "PROVIDER, CLUSTER, or PRINCIPAL after ADD",
            )),
        }
    }
}

// ── SELECT ───────────────────────────────────────────────────────────

impl Parser {
    fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        self.expect(&Token::Select)?;
        let fields = self.parse_field_list()?;
        self.expect(&Token::From)?;
        let from = self.parse_select_source()?;

        let on = if self.check(&Token::On) {
            Some(self.parse_target_spec()?)
        } else {
            None
        };

        let where_clause = if self.eat(&Token::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };

        let group_by = if self.check(&Token::Group) {
            self.advance();
            self.expect(&Token::By)?;
            Some(self.parse_field_list()?)
        } else {
            None
        };

        let order_by = if self.check(&Token::Order) {
            self.advance();
            self.expect(&Token::By)?;
            Some(self.parse_order_list()?)
        } else {
            None
        };

        let limit = if self.eat(&Token::Limit) {
            Some(self.expect_integer()?)
        } else {
            None
        };

        let offset = if self.eat(&Token::Offset) {
            Some(self.expect_integer()?)
        } else {
            None
        };

        Ok(SelectStmt {
            fields,
            from,
            on,
            where_clause,
            group_by,
            order_by,
            limit,
            offset,
        })
    }
}

// ── CREATE MICROVM / VOLUME ──────────────────────────────────────────

impl Parser {
    fn parse_create_microvm(&mut self) -> Result<CreateMicrovmStmt, ParseError> {
        let params = self.parse_param_list()?;

        let mut volumes = Vec::new();
        while self.check(&Token::Volume) {
            self.advance();
            self.expect(&Token::LParen)?;
            let vparams = self.parse_param_list()?;
            self.expect(&Token::RParen)?;
            volumes.push(VolumeInline { params: vparams });
        }

        let on = if self.check(&Token::On) {
            Some(self.parse_target_spec()?)
        } else {
            None
        };

        let placement_policy = if self.check(&Token::Placement) {
            self.advance();
            self.expect(&Token::Policy)?;
            self.expect(&Token::Eq)?;
            Some(self.expect_string()?)
        } else {
            None
        };

        let mut require = Vec::new();
        if self.eat(&Token::Require) {
            loop {
                require.push(self.parse_require_clause()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }

        Ok(CreateMicrovmStmt {
            if_not_exists: false,
            params,
            volumes,
            on,
            placement_policy,
            require,
        })
    }

    fn parse_create_volume(&mut self) -> Result<CreateVolumeStmt, ParseError> {
        let params = self.parse_param_list()?;
        let on = if self.check(&Token::On) {
            Some(self.parse_target_spec()?)
        } else {
            None
        };
        Ok(CreateVolumeStmt { if_not_exists: false, params, on })
    }

    fn parse_require_clause(&mut self) -> Result<RequireClause, ParseError> {
        let key = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        match key.as_str() {
            "capability" => Ok(RequireClause::Capability(self.expect_string()?)),
            "provider" => Ok(RequireClause::Provider(self.expect_string()?)),
            "label" => {
                let label_key = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                let label_val = self.expect_string()?;
                Ok(RequireClause::Label {
                    key: label_key,
                    value: label_val,
                })
            }
            _ => Err(self.error_expected("capability, provider, or label")),
        }
    }
}

// ── ALTER ─────────────────────────────────────────────────────────────

impl Parser {
    fn parse_alter_microvm(&mut self) -> Result<AlterMicrovmStmt, ParseError> {
        let id = self.expect_id_expr()?;
        self.expect(&Token::Set)?;
        let set_items = self.parse_set_list()?;
        Ok(AlterMicrovmStmt { id, set_items })
    }

    fn parse_alter_volume(&mut self) -> Result<AlterVolumeStmt, ParseError> {
        let id = self.expect_id_expr()?;
        self.expect(&Token::Set)?;
        let set_items = self.parse_set_list()?;
        Ok(AlterVolumeStmt { id, set_items })
    }

    fn parse_alter_provider(&mut self) -> Result<AlterProviderStmt, ParseError> {
        let name = self.expect_string()?;
        self.expect(&Token::Set)?;
        let set_items = self.parse_set_list()?;
        Ok(AlterProviderStmt { name, set_items })
    }

    fn parse_alter_cluster(&mut self) -> Result<AlterClusterStmt, ParseError> {
        let name = self.expect_string()?;
        let action = match self.peek() {
            Some(Token::Add) => {
                self.advance();
                self.expect(&Token::Member)?;
                ClusterAlterAction::AddMember(self.expect_string()?)
            }
            Some(Token::Remove) => {
                self.advance();
                self.expect(&Token::Member)?;
                ClusterAlterAction::RemoveMember(self.expect_string()?)
            }
            _ => return Err(self.error_expected("ADD MEMBER or REMOVE MEMBER")),
        };
        Ok(AlterClusterStmt { name, action })
    }
}

// ── DESTROY ──────────────────────────────────────────────────────────

impl Parser {
    fn parse_destroy_dispatch(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Destroy)?;
        match self.peek() {
            Some(Token::MicroVm) => {
                self.advance();
                let id = self.expect_id_expr()?;
                let force = self.eat(&Token::Force);
                Ok(Statement::Destroy(DestroyStmt { target: DestroyTarget::Microvm, id, force }))
            }
            Some(Token::Volume) => {
                self.advance();
                let id = self.expect_id_expr()?;
                let force = self.eat(&Token::Force);
                Ok(Statement::Destroy(DestroyStmt { target: DestroyTarget::Volume, id, force }))
            }
            Some(Token::Resource) => {
                self.advance();
                self.parse_destroy_resource().map(Statement::DestroyResource)
            }
            _ => Err(self.error_expected("MICROVM, VOLUME, or RESOURCE after DESTROY")),
        }
    }
}

// ── RESOURCE ─────────────────────────────────────────────────────────

impl Parser {
    fn parse_create_resource(&mut self) -> Result<CreateResourceStmt, ParseError> {
        let resource_type = self.expect_string()?;
        let params = self.parse_param_list()?;
        let on = if self.check(&Token::On) {
            Some(self.parse_target_spec()?)
        } else {
            None
        };
        Ok(CreateResourceStmt { if_not_exists: false, resource_type, params, on })
    }

    fn parse_alter_resource(&mut self) -> Result<AlterResourceStmt, ParseError> {
        let resource_type = self.expect_string()?;
        let id = self.expect_string()?;
        self.expect(&Token::Set)?;
        let set_items = self.parse_set_list()?;
        Ok(AlterResourceStmt { resource_type, id, set_items })
    }

    fn parse_destroy_resource(&mut self) -> Result<DestroyResourceStmt, ParseError> {
        let resource_type = self.expect_string()?;
        let id = self.expect_string()?;
        let force = self.eat(&Token::Force);
        Ok(DestroyResourceStmt { resource_type, id, force })
    }
}

// ── Lifecycle: PAUSE, RESUME, SNAPSHOT, RESTORE ──────────────────────

impl Parser {
    fn parse_pause(&mut self) -> Result<PauseStmt, ParseError> {
        self.expect(&Token::Pause)?;
        self.expect(&Token::MicroVm)?;
        let id = self.expect_id_expr()?;
        Ok(PauseStmt { id })
    }

    fn parse_resume(&mut self) -> Result<ResumeStmt, ParseError> {
        self.expect(&Token::Resume)?;
        self.expect(&Token::MicroVm)?;
        let id = self.expect_id_expr()?;
        Ok(ResumeStmt { id })
    }

    fn parse_snapshot(&mut self) -> Result<SnapshotStmt, ParseError> {
        self.expect(&Token::Snapshot)?;
        self.expect(&Token::MicroVm)?;
        let id = self.expect_id_expr()?;
        self.expect(&Token::Into)?;
        let destination = self.expect_string()?;
        let tag = if self.eat(&Token::Tag) {
            Some(self.expect_string()?)
        } else {
            None
        };
        Ok(SnapshotStmt {
            id,
            destination,
            tag,
        })
    }

    fn parse_restore(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Restore)?;
        match self.peek() {
            Some(Token::MicroVm) => {
                self.advance();
                let id = self.expect_id_expr()?;
                self.expect(&Token::From)?;
                let source = self.expect_string()?;
                Ok(Statement::Restore(RestoreStmt { id, source }))
            }
            Some(Token::Resource) => {
                self.advance();
                let resource_type = self.expect_string()?;
                let id = self.expect_string()?;
                self.expect(&Token::From)?;
                let source = self.expect_string()?;
                Ok(Statement::RestoreResource(RestoreResourceStmt {
                    resource_type,
                    id,
                    source,
                }))
            }
            _ => Err(self.error_expected("MICROVM or RESOURCE after RESTORE")),
        }
    }
}

// ── Day-2 Operations: BACKUP, SCALE, UPGRADE ────────────────────────

impl Parser {
    fn parse_backup(&mut self) -> Result<BackupStmt, ParseError> {
        self.expect(&Token::Backup)?;
        self.expect(&Token::Resource)?;
        let resource_type = self.expect_string()?;
        let id = self.expect_string()?;
        let destination = if self.eat(&Token::Into) {
            Some(self.expect_string()?)
        } else {
            None
        };
        let tag = if self.eat(&Token::Tag) {
            Some(self.expect_string()?)
        } else {
            None
        };
        Ok(BackupStmt {
            resource_type,
            id,
            destination,
            tag,
        })
    }

    fn parse_scale(&mut self) -> Result<ScaleStmt, ParseError> {
        self.expect(&Token::Scale)?;
        self.expect(&Token::Resource)?;
        let resource_type = self.expect_string()?;
        let id = self.expect_string()?;
        let params = self.parse_param_list()?;
        Ok(ScaleStmt {
            resource_type,
            id,
            params,
        })
    }

    fn parse_upgrade(&mut self) -> Result<UpgradeStmt, ParseError> {
        self.expect(&Token::Upgrade)?;
        self.expect(&Token::Resource)?;
        let resource_type = self.expect_string()?;
        let id = self.expect_string()?;
        let params = self.parse_param_list()?;
        Ok(UpgradeStmt {
            resource_type,
            id,
            params,
        })
    }

    fn parse_rollback(&mut self) -> Result<RollbackStmt, ParseError> {
        self.expect(&Token::Rollback)?;
        let target = match self.peek() {
            Some(Token::Last) => {
                self.advance();
                RollbackTarget::Last
            }
            Some(Token::To) => {
                self.advance();
                self.expect(&Token::Tag)?;
                let tag = self.expect_string()?;
                RollbackTarget::Tag(tag)
            }
            Some(Token::Resource) => {
                self.advance();
                let resource_type = self.expect_string()?;
                let id = self.expect_string()?;
                RollbackTarget::Resource { resource_type, id }
            }
            _ => return Err(self.error_expected("LAST, TO TAG, or RESOURCE after ROLLBACK")),
        };
        Ok(RollbackStmt { target })
    }

    fn parse_assert(&mut self) -> Result<AssertStmt, ParseError> {
        self.expect(&Token::Assert)?;
        let condition = self.parse_predicate()?;
        let message = if self.eat(&Token::Comma) {
            Some(self.expect_string()?)
        } else {
            None
        };
        Ok(AssertStmt { condition, message })
    }
}

// ── WATCH ────────────────────────────────────────────────────────────

impl Parser {
    fn parse_watch(&mut self) -> Result<WatchStmt, ParseError> {
        self.expect(&Token::Watch)?;
        self.expect(&Token::Metric)?;
        let metrics = self.parse_field_list()?;
        self.expect(&Token::From)?;
        let from = self.parse_noun()?;
        let where_clause = if self.eat(&Token::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };
        self.expect(&Token::Interval)?;
        let interval = self.parse_duration()?;
        Ok(WatchStmt {
            metrics,
            from,
            where_clause,
            interval,
        })
    }
}

// ── ATTACH / DETACH / RESIZE ─────────────────────────────────────────

impl Parser {
    fn parse_attach(&mut self) -> Result<AttachStmt, ParseError> {
        self.expect(&Token::Attach)?;
        self.expect(&Token::Volume)?;
        let volume_id = self.expect_id_expr()?;
        self.expect(&Token::To)?;
        self.expect(&Token::MicroVm)?;
        let microvm_id = self.expect_id_expr()?;
        let device_name = if self.eat(&Token::As) {
            Some(self.expect_string()?)
        } else {
            None
        };
        Ok(AttachStmt {
            volume_id,
            microvm_id,
            device_name,
        })
    }

    fn parse_detach(&mut self) -> Result<DetachStmt, ParseError> {
        self.expect(&Token::Detach)?;
        self.expect(&Token::Volume)?;
        let volume_id = self.expect_id_expr()?;
        self.expect(&Token::From)?;
        self.expect(&Token::MicroVm)?;
        let microvm_id = self.expect_id_expr()?;
        Ok(DetachStmt {
            volume_id,
            microvm_id,
        })
    }

    fn parse_resize(&mut self) -> Result<ResizeStmt, ParseError> {
        self.expect(&Token::Resize)?;
        self.expect(&Token::Volume)?;
        let volume_id = self.expect_id_expr()?;
        self.expect(&Token::To)?;
        let new_size_gb = self.expect_integer()?;
        self.expect(&Token::Gb)?;
        Ok(ResizeStmt {
            volume_id,
            new_size_gb,
        })
    }
}

// ── Image Management ─────────────────────────────────────────────────

impl Parser {
    fn parse_import_image(&mut self) -> Result<ImportImageStmt, ParseError> {
        self.expect(&Token::Import)?;
        self.expect(&Token::Image)?;
        let params = self.parse_param_list()?;
        Ok(ImportImageStmt { params })
    }

    fn parse_publish_image(&mut self) -> Result<PublishImageStmt, ParseError> {
        self.expect(&Token::Publish)?;
        self.expect(&Token::Image)?;
        let image_id = self.expect_id_expr()?;
        self.expect(&Token::To)?;
        self.expect(&Token::Provider)?;
        let provider = self.expect_string()?;
        Ok(PublishImageStmt { image_id, provider })
    }

    fn parse_remove_image(&mut self) -> Result<RemoveImageStmt, ParseError> {
        let image_id = self.expect_id_expr()?;
        let force = self.eat(&Token::Force);
        Ok(RemoveImageStmt { image_id, force })
    }
}

// ── Cluster ──────────────────────────────────────────────────────────

impl Parser {
    fn parse_add_cluster(&mut self) -> Result<AddClusterStmt, ParseError> {
        let name = self.expect_string()?;
        self.expect(&Token::Members)?;
        self.expect(&Token::Eq)?;
        self.expect(&Token::LBracket)?;
        let mut members = Vec::new();
        if !self.check(&Token::RBracket) {
            members.push(self.expect_string()?);
            while self.eat(&Token::Comma) {
                members.push(self.expect_string()?);
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(AddClusterStmt { if_not_exists: false, name, members })
    }
}

// ── Access Control ───────────────────────────────────────────────────

impl Parser {
    fn parse_grant(&mut self) -> Result<GrantStmt, ParseError> {
        self.expect(&Token::Grant)?;
        let verbs = self.parse_verb_list()?;
        self.expect(&Token::On)?;
        let scope = self.parse_grant_scope()?;
        let where_clause = if self.eat(&Token::Where) {
            Some(self.parse_predicate()?)
        } else {
            None
        };
        self.expect(&Token::To)?;
        let principal = self.expect_string()?;
        Ok(GrantStmt {
            verbs,
            scope,
            where_clause,
            principal,
        })
    }

    fn parse_revoke(&mut self) -> Result<RevokeStmt, ParseError> {
        self.expect(&Token::Revoke)?;
        let verbs = self.parse_verb_list()?;
        self.expect(&Token::On)?;
        let scope = self.parse_grant_scope()?;
        self.expect(&Token::From)?;
        let principal = self.expect_string()?;
        Ok(RevokeStmt {
            verbs,
            scope,
            principal,
        })
    }

    fn parse_verb_list(&mut self) -> Result<Vec<Verb>, ParseError> {
        let mut verbs = vec![self.parse_verb()?];
        while self.eat(&Token::Comma) {
            verbs.push(self.parse_verb()?);
        }
        Ok(verbs)
    }

    fn parse_verb(&mut self) -> Result<Verb, ParseError> {
        let v = match self.peek() {
            Some(Token::Select) => Verb::Select,
            Some(Token::Create) => Verb::Create,
            Some(Token::Alter) => Verb::Alter,
            Some(Token::Destroy) => Verb::Destroy,
            Some(Token::Pause) => Verb::Pause,
            Some(Token::Resume) => Verb::Resume,
            Some(Token::Snapshot) => Verb::Snapshot,
            Some(Token::Restore) => Verb::Restore,
            Some(Token::Attach) => Verb::Attach,
            Some(Token::Detach) => Verb::Detach,
            Some(Token::Resize) => Verb::Resize,
            Some(Token::Watch) => Verb::Watch,
            Some(Token::Import) => Verb::Import,
            Some(Token::Publish) => Verb::Publish,
            _ => return Err(self.error_expected("verb (SELECT, CREATE, ALTER, ...)")),
        };
        self.advance();
        Ok(v)
    }

    fn parse_grant_scope(&mut self) -> Result<GrantScope, ParseError> {
        match self.peek().cloned() {
            Some(Token::Cluster) => {
                self.advance();
                Ok(GrantScope::Cluster(self.expect_string()?))
            }
            Some(Token::Provider) => {
                self.advance();
                Ok(GrantScope::Provider(self.expect_string()?))
            }
            _ => {
                // Handle noun identifiers (may be Ident or keyword tokens)
                if let Some(name) = self.peek_ident_like() {
                    match name.to_lowercase().as_str() {
                        "microvms" => { self.advance(); Ok(GrantScope::Microvms) }
                        "volumes" => { self.advance(); Ok(GrantScope::Volumes) }
                        "images" => { self.advance(); Ok(GrantScope::Images) }
                        _ => Err(self.error_expected(
                            "CLUSTER, PROVIDER, microvms, volumes, or images",
                        )),
                    }
                } else {
                    Err(self.error_expected(
                        "CLUSTER, PROVIDER, microvms, volumes, or images",
                    ))
                }
            }
        }
    }
}

// ── SET / SHOW ───────────────────────────────────────────────────────

impl Parser {
    fn parse_set_stmt(&mut self) -> Result<SetStmt, ParseError> {
        self.expect(&Token::Set)?;
        // Check for @variable syntax
        if let Some(Token::Variable(name)) = self.peek().cloned() {
            self.advance();
            self.expect(&Token::Eq)?;
            let value = self.parse_value()?;
            return Ok(SetStmt { key: format!("@{name}"), value });
        }
        let key = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SetStmt { key, value })
    }

    fn parse_show(&mut self) -> Result<ShowStmt, ParseError> {
        self.expect(&Token::Show)?;
        let target = match self.peek() {
            Some(Token::Providers) => {
                self.advance();
                ShowTarget::Providers
            }
            Some(Token::Clusters) => {
                self.advance();
                ShowTarget::Clusters
            }
            Some(Token::Capabilities) => {
                self.advance();
                let for_provider = if self.eat(&Token::For) {
                    self.expect(&Token::Provider)?;
                    Some(self.expect_string()?)
                } else {
                    None
                };
                ShowTarget::Capabilities { for_provider }
            }
            Some(Token::Grants) => {
                self.advance();
                let for_principal = if self.eat(&Token::For) {
                    Some(self.expect_string()?)
                } else {
                    None
                };
                ShowTarget::Grants { for_principal }
            }
            Some(Token::Images) => {
                self.advance();
                ShowTarget::Images
            }
            Some(Token::Version) => {
                self.advance();
                ShowTarget::Version
            }
            _ => {
                return Err(self.error_expected(
                    "PROVIDERS, CLUSTERS, CAPABILITIES, GRANTS, IMAGES, or VERSION",
                ));
            }
        };
        Ok(ShowStmt { target })
    }
}

// ── Clause Helpers ───────────────────────────────────────────────────

impl Parser {
    fn parse_field_list(&mut self) -> Result<FieldList, ParseError> {
        if self.eat(&Token::Star) {
            return Ok(FieldList::All);
        }
        let mut fields = vec![self.parse_field()?];
        while self.eat(&Token::Comma) {
            fields.push(self.parse_field()?);
        }
        Ok(FieldList::Fields(fields))
    }

    fn parse_field(&mut self) -> Result<Field, ParseError> {
        let name = self.expect_ident_like()?;
        // Function call in projection: `count(*)`, `sum(x)`, etc.
        if self.check(&Token::LParen) {
            self.expect(&Token::LParen)?;
            if self.eat(&Token::Star) {
                self.expect(&Token::RParen)?;
                return Ok(Field::FnCall {
                    name,
                    star: true,
                    args: vec![],
                });
            }
            let mut args = Vec::new();
            if !self.check(&Token::RParen) {
                args.push(self.parse_expr()?);
                while self.eat(&Token::Comma) {
                    args.push(self.parse_expr()?);
                }
            }
            self.expect(&Token::RParen)?;
            return Ok(Field::FnCall {
                name,
                star: false,
                args,
            });
        }
        if self.eat(&Token::Dot) {
            let sub = self.expect_ident_like()?;
            Ok(Field::Qualified(name, sub))
        } else {
            Ok(Field::Simple(name))
        }
    }

    /// Parse the target of a `FROM` clause: either a built-in noun or a
    /// table-valued function call (e.g. `dns_lookup('example.com', 'A')`).
    fn parse_select_source(&mut self) -> Result<SelectSource, ParseError> {
        // Look-ahead: if we see an identifier-like token immediately followed
        // by `(`, it's a table-valued function. Otherwise parse a noun.
        let is_function = self.peek_ident_like().is_some()
            && self.peek_at(1) == Some(&Token::LParen);

        if !is_function {
            return self.parse_noun().map(SelectSource::Noun);
        }

        let name = self.expect_ident_like()?;
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if !self.check(&Token::RParen) {
            args.push(self.parse_expr()?);
            while self.eat(&Token::Comma) {
                args.push(self.parse_expr()?);
            }
        }
        self.expect(&Token::RParen)?;
        Ok(SelectSource::Function(FunctionCall { name, args }))
    }

    fn parse_noun(&mut self) -> Result<Noun, ParseError> {
        // Nouns may be identifiers or keyword tokens that overlap
        let noun_str = self
            .peek_ident_like()
            .ok_or_else(|| self.error_here(ParseErrorKind::ExpectedIdentifier))?;

        let noun = match noun_str.to_lowercase().as_str() {
            "microvms" => Noun::Microvms,
            "volumes" => Noun::Volumes,
            "images" => Noun::Images,
            "providers" => Noun::Providers,
            "clusters" => Noun::Clusters,
            "capabilities" => Noun::Capabilities,
            "snapshots" => Noun::Snapshots,
            "metrics" => Noun::Metrics,
            "events" => Noun::Events,
            "query_history" => Noun::QueryHistory,
            "audit_log" => Noun::AuditLog,
            "principals" => Noun::Principals,
            "grants" => Noun::Grants,
            "cluster_members" => Noun::ClusterMembers,
            "plans" => Noun::Plans,
            "resources" => Noun::Resources,
            "applied_files" => Noun::AppliedFiles,
            "k8s_pods" => Noun::K8sPods,
            "k8s_deployments" => Noun::K8sDeployments,
            "k8s_services" => Noun::K8sServices,
            "k8s_ingresses" => Noun::K8sIngresses,
            "k8s_configmaps" => Noun::K8sConfigmaps,
            "k8s_secrets" => Noun::K8sSecrets,
            "k8s_namespaces" => Noun::K8sNamespaces,
            "k8s_nodes" => Noun::K8sNodes,
            other => {
                return Err(self.error_here(ParseErrorKind::InvalidNoun {
                    found: other.to_string(),
                }));
            }
        };
        self.advance();
        Ok(noun)
    }

    fn parse_target_spec(&mut self) -> Result<TargetSpec, ParseError> {
        self.expect(&Token::On)?;
        let target = match self.peek() {
            Some(Token::Provider) => {
                self.advance();
                TargetKind::Provider(self.expect_string()?)
            }
            Some(Token::Cluster) => {
                self.advance();
                TargetKind::Cluster(self.expect_string()?)
            }
            _ => return Err(self.error_expected("PROVIDER or CLUSTER after ON")),
        };
        let live = self.eat(&Token::Live);
        Ok(TargetSpec { target, live })
    }

    fn parse_order_list(&mut self) -> Result<Vec<OrderItem>, ParseError> {
        let mut items = vec![self.parse_order_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_order_item()?);
        }
        Ok(items)
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, ParseError> {
        let field = self.expect_ident_like()?;
        let direction = if self.eat(&Token::Asc) {
            SortDirection::Asc
        } else if self.eat(&Token::Desc) {
            SortDirection::Desc
        } else {
            SortDirection::default()
        };
        Ok(OrderItem { field, direction })
    }

    fn parse_set_list(&mut self) -> Result<Vec<SetItem>, ParseError> {
        let mut items = vec![self.parse_set_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_set_item()?);
        }
        Ok(items)
    }

    fn parse_set_item(&mut self) -> Result<SetItem, ParseError> {
        let key = self.expect_ident_like()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(Param { key, value })
    }

    fn parse_param_list(&mut self) -> Result<Vec<Param>, ParseError> {
        let mut params = Vec::new();
        // Greedy: consume `ident_like = value` while pattern matches.
        // Keywords like `image`, `type`, `status` are valid param keys.
        while self.peek_ident_like().is_some() {
            if self.peek_at(1) != Some(&Token::Eq) {
                break;
            }
            let key = self.try_ident_like().unwrap();
            self.expect(&Token::Eq)?;
            let value = self.parse_value()?;
            params.push(Param { key, value });
        }
        Ok(params)
    }
}

// ── Predicate Parsing (precedence: NOT > AND > OR) ───────────────────

impl Parser {
    fn parse_predicate(&mut self) -> Result<Predicate, ParseError> {
        self.parse_or_predicate()
    }

    fn parse_or_predicate(&mut self) -> Result<Predicate, ParseError> {
        let mut left = self.parse_and_predicate()?;
        while self.eat(&Token::Or) {
            let right = self.parse_and_predicate()?;
            left = Predicate::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_predicate(&mut self) -> Result<Predicate, ParseError> {
        let mut left = self.parse_not_predicate()?;
        while self.eat(&Token::And) {
            let right = self.parse_not_predicate()?;
            left = Predicate::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not_predicate(&mut self) -> Result<Predicate, ParseError> {
        if self.check(&Token::Not) {
            // Check if this is NOT as unary predicate operator vs NOT IN
            // If NOT is followed by IN, it's part of a comparison, not unary NOT
            if self.peek_at(1) == Some(&Token::In) {
                // This is a comparison with NOT IN, handled by parse_comparison
                return self.parse_primary_predicate();
            }
            self.advance(); // consume NOT
            let inner = self.parse_not_predicate()?;
            Ok(Predicate::Not(Box::new(inner)))
        } else {
            self.parse_primary_predicate()
        }
    }

    fn parse_primary_predicate(&mut self) -> Result<Predicate, ParseError> {
        // EXISTS ( SELECT ... )
        if self.check(&Token::Exists) {
            self.advance();
            self.expect(&Token::LParen)?;
            let select = self.parse_select()?;
            self.expect(&Token::RParen)?;
            return Ok(Predicate::Exists(Box::new(select)));
        }
        // `( SELECT ... )` at this position is a scalar subquery that belongs
        // to a comparison expression, not a grouped predicate. Defer to
        // `parse_comparison` so the expression parser can handle it.
        if self.check(&Token::LParen) && matches!(self.peek_at(1), Some(Token::Select)) {
            return self.parse_comparison().map(Predicate::Comparison);
        }
        if self.eat(&Token::LParen) {
            let inner = self.parse_predicate()?;
            self.expect(&Token::RParen)?;
            Ok(Predicate::Grouped(Box::new(inner)))
        } else {
            self.parse_comparison().map(Predicate::Comparison)
        }
    }

    fn parse_comparison(&mut self) -> Result<Comparison, ParseError> {
        let left = self.parse_expr()?;

        // IS NULL / IS NOT NULL — no right-hand expression
        if self.eat(&Token::Is) {
            if self.eat(&Token::Not) {
                self.expect(&Token::Null)?;
                return Ok(Comparison {
                    left,
                    op: ComparisonOp::IsNotNull,
                    right: Expr::Null,
                });
            }
            self.expect(&Token::Null)?;
            return Ok(Comparison {
                left,
                op: ComparisonOp::IsNull,
                right: Expr::Null,
            });
        }

        let op = self.parse_comparison_op()?;
        let right = self.parse_expr()?;
        Ok(Comparison { left, op, right })
    }

    fn parse_comparison_op(&mut self) -> Result<ComparisonOp, ParseError> {
        let op = match self.peek() {
            Some(Token::Eq) => ComparisonOp::Eq,
            Some(Token::Neq) => ComparisonOp::NotEq,
            Some(Token::Gt) => ComparisonOp::Gt,
            Some(Token::Lt) => ComparisonOp::Lt,
            Some(Token::Gte) => ComparisonOp::GtEq,
            Some(Token::Lte) => ComparisonOp::LtEq,
            Some(Token::In) => ComparisonOp::In,
            Some(Token::Like) => ComparisonOp::Like,
            Some(Token::Not) => {
                self.advance();
                self.expect(&Token::In)?;
                return Ok(ComparisonOp::NotIn);
            }
            _ => {
                return Err(
                    self.error_expected("comparison operator (=, !=, >, <, IN, LIKE, ...)")
                );
            }
        };
        self.advance();
        Ok(op)
    }
}

// ── Expression Parsing ───────────────────────────────────────────────

impl Parser {
    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary_expr()?;

        // Left-fold chains of `+`, `-`, and `||` at the same precedence
        // level. For our use case keeping concat at arithmetic level is fine.
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinaryOp::Add,
                Some(Token::Minus) => BinaryOp::Subtract,
                Some(Token::Concat) => BinaryOp::Concat,
                _ => return Ok(left),
            };
            self.advance();
            let right = self.parse_primary_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek().cloned() {
            Some(Token::LParen) => {
                self.advance();
                // `( SELECT ... )` is a scalar subquery.
                if matches!(self.peek(), Some(Token::Select)) {
                    let select = self.parse_select()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(select)));
                }
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Grouped(Box::new(inner)))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(Expr::StringLit(s))
            }
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(Expr::Integer(n))
            }
            Some(Token::Float(f)) => {
                self.advance();
                Ok(Expr::Float(f))
            }
            Some(Token::BoolTrue) => {
                self.advance();
                Ok(Expr::Boolean(true))
            }
            Some(Token::BoolFalse) => {
                self.advance();
                Ok(Expr::Boolean(false))
            }
            Some(Token::Variable(name)) => {
                self.advance();
                Ok(Expr::Variable(name))
            }
            Some(Token::Duration(_)) => {
                let d = self.parse_duration()?;
                Ok(Expr::Duration(d))
            }
            _ => {
                // Try identifier-like tokens (keywords used as field names)
                if let Some(name) = self.peek_ident_like() {
                    self.advance();
                    if self.check(&Token::LParen) {
                        self.advance();
                        let mut args = Vec::new();
                        if !self.check(&Token::RParen) {
                            if self.check(&Token::Star) {
                                self.advance();
                            } else {
                                args.push(self.parse_expr()?);
                                while self.eat(&Token::Comma) {
                                    args.push(self.parse_expr()?);
                                }
                            }
                        }
                        self.expect(&Token::RParen)?;
                        Ok(Expr::FunctionCall(FunctionCall { name, args }))
                    } else {
                        Ok(Expr::Identifier(name))
                    }
                } else {
                    Err(self.error_here(ParseErrorKind::ExpectedExpression))
                }
            }
        }
    }

    fn parse_duration(&mut self) -> Result<DurationValue, ParseError> {
        match self.peek().cloned() {
            Some(Token::Duration(s)) => {
                self.advance();
                let (mag, unit) = s.split_at(s.len() - 1);
                let magnitude = mag
                    .parse::<i64>()
                    .map_err(|_| self.error_here(ParseErrorKind::IntegerOverflow))?;
                let unit = match unit {
                    "s" => DurationUnit::Seconds,
                    "m" => DurationUnit::Minutes,
                    "h" => DurationUnit::Hours,
                    "d" => DurationUnit::Days,
                    _ => return Err(self.error_here(ParseErrorKind::ExpectedDuration)),
                };
                Ok(DurationValue { magnitude, unit })
            }
            _ => Err(self.error_here(ParseErrorKind::ExpectedDuration)),
        }
    }
}

// ── Value Parsing ────────────────────────────────────────────────────

impl Parser {
    fn parse_value(&mut self) -> Result<Value, ParseError> {
        match self.peek().cloned() {
            Some(Token::Null) => {
                self.advance();
                Ok(Value::Null)
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(Value::String(s))
            }
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(Value::Integer(n))
            }
            Some(Token::Float(f)) => {
                self.advance();
                Ok(Value::Float(f))
            }
            Some(Token::BoolTrue) => {
                self.advance();
                Ok(Value::Boolean(true))
            }
            Some(Token::BoolFalse) => {
                self.advance();
                Ok(Value::Boolean(false))
            }
            Some(Token::Duration(_)) => {
                let d = self.parse_duration()?;
                Ok(Value::Duration(d))
            }
            Some(Token::LBrace) => self.parse_map_literal(),
            Some(Token::LBracket) => self.parse_array_literal(),
            Some(Token::Variable(name)) => {
                self.advance();
                Ok(Value::Variable(name))
            }
            _ => Err(self.error_here(ParseErrorKind::ExpectedValue)),
        }
    }

    fn parse_map_literal(&mut self) -> Result<Value, ParseError> {
        self.expect(&Token::LBrace)?;
        let mut entries = Vec::new();
        if !self.check(&Token::RBrace) {
            entries.push(self.parse_map_entry()?);
            while self.eat(&Token::Comma) {
                if self.check(&Token::RBrace) {
                    break;
                }
                entries.push(self.parse_map_entry()?);
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(Value::Map(entries))
    }

    fn parse_map_entry(&mut self) -> Result<MapEntry, ParseError> {
        let key = self.expect_ident_like()?;
        self.expect(&Token::Colon)?;
        let value = self.parse_value()?;
        Ok(MapEntry { key, value })
    }

    fn parse_array_literal(&mut self) -> Result<Value, ParseError> {
        self.expect(&Token::LBracket)?;
        let mut values = Vec::new();
        if !self.check(&Token::RBracket) {
            values.push(self.parse_value()?);
            while self.eat(&Token::Comma) {
                if self.check(&Token::RBracket) {
                    break;
                }
                values.push(self.parse_value()?);
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(Value::Array(values))
    }
}
