/// KVMQL Access Control — Auth checker (Phase 11)
///
/// Evaluates whether a principal's grants permit a requested verb on a
/// target scope.  The algorithm follows the spec Section 17:
///
///   1. Load all grants for the principal.
///   2. For each grant, check verb, scope type, and scope id.
///   3. First matching grant → PERMIT.
///   4. No match → DENY.

/// A parsed grant ready for authorization checks.
#[derive(Debug, Clone)]
pub struct Grant {
    pub id: String,
    pub principal_id: String,
    pub verbs: Vec<String>,
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub conditions: Option<String>,
}

/// The outcome of an authorization check.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthDecision {
    Permitted,
    Denied { reason: String },
}

pub struct AccessChecker;

impl AccessChecker {
    /// Check whether a principal is permitted to execute a verb on a target
    /// scope.
    ///
    /// `grants` — all grants currently active for the principal.
    /// `verb` — the KVMQL verb (e.g. "SELECT", "DESTROY").
    /// `target_scope_type` — "cluster", "provider", or `None` for resource-level.
    /// `target_scope_id` — specific cluster/provider id, or `None`.
    pub fn check(
        grants: &[Grant],
        verb: &str,
        target_scope_type: Option<&str>,
        target_scope_id: Option<&str>,
    ) -> AuthDecision {
        for grant in grants {
            // Check verb
            if !grant.verbs.iter().any(|v| v.eq_ignore_ascii_case(verb)) {
                continue;
            }

            // Global scope matches everything
            if grant.scope_type == "global" {
                return AuthDecision::Permitted;
            }

            // Check scope match
            if let Some(target_type) = target_scope_type {
                if grant.scope_type == target_type {
                    match (&grant.scope_id, target_scope_id) {
                        (Some(grant_id), Some(target_id)) if grant_id == target_id => {
                            return AuthDecision::Permitted;
                        }
                        (None, _) => {
                            // Grant on scope type without specific ID matches all
                            return AuthDecision::Permitted;
                        }
                        _ => continue,
                    }
                }
            } else {
                // No specific target scope — resource-level check.
                // A grant with a matching verb is sufficient.
                return AuthDecision::Permitted;
            }
        }

        AuthDecision::Denied {
            reason: format!("no grant permits verb '{}' on this scope", verb),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_grant(
        verbs: &[&str],
        scope_type: &str,
        scope_id: Option<&str>,
    ) -> Grant {
        Grant {
            id: "g-test".into(),
            principal_id: "p-test".into(),
            verbs: verbs.iter().map(|v| v.to_string()).collect(),
            scope_type: scope_type.into(),
            scope_id: scope_id.map(|s| s.into()),
            conditions: None,
        }
    }

    #[test]
    fn test_global_grant_permits_everything() {
        let grants = vec![make_grant(
            &["SELECT", "CREATE", "DESTROY"],
            "global",
            None,
        )];
        assert_eq!(
            AccessChecker::check(&grants, "SELECT", Some("cluster"), Some("prod")),
            AuthDecision::Permitted,
        );
        assert_eq!(
            AccessChecker::check(&grants, "DESTROY", None, None),
            AuthDecision::Permitted,
        );
    }

    #[test]
    fn test_specific_verb_denied() {
        let grants = vec![make_grant(&["SELECT"], "global", None)];
        assert_eq!(
            AccessChecker::check(&grants, "DESTROY", None, None),
            AuthDecision::Denied {
                reason: "no grant permits verb 'DESTROY' on this scope".into(),
            },
        );
    }

    #[test]
    fn test_scope_match() {
        let grants = vec![make_grant(
            &["SELECT", "DESTROY"],
            "cluster",
            Some("prod"),
        )];
        assert_eq!(
            AccessChecker::check(&grants, "DESTROY", Some("cluster"), Some("prod")),
            AuthDecision::Permitted,
        );
    }

    #[test]
    fn test_scope_mismatch() {
        let grants = vec![make_grant(
            &["SELECT", "DESTROY"],
            "cluster",
            Some("prod"),
        )];
        assert_eq!(
            AccessChecker::check(&grants, "DESTROY", Some("cluster"), Some("staging")),
            AuthDecision::Denied {
                reason: "no grant permits verb 'DESTROY' on this scope".into(),
            },
        );
    }

    #[test]
    fn test_no_grants_denied() {
        let grants: Vec<Grant> = vec![];
        assert_eq!(
            AccessChecker::check(&grants, "SELECT", None, None),
            AuthDecision::Denied {
                reason: "no grant permits verb 'SELECT' on this scope".into(),
            },
        );
    }

    #[test]
    fn test_multiple_grants_any_match() {
        let grants = vec![
            make_grant(&["SELECT"], "cluster", Some("staging")),
            make_grant(&["DESTROY"], "cluster", Some("prod")),
        ];
        // First grant doesn't match verb DESTROY, second does match
        assert_eq!(
            AccessChecker::check(&grants, "DESTROY", Some("cluster"), Some("prod")),
            AuthDecision::Permitted,
        );
        // First grant matches SELECT on staging
        assert_eq!(
            AccessChecker::check(&grants, "SELECT", Some("cluster"), Some("staging")),
            AuthDecision::Permitted,
        );
        // Neither grant matches SELECT on prod (first is staging, second is DESTROY only)
        assert_eq!(
            AccessChecker::check(&grants, "SELECT", Some("cluster"), Some("prod")),
            AuthDecision::Denied {
                reason: "no grant permits verb 'SELECT' on this scope".into(),
            },
        );
    }
}
