/// Error messages with remediation advice.
///
/// Every user-facing error should tell the user:
///   1. What happened
///   2. Why
///   3. The exact command to fix it

/// Context passed to the remediation builder so advice can be tailored.
#[derive(Default, Debug, Clone)]
pub struct ErrorContext {
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub provider_id: Option<String>,
    pub provider_type: Option<String>,
}

/// Build an error message with remediation advice appended.
///
/// The returned string has the form:
///
/// ```text
/// <message>
///
/// <remediation advice with exact commands>
/// ```
///
/// If no remediation is known for the given code, the original message is
/// returned unchanged.
pub fn with_remediation(code: &str, message: &str, context: &ErrorContext) -> String {
    let mut msg = format!("{}\n\n", message);

    if let Some(advice) = remediation_for(code, context) {
        msg.push_str(&advice);
    } else {
        // No remediation available — return original message without trailing newlines
        return message.to_string();
    }

    msg
}

fn remediation_for(code: &str, ctx: &ErrorContext) -> Option<String> {
    match code {
        "PROVIDER_NOT_FOUND" => {
            let pid = ctx.provider_id.as_deref().unwrap_or("<id>");
            Some(format!(
                "The provider '{pid}' is not registered.\n\
                 \n\
                 Register it first:\n\
                 ADD PROVIDER id = '{pid}' type = 'azure' driver = 'azure_vm' auth = 'env:...';\n\
                 \n\
                 Or list existing providers:\n\
                 SHOW PROVIDERS;"
            ))
        }

        "RESOURCE_NOT_FOUND" => {
            let rid = ctx.resource_id.as_deref().unwrap_or("<id>");
            let rtype = ctx.resource_type.as_deref().unwrap_or("<type>");
            Some(format!(
                "Resource '{rtype}' '{rid}' not found in the registry.\n\
                 \n\
                 It may have been deleted or never created. Check:\n\
                 SELECT * FROM resources WHERE id = '{rid}';\n\
                 \n\
                 Or create it:\n\
                 CREATE RESOURCE '{rtype}' id = '{rid}' ...;"
            ))
        }

        "AZ_PROVISION_FAILED" => {
            let rid = ctx.resource_id.as_deref().unwrap_or("<id>");
            let rtype = ctx.resource_type.as_deref().unwrap_or("<type>");
            Some(format!(
                "Azure provisioning failed for {rtype} '{rid}'.\n\
                 \n\
                 Common causes:\n\
                 1. Not logged in: run 'az login'\n\
                 2. Wrong subscription: run 'az account show'\n\
                 3. Missing permissions: run 'az role assignment list --assignee <principal>'\n\
                 4. Resource group doesn't exist: run 'az group list'\n\
                 5. az CLI not installed: install from https://aka.ms/installazurecli\n\
                 \n\
                 The resource was saved in the registry with status 'pending'.\n\
                 After fixing the issue, retry:\n\
                 DESTROY RESOURCE '{rtype}' '{rid}'; CREATE RESOURCE '{rtype}' id = '{rid}' ...;\n\
                 \n\
                 Or sync from cloud:\n\
                 SYNC RESOURCE '{rtype}' '{rid}';"
            ))
        }

        "AWS_PROVISION_FAILED" => {
            let rid = ctx.resource_id.as_deref().unwrap_or("<id>");
            let rtype = ctx.resource_type.as_deref().unwrap_or("<type>");
            Some(format!(
                "AWS provisioning failed for {rtype} '{rid}'.\n\
                 \n\
                 Common causes:\n\
                 1. Not configured: run 'aws configure' or 'aws sso login'\n\
                 2. Wrong region: run 'aws configure get region'\n\
                 3. Missing permissions: run 'aws sts get-caller-identity'\n\
                 4. aws CLI not installed: install from https://aws.amazon.com/cli/\n\
                 \n\
                 The resource was saved in the registry with status 'pending'.\n\
                 After fixing the issue, retry the CREATE command."
            ))
        }

        "AUTH_DENIED" => {
            let principal = ctx.resource_id.as_deref().unwrap_or("<principal>");
            Some(format!(
                "Permission denied for principal '{principal}'.\n\
                 \n\
                 Check your grants:\n\
                 SHOW GRANTS FOR '{principal}';\n\
                 \n\
                 Grant the required permission:\n\
                 GRANT <verb> ON <scope> TO '{principal}';\n\
                 \n\
                 Or disable auth checking:\n\
                 SET auth_enabled = false;"
            ))
        }

        "PARSE_ERROR" => Some(
            "The statement could not be parsed.\n\
             \n\
             Check your syntax:\n\
             - Statements end with ;\n\
             - String values use single quotes: 'value'\n\
             - Keywords are case-insensitive\n\
             \n\
             Type \\h in the shell for a list of all verbs.\n\
             Type \\d for a list of all queryable nouns."
                .into(),
        ),

        "NO_DRIVERS" => Some(
            "No providers are registered.\n\
             \n\
             Add a provider first:\n\
             ADD PROVIDER id = 'local' type = 'kvm' driver = 'firecracker' auth = 'env:X';\n\
             \n\
             Or run in simulation mode:\n\
             kvmql --simulate \"CREATE RESOURCE 'postgres' id = 'test' version = '16';\"\n\
             \n\
             List providers:\n\
             SHOW PROVIDERS;"
                .into(),
        ),

        "VOLUME_ATTACHED" => {
            let vid = ctx.resource_id.as_deref().unwrap_or("<vol-id>");
            Some(format!(
                "Volume '{vid}' is currently attached to a VM.\n\
                 \n\
                 Detach it first:\n\
                 DETACH VOLUME '{vid}' FROM MICROVM '<vm-id>';\n\
                 \n\
                 Or force-destroy (detaches automatically):\n\
                 DESTROY VOLUME '{vid}' FORCE;"
            ))
        }

        "IMAGE_IN_USE" => {
            let iid = ctx.resource_id.as_deref().unwrap_or("<image-id>");
            Some(format!(
                "Image '{iid}' is in use by running VMs.\n\
                 \n\
                 Check which VMs use it:\n\
                 SELECT id, status FROM microvms WHERE image_id = '{iid}';\n\
                 \n\
                 Or force-remove (does not destroy VMs):\n\
                 REMOVE IMAGE '{iid}' FORCE;"
            ))
        }

        "CAPABILITY_UNSUPPORTED" => {
            let pid = ctx.provider_id.as_deref().unwrap_or("<provider>");
            Some(format!(
                "This operation is not supported by provider '{pid}'.\n\
                 \n\
                 Check what the provider supports:\n\
                 SHOW CAPABILITIES FOR PROVIDER '{pid}';\n\
                 \n\
                 Or switch to a provider that supports this operation:\n\
                 SELECT * FROM capabilities WHERE capability = '<cap>' AND supported = true;"
            ))
        }

        "ROLLBACK_NO_SNAPSHOTS" => Some(
            "No state snapshots available for rollback.\n\
             \n\
             Snapshots are captured automatically before destructive operations\n\
             (DESTROY, ALTER). There is nothing to roll back.\n\
             \n\
             To tag future snapshots:\n\
             SET @snapshot_tag = 'before-migration';\n\
             -- Next mutation will be tagged"
                .into(),
        ),

        "RESOURCE_TYPE_UNKNOWN" => {
            let rtype = ctx.resource_type.as_deref().unwrap_or("<type>");
            Some(format!(
                "Unknown resource type '{rtype}'.\n\
                 \n\
                 Supported types: postgres, redis, aks, storage_account, vnet, subnet,\n\
                 nsg, nsg_rule, vnet_peering, container_registry, dns_zone, dns_vnet_link,\n\
                 container_app, container_job, load_balancer, pg_database,\n\
                 rds_postgres, vpc, aws_subnet, security_group, sg_rule.\n\
                 \n\
                 Or use any custom type — KVMQL will track it in the registry."
            ))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remediation_provider_not_found() {
        let ctx = ErrorContext {
            provider_id: Some("my-azure".into()),
            ..Default::default()
        };
        let msg = with_remediation("PROVIDER_NOT_FOUND", "provider 'my-azure' not found", &ctx);
        assert!(msg.contains("ADD PROVIDER"), "should suggest ADD PROVIDER: {msg}");
        assert!(msg.contains("SHOW PROVIDERS"), "should suggest SHOW PROVIDERS: {msg}");
        assert!(msg.contains("my-azure"), "should include the provider id: {msg}");
    }

    #[test]
    fn test_remediation_resource_not_found() {
        let ctx = ErrorContext {
            resource_id: Some("db-1".into()),
            resource_type: Some("postgres".into()),
            ..Default::default()
        };
        let msg = with_remediation("RESOURCE_NOT_FOUND", "resource 'db-1' not found", &ctx);
        assert!(
            msg.contains("SELECT * FROM resources"),
            "should suggest SELECT query: {msg}"
        );
        assert!(msg.contains("db-1"), "should include the resource id: {msg}");
        assert!(
            msg.contains("CREATE RESOURCE"),
            "should suggest CREATE RESOURCE: {msg}"
        );
    }

    #[test]
    fn test_remediation_no_drivers() {
        let ctx = ErrorContext::default();
        let msg = with_remediation("NO_DRIVERS", "no drivers registered", &ctx);
        assert!(
            msg.contains("SHOW PROVIDERS"),
            "should suggest SHOW PROVIDERS: {msg}"
        );
        assert!(
            msg.contains("--simulate"),
            "should suggest --simulate mode: {msg}"
        );
        assert!(
            msg.contains("ADD PROVIDER"),
            "should suggest ADD PROVIDER: {msg}"
        );
    }

    #[test]
    fn test_remediation_auth_denied() {
        let ctx = ErrorContext {
            resource_id: Some("usr-readonly".into()),
            ..Default::default()
        };
        let msg = with_remediation("AUTH_DENIED", "permission denied", &ctx);
        assert!(
            msg.contains("SHOW GRANTS"),
            "should suggest SHOW GRANTS: {msg}"
        );
        assert!(
            msg.contains("GRANT"),
            "should suggest GRANT command: {msg}"
        );
        assert!(
            msg.contains("usr-readonly"),
            "should include the principal: {msg}"
        );
    }

    #[test]
    fn test_remediation_unknown_code() {
        let ctx = ErrorContext::default();
        let msg = with_remediation("TOTALLY_UNKNOWN_CODE", "something broke", &ctx);
        // Unknown codes return the original message unchanged (no remediation)
        assert_eq!(msg, "something broke");
    }

    #[test]
    fn test_remediation_rollback_no_snapshots() {
        let ctx = ErrorContext::default();
        let msg = with_remediation(
            "ROLLBACK_NO_SNAPSHOTS",
            "no snapshots available for rollback",
            &ctx,
        );
        assert!(
            msg.contains("Snapshots are captured automatically"),
            "should explain snapshot behavior: {msg}"
        );
        assert!(
            msg.contains("SET @snapshot_tag"),
            "should suggest tagging: {msg}"
        );
    }

    #[test]
    fn test_remediation_az_provision_failed() {
        let ctx = ErrorContext {
            resource_id: Some("pg-1".into()),
            resource_type: Some("postgres".into()),
            ..Default::default()
        };
        let msg = with_remediation("AZ_PROVISION_FAILED", "Azure provisioning failed", &ctx);
        assert!(msg.contains("az login"), "should suggest az login: {msg}");
        assert!(
            msg.contains("az account show"),
            "should suggest checking subscription: {msg}"
        );
        assert!(
            msg.contains("SYNC RESOURCE"),
            "should suggest SYNC: {msg}"
        );
    }

    #[test]
    fn test_remediation_aws_provision_failed() {
        let ctx = ErrorContext {
            resource_id: Some("rds-1".into()),
            resource_type: Some("rds_postgres".into()),
            ..Default::default()
        };
        let msg = with_remediation("AWS_PROVISION_FAILED", "AWS provisioning failed", &ctx);
        assert!(
            msg.contains("aws configure"),
            "should suggest aws configure: {msg}"
        );
        assert!(
            msg.contains("aws sts get-caller-identity"),
            "should suggest checking identity: {msg}"
        );
    }

    #[test]
    fn test_remediation_volume_attached() {
        let ctx = ErrorContext {
            resource_id: Some("vol-99".into()),
            ..Default::default()
        };
        let msg = with_remediation("VOLUME_ATTACHED", "volume is attached", &ctx);
        assert!(
            msg.contains("DETACH VOLUME"),
            "should suggest DETACH: {msg}"
        );
        assert!(
            msg.contains("DESTROY VOLUME"),
            "should suggest DESTROY FORCE: {msg}"
        );
        assert!(msg.contains("vol-99"), "should include volume id: {msg}");
    }

    #[test]
    fn test_remediation_capability_unsupported() {
        let ctx = ErrorContext {
            provider_id: Some("prov-1".into()),
            ..Default::default()
        };
        let msg = with_remediation("CAPABILITY_UNSUPPORTED", "not supported", &ctx);
        assert!(
            msg.contains("SHOW CAPABILITIES"),
            "should suggest SHOW CAPABILITIES: {msg}"
        );
        assert!(msg.contains("prov-1"), "should include provider id: {msg}");
    }
}
