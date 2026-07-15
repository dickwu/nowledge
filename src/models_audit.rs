use super::*;

use uuid::Uuid;

/// The complete, bounded set of shared-knowledge mutations that require a
/// durable audit record before they may execute.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuditAction {
    #[serde(rename = "company_doc.preflight")]
    CompanyDocPreflight,
    #[serde(rename = "company_doc.create_revision")]
    CompanyDocCreateRevision,
    #[serde(rename = "company_doc.activate_revision")]
    CompanyDocActivateRevision,
    #[serde(rename = "company_doc.delete")]
    CompanyDocDelete,
    #[serde(rename = "dataset.upsert_schema")]
    DatasetUpsertSchema,
}

/// Authenticated authority used for the attempted mutation. Owner identities
/// are stored separately as HMACs, never in this scope discriminator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditPrincipalScope {
    Unauthenticated,
    Owner,
    TenantService,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Attempted,
    Success,
    Failure,
    Denied,
}

/// Reasons are deliberately an enum. Caller-provided explanation text is
/// represented only by `CallerSupplied` plus `reason_fingerprint`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditReasonCode {
    AuthenticationFailed,
    CompanyWriterRequired,
    AdminRequired,
    PreflightRequested,
    RevisionCreateRequested,
    ActivationReasonUnspecified,
    AdminDelete,
    SchemaUpsert,
    CallerSupplied,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditErrorKind {
    BadRequest,
    ValidationError,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    PayloadTooLarge,
    TooManyRequests,
    ServiceUnavailable,
    Timeout,
    UpstreamError,
    InternalError,
}

/// Durable record for a shared-knowledge mutation or authorization denial.
///
/// This type intentionally has no fields capable of storing raw resource or
/// owner identifiers, request bodies, queries, prompts, paths, provider
/// responses, tokens, or free-form reasons.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    pub id: String,
    pub tenant_id: String,
    pub request_id: String,
    pub principal_scope: AuditPrincipalScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_owner_user_id_hash: Option<String>,
    pub resource_id_hash: String,
    pub action: AuditAction,
    pub reason_code: AuditReasonCode,
    pub reason_fingerprint: String,
    pub outcome: AuditOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<AuditErrorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AuditRecord {
    pub fn validate(&self) -> Result<(), String> {
        require_non_empty_bounded("tenant_id", &self.tenant_id, 256)?;
        validate_prefixed_v7("audit record id", &self.id, "audit_")?;
        validate_v7("audit request id", &self.request_id)?;
        validate_hmac("resource_id_hash", &self.resource_id_hash)?;
        validate_hmac("reason_fingerprint", &self.reason_fingerprint)?;
        if let Some(owner_hash) = &self.principal_owner_user_id_hash {
            validate_hmac("principal_owner_user_id_hash", owner_hash)?;
            if self.principal_scope != AuditPrincipalScope::Owner {
                return Err("only an owner principal may carry an owner hash".to_string());
            }
        } else if self.principal_scope == AuditPrincipalScope::Owner {
            return Err("owner principals must carry an owner hash".to_string());
        }
        if self.updated_at < self.occurred_at {
            return Err("audit updated_at precedes occurred_at".to_string());
        }
        match self.outcome {
            AuditOutcome::Attempted | AuditOutcome::Success if self.error_kind.is_some() => {
                return Err("attempted and successful audits cannot carry an error kind".to_string())
            }
            AuditOutcome::Failure | AuditOutcome::Denied if self.error_kind.is_none() => {
                return Err("failed and denied audits require an error kind".to_string())
            }
            _ => {}
        }
        if let Some(operation_id) = &self.operation_id {
            require_non_empty_bounded("operation_id", operation_id, 128)?;
            if !operation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err("operation_id contains unsupported characters".to_string());
            }
        }
        Ok(())
    }
}

fn require_non_empty_bounded(field: &str, value: &str, max_chars: usize) -> Result<(), String> {
    let length = value.chars().count();
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if length > max_chars {
        return Err(format!("{field} exceeds {max_chars} characters"));
    }
    Ok(())
}

fn validate_prefixed_v7(field: &str, value: &str, prefix: &str) -> Result<(), String> {
    let payload = value
        .strip_prefix(prefix)
        .ok_or_else(|| format!("{field} must start with {prefix}"))?;
    validate_v7(field, payload)
}

fn validate_v7(field: &str, value: &str) -> Result<(), String> {
    let uuid = Uuid::parse_str(value).map_err(|_| format!("{field} must be a UUID"))?;
    if uuid.get_version_num() != 7 {
        return Err(format!("{field} must be UUIDv7"));
    }
    Ok(())
}

fn validate_hmac(field: &str, value: &str) -> Result<(), String> {
    let Some(payload) = value.strip_prefix("hmac:") else {
        return Err(format!("{field} must be HMAC-derived"));
    };
    if payload.len() != 32 || !payload.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{field} must contain a 128-bit hexadecimal HMAC"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_record_has_no_raw_identifier_or_reason_fields() {
        let now = Utc::now();
        let record = AuditRecord {
            id: format!("audit_{}", Uuid::now_v7().simple()),
            tenant_id: "trusted-tenant".to_string(),
            request_id: Uuid::now_v7().to_string(),
            principal_scope: AuditPrincipalScope::Owner,
            principal_owner_user_id_hash: Some(format!("hmac:{}", "a".repeat(32))),
            resource_id_hash: format!("hmac:{}", "b".repeat(32)),
            action: AuditAction::CompanyDocActivateRevision,
            reason_code: AuditReasonCode::CallerSupplied,
            reason_fingerprint: format!("hmac:{}", "c".repeat(32)),
            outcome: AuditOutcome::Attempted,
            error_kind: None,
            operation_id: None,
            occurred_at: now,
            updated_at: now,
        };
        record.validate().unwrap();

        let encoded = serde_json::to_string(&record).unwrap();
        for forbidden in [
            "raw-owner-id",
            "raw-resource-id",
            "free-form reason",
            "request_body",
            "query",
            "prompt",
            "path",
            "token",
            "provider_body",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }

    #[test]
    fn outcome_and_identity_invariants_are_enforced() {
        let now = Utc::now();
        let mut record = AuditRecord {
            id: format!("audit_{}", Uuid::now_v7().simple()),
            tenant_id: "trusted-tenant".to_string(),
            request_id: Uuid::now_v7().to_string(),
            principal_scope: AuditPrincipalScope::Admin,
            principal_owner_user_id_hash: None,
            resource_id_hash: format!("hmac:{}", "b".repeat(32)),
            action: AuditAction::CompanyDocDelete,
            reason_code: AuditReasonCode::AdminDelete,
            reason_fingerprint: format!("hmac:{}", "c".repeat(32)),
            outcome: AuditOutcome::Denied,
            error_kind: Some(AuditErrorKind::Forbidden),
            operation_id: None,
            occurred_at: now,
            updated_at: now,
        };
        record.validate().unwrap();
        record.error_kind = None;
        assert!(record.validate().is_err());
        record.error_kind = Some(AuditErrorKind::Forbidden);
        record.resource_id_hash = "raw-resource".to_string();
        assert!(record.validate().is_err());
    }
}
