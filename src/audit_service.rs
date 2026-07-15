use std::{future::Future, sync::Arc};

use async_trait::async_trait;

use crate::{
    config::Config,
    error::{safe_cause_diagnostic, ApiError},
    models::{
        AuditAction, AuditErrorKind, AuditOutcome, AuditPrincipalScope, AuditReasonCode,
        AuditRecord,
    },
    request_context,
    store::Store,
    util::{hmac_hex, new_id, now},
};

#[async_trait]
trait AuditRecordSink: Send + Sync {
    async fn persist(&self, record: &AuditRecord) -> Result<(), ApiError>;
}

#[derive(Clone)]
struct StoreAuditRecordSink {
    store: Store,
}

#[async_trait]
impl AuditRecordSink for StoreAuditRecordSink {
    async fn persist(&self, record: &AuditRecord) -> Result<(), ApiError> {
        self.store.persist_audit_record(record).await
    }
}

/// Narrow, cloneable capability used by request authorization and mutation
/// services without coupling either surface to Store or AppState.
#[derive(Clone)]
pub(crate) struct AuditRecorder {
    config: Arc<Config>,
    sink: Arc<dyn AuditRecordSink>,
}

impl AuditRecorder {
    pub(crate) fn new(config: Arc<Config>, store: Store) -> Self {
        Self {
            config,
            sink: Arc::new(StoreAuditRecordSink { store }),
        }
    }

    #[cfg(test)]
    fn with_sink(config: Arc<Config>, sink: Arc<dyn AuditRecordSink>) -> Self {
        Self { config, sink }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_mutation<T, F, Fut>(
        &self,
        principal_scope: AuditPrincipalScope,
        owner_user_id: Option<&str>,
        action: AuditAction,
        resource_identity: &str,
        reason: &str,
        operation_id: Option<&str>,
        mutation: F,
    ) -> Result<T, ApiError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        let attempted = self.build_record(
            principal_scope,
            owner_user_id,
            action,
            resource_identity,
            reason,
            AuditOutcome::Attempted,
            None,
            operation_id,
        );
        if let Err(error) = self.sink.persist(&attempted).await {
            emit_persistence_diagnostic(&self.config, &attempted, "attempt", &error);
            return Err(ApiError::service_unavailable(1));
        }

        let result = mutation().await;
        let mut finalized = attempted;
        finalized.updated_at = now();
        match &result {
            Ok(_) => finalized.outcome = AuditOutcome::Success,
            Err(error) => {
                finalized.outcome = AuditOutcome::Failure;
                finalized.error_kind = Some(api_error_kind(error));
            }
        }
        if let Err(error) = self.sink.persist(&finalized).await {
            // The accepted attempt is intentionally retained. Finalization is
            // best-effort because replacing an already-observed mutation
            // result would lie about whether the mutation itself completed.
            emit_persistence_diagnostic(&self.config, &finalized, "finalize", &error);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn record_denial(
        &self,
        principal_scope: AuditPrincipalScope,
        owner_user_id: Option<&str>,
        action: AuditAction,
        resource_identity: &str,
        reason: &str,
        error: &ApiError,
    ) {
        let denied = self.build_record(
            principal_scope,
            owner_user_id,
            action,
            resource_identity,
            reason,
            AuditOutcome::Denied,
            Some(api_error_kind(error)),
            None,
        );
        if let Err(persistence_error) = self.sink.persist(&denied).await {
            // Authorization has already rejected the request. Audit
            // persistence failure must never turn its stable 401/403 into a
            // different response.
            emit_persistence_diagnostic(&self.config, &denied, "denial", &persistence_error);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_record(
        &self,
        principal_scope: AuditPrincipalScope,
        owner_user_id: Option<&str>,
        action: AuditAction,
        resource_identity: &str,
        reason: &str,
        outcome: AuditOutcome,
        error_kind: Option<AuditErrorKind>,
        operation_id: Option<&str>,
    ) -> AuditRecord {
        let occurred_at = now();
        AuditRecord {
            id: new_id("audit"),
            tenant_id: self.config.tenant_id.clone(),
            request_id: request_context::current_or_new_id(),
            principal_scope,
            principal_owner_user_id_hash: owner_user_id
                .map(|owner| audit_identifier(&self.config, "principal-owner", owner)),
            resource_id_hash: audit_identifier(&self.config, "resource", resource_identity),
            action,
            reason_code: audit_reason_code(reason),
            reason_fingerprint: audit_identifier(&self.config, "audit-reason", reason),
            outcome,
            error_kind,
            operation_id: operation_id.map(ToString::to_string),
            occurred_at,
            updated_at: occurred_at,
        }
    }
}

fn audit_identifier(config: &Config, namespace: &str, value: &str) -> String {
    format!(
        "hmac:{}",
        hmac_hex(&config.index_hash_secret, namespace, value, 32)
    )
}

fn audit_reason_code(reason: &str) -> AuditReasonCode {
    match reason {
        "authentication_failed" => AuditReasonCode::AuthenticationFailed,
        "company_writer_required" => AuditReasonCode::CompanyWriterRequired,
        "admin_required" => AuditReasonCode::AdminRequired,
        "preflight_requested" => AuditReasonCode::PreflightRequested,
        "revision_create_requested" => AuditReasonCode::RevisionCreateRequested,
        "activation_reason_unspecified" => AuditReasonCode::ActivationReasonUnspecified,
        "admin_delete" => AuditReasonCode::AdminDelete,
        "schema_upsert" => AuditReasonCode::SchemaUpsert,
        _ => AuditReasonCode::CallerSupplied,
    }
}

pub(crate) fn api_error_kind(error: &ApiError) -> AuditErrorKind {
    match error {
        ApiError::BadRequest(_) => AuditErrorKind::BadRequest,
        ApiError::Validation { .. } => AuditErrorKind::ValidationError,
        ApiError::Unauthorized(_) => AuditErrorKind::Unauthorized,
        ApiError::Forbidden(_) => AuditErrorKind::Forbidden,
        ApiError::NotFound(_) => AuditErrorKind::NotFound,
        ApiError::Conflict(_) => AuditErrorKind::Conflict,
        ApiError::PayloadTooLarge => AuditErrorKind::PayloadTooLarge,
        ApiError::TooManyRequests(_) => AuditErrorKind::TooManyRequests,
        ApiError::ServiceUnavailable(_) => AuditErrorKind::ServiceUnavailable,
        ApiError::Timeout => AuditErrorKind::Timeout,
        ApiError::Upstream(_) => AuditErrorKind::UpstreamError,
        ApiError::Internal(_) => AuditErrorKind::InternalError,
    }
}

fn emit_persistence_diagnostic(
    config: &Config,
    record: &AuditRecord,
    stage: &'static str,
    error: &ApiError,
) {
    let diagnostic = safe_cause_diagnostic(error);
    let tenant_id_hash = audit_identifier(config, "tenant", &record.tenant_id);
    tracing::error!(
        target: "nowledge::audit",
        audit_id = %record.id,
        request_id = %record.request_id,
        %tenant_id_hash,
        resource_id_hash = %record.resource_id_hash,
        action = ?record.action,
        outcome = ?record.outcome,
        stage,
        cause_category = diagnostic.category,
        cause_fingerprint = %diagnostic.fingerprint,
        "durable shared-mutation audit persistence failed"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        },
    };

    use super::*;

    #[derive(Default)]
    struct ScriptedSink {
        calls: AtomicUsize,
        fail_calls: BTreeSet<usize>,
        accepted: Mutex<Vec<AuditRecord>>,
    }

    impl ScriptedSink {
        fn failing(calls: impl IntoIterator<Item = usize>) -> Self {
            Self {
                fail_calls: calls.into_iter().collect(),
                ..Self::default()
            }
        }

        fn records(&self) -> Vec<AuditRecord> {
            self.accepted.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AuditRecordSink for ScriptedSink {
        async fn persist(&self, record: &AuditRecord) -> Result<(), ApiError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_calls.contains(&call) {
                return Err(ApiError::Upstream("private sink failure".to_string()));
            }
            record
                .validate()
                .map_err(|error| ApiError::Internal(format!("invalid audit record: {error}")))?;
            self.accepted.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    fn recorder(sink: Arc<ScriptedSink>) -> AuditRecorder {
        AuditRecorder::with_sink(Arc::new(Config::test()), sink)
    }

    async fn recorded_success(
        recorder: &AuditRecorder,
        invoked: Arc<AtomicBool>,
    ) -> Result<&'static str, ApiError> {
        recorder
            .record_mutation(
                AuditPrincipalScope::Admin,
                None,
                AuditAction::CompanyDocDelete,
                "raw-source-id",
                "admin_delete",
                None,
                || async move {
                    invoked.store(true, Ordering::SeqCst);
                    Ok("mutated")
                },
            )
            .await
    }

    #[tokio::test]
    async fn accepted_attempt_precedes_authorized_mutation_and_is_finalized_in_place() {
        let sink = Arc::new(ScriptedSink::default());
        let invoked = Arc::new(AtomicBool::new(false));
        assert_eq!(
            recorded_success(&recorder(sink.clone()), invoked.clone())
                .await
                .unwrap(),
            "mutated"
        );
        assert!(invoked.load(Ordering::SeqCst));
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].outcome, AuditOutcome::Attempted);
        assert_eq!(records[1].outcome, AuditOutcome::Success);
        assert_eq!(records[0].id, records[1].id);
        assert_eq!(records[0].request_id, records[1].request_id);
        assert!(!records[0].resource_id_hash.contains("raw-source-id"));
    }

    #[tokio::test]
    async fn rejected_attempt_fails_closed_without_invoking_mutation() {
        let sink = Arc::new(ScriptedSink::failing([1]));
        let invoked = Arc::new(AtomicBool::new(false));
        let error = recorded_success(&recorder(sink.clone()), invoked.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::ServiceUnavailable(1)));
        assert!(!invoked.load(Ordering::SeqCst));
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn finalization_failure_preserves_mutation_result_and_attempt() {
        let sink = Arc::new(ScriptedSink::failing([2]));
        let invoked = Arc::new(AtomicBool::new(false));
        assert_eq!(
            recorded_success(&recorder(sink.clone()), invoked)
                .await
                .unwrap(),
            "mutated"
        );
        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].outcome, AuditOutcome::Attempted);
    }

    #[tokio::test]
    async fn mutation_failure_is_finalized_without_replacing_original_error() {
        let sink = Arc::new(ScriptedSink::default());
        let error = recorder(sink.clone())
            .record_mutation::<(), _, _>(
                AuditPrincipalScope::TenantService,
                None,
                AuditAction::DatasetUpsertSchema,
                "raw-dataset-key",
                "schema_upsert",
                None,
                || async { Err(ApiError::Conflict("original conflict".to_string())) },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::Conflict(message) if message == "original conflict"));
        let records = sink.records();
        assert_eq!(records[1].outcome, AuditOutcome::Failure);
        assert_eq!(records[1].error_kind, Some(AuditErrorKind::Conflict));
    }

    #[tokio::test]
    async fn denial_persistence_failure_is_ignored() {
        let sink = Arc::new(ScriptedSink::failing([1]));
        recorder(sink.clone())
            .record_denial(
                AuditPrincipalScope::Unauthenticated,
                None,
                AuditAction::CompanyDocPreflight,
                "company-doc:preflight",
                "authentication_failed",
                &ApiError::Unauthorized("original rejection".to_string()),
            )
            .await;
        assert!(sink.records().is_empty());
    }
}
