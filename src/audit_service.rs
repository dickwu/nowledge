use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::{
    config::Config,
    error::{safe_cause_diagnostic, ApiError},
    metrics::Metrics,
    models::{
        AuditAction, AuditErrorKind, AuditOutcome, AuditPrincipalScope, AuditReasonCode,
        AuditRecord,
    },
    request_context,
    runtime::RuntimeSupervisor,
    store::Store,
    util::{hmac_hex, new_id, now},
};

const MAX_IN_FLIGHT_DENIAL_WRITES: usize = 64;
const MAX_IN_FLIGHT_FINALIZATION_WRITES: usize = 64;
const MAX_DENIAL_WRITES_PER_MINUTE: u64 = 10;
const MAX_DENIAL_PRINCIPAL_BUCKETS: usize = MAX_DENIAL_WRITES_PER_MINUTE as usize;
const DENIAL_PRINCIPAL_HMAC_HEX_LEN: usize = 32;
const DENIAL_RATE_WINDOW: Duration = Duration::from_secs(60);

/// Credential-independent, bounded logical-principal identity used only for
/// in-memory denial admission. The private field prevents callers from
/// accidentally supplying a raw token, tenant ID, or owner ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DenialPrincipalIdentity(String);

impl DenialPrincipalIdentity {
    pub(crate) fn from_hmac_hex(value: String) -> Option<Self> {
        (value.len() == DENIAL_PRINCIPAL_HMAC_HEX_LEN
            && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(Self(value))
    }

    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

struct DenialRateState {
    window_started: Instant,
    authenticated_admitted: u64,
    authenticated_by_principal: HashMap<DenialPrincipalIdentity, u64>,
    unauthenticated_admitted: u64,
}

impl DenialRateState {
    fn new() -> Self {
        Self {
            window_started: Instant::now(),
            authenticated_admitted: 0,
            authenticated_by_principal: HashMap::new(),
            unauthenticated_admitted: 0,
        }
    }
}

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
    runtime: RuntimeSupervisor,
    metrics: Metrics,
    denial_permits: Arc<Semaphore>,
    finalization_permits: Arc<Semaphore>,
    denial_rate: Arc<Mutex<DenialRateState>>,
}

impl AuditRecorder {
    pub(crate) fn new(
        config: Arc<Config>,
        store: Store,
        runtime: RuntimeSupervisor,
        metrics: Metrics,
    ) -> Self {
        Self {
            config,
            sink: Arc::new(StoreAuditRecordSink { store }),
            runtime,
            metrics,
            denial_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_DENIAL_WRITES)),
            finalization_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_FINALIZATION_WRITES)),
            denial_rate: Arc::new(Mutex::new(DenialRateState::new())),
        }
    }

    #[cfg(test)]
    fn with_sink(config: Arc<Config>, sink: Arc<dyn AuditRecordSink>) -> Self {
        Self {
            config,
            sink,
            runtime: RuntimeSupervisor::new(),
            metrics: Metrics::new(),
            denial_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_DENIAL_WRITES)),
            finalization_permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_FINALIZATION_WRITES)),
            denial_rate: Arc::new(Mutex::new(DenialRateState::new())),
        }
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
        // The accepted attempt is intentionally retained. Finalization is a
        // supervised best-effort write because waiting here could let the
        // outer request deadline replace an already-observed mutation result
        // with a misleading timeout.
        self.persist_best_effort(finalized, "finalization", self.finalization_permits.clone());
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_denial(
        &self,
        principal_scope: AuditPrincipalScope,
        principal_identity: Option<&DenialPrincipalIdentity>,
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
        if !self.admit_denial(principal_scope, principal_identity) {
            return;
        }
        self.persist_best_effort(denied, "denial", self.denial_permits.clone());
    }

    fn admit_denial(
        &self,
        principal_scope: AuditPrincipalScope,
        principal_identity: Option<&DenialPrincipalIdentity>,
    ) -> bool {
        let mut rate = self
            .denial_rate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if rate.window_started.elapsed() >= DENIAL_RATE_WINDOW {
            rate.window_started = Instant::now();
            rate.authenticated_admitted = 0;
            rate.authenticated_by_principal.clear();
            rate.unauthenticated_admitted = 0;
        }
        let limit = self
            .config
            .rate_limit_requests_per_minute
            .clamp(1, MAX_DENIAL_WRITES_PER_MINUTE);
        let unauthenticated_limit = (limit / 2).max(1);
        if principal_scope == AuditPrincipalScope::Unauthenticated {
            if principal_identity.is_some() {
                self.metrics
                    .record_audit_background_drop("denial", "rate_limit");
                return false;
            }
            if rate.unauthenticated_admitted >= unauthenticated_limit {
                self.metrics
                    .record_audit_background_drop("denial", "rate_limit");
                return false;
            }
            rate.unauthenticated_admitted += 1;
            return true;
        }

        let Some(principal_identity) = principal_identity else {
            self.metrics
                .record_audit_background_drop("denial", "rate_limit");
            return false;
        };
        let per_principal_limit = (limit / 2).max(1);
        let principal_admitted = rate
            .authenticated_by_principal
            .get(principal_identity)
            .copied()
            .unwrap_or_default();
        if rate.authenticated_admitted >= limit || principal_admitted >= per_principal_limit {
            self.metrics
                .record_audit_background_drop("denial", "rate_limit");
            return false;
        }
        if principal_admitted == 0
            && rate.authenticated_by_principal.len() >= MAX_DENIAL_PRINCIPAL_BUCKETS
        {
            self.metrics
                .record_audit_background_drop("denial", "capacity");
            return false;
        }
        rate.authenticated_admitted += 1;
        *rate
            .authenticated_by_principal
            .entry(principal_identity.clone())
            .or_default() += 1;
        true
    }

    fn persist_best_effort(
        &self,
        record: AuditRecord,
        stage: &'static str,
        permits: Arc<Semaphore>,
    ) {
        let Ok(permit) = permits.try_acquire_owned() else {
            self.metrics.record_audit_background_drop(stage, "capacity");
            return;
        };
        let sink = self.sink.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        if !self.runtime.spawn(async move {
            let _permit = permit;
            if let Err(persistence_error) = sink.persist(&record).await {
                emit_persistence_diagnostic(&config, &record, stage, &persistence_error);
                metrics.record_audit_background_drop(stage, "persistence");
            }
        }) {
            self.metrics.record_audit_background_drop(stage, "shutdown");
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

pub(crate) fn caller_supplied_audit_reason(reason: &str) -> String {
    format!("caller_supplied:{reason}")
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
        future::pending,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        },
        time::Duration,
    };

    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn caller_explanations_cannot_select_reserved_audit_reason_codes() {
        for reserved in [
            "authentication_failed",
            "company_writer_required",
            "admin_required",
            "preflight_requested",
            "revision_create_requested",
            "activation_reason_unspecified",
            "admin_delete",
            "schema_upsert",
        ] {
            assert_eq!(
                audit_reason_code(&caller_supplied_audit_reason(reserved)),
                AuditReasonCode::CallerSupplied,
                "caller explanation selected reserved reason {reserved}"
            );
        }
    }

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

    #[derive(Default)]
    struct BlockingSink {
        started: Notify,
    }

    #[async_trait]
    impl AuditRecordSink for BlockingSink {
        async fn persist(&self, _record: &AuditRecord) -> Result<(), ApiError> {
            self.started.notify_one();
            pending().await
        }
    }

    #[derive(Default)]
    struct BlockingFinalizationSink {
        calls: AtomicUsize,
        finalization_started: Notify,
        accepted: Mutex<Vec<AuditRecord>>,
    }

    #[async_trait]
    impl AuditRecordSink for BlockingFinalizationSink {
        async fn persist(&self, record: &AuditRecord) -> Result<(), ApiError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                self.accepted.lock().unwrap().push(record.clone());
                return Ok(());
            }
            self.finalization_started.notify_one();
            pending().await
        }
    }

    fn recorder(sink: Arc<ScriptedSink>) -> AuditRecorder {
        AuditRecorder::with_sink(Arc::new(Config::test()), sink)
    }

    fn denial_identity(label: &str) -> DenialPrincipalIdentity {
        DenialPrincipalIdentity::from_hmac_hex(hmac_hex(
            b"audit-denial-test-secret",
            "rate-limit-principal",
            label,
            DENIAL_PRINCIPAL_HMAC_HEX_LEN,
        ))
        .unwrap()
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background audit write did not run");
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
        let recorder = recorder(sink.clone());
        assert_eq!(
            recorded_success(&recorder, invoked.clone()).await.unwrap(),
            "mutated"
        );
        wait_for_calls(&sink.calls, 2).await;
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
        let recorder = recorder(sink.clone());
        assert_eq!(
            recorded_success(&recorder, invoked).await.unwrap(),
            "mutated"
        );
        wait_for_calls(&sink.calls, 2).await;
        let records = sink.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].outcome, AuditOutcome::Attempted);
    }

    #[tokio::test]
    async fn mutation_failure_is_finalized_without_replacing_original_error() {
        let sink = Arc::new(ScriptedSink::default());
        let recorder = recorder(sink.clone());
        let error = recorder
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
        wait_for_calls(&sink.calls, 2).await;
        let records = sink.records();
        assert_eq!(records[1].outcome, AuditOutcome::Failure);
        assert_eq!(records[1].error_kind, Some(AuditErrorKind::Conflict));
    }

    #[tokio::test]
    async fn denial_persistence_failure_is_ignored() {
        let sink = Arc::new(ScriptedSink::failing([1]));
        let recorder = recorder(sink.clone());
        recorder.record_denial(
            AuditPrincipalScope::Unauthenticated,
            None,
            None,
            AuditAction::CompanyDocPreflight,
            "company-doc:preflight",
            "authentication_failed",
            &ApiError::Unauthorized("original rejection".to_string()),
        );
        wait_for_calls(&sink.calls, 1).await;
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn blocked_finalization_never_delays_a_completed_mutation_result() {
        let sink = Arc::new(BlockingFinalizationSink::default());
        let recorder = AuditRecorder::with_sink(Arc::new(Config::test()), sink.clone());
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            recorder.record_mutation(
                AuditPrincipalScope::Admin,
                None,
                AuditAction::CompanyDocDelete,
                "raw-source-id",
                "admin_delete",
                None,
                || async { Ok("mutated") },
            ),
        )
        .await
        .expect("blocked finalization must not delay a completed mutation")
        .unwrap();
        assert_eq!(result, "mutated");
        tokio::time::timeout(Duration::from_secs(1), sink.finalization_started.notified())
            .await
            .expect("supervised finalization write should start in the background");
        let records = sink.accepted.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].outcome, AuditOutcome::Attempted);
    }

    #[tokio::test]
    async fn denial_writes_are_rate_limited_before_background_admission() {
        let sink = Arc::new(ScriptedSink::default());
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 2;
        let recorder = AuditRecorder::with_sink(Arc::new(config), sink.clone());
        for _ in 0..2 {
            recorder.record_denial(
                AuditPrincipalScope::Unauthenticated,
                None,
                None,
                AuditAction::CompanyDocPreflight,
                "company-doc:preflight",
                "authentication_failed",
                &ApiError::Unauthorized("original rejection".to_string()),
            );
        }
        wait_for_calls(&sink.calls, 1).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn one_authenticated_principal_cannot_consume_another_denial_bucket() {
        let sink = Arc::new(ScriptedSink::default());
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 4;
        let recorder = AuditRecorder::with_sink(Arc::new(config), sink.clone());
        let first = denial_identity("owner:first");
        let second = denial_identity("owner:second");

        for _ in 0..3 {
            recorder.record_denial(
                AuditPrincipalScope::Owner,
                Some(&first),
                Some("first"),
                AuditAction::CompanyDocCreateRevision,
                "company-doc:first",
                "company_writer_required",
                &ApiError::Forbidden("original rejection".to_string()),
            );
        }
        recorder.record_denial(
            AuditPrincipalScope::Owner,
            Some(&second),
            Some("second"),
            AuditAction::CompanyDocCreateRevision,
            "company-doc:second",
            "company_writer_required",
            &ApiError::Forbidden("original rejection".to_string()),
        );

        wait_for_calls(&sink.calls, 3).await;
        tokio::task::yield_now().await;
        assert_eq!(sink.calls.load(Ordering::SeqCst), 3);
        assert_eq!(sink.records().len(), 3);
    }

    #[test]
    fn denial_buckets_keep_global_bounds_and_reset_all_identity_state() {
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 4;
        let recorder =
            AuditRecorder::with_sink(Arc::new(config), Arc::new(ScriptedSink::default()));

        for index in 0..4 {
            let identity = denial_identity(&format!("principal:{index}"));
            assert!(recorder.admit_denial(AuditPrincipalScope::Owner, Some(&identity)));
        }
        let overflow = denial_identity("principal:overflow");
        assert!(!recorder.admit_denial(AuditPrincipalScope::Admin, Some(&overflow)));

        {
            let rate = recorder.denial_rate.lock().unwrap();
            assert_eq!(rate.authenticated_admitted, 4);
            assert_eq!(rate.authenticated_by_principal.len(), 4);
            assert!(rate.authenticated_by_principal.len() <= MAX_DENIAL_PRINCIPAL_BUCKETS);
        }

        {
            let mut rate = recorder.denial_rate.lock().unwrap();
            rate.window_started = Instant::now() - DENIAL_RATE_WINDOW;
        }
        assert!(recorder.admit_denial(AuditPrincipalScope::Admin, Some(&overflow)));
        let rate = recorder.denial_rate.lock().unwrap();
        assert_eq!(rate.authenticated_admitted, 1);
        assert_eq!(rate.authenticated_by_principal.len(), 1);
        assert_eq!(rate.unauthenticated_admitted, 0);
    }

    #[test]
    fn unauthenticated_denials_stay_bounded_and_identity_mismatches_fail_closed() {
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 4;
        let recorder =
            AuditRecorder::with_sink(Arc::new(config), Arc::new(ScriptedSink::default()));
        assert!(recorder.admit_denial(AuditPrincipalScope::Unauthenticated, None));
        assert!(recorder.admit_denial(AuditPrincipalScope::Unauthenticated, None));
        assert!(!recorder.admit_denial(AuditPrincipalScope::Unauthenticated, None));

        let identity = denial_identity("owner:first");
        assert!(!recorder.admit_denial(AuditPrincipalScope::Unauthenticated, Some(&identity)));
        assert!(!recorder.admit_denial(AuditPrincipalScope::Owner, None));
        let rate = recorder.denial_rate.lock().unwrap();
        assert_eq!(rate.unauthenticated_admitted, 2);
        assert_eq!(rate.authenticated_admitted, 0);
        assert!(rate.authenticated_by_principal.is_empty());
    }

    #[tokio::test]
    async fn one_write_budget_records_both_denial_classes_independently() {
        let sink = Arc::new(ScriptedSink::default());
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 1;
        let recorder = AuditRecorder::with_sink(Arc::new(config), sink.clone());
        recorder.record_denial(
            AuditPrincipalScope::Unauthenticated,
            None,
            None,
            AuditAction::CompanyDocPreflight,
            "company-doc:preflight",
            "authentication_failed",
            &ApiError::Unauthorized("original rejection".to_string()),
        );
        let tenant_service = denial_identity("tenant-service");
        recorder.record_denial(
            AuditPrincipalScope::TenantService,
            Some(&tenant_service),
            None,
            AuditAction::CompanyDocDelete,
            "company-doc:protected",
            "company_writer_required",
            &ApiError::Forbidden("original rejection".to_string()),
        );

        wait_for_calls(&sink.calls, 2).await;
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .any(|record| record.principal_scope == AuditPrincipalScope::Unauthenticated));
        assert!(records
            .iter()
            .any(|record| record.principal_scope == AuditPrincipalScope::TenantService));
    }

    #[tokio::test]
    async fn unauthenticated_flood_cannot_exhaust_authenticated_denial_reserve() {
        let sink = Arc::new(ScriptedSink::default());
        let mut config = Config::test();
        config.rate_limit_requests_per_minute = 2;
        let recorder = AuditRecorder::with_sink(Arc::new(config), sink.clone());

        for _ in 0..2 {
            recorder.record_denial(
                AuditPrincipalScope::Unauthenticated,
                None,
                None,
                AuditAction::CompanyDocPreflight,
                "company-doc:preflight",
                "authentication_failed",
                &ApiError::Unauthorized("original rejection".to_string()),
            );
        }
        let tenant_service = denial_identity("tenant-service");
        recorder.record_denial(
            AuditPrincipalScope::TenantService,
            Some(&tenant_service),
            None,
            AuditAction::CompanyDocDelete,
            "company-doc:protected",
            "company_writer_required",
            &ApiError::Forbidden("original rejection".to_string()),
        );

        wait_for_calls(&sink.calls, 2).await;
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .any(|record| record.principal_scope == AuditPrincipalScope::Unauthenticated));
        assert!(records
            .iter()
            .any(|record| record.principal_scope == AuditPrincipalScope::TenantService));
    }

    #[tokio::test]
    async fn background_persistence_failure_is_exported_as_a_bounded_metric() {
        let sink = Arc::new(ScriptedSink::failing([1]));
        let recorder = recorder(sink.clone());
        let metrics = recorder.metrics.clone();
        recorder.record_denial(
            AuditPrincipalScope::Unauthenticated,
            None,
            None,
            AuditAction::CompanyDocPreflight,
            "company-doc:preflight",
            "authentication_failed",
            &ApiError::Unauthorized("original rejection".to_string()),
        );
        wait_for_calls(&sink.calls, 1).await;

        let rendered = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let rendered = metrics
                    .render(
                        crate::metrics::IngestRuntimeMetrics::default(),
                        &crate::metrics::StoreMetricsSnapshot::default(),
                    )
                    .unwrap();
                if rendered.contains(
                    "nowledge_audit_background_drops_total{stage=\"denial\",reason=\"persistence\"} 1",
                ) {
                    break rendered;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("persistence failure metric was not published");

        assert!(!rendered.contains("private sink failure"));
    }

    #[tokio::test]
    async fn blocked_denial_persistence_never_delays_the_authorization_result() {
        let sink = Arc::new(BlockingSink::default());
        let recorder = AuditRecorder::with_sink(Arc::new(Config::test()), sink.clone());
        recorder.record_denial(
            AuditPrincipalScope::Unauthenticated,
            None,
            None,
            AuditAction::CompanyDocPreflight,
            "company-doc:preflight",
            "authentication_failed",
            &ApiError::Unauthorized("original rejection".to_string()),
        );
        tokio::time::timeout(Duration::from_secs(1), sink.started.notified())
            .await
            .expect("supervised denial write should start in the background");
    }
}
