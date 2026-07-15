#[path = "../src/operations_v1.rs"]
mod operations_v1;

use std::sync::Mutex;

use anyhow::{bail, Result};
use async_trait::async_trait;
use operations_v1::{
    apply_plan, create_plan, plan_report, validate_plan, verify_plan, IndexInspection, IndexState,
    OperationsV1Backend, PlannedAction, TARGET_INDEX_UID, TARGET_PRIMARY_KEY,
};

#[derive(Debug)]
struct FakeState {
    inspection: IndexInspection,
    created_at: Option<String>,
    calls: Vec<String>,
    create_calls: usize,
    reconcile_calls: usize,
    converge_on_create: bool,
    appear_during_strict_create: bool,
    disappear_on_reconcile: bool,
}

#[derive(Debug)]
struct FakeBackend {
    state: Mutex<FakeState>,
}

impl FakeBackend {
    fn new(inspection: IndexInspection) -> Self {
        let created_at = inspection
            .exists
            .then(|| "2026-07-14T00:00:00Z".to_string());
        Self {
            state: Mutex::new(FakeState {
                inspection,
                created_at,
                calls: Vec::new(),
                create_calls: 0,
                reconcile_calls: 0,
                converge_on_create: true,
                appear_during_strict_create: false,
                disappear_on_reconcile: false,
            }),
        }
    }

    fn inspection(&self) -> IndexInspection {
        self.state.lock().unwrap().inspection
    }

    fn set_inspection(&self, inspection: IndexInspection) {
        let mut state = self.state.lock().unwrap();
        state.inspection = inspection;
        if inspection.exists {
            state
                .created_at
                .get_or_insert_with(|| "2026-07-14T00:01:00Z".to_string());
        } else {
            state.created_at = None;
        }
    }

    fn set_converge_on_create(&self, converge: bool) {
        self.state.lock().unwrap().converge_on_create = converge;
    }

    fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn create_calls(&self) -> usize {
        self.state.lock().unwrap().create_calls
    }

    fn reconcile_calls(&self) -> usize {
        self.state.lock().unwrap().reconcile_calls
    }

    fn set_disappear_on_reconcile(&self) {
        self.state.lock().unwrap().disappear_on_reconcile = true;
    }

    fn replace_generation(&self) {
        let mut state = self.state.lock().unwrap();
        state.created_at = Some("2026-07-14T02:00:00Z".to_string());
    }

    fn set_appear_during_strict_create(&self) {
        self.state.lock().unwrap().appear_during_strict_create = true;
    }
}

#[async_trait]
impl OperationsV1Backend for FakeBackend {
    async fn index_exists(&self, index_uid: &str) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!("exists:{index_uid}"));
        Ok(state.inspection.exists)
    }

    async fn index_primary_key(&self, index_uid: &str) -> Result<Option<String>> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!("primary-key:{index_uid}"));
        if !state.inspection.exists {
            bail!("primary key was inspected for a missing index");
        }
        Ok(Some(
            if state.inspection.primary_key_match {
                TARGET_PRIMARY_KEY
            } else {
                "legacy_operation_id"
            }
            .to_string(),
        ))
    }

    async fn index_created_at(&self, index_uid: &str) -> Result<Option<String>> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!("created-at:{index_uid}"));
        Ok(state.created_at.clone())
    }

    async fn index_settings_match(&self, index_uid: &str) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!("settings:{index_uid}"));
        if !state.inspection.exists {
            bail!("settings were inspected for a missing index");
        }
        Ok(state.inspection.settings_match)
    }

    async fn create_index_strict(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!(
            "strict-create:{index_uid}:{primary_key}:{apply_settings}"
        ));
        state.create_calls += 1;
        if index_uid != TARGET_INDEX_UID || primary_key != TARGET_PRIMARY_KEY || !apply_settings {
            bail!("operations_v1 attempted an unexpected index mutation");
        }
        if state.appear_during_strict_create {
            state.inspection = IndexInspection::ready();
            state.created_at = Some("2026-07-14T03:00:00Z".to_string());
            bail!("strict create rejected a concurrently-created same-UID index");
        }
        if state.converge_on_create {
            state.inspection = IndexInspection::ready();
            state.created_at = Some("2026-07-14T01:00:00Z".to_string());
        }
        // The production adapter returns only after MeiliAdmin has waited for
        // both index-creation and settings tasks.
        Ok(vec!["create-task".to_string(), "settings-task".to_string()])
    }

    async fn reconcile_existing_index(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(format!(
            "reconcile:{index_uid}:{primary_key}:{apply_settings}"
        ));
        state.reconcile_calls += 1;
        if index_uid != TARGET_INDEX_UID || primary_key != TARGET_PRIMARY_KEY || !apply_settings {
            bail!("operations_v1 attempted an unexpected index reconciliation");
        }
        if state.disappear_on_reconcile {
            state.inspection = IndexInspection::missing();
            state.created_at = None;
        }
        if !state.inspection.exists {
            bail!("existing operations index disappeared; refusing empty recreation");
        }
        if state.converge_on_create {
            state.inspection = IndexInspection::ready();
        }
        Ok(vec!["settings-task".to_string()])
    }
}

#[tokio::test]
async fn plan_is_mutation_free_and_distinguishes_all_initial_states() {
    for (inspection, expected_state, expected_action) in [
        (
            IndexInspection::missing(),
            IndexState::Missing,
            PlannedAction::Create,
        ),
        (
            IndexInspection::ready(),
            IndexState::AlreadyPresent,
            PlannedAction::None,
        ),
        (
            IndexInspection::settings_drift(),
            IndexState::SettingsDrift,
            PlannedAction::ReconcileSettings,
        ),
    ] {
        let backend = FakeBackend::new(inspection);
        let plan = create_plan(&backend).await.unwrap();
        let report = plan_report(&plan);

        assert_eq!(plan.index_uid, TARGET_INDEX_UID);
        assert_eq!(plan.primary_key, TARGET_PRIMARY_KEY);
        assert_eq!(plan.observed_state, expected_state);
        assert_eq!(plan.action, expected_action);
        assert!(!plan.destructive);
        assert!(report.mutation_free);
        assert_eq!(report.already_present, inspection.exists);
        assert_eq!(backend.create_calls(), 0);
        assert!(backend
            .calls()
            .iter()
            .all(|call| call.contains(TARGET_INDEX_UID)));
    }
}

#[tokio::test]
async fn incompatible_primary_key_is_refused_by_plan_apply_and_verify() {
    let incompatible = FakeBackend::new(IndexInspection::primary_key_mismatch());
    let error = create_plan(&incompatible).await.unwrap_err().to_string();
    assert!(error.contains("incompatible primary key"));
    assert!(error.contains(TARGET_PRIMARY_KEY));
    assert_eq!(incompatible.create_calls(), 0);
    assert!(!incompatible
        .calls()
        .iter()
        .any(|call| call.starts_with("settings:")));

    let backend = FakeBackend::new(IndexInspection::ready());
    let plan = create_plan(&backend).await.unwrap();
    backend.set_inspection(IndexInspection::primary_key_mismatch());

    let error = apply_plan(&backend, &plan, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("incompatible primary key"));
    assert_eq!(backend.create_calls(), 0);

    let report = verify_plan(&backend, &plan).await.unwrap();
    assert!(report.index_present);
    assert!(!report.primary_key_match);
    assert!(!report.settings_match);
    assert!(!report.ready);
    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].contains("primary key id"));
    assert_eq!(backend.create_calls(), 0);
}

#[tokio::test]
async fn apply_creates_only_the_operations_index_and_is_idempotent_after_replanning() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let plan = create_plan(&backend).await.unwrap();

    let first = apply_plan(&backend, &plan, false).await.unwrap();
    assert!(first.creation_performed);
    assert!(!first.already_present);
    assert!(!first.mutation_free);
    assert_eq!(first.waited_task_count, 2);
    assert!(!first.ready_to_verify);
    assert_eq!(backend.inspection(), IndexInspection::ready());
    assert_eq!(backend.create_calls(), 1);

    let stale_verification = verify_plan(&backend, &plan).await.unwrap();
    assert!(!stale_verification.ready);
    assert!(stale_verification
        .failures
        .iter()
        .any(|failure| failure.contains("fresh post-create plan")));

    let present_plan = create_plan(&backend).await.unwrap();
    assert_eq!(present_plan.observed_state, IndexState::AlreadyPresent);
    assert_eq!(
        present_plan.observed_created_at.as_deref(),
        Some("2026-07-14T01:00:00Z")
    );
    let second = apply_plan(&backend, &present_plan, false).await.unwrap();
    assert!(!second.creation_performed);
    assert!(second.already_present);
    assert!(second.already_ready);
    assert!(second.mutation_free);
    assert_eq!(second.waited_task_count, 0);
    assert_eq!(backend.create_calls(), 1);

    let verified = verify_plan(&backend, &present_plan).await.unwrap();
    assert!(verified.ready);
    assert!(verified.generation_match);

    let mutation_calls = backend
        .calls()
        .into_iter()
        .filter(|call| call.starts_with("strict-create:"))
        .collect::<Vec<_>>();
    assert_eq!(
        mutation_calls,
        vec![format!(
            "strict-create:{TARGET_INDEX_UID}:{TARGET_PRIMARY_KEY}:true"
        )]
    );
}

#[tokio::test]
async fn apply_refuses_an_index_that_appears_after_a_missing_plan_without_reconciliation() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let plan = create_plan(&backend).await.unwrap();
    backend.set_inspection(IndexInspection::settings_drift());

    let error = apply_plan(&backend, &plan, false)
        .await
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("was missing when the plan was created"),
        "{error}"
    );
    assert!(error.contains("not reviewed by this plan"), "{error}");
    assert_eq!(backend.create_calls(), 0);
    assert_eq!(backend.reconcile_calls(), 0);
    assert_eq!(backend.inspection(), IndexInspection::settings_drift());
}

#[tokio::test]
async fn strict_create_refuses_an_index_that_appears_during_creation() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let plan = create_plan(&backend).await.unwrap();
    backend.set_appear_during_strict_create();

    let error = apply_plan(&backend, &plan, false)
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("concurrently-created"), "{error}");
    assert_eq!(backend.create_calls(), 1);
    assert_eq!(backend.reconcile_calls(), 0);
    assert_eq!(backend.inspection(), IndexInspection::ready());
}

#[tokio::test]
async fn apply_and_verify_reject_a_replaced_same_uid_generation() {
    let backend = FakeBackend::new(IndexInspection::ready());
    let plan = create_plan(&backend).await.unwrap();
    backend.replace_generation();

    let error = apply_plan(&backend, &plan, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("createdAt"), "{error}");
    assert!(error.contains("same-UID replacement"), "{error}");
    assert_eq!(backend.create_calls(), 0);
    assert_eq!(backend.reconcile_calls(), 0);

    let verification = verify_plan(&backend, &plan).await.unwrap();
    assert!(!verification.ready);
    assert!(!verification.generation_match);
    assert!(verification
        .failures
        .iter()
        .any(|failure| failure.contains("createdAt")));
}

#[tokio::test]
async fn apply_reconciles_settings_without_reporting_index_creation() {
    let backend = FakeBackend::new(IndexInspection::settings_drift());
    let plan = create_plan(&backend).await.unwrap();

    let report = apply_plan(&backend, &plan, false).await.unwrap();

    assert!(report.already_present);
    assert!(!report.creation_performed);
    assert!(report.settings_reconciled);
    assert!(!report.mutation_free);
    assert!(report.ready_to_verify);
    assert_eq!(backend.create_calls(), 0);
    assert_eq!(backend.reconcile_calls(), 1);
}

#[tokio::test]
async fn apply_never_recreates_an_existing_index_that_disappears_before_reconciliation() {
    let backend = FakeBackend::new(IndexInspection::settings_drift());
    let plan = create_plan(&backend).await.unwrap();
    backend.set_disappear_on_reconcile();

    let error = apply_plan(&backend, &plan, false)
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("refusing empty recreation"), "{error}");
    assert_eq!(backend.create_calls(), 0);
    assert_eq!(backend.reconcile_calls(), 1);
    assert_eq!(backend.inspection(), IndexInspection::missing());
}

#[tokio::test]
async fn apply_dry_run_never_mutates() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let plan = create_plan(&backend).await.unwrap();

    let report = apply_plan(&backend, &plan, true).await.unwrap();

    assert!(report.dry_run);
    assert!(report.mutation_free);
    assert!(!report.creation_performed);
    assert!(!report.ready_to_verify);
    assert_eq!(backend.create_calls(), 0);
    assert_eq!(backend.inspection(), IndexInspection::missing());
}

#[tokio::test]
async fn apply_refuses_to_hide_disappearance_or_incomplete_convergence() {
    let disappeared = FakeBackend::new(IndexInspection::ready());
    let present_plan = create_plan(&disappeared).await.unwrap();
    disappeared.set_inspection(IndexInspection::missing());
    let error = apply_plan(&disappeared, &present_plan, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("refusing to hide possible data loss"));
    assert_eq!(disappeared.create_calls(), 0);

    let incomplete = FakeBackend::new(IndexInspection::missing());
    let missing_plan = create_plan(&incomplete).await.unwrap();
    incomplete.set_converge_on_create(false);
    let error = apply_plan(&incomplete, &missing_plan, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("did not reach the required index and settings state"));
    assert_eq!(incomplete.create_calls(), 1);
}

#[tokio::test]
async fn verify_is_mutation_free_and_requires_both_index_and_exact_settings() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let missing_plan = create_plan(&backend).await.unwrap();

    let missing = verify_plan(&backend, &missing_plan).await.unwrap();
    assert!(!missing.ready);
    assert!(!missing.index_present);
    assert!(!missing.primary_key_match);
    assert_eq!(missing.failures.len(), 1);
    assert_eq!(backend.create_calls(), 0);

    backend.set_inspection(IndexInspection::ready());
    let stale_missing_plan = verify_plan(&backend, &missing_plan).await.unwrap();
    assert!(!stale_missing_plan.ready);
    assert!(stale_missing_plan
        .failures
        .iter()
        .any(|failure| failure.contains("fresh post-create plan")));

    let present_plan = create_plan(&backend).await.unwrap();
    backend.set_inspection(IndexInspection::settings_drift());
    let drift = verify_plan(&backend, &present_plan).await.unwrap();
    assert!(!drift.ready);
    assert!(drift.index_present);
    assert!(drift.primary_key_match);
    assert!(!drift.settings_match);
    assert!(drift.failures[0].contains("settings"));
    assert_eq!(backend.create_calls(), 0);

    backend.set_inspection(IndexInspection::ready());
    let ready = verify_plan(&backend, &present_plan).await.unwrap();
    assert!(ready.ready);
    assert!(ready.generation_match);
    assert!(ready.primary_key_match);
    assert!(ready.mutation_free);
    assert!(ready.failures.is_empty());
    assert_eq!(backend.create_calls(), 0);
}

#[tokio::test]
async fn tampered_or_destructive_plans_are_refused_before_mutation() {
    let backend = FakeBackend::new(IndexInspection::missing());
    let plan = create_plan(&backend).await.unwrap();

    let mut wrong_target = plan.clone();
    wrong_target.index_uid = "rag_state_items".to_string();
    assert!(validate_plan(&wrong_target).is_err());
    assert!(apply_plan(&backend, &wrong_target, false).await.is_err());

    let mut destructive = plan.clone();
    destructive.destructive = true;
    assert!(validate_plan(&destructive).is_err());
    assert!(apply_plan(&backend, &destructive, false).await.is_err());

    let mut wrong_settings = plan;
    wrong_settings.desired_settings["filterableAttributes"] = serde_json::json!(["id"]);
    assert!(validate_plan(&wrong_settings).is_err());
    assert!(apply_plan(&backend, &wrong_settings, false).await.is_err());

    assert_eq!(backend.create_calls(), 0);

    let existing = FakeBackend::new(IndexInspection::ready());
    let mut tampered_generation = create_plan(&existing).await.unwrap();
    tampered_generation.observed_created_at = Some("2026-07-14T09:00:00Z".to_string());
    let error = validate_plan(&tampered_generation).unwrap_err().to_string();
    assert!(error.contains("checksum"), "{error}");
}
