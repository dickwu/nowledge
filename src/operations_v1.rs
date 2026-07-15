use anyhow::{bail, Result};
use async_trait::async_trait;
use nowledge::{
    meili::{settings_for, MeiliAdmin},
    tenant_scope_v1::checksum_value,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MIGRATION_NAME: &str = "operations_v1";
pub const ARTIFACT_SCHEMA_VERSION: u32 = 2;
pub const TARGET_INDEX_UID: &str = "rag_operations";
pub const TARGET_PRIMARY_KEY: &str = "id";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexState {
    Missing,
    AlreadyPresent,
    SettingsDrift,
    PrimaryKeyMismatch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlannedAction {
    Create,
    None,
    ReconcileSettings,
    Refuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexInspection {
    pub exists: bool,
    pub primary_key_match: bool,
    pub settings_match: bool,
}

impl IndexInspection {
    pub const fn missing() -> Self {
        Self {
            exists: false,
            primary_key_match: false,
            settings_match: false,
        }
    }

    pub const fn ready() -> Self {
        Self {
            exists: true,
            primary_key_match: true,
            settings_match: true,
        }
    }

    pub const fn settings_drift() -> Self {
        Self {
            exists: true,
            primary_key_match: true,
            settings_match: false,
        }
    }

    pub const fn primary_key_mismatch() -> Self {
        Self {
            exists: true,
            primary_key_match: false,
            settings_match: false,
        }
    }

    pub const fn state(self) -> IndexState {
        match (self.exists, self.primary_key_match, self.settings_match) {
            (false, _, _) => IndexState::Missing,
            (true, false, _) => IndexState::PrimaryKeyMismatch,
            (true, true, true) => IndexState::AlreadyPresent,
            (true, true, false) => IndexState::SettingsDrift,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedIndex {
    inspection: IndexInspection,
    created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MigrationPlan {
    pub migration: String,
    pub schema_version: u32,
    pub index_uid: String,
    pub primary_key: String,
    pub desired_settings: Value,
    pub desired_settings_checksum: String,
    pub observed_state: IndexState,
    pub observed_created_at: Option<String>,
    pub action: PlannedAction,
    pub destructive: bool,
    pub plan_checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub index_uid: String,
    pub observed_state: IndexState,
    pub observed_created_at: Option<String>,
    pub action: PlannedAction,
    pub already_present: bool,
    pub settings_match: bool,
    pub mutation_free: bool,
    pub destructive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplyReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub index_uid: String,
    pub dry_run: bool,
    pub mutation_free: bool,
    pub pre_apply_state: IndexState,
    pub already_present: bool,
    pub already_ready: bool,
    pub creation_performed: bool,
    pub settings_reconciled: bool,
    pub waited_task_count: usize,
    pub ready_to_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub index_uid: String,
    pub planned_initial_state: IndexState,
    pub already_present_at_plan: bool,
    pub index_present: bool,
    pub primary_key_match: bool,
    pub settings_match: bool,
    pub expected_created_at: Option<String>,
    pub actual_created_at: Option<String>,
    pub generation_match: bool,
    pub ready: bool,
    pub mutation_free: bool,
    pub failures: Vec<String>,
}

#[async_trait]
pub trait OperationsV1Backend: Send + Sync {
    async fn index_exists(&self, index_uid: &str) -> Result<bool>;
    async fn index_primary_key(&self, index_uid: &str) -> Result<Option<String>>;
    async fn index_created_at(&self, index_uid: &str) -> Result<Option<String>>;
    async fn index_settings_match(&self, index_uid: &str) -> Result<bool>;

    /// Strictly create a missing index, refusing any concurrent same-UID
    /// winner, apply its managed settings, and wait for every returned task.
    async fn create_index_strict(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>>;
    async fn reconcile_existing_index(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>>;
}

#[async_trait]
impl OperationsV1Backend for MeiliAdmin {
    async fn index_exists(&self, index_uid: &str) -> Result<bool> {
        Ok(MeiliAdmin::index_exists(self, index_uid).await?)
    }

    async fn index_primary_key(&self, index_uid: &str) -> Result<Option<String>> {
        Ok(MeiliAdmin::index_primary_key(self, index_uid).await?)
    }

    async fn index_created_at(&self, index_uid: &str) -> Result<Option<String>> {
        Ok(MeiliAdmin::index_created_at(self, index_uid).await?)
    }

    async fn index_settings_match(&self, index_uid: &str) -> Result<bool> {
        Ok(MeiliAdmin::index_settings_match(self, index_uid).await?)
    }

    async fn create_index_strict(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>> {
        let task_uids =
            MeiliAdmin::create_index_strict(self, index_uid, primary_key, apply_settings).await?;
        MeiliAdmin::wait_for_tasks(self, &task_uids).await?;
        Ok(task_uids)
    }

    async fn reconcile_existing_index(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>> {
        let task_uids = MeiliAdmin::reconcile_existing_index_with_primary_key(
            self,
            index_uid,
            primary_key,
            apply_settings,
        )
        .await?;
        MeiliAdmin::wait_for_tasks(self, &task_uids).await?;
        Ok(task_uids)
    }
}

pub async fn create_plan<B: OperationsV1Backend>(backend: &B) -> Result<MigrationPlan> {
    let observed = inspect(backend).await?;
    require_compatible_primary_key(observed.inspection)?;
    let desired_settings = settings_for(TARGET_INDEX_UID);
    let desired_settings_checksum = checksum_value(&desired_settings);
    let observed_state = observed.inspection.state();
    let action = action_for(observed_state);
    let mut plan = MigrationPlan {
        migration: MIGRATION_NAME.to_string(),
        schema_version: ARTIFACT_SCHEMA_VERSION,
        index_uid: TARGET_INDEX_UID.to_string(),
        primary_key: TARGET_PRIMARY_KEY.to_string(),
        desired_settings,
        desired_settings_checksum,
        observed_state,
        observed_created_at: observed.created_at,
        action,
        destructive: false,
        plan_checksum: String::new(),
    };
    plan.plan_checksum = compute_plan_checksum(&plan)?;
    validate_plan(&plan)?;
    Ok(plan)
}

pub fn plan_report(plan: &MigrationPlan) -> PlanReport {
    PlanReport {
        mode: "plan".to_string(),
        migration: plan.migration.clone(),
        plan_checksum: plan.plan_checksum.clone(),
        index_uid: plan.index_uid.clone(),
        observed_state: plan.observed_state,
        observed_created_at: plan.observed_created_at.clone(),
        action: plan.action,
        already_present: plan.observed_state != IndexState::Missing,
        settings_match: plan.observed_state == IndexState::AlreadyPresent,
        mutation_free: true,
        destructive: false,
    }
}

pub async fn apply_plan<B: OperationsV1Backend>(
    backend: &B,
    plan: &MigrationPlan,
    dry_run: bool,
) -> Result<ApplyReport> {
    validate_plan(plan)?;
    let before = inspect(backend).await?;
    require_compatible_primary_key(before.inspection)?;
    require_planned_generation(plan, &before)?;

    let pre_apply_state = before.inspection.state();
    let already_present = before.inspection.exists;
    let already_ready = before.inspection == IndexInspection::ready();
    let creation_performed = !before.inspection.exists && !dry_run;
    let settings_reconciled =
        before.inspection.exists && !before.inspection.settings_match && !dry_run;
    let mut waited_task_count = 0;

    let ready_to_verify = if dry_run {
        already_ready
    } else {
        if !already_ready {
            let task_uids = if before.inspection.exists {
                backend
                    .reconcile_existing_index(TARGET_INDEX_UID, TARGET_PRIMARY_KEY, true)
                    .await?
            } else {
                backend
                    .create_index_strict(TARGET_INDEX_UID, TARGET_PRIMARY_KEY, true)
                    .await?
            };
            waited_task_count = task_uids.len();
        }
        let after = inspect(backend).await?;
        require_compatible_primary_key(after.inspection)?;
        if plan.observed_state != IndexState::Missing {
            require_planned_generation(plan, &after)?;
        }
        if after.inspection != IndexInspection::ready() {
            bail!(
                "{TARGET_INDEX_UID} did not reach the required index and settings state after apply"
            );
        }
        plan.observed_state != IndexState::Missing
    };

    Ok(ApplyReport {
        mode: "apply".to_string(),
        migration: MIGRATION_NAME.to_string(),
        plan_checksum: plan.plan_checksum.clone(),
        index_uid: TARGET_INDEX_UID.to_string(),
        dry_run,
        mutation_free: dry_run || already_ready,
        pre_apply_state,
        already_present,
        already_ready,
        creation_performed,
        settings_reconciled,
        waited_task_count,
        ready_to_verify,
    })
}

pub async fn verify_plan<B: OperationsV1Backend>(
    backend: &B,
    plan: &MigrationPlan,
) -> Result<VerificationReport> {
    validate_plan(plan)?;
    let observed = inspect(backend).await?;
    let inspection = observed.inspection;
    let mut failures = Vec::new();
    if !inspection.exists {
        failures.push(format!(
            "required Meilisearch index {TARGET_INDEX_UID} is missing"
        ));
    } else {
        if !inspection.primary_key_match {
            failures.push(format!(
                "Meilisearch index {TARGET_INDEX_UID} must use primary key {TARGET_PRIMARY_KEY}"
            ));
        }
        if inspection.primary_key_match && !inspection.settings_match {
            failures.push(format!(
                "managed settings for Meilisearch index {TARGET_INDEX_UID} do not match"
            ));
        }
    }
    let generation_match = plan.observed_state != IndexState::Missing
        && observed.created_at == plan.observed_created_at;
    if plan.observed_state == IndexState::Missing && inspection.exists {
        failures.push(format!(
            "{TARGET_INDEX_UID} was missing in this plan; create a fresh post-create plan to bind the index createdAt before verification"
        ));
    } else if plan.observed_state != IndexState::Missing && !generation_match {
        failures.push(format!(
            "Meilisearch index {TARGET_INDEX_UID} createdAt does not match the generation bound by this plan"
        ));
    }
    Ok(VerificationReport {
        mode: "verify".to_string(),
        migration: MIGRATION_NAME.to_string(),
        plan_checksum: plan.plan_checksum.clone(),
        index_uid: TARGET_INDEX_UID.to_string(),
        planned_initial_state: plan.observed_state,
        already_present_at_plan: plan.observed_state != IndexState::Missing,
        index_present: inspection.exists,
        primary_key_match: inspection.exists && inspection.primary_key_match,
        settings_match: inspection.exists && inspection.settings_match,
        expected_created_at: plan.observed_created_at.clone(),
        actual_created_at: observed.created_at,
        generation_match,
        ready: inspection == IndexInspection::ready() && generation_match,
        mutation_free: true,
        failures,
    })
}

pub fn validate_plan(plan: &MigrationPlan) -> Result<()> {
    if plan.migration != MIGRATION_NAME || plan.schema_version != ARTIFACT_SCHEMA_VERSION {
        bail!("unsupported operations_v1 migration plan version");
    }
    if plan.index_uid != TARGET_INDEX_UID || plan.primary_key != TARGET_PRIMARY_KEY {
        bail!("operations_v1 may target only {TARGET_INDEX_UID} with primary key id");
    }
    if plan.destructive {
        bail!("operations_v1 refuses destructive migration plans");
    }
    if plan.observed_state == IndexState::PrimaryKeyMismatch {
        bail!("operations_v1 refuses plans for an index with an incompatible primary key");
    }
    match (plan.observed_state, plan.observed_created_at.as_deref()) {
        (IndexState::Missing, None) => {}
        (IndexState::Missing, Some(_)) => {
            bail!("operations_v1 missing-index plans cannot bind a createdAt generation")
        }
        (_, Some(created_at)) if !created_at.trim().is_empty() => {}
        _ => bail!("operations_v1 existing-index plans must bind a non-empty createdAt generation"),
    }
    let desired_settings = settings_for(TARGET_INDEX_UID);
    if plan.desired_settings != desired_settings
        || plan.desired_settings_checksum != checksum_value(&desired_settings)
    {
        bail!("operations_v1 plan settings do not match this binary");
    }
    if plan.action != action_for(plan.observed_state) {
        bail!("operations_v1 plan action is inconsistent with its observed state");
    }
    if plan.plan_checksum != compute_plan_checksum(plan)? {
        bail!("operations_v1 migration plan checksum does not match its contents");
    }
    Ok(())
}

async fn inspect<B: OperationsV1Backend>(backend: &B) -> Result<ObservedIndex> {
    if !backend.index_exists(TARGET_INDEX_UID).await? {
        return Ok(ObservedIndex {
            inspection: IndexInspection::missing(),
            created_at: None,
        });
    }
    let created_at = backend.index_created_at(TARGET_INDEX_UID).await?;
    if created_at
        .as_deref()
        .is_none_or(|created_at| created_at.trim().is_empty())
    {
        bail!("Meilisearch index {TARGET_INDEX_UID} returned no valid createdAt generation");
    }
    if backend
        .index_primary_key(TARGET_INDEX_UID)
        .await?
        .as_deref()
        != Some(TARGET_PRIMARY_KEY)
    {
        return Ok(ObservedIndex {
            inspection: IndexInspection::primary_key_mismatch(),
            created_at,
        });
    }
    let inspection = if backend.index_settings_match(TARGET_INDEX_UID).await? {
        IndexInspection::ready()
    } else {
        IndexInspection::settings_drift()
    };
    Ok(ObservedIndex {
        inspection,
        created_at,
    })
}

fn require_planned_generation(plan: &MigrationPlan, observed: &ObservedIndex) -> Result<()> {
    match plan.observed_state {
        IndexState::Missing if observed.inspection.exists => bail!(
            "{TARGET_INDEX_UID} was missing when the plan was created but is now present; refusing to adopt or reconcile an index that was not reviewed by this plan"
        ),
        IndexState::Missing => Ok(()),
        _ if !observed.inspection.exists => bail!(
            "{TARGET_INDEX_UID} existed when the plan was created but is now missing; refusing to hide possible data loss by recreating it"
        ),
        _ if observed.created_at != plan.observed_created_at => bail!(
            "{TARGET_INDEX_UID} createdAt does not match the generation bound by this plan; refusing same-UID replacement"
        ),
        _ => Ok(()),
    }
}

const fn action_for(state: IndexState) -> PlannedAction {
    match state {
        IndexState::Missing => PlannedAction::Create,
        IndexState::AlreadyPresent => PlannedAction::None,
        IndexState::SettingsDrift => PlannedAction::ReconcileSettings,
        IndexState::PrimaryKeyMismatch => PlannedAction::Refuse,
    }
}

fn require_compatible_primary_key(inspection: IndexInspection) -> Result<()> {
    if inspection.state() == IndexState::PrimaryKeyMismatch {
        bail!(
            "Meilisearch index {TARGET_INDEX_UID} has an incompatible primary key; expected {TARGET_PRIMARY_KEY}; refusing non-destructive migration"
        );
    }
    Ok(())
}

fn compute_plan_checksum(plan: &MigrationPlan) -> Result<String> {
    let mut value = serde_json::to_value(plan)?;
    value["plan_checksum"] = Value::String(String::new());
    Ok(checksum_value(&value))
}
