use anyhow::{bail, Result};
use async_trait::async_trait;
use nowledge::{
    meili::{settings_for, MeiliAdmin},
    tenant_scope_v1::checksum_value,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MIGRATION_NAME: &str = "operations_v1";
pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;
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
    pub ready: bool,
    pub mutation_free: bool,
    pub failures: Vec<String>,
}

#[async_trait]
pub trait OperationsV1Backend: Send + Sync {
    async fn index_exists(&self, index_uid: &str) -> Result<bool>;
    async fn index_primary_key(&self, index_uid: &str) -> Result<Option<String>>;
    async fn index_settings_match(&self, index_uid: &str) -> Result<bool>;

    /// Create the index if absent, apply its managed settings, and wait for all
    /// returned Meilisearch tasks before returning.
    async fn ensure_index(
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

    async fn index_settings_match(&self, index_uid: &str) -> Result<bool> {
        Ok(MeiliAdmin::index_settings_match(self, index_uid).await?)
    }

    async fn ensure_index(
        &self,
        index_uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>> {
        let task_uids =
            MeiliAdmin::ensure_index(self, index_uid, primary_key, apply_settings).await?;
        MeiliAdmin::wait_for_tasks(self, &task_uids).await?;
        Ok(task_uids)
    }
}

pub async fn create_plan<B: OperationsV1Backend>(backend: &B) -> Result<MigrationPlan> {
    let inspection = inspect(backend).await?;
    require_compatible_primary_key(inspection)?;
    let desired_settings = settings_for(TARGET_INDEX_UID);
    let desired_settings_checksum = checksum_value(&desired_settings);
    let observed_state = inspection.state();
    let action = action_for(observed_state);
    let mut plan = MigrationPlan {
        migration: MIGRATION_NAME.to_string(),
        schema_version: ARTIFACT_SCHEMA_VERSION,
        index_uid: TARGET_INDEX_UID.to_string(),
        primary_key: TARGET_PRIMARY_KEY.to_string(),
        desired_settings,
        desired_settings_checksum,
        observed_state,
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
    require_compatible_primary_key(before)?;
    if plan.observed_state != IndexState::Missing && !before.exists {
        bail!(
            "{TARGET_INDEX_UID} existed when the plan was created but is now missing; refusing to hide possible data loss by recreating it"
        );
    }

    let pre_apply_state = before.state();
    let already_present = before.exists;
    let already_ready = before == IndexInspection::ready();
    let creation_performed = !before.exists && !dry_run;
    let settings_reconciled = before.exists && !before.settings_match && !dry_run;
    let mut waited_task_count = 0;

    let ready_to_verify = if dry_run {
        already_ready
    } else {
        if !already_ready {
            let task_uids = backend
                .ensure_index(TARGET_INDEX_UID, TARGET_PRIMARY_KEY, true)
                .await?;
            waited_task_count = task_uids.len();
        }
        let after = inspect(backend).await?;
        require_compatible_primary_key(after)?;
        if after != IndexInspection::ready() {
            bail!(
                "{TARGET_INDEX_UID} did not reach the required index and settings state after apply"
            );
        }
        true
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
    let inspection = inspect(backend).await?;
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
        ready: inspection == IndexInspection::ready(),
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

async fn inspect<B: OperationsV1Backend>(backend: &B) -> Result<IndexInspection> {
    if !backend.index_exists(TARGET_INDEX_UID).await? {
        return Ok(IndexInspection::missing());
    }
    if backend
        .index_primary_key(TARGET_INDEX_UID)
        .await?
        .as_deref()
        != Some(TARGET_PRIMARY_KEY)
    {
        return Ok(IndexInspection::primary_key_mismatch());
    }
    if backend.index_settings_match(TARGET_INDEX_UID).await? {
        Ok(IndexInspection::ready())
    } else {
        Ok(IndexInspection::settings_drift())
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
