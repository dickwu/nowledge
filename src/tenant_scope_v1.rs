use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    meili::{DocumentPage, MeiliAdmin, FIXED_INDEXES},
    tenant_scope::{
        is_tenant_document, owner_scoped_storage_identity, persisted_document_id,
        restore_logical_id, scoped_storage_identity, tenant_document_with_storage_identity,
        tenant_structured_row_document,
    },
};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const MIGRATION_NAME: &str = "tenant_scope_v1";
pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_BATCH_SIZE: usize = 250;
pub const MAX_BATCH_SIZE: usize = 1_000;
const REPRESENTATIVE_CHECKSUM_LIMIT: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LegacyTenantMapping {
    pub migration: String,
    #[serde(default, alias = "assignments")]
    pub documents: Vec<LegacyTenantAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct LegacyTenantAssignment {
    pub index_uid: String,
    pub legacy_id: String,
    pub tenant_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MigrationOperation {
    pub index_uid: String,
    pub persistence_kind: String,
    pub tenant_id: String,
    pub legacy_id: String,
    pub logical_id: String,
    pub storage_identity: String,
    pub target_id: String,
    pub legacy_checksum: String,
    pub target_checksum: String,
    pub previous_target_checksum: Option<String>,
    pub previous_target_document: Option<Value>,
    pub document: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuarantinedRow {
    pub index_uid: String,
    pub legacy_id: Option<String>,
    pub source_checksum: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RepresentativeChecksum {
    pub persistence_kind: String,
    pub logical_id: String,
    pub storage_identity: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TenantInventory {
    pub expected_count: usize,
    pub planned_count: usize,
    pub already_migrated_count: usize,
    pub expected_checksum: String,
    pub planned_checksum: String,
    pub already_migrated_checksum: String,
    pub representative_checksums: Vec<RepresentativeChecksum>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexInventory {
    pub source_count: usize,
    pub source_checksum: String,
    pub post_apply_count: usize,
    pub post_apply_checksum: String,
    pub planned_count: usize,
    pub already_migrated_count: usize,
    pub quarantined_count: usize,
    pub tenants: BTreeMap<String, TenantInventory>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MigrationPlan {
    pub migration: String,
    pub schema_version: u32,
    pub batch_size: usize,
    pub mapping_checksum: String,
    pub indexes: BTreeMap<String, IndexInventory>,
    pub operations: Vec<MigrationOperation>,
    pub quarantined: Vec<QuarantinedRow>,
    pub unused_mappings: Vec<LegacyTenantAssignment>,
    pub plan_checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RollbackAction {
    pub index_uid: String,
    pub tenant_id: String,
    pub logical_id: String,
    pub storage_identity: String,
    pub target_id: String,
    pub action: String,
    pub restore_checksum: Option<String>,
    pub restore_document: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RollbackPlan {
    pub migration: String,
    pub schema_version: u32,
    pub plan_checksum: String,
    pub preserves_legacy_rows: bool,
    pub actions: Vec<RollbackAction>,
    pub action_checksum: String,
    pub acknowledgement: String,
    pub rollback_checksum: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MigrationCheckpoint {
    pub migration: String,
    pub plan_checksum: String,
    pub next_operation: usize,
    pub completed_batches: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub mapping_checksum: String,
    pub mutation_free: bool,
    pub operation_count: usize,
    pub quarantined_count: usize,
    pub unused_mapping_count: usize,
    pub indexes: BTreeMap<String, IndexInventory>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApplyReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub dry_run: bool,
    pub mutation_free: bool,
    pub operation_count: usize,
    pub starting_operation: usize,
    pub completed_operations: usize,
    pub completed_batches: usize,
    pub remote_batches_written: usize,
    pub checkpoint_writes: usize,
    pub quarantined_count: usize,
    pub ready_to_verify: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TenantVerification {
    pub expected_count: usize,
    pub verified_count: usize,
    pub expected_checksum: String,
    pub observed_checksum: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexVerification {
    pub expected_total_count: usize,
    pub observed_total_count: usize,
    pub expected_total_checksum: String,
    pub observed_total_checksum: String,
    pub snapshot_match: bool,
    pub expected_count: usize,
    pub verified_count: usize,
    pub missing_count: usize,
    pub changed_count: usize,
    pub legacy_preserved_count: usize,
    pub legacy_missing_or_changed_count: usize,
    pub tenants: BTreeMap<String, TenantVerification>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    pub mode: String,
    pub migration: String,
    pub plan_checksum: String,
    pub writes_verified: bool,
    pub legacy_rows_preserved: bool,
    pub unresolved_quarantine: usize,
    pub ready_to_cutover: bool,
    pub indexes: BTreeMap<String, IndexVerification>,
    pub failures: Vec<String>,
}

#[async_trait]
pub trait MigrationBackend: Send + Sync {
    async fn fetch_documents_page(
        &self,
        index_uid: &str,
        offset: usize,
        limit: usize,
    ) -> Result<DocumentPage>;

    async fn write_documents(&self, index_uid: &str, documents: &[Value]) -> Result<()>;
}

#[async_trait]
impl MigrationBackend for MeiliAdmin {
    async fn fetch_documents_page(
        &self,
        index_uid: &str,
        offset: usize,
        limit: usize,
    ) -> Result<DocumentPage> {
        MeiliAdmin::fetch_documents_page(self, index_uid, offset, limit)
            .await
            .map_err(|error| anyhow!(error.to_string()))
    }

    async fn write_documents(&self, index_uid: &str, documents: &[Value]) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        if let Some(task_uid) = self
            .add_documents(index_uid, documents)
            .await
            .map_err(|error| anyhow!(error.to_string()))?
        {
            self.wait_for_task(&task_uid)
                .await
                .map_err(|error| anyhow!(error.to_string()))?;
        }
        Ok(())
    }
}

pub trait CheckpointStore {
    fn load(&self) -> Result<Option<MigrationCheckpoint>>;
    fn save(&mut self, checkpoint: &MigrationCheckpoint) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct FileCheckpointStore {
    path: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn load(&self) -> Result<Option<MigrationCheckpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }
        read_json(&self.path).map(Some)
    }

    fn save(&mut self, checkpoint: &MigrationCheckpoint) -> Result<()> {
        write_json_atomic(&self.path, checkpoint)
    }
}

pub async fn create_plan<B: MigrationBackend>(
    backend: &B,
    mapping: &LegacyTenantMapping,
    batch_size: usize,
) -> Result<MigrationPlan> {
    validate_mapping(mapping)?;
    validate_batch_size(batch_size)?;

    let mapping_checksum = mapping_checksum(mapping)?;
    let assignment_index = assignment_index(mapping);
    let mut used_assignments = BTreeSet::new();
    let mut operations = Vec::new();
    let mut quarantined = Vec::new();
    let mut indexes = BTreeMap::new();

    for index_uid in FIXED_INDEXES {
        let mut documents = scan_index(backend, index_uid, batch_size).await?;
        documents.sort_by_key(document_sort_key);
        let mut post_apply_documents = documents.clone();
        let index_operation_start = operations.len();

        let existing_checksums = documents
            .iter()
            .filter_map(|document| {
                string_field(document, "id").map(|id| (id.to_string(), checksum_value(document)))
            })
            .collect::<BTreeMap<_, _>>();
        let existing_documents = documents
            .iter()
            .filter_map(|document| {
                string_field(document, "id").map(|id| (id.to_string(), document.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        let valid_migrated_ids = documents
            .iter()
            .filter_map(|document| {
                valid_migrated_identity(index_uid, document)
                    .and_then(|_| string_field(document, "id").map(ToString::to_string))
            })
            .collect::<BTreeSet<_>>();
        let mut expected_targets = documents
            .iter()
            .filter_map(|document| {
                valid_migrated_identity(index_uid, document)
                    .map(|identity| (identity, (checksum_value(document), false)))
            })
            .collect::<BTreeMap<_, _>>();

        let source_checksums = documents.iter().map(checksum_value).collect::<Vec<_>>();
        let mut inventory = IndexInventory {
            source_count: documents.len(),
            source_checksum: checksum_strings(&source_checksums),
            ..IndexInventory::default()
        };
        let mut claimed_targets: BTreeMap<String, String> = BTreeMap::new();

        for document in documents {
            let source_checksum = checksum_value(&document);
            if valid_migrated_identity(index_uid, &document).is_some() {
                continue;
            }

            let Some(legacy_id) = string_field(&document, "id").map(ToString::to_string) else {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    None,
                    source_checksum,
                    "missing_or_non_string_legacy_id",
                );
                continue;
            };

            let key = (index_uid.to_string(), legacy_id.clone());
            let Some(tenants) = assignment_index.get(&key) else {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "missing_explicit_tenant_mapping",
                );
                continue;
            };
            if tenants.len() != 1 {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "ambiguous_explicit_tenant_mapping",
                );
                continue;
            }
            let tenant_id = tenants.iter().next().expect("one tenant").clone();
            used_assignments.insert((index_uid.to_string(), legacy_id.clone(), tenant_id.clone()));

            if let Some(existing_tenant) = string_field(&document, "tenant_id") {
                if !existing_tenant.trim().is_empty() && existing_tenant != tenant_id {
                    quarantine(
                        &mut quarantined,
                        &mut inventory,
                        index_uid,
                        Some(legacy_id),
                        source_checksum,
                        "mapping_conflicts_with_document_tenant_id",
                    );
                    continue;
                }
            } else if document.get("tenant_id").is_some() {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "document_tenant_id_is_not_a_string",
                );
                continue;
            }

            let Some(logical_id) = legacy_logical_id(index_uid, &document, &legacy_id) else {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "missing_or_non_string_logical_identity",
                );
                continue;
            };
            let Some(persistence_kind) = persistence_kind(index_uid, &document) else {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "missing_or_invalid_persistence_kind",
                );
                continue;
            };
            let Some(storage_identity) =
                document_storage_identity(index_uid, &document, &logical_id)
            else {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    match *index_uid {
                        "rag_structured_rows" => "missing_or_non_string_snapshot_id",
                        "rag_parse_artifacts" => "invalid_owner_user_id_scope",
                        _ => "invalid_storage_identity",
                    },
                );
                continue;
            };
            let target_id = persisted_document_id(&tenant_id, &persistence_kind, &storage_identity)
                .map_err(|error| anyhow!(error.to_string()))?;
            if target_id == legacy_id {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "target_id_matches_legacy_id",
                );
                continue;
            }
            if let Some(first_legacy_id) =
                claimed_targets.insert(target_id.clone(), legacy_id.clone())
            {
                if first_legacy_id != legacy_id {
                    quarantine(
                        &mut quarantined,
                        &mut inventory,
                        index_uid,
                        Some(legacy_id),
                        source_checksum,
                        "multiple_legacy_rows_share_logical_identity",
                    );
                    continue;
                }
            }
            if existing_checksums.contains_key(&target_id)
                && !valid_migrated_ids.contains(&target_id)
            {
                quarantine(
                    &mut quarantined,
                    &mut inventory,
                    index_uid,
                    Some(legacy_id),
                    source_checksum,
                    "target_id_collides_with_existing_row",
                );
                continue;
            }

            let target = if *index_uid == "rag_structured_rows" {
                tenant_structured_row_document(&tenant_id, &document)
            } else {
                tenant_document_with_storage_identity(
                    &tenant_id,
                    &persistence_kind,
                    &logical_id,
                    &storage_identity,
                    &document,
                )
            }
            .map_err(|error| anyhow!(error.to_string()))?;
            let target_checksum = checksum_value(&target);
            if existing_checksums.get(&target_id) == Some(&target_checksum) {
                continue;
            }

            expected_targets.insert(
                (
                    tenant_id.clone(),
                    persistence_kind.clone(),
                    storage_identity.clone(),
                    logical_id.clone(),
                ),
                (target_checksum.clone(), true),
            );
            upsert_by_string_id(&mut post_apply_documents, target.clone());
            let previous_target_checksum = existing_checksums.get(&target_id).cloned();
            let previous_target_document = existing_documents.get(&target_id).cloned();
            operations.push(MigrationOperation {
                index_uid: index_uid.to_string(),
                persistence_kind,
                tenant_id,
                legacy_id,
                logical_id,
                storage_identity,
                target_id,
                legacy_checksum: source_checksum,
                target_checksum,
                previous_target_checksum,
                previous_target_document,
                document: target,
            });
        }

        inventory.planned_count = operations.len() - index_operation_start;
        inventory.already_migrated_count = expected_targets
            .values()
            .filter(|(_, planned)| !planned)
            .count();
        let post_apply_checksums = post_apply_documents
            .iter()
            .map(checksum_value)
            .collect::<Vec<_>>();
        inventory.post_apply_count = post_apply_documents.len();
        inventory.post_apply_checksum = checksum_strings(&post_apply_checksums);

        let tenant_ids = expected_targets
            .keys()
            .map(|(tenant_id, _, _, _)| tenant_id.clone())
            .collect::<BTreeSet<_>>();
        for tenant_id in tenant_ids {
            let mut planned = Vec::new();
            let mut migrated = Vec::new();
            for (
                (target_tenant, target_kind, storage_identity, logical_id),
                (checksum, is_planned),
            ) in &expected_targets
            {
                if target_tenant != &tenant_id {
                    continue;
                }
                let target = (
                    target_kind.clone(),
                    storage_identity.clone(),
                    logical_id.clone(),
                    checksum.clone(),
                );
                if *is_planned {
                    planned.push(target);
                } else {
                    migrated.push(target);
                }
            }
            planned.sort();
            migrated.sort();
            let mut expected = planned.clone();
            expected.extend(migrated.iter().cloned());
            expected.sort();
            let representative_checksums = expected
                .iter()
                .take(REPRESENTATIVE_CHECKSUM_LIMIT)
                .map(
                    |(persistence_kind, storage_identity, logical_id, checksum)| {
                        RepresentativeChecksum {
                            persistence_kind: persistence_kind.clone(),
                            logical_id: logical_id.clone(),
                            storage_identity: storage_identity.clone(),
                            checksum: checksum.clone(),
                        }
                    },
                )
                .collect();
            inventory.tenants.insert(
                tenant_id,
                TenantInventory {
                    expected_count: expected.len(),
                    planned_count: planned.len(),
                    already_migrated_count: migrated.len(),
                    expected_checksum: checksum_targets(&expected),
                    planned_checksum: checksum_targets(&planned),
                    already_migrated_checksum: checksum_targets(&migrated),
                    representative_checksums,
                },
            );
        }

        indexes.insert(index_uid.to_string(), inventory);
    }

    operations.sort_by(|left, right| {
        (
            &left.index_uid,
            &left.tenant_id,
            &left.persistence_kind,
            &left.storage_identity,
            &left.logical_id,
            &left.legacy_id,
        )
            .cmp(&(
                &right.index_uid,
                &right.tenant_id,
                &right.persistence_kind,
                &right.storage_identity,
                &right.logical_id,
                &right.legacy_id,
            ))
    });
    quarantined.sort_by(|left, right| {
        (&left.index_uid, &left.legacy_id, &left.source_checksum).cmp(&(
            &right.index_uid,
            &right.legacy_id,
            &right.source_checksum,
        ))
    });
    let mut unused_mappings = mapping
        .documents
        .iter()
        .filter(|assignment| {
            !used_assignments.contains(&(
                assignment.index_uid.clone(),
                assignment.legacy_id.clone(),
                assignment.tenant_id.clone(),
            ))
        })
        .cloned()
        .collect::<Vec<_>>();
    unused_mappings.sort();
    unused_mappings.dedup();

    let mut plan = MigrationPlan {
        migration: MIGRATION_NAME.to_string(),
        schema_version: ARTIFACT_SCHEMA_VERSION,
        batch_size,
        mapping_checksum,
        indexes,
        operations,
        quarantined,
        unused_mappings,
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
        mapping_checksum: plan.mapping_checksum.clone(),
        mutation_free: true,
        operation_count: plan.operations.len(),
        quarantined_count: plan.quarantined.len(),
        unused_mapping_count: plan.unused_mappings.len(),
        indexes: plan.indexes.clone(),
    }
}

pub fn create_rollback_plan(plan: &MigrationPlan) -> Result<RollbackPlan> {
    validate_plan(plan)?;
    let actions = plan
        .operations
        .iter()
        .map(|operation| RollbackAction {
            index_uid: operation.index_uid.clone(),
            tenant_id: operation.tenant_id.clone(),
            logical_id: operation.logical_id.clone(),
            storage_identity: operation.storage_identity.clone(),
            target_id: operation.target_id.clone(),
            action: if operation.previous_target_document.is_some() {
                "restore_previous_migrated_copy_after_traffic_rollback"
            } else {
                "delete_migrated_copy_after_traffic_rollback"
            }
            .to_string(),
            restore_checksum: operation.previous_target_checksum.clone(),
            restore_document: operation.previous_target_document.clone(),
        })
        .collect::<Vec<_>>();
    let action_checksum = checksum_serializable(&actions)?;
    let acknowledgement = format!(
        "ack:{MIGRATION_NAME}:{}:{action_checksum}",
        plan.plan_checksum
    );
    let mut rollback = RollbackPlan {
        migration: MIGRATION_NAME.to_string(),
        schema_version: ARTIFACT_SCHEMA_VERSION,
        plan_checksum: plan.plan_checksum.clone(),
        preserves_legacy_rows: true,
        actions,
        action_checksum,
        acknowledgement,
        rollback_checksum: String::new(),
    };
    rollback.rollback_checksum = compute_rollback_checksum(&rollback)?;
    validate_rollback_plan(plan, &rollback, &rollback.acknowledgement)?;
    Ok(rollback)
}

pub async fn apply_plan<B: MigrationBackend, C: CheckpointStore>(
    backend: &B,
    plan: &MigrationPlan,
    rollback: &RollbackPlan,
    acknowledgement: &str,
    checkpoint_store: &mut C,
    dry_run: bool,
) -> Result<ApplyReport> {
    validate_plan(plan)?;
    validate_rollback_plan(plan, rollback, acknowledgement)?;
    verify_apply_preconditions(backend, plan).await?;

    let checkpoint = checkpoint_store.load()?.unwrap_or(MigrationCheckpoint {
        migration: MIGRATION_NAME.to_string(),
        plan_checksum: plan.plan_checksum.clone(),
        next_operation: 0,
        completed_batches: 0,
    });
    validate_checkpoint(plan, &checkpoint)?;
    let starting_operation = checkpoint.next_operation;

    if dry_run {
        return Ok(ApplyReport {
            mode: "apply".to_string(),
            migration: MIGRATION_NAME.to_string(),
            plan_checksum: plan.plan_checksum.clone(),
            dry_run: true,
            mutation_free: true,
            operation_count: plan.operations.len(),
            starting_operation,
            completed_operations: checkpoint.next_operation,
            completed_batches: checkpoint.completed_batches,
            remote_batches_written: 0,
            checkpoint_writes: 0,
            quarantined_count: plan.quarantined.len(),
            ready_to_verify: checkpoint.next_operation == plan.operations.len(),
        });
    }

    let mut next_operation = checkpoint.next_operation;
    let mut completed_batches = checkpoint.completed_batches;
    let mut remote_batches_written = 0;
    let mut checkpoint_writes = 0;
    while next_operation < plan.operations.len() {
        let index_uid = plan.operations[next_operation].index_uid.clone();
        let mut end = next_operation;
        while end < plan.operations.len()
            && end - next_operation < plan.batch_size
            && plan.operations[end].index_uid == index_uid
        {
            end += 1;
        }
        let documents = plan.operations[next_operation..end]
            .iter()
            .map(|operation| operation.document.clone())
            .collect::<Vec<_>>();
        backend.write_documents(&index_uid, &documents).await?;
        remote_batches_written += 1;
        completed_batches += 1;
        next_operation = end;
        checkpoint_store.save(&MigrationCheckpoint {
            migration: MIGRATION_NAME.to_string(),
            plan_checksum: plan.plan_checksum.clone(),
            next_operation,
            completed_batches,
        })?;
        checkpoint_writes += 1;
    }

    Ok(ApplyReport {
        mode: "apply".to_string(),
        migration: MIGRATION_NAME.to_string(),
        plan_checksum: plan.plan_checksum.clone(),
        dry_run: false,
        mutation_free: remote_batches_written == 0,
        operation_count: plan.operations.len(),
        starting_operation,
        completed_operations: next_operation,
        completed_batches,
        remote_batches_written,
        checkpoint_writes,
        quarantined_count: plan.quarantined.len(),
        ready_to_verify: next_operation == plan.operations.len(),
    })
}

pub async fn verify_plan<B: MigrationBackend>(
    backend: &B,
    plan: &MigrationPlan,
) -> Result<VerificationReport> {
    validate_plan(plan)?;
    let mut indexes = BTreeMap::new();
    let mut failures = Vec::new();

    for index_uid in FIXED_INDEXES {
        let documents = scan_index(backend, index_uid, plan.batch_size).await?;
        let by_id = documents
            .iter()
            .filter_map(|document| {
                let id = string_field(document, "id")?.to_string();
                Some((id, document))
            })
            .collect::<BTreeMap<_, _>>();
        let legacy_checksums = legacy_checksums_by_identity(index_uid, &documents);
        let inventory = plan
            .indexes
            .get(*index_uid)
            .ok_or_else(|| anyhow!("plan is missing inventory for {index_uid}"))?;
        let observed_total_checksum =
            checksum_strings(&documents.iter().map(checksum_value).collect::<Vec<_>>());
        let snapshot_match = documents.len() == inventory.post_apply_count
            && observed_total_checksum == inventory.post_apply_checksum;
        let expected = plan
            .operations
            .iter()
            .filter(|operation| operation.index_uid == *index_uid)
            .collect::<Vec<_>>();
        let mut verification = IndexVerification {
            expected_total_count: inventory.post_apply_count,
            observed_total_count: documents.len(),
            expected_total_checksum: inventory.post_apply_checksum.clone(),
            observed_total_checksum,
            snapshot_match,
            expected_count: inventory
                .tenants
                .values()
                .map(|tenant| tenant.expected_count)
                .sum(),
            ..IndexVerification::default()
        };
        let mut observed_tenants: BTreeMap<String, Vec<(String, String, String, String)>> =
            BTreeMap::new();

        if !snapshot_match {
            failures.push(format!(
                "{index_uid}: full post-apply count/checksum differs from the plan inventory"
            ));
        }
        for document in &documents {
            if let Some((tenant_id, persistence_kind, storage_identity, logical_id)) =
                valid_migrated_identity(index_uid, document)
            {
                observed_tenants.entry(tenant_id).or_default().push((
                    persistence_kind,
                    storage_identity,
                    logical_id,
                    checksum_value(document),
                ));
            }
        }

        for operation in expected {
            match by_id.get(&operation.target_id) {
                Some(document) if checksum_value(document) == operation.target_checksum => {}
                Some(_) => {
                    verification.changed_count += 1;
                    failures.push(format!(
                        "{index_uid}: migrated document {} changed",
                        operation.target_id
                    ));
                }
                None => {
                    verification.missing_count += 1;
                    failures.push(format!(
                        "{index_uid}: migrated document {} is missing",
                        operation.target_id
                    ));
                }
            }
            match legacy_checksums.get(&(
                operation.legacy_id.clone(),
                operation.persistence_kind.clone(),
                operation.storage_identity.clone(),
            )) {
                Some(checksums) if checksums.contains(&operation.legacy_checksum) => {
                    verification.legacy_preserved_count += 1;
                }
                _ => {
                    verification.legacy_missing_or_changed_count += 1;
                    failures.push(format!(
                        "{index_uid}: legacy document {} is missing or changed",
                        operation.legacy_id
                    ));
                }
            }
        }

        let tenant_ids = inventory
            .tenants
            .keys()
            .chain(observed_tenants.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for tenant_id in tenant_ids {
            let expected_tenant = inventory.tenants.get(&tenant_id);
            let mut observed_pairs = observed_tenants.remove(&tenant_id).unwrap_or_default();
            observed_pairs.sort();
            let observed_checksum = checksum_targets(&observed_pairs);
            let expected_count = expected_tenant
                .map(|tenant| tenant.expected_count)
                .unwrap_or_default();
            let expected_checksum = expected_tenant
                .map(|tenant| tenant.expected_checksum.clone())
                .unwrap_or_else(|| checksum_targets(&[]));
            if observed_pairs.len() != expected_count || observed_checksum != expected_checksum {
                failures.push(format!(
                    "{index_uid}: tenant {tenant_id} count/checksum differs from the plan inventory"
                ));
            }
            verification.tenants.insert(
                tenant_id,
                TenantVerification {
                    expected_count,
                    verified_count: observed_pairs.len(),
                    expected_checksum,
                    observed_checksum,
                },
            );
        }
        verification.verified_count = verification
            .tenants
            .values()
            .map(|tenant| tenant.verified_count)
            .sum();
        indexes.insert(index_uid.to_string(), verification);
    }

    let writes_verified = indexes.values().all(|index| {
        index.snapshot_match
            && index.missing_count == 0
            && index.changed_count == 0
            && index.tenants.values().all(|tenant| {
                tenant.expected_count == tenant.verified_count
                    && tenant.expected_checksum == tenant.observed_checksum
            })
    });
    let legacy_rows_preserved = indexes
        .values()
        .all(|index| index.snapshot_match && index.legacy_missing_or_changed_count == 0);
    let unresolved_quarantine = plan.quarantined.len();
    if unresolved_quarantine > 0 {
        failures.push(format!(
            "{unresolved_quarantine} legacy rows still require explicit tenant mapping"
        ));
    }
    let ready_to_cutover = writes_verified && legacy_rows_preserved && unresolved_quarantine == 0;

    Ok(VerificationReport {
        mode: "verify".to_string(),
        migration: MIGRATION_NAME.to_string(),
        plan_checksum: plan.plan_checksum.clone(),
        writes_verified,
        legacy_rows_preserved,
        unresolved_quarantine,
        ready_to_cutover,
        indexes,
        failures,
    })
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse JSON from {}", path.display()))
}

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("artifact path must have a UTF-8 file name"))?;
    let temporary = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(value)?;

    let write_result = (|| -> Result<()> {
        #[cfg(unix)]
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)?
        };
        #[cfg(not(unix))]
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result.with_context(|| format!("failed to atomically write {}", path.display()))
}

fn validate_mapping(mapping: &LegacyTenantMapping) -> Result<()> {
    if mapping.migration != MIGRATION_NAME {
        bail!(
            "mapping migration must be {MIGRATION_NAME}, got {}",
            mapping.migration
        );
    }
    let fixed = FIXED_INDEXES.iter().copied().collect::<BTreeSet<_>>();
    for assignment in &mapping.documents {
        if !fixed.contains(assignment.index_uid.as_str()) {
            bail!(
                "mapping references unknown fixed index {}",
                assignment.index_uid
            );
        }
        if assignment.legacy_id.trim().is_empty() {
            bail!("mapping legacy_id must be non-empty");
        }
        if assignment.tenant_id.trim().is_empty() {
            bail!("mapping tenant_id must be non-empty");
        }
    }
    Ok(())
}

fn validate_batch_size(batch_size: usize) -> Result<()> {
    if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
        bail!("batch size must be between 1 and {MAX_BATCH_SIZE}");
    }
    Ok(())
}

pub fn validate_plan(plan: &MigrationPlan) -> Result<()> {
    if plan.migration != MIGRATION_NAME || plan.schema_version != ARTIFACT_SCHEMA_VERSION {
        bail!("unsupported migration plan version");
    }
    validate_batch_size(plan.batch_size)?;
    if plan.plan_checksum != compute_plan_checksum(plan)? {
        bail!("migration plan checksum does not match its contents");
    }
    let fixed = FIXED_INDEXES.iter().copied().collect::<BTreeSet<_>>();
    let inventory_indexes = plan
        .indexes
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if inventory_indexes != fixed {
        bail!("plan inventory must cover every fixed index exactly once");
    }
    let mut targets = BTreeSet::new();
    for operation in &plan.operations {
        if !fixed.contains(operation.index_uid.as_str()) {
            bail!("plan operation references unknown fixed index");
        }
        if string_field(&operation.document, "id") != Some(operation.target_id.as_str())
            || string_field(&operation.document, "logical_id")
                != Some(operation.logical_id.as_str())
            || string_field(&operation.document, "tenant_id") != Some(operation.tenant_id.as_str())
            || !is_tenant_document(&operation.index_uid, &operation.document)
        {
            bail!("plan operation document identity is inconsistent");
        }
        if persistence_kind(&operation.index_uid, &operation.document).as_deref()
            != Some(operation.persistence_kind.as_str())
        {
            bail!("plan operation persistence kind is inconsistent");
        }
        if document_storage_identity(
            &operation.index_uid,
            &operation.document,
            &operation.logical_id,
        )
        .as_deref()
            != Some(operation.storage_identity.as_str())
        {
            bail!("plan operation storage identity is inconsistent");
        }
        if operation.index_uid == "rag_structured_rows" {
            let restored = restore_logical_id(&operation.index_uid, operation.document.clone());
            let rebuilt = tenant_structured_row_document(&operation.tenant_id, &restored)
                .map_err(|error| anyhow!(error.to_string()))?;
            if rebuilt != operation.document {
                bail!("plan operation structured-row payload shadow is inconsistent");
            }
        }
        if operation.target_checksum != checksum_value(&operation.document) {
            bail!("plan operation target checksum is inconsistent");
        }
        match (
            &operation.previous_target_checksum,
            &operation.previous_target_document,
        ) {
            (None, None) => {}
            (Some(checksum), Some(document))
                if string_field(document, "id") == Some(operation.target_id.as_str())
                    && checksum == &checksum_value(document)
                    && checksum != &operation.target_checksum => {}
            _ => bail!("plan operation previous-target backup is inconsistent"),
        }
        let expected_target = persisted_document_id(
            &operation.tenant_id,
            &operation.persistence_kind,
            &operation.storage_identity,
        )
        .map_err(|error| anyhow!(error.to_string()))?;
        if operation.target_id != expected_target || operation.target_id == operation.legacy_id {
            bail!("plan operation target ID is not tenant-safe");
        }
        if !targets.insert((operation.index_uid.clone(), operation.target_id.clone())) {
            bail!("plan contains duplicate target document IDs");
        }
    }
    let mut sorted = plan.operations.clone();
    sorted.sort_by(|left, right| {
        (
            &left.index_uid,
            &left.tenant_id,
            &left.persistence_kind,
            &left.storage_identity,
            &left.logical_id,
            &left.legacy_id,
        )
            .cmp(&(
                &right.index_uid,
                &right.tenant_id,
                &right.persistence_kind,
                &right.storage_identity,
                &right.logical_id,
                &right.legacy_id,
            ))
    });
    if sorted != plan.operations {
        bail!("plan operations are not in deterministic order");
    }
    for (index_uid, inventory) in &plan.indexes {
        let planned = plan
            .operations
            .iter()
            .filter(|operation| operation.index_uid == *index_uid)
            .count();
        let tenant_planned = inventory
            .tenants
            .values()
            .map(|tenant| tenant.planned_count)
            .sum::<usize>();
        let tenant_migrated = inventory
            .tenants
            .values()
            .map(|tenant| tenant.already_migrated_count)
            .sum::<usize>();
        if inventory.planned_count != planned
            || tenant_planned != planned
            || tenant_migrated != inventory.already_migrated_count
            || inventory.tenants.values().any(|tenant| {
                tenant.expected_count != tenant.planned_count + tenant.already_migrated_count
            })
        {
            bail!("plan inventory counts are internally inconsistent");
        }
    }
    Ok(())
}

fn validate_rollback_plan(
    plan: &MigrationPlan,
    rollback: &RollbackPlan,
    acknowledgement: &str,
) -> Result<()> {
    if rollback.migration != MIGRATION_NAME
        || rollback.schema_version != ARTIFACT_SCHEMA_VERSION
        || rollback.plan_checksum != plan.plan_checksum
        || !rollback.preserves_legacy_rows
    {
        bail!("rollback plan is not compatible with the migration plan");
    }
    if rollback.action_checksum != checksum_serializable(&rollback.actions)?
        || rollback.rollback_checksum != compute_rollback_checksum(rollback)?
    {
        bail!("rollback plan checksum does not match its contents");
    }
    let expected_acknowledgement = format!(
        "ack:{MIGRATION_NAME}:{}:{}",
        plan.plan_checksum, rollback.action_checksum
    );
    if rollback.acknowledgement != expected_acknowledgement
        || acknowledgement != expected_acknowledgement
    {
        bail!("apply requires the exact acknowledgement from its rollback plan");
    }
    let expected_targets = plan
        .operations
        .iter()
        .map(|operation| {
            (
                &operation.index_uid,
                &operation.tenant_id,
                &operation.storage_identity,
                &operation.logical_id,
                &operation.target_id,
            )
        })
        .collect::<BTreeSet<_>>();
    let rollback_targets = rollback
        .actions
        .iter()
        .map(|action| {
            (
                &action.index_uid,
                &action.tenant_id,
                &action.storage_identity,
                &action.logical_id,
                &action.target_id,
            )
        })
        .collect::<BTreeSet<_>>();
    if expected_targets != rollback_targets
        || rollback.actions.len() != plan.operations.len()
        || rollback_targets.len() != rollback.actions.len()
    {
        bail!("rollback plan does not cover every migrated target exactly once");
    }
    for (operation, action) in plan.operations.iter().zip(&rollback.actions) {
        let expected_action = if operation.previous_target_document.is_some() {
            "restore_previous_migrated_copy_after_traffic_rollback"
        } else {
            "delete_migrated_copy_after_traffic_rollback"
        };
        if action.action != expected_action
            || action.storage_identity != operation.storage_identity
            || action.restore_checksum != operation.previous_target_checksum
            || action.restore_document != operation.previous_target_document
        {
            bail!("rollback action does not preserve the pre-apply target state");
        }
    }
    Ok(())
}

fn validate_checkpoint(plan: &MigrationPlan, checkpoint: &MigrationCheckpoint) -> Result<()> {
    if checkpoint.migration != MIGRATION_NAME
        || checkpoint.plan_checksum != plan.plan_checksum
        || checkpoint.next_operation > plan.operations.len()
    {
        bail!("checkpoint does not belong to this migration plan");
    }
    let mut next_operation = 0;
    let mut completed_batches = 0;
    let mut valid_position = checkpoint.next_operation == 0 && checkpoint.completed_batches == 0;
    while next_operation < plan.operations.len() {
        let index_uid = &plan.operations[next_operation].index_uid;
        let mut end = next_operation;
        while end < plan.operations.len()
            && end - next_operation < plan.batch_size
            && plan.operations[end].index_uid == *index_uid
        {
            end += 1;
        }
        next_operation = end;
        completed_batches += 1;
        valid_position |= checkpoint.next_operation == next_operation
            && checkpoint.completed_batches == completed_batches;
    }
    if !valid_position {
        bail!("checkpoint is not on a completed migration batch boundary");
    }
    Ok(())
}

async fn verify_apply_preconditions<B: MigrationBackend>(
    backend: &B,
    plan: &MigrationPlan,
) -> Result<()> {
    let mut operations_by_index: BTreeMap<&str, Vec<&MigrationOperation>> = BTreeMap::new();
    for operation in &plan.operations {
        operations_by_index
            .entry(&operation.index_uid)
            .or_default()
            .push(operation);
    }
    for (index_uid, operations) in operations_by_index {
        let documents = scan_index(backend, index_uid, plan.batch_size).await?;
        let legacy_checksums = legacy_checksums_by_identity(index_uid, &documents);
        let mut target_checksums: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for document in &documents {
            let Some(id) = string_field(document, "id") else {
                continue;
            };
            target_checksums
                .entry(id.to_string())
                .or_default()
                .insert(checksum_value(document));
        }
        for operation in operations {
            match legacy_checksums.get(&(
                operation.legacy_id.clone(),
                operation.persistence_kind.clone(),
                operation.storage_identity.clone(),
            )) {
                Some(checksums) if checksums.contains(&operation.legacy_checksum) => {}
                _ => bail!(
                    "legacy source {} in {index_uid} changed after the plan snapshot",
                    operation.legacy_id
                ),
            }

            let observed = target_checksums.get(&operation.target_id);
            let exact_target = observed.is_some_and(|checksums| {
                checksums.len() == 1 && checksums.contains(&operation.target_checksum)
            });
            let exact_previous =
                operation
                    .previous_target_checksum
                    .as_ref()
                    .is_some_and(|previous_checksum| {
                        observed.is_some_and(|checksums| {
                            checksums.len() == 1 && checksums.contains(previous_checksum)
                        })
                    });
            let absent_without_previous =
                observed.is_none() && operation.previous_target_checksum.is_none();
            if !absent_without_previous && !exact_target && !exact_previous {
                bail!(
                    "migration target {} in {index_uid} changed after the plan snapshot",
                    operation.target_id
                );
            }
        }
    }
    Ok(())
}

async fn scan_index<B: MigrationBackend>(
    backend: &B,
    index_uid: &str,
    page_size: usize,
) -> Result<Vec<Value>> {
    let mut offset = 0;
    let mut expected_total = None;
    let mut documents = Vec::new();
    loop {
        let page = backend
            .fetch_documents_page(index_uid, offset, page_size)
            .await
            .with_context(|| format!("failed to scan fixed index {index_uid}"))?;
        if page.offset != offset {
            bail!(
                "fixed index {index_uid} returned offset {} while {offset} was requested",
                page.offset
            );
        }
        if let Some(total) = expected_total {
            if page.total != total {
                bail!("fixed index {index_uid} changed during its plan/verify scan");
            }
        } else {
            expected_total = Some(page.total);
        }
        if page.results.is_empty() {
            if offset < page.total {
                bail!("fixed index {index_uid} returned an incomplete document page");
            }
            break;
        }
        offset = offset
            .checked_add(page.results.len())
            .ok_or_else(|| anyhow!("document count overflow while scanning {index_uid}"))?;
        documents.extend(page.results);
        if offset >= page.total {
            break;
        }
    }
    if documents.len() != expected_total.unwrap_or_default() {
        bail!(
            "fixed index {index_uid} scan count changed: expected {}, observed {}",
            expected_total.unwrap_or_default(),
            documents.len()
        );
    }
    Ok(documents)
}

fn assignment_index(mapping: &LegacyTenantMapping) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut result: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for assignment in &mapping.documents {
        result
            .entry((assignment.index_uid.clone(), assignment.legacy_id.clone()))
            .or_default()
            .insert(assignment.tenant_id.clone());
    }
    result
}

fn quarantine(
    rows: &mut Vec<QuarantinedRow>,
    inventory: &mut IndexInventory,
    index_uid: &str,
    legacy_id: Option<String>,
    source_checksum: String,
    reason: &str,
) {
    inventory.quarantined_count += 1;
    rows.push(QuarantinedRow {
        index_uid: index_uid.to_string(),
        legacy_id,
        source_checksum,
        reason: reason.to_string(),
    });
}

fn valid_migrated_identity(
    index_uid: &str,
    document: &Value,
) -> Option<(String, String, String, String)> {
    if !is_tenant_document(index_uid, document) {
        return None;
    }
    let id = string_field(document, "id")?;
    let tenant_id = string_field(document, "tenant_id")?;
    let logical_id = string_field(document, "logical_id")?;
    if index_uid == "rag_structured_rows" {
        let restored = restore_logical_id(index_uid, document.clone());
        let rebuilt = tenant_structured_row_document(tenant_id, &restored).ok()?;
        if &rebuilt != document {
            return None;
        }
    }
    let kind = persistence_kind(index_uid, document)?;
    let storage_identity = document_storage_identity(index_uid, document, logical_id)?;
    let expected = persisted_document_id(tenant_id, &kind, &storage_identity).ok()?;
    if id != expected {
        return None;
    }
    Some((
        tenant_id.to_string(),
        kind,
        storage_identity,
        logical_id.to_string(),
    ))
}

fn document_storage_identity(
    index_uid: &str,
    document: &Value,
    logical_id: &str,
) -> Option<String> {
    if logical_id.trim().is_empty() {
        return None;
    }
    if index_uid == "rag_structured_rows" {
        let snapshot_id = string_field(document, "snapshot_id")?;
        if snapshot_id.trim().is_empty() {
            return None;
        }
        return scoped_storage_identity(snapshot_id, logical_id).ok();
    }
    if index_uid == "rag_parse_artifacts" {
        let owner_user_id = match document.get("owner_user_id") {
            Some(Value::String(owner_user_id)) if !owner_user_id.trim().is_empty() => {
                Some(owner_user_id.as_str())
            }
            Some(Value::Null) | None => None,
            Some(_) => return None,
        };
        return owner_scoped_storage_identity(owner_user_id, logical_id).ok();
    }
    Some(logical_id.to_string())
}

fn persistence_kind(index_uid: &str, document: &Value) -> Option<String> {
    if index_uid == "rag_harness_components" {
        let doc_kind = string_field(document, "doc_kind")?;
        if !matches!(doc_kind, "component" | "revision") {
            return None;
        }
        return Some(format!("{index_uid}:{doc_kind}"));
    }
    Some(index_uid.to_string())
}

fn legacy_checksums_by_identity(
    index_uid: &str,
    documents: &[Value],
) -> BTreeMap<(String, String, String), BTreeSet<String>> {
    let mut checksums: BTreeMap<(String, String, String), BTreeSet<String>> = BTreeMap::new();
    for document in documents {
        let Some(id) = string_field(document, "id") else {
            continue;
        };
        let (kind, storage_identity) = match valid_migrated_identity(index_uid, document) {
            Some((_, kind, storage_identity, _)) => (kind, storage_identity),
            None => {
                let Some(kind) = persistence_kind(index_uid, document) else {
                    continue;
                };
                let Some(logical_id) = legacy_logical_id(index_uid, document, id) else {
                    continue;
                };
                let Some(storage_identity) =
                    document_storage_identity(index_uid, document, &logical_id)
                else {
                    continue;
                };
                (kind, storage_identity)
            }
        };
        checksums
            .entry((id.to_string(), kind, storage_identity))
            .or_default()
            .insert(checksum_value(document));
    }
    checksums
}

fn legacy_logical_id(index_uid: &str, document: &Value, legacy_id: &str) -> Option<String> {
    if index_uid == "rag_company_context" {
        return string_field(document, "uri")
            .filter(|uri| !uri.trim().is_empty())
            .map(ToString::to_string);
    }
    Some(legacy_id.to_string())
}

fn upsert_by_string_id(documents: &mut Vec<Value>, replacement: Value) {
    let Some(replacement_id) = string_field(&replacement, "id").map(ToString::to_string) else {
        return;
    };
    if let Some(position) = documents
        .iter()
        .position(|document| string_field(document, "id") == Some(replacement_id.as_str()))
    {
        documents[position] = replacement;
    } else {
        documents.push(replacement);
    }
}

fn string_field<'a>(document: &'a Value, field: &str) -> Option<&'a str> {
    document.get(field).and_then(Value::as_str)
}

fn document_sort_key(document: &Value) -> (String, String) {
    (
        string_field(document, "id").unwrap_or_default().to_string(),
        checksum_value(document),
    )
}

fn compute_plan_checksum(plan: &MigrationPlan) -> Result<String> {
    let mut value = serde_json::to_value(plan)?;
    value["plan_checksum"] = Value::String(String::new());
    Ok(checksum_value(&value))
}

fn compute_rollback_checksum(rollback: &RollbackPlan) -> Result<String> {
    let mut value = serde_json::to_value(rollback)?;
    value["rollback_checksum"] = Value::String(String::new());
    Ok(checksum_value(&value))
}

fn checksum_serializable<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    Ok(checksum_value(&serde_json::to_value(value)?))
}

fn mapping_checksum(mapping: &LegacyTenantMapping) -> Result<String> {
    let mut assignments = mapping.documents.clone();
    assignments.sort();
    checksum_serializable(&json!({
        "migration": &mapping.migration,
        "documents": assignments
    }))
}

pub fn checksum_value(value: &Value) -> String {
    let mut canonical = String::new();
    write_canonical_json(value, &mut canonical);
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

fn checksum_strings(values: &[String]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort();
    checksum_value(&json!(sorted))
}

fn checksum_targets(values: &[(String, String, String, String)]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort();
    checksum_value(&json!(sorted))
}

fn write_canonical_json(value: &Value, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => output.push_str(
            &serde_json::to_string(value).expect("serializing a JSON string cannot fail"),
        ),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical_json(value, output);
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut fields = values.iter().collect::<Vec<_>>();
            fields.sort_by_key(|(key, _)| *key);
            for (index, (key, value)) in fields.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(
                    &serde_json::to_string(key).expect("serializing a JSON object key cannot fail"),
                );
                output.push(':');
                write_canonical_json(value, output);
            }
            output.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use crate::tenant_scope::tenant_document;

    use super::*;

    #[derive(Default)]
    struct FakeBackend {
        indexes: Mutex<BTreeMap<String, Vec<Value>>>,
        writes: AtomicUsize,
    }

    impl FakeBackend {
        fn with_document(index_uid: &str, document: Value) -> Self {
            let backend = Self::default();
            backend.insert(index_uid, document);
            backend
        }

        fn insert(&self, index_uid: &str, document: Value) {
            let id = string_field(&document, "id").unwrap().to_string();
            let mut indexes = self.indexes.lock().unwrap();
            let documents = indexes.entry(index_uid.to_string()).or_default();
            if let Some(position) = documents
                .iter()
                .position(|existing| string_field(existing, "id") == Some(id.as_str()))
            {
                documents[position] = document;
            } else {
                documents.push(document);
            }
        }

        fn push_raw(&self, index_uid: &str, document: Value) {
            self.indexes
                .lock()
                .unwrap()
                .entry(index_uid.to_string())
                .or_default()
                .push(document);
        }

        fn remove(&self, index_uid: &str, id: &str) {
            self.indexes
                .lock()
                .unwrap()
                .entry(index_uid.to_string())
                .or_default()
                .retain(|document| string_field(document, "id") != Some(id));
        }

        fn writes(&self) -> usize {
            self.writes.load(Ordering::SeqCst)
        }

        fn document_count(&self, index_uid: &str) -> usize {
            self.indexes
                .lock()
                .unwrap()
                .get(index_uid)
                .map(Vec::len)
                .unwrap_or_default()
        }

        fn contains_id(&self, index_uid: &str, id: &str) -> bool {
            self.indexes
                .lock()
                .unwrap()
                .get(index_uid)
                .is_some_and(|documents| {
                    documents
                        .iter()
                        .any(|document| string_field(document, "id") == Some(id))
                })
        }
    }

    #[async_trait]
    impl MigrationBackend for FakeBackend {
        async fn fetch_documents_page(
            &self,
            index_uid: &str,
            offset: usize,
            limit: usize,
        ) -> Result<DocumentPage> {
            let indexes = self.indexes.lock().unwrap();
            let mut documents = indexes.get(index_uid).cloned().unwrap_or_default();
            documents.sort_by_key(document_sort_key);
            Ok(DocumentPage {
                results: documents.iter().skip(offset).take(limit).cloned().collect(),
                offset,
                limit,
                total: documents.len(),
            })
        }

        async fn write_documents(&self, index_uid: &str, documents: &[Value]) -> Result<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            for document in documents {
                self.insert(index_uid, document.clone());
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemoryCheckpointStore {
        checkpoint: Option<MigrationCheckpoint>,
        saves: usize,
    }

    impl CheckpointStore for MemoryCheckpointStore {
        fn load(&self) -> Result<Option<MigrationCheckpoint>> {
            Ok(self.checkpoint.clone())
        }

        fn save(&mut self, checkpoint: &MigrationCheckpoint) -> Result<()> {
            self.checkpoint = Some(checkpoint.clone());
            self.saves += 1;
            Ok(())
        }
    }

    fn mapping(assignments: &[(&str, &str, &str)]) -> LegacyTenantMapping {
        LegacyTenantMapping {
            migration: MIGRATION_NAME.to_string(),
            documents: assignments
                .iter()
                .map(|(index_uid, legacy_id, tenant_id)| LegacyTenantAssignment {
                    index_uid: (*index_uid).to_string(),
                    legacy_id: (*legacy_id).to_string(),
                    tenant_id: (*tenant_id).to_string(),
                })
                .collect(),
        }
    }

    async fn one_document_plan(backend: &FakeBackend) -> MigrationPlan {
        create_plan(
            backend,
            &mapping(&[("rag_sources", "source-1", "tenant-a")]),
            2,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn transforms_with_tenant_safe_primary_and_preserves_logical_id() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let plan = one_document_plan(&backend).await;

        assert_eq!(plan.operations.len(), 1);
        let operation = &plan.operations[0];
        assert_eq!(operation.document["tenant_id"], "tenant-a");
        assert_eq!(operation.document["logical_id"], "source-1");
        assert_eq!(operation.document["id"], operation.target_id);
        assert_ne!(operation.target_id, operation.legacy_id);
        assert_eq!(backend.document_count("rag_sources"), 1);
    }

    #[tokio::test]
    async fn company_context_uses_uri_as_public_logical_identity() {
        let backend = FakeBackend::with_document(
            "rag_company_context",
            json!({
                "id": "legacy-context-hash",
                "uri": "ctx://company/handbook",
                "title": "Handbook"
            }),
        );
        let plan = create_plan(
            &backend,
            &mapping(&[("rag_company_context", "legacy-context-hash", "tenant-a")]),
            2,
        )
        .await
        .unwrap();

        let operation = &plan.operations[0];
        assert_eq!(operation.legacy_id, "legacy-context-hash");
        assert_eq!(operation.logical_id, "ctx://company/handbook");
        assert_eq!(operation.document["logical_id"], "ctx://company/handbook");
        assert_eq!(
            operation.target_id,
            persisted_document_id("tenant-a", "rag_company_context", "ctx://company/handbook")
                .unwrap()
        );
    }

    #[tokio::test]
    async fn structured_rows_with_the_same_row_id_use_snapshot_scoped_targets() {
        let backend = FakeBackend::default();
        backend.push_raw(
            "rag_structured_rows",
            json!({
                "id": "row-1",
                "snapshot_id": "snapshot-a",
                "logical_id": "business-a",
                "value": "first"
            }),
        );
        backend.push_raw(
            "rag_structured_rows",
            json!({
                "id": "row-1",
                "snapshot_id": "snapshot-b",
                "logical_id": "business-b",
                "value": "second"
            }),
        );
        let plan = create_plan(
            &backend,
            &mapping(&[("rag_structured_rows", "row-1", "tenant-a")]),
            2,
        )
        .await
        .unwrap();

        assert_eq!(plan.operations.len(), 2);
        assert_eq!(
            plan.indexes["rag_structured_rows"].tenants["tenant-a"].expected_count,
            2
        );
        let first_storage = scoped_storage_identity("snapshot-a", "row-1").unwrap();
        let second_storage = scoped_storage_identity("snapshot-b", "row-1").unwrap();
        let storage_identities = plan
            .operations
            .iter()
            .map(|operation| operation.storage_identity.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            storage_identities,
            BTreeSet::from([first_storage.clone(), second_storage.clone()])
        );
        assert_ne!(plan.operations[0].target_id, plan.operations[1].target_id);
        for operation in &plan.operations {
            assert_eq!(operation.logical_id, "row-1");
            assert_eq!(operation.document["logical_id"], "row-1");
            let restored = restore_logical_id("rag_structured_rows", operation.document.clone());
            assert_eq!(restored["id"], "row-1");
            let expected_business_id = match restored["snapshot_id"].as_str().unwrap() {
                "snapshot-a" => "business-a",
                "snapshot-b" => "business-b",
                snapshot_id => panic!("unexpected snapshot {snapshot_id}"),
            };
            assert_eq!(restored["logical_id"], expected_business_id);
            assert_eq!(
                operation.target_id,
                persisted_document_id(
                    "tenant-a",
                    "rag_structured_rows",
                    &operation.storage_identity
                )
                .unwrap()
            );
        }

        let mut wrong_snapshot = plan.operations[0].document.clone();
        wrong_snapshot["snapshot_id"] = Value::String("different-snapshot".to_string());
        assert!(valid_migrated_identity("rag_structured_rows", &wrong_snapshot).is_none());

        let mut missing_payload_shadow = plan.clone();
        missing_payload_shadow.operations[0]
            .document
            .as_object_mut()
            .unwrap()
            .remove("__nowledge_row_payload_v1");
        missing_payload_shadow.operations[0].target_checksum =
            checksum_value(&missing_payload_shadow.operations[0].document);
        missing_payload_shadow.plan_checksum =
            compute_plan_checksum(&missing_payload_shadow).unwrap();
        assert!(validate_plan(&missing_payload_shadow).is_err());

        let rollback = create_rollback_plan(&plan).unwrap();
        apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut MemoryCheckpointStore::default(),
            false,
        )
        .await
        .unwrap();
        let verification = verify_plan(&backend, &plan).await.unwrap();
        assert!(verification.ready_to_cutover);
        assert_eq!(
            verification.indexes["rag_structured_rows"].tenants["tenant-a"].verified_count,
            2
        );
    }

    #[tokio::test]
    async fn structured_rows_without_a_non_empty_snapshot_are_quarantined() {
        let backend = FakeBackend::default();
        backend.insert("rag_structured_rows", json!({"id": "missing"}));
        backend.insert(
            "rag_structured_rows",
            json!({"id": "empty", "snapshot_id": "  "}),
        );
        let plan = create_plan(
            &backend,
            &mapping(&[
                ("rag_structured_rows", "missing", "tenant-a"),
                ("rag_structured_rows", "empty", "tenant-a"),
            ]),
            2,
        )
        .await
        .unwrap();

        assert!(plan.operations.is_empty());
        assert_eq!(plan.quarantined.len(), 2);
        assert!(plan
            .quarantined
            .iter()
            .all(|row| row.reason == "missing_or_non_string_snapshot_id"));
    }

    #[tokio::test]
    async fn parse_artifacts_with_the_same_id_use_owner_scoped_targets_and_replay_idempotently() {
        let backend = FakeBackend::default();
        backend.push_raw(
            "rag_parse_artifacts",
            json!({
                "id": "artifact-1",
                "owner_user_id": "owner-a",
                "artifact_kind": "markdown",
                "uri": "ctx://owner-a/artifact"
            }),
        );
        backend.push_raw(
            "rag_parse_artifacts",
            json!({
                "id": "artifact-1",
                "owner_user_id": "owner-b",
                "artifact_kind": "markdown",
                "uri": "ctx://owner-b/artifact"
            }),
        );
        backend.push_raw(
            "rag_parse_artifacts",
            json!({
                "id": "artifact-1",
                "owner_user_id": null,
                "artifact_kind": "markdown",
                "uri": "ctx://company/artifact"
            }),
        );
        let tenant_mapping = mapping(&[("rag_parse_artifacts", "artifact-1", "tenant-a")]);
        let plan = create_plan(&backend, &tenant_mapping, 2).await.unwrap();

        assert_eq!(plan.operations.len(), 3);
        assert_eq!(
            plan.indexes["rag_parse_artifacts"].tenants["tenant-a"].expected_count,
            3
        );
        let expected_storage_identities = BTreeSet::from([
            owner_scoped_storage_identity(Some("owner-a"), "artifact-1").unwrap(),
            owner_scoped_storage_identity(Some("owner-b"), "artifact-1").unwrap(),
            owner_scoped_storage_identity(None, "artifact-1").unwrap(),
        ]);
        let storage_identities = plan
            .operations
            .iter()
            .map(|operation| operation.storage_identity.clone())
            .collect::<BTreeSet<_>>();
        let target_ids = plan
            .operations
            .iter()
            .map(|operation| operation.target_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(storage_identities, expected_storage_identities);
        assert_eq!(target_ids.len(), 3);
        for operation in &plan.operations {
            assert_eq!(operation.logical_id, "artifact-1");
            assert_eq!(operation.document["logical_id"], "artifact-1");
            assert_eq!(
                operation.target_id,
                persisted_document_id(
                    "tenant-a",
                    "rag_parse_artifacts",
                    &operation.storage_identity
                )
                .unwrap()
            );
            assert!(valid_migrated_identity("rag_parse_artifacts", &operation.document).is_some());
        }
        let mut wrong_owner = plan
            .operations
            .iter()
            .find(|operation| operation.document["owner_user_id"] == "owner-a")
            .unwrap()
            .document
            .clone();
        wrong_owner["owner_user_id"] = Value::String("owner-b".to_string());
        assert!(valid_migrated_identity("rag_parse_artifacts", &wrong_owner).is_none());

        let rollback = create_rollback_plan(&plan).unwrap();
        let mut checkpoints = MemoryCheckpointStore::default();
        apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();
        assert_eq!(backend.document_count("rag_parse_artifacts"), 6);

        checkpoints.checkpoint = None;
        apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();
        assert_eq!(backend.document_count("rag_parse_artifacts"), 6);

        let verification = verify_plan(&backend, &plan).await.unwrap();
        assert!(verification.ready_to_cutover);
        assert_eq!(
            verification.indexes["rag_parse_artifacts"].tenants["tenant-a"].verified_count,
            3
        );

        let idempotent_plan = create_plan(&backend, &tenant_mapping, 2).await.unwrap();
        assert!(idempotent_plan.operations.is_empty());
        assert_eq!(
            idempotent_plan.indexes["rag_parse_artifacts"].already_migrated_count,
            3
        );
    }

    #[tokio::test]
    async fn parse_artifacts_with_invalid_owner_scopes_are_quarantined() {
        let backend = FakeBackend::default();
        backend.insert(
            "rag_parse_artifacts",
            json!({"id": "blank-owner", "owner_user_id": "  "}),
        );
        backend.insert(
            "rag_parse_artifacts",
            json!({"id": "numeric-owner", "owner_user_id": 42}),
        );
        let plan = create_plan(
            &backend,
            &mapping(&[
                ("rag_parse_artifacts", "blank-owner", "tenant-a"),
                ("rag_parse_artifacts", "numeric-owner", "tenant-a"),
            ]),
            2,
        )
        .await
        .unwrap();

        assert!(plan.operations.is_empty());
        assert_eq!(plan.quarantined.len(), 2);
        assert!(plan
            .quarantined
            .iter()
            .all(|row| row.reason == "invalid_owner_user_id_scope"));
    }

    #[test]
    fn harness_component_and_revision_kinds_do_not_collide_on_the_same_logical_id() {
        let component = tenant_document(
            "tenant-a",
            "rag_harness_components:component",
            "shared-id",
            &json!({"doc_kind": "component", "name": "component"}),
        )
        .unwrap();
        let revision = tenant_document(
            "tenant-a",
            "rag_harness_components:revision",
            "shared-id",
            &json!({"doc_kind": "revision", "name": "revision"}),
        )
        .unwrap();

        assert_ne!(component["id"], revision["id"]);
        assert_eq!(
            valid_migrated_identity("rag_harness_components", &component),
            Some((
                "tenant-a".to_string(),
                "rag_harness_components:component".to_string(),
                "shared-id".to_string(),
                "shared-id".to_string()
            ))
        );
        assert_eq!(
            valid_migrated_identity("rag_harness_components", &revision),
            Some((
                "tenant-a".to_string(),
                "rag_harness_components:revision".to_string(),
                "shared-id".to_string(),
                "shared-id".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn harness_same_id_component_and_revision_plan_apply_and_verify_distinct_targets() {
        let backend = FakeBackend::default();
        backend.push_raw(
            "rag_harness_components",
            json!({"id": "shared-id", "doc_kind": "component", "name": "component"}),
        );
        backend.push_raw(
            "rag_harness_components",
            json!({"id": "shared-id", "doc_kind": "revision", "name": "revision"}),
        );
        let plan = create_plan(
            &backend,
            &mapping(&[("rag_harness_components", "shared-id", "tenant-a")]),
            2,
        )
        .await
        .unwrap();

        assert_eq!(plan.operations.len(), 2);
        assert_ne!(plan.operations[0].target_id, plan.operations[1].target_id);
        let inventory = &plan.indexes["rag_harness_components"];
        assert_eq!(inventory.planned_count, 2);
        assert_eq!(inventory.tenants["tenant-a"].expected_count, 2);

        let rollback = create_rollback_plan(&plan).unwrap();
        apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut MemoryCheckpointStore::default(),
            false,
        )
        .await
        .unwrap();
        let verification = verify_plan(&backend, &plan).await.unwrap();
        assert!(verification.ready_to_cutover);
        assert_eq!(
            verification.indexes["rag_harness_components"].tenants["tenant-a"].verified_count,
            2
        );
    }

    #[tokio::test]
    async fn missing_and_ambiguous_explicit_mappings_are_quarantined() {
        let backend = FakeBackend::default();
        backend.insert("rag_sources", json!({"id": "missing"}));
        backend.insert("rag_sources", json!({"id": "ambiguous"}));
        let mapping = mapping(&[
            ("rag_sources", "ambiguous", "tenant-a"),
            ("rag_sources", "ambiguous", "tenant-b"),
        ]);

        let plan = create_plan(&backend, &mapping, 2).await.unwrap();

        assert!(plan.operations.is_empty());
        assert_eq!(plan.quarantined.len(), 2);
        assert!(plan
            .quarantined
            .iter()
            .any(|row| row.reason == "missing_explicit_tenant_mapping"));
        assert!(plan
            .quarantined
            .iter()
            .any(|row| row.reason == "ambiguous_explicit_tenant_mapping"));
    }

    #[test]
    fn canonical_checksum_is_deterministic_across_object_key_order() {
        let left = serde_json::from_str::<Value>(r#"{"b":2,"a":{"y":1,"x":0}}"#).unwrap();
        let right = serde_json::from_str::<Value>(r#"{"a":{"x":0,"y":1},"b":2}"#).unwrap();
        assert_eq!(checksum_value(&left), checksum_value(&right));
    }

    #[tokio::test]
    async fn plan_and_inventory_checksums_are_independent_of_mapping_order() {
        let backend = FakeBackend::default();
        backend.insert("rag_sources", json!({"id": "source-2", "title": "Two"}));
        backend.insert("rag_sources", json!({"id": "source-1", "title": "One"}));
        let first = create_plan(
            &backend,
            &mapping(&[
                ("rag_sources", "source-1", "tenant-a"),
                ("rag_sources", "source-2", "tenant-a"),
            ]),
            1,
        )
        .await
        .unwrap();
        let second = create_plan(
            &backend,
            &mapping(&[
                ("rag_sources", "source-2", "tenant-a"),
                ("rag_sources", "source-1", "tenant-a"),
            ]),
            1,
        )
        .await
        .unwrap();

        assert_eq!(first.mapping_checksum, second.mapping_checksum);
        assert_eq!(first.plan_checksum, second.plan_checksum);
        assert_eq!(first.indexes, second.indexes);
    }

    #[tokio::test]
    async fn apply_dry_run_is_mutation_free_and_does_not_checkpoint() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let plan = one_document_plan(&backend).await;
        let rollback = create_rollback_plan(&plan).unwrap();
        let mut checkpoints = MemoryCheckpointStore::default();

        let rejected = apply_plan(
            &backend,
            &plan,
            &rollback,
            "wrong acknowledgement",
            &mut checkpoints,
            false,
        )
        .await;
        assert!(rejected.is_err());
        assert_eq!(backend.writes(), 0);

        let report = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            true,
        )
        .await
        .unwrap();

        assert!(report.mutation_free);
        assert_eq!(report.remote_batches_written, 0);
        assert_eq!(backend.writes(), 0);
        assert_eq!(checkpoints.saves, 0);
        assert_eq!(backend.document_count("rag_sources"), 1);
    }

    #[tokio::test]
    async fn apply_rejects_a_conflicting_target_created_after_planning_before_any_write() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let plan = one_document_plan(&backend).await;
        let rollback = create_rollback_plan(&plan).unwrap();
        let mut conflict = plan.operations[0].document.clone();
        conflict["title"] = Value::String("Late conflicting target".to_string());
        backend.insert("rag_sources", conflict);
        let mut checkpoints = MemoryCheckpointStore::default();

        let error = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .expect_err("late conflicting target must abort apply");

        assert!(error
            .to_string()
            .contains("changed after the plan snapshot"));
        assert_eq!(backend.writes(), 0);
        assert_eq!(checkpoints.saves, 0);
    }

    #[tokio::test]
    async fn apply_rejects_a_prior_target_deleted_after_correction_planning() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let prior_target = tenant_document(
            "tenant-a",
            "rag_sources",
            "source-1",
            &json!({"title": "Prior migrated target"}),
        )
        .unwrap();
        let target_id = prior_target["id"].as_str().unwrap().to_string();
        backend.insert("rag_sources", prior_target);
        let plan = one_document_plan(&backend).await;
        assert_eq!(plan.operations.len(), 1);
        assert!(plan.operations[0].previous_target_checksum.is_some());
        let rollback = create_rollback_plan(&plan).unwrap();
        backend.remove("rag_sources", &target_id);
        let mut checkpoints = MemoryCheckpointStore::default();

        let error = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .expect_err("missing prior correction target must abort apply");

        assert!(error
            .to_string()
            .contains("changed after the plan snapshot"));
        assert_eq!(backend.writes(), 0);
        assert_eq!(checkpoints.saves, 0);
    }

    #[tokio::test]
    async fn checkpointed_apply_is_restartable_and_lost_checkpoint_replay_is_idempotent() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let plan = one_document_plan(&backend).await;
        let rollback = create_rollback_plan(&plan).unwrap();
        let mut checkpoints = MemoryCheckpointStore::default();

        let first = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();
        let second = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();

        assert_eq!(first.remote_batches_written, 1);
        assert_eq!(second.remote_batches_written, 0);
        assert_eq!(backend.writes(), 1);
        assert_eq!(backend.document_count("rag_sources"), 2);

        checkpoints.checkpoint = None;
        let replay = apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();
        assert_eq!(replay.remote_batches_written, 1);
        assert_eq!(backend.writes(), 2);
        assert_eq!(backend.document_count("rag_sources"), 2);
        assert!(verify_plan(&backend, &plan).await.unwrap().ready_to_cutover);
    }

    #[tokio::test]
    async fn replanning_skips_an_identical_migrated_copy_and_corrects_a_changed_copy() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let first_plan = one_document_plan(&backend).await;
        let rollback = create_rollback_plan(&first_plan).unwrap();
        apply_plan(
            &backend,
            &first_plan,
            &rollback,
            &rollback.acknowledgement,
            &mut MemoryCheckpointStore::default(),
            false,
        )
        .await
        .unwrap();

        let no_op_plan = one_document_plan(&backend).await;
        let inventory = &no_op_plan.indexes["rag_sources"];
        assert!(no_op_plan.operations.is_empty());
        assert_eq!(inventory.planned_count, 0);
        assert_eq!(inventory.already_migrated_count, 1);
        assert_eq!(inventory.tenants["tenant-a"].expected_count, 1);

        backend.insert(
            "rag_sources",
            json!({
                "id": first_plan.operations[0].target_id,
                "logical_id": "source-1",
                "tenant_id": "tenant-a",
                "title": "Changed migrated copy"
            }),
        );
        let correction_plan = one_document_plan(&backend).await;
        assert_eq!(correction_plan.operations.len(), 1);
        assert_eq!(
            correction_plan.operations[0].target_id,
            first_plan.operations[0].target_id
        );
        assert_eq!(correction_plan.indexes["rag_sources"].planned_count, 1);
        assert_eq!(
            correction_plan.indexes["rag_sources"].already_migrated_count,
            0
        );
        assert!(correction_plan.operations[0]
            .previous_target_document
            .is_some());
        let correction_rollback = create_rollback_plan(&correction_plan).unwrap();
        assert_eq!(
            correction_rollback.actions[0].action,
            "restore_previous_migrated_copy_after_traffic_rollback"
        );
        assert!(correction_rollback.actions[0].restore_document.is_some());
    }

    #[tokio::test]
    async fn verification_includes_preexisting_migrated_targets_in_tenant_inventory() {
        let target = tenant_document(
            "tenant-a",
            "rag_sources",
            "source-1",
            &json!({"title": "Existing migrated copy"}),
        )
        .unwrap();
        let target_id = target["id"].as_str().unwrap().to_string();
        let backend = FakeBackend::with_document("rag_sources", target.clone());
        let plan = create_plan(&backend, &mapping(&[]), 2).await.unwrap();

        assert!(plan.operations.is_empty());
        assert_eq!(plan.indexes["rag_sources"].already_migrated_count, 1);
        assert!(verify_plan(&backend, &plan).await.unwrap().ready_to_cutover);

        let mut changed = target;
        changed["title"] = Value::String("Changed after plan".to_string());
        backend.insert("rag_sources", changed);
        let verification = verify_plan(&backend, &plan).await.unwrap();
        assert!(!verification.writes_verified);
        assert!(!verification.indexes["rag_sources"].snapshot_match);
        assert_ne!(
            verification.indexes["rag_sources"].tenants["tenant-a"].expected_checksum,
            verification.indexes["rag_sources"].tenants["tenant-a"].observed_checksum
        );
        assert!(backend.contains_id("rag_sources", &target_id));
    }

    #[tokio::test]
    async fn verification_checks_migrated_checksum_and_legacy_preservation() {
        let backend =
            FakeBackend::with_document("rag_sources", json!({"id": "source-1", "title": "Legacy"}));
        let plan = one_document_plan(&backend).await;
        let rollback = create_rollback_plan(&plan).unwrap();
        let mut checkpoints = MemoryCheckpointStore::default();
        apply_plan(
            &backend,
            &plan,
            &rollback,
            &rollback.acknowledgement,
            &mut checkpoints,
            false,
        )
        .await
        .unwrap();

        let verified = verify_plan(&backend, &plan).await.unwrap();
        assert!(verified.ready_to_cutover);
        assert!(verified.writes_verified);
        assert!(verified.legacy_rows_preserved);

        backend.insert(
            "rag_sources",
            json!({"id": plan.operations[0].target_id, "title": "tampered"}),
        );
        backend.remove("rag_sources", "source-1");
        let failed = verify_plan(&backend, &plan).await.unwrap();
        assert!(!failed.ready_to_cutover);
        assert!(!failed.writes_verified);
        assert!(!failed.legacy_rows_preserved);
    }
}
