use super::*;

use crate::metrics::{StoreMetricsSnapshot, INGEST_STATES, OPERATION_STATUSES};

impl Store {
    pub(crate) fn operational_metrics_snapshot(
        &self,
        tenant_id: &str,
    ) -> Result<StoreMetricsSnapshot, ApiError> {
        let data = self.read()?;
        let mut snapshot = StoreMetricsSnapshot::default();

        for task in data
            .ingest_tasks
            .values()
            .filter(|task| task.tenant_id == tenant_id)
        {
            let index = INGEST_STATES
                .iter()
                .position(|state| *state == task.state)
                .unwrap_or(INGEST_STATES.len() - 1);
            snapshot.ingest_tasks[index] = snapshot.ingest_tasks[index].saturating_add(1);
        }

        for operation in data
            .operations
            .values()
            .filter(|operation| operation.tenant_id == tenant_id)
        {
            let status = match operation.status {
                OperationStatus::Pending => "pending",
                OperationStatus::PrimaryCommitted => "primary_committed",
                OperationStatus::EffectsSubmitted => "effects_submitted",
                OperationStatus::PartiallyFailed => "partially_failed",
                OperationStatus::Completed => "completed",
                OperationStatus::Failed => "failed",
            };
            let index = OPERATION_STATUSES
                .iter()
                .position(|candidate| *candidate == status)
                .expect("operation status labels cover every enum variant");
            snapshot.operations[index] = snapshot.operations[index].saturating_add(1);
        }

        Ok(snapshot)
    }

    pub fn get_trace(&self, tenant_id: &str, trace_id: &str) -> Result<TraceRecord, ApiError> {
        let data = self.read()?;
        data.traces
            .get(trace_id)
            .filter(|trace| trace.tenant_id == tenant_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("trace not found"))
    }

    pub fn trace_owner_id(
        &self,
        tenant_id: &str,
        trace_id: &str,
    ) -> Result<Option<String>, ApiError> {
        let data = self.read()?;
        data.traces
            .get(trace_id)
            .filter(|trace| trace.tenant_id == tenant_id)
            .map(|trace| trace.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("trace not found"))
    }

    pub fn debug_meili_search(
        &self,
        tenant_id: &str,
        index_uid: &str,
        query: &str,
    ) -> Result<Value, ApiError> {
        let data = self.read()?;
        let nodes: Vec<ContextNode> = if index_uid == "rag_company_context" {
            data.company_context
                .iter()
                .filter(|node| node.tenant_id == tenant_id)
                .cloned()
                .collect()
        } else {
            data.personal_context
                .get(index_uid)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|node| node.tenant_id == tenant_id)
                .collect()
        };
        let vector_scores = self.vector_score_map(query, &nodes);
        let doc_scores = self.vector_doc_score_map(query, doc_candidates_locked(&data, &nodes));
        let hits = rank_nodes(nodes.into_iter(), query, 20, &vector_scores, &doc_scores)
            .into_iter()
            .map(|(node, score)| {
                json!({
                    "uri": node.uri,
                    "title": node.title,
                    "layer": node.layer,
                    "score": score,
                    "index_uid": index_uid
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({ "index_uid": index_uid, "hits": hits }))
    }

    pub fn insight_owner(&self, tenant_id: &str, insight_id: &str) -> Result<String, ApiError> {
        let data = self.read()?;
        data.insights
            .get(insight_id)
            .filter(|insight| insight.tenant_id == tenant_id)
            .map(|insight| insight.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("insight not found"))
    }

    pub fn snapshot_owner(&self, tenant_id: &str, snapshot_id: &str) -> Result<String, ApiError> {
        let data = self.read()?;
        data.snapshots
            .get(snapshot_id)
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .map(|snapshot| snapshot.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("snapshot not found"))
    }

    pub fn session_owner_id(&self, session_id: &str) -> Result<String, ApiError> {
        self.session_owner(session_id)?
            .ok_or_else(|| ApiError::not_found("session not found"))
    }
}
