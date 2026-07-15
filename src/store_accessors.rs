use super::*;

impl Store {
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

    pub fn insight_owner(&self, insight_id: &str) -> Result<String, ApiError> {
        let data = self.read()?;
        data.insights
            .get(insight_id)
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
