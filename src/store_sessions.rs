use super::*;

impl Store {
    pub fn create_session(
        &self,
        tenant_id: &str,
        req: SessionCreateRequest,
    ) -> Result<SessionResponse, ApiError> {
        let owner_user_id = require_string(req.owner_user_id, "owner_user_id")?;
        let session = SessionRecord {
            id: new_id("session"),
            tenant_id: tenant_id.to_string(),
            owner_user_id,
            title: req.title.unwrap_or_else(|| "Untitled session".to_string()),
            status: "active".to_string(),
            messages: Vec::new(),
            created_at: now(),
        };
        let mut data = self.write()?;
        data.sessions.insert(session.id.clone(), session.clone());
        Ok(SessionResponse {
            session_id: session.id,
            status: "active".to_string(),
        })
    }

    pub fn add_session_message(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionMessageRequest,
    ) -> Result<Value, ApiError> {
        let role = require_string(req.role, "role")?;
        let content = require_string(req.content, "content")?;
        let owner_user_id = {
            let mut data = self.write()?;
            let session = data
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            session.messages.push(json!({
                "role": role,
                "content": content,
                "created_at": now()
            }));
            session.owner_user_id.clone()
        };

        let history_event_id = if req.write_history_event {
            Some(
                self.append_internal_event(
                    tenant_id,
                    &owner_user_id,
                    "session.message",
                    "session",
                    session_id,
                    content,
                    json!({ "role": role }),
                )?
                .event
                .id,
            )
        } else {
            None
        };

        Ok(json!({
            "session_id": session_id,
            "history_event_id": history_event_id
        }))
    }

    pub fn commit_session(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionCommitRequest,
    ) -> Result<SessionCommitResponse, ApiError> {
        let session = {
            let mut data = self.write()?;
            let session = data
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            session.status = "archived".to_string();
            session.clone()
        };

        let archive_uri = if req.archive_context {
            let routing = self
                .resolver
                .resolve(tenant_id, &session.owner_user_id, false, true)?;
            let uri = format!(
                "ctx://session/{}/history/archive_0001",
                sanitize_slug(session_id)
            );
            let mut data = self.write()?;
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default()
                .push(ContextNode {
                    uri: uri.clone(),
                    title: session.title.clone(),
                    layer: 2,
                    body: serde_json::to_string(&session.messages).unwrap_or_default(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: Some(session.owner_user_id.clone()),
                    index_uid: routing.personal_context_index_uid,
                    index_kind: "personal".to_string(),
                    ancestor_uris: ancestor_uris(&uri),
                    node_kind: "source_doc".to_string(),
                    retrieval_role: "none".to_string(),
                    retrieval_enabled: false,
                    parent_uri: None,
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
                    status: "active".to_string(),
                    privacy: "private".to_string(),
                    updated_at: now(),
                });
            Some(uri)
        } else {
            None
        };

        let history = self.append_internal_event(
            tenant_id,
            &session.owner_user_id,
            "session.committed",
            "session",
            session_id,
            format!("Session {} committed", session.title),
            json!({ "archive_uri": archive_uri }),
        )?;

        Ok(SessionCommitResponse {
            session_id: session_id.to_string(),
            archive_uri,
            history_event_ids: vec![history.event.id],
            insight_candidate_ids: if req.extract_insights {
                vec![new_id("candidate")]
            } else {
                Vec::new()
            },
            memory_diff_ids: Vec::new(),
        })
    }
}

impl Store {
    pub async fn commit_session_async(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionCommitRequest,
    ) -> Result<SessionCommitResponse, ApiError> {
        let owner = self.session_owner_id(session_id)?;
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "session.commit",
                Some(&owner),
                None,
                MutationPrimary::Session,
                |staged| staged.commit_session(tenant_id, session_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn add_session_message_async(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionMessageRequest,
    ) -> Result<Value, ApiError> {
        let owner = self.session_owner_id(session_id)?;
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "session.message.add",
                Some(&owner),
                None,
                MutationPrimary::Session,
                |staged| staged.add_session_message(tenant_id, session_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn create_session_async(
        &self,
        tenant_id: &str,
        req: SessionCreateRequest,
    ) -> Result<SessionResponse, ApiError> {
        let owner = require_string(req.owner_user_id.clone(), "owner_user_id")?;
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "session.create",
                Some(&owner),
                None,
                MutationPrimary::Session,
                |staged| staged.create_session(tenant_id, req),
            )
            .await?;
        Ok(response)
    }
}
