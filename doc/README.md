# API Documentation

This directory contains one standalone English document for every HTTP API registered in `src/routes.rs::build_router`.

Total documented APIs: 60.

Each endpoint file includes request parameters, response fields, access rules, and a Mermaid internal logic call graph.

## Retrieval and Source Documents

Long documents are stored as non-retrieval `SourceDocument` records and fragmented into active `ContextNode` fragments. Default context search and RAG retrieval only search active fragments with `retrieval_enabled=true`, `retrieval_role=fragment`, and `status=active`; raw source documents are kept out of default retrieval.

Use `POST /v1/context/traceback` or explicit `GET /v1/fs/read` with normal ACL checks to trace a fragment back to its full source document.

`ContextNode.node_kind` is one of `source_doc`, `fragment`, `abstract`, or `overview`. `ContextNode.retrieval_role` is one of `none`, `fragment`, or `overview`. Source documents are stored in `rag_source_documents` with `retrieval_enabled=false` by default.

## Endpoint Index

| Method | Path | Handler | Document |
| --- | --- | --- | --- |
| `GET` | `/healthz` | `healthz` | [api/get_healthz.md](api/get_healthz.md) |
| `GET` | `/livez` | `livez` | [api/get_livez.md](api/get_livez.md) |
| `GET` | `/readyz` | `readyz` | [api/get_readyz.md](api/get_readyz.md) |
| `POST` | `/v1/admin/bootstrap` | `bootstrap` | [api/post_v1_admin_bootstrap.md](api/post_v1_admin_bootstrap.md) |
| `GET` | `/v1/admin/history/user-event-indexes` | `list_user_event_indexes` | [api/get_v1_admin_history_user_event_indexes.md](api/get_v1_admin_history_user_event_indexes.md) |
| `POST` | `/v1/admin/history/user-event-indexes:reconcile` | `reconcile_user_event_indexes` | [api/post_v1_admin_history_user_event_indexes_reconcile.md](api/post_v1_admin_history_user_event_indexes_reconcile.md) |
| `POST` | `/v1/analysis/insights` | `analyze_insights` | [api/post_v1_analysis_insights.md](api/post_v1_analysis_insights.md) |
| `POST` | `/v1/context/reveal` | `context_reveal` | [api/post_v1_context_reveal.md](api/post_v1_context_reveal.md) |
| `POST` | `/v1/context/search` | `context_search` | [api/post_v1_context_search.md](api/post_v1_context_search.md) |
| `POST` | `/v1/context/traceback` | `context_traceback` | [api/post_v1_context_traceback.md](api/post_v1_context_traceback.md) |
| `POST` | `/v1/debug/meili/search` | `debug_meili_search` | [api/post_v1_debug_meili_search.md](api/post_v1_debug_meili_search.md) |
| `POST` | `/v1/debug/prompt/preview` | `prompt_preview` | [api/post_v1_debug_prompt_preview.md](api/post_v1_debug_prompt_preview.md) |
| `GET` | `/v1/debug/traces/{trace_id}` | `get_trace` | [api/get_v1_debug_traces_trace_id.md](api/get_v1_debug_traces_trace_id.md) |
| `GET` | `/v1/fs/abstract` | `fs_abstract` | [api/get_v1_fs_abstract.md](api/get_v1_fs_abstract.md) |
| `GET` | `/v1/fs/ls` | `fs_ls` | [api/get_v1_fs_ls.md](api/get_v1_fs_ls.md) |
| `GET` | `/v1/fs/overview` | `fs_overview` | [api/get_v1_fs_overview.md](api/get_v1_fs_overview.md) |
| `GET` | `/v1/fs/read` | `fs_read` | [api/get_v1_fs_read.md](api/get_v1_fs_read.md) |
| `GET` | `/v1/fs/tree` | `fs_tree` | [api/get_v1_fs_tree.md](api/get_v1_fs_tree.md) |
| `GET` | `/v1/history/company-docs/{source_id}/revisions` | `list_revisions` | [api/get_v1_history_company_docs_source_id_revisions.md](api/get_v1_history_company_docs_source_id_revisions.md) |
| `POST` | `/v1/history/events` | `append_event_alias` | [api/post_v1_history_events.md](api/post_v1_history_events.md) |
| `GET` | `/v1/history/events/{event_id}` | `get_event_alias` | [api/get_v1_history_events_event_id.md](api/get_v1_history_events_event_id.md) |
| `POST` | `/v1/history/events:bulk` | `append_events_bulk_alias` | [api/post_v1_history_events_bulk.md](api/post_v1_history_events_bulk.md) |
| `GET` | `/v1/history/insights/{insight_id}/events` | `insight_events` | [api/get_v1_history_insights_insight_id_events.md](api/get_v1_history_insights_insight_id_events.md) |
| `POST` | `/v1/history/search` | `search_events_alias` | [api/post_v1_history_search.md](api/post_v1_history_search.md) |
| `POST` | `/v1/history/structured/snapshots` | `create_snapshot` | [api/post_v1_history_structured_snapshots.md](api/post_v1_history_structured_snapshots.md) |
| `GET` | `/v1/history/structured/snapshots/{snapshot_id}` | `get_snapshot` | [api/get_v1_history_structured_snapshots_snapshot_id.md](api/get_v1_history_structured_snapshots_snapshot_id.md) |
| `GET` | `/v1/history/structured/snapshots/{snapshot_id}/rows` | `list_rows` | [api/get_v1_history_structured_snapshots_snapshot_id_rows.md](api/get_v1_history_structured_snapshots_snapshot_id_rows.md) |
| `POST` | `/v1/history/structured/snapshots/{snapshot_id}/rows:bulk` | `bulk_rows` | [api/post_v1_history_structured_snapshots_snapshot_id_rows_bulk.md](api/post_v1_history_structured_snapshots_snapshot_id_rows_bulk.md) |
| `POST` | `/v1/history/timeline` | `timeline_alias` | [api/post_v1_history_timeline.md](api/post_v1_history_timeline.md) |
| `GET` | `/v1/history/users/{owner_user_id}/event-index` | `get_user_event_index` | [api/get_v1_history_users_owner_user_id_event_index.md](api/get_v1_history_users_owner_user_id_event_index.md) |
| `PUT` | `/v1/history/users/{owner_user_id}/event-index` | `ensure_user_event_index` | [api/put_v1_history_users_owner_user_id_event_index.md](api/put_v1_history_users_owner_user_id_event_index.md) |
| `POST` | `/v1/history/users/{owner_user_id}/events` | `append_user_event` | [api/post_v1_history_users_owner_user_id_events.md](api/post_v1_history_users_owner_user_id_events.md) |
| `GET` | `/v1/history/users/{owner_user_id}/events/{event_id}` | `get_user_event` | [api/get_v1_history_users_owner_user_id_events_event_id.md](api/get_v1_history_users_owner_user_id_events_event_id.md) |
| `POST` | `/v1/history/users/{owner_user_id}/events:bulk` | `append_user_events_bulk` | [api/post_v1_history_users_owner_user_id_events_bulk.md](api/post_v1_history_users_owner_user_id_events_bulk.md) |
| `POST` | `/v1/history/users/{owner_user_id}/search` | `search_user_events` | [api/post_v1_history_users_owner_user_id_search.md](api/post_v1_history_users_owner_user_id_search.md) |
| `POST` | `/v1/history/users/{owner_user_id}/timeline` | `user_timeline` | [api/post_v1_history_users_owner_user_id_timeline.md](api/post_v1_history_users_owner_user_id_timeline.md) |
| `POST` | `/v1/links` | `upsert_link` | [api/post_v1_links.md](api/post_v1_links.md) |
| `POST` | `/v1/links/search` | `search_links` | [api/post_v1_links_search.md](api/post_v1_links_search.md) |
| `GET` | `/v1/llm/status` | `llm_status` | [api/get_v1_llm_status.md](api/get_v1_llm_status.md) |
| `POST` | `/v1/llm/test` | `llm_test` | [api/post_v1_llm_test.md](api/post_v1_llm_test.md) |
| `POST` | `/v1/rag/answer` | `rag_answer` | [api/post_v1_rag_answer.md](api/post_v1_rag_answer.md) |
| `POST` | `/v1/rag/debug` | `rag_debug` | [api/post_v1_rag_debug.md](api/post_v1_rag_debug.md) |
| `POST` | `/v1/rag/stream` | `rag_stream` | [api/post_v1_rag_stream.md](api/post_v1_rag_stream.md) |
| `POST` | `/v1/sessions` | `create_session` | [api/post_v1_sessions.md](api/post_v1_sessions.md) |
| `POST` | `/v1/sessions/{session_id}/commit` | `commit_session` | [api/post_v1_sessions_session_id_commit.md](api/post_v1_sessions_session_id_commit.md) |
| `POST` | `/v1/sessions/{session_id}/messages` | `add_session_message` | [api/post_v1_sessions_session_id_messages.md](api/post_v1_sessions_session_id_messages.md) |
| `POST` | `/v1/state/company-docs/preflight` | `preflight_doc` | [api/post_v1_state_company_docs_preflight.md](api/post_v1_state_company_docs_preflight.md) |
| `POST` | `/v1/state/company-docs/{source_id}/revisions` | `create_revision` | [api/post_v1_state_company_docs_source_id_revisions.md](api/post_v1_state_company_docs_source_id_revisions.md) |
| `POST` | `/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate` | `activate_revision` | [api/post_v1_state_company_docs_source_id_revisions_revision_id_activate.md](api/post_v1_state_company_docs_source_id_revisions_revision_id_activate.md) |
| `POST` | `/v1/state/insights` | `upsert_insight` | [api/post_v1_state_insights.md](api/post_v1_state_insights.md) |
| `POST` | `/v1/state/insights/search` | `search_insights` | [api/post_v1_state_insights_search.md](api/post_v1_state_insights_search.md) |
| `PATCH` | `/v1/state/insights/{insight_id}` | `patch_insight` | [api/patch_v1_state_insights_insight_id.md](api/patch_v1_state_insights_insight_id.md) |
| `GET` | `/v1/state/profile/facts/{fact_key}` | `get_state_fact` | [api/get_v1_state_profile_facts_fact_key.md](api/get_v1_state_profile_facts_fact_key.md) |
| `PATCH` | `/v1/state/profile/facts/{fact_key}` | `patch_state_fact` | [api/patch_v1_state_profile_facts_fact_key.md](api/patch_v1_state_profile_facts_fact_key.md) |
| `PUT` | `/v1/state/profile/facts/{fact_key}` | `upsert_state_fact` | [api/put_v1_state_profile_facts_fact_key.md](api/put_v1_state_profile_facts_fact_key.md) |
| `POST` | `/v1/state/search` | `search_state` | [api/post_v1_state_search.md](api/post_v1_state_search.md) |
| `GET` | `/v1/state/structured/current` | `current_structured` | [api/get_v1_state_structured_current.md](api/get_v1_state_structured_current.md) |
| `PUT` | `/v1/state/structured/datasets/{dataset_key}` | `upsert_dataset` | [api/put_v1_state_structured_datasets_dataset_key.md](api/put_v1_state_structured_datasets_dataset_key.md) |
| `POST` | `/v1/state/structured/datasets/{dataset_key}/apply-snapshot` | `apply_snapshot` | [api/post_v1_state_structured_datasets_dataset_key_apply_snapshot.md](api/post_v1_state_structured_datasets_dataset_key_apply_snapshot.md) |
| `GET` | `/v1/usage` | `usage` | [api/get_v1_usage.md](api/get_v1_usage.md) |
