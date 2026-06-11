# API Documentation

This directory contains one standalone English document for every HTTP API registered in `src/routes.rs::build_router`.

Total documented APIs: 68.

Each endpoint file includes request parameters, response fields, access rules, and a Mermaid internal logic call graph.

## Retrieval and Source Documents

Long documents are stored as non-retrieval `SourceDocument` records and fragmented into active `ContextNode` fragments. Default context search and RAG retrieval only search active fragments with `retrieval_enabled=true`, `retrieval_role=fragment`, and `status=active`; raw source documents are kept out of default retrieval.

Document matching is hybrid. Every fragment and every saved source document is embedded into a [turbovec](https://github.com/RyanCodrai/turbovec) quantized vector index at save time (`src/vector_match.rs`); searches blend the legacy lexical substring score with fragment-level vector similarity and document-level vector evidence from the fragment's full source document. Inflected or reordered queries that contain no exact substring can therefore still match, and document-level evidence boosts fragments that already match on their own — it never admits a fragment by itself, so raw source document bodies stay out of default retrieval. Vector scoring is allowlist-restricted to the caller's isolation-filtered candidate set, so per-user scope is enforced before any vector search runs. Tune with `RAG_VECTOR_MATCH_ENABLED`, `RAG_VECTOR_MATCH_WEIGHT`, `RAG_VECTOR_MATCH_DOC_WEIGHT`, and `RAG_VECTOR_MATCH_MIN_SCORE`.

`POST /v1/context/search` has three response profiles:

- `compact` returns minimal hit fields for lightweight callers.
- `standard` is the default and returns source/location/block provenance plus source groups.
- `full` adds source summaries and active `part_of` links; `include: ["links"]` also adds up to 5 non-`part_of` related links per hit.

Supported context-search include values are `traceback`, `links`, `neighbor_fragments`, `source_summary`, `artifact_refs`, `score_breakdown`, and `raw_stage_debug`. Supported structured filters include `source_id`, `revision_id`, `source_document_uri`, `block_type`, `page_idx`, `page_idx_gte`, `page_idx_lte`, `section_path_contains`, and `artifact_kind`.

Use `include: ["traceback"]`, `POST /v1/context/traceback`, or explicit `GET /v1/fs/read` with normal ACL checks to trace a fragment back to its full source document. Search traceback returns source metadata only; it never includes the full source document body.

`/v1/rag/answer` citations preserve the same source/location provenance as context hits, including `source_document_uri`, `page_idx`, `bbox`, `block_type`, `section_path`, parser artifact references, fragment offsets, and checksums when available.

`ContextNode.node_kind` is one of `source_doc`, `fragment`, `abstract`, or `overview`. `ContextNode.retrieval_role` is one of `none`, `fragment`, or `overview`. Source documents are stored in `rag_source_documents` with `retrieval_enabled=false` by default.

## Endpoint Index

| Method | Path | Handler | Document |
| --- | --- | --- | --- |
| `GET` | `/healthz` | `healthz` | [api/get_healthz.md](api/get_healthz.md) |
| `GET` | `/livez` | `livez` | [api/get_livez.md](api/get_livez.md) |
| `GET` | `/readyz` | `readyz` | [api/get_readyz.md](api/get_readyz.md) |
| `POST` | `/v1/admin/bootstrap` | `bootstrap` | [api/post_v1_admin_bootstrap.md](api/post_v1_admin_bootstrap.md) |
| `POST` | `/v1/admin/harness/evolution/changes/{change_id}/compare` | `compare_harness_change` | [api/post_v1_admin_harness_evolution_changes_change_id_compare.md](api/post_v1_admin_harness_evolution_changes_change_id_compare.md) |
| `GET` | `/v1/admin/harness/evolution/changes/{change_id}/delta` | `get_harness_change_delta` | [api/get_v1_admin_harness_evolution_changes_change_id_delta.md](api/get_v1_admin_harness_evolution_changes_change_id_delta.md) |
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
| `POST` | `/v1/ingest/files:sync` | `ingest_file_sync` | [api/post_v1_ingest_files_sync.md](api/post_v1_ingest_files_sync.md) |
| `POST` | `/v1/ingest/tasks` | `create_ingest_task` | [api/post_v1_ingest_tasks.md](api/post_v1_ingest_tasks.md) |
| `GET` | `/v1/ingest/tasks/{task_id}` | `get_ingest_task` | [api/get_v1_ingest_tasks_task_id.md](api/get_v1_ingest_tasks_task_id.md) |
| `GET` | `/v1/ingest/tasks/{task_id}/result` | `get_ingest_task_result` | [api/get_v1_ingest_tasks_task_id_result.md](api/get_v1_ingest_tasks_task_id_result.md) |
| `POST` | `/v1/ingest/uploads` | `create_ingest_upload` | [api/post_v1_ingest_uploads.md](api/post_v1_ingest_uploads.md) |
| `POST` | `/v1/ingest/uploads:sync` | `ingest_upload_sync` | [api/post_v1_ingest_uploads_sync.md](api/post_v1_ingest_uploads_sync.md) |
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
