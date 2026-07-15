use serde::Serialize;
use serde_json::{Map, Value};

use crate::{error::ApiError, util::hmac_hex};

const DOCUMENT_ID_KEY: &[u8] = b"nowledge-tenant-scope-v1-document-id";
const STRUCTURED_ROW_PAYLOAD_FIELD: &str = "__nowledge_row_payload_v1";

pub fn persisted_document_id(
    tenant_id: &str,
    kind: &str,
    logical_id: &str,
) -> Result<String, ApiError> {
    require_non_empty("tenant_id", tenant_id)?;
    require_non_empty("persisted kind", kind)?;
    require_non_empty("logical_id", logical_id)?;
    let identity = length_prefixed_identity(&[tenant_id, kind, logical_id]);
    Ok(format!(
        "ts1_{}",
        hmac_hex(DOCUMENT_ID_KEY, "fixed-document", &identity, 40)
    ))
}

pub fn tenant_document<T: Serialize + ?Sized>(
    tenant_id: &str,
    kind: &str,
    logical_id: &str,
    value: &T,
) -> Result<Value, ApiError> {
    tenant_document_with_storage_identity(tenant_id, kind, logical_id, logical_id, value)
}

pub fn tenant_document_with_storage_identity<T: Serialize + ?Sized>(
    tenant_id: &str,
    kind: &str,
    logical_id: &str,
    storage_identity: &str,
    value: &T,
) -> Result<Value, ApiError> {
    require_non_empty("tenant_id", tenant_id)?;
    require_non_empty("logical_id", logical_id)?;
    require_non_empty("storage_identity", storage_identity)?;
    let mut document =
        match serde_json::to_value(value).map_err(|error| ApiError::Internal(error.to_string()))? {
            Value::Object(map) => map,
            other => {
                let mut map = Map::new();
                map.insert("value".to_string(), other);
                map
            }
        };

    match document.get("tenant_id") {
        Some(Value::String(existing)) if existing == tenant_id => {}
        Some(Value::String(existing)) if existing.trim().is_empty() => {}
        Some(Value::String(_)) => {
            return Err(ApiError::Internal(
                "persisted document tenant_id does not match its repository scope".to_string(),
            ));
        }
        Some(_) => {
            return Err(ApiError::Internal(
                "persisted document tenant_id must be a string".to_string(),
            ));
        }
        None => {}
    }

    document.insert(
        "id".to_string(),
        Value::String(persisted_document_id(tenant_id, kind, storage_identity)?),
    );
    document.insert(
        "logical_id".to_string(),
        Value::String(logical_id.to_string()),
    );
    document.insert(
        "tenant_id".to_string(),
        Value::String(tenant_id.to_string()),
    );
    Ok(Value::Object(document))
}

pub fn scoped_storage_identity(scope: &str, logical_id: &str) -> Result<String, ApiError> {
    require_non_empty("storage scope", scope)?;
    require_non_empty("logical_id", logical_id)?;
    Ok(length_prefixed_identity(&[scope, logical_id]))
}

pub fn owner_scoped_storage_identity(
    owner_user_id: Option<&str>,
    logical_id: &str,
) -> Result<String, ApiError> {
    require_non_empty("logical_id", logical_id)?;
    let owner_scope = match owner_user_id {
        Some(owner_user_id) => {
            require_non_empty("owner_user_id", owner_user_id)?;
            length_prefixed_identity(&["owner", owner_user_id])
        }
        None => length_prefixed_identity(&["company"]),
    };
    Ok(length_prefixed_identity(&[&owner_scope, logical_id]))
}

pub fn tenant_structured_row_document(tenant_id: &str, row: &Value) -> Result<Value, ApiError> {
    let mut payload = row
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::Internal("structured row must be a JSON object".to_string()))?;
    let logical_id = payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::Internal("structured row is missing id".to_string()))?
        .to_string();
    let snapshot_id = payload
        .get("snapshot_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::Internal("structured row is missing snapshot_id".to_string()))?
        .to_string();
    let storage_identity = scoped_storage_identity(&snapshot_id, &logical_id)?;
    let mut document = tenant_document_with_storage_identity(
        tenant_id,
        "rag_structured_rows",
        &logical_id,
        &storage_identity,
        row,
    )?;

    payload.insert("id".to_string(), Value::String(logical_id));
    payload.insert(
        "tenant_id".to_string(),
        Value::String(tenant_id.to_string()),
    );
    document[STRUCTURED_ROW_PAYLOAD_FIELD] = Value::Object(payload);
    Ok(document)
}

pub fn is_tenant_document(index_uid: &str, document: &Value) -> bool {
    let Some(id) = document.get("id").and_then(Value::as_str) else {
        return false;
    };
    let Some(tenant_id) = document.get("tenant_id").and_then(Value::as_str) else {
        return false;
    };
    let Some(logical_id) = document.get("logical_id").and_then(Value::as_str) else {
        return false;
    };
    if tenant_id.trim().is_empty() || logical_id.trim().is_empty() {
        return false;
    }

    let kind = if index_uid == "rag_harness_components" {
        let Some(doc_kind @ ("component" | "revision")) =
            document.get("doc_kind").and_then(Value::as_str)
        else {
            return false;
        };
        format!("{index_uid}:{doc_kind}")
    } else {
        index_uid.to_string()
    };
    let storage_identity = if index_uid == "rag_structured_rows" {
        if !document
            .get(STRUCTURED_ROW_PAYLOAD_FIELD)
            .is_some_and(Value::is_object)
        {
            return false;
        }
        let Some(snapshot_id) = document.get("snapshot_id").and_then(Value::as_str) else {
            return false;
        };
        let Ok(identity) = scoped_storage_identity(snapshot_id, logical_id) else {
            return false;
        };
        identity
    } else if index_uid == "rag_parse_artifacts" {
        let owner_user_id = match document.get("owner_user_id") {
            Some(Value::String(owner_user_id)) => Some(owner_user_id.as_str()),
            Some(Value::Null) | None => None,
            Some(_) => return false,
        };
        let Ok(identity) = owner_scoped_storage_identity(owner_user_id, logical_id) else {
            return false;
        };
        identity
    } else {
        logical_id.to_string()
    };
    if index_uid == "rag_company_context"
        && document.get("uri").and_then(Value::as_str) != Some(logical_id)
    {
        return false;
    }

    persisted_document_id(tenant_id, &kind, &storage_identity).is_ok_and(|expected| expected == id)
}

pub fn restore_logical_id(index_uid: &str, mut document: Value) -> Value {
    if !is_tenant_document(index_uid, &document) {
        return document;
    }
    if index_uid == "rag_structured_rows" {
        if let Some(payload) = document.get(STRUCTURED_ROW_PAYLOAD_FIELD).cloned() {
            return payload;
        }
        return document;
    }
    let Value::Object(map) = &mut document else {
        return document;
    };
    if let Some(Value::String(logical_id)) = map.remove("logical_id") {
        map.insert("id".to_string(), Value::String(logical_id));
    }
    document
}

#[derive(Debug, Clone)]
pub struct TenantFilter {
    clauses: Vec<String>,
}

impl TenantFilter {
    pub fn new(tenant_id: &str) -> Result<Self, ApiError> {
        require_non_empty("tenant_id", tenant_id)?;
        Ok(Self {
            clauses: vec![format!("tenant_id = {}", meili_string(tenant_id)?)],
        })
    }

    pub fn eq(mut self, field: &'static str, value: &str) -> Result<Self, ApiError> {
        self.clauses
            .push(format!("{field} = {}", meili_string(value)?));
        Ok(self)
    }

    pub fn eq_u64(mut self, field: &'static str, value: u64) -> Self {
        self.clauses.push(format!("{field} = {value}"));
        self
    }

    pub fn is_null(mut self, field: &'static str) -> Self {
        self.clauses
            .push(format!("({field} IS NULL OR {field} NOT EXISTS)"));
        self
    }

    pub fn eq_or_null(mut self, field: &'static str, value: &str) -> Result<Self, ApiError> {
        self.clauses.push(format!(
            "({field} = {} OR {field} IS NULL OR {field} NOT EXISTS)",
            meili_string(value)?
        ));
        Ok(self)
    }

    pub fn in_strings(mut self, field: &'static str, values: &[String]) -> Result<Self, ApiError> {
        let values =
            serde_json::to_string(values).map_err(|error| ApiError::Internal(error.to_string()))?;
        self.clauses.push(format!("{field} IN {values}"));
        Ok(self)
    }

    /// Add one parenthesized OR group of string-set predicates while retaining
    /// the mandatory tenant clause. This keeps multi-field destructive filters
    /// value-encoded instead of admitting caller-built filter fragments.
    pub fn any_in_strings(
        mut self,
        conditions: &[(&'static str, &[String])],
    ) -> Result<Self, ApiError> {
        if conditions.is_empty() {
            return Err(ApiError::Internal(
                "tenant filter OR string-set clause must not be empty".to_string(),
            ));
        }
        let clauses = conditions
            .iter()
            .map(|(field, values)| {
                if values.is_empty() {
                    return Err(ApiError::Internal(format!(
                        "tenant filter {field} string-set must not be empty"
                    )));
                }
                for value in *values {
                    require_non_empty(field, value)?;
                }
                let values = serde_json::to_string(values)
                    .map_err(|error| ApiError::Internal(error.to_string()))?;
                Ok(format!("{field} IN {values}"))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        self.clauses.push(format!("({})", clauses.join(" OR ")));
        Ok(self)
    }

    pub fn any_not_eq(mut self, conditions: &[(&'static str, &str)]) -> Result<Self, ApiError> {
        if conditions.is_empty() {
            return Err(ApiError::Internal(
                "tenant filter OR clause must not be empty".to_string(),
            ));
        }
        let clauses = conditions
            .iter()
            .map(|(field, value)| Ok(format!("{field} != {}", meili_string(value)?)))
            .collect::<Result<Vec<_>, ApiError>>()?;
        self.clauses.push(format!("({})", clauses.join(" OR ")));
        Ok(self)
    }

    pub fn logical_id(mut self, logical_id: &str) -> Result<Self, ApiError> {
        let value = meili_string(logical_id)?;
        self.clauses.push(format!(
            "(logical_id = {value} OR ((logical_id NOT EXISTS OR logical_id IS NULL) AND id = {value}))"
        ));
        Ok(self)
    }

    pub fn logical_ids(mut self, logical_ids: &[String]) -> Result<Self, ApiError> {
        if logical_ids.is_empty() {
            return Err(ApiError::Internal(
                "tenant filter logical-id set must not be empty".to_string(),
            ));
        }
        for logical_id in logical_ids {
            require_non_empty("logical_id", logical_id)?;
        }
        let values = serde_json::to_string(logical_ids)
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        self.clauses.push(format!(
            "(logical_id IN {values} OR ((logical_id NOT EXISTS OR logical_id IS NULL) AND id IN {values}))"
        ));
        Ok(self)
    }

    pub fn finish(self) -> String {
        self.clauses.join(" AND ")
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(ApiError::Internal(format!(
            "{field} must be non-empty for fixed-index persistence"
        )));
    }
    Ok(())
}

fn length_prefixed_identity(parts: &[&str]) -> String {
    let mut identity = String::new();
    for part in parts {
        identity.push_str(&part.len().to_string());
        identity.push(':');
        identity.push_str(part);
    }
    identity
}

fn meili_string(value: &str) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|error| ApiError::Internal(error.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::meili::FIXED_INDEXES;

    #[test]
    fn persisted_ids_are_stable_tenant_and_kind_scoped_meili_ids() {
        let first = persisted_document_id("tenant-a", "rag_sources", "source-1").unwrap();
        assert_eq!(
            first,
            persisted_document_id("tenant-a", "rag_sources", "source-1").unwrap()
        );
        assert_ne!(
            first,
            persisted_document_id("tenant-b", "rag_sources", "source-1").unwrap()
        );
        assert_ne!(
            first,
            persisted_document_id("tenant-a", "rag_source_revisions", "source-1").unwrap()
        );
        assert!(first
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_')));
    }

    #[test]
    fn tenant_documents_preserve_logical_identity_and_reject_scope_mismatch() {
        let document = tenant_document(
            "tenant-a",
            "rag_sources",
            "source-1",
            &json!({"id": "source-1", "title": "Source"}),
        )
        .unwrap();
        assert_eq!(document["tenant_id"], "tenant-a");
        assert_eq!(document["logical_id"], "source-1");
        assert_ne!(document["id"], "source-1");

        let restored = restore_logical_id("rag_sources", document);
        assert_eq!(restored["id"], "source-1");
        assert!(restored.get("logical_id").is_none());

        let error = tenant_document(
            "tenant-a",
            "rag_sources",
            "source-1",
            &json!({"tenant_id": "tenant-b"}),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("tenant_id does not match its repository scope"));
    }

    #[test]
    fn storage_identity_can_be_scoped_without_changing_the_public_id() {
        let first = tenant_structured_row_document(
            "tenant-a",
            &json!({
                "id": "row-1",
                "snapshot_id": "snapshot-a",
                "logical_id": "business-a"
            }),
        )
        .unwrap();
        let second = tenant_structured_row_document(
            "tenant-a",
            &json!({
                "id": "row-1",
                "snapshot_id": "snapshot-b",
                "logical_id": "business-b"
            }),
        )
        .unwrap();

        assert_ne!(first["id"], second["id"]);
        let first = restore_logical_id("rag_structured_rows", first);
        let second = restore_logical_id("rag_structured_rows", second);
        assert_eq!(first["id"], "row-1");
        assert_eq!(second["id"], "row-1");
        assert_eq!(first["logical_id"], "business-a");
        assert_eq!(second["logical_id"], "business-b");
    }

    #[test]
    fn unverified_logical_id_is_never_treated_as_internal_metadata() {
        let legacy = json!({
            "id": "public-row",
            "logical_id": "business-column",
            "tenant_id": "tenant-a",
            "snapshot_id": "snapshot-a"
        });
        assert!(!is_tenant_document("rag_structured_rows", &legacy));
        assert_eq!(
            restore_logical_id("rag_structured_rows", legacy.clone()),
            legacy
        );
    }

    #[test]
    fn length_prefixing_prevents_delimiter_ambiguity() {
        assert_ne!(
            length_prefixed_identity(&["a\0b", "c"]),
            length_prefixed_identity(&["a", "b\0c"])
        );
    }

    #[test]
    fn owner_scoped_identity_separates_equal_artifact_ids() {
        let first = owner_scoped_storage_identity(Some("owner-a"), "artifact-1").unwrap();
        let second = owner_scoped_storage_identity(Some("owner-b"), "artifact-1").unwrap();
        let company = owner_scoped_storage_identity(None, "artifact-1").unwrap();
        assert_ne!(first, second);
        assert_ne!(first, company);
        assert_ne!(second, company);
    }

    #[test]
    fn tenant_filters_cannot_be_constructed_without_tenant_scope() {
        assert!(TenantFilter::new("").is_err());
        let filter = TenantFilter::new("tenant-a")
            .unwrap()
            .eq("status", "active")
            .unwrap()
            .logical_id("source-1")
            .unwrap()
            .finish();
        assert!(filter.starts_with("tenant_id = \"tenant-a\" AND "));
        assert!(filter.contains("logical_id = \"source-1\""));
        assert!(filter.contains("logical_id NOT EXISTS"));
        assert!(filter.contains("id = \"source-1\""));

        let ownerless = TenantFilter::new("tenant-a")
            .unwrap()
            .is_null("owner_user_id")
            .finish();
        assert!(ownerless.contains("owner_user_id IS NULL"));
        assert!(ownerless.contains("owner_user_id NOT EXISTS"));
    }

    #[test]
    fn tenant_filter_batches_current_and_legacy_logical_ids() {
        let filter = TenantFilter::new("tenant-a")
            .unwrap()
            .logical_ids(&["operation-1".to_string(), "operation-\"2".to_string()])
            .unwrap()
            .finish();

        assert!(filter.starts_with("tenant_id = \"tenant-a\" AND "));
        assert!(
            filter.contains("logical_id IN [\"operation-1\",\"operation-\\\"2\"]"),
            "{filter}"
        );
        assert!(filter.contains("logical_id NOT EXISTS"), "{filter}");
        assert!(filter.contains("logical_id IS NULL"), "{filter}");
        assert!(
            filter.contains("id IN [\"operation-1\",\"operation-\\\"2\"]"),
            "{filter}"
        );

        assert!(TenantFilter::new("tenant-a")
            .unwrap()
            .logical_ids(&[])
            .is_err());
        assert!(TenantFilter::new("tenant-a")
            .unwrap()
            .logical_ids(&["  ".to_string()])
            .is_err());
    }

    #[test]
    fn tenant_filter_encodes_multi_field_string_set_or_groups() {
        let ids = ["link-1".to_string(), "link-\"2".to_string()];
        let uris = ["ctx://company/source/custom".to_string()];
        let filter = TenantFilter::new("tenant-a")
            .unwrap()
            .any_in_strings(&[
                ("logical_id", &ids),
                ("source_uri", &uris),
                ("target_uri", &uris),
            ])
            .unwrap()
            .finish();

        assert!(
            filter.starts_with("tenant_id = \"tenant-a\" AND ("),
            "{filter}"
        );
        assert!(
            filter.contains("logical_id IN [\"link-1\",\"link-\\\"2\"]"),
            "{filter}"
        );
        assert!(
            filter.contains("source_uri IN [\"ctx://company/source/custom\"]"),
            "{filter}"
        );
        assert!(filter.contains(" OR target_uri IN "), "{filter}");
        assert!(TenantFilter::new("tenant-a")
            .unwrap()
            .any_in_strings(&[])
            .is_err());
        assert!(TenantFilter::new("tenant-a")
            .unwrap()
            .any_in_strings(&[("source_uri", &[])])
            .is_err());
    }

    #[test]
    fn every_fixed_index_separates_identical_logical_ids_by_tenant() {
        for index_uid in FIXED_INDEXES {
            let make_document = |tenant_id: &str, value: &str| {
                if *index_uid == "rag_structured_rows" {
                    return tenant_structured_row_document(
                        tenant_id,
                        &json!({
                            "id": "shared-logical-id",
                            "snapshot_id": "snapshot-a",
                            "value": value
                        }),
                    );
                }
                if *index_uid == "rag_parse_artifacts" {
                    let storage_identity =
                        owner_scoped_storage_identity(Some("owner-a"), "shared-logical-id")?;
                    return tenant_document_with_storage_identity(
                        tenant_id,
                        index_uid,
                        "shared-logical-id",
                        &storage_identity,
                        &json!({"owner_user_id": "owner-a", "value": value}),
                    );
                }
                let mut value = json!({"value": value});
                let kind = if *index_uid == "rag_harness_components" {
                    value["doc_kind"] = Value::String("component".to_string());
                    format!("{index_uid}:component")
                } else {
                    index_uid.to_string()
                };
                if *index_uid == "rag_company_context" {
                    value["uri"] = Value::String("shared-logical-id".to_string());
                }
                tenant_document(tenant_id, &kind, "shared-logical-id", &value)
            };
            let first = make_document("tenant-a", "a").unwrap();
            let second = make_document("tenant-b", "b").unwrap();

            assert_ne!(first["id"], second["id"], "{index_uid}");
            assert_eq!(first["tenant_id"], "tenant-a", "{index_uid}");
            assert_eq!(second["tenant_id"], "tenant-b", "{index_uid}");
            assert_eq!(first["logical_id"], "shared-logical-id", "{index_uid}");
            assert_eq!(second["logical_id"], "shared-logical-id", "{index_uid}");
            assert_eq!(
                restore_logical_id(index_uid, first)["id"],
                "shared-logical-id"
            );
            assert_eq!(
                restore_logical_id(index_uid, second)["id"],
                "shared-logical-id"
            );
        }
    }
}
