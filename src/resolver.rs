use crate::{
    error::ApiError,
    models::EventIndexRouting,
    util::{hmac_hex, validate_meili_uid},
};

pub const EVENT_INDEX_SCHEMA_VERSION: u32 = 1;
pub const EVENT_SETTINGS_HASH: &str = "events-v1";

#[derive(Debug, Clone)]
pub struct EventIndexResolver {
    secret: Vec<u8>,
}

impl EventIndexResolver {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    pub fn resolve(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        created: bool,
        reused: bool,
    ) -> Result<EventIndexRouting, ApiError> {
        let tenant_hash = self.tenant_hash(tenant_id);
        let owner_user_id_hash = self.user_hash(owner_user_id);
        let event_index_uid = format!("rag_events__t_{tenant_hash}__u_{owner_user_id_hash}");
        let personal_context_index_uid =
            format!("rag_context__t_{tenant_hash}__u_{owner_user_id_hash}");

        validate_meili_uid(&event_index_uid)?;
        validate_meili_uid(&personal_context_index_uid)?;

        Ok(EventIndexRouting {
            tenant_id: tenant_id.to_string(),
            owner_user_id_hash,
            event_index_uid,
            personal_context_index_uid,
            strategy: "per_user".to_string(),
            schema_version: EVENT_INDEX_SCHEMA_VERSION,
            settings_hash: EVENT_SETTINGS_HASH.to_string(),
            created,
            reused,
        })
    }

    pub fn tenant_hash(&self, tenant_id: &str) -> String {
        hmac_hex(&self.secret, "tenant", tenant_id, 12)
    }

    pub fn user_hash(&self, owner_user_id: &str) -> String {
        hmac_hex(&self.secret, "user", owner_user_id, 16)
    }

    pub fn idempotency_hash(&self, key: &str) -> String {
        hmac_hex(&self.secret, "idempotency", key, 24)
    }
}
