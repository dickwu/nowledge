use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use uuid::Uuid;

use crate::error::ApiError;

type HmacSha256 = Hmac<Sha256>;

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7().simple())
}

pub fn hmac_hex(secret: &[u8], namespace: &str, value: &str, len: usize) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any size");
    mac.update(namespace.as_bytes());
    mac.update(b":");
    mac.update(value.as_bytes());
    let hex = hex::encode(mac.finalize().into_bytes());
    hex.chars().take(len).collect()
}

pub fn sanitize_slug(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut last_dash = false;

    for ch in input.chars().flat_map(|c| c.to_lowercase()) {
        let valid = ch.is_ascii_alphanumeric() || ch == '-' || ch == '_';
        if valid {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }

    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "item".to_string()
    } else {
        trimmed
    }
}

pub fn validate_meili_uid(uid: &str) -> Result<(), ApiError> {
    if uid
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        Ok(())
    } else {
        Err(ApiError::Internal(format!(
            "generated invalid Meilisearch uid: {uid}"
        )))
    }
}

pub fn ancestor_uris(uri: &str) -> Vec<String> {
    let Some(rest) = uri.strip_prefix("ctx://") else {
        return Vec::new();
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let mut acc = "ctx://".to_string();
    let mut ancestors = Vec::new();
    for (idx, part) in parts.iter().enumerate() {
        if idx + 1 == parts.len() {
            break;
        }
        if idx > 0 {
            acc.push('/');
        }
        acc.push_str(part);
        ancestors.push(acc.clone());
    }
    ancestors
}

pub fn require_string(value: Option<String>, field: &str) -> Result<String, ApiError> {
    value
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("{field} is required")))
}

pub fn text_score(text: &str, query: &str) -> f32 {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return 1.0;
    }

    let text = text.to_lowercase();
    if text.contains(&query) {
        return 10.0 + query.len() as f32 / 100.0;
    }

    let mut score = 0.0;
    for token in query.split_whitespace().filter(|t| !t.is_empty()) {
        if text.contains(token) {
            score += 1.0;
        }
    }
    score
}

pub fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

pub fn redact_secrets(value: &Value, known_secrets: &[String]) -> Value {
    match value {
        Value::String(s) => Value::String(redact_string(s, known_secrets)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|v| redact_secrets(v, known_secrets))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let redacted = if is_secret_key(k) {
                        json!("[REDACTED]")
                    } else {
                        redact_secrets(v, known_secrets)
                    };
                    (k.clone(), redacted)
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}

pub fn redact_string(input: &str, known_secrets: &[String]) -> String {
    let mut out = input.to_string();
    for secret in known_secrets.iter().filter(|s| !s.is_empty()) {
        out = out.replace(secret, "[REDACTED]");
    }

    for prefix in ["sk-", "sess-", "codex-", "oaic-", "Bearer "] {
        while let Some(start) = out.find(prefix) {
            let end = out[start..]
                .find(char::is_whitespace)
                .map(|offset| start + offset)
                .unwrap_or(out.len());
            out.replace_range(start..end, "[REDACTED]");
        }
    }
    out
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    ["token", "api_key", "apikey", "authorization", "secret"]
        .iter()
        .any(|needle| key.contains(needle))
}
