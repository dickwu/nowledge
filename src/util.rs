use std::collections::HashSet;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use uuid::Uuid;

use crate::error::ApiError;

type HmacSha256 = Hmac<Sha256>;
const MIN_REDACTABLE_SECRET_CHARS: usize = 4;
const SECRET_SUBSTRING_WINDOW_CHARS: usize = 8;
const RESPONSE_SECRET_FRAGMENT_CHARS: usize = 8;
const REDACTION_MARKER: &str = "[REDACTED]";

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
    let matcher = SecretMatcher::new(known_secrets);
    let policy = if matches!(value, Value::Object(_) | Value::Array(_)) {
        ResponseTextPolicy::ExactOnly
    } else {
        ResponseTextPolicy::Fragment
    };
    redact_secrets_inner(value, &matcher, policy)
}

#[derive(Clone, Copy)]
enum ResponseTextPolicy {
    ConfiguredExactOnly,
    Locator,
    ExactOnly,
    Windowed,
    Fragment,
}

fn redact_secrets_inner(
    value: &Value,
    matcher: &SecretMatcher<'_>,
    policy: ResponseTextPolicy,
) -> Value {
    match value {
        Value::String(s) => Value::String(matcher.redact_response_string(s, policy)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|v| redact_secrets_inner(v, matcher, policy))
                .collect(),
        ),
        Value::Object(map) => {
            let mut redacted_map = serde_json::Map::with_capacity(map.len());
            for (key, child) in map {
                let redacted_child = if is_secret_key(key) {
                    json!(matcher.redaction_marker.as_str())
                } else {
                    // Classify every field independently. In particular,
                    // `content: { tenant_id: ... }` must not make the nested
                    // tenant identifier look like free-form content.
                    let child_policy = if is_locator_key(key) {
                        ResponseTextPolicy::Locator
                    } else if is_structural_metadata_key(key) {
                        ResponseTextPolicy::ConfiguredExactOnly
                    } else if is_fragment_sensitive_text_key(key) {
                        ResponseTextPolicy::Fragment
                    } else if is_arbitrary_text_container_key(key) {
                        ResponseTextPolicy::Windowed
                    } else {
                        match policy {
                            ResponseTextPolicy::ConfiguredExactOnly
                            | ResponseTextPolicy::Locator
                            | ResponseTextPolicy::ExactOnly => ResponseTextPolicy::ExactOnly,
                            ResponseTextPolicy::Windowed | ResponseTextPolicy::Fragment => {
                                ResponseTextPolicy::Windowed
                            }
                        }
                    };
                    redact_secrets_inner(child, matcher, child_policy)
                };
                let key_policy = if is_secret_key(key)
                    || is_structural_metadata_key(key)
                    || is_fragment_sensitive_text_key(key)
                    || is_arbitrary_text_container_key(key)
                {
                    // Preserve the API's known field names. Exact configured
                    // secrets are still removed, while only arbitrary object
                    // keys receive heuristic window projection.
                    ResponseTextPolicy::ConfiguredExactOnly
                } else {
                    ResponseTextPolicy::Windowed
                };
                let candidate = matcher.redact_response_string(key, key_policy);
                let redacted_key = unique_json_key(candidate, &redacted_map, matcher, key_policy);
                redacted_map.insert(redacted_key, redacted_child);
            }
            Value::Object(redacted_map)
        }
        _ => value.clone(),
    }
}

pub fn redact_egress_text(input: &str, known_secrets: &[String]) -> String {
    SecretMatcher::new(known_secrets).redact_egress_text(input)
}

pub fn redact_locator(input: &str, known_secrets: &[String]) -> String {
    SecretMatcher::new(known_secrets).redact_locator(input)
}

pub fn redact_string(input: &str, known_secrets: &[String]) -> String {
    SecretMatcher::new(known_secrets).redact_exact(input)
}

pub fn mask_secrets_preserving_chars(input: &str, known_secrets: &[String]) -> String {
    SecretMatcher::new(known_secrets).mask_exact(input)
}

pub fn mask_secret_fragment_projection_preserving_chars(
    input: &str,
    known_secrets: &[String],
) -> String {
    SecretMatcher::new(known_secrets).mask_fragment_projection(input)
}

pub fn mask_secret_egress_projection_preserving_chars(
    input: &str,
    known_secrets: &[String],
) -> String {
    SecretMatcher::new(known_secrets).mask_egress_projection(input)
}

pub fn mask_secret_boundary_fragments_preserving_chars(
    input: &str,
    known_secrets: &[String],
    minimum_fragment_chars: usize,
) -> String {
    SecretMatcher::new(known_secrets).mask_boundaries(input, minimum_fragment_chars)
}

/// Incrementally projects provider text without allowing a secret to straddle
/// two emitted chunks.
///
/// `push` may return an empty string while the trailing input can still grow
/// into a configured secret or a credential-shaped value. Configured secrets
/// shorter than the regular projection window are matched in full; longer
/// secrets use the same eight-character windows as [`redact_egress_text`]. A
/// credential-shaped value is held until its existing minimum length is met,
/// then masked through the first non-credential delimiter.
///
/// Call [`Self::finish`] only after a successful end-of-stream. Dropping the
/// value, or consuming it through [`Self::abort`], discards any un-emitted
/// suffix.
pub struct StreamingTextRedactor {
    literal_patterns: HashSet<Vec<char>>,
    literal_prefixes: HashSet<Vec<char>>,
    maximum_literal_pattern_chars: usize,
    pending: Vec<StreamingCharacter>,
    preceding_projected_character: Option<char>,
    suppressing_credential: bool,
    mask_character: char,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum StreamingMask {
    None,
    Configured,
    Credential,
}

#[derive(Clone, Copy, Debug)]
struct StreamingCharacter {
    character: char,
    mask: StreamingMask,
}

impl StreamingCharacter {
    fn projected(self, mask_character: char) -> char {
        match self.mask {
            StreamingMask::None => self.character,
            StreamingMask::Configured if self.character.is_whitespace() => self.character,
            StreamingMask::Configured | StreamingMask::Credential => mask_character,
        }
    }
}

impl StreamingTextRedactor {
    pub fn new(known_secrets: &[String]) -> Self {
        let matcher = SecretMatcher::new(known_secrets);
        let mask_character = matcher.mask_character;
        let mut literal_patterns = HashSet::new();

        for secret in known_secrets.iter().filter(|secret| !secret.is_empty()) {
            let characters = secret.chars().collect::<Vec<_>>();
            if characters.len() <= SECRET_SUBSTRING_WINDOW_CHARS {
                literal_patterns.insert(characters);
            } else {
                literal_patterns.extend(
                    characters
                        .windows(SECRET_SUBSTRING_WINDOW_CHARS)
                        .map(<[char]>::to_vec),
                );
            }
        }

        let maximum_literal_pattern_chars =
            literal_patterns.iter().map(Vec::len).max().unwrap_or(0);
        let literal_prefixes = literal_patterns
            .iter()
            .flat_map(|pattern| (1..pattern.len()).map(|end| pattern[..end].to_vec()))
            .collect();

        Self {
            literal_patterns,
            literal_prefixes,
            maximum_literal_pattern_chars,
            pending: Vec::new(),
            preceding_projected_character: None,
            suppressing_credential: false,
            mask_character,
        }
    }

    /// Adds one provider delta and returns only text that is safe to emit now.
    pub fn push(&mut self, delta: &str) -> String {
        let mut output = String::with_capacity(delta.len());
        for character in delta.chars() {
            if self.suppressing_credential && is_credential_character(character) {
                self.pending.push(StreamingCharacter {
                    character,
                    mask: StreamingMask::Credential,
                });
            } else {
                self.suppressing_credential = false;
                self.pending.push(StreamingCharacter {
                    character,
                    mask: StreamingMask::None,
                });
                self.mask_complete_configured_suffixes();
                self.start_credential_suppression_if_complete();
            }

            let retained = self.longest_sensitive_suffix();
            self.emit_prefix(self.pending.len() - retained, &mut output);
        }
        output
    }

    /// Flushes the final suffix after a successful provider end-of-stream.
    pub fn finish(mut self) -> String {
        let mut output = String::new();
        self.emit_prefix(self.pending.len(), &mut output);
        output
    }

    /// Discards the suffix retained for a stream that did not complete.
    pub fn abort(mut self) {
        self.pending.clear();
    }

    fn mask_complete_configured_suffixes(&mut self) {
        let maximum = self.maximum_literal_pattern_chars.min(self.pending.len());
        for length in 1..=maximum {
            let start = self.pending.len() - length;
            let candidate = self.pending[start..]
                .iter()
                .map(|character| character.character)
                .collect::<Vec<_>>();
            if self.literal_patterns.contains(&candidate) {
                for character in &mut self.pending[start..] {
                    character.mask = character.mask.max(StreamingMask::Configured);
                }
            }
        }
    }

    fn start_credential_suppression_if_complete(&mut self) {
        for (prefix, minimum_chars) in credential_shapes() {
            if self.pending.len() < minimum_chars {
                continue;
            }
            let start = self.pending.len() - minimum_chars;
            if !self.has_credential_left_boundary(start) {
                continue;
            }
            let candidate = self.pending[start..]
                .iter()
                .map(|character| character.projected(self.mask_character))
                .collect::<Vec<_>>();
            let prefix = prefix.chars().collect::<Vec<_>>();
            if candidate.starts_with(&prefix)
                && candidate[prefix.len()..]
                    .iter()
                    .all(|character| is_credential_character(*character))
            {
                for character in &mut self.pending[start..] {
                    character.mask = StreamingMask::Credential;
                }
                self.suppressing_credential = true;
                return;
            }
        }
    }

    fn longest_sensitive_suffix(&self) -> usize {
        let configured = (1..=self
            .maximum_literal_pattern_chars
            .saturating_sub(1)
            .min(self.pending.len()))
            .rev()
            .find(|length| {
                let start = self.pending.len() - length;
                let candidate = self.pending[start..]
                    .iter()
                    .map(|character| character.character)
                    .collect::<Vec<_>>();
                self.literal_prefixes.contains(&candidate)
            })
            .unwrap_or(0);

        let credential = credential_shapes()
            .into_iter()
            .filter_map(|(prefix, minimum_chars)| {
                let maximum = minimum_chars.saturating_sub(1).min(self.pending.len());
                (1..=maximum).rev().find(|length| {
                    let start = self.pending.len() - length;
                    self.has_credential_left_boundary(start)
                        && self.pending[start..]
                            .iter()
                            .map(|character| character.projected(self.mask_character))
                            .eq(prefix.chars().take(*length))
                        || self.has_credential_left_boundary(start)
                            && *length >= prefix.chars().count()
                            && self.pending[start..]
                                .iter()
                                .take(prefix.chars().count())
                                .map(|character| character.projected(self.mask_character))
                                .eq(prefix.chars())
                            && self.pending[start + prefix.chars().count()..].iter().all(
                                |character| {
                                    is_credential_character(
                                        character.projected(self.mask_character),
                                    )
                                },
                            )
                })
            })
            .max()
            .unwrap_or(0);

        configured.max(credential)
    }

    fn has_credential_left_boundary(&self, start: usize) -> bool {
        let previous = if start == 0 {
            self.preceding_projected_character
        } else {
            Some(self.pending[start - 1].projected(self.mask_character))
        };
        previous.is_none_or(|character| !is_credential_left_context_character(character))
    }

    fn emit_prefix(&mut self, length: usize, output: &mut String) {
        for character in self.pending.drain(..length) {
            let projected = character.projected(self.mask_character);
            output.push(projected);
            self.preceding_projected_character = Some(projected);
        }
    }
}

struct SecretMatcher<'a> {
    ordered: Vec<&'a str>,
    short_exact: Vec<&'a str>,
    substring_windows: HashSet<Vec<char>>,
    boundary_patterns: Vec<BoundaryPattern>,
    marker_crossing_suffixes: Vec<&'a str>,
    redaction_marker: String,
    protect_redaction_marker: bool,
    mask_character: char,
}

struct BoundaryPattern {
    forward: Vec<char>,
    forward_prefix: Vec<usize>,
    reverse: Vec<char>,
    reverse_prefix: Vec<usize>,
}

impl<'a> SecretMatcher<'a> {
    fn new(known_secrets: &'a [String]) -> Self {
        let mask_character = collision_free_mask_character(known_secrets);
        let minimum_secret_chars = known_secrets
            .iter()
            .map(|secret| secret.chars().count())
            .filter(|chars| *chars > 0)
            .min();
        let protect_redaction_marker = known_secrets
            .iter()
            .filter(|secret| !secret.is_empty())
            .all(|secret| !REDACTION_MARKER.contains(secret));
        let redaction_marker = if protect_redaction_marker {
            REDACTION_MARKER.to_string()
        } else {
            // A replacement shorter than every configured secret cannot
            // reproduce any of them. Empty output is the only safe marker
            // when a caller supplies a one-character secret.
            mask_character
                .to_string()
                .repeat(minimum_secret_chars.unwrap_or(1).saturating_sub(1))
        };
        let ordered = ordered_known_secrets(known_secrets);
        let marker_crossing_suffixes = ordered
            .iter()
            .flat_map(|secret| {
                (0..REDACTION_MARKER.len()).filter_map(move |marker_start| {
                    let marker_suffix = &REDACTION_MARKER[marker_start..];
                    (secret.len() > marker_suffix.len() && secret.starts_with(marker_suffix))
                        .then(|| &secret[marker_suffix.len()..])
                })
            })
            .collect();
        let mut short_exact = known_secrets
            .iter()
            .map(String::as_str)
            .filter(|secret| {
                let chars = secret.chars().count();
                chars > 0 && chars < MIN_REDACTABLE_SECRET_CHARS
            })
            .collect::<Vec<_>>();
        short_exact.sort_unstable();
        short_exact.dedup();
        let substring_windows = ordered
            .iter()
            .flat_map(|secret| {
                secret
                    .chars()
                    .collect::<Vec<_>>()
                    .windows(SECRET_SUBSTRING_WINDOW_CHARS)
                    .map(<[char]>::to_vec)
                    .collect::<Vec<_>>()
            })
            .collect();
        let boundary_patterns = ordered
            .iter()
            .map(|secret| {
                let forward = secret.chars().collect::<Vec<_>>();
                let reverse = forward.iter().rev().copied().collect::<Vec<_>>();
                BoundaryPattern {
                    forward_prefix: prefix_table(&forward),
                    reverse_prefix: prefix_table(&reverse),
                    forward,
                    reverse,
                }
            })
            .collect();
        Self {
            ordered,
            short_exact,
            substring_windows,
            boundary_patterns,
            marker_crossing_suffixes,
            redaction_marker,
            protect_redaction_marker,
            mask_character,
        }
    }

    fn redact_response_string(&self, input: &str, policy: ResponseTextPolicy) -> String {
        let exact = match policy {
            ResponseTextPolicy::ConfiguredExactOnly => self.redact_configured_exact(input),
            ResponseTextPolicy::Locator => self.redact_locator(input),
            ResponseTextPolicy::ExactOnly
            | ResponseTextPolicy::Windowed
            | ResponseTextPolicy::Fragment => self.redact_exact(input),
        };
        match policy {
            ResponseTextPolicy::ConfiguredExactOnly
            | ResponseTextPolicy::Locator
            | ResponseTextPolicy::ExactOnly => exact,
            ResponseTextPolicy::Windowed => self.mask_secret_substring_windows(&exact),
            ResponseTextPolicy::Fragment => {
                let bounded = self.mask_boundaries(&exact, RESPONSE_SECRET_FRAGMENT_CHARS);
                let whole =
                    self.mask_if_whole_secret_substring(&bounded, RESPONSE_SECRET_FRAGMENT_CHARS);
                self.mask_secret_substring_windows(&whole)
            }
        }
    }

    fn redact_egress_text(&self, input: &str) -> String {
        let exact = self.redact_exact(input);
        let bounded = self.mask_boundaries(&exact, RESPONSE_SECRET_FRAGMENT_CHARS);
        let whole = self.mask_if_whole_secret_substring(&bounded, RESPONSE_SECRET_FRAGMENT_CHARS);
        self.mask_secret_substring_windows(&whole)
    }

    fn redact_locator(&self, input: &str) -> String {
        // Locators are protocol identifiers. Heuristic fragment masking can
        // make a valid ctx:// URI impossible to dereference, so remove only
        // complete configured secrets and preserve incidental substrings and
        // token-like slugs.
        self.redact_configured_exact(input)
    }

    fn mask_egress_projection(&self, input: &str) -> String {
        let exact = self.mask_exact(input);
        let bounded = self.mask_boundaries(&exact, RESPONSE_SECRET_FRAGMENT_CHARS);
        let whole = self.mask_if_whole_secret_substring(&bounded, RESPONSE_SECRET_FRAGMENT_CHARS);
        self.mask_secret_substring_windows(&whole)
    }

    fn mask_fragment_projection(&self, input: &str) -> String {
        let exact = self.mask_exact(input);
        let bounded = self.mask_boundaries(&exact, 4);
        let whole = self.mask_if_whole_secret_substring(&bounded, 4);
        self.mask_secret_substring_windows(&whole)
    }

    fn redact_exact(&self, input: &str) -> String {
        let mut out = self.redact_configured_exact(input);
        for (prefix, minimum_chars) in credential_shapes() {
            redact_credential_shaped_value(&mut out, prefix, minimum_chars, &self.redaction_marker);
        }
        // Generic credential-shape replacement can place the marker beside
        // caller text that completes a configured secret. Recheck the final
        // projection before returning it.
        self.redact_configured_exact(&out)
    }

    fn redact_configured_exact(&self, input: &str) -> String {
        let mut current = input.to_string();
        for _ in 0..64 {
            let redacted = self.redact_configured_exact_once(&current);
            if !self.contains_configured_secret(&redacted) {
                return redacted;
            }
            current = redacted;
        }

        // A pathological chain of mutually synthesizing secrets must fail
        // closed rather than return configured material.
        String::new()
    }

    fn redact_configured_exact_once(&self, input: &str) -> String {
        if self.short_exact.contains(&input) {
            return self.redaction_marker.clone();
        }
        let mut out = String::with_capacity(input.len());
        let mut offset = 0;
        while offset < input.len() {
            let matched_secret = self
                .ordered
                .iter()
                .find(|secret| input[offset..].starts_with(**secret))
                .copied();
            if self.can_preserve_marker_at(input, offset) {
                out.push_str(REDACTION_MARKER);
                offset += REDACTION_MARKER.len();
                continue;
            }
            if let Some(secret) = matched_secret {
                out.push_str(&self.redaction_marker);
                offset += secret.len();
                continue;
            }
            let ch = input[offset..]
                .chars()
                .next()
                .expect("offset remains on a character boundary");
            out.push(ch);
            offset += ch.len_utf8();
        }

        out
    }

    fn contains_configured_secret(&self, input: &str) -> bool {
        self.ordered.iter().any(|secret| input.contains(*secret))
    }

    fn can_preserve_marker_at(&self, input: &str, offset: usize) -> bool {
        if !self.protect_redaction_marker || !input[offset..].starts_with(REDACTION_MARKER) {
            return false;
        }

        let after_marker = &input[offset + REDACTION_MARKER.len()..];
        !self
            .marker_crossing_suffixes
            .iter()
            .any(|suffix| after_marker.starts_with(*suffix))
    }

    fn mask_exact(&self, input: &str) -> String {
        if self.short_exact.contains(&input) {
            return input
                .chars()
                .map(|ch| {
                    if ch.is_whitespace() {
                        ch
                    } else {
                        self.mask_character
                    }
                })
                .collect();
        }
        let mut out = String::with_capacity(input.len());
        let mut offset = 0;
        while offset < input.len() {
            let matched_secret = self
                .ordered
                .iter()
                .find(|secret| input[offset..].starts_with(**secret))
                .copied();
            if self.can_preserve_marker_at(input, offset) {
                out.push_str(REDACTION_MARKER);
                offset += REDACTION_MARKER.len();
                continue;
            }
            if let Some(secret) = matched_secret {
                for ch in secret.chars() {
                    out.push(if ch.is_whitespace() {
                        ch
                    } else {
                        self.mask_character
                    });
                }
                offset += secret.len();
                continue;
            }
            let ch = input[offset..]
                .chars()
                .next()
                .expect("offset remains on a character boundary");
            out.push(ch);
            offset += ch.len_utf8();
        }

        for (prefix, minimum_chars) in credential_shapes() {
            mask_credential_shaped_value(&mut out, prefix, minimum_chars, self.mask_character);
        }
        out
    }

    fn mask_boundaries(&self, input: &str, minimum_fragment_chars: usize) -> String {
        let input_chars = input.chars().collect::<Vec<_>>();
        if input_chars.is_empty() {
            return String::new();
        }
        let input_len = input_chars.len();
        let suffix_boundary = if input_chars.ends_with(&['.', '.', '.']) {
            input_len - 3
        } else {
            input_len
        };

        let mut prefix_chars_to_mask = 0;
        let mut suffix_chars_to_mask = 0;
        for pattern in &self.boundary_patterns {
            let max_overlap = pattern.forward.len().saturating_sub(1);
            if max_overlap < minimum_fragment_chars {
                continue;
            }

            let suffix_start = suffix_boundary.saturating_sub(max_overlap);
            let suffix_overlap = longest_suffix_matching_prefix(
                &pattern.forward,
                &pattern.forward_prefix,
                input_chars[suffix_start..suffix_boundary].iter().copied(),
            );
            if suffix_overlap >= minimum_fragment_chars {
                suffix_chars_to_mask = suffix_chars_to_mask.max(suffix_overlap);
            }

            let prefix_end = input_len.min(max_overlap);
            let prefix_overlap = longest_suffix_matching_prefix(
                &pattern.reverse,
                &pattern.reverse_prefix,
                input_chars[..prefix_end].iter().rev().copied(),
            );
            if prefix_overlap >= minimum_fragment_chars {
                prefix_chars_to_mask = prefix_chars_to_mask.max(prefix_overlap);
            }
        }

        let protected = redaction_marker_spans(&input_chars, self.protect_redaction_marker);

        input_chars
            .into_iter()
            .enumerate()
            .map(|(index, ch)| {
                let in_prefix = index < prefix_chars_to_mask;
                let in_suffix = index >= suffix_boundary.saturating_sub(suffix_chars_to_mask)
                    && index < suffix_boundary;
                if (in_prefix || in_suffix) && !protected[index] && !ch.is_whitespace() {
                    self.mask_character
                } else {
                    ch
                }
            })
            .collect()
    }

    fn mask_if_whole_secret_substring(&self, input: &str, minimum_chars: usize) -> String {
        let trimmed = input.trim();
        if trimmed.chars().count() < minimum_chars
            || (self.protect_redaction_marker && trimmed == REDACTION_MARKER)
            || !self.ordered.iter().any(|secret| secret.contains(trimmed))
        {
            return input.to_string();
        }
        let chars = input.chars().collect::<Vec<_>>();
        let protected = redaction_marker_spans(&chars, self.protect_redaction_marker);
        chars
            .into_iter()
            .enumerate()
            .map(|(index, ch)| {
                if protected[index] || ch.is_whitespace() {
                    ch
                } else {
                    self.mask_character
                }
            })
            .collect()
    }

    fn mask_secret_substring_windows(&self, input: &str) -> String {
        if self.substring_windows.is_empty() {
            return input.to_string();
        }
        let chars = input.chars().collect::<Vec<_>>();
        if chars.len() < SECRET_SUBSTRING_WINDOW_CHARS {
            return input.to_string();
        }
        let mut masked = vec![false; chars.len()];
        let protected = redaction_marker_spans(&chars, self.protect_redaction_marker);
        for (index, window) in chars.windows(SECRET_SUBSTRING_WINDOW_CHARS).enumerate() {
            if self.substring_windows.contains(window)
                && !protected[index..index + SECRET_SUBSTRING_WINDOW_CHARS]
                    .iter()
                    .any(|is_protected| *is_protected)
            {
                masked[index..index + SECRET_SUBSTRING_WINDOW_CHARS].fill(true);
            }
        }
        chars
            .into_iter()
            .enumerate()
            .map(|(index, ch)| {
                if masked[index] && !ch.is_whitespace() {
                    self.mask_character
                } else {
                    ch
                }
            })
            .collect()
    }
}

fn redaction_marker_spans(chars: &[char], protect_marker: bool) -> Vec<bool> {
    const MARKER: [char; 10] = ['[', 'R', 'E', 'D', 'A', 'C', 'T', 'E', 'D', ']'];
    let mut protected = vec![false; chars.len()];
    if !protect_marker {
        return protected;
    }
    for (index, candidate) in chars.windows(MARKER.len()).enumerate() {
        if candidate == MARKER {
            protected[index..index + MARKER.len()].fill(true);
        }
    }
    protected
}

fn collision_free_mask_character(known_secrets: &[String]) -> char {
    let used = known_secrets
        .iter()
        .flat_map(|secret| secret.chars())
        .collect::<HashSet<_>>();
    ['*', '•', '█', '◆']
        .into_iter()
        .find(|candidate| !used.contains(candidate))
        .or_else(|| {
            (0xE000..=0x10FFFF)
                .filter_map(char::from_u32)
                .find(|candidate| !used.contains(candidate))
        })
        .or_else(|| {
            (0..0xE000)
                .filter_map(char::from_u32)
                .find(|candidate| !used.contains(candidate))
        })
        .expect("configured secrets leave at least one Unicode scalar available for masking")
}

fn prefix_table(pattern: &[char]) -> Vec<usize> {
    let mut table = vec![0; pattern.len()];
    for index in 1..pattern.len() {
        let mut matched = table[index - 1];
        while matched > 0 && pattern[index] != pattern[matched] {
            matched = table[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
        }
        table[index] = matched;
    }
    table
}

fn longest_suffix_matching_prefix(
    pattern: &[char],
    prefix_table: &[usize],
    text: impl Iterator<Item = char>,
) -> usize {
    if pattern.is_empty() {
        return 0;
    }
    let mut matched = 0;
    for character in text {
        while matched > 0 && pattern[matched] != character {
            matched = prefix_table[matched - 1];
        }
        if pattern[matched] == character {
            matched += 1;
        }
        if matched == pattern.len() {
            matched = prefix_table[matched - 1];
        }
    }
    matched
}

fn ordered_known_secrets(known_secrets: &[String]) -> Vec<&str> {
    let mut secrets = known_secrets
        .iter()
        .map(String::as_str)
        .filter(|secret| secret.chars().count() >= MIN_REDACTABLE_SECRET_CHARS)
        .collect::<Vec<_>>();
    secrets
        .sort_unstable_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    secrets.dedup();
    secrets
}

fn credential_shapes() -> [(&'static str, usize); 5] {
    [
        ("sk-", 12),
        ("sess-", 16),
        ("codex-", 20),
        ("oaic-", 16),
        ("Bearer ", 10),
    ]
}

fn redact_credential_shaped_value(
    out: &mut String,
    prefix: &str,
    minimum_chars: usize,
    redaction_marker: &str,
) {
    let mut search_from = 0;
    while let Some(offset) = out[search_from..].find(prefix) {
        let start = search_from + offset;
        if !has_credential_left_boundary(out, start) {
            search_from = start + prefix.len();
            continue;
        }
        let value_start = start + prefix.len();
        let end = out[value_start..]
            .find(|character: char| !is_credential_character(character))
            .map(|offset| value_start + offset)
            .unwrap_or(out.len());
        if out[start..end].chars().count() >= minimum_chars {
            out.replace_range(start..end, redaction_marker);
            search_from = start + redaction_marker.len();
        } else {
            search_from = start + prefix.len();
        }
    }
}

fn mask_credential_shaped_value(
    out: &mut String,
    prefix: &str,
    minimum_chars: usize,
    mask_character: char,
) {
    let mut search_from = 0;
    while let Some(offset) = out[search_from..].find(prefix) {
        let start = search_from + offset;
        if !has_credential_left_boundary(out, start) {
            search_from = start + prefix.len();
            continue;
        }
        let value_start = start + prefix.len();
        let end = out[value_start..]
            .find(|character: char| !is_credential_character(character))
            .map(|offset| value_start + offset)
            .unwrap_or(out.len());
        if out[start..end].chars().count() >= minimum_chars {
            let replacement = mask_character
                .to_string()
                .repeat(out[start..end].chars().count());
            out.replace_range(start..end, &replacement);
            search_from = start + replacement.len();
        } else {
            search_from = start + prefix.len();
        }
    }
}

fn has_credential_left_boundary(value: &str, start: usize) -> bool {
    start == 0
        || value[..start]
            .chars()
            .next_back()
            .is_none_or(|character| !is_credential_left_context_character(character))
}

fn is_credential_left_context_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

fn is_credential_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(character, '-' | '_' | '.' | '~' | '+' | '/' | '=')
}

fn is_secret_key(key: &str) -> bool {
    let key = normalized_json_key(key);
    matches!(
        key.as_str(),
        "token" | "api_key" | "apikey" | "authorization" | "secret"
    ) || key.ends_with("_token")
        || key.ends_with("_api_key")
        || key.ends_with("_apikey")
        || key.ends_with("_authorization")
        || key.ends_with("_secret")
}

fn is_structural_metadata_key(key: &str) -> bool {
    let key = normalized_json_key(key);
    key == "id"
        || key.ends_with("_id")
        || key == "uid"
        || key.ends_with("_uid")
        || key == "uri"
        || key.ends_with("_uri")
        || key.ends_with("_uris")
        || matches!(
            key.as_str(),
            "tenant"
                | "owner_user"
                | "privacy"
                | "status"
                | "state"
                | "type"
                | "kind"
                | "role"
                | "roles"
                | "scope"
                | "mode"
                | "format"
                | "version"
                | "code"
                | "action"
                | "operation"
                | "outcome"
                | "category"
                | "decision"
                | "phase"
                | "method"
                | "model"
                | "provider"
                | "backend"
                | "relation"
                | "content_type"
                | "mime_type"
                | "node_kind"
                | "retrieval_role"
                | "index_kind"
                | "source_kind"
                | "block_type"
                | "created_by"
        )
        || key.ends_with("_type")
        || key.ends_with("_kind")
        || key.ends_with("_status")
        || key.ends_with("_role")
}

fn is_locator_key(key: &str) -> bool {
    let key = normalized_json_key(key);
    key == "uri" || key.ends_with("_uri") || key.ends_with("_uris")
}

fn is_fragment_sensitive_text_key(key: &str) -> bool {
    let key = normalized_json_key(key);
    matches!(
        key.as_str(),
        "body"
            | "content"
            | "text"
            | "snippet"
            | "quote"
            | "html"
            | "latex"
            | "caption"
            | "footnote"
            | "prompt"
            | "question"
            | "query"
            | "title"
            | "statement"
            | "rationale"
            | "evidence"
            | "evidence_text"
            | "raw_response_preview"
            | "answer"
            | "message"
            | "reason"
            | "description"
            | "summary"
            | "section_path"
            | "image_ref"
            | "tags"
    ) || key.ends_with("_body")
        || key.ends_with("_content")
        || key.ends_with("_text")
        || key.ends_with("_title")
        || key.ends_with("_rationale")
}

fn is_arbitrary_text_container_key(key: &str) -> bool {
    matches!(
        normalized_json_key(key).as_str(),
        "payload" | "value" | "values" | "metadata" | "details" | "data"
    )
}

fn unique_json_key(
    candidate: String,
    map: &serde_json::Map<String, Value>,
    matcher: &SecretMatcher<'_>,
    policy: ResponseTextPolicy,
) -> String {
    if !map.contains_key(&candidate) && !matcher.contains_configured_secret(&candidate) {
        return candidate;
    }
    for suffix in 2.. {
        // A uniqueness suffix can itself complete a configured secret (for
        // example `[REDACTED]` + `_2`). Re-run every synthesized key through
        // the same policy before it crosses the response boundary.
        let unique = matcher.redact_response_string(&format!("{candidate}_{suffix}"), policy);
        if !map.contains_key(&unique) && !matcher.contains_configured_secret(&unique) {
            return unique;
        }
    }
    unreachable!("an unused JSON object key suffix always exists")
}

fn normalized_json_key(key: &str) -> String {
    let mut normalized = String::with_capacity(key.len());
    let mut previous_was_lowercase_or_digit = false;
    for ch in key.chars() {
        if ch == '-' {
            normalized.push('_');
            previous_was_lowercase_or_digit = false;
        } else {
            if ch.is_ascii_uppercase() && previous_was_lowercase_or_digit {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
            previous_was_lowercase_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    normalized
}

#[cfg(test)]
mod redaction_tests {
    use super::{
        mask_secret_egress_projection_preserving_chars,
        mask_secret_fragment_projection_preserving_chars, mask_secrets_preserving_chars,
        redact_egress_text, redact_secrets, redact_string, StreamingTextRedactor, REDACTION_MARKER,
    };
    use serde_json::json;

    #[test]
    fn response_redaction_preserves_token_counts_and_codex_model_names() {
        let value = json!({
            "model": "gpt-5.3-codex-spark",
            "input_tokens": 12,
            "output_tokens": 7,
            "access_token": "private-access-token",
            "accessToken": "private-camel-access-token",
            "clientSecret": "private-camel-client-secret"
        });

        let redacted = redact_secrets(&value, &[]);
        assert_eq!(redacted["model"], "gpt-5.3-codex-spark");
        assert_eq!(redacted["input_tokens"], 12);
        assert_eq!(redacted["output_tokens"], 7);
        assert_eq!(redacted["access_token"], "[REDACTED]");
        assert_eq!(redacted["accessToken"], "[REDACTED]");
        assert_eq!(redacted["clientSecret"], "[REDACTED]");
    }

    #[test]
    fn credential_shaped_values_and_known_secrets_are_redacted() {
        let known = "configured-private-value".to_string();
        let input = format!(
            "model=gpt-5.3-codex-spark key=sk-test-secret-123456 auth=Bearer owner-token known={known}"
        );
        let redacted = redact_string(&input, &[known]);

        assert!(redacted.contains("gpt-5.3-codex-spark"));
        assert!(!redacted.contains("sk-test-secret-123456"));
        assert!(!redacted.contains("owner-token"));
        assert!(!redacted.contains("configured-private-value"));
    }

    #[test]
    fn overlapping_known_secrets_are_redacted_longest_first() {
        let redacted = redact_string(
            "short=abcd long=abcdefgh",
            &[
                "abcd".to_string(),
                "abcdefgh".to_string(),
                "abcd".to_string(),
                "wxyz".to_string(),
            ],
        );

        assert_eq!(redacted, "short=[REDACTED] long=[REDACTED]");
        assert!(!redacted.contains("def"));
    }

    #[test]
    fn response_redaction_uses_a_collision_free_marker() {
        for secret in ["REDA", "REDACTED", "[REDACTED]"] {
            let known_secrets = vec![secret.to_string()];
            let once = redact_string(&format!("configured={secret}"), &known_secrets);
            let twice = redact_string(&once, &known_secrets);
            let secret_key = redact_secrets(&json!({ "access_token": secret }), &known_secrets);
            let preexisting_marker = redact_egress_text(REDACTION_MARKER, &known_secrets);

            assert!(!once.contains(secret), "{once}");
            assert_eq!(twice, once);
            assert!(!secret_key.to_string().contains(secret), "{secret_key}");
            assert!(!preexisting_marker.contains(secret), "{preexisting_marker}");
        }
    }

    #[test]
    fn boundary_projection_preserves_existing_redaction_markers() {
        for secret in [
            "[REDACTED]-private-token".to_string(),
            "private-token-[REDACTED]".to_string(),
        ] {
            assert_eq!(
                redact_string(&secret, std::slice::from_ref(&secret)),
                "[REDACTED]",
                "{secret}"
            );
            let redacted = redact_secrets(
                &json!({ "body": "[REDACTED]" }),
                std::slice::from_ref(&secret),
            );
            assert_eq!(redacted["body"], "[REDACTED]", "{secret}");
            assert_eq!(
                redact_egress_text("[REDACTED]", std::slice::from_ref(&secret)),
                "[REDACTED]",
                "{secret}"
            );
            assert_eq!(
                mask_secret_egress_projection_preserving_chars(
                    "[REDACTED]",
                    std::slice::from_ref(&secret),
                ),
                "[REDACTED]",
                "{secret}"
            );
            assert_eq!(
                mask_secret_fragment_projection_preserving_chars(
                    "[REDACTED]",
                    std::slice::from_ref(&secret),
                ),
                "[REDACTED]",
                "{secret}"
            );
        }

        let containing_secret = "xxprefix [REDACTED] suffixyy".to_string();
        let partial = "prefix [REDACTED] suffix";
        assert_eq!(
            mask_secret_egress_projection_preserving_chars(
                partial,
                std::slice::from_ref(&containing_secret),
            ),
            "****** [REDACTED] ******"
        );
        assert_eq!(
            mask_secret_fragment_projection_preserving_chars(
                partial,
                std::slice::from_ref(&containing_secret),
            ),
            "****** [REDACTED] ******"
        );
    }

    #[test]
    fn response_redaction_masks_configured_secret_fragments_at_string_boundaries() {
        let secret = "zxqv-super-secret-admin-token-private-value".to_string();
        let split = 13;
        let left = format!("before {}", &secret[..split]);
        let right = format!("{} after", &secret[split..]);
        let value = json!({
            "left": { "body": left },
            "right": { "body": right }
        });

        let redacted = redact_secrets(&value, std::slice::from_ref(&secret));
        let redacted_left = redacted["left"]["body"].as_str().unwrap();
        let redacted_right = redacted["right"]["body"].as_str().unwrap();

        assert_eq!(redacted_left.chars().count(), left.chars().count());
        assert_eq!(redacted_right.chars().count(), right.chars().count());
        assert!(!redacted_left.contains(&secret[..split]));
        assert!(!redacted_right.contains(&secret[split..]));
        assert!(redacted_left.ends_with('*'));
        assert!(redacted_right.starts_with('*'));
    }

    #[test]
    fn response_boundary_masking_does_not_mutate_structural_identifiers() {
        let secret = "tenant-service-token".to_string();
        let mode_secret = "production-admin-token".to_string();
        let value = json!({
            "tenant_id": "test-tenant",
            "body": "fragment ends with tenant-service",
            "privacy": "private",
            "status": "active",
            "uri": "ctx://tenant/tenant-service",
            "source_document_uri": "ctx://tenant/tenant-service/source",
            "content": {
                "tenant_id": "tenant",
                "owner_user_id": "tenant-service",
                "mode": "production"
            }
        });

        let redacted = redact_secrets(&value, &[secret, mode_secret]);

        assert_eq!(redacted["tenant_id"], "test-tenant");
        assert_eq!(redacted["body"], "fragment ends with **************");
        assert_eq!(redacted["privacy"], "private");
        assert_eq!(redacted["status"], "active");
        assert_eq!(redacted["uri"], "ctx://tenant/tenant-service");
        assert_eq!(
            redacted["source_document_uri"],
            "ctx://tenant/tenant-service/source"
        );
        assert_eq!(redacted["content"]["tenant_id"], "tenant");
        assert_eq!(redacted["content"]["owner_user_id"], "tenant-service");
        assert_eq!(redacted["content"]["mode"], "production");
    }

    #[test]
    fn structural_locators_do_not_apply_generic_credential_shape_redaction() {
        let known_secret = "old-token-with-boundary-private-value".to_string();
        let value = json!({
            "uri": "ctx://docs/sk-management-guide",
            "source_document_uri": "ctx://guides/codex-migration-reference",
            "fragment_uris": ["ctx://docs/snippet-boundary-source"],
            "body": "credential sk-test-secret-123456 must still be removed"
        });

        let redacted = redact_secrets(&value, &[known_secret]);

        assert_eq!(redacted["uri"], "ctx://docs/sk-management-guide");
        assert_eq!(
            redacted["source_document_uri"],
            "ctx://guides/codex-migration-reference"
        );
        assert_eq!(
            redacted["fragment_uris"][0],
            "ctx://docs/snippet-boundary-source"
        );
        assert_eq!(
            redacted["body"],
            "credential [REDACTED] must still be removed"
        );
    }

    #[test]
    fn response_boundary_masking_recognizes_fragments_before_truncation_ellipsis() {
        let secret = "zxqv-super-secret-admin-token-private-value".to_string();
        let visible_prefix = &secret[..8];
        let value = json!({
            "body": format!("{}{}...", "x".repeat(490), visible_prefix)
        });

        let redacted = redact_secrets(&value, std::slice::from_ref(&secret));
        let body = redacted["body"].as_str().unwrap();

        assert!(!body.contains(visible_prefix));
        assert!(body.ends_with("********..."), "{body}");
    }

    #[test]
    fn response_redaction_sanitizes_secret_material_in_object_keys() {
        let secret = "zxqv-configured-secret-object-key".to_string();
        let value = json!({ secret.clone(): "safe value" });

        let redacted = redact_secrets(&value, std::slice::from_ref(&secret));
        let serialized = serde_json::to_string(&redacted).unwrap();

        assert!(!serialized.contains(&secret));
        assert_eq!(redacted["[REDACTED]"], "safe value");
    }

    #[test]
    fn response_redaction_preserves_colliding_secret_key_values() {
        let first = "zxqv-first-secret-object-key".to_string();
        let second = "zxqv-second-secret-object-key".to_string();
        let value = json!({
            first.clone(): "first value",
            second.clone(): "second value",
            "[REDACTED]": "literal value"
        });

        let redacted = redact_secrets(&value, &[first, second]);
        let values = redacted
            .as_object()
            .unwrap()
            .values()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(values.len(), 3);
        assert!(values.contains(&"first value"));
        assert!(values.contains(&"second value"));
        assert!(values.contains(&"literal value"));
    }

    #[test]
    fn response_key_collision_suffixes_cannot_recreate_a_configured_secret() {
        let secret = "[REDACTED]_2".to_string();
        let value = json!({
            "[REDACTED]": "literal value",
            secret.clone(): "secret-key value"
        });

        let redacted = redact_secrets(&value, std::slice::from_ref(&secret));
        let serialized = serde_json::to_string(&redacted).unwrap();
        let values = redacted
            .as_object()
            .unwrap()
            .values()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();

        assert!(!serialized.contains(&secret), "{serialized}");
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"literal value"));
        assert!(values.contains(&"secret-key value"));
    }

    #[test]
    fn redaction_rechecks_secrets_spanning_marker_boundaries() {
        let secrets = vec!["secret-key-one".to_string(), "ACTED]_2".to_string()];
        let value = json!({
            "[REDACTED]": "literal value",
            "secret-key-one": "secret-key value"
        });

        let redacted = redact_secrets(&value, &secrets);
        let serialized = serde_json::to_string(&redacted).unwrap();
        let synthesized = redact_string("secret-key-one_2", &secrets);
        let direct = redact_egress_text("[REDACTED]_2", &secrets);

        for secret in &secrets {
            assert!(!serialized.contains(secret), "{serialized}");
            assert!(!synthesized.contains(secret), "{synthesized}");
            assert!(!direct.contains(secret), "{direct}");
        }
        assert_eq!(redacted.as_object().unwrap().len(), 2);

        let repeated_suffix_secret = "TED]_".to_string();
        let repeated_suffix_secrets =
            vec!["secret-key-one".to_string(), repeated_suffix_secret.clone()];
        let repeated_suffix = redact_secrets(
            &json!({
                "[REDACTED]": "literal value",
                "secret-key-one": "secret-key value"
            }),
            &repeated_suffix_secrets,
        );
        let repeated_suffix_serialized = serde_json::to_string(&repeated_suffix).unwrap();
        assert!(
            !repeated_suffix_serialized.contains(&repeated_suffix_secret),
            "{repeated_suffix_serialized}"
        );
        assert_eq!(repeated_suffix.as_object().unwrap().len(), 2);

        let credential_boundary_secret = "ACTED]:2".to_string();
        let credential_projection = redact_egress_text(
            "sk-abcdefghijklmnop:2",
            std::slice::from_ref(&credential_boundary_secret),
        );
        assert!(
            !credential_projection.contains(&credential_boundary_secret),
            "{credential_projection}"
        );
    }

    #[test]
    fn preserving_character_masks_cannot_equal_a_configured_mask_run() {
        let secret = "********".to_string();
        let masked =
            mask_secret_egress_projection_preserving_chars(&secret, std::slice::from_ref(&secret));

        assert_eq!(masked.chars().count(), secret.chars().count());
        assert!(!masked.contains(&secret), "{masked}");
    }

    #[test]
    fn known_wire_keys_are_not_rewritten_by_secret_windows() {
        let secret = "codex-old-three-fragment-token".to_string();
        let redacted = redact_secrets(
            &json!({
                "fragment_uris": ["ctx://source/fragments/0001"],
                "status": "active"
            }),
            std::slice::from_ref(&secret),
        );

        assert!(redacted.get("fragment_uris").is_some(), "{redacted}");
        assert_eq!(redacted["status"], "active");
    }

    #[test]
    fn list_and_detail_shapes_apply_identical_field_policies() {
        let secret = "old-token-with-boundary-private-value".to_string();
        let detail = json!({
            "id": "change-1",
            "files": ["src/boundary.rs"],
            "title": "ends with boundary"
        });
        let list = json!([detail.clone()]);

        let redacted_detail = redact_secrets(&detail, std::slice::from_ref(&secret));
        let redacted_list = redact_secrets(&list, std::slice::from_ref(&secret));

        assert_eq!(redacted_list[0], redacted_detail);
        assert_eq!(redacted_detail["files"][0], "src/boundary.rs");
        assert_eq!(redacted_detail["title"], "ends with ********");
    }

    #[test]
    fn long_secret_many_fields_use_linear_precomputed_boundary_matching() {
        let secret = format!("zxqv-{}-private-tail", "a".repeat(4_080));
        let split = 2_048;
        let left = &secret[..split];
        let right = &secret[split..];
        let fields = (0..64)
            .flat_map(|index| {
                [
                    format!("field {index} ends with {left}"),
                    format!("{right} starts field {index}"),
                ]
            })
            .collect::<Vec<_>>();

        let redacted = redact_secrets(&json!({ "body": fields }), std::slice::from_ref(&secret));

        let fields = redacted["body"].as_array().unwrap();
        assert_eq!(fields.len(), 128);
        assert!(fields.iter().all(|field| {
            let field = field.as_str().unwrap();
            !field.contains(left) && !field.contains(right)
        }));
    }

    #[test]
    fn many_markers_with_an_absent_crossing_secret_stay_bounded() {
        let secret = format!("ACTED]{}", "x".repeat(4_096));
        let input = "[REDACTED]safe|".repeat(10_000);

        let redacted = redact_egress_text(&input, std::slice::from_ref(&secret));

        assert_eq!(redacted, input);
    }

    #[test]
    fn response_redaction_masks_fragments_in_user_controlled_fields() {
        let secret = "zxqv-super-secret-admin-token-private-value".to_string();
        let left = &secret[..15];
        let middle = &secret[15..29];
        let right = &secret[29..];
        let value = json!({
            "title": left,
            "rationale": middle,
            "image_ref": right,
            "uri": "ctx://document/stable-source",
            "payload": { "arbitrary": middle },
            format!("prefix-{right}"): "safe"
        });

        let serialized =
            serde_json::to_string(&redact_secrets(&value, std::slice::from_ref(&secret))).unwrap();

        assert!(!serialized.contains(left));
        assert!(!serialized.contains(middle));
        assert!(!serialized.contains(right));
    }

    #[test]
    fn projection_masks_short_whole_fragments_and_long_secret_windows() {
        let short = "abcdefghij".to_string();
        let long = format!("{}middle-secret-window{}", "a".repeat(300), "z".repeat(300));

        let short_masked =
            mask_secret_fragment_projection_preserving_chars("defg", std::slice::from_ref(&short));
        let window_masked = redact_egress_text(
            "provider context contains middle-secret-window internally",
            std::slice::from_ref(&long),
        );

        assert_eq!(short_masked, "****");
        assert!(!window_masked.contains("middle-secret-window"));
    }

    #[test]
    fn very_short_secrets_only_redact_whole_values_without_amplification() {
        let secret = "秘密秘".to_string();
        let redacted = redact_secrets(
            &json!({
                "tenant_id": secret,
                "message": "ordinary text remains intact"
            }),
            &["秘密秘".to_string()],
        );

        assert_eq!(redacted["tenant_id"], "[REDACTED]");
        assert_eq!(redacted["message"], "ordinary text remains intact");
    }

    #[test]
    fn one_to_three_character_free_text_is_not_treated_as_a_secret_fragment() {
        let secret = "admin-root-token".to_string();
        let redacted = redact_secrets(
            &json!({
                "title": "a",
                "message": "in",
                "body": "adm"
            }),
            std::slice::from_ref(&secret),
        );

        assert_eq!(redacted["title"], "a");
        assert_eq!(redacted["message"], "in");
        assert_eq!(redacted["body"], "adm");
    }

    #[test]
    fn credential_shape_detection_requires_token_boundaries() {
        let input = "task-management uses disk-backed storage";

        assert_eq!(redact_string(input, &[]), input);
        assert_eq!(mask_secrets_preserving_chars(input, &[]), input);
    }

    #[test]
    fn retrieval_masking_preserves_character_offsets_and_hides_generic_credentials() {
        let configured = "秘密 configured value".to_string();
        let input = format!("before {configured} middle sk-test-secret-123456 after");

        let masked = mask_secrets_preserving_chars(&input, std::slice::from_ref(&configured));

        assert_eq!(masked.chars().count(), input.chars().count());
        assert!(!masked.contains(&configured));
        assert!(!masked.contains("sk-test-secret-123456"));
        assert!(masked.contains(' '));
    }

    fn stream_at_split(input: &str, secrets: &[String], split: usize) -> String {
        let mut redactor = StreamingTextRedactor::new(secrets);
        let mut output = redactor.push(&input[..split]);
        output.push_str(&redactor.push(&input[split..]));
        output.push_str(&redactor.finish());
        output
    }

    fn character_boundaries(input: &str) -> Vec<usize> {
        input
            .char_indices()
            .map(|(index, _)| index)
            .chain(std::iter::once(input.len()))
            .collect()
    }

    #[test]
    fn streaming_redactor_matches_configured_secrets_at_every_split_point() {
        let secret = "秘密🔐-configured-private-token".to_string();
        let input = format!("unicode before {secret} after ✅");
        let expected =
            mask_secret_egress_projection_preserving_chars(&input, std::slice::from_ref(&secret));

        for split in character_boundaries(&input) {
            let output = stream_at_split(&input, std::slice::from_ref(&secret), split);
            assert_eq!(output, expected, "split at byte {split}");
            assert!(!output.contains(&secret), "split at byte {split}: {output}");
        }
    }

    #[test]
    fn streaming_redactor_uses_bounded_windows_for_long_secret_projection() {
        let secret = format!(
            "{}middle-secret-window{}",
            "前".repeat(128),
            "後".repeat(128)
        );
        let exposed_window = "middle-secret-window";
        let input = format!("provider returned {exposed_window} in otherwise safe text");
        let expected =
            mask_secret_egress_projection_preserving_chars(&input, std::slice::from_ref(&secret));
        let mut redactor = StreamingTextRedactor::new(std::slice::from_ref(&secret));
        let mut output = String::new();

        for character in input.chars() {
            output.push_str(&redactor.push(&character.to_string()));
            assert!(!output.contains(exposed_window), "{output}");
            assert!(redactor.pending.len() <= 7);
        }
        output.push_str(&redactor.finish());

        assert_eq!(output, expected);
        assert!(!output.contains(exposed_window));
    }

    #[test]
    fn streaming_redactor_masks_overlapping_secrets_without_reconstruction() {
        let secrets = vec![
            "abcdefghi".to_string(),
            "bcdefghij".to_string(),
            "abcdefghi".to_string(),
            String::new(),
        ];
        let input = "prefix abcdefghij suffix";
        let mut redactor = StreamingTextRedactor::new(&secrets);
        let mut output = String::new();

        for character in input.chars() {
            output.push_str(&redactor.push(&character.to_string()));
            assert!(secrets
                .iter()
                .filter(|secret| !secret.is_empty())
                .all(|secret| !output.contains(secret)));
        }
        output.push_str(&redactor.finish());

        assert_eq!(output.chars().count(), input.chars().count());
        assert_eq!(output, "prefix ********** suffix");
        assert!(!output.contains("abcdefghi"));
        assert!(!output.contains("bcdefghij"));
    }

    #[test]
    fn streaming_redactor_masks_every_credential_shape_at_every_split_point() {
        let credentials = [
            "sk-123456789abcdef",
            "sess-12345678901abcdef",
            "codex-12345678901234abcdef",
            "oaic-12345678901abcdef",
            "Bearer abcdefghijklmnop",
        ];
        let input = credentials.join(" | ");
        let expected = mask_secret_egress_projection_preserving_chars(&input, &[]);

        for split in character_boundaries(&input) {
            let output = stream_at_split(&input, &[], split);
            assert_eq!(output, expected, "split at byte {split}");
            for credential in credentials {
                assert!(
                    !output.contains(credential),
                    "split at byte {split}: {output}"
                );
            }
        }
    }

    #[test]
    fn streaming_redactor_preserves_incomplete_and_non_boundary_credentials() {
        let input = "sk-12345678 task-sk-123456789abcdef sess-short";
        let mut redactor = StreamingTextRedactor::new(&[]);
        assert_eq!(redactor.push("sk-"), "");
        assert_eq!(redactor.push("12345678"), "");
        let mut output = redactor.push(" task-sk-123456789abcdef sess-short");
        output.push_str(&redactor.finish());

        assert_eq!(output, input);
    }

    #[test]
    fn streaming_redactor_uses_collision_free_masks_for_markers_and_mask_runs() {
        let secrets = vec![
            "********".to_string(),
            "[REDACTED]_2".to_string(),
            "ACTED]_2".to_string(),
            "REDA".to_string(),
        ];
        let input = "******** [REDACTED]_2 [REDACTED] credential sk-123456789abcdef";
        let mut redactor = StreamingTextRedactor::new(&secrets);
        let mut output = String::new();

        for character in input.chars() {
            output.push_str(&redactor.push(&character.to_string()));
            for secret in &secrets {
                assert!(!output.contains(secret), "{output}");
            }
            assert!(!output.contains("sk-123456789abcdef"), "{output}");
        }
        output.push_str(&redactor.finish());

        for secret in &secrets {
            assert!(!output.contains(secret), "{output}");
        }
        assert!(!output.contains("sk-123456789abcdef"), "{output}");
    }

    #[test]
    fn streaming_redactor_retains_only_the_longest_completable_suffix() {
        let secrets = vec!["secret-value".to_string()];
        let mut redactor = StreamingTextRedactor::new(&secrets);

        assert_eq!(redactor.push("ordinary sec"), "ordinary ");
        assert_eq!(redactor.pending.len(), 3);
        assert_eq!(redactor.push("ular "), "secular ");
        assert!(redactor.pending.is_empty());

        assert_eq!(redactor.push("Bearer "), "");
        assert_eq!(redactor.pending.len(), 7);
        redactor.abort();
    }
}
