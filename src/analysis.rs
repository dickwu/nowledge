//! Strict, fail-closed validation for analysis-provider output.
//!
//! The provider is an untrusted proposal generator. This module deliberately
//! has no tenant or owner fields: callers must supply an allowlist assembled
//! from context resources they already authorized through the server-side ACL.

use std::{collections::HashSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::util::sanitize_slug;

pub const MAX_ANALYSIS_RESPONSE_BYTES: usize = 256 * 1024;
pub const MAX_LINK_CANDIDATES: usize = 32;
pub const MAX_INSIGHT_CANDIDATES: usize = 16;
pub const MAX_SOURCE_URIS_PER_INSIGHT: usize = 16;
pub const MAX_TAGS_PER_CANDIDATE: usize = 64;
pub const MAX_URI_BYTES: usize = 2 * 1024;
pub const MAX_RELATION_BYTES: usize = 64;
pub const MAX_INSIGHT_TYPE_BYTES: usize = 64;
pub const MAX_TITLE_BYTES: usize = 256;
pub const MAX_STATEMENT_BYTES: usize = 8 * 1024;
pub const MAX_RATIONALE_BYTES: usize = 2 * 1024;
pub const MAX_TAG_BYTES: usize = 128;

/// Relations that an analysis provider may propose for materialization.
///
/// `part_of` is intentionally absent because it is reserved for server-created
/// source provenance links.
pub const ALLOWED_ANALYSIS_RELATIONS: &[&str] = &["related", "supports"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Link,
    Insight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRejectionCode {
    InvalidCandidateSchema,
    EmptyField,
    FieldTooLong,
    InvalidUri,
    UnauthorizedUri,
    SelfLink,
    UnsupportedRelation,
    InvalidScore,
    MissingEvidenceUri,
    TooManySourceUris,
    TooManyTags,
    InvalidTag,
    DuplicateCandidate,
}

/// Safe admin-debug diagnostic. It contains only the candidate location and a
/// stable code; provider values and parser error strings are never retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CandidateRejection {
    pub kind: CandidateKind,
    pub index: usize,
    pub code: CandidateRejectionCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisOutputErrorCode {
    ResponseTooLarge,
    InvalidJson,
    InvalidEnvelope,
    CandidateLimitExceeded,
}

/// Fatal response-level validation failure. Candidate limit failures identify
/// only the kind and first disallowed index, never the supplied value or count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AnalysisOutputError {
    pub code: AnalysisOutputErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<CandidateKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,
}

impl AnalysisOutputError {
    fn response(code: AnalysisOutputErrorCode) -> Self {
        Self {
            code,
            kind: None,
            index: None,
        }
    }

    fn candidate_limit(kind: CandidateKind, index: usize) -> Self {
        Self {
            code: AnalysisOutputErrorCode::CandidateLimitExceeded,
            kind: Some(kind),
            index: Some(index),
        }
    }
}

impl fmt::Display for AnalysisOutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("analysis provider output failed validation")
    }
}

impl Error for AnalysisOutputError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedLinkCandidate {
    pub source_uri: String,
    pub target_uri: String,
    pub relation: String,
    pub rationale: Option<String>,
    pub confidence: f32,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedInsightCandidate {
    pub insight_type: String,
    pub title: String,
    pub statement: String,
    pub confidence: f32,
    pub salience: f32,
    pub source_uris: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValidatedAnalysisOutput {
    pub links: Vec<ValidatedLinkCandidate>,
    pub insights: Vec<ValidatedInsightCandidate>,
    pub rejections: Vec<CandidateRejection>,
}

/// Canonical, server-authorized ContextFS URI set used for exact candidate
/// membership checks. Construct this only from ACL-filtered context resources
/// and caller seed URIs that were successfully resolved through the ACL.
#[derive(Debug, Clone, Default)]
pub struct AnalysisUriAllowlist {
    uris: HashSet<String>,
}

impl AnalysisUriAllowlist {
    pub fn from_authorized<I, S>(uris: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            uris: uris
                .into_iter()
                .filter_map(|uri| canonicalize_analysis_uri(uri.as_ref()))
                .collect(),
        }
    }

    pub fn contains(&self, canonical_uri: &str) -> bool {
        self.uris.contains(canonical_uri)
    }

    pub fn is_empty(&self) -> bool {
        self.uris.is_empty()
    }

    pub fn len(&self) -> usize {
        self.uris.len()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderEnvelope {
    links: Vec<Value>,
    insights: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderLinkCandidate {
    source_uri: String,
    target_uri: String,
    relation: String,
    rationale: Option<String>,
    confidence: f64,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderInsightCandidate {
    insight_type: String,
    title: String,
    statement: String,
    confidence: f64,
    salience: f64,
    source_uris: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
}

/// Parses and validates one untrusted provider response.
///
/// Response-level shape and count failures are fatal. Individual malformed or
/// unauthorized candidates are omitted and represented by safe, index-only
/// diagnostics. Valid candidates preserve provider order; duplicate persisted
/// identities use a deterministic first-valid-candidate-wins policy.
pub fn validate_analysis_output(
    response: &str,
    allowed_uris: &AnalysisUriAllowlist,
) -> Result<ValidatedAnalysisOutput, AnalysisOutputError> {
    if response.len() > MAX_ANALYSIS_RESPONSE_BYTES {
        return Err(AnalysisOutputError::response(
            AnalysisOutputErrorCode::ResponseTooLarge,
        ));
    }

    let value = serde_json::from_str::<Value>(response)
        .map_err(|_| AnalysisOutputError::response(AnalysisOutputErrorCode::InvalidJson))?;
    let envelope = serde_json::from_value::<ProviderEnvelope>(value)
        .map_err(|_| AnalysisOutputError::response(AnalysisOutputErrorCode::InvalidEnvelope))?;

    if envelope.links.len() > MAX_LINK_CANDIDATES {
        return Err(AnalysisOutputError::candidate_limit(
            CandidateKind::Link,
            MAX_LINK_CANDIDATES,
        ));
    }
    if envelope.insights.len() > MAX_INSIGHT_CANDIDATES {
        return Err(AnalysisOutputError::candidate_limit(
            CandidateKind::Insight,
            MAX_INSIGHT_CANDIDATES,
        ));
    }

    let mut output = ValidatedAnalysisOutput::default();
    let mut seen_links = HashSet::new();
    let mut seen_insights = HashSet::new();

    for (index, value) in envelope.links.into_iter().enumerate() {
        let candidate = match serde_json::from_value::<ProviderLinkCandidate>(value) {
            Ok(candidate) => candidate,
            Err(_) => {
                output.rejections.push(CandidateRejection {
                    kind: CandidateKind::Link,
                    index,
                    code: CandidateRejectionCode::InvalidCandidateSchema,
                });
                continue;
            }
        };

        match validate_link_candidate(candidate, allowed_uris) {
            Ok(candidate) => {
                let key = (
                    candidate.source_uri.clone(),
                    candidate.target_uri.clone(),
                    candidate.relation.clone(),
                );
                if seen_links.insert(key) {
                    output.links.push(candidate);
                } else {
                    output.rejections.push(CandidateRejection {
                        kind: CandidateKind::Link,
                        index,
                        code: CandidateRejectionCode::DuplicateCandidate,
                    });
                }
            }
            Err(code) => output.rejections.push(CandidateRejection {
                kind: CandidateKind::Link,
                index,
                code,
            }),
        }
    }

    for (index, value) in envelope.insights.into_iter().enumerate() {
        let candidate = match serde_json::from_value::<ProviderInsightCandidate>(value) {
            Ok(candidate) => candidate,
            Err(_) => {
                output.rejections.push(CandidateRejection {
                    kind: CandidateKind::Insight,
                    index,
                    code: CandidateRejectionCode::InvalidCandidateSchema,
                });
                continue;
            }
        };

        match validate_insight_candidate(candidate, allowed_uris) {
            Ok(candidate) => {
                let key = analysis_insight_context_uri(&candidate.insight_type, &candidate.title);
                if seen_insights.insert(key) {
                    output.insights.push(candidate);
                } else {
                    output.rejections.push(CandidateRejection {
                        kind: CandidateKind::Insight,
                        index,
                        code: CandidateRejectionCode::DuplicateCandidate,
                    });
                }
            }
            Err(code) => output.rejections.push(CandidateRejection {
                kind: CandidateKind::Insight,
                index,
                code,
            }),
        }
    }

    Ok(output)
}

/// Applies the same layer-suffix equivalence already used by ContextFS reads
/// and link persistence, then validates the bounded `ctx://` locator shape.
pub fn canonicalize_analysis_uri(uri: &str) -> Option<String> {
    normalize_candidate_uri(uri).ok()
}

/// Exact natural identity used by durable insight ContextFS projections.
pub fn analysis_insight_context_uri(insight_type: &str, title: &str) -> String {
    format!(
        "ctx://user/insights/{}/{}",
        sanitize_slug(insight_type),
        sanitize_slug(title)
    )
}

fn validate_link_candidate(
    candidate: ProviderLinkCandidate,
    allowed_uris: &AnalysisUriAllowlist,
) -> Result<ValidatedLinkCandidate, CandidateRejectionCode> {
    let source_uri = normalize_candidate_uri(&candidate.source_uri)?;
    let target_uri = normalize_candidate_uri(&candidate.target_uri)?;

    if !allowed_uris.contains(&source_uri) || !allowed_uris.contains(&target_uri) {
        return Err(CandidateRejectionCode::UnauthorizedUri);
    }
    if source_uri == target_uri {
        return Err(CandidateRejectionCode::SelfLink);
    }

    let relation = normalize_relation(&candidate.relation)?;
    let rationale = normalize_optional_field(candidate.rationale, MAX_RATIONALE_BYTES)?;
    let confidence = validate_score(candidate.confidence)?;
    let tags = normalize_tags(candidate.tags)?;

    Ok(ValidatedLinkCandidate {
        source_uri,
        target_uri,
        relation,
        rationale,
        confidence,
        tags,
    })
}

fn validate_insight_candidate(
    candidate: ProviderInsightCandidate,
    allowed_uris: &AnalysisUriAllowlist,
) -> Result<ValidatedInsightCandidate, CandidateRejectionCode> {
    let insight_type = normalize_required_field(candidate.insight_type, MAX_INSIGHT_TYPE_BYTES)?;
    let title = normalize_required_field(candidate.title, MAX_TITLE_BYTES)?;
    let statement = normalize_required_field(candidate.statement, MAX_STATEMENT_BYTES)?;
    let confidence = validate_score(candidate.confidence)?;
    let salience = validate_score(candidate.salience)?;

    if candidate.source_uris.is_empty() {
        return Err(CandidateRejectionCode::MissingEvidenceUri);
    }
    if candidate.source_uris.len() > MAX_SOURCE_URIS_PER_INSIGHT {
        return Err(CandidateRejectionCode::TooManySourceUris);
    }

    let mut source_uris = Vec::with_capacity(candidate.source_uris.len());
    let mut seen_source_uris = HashSet::new();
    for source_uri in candidate.source_uris {
        let source_uri = normalize_candidate_uri(&source_uri)?;
        if !allowed_uris.contains(&source_uri) {
            return Err(CandidateRejectionCode::UnauthorizedUri);
        }
        if seen_source_uris.insert(source_uri.clone()) {
            source_uris.push(source_uri);
        }
    }
    if source_uris.is_empty() {
        return Err(CandidateRejectionCode::MissingEvidenceUri);
    }

    let tags = normalize_tags(candidate.tags)?;

    Ok(ValidatedInsightCandidate {
        insight_type,
        title,
        statement,
        confidence,
        salience,
        source_uris,
        tags,
    })
}

fn normalize_candidate_uri(uri: &str) -> Result<String, CandidateRejectionCode> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(CandidateRejectionCode::EmptyField);
    }
    if uri.len() > MAX_URI_BYTES {
        return Err(CandidateRejectionCode::FieldTooLong);
    }
    if !uri.starts_with("ctx://")
        || uri.len() == "ctx://".len()
        || uri.chars().any(char::is_whitespace)
        || uri.chars().any(char::is_control)
    {
        return Err(CandidateRejectionCode::InvalidUri);
    }

    let canonical = uri
        .strip_suffix("/.abstract")
        .or_else(|| uri.strip_suffix("/.overview"))
        .or_else(|| uri.strip_suffix("/detail"))
        .or_else(|| uri.strip_suffix("/chunks/0001"))
        .unwrap_or(uri);
    if canonical.len() == "ctx://".len() {
        return Err(CandidateRejectionCode::InvalidUri);
    }
    Ok(canonical.to_string())
}

fn normalize_relation(relation: &str) -> Result<String, CandidateRejectionCode> {
    let relation = relation.trim();
    if relation.is_empty() {
        return Err(CandidateRejectionCode::EmptyField);
    }
    if relation.len() > MAX_RELATION_BYTES {
        return Err(CandidateRejectionCode::FieldTooLong);
    }

    let normalized = relation.to_ascii_lowercase();
    if !normalized.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
    }) || !ALLOWED_ANALYSIS_RELATIONS.contains(&normalized.as_str())
    {
        return Err(CandidateRejectionCode::UnsupportedRelation);
    }
    Ok(normalized)
}

fn normalize_required_field(
    value: String,
    max_bytes: usize,
) -> Result<String, CandidateRejectionCode> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CandidateRejectionCode::EmptyField);
    }
    if value.len() > max_bytes {
        return Err(CandidateRejectionCode::FieldTooLong);
    }
    Ok(value.to_string())
}

fn normalize_optional_field(
    value: Option<String>,
    max_bytes: usize,
) -> Result<Option<String>, CandidateRejectionCode> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > max_bytes {
        return Err(CandidateRejectionCode::FieldTooLong);
    }
    Ok(Some(value.to_string()))
}

fn normalize_tags(tags: Vec<String>) -> Result<Vec<String>, CandidateRejectionCode> {
    if tags.len() > MAX_TAGS_PER_CANDIDATE {
        return Err(CandidateRejectionCode::TooManyTags);
    }

    let mut normalized = Vec::with_capacity(tags.len());
    let mut seen = HashSet::new();
    for tag in tags {
        let tag = tag.trim();
        if tag.is_empty() {
            return Err(CandidateRejectionCode::InvalidTag);
        }
        if tag.len() > MAX_TAG_BYTES {
            return Err(CandidateRejectionCode::FieldTooLong);
        }
        if tag.chars().any(char::is_control) {
            return Err(CandidateRejectionCode::InvalidTag);
        }
        if seen.insert(tag.to_string()) {
            normalized.push(tag.to_string());
        }
    }
    Ok(normalized)
}

fn validate_score(score: f64) -> Result<f32, CandidateRejectionCode> {
    if !score.is_finite() || !(0.0..=1.0).contains(&score) {
        return Err(CandidateRejectionCode::InvalidScore);
    }
    Ok(score as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn allowlist() -> AnalysisUriAllowlist {
        AnalysisUriAllowlist::from_authorized([
            "ctx://tenant/a",
            "ctx://tenant/b",
            "ctx://tenant/c",
        ])
    }

    fn valid_link() -> Value {
        json!({
            "source_uri": "ctx://tenant/a",
            "target_uri": "ctx://tenant/b",
            "relation": "related",
            "rationale": "Grounded by both contexts",
            "confidence": 0.75
        })
    }

    fn valid_insight() -> Value {
        json!({
            "insight_type": "analysis",
            "title": "Grounded insight",
            "statement": "The evidence supports the insight.",
            "confidence": 0.8,
            "salience": 0.6,
            "source_uris": ["ctx://tenant/a"]
        })
    }

    fn response(links: Vec<Value>, insights: Vec<Value>) -> String {
        json!({ "links": links, "insights": insights }).to_string()
    }

    #[test]
    fn strict_schema_rejects_unknown_envelope_and_candidate_fields() {
        let err = validate_analysis_output(
            &json!({ "links": [], "insights": [], "owner_user_id": "attacker" }).to_string(),
            &allowlist(),
        )
        .unwrap_err();
        assert_eq!(err.code, AnalysisOutputErrorCode::InvalidEnvelope);

        let mut link = valid_link();
        link["tenant_id"] = json!("attacker");
        let output = validate_analysis_output(&response(vec![link], vec![]), &allowlist())
            .expect("candidate rejection is not a response-level failure");
        assert!(output.links.is_empty());
        assert_eq!(
            output.rejections,
            vec![CandidateRejection {
                kind: CandidateKind::Link,
                index: 0,
                code: CandidateRejectionCode::InvalidCandidateSchema,
            }]
        );
    }

    #[test]
    fn exact_allowlist_rejects_prefix_spoofs_and_accepts_canonical_layers() {
        let links = vec![
            valid_link(),
            json!({
                "source_uri": "ctx://tenant/a.evil",
                "target_uri": "ctx://tenant/b",
                "relation": "related",
                "rationale": null,
                "confidence": 0.5
            }),
            json!({
                "source_uri": "ctx://tenant/a/.abstract",
                "target_uri": "ctx://tenant/c/chunks/0001",
                "relation": "SUPPORTS",
                "rationale": null,
                "confidence": 1.0
            }),
        ];
        let output = validate_analysis_output(&response(links, vec![]), &allowlist()).unwrap();

        assert_eq!(output.links.len(), 2);
        assert_eq!(output.links[1].source_uri, "ctx://tenant/a");
        assert_eq!(output.links[1].target_uri, "ctx://tenant/c");
        assert_eq!(output.links[1].relation, "supports");
        assert_eq!(
            output.rejections,
            vec![CandidateRejection {
                kind: CandidateKind::Link,
                index: 1,
                code: CandidateRejectionCode::UnauthorizedUri,
            }]
        );
    }

    #[test]
    fn self_links_reserved_relations_and_invalid_scores_are_rejected() {
        let cases = [
            (
                json!({
                    "source_uri": "ctx://tenant/a",
                    "target_uri": "ctx://tenant/a/.overview",
                    "relation": "related",
                    "rationale": null,
                    "confidence": 0.5
                }),
                CandidateRejectionCode::SelfLink,
            ),
            (
                json!({
                    "source_uri": "ctx://tenant/a",
                    "target_uri": "ctx://tenant/b",
                    "relation": "part_of",
                    "rationale": null,
                    "confidence": 0.5
                }),
                CandidateRejectionCode::UnsupportedRelation,
            ),
            (
                json!({
                    "source_uri": "ctx://tenant/a",
                    "target_uri": "ctx://tenant/b",
                    "relation": "related",
                    "rationale": null,
                    "confidence": 1.01
                }),
                CandidateRejectionCode::InvalidScore,
            ),
        ];

        for (index, (candidate, code)) in cases.into_iter().enumerate() {
            let output =
                validate_analysis_output(&response(vec![candidate], vec![]), &allowlist()).unwrap();
            assert_eq!(output.links.len(), 0, "case {index}");
            assert_eq!(output.rejections[0].code, code, "case {index}");
        }
        assert_eq!(
            validate_score(f64::NAN),
            Err(CandidateRejectionCode::InvalidScore)
        );
        assert_eq!(
            validate_score(f64::INFINITY),
            Err(CandidateRejectionCode::InvalidScore)
        );
    }

    #[test]
    fn duplicate_links_are_first_valid_candidate_wins_and_directional() {
        let mut duplicate = valid_link();
        duplicate["rationale"] = json!("Second proposal");
        let reversed = json!({
            "source_uri": "ctx://tenant/b",
            "target_uri": "ctx://tenant/a",
            "relation": "related",
            "rationale": null,
            "confidence": 0.4
        });
        let output = validate_analysis_output(
            &response(vec![valid_link(), duplicate, reversed], vec![]),
            &allowlist(),
        )
        .unwrap();

        assert_eq!(output.links.len(), 2);
        assert_eq!(
            output.links[0].rationale.as_deref(),
            Some("Grounded by both contexts")
        );
        assert_eq!(
            output.rejections,
            vec![CandidateRejection {
                kind: CandidateKind::Link,
                index: 1,
                code: CandidateRejectionCode::DuplicateCandidate,
            }]
        );
    }

    #[test]
    fn colliding_insight_context_identities_are_first_valid_candidate_wins() {
        let first = valid_insight();
        let mut collision = valid_insight();
        collision["insight_type"] = json!("Analysis");
        collision["title"] = json!("grounded-insight");
        collision["statement"] = json!("A second proposal must not overwrite the first.");

        let output =
            validate_analysis_output(&response(vec![], vec![first, collision]), &allowlist())
                .unwrap();

        assert_eq!(output.insights.len(), 1);
        assert_eq!(
            output.insights[0].statement,
            "The evidence supports the insight."
        );
        assert_eq!(
            output.rejections,
            vec![CandidateRejection {
                kind: CandidateKind::Insight,
                index: 1,
                code: CandidateRejectionCode::DuplicateCandidate,
            }]
        );
    }

    #[test]
    fn an_unauthorized_insight_source_rejects_the_whole_candidate() {
        let mut insight = valid_insight();
        insight["source_uris"] = json!([
            "ctx://tenant/a",
            "ctx://tenant/private",
            "ctx://tenant/a/.abstract"
        ]);
        let output =
            validate_analysis_output(&response(vec![], vec![insight]), &allowlist()).unwrap();

        assert!(output.insights.is_empty());
        assert_eq!(
            output.rejections,
            vec![CandidateRejection {
                kind: CandidateKind::Insight,
                index: 0,
                code: CandidateRejectionCode::UnauthorizedUri,
            }]
        );
    }

    #[test]
    fn insight_evidence_and_tags_are_bounded_and_deduplicated() {
        let mut valid = valid_insight();
        valid["source_uris"] = json!([
            "ctx://tenant/a",
            "ctx://tenant/a/.abstract",
            "ctx://tenant/b"
        ]);
        valid["tags"] = json!(["grounded", "grounded", "reviewed"]);
        let mut missing_evidence = valid_insight();
        missing_evidence["source_uris"] = json!([]);
        let mut too_many_sources = valid_insight();
        too_many_sources["source_uris"] = Value::Array(
            (0..=MAX_SOURCE_URIS_PER_INSIGHT)
                .map(|_| json!("ctx://tenant/a"))
                .collect(),
        );
        let mut too_many_tags = valid_insight();
        too_many_tags["tags"] = Value::Array(
            (0..=MAX_TAGS_PER_CANDIDATE)
                .map(|index| json!(format!("tag-{index}")))
                .collect(),
        );

        let output = validate_analysis_output(
            &response(
                vec![],
                vec![valid, missing_evidence, too_many_sources, too_many_tags],
            ),
            &allowlist(),
        )
        .unwrap();

        assert_eq!(output.insights.len(), 1);
        assert_eq!(
            output.insights[0].source_uris,
            vec!["ctx://tenant/a", "ctx://tenant/b"]
        );
        assert_eq!(output.insights[0].tags, vec!["grounded", "reviewed"]);
        assert_eq!(
            output
                .rejections
                .iter()
                .map(|rejection| rejection.code)
                .collect::<Vec<_>>(),
            vec![
                CandidateRejectionCode::MissingEvidenceUri,
                CandidateRejectionCode::TooManySourceUris,
                CandidateRejectionCode::TooManyTags,
            ]
        );
    }

    #[test]
    fn response_candidate_and_field_limits_fail_closed() {
        let oversized = " ".repeat(MAX_ANALYSIS_RESPONSE_BYTES + 1);
        assert_eq!(
            validate_analysis_output(&oversized, &allowlist())
                .unwrap_err()
                .code,
            AnalysisOutputErrorCode::ResponseTooLarge
        );

        let too_many_links = response(
            (0..=MAX_LINK_CANDIDATES).map(|_| valid_link()).collect(),
            vec![],
        );
        let err = validate_analysis_output(&too_many_links, &allowlist()).unwrap_err();
        assert_eq!(err.code, AnalysisOutputErrorCode::CandidateLimitExceeded);
        assert_eq!(err.kind, Some(CandidateKind::Link));
        assert_eq!(err.index, Some(MAX_LINK_CANDIDATES));

        let mut long_title = valid_insight();
        long_title["title"] = json!("x".repeat(MAX_TITLE_BYTES + 1));
        let output =
            validate_analysis_output(&response(vec![], vec![long_title]), &allowlist()).unwrap();
        assert_eq!(
            output.rejections[0].code,
            CandidateRejectionCode::FieldTooLong
        );
    }

    #[test]
    fn diagnostics_serialize_without_provider_values_or_parser_causes() {
        let diagnostic = CandidateRejection {
            kind: CandidateKind::Link,
            index: 7,
            code: CandidateRejectionCode::UnauthorizedUri,
        };
        assert_eq!(
            serde_json::to_value(diagnostic).unwrap(),
            json!({
                "kind": "link",
                "index": 7,
                "code": "unauthorized_uri"
            })
        );
    }

    #[test]
    fn strict_envelope_requires_both_candidate_arrays() {
        for value in [json!({}), json!({ "links": [] }), json!({ "insights": [] })] {
            let err = validate_analysis_output(&value.to_string(), &allowlist()).unwrap_err();
            assert_eq!(err.code, AnalysisOutputErrorCode::InvalidEnvelope);
        }
    }
}
