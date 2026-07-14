//! Fuzzy document matching backed by the `turbovec` quantized vector index.
//!
//! `util::text_score` only rewards exact substring containment, so inflected
//! or reordered queries ("deployment pipelines" vs "deploy pipeline") miss
//! entirely. This module adds a deterministic lexical embedding — signed
//! feature hashing over word unigrams, word bigrams, and boundary-marked
//! character trigrams with log-TF weighting — stored in a
//! `turbovec::IdMapIndex` (TurboQuant, 4-bit). Cosine-style scores from the
//! index are blended with `text_score` by the store's ranking paths.
//!
//! Isolation: the matcher itself is scope-blind. Callers must key entries
//! with [`scoped key`](crate::store) material that already encodes the
//! resolver-derived index UID, and must pass only isolation-filtered
//! candidates; scoring is restricted to that candidate set via turbovec's
//! allowlist search, so foreign entries can never surface.

use std::collections::{HashMap, HashSet};

use turbovec::IdMapIndex;

use crate::{config::Config, error::safe_cause_diagnostic};

/// Embedding dimensionality. turbovec requires a positive multiple of 8;
/// 512 keeps hashed lexical features sparse enough while the quantized
/// index stores each entry in 256 bytes.
pub const VECTOR_DIM: usize = 512;
/// 4-bit quantization — best recall of turbovec's supported widths.
const VECTOR_BIT_WIDTH: usize = 4;
const WORD_WEIGHT: f32 = 1.0;
const BIGRAM_WEIGHT: f32 = 0.6;
const TRIGRAM_WEIGHT: f32 = 0.4;
/// Stem-like prefix features ("deployment" and "deploy" both bucket as
/// "p:deplo") carry inflection matches that exact unigrams miss.
const PREFIX_WEIGHT: f32 = 0.8;
const PREFIX_CHARS: usize = 5;
/// Guardrails for the operator-tunable blend knobs.
const MIN_SCORE_FLOOR: f32 = 0.001;
const MIN_SCORE_CEILING: f32 = 1.0;
/// Fallback when the configured min score is non-finite (NaN/inf parse as
/// valid f32 but defeat `clamp`); mirrors the config default.
const MIN_SCORE_DEFAULT: f32 = 0.25;
/// Hashing is O(text length); cap the embedded prefix so one pathological
/// full-document save cannot stall scoring. The head of a document carries
/// its title/abstract/topic signal, which is what document-level matching
/// needs.
const EMBED_MAX_CHARS: usize = 100_000;

/// FNV-1a 64-bit. Hand-rolled so feature bucketing is deterministic across
/// processes and Rust versions (`DefaultHasher` guarantees neither).
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn accumulate_feature(counts: &mut HashMap<u64, (f32, f32)>, feature: &str, weight: f32) {
    let hash = fnv1a64(feature.as_bytes());
    let entry = counts.entry(hash).or_insert((0.0, weight));
    entry.0 += 1.0;
}

/// Embed text into an L2-normalized `VECTOR_DIM` vector via signed feature
/// hashing. Returns `None` when the text has no alphanumeric tokens, so
/// callers can skip vector scoring instead of feeding turbovec a zero
/// vector (which would become NaN under normalization).
pub fn embed_text(text: &str) -> Option<Vec<f32>> {
    let bounded: String = text.chars().take(EMBED_MAX_CHARS).collect();
    let lowered = bounded.to_lowercase();
    let tokens: Vec<&str> = lowered
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    if tokens.is_empty() {
        return None;
    }

    let mut counts: HashMap<u64, (f32, f32)> = HashMap::new();
    for token in &tokens {
        accumulate_feature(&mut counts, &format!("w:{token}"), WORD_WEIGHT);

        if token.chars().count() >= PREFIX_CHARS {
            let prefix: String = token.chars().take(PREFIX_CHARS).collect();
            accumulate_feature(&mut counts, &format!("p:{prefix}"), PREFIX_WEIGHT);
        }

        let chars: Vec<char> = std::iter::once('^')
            .chain(token.chars())
            .chain(std::iter::once('$'))
            .collect();
        for window in chars.windows(3) {
            let trigram: String = window.iter().collect();
            accumulate_feature(&mut counts, &format!("t:{trigram}"), TRIGRAM_WEIGHT);
        }
    }
    for pair in tokens.windows(2) {
        accumulate_feature(
            &mut counts,
            &format!("b:{} {}", pair[0], pair[1]),
            BIGRAM_WEIGHT,
        );
    }

    let mut vector = vec![0.0f32; VECTOR_DIM];
    for (hash, (count, weight)) in counts {
        let bucket = (hash % VECTOR_DIM as u64) as usize;
        let sign = if hash & (1 << 63) == 0 { 1.0 } else { -1.0 };
        vector[bucket] += sign * weight * (1.0 + count.ln());
    }

    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if !norm.is_finite() || norm <= f32::EPSILON {
        return None;
    }
    for value in &mut vector {
        *value /= norm;
    }
    Some(vector)
}

struct VectorEntry {
    id: u64,
    fingerprint: u64,
}

/// Quantized lexical index over scoped document keys.
///
/// Entries are maintained lazily: `score_map` (re-)embeds whatever
/// candidates it is handed, fingerprinting content so text edits re-embed
/// on the next search. Stale entries for deleted documents stay in the
/// index but are unreachable — scoring is always allowlist-restricted to
/// the live candidate set.
pub struct VectorMatcher {
    enabled: bool,
    weight: f32,
    doc_weight: f32,
    min_score: f32,
    index: IdMapIndex,
    entries: HashMap<String, VectorEntry>,
    next_id: u64,
}

impl VectorMatcher {
    pub fn from_config(config: &Config) -> Self {
        Self::new(
            config.vector_match_enabled,
            config.vector_match_weight,
            config.vector_match_doc_weight,
            config.vector_match_min_score,
        )
    }

    pub fn new(enabled: bool, weight: f32, doc_weight: f32, min_score: f32) -> Self {
        let index = IdMapIndex::new(VECTOR_DIM, VECTOR_BIT_WIDTH)
            .expect("VECTOR_DIM and VECTOR_BIT_WIDTH are valid turbovec parameters");
        // NaN survives `clamp` and would make every threshold comparison
        // false, silently disabling vector-only matches.
        let min_score = if min_score.is_finite() {
            min_score.clamp(MIN_SCORE_FLOOR, MIN_SCORE_CEILING)
        } else {
            MIN_SCORE_DEFAULT
        };
        Self {
            enabled,
            weight: weight.max(0.0),
            doc_weight: doc_weight.max(0.0),
            min_score,
            index,
            entries: HashMap::new(),
            next_id: 1,
        }
    }

    /// Pre-embed entries at save time so the first search after an ingest
    /// does not pay the embedding cost. Functionally optional — `score_map`
    /// lazily embeds anything missing or stale.
    pub fn warm<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        if !self.enabled {
            return;
        }
        let _ = self.ensure_embedded(entries);
    }

    /// Score `candidates` (scoped key, text) against `query`.
    ///
    /// Embeds the query, lazily (re-)embeds candidates whose content
    /// fingerprint is missing or stale, then runs one allowlist-restricted
    /// turbovec search covering exactly the candidate set. Returns an empty
    /// map (legacy text-only behavior) when disabled, when the query has no
    /// tokens, or when no candidate is embeddable.
    pub fn score_map<I>(&mut self, query: &str, candidates: I) -> VectorScoreMap
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.score_map_with_weight(query, candidates, self.weight)
    }

    /// Like [`Self::score_map`], stamped with the document-level blend
    /// weight. Used for whole-document candidates whose evidence supports
    /// fragments rather than competing with them.
    pub fn doc_score_map<I>(&mut self, query: &str, candidates: I) -> VectorScoreMap
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.score_map_with_weight(query, candidates, self.doc_weight)
    }

    fn score_map_with_weight<I>(
        &mut self,
        query: &str,
        candidates: I,
        weight: f32,
    ) -> VectorScoreMap
    where
        I: IntoIterator<Item = (String, String)>,
    {
        if !self.enabled {
            return VectorScoreMap::default();
        }
        let Some(query_vector) = embed_text(query) else {
            return VectorScoreMap::default();
        };

        let (key_by_id, allowlist) = self.ensure_embedded(candidates);
        if allowlist.is_empty() {
            return VectorScoreMap::default();
        }

        let (scores, ids) =
            self.index
                .search_with_allowlist(&query_vector, allowlist.len(), Some(&allowlist));
        let mut map = HashMap::with_capacity(ids.len());
        for (score, id) in scores.into_iter().zip(ids) {
            if let Some(key) = key_by_id.get(&id) {
                map.insert(key.clone(), score.clamp(0.0, 1.0));
            }
        }
        VectorScoreMap {
            scores: map,
            weight,
            min_score: self.min_score,
        }
    }

    /// Ensure every candidate has a current embedding; returns the id→key
    /// map plus the allowlist covering exactly the candidate set.
    fn ensure_embedded<I>(&mut self, candidates: I) -> (HashMap<u64, String>, Vec<u64>)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        // Batch new/stale embeddings into one add so turbovec's first-add
        // TQ+ calibration sees the whole corpus, not a single vector.
        let mut pending_vectors: Vec<f32> = Vec::new();
        let mut pending: Vec<(String, u64, u64)> = Vec::new(); // (key, id, fingerprint)
        let mut key_by_id: HashMap<u64, String> = HashMap::new();
        let mut allowlist: Vec<u64> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        for (key, text) in candidates {
            if !seen.insert(key.clone()) {
                continue;
            }
            let fingerprint = fnv1a64(text.as_bytes());
            match self.entries.get(&key) {
                Some(entry) if entry.fingerprint == fingerprint => {
                    key_by_id.insert(entry.id, key.clone());
                    allowlist.push(entry.id);
                    continue;
                }
                Some(entry) => {
                    self.index.remove(entry.id);
                    self.entries.remove(&key);
                }
                None => {}
            }
            let Some(vector) = embed_text(&text) else {
                continue;
            };
            let id = self.next_id;
            self.next_id += 1;
            pending_vectors.extend_from_slice(&vector);
            pending.push((key, id, fingerprint));
        }

        if !pending.is_empty() {
            let ids: Vec<u64> = pending.iter().map(|(_, id, _)| *id).collect();
            match self.index.add_with_ids(&pending_vectors, &ids) {
                Ok(()) => {
                    for (key, id, fingerprint) in pending {
                        key_by_id.insert(id, key.clone());
                        allowlist.push(id);
                        self.entries.insert(key, VectorEntry { id, fingerprint });
                    }
                }
                Err(error) => {
                    // Vector matching is an enhancement layer over
                    // text_score; degrade to text-only rather than failing
                    // the search.
                    let diagnostic = safe_cause_diagnostic(&error);
                    tracing::warn!(
                        target: "nowledge::vector_match",
                        cause_category = diagnostic.category,
                        cause_fingerprint = %diagnostic.fingerprint,
                        "vector index add failed; text-only scoring"
                    );
                }
            }
        }

        (key_by_id, allowlist)
    }

    #[cfg(test)]
    fn indexed_len(&self) -> usize {
        self.index.len()
    }
}

/// Per-query vector scores plus the blend policy, detached from the matcher
/// lock so ranking can proceed without holding it.
#[derive(Debug, Clone)]
pub struct VectorScoreMap {
    scores: HashMap<String, f32>,
    weight: f32,
    min_score: f32,
}

impl Default for VectorScoreMap {
    /// The empty map reproduces legacy text-only matching: an infinite
    /// `min_score` means no candidate can qualify on vector evidence alone.
    fn default() -> Self {
        Self {
            scores: HashMap::new(),
            weight: 0.0,
            min_score: f32::INFINITY,
        }
    }
}

impl VectorScoreMap {
    pub fn vector_score(&self, key: &str) -> Option<f32> {
        self.scores.get(key).copied()
    }

    /// Blend `text_score` with the vector score for `key`.
    ///
    /// Returns `None` when the candidate matches neither lexically
    /// (`text_score == 0`) nor semantically (vector score below
    /// `min_score`); otherwise the combined ranking score
    /// `text_score + weight * vector_score`.
    pub fn combined_score(&self, key: &str, text_score: f32) -> Option<f32> {
        let vector = self.vector_score(key).unwrap_or(0.0);
        let matched = text_score > 0.0 || vector >= self.min_score;
        matched.then_some(text_score + self.weight * vector)
    }

    /// Vector-only evidence for `key`: `weight * score` when the score
    /// clears `min_score`, `None` otherwise. Used for document-level
    /// evidence, where there is no per-candidate lexical score to blend
    /// with — the document either supports its fragments or stays silent.
    pub fn evidence(&self, key: &str) -> Option<f32> {
        let vector = self.vector_score(key)?;
        (vector >= self.min_score).then_some(self.weight * vector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_text_is_deterministic_and_normalized() {
        let a = embed_text("Deployment pipelines for staging rollouts").expect("embeds");
        let b = embed_text("Deployment pipelines for staging rollouts").expect("embeds");
        assert_eq!(a, b);
        assert_eq!(a.len(), VECTOR_DIM);
        let norm: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "norm was {norm}");
    }

    #[test]
    fn embed_text_rejects_tokenless_input() {
        assert!(embed_text("").is_none());
        assert!(embed_text("   ").is_none());
        assert!(embed_text("?!– —").is_none());
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    #[test]
    fn related_text_scores_above_unrelated_text() {
        let query = embed_text("deployment pipelines").expect("embeds");
        let related = embed_text("how we deploy the pipeline to staging").expect("embeds");
        let unrelated = embed_text("quarterly nutrition macro spreadsheet totals").expect("embeds");
        assert!(
            cosine(&query, &related) > cosine(&query, &unrelated),
            "related {} <= unrelated {}",
            cosine(&query, &related),
            cosine(&query, &unrelated)
        );
    }

    fn candidates(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(key, text)| (key.to_string(), text.to_string()))
            .collect()
    }

    #[test]
    fn score_map_scores_all_candidates_and_ranks_related_first() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        let map = matcher.score_map(
            "deployment pipelines",
            candidates(&[
                ("idx|ctx://a", "deploy pipeline runbook for staging"),
                ("idx|ctx://b", "team lunch menu and seating chart"),
            ]),
        );
        let related = map.vector_score("idx|ctx://a").expect("scored");
        let unrelated = map.vector_score("idx|ctx://b").expect("scored");
        assert!(
            related > unrelated,
            "related {related} <= unrelated {unrelated}"
        );
    }

    #[test]
    fn score_map_is_restricted_to_the_candidate_allowlist() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        // Index both owners' documents.
        matcher.score_map(
            "deployment pipelines",
            candidates(&[
                (
                    "owner_a|ctx://doc",
                    "deployment pipeline secrets for owner a",
                ),
                ("owner_b|ctx://doc", "deployment pipeline notes for owner b"),
            ]),
        );
        // A search scoped to owner B's candidates must never surface owner A.
        let map = matcher.score_map(
            "deployment pipelines",
            candidates(&[("owner_b|ctx://doc", "deployment pipeline notes for owner b")]),
        );
        assert!(map.vector_score("owner_b|ctx://doc").is_some());
        assert!(map.vector_score("owner_a|ctx://doc").is_none());
    }

    #[test]
    fn score_map_reembeds_when_content_changes() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        let before = matcher
            .score_map(
                "deployment pipelines",
                candidates(&[("idx|ctx://doc", "deployment pipeline runbook")]),
            )
            .vector_score("idx|ctx://doc")
            .expect("scored");
        let after = matcher
            .score_map(
                "deployment pipelines",
                candidates(&[("idx|ctx://doc", "completely unrelated gardening notes")]),
            )
            .vector_score("idx|ctx://doc")
            .expect("scored");
        assert!(after < before, "stale embedding survived content change");
        // The stale vector was removed, not orphaned alongside the new one.
        assert_eq!(matcher.indexed_len(), 1);
    }

    #[test]
    fn disabled_matcher_returns_legacy_text_only_map() {
        let mut matcher = VectorMatcher::new(false, 4.0, 2.0, 0.25);
        let map = matcher.score_map(
            "deployment pipelines",
            candidates(&[("idx|ctx://doc", "deployment pipeline runbook")]),
        );
        assert!(map.vector_score("idx|ctx://doc").is_none());
        assert_eq!(map.combined_score("idx|ctx://doc", 0.0), None);
        assert_eq!(map.combined_score("idx|ctx://doc", 2.0), Some(2.0));
    }

    #[test]
    fn combined_score_admits_vector_only_matches_above_threshold() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        let map = matcher.score_map(
            "deployment pipelines",
            candidates(&[("idx|ctx://doc", "deploy pipeline runbook for staging")]),
        );
        let vector = map.vector_score("idx|ctx://doc").expect("scored");
        assert!(vector >= 0.25, "expected strong fuzzy match, got {vector}");
        // No lexical evidence at all, admitted on vector evidence.
        let combined = map.combined_score("idx|ctx://doc", 0.0).expect("matched");
        assert!((combined - 4.0 * vector).abs() < 1e-5);
        // Lexical evidence keeps contributing.
        let boosted = map.combined_score("idx|ctx://doc", 2.0).expect("matched");
        assert!(boosted > combined);
    }

    #[test]
    fn combined_score_rejects_noise_below_threshold() {
        let map = VectorScoreMap {
            scores: HashMap::from([("idx|ctx://doc".to_string(), 0.05)]),
            weight: 4.0,
            min_score: 0.25,
        };
        assert_eq!(map.combined_score("idx|ctx://doc", 0.0), None);
        // Text match still admits the candidate; weak vector still blends.
        let combined = map.combined_score("idx|ctx://doc", 1.0).expect("matched");
        assert!((combined - 1.2).abs() < 1e-5);
    }

    #[test]
    fn warm_preembeds_entries_without_duplicating() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        matcher.warm(candidates(&[("idx|ctx://doc", "deploy pipeline runbook")]));
        assert_eq!(matcher.indexed_len(), 1);
        // Re-warming unchanged content is a no-op, changed content re-embeds.
        matcher.warm(candidates(&[("idx|ctx://doc", "deploy pipeline runbook")]));
        assert_eq!(matcher.indexed_len(), 1);
        matcher.warm(candidates(&[("idx|ctx://doc", "updated pipeline runbook")]));
        assert_eq!(matcher.indexed_len(), 1);
    }

    #[test]
    fn doc_score_map_applies_document_weight_to_evidence() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, 0.25);
        let map = matcher.doc_score_map(
            "deployment pipelines",
            candidates(&[("doc|idx|ctx://doc", "deploy pipeline runbook for staging")]),
        );
        let vector = map.vector_score("doc|idx|ctx://doc").expect("scored");
        assert!(
            vector >= 0.25,
            "expected strong document match, got {vector}"
        );
        let evidence = map.evidence("doc|idx|ctx://doc").expect("evidence");
        assert!((evidence - 2.0 * vector).abs() < 1e-5);
    }

    #[test]
    fn non_finite_min_score_falls_back_to_default() {
        let mut matcher = VectorMatcher::new(true, 4.0, 2.0, f32::NAN);
        let map = matcher.score_map(
            "deployment pipelines",
            candidates(&[("idx|ctx://doc", "deploy pipeline runbook for staging")]),
        );
        // A NaN threshold would reject every vector-only match; the
        // fallback default must keep strong fuzzy matches admissible.
        assert!(map.combined_score("idx|ctx://doc", 0.0).is_some());
    }

    #[test]
    fn evidence_is_none_below_threshold_or_for_unknown_keys() {
        let map = VectorScoreMap {
            scores: HashMap::from([("doc|idx|ctx://doc".to_string(), 0.1)]),
            weight: 2.0,
            min_score: 0.25,
        };
        assert_eq!(map.evidence("doc|idx|ctx://doc"), None);
        assert_eq!(map.evidence("missing"), None);
    }

    /// Regression guard for the embedding calibration that backs the
    /// `RAG_VECTOR_MATCH_MIN_SCORE` default (0.25): supported fuzzy matches
    /// must stay above the threshold and known noise patterns below it.
    /// Measured borderline near-misses, documented as accepted limitations:
    /// "Deploy Pipeline Handbook" full doc vs "deployment pipelines" scores
    /// ~0.215 and its low-signal fragment ~0.219 — document-level evidence
    /// only boosts fragments that already match, so these stay out on their
    /// own. If a feature-weight change moves any cluster across the default,
    /// re-derive the threshold (see config vector_match_min_score).
    #[test]
    fn embedding_calibration_separates_supported_matches_from_noise() {
        let min_score = crate::config::Config::test().vector_match_min_score;
        let query = embed_text("deployment pipelines").expect("embeds");
        // Inflected/reordered phrasings of the query topic — must match.
        let supported = [
            "Deploy Pipeline Runbook fragment 1 Deploy pipeline runbook for the staging cluster.",
            "Deploy Pipeline Handbook fragment 2 Pipeline schedule for nightly batches.",
            "Deploy Pipeline Handbook fragment 1 Deploy the pipeline to staging. Pipeline deploys run nightly.",
        ];
        // Single shared generic tokens or disjoint topics — must stay out.
        // The first four mirror the regression-suite supersede/source-only
        // fixtures that hybrid matching must not surface.
        let noise = [
            "MinerU Fixture fragment 2 table-block-keyword Revenue table caption",
            "MinerU Fixture fragment 3 E = mc^2 + equation-block-keyword",
            "MinerU Fixture fragment 4 Architecture image-block-keyword",
            "MinerU Fixture fragment 1 MinerU Fixture",
            "Team Lunch fragment 1 team lunch menu and seating chart",
            "Nutrition Sheet fragment 1 quarterly nutrition macro spreadsheet totals for member check-ins",
            "Referral Report fragment 1 referred user emails join hubspot rows with empty names",
        ];
        let noise_queries = ["deployment pipelines", "raw-source-only-keyword"];
        for text in supported {
            let vector = embed_text(text).expect("embeds");
            let score = cosine(&query, &vector);
            assert!(
                score >= min_score,
                "supported match fell below min_score {min_score}: {score:.4} for {text:?}"
            );
        }
        for noise_query in noise_queries {
            let noise_vector = embed_text(noise_query).expect("embeds");
            for text in noise {
                let vector = embed_text(text).expect("embeds");
                let score = cosine(&noise_vector, &vector);
                assert!(
                    score < min_score,
                    "noise crossed min_score {min_score}: {score:.4} for query {noise_query:?} vs {text:?}"
                );
            }
        }
    }
}
