use std::collections::BTreeSet;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::models::{FragmentPolicy, ParsedBlock};

/// Largest `chunk_size_chars` a caller may request. The fragmenter clamps to
/// it as well, so internal callers cannot bypass request validation.
pub const MAX_FRAGMENT_CHUNK_CHARS: usize = 8_000;

/// Most fragments one document may produce. A policy that would exceed it on
/// a large document is coarsened (larger effective chunks) instead of turning
/// the document into an unbounded number of fragments, nodes and links.
pub const MAX_FRAGMENTS_PER_DOCUMENT: usize = 10_000;

#[derive(Debug, Clone)]
pub struct DocumentFragment {
    pub fragment_index: u32,
    pub content: String,
    pub char_start: usize,
    pub char_end: usize,
    pub token_estimate: usize,
    pub checksum: String,
}

#[derive(Debug, Clone)]
pub struct FragmentChunk {
    pub fragment_index: u32,
    pub content: String,
    pub char_start: Option<usize>,
    pub char_end: Option<usize>,
    pub token_estimate: usize,
    pub checksum: String,
    pub block_type: Option<String>,
    pub page_idx: Option<u32>,
    pub bbox: Option<Value>,
    pub section_path: Vec<String>,
    pub heading_level: Option<u8>,
    pub asset_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DocumentFragmenter {
    pub chunk_size_chars: usize,
    pub overlap_chars: usize,
    pub min_chunk_chars: usize,
}

impl Default for DocumentFragmenter {
    fn default() -> Self {
        Self {
            chunk_size_chars: 1200,
            overlap_chars: 150,
            min_chunk_chars: 200,
        }
    }
}

impl DocumentFragmenter {
    pub fn from_policy(policy: Option<&FragmentPolicy>) -> Self {
        let mut fragmenter = Self::default();
        if let Some(policy) = policy {
            if let Some(chunk_size_chars) = policy.chunk_size_chars {
                fragmenter.chunk_size_chars = chunk_size_chars.clamp(1, MAX_FRAGMENT_CHUNK_CHARS);
            }
            if let Some(overlap_chars) = policy.overlap_chars {
                fragmenter.overlap_chars = overlap_chars;
            }
            if let Some(min_chunk_chars) = policy.min_chunk_chars {
                fragmenter.min_chunk_chars = min_chunk_chars.max(1);
            }
        }
        fragmenter.overlap_chars = fragmenter
            .overlap_chars
            .min(fragmenter.chunk_size_chars.saturating_sub(1));
        fragmenter.min_chunk_chars = fragmenter.min_chunk_chars.min(fragmenter.chunk_size_chars);
        fragmenter
    }

    pub fn fragment(&self, content: &str) -> Vec<DocumentFragment> {
        self.fragment_with_min_step(content, min_step_for(content.chars().count()))
    }

    /// Fragments `content` in time linear in its length. Every non-final step
    /// advances by at least `min_step` characters and a quarter of the
    /// fragment just emitted, which bounds both the fragment count and the
    /// total emitted text whatever policy the caller supplied.
    fn fragment_with_min_step(&self, content: &str, min_step: usize) -> Vec<DocumentFragment> {
        // Index the characters once. Re-walking the document from the start
        // for every fragment made fragmentation quadratic in its length.
        let chars: Vec<char> = content.chars().collect();
        let char_len = chars.len();
        if char_len == 0 {
            return Vec::new();
        }

        let min_step = min_step.max(1);
        let chunk_size_chars = self.chunk_size_chars.max(min_step);
        let min_chunk_chars = self.min_chunk_chars.max(min_step).min(chunk_size_chars);
        let breaks = Breakpoints::new(content, &chars);
        let mut fragments = Vec::new();
        let mut start = 0;

        while start < char_len {
            let max_end = (start + chunk_size_chars).min(char_len);
            let end = if max_end == char_len {
                char_len
            } else {
                breaks
                    .best_between(start + min_chunk_chars, max_end)
                    .unwrap_or(max_end)
            };
            let (trimmed_start, trimmed_end) = trim_range(&chars, start, end);
            if trimmed_start < trimmed_end {
                let text: String = chars[trimmed_start..trimmed_end].iter().collect();
                let fragment_index = fragments.len() as u32;
                fragments.push(DocumentFragment {
                    fragment_index,
                    token_estimate: estimate_tokens(&text),
                    checksum: fragment_checksum(fragment_index, trimmed_start, trimmed_end, &text),
                    content: text,
                    char_start: trimmed_start,
                    char_end: trimmed_end,
                });
            }

            if end >= char_len {
                break;
            }
            // `advance` never exceeds `end - start`, so no content is skipped.
            let advance = ((end - start) / 4).max(min_step);
            let next_start = end.saturating_sub(self.overlap_chars).max(start + advance);
            start = next_start.min(char_len);
        }

        fragments
    }
}

#[derive(Debug, Clone)]
pub struct BlockAwareFragmenter {
    fallback: DocumentFragmenter,
}

impl BlockAwareFragmenter {
    pub fn from_policy(policy: Option<&FragmentPolicy>) -> Self {
        Self {
            fallback: DocumentFragmenter::from_policy(policy),
        }
    }

    pub fn fragment(&self, content: &str, blocks: &[ParsedBlock]) -> Vec<FragmentChunk> {
        if blocks.is_empty() {
            return self
                .fallback
                .fragment(content)
                .into_iter()
                .map(FragmentChunk::from)
                .collect();
        }

        let mut ordered = blocks.to_vec();
        ordered.sort_by_key(|block| {
            (
                block.reading_order,
                block.page_idx.unwrap_or(u32::MAX),
                block.block_id.clone(),
            )
        });

        let mut bodies = Vec::with_capacity(ordered.len());
        for block in ordered {
            let Some(body) = block_body(&block) else {
                continue;
            };
            if body.trim().is_empty() {
                continue;
            }
            bodies.push((block, body));
        }
        // Share one per-document fragment budget across every long paragraph
        // instead of granting each block its own allowance.
        let min_step = min_step_for(bodies.iter().map(|(_, body)| body.chars().count()).sum());

        let mut chunks = Vec::new();
        for (block, body) in bodies {
            let block_type = block.block_type.clone();
            let atomic = matches!(
                block_type.as_str(),
                "table" | "equation" | "image" | "chart" | "code"
            );
            if block_type == "paragraph" && body.chars().count() > self.fallback.chunk_size_chars {
                for fragment in self.fallback.fragment_with_min_step(&body, min_step) {
                    let mut chunk = FragmentChunk::from(fragment);
                    chunk.fragment_index = chunks.len() as u32;
                    chunk.block_type = Some(block_type.clone());
                    chunk.page_idx = block.page_idx;
                    chunk.bbox = block.bbox.clone();
                    chunk.section_path = block.section_path.clone();
                    chunk.heading_level = block.text_level;
                    chunk.asset_refs = block.image_ref.clone().into_iter().collect();
                    chunk.checksum = block_fragment_checksum(
                        chunk.fragment_index,
                        &block.block_id,
                        chunk.char_start,
                        chunk.char_end,
                        &chunk.content,
                    );
                    chunks.push(chunk);
                }
                continue;
            }

            let fragment_index = chunks.len() as u32;
            let text_level = block.text_level;
            chunks.push(FragmentChunk {
                fragment_index,
                token_estimate: estimate_tokens(&body),
                checksum: block_fragment_checksum(
                    fragment_index,
                    &block.block_id,
                    None,
                    None,
                    &body,
                ),
                content: body,
                char_start: None,
                char_end: None,
                block_type: Some(if atomic { block_type } else { block.block_type }),
                page_idx: block.page_idx,
                bbox: block.bbox,
                section_path: block.section_path,
                heading_level: text_level,
                asset_refs: block.image_ref.into_iter().collect(),
            });
        }

        chunks
    }
}

impl From<DocumentFragment> for FragmentChunk {
    fn from(fragment: DocumentFragment) -> Self {
        Self {
            fragment_index: fragment.fragment_index,
            content: fragment.content,
            char_start: Some(fragment.char_start),
            char_end: Some(fragment.char_end),
            token_estimate: fragment.token_estimate,
            checksum: fragment.checksum,
            block_type: Some("paragraph".to_string()),
            page_idx: None,
            bbox: None,
            section_path: Vec::new(),
            heading_level: None,
            asset_refs: Vec::new(),
        }
    }
}

struct Breakpoints {
    headings: BTreeSet<usize>,
    paragraphs: BTreeSet<usize>,
    sentences: BTreeSet<usize>,
}

impl Breakpoints {
    fn new(content: &str, chars: &[char]) -> Self {
        let mut headings = BTreeSet::new();
        let mut paragraphs = BTreeSet::new();
        let mut sentences = BTreeSet::new();

        let mut char_pos = 0;
        for line in content.split_inclusive('\n') {
            let trimmed = line.trim_start();
            if char_pos > 0 && trimmed.starts_with('#') {
                headings.insert(char_pos);
            }
            char_pos += line.chars().count();
        }

        for idx in 0..chars.len() {
            if chars[idx] == '\n' && chars.get(idx + 1) == Some(&'\n') {
                paragraphs.insert((idx + 2).min(chars.len()));
            }
            if matches!(chars[idx], '.' | '!' | '?')
                && chars.get(idx + 1).is_some_and(|ch| ch.is_whitespace())
            {
                sentences.insert(idx + 1);
            }
        }

        Self {
            headings,
            paragraphs,
            sentences,
        }
    }

    fn best_between(&self, min: usize, max: usize) -> Option<usize> {
        [&self.headings, &self.paragraphs, &self.sentences]
            .into_iter()
            .filter_map(|set| set.range(min..=max).next_back().copied())
            .next()
    }
}

/// Smallest step that keeps a document of `char_len` characters within
/// [`MAX_FRAGMENTS_PER_DOCUMENT`]; 1 for every document below the cap.
fn min_step_for(char_len: usize) -> usize {
    char_len.div_ceil(MAX_FRAGMENTS_PER_DOCUMENT).max(1)
}

fn trim_range(chars: &[char], start: usize, end: usize) -> (usize, usize) {
    let mut left = start.min(chars.len());
    let mut right = end.min(chars.len());
    while left < right && chars[left].is_whitespace() {
        left += 1;
    }
    while right > left && chars[right - 1].is_whitespace() {
        right -= 1;
    }
    (left, right)
}

fn estimate_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4).max(1)
}

fn fragment_checksum(index: u32, start: usize, end: usize, text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(index.to_le_bytes());
    hasher.update(start.to_le_bytes());
    hasher.update(end.to_le_bytes());
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

fn block_fragment_checksum(
    index: u32,
    block_id: &str,
    start: Option<usize>,
    end: Option<usize>,
    text: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(index.to_le_bytes());
    hasher.update(block_id.as_bytes());
    hasher.update(start.unwrap_or_default().to_le_bytes());
    hasher.update(end.unwrap_or_default().to_le_bytes());
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

fn block_body(block: &ParsedBlock) -> Option<String> {
    let mut parts = Vec::new();
    match block.block_type.as_str() {
        "table" => {
            if let Some(html) = block
                .html
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(html.to_string());
            } else if let Some(text) = block
                .text
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(text.to_string());
            }
        }
        "equation" => {
            if let Some(latex) = block
                .latex
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(latex.to_string());
            } else if let Some(text) = block
                .text
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(text.to_string());
            }
        }
        "image" | "chart" => {
            if let Some(caption) = block
                .caption
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(caption.to_string());
            }
            if let Some(text) = block
                .text
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(text.to_string());
            }
            if let Some(image_ref) = block
                .image_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(image_ref.to_string());
            }
        }
        _ => {
            if let Some(text) = block
                .text
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(text.to_string());
            } else if let Some(html) = block
                .html
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(html.to_string());
            } else if let Some(latex) = block
                .latex
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                parts.push(latex.to_string());
            }
        }
    }

    if let Some(caption) = block
        .caption
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if !parts.iter().any(|part| part == caption) {
            parts.push(caption.to_string());
        }
    }
    if let Some(footnote) = block
        .footnote
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(footnote.to_string());
    }

    (!parts.is_empty()).then(|| parts.join("\n\n"))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    fn policy(chunk: usize, overlap: usize, min: usize) -> FragmentPolicy {
        FragmentPolicy {
            chunk_size_chars: Some(chunk),
            overlap_chars: Some(overlap),
            min_chunk_chars: Some(min),
        }
    }

    /// Mixed headings, paragraphs, sentences and multi-byte characters, with
    /// headings placed just past the default minimum chunk length.
    fn document(target_chars: usize) -> String {
        let section = "# Heading é\n\nThe quick brown fox — jumps. Over the lazy dog! \
                       Does it?\nPlain words without any breakpoint at all here ";
        let section_chars = section.chars().count();
        let mut doc = String::new();
        let (mut chars, mut index) = (0, 0);
        while chars < target_chars {
            let filler = 80 + index % 170;
            doc.push_str(section);
            doc.push_str(&"x".repeat(filler));
            doc.push_str(".\n");
            chars += section_chars + filler + 2;
            index += 1;
        }
        doc
    }

    /// The previous implementation, kept to prove the linear rewrite emits
    /// identical fragments whenever the new progress floors do not bind.
    fn reference_fragments(
        fragmenter: &DocumentFragmenter,
        content: &str,
    ) -> Vec<(usize, usize, String)> {
        let chars: Vec<char> = content.chars().collect();
        let breaks = Breakpoints::new(content, &chars);
        let mut out = Vec::new();
        let mut start = 0;
        while start < chars.len() {
            let max_end = (start + fragmenter.chunk_size_chars).min(chars.len());
            let end = if max_end == chars.len() {
                chars.len()
            } else {
                breaks
                    .best_between(start + fragmenter.min_chunk_chars, max_end)
                    .unwrap_or(max_end)
            };
            let (left, right) = trim_range(&chars, start, end);
            if left < right {
                out.push((left, right, chars[left..right].iter().collect()));
            }
            if end >= chars.len() {
                break;
            }
            start = end
                .saturating_sub(fragmenter.overlap_chars)
                .max(start + 1)
                .min(chars.len());
        }
        out
    }

    fn assert_covers_all_text(chars: &[char], fragments: &[DocumentFragment]) {
        let mut covered_to = 0;
        for fragment in fragments {
            assert!(
                chars[covered_to.min(fragment.char_start)..fragment.char_start]
                    .iter()
                    .all(|ch| ch.is_whitespace()),
                "non-whitespace text skipped before {}",
                fragment.char_start
            );
            covered_to = covered_to.max(fragment.char_end);
        }
        assert!(chars[covered_to..].iter().all(|ch| ch.is_whitespace()));
    }

    #[test]
    fn linear_fragmenter_matches_previous_output_for_supported_policies() {
        // Below the fragment cap, so only the quarter-fragment floor applies.
        let content = document(9_000);
        for policy in [
            None,
            Some(policy(240, 0, 240)),
            Some(policy(4, 0, 4)),
            Some(policy(300, 50, 100)),
        ] {
            let fragmenter = DocumentFragmenter::from_policy(policy.as_ref());
            let actual = fragmenter
                .fragment(&content)
                .into_iter()
                .map(|fragment| (fragment.char_start, fragment.char_end, fragment.content))
                .collect::<Vec<_>>();
            let expected = reference_fragments(&fragmenter, &content);
            let first_difference = actual
                .iter()
                .zip(&expected)
                .position(|(left, right)| left != right);
            assert!(
                first_difference.is_none() && actual.len() == expected.len(),
                "{policy:?}: first difference at {first_difference:?}, {} vs {} fragments",
                actual.len(),
                expected.len()
            );
        }
    }

    #[test]
    fn large_document_fragments_in_bounded_time_and_output_for_any_policy() {
        let content = document(2 * 1024 * 1024);
        let chars: Vec<char> = content.chars().collect();
        for policy in [
            None,
            Some(policy(1, 0, 1)),
            Some(policy(
                MAX_FRAGMENT_CHUNK_CHARS,
                MAX_FRAGMENT_CHUNK_CHARS - 1,
                1,
            )),
            // An oversized request is clamped rather than trusted.
            Some(policy(usize::MAX, usize::MAX, usize::MAX)),
        ] {
            let started = Instant::now();
            let fragments = DocumentFragmenter::from_policy(policy.as_ref()).fragment(&content);
            let elapsed = started.elapsed();
            // The previous quadratic implementation needed minutes here.
            assert!(elapsed < Duration::from_secs(30), "{policy:?}: {elapsed:?}");
            assert!(!fragments.is_empty());
            assert!(fragments.len() <= MAX_FRAGMENTS_PER_DOCUMENT, "{policy:?}");
            let emitted: usize = fragments
                .iter()
                .map(|fragment| fragment.char_end - fragment.char_start)
                .sum();
            assert!(
                emitted <= 4 * chars.len() + MAX_FRAGMENT_CHUNK_CHARS,
                "{policy:?}: emitted {emitted} chars for {}",
                chars.len()
            );
            assert_covers_all_text(&chars, &fragments);
        }
    }

    #[test]
    fn block_fragment_budget_is_shared_across_the_document() {
        let paragraph = "word ".repeat(4_000);
        let blocks = (0..20)
            .map(|index| ParsedBlock {
                block_id: format!("b{index}"),
                block_type: "paragraph".to_string(),
                text: Some(paragraph.clone()),
                reading_order: index,
                ..ParsedBlock::default()
            })
            .collect::<Vec<_>>();
        let chunks =
            BlockAwareFragmenter::from_policy(Some(&policy(1, 0, 1))).fragment("", &blocks);
        assert!(
            chunks.len() <= MAX_FRAGMENTS_PER_DOCUMENT + blocks.len(),
            "{} chunks",
            chunks.len()
        );
    }

    #[test]
    fn small_documents_keep_requested_tiny_chunks() {
        let fragments =
            DocumentFragmenter::from_policy(Some(&policy(4, 0, 4))).fragment("Xabcdefghij");
        let contents = fragments
            .iter()
            .map(|fragment| fragment.content.as_str())
            .collect::<Vec<_>>();
        assert_eq!(contents, ["Xabc", "defg", "hij"]);
    }
}
