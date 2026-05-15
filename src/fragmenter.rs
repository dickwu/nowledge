use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::models::FragmentPolicy;

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
                fragmenter.chunk_size_chars = chunk_size_chars.max(1);
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
        let char_len = content.chars().count();
        if char_len == 0 {
            return Vec::new();
        }

        let breaks = Breakpoints::new(content);
        let mut fragments = Vec::new();
        let mut start = 0;

        while start < char_len {
            let max_end = (start + self.chunk_size_chars).min(char_len);
            let end = if max_end == char_len {
                char_len
            } else {
                breaks
                    .best_between(start + self.min_chunk_chars, max_end)
                    .unwrap_or(max_end)
            };
            let (trimmed_start, trimmed_end) = trim_range(content, start, end);
            if trimmed_start < trimmed_end {
                let text = slice_chars(content, trimmed_start, trimmed_end);
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
            let next_start = end.saturating_sub(self.overlap_chars).max(start + 1);
            start = next_start.min(char_len);
        }

        fragments
    }
}

struct Breakpoints {
    headings: BTreeSet<usize>,
    paragraphs: BTreeSet<usize>,
    sentences: BTreeSet<usize>,
}

impl Breakpoints {
    fn new(content: &str) -> Self {
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

        let chars: Vec<char> = content.chars().collect();
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

fn trim_range(content: &str, start: usize, end: usize) -> (usize, usize) {
    let chars: Vec<char> = content.chars().collect();
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

fn slice_chars(content: &str, start: usize, end: usize) -> String {
    content
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
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
