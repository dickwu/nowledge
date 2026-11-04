use crate::{
    config::Config,
    error::ApiError,
    fragmenter::{DocumentFragmenter, MAX_FRAGMENT_CHUNK_CHARS},
    models::{AppendHistoryEventRequest, BulkHistoryEventsRequest, FragmentPolicy},
};

pub(crate) fn validate_max_items(
    field: &str,
    actual: usize,
    maximum: usize,
) -> Result<(), ApiError> {
    if actual > maximum {
        return Err(ApiError::validation(
            field,
            format!("must contain at most {maximum} items"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_search_limit(
    field: &str,
    limit: usize,
    config: &Config,
) -> Result<(), ApiError> {
    if limit > config.max_search_limit {
        return Err(ApiError::validation(
            field,
            format!("must be at most {}", config.max_search_limit),
        ));
    }
    Ok(())
}

/// Rejects fragment policies outside the supported envelope instead of letting
/// callers choose values that amplify one document into unbounded work. Unset
/// fields keep their defaults, which the fragmenter clamps to the chunk size,
/// so only explicitly supplied values are checked against each other.
pub(crate) fn validate_fragment_policy(
    field: &str,
    policy: Option<&FragmentPolicy>,
) -> Result<(), ApiError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let chunk_size = policy
        .chunk_size_chars
        .unwrap_or(DocumentFragmenter::default().chunk_size_chars);
    if !(1..=MAX_FRAGMENT_CHUNK_CHARS).contains(&chunk_size) {
        return Err(ApiError::validation(
            format!("{field}.chunk_size_chars"),
            format!("must be between 1 and {MAX_FRAGMENT_CHUNK_CHARS}"),
        ));
    }
    if policy
        .overlap_chars
        .is_some_and(|overlap| overlap >= chunk_size)
    {
        return Err(ApiError::validation(
            format!("{field}.overlap_chars"),
            "must be less than chunk_size_chars",
        ));
    }
    if policy
        .min_chunk_chars
        .is_some_and(|min_chunk| !(1..=chunk_size).contains(&min_chunk))
    {
        return Err(ApiError::validation(
            format!("{field}.min_chunk_chars"),
            "must be between 1 and chunk_size_chars",
        ));
    }
    Ok(())
}

pub(crate) fn validate_tags(field: &str, tags: &[String], config: &Config) -> Result<(), ApiError> {
    validate_max_items(field, tags.len(), config.max_tags_per_item)?;
    for (index, tag) in tags.iter().enumerate() {
        if tag.len() > config.max_tag_bytes {
            return Err(ApiError::validation(
                format!("{field}[{index}]"),
                format!("must be at most {} UTF-8 bytes", config.max_tag_bytes),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_history_event(
    field: &str,
    request: &AppendHistoryEventRequest,
    config: &Config,
) -> Result<(), ApiError> {
    validate_tags(&format!("{field}.tags"), &request.tags, config)
}

pub(crate) fn validate_history_bulk(
    request: &BulkHistoryEventsRequest,
    config: &Config,
) -> Result<(), ApiError> {
    validate_max_items("events", request.events.len(), config.max_bulk_events)?;
    for (index, event) in request.events.iter().enumerate() {
        validate_history_event(&format!("events[{index}]"), event, config)?;
    }
    Ok(())
}
