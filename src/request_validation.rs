use crate::{
    config::Config,
    error::ApiError,
    models::{AppendHistoryEventRequest, BulkHistoryEventsRequest},
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
