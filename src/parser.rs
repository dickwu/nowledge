use std::time::Instant;

use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::{timeout, Duration};

use crate::{config::Config, error::ApiError, models::*};

#[derive(Debug, Clone)]
pub struct ParserInput {
    pub content: String,
    pub content_type: Option<String>,
    pub file_name: Option<String>,
    pub content_list: Option<Value>,
    pub content_list_v2: Option<Value>,
    pub middle_json: Option<Value>,
    pub model_json: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ParserOutput {
    pub provider: String,
    pub backend: String,
    pub parser_version: Option<String>,
    pub markdown: Option<String>,
    pub content_list: Option<Value>,
    pub content_list_v2: Option<Value>,
    pub middle_json: Option<Value>,
    pub model_json: Option<Value>,
    pub images: Vec<Value>,
    pub blocks: Vec<ParsedBlock>,
}

#[async_trait]
pub trait DocumentParser: Send + Sync {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError>;
}

#[derive(Debug, Clone)]
pub struct BuiltinTextParser;

#[async_trait]
impl DocumentParser for BuiltinTextParser {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError> {
        let blocks =
            parse_supplied_blocks(input.content_list.as_ref(), input.content_list_v2.as_ref());
        Ok(ParserOutput {
            provider: "builtin".to_string(),
            backend: "text".to_string(),
            parser_version: Some("builtin-text".to_string()),
            markdown: Some(input.content),
            content_list: input.content_list,
            content_list_v2: input.content_list_v2,
            middle_json: input.middle_json,
            model_json: input.model_json,
            images: Vec::new(),
            blocks,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MineruParserClient {
    api_url: String,
    backend: String,
    return_md: bool,
    return_content_list: bool,
    return_middle_json: bool,
    return_images: bool,
    client: reqwest::Client,
}

impl MineruParserClient {
    pub fn new(config: &Config) -> Self {
        Self {
            api_url: config.mineru_api_url.clone(),
            backend: config.mineru_backend.clone(),
            return_md: config.mineru_return_md,
            return_content_list: config.mineru_return_content_list,
            return_middle_json: config.mineru_return_middle_json,
            return_images: config.mineru_return_images,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl DocumentParser for MineruParserClient {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError> {
        if input.content_list.is_some() || input.content_list_v2.is_some() {
            let blocks =
                parse_supplied_blocks(input.content_list.as_ref(), input.content_list_v2.as_ref());
            return Ok(ParserOutput {
                provider: "mineru".to_string(),
                backend: self.backend.clone(),
                parser_version: Some("mineru-supplied".to_string()),
                markdown: Some(input.content),
                content_list: input.content_list,
                content_list_v2: input.content_list_v2,
                middle_json: input.middle_json,
                model_json: input.model_json,
                images: Vec::new(),
                blocks,
            });
        }

        let file_name = input
            .file_name
            .unwrap_or_else(|| "document.txt".to_string());
        let mut part = Part::bytes(input.content.into_bytes()).file_name(file_name);
        if let Some(content_type) = input.content_type.as_deref() {
            part = part
                .mime_str(content_type)
                .map_err(|err| ApiError::bad_request(format!("invalid content_type: {err}")))?;
        }
        let form = Form::new()
            .part("files", part)
            .text("backend", self.backend.clone())
            .text("return_md", self.return_md.to_string())
            .text("return_content_list", self.return_content_list.to_string())
            .text("return_middle_json", self.return_middle_json.to_string())
            .text("return_images", self.return_images.to_string());
        let response = self
            .client
            .post(format!("{}/file_parse", self.api_url.trim_end_matches('/')))
            .multipart(form)
            .send()
            .await
            .map_err(|err| ApiError::Upstream(format!("MinerU parse request failed: {err}")))?;

        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "MinerU parse request failed with status {}",
                response.status()
            )));
        }

        let value = response.json::<Value>().await.map_err(|err| {
            ApiError::Upstream(format!("MinerU parse response was not JSON: {err}"))
        })?;
        let payload = mineru_payload(&value);
        let markdown = first_string_field(payload, &["markdown", "md", "content", "text"]);
        let content_list = first_value_field(payload, &["content_list"]);
        let content_list_v2 = first_value_field(payload, &["content_list_v2"]);
        let middle_json =
            first_value_field(payload, &["middle_json", "middle", "middle_json_data"]);
        let model_json = first_value_field(payload, &["model_json", "model"]);
        let images = first_value_field(payload, &["images"])
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        let blocks = parse_supplied_blocks(content_list.as_ref(), content_list_v2.as_ref());

        Ok(ParserOutput {
            provider: "mineru".to_string(),
            backend: self.backend.clone(),
            parser_version: first_string_field(payload, &["parser_version", "version"])
                .or_else(|| Some("mineru-api".to_string())),
            markdown,
            content_list,
            content_list_v2,
            middle_json,
            model_json,
            images,
            blocks,
        })
    }
}

pub fn parser_from_config(config: &Config) -> Box<dyn DocumentParser> {
    if config.parser_provider == "mineru" {
        Box::new(MineruParserClient::new(config))
    } else {
        Box::new(BuiltinTextParser)
    }
}

pub async fn parser_health_status(config: &Config) -> Value {
    if config.parser_provider != "mineru" {
        return json!({
            "provider": "builtin",
            "backend": "text",
            "healthy": true,
            "configured": true,
            "status": "ok",
            "latency_ms": 0
        });
    }

    let started = Instant::now();
    let client = reqwest::Client::new();
    let response = timeout(
        Duration::from_secs(2),
        client
            .get(format!(
                "{}/health",
                config.mineru_api_url.trim_end_matches('/')
            ))
            .send(),
    )
    .await;

    match response {
        Ok(Ok(response)) if response.status().is_success() => {
            let status = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            json!({
                "provider": "mineru",
                "backend": &config.mineru_backend,
                "healthy": true,
                "configured": true,
                "status": "ok",
                "latency_ms": started.elapsed().as_millis() as u64,
                "mineru": status
            })
        }
        Ok(Ok(response)) => json!({
            "provider": "mineru",
            "backend": &config.mineru_backend,
            "healthy": false,
            "configured": true,
            "status": "unhealthy",
            "http_status": response.status().as_u16(),
            "latency_ms": started.elapsed().as_millis() as u64
        }),
        Ok(Err(err)) => json!({
            "provider": "mineru",
            "backend": &config.mineru_backend,
            "healthy": false,
            "configured": true,
            "status": "unhealthy",
            "error": err.to_string(),
            "latency_ms": started.elapsed().as_millis() as u64
        }),
        Err(_) => json!({
            "provider": "mineru",
            "backend": &config.mineru_backend,
            "healthy": false,
            "configured": true,
            "status": "unhealthy",
            "error": "MinerU health check timed out",
            "latency_ms": started.elapsed().as_millis() as u64
        }),
    }
}

pub struct MineruContentListParser;

impl MineruContentListParser {
    pub fn parse(value: &Value) -> Vec<ParsedBlock> {
        let mut items = Vec::new();
        collect_block_items(value, &mut items);

        let mut section_stack: Vec<String> = Vec::new();
        let mut blocks = Vec::new();
        for (idx, item) in items.iter().enumerate() {
            let block_type = normalize_block_type(
                first_string_field(item, &["type", "content_type"]).as_deref(),
            );
            let text = first_string_field(item, &["text", "content", "md", "markdown"]);
            let html = first_string_field(item, &["html", "table_html", "table_body"]);
            let latex = first_string_field(item, &["latex", "latex_text", "formula", "equation"]);
            let image_ref = first_string_field(
                item,
                &["image_ref", "image_path", "img_path", "image", "src"],
            );
            let caption = first_textish_field(item, &["caption", "image_caption", "table_caption"]);
            let footnote =
                first_textish_field(item, &["footnote", "image_footnote", "table_footnote"]);
            let text_level = first_u8_field(item, &["text_level", "level", "heading_level"])
                .or_else(|| (block_type == "title").then_some(1));
            let section_path = section_path_from_item(item).unwrap_or_else(|| {
                if block_type == "title" {
                    if let Some(title) = text.as_ref().filter(|value| !value.trim().is_empty()) {
                        let level = text_level.unwrap_or(1).max(1) as usize;
                        section_stack.truncate(level.saturating_sub(1));
                        section_stack.push(title.clone());
                    }
                }
                section_stack.clone()
            });
            let reading_order =
                first_u32_field(item, &["reading_order", "order", "index"]).unwrap_or(idx as u32);
            let page_idx = first_u32_field(item, &["page_idx", "page_index", "page"]);
            let bbox = first_value_field(item, &["bbox", "box", "bounding_box"]);
            let block_id = first_string_field(item, &["block_id", "id"]).unwrap_or_else(|| {
                stable_block_id(
                    idx as u32,
                    &block_type,
                    page_idx,
                    text.as_deref()
                        .or(html.as_deref())
                        .or(latex.as_deref())
                        .or(caption.as_deref())
                        .unwrap_or_default(),
                )
            });

            blocks.push(ParsedBlock {
                block_id,
                block_type,
                page_idx,
                bbox,
                text,
                html,
                latex,
                image_ref,
                caption,
                footnote,
                text_level,
                section_path,
                reading_order,
            });
        }
        blocks
    }
}

fn parse_supplied_blocks(
    content_list: Option<&Value>,
    content_list_v2: Option<&Value>,
) -> Vec<ParsedBlock> {
    if let Some(value) = content_list_v2 {
        let blocks = MineruContentListParser::parse(value);
        if !blocks.is_empty() {
            return blocks;
        }
    }
    content_list
        .map(MineruContentListParser::parse)
        .unwrap_or_default()
}

fn collect_block_items(value: &Value, out: &mut Vec<Value>) {
    match value {
        Value::Array(items) => out.extend(items.iter().cloned()),
        Value::Object(map) => {
            for key in [
                "content_list_v2",
                "content_list",
                "blocks",
                "items",
                "data",
                "result",
            ] {
                if let Some(nested) = map.get(key) {
                    collect_block_items(nested, out);
                    return;
                }
            }
            out.push(value.clone());
        }
        _ => {}
    }
}

fn mineru_payload(value: &Value) -> &Value {
    if let Some(data) = value.get("data") {
        if let Some(results) = data.get("results").and_then(Value::as_array) {
            return results.first().unwrap_or(data);
        }
        return data;
    }
    if let Some(result) = value.get("result") {
        return result;
    }
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        return results.first().unwrap_or(value);
    }
    value
}

fn first_value_field(value: &Value, keys: &[&str]) -> Option<Value> {
    keys.iter().find_map(|key| value.get(*key).cloned())
}

fn first_string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| stringish(value.get(*key)?))
}

fn first_textish_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| textish(value.get(*key)?))
}

fn first_u32_field(value: &Value, keys: &[&str]) -> Option<u32> {
    keys.iter().find_map(|key| u32ish(value.get(*key)?))
}

fn first_u8_field(value: &Value, keys: &[&str]) -> Option<u8> {
    keys.iter()
        .find_map(|key| u32ish(value.get(*key)?).and_then(|value| u8::try_from(value).ok()))
}

fn stringish(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn textish(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Array(items) => {
            let parts = items.iter().filter_map(textish).collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        Value::Object(_) => first_string_field(value, &["text", "content", "caption"]),
        _ => None,
    }
}

fn u32ish(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(text) => text.parse::<u32>().ok(),
        _ => None,
    }
}

fn section_path_from_item(value: &Value) -> Option<Vec<String>> {
    let path = value.get("section_path")?;
    match path {
        Value::Array(items) => {
            let sections = items.iter().filter_map(stringish).collect::<Vec<_>>();
            (!sections.is_empty()).then_some(sections)
        }
        Value::String(text) if !text.trim().is_empty() => Some(vec![text.clone()]),
        _ => None,
    }
}

fn normalize_block_type(value: Option<&str>) -> String {
    match value.unwrap_or("paragraph").to_ascii_lowercase().as_str() {
        "title" | "heading" => "title",
        "paragraph" | "para" | "text" | "plain_text" => "paragraph",
        "table" | "table_body" => "table",
        "equation" | "formula" | "interline_equation" | "inline_equation" => "equation",
        "image" | "figure" | "image_body" => "image",
        "chart" => "chart",
        "code" | "code_block" => "code",
        "list" | "list_item" => "list",
        "seal" => "seal",
        "header" => "header",
        "footer" => "footer",
        "page_number" | "page" => "page_number",
        "footnote" => "footnote",
        _ => "paragraph",
    }
    .to_string()
}

fn stable_block_id(index: u32, block_type: &str, page_idx: Option<u32>, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(index.to_le_bytes());
    hasher.update(block_type.as_bytes());
    hasher.update(page_idx.unwrap_or_default().to_le_bytes());
    hasher.update(body.as_bytes());
    format!(
        "block_{}",
        hex::encode(hasher.finalize())
            .chars()
            .take(24)
            .collect::<String>()
    )
}
