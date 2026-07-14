use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
struct ManifestEntry {
    method: String,
    path: String,
    handler: String,
    file: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RouteTriple {
    method: String,
    path: String,
    handler: String,
}

impl From<&ManifestEntry> for RouteTriple {
    fn from(entry: &ManifestEntry) -> Self {
        Self {
            method: entry.method.clone(),
            path: entry.path.clone(),
            handler: entry.handler.clone(),
        }
    }
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    index += 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            byte if byte == quote => return index + 1,
            _ => index += 1,
        }
    }
    bytes.len()
}

fn skip_raw_string(bytes: &[u8], index: usize) -> Option<usize> {
    if bytes.get(index) != Some(&b'r') {
        return None;
    }
    let mut cursor = index + 1;
    while bytes.get(cursor) == Some(&b'#') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'"') {
        return None;
    }
    let hashes = cursor - index - 1;
    cursor += 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"'
            && bytes.get(cursor + 1..cursor + 1 + hashes)
                == Some(&bytes[index + 1..index + 1 + hashes])
        {
            return Some(cursor + 1 + hashes);
        }
        cursor += 1;
    }
    Some(bytes.len())
}

fn skip_non_code(bytes: &[u8], index: usize) -> Option<usize> {
    if bytes.get(index..index + 2) == Some(b"//") {
        return Some(
            bytes[index + 2..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|offset| index + 3 + offset)
                .unwrap_or(bytes.len()),
        );
    }
    if bytes.get(index..index + 2) == Some(b"/*") {
        let mut cursor = index + 2;
        let mut depth = 1usize;
        while cursor < bytes.len() && depth > 0 {
            if bytes.get(cursor..cursor + 2) == Some(b"/*") {
                depth += 1;
                cursor += 2;
            } else if bytes.get(cursor..cursor + 2) == Some(b"*/") {
                depth -= 1;
                cursor += 2;
            } else {
                cursor += 1;
            }
        }
        return Some(cursor);
    }
    if let Some(end) = skip_raw_string(bytes, index) {
        return Some(end);
    }
    match bytes.get(index) {
        Some(b'"') => Some(skip_quoted(bytes, index, b'"')),
        Some(b'\'') => Some(skip_quoted(bytes, index, b'\'')),
        _ => None,
    }
}

fn find_matching(source: &str, open: usize, opening: u8, closing: u8) -> usize {
    let bytes = source.as_bytes();
    assert_eq!(bytes[open], opening);
    let mut depth = 0usize;
    let mut index = open;
    while index < bytes.len() {
        if let Some(next) = skip_non_code(bytes, index) {
            index = next;
            continue;
        }
        match bytes[index] {
            byte if byte == opening => depth += 1,
            byte if byte == closing => {
                depth -= 1;
                if depth == 0 {
                    return index;
                }
            }
            _ => {}
        }
        index += 1;
    }
    panic!("unterminated delimiter starting at byte {open}");
}

fn split_route_arguments(arguments: &str) -> (&str, &str) {
    let bytes = arguments.as_bytes();
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if let Some(next) = skip_non_code(bytes, index) {
            index = next;
            continue;
        }
        match bytes[index] {
            b'(' => paren_depth += 1,
            b')' => paren_depth -= 1,
            b'[' => bracket_depth += 1,
            b']' => bracket_depth -= 1,
            b'{' => brace_depth += 1,
            b'}' => brace_depth -= 1,
            b',' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                return (&arguments[..index], &arguments[index + 1..]);
            }
            _ => {}
        }
        index += 1;
    }
    panic!("route call does not contain a top-level argument separator: {arguments}");
}

fn parse_path(argument: &str) -> String {
    let argument = argument.trim();
    assert!(
        argument.starts_with('"') && argument.ends_with('"'),
        "route path must be a plain string literal: {argument}"
    );
    argument[1..argument.len() - 1].to_string()
}

fn parse_route_methods(expression: &str, path: &str) -> Vec<RouteTriple> {
    let mut expression = expression.trim();
    if let Some(without_comma) = expression.strip_suffix(',') {
        expression = without_comma.trim_end();
    }
    let bytes = expression.as_bytes();
    let mut index = 0usize;
    let mut routes = Vec::new();
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index) == Some(&b'.') {
            index += 1;
            while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
                index += 1;
            }
        }
        if index == bytes.len() {
            break;
        }

        let method_start = index;
        while bytes
            .get(index)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            index += 1;
        }
        let method = &expression[method_start..index];
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        assert_eq!(
            bytes.get(index),
            Some(&b'('),
            "route method {method} for {path} is not a call: {expression}"
        );
        let close = find_matching(expression, index, b'(', b')');
        let handler = expression[index + 1..close].trim();
        assert!(
            !handler.is_empty()
                && handler
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "route handler for {method} {path} is not a plain identifier: {handler}"
        );
        let http_method = match method {
            "get" | "post" | "put" | "patch" | "delete" | "head" | "options" => {
                method.to_ascii_uppercase()
            }
            other => panic!("unsupported route method builder {other} for {path}"),
        };
        routes.push(RouteTriple {
            method: http_method,
            path: path.to_string(),
            handler: handler.to_string(),
        });
        index = close + 1;

        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if index == bytes.len() {
            break;
        }
        assert_eq!(
            bytes.get(index),
            Some(&b'.'),
            "unexpected trailing route expression for {path}: {}",
            &expression[index..]
        );
    }
    assert!(!routes.is_empty(), "route {path} has no HTTP methods");
    routes
}

fn code_contains(source: &str, needle: &str) -> bool {
    let bytes = source.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if let Some(next) = skip_non_code(bytes, index) {
            index = next;
            continue;
        }
        if bytes.get(index..index + needle.len()) == Some(needle.as_bytes()) {
            return true;
        }
        index += 1;
    }
    false
}

fn first_code_index(source: &str) -> usize {
    let bytes = source.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
        } else if let Some(next) = skip_non_code(bytes, index) {
            index = next;
        } else {
            return index;
        }
    }
    source.len()
}

fn assert_flat_router_shape(body: &str) {
    // This characterization parser is intentionally temporary and only supports
    // build_router's current single, flat Router::new() chain. Reject composition
    // explicitly so a future modular router cannot make this test silently miss routes.
    for unsupported in [".merge", ".nest"] {
        assert!(
            !code_contains(body, unsupported),
            "temporary flat-router parser does not support {unsupported} composition; replace or extend the parser before modularizing build_router"
        );
    }

    let first_code = first_code_index(body);
    assert!(
        body[first_code..].starts_with("Router::new()"),
        "temporary flat-router parser requires build_router to be one direct Router::new() chain; helper-router composition is unsupported"
    );
}

fn router_triples(source: &str) -> BTreeSet<RouteTriple> {
    let function_start = source
        .find("pub fn build_router")
        .expect("routes.rs must define build_router");
    let body_open = source[function_start..]
        .find('{')
        .map(|offset| function_start + offset)
        .expect("build_router must have a body");
    let body_close = find_matching(source, body_open, b'{', b'}');
    let body = &source[body_open + 1..body_close];
    assert_flat_router_shape(body);
    let bytes = body.as_bytes();
    let mut index = 0usize;
    let mut routes = Vec::new();

    while index < bytes.len() {
        if let Some(next) = skip_non_code(bytes, index) {
            index = next;
            continue;
        }
        if bytes.get(index..index + 6) == Some(b".route") {
            let mut open = index + 6;
            while bytes.get(open).is_some_and(u8::is_ascii_whitespace) {
                open += 1;
            }
            assert_eq!(bytes.get(open), Some(&b'('), ".route must be a call");
            let close = find_matching(body, open, b'(', b')');
            let (path_argument, method_expression) = split_route_arguments(&body[open + 1..close]);
            let path = parse_path(path_argument);
            routes.extend(parse_route_methods(method_expression, &path));
            index = close + 1;
            continue;
        }
        index += 1;
    }

    let unique: BTreeSet<_> = routes.iter().cloned().collect();
    assert_eq!(
        routes.len(),
        unique.len(),
        "build_router contains duplicate method/path/handler triples"
    );
    unique
}

fn manifest() -> Vec<ManifestEntry> {
    serde_json::from_str(include_str!("../doc/api_manifest.json"))
        .expect("doc/api_manifest.json must be valid")
}

#[test]
fn router_and_api_manifest_have_the_same_exact_endpoint_triples() {
    let routes = router_triples(include_str!("../src/routes.rs"));
    let manifest = manifest();
    let documented: BTreeSet<_> = manifest.iter().map(RouteTriple::from).collect();

    assert_eq!(manifest.len(), 87, "the endpoint manifest count changed");
    assert_eq!(
        documented.len(),
        manifest.len(),
        "doc/api_manifest.json contains duplicate endpoint triples"
    );

    let undocumented: Vec<_> = routes.difference(&documented).cloned().collect();
    let stale: Vec<_> = documented.difference(&routes).cloned().collect();
    assert!(
        undocumented.is_empty() && stale.is_empty(),
        "router/manifest drift\nundocumented router entries: {undocumented:#?}\nstale manifest entries: {stale:#?}"
    );
}

#[test]
fn every_manifest_entry_has_one_readme_row_and_endpoint_file_with_no_orphans() {
    let manifest = manifest();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let doc_root = root.join("doc");
    let readme = fs::read_to_string(doc_root.join("README.md")).unwrap();
    let mut expected_files = BTreeSet::new();

    for entry in &manifest {
        let row = format!(
            "| `{}` | `{}` | `{}` | [{}]({}) |",
            entry.method, entry.path, entry.handler, entry.file, entry.file
        );
        let row_count = readme.lines().filter(|line| *line == row).count();
        assert_eq!(
            row_count, 1,
            "doc/README.md must contain the exact endpoint row once: {row}"
        );

        let file = doc_root.join(&entry.file);
        assert!(
            file.is_file(),
            "manifest endpoint file is missing: {}",
            file.display()
        );
        assert!(
            expected_files.insert(entry.file.clone()),
            "manifest endpoint file is reused: {}",
            entry.file
        );
    }

    let actual_files: BTreeSet<_> = fs::read_dir(doc_root.join("api"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("md"))
        .filter(|path| path.file_name().and_then(|value| value.to_str()) != Some("AGENTS.md"))
        .map(|path| {
            Path::new("api")
                .join(path.file_name().unwrap())
                .to_string_lossy()
                .into_owned()
        })
        .collect();

    let orphaned: Vec<_> = actual_files.difference(&expected_files).cloned().collect();
    let missing: Vec<_> = expected_files.difference(&actual_files).cloned().collect();
    assert!(
        orphaned.is_empty() && missing.is_empty(),
        "endpoint file drift\norphaned files: {orphaned:#?}\nmissing files: {missing:#?}"
    );
}

#[cfg(test)]
mod parser_tests {
    use super::{router_triples, RouteTriple};
    use std::collections::BTreeSet;

    #[test]
    fn parser_handles_multiline_and_chained_route_methods() {
        let source = r###"
            pub fn build_router(state: AppState) -> Router {
                Router::new()
                    // .route("/ignored", get(ignored))
                    .route("/one", get(one))
                    .route(
                        "/many/{id}",
                        post(create_many).get(get_many).patch(patch_many).delete(delete_many),
                    )
                    .with_state(state)
            }
        "###;
        let actual = router_triples(source);
        let expected: BTreeSet<_> = [
            ("GET", "/one", "one"),
            ("POST", "/many/{id}", "create_many"),
            ("GET", "/many/{id}", "get_many"),
            ("PATCH", "/many/{id}", "patch_many"),
            ("DELETE", "/many/{id}", "delete_many"),
        ]
        .into_iter()
        .map(|(method, path, handler)| RouteTriple {
            method: method.to_string(),
            path: path.to_string(),
            handler: handler.to_string(),
        })
        .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(expected = "does not support .merge composition")]
    fn parser_rejects_merged_helper_routers() {
        router_triples(
            r###"
                pub fn build_router(state: AppState) -> Router {
                    Router::new().merge(helper_router()).with_state(state)
                }
            "###,
        );
    }

    #[test]
    #[should_panic(expected = "does not support .nest composition")]
    fn parser_rejects_nested_helper_routers() {
        router_triples(
            r###"
                pub fn build_router(state: AppState) -> Router {
                    Router::new().nest("/v1", helper_router()).with_state(state)
                }
            "###,
        );
    }

    #[test]
    #[should_panic(expected = "helper-router composition is unsupported")]
    fn parser_rejects_delegated_helper_routers() {
        router_triples(
            r###"
                pub fn build_router(state: AppState) -> Router {
                    helper_router().with_state(state)
                }
            "###,
        );
    }
}
