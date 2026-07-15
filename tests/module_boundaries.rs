use std::{fs, path::Path};

use nowledge::{RouteGuard, REGISTERED_ROUTES};

fn source(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}

fn route_module_source() -> String {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = fs::read_dir(src)
        .expect("src directory must be readable")
        .map(|entry| entry.expect("src entry must be readable").path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.starts_with("route_") && name.ends_with(".rs"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .iter()
        .map(|path| source(path))
        .collect::<Vec<_>>()
        .join("\n")
}

fn handler_signature<'a>(source: &'a str, handler: &str) -> &'a str {
    let marker = format!("fn {handler}(");
    let matches = source.match_indices(&marker).collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "registered handler {handler} must have exactly one route-module definition"
    );
    let open = matches[0].0 + marker.len() - 1;
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    for index in open..bytes.len() {
        match bytes[index] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open + 1..index];
                }
            }
            _ => {}
        }
    }
    panic!("registered handler {handler} has an unterminated parameter list");
}

fn may_import_axum(file_name: &str) -> bool {
    matches!(
        file_name,
        "app.rs"
            | "auth.rs"
            | "config.rs"
            | "error.rs"
            | "http_boundary.rs"
            | "main.rs"
            | "request_context.rs"
            | "route_registry.rs"
            | "routes.rs"
            | "shared_audit.rs"
    ) || file_name.starts_with("route_")
}

#[test]
fn axum_stays_inside_http_boundary_modules() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();

    for entry in fs::read_dir(&src).expect("src directory must be readable") {
        let entry = entry.expect("src entry must be readable");
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .expect("Rust source names must be UTF-8");
        if may_import_axum(file_name) {
            continue;
        }
        let contents = source(&path);
        let production = contents
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&contents);
        if production.contains("use axum") || production.contains("axum::") {
            violations.push(file_name.to_string());
        }
    }

    assert!(
        violations.is_empty(),
        "non-HTTP modules import Axum: {}",
        violations.join(", ")
    );
}

#[test]
fn auth_extractors_depend_on_auth_state_not_the_router_state() {
    let auth = source(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/auth.rs"));
    assert!(!auth.contains("routes::"), "auth must not import routes");
    assert!(
        !auth.contains("AppState"),
        "auth extractors must depend on AuthState, not AppState"
    );
}

#[test]
fn routes_rs_is_a_router_facade_not_a_domain_handler_home() {
    let routes = source(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/routes.rs"));
    let inlined = REGISTERED_ROUTES
        .iter()
        .filter(|route| routes.contains(&format!("fn {}(", route.handler)))
        .map(|route| route.handler)
        .collect::<Vec<_>>();

    assert!(
        inlined.is_empty(),
        "registered domain handlers remain in routes.rs: {}",
        inlined.join(", ")
    );
}

#[test]
fn build_router_cannot_compose_routes_outside_the_registry() {
    let routes = source(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/routes.rs"));
    let start = routes
        .find("pub fn build_router")
        .expect("routes.rs must define build_router");
    let end = routes[start..]
        .find("\nasync fn redact_json_response")
        .map(|offset| start + offset)
        .expect("redaction middleware must follow build_router");
    let section = compact(&routes[start..end]);
    let body = section
        .split_once('{')
        .map(|(_, body)| body)
        .expect("build_router must have a body");

    assert!(
        body.starts_with("registered_router()"),
        "build_router must layer middleware directly onto registered_router()"
    );
    for forbidden in [
        "Router::new()",
        ".route(",
        ".route_service(",
        ".merge(",
        ".nest(",
        ".nest_service(",
        ".fallback(",
        ".fallback_service(",
    ] {
        assert!(
            !body.contains(forbidden),
            "build_router must not bypass declare_routes! with {forbidden}"
        );
    }
}

#[test]
fn registered_handlers_declare_exactly_their_one_guard() {
    let route_modules = route_module_source();
    for route in REGISTERED_ROUTES {
        let signature = handler_signature(&route_modules, route.handler);
        let present = [
            ("UserGuard", RouteGuard::User),
            ("CompanyWriterGuard", RouteGuard::CompanyWriter),
            ("AdminGuard", RouteGuard::Admin),
        ]
        .into_iter()
        .filter_map(|(name, guard)| signature.contains(name).then_some(guard))
        .collect::<Vec<_>>();
        let expected = match route.guard {
            RouteGuard::Public => Vec::new(),
            guard => vec![guard],
        };
        assert_eq!(
            present, expected,
            "{} {} handler {} has the wrong guard signature: {signature}",
            route.method, route.path, route.handler
        );
    }
}
