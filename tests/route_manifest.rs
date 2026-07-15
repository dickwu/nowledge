use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use nowledge::{RouteMetadata, REGISTERED_ROUTES};
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

impl From<&RouteMetadata> for RouteTriple {
    fn from(route: &RouteMetadata) -> Self {
        Self {
            method: route.method.to_string(),
            path: route.path.to_string(),
            handler: route.handler.to_string(),
        }
    }
}

fn manifest() -> Vec<ManifestEntry> {
    serde_json::from_str(include_str!("../doc/api_manifest.json"))
        .expect("doc/api_manifest.json must be valid")
}

#[test]
fn router_and_api_manifest_have_the_same_exact_endpoint_triples() {
    let routes: BTreeSet<_> = REGISTERED_ROUTES.iter().map(RouteTriple::from).collect();
    let manifest = manifest();
    let documented: BTreeSet<_> = manifest.iter().map(RouteTriple::from).collect();

    assert_eq!(
        REGISTERED_ROUTES.len(),
        90,
        "the registered endpoint count changed"
    );
    assert_eq!(
        routes.len(),
        REGISTERED_ROUTES.len(),
        "the route registry contains duplicate endpoint triples"
    );
    assert_eq!(manifest.len(), 90, "the endpoint manifest count changed");
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
