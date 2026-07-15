use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=NOWLEDGE_GIT_REVISION");
    let git_head = git_head();
    let rev = configured_revision(git_head.as_deref())
        .or_else(|| git_short_rev(git_head.as_deref()))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=NOWLEDGE_GIT_REV={rev}");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}

fn configured_revision(git_head: Option<&str>) -> Option<String> {
    let revision = std::env::var("NOWLEDGE_GIT_REVISION").ok()?;
    if !(7..=64).contains(&revision.len()) || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        panic!("NOWLEDGE_GIT_REVISION must contain 7 to 64 hexadecimal characters");
    }
    let revision = revision.to_ascii_lowercase();
    if let Some(git_head) = git_head {
        if !git_head.starts_with(&revision) {
            panic!(
                "NOWLEDGE_GIT_REVISION does not identify the checked-out Git HEAD; refusing a false build attestation"
            );
        }
        match git_tracked_dirty() {
            Ok(false) => {}
            Ok(true) => panic!(
                "NOWLEDGE_GIT_REVISION requires a clean tracked checkout; refusing to attest modified source"
            ),
            Err(error) => panic!(
                "NOWLEDGE_GIT_REVISION requires verified checkout cleanliness; Git status failed: {error}"
            ),
        }
    }
    Some(revision)
}

fn git_head() -> Option<String> {
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !head.status.success() {
        return None;
    }
    let rev = String::from_utf8(head.stdout)
        .ok()?
        .trim()
        .to_ascii_lowercase();
    (!rev.is_empty()).then_some(rev)
}

fn git_tracked_dirty() -> Result<bool, String> {
    // Untracked files are excluded so runtime artifacts next to a deploy
    // checkout (logs, env backups) do not permanently mark builds dirty.
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .map_err(|error| error.to_string())?;
    if !status.status.success() {
        return Err(format!("git status exited with {}", status.status));
    }
    Ok(!status.stdout.is_empty())
}

fn git_short_rev(git_head: Option<&str>) -> Option<String> {
    let mut rev = git_head?.chars().take(7).collect::<String>();
    match git_tracked_dirty() {
        Ok(true) => rev.push_str("-dirty"),
        Ok(false) => {}
        Err(_) => return None,
    }
    Some(rev)
}
