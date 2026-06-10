use std::process::Command;

fn main() {
    let rev = git_short_rev().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=NOWLEDGE_GIT_REV={rev}");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}

fn git_short_rev() -> Option<String> {
    let head = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !head.status.success() {
        return None;
    }
    let mut rev = String::from_utf8(head.stdout).ok()?.trim().to_string();
    if rev.is_empty() {
        return None;
    }
    // Untracked files are excluded so runtime artifacts next to a deploy
    // checkout (logs, env backups) do not permanently mark builds dirty.
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()?;
    if status.status.success() && !status.stdout.is_empty() {
        rev.push_str("-dirty");
    }
    Some(rev)
}
