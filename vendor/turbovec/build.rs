use std::process::Command;

fn rustc_minor_version() -> Option<u32> {
    let rustc = std::env::var_os("RUSTC")?;
    let output = Command::new(rustc).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }

    let version = std::str::from_utf8(&output.stdout)
        .ok()?
        .split_whitespace()
        .nth(1)?;
    let mut components = version.split('.');
    let major = components.next()?.parse::<u32>().ok()?;
    let minor = components.next()?.parse::<u32>().ok()?;
    (major == 1).then_some(minor)
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(turbovec_avx512_stable)");
    if rustc_minor_version().is_some_and(|minor| minor >= 89) {
        println!("cargo:rustc-cfg=turbovec_avx512_stable");
    }

    match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("linux") => println!("cargo:rustc-link-lib=openblas"),
        Ok("macos") => println!("cargo:rustc-link-lib=framework=Accelerate"),
        _ => {}
    }
}
