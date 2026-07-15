# Nowledge patch provenance

This directory vendors the crates.io `turbovec` 0.9.0 release from upstream
tag `v0.9.0` (`1e7200cfd8f26c92ce2855652db64bc7f85bc039`; crates.io checksum
`0715a5a1365e86d0ae6396fa0c011f319b8e8024b3896cd16e6468e29ed3a325`).

The source differs only to make the upstream AVX-512 kernel conditional on a
compiler where AVX-512 intrinsics are stable. Rust 1.88 therefore uses the
existing AVX2/scalar runtime fallback, while Rust 1.89 and newer preserve the
upstream AVX-512 dispatch. The build script fails closed to the AVX2 path when
it cannot identify the compiler version.

The root dependency is pinned to `=0.9.0` so a lockfile refresh cannot silently
select a newer registry release and bypass this patch.

Remove this patch after upstream publishes an equivalent release and the
Nowledge MSRV check passes against it on Linux x86_64.

Do not publish the Nowledge crate while this patch is required. Cargo omits a
root `[patch.crates-io]` override from the normalized published manifest, so a
downstream package would resolve the unpatched crates.io release and would not
actually satisfy the advertised x86_64 Rust 1.88 contract. The root manifest's
`publish = false` setting enforces that boundary.
