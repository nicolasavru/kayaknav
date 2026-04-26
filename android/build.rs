//! Build-time cache-busting stamp for the Gradle Android build.
//!
//! The root Gradle problem: `rust-android-gradle`'s `cargoBuild` task
//! caches its outputs based on inputs it declares internally —
//! typically *this* crate's `src/` and `Cargo.toml`. The main
//! `kayaknav` crate (a path dependency at `../`) and the `noaa_tides`
//! crate (`../tides`) are NOT in that declared input set, so after a
//! change in those sources Gradle may skip re-invoking cargo entirely
//! and leave a stale `.so` in the packaged APK.
//!
//! This build script plugs the gap by writing a single content-hash
//! file that `app/build.gradle.kts` declares as an input to
//! `mergeDebugJniLibFolders` / `mergeReleaseJniLibFolders`. When the
//! hash shifts, those tasks invalidate and AGP repackages.
//!
//! # Why a build script, not a Gradle-side directory input
//!
//! Listing each Rust source tree directly as `inputs.dir(...)` on
//! `cargoBuild` would also work, but it duplicates the dep graph in
//! two places (Cargo.toml's `path = ...` entries and the Gradle
//! file), so adding a new path dependency becomes a two-file edit.
//! Hashing from the Rust side keeps the set of tracked files rooted
//! in the Rust build's view of the world.
//!
//! # Interaction with cargoBuild caching
//!
//! For the stamp to ever refresh, `cargoBuild` itself must run at
//! least long enough for cargo to decide whether to re-execute this
//! script. `app/build.gradle.kts` forces this with
//! `outputs.upToDateWhen { false }` on the `cargoBuild*` tasks —
//! cargo's own incremental fingerprint check is fast on a no-op
//! build, so the extra invocation is cheap. Cargo re-runs this
//! script only when one of the paths below actually changed.

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let parent_crate = manifest_dir
        .parent()
        .expect("android/ must have a parent directory");

    // Gather every input we want the stamp to reflect. The hardcoded
    // list mirrors the path dependencies declared in
    // `android/Cargo.toml` and the root `Cargo.toml`: kayaknav (`..`),
    // noaa_tides (`../tides`), and this crate itself. If a new path
    // dep is added, include its `src/` here too.
    let mut inputs: Vec<PathBuf> = Vec::new();
    collect_rs(&manifest_dir.join("src"), &mut inputs);
    collect_rs(&parent_crate.join("src"), &mut inputs);
    collect_rs(&parent_crate.join("tides").join("src"), &mut inputs);
    for aux in [
        manifest_dir.join("Cargo.toml"),
        manifest_dir.join("Cargo.lock"),
        parent_crate.join("Cargo.toml"),
        parent_crate.join("Cargo.lock"),
    ] {
        if aux.exists() {
            inputs.push(aux);
        }
    }

    // Stabilise order: `read_dir` yields entries in OS-dependent
    // order, which would make the hash host-specific.
    inputs.sort();

    // Tell cargo to re-run this script whenever any of these files
    // change. Cargo already tracks `.rs` files it compiles directly,
    // but listing them here also covers `include_str!`-style embeds
    // and keeps the "when to regenerate the stamp" decision rooted in
    // one place.
    for p in &inputs {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    // Content hash of the tree. SipHash-1-3 (DefaultHasher) is plenty —
    // we only need "same input → same digest, different input →
    // different digest with overwhelming probability." The relative
    // path is folded in too so a renamed-but-identical file still
    // shifts the hash.
    let mut hasher = DefaultHasher::new();
    for p in &inputs {
        p.strip_prefix(parent_crate).unwrap_or(p).hash(&mut hasher);
        if let Ok(bytes) = fs::read(p) {
            bytes.hash(&mut hasher);
        }
    }
    let hash = hasher.finish();

    // The stamp lives under the Gradle app module's build/ directory,
    // so `gradle clean` wipes it along with all other build outputs —
    // matches the rest of the Android build lifecycle. The `generated/`
    // parent is the idiomatic location for machine-written inputs.
    let stamp = manifest_dir
        .join("app")
        .join("build")
        .join("generated")
        .join("rust-cachebust-stamp.txt");
    if let Some(parent) = stamp.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(&stamp, format!("{hash:016x}\n"));
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}
