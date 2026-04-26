import org.gradle.api.tasks.PathSensitivity

plugins {
    id("com.android.application")
    id("org.mozilla.rust-android-gradle.rust-android")
}

android {
    namespace = "net.avrutin.kayaknav"
    // compileSdk 34 (Android 14) is the current target; minSdk 26
    // (Android 8.0) is the floor android_activity's native-activity
    // feature supports without extra work.
    compileSdk = 34

    // Pin the NDK version AGP resolves. Without this, AGP picks an
    // auto-detected NDK and throws "NDK is not installed" if the
    // side-by-side NDK it expects isn't present. Reading from the
    // `ndkVersion` project property (declared in gradle.properties)
    // keeps the version defined in one place and makes it overridable
    // on the command line with `-PndkVersion=...`.
    ndkVersion = (findProperty("ndkVersion") as String?)
        ?: error("Set `ndkVersion` in gradle.properties (e.g. 30.0.14904198)")

    defaultConfig {
        applicationId = "net.avrutin.kayaknav"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "0.1.0"
    }

    // The Rust cdylib contains all executable logic — there's no Java
    // or Kotlin, so the default bytecode directories stay empty. We
    // still need to declare a source set so AGP knows not to look for
    // a MainActivity class.
    sourceSets {
        getByName("main") {
            java.setSrcDirs(emptyList<String>())
            manifest.srcFile("src/main/AndroidManifest.xml")
        }
    }

    buildTypes {
        getByName("release") {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("debug")
        }
    }

    // NOTE: we intentionally do NOT register
    // `build/rustJniLibs/android` as a jniLibs source dir here.
    // `rust-android-gradle` 0.9.x already wires its output directory
    // into AGP's main jniLibs source set via its Android integration.
    // Adding it a second time makes AGP enumerate the same directory
    // twice and fail `mergeDebugJniLibFolders` with "Duplicate
    // resources" errors on every .so.

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }


}

cargo {
    // `module` is the relative path from this Gradle module to the
    // Cargo.toml that produces the cdylib. Cargo.toml is one level up
    // (`android/Cargo.toml`), so `..` from `android/app/`.
    module = ".."
    libname = "kayaknav_android"

    // ABIs to build. We target `arm64` only — that covers every modern
    // physical Android device (phones, tablets). Add `x86_64` back if
    // you want to run on the Android emulator on an Intel/AMD dev
    // machine, or `arm`/`x86` for legacy 32-bit hardware. Each extra
    // triple adds a full cross-compile to every build, so keep the list
    // minimal.
    targets = listOf("arm64")

    // Rust release profile by default — debug builds of wgpu/galileo are
    // dramatically slower on-device (tile rendering, shader compilation)
    // and produce a much larger .so. Override with `-PcargoProfile=debug`
    // when you actually need unoptimized Rust (e.g. for backtraces or
    // `dbg!`-style prints). Note: this is independent of AGP's Java/
    // Kotlin debug/release build types — a `./gradlew assembleDebug`
    // still packages a release-optimized cdylib unless overridden.
    profile = (findProperty("cargoProfile") as String?) ?: "release"

    // Pass through the NDK version AGP resolved. `pythonCommand` is set
    // because rust-android-gradle's post-build step shells out to python
    // for arch layout fixup on some hosts.
    pythonCommand = "python3"
}

// The Android crate's `build.rs` writes a content-hash of every
// tracked Rust source (this crate + kayaknav + noaa_tides + both
// Cargo.toml/Cargo.lock pairs) into this file on every cargo
// invocation. When a Rust source changes, its hash shifts; declaring
// the file as an input to the merge tasks below turns that shift
// into a Gradle cache invalidation, so AGP repackages the APK with
// the fresh `.so`. Without this, Gradle can cache `mergeJniLibFolders`
// based on an unchanged `.so` file even when cargo produced new code
// — uncommon in debug builds, but possible with fully-deterministic
// release builds.
val rustCacheBustStamp = file("build/generated/rust-cachebust-stamp.txt")

// For the stamp to ever refresh, cargoBuild itself has to run —
// otherwise cargo never executes `build.rs` and the stamp stays
// stale. `doNotTrackState` achieves that ("always run, never treat
// as up-to-date") while *also* disabling Gradle's input/output
// snapshotting, which is what we actually need here: the
// rust-android-gradle 0.9.6 plugin exposes its `ndk` task input
// using a non-Serializable Kotlin `data class Ndk(path, version)`,
// so any Gradle code path that tries to snapshot the inputs fails
// with "Cannot fingerprint input property 'ndk': ... cannot be
// serialized." Skipping state tracking short-circuits that.
//
// Trade-off: cargoBuild always runs, so every Gradle invocation
// incurs a ~1-2 s cargo-fingerprint scan even when nothing changed.
// Cargo itself is the authority on whether to rebuild; Gradle just
// has to get out of the way. This also gives `build.rs` a
// guaranteed chance to refresh the cache-bust stamp below.
tasks.matching { it.name.startsWith("cargoBuild") }.configureEach {
    doNotTrackState(
        "cargoBuild manages its own incremental compilation; " +
            "Gradle state tracking is both redundant and incompatible " +
            "with rust-android-gradle 0.9.6's non-Serializable `ndk` " +
            "input property (fingerprint serialization fails)."
    )
}

// Hook so `./gradlew build` triggers the Rust cross-compile before AGP
// packages the APK. Without this, the .so files would be missing on
// the first build.
tasks.whenTaskAdded {
    if (name == "mergeDebugJniLibFolders" || name == "mergeReleaseJniLibFolders") {
        dependsOn("cargoBuild")
        // `optional(true)` is important on a clean checkout: the stamp
        // doesn't exist until cargoBuild has run once. Gradle would
        // otherwise fail task-input validation at configuration time.
        inputs.file(rustCacheBustStamp)
            .withPropertyName("rustCacheBustStamp")
            .withPathSensitivity(PathSensitivity.NONE)
            .optional(true)
    }
}

// Make `cargoBuild` visible as a named dependency target the Android
// plugin can consume. Plugin registers it dynamically, so we name it
// here for clarity.
tasks.register("rebuildRust") {
    group = "rust"
    description = "Clean + rebuild the Rust cdylib for every configured ABI."
    dependsOn("cargoBuild")
}
