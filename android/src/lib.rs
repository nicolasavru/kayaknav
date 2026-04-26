//! Android entry point for kayaknav.
//!
//! # Architecture
//!
//! The Android build is structurally different from the desktop + wasm
//! builds in one important way: there is no `main()`. Android's
//! `NativeActivity` class loads this crate as a shared library
//! (`libkayaknav_android.so`) and calls into `android_main` (mangled as
//! `ANativeActivity_onCreate` by the `android_activity` crate). That
//! function is given an `AndroidApp` handle, which is what eframe needs
//! to hook into the platform event loop.
//!
//! This file is intentionally thin — all the actual work (map setup,
//! trip state, UI) lives in the main `kayaknav` crate and is shared
//! with the desktop and wasm builds. This crate only does three things:
//! install the Android logger bridge, pick a cache directory under the
//! app's private internal data path, and hand the `AndroidApp` off to
//! `kayaknav::launch_android`.

#![cfg(target_os = "android")]

use android_activity::AndroidApp;
use log::LevelFilter;

/// Entry point called by Android's NativeActivity once the app process
/// is ready. `android_activity`'s proc-macro equivalent is `#[no_mangle]`
/// + `android_main(app: AndroidApp)`; doing it by hand keeps the call
/// site fully visible and avoids pulling in the macro-only feature.
#[unsafe(no_mangle)]
fn android_main(app: AndroidApp) {
    android_logger::init_once(
        android_logger::Config::default()
            .with_max_level(LevelFilter::Info)
            .with_tag("kayaknav"),
    );

    log::info!("kayaknav_android: entering android_main");

    // Use the app's private internal data path for the tile cache.
    // `internal_data_path()` returns `Some(...)` in the normal NativeActivity
    // flow; if it ever returns `None` (e.g. unusual embedding) we fall back
    // to the CWD-relative default, which at worst means tiles are re-fetched
    // every session.
    let mut config = kayaknav::Config::default();
    if let Some(data_dir) = app.internal_data_path() {
        config.tile_cache_dir = data_dir.join("tile_cache");
        log::info!(
            "kayaknav_android: tile cache at {}",
            config.tile_cache_dir.display()
        );
    } else {
        log::warn!(
            "kayaknav_android: AndroidApp::internal_data_path() returned None — \
             falling back to default cache path ({})",
            config.tile_cache_dir.display()
        );
    }

    // `setup::build` is async but performs no real I/O that needs a
    // runtime — it's mostly harmonic math over the embedded station
    // store plus channel wiring. `block_on` on the Android main thread
    // is fine because this runs before the event loop starts; once
    // `launch_android` hands control to eframe, the background loader
    // runs on its own `std::thread` (spawned inside `setup::build`).
    let bundle = match futures::executor::block_on(kayaknav::setup::build(config)) {
        Ok(bundle) => bundle,
        Err(err) => {
            log::error!("kayaknav_android: setup failed: {err:#}");
            return;
        },
    };

    if let Err(err) = kayaknav::launch_android(bundle, app) {
        log::error!("kayaknav_android: launch_android returned error: {err}");
    }
}
