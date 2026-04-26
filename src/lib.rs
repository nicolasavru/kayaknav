use std::path::PathBuf;
use std::sync::Arc;

#[cfg(not(target_os = "android"))]
use galileo_egui::InitBuilder;
use parking_lot::RwLock;

pub mod app;
mod error_utils;
mod features;
pub mod noaa;
pub mod prelude;
mod run_ui;
mod saturating;
pub mod scheduling;
pub mod setup;
pub mod sun;

use crate::app::App;
use crate::app::make_waypoint_handler;
use crate::run_ui::UiState;
/// Re-export for CLI/external callers that want to toggle Trip's
/// weekday filter without reaching into the UI-internal `run_ui`
/// module. The bitflags type itself has no UI dependencies — it's the
/// surrounding egui widgets in `run_ui` that do — so re-exporting the
/// type alone keeps the contamination contained.
pub use crate::run_ui::WeekdayFlags;
use crate::setup::SetupBundle;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Config {
    pub use_api_proxy: bool,
    pub api_proxy_url: String,
    /// Directory for the raster tile cache. The default `.tile_cache`
    /// is CWD-relative and works for desktop/CLI usage launched from
    /// the repo root. Android callers MUST override this with an
    /// absolute path (typically `AndroidApp::internal_data_path().join(...)`) —
    /// Android processes run with `/` as CWD and no write access to it.
    /// On wasm this is ignored: `with_file_cache_checked` is a no-op
    /// under wasm32.
    pub tile_cache_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            use_api_proxy: false,
            api_proxy_url: "https://kayaknav.com/proxy".to_string(),
            tile_cache_dir: PathBuf::from(".tile_cache"),
        }
    }
}

// Desktop + wasm entry point. On Android we use `launch_android`
// instead (which threads an `AndroidApp` handle into App for safe-area
// inset queries); this function has no way to construct that handle, so
// it's cfg-gated out of the Android target to avoid an E0063 "missing
// field `android_app`" error when the App struct grows an Android-only
// field.
#[cfg(not(target_os = "android"))]
pub fn launch(bundle: SetupBundle) -> eframe::Result {
    let SetupBundle {
        map,
        trip,
        time_idx,
        battery_tide_predictions,
        waypoint_mode,
        current_prediction_layer,
        load_progress,
        harcon_bytes,
        pending_center,
        repaint,
        pending_merges,
    } = bundle;

    let last_pointer_position = Arc::new(RwLock::new(None));
    let handler = make_waypoint_handler(
        trip.clone(),
        waypoint_mode.clone(),
        last_pointer_position.clone(),
    );

    let ui_state = UiState::new(
        time_idx,
        battery_tide_predictions,
        waypoint_mode,
        trip,
        current_prediction_layer,
        load_progress,
        harcon_bytes,
        pending_merges,
    );

    let mut builder = InitBuilder::new(map)
        .with_handlers([handler])
        .with_logging(false)
        .with_app_builder(move |map_state, _cc| {
            Box::new(App {
                map_state,
                ui_state,
                last_pointer_position,
                pending_center,
                repaint,
            })
        });

    #[cfg(all(not(target_arch = "wasm32"), not(target_os = "android")))]
    {
        builder = builder.with_app_name("KayakNav");
    }

    #[cfg(target_arch = "wasm32")]
    {
        builder = builder.with_canvas_id("the_canvas_id");
    }

    builder.init()
}

/// Android entry point. Same plumbing as [`launch`] but with the
/// `AndroidApp` handle threaded into winit via `event_loop_builder`,
/// and the `Wgpu` renderer forced (eframe's default `Glow` backend has
/// no Android wiring in the current release).
///
/// Mirrors the per-feature structure of `launch` intentionally — the
/// full UI (map, trip table, waypoint clicks) is the same on Android
/// as on desktop; only the platform hook-up differs. If the Android UI
/// eventually needs a different layout (e.g. collapsible panels for
/// small screens), branch inside `App::update`, not here.
#[cfg(target_os = "android")]
pub fn launch_android(
    bundle: SetupBundle,
    android_app: android_activity::AndroidApp,
) -> eframe::Result {
    let SetupBundle {
        map,
        trip,
        time_idx,
        battery_tide_predictions,
        waypoint_mode,
        current_prediction_layer,
        load_progress,
        harcon_bytes,
        pending_center,
        repaint,
        pending_merges,
    } = bundle;

    let last_pointer_position = Arc::new(RwLock::new(None));
    let handler = make_waypoint_handler(
        trip.clone(),
        waypoint_mode.clone(),
        last_pointer_position.clone(),
    );

    let ui_state = UiState::new(
        time_idx,
        battery_tide_predictions,
        waypoint_mode,
        trip,
        current_prediction_layer,
        load_progress,
        harcon_bytes,
        pending_merges,
    );

    // eframe 0.34's native runner `take()`s `android_app` out of
    // `NativeOptions` and feeds it to winit's
    // `EventLoopBuilderExtAndroid::with_android_app` internally; it
    // errors with "NativeOptions is missing required android_app" if
    // this is left `None`. Setting `event_loop_builder` ourselves to
    // do the same call is not equivalent — the runner does not skip
    // its own check when we provide one.
    log::info!("launch_android: populating NativeOptions.android_app");
    // AndroidApp is internally Arc-backed, so cloning it just bumps
    // a refcount. We give one clone to NativeOptions (eframe moves
    // it into winit) and keep another in App so we can query
    // `content_rect()` / `native_window()` every frame for safe-area
    // insets — NativeActivity won't honor `fitsSystemWindows`, so we
    // have to compute the inset ourselves.
    let android_app_for_app = android_app.clone();
    let native_options = eframe::NativeOptions {
        android_app: Some(android_app),
        renderer: eframe::Renderer::Wgpu,
        ..Default::default()
    };

    galileo_egui::InitBuilder::new(map)
        .with_handlers([handler])
        .with_logging(false)
        .with_native_options(native_options)
        .with_app_builder(move |map_state, _cc| {
            Box::new(App {
                map_state,
                ui_state,
                last_pointer_position,
                pending_center,
                repaint,
                android_app: android_app_for_app,
            })
        })
        .init()
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(start)]
pub async fn init() {
    let config = Config {
        use_api_proxy: option_env!("KAYAKNAV_USE_API_PROXY")
            .map(|s| {
                s.parse::<bool>()
                    .expect("KAYAKNAV_USE_API_PROXY must be 'true' or 'false'")
            })
            .unwrap_or(true),
        api_proxy_url: option_env!("KAYAKNAV_API_PROXY_URL")
            .map(str::to_string)
            .unwrap_or_else(|| Config::default().api_proxy_url),
        // On wasm, `with_file_cache_checked` is a no-op — the value is
        // ignored. The default is kept so construction is infallible.
        ..Config::default()
    };

    let bundle = setup::build(config).await.expect("failed to set up app");
    let _ = launch(bundle);
}
