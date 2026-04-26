use std::sync::Arc;
use std::sync::atomic::Ordering;

use bitflags::bitflags;
use chrono::DateTime;
use chrono::offset::Local;
use egui::Align;
use egui::Align2;
use egui::Hyperlink;
use egui::Layout;
use egui::Panel;
use egui::ScrollArea;
use egui::Slider;
use egui::Window;
use egui_extras::Column;
use egui_extras::TableBuilder;
use galileo_egui::EguiMapState;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::impls::GeoPoint2d;
use parking_lot::Mutex;
use parking_lot::RwLock;
use polars::prelude::*;
use uom::fmt::DisplayStyle::Abbreviation;
use uom::si::f64::Time;
use uom::si::length::mile;
use uom::si::time::hour;
use uom::si::velocity::knot;

use crate::app::CurrentPredictionLayer;
use crate::app::WaypointClickAction;
use crate::noaa::CurrentPrediction;
use crate::saturating::Saturating;
use crate::scheduling::Trip;
use crate::setup::LoadProgress;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct WeekdayFlags: u8 {
        const Mon = 0b00000001;
        const Tue = 0b00000010;
        const Wed = 0b00000100;
        const Thu = 0b00001000;
        const Fri = 0b00010000;
        const Sat = 0b00100000;
        const Sun = 0b01000000;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Weekdays {
    pub mon: bool,
    pub tue: bool,
    pub wed: bool,
    pub thu: bool,
    pub fri: bool,
    pub sat: bool,
    pub sun: bool,
}

impl Default for Weekdays {
    fn default() -> Self {
        Self {
            mon: true,
            tue: true,
            wed: true,
            thu: true,
            fri: true,
            sat: true,
            sun: true,
        }
    }
}

impl From<Weekdays> for WeekdayFlags {
    fn from(value: Weekdays) -> Self {
        let mut flags = Self::empty();
        flags.set(Self::Mon, value.mon);
        flags.set(Self::Tue, value.tue);
        flags.set(Self::Wed, value.wed);
        flags.set(Self::Thu, value.thu);
        flags.set(Self::Fri, value.fri);
        flags.set(Self::Sat, value.sat);
        flags.set(Self::Sun, value.sun);
        flags
    }
}

pub struct UiState {
    pub pointer_position: Option<GeoPoint2d>,
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
    pub battery_tide_predictions: DataFrame,
    pub waypoint_mode: Arc<RwLock<WaypointClickAction>>,
    pub current_prediction_layer: CurrentPredictionLayer,
    pub sweep_weekdays: Weekdays,
    pub daytime: bool,
    pub arrive_before_sunset: bool,
    pub trip: Arc<RwLock<Trip>>,
    pub redraw_requested: bool,
    pub load_progress: LoadProgress,
    pub harcon_bytes: usize,
    /// Queue of prediction batches staged by the background loader.
    /// Drained at the top of every `run_ui` frame so the merge work
    /// runs under the UI thread's own locks, with no cross-thread
    /// contention against galileo's per-frame layer read.
    pub pending_merges: Arc<Mutex<Vec<CurrentPrediction<30>>>>,
    /// Scratch buffer bound to the Import/Export text box. Holds the
    /// most-recent Export output or pending Import input.
    pub waypoint_io_text: String,
    /// Last import/export status message — `Some` after a click that
    /// succeeded or failed, so the error or "exported N" info is
    /// visible next to the buttons. Cleared by the next action.
    pub waypoint_io_status: Option<String>,
    /// Scratch buffer for the "Export Trips" section. Holds the JSON
    /// produced by `Trip::export_best_departures_json` (the same
    /// format the `kayaknav_trip` CLI emits). Kept separate from
    /// `waypoint_io_text` so the two exports don't overwrite each
    /// other's content.
    pub trip_export_text: String,
    /// Last trip-export status message (mirrors `waypoint_io_status`).
    pub trip_export_status: Option<String>,
    /// Whether the narrow-screen modal is currently open. Only used in
    /// the narrow (mobile / phone) layout where the left panel is
    /// replaced by a FAB + modal overlay; ignored on wide screens.
    pub narrow_modal_open: bool,
    /// System-bar safe-area insets in *logical points*. On Android
    /// these are populated from `AndroidApp::content_rect()` each frame
    /// by `App::ui`; on other platforms they stay at zero. Used by the
    /// narrow-mode layout to keep the bottom control strip above the
    /// navigation bar and the ☰ / modal below the status bar.
    pub safe_area_top: f32,
    pub safe_area_bottom: f32,
}

impl UiState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time_idx: Arc<RwLock<Saturating<usize>>>,
        battery_tide_predictions: DataFrame,
        waypoint_mode: Arc<RwLock<WaypointClickAction>>,
        trip: Arc<RwLock<Trip>>,
        current_prediction_layer: CurrentPredictionLayer,
        load_progress: LoadProgress,
        harcon_bytes: usize,
        pending_merges: Arc<Mutex<Vec<CurrentPrediction<30>>>>,
    ) -> Self {
        Self {
            pointer_position: None,
            time_idx,
            battery_tide_predictions,
            waypoint_mode,
            current_prediction_layer,
            sweep_weekdays: Weekdays::default(),
            daytime: true,
            arrive_before_sunset: false,
            trip,
            redraw_requested: false,
            load_progress,
            harcon_bytes,
            pending_merges,
            waypoint_io_text: String::new(),
            waypoint_io_status: None,
            trip_export_text: String::new(),
            trip_export_status: None,
            narrow_modal_open: false,
            safe_area_top: 0.0,
            safe_area_bottom: 0.0,
        }
    }

    pub fn redraw_map(&mut self) {
        self.current_prediction_layer.write().update_all_features();
        self.redraw_requested = true;
    }
}

/// Format a byte count in KB/MB/GB with two decimals of precision. Uses
/// 1024-based units so the UI matches what most tools (browser task
/// manager, `du -h`, etc.) show.
fn fmt_bytes(n: usize) -> String {
    let n = n as f64;
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    if n >= GB {
        format!("{:.2} GB", n / GB)
    } else if n >= MB {
        format!("{:.2} MB", n / MB)
    } else if n >= KB {
        format!("{:.2} KB", n / KB)
    } else {
        format!("{} B", n as usize)
    }
}

fn degree_to_cardinal_direction(heading: f64) -> &'static str {
    const ARROWS: [&str; 8] = ["➡", "↗", "⬆", "↖", "⬅", "↙", "⬇", "↘"];
    let octant = ((heading / 45.0).round() as isize).rem_euclid(8) as usize;
    ARROWS[octant]
}

/// Format an i64 millisecond-since-epoch timestamp in the UI's
/// canonical `Day YYYY-MM-DD HH:MM:SS` form.
fn fmt_time_ms(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .unwrap()
        .naive_utc()
        .format("%a %Y-%m-%d %H:%M:%S")
        .to_string()
}

/// `"  sunset HH:MM"` for the given coordinate at the local date of
/// `ms`, or `""` if `coord` is None (map view isn't positioned yet)
/// or the `sunrise` crate returned None (polar day/night). Shown in
/// the device's local timezone because sunset is a geographic/visual
/// phenomenon — a NYC user expects "sunset 19:40", not the underlying
/// 23:40 UTC even though the rest of the UI is UTC-native. The
/// `sunrise` crate returns UTC, so we convert via `chrono::Local`
/// which relies on the OS timezone (accurate when the user is near
/// the viewed area, which is the overwhelming common case for a
/// kayak planner).
///
/// Decouples the readout from the trip: the top bar uses the *map
/// center*, so sunset shows up immediately when the user pans over a
/// region even before any waypoints are placed. The filter/export
/// paths still pin to the first waypoint because that's the actual
/// departure point and the only coordinate with planning meaning.
fn sunset_readout(coord: Option<(f64, f64)>, ms: i64) -> String {
    let Some((lat, lon)) = coord else {
        return String::new();
    };
    // Use the LOCAL date (not UTC date) so late-evening UTC rollover
    // doesn't pick tomorrow's sunset for tonight's session.
    let date = DateTime::from_timestamp_millis(ms)
        .unwrap()
        .with_timezone(&Local)
        .date_naive();
    crate::sun::sunset_utc(lat, lon, date)
        .map(|dt| format!("  sunset {}", dt.with_timezone(&Local).format("%H:%M")))
        .unwrap_or_default()
}

/// Index of the first `time_vec` entry at or after the device's local
/// "now". Falls back to the last index when "now" is past the cached
/// window (e.g. the app was left running for hours past the last
/// prediction) — more useful than the panic `.unwrap()` would produce.
fn now_time_idx(time_vec: &[i64]) -> usize {
    let now = Local::now().naive_local();
    time_vec
        .iter()
        .position(|dt| DateTime::from_timestamp_millis(*dt).unwrap().naive_utc() >= now)
        .unwrap_or(time_vec.len().saturating_sub(1))
}

/// Render the Move/Pause/Remove selectable triplet for waypoint-click
/// mode. Caller controls layout (horizontal vs. left_to_right) and any
/// spacing/padding adjustments; this just emits the three selectables
/// bound to the shared `state.waypoint_mode`.
fn render_waypoint_mode_selectors(ui: &mut egui::Ui, state: &UiState) {
    let mut waypoint_mode = state.waypoint_mode.write();
    ui.selectable_value(&mut *waypoint_mode, WaypointClickAction::Move, "Move");
    ui.selectable_value(&mut *waypoint_mode, WaypointClickAction::Pause, "Pause");
    ui.selectable_value(&mut *waypoint_mode, WaypointClickAction::Remove, "Remove");
}

/// Render the time slider + "Now" button pair. Assumes the caller has
/// already set `slider_width` via `ui.spacing_mut()` and set up any
/// outer layout wrapper. Triggers `state.redraw_map()` on slider drag
/// or on a "Now" click that actually moves the index.
fn render_time_slider_and_now(ui: &mut egui::Ui, state: &mut UiState, time_vec: &[i64]) {
    let time_range = 0..=state.time_idx.read().upper_bound();
    let slider = state.time_idx.write().with_val_mut(|val| {
        ui.add(
            Slider::new(val, time_range)
                .clamping(egui::SliderClamping::Always)
                .show_value(false),
        )
    });
    if slider.dragged() {
        state.redraw_map();
    }
    if ui.button("Now").clicked() && state.time_idx.write().set(now_time_idx(time_vec)) {
        state.redraw_map();
    }
}

/// Single-line "time  high_low  sunset" readout shown above the map
/// in both narrow and wide layouts. `high_low` comes from the
/// pre-computed battery-station tide labels; `sunset` follows the
/// user's current map center (see [`sunset_readout`]).
fn time_high_low_sunset_line(
    state: &UiState,
    time_vec: &[i64],
    map_center: Option<(f64, f64)>,
) -> String {
    let current_time_idx = state.time_idx.read().val();
    let time_str = fmt_time_ms(time_vec[current_time_idx]);
    let high_low = state.battery_tide_predictions["high_low"]
        .str()
        .unwrap()
        .get(current_time_idx)
        .unwrap();
    let sunset_str = sunset_readout(map_center, time_vec[current_time_idx]);
    format!("{time_str}  {high_low}{sunset_str}")
}

pub fn run_ui(state: &mut UiState, map_state: &EguiMapState, root_ui: &mut egui::Ui) {
    // Drain any predictions the background loader staged while we were
    // asleep and fold them into the trip and feature layer. Doing this
    // on the UI thread means the loader never takes `trip.write()` or
    // `current_prediction_layer.write()`, so it can't stall a paint
    // frame or compete with galileo's per-frame layer read.
    let drained: Vec<CurrentPrediction<30>> = std::mem::take(&mut *state.pending_merges.lock());
    if !drained.is_empty() {
        {
            let mut layer_w = state.current_prediction_layer.write();
            let store = layer_w.features_mut();
            for pred in &drained {
                store.add(pred.clone());
            }
            layer_w.update_all_features();
        }
        let _ = state.trip.write().add_predictions(drained);
        // Ensure galileo re-renders with the new features at the end
        // of this frame. The repaint-signal path also sets its dirty
        // flag, but that flag is consumed *before* run_ui runs — so
        // without this we'd paint once with the old layer state.
        state.redraw_requested = true;
    }

    let ctx = root_ui.ctx().clone();
    let screen_width = ctx.content_rect().width();
    let narrow = screen_width < 600.0;

    // Snapshot the map center once so the closures below can capture
    // it by Copy without re-borrowing `map_state`. `view().position()`
    // is `None` until galileo has finished its first view
    // computation; treating None as "no sunset yet" keeps the readout
    // quiet during the startup window rather than defaulting to an
    // arbitrary coordinate.
    let map_center: Option<(f64, f64)> = map_state
        .map()
        .view()
        .position()
        .map(|p| (p.lat(), p.lon()));

    let time_vec = state.battery_tide_predictions["time"]
        .datetime()
        .unwrap()
        .to_vec_null_aware()
        .unwrap_left();

    let render_controls = |ui: &mut egui::Ui, state: &mut UiState| {
        ui.spacing_mut().button_padding = (30.0, 10.00).into();
        ui.label("Time");
        ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
            if ui.button("⬅").clicked() && state.time_idx.write().dec() {
                state.redraw_map();
            }
            if ui.button("➡").clicked() && state.time_idx.write().inc() {
                state.redraw_map();
            }
        });

        ui.separator();

        ui.label("Waypoint mode for touch events (not yet implemented) or single mouse button operation.");
        ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
            render_waypoint_mode_selectors(ui, state);
        });

        ui.separator();

        if ui.button("Clear Waypoints").clicked() {
            state.trip.write().clear_waypoints();
            // Waypoint marks live on a galileo feature layer — dropping them
            // from the trip only mutates data; the map doesn't repaint until
            // something else (e.g. a mouse event) wakes galileo. Request a
            // redraw so they disappear immediately.
            state.redraw_map();
        }

        ui.separator();

        ui.collapsing("Import/Export Waypoints", |ui| {
            ui.label("Paste JSON here to import, or click Export to fill this box with the current waypoints.");
            ScrollArea::vertical()
                .max_height(160.0)
                .id_salt("waypoint_io_scroll")
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut state.waypoint_io_text)
                            .desired_width(f32::INFINITY)
                            .desired_rows(6)
                            .code_editor(),
                    );
                });

            ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                if ui.button("Export").clicked() {
                    match state.trip.read().export_waypoints_json() {
                        Ok(json) => {
                            let count = state.trip.read().waypoints.len();
                            state.waypoint_io_text = json;
                            state.waypoint_io_status = Some(format!("Exported {count} waypoint(s)."));
                        }
                        Err(e) => {
                            state.waypoint_io_status = Some(format!("Export failed: {e}"));
                        }
                    }
                }
                if ui.button("Copy").clicked() {
                    ctx.copy_text(state.waypoint_io_text.clone());
                    state.waypoint_io_status = Some("Copied to clipboard.".to_string());
                }
                if ui.button("Import").clicked() {
                    match state.trip.write().import_waypoints_json(&state.waypoint_io_text) {
                        Ok(n) => {
                            state.waypoint_io_status = Some(format!("Imported {n} waypoint(s)."));
                            // Feature layer mutated — force the map to repaint.
                            state.redraw_requested = true;
                        }
                        Err(e) => {
                            state.waypoint_io_status = Some(format!("Import failed: {e}"));
                        }
                    }
                }
            });

            if let Some(msg) = &state.waypoint_io_status {
                ui.label(msg);
            }
        });

        ui.collapsing("Export Trips", |ui| {
            ui.label(
                "Compute best departures (same output as the kayaknav_trip CLI) \
                 and drop the JSON here for copy/save.",
            );
            ScrollArea::vertical()
                .max_height(160.0)
                .id_salt("trip_export_scroll")
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut state.trip_export_text)
                            .desired_width(f32::INFINITY)
                            .desired_rows(6)
                            .code_editor(),
                    );
                });

            ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                // `export_best_departures_json` internally drives
                // `sweep_blocking`, which can take a few seconds on a
                // long trip. That matches the CLI's behavior; the UI
                // freeze is acceptable for a click-to-export flow.
                // Offloading to a worker would require threading the
                // result back through channels like the sweep worker
                // does — worth doing if users complain.
                if ui.button("Export").clicked() {
                    match state.trip.write().export_best_departures_json() {
                        Ok(json) => {
                            state.trip_export_status =
                                Some(format!("Exported {} bytes.", json.len()));
                            state.trip_export_text = json;
                        },
                        Err(e) => {
                            state.trip_export_status = Some(format!("Export failed: {e}"));
                        },
                    }
                }
                if ui.button("Copy").clicked() {
                    ctx.copy_text(state.trip_export_text.clone());
                    state.trip_export_status = Some("Copied to clipboard.".to_string());
                }
            });

            if let Some(msg) = &state.trip_export_status {
                ui.label(msg);
            }
        });
    };

    // Compact quick-access controls for the narrow-mode bottom bar.
    // Subset of `render_controls`: just the actions that are needed
    // while actively interacting with the map (scrubbing time, switching
    // waypoint mode, clearing). Less-frequent actions (Import/Export,
    // etc.) live in the modal's Controls collapsible.
    let render_bottom_bar = |ui: &mut egui::Ui, state: &mut UiState| {
        ui.add_space(4.0);
        // Compute uniform button height BEFORE allocating the row so
        // we can bound the row's `desired_size.y` to exactly `btn_h`.
        //
        // Why this matters: `ui.with_layout(right_to_left(Align::Center))`
        // creates a sub-Ui whose `max_rect` is the parent's full
        // remaining vertical space. `Align::Center` then vertically
        // centers content *within that full-height rect*, so the Ui's
        // `min_rect` grows to span top-of-panel → center-of-panel and
        // the bottom panel sizes itself to that huge min_rect —
        // swallowing the whole screen. Wrapping the whole row in an
        // `allocate_ui_with_layout(vec2(W, btn_h), …)` pins the row's
        // max_rect height to `btn_h`, so vertical centering happens
        // within a thin strip and the panel stays thin.
        ui.spacing_mut().button_padding = (12.0, 8.0).into();
        let btn_h =
            ui.text_style_height(&egui::TextStyle::Button) + 2.0 * ui.spacing().button_padding.y;
        let row_w = ui.available_width();

        ui.allocate_ui_with_layout(
            egui::vec2(row_w, btn_h),
            egui::Layout::right_to_left(egui::Align::Center),
            |ui| {
                // RIGHT — Clear lands first (rightmost) because of the
                // outer right_to_left layout. Because max_rect is
                // height-constrained above, Align::Center here only
                // centers Clear vertically within `btn_h`, not the
                // whole panel.
                if ui.button("Clear").clicked() {
                    state.trip.write().clear_waypoints();
                    state.redraw_map();
                }

                // LEFT & MIDDLE — pivot to left_to_right, then use the
                // remaining width (which is "everything except Clear")
                // for arrows and centered markers.
                ui.with_layout(egui::Layout::left_to_right(egui::Align::Center), |ui| {
                    let gap = ui.spacing().item_spacing.x;
                    let inner_w = ui.available_width();

                    // LEFT — arrows, sized to fill the left ~third
                    // of the remaining space. `add_sized([w, btn_h])`
                    // gives each arrow a consistent tap target and
                    // pins height so the emoji-font glyph can't
                    // stretch the row.
                    let arrow_w = ((inner_w / 3.0) - gap) / 2.0;
                    if ui
                        .add_sized([arrow_w, btn_h], egui::Button::new("⬅"))
                        .clicked()
                        && state.time_idx.write().dec()
                    {
                        state.redraw_map();
                    }
                    if ui
                        .add_sized([arrow_w, btn_h], egui::Button::new("➡"))
                        .clicked()
                        && state.time_idx.write().inc()
                    {
                        state.redraw_map();
                    }

                    // MIDDLE — markers centered in the leftover
                    // width. `top_down(Align::Center)` lays children
                    // vertically, centering each horizontally, so
                    // the single `horizontal` row ends up centered
                    // in the block. Button padding is trimmed
                    // horizontally so all three fit without eating
                    // into the arrows' space.
                    let remaining = ui.available_width();
                    ui.allocate_ui_with_layout(
                        egui::vec2(remaining, btn_h),
                        egui::Layout::top_down(egui::Align::Center),
                        |ui| {
                            ui.horizontal(|ui| {
                                ui.spacing_mut().button_padding = (8.0, 8.0).into();
                                render_waypoint_mode_selectors(ui, state);
                            });
                        },
                    );
                });
            },
        );
        ui.add_space(4.0);
    };

    // Narrow-mode top strip: the time slider, Now button, current
    // date/time, and battery high/low marker on a single always-visible
    // row above the map. Mirrors the controls in the modal's main
    // section so the common scrubbing action ("show me another time
    // index") doesn't require opening the modal.
    let render_top_bar = |ui: &mut egui::Ui, state: &mut UiState| {
        ui.add_space(4.0);
        ui.spacing_mut().button_padding = (12.0, 8.0).into();

        // Row 1: ☰ + slider + Now. The ☰ (modal-open) button used to
        // float in an Area in the top-left corner; with a real top
        // panel now in place it would overlap, so it's folded in as
        // the leftmost control here. Slider fills whatever remains
        // between ☰ and Now.
        ui.horizontal(|ui| {
            let gap = ui.spacing().item_spacing.x;
            let menu_w = ui.text_style_height(&egui::TextStyle::Button) * 1.5
                + 2.0 * ui.spacing().button_padding.x;
            let now_w = ui.text_style_height(&egui::TextStyle::Button) * 2.5
                + 2.0 * ui.spacing().button_padding.x;
            let slider_w = (ui.available_width() - menu_w - now_w - 2.0 * gap).max(1.0);

            if ui.button("☰").clicked() {
                state.narrow_modal_open = true;
            }

            ui.spacing_mut().slider_width = slider_w;
            render_time_slider_and_now(ui, state, &time_vec);
        });

        // Row 2: current time + high/low annotation + sunset, in a
        // read-only single-line TextEdit so long strings can be
        // horizontally scrolled/selected rather than clipped.
        let mut line: &str = &time_high_low_sunset_line(state, &time_vec, map_center);
        let _ = ui.add(egui::TextEdit::singleline(&mut line).desired_width(f32::INFINITY));

        ui.add_space(4.0);
    };

    let render_about = |ui: &mut egui::Ui| {
        ui.label("Source code is available at: ");
        ui.add(Hyperlink::new("https://github.com/nicolasavru/kayaknav").open_in_new_tab(true));

        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            ui.label("Map data from ");
            ui.add(
                Hyperlink::from_label_and_url(
                    "OpenStreetMap",
                    "https://www.openstreetmap.org/copyright",
                )
                .open_in_new_tab(true),
            );
            ui.label(".");
        });
    };

    if !narrow {
        Window::new("Controls")
            .anchor(Align2::RIGHT_TOP, [0.0, 0.0])
            .default_width(240.0)
            .show(&ctx, |ui| render_controls(ui, state));

        Window::new("About")
            .anchor(Align2::RIGHT_BOTTOM, [0.0, 0.0])
            .default_width(240.0)
            .show(&ctx, |ui| render_about(ui));
    }

    let panel_default = if narrow {
        screen_width.min(320.0)
    } else {
        380.0
    };
    let panel_min = if narrow { 120.0 } else { 200.0 };
    let panel_max = screen_width.max(panel_default);
    let station_col = if narrow { 140.0 } else { 256.0 };
    let depart_col = if narrow { 140.0 } else { 184.0 };

    // Body renderer: the entire scrollable content of the left panel /
    // modal. Takes `state` as a parameter rather than capturing it so we
    // can freely update `state.narrow_modal_open` before/after calling
    // it without tripping the borrow checker. Called from exactly one of
    // the two layout branches below per frame.
    let render_body = |ui: &mut egui::Ui, state: &mut UiState| {
        ScrollArea::vertical().show(ui, |ui| {
                if narrow {
                    ui.collapsing("Controls", |ui| render_controls(ui, state));
                    ui.separator();
                }
                ui.label("Arrows indicate current predictions; blue are harmonic stations and red are subordinate stations.");
                ui.add(Hyperlink::from_label_and_url(
                    "Details.",
                    "https://tidesandcurrents.noaa.gov/noaacurrents/Help")
                       .open_in_new_tab(true));

                ui.label("Use the left and right arrow keys to shift the time.");
                ui.label("Left click to place movement waypoints, middle click to place 0.5h pause waypoints, and right click to remove waypoints. Place multiple pause waypoints for a longer pause. Trips are calculated using waypoints in the order they were placed.");
                ui.label("A base travel speed of 3kt is assumed.");
                ui.label("WARNING: the current predictions (and, consequently, trip calculation) here are baseline predictions and do not take into account weather (recent rains, wind, etc.).");

                ui.separator();

                ui.label("Pointer position:");
                if let Some(pointer_position) = state.pointer_position {
                    ui.label(format!(
                        "Lat: {:.4} Lon: {:.4}",
                        pointer_position.lat(),
                        pointer_position.lon()
                    ));
                } else {
                    ui.label("<unavaliable>");
                }

                ui.separator();

                // Station load progress + memory footprint. The trip read-lock
                // is short because we only sum per-prediction DataFrame sizes
                // (a handful of column-chunk lengths per prediction).
                let completed = state.load_progress.completed.load(Ordering::Relaxed);
                let queued = state.load_progress.queued.load(Ordering::Relaxed);
                let total = state.load_progress.total;
                ui.label(format!(
                    "Stations loaded: {} / {} (out of {} total)",
                    completed, queued, total
                ));
                let prediction_bytes: usize = {
                    let trip = state.trip.read();
                    let sum_30 = trip.current_predictions_30m.values().map(|p| p.df.estimated_size()).sum::<usize>();
                    let sum_5 = trip.current_predictions_5m.values().map(|p| p.df.estimated_size()).sum::<usize>();
                    sum_30 + sum_5
                };
                ui.label(format!("Harcon data: {}", fmt_bytes(state.harcon_bytes)));
                ui.label(format!("Prediction cache: {}", fmt_bytes(prediction_bytes)));

                ui.separator();

                ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                    ui.spacing_mut().slider_width = 280.0;
                    render_time_slider_and_now(ui, state, &time_vec);
                });

                let mut line: &str = &time_high_low_sunset_line(state, &time_vec, map_center);
                let _ = ui.add(egui::TextEdit::singleline(&mut line));

                // Remaining sections index into per-station predictions
                // and the trip calculator by the same time slot shown
                // above. Read once after the string is built so the
                // value isn't tied to any specific render block.
                let current_time_idx = state.time_idx.read().val();

                ui.separator();

                let mut trip = state.trip.write();

                // Filter the displayed station list to those currently on
                // the visible map rect. `map_geo_to_screen_clipped` returns
                // `Some` iff the station's lat/lon projects inside the
                // screen bounds, so this matches exactly what the user sees
                // as arrows on the map.
                let view = map_state.map().view();
                let visible_indices: Vec<usize> = trip
                    .stations
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| view.map_geo_to_screen_clipped(&s.loc).map(|_| i))
                    .collect();

                TableBuilder::new(ui)
                    .max_scroll_height(400.0)
                    .column(Column::exact(station_col))
                    .column(Column::exact(32.0))
                    .column(Column::remainder())
                    .header(18.0, |mut header| {
                        header.col(|ui| {
                            ui.heading("Station (N to S, W to E)");
                        });
                        header.col(|ui| {
                            ui.heading("kt");
                        });
                        header.col(|ui| {
                            ui.heading("Dir.");
                        });
                    })
                    .body(|body| {
                        let row_height = 18.0;
                        let num_rows = visible_indices.len();
                        body.rows(row_height, num_rows, |mut row| {
                            let row_index = row.index();
                            let station = &trip.stations[visible_indices[row_index]];
                            let pred = &trip.current_predictions_30m[station];

                            let heading = pred.direction[current_time_idx];
                            let speed = pred.speed[current_time_idx];

                            row.col(|ui| {
                                ui.label(&pred.station.name);
                            });

                            row.col(|ui| {
                                ui.label(format!("{:.2}", speed));
                            });

                            row.col(|ui| {
                                ui.label(degree_to_cardinal_direction(heading));
                            });
                        });

                    });

                ui.separator();

                let waypoint_time_idx = crate::scheduling::sweep_time_ratio() * current_time_idx;

                let trip_result = trip.calculate(waypoint_time_idx);

                let mut distance_time: &str = match &trip_result {
                    Some(trip_result) => &format!(
                        "Total: {:.2}, {:.1}",
                        trip_result.distance().into_format_args(mile, Abbreviation),
                        trip_result.time().into_format_args(hour, Abbreviation),
                    ),
                    None => "Exceeded fetched data.",
                };
                ui.add(egui::TextEdit::singleline(&mut distance_time));

                ui.separator();

                let mut cumulative_time = Time::default();

                if let Some(trip_result) = trip_result {
                    for (i, (waypoint, step)) in trip.waypoints
                        .iter()
                        .zip(trip_result.steps)
                        .enumerate() {
                        cumulative_time += step.time;
                        let mut s: &str = &format!(
                            "{:?}. ({:.4}, {:.4}): {:.2}, {:.1}. {:.1}, {:.1}",
                            i,
                            waypoint.lat(),
                            waypoint.lon(),
                            step.distance.into_format_args(mile, Abbreviation),
                            step.time.into_format_args(hour, Abbreviation),
                            step.speed().into_format_args(knot, Abbreviation),
                            cumulative_time.into_format_args(hour, Abbreviation),
                        );
                        ui.add(egui::TextEdit::singleline(&mut s));
                    }
                }

                ui.separator();

                ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                    ui.toggle_value(&mut state.sweep_weekdays.mon, "Mon");
                    ui.toggle_value(&mut state.sweep_weekdays.tue, "Tue");
                    ui.toggle_value(&mut state.sweep_weekdays.wed, "Wed");
                    ui.toggle_value(&mut state.sweep_weekdays.thu, "Thu");
                    ui.toggle_value(&mut state.sweep_weekdays.fri, "Fri");
                    ui.toggle_value(&mut state.sweep_weekdays.sat, "Sat");
                    ui.toggle_value(&mut state.sweep_weekdays.sun, "Sun");
                });

                trip.set_weekdays(state.sweep_weekdays.into());

                ui.toggle_value(&mut state.daytime, "Leave after 8, Arrive before 9");
                trip.set_daytime(state.daytime);

                ui.toggle_value(&mut state.arrive_before_sunset, "Arrive before sunset");
                trip.set_arrive_before_sunset(state.arrive_before_sunset);

                ui.separator();


                // `sweep()` returns None while the current inputs' result
                // is still in flight on a background thread (and trivially
                // None when there are <2 waypoints). The table renders
                // empty in that window; a subsequent frame (triggered by
                // input or the next redraw) pulls the result in.
                let sw = trip.sweep();
                let sweep_pending = trip.waypoints.len() > 1 && sw.is_none();
                let (sweep_idx_vec, sweep_duration_vec): (Vec<u64>, Vec<f64>) = sw
                    .map(|df| {
                        (df["idx"].u64().unwrap().to_vec_null_aware().unwrap_left(),
                         df["duration"].f64().unwrap().to_vec_null_aware().unwrap_left())
                    })
                    .unwrap_or_default();

                if sweep_pending {
                    // Poke egui to repaint soon so the user sees the sweep
                    // appear without needing to nudge input.
                    ctx.request_repaint_after(std::time::Duration::from_millis(100));
                    let done = trip.sweep_progress.completed.load(Ordering::Relaxed);
                    let total = trip.sweep_progress.total.load(Ordering::Relaxed);
                    // `total` stays 0 until the worker has computed the
                    // start-index list — on the very first frame after
                    // kickoff we may land in that window, so omit the
                    // fraction rather than showing "0 / 0".
                    let suffix = if total > 0 { format!(" {} / {}", done, total) } else { String::new() };
                    ui.label(format!("Calculating departure-time sweep…{}", suffix));
                }

                ui.push_id(1, |ui| {
                    TableBuilder::new(ui)
                        .max_scroll_height(400.0)
                        .column(Column::exact(depart_col))
                        .column(Column::remainder())
                        .header(18.0, |mut header| {
                            header.col(|ui| {
                                ui.heading("Departure Time");
                            });
                            header.col(|ui| {
                                ui.heading("Duration");
                            });
                        })
                        .body(|body| {
                            let row_height = 18.0;
                            let num_rows = sweep_idx_vec.len();
                            body.rows(row_height, num_rows, |mut row| {
                                let row_index = row.index();

                                let idx = sweep_idx_vec[row_index];
                                let duration = sweep_duration_vec[row_index];

                                let time_str = fmt_time_ms(time_vec[idx as usize]);

                                row.col(|ui| {
                                    ui.label(time_str);
                                });

                                row.col(|ui| {
                                    ui.label(format!("{:.1}h", duration / 3600.0));
                                });
                            });
                        });
                });

                if narrow {
                    ui.separator();
                    ui.collapsing("About", |ui| render_about(ui));
                }
            });
    };

    if narrow {
        let safe_top = state.safe_area_top;
        let safe_bottom = state.safe_area_bottom;

        // Reserve blank strips for the status and navigation bars. These
        // are plain empty panels whose only purpose is to carve space
        // out of `root_ui`'s available rect so the CentralPanel (map)
        // and `narrow_bottom_controls` don't render underneath the
        // system bars. Skipped when the corresponding inset is zero
        // (desktop, or before Android's first ContentRectChanged).
        if safe_top > 0.0 {
            Panel::top("narrow_top_inset")
                .resizable(false)
                .exact_size(safe_top)
                .show_inside(root_ui, |_ui| {});
        }
        if safe_bottom > 0.0 {
            Panel::bottom("narrow_bottom_inset")
                .resizable(false)
                .exact_size(safe_bottom)
                .show_inside(root_ui, |_ui| {});
        }

        // Persistent top bar — time slider + Now + current-time
        // readout, always visible without opening the modal.
        // Registered *before* the bottom panel (egui's panel ordering
        // is registration-order-based) and after the status-bar inset
        // so it stacks directly below the system bar.
        Panel::top("narrow_top_controls")
            .resizable(false)
            .show_inside(root_ui, |ui| {
                render_top_bar(ui, state);
            });

        // Persistent bottom bar — quick time/waypoint controls always
        // visible without opening the modal. Rendered after the inset
        // spacer so it sits directly on top of the nav bar's reserved
        // strip rather than behind it.
        Panel::bottom("narrow_bottom_controls")
            .resizable(false)
            .show_inside(root_ui, |ui| {
                render_bottom_bar(ui, state);
            });

        // The narrow-layout ☰ (modal-open) button used to live in a
        // floating egui::Area at the top-left. Now that
        // `narrow_top_controls` is a real panel carrying the slider +
        // Now, the ☰ is folded into that panel as its leftmost
        // control — see `render_top_bar` above. No floating Area here
        // any more.

        if state.narrow_modal_open {
            // Defer the close write to after the modal body renders —
            // `render_body` borrows `state` mutably, so we can't also
            // touch `state.narrow_modal_open` from inside the same
            // closure scope. `modal.should_close()` covers both ESC and
            // backdrop clicks; the explicit ✕ is for touch-only users
            // who can't easily trigger either.
            let mut close_modal = false;
            let modal = egui::Modal::new(egui::Id::new("narrow_modal")).show(&ctx, |ui| {
                let max_w = screen_width.min(560.0) - 32.0;
                // Shrink the modal so both its top edge sits below the
                // status bar and its bottom edge sits above the nav
                // bar. 48.0 is slack for the Modal's own title padding.
                let max_h = ctx.content_rect().height() - safe_top - safe_bottom - 48.0;
                ui.set_max_width(max_w);
                ui.set_max_height(max_h);
                ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
                    if ui.button("✕").clicked() {
                        close_modal = true;
                    }
                });
                render_body(ui, state);
            });
            if modal.should_close() {
                close_modal = true;
            }
            if close_modal {
                state.narrow_modal_open = false;
            }
        }
    } else {
        Panel::left("KayakNav")
            .resizable(true)
            .default_size(panel_default)
            .size_range(panel_min..=panel_max)
            .show_inside(root_ui, |ui| render_body(ui, state));
    }
}
