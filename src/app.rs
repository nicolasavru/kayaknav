use std::sync::Arc;

use galileo::Map;
use galileo::control::EventPropagation;
use galileo::control::MouseButton;
use galileo::control::MouseEvent;
use galileo::control::UserEvent;
use galileo::control::UserEventHandler;
use galileo::layer::feature_layer::FeatureLayer;
use galileo_egui::EguiMapState;
use galileo_types::cartesian::Point2;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geometry_type::GeoSpace2d;
use parking_lot::RwLock;

use crate::features;
use crate::features::CurrentPredictionSymbol;
use crate::features::WaypointType;
use crate::noaa::CurrentPrediction;
use crate::run_ui::UiState;
use crate::run_ui::run_ui;
use crate::scheduling::Trip;
use crate::setup::RepaintSignal;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WaypointClickAction {
    Move,
    Pause,
    Remove,
}

pub type CurrentPredictionLayer = Arc<
    RwLock<FeatureLayer<GeoPoint2d, CurrentPrediction<30>, CurrentPredictionSymbol, GeoSpace2d>>,
>;

pub struct App {
    pub map_state: EguiMapState,
    pub ui_state: UiState,
    pub last_pointer_position: Arc<RwLock<Option<Point2>>>,
    /// Published to the loader every frame. The loader decides when a
    /// meaningful move has occurred and does the station-list scan
    /// itself, so the UI thread only pays for a single RwLock write
    /// here rather than iterating ~4000 stations inline.
    pub pending_center: Arc<RwLock<Option<(f64, f64)>>>,
    /// Shared with the loader. On each frame we install the egui
    /// `Context` (once) and check the dirty flag — if the loader has
    /// merged new predictions since our last frame, call
    /// `map.redraw()` so galileo picks them up.
    pub repaint: Arc<RepaintSignal>,
    /// Handle used to query the system safe-area insets every frame.
    /// NativeActivity's drawable surface always fills the full window,
    /// so a theme-level `fitsSystemWindows` has no effect — we have to
    /// read `content_rect()` and inset our own layout. Kept here (not
    /// in UiState) because the handle itself is Android-specific; the
    /// inset values it yields are platform-neutral f32 points.
    #[cfg(target_os = "android")]
    pub android_app: android_activity::AndroidApp,
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Idempotent: first frame plumbs the egui context into the
        // repaint signal so the loader can wake eframe between user
        // inputs. Subsequent frames are no-ops.
        self.repaint.install_ctx(ui.ctx());

        // Refresh system safe-area insets (Android only). `content_rect`
        // comes in physical pixels with (0,0) at window top-left; the
        // native window dims give us the full drawable rect. Both can
        // shift at runtime (rotation, fold, keyboard), so we re-read
        // each frame — the cost is two mutex reads behind AndroidApp's
        // `Arc`. `content_rect` may be empty before Android has fired
        // `onContentRectChanged`; in that window we leave insets at 0.
        #[cfg(target_os = "android")]
        {
            let rect = self.android_app.content_rect();
            if rect.right > rect.left && rect.bottom > rect.top {
                if let Some(window) = self.android_app.native_window() {
                    let ppp = ui.ctx().pixels_per_point().max(1.0);
                    let win_h = window.height() as f32;
                    let top_px = rect.top as f32;
                    let bottom_px = (win_h - rect.bottom as f32).max(0.0);
                    self.ui_state.safe_area_top = top_px / ppp;
                    self.ui_state.safe_area_bottom = bottom_px / ppp;
                }
            }
        }

        let screen_pos = *self.last_pointer_position.read();
        self.ui_state.pointer_position =
            screen_pos.and_then(|p| self.map_state.map().view().screen_to_map_geo(p));

        // Publish the current map center to the loader. Cheap: one
        // `RwLock` write with no contention. The loader consults this
        // between batches and triggers its own scan when the center
        // has moved far enough.
        if let Some(center) = self.map_state.map().view().position() {
            *self.pending_center.write() = Some((center.lat(), center.lon()));
        }

        // Drain any background-merge repaint request. `map.redraw()`
        // has to run on the UI thread (it needs `&mut Map`), so the
        // loader can only flip the flag; we consume it here.
        if self.repaint.take_dirty() {
            self.map_state.map().redraw();
        }

        let (right, left) = ui.ctx().input(|i| {
            (
                i.key_pressed(egui::Key::ArrowRight),
                i.key_pressed(egui::Key::ArrowLeft),
            )
        });
        if right && self.ui_state.time_idx.write().inc() {
            self.ui_state.redraw_map();
        }
        if left && self.ui_state.time_idx.write().dec() {
            self.ui_state.redraw_map();
        }

        run_ui(&mut self.ui_state, &self.map_state, ui);

        if std::mem::take(&mut self.ui_state.redraw_requested) {
            self.map_state.map().redraw();
        }

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.map_state.render(ui);
        });
    }
}

pub fn make_waypoint_handler(
    trip: Arc<RwLock<Trip>>,
    waypoint_mode: Arc<RwLock<WaypointClickAction>>,
    last_pointer_position: Arc<RwLock<Option<Point2>>>,
) -> Box<dyn UserEventHandler> {
    Box::new(move |ev: &UserEvent, map: &mut Map| {
        // Pointer move updates are always safe, and not a Click.
        if let UserEvent::PointerMoved(MouseEvent {
            screen_pointer_position,
            ..
        }) = ev
        {
            *last_pointer_position.write() = Some(*screen_pointer_position);
            return EventPropagation::Propagate;
        }

        // Map a Click to the concrete waypoint action it triggers, if
        // any. A touch tap arrives as `MouseButton::Other` (galileo
        // suppresses the synthetic-mouse `Left` click that follows), so
        // it's treated exactly like `Left` — i.e. whatever the current
        // `waypoint_mode` dictates. `Middle` / `Right` are desktop-only
        // shortcuts for Pause / Remove regardless of mode.
        if let UserEvent::Click(
            btn,
            MouseEvent {
                screen_pointer_position: pos,
                ..
            },
        ) = ev
        {
            let act = match btn {
                MouseButton::Left | MouseButton::Other => *waypoint_mode.read(),
                MouseButton::Middle => WaypointClickAction::Pause,
                MouseButton::Right => WaypointClickAction::Remove,
            };
            let _ = match act {
                WaypointClickAction::Move => {
                    features::add_waypoint(map, trip.clone(), *pos, WaypointType::Move)
                },
                WaypointClickAction::Pause => {
                    features::add_waypoint(map, trip.clone(), *pos, WaypointType::Pause)
                },
                WaypointClickAction::Remove => features::remove_waypoints(map, trip.clone(), *pos),
            };
        }

        EventPropagation::Propagate
    })
}
