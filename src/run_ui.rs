use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

use bitflags::bitflags;
use chrono::offset::Local;
use chrono::DateTime;
use egui::Align;
use egui::Align2;
use egui::Context;
use egui::Hyperlink;
use egui::Layout;
use egui::ScrollArea;
use egui::SidePanel;
use egui::Slider;
use egui::Window;
use egui_extras::Column;
use egui_extras::TableBuilder;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geo::GeoPoint;
use ordered_float::OrderedFloat;
use polars::prelude::*;
use uom::fmt::DisplayStyle::Abbreviation;
use uom::si::f64::Ratio;
use uom::si::f64::Time;
use uom::si::length::mile;
use uom::si::time::hour;
use uom::si::time::minute;
use uom::si::velocity::knot;

use crate::saturating::Saturating;
use crate::scheduling::Trip;
use crate::state::galileo_state::GalileoState;
use crate::state::WaypointClickAction;

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

#[derive(Clone)]
pub struct UiState {
    pub pointer_position: Option<GeoPoint2d>,
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
    pub battery_tide_predictions: DataFrame,
    pub galileo_state: Rc<RwLock<GalileoState>>,
    pub waypoint_mode: Arc<RwLock<WaypointClickAction>>,
    pub sweep_weekdays: Weekdays,
    // TODO: get actual sunrise and sunset
    // TODO: make customizable
    // leave before 8am, arrive before 9pm
    pub daytime: bool,
    trip: Arc<RwLock<Trip>>,
}

impl UiState {
    pub fn new(
        time_idx: Arc<RwLock<Saturating<usize>>>,
        battery_tide_predictions: DataFrame,
        waypoint_mode: Arc<RwLock<WaypointClickAction>>,
        trip: Arc<RwLock<Trip>>,
        galileo_state: Rc<RwLock<GalileoState>>,
    ) -> Self {
        Self {
            pointer_position: None,
            time_idx,
            battery_tide_predictions,
            galileo_state,
            waypoint_mode,
            sweep_weekdays: Weekdays::default(),
            daytime: true,
            trip,
        }
    }
}

fn degree_to_cardinal_direction(heading: f64) -> String {
    let rem = heading % 45.0;
    let floor = heading - rem;
    let mut rounded = if rem > 45.0 / 2.0 {
        floor + 45.0
    } else {
        floor
    };
    rounded %= 360.0;
    let rounded = OrderedFloat(rounded);
    let mapping = HashMap::from([
        (&OrderedFloat(0.0), "➡"),
        (&OrderedFloat(45.0), "↗"),
        (&OrderedFloat(90.0), "⬆"),
        (&OrderedFloat(135.0), "↖"),
        (&OrderedFloat(180.0), "⬅"),
        (&OrderedFloat(225.0), "↙"),
        (&OrderedFloat(270.0), "⬇"),
        (&OrderedFloat(315.0), "↘"),
    ]);
    mapping[&rounded].to_string()
}

pub fn run_ui(state: &mut UiState, ui: &Context) {
    // TODO: is this too long?
    let time_vec = state.battery_tide_predictions["time"]
        .datetime()
        .unwrap()
        .to_vec_null_aware()
        .unwrap_left();

    Window::new("Controls")
        .anchor(Align2::RIGHT_TOP, [0.0, 0.0])
        .default_width(240.0)
        .show(ui, |ui| {
            ui.spacing_mut().button_padding = (30.0, 10.00).into();
            ui.label("Time");
            ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                if ui.button("⬅").clicked() && state.time_idx.write().unwrap().dec() {
                    state.galileo_state.read().unwrap().redraw_map();
                }
                if ui.button("➡").clicked() && state.time_idx.write().unwrap().inc() {
                    state.galileo_state.read().unwrap().redraw_map();
                }
            });

            ui.separator();

            ui.label("Waypoint mode for touch events (not yet implemented) or single mouse button operation.");
            ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                let mut waypoint_mode = state.waypoint_mode.write().unwrap();
                ui.selectable_value(
                    &mut *waypoint_mode,
                    WaypointClickAction::Move,
                    "Move"
                );
                ui.selectable_value(
                    &mut *waypoint_mode,
                    WaypointClickAction::Pause,
                    "Pause"
                );
                ui.selectable_value(
                    &mut *waypoint_mode,
                    WaypointClickAction::Remove,
                    "Remove"
                );
            });

            ui.separator();

            if ui.button("Clear Waypoints").clicked() {
                state.trip.write().unwrap().clear_waypoints();
            }
        });

    Window::new("About")
        .anchor(Align2::RIGHT_BOTTOM, [0.0, 0.0])
        .default_width(240.0)
        .show(ui, |ui| {
            // TODO: add MPL section to licenses, https://www.mozilla.org/en-US/MPL/2.0/FAQ/ Q8.
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
        });

    SidePanel::left("KayakNav")
        .default_width(380.0)
        .show(ui, |ui| {
            ScrollArea::vertical().show(ui, |ui| {
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

                ui.with_layout(Layout::left_to_right(Align::Min), |ui| {
                    ui.spacing_mut().slider_width = 280.0;

                    // avoid deadlock with the write reference
                    let time_range = 0..=state.time_idx.read().unwrap().upper_bound();
                    let slider = state.time_idx.write().unwrap().with_val_mut(
                        |val| ui.add(
                            Slider::new(
                                val,
                                time_range,
                            )
                                .clamp_to_range(true)
                                .show_value(false),
                        ));
                    if slider.dragged() {
                        state.galileo_state.read().unwrap().redraw_map();
                    }

                    if ui.button("Now").clicked() {
                        let now = Local::now().naive_local();
                        let current_time_idx = time_vec
                            .iter()
                            .enumerate()
                            .find(|(_, dt)| {
                                DateTime::from_timestamp_millis(**dt)
                                    .unwrap()
                                    .naive_utc() >= now
                            })
                            .unwrap()
                            .0;
                        if state.time_idx.write().unwrap().set(current_time_idx) {
                            state.galileo_state.read().unwrap().redraw_map();
                        }
                    }
                });

                let time_str: &str =
                    &DateTime::from_timestamp_millis(time_vec[state.time_idx.read().unwrap().val()])
                    .unwrap()
                    .naive_utc()
                    .format("%a %Y-%m-%d %H:%M:%S")
                    .to_string();

                let high_low: &str = state.battery_tide_predictions["high_low"]
                    .str()
                    .unwrap()
                    .get(state.time_idx.read().unwrap().val())
                    .unwrap();

                let mut time_high_low: &str = &format!("{}  {}", time_str, high_low);
                let _ = ui.add(egui::TextEdit::singleline(&mut time_high_low));

                ui.separator();

                let mut trip = state.trip.write().unwrap();

                TableBuilder::new(ui)
                    .max_scroll_height(400.0)
                    .column(Column::exact(256.0))
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
                        let num_rows = trip.current_predictions_30m.len();
                        body.rows(row_height, num_rows, |mut row| {
                            let row_index = row.index();
                            let station = &trip.stations[row_index];
                            let pred = &trip.current_predictions_30m[station];

                            let heading = pred.df["direction"]
                                .f64()
                                .unwrap()
                                .get(state.time_idx.read().unwrap().val())
                                .unwrap();

                            let speed = pred.df["speed"]
                                .f64()
                                .unwrap()
                                .get(state.time_idx.read().unwrap().val())
                                .unwrap();

                            row.col(|ui| {
                                ui.label(pred.station.name.clone());
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

                let internal_time_step = Time::new::<minute>(5.0);
                let time_ratio: Ratio = Time::new::<minute>(30.0) / internal_time_step;
                let waypoint_time_idx =
                    time_ratio.value as usize * state.time_idx.read().unwrap().val();

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

                ui.separator();


                let (sweep_idx_vec, sweep_duration_vec) = if trip.waypoints.len() > 1 {
                    let sweep_df = trip.sweep();

                    (sweep_df["idx"]
                     .u64()
                     .unwrap()
                     .to_vec_null_aware()
                     .unwrap_left(),

                     sweep_df["duration"]
                     .f64()
                     .unwrap()
                     .to_vec_null_aware()
                     .unwrap_left())
                } else {
                    (vec![], vec![])
                };

                ui.push_id(1, |ui| {
                    TableBuilder::new(ui)
                        .max_scroll_height(400.0)
                        .column(Column::exact(184.0))
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

                                let time_str: &str =
                                    &DateTime::from_timestamp_millis(time_vec[idx as usize])
                                        .unwrap()
                                        .naive_utc()
                                        .format("%a %Y-%m-%d %H:%M:%S")
                                        .to_string();

                                row.col(|ui| {
                                    ui.label(time_str);
                                });

                                row.col(|ui| {
                                    ui.label(format!("{:.1}h", duration / 3600.0));
                                });
                            });
                        });
                });

            });
        });
}
