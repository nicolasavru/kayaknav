use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDateTime;
use chrono::TimeDelta;
use chrono::Timelike;
use galileo::layer::feature_layer::FeatureId;
use galileo::layer::feature_layer::FeatureLayer;
use galileo_types::cartesian::Point2;
use galileo_types::geo::Crs;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::NewGeoPoint;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geometry_type::CartesianSpace2d;
use itertools::Itertools;
use jord::Angle;
use jord::LatLong;
use jord::LocalFrame;
use jord::LocalPositionVector;
use jord::ellipsoidal::Ellipsoid;
use lru::LruCache;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use polars::prelude::*;
use rstar::RTree;
use uom::si::f64::Length;
use uom::si::f64::Ratio;
use uom::si::f64::Time;
use uom::si::f64::Velocity;
use uom::si::length::meter;
use uom::si::time::hour;
use uom::si::time::minute;
use uom::si::time::second;
use uom::si::velocity::knot;

use crate::features;
use crate::features::TripPath;
use crate::features::TripPathSymbol;
use crate::features::Waypoint;
use crate::features::WaypointSymbol;
use crate::features::WaypointType;
use crate::noaa::CurrentPrediction;
use crate::noaa::Station;
use crate::noaa::geo_pos;
use crate::prelude::*;
use crate::run_ui::WeekdayFlags;

/// Schema version for `WaypointsFile`. Bump when breaking the on-disk
/// format so imports of newer files can reject cleanly instead of
/// mis-parsing.
const WAYPOINTS_FILE_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum WaypointTypeDto {
    Move,
    Pause,
}

impl From<WaypointType> for WaypointTypeDto {
    fn from(t: WaypointType) -> Self {
        match t {
            WaypointType::Move => Self::Move,
            WaypointType::Pause => Self::Pause,
        }
    }
}

impl From<WaypointTypeDto> for WaypointType {
    fn from(t: WaypointTypeDto) -> Self {
        match t {
            WaypointTypeDto::Move => Self::Move,
            WaypointTypeDto::Pause => Self::Pause,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WaypointDto {
    lat: f64,
    lon: f64,
    #[serde(rename = "type")]
    type_: WaypointTypeDto,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WaypointsFile {
    version: u32,
    waypoints: Vec<WaypointDto>,
}

/// Schema version for the best-departures export produced by
/// `Trip::export_best_departures_json`. Bump whenever the shape changes
/// so downstream consumers can reject old/new files cleanly.
const TRIP_EXPORT_VERSION: u32 = 2;

/// ISO-8601 timestamp format used in exports. Chosen deliberately
/// instead of enabling chrono's `serde` feature — the cost of that
/// feature ripples into every target (including wasm) for a payload
/// that's one-way output-only.
const TRIP_EXPORT_TS_FMT: &str = "%Y-%m-%dT%H:%M:%S";

fn format_ts(dt: NaiveDateTime) -> String {
    dt.format(TRIP_EXPORT_TS_FMT).to_string()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SegmentExport {
    /// Great-circle distance traveled on this segment, in meters. Zero
    /// for a Pause segment (the "end" waypoint is a 30-minute hold).
    distance_m: f64,
    /// Elapsed time on this segment, in seconds.
    duration_s: f64,
    /// Net over-ground speed for this segment, in knots. Equals
    /// `distance_m / duration_s` converted; zero on a Pause.
    speed_kt: f64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DepartureExport {
    start_time: String,
    end_time: String,
    duration_s: f64,
    /// Sunset at the starting waypoint's coordinate on the local
    /// (UTC) calendar date of `start_time`, in the same
    /// `TRIP_EXPORT_TS_FMT`. `None` when there is no start waypoint
    /// or when `sunrise` can't compute sunset for the latitude (polar
    /// day/night). Emitted on a per-departure basis because the
    /// sunset time shifts by ~1 minute/day — a schedule spanning
    /// weeks needs per-departure values, not a single "sunset today."
    sunset_time: Option<String>,
    /// Cumulative arrival timestamps — one per waypoint, first equals
    /// `start_time`. Length matches the imported waypoint list.
    waypoint_arrivals: Vec<String>,
    /// One entry per leg. Length equals `waypoints.len() - 1`.
    segments: Vec<SegmentExport>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TripExport {
    version: u32,
    /// Base over-ground speed (kayaker's speed in still water), in knots.
    speed_kt: f64,
    /// Best departures surviving the 20th-percentile duration filter,
    /// plus any active weekday/daytime filters on the Trip. Sorted
    /// ascending by start_time for stable downstream consumption.
    departures: Vec<DepartureExport>,
}

#[derive(Copy, Clone, Default, Debug)]
pub struct StepResult {
    pub distance: Length,
    pub time: Time,
    pub time_steps: usize,
}

impl StepResult {
    pub fn speed(&self) -> Velocity {
        self.distance / self.time
    }
}

pub fn calculate_step(
    start: &Waypoint,
    end: &Waypoint,
    base_speed: Velocity,
    predictions_30m: &HashMap<Station, CurrentPrediction<30>>,
    predictions_5m: &mut HashMap<Station, CurrentPrediction<5>>,
    start_time_idx: usize,
    nn_calc: &mut NearestNeighborCalculator,
) -> Option<StepResult> {
    // TODO: derive from argument
    let internal_time_step =
        Time::new::<minute>(CurrentPrediction::<5>::resolution_minutes() as f64);

    if matches!(end.type_, WaypointType::Pause) {
        return Some(StepResult {
            distance: Length::new::<meter>(0.0),
            time: Time::new::<hour>(0.5),
            time_steps: (Time::new::<hour>(0.5) / internal_time_step).value as usize,
        });
    }

    let start = geo_pos(start.lat(), start.lon());
    let end = geo_pos(end.lat(), end.lon());

    let ned = LocalFrame::ned(start, Ellipsoid::WGS84);
    let delta = ned.geodetic_to_local_pos(end);

    let mut time_idx = start_time_idx;

    let mut step_start = start;
    let mut distance_remaining = Length::new::<meter>(delta.slant_range().as_metres());
    let mut total_time = Time::new::<hour>(0.0);
    let mut total_distance = Length::new::<meter>(0.0);

    while distance_remaining > Length::new::<meter>(0.0) {
        let l_frame = LocalFrame::local_level(delta.azimuth(), step_start, Ellipsoid::WGS84);

        let ll_step_start = LatLong::from_nvector(step_start.horizontal_position());
        // `?` handles the empty-store case: before any station
        // predictions have streamed in, there's no nearest station, so
        // the step — and the whole trip — resolves to None, and the UI
        // shows "Exceeded fetched data." until background load delivers
        // the first batch.
        let station = nn_calc.nearest_neighbor(ll_step_start)?;
        // Lazily resample 30m → 5m on first visit per station. Cheap
        // (polars upsample + linear interp over ~3k rows) and bounded by
        // the stations a trip actually touches, so the 5m map stays tiny
        // compared to the full 4k-station store.
        if !predictions_5m.contains_key(&station) {
            let pred_30m = predictions_30m.get(&station)?;
            let pred_5m = pred_30m.resampled::<5>().ok()?;
            predictions_5m.insert(station.clone(), pred_5m);
        }
        let prediction = &predictions_5m[&station];

        if time_idx >= prediction.speed.len() {
            return None;
        }

        // Fast path: read the pre-materialized Vec<f64> extracts instead of
        // doing `df[col].f64().unwrap().get(i)` every step. Polars returns
        // O(1) values but the per-call downcast overhead dominated this
        // loop when the sweep runs thousands of starts × dozens of steps.
        let current_speed = prediction.speed[time_idx];
        let current_direction = Angle::from_degrees(prediction.direction[time_idx]);

        let angle_delta = delta.azimuth() - current_direction;
        let angle_delta_cos = angle_delta.as_radians().cos();
        let net_speed = base_speed + angle_delta_cos * Velocity::new::<knot>(current_speed);

        let step_distance = internal_time_step * net_speed;
        distance_remaining -= step_distance;

        let step_delta = LocalPositionVector::from_metres(step_distance.get::<meter>(), 0.0, 0.0);

        let step_end = l_frame.local_to_geodetic_pos(step_delta);

        step_start = step_end;
        time_idx += 1;
        total_time += internal_time_step;
        total_distance += step_distance;
    }

    Some(StepResult {
        distance: total_distance,
        time: total_time,
        time_steps: time_idx - start_time_idx,
    })
}

#[derive(Clone, Default, Debug)]
pub struct TripResult {
    pub steps: Vec<StepResult>,
}

impl TripResult {
    pub fn distance(&self) -> Length {
        self.steps.iter().map(|s| s.distance).sum()
    }

    pub fn time(&self) -> Time {
        self.steps.iter().map(|s| s.time).sum()
    }
}

#[derive(Debug, Clone)]
pub struct NearestNeighborCalculator {
    cache: LruCache<(OrderedFloat<f64>, OrderedFloat<f64>), Station>,
    tree: RTree<Station>,
}

impl NearestNeighborCalculator {
    pub fn new(stations: &[Station]) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(1024 * 1024).unwrap()),
            tree: RTree::bulk_load(stations.to_vec()),
        }
    }

    /// Insert new stations into the RTree and invalidate the lookup cache —
    /// a station added later may be closer than any previously-cached answer.
    pub fn extend(&mut self, stations: impl IntoIterator<Item = Station>) {
        for s in stations {
            self.tree.insert(s);
        }
        self.cache.clear();
    }

    /// Returns `None` if the RTree is empty — e.g. before any current
    /// predictions have streamed in from the background loader. Callers
    /// (`calculate_step`) propagate the `None` up so trip/sweep simply
    /// bail until stations arrive.
    pub fn nearest_neighbor(&mut self, point: LatLong) -> Option<Station> {
        let lat = point.latitude().as_degrees();
        let lon = point.longitude().as_degrees();
        let key = (OrderedFloat(lat), OrderedFloat(lon));

        if let Some(p) = self.cache.get(&key) {
            return Some(p.clone());
        }

        let station = self.tree.nearest_neighbor(&[lat, lon])?.clone();
        self.cache.put(key, station.clone());
        Some(station)
    }
}

/// Progress counters for an in-flight sweep. Shared between the
/// sweep worker (writer) and the UI (reader) via `Arc`. A fresh
/// instance is created on every `start_sweep`, so a stale worker
/// from a prior generation writes to its own orphaned `Arc` and
/// cannot contaminate the progress the UI currently displays.
pub struct SweepProgress {
    /// Trip-steps computed so far, out of `total`. Units are
    /// `starts × legs`: a fresh sweep starts at 0 and ends at `N × L`;
    /// an append-driven extend starts at `N × L_prior` and ends at
    /// `N × L_new`, so the bar visually picks up where the prior sweep
    /// left off rather than resetting to zero. Reflects the honest
    /// fraction of the *trip* that's computed; step_cache hits for
    /// already-covered legs contribute zero CPU and zero counter
    /// movement.
    pub completed: AtomicUsize,
    pub total: AtomicUsize,
    /// Flipped by `clear_cache` when inputs change under an in-flight
    /// sweep. The worker polls this between starts and bails early; its
    /// partial result is discarded by the generation check in `sweep()`
    /// anyway, but exiting early spares wasted CPU.
    pub cancelled: AtomicBool,
}

impl SweepProgress {
    fn new() -> Self {
        Self {
            completed: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
        }
    }
}

/// Hot scratch caches handed to the sweep worker at start and returned
/// on completion. Shipping them back lets the next sweep start warm —
/// the key payoff is `step_cache`, whose per-leg entries are reusable
/// across waypoint edits that only change the *tail* of the trip
/// (append/remove-last) or the legs after a middle edit. Without this
/// round-trip the worker would throw away its computation every time.
struct SweepCaches {
    /// Number of legs the shipped `results` entries cover. The main
    /// thread adopts this into `Trip::results_leg_count` so `sweep()`
    /// can tell the difference between "materialize the DataFrame" and
    /// "an append happened during the sweep — dispatch an extend to
    /// fill the missing tail legs".
    target_leg_count: usize,
    predictions_5m: HashMap<Station, CurrentPrediction<5>>,
    results: HashMap<usize, Option<TripResult>>,
    step_cache: HashMap<(usize, usize), Option<StepResult>>,
}

/// Inputs/state for the sweep-worker path. Behind an `Arc` for the 30m map
/// so dispatching a new worker is effectively a refcount bump rather than a
/// full 4k-entry clone. Shared for both native (background thread) and wasm
/// (chunked `spawn_local` task) — both report the finished DataFrame back
/// via `rx` so the `sweep()` polling path stays identical.
struct SweepJob {
    // `mpsc::Receiver` is `Send` but not `Sync`, and `Trip` is held inside
    // `Arc<RwLock<Trip>>` — so wrap to satisfy `Sync`. Only the UI thread
    // ever polls this channel (it holds the write lock), so the Mutex
    // contention is zero in practice. On wasm there's only one thread
    // anyway; the channel is a completion signal, not cross-thread sync.
    rx: parking_lot::Mutex<std::sync::mpsc::Receiver<(u64, SweepCaches)>>,
}

pub struct Trip {
    pub waypoints: Vec<Waypoint>,
    pub waypoint_ids: Vec<FeatureId>,
    pub speed: Velocity,
    pub waypoint_layer:
        Arc<RwLock<FeatureLayer<Point2, Waypoint, WaypointSymbol, CartesianSpace2d>>>,
    pub path_layer: Arc<RwLock<FeatureLayer<Point2, TripPath, TripPathSymbol, CartesianSpace2d>>>,
    pub stations: Vec<Station>,
    pub current_predictions_30m: Arc<HashMap<Station, CurrentPrediction<30>>>,
    pub current_predictions_5m: HashMap<Station, CurrentPrediction<5>>,
    pub weekdays: WeekdayFlags,
    pub daytime: bool,
    /// When true, drop every departure whose end time (= start +
    /// duration) lands after sunset at the starting waypoint's
    /// coordinate on that departure's local date. Sunset is computed
    /// via the `sunrise` crate per calendar date; a trip that
    /// straddles midnight UTC uses the sunset for the *start* date.
    /// Filter runs in `materialize_sweep_df` alongside weekday/daytime
    /// so a toggle doesn't invalidate the background sweep.
    pub arrive_before_sunset: bool,
    results: HashMap<usize, Option<TripResult>>,
    // Per-leg memoization across all `calculate_trip` calls against the
    // current (waypoints, speed, predictions_30m) triple. Key is
    // `(leg_idx, entry_time_idx)` — deterministic given those inputs, so
    // collisions across sweep starts or slider moves all reuse.
    step_cache: HashMap<(usize, usize), Option<StepResult>>,
    sweep_result: Option<DataFrame>,
    /// Number of legs covered by every non-None entry in `results`.
    /// `None` means `results` is empty or mixed (e.g. main-thread
    /// slider calls wrote entries at a different leg count). `Some(L)`
    /// means the sweep can be materialized iff `L == waypoints.len() -
    /// 1`; if `L` is smaller, the user appended waypoints after the
    /// sweep finished and an extend-sweep is needed to grow the
    /// tail. Set on worker adopt (= `target_leg_count` of the caches
    /// shipped back); reset to `None` by `clear_cache_from_leg` on
    /// invalidating edits.
    results_leg_count: Option<usize>,
    /// Leg count up to which `step_cache` is populated for every
    /// start index a prior complete sweep visited. Independent of
    /// `results_leg_count` — a prefix-preserving edit (remove at
    /// `idx`, `idx >= 2`) drops `results` but keeps step_cache legs
    /// `0..idx-2`, so the next sweep can reuse them as cache hits
    /// even though no full `TripResult` survives. Drives the
    /// `prior_leg_count` baseline passed to the worker so the
    /// progress bar doesn't pretend the warm prefix needs
    /// recomputing. Bumped to `target_leg_count` on worker adopt,
    /// set to `from_leg` on prefix-preserving invalidation, reset
    /// to 0 on full invalidation.
    prefix_cached_legs: usize,
    nn_calc: NearestNeighborCalculator,
    // Bumped every time inputs invalidate — used to ignore late-arriving
    // worker results from a prior generation.
    sweep_generation: u64,
    sweep_job: Option<SweepJob>,
    /// Counters written by the in-flight sweep worker. Replaced on
    /// every `start_sweep` so the UI reads only the current sweep's
    /// progress.
    pub sweep_progress: Arc<SweepProgress>,
}

/// Canonical station ordering: north-to-south, then west-to-east. Used
/// both at `Trip::new` and after `add_predictions` so the displayed
/// station list stays stable across background merges.
fn station_sort_key(station: &Station) -> (OrderedFloat<f64>, OrderedFloat<f64>) {
    (
        OrderedFloat(-station.loc.lat()),
        OrderedFloat(station.loc.lon()),
    )
}

impl Trip {
    pub fn new(
        speed: Velocity,
        waypoint_layer: Arc<
            RwLock<FeatureLayer<Point2, Waypoint, WaypointSymbol, CartesianSpace2d>>,
        >,
        path_layer: Arc<RwLock<FeatureLayer<Point2, TripPath, TripPathSymbol, CartesianSpace2d>>>,
        current_predictions_30m: Vec<CurrentPrediction<30>>,
    ) -> Result<Self> {
        let mut stations: Vec<Station> = current_predictions_30m
            .iter()
            .map(|p| p.station.clone())
            .collect();

        stations.sort_unstable_by_key(station_sort_key);

        let current_predictions_30m: Arc<HashMap<Station, CurrentPrediction<30>>> =
            Arc::new(HashMap::from_iter(
                current_predictions_30m
                    .into_iter()
                    .map(|p| (p.station.clone(), p)),
            ));

        // 5-minute predictions are derived on-demand by `calculate_step` and
        // cached here. Starting empty avoids materializing a ~6× blowup of
        // the 30m data across ~4k stations (~1.7 GB) that the trip
        // calculator only ever samples a handful of.
        let current_predictions_5m: HashMap<Station, CurrentPrediction<5>> = HashMap::new();

        Ok(Self {
            waypoints: Vec::new(),
            waypoint_ids: Vec::new(),
            speed,
            waypoint_layer,
            path_layer,
            current_predictions_30m,
            current_predictions_5m,
            weekdays: WeekdayFlags::empty(),
            daytime: false,
            arrive_before_sunset: false,
            results: HashMap::new(),
            step_cache: HashMap::new(),
            sweep_result: None,
            results_leg_count: None,
            prefix_cached_legs: 0,
            nn_calc: NearestNeighborCalculator::new(&stations),
            sweep_generation: 0,
            sweep_job: None,
            sweep_progress: Arc::new(SweepProgress::new()),
            // Moved last in source order so the `&stations` borrow above
            // can stand; avoids an otherwise-needed full `stations.clone()`.
            stations,
        })
    }

    /// Invalidate caches after a waypoint edit, keeping the entries that
    /// are still valid. `from_leg = None` nukes everything (speed change,
    /// new predictions, full waypoint clear). `from_leg = Some(k)` keeps
    /// `step_cache` entries for legs `< k` — their endpoints and entry
    /// time indices are unchanged, so the next sweep/trip can reuse them
    /// directly. Results and sweep_result always go, since both are
    /// aggregates over *all* legs.
    fn clear_cache_from_leg(&mut self, from_leg: Option<usize>) {
        match from_leg {
            None => {
                self.step_cache.clear();
                self.prefix_cached_legs = 0;
            },
            Some(from) => {
                self.step_cache.retain(|(leg_idx, _), _| *leg_idx < from);
                // Prefix invariant: we kept legs `< from` for every
                // start the prior sweep visited. Cap against the old
                // floor in case a partial sweep never covered `from`
                // legs in the first place — we can't claim more than
                // was actually populated.
                self.prefix_cached_legs = self.prefix_cached_legs.min(from);
            },
        }
        self.results.clear();
        self.results_leg_count = None;
        self.sweep_result = None;
        // Any in-flight sweep computed against the old inputs — its eventual
        // result won't match the current waypoints. Bump generation so
        // `sweep()` discards it when it arrives.
        self.sweep_generation = self.sweep_generation.wrapping_add(1);
        // Signal the stale worker (if any) to stop early. It holds its
        // own Arc to the *old* SweepProgress, so this flag reaches it
        // even after we replace `self.sweep_progress` on the next
        // `start_sweep`.
        self.sweep_progress.cancelled.store(true, Ordering::Relaxed);
        self.sweep_job = None;
    }

    fn clear_cache(&mut self) {
        self.clear_cache_from_leg(None);
    }

    /// Filter-only invalidation: weekdays/daytime change which starts
    /// are surfaced in the displayed DataFrame, and which finished
    /// trips are retained. They do NOT change any per-start
    /// `TripResult` or per-leg `StepResult`, so `results`/`step_cache`
    /// stay valid. The in-flight worker (if any) computes over every
    /// start regardless of filter, so its eventual output is also
    /// still valid — don't cancel it. Just drop the cached DataFrame
    /// so the next `sweep()` rebuilds it with the fresh filter.
    fn invalidate_sweep_only(&mut self) {
        self.sweep_result = None;
    }

    /// Merge in newly-computed predictions. Used by progressive station
    /// loading in `setup::build` — we start with a near subset and fold in
    /// the rest as they're computed in the background.
    pub fn add_predictions(
        &mut self,
        new_predictions_30m: Vec<CurrentPrediction<30>>,
    ) -> Result<()> {
        if new_predictions_30m.is_empty() {
            return Ok(());
        }

        self.stations
            .extend(new_predictions_30m.iter().map(|p| p.station.clone()));
        self.stations.sort_unstable_by_key(station_sort_key);
        self.nn_calc
            .extend(new_predictions_30m.iter().map(|p| p.station.clone()));

        // `Arc::make_mut` clones only if a sweep worker still holds a ref;
        // otherwise we mutate in place.
        let preds_mut = Arc::make_mut(&mut self.current_predictions_30m);
        for pred in new_predictions_30m {
            preds_mut.insert(pred.station.clone(), pred);
        }
        // No 5m resample here — `calculate_step` populates the 5m cache
        // lazily when a station is actually visited.

        self.clear_cache();
        Ok(())
    }

    /// Rebuild the single path feature from the current waypoint list.
    /// Called from every mutation method so the line+arrows always
    /// reflect `self.waypoints` in order.
    fn rebuild_path(&self) {
        features::set_trip_path_from_waypoints(self.path_layer.clone(), &self.waypoints);
    }

    pub fn add_waypoint(&mut self, waypoint: Waypoint) {
        let id = {
            let mut layer = self.waypoint_layer.write();
            let id = layer.features_mut().add(waypoint);
            layer.update_all_features();
            id
        };
        self.waypoints.push(waypoint);
        self.waypoint_ids.push(id);
        self.rebuild_path();
        // Append-only edit: every prior leg's step_cache entry and
        // every prior `TripResult` in `results` is a valid prefix of
        // the extended trip. We deliberately do NOT:
        //   - clear step_cache or results (the prefix is gold),
        //   - cancel the in-flight worker (its partial output is still
        //     a valid prefix — `sweep()` will see `results_leg_count <
        //     current_legs` when it lands and dispatch an extend),
        //   - bump `sweep_generation` (the adopted caches must not be
        //     discarded as stale).
        // Dropping `sweep_result` forces `sweep()` to either
        // materialize (if already complete for the new leg count) or
        // dispatch the extend.
        self.sweep_result = None;
    }

    pub fn remove_waypoint(&mut self, idx: usize) {
        let id = self.waypoint_ids.remove(idx);
        self.waypoints.remove(idx);
        {
            let mut layer = self.waypoint_layer.write();
            layer.features_mut().remove(id);
            layer.update_all_features();
        }
        self.rebuild_path();
        // Leg `idx - 1` ended at the removed waypoint and now ends at
        // `waypoints[idx]` instead — a different leg. Everything at or
        // after leg `idx - 1` is shifted or re-endpointed; everything
        // before is identical.
        self.clear_cache_from_leg(Some(idx.saturating_sub(1)));
    }

    pub fn remove_waypoint_by_id(&mut self, id: FeatureId) {
        if let Some(idx) = self.waypoint_ids.iter().position(|wid| *wid == id) {
            self.remove_waypoint(idx);
        }
    }

    pub fn clear_waypoints(&mut self) {
        self.waypoints.clear();
        self.waypoint_ids.clear();
        features::clear_waypoint_features(self.waypoint_layer.clone());
        self.rebuild_path();
        self.clear_cache();
    }

    /// Serialize the current waypoint list as JSON. Coordinates are stored
    /// as lat/lon (EPSG:4326) — the internal `Point2` is EPSG:3857 meters,
    /// which would be unreadable and projection-specific in a file.
    pub fn export_waypoints_json(&self) -> Result<String> {
        let file = WaypointsFile {
            version: WAYPOINTS_FILE_VERSION,
            waypoints: self
                .waypoints
                .iter()
                .map(|w| WaypointDto {
                    lat: w.lat(),
                    lon: w.lon(),
                    type_: WaypointTypeDto::from(w.type_),
                })
                .collect(),
        };
        Ok(serde_json::to_string_pretty(&file)?)
    }

    /// Headless constructor for non-UI consumers (CLI tools, tests).
    /// Builds the empty galileo feature layers internally so callers
    /// don't need to depend on the `features` module, and seeds
    /// `weekdays = all` so the CLI-style "compute every day" default
    /// doesn't hide behind the empty bitflags initializer that makes
    /// sense for the UI's reactive-filter model.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new_headless(
        speed: Velocity,
        current_predictions_30m: Vec<CurrentPrediction<30>>,
    ) -> Result<Self> {
        let waypoint_layer = Arc::new(RwLock::new(FeatureLayer::new(
            Vec::<Waypoint>::new(),
            WaypointSymbol {},
            Crs::EPSG3857,
        )));
        let path_layer = Arc::new(RwLock::new(FeatureLayer::new(
            Vec::<TripPath>::new(),
            TripPathSymbol {},
            Crs::EPSG3857,
        )));
        let mut trip = Self::new(speed, waypoint_layer, path_layer, current_predictions_30m)?;
        trip.weekdays = crate::run_ui::WeekdayFlags::all();
        Ok(trip)
    }

    /// Replace the current waypoint list with the contents of `json`.
    /// Returns the number of waypoints imported on success. If parsing or
    /// projection fails, existing waypoints are left untouched.
    pub fn import_waypoints_json(&mut self, json: &str) -> Result<usize> {
        let file: WaypointsFile =
            serde_json::from_str(json).context("failed to parse waypoints JSON")?;
        if file.version != WAYPOINTS_FILE_VERSION {
            bail!(
                "unsupported waypoints file version: {} (expected {})",
                file.version,
                WAYPOINTS_FILE_VERSION
            );
        }
        // Project all points up front so a bad coordinate aborts the import
        // before we mutate state.
        let proj = Crs::EPSG3857
            .get_projection::<GeoPoint2d, Point2>()
            .ok_or_else(|| anyhow!("EPSG:3857 projection unavailable"))?;
        let projected: Vec<Waypoint> = file
            .waypoints
            .into_iter()
            .map(|dto| {
                proj.project(&GeoPoint2d::latlon(dto.lat, dto.lon))
                    .map(|point| Waypoint {
                        point,
                        type_: dto.type_.into(),
                    })
                    .ok_or_else(|| {
                        anyhow!("could not project waypoint lat={} lon={}", dto.lat, dto.lon)
                    })
            })
            .collect::<Result<_>>()?;

        self.clear_waypoints();
        let count = projected.len();
        for w in projected {
            self.add_waypoint(w);
        }
        Ok(count)
    }

    pub fn set_speed(&mut self, speed: Velocity) {
        self.speed = speed;
        self.clear_cache()
    }

    pub fn set_weekdays(&mut self, weekdays: WeekdayFlags) {
        if self.weekdays != weekdays {
            self.weekdays = weekdays;
            self.invalidate_sweep_only();
        }
    }

    pub fn set_daytime(&mut self, daytime: bool) {
        if self.daytime != daytime {
            self.daytime = daytime;
            self.invalidate_sweep_only();
        }
    }

    pub fn set_arrive_before_sunset(&mut self, arrive_before_sunset: bool) {
        if self.arrive_before_sunset != arrive_before_sunset {
            self.arrive_before_sunset = arrive_before_sunset;
            self.invalidate_sweep_only();
        }
    }

    pub fn calculate(&mut self, start_time_idx: usize) -> Option<TripResult> {
        calculate_trip(
            &self.waypoints,
            self.speed,
            &self.current_predictions_30m,
            &mut self.current_predictions_5m,
            &mut self.nn_calc,
            &mut self.results,
            &mut self.step_cache,
            start_time_idx,
        )
    }

    /// Returns the filtered Departure-Time/Duration DataFrame if one
    /// is available. The worker computes per-start trip results over
    /// *every* start index (ignoring weekdays/daytime); filters are
    /// applied here when materializing the DataFrame. Consequences:
    /// - A weekday/daytime toggle costs a materialize (tens of ms),
    ///   not a re-sweep.
    /// - An in-flight worker isn't cancelled when filters change; its
    ///   output is still valid.
    /// - `results.is_empty()` isn't a reliable "complete" check
    ///   because `calculate()` can populate a single entry from the
    ///   slider while the worker runs. Instead, a dedicated
    ///   `sweep_results_complete` flag is flipped when the worker
    ///   delivers with a matching generation.
    pub fn sweep(&mut self) -> Option<DataFrame> {
        if self.waypoints.len() < 2 {
            return None;
        }
        // Background loader hasn't delivered any predictions yet.
        // `all_sweep_start_indices` reads an arbitrary prediction's
        // time axis and would panic on an empty map.
        if self.current_predictions_30m.is_empty() {
            return None;
        }
        let current_legs = self.waypoints.len() - 1;

        // Drain any completed worker result; adopt caches only if
        // generation matches (otherwise waypoints/speed changed while
        // it ran and the partial output is stale).
        let received = self
            .sweep_job
            .as_ref()
            .and_then(|job| job.rx.lock().try_recv().ok());
        if let Some((result_gen, caches)) = received {
            if result_gen == self.sweep_generation {
                // Worker ran to completion for `target_leg_count`.
                // That may be less than the CURRENT leg count if
                // the user appended waypoints during the sweep —
                // in that case we adopt the caches (still a valid
                // prefix) and let the dispatch below kick off an
                // extend that grows the tail with warm caches.
                self.step_cache.extend(caches.step_cache);
                self.results.extend(caches.results);
                self.current_predictions_5m.extend(caches.predictions_5m);
                self.results_leg_count = Some(caches.target_leg_count);
                self.prefix_cached_legs = caches.target_leg_count;
                self.sweep_result = None; // force (re)materialize below
            }
            self.sweep_job = None;
        }

        // Materialize the displayed DataFrame from per-start results.
        // Requires `results` to cover exactly the current leg count;
        // otherwise the TripResults in there describe a shorter or
        // longer trip and durations would be wrong.
        if self.sweep_result.is_none() && self.results_leg_count == Some(current_legs) {
            self.rematerialize_sweep_result();
        }

        // Need more legs (fresh trip, edit, or mid-sweep append) and no
        // worker is in flight — dispatch a sweep targeting current legs.
        // If `results` already has shorter-leg entries, the worker's
        // `calculate_trip` calls see a leg-count mismatch on cache hits
        // and fall through, computing only the NEW tail legs; prior legs
        // come straight from `step_cache`.
        //
        // `prior_leg_count` tells the worker how many legs are already
        // covered so `SweepProgress.completed` starts at `N × prior`
        // rather than 0. The progress bar then represents the fraction
        // of the whole trip computed; on append it resumes near the
        // prior high-water mark instead of snapping back to zero.
        // Sourced from `prefix_cached_legs` (not `results_leg_count`)
        // because prefix-preserving edits — remove at an intermediate
        // index — drop `results` entirely but keep the `step_cache`
        // prefix. Using `results_leg_count` here would mis-report
        // remove as a full recomputation.
        if self.results_leg_count != Some(current_legs) && self.sweep_job.is_none() {
            let prior_leg_count = self.prefix_cached_legs.min(current_legs);
            self.start_sweep(current_legs, prior_leg_count);
        }

        self.sweep_result.clone()
    }

    /// Run the sweep synchronously on the calling thread, bypassing the
    /// background worker. Intended for headless paths (CLI, integration
    /// tests) that want a simple blocking call instead of polling
    /// `sweep()` across frames. On native, reuses the same async
    /// `compute_sweep` body — its yields compile to no-op `Ready`
    /// points, so this has zero scheduling overhead vs a hand-written
    /// sync loop.
    pub fn sweep_blocking(&mut self) -> Option<DataFrame> {
        if self.waypoints.len() < 2 {
            return None;
        }
        if self.current_predictions_30m.is_empty() {
            return None;
        }
        let current_legs = self.waypoints.len() - 1;

        // Cancel any in-flight worker and discard its partial caches —
        // we're running to completion inline anyway, and mixing
        // worker-shipped results with inline results risks duplicate
        // work and stale generation checks.
        self.sweep_progress.cancelled.store(true, Ordering::Relaxed);
        self.sweep_job = None;

        if self.results_leg_count != Some(current_legs) {
            let prior_leg_count = self.prefix_cached_legs.min(current_legs);
            let progress = Arc::new(SweepProgress::new());
            futures::executor::block_on(compute_sweep(
                &self.waypoints,
                self.speed,
                &self.current_predictions_30m,
                &mut self.current_predictions_5m,
                &mut self.nn_calc,
                &mut self.results,
                &mut self.step_cache,
                current_legs,
                prior_leg_count,
                &progress,
            ));
            self.results_leg_count = Some(current_legs);
            self.prefix_cached_legs = current_legs;
            self.sweep_result = None;
        }

        if self.sweep_result.is_none() {
            self.rematerialize_sweep_result();
        }
        self.sweep_result.clone()
    }

    /// Lat/lon at the first waypoint, used for the arrive-before-sunset
    /// filter. Returns `None` when there are no waypoints — in which
    /// case no sunset computation is possible and the filter
    /// degenerates to a no-op.
    fn sunset_filter_location(&self) -> Option<(f64, f64)> {
        self.waypoints.first().map(|w| (w.lat(), w.lon()))
    }

    /// Rebuild `sweep_result` from the current per-start `results`
    /// map by applying the active filters (weekdays, daytime,
    /// arrive-before-sunset). Called from both the async `sweep()`
    /// path and the blocking `sweep_blocking()` path, which is why
    /// it's a method rather than inline in either.
    fn rematerialize_sweep_result(&mut self) {
        let indices = all_sweep_start_indices(&self.current_predictions_30m);
        let sunset_loc = self.sunset_filter_location();
        self.sweep_result = materialize_sweep_df(
            &self.results,
            &indices,
            self.weekdays,
            self.daytime,
            self.arrive_before_sunset,
            sunset_loc,
        );
    }

    /// Drive the sweep to completion and serialize every surviving
    /// "best" departure as JSON. Each entry carries the start/end
    /// timestamps, cumulative per-waypoint arrivals, and per-segment
    /// distance/duration/speed. Output is sorted by start_time so
    /// downstream tools get stable ordering across runs.
    pub fn export_best_departures_json(&mut self) -> Result<String> {
        // Snapshot start-waypoint coords before the `&mut self` chain
        // below — the export body can't re-borrow `self` mid-loop to
        // compute sunset. Empty-waypoint trips return None here; we
        // never reach the loop anyway because `sweep_blocking` would
        // have produced an error first.
        let sunset_loc = self.sunset_filter_location();
        let df = self
            .sweep_blocking()
            .ok_or_else(|| anyhow!("sweep produced no results"))?;

        let time_ratio = sweep_time_ratio();
        // Map 30m idx → NaiveDateTime. Built once per export; the trip
        // uses this for start/arrival timestamp resolution without
        // re-walking the time column for each row.
        let idx_to_time: HashMap<usize, NaiveDateTime> =
            all_sweep_start_indices(&self.current_predictions_30m)
                .into_iter()
                .collect();

        let idx_col = df["idx"].u64().log()?;
        let duration_col = df["duration"].f64().log()?;

        let mut departures: Vec<DepartureExport> = Vec::with_capacity(df.height());
        for row in 0..df.height() {
            let idx_30m = idx_col
                .get(row)
                .ok_or_else(|| anyhow!("missing idx at row {row}"))?
                as usize;
            let duration_s = duration_col
                .get(row)
                .ok_or_else(|| anyhow!("missing duration at row {row}"))?;
            let start_dt = *idx_to_time
                .get(&idx_30m)
                .ok_or_else(|| anyhow!("no timestamp for idx {idx_30m}"))?;
            let tr = self
                .results
                .get(&(time_ratio * idx_30m))
                .and_then(|o| o.as_ref())
                .ok_or_else(|| anyhow!("no trip result for 30m idx {idx_30m}"))?;

            let end_dt = start_dt + TimeDelta::seconds(duration_s as i64);

            // Cumulative arrival at each waypoint. steps[0] is the zero
            // placeholder (waypoints[0] is the start — arrive at t=0),
            // steps[i>=1] is the leg from waypoints[i-1] to
            // waypoints[i], so the cumulative sum through i gives the
            // arrival time at waypoints[i].
            let mut arrivals: Vec<String> = Vec::with_capacity(tr.steps.len());
            let mut cum_s: f64 = 0.0;
            for step in &tr.steps {
                cum_s += step.time.get::<second>();
                arrivals.push(format_ts(start_dt + TimeDelta::seconds(cum_s as i64)));
            }

            // One segment per real leg. steps[0] is the placeholder —
            // skip it; the remaining N-1 entries correspond 1:1 to the
            // waypoint pairs.
            let segments: Vec<SegmentExport> = tr
                .steps
                .iter()
                .skip(1)
                .map(|s| {
                    let duration_s = s.time.get::<second>();
                    SegmentExport {
                        distance_m: s.distance.get::<meter>(),
                        duration_s,
                        // Guard against zero-time steps (shouldn't happen,
                        // but avoids a NaN in the JSON if it ever does).
                        speed_kt: if duration_s > 0.0 {
                            s.speed().get::<knot>()
                        } else {
                            0.0
                        },
                    }
                })
                .collect();

            // Sunset at the starting waypoint's coordinate on the
            // start date. `sunset_loc` is captured once outside this
            // loop, but `date` changes per departure so sunset
            // follows calendar drift across the schedule.
            let sunset_time = sunset_loc.and_then(|(lat, lon)| {
                crate::sun::sunset_naive_utc(lat, lon, start_dt.date()).map(format_ts)
            });

            departures.push(DepartureExport {
                start_time: format_ts(start_dt),
                end_time: format_ts(end_dt),
                duration_s,
                sunset_time,
                waypoint_arrivals: arrivals,
                segments,
            });
        }

        departures.sort_by(|a, b| a.start_time.cmp(&b.start_time));

        let file = TripExport {
            version: TRIP_EXPORT_VERSION,
            speed_kt: self.speed.get::<knot>(),
            departures,
        };
        Ok(serde_json::to_string_pretty(&file)?)
    }

    fn start_sweep(&mut self, target_leg_count: usize, prior_leg_count: usize) {
        // Snapshot everything the worker needs. The 30m map is Arc'd so this
        // is a refcount bump; nn_calc is cloned (small R-tree over ~4k
        // stations); scalars are Copy. The step/results/5m caches are
        // *cloned* into the worker (small HashMaps) and shipped back via
        // the channel so subsequent sweeps start warm. Filters
        // (weekdays/daytime) are intentionally NOT passed — worker
        // computes every start; filtering happens at materialize time.
        let waypoints = self.waypoints.clone();
        let speed = self.speed;
        let predictions_30m = self.current_predictions_30m.clone();
        let mut nn_calc = self.nn_calc.clone();
        let mut predictions_5m = self.current_predictions_5m.clone();
        let mut results = self.results.clone();
        let mut step_cache = self.step_cache.clone();
        let generation = self.sweep_generation;

        // Fresh progress struct per sweep. The old one (if any) stays
        // live for whatever stale worker may still be incrementing it,
        // but nothing else reads it anymore.
        self.sweep_progress = Arc::new(SweepProgress::new());
        let progress = self.sweep_progress.clone();

        let (tx, rx) = std::sync::mpsc::channel();
        spawn_sweep_task(async move {
            compute_sweep(
                &waypoints,
                speed,
                &predictions_30m,
                &mut predictions_5m,
                &mut nn_calc,
                &mut results,
                &mut step_cache,
                target_leg_count,
                prior_leg_count,
                &progress,
            )
            .await;
            let _ = tx.send((
                generation,
                SweepCaches {
                    target_leg_count,
                    predictions_5m,
                    results,
                    step_cache,
                },
            ));
        });
        self.sweep_job = Some(SweepJob {
            rx: parking_lot::Mutex::new(rx),
        });
    }
}

/// Spawn the sweep future.
///
/// Native: `std::thread::spawn` + `futures::executor::block_on` runs the
/// future to completion on a dedicated OS thread. Async yields compile
/// to near-no-op rescheduled polls on this executor, so the async-ness
/// is free and the OS-level parallelism is what we'd get from the old
/// sync `thread::spawn` anyway.
///
/// Wasm: `spawn_local` runs the future on the browser's microtask/task
/// loop. Yields between sweep chunks let paint and input land.
#[cfg(not(target_arch = "wasm32"))]
fn spawn_sweep_task<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    std::thread::spawn(move || {
        futures::executor::block_on(fut);
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_sweep_task<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(fut);
}

/// Cooperative yield for the sweep worker.
///
/// Wasm: macrotask via `setTimeout(0)`, so the browser gets to paint
/// and process input between chunks. Microtask-based yields
/// (`Promise::resolve`) drain before paint and would starve the UI.
/// Mirrors the helper in `setup.rs`.
///
/// Native: no-op. The sweep runs on a dedicated OS thread via
/// `block_on`, so there's no event loop to relinquish and no paint to
/// let through. The `async fn` resolves to `Ready` on first poll, so
/// the executor doesn't park — essentially zero cost. Keeping the
/// yield call sites in the unified `compute_sweep` body costs us
/// nothing and lets one function serve both targets.
#[cfg(target_arch = "wasm32")]
async fn yield_to_event_loop() {
    use wasm_bindgen::JsCast;
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window
                .set_timeout_with_callback_and_timeout_and_arguments_0(resolve.unchecked_ref(), 0);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::unused_async)]
async fn yield_to_event_loop() {}

/// Non-method sibling of `Trip::calculate`. Extracted so the sweep worker
/// can call it without needing `&mut Trip`. Memoizes per-leg results in
/// `step_cache` — inputs (waypoints, speed, predictions) are invariant
/// across all calls against the same cache, so any `(leg_idx, entry_ts)`
/// pair hit more than once reuses the prior `StepResult`.
fn calculate_trip(
    waypoints: &[Waypoint],
    speed: Velocity,
    predictions_30m: &HashMap<Station, CurrentPrediction<30>>,
    predictions_5m: &mut HashMap<Station, CurrentPrediction<5>>,
    nn_calc: &mut NearestNeighborCalculator,
    results: &mut HashMap<usize, Option<TripResult>>,
    step_cache: &mut HashMap<(usize, usize), Option<StepResult>>,
    start_time_idx: usize,
) -> Option<TripResult> {
    match results.get(&start_time_idx) {
        // A cached success is only safe to reuse if it covers every
        // leg of the current trip. After an append, previously-cached
        // entries are prefix-short — falling through and recomputing
        // lets step_cache supply all the prior legs for free and only
        // the NEW tail legs incur real work.
        Some(Some(tr)) if tr.steps.len() == waypoints.len() => return Some(tr.clone()),
        // A cached failure is a prefix failure; extending the trip
        // can't un-fail an earlier leg. Safe to short-circuit.
        Some(None) => return None,
        _ => {},
    }

    let mut ts = start_time_idx;
    let mut steps: Vec<StepResult> = vec![StepResult::default()];
    let result = 'done: {
        for (leg_idx, (a, b)) in waypoints.iter().tuple_windows().enumerate() {
            let Some(res) = *step_cache.entry((leg_idx, ts)).or_insert_with(|| {
                calculate_step(a, b, speed, predictions_30m, predictions_5m, ts, nn_calc)
            }) else {
                break 'done None;
            };
            ts += res.time_steps;
            steps.push(res);
        }
        Some(TripResult { steps })
    };
    results.insert(start_time_idx, result.clone());
    result
}

/// Time ratio between the 30-minute outer grid (sweep start indices) and
/// the 5-minute inner step. Used to map "sweep start i" → "starting
/// time_idx in the 5m series".
pub fn sweep_time_ratio() -> usize {
    let internal_time_step = Time::new::<minute>(5.0);
    let ratio: Ratio = Time::new::<minute>(30.0) / internal_time_step;
    ratio.value as usize
}

/// Enumerate every 30m start index with its timestamp. No filtering
/// by weekday/daytime — those are applied at DataFrame materialize
/// time so a filter toggle doesn't invalidate worker output.
fn all_sweep_start_indices(
    predictions_30m: &HashMap<Station, CurrentPrediction<30>>,
) -> Vec<(usize, NaiveDateTime)> {
    let any_pred = predictions_30m.values().next().unwrap();
    any_pred.df["time"]
        .datetime()
        .unwrap()
        .to_vec_null_aware()
        .unwrap_left()
        .iter()
        .map(|ts| DateTime::from_timestamp_millis(*ts).unwrap().naive_utc())
        .enumerate()
        .collect()
}

/// Build the displayed DataFrame from fully-populated per-start
/// `results`. This is the filter-application seam: weekdays and the
/// start-hour daytime filter drop entries here, then
/// `finalize_sweep_df` applies the end-before-21:00 filter and the
/// 20th-percentile "fast starts" cutoff. Returns `None` if no start
/// survives the filters — callers treat that the same as "still
/// computing" in the existing sweep-return contract.
fn materialize_sweep_df(
    results: &HashMap<usize, Option<TripResult>>,
    all_indices: &[(usize, NaiveDateTime)],
    weekdays: WeekdayFlags,
    daytime: bool,
    arrive_before_sunset: bool,
    sunset_loc: Option<(f64, f64)>,
) -> Option<DataFrame> {
    let time_ratio = sweep_time_ratio();
    let trip_results: Vec<(usize, NaiveDateTime, TripResult)> = all_indices
        .iter()
        .filter(|(_, dt)| {
            weekdays.contains(
                WeekdayFlags::from_bits(1 << dt.weekday().num_days_from_monday()).unwrap(),
            )
        })
        .filter(|(_, dt)| !daytime || dt.hour() >= 8)
        .filter_map(|(idx, dt)| {
            let tr = results.get(&(time_ratio * idx))?.as_ref()?;
            Some((*idx, *dt, tr.clone()))
        })
        .collect();

    if trip_results.is_empty() {
        return None;
    }
    Some(finalize_sweep_df(
        trip_results,
        daytime,
        arrive_before_sunset,
        sunset_loc,
    ))
}

/// Turn per-start trip results into the Departure-Time/Duration DataFrame
/// the UI plots. Applies the optional "must finish before 21:00" filter
/// and the 20th-percentile duration cutoff that keeps only "fast" starts.
fn finalize_sweep_df(
    mut trip_results: Vec<(usize, NaiveDateTime, TripResult)>,
    daytime: bool,
    arrive_before_sunset: bool,
    sunset_loc: Option<(f64, f64)>,
) -> DataFrame {
    if daytime {
        trip_results.retain(|(_, dt, result)| {
            (*dt + TimeDelta::seconds(result.time().get::<second>() as i64))
                < dt.date().and_hms_opt(21, 0, 0).unwrap()
        });
    }

    // Drop departures whose *end time* sits past sunset at the
    // starting waypoint. Sunset is computed per start date from the
    // `sunrise` crate; a None return (polar day/night) is treated as
    // "no constraint" so the feature degrades gracefully at extreme
    // latitudes rather than removing every row.
    if arrive_before_sunset && let Some((lat, lon)) = sunset_loc {
        trip_results.retain(|(_, dt, result)| {
            let end_dt = *dt + TimeDelta::seconds(result.time().get::<second>() as i64);
            crate::sun::sunset_naive_utc(lat, lon, dt.date()).is_none_or(|sunset| end_dt < sunset)
        });
    }

    let (time_idx_vec, durations): (Vec<u64>, Vec<f64>) = trip_results
        .iter()
        .map(|(i, _, result)| (*i as u64, result.time().value))
        .unzip();

    let df = DataFrame::new(vec![
        Series::new("idx", time_idx_vec),
        Series::new("duration", durations),
    ])
    .unwrap();
    df.lazy()
        .filter(
            col("duration")
                .lt_eq(col("duration").quantile(lit(0.2), QuantileInterpolOptions::Nearest)),
        )
        .collect()
        .unwrap()
}

/// Walk each leg's great-circle path, sample at `SAMPLE_INTERVAL_M`,
/// and return the unique nearest stations in the order they're first
/// encountered. Feeds the pre-warm pass so the first sweep start
/// doesn't pay the full 30m→5m resample cost for every route station
/// inside `calculate_step`'s hot loop — the single biggest source of
/// spike latency on both targets.
fn stations_on_route(
    waypoints: &[Waypoint],
    nn_calc: &mut NearestNeighborCalculator,
) -> Vec<Station> {
    // Stations model tidal-current areas kilometers across, so 500m
    // sampling won't miss transitions. Cost is an nn-lookup per
    // sample (RTree query + LRU cache), microseconds each — trivial
    // next to the resamples this saves.
    const SAMPLE_INTERVAL_M: f64 = 500.0;

    let mut seen: HashSet<Station> = HashSet::new();
    let mut out: Vec<Station> = Vec::new();

    for (a, b) in waypoints.iter().tuple_windows() {
        if matches!(b.type_, WaypointType::Pause) {
            continue;
        }
        let a_pos = geo_pos(a.lat(), a.lon());
        let b_pos = geo_pos(b.lat(), b.lon());
        let ned = LocalFrame::ned(a_pos, Ellipsoid::WGS84);
        let delta = ned.geodetic_to_local_pos(b_pos);
        let distance_m = delta.slant_range().as_metres();
        let n = (distance_m / SAMPLE_INTERVAL_M).ceil().max(1.0) as usize;
        let l_frame = LocalFrame::local_level(delta.azimuth(), a_pos, Ellipsoid::WGS84);
        for i in 0..=n {
            let t = i as f64 / n as f64;
            let step = LocalPositionVector::from_metres(distance_m * t, 0.0, 0.0);
            let pos = l_frame.local_to_geodetic_pos(step);
            let ll = LatLong::from_nvector(pos.horizontal_position());
            if let Some(station) = nn_calc.nearest_neighbor(ll)
                && seen.insert(station.clone())
            {
                out.push(station);
            }
        }
    }
    out
}

/// Sweep worker body. Computes `TripResult` for every start index,
/// populating `results` and `step_cache` as a side effect. No filters —
/// weekdays/daytime apply at materialize time on the main thread. On
/// cancel, returns early; the partial caches that come back are
/// discarded by the generation check in `sweep()`, so partial state
/// never leaks into Trip.
///
/// One async body serves both targets. Wasm runs it on the browser's
/// task loop (via `spawn_local`) and the yields are real macrotask
/// suspensions that let paint and input through. Native runs it on a
/// dedicated OS thread (via `std::thread::spawn` + `block_on`) and the
/// yields are no-ops — the `async fn` resolves `Ready` immediately, so
/// no scheduler round-trip. The cancel/progress contract is identical.
#[allow(clippy::too_many_arguments)]
async fn compute_sweep(
    waypoints: &[Waypoint],
    speed: Velocity,
    predictions_30m: &HashMap<Station, CurrentPrediction<30>>,
    predictions_5m: &mut HashMap<Station, CurrentPrediction<5>>,
    nn_calc: &mut NearestNeighborCalculator,
    results: &mut HashMap<usize, Option<TripResult>>,
    step_cache: &mut HashMap<(usize, usize), Option<StepResult>>,
    target_leg_count: usize,
    prior_leg_count: usize,
    progress: &SweepProgress,
) {
    // Half a frame at 60fps. Short enough that paint and input still
    // feel smooth on wasm; long enough that per-yield overhead
    // (setTimeout min-delay clamp, ~1–4 ms in most browsers) doesn't
    // dominate total sweep time. Native ignores it since the yield is
    // a no-op, but the wall-clock check is still O(ns) and harmless.
    const YIELD_BUDGET_MS: f64 = 8.0;

    // Let the browser paint at least once (the "Calculating…" label)
    // before we start any synchronous work. No-op on native.
    yield_to_event_loop().await;

    let time_ratio = sweep_time_ratio();
    let time_idx_vec = all_sweep_start_indices(predictions_30m);
    // Units are trip-steps = `starts × legs`. A fresh sweep starts at
    // 0, an append-driven extend starts at the high-water mark of the
    // prior coverage, so the bar doesn't snap back to 0. Per-start
    // delta is only the *new* legs — cached prior legs come from
    // step_cache at ~zero CPU and we don't claim credit for them.
    let per_start_delta = target_leg_count.saturating_sub(prior_leg_count);
    progress
        .total
        .store(time_idx_vec.len() * target_leg_count, Ordering::Relaxed);
    progress
        .completed
        .store(time_idx_vec.len() * prior_leg_count, Ordering::Relaxed);

    // Pre-warm 5m cache for every station along the route. Without
    // this, the first sweep start would synchronously resample for
    // every touched station, making start #1 ~100× slower than the
    // rest. Flattens the spike so sweep wall-clock is dominated by
    // the constant per-start cost. Each resample is ~tens of ms
    // (polars upsample + linear interp); the post-station yield gives
    // wasm paint a chance between resamples.
    for station in stations_on_route(waypoints, nn_calc) {
        if progress.cancelled.load(Ordering::Relaxed) {
            return;
        }
        if !predictions_5m.contains_key(&station)
            && let Some(pred_30m) = predictions_30m.get(&station)
            && let Ok(pred_5m) = pred_30m.resampled::<5>()
        {
            predictions_5m.insert(station, pred_5m);
        }
        yield_to_event_loop().await;
    }

    let mut budget_start = web_time::Instant::now();
    for (idx, _dt) in &time_idx_vec {
        // Check at the top of each start — a yield may have happened at
        // the previous iteration's bottom, and that's exactly where a
        // UI waypoint edit could have flipped the cancel flag.
        if progress.cancelled.load(Ordering::Relaxed) {
            return;
        }
        calculate_trip(
            waypoints,
            speed,
            predictions_30m,
            predictions_5m,
            nn_calc,
            results,
            step_cache,
            time_ratio * idx,
        );
        progress
            .completed
            .fetch_add(per_start_delta, Ordering::Relaxed);
        if budget_start.elapsed().as_secs_f64() * 1000.0 >= YIELD_BUDGET_MS {
            yield_to_event_loop().await;
            budget_start = web_time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use galileo::layer::feature_layer::FeatureLayer;
    use galileo_types::geo::Crs;
    use galileo_types::geo::NewGeoPoint;
    use galileo_types::geo::impls::GeoPoint2d;
    use uom::si::f64::Velocity;
    use uom::si::length::mile;
    use uom::si::time::hour;
    use uom::si::velocity::knot;

    use super::*;
    use crate::features::TripPath;
    use crate::features::TripPathSymbol;
    use crate::features::Waypoint;
    use crate::features::WaypointSymbol;
    use crate::features::WaypointType;
    use crate::noaa::Station;

    /// Build an empty `TripPath` feature layer for test harnesses.
    /// Inline at call sites would need the same `#[allow(..)]` and a
    /// three-line `new(...)` call — this wraps both.
    fn empty_path_layer()
    -> Arc<RwLock<FeatureLayer<Point2, TripPath, TripPathSymbol, CartesianSpace2d>>> {
        #[allow(clippy::arc_with_non_send_sync)]
        Arc::new(RwLock::new(FeatureLayer::new(
            Vec::<TripPath>::new(),
            TripPathSymbol {},
            Crs::EPSG3857,
        )))
    }

    /// Convert decimal-degree lat/lon to the map-internal EPSG:3857 Point2.
    /// Mirrors the projection path `add_waypoint` uses when the UI hands us
    /// a screen-projected position — the test bypasses the screen step and
    /// drops waypoints directly on the map plane.
    fn to_web_mercator(lat: f64, lon: f64) -> galileo_types::cartesian::Point2 {
        Crs::EPSG3857
            .get_projection()
            .unwrap()
            .project(&GeoPoint2d::latlon(lat, lon))
            .unwrap()
    }

    /// Regression test for trip calculation. Uses a fixed date and a fixed
    /// lat/lon box so predictions + waypoints are fully deterministic
    /// across runs. Prints the aggregated distance/time with a greppable
    /// prefix so external tooling (or a parallel git-worktree run at a
    /// different commit) can diff the two outputs line-for-line.
    #[test]
    fn trip_calculation_regression() {
        let api_proxy = None;
        let start = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
        let hours: u32 = 24;

        let stations_set =
            futures::executor::block_on(Station::in_area((40.5, 40.9), (-74.2, -73.8), api_proxy))
                .unwrap();
        // `in_area` returns a HashSet — sort by id so the prediction vector
        // (and therefore any order-dependent state inside Trip/nn_calc) is
        // stable across runs.
        let mut stations: Vec<Station> = stations_set.into_iter().collect();
        stations.sort_by(|a, b| a.id.cmp(&b.id));

        let predictions: Vec<CurrentPrediction<30>> = stations
            .iter()
            .map(|s| futures::executor::block_on(s.current_prediction(start, hours)).unwrap())
            .collect();

        #[allow(clippy::arc_with_non_send_sync)]
        let waypoint_layer = Arc::new(RwLock::new(FeatureLayer::new(
            Vec::<Waypoint>::new(),
            WaypointSymbol {},
            Crs::EPSG3857,
        )));

        let mut trip = Trip::new(
            Velocity::new::<knot>(3.0),
            waypoint_layer,
            empty_path_layer(),
            predictions,
        )
        .unwrap();

        // Three-leg trip through NY harbor. Coordinates are fixed so the
        // calculated great-circle distance, and the nearest-station lookup
        // along every step, are deterministic.
        trip.waypoints = vec![
            Waypoint {
                point: to_web_mercator(40.70, -74.02),
                type_: WaypointType::Move,
            },
            Waypoint {
                point: to_web_mercator(40.68, -74.04),
                type_: WaypointType::Pause,
            },
            Waypoint {
                point: to_web_mercator(40.65, -74.06),
                type_: WaypointType::Move,
            },
        ];

        let result = trip
            .calculate(0)
            .expect("trip calculation returned None — no stations in test radius?");

        let distance_mi = result.distance().get::<mile>();
        let time_h = result.time().get::<hour>();
        let n_time_steps: usize = result.steps.iter().map(|s| s.time_steps).sum();

        // Greppable line: any drift in trip-calc math shifts these numbers,
        // and a diff between two runs will surface it immediately.
        eprintln!(
            "REGRESSION stations={} waypoints={} steps={} time_steps={} distance_mi={:.8} time_h={:.8}",
            stations.len(),
            trip.waypoints.len(),
            result.steps.len(),
            n_time_steps,
            distance_mi,
            time_h,
        );
    }

    /// Round-trip a waypoints file through JSON and confirm coordinates
    /// survive the lat/lon ↔ EPSG:3857 projection within float tolerance.
    /// Avoids touching the full `Trip` (which needs network + stations)
    /// by testing the DTO + projection directly.
    #[test]
    fn waypoints_json_roundtrip() {
        let proj = Crs::EPSG3857
            .get_projection::<GeoPoint2d, galileo_types::cartesian::Point2>()
            .unwrap();

        let original = WaypointsFile {
            version: WAYPOINTS_FILE_VERSION,
            waypoints: vec![
                WaypointDto {
                    lat: 40.7127,
                    lon: -74.0059,
                    type_: WaypointTypeDto::Move,
                },
                WaypointDto {
                    lat: 40.68,
                    lon: -74.04,
                    type_: WaypointTypeDto::Pause,
                },
            ],
        };

        let json = serde_json::to_string(&original).unwrap();
        assert!(json.contains("\"type\":\"move\""));
        assert!(json.contains("\"type\":\"pause\""));
        assert!(json.contains("\"version\":1"));

        let back: WaypointsFile = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, 1);
        assert_eq!(back.waypoints.len(), 2);

        // Project each DTO through Point2 and unproject — coordinates
        // should come back within a few micro-degrees.
        for (orig, round) in original.waypoints.iter().zip(back.waypoints.iter()) {
            let pt = proj
                .project(&GeoPoint2d::latlon(round.lat, round.lon))
                .unwrap();
            let back_geo = proj.unproject(&pt).unwrap();
            assert!((back_geo.lat() - orig.lat).abs() < 1e-9);
            assert!((back_geo.lon() - orig.lon).abs() < 1e-9);
        }
    }

    /// End-to-end smoke for `export_best_departures_json`: loads real
    /// stations + predictions via the same path as the regression test,
    /// drives `sweep_blocking` to completion through the export, and
    /// parses the resulting JSON to confirm the schema is stable and
    /// populated. Doesn't pin specific numbers — those would drift with
    /// any tide-calc change; the regression test already anchors
    /// per-leg math. This test anchors the *shape* and the guarantee
    /// that we get at least one best departure.
    #[test]
    fn best_departures_export_smoke() {
        let api_proxy = None;
        let start = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
        let hours: u32 = 24 * 3;

        let stations_set =
            futures::executor::block_on(Station::in_area((40.5, 40.9), (-74.2, -73.8), api_proxy))
                .unwrap();
        let mut stations: Vec<Station> = stations_set.into_iter().collect();
        stations.sort_by(|a, b| a.id.cmp(&b.id));

        let predictions: Vec<CurrentPrediction<30>> = stations
            .iter()
            .map(|s| futures::executor::block_on(s.current_prediction(start, hours)).unwrap())
            .collect();

        let mut trip = Trip::new_headless(Velocity::new::<knot>(3.0), predictions).unwrap();

        trip.waypoints = vec![
            Waypoint {
                point: to_web_mercator(40.70, -74.02),
                type_: WaypointType::Move,
            },
            Waypoint {
                point: to_web_mercator(40.65, -74.06),
                type_: WaypointType::Move,
            },
        ];
        let current_legs = trip.waypoints.len() - 1;

        let json = trip
            .export_best_departures_json()
            .expect("export returned None");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["version"], TRIP_EXPORT_VERSION);
        let departures = parsed["departures"].as_array().unwrap();
        assert!(!departures.is_empty(), "no best departures surfaced");

        let first = &departures[0];
        assert!(first["start_time"].is_string());
        assert!(first["end_time"].is_string());
        let arrivals = first["waypoint_arrivals"].as_array().unwrap();
        assert_eq!(arrivals.len(), 2, "expected 1 arrival per waypoint");
        let segments = first["segments"].as_array().unwrap();
        assert_eq!(segments.len(), current_legs);
        assert!(segments[0]["distance_m"].as_f64().unwrap() > 0.0);
        assert!(segments[0]["speed_kt"].as_f64().unwrap() > 0.0);
    }

    /// A file with an unknown version must be rejected rather than
    /// silently accepted — keeps the format forward-compatible.
    #[test]
    fn waypoints_import_rejects_wrong_version() {
        let bad = r#"{"version":999,"waypoints":[]}"#;

        #[allow(clippy::arc_with_non_send_sync)]
        let waypoint_layer = Arc::new(RwLock::new(FeatureLayer::new(
            Vec::<Waypoint>::new(),
            WaypointSymbol {},
            Crs::EPSG3857,
        )));
        let mut trip = Trip::new(
            Velocity::new::<knot>(3.0),
            waypoint_layer,
            empty_path_layer(),
            vec![],
        )
        .unwrap();

        let err = trip.import_waypoints_json(bad).unwrap_err();
        assert!(err.to_string().contains("version"));
    }
}
