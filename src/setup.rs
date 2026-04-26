use std::collections::HashSet;
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::Datelike;
use chrono::Months;
use chrono::NaiveDate;
use chrono::offset::Local;
use galileo::Map;
use galileo::MapBuilder;
use galileo::layer::feature_layer::FeatureLayer;
use galileo::layer::raster_tile_layer::RasterTileLayerBuilder;
use galileo_types::geo::Crs;
use galileo_types::geo::GeoPoint;
use noaa_tides::ApiProxy;
use parking_lot::Mutex;
use parking_lot::RwLock;
use polars::prelude::*;
use uom::si::f64::Velocity;
use uom::si::velocity::knot;

use crate::Config;
use crate::app::CurrentPredictionLayer;
use crate::app::WaypointClickAction;
use crate::features::CurrentPredictionSymbol;
use crate::features::StationMarker;
use crate::features::StationMarkerSymbol;
use crate::features::TripPathSymbol;
use crate::features::WaypointSymbol;
use crate::noaa;
use crate::noaa::CurrentPrediction;
use crate::noaa::Station;
use crate::prelude::*;
use crate::saturating::Saturating;
use crate::scheduling::Trip;

pub const MAP_CENTER_LAT: f64 = 40.7127;
pub const MAP_CENTER_LON: f64 = -74.0059;
const MAP_CENTER_Z: u32 = 12;

/// Radius around the current map center for which we compute predictions.
/// Stations outside this radius are ignored until the user pans the map
/// close enough to bring them inside — at which point they're queued for
/// background loading. Nothing synchronous happens during `build()`; the
/// map appears with an empty prediction store and stations fade in as
/// the loader drains the queue.
pub const VISIBLE_RADIUS_METERS: f64 = 100.0 * 1609.344;

/// How many stations each background chunk processes before publishing.
/// Small enough that merges don't stall the UI, large enough that
/// per-chunk locking overhead is minor.
#[cfg(not(target_arch = "wasm32"))]
const BG_CHUNK: usize = 64;

/// On the single-threaded wasm main thread, the chunk is also how much
/// synchronous compute runs between yields to the browser event loop.
/// Small chunks keep paint and input latency under a frame.
#[cfg(target_arch = "wasm32")]
const BG_CHUNK: usize = 8;

/// How long the loader waits before re-polling the queue when it's
/// empty. On native the thread sleeps; on wasm we use setTimeout. The
/// gap is short enough that newly-enqueued stations start loading
/// promptly after the user pans, without busy-polling the event loop.
const IDLE_POLL_MS: u64 = 100;

/// FIFO of stations waiting for their predictions to be computed, plus a
/// "seen" set so re-enqueueing the same station (user pans away and
/// back) is a no-op. Shared between the UI thread (which enqueues
/// stations whose distance to the current map center drops inside
/// `VISIBLE_RADIUS_METERS`) and the background loader (which drains
/// chunks of up to `BG_CHUNK`).
pub struct LoaderQueue {
    pub pending: VecDeque<Station>,
    pub seen: HashSet<String>,
}

impl LoaderQueue {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            seen: HashSet::new(),
        }
    }
}

/// Cross-thread "the feature layer changed, please redraw" signal.
///
/// The loader runs off the UI thread (or, on wasm, inside a
/// `spawn_local` task) so it can't call `map.redraw()` directly — that
/// needs the `&mut Map` the App owns. Instead the loader flips
/// `dirty` and pokes egui via `request_repaint`, and the App consumes
/// the flag on its next frame and calls `redraw()` there.
///
/// The `ctx` slot is populated by the App on its first paint, since
/// eframe only hands us an `egui::Context` after `with_app_builder`
/// returns. Until then `notify()` still sets `dirty` — the next input
/// event (or the default 60 Hz egui tick while focused) will pick it
/// up.
pub struct RepaintSignal {
    dirty: AtomicBool,
    ctx: Mutex<Option<egui::Context>>,
}

impl RepaintSignal {
    fn new() -> Self {
        Self {
            dirty: AtomicBool::new(false),
            ctx: Mutex::new(None),
        }
    }

    fn notify(&self) {
        self.dirty.store(true, Ordering::Relaxed);
        if let Some(ctx) = &*self.ctx.lock() {
            ctx.request_repaint();
        }
    }

    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::Relaxed)
    }

    pub fn install_ctx(&self, ctx: &egui::Context) {
        self.ctx.lock().get_or_insert_with(|| ctx.clone());
    }
}

/// Station-load progress exposed to the UI.
///
/// * `completed` — bumped once per successful prediction by the compute
///   workers (native + wasm).
/// * `queued` — cumulative number of stations ever pushed onto the
///   loader queue. Grows as the user pans new territory into the
///   visible radius. Lets the UI show progress against the *currently
///   requested* workload rather than the full store.
/// * `total` — immutable count of every station in the embedded store.
///   Upper bound on both of the above.
#[derive(Clone)]
pub struct LoadProgress {
    pub completed: Arc<AtomicUsize>,
    pub queued: Arc<AtomicUsize>,
    pub total: usize,
}

pub struct SetupBundle {
    pub map: Map,
    pub trip: Arc<RwLock<Trip>>,
    pub time_idx: Arc<RwLock<Saturating<usize>>>,
    pub battery_tide_predictions: DataFrame,
    pub waypoint_mode: Arc<RwLock<WaypointClickAction>>,
    pub current_prediction_layer: CurrentPredictionLayer,
    pub load_progress: LoadProgress,
    pub harcon_bytes: usize,
    /// Current map center published by the UI every frame. The loader
    /// compares it to its own last-scan center between batches; if the
    /// user has panned far enough, the loader re-filters the full
    /// station list against the new center and appends any newly
    /// in-radius stations to the queue. Moving the scan here keeps the
    /// ~4000-station iteration off the paint path.
    pub pending_center: Arc<RwLock<Option<(f64, f64)>>>,
    /// Poked by the loader after each merged batch so the map repaints
    /// without waiting for user input. Consumed on the UI thread,
    /// which calls `map.redraw()` to surface the new features.
    pub repaint: Arc<RepaintSignal>,
    /// Clipped prediction batches staged by the loader for the UI to
    /// fold into the trip and feature layer. Keeping the merge on the
    /// UI thread avoids cross-thread contention with `trip.write()`
    /// (held by the UI every frame while rendering the table) and
    /// `layer.read()` (held by galileo while painting).
    pub pending_merges: Arc<Mutex<Vec<CurrentPrediction<30>>>>,
}

pub async fn build(config: Config) -> Result<SetupBundle> {
    let api_proxy = if config.use_api_proxy {
        Some(ApiProxy {
            url: config.api_proxy_url,
        })
    } else {
        None
    };

    let battery = Station::new("8518750", api_proxy.clone()).await.log()?;

    let today = Local::now().date_naive();
    let start_date = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).log()?;
    // Cover the current month plus the next month. Calendar-aware so the
    // slider range (and every station's prediction window) ends exactly at
    // the last moment of next month instead of clipping a day or two.
    let end_date = start_date.checked_add_months(Months::new(2)).log()?;
    let duration_hours = ((end_date - start_date).num_days() * 24) as u32;

    let battery_tide_predictions = battery
        .tide_prediction(start_date, duration_hours)
        .await
        .log()?;

    let time_vec = battery_tide_predictions["time"]
        .datetime()
        .log()?
        .to_vec_null_aware()
        .unwrap_left();

    let max_time_idx = time_vec.len() - 1;
    let time_floor_ms = time_vec[0];

    // Full candidate station list — moved into the loader task via
    // `Arc`. Not pre-sorted: the radius scan runs inside the loader,
    // and the queue is populated in whatever order stations pass the
    // radius test (which roughly means near-first anyway since the
    // first scan only covers the current center).
    let all_stations: Arc<Vec<Station>> = Arc::new(Station::all(api_proxy.clone()));
    info!("stations: {} total in embedded store", all_stations.len());

    let total_stations = all_stations.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let queued = Arc::new(AtomicUsize::new(0));
    let load_progress = LoadProgress {
        completed: completed.clone(),
        queued: queued.clone(),
        total: total_stations,
    };

    // Shared queue. Seeded by the loader on its first iteration via
    // `pending_center`, which is initialized to the initial map center.
    let loader_queue = Arc::new(Mutex::new(LoaderQueue::new()));
    let pending_center = Arc::new(RwLock::new(Some((MAP_CENTER_LAT, MAP_CENTER_LON))));
    let repaint = Arc::new(RepaintSignal::new());
    let pending_merges: Arc<Mutex<Vec<CurrentPrediction<30>>>> = Arc::new(Mutex::new(Vec::new()));

    let time_idx = Arc::new(RwLock::new(Saturating::new(0, 0, max_time_idx)));

    // Map, trip, and current-prediction layer all start empty. They're
    // populated by the background loader after `launch()` returns — so
    // the UI runner can wire up the map, tile layer starts fetching,
    // and egui paints its first frame without waiting on any prediction
    // math.
    let current_prediction_layer = FeatureLayer::new(
        Vec::<CurrentPrediction<30>>::new(),
        CurrentPredictionSymbol {
            time_idx: time_idx.clone(),
        },
        Crs::EPSG3857,
    );
    // Galileo layers and `Trip` hold `Rc`-backed handles, so they aren't
    // `Send+Sync`. On native we still need shared ownership across threads
    // for the background loader — `parking_lot::RwLock` is `Send+Sync` and
    // the handles inside are only touched from locked sections. On wasm
    // everything runs on one thread; clippy still flags the `Arc`, but the
    // alternative (`Rc<RefCell>`) would re-diverge the native/wasm paths.
    #[allow(clippy::arc_with_non_send_sync)]
    let current_prediction_layer = Arc::new(RwLock::new(current_prediction_layer));

    let waypoint_layer = FeatureLayer::new(vec![], WaypointSymbol {}, Crs::EPSG3857);
    #[allow(clippy::arc_with_non_send_sync)]
    let waypoint_layer = Arc::new(RwLock::new(waypoint_layer));

    let path_layer = FeatureLayer::new(vec![], TripPathSymbol {}, Crs::EPSG3857);
    #[allow(clippy::arc_with_non_send_sync)]
    let path_layer = Arc::new(RwLock::new(path_layer));

    // Static "station exists here" dot for every station in the embedded
    // store. Built synchronously at setup — no staging or background
    // work needed because the feature set never changes after this
    // point. Lets the user see the distribution of available stations
    // as they zoom out, before the prediction loader has touched any of
    // them. Drops the heavy `Station` (id, name, api_proxy) so we only
    // carry the two fields the symbol actually reads.
    let station_marker_features: Vec<StationMarker> = all_stations
        .iter()
        .map(|s| StationMarker {
            loc: s.loc,
            type_: s.type_,
        })
        .collect();
    let station_marker_layer = FeatureLayer::new(
        station_marker_features,
        StationMarkerSymbol {},
        Crs::EPSG3857,
    );

    #[allow(clippy::arc_with_non_send_sync)]
    let trip = Arc::new(RwLock::new(Trip::new(
        Velocity::new::<knot>(3.0),
        waypoint_layer.clone(),
        path_layer.clone(),
        Vec::new(),
    )?));

    spawn_background_load(
        loader_queue,
        queued,
        all_stations,
        pending_center.clone(),
        start_date,
        duration_hours,
        time_floor_ms,
        completed,
        repaint.clone(),
        pending_merges.clone(),
    );

    let waypoint_mode = Arc::new(RwLock::new(WaypointClickAction::Move));

    // `tile_cache_dir` is Config-driven so the Android caller can pass
    // an absolute path under the app's internal data directory. On
    // wasm, `with_file_cache_checked` is a no-op regardless, so the
    // value is ignored there.
    let mut raster_layer = RasterTileLayerBuilder::new_osm()
        .with_file_cache_checked(&config.tile_cache_dir)
        .build()
        .log()?;
    // Disable the 300ms fade-in animation galileo applies to every
    // tile that enters the displayed set. The fade is meant to hide
    // network latency on fresh fetches, but it fires on cached
    // re-mounts too (zoom out → in re-inserts tiles into the
    // displayed set and restarts the animation from opacity 0), which
    // makes cached re-visits feel sluggish. Skipping it trades a
    // small visual pop on genuine fresh loads for instant feedback
    // everywhere else.
    raster_layer.set_fade_in_duration(std::time::Duration::ZERO);

    let map = MapBuilder::default()
        .with_latlon(MAP_CENTER_LAT, MAP_CENTER_LON)
        .with_z_level(MAP_CENTER_Z)
        .with_layer(raster_layer)
        // Station-existence dots sit above tiles but below every other
        // overlay. Once a current arrow loads, its world-space shaft
        // emanates from the same projected point as the dot, so the dot
        // becomes the visual anchor at the base of the arrow rather
        // than competing with it.
        .with_layer(station_marker_layer)
        // Path goes below the waypoint dots so the arrows don't obscure
        // the waypoint markers the user just clicked to place.
        .with_layer(path_layer)
        .with_layer(waypoint_layer)
        .with_layer(current_prediction_layer.clone())
        .build();

    // Walk the embedded harcon store once; immutable after `LazyLock`
    // init, so the number never changes and the UI reads a constant.
    let harcon_bytes = noaa::STORE.bytes_in_memory();

    Ok(SetupBundle {
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
    })
}

/// Enqueue every station within `radius_m` of `(lat, lon)` that hasn't
/// been enqueued already. Runs inside the loader task, so the cost is
/// off the UI paint path.
///
/// Uses equirectangular squared-degree distance rather than haversine:
/// for a simple visibility-gate filter at a ~100-mile radius, the
/// great-circle error is negligible and the per-station cost is ~10×
/// cheaper. The `seen` set means a station that's already been queued
/// will not be re-queued when the user pans away and back.
///
/// Every newly-added station bumps `queued_counter`, which is how the
/// UI shows "loaded / cumulative-queued (out of total)".
fn enqueue_within_radius(
    queue: &Mutex<LoaderQueue>,
    queued_counter: &AtomicUsize,
    all_stations: &[Station],
    lat: f64,
    lon: f64,
    radius_m: f64,
) {
    const METERS_PER_DEG_LAT: f64 = 111_320.0;
    let radius_deg = radius_m / METERS_PER_DEG_LAT;
    let radius_deg_sq = radius_deg * radius_deg;
    let lon_scale = lat.to_radians().cos();

    let mut q = queue.lock();
    let mut added = 0;
    for s in all_stations {
        if q.seen.contains(&s.id) {
            continue;
        }
        let dlat = s.loc.lat() - lat;
        let dlon = (s.loc.lon() - lon) * lon_scale;
        if dlat * dlat + dlon * dlon <= radius_deg_sq {
            q.seen.insert(s.id.clone());
            q.pending.push_back(s.clone());
            added += 1;
        }
    }
    queued_counter.fetch_add(added, Ordering::Relaxed);
}

/// How far (in squared equirectangular degrees) the published map
/// center must move from the loader's last scan before it re-scans the
/// full station list. ~10 miles of hysteresis keeps small pans free
/// without starving newly-visible stations.
const ENQUEUE_MOVE_THRESHOLD_DEG_SQ: f64 = {
    // 10 miles ≈ 0.145° at mid-latitudes (69 mi per degree of lat).
    let deg = 10.0 / 69.0;
    deg * deg
};

/// Called by the loader between batches. If the UI-published center
/// has moved more than the threshold since the last scan, re-filter
/// the station list and append any newly in-radius stations.
fn maybe_rescan(
    queue: &Mutex<LoaderQueue>,
    queued_counter: &AtomicUsize,
    all_stations: &[Station],
    pending_center: &RwLock<Option<(f64, f64)>>,
    last_scan_center: &mut Option<(f64, f64)>,
) {
    let Some((lat, lon)) = *pending_center.read() else {
        return;
    };
    let should_scan = last_scan_center.is_none_or(|(p_lat, p_lon)| {
        let dlat = lat - p_lat;
        let dlon = (lon - p_lon) * lat.to_radians().cos();
        dlat * dlat + dlon * dlon > ENQUEUE_MOVE_THRESHOLD_DEG_SQ
    });
    if should_scan {
        enqueue_within_radius(
            queue,
            queued_counter,
            all_stations,
            lat,
            lon,
            VISIBLE_RADIUS_METERS,
        );
        *last_scan_center = Some((lat, lon));
    }
}

/// Drop prediction rows before `time_floor_ms` so every station's time
/// series starts at the same moment as the Battery tide baseline.
fn clip_to_time_floor(predictions: &mut [CurrentPrediction<30>], time_floor_ms: i64) -> Result<()> {
    for pred in predictions.iter_mut() {
        pred.df = mem::take(&mut pred.df)
            .lazy()
            .filter(col("time").gt_eq(time_floor_ms))
            .collect()
            .log()?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::unused_async)] // async for parity with the wasm variant
async fn compute_predictions(
    stations: &[Station],
    start: NaiveDate,
    hours: u32,
    progress: &AtomicUsize,
) -> Vec<CurrentPrediction<30>> {
    if stations.is_empty() {
        return Vec::new();
    }
    let cores = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(4);
    let n = stations.len();
    let results = parking_lot::Mutex::new(vec![None::<CurrentPrediction<30>>; n]);
    let next = AtomicUsize::new(0);

    // Work-stealing over the priority-sorted slice: each worker claims the
    // next-closest station, so near-first ordering is preserved even under
    // parallel execution. Progress is bumped per successful prediction so
    // the UI sees smooth station-granular updates.
    std::thread::scope(|s| {
        for _ in 0..cores {
            let results = &results;
            let next = &next;
            s.spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    if let Ok(pred) =
                        futures::executor::block_on(stations[i].current_prediction(start, hours))
                    {
                        results.lock()[i] = Some(pred);
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    results.into_inner().into_iter().flatten().collect()
}

#[cfg(target_arch = "wasm32")]
async fn compute_predictions(
    stations: &[Station],
    start: NaiveDate,
    hours: u32,
    progress: &AtomicUsize,
) -> Vec<CurrentPrediction<30>> {
    // `current_prediction` is declared `async` but does no real awaits —
    // it's all synchronous CPU inside harmonic/subordinate DF builders.
    // `join_all` therefore runs the whole batch back-to-back with zero
    // yields, so on wasm the main thread is stuck until *all* the near
    // stations finish (→ tiles, paint, and input all starve). Process
    // them one at a time with a macrotask yield between each so the
    // browser gets to paint progress, process tile fetches, and handle
    // input while the prediction pass runs.
    let mut out = Vec::with_capacity(stations.len());
    for s in stations {
        if let Ok(pred) = s.current_prediction(start, hours).await {
            progress.fetch_add(1, Ordering::Relaxed);
            out.push(pred);
        }
        sleep_ms(0).await;
    }
    out
}

/// Persistent loader. Drains chunks of up to `BG_CHUNK` stations from
/// `queue`, computes their predictions, and merges them into the trip
/// and feature layer. When the queue is empty the loader sleeps briefly
/// before re-checking — so stations the UI enqueues after the user pans
/// start loading within `IDLE_POLL_MS` without busy-waiting.
///
/// The loop never exits; it stays alive for the lifetime of the app.
#[allow(clippy::too_many_arguments)]
fn spawn_background_load(
    queue: Arc<Mutex<LoaderQueue>>,
    queued_counter: Arc<AtomicUsize>,
    all_stations: Arc<Vec<Station>>,
    pending_center: Arc<RwLock<Option<(f64, f64)>>>,
    start: NaiveDate,
    hours: u32,
    time_floor_ms: i64,
    progress: Arc<AtomicUsize>,
    repaint: Arc<RepaintSignal>,
    pending_merges: Arc<Mutex<Vec<CurrentPrediction<30>>>>,
) {
    #[cfg(not(target_arch = "wasm32"))]
    std::thread::spawn(move || {
        let mut last_scan_center: Option<(f64, f64)> = None;
        loop {
            maybe_rescan(
                &queue,
                &queued_counter,
                &all_stations,
                &pending_center,
                &mut last_scan_center,
            );
            let batch = pop_batch(&queue);
            if batch.is_empty() {
                std::thread::sleep(std::time::Duration::from_millis(IDLE_POLL_MS));
                continue;
            }
            let preds =
                futures::executor::block_on(compute_predictions(&batch, start, hours, &progress));
            stage_predictions(&pending_merges, preds, time_floor_ms, &repaint);
        }
    });

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(async move {
        let mut last_scan_center: Option<(f64, f64)> = None;
        loop {
            maybe_rescan(
                &queue,
                &queued_counter,
                &all_stations,
                &pending_center,
                &mut last_scan_center,
            );
            // Let the browser paint/process input between the scan
            // (which briefly holds the queue mutex and iterates every
            // station) and the next compute batch.
            sleep_ms(0).await;
            let batch = pop_batch(&queue);
            if batch.is_empty() {
                sleep_ms(IDLE_POLL_MS as u32).await;
                continue;
            }
            let preds = compute_predictions(&batch, start, hours, &progress).await;
            stage_predictions(&pending_merges, preds, time_floor_ms, &repaint);
        }
    });
}

/// Drain up to `BG_CHUNK` stations off the queue under a short lock
/// scope. Returns an empty Vec if the queue is empty, which signals the
/// loader loop to sleep.
fn pop_batch(queue: &Mutex<LoaderQueue>) -> Vec<Station> {
    let mut q = queue.lock();
    let n = q.pending.len().min(BG_CHUNK);
    q.pending.drain(..n).collect()
}

/// Wasm-only async sleep via `setTimeout`. `sleep_ms(0)` is the
/// macrotask yield used between compute chunks — microtasks
/// (`Promise::resolve`) all drain before paint, so relying on them
/// starves tiles/input; `setTimeout(0)` schedules a real task, letting
/// the browser paint and process fetch completions in between.
#[cfg(target_arch = "wasm32")]
async fn sleep_ms(ms: u32) {
    use wasm_bindgen::JsCast;
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                resolve.unchecked_ref(),
                ms as i32,
            );
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// Clip predictions to the shared time floor and hand them off to the
/// UI for merging. All lock work on `trip` and `current_prediction_layer`
/// happens on the UI thread, so this function only touches the staging
/// mutex — keeping the loader completely off the contended locks.
fn stage_predictions(
    pending: &Mutex<Vec<CurrentPrediction<30>>>,
    mut predictions: Vec<CurrentPrediction<30>>,
    time_floor_ms: i64,
    repaint: &RepaintSignal,
) {
    if predictions.is_empty() {
        return;
    }
    if clip_to_time_floor(&mut predictions, time_floor_ms).is_err() {
        return;
    }
    pending.lock().extend(predictions);

    // Wake the UI so it drains the pending queue and the newly-merged
    // stations show up without the user needing to click or pan.
    repaint.notify();
}
