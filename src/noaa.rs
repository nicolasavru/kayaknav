use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::NewGeoPoint;
use galileo_types::geo::impls::GeoPoint2d;
use itertools::Itertools;
use jord::GeodeticPos;
use jord::Length as jLength;
use jord::LocalFrame;
use jord::NVector;
use jord::ellipsoidal::Ellipsoid;
use noaa_tides::ApiProxy;
pub use noaa_tides::STORE;
use noaa_tides::StationInfo;
use noaa_tides::SubordinateOffsets;
use noaa_tides::apply_offsets;
use noaa_tides::cached_reference_events;
use noaa_tides::interp_events;
use noaa_tides::local_to_utc;
use noaa_tides::round_to_30m;
use noaa_tides::util::CMS_PER_KNOT;
use polars::prelude::Duration as PolarsDuration;
use polars::prelude::*;
use rstar::AABB;
use rstar::Envelope;
use rstar::Point;
use rstar::PointDistance;
use rstar::RTreeObject;

use crate::prelude::*;

/// Build a sea-level `GeodeticPos` from decimal-degree lat/lon. Hides the
/// `NVector`-wrap + `jLength::ZERO` boilerplate that `distance_to_m` and
/// its siblings in `scheduling.rs` repeat at every conversion. Lives here
/// (rather than a util module) because `noaa.rs` is the most fundamental
/// geographic-math consumer; `scheduling.rs` imports it as needed.
pub(crate) fn geo_pos(lat: f64, lon: f64) -> GeodeticPos {
    GeodeticPos::new(NVector::from_lat_long_degrees(lat, lon), jLength::ZERO)
}

#[derive(Debug, Copy, Clone)]
pub enum StationType {
    Harmonic,
    Subordinate,
}

#[derive(Debug, Clone)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub loc: GeoPoint2d,
    pub type_: StationType,
    pub api_proxy: Option<ApiProxy>,
}

impl PartialEq for Station {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Station {}

impl Hash for Station {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Station {
    #[allow(clippy::unused_async)] // async preserved for API shape (wasm callers await)
    pub async fn new(id: &str, api_proxy: Option<ApiProxy>) -> Result<Self> {
        let info = STORE
            .station_info(id)
            .ok_or_else(|| anyhow!("station {id} not in embedded harcon store"))
            .log()?;
        let (lat, lon) = info
            .lat
            .zip(info.lon)
            .ok_or_else(|| anyhow!("station {id} missing lat/lon"))
            .log()?;
        // Classify by which store list the station lives in — the NOAA
        // `station_type` string is ambiguous (both tide-subordinate and
        // current-subordinate stations use "S"). Tide stations fall
        // through to Harmonic since only `tide_prediction` is meaningful
        // for them and it doesn't branch on `type_`.
        let type_ = if STORE.subordinate(id).is_some() {
            StationType::Subordinate
        } else {
            StationType::Harmonic
        };
        Ok(Self {
            id: info.id.clone(),
            name: info.name.clone(),
            loc: GeoPoint2d::latlon(lat, lon),
            type_,
            api_proxy,
        })
    }

    /// Shared iterator over the bundled harcon store's current stations
    /// (harmonic + subordinate, in store order), skipping any without
    /// lat/lon. `keep` gates which infos turn into `Station`s.
    fn from_store(
        api_proxy: Option<ApiProxy>,
        keep: impl Fn(&StationInfo) -> bool,
    ) -> impl Iterator<Item = Self> {
        let harmonic = STORE
            .currents
            .iter()
            .map(|e| (&e.info, StationType::Harmonic));
        let subordinate = STORE
            .subordinates
            .iter()
            .map(|e| (&e.info, StationType::Subordinate));
        harmonic
            .chain(subordinate)
            .filter(move |(info, _)| keep(info))
            .filter_map(move |(info, type_)| {
                let lat = info.lat?;
                let lon = info.lon?;
                Some(Self {
                    id: info.id.clone(),
                    name: info.name.clone(),
                    loc: GeoPoint2d::latlon(lat, lon),
                    type_,
                    api_proxy: api_proxy.clone(),
                })
            })
    }

    /// Every current station (harmonic + subordinate) in the bundled harcon
    /// store, in store order. Stations missing lat/lon are skipped — they
    /// can't be placed on the map or used for nearest-neighbor routing.
    pub fn all(api_proxy: Option<ApiProxy>) -> Vec<Self> {
        Self::from_store(api_proxy, |_| true).collect()
    }

    /// Great-circle distance in meters from this station to `(lat, lon)`.
    pub fn distance_to_m(&self, lat: f64, lon: f64) -> f64 {
        let origin = geo_pos(lat, lon);
        let self_pos = geo_pos(self.loc.lat(), self.loc.lon());
        LocalFrame::ned(origin, Ellipsoid::WGS84)
            .geodetic_to_local_pos(self_pos)
            .slant_range()
            .as_metres()
    }

    #[allow(clippy::unused_async)] // async preserved for API shape (wasm callers await)
    pub async fn in_area(
        lat: (f64, f64),
        lon: (f64, f64),
        api_proxy: Option<ApiProxy>,
    ) -> Result<HashSet<Self>> {
        let (lat_min, lat_max) = (f64::min(lat.0, lat.1), f64::max(lat.0, lat.1));
        let (lon_min, lon_max) = (f64::min(lon.0, lon.1), f64::max(lon.0, lon.1));

        // Match the legacy NOAA `type=currentpredictions` endpoint:
        // harmonic currents + subordinate currents only. `from_store`
        // iterates these store lists directly so we don't accidentally
        // include tide stations (whose `station_type` field reuses "S" for
        // subordinate tides, not subordinate currents).
        let in_box = move |info: &StationInfo| {
            info.lat.zip(info.lon).is_some_and(|(la, lo)| {
                la >= lat_min && la <= lat_max && lo >= lon_min && lo <= lon_max
            })
        };
        Ok(Self::from_store(api_proxy, in_box).collect())
    }

    #[instrument(level = "debug")]
    pub async fn current_prediction(
        &self,
        start: NaiveDate,
        hours: u32,
    ) -> Result<CurrentPrediction<30>> {
        let t0_local = start.and_hms_opt(0, 0, 0).unwrap();
        let t_ref_utc = local_to_utc(t0_local + Duration::hours(hours as i64 / 2));
        let n_slots = (hours as i64) * 2 + 1;

        match self.type_ {
            StationType::Harmonic => {
                harmonic_current_df(self.clone(), t0_local, t_ref_utc, n_slots).log()
            },
            StationType::Subordinate => {
                subordinate_current_df(self.clone(), t0_local, t_ref_utc, hours, n_slots).log()
            },
        }
    }

    #[allow(clippy::unused_async)] // async preserved for API shape (wasm callers await)
    pub async fn tide_prediction(&self, start: NaiveDate, hours: u32) -> Result<DataFrame> {
        let t0_local = start.and_hms_opt(0, 0, 0).unwrap();
        let t_ref_utc = local_to_utc(t0_local + Duration::hours(hours as i64 / 2));

        let predictor = STORE
            .tide_predictor(&self.id, t_ref_utc)
            .ok_or_else(|| anyhow!("no tide harcon for station {}", self.id))
            .log()?;

        // Sample every minute in a padded window so the first and last slots
        // of the target window can inherit an H/L label from an adjacent
        // extremum even if the real extremum lies just outside the window.
        //
        // Padding must span at least one full tidal period so that every
        // slot in the target window has *some* prior extremum to anchor
        // the "H + n" / "L + n" counter against. Semidiurnal stations
        // have ~6.2 h between extrema; diurnal stations (rare; mostly
        // Gulf of Mexico) up to ~25 h. We pad 30 h on each side to
        // cover both regimes with headroom. The extra samples are
        // ~1800 evaluations of an analytic harmonic sum — negligible
        // compared to the per-station/per-month setup cost.
        let pad_min: i64 = 30 * 60;
        let n_samples = hours as i64 * 60 + 2 * pad_min;
        let samples: Vec<(NaiveDateTime, f64)> = (0..=n_samples)
            .map(|i| {
                let t_local = t0_local + Duration::minutes(i - pad_min);
                (t_local, predictor.at(local_to_utc(t_local)))
            })
            .collect();

        // Parabolic-vertex extrema detection; denom<0 is a maximum (high).
        let mut events: Vec<(NaiveDateTime, &'static str)> = Vec::new();
        for ((_, a), (tb, b), (_, c)) in samples.iter().copied().tuple_windows() {
            let denom = a - 2.0 * b + c;
            if denom.abs() > 1e-12 && (b - a).signum() != (c - b).signum() {
                let x = 0.5 * (a - c) / denom;
                if x.abs() <= 1.0 {
                    let t = tb + Duration::milliseconds((x * 60_000.0) as i64);
                    let kind = if denom < 0.0 { "H" } else { "L" };
                    events.push((t, kind));
                }
            }
        }
        events.sort_by_key(|e| e.0);

        // Snap each event to the nearest 30-minute slot on the target grid.
        let slot_map: HashMap<NaiveDateTime, &'static str> =
            events.iter().map(|(t, k)| (round_to_30m(*t), *k)).collect();

        let window_start = t0_local;
        // Carry forward the most-recent pre-window event so the first slot
        // can emit "H + 0.5" rather than a blank. `None` means no anchor
        // yet — the counter is only meaningful once we have one.
        let mut past: Option<(&'static str, f32)> = events
            .iter()
            .rev()
            .find(|(t, _)| *t < window_start)
            .map(|(_, k)| (*k, 0.0));

        let n_slots = hours as i64 * 2 + 1;
        let mut times: Vec<NaiveDateTime> = Vec::with_capacity(n_slots as usize);
        let mut high_low: Vec<String> = Vec::with_capacity(n_slots as usize);
        for i in 0..n_slots {
            let t = t0_local + Duration::minutes(30 * i);
            times.push(t);
            if let Some(kind) = slot_map.get(&t) {
                past = Some((*kind, 0.0));
                high_low.push((*kind).to_string());
            } else if let Some((k, n)) = past.as_mut() {
                *n += 0.5;
                high_low.push(format!("{} + {}", k, n));
            } else {
                high_low.push(String::new());
            }
        }

        // Backfill any leading blanks. This happens when no extremum
        // was detected before `window_start` AND the first slot isn't
        // itself an extremum — the forward pass above has nothing to
        // anchor on. Once we know the first real extremum's type, the
        // slots immediately before it are approaching the OPPOSITE
        // type: a tidal cycle alternates H → L → H, so the step
        // immediately before a detected "H" is implicitly post-"L".
        // We count forward from the (unknown) prior extremum, so the
        // slot right before the H gets the largest offset and the
        // earliest slot gets the smallest — the counter *increases*
        // as we walk toward the detected extremum, matching the
        // "L + n" meaning of "n hours since L".
        if let Some(first_non_blank) = high_low.iter().position(|s| !s.is_empty())
            && first_non_blank > 0
        {
            let anchor_kind = match high_low[first_non_blank].chars().next() {
                Some('H') => "L",
                Some('L') => "H",
                // Any other shape shouldn't happen — the forward pass
                // only ever emits "H"/"L" prefixes. Bail rather than
                // invent a label.
                _ => "",
            };
            if !anchor_kind.is_empty() {
                for (i, slot) in high_low.iter_mut().take(first_non_blank).enumerate() {
                    // Largest offset nearest the known extremum:
                    // slot `first_non_blank - 1` is 0.5 h past the
                    // implicit prior; slot 0 is `first_non_blank * 0.5`
                    // hours past.
                    let n = 0.5 * (first_non_blank - i) as f32;
                    *slot = format!("{} + {}", anchor_kind, n);
                }
            }
        }

        let df = DataFrame::new(vec![
            Series::new("time", times),
            Series::new("high_low", high_low),
        ])
        .log()?
        .lazy()
        .with_columns([col("time").cast(DataType::Datetime(TimeUnit::Milliseconds, None))])
        .collect()
        .log()?;
        Ok(df)
    }
}

/// Build a 30m `CurrentPrediction` by sampling `sample(t_utc) -> (speed_kt, direction_deg)`
/// at each half-hour slot and assembling the canonical DataFrame.
fn build_current_prediction(
    station: Station,
    t0_local: NaiveDateTime,
    n_slots: i64,
    mut sample: impl FnMut(NaiveDateTime) -> (f64, f64),
) -> Result<CurrentPrediction<30>> {
    let (times, speeds, dirs): (Vec<NaiveDateTime>, Vec<f64>, Vec<f64>) = (0..n_slots)
        .map(|i| {
            let t_local = t0_local + Duration::minutes(30 * i);
            let (speed, direction) = sample(local_to_utc(t_local));
            (t_local, speed, direction)
        })
        .multiunzip();

    let df = DataFrame::new(vec![
        Series::new("time", times),
        Series::new("speed", speeds),
        Series::new("direction", dirs),
    ])
    .log()?
    .lazy()
    .with_columns([col("time").cast(DataType::Datetime(TimeUnit::Milliseconds, None))])
    .collect()
    .log()?;

    CurrentPrediction::<30>::from_df(station, df)
}

fn harmonic_current_df(
    station: Station,
    t0_local: NaiveDateTime,
    t_ref_utc: NaiveDateTime,
    n_slots: i64,
) -> Result<CurrentPrediction<30>> {
    // `bin_hint: None` picks the shallowest bin — appropriate for surface
    // kayaking. Note this differs from NOAA's online currents_predictions
    // default bin, which is station-specific; validators probe it explicitly.
    let predictor = STORE
        .current_predictor(&station.id, None, t_ref_utc)
        .ok_or_else(|| anyhow!("no current harcon for station {}", station.id))
        .log()?;

    build_current_prediction(station, t0_local, n_slots, |t_utc| {
        let s = predictor.at(t_utc);
        (s.speed / CMS_PER_KNOT, s.direction)
    })
}

fn subordinate_current_df(
    station: Station,
    t0_local: NaiveDateTime,
    t_ref_utc: NaiveDateTime,
    hours: u32,
    n_slots: i64,
) -> Result<CurrentPrediction<30>> {
    let sub = STORE
        .subordinate(&station.id)
        .ok_or_else(|| anyhow!("no subordinate offsets for station {}", station.id))
        .log()?;
    let offsets: SubordinateOffsets = sub.offsets.clone();

    let flood_dir = offsets.mean_flood_dir.unwrap_or(0.0);
    let ebb_dir = offsets
        .mean_ebb_dir
        .unwrap_or_else(|| (flood_dir + 180.0).rem_euclid(360.0));

    // Pad beyond the target window so interpolation at the edges uses real
    // (not extrapolated) events.
    let pad = Duration::hours(3);
    let sim_start_utc = local_to_utc(t0_local) - pad;
    let sim_end_utc = local_to_utc(t0_local + Duration::hours(hours as i64)) + pad;
    let raw_events = cached_reference_events(
        &offsets.ref_id,
        offsets.ref_bin,
        t_ref_utc,
        sim_start_utc,
        sim_end_utc,
    )
    .log()?;
    let events = apply_offsets(&raw_events, &offsets);

    build_current_prediction(station, t0_local, n_slots, |t_utc| {
        let signed = interp_events(&events, t_utc);
        let dir = if signed >= 0.0 { flood_dir } else { ebb_dir };
        (signed.abs(), dir)
    })
}

#[derive(Debug, Clone)]
pub struct CurrentPrediction<const R: u8> {
    pub station: Station,
    pub df: DataFrame,
    // Cached column extracts. Polars `df[col].f64().unwrap().get(i)` is O(1)
    // but the per-call downcast/lookup dominates the trip-sim inner loop;
    // pulling them out once makes the hot path a single Vec index.
    // `Arc` so sweep-thread snapshots and the main trip both share the
    // underlying buffers without a per-clone alloc.
    pub speed: Arc<Vec<f64>>,
    pub direction: Arc<Vec<f64>>,
}

impl<const R: u8> CurrentPrediction<R> {
    pub fn resolution_minutes() -> u8 {
        R
    }

    pub fn resolution() -> PolarsDuration {
        PolarsDuration::parse(&format!("{}m", Self::resolution_minutes()))
    }

    /// Build a prediction from its DataFrame, extracting `speed`/`direction`
    /// into plain Vecs for use in tight loops. Every construction site goes
    /// through here so the cached vectors can never drift from `df`.
    fn from_df(station: Station, df: DataFrame) -> Result<Self> {
        let speed: Vec<f64> = df["speed"].f64().log()?.to_vec_null_aware().unwrap_left();
        let direction: Vec<f64> = df["direction"]
            .f64()
            .log()?
            .to_vec_null_aware()
            .unwrap_left();
        Ok(Self {
            station,
            df,
            speed: Arc::new(speed),
            direction: Arc::new(direction),
        })
    }

    pub fn resampled<const R2: u8>(&self) -> Result<CurrentPrediction<R2>> {
        let df = self
            .df
            .clone()
            .sort(["time"], Default::default())
            .log()?
            .upsample::<[String; 0]>(
                [],
                "time",
                CurrentPrediction::<R2>::resolution(),
                PolarsDuration::parse("0"),
            )
            .log()?
            .lazy()
            .with_columns([
                col("speed").interpolate(InterpolationMethod::Linear),
                col("direction").interpolate(InterpolationMethod::Linear),
            ])
            .collect()
            .log()?
            .fill_null(FillNullStrategy::Forward(None))
            .log()?;

        CurrentPrediction::<R2>::from_df(self.station.clone(), df)
    }
}

impl RTreeObject for Station {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.loc.lat(), self.loc.lon()])
    }
}

impl PointDistance for Station {
    fn distance_2(
        &self,
        point: &<Self::Envelope as Envelope>::Point,
    ) -> <<Self::Envelope as Envelope>::Point as Point>::Scalar {
        self.distance_to_m(point[0], point[1])
    }
}
