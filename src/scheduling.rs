use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;

use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDateTime;
use chrono::TimeDelta;
use chrono::Timelike;
use galileo::layer::feature_layer::FeatureLayer;
use galileo_types::cartesian::Point2d;
use galileo_types::geo::GeoPoint;
use galileo_types::geometry_type::CartesianSpace2d;
use itertools::Itertools;
use jord::ellipsoidal::Ellipsoid;
use jord::Angle;
use jord::GeodeticPos;
use jord::LatLong;
use jord::Length as jLength;
use jord::LocalFrame;
use jord::LocalPositionVector;
use jord::NVector;
use lru::LruCache;
use ordered_float::OrderedFloat;
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
use crate::features::Waypoint;
use crate::features::WaypointSymbol;
use crate::features::WaypointType;
use crate::noaa::CurrentPrediction;
use crate::noaa::Station;
use crate::prelude::*;
use crate::run_ui::WeekdayFlags;

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
    current_predictions: &HashMap<Station, CurrentPrediction<5>>,
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

    let start = GeodeticPos::new(
        NVector::from_lat_long_degrees(start.lat(), start.lon()),
        jLength::ZERO,
    );

    let end = GeodeticPos::new(
        NVector::from_lat_long_degrees(end.lat(), end.lon()),
        jLength::ZERO,
    );

    let ned = LocalFrame::ned(start, Ellipsoid::WGS84);
    let delta = ned.geodetic_to_local_pos(end);

    let mut time_idx = start_time_idx;

    let mut step_start = start;
    let step_remaining_delta = ned.geodetic_to_local_pos(end);
    let mut distance_remaining = Length::new::<meter>(delta.slant_range().as_metres());
    let mut total_time = Time::new::<hour>(0.0);
    let mut total_distance = Length::new::<meter>(0.0);

    while distance_remaining > Length::new::<meter>(0.0) {
        let l_frame = LocalFrame::local_level(delta.azimuth(), step_start, Ellipsoid::WGS84);

        let ll_step_start = LatLong::from_nvector(step_start.horizontal_position());
        let station = nn_calc.nearest_neighbor(ll_step_start);
        let prediction = &current_predictions[&station];

        if time_idx >= prediction.df.height() {
            return None;
        }

        let current_speed = prediction.df["speed"].f64().unwrap().get(time_idx).unwrap();

        let current_direction = Angle::from_degrees(
            prediction.df["direction"]
                .f64()
                .unwrap()
                .get(time_idx)
                .unwrap(),
        );

        let angle_delta = step_remaining_delta.azimuth() - current_direction;
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

    pub fn nearest_neighbor(&mut self, point: LatLong) -> Station {
        let lat = point.latitude().as_degrees();
        let lon = point.longitude().as_degrees();

        if let Some(p) = self.cache.get(&(OrderedFloat(lat), OrderedFloat(lon))) {
            return p.clone();
        }

        let station = self.tree.nearest_neighbor(&[lat, lon]).unwrap();
        self.cache
            .put((OrderedFloat(lat), OrderedFloat(lon)), station.clone());
        station.clone()
    }
}

#[derive(Clone)]
pub struct Trip {
    pub waypoints: Vec<Waypoint>,
    pub speed: Velocity,
    pub waypoint_layer:
        Arc<RwLock<FeatureLayer<Point2d, Waypoint, WaypointSymbol, CartesianSpace2d>>>,
    pub stations: Vec<Station>,
    pub current_predictions_30m: HashMap<Station, CurrentPrediction<30>>,
    pub current_predictions_5m: HashMap<Station, CurrentPrediction<5>>,
    pub weekdays: WeekdayFlags,
    pub daytime: bool,
    results: HashMap<usize, Option<TripResult>>,
    sweep_result: Option<DataFrame>,
    nn_calc: NearestNeighborCalculator,
}

impl Trip {
    pub fn new(
        speed: Velocity,
        waypoint_layer: Arc<
            RwLock<FeatureLayer<Point2d, Waypoint, WaypointSymbol, CartesianSpace2d>>,
        >,
        current_predictions_30m: Vec<CurrentPrediction<30>>,
    ) -> Result<Self> {
        let mut stations: Vec<Station> = current_predictions_30m
            .iter()
            .map(|p| p.station.clone())
            .collect();

        stations.sort_unstable_by_key(|station| {
            (
                OrderedFloat(-1.0 * station.loc.lat()),
                OrderedFloat(station.loc.lon()),
            )
        });

        let current_predictions_5m: Vec<CurrentPrediction<5>> = current_predictions_30m
            .iter()
            .fallible()
            .map(CurrentPrediction::resampled::<5>)
            .collect()?;

        let current_predictions_30m = HashMap::from_iter(
            current_predictions_30m
                .into_iter()
                .map(|p| (p.station.clone(), p)),
        );

        let current_predictions_5m = HashMap::from_iter(
            current_predictions_5m
                .into_iter()
                .map(|p| (p.station.clone(), p)),
        );

        Ok(Self {
            waypoints: Vec::new(),
            speed,
            waypoint_layer,
            stations: stations.clone(),
            current_predictions_30m,
            current_predictions_5m,
            weekdays: WeekdayFlags::empty(),
            daytime: false,
            results: HashMap::new(),
            sweep_result: None,
            nn_calc: NearestNeighborCalculator::new(&stations),
        })
    }

    fn clear_cache(&mut self) {
        self.results.clear();
        self.sweep_result = None;
    }

    pub fn add_waypoint(&mut self, waypoint: Waypoint) {
        self.waypoints.push(waypoint);
        self.waypoint_layer
            .write()
            .unwrap()
            .features_mut()
            .insert(waypoint);
        self.clear_cache();
    }

    pub fn remove_waypoint(&mut self, idx: usize) {
        self.waypoints.remove(idx);
        self.waypoint_layer
            .write()
            .unwrap()
            .features_mut()
            .remove(idx);
        self.clear_cache();
    }

    pub fn clear_waypoints(&mut self) {
        self.waypoints.clear();
        features::clear_features(self.waypoint_layer.clone());
        self.clear_cache();
    }

    pub fn set_speed(&mut self, speed: Velocity) {
        self.speed = speed;
        self.clear_cache()
    }

    pub fn set_weekdays(&mut self, weekdays: WeekdayFlags) {
        if self.weekdays != weekdays {
            self.weekdays = weekdays;
            self.clear_cache();
        }
    }

    pub fn set_daytime(&mut self, daytime: bool) {
        if self.daytime != daytime {
            self.daytime = daytime;
            self.clear_cache();
        }
    }

    pub fn calculate(&mut self, mut start_time_idx: usize) -> Option<TripResult> {
        self.results
            .entry(start_time_idx)
            .or_insert_with(|| {
                let mut steps: Vec<StepResult> = vec![StepResult::default()];

                for (a, b) in self.waypoints[..].iter().tuple_windows() {
                    let res = calculate_step(
                        a,
                        b,
                        self.speed,
                        &self.current_predictions_5m,
                        start_time_idx,
                        &mut self.nn_calc,
                    );
                    if let Some(res) = res {
                        start_time_idx += res.time_steps;
                        steps.push(res)
                    } else {
                        return None;
                    }
                }

                Some(TripResult { steps })
            })
            .clone()
    }

    pub fn sweep(&mut self) -> DataFrame {
        match &self.sweep_result {
            Some(sweep_result) => sweep_result.clone(),
            None => {
                // TODO: derive from arguments
                let internal_time_step = Time::new::<minute>(5.0);
                let time_ratio: Ratio = Time::new::<minute>(30.0) / internal_time_step;
                let mut time_idx_vec: Vec<(usize, NaiveDateTime)> =
                    self.current_predictions_30m.values().next().unwrap().df["time"]
                        .datetime()
                        .unwrap()
                        .to_vec_null_aware()
                        .unwrap_left()
                        .iter()
                        .map(|ts| DateTime::from_timestamp_millis(*ts).unwrap().naive_utc())
                        .enumerate()
                        .filter(|(_, dt)| {
                            self.weekdays.contains(
                                WeekdayFlags::from_bits(1 << dt.weekday().num_days_from_monday())
                                    .unwrap(),
                            )
                        })
                        .collect();

                if self.daytime {
                    time_idx_vec.retain(|(_, dt)| dt.hour() >= 8);
                }

                let mut trip_results: Vec<_> = time_idx_vec
                    .iter()
                    .map(|(idx, dt)| (idx, dt, self.calculate(time_ratio.value as usize * idx)))
                    .filter(|(_, _, result)| result.is_some())
                    .map(|(i, dt, result)| (i, dt, result.unwrap()))
                    .collect();

                if self.daytime {
                    trip_results.retain(|(_, dt, result)| {
                        (**dt + TimeDelta::seconds(result.time().get::<second>() as i64))
                            < dt.date().and_hms_opt(21, 0, 0).unwrap()
                    });
                }

                let time_idx_vec: Vec<usize> = trip_results.iter().map(|(i, _, _)| **i).collect();

                let trip_results: Vec<_> = trip_results
                    .iter()
                    .map(|(_, _, result)| result.time().value)
                    .collect();

                let idx = Series::new(
                    "idx",
                    time_idx_vec.iter().map(|i| *i as u64).collect::<Vec<u64>>(),
                );
                let duration = Series::new("duration", trip_results.clone());

                let mut quant_df = DataFrame::new(vec![duration]).unwrap();

                quant_df = quant_df
                    .lazy()
                    .with_columns([
                        col("duration").quantile(lit(0.2), QuantileInterpolOptions::Nearest)
                    ])
                    .collect()
                    .unwrap();

                let percentile = quant_df["duration"].f64().unwrap().get(0).unwrap();
                let duration = Series::new("duration", trip_results);
                let mut df = DataFrame::new(vec![idx, duration]).unwrap();
                df = df
                    .lazy()
                    .filter(col("duration").lt_eq(lit(percentile)))
                    .collect()
                    .unwrap();

                self.sweep_result = Some(df);
                self.sweep_result.as_ref().unwrap().clone()
            },
        }
    }
}
