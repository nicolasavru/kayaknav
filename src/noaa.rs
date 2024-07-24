use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;

// use backon::ExponentialBuilder;
// use backon::Retryable;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use galileo_types::geo::impls::GeoPoint2d;
use galileo_types::geo::GeoPoint;
use galileo_types::geo::NewGeoPoint;
use jord::ellipsoidal::Ellipsoid;
use jord::GeodeticPos;
use jord::Length as jLength;
use jord::LocalFrame;
use jord::NVector;
use polars::prelude::*;
use rstar::Envelope;
use rstar::Point;
use rstar::PointDistance;
use rstar::RTreeObject;
use rstar::AABB;
use serde_json::json;

use crate::http;
use crate::http::ApiProxy;
use crate::prelude::*;

fn metadata_url(station_id: &str) -> String {
    format!("https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station_id}.json")
}

const URL_BASE: &str = concat!(
    "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
    "?time_zone=lst_ldt",
    "&units=english",
    "&application=KayakNav",
    "&format=json",
);

fn common_url(station_id: &str, begin_date: NaiveDate, hours: u32) -> String {
    let begin_date = begin_date.format("%Y%m%d");
    format!("{URL_BASE}&station={station_id}&begin_date={begin_date}&range={hours}")
}

fn current_prediction_url(
    station_id: &str,
    begin_date: NaiveDate,
    hours: u32,
    interval: &str,
    vel_type: &str,
) -> String {
    // TODO: add comment about omitting bin being sufrace-most with link.
    format!(
        "{}&product=currents_predictions&interval={interval}&vel_type={vel_type}",
        common_url(station_id, begin_date, hours)
    )
}

fn tide_prediction_url(station_id: &str, begin_date: NaiveDate, hours: u32) -> String {
    format!(
        "{}&product=predictions&interval=hilo&datum=MLLW",
        common_url(station_id, begin_date, hours)
    )
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
    pub async fn new(id: &str, api_proxy: Option<ApiProxy>) -> Result<Self> {
        let mut url = metadata_url(id);
        if let Some(api_proxy) = &api_proxy {
            url = api_proxy.proxied_url(&url);
        }

        let resp = http::fetch_json(&url).await.log()?;

        let station_obj = &resp["stations"][0];
        Ok(Self {
            id: id.to_string(),
            name: station_obj["name"]
                .as_str()
                .ok_or(anyhow!("'name' was not a string"))
                .log()?
                .to_string(),
            loc: GeoPoint2d::latlon(
                station_obj["lat"].as_f64().log()?,
                station_obj["lng"].as_f64().log()?,
            ),
            type_: if station_obj.get("type") == Some(&json!("S")) {
                StationType::Subordinate
            } else {
                StationType::Harmonic
            },
            api_proxy,
        })
    }

    pub async fn in_area(
        lat: (f64, f64),
        lon: (f64, f64),
        api_proxy: Option<ApiProxy>,
    ) -> Result<HashSet<Self>> {
        let lat = (f64::min(lat.0, lat.1), f64::max(lat.0, lat.1));
        let lon = (f64::min(lon.0, lon.1), f64::max(lon.0, lon.1));

        let mut url = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=currentpredictions".to_string();
        if let Some(api_proxy) = &api_proxy {
            url = api_proxy.proxied_url(&url);
        }

        let resp = http::fetch_json(&url).await.log()?;

        resp["stations"]
            .as_array()
            .log()?
            .iter()
            .fallible()
            .filter(|s| {
                let s_lat = s["lat"].as_f64().log()?;
                let s_lon = s["lng"].as_f64().log()?;

                Ok(lat.0 <= s_lat
                    && s_lat <= lat.1
                    && lon.0 <= s_lon
                    && s_lon <= lon.1
                   // TODO: check for "H" or "S" explicitly
                    && s["type"].as_str().log()? != "W")
            })
            .map(|s| {
                Ok(Self {
                    id: s["id"].as_str().log()?.to_string(),
                    name: s["name"].as_str().log()?.to_string(),
                    loc: GeoPoint2d::latlon(s["lat"].as_f64().log()?, s["lng"].as_f64().log()?),
                    type_: if s["type"] == json!("H") {
                        StationType::Harmonic
                    } else {
                        StationType::Subordinate
                    },
                    api_proxy: api_proxy.clone(),
                })
            })
            .collect()
    }

    #[instrument(level = "debug")]
    pub async fn current_prediction(
        &self,
        start: NaiveDate,
        hours: u32,
    ) -> Result<CurrentPrediction<30>> {
        let (interval, vel_type) = match self.type_ {
            StationType::Harmonic => ("h", "speed_dir"),
            StationType::Subordinate => ("max_slack", "default"),
        };

        let mut url = current_prediction_url(&self.id, start, hours, interval, vel_type);
        if let Some(api_proxy) = &self.api_proxy {
            url = api_proxy.proxied_url(&url);
        }

        let resp = http::fetch_json(&url).await.log()?;

        let resp_predictions = resp["current_predictions"]["cp"].as_array();

        let Some(resp_predictions) = resp_predictions else {
            // cache.delete(&format!("GET:")).await;
            Err(anyhow!(
                "Missing current predictions in response: {:?}",
                resp
            ))
            .log()?
        };

        if resp_predictions.is_empty() {
            // cache.delete(&format!("GET:")).await;
            Err(anyhow!(
                "Current predictions were empty in response: {:?}",
                resp
            ))
            .log()?
        };

        let time = Series::new(
            "time",
            resp_predictions
                .iter()
                .fallible()
                .map(|p| {
                    Ok(
                        NaiveDateTime::parse_from_str(p["Time"].as_str().log()?, "%Y-%m-%d %H:%M")
                            .log()?,
                    )
                })
                .collect::<Vec<NaiveDateTime>>()
                .log()?,
        );

        let df = match self.type_ {
            StationType::Harmonic => {
                let speed = Series::new(
                    "speed",
                    resp_predictions
                        .iter()
                        .fallible()
                        .map(|p| Ok(f64::from_str(p["Speed"].as_str().log()?).log()?))
                        .collect::<Vec<f64>>()
                        .log()?,
                );

                let direction = Series::new(
                    "direction",
                    resp_predictions
                        .iter()
                        .fallible()
                        .map(|p| Ok(p["Direction"].as_u64().log()? as f64))
                        .collect::<Vec<f64>>()
                        .log()?,
                );

                let df = DataFrame::new(vec![time, direction, speed]).log()?;

                df.sort(["time"], Default::default())
                    .log()?
                    .upsample::<[String; 0]>(
                        [],
                        "time",
                        Duration::parse("30m"),
                        Duration::parse("0"),
                    )
                    .log()?
                    .lazy()
                    .with_column(col("speed").interpolate(InterpolationMethod::Linear))
                    .collect()
                    .log()?
                    .fill_null(FillNullStrategy::Forward(None))
                    .log()?
            },
            StationType::Subordinate => {
                let speed = Series::new(
                    "speed",
                    resp_predictions
                        .iter()
                        .fallible()
                        .map(|p| p["Velocity_Major"].as_f64().log())
                        .collect::<Vec<f64>>()
                        .log()?,
                );

                let flood_direction = Series::new(
                    "flood_direction",
                    resp_predictions
                        .iter()
                        .fallible()
                        .map(|p| Ok(p["meanFloodDir"].as_u64().log()? as f64))
                        .collect::<Vec<f64>>()
                        .log()?,
                );

                let fallback_ebb_dir = flood_direction.f64().log()?.get(0).log()? + 180.0;

                let ebb_direction = Series::new(
                    "ebb_direction",
                    resp_predictions
                        .iter()
                        .map(|p| p["meanEbbDir"].as_u64().unwrap_or(fallback_ebb_dir as u64) as f64)
                        .collect::<Vec<f64>>(),
                );

                let mut df =
                    DataFrame::new(vec![time, speed, flood_direction, ebb_direction]).log()?;

                df = df
                    .lazy()
                    .with_column(col("time").dt().round(lit("30m"), "0"))
                    .collect()
                    .log()?;

                df = df
                    .sort(["time"], Default::default())
                    .log()?
                    .upsample::<[String; 0]>(
                        [],
                        "time",
                        Duration::parse("30m"),
                        Duration::parse("0"),
                    )
                    .log()?
                    .lazy()
                    .with_column(col("speed").interpolate(InterpolationMethod::Linear))
                    .collect()
                    .log()?;

                df = df.fill_null(FillNullStrategy::Forward(None)).log()?;

                df = df
                    .lazy()
                    .select([
                        col("time"),
                        col("speed"),
                        as_struct(vec![
                            col("speed"),
                            col("flood_direction"),
                            col("ebb_direction"),
                        ])
                        .map(
                            |s| {
                                let ca = s.struct_().log()?;

                                let speed = ca.field_by_name("speed").log()?;
                                let speed = speed.f64().log()?;
                                let flood_dir = ca.field_by_name("flood_direction").log()?;
                                let ebb_dir = ca.field_by_name("ebb_direction").log()?;
                                let flood_dir = flood_dir.f64().log()?;
                                let ebb_dir = ebb_dir.f64().log()?;

                                let out: Float64Chunked = speed
                                    .into_iter()
                                    .zip(flood_dir.into_iter())
                                    .zip(ebb_dir.into_iter())
                                    .map(|((opt_speed, opt_flood_dir), opt_ebb_dir)| {
                                        match (opt_speed, opt_flood_dir, opt_ebb_dir) {
                                            (Some(speed), Some(flood_dir), Some(ebb_dir)) => {
                                                Some(if speed >= 0.0 { flood_dir } else { ebb_dir })
                                            },
                                            _ => None,
                                        }
                                    })
                                    .collect();
                                Ok(Some(out.into_series()))
                            },
                            GetOutput::from_type(DataType::Float64),
                        )
                        .alias("direction"),
                    ])
                    .with_columns([
                        col("time").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    ])
                    .with_column(col("speed").abs())
                    .collect()
                    .log()?;

                df
            },
        };

        Ok(CurrentPrediction::<30> {
            station: self.clone(),
            df,
        })
    }

    pub async fn tide_prediction(&self, start: NaiveDate, hours: u32) -> Result<DataFrame> {
        let mut url = tide_prediction_url(&self.id, start, hours);
        if let Some(api_proxy) = &self.api_proxy {
            url = api_proxy.proxied_url(&url);
        }

        let resp = http::fetch_json(&url).await.log()?;

        let resp_predictions = resp["predictions"].as_array();

        let Some(resp_predictions) = resp_predictions else {
            // cache.delete(&format!("GET:")).await;
            Err(anyhow!(
                "Missing current predictions in response: {:?}",
                resp
            ))
            .log()?
        };

        if resp_predictions.is_empty() {
            // cache.delete(&format!("GET:")).await;
            Err(anyhow!(
                "Current predictions were empty in response: {:?}",
                resp
            ))
            .log()?
        };

        let time = Series::new(
            "time",
            resp_predictions
                .iter()
                .fallible()
                .map(|p| {
                    Ok(
                        NaiveDateTime::parse_from_str(p["t"].as_str().log()?, "%Y-%m-%d %H:%M")
                            .log()?,
                    )
                })
                .collect::<Vec<NaiveDateTime>>()
                .log()?,
        );

        let high_low = Series::new(
            "high_low",
            resp_predictions
                .iter()
                .fallible()
                .map(|p| Ok(p["type"].as_str().log()?.to_string()))
                .collect::<Vec<String>>()
                .log()?,
        );

        let mut df = DataFrame::new(vec![time, high_low]).log()?;

        df = df
            .lazy()
            .with_column(col("time").dt().round(lit("30m"), "0"))
            .collect()
            .log()?;

        df = df
            .sort(["time"], Default::default())
            .log()?
            .upsample::<[String; 0]>([], "time", Duration::parse("30m"), Duration::parse("0"))
            .log()?;

        df = df
            .lazy()
            .with_column(col("high_low").map(
                |s| {
                    let mut past_entry: (Option<&str>, f32) = (None, 0.0);

                    Ok(Some(
                        s.str()
                            .log()?
                            .iter()
                            .map(|entry| {
                                if let Some(entry) = entry {
                                    past_entry = (Some(entry), 0.0);
                                    return entry.to_string();
                                }

                                past_entry = (past_entry.0, past_entry.1 + 0.5);
                                format!("{} + {}", past_entry.0.log().unwrap(), past_entry.1)
                            })
                            .collect::<Series>(),
                    ))
                },
                GetOutput::from_type(DataType::String),
            ))
            .collect()
            .log()?;

        Ok(df)
    }
}

#[derive(Debug, Clone)]
pub struct CurrentPrediction<const R: u8> {
    pub station: Station,
    pub df: DataFrame,
}

impl<const R: u8> CurrentPrediction<R> {
    pub fn resolution_minutes() -> u8 {
        R
    }

    pub fn resolution() -> Duration {
        Duration::parse(&format!("{}m", Self::resolution_minutes()))
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
                Duration::parse("0"),
            )
            .log()?
            .lazy()
            .with_columns([col("speed").interpolate(InterpolationMethod::Linear)])
            .with_columns([col("direction").interpolate(InterpolationMethod::Linear)])
            .collect()
            .log()?
            .fill_null(FillNullStrategy::Forward(None))
            .log()?;

        Ok(CurrentPrediction::<R2> {
            station: self.station.clone(),
            df,
        })
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
        let point = GeodeticPos::new(
            NVector::from_lat_long_degrees(point[0], point[1]),
            jLength::ZERO,
        );
        let ned = LocalFrame::ned(point, Ellipsoid::WGS84);

        let self_point = GeodeticPos::new(
            NVector::from_lat_long_degrees(self.loc.lat(), self.loc.lon()),
            jLength::ZERO,
        );

        ned.geodetic_to_local_pos(self_point)
            .slant_range()
            .as_metres()
    }
}
